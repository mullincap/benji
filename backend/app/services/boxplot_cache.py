"""
backend/app/services/boxplot_cache.py
========================================
Per-symbol 24h × 5m kline distribution + linear-regression slope,
Redis-cached.

Drives the Live tab's Trailing Distribution box plot strip
(Data Dictionary §10). The on-demand pattern matches the EMA cache
(`backend/app/services/ema_cache.py`):

    boxplot:{binance_symbol}:{last_5m_close_ts}

`last_5m_close_ts` (open_time of the most recent CLOSED 5m bar) is part
of the key so the cache rotates automatically at every 5m bar close.
TTL = ~10 minutes (2× the 5m cadence) is the secondary expiry.

----

Trend σ-normalization deviates from Data Dictionary §10 Computation
Note D's literal formula. The note writes:

    slope_σ = β × bars_per_window / σ_typical

…which yields very large σ values for typical 24h drifts (a 1% move
over 288 bars at σ_typical=0.1%/bar gives ~10σ — far above the +1.5σ
"strong up" threshold for almost any non-flat day).

The mockup's displayed values (BTC +2.1σ, AIXBT −2.6σ, etc.) are
consistent with the random-walk-normalized form:

    slope_σ = β × √N / σ_typical

…which expresses the trend's total move in units of expected √N drift,
the standard finance trend-vs-noise framing. We compute this form so
the bin thresholds in §10 ("+1.5σ strong up" etc.) match the visual
intent of the mockup. If a future revision wants the literal Note D
formula, only this function (and the bin thresholds) need updating.
"""

from __future__ import annotations

import json
import logging
import math
import statistics
import time
from typing import Any

import redis as redis_lib

from ..core.config import settings
from .exchanges.binance_market import BinanceMarketClient

log = logging.getLogger(__name__)

KLINE_TF = "5m"
KLINE_LIMIT = 288             # 24h × 5m
TF_MS = 5 * 60 * 1000
TTL_S = 10 * 60               # 2× cadence
MIN_BARS = 50                 # per spec — insufficient-data threshold

TrendDirection = str  # one of: 'strong-up','up','flat','down','strong-down'

_redis: redis_lib.Redis | None = None


def _get_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = redis_lib.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


def latest_closed_5m_open_ms(now_ms: int | None = None) -> int:
    """Open-time (ms) of the most recently closed 5m bar."""
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    in_progress_open = (now_ms // TF_MS) * TF_MS
    return in_progress_open - TF_MS


def classify_trend(slope_sigma: float) -> TrendDirection:
    if slope_sigma > 1.5:
        return "strong-up"
    if slope_sigma > 0.5:
        return "up"
    if slope_sigma >= -0.5:
        return "flat"
    if slope_sigma >= -1.5:
        return "down"
    return "strong-down"


def compute_distribution(closes: list[float]) -> dict[str, Any]:
    """Percentiles + window min/max + regression slope σ.

    Caller passes a list of CLOSE prices (in chronological order) over
    the 24h window. Returns a dict with all the statistical fields the
    box-plot SVG needs.
    """
    n = len(closes)
    sorted_c = sorted(closes)

    def pct(p: float) -> float:
        # Nearest-rank percentile — rough but consistent with Binance's
        # exchange charts and avoids interpolation surprises on
        # discontinuous price grids (small-cap perps with wide ticks).
        idx = max(0, min(n - 1, int(round(p / 100 * (n - 1)))))
        return sorted_c[idx]

    # Linear regression slope: β = Σ(xᵢ − x̄)(yᵢ − ȳ) / Σ(xᵢ − x̄)²
    x_mean = (n - 1) / 2.0
    y_mean = sum(closes) / n
    num = sum((i - x_mean) * (c - y_mean) for i, c in enumerate(closes))
    den = sum((i - x_mean) ** 2 for i in range(n))
    beta = (num / den) if den > 0 else 0.0

    # σ_typical = stdev of bar-to-bar absolute price moves.
    deltas = [closes[i] - closes[i - 1] for i in range(1, n)]
    sigma_typical = statistics.pstdev(deltas) if len(deltas) >= 2 else 0.0

    # Random-walk-normalized slope σ (see module-level note).
    if sigma_typical > 0 and n > 1:
        slope_sigma = beta * math.sqrt(n) / sigma_typical
    else:
        slope_sigma = 0.0

    return {
        "p5":  pct(5),
        "p25": pct(25),
        "p50": pct(50),
        "p75": pct(75),
        "p95": pct(95),
        "min": min(closes),
        "max": max(closes),
        "slope_sigma": slope_sigma,
        "trend": classify_trend(slope_sigma),
    }


def compute_and_cache_boxplot(
    *, binance_symbol: str | None,
    client: BinanceMarketClient | None = None,
) -> dict[str, Any]:
    """Cached 24h × 5m percentiles + regression slope for one symbol.

    Returns:
      success:
        {p5, p25, p50, p75, p95, min, max, slope_sigma, trend,
         last_close_ts, computed_at, reason: null}
      no listing / no data / partial:
        {... null fields ..., reason: 'not_listed' | 'insufficient_data'
         | 'fetch_error'}
    """
    computed_at = int(time.time() * 1000)
    if not binance_symbol:
        return {**_null_result(), "computed_at": computed_at, "reason": "not_listed"}

    bar_open_ms = latest_closed_5m_open_ms(computed_at)
    cache_key = f"boxplot:{binance_symbol}:{bar_open_ms}"

    r = _get_redis()
    try:
        cached = r.get(cache_key)
    except redis_lib.RedisError as e:
        log.warning("boxplot_cache: Redis GET failed: %s", e)
        cached = None

    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass  # fall through

    if client is None:
        client = BinanceMarketClient()
    try:
        candles = client.klines(binance_symbol, KLINE_TF, limit=KLINE_LIMIT)
    except Exception as e:
        log.warning("boxplot_cache: klines fetch failed for %s: %s", binance_symbol, e)
        return {**_null_result(), "computed_at": computed_at, "reason": "fetch_error"}

    if not candles:
        return _miss_and_cache(r, cache_key, computed_at, "insufficient_data")

    closed = [c for c in candles if int(c[6]) < computed_at]
    if len(closed) < MIN_BARS:
        return _miss_and_cache(r, cache_key, computed_at, "insufficient_data")

    closes = [float(c[4]) for c in closed]
    dist = compute_distribution(closes)
    last_close_ts = int(closed[-1][0])

    result = {
        **dist,
        "last_close_ts": last_close_ts,
        "computed_at": computed_at,
        "reason": None,
    }
    _cache_set(r, cache_key, result)
    return result


def _null_result() -> dict[str, Any]:
    return {
        "p5": None, "p25": None, "p50": None, "p75": None, "p95": None,
        "min": None, "max": None,
        "slope_sigma": None, "trend": None,
        "last_close_ts": None,
    }


def _miss_and_cache(r: redis_lib.Redis, key: str, computed_at: int, reason: str) -> dict[str, Any]:
    result = {**_null_result(), "computed_at": computed_at, "reason": reason}
    _cache_set(r, key, result)
    return result


def _cache_set(r: redis_lib.Redis, key: str, value: dict) -> None:
    try:
        r.setex(key, TTL_S, json.dumps(value, default=str))
    except redis_lib.RedisError as e:
        log.warning("boxplot_cache: SETEX %s failed: %s", key, e)


# ── Display classification helpers ─────────────────────────────────────

def mark_dot_class(*, mark: float, p25: float, p50: float, p75: float, side: str) -> str:
    """Color class for the mark dot per Data Dictionary §10:

      Long, mark > p50  OR  short, mark < p50   → 'good'
      Long, mark < p25  OR  short, mark > p75   → 'bad'
      Otherwise                                 → 'neu'
    """
    if side == "long":
        if mark > p50:
            return "good"
        if mark < p25:
            return "bad"
        return "neu"
    # short
    if mark < p50:
        return "good"
    if mark > p75:
        return "bad"
    return "neu"


def trend_color(trend: TrendDirection, side: str) -> str:
    """Trend arrow color: aligned with position = green, against = red,
    flat = amber. Maps the trend direction to long/short-relative
    alignment classes the frontend uses."""
    if trend == "flat":
        return "neu"
    is_up = trend in ("up", "strong-up")
    aligned = (side == "long" and is_up) or (side == "short" and not is_up)
    return "good" if aligned else "bad"

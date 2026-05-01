"""
backend/app/services/ema_cache.py
====================================
Per-(symbol, timeframe) EMA20 computation with Redis caching.

Drives the Live tab's MA Alignment Heatmap (Data Dictionary §11). The
spec calls for a Celery worker that fires on bar close per timeframe;
v1 ships only the on-demand path — when a cache miss occurs at request
time, the cache is populated synchronously from a Binance kline fetch.
TTLs aligned to refresh tier (e.g. 5m TF → 10m TTL) so subsequent calls
within the bar are cache hits.

Cache key format:
    ema:{symbol}:{tf}:ema20:{bar_close_ts}

`bar_close_ts` (the open_time of the most recently closed bar at this
TF) is part of the key so that the bar-close transition is automatic:
once the next bar closes, the consumer's computed key changes and the
old key expires naturally.

Symbol-not-listed and insufficient-history (< 20 closed bars) fall
back to a `reason`-tagged null result that the heatmap renders as a
neutral "—" cell.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import redis as redis_lib

from ..core.config import settings
from .exchanges.binance_market import BinanceMarketClient

log = logging.getLogger(__name__)

# Timeframes the heatmap renders. Order matters — drives column order
# in the response.
TF_ORDER: list[str] = ["5m", "15m", "30m", "1h", "4h", "8h", "1d"]

# TF → milliseconds. Used to compute bar_close_ts and TTL.
TF_MS: dict[str, int] = {
    "5m":  5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h":  60 * 60 * 1000,
    "4h":  4 * 60 * 60 * 1000,
    "8h":  8 * 60 * 60 * 1000,
    "1d":  24 * 60 * 60 * 1000,
}

# Number of bars to pull. 200 ensures EMA20 has converged regardless of
# starting condition; well within Binance kline weight budget (1 weight
# per ≤500 limit).
KLINE_LIMIT = 200

# EMA period.
EMA_N = 20

# Required minimum bars to compute an EMA20. The first 20 bars are used
# to seed the EMA; need at least 20 closed bars for a meaningful value.
MIN_BARS = EMA_N

# Module-level Redis singleton (project pattern — same as manager_live).
_redis: redis_lib.Redis | None = None


def _get_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = redis_lib.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


def latest_closed_bar_open_ms(tf: str, now_ms: int | None = None) -> int:
    """Open-time (ms) of the most recently CLOSED bar at this TF.

    Aligned to UTC midnight for daily; aligned to TF boundaries for
    intra-day. If `now` falls inside the in-progress bar, the previous
    bar's open_ms is returned.
    """
    tf_ms = TF_MS[tf]
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    # Open of in-progress bar:
    in_progress_open = (now_ms // tf_ms) * tf_ms
    # Last closed bar's open is one TF earlier.
    return in_progress_open - tf_ms


def compute_ema20_from_closes(closes: list[float]) -> float:
    """Standard EMA20: seed with SMA of first 20 closes, then iterate
    `α * close + (1-α) * prev_ema` with α = 2 / (N+1)."""
    alpha = 2.0 / (EMA_N + 1)
    seed = sum(closes[:EMA_N]) / EMA_N
    ema = seed
    for c in closes[EMA_N:]:
        ema = alpha * c + (1.0 - alpha) * ema
    return ema


def compute_and_cache_ema20(
    *, binance_symbol: str | None, tf: str,
    client: BinanceMarketClient | None = None,
) -> dict[str, Any]:
    """Resolve cached EMA20 for (binance_symbol, tf); fetch + cache on miss.

    Returns one of:
      {"ema_value": float, "last_close_ts": int, "computed_at": int,
       "reason": null}
      {"ema_value": null, "reason": "not_listed" | "insufficient_history"
       | "fetch_error", "computed_at": int}

    `binance_symbol` may be None when the position's base symbol has no
    Binance USDM listing in our registry — short-circuits to
    reason='not_listed' without an API call.
    """
    computed_at = int(time.time() * 1000)
    if not binance_symbol:
        return {
            "ema_value": None,
            "last_close_ts": None,
            "computed_at": computed_at,
            "reason": "not_listed",
        }

    bar_open_ms = latest_closed_bar_open_ms(tf, computed_at)
    cache_key = f"ema:{binance_symbol}:{tf}:ema20:{bar_open_ms}"

    r = _get_redis()
    try:
        cached = r.get(cache_key)
    except redis_lib.RedisError as e:
        log.warning("ema_cache: Redis GET %s failed: %s", cache_key, e)
        cached = None

    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass  # fall through to fresh fetch

    # Miss — fetch from Binance.
    if client is None:
        client = BinanceMarketClient()
    try:
        # +1 ensures we have at least one closed bar of each timeframe
        # even when we filter out the in-progress bar.
        candles = client.klines(binance_symbol, tf, limit=KLINE_LIMIT)
    except Exception as e:
        log.warning("ema_cache: klines fetch failed for %s/%s: %s", binance_symbol, tf, e)
        return {
            "ema_value": None,
            "last_close_ts": None,
            "computed_at": computed_at,
            "reason": "fetch_error",
        }

    if not candles:
        result = {
            "ema_value": None,
            "last_close_ts": None,
            "computed_at": computed_at,
            "reason": "insufficient_history",
        }
        _cache_set(r, cache_key, result, ttl_s=_ttl_for(tf))
        return result

    # Drop the in-progress bar (close_time > now). Binance's kline
    # close_time is the inclusive end of the bar (open_time + tf_ms - 1).
    closed = [c for c in candles if int(c[6]) < computed_at]
    if len(closed) < MIN_BARS:
        result = {
            "ema_value": None,
            "last_close_ts": None,
            "computed_at": computed_at,
            "reason": "insufficient_history",
        }
        _cache_set(r, cache_key, result, ttl_s=_ttl_for(tf))
        return result

    closes = [float(c[4]) for c in closed]
    ema = compute_ema20_from_closes(closes)
    last_close_ts = int(closed[-1][0])  # open_time of the last closed bar

    result = {
        "ema_value": ema,
        "last_close_ts": last_close_ts,
        "computed_at": computed_at,
        "reason": None,
    }
    _cache_set(r, cache_key, result, ttl_s=_ttl_for(tf))
    return result


def _ttl_for(tf: str) -> int:
    """TTL = TF × 2 (per spec). Bar close transitions invalidate via
    key change; TTL is the secondary expiry guarantee."""
    return max(2 * TF_MS[tf] // 1000, 60)  # min 60s


def _cache_set(r: redis_lib.Redis, key: str, value: dict, *, ttl_s: int) -> None:
    try:
        r.setex(key, ttl_s, json.dumps(value, default=str))
    except redis_lib.RedisError as e:
        log.warning("ema_cache: SETEX %s failed: %s", key, e)


# ── Tier classification ────────────────────────────────────────────────

def alignment_tier(distance_pct: float, side: str) -> str:
    """Bin a signed distance % into one of seven heatmap tiers.

    For LONGs: positive distance = aligned; for SHORTs: invert.

    Tiers (matching mockup CSS):
      ma-aligned-strong  (|d| > 2 and aligned)
      ma-aligned-mid     (0.5 < |d| ≤ 2 and aligned)
      ma-aligned-soft    (0 < |d| ≤ 0.5 and aligned)
      ma-neutral         (|d| ≤ 0.05)
      ma-against-soft    (0 < |d| ≤ 0.5 and against)
      ma-against-mid     (0.5 < |d| ≤ 2 and against)
      ma-against-strong  (|d| > 2 and against)
    """
    abs_d = abs(distance_pct)
    if abs_d <= 0.05:
        return "neutral"
    aligned = (side == "long" and distance_pct > 0) or (side == "short" and distance_pct < 0)
    if aligned:
        if abs_d > 2:
            return "aligned-strong"
        if abs_d > 0.5:
            return "aligned-mid"
        return "aligned-soft"
    else:
        if abs_d > 2:
            return "against-strong"
        if abs_d > 0.5:
            return "against-mid"
        return "against-soft"

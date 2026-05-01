"""
backend/app/services/ema_cache.py
====================================
Per-(symbol, timeframe, variant) moving-average computation with Redis
caching. Drives the Live tab's MA Alignment Heatmap (Data Dictionary §11).

Filename retains "ema_cache" for historical reasons; the module now
handles SMA variants too. Variant is selected per-request via the
endpoint's `ma_variant` query parameter.

Currently supported variants (declared in MA_VARIANTS):
    sma20, sma60, ema20

Cache key format:
    ma:{symbol}:{tf}:{variant}:{bar_close_ts}

`bar_close_ts` (the open_time of the most recently closed bar at this
TF) is part of the key so that the bar-close transition is automatic:
once the next bar closes, the consumer's computed key changes and the
old key expires naturally.

Symbol-not-listed and insufficient-history (< period closed bars)
fall back to a `reason`-tagged null result that the heatmap renders
as a neutral "—" cell.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Literal

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

# Number of bars to pull. 200 covers the longest declared variant
# (60-period) with plenty of seed room for EMA convergence; well
# within Binance kline weight budget (1 weight per ≤500 limit).
KLINE_LIMIT = 200


# ── Variant registry ──────────────────────────────────────────────────

MAVariant = Literal["sma20", "sma60", "ema20"]

# variant_id → (kind, period). `kind` ∈ {"sma","ema"}; period is the
# number of closed bars the MA averages over. Adding a new variant is
# a one-line config edit; no other code change required.
MA_VARIANTS: dict[str, tuple[str, int]] = {
    "sma20": ("sma", 20),
    "sma60": ("sma", 60),
    "ema20": ("ema", 20),
}

DEFAULT_VARIANT: MAVariant = "sma20"

# Variant set the celery beat warmer pre-computes on each bar close.
# Listing both SMA variants here means a panel toggle is always served
# from a hot cache — first-render lag at toggle time is the trade-off
# for warming both. EMA20 is intentionally omitted from the warmer set
# until/unless someone wires a UI option for it.
WARM_VARIANTS: tuple[MAVariant, ...] = ("sma20", "sma60")


def variant_period(variant: str) -> int:
    """Number of closed bars required for the given variant."""
    return MA_VARIANTS[variant][1]


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
    in_progress_open = (now_ms // tf_ms) * tf_ms
    return in_progress_open - tf_ms


# ── Math kernels ──────────────────────────────────────────────────────


def _sma_from_closes(closes: list[float], n: int) -> float:
    """Simple moving average — arithmetic mean of the last n closes.
    Caller guarantees len(closes) >= n; the endpoint's MIN_BARS guard
    short-circuits earlier with `insufficient_history` reason."""
    return sum(closes[-n:]) / n


def _ema_from_closes(closes: list[float], n: int) -> float:
    """Standard EMA: seed with SMA of first n closes, then iterate
    α·close + (1−α)·prev with α = 2/(n+1)."""
    alpha = 2.0 / (n + 1)
    seed = sum(closes[:n]) / n
    ema = seed
    for c in closes[n:]:
        ema = alpha * c + (1.0 - alpha) * ema
    return ema


def _compute_ma(closes: list[float], variant: str) -> float:
    kind, period = MA_VARIANTS[variant]
    if kind == "sma":
        return _sma_from_closes(closes, period)
    if kind == "ema":
        return _ema_from_closes(closes, period)
    raise ValueError(f"unknown MA kind: {kind}")


# ── Compute + cache ───────────────────────────────────────────────────


def compute_and_cache_ma(
    *, binance_symbol: str | None, tf: str, variant: str,
    client: BinanceMarketClient | None = None,
) -> dict[str, Any]:
    """Resolve cached MA value for (binance_symbol, tf, variant);
    fetch + cache on miss.

    Returns one of:
      {"ma_value": float, "last_close_ts": int, "computed_at": int,
       "reason": null}
      {"ma_value": null, "reason": "not_listed" | "insufficient_history"
       | "fetch_error", "computed_at": int}

    `binance_symbol` may be None when the position's base symbol has no
    Binance USDM listing — short-circuits to reason='not_listed' without
    an API call.

    Raises ValueError on unknown variant; the endpoint validates the
    query param against MA_VARIANTS before reaching this layer.
    """
    if variant not in MA_VARIANTS:
        raise ValueError(f"unknown ma variant: {variant!r}")
    period = variant_period(variant)

    computed_at = int(time.time() * 1000)
    if not binance_symbol:
        return {
            "ma_value": None,
            "last_close_ts": None,
            "computed_at": computed_at,
            "reason": "not_listed",
        }

    bar_open_ms = latest_closed_bar_open_ms(tf, computed_at)
    cache_key = f"ma:{binance_symbol}:{tf}:{variant}:{bar_open_ms}"

    r = _get_redis()
    try:
        cached = r.get(cache_key)
    except redis_lib.RedisError as e:
        log.warning("ma_cache: Redis GET %s failed: %s", cache_key, e)
        cached = None
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass  # fall through to fresh fetch

    if client is None:
        client = BinanceMarketClient()
    try:
        candles = client.klines(binance_symbol, tf, limit=KLINE_LIMIT)
    except Exception as e:
        log.warning(
            "ma_cache: klines fetch failed for %s/%s: %s",
            binance_symbol, tf, e,
        )
        return {
            "ma_value": None,
            "last_close_ts": None,
            "computed_at": computed_at,
            "reason": "fetch_error",
        }

    closed = [c for c in candles if int(c[6]) < computed_at] if candles else []
    if len(closed) < period:
        result = {
            "ma_value": None,
            "last_close_ts": None,
            "computed_at": computed_at,
            "reason": "insufficient_history",
        }
        _cache_set(r, cache_key, result, ttl_s=_ttl_for(tf))
        return result

    closes = [float(c[4]) for c in closed]
    ma_value = _compute_ma(closes, variant)
    last_close_ts = int(closed[-1][0])

    result = {
        "ma_value": ma_value,
        "last_close_ts": last_close_ts,
        "computed_at": computed_at,
        "reason": None,
    }
    _cache_set(r, cache_key, result, ttl_s=_ttl_for(tf))
    return result


def _ttl_for(tf: str) -> int:
    """TTL = TF × 2. Bar-close transitions invalidate via key change;
    TTL is the secondary expiry guarantee."""
    return max(2 * TF_MS[tf] // 1000, 60)


def _cache_set(r: redis_lib.Redis, key: str, value: dict, *, ttl_s: int) -> None:
    try:
        r.setex(key, ttl_s, json.dumps(value, default=str))
    except redis_lib.RedisError as e:
        log.warning("ma_cache: SETEX %s failed: %s", key, e)


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

    Same thresholds for every variant. Longer-period MAs (e.g. SMA60)
    will have wider typical distances so the strong tiers light up
    more often — that's an honest read, not a bug. Per-variant
    rescaling is a later refinement if it becomes a usability issue.
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

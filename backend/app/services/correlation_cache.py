"""
backend/app/services/correlation_cache.py
============================================
Pairwise position-PnL correlation matrix + diversification-aware
effective-N gauge. Drives the Live tab's Coverage Matrix (§8) and
Effective-N gauge (§9a).

Two-layer cache:

  1. Per-symbol 30d daily-close series — keyed on
     `daily_closes:{binance_symbol}:{day_close_ts}`, TTL 25h. Closes
     change once a day at 00:00 UTC (Binance's daily kline boundary)
     so a single day-spanning cache key is enough; the TTL guarantees
     expiry within the day after.

  2. Per-(position-set, hour) compute result — keyed on
     `corr_matrix:{position_set_hash}:{1h_bar_close_ts}`, TTL 2h.
     The bar_close_ts in the key auto-rotates on each 1H bar close
     (the §8 T5 trigger).

Compute step at request time:
  * Pull cached daily closes for each symbol (or fetch on miss).
  * Build per-position dollar-PnL series:
        pnl_t = notional × ((close_t − close_{t−1}) / close_{t−1}) × dir_sign
    Returns instead of absolute deltas keep the math numerically stable
    on both BTC ($90k) and NEIRO ($0.00009) without hitting float
    underflow on the small-cap side.
  * Pearson correlation pairwise → matrix (signs flip naturally for
    short positions because dir_sign is in the series).

Effective-N (deviation from §9a Note B literal — see comment in
compute_effective_n below).
"""

from __future__ import annotations

import hashlib
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

DAILY_TF = "1d"
DAILY_LIMIT = 31  # 30 closes + 1 to compute deltas
DAILY_MS = 24 * 60 * 60 * 1000
ONE_HOUR_MS = 60 * 60 * 1000
DAILY_CLOSES_TTL_S = 25 * 60 * 60  # 25h, slightly past one daily-bar window
MATRIX_TTL_S = 2 * 60 * 60         # 2h
MIN_DAYS = 14                      # per spec — insufficient-history threshold
TARGET_DAYS = 30

_redis: redis_lib.Redis | None = None


def _get_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = redis_lib.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


def latest_daily_close_ms(now_ms: int | None = None) -> int:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    return ((now_ms // DAILY_MS) - 1) * DAILY_MS


def latest_hourly_close_ms(now_ms: int | None = None) -> int:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    return ((now_ms // ONE_HOUR_MS) - 1) * ONE_HOUR_MS


# ── Per-symbol daily-closes cache ─────────────────────────────────────

def get_30d_daily_closes(
    binance_symbol: str | None,
    *, client: BinanceMarketClient | None = None,
) -> list[float] | None:
    """Returns last 30 daily closes for the symbol (oldest → newest), or
    None if the symbol isn't listed / has insufficient history."""
    if not binance_symbol:
        return None

    day_close_ms = latest_daily_close_ms()
    cache_key = f"daily_closes:{binance_symbol}:{day_close_ms}"

    r = _get_redis()
    try:
        cached = r.get(cache_key)
    except redis_lib.RedisError as e:
        log.warning("correlation_cache: Redis GET %s failed: %s", cache_key, e)
        cached = None

    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass  # fall through

    if client is None:
        client = BinanceMarketClient()
    try:
        candles = client.klines(binance_symbol, DAILY_TF, limit=DAILY_LIMIT)
    except Exception as e:
        log.warning("correlation_cache: klines fetch failed for %s: %s", binance_symbol, e)
        return None

    if not candles:
        return None

    now_ms = int(time.time() * 1000)
    closed = [c for c in candles if int(c[6]) < now_ms]
    if len(closed) < MIN_DAYS:
        # Cache the partial series so we don't hammer Binance for symbols
        # that genuinely lack history; on next-day boundary the cache key
        # rotates and a fresh fetch will pick up new bars.
        return None

    closes = [float(c[4]) for c in closed[-TARGET_DAYS:]]
    try:
        r.setex(cache_key, DAILY_CLOSES_TTL_S, json.dumps(closes))
    except redis_lib.RedisError as e:
        log.warning("correlation_cache: SETEX %s failed: %s", cache_key, e)
    return closes


# ── Pure-Python statistics helpers ────────────────────────────────────

def _pearson(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2 or n != len(ys):
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / n)
    sy = math.sqrt(sum((y - my) ** 2 for y in ys) / n)
    if sx == 0 or sy == 0:
        return 0.0
    return cov / (sx * sy)


def _pnl_series(closes: list[float], notional: float, side: str) -> list[float]:
    """Daily $ PnL series.

    Returns rather than absolute deltas: a 1% move on a $90k coin and
    on a $0.00009 coin produce the same numerical delta when scaled by
    the notional. Avoids float-precision loss on small-cap tokens.
    """
    if len(closes) < 2:
        return []
    dir_sign = 1.0 if side == "long" else -1.0
    series: list[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        if prev <= 0:
            series.append(0.0)
            continue
        ret = (closes[i] - prev) / prev
        series.append(notional * ret * dir_sign)
    return series


def _correlation_tier(corr: float) -> str:
    """Bin a signed correlation into the seven mockup CSS tiers.
       > +0.7 strong-con, +0.4 to +0.7 mid-con, +0.2 to +0.4 soft-con,
       −0.2 to +0.2 neutral,
       −0.4 to −0.2 soft-hedge, −0.7 to −0.4 mid-hedge, < −0.7 strong-hedge.
    """
    if corr > 0.7:
        return "strong-con"
    if corr > 0.4:
        return "mid-con"
    if corr > 0.2:
        return "soft-con"
    if corr >= -0.2:
        return "neutral"
    if corr >= -0.4:
        return "soft-hedge"
    if corr >= -0.7:
        return "mid-hedge"
    return "strong-hedge"


# ── Compute step ──────────────────────────────────────────────────────

def compute_correlation_matrix_and_effective_n(
    positions: list[dict],
    *, client: BinanceMarketClient | None = None,
    binance_id_resolver=None,
) -> dict[str, Any]:
    """positions: list of dicts with keys {symbol, symbol_base, side, notional_usd}.
       binance_id_resolver(symbol_base) → binance_symbol or None.

    Returns:
      {
        "rows": [{symbol, symbol_base, side, notional_usd,
                   binance_symbol, has_history, sigma_daily}, ...],
        "matrix": [[corr, ...], ...]   # NxN, diag = 1.0
        "tiers": [["strong-con", ...], ...]  # cell tier names
        "effective_n": float | None,
        "diversification_benefit_pct": float | None,
        "nominal_count": int,
        "reasons": {symbol: 'insufficient_history' | 'not_listed', ...}
      }

    `effective_n` is the diversification-ratio-squared formulation
    (sometimes called "effective number of independent bets"):

        effective_N = (Σ σ_i)² / (Σ_ij σ_i σ_j ρ_ij)

    Deviation from §9a Note B: Note B's literal formula uses inverse
    Herfindahl on σ-weighted notional weights, which doesn't account
    for correlation — N perfectly-correlated equal positions still
    yield effective_N = N under that form. The diversification-ratio
    form gives effective_N = 1 in that limit and = N for perfectly
    independent positions, matching the spec's stated reference cases.
    """
    n = len(positions)
    rows_meta: list[dict] = []
    pnl_series_per_pos: dict[str, list[float]] = {}
    reasons: dict[str, str] = {}

    if client is None:
        client = BinanceMarketClient()

    for p in positions:
        sym = p["symbol"]
        sym_base = p["symbol_base"]
        side = p["side"]
        notional = float(p.get("notional_usd") or 0)
        binance_id = binance_id_resolver(sym_base) if binance_id_resolver else None

        closes = get_30d_daily_closes(binance_id, client=client) if binance_id else None
        if closes is None or len(closes) < MIN_DAYS:
            rows_meta.append({
                "symbol": sym,
                "symbol_base": sym_base,
                "side": side,
                "notional_usd": notional,
                "binance_symbol": binance_id,
                "has_history": False,
                "sigma_daily": None,
            })
            reasons[sym] = "not_listed" if binance_id is None else "insufficient_history"
            continue

        series = _pnl_series(closes, notional, side)
        sigma = statistics.pstdev(series) if len(series) >= 2 else 0.0
        pnl_series_per_pos[sym] = series
        rows_meta.append({
            "symbol": sym,
            "symbol_base": sym_base,
            "side": side,
            "notional_usd": notional,
            "binance_symbol": binance_id,
            "has_history": True,
            "sigma_daily": sigma,
        })

    # Correlation matrix.
    matrix: list[list[float | None]] = [[None] * n for _ in range(n)]
    tiers: list[list[str]] = [[""] * n for _ in range(n)]
    # Need equal-length series for pairwise corr.
    common_len = (
        min(len(s) for s in pnl_series_per_pos.values())
        if pnl_series_per_pos else 0
    )
    for i in range(n):
        for j in range(n):
            sym_i = rows_meta[i]["symbol"]
            sym_j = rows_meta[j]["symbol"]
            if i == j:
                matrix[i][j] = 1.0
                tiers[i][j] = "diag"
                continue
            si = pnl_series_per_pos.get(sym_i)
            sj = pnl_series_per_pos.get(sym_j)
            if si is None or sj is None or common_len < MIN_DAYS:
                matrix[i][j] = None
                tiers[i][j] = "insufficient"
                continue
            corr = _pearson(si[-common_len:], sj[-common_len:])
            matrix[i][j] = corr
            tiers[i][j] = _correlation_tier(corr)

    # Diversification-aware effective-N.
    sigmas = [m["sigma_daily"] for m in rows_meta if m["has_history"]]
    valid_indices = [i for i, m in enumerate(rows_meta) if m["has_history"]]
    if len(sigmas) < 2:
        return {
            "rows": rows_meta,
            "matrix": matrix,
            "tiers": tiers,
            "effective_n": None,
            "diversification_benefit_pct": None,
            "nominal_count": n,
            "reasons": reasons,
        }

    # σ_silo: sum of per-position σ (silo'd risk if all independent)
    sigma_silo = sum(sigmas)

    # σ_portfolio² = Σ_ij σ_i × σ_j × ρ_ij  (using the FULL covariance matrix
    # built from σ_i × σ_j × correlation_ij — equivalent to np.cov().sum())
    cov_sum = 0.0
    for ii_idx, i in enumerate(valid_indices):
        for jj_idx, j in enumerate(valid_indices):
            sigma_i = sigmas[ii_idx]
            sigma_j = sigmas[jj_idx]
            corr_ij = matrix[i][j] if matrix[i][j] is not None else 0.0
            cov_sum += sigma_i * sigma_j * corr_ij
    sigma_portfolio = math.sqrt(cov_sum) if cov_sum > 0 else 0.0

    if sigma_portfolio <= 0 or sigma_silo <= 0:
        eff_n: float | None = None
        diversification_pct: float | None = None
    else:
        eff_n = (sigma_silo / sigma_portfolio) ** 2
        diversification_pct = (1 - sigma_portfolio / sigma_silo) * 100.0

    return {
        "rows": rows_meta,
        "matrix": matrix,
        "tiers": tiers,
        "effective_n": eff_n,
        "diversification_benefit_pct": diversification_pct,
        "nominal_count": n,
        "reasons": reasons,
    }


# ── Top-level cached entry ────────────────────────────────────────────

def _position_set_hash(positions: list[dict]) -> str:
    """Stable hash over (symbol, side, rounded_notional). notional is
    rounded to 2 decimals so jitter on each tick doesn't invalidate the
    cache key — but a real notional change still rotates it."""
    canon = sorted(
        (p["symbol"], p["side"], round(float(p.get("notional_usd") or 0), 2))
        for p in positions
    )
    h = hashlib.sha1(json.dumps(canon).encode()).hexdigest()
    return h[:12]


def get_coverage_matrix_cached(
    positions: list[dict],
    *, client: BinanceMarketClient | None = None,
    binance_id_resolver=None,
) -> dict[str, Any]:
    """Cache-fronted wrapper. Cache key includes the 1H bar boundary so
    matrices auto-rotate on each hourly close."""
    if not positions:
        return {
            "rows": [], "matrix": [], "tiers": [],
            "effective_n": None, "diversification_benefit_pct": None,
            "nominal_count": 0, "reasons": {},
        }

    pos_hash = _position_set_hash(positions)
    bar_ms = latest_hourly_close_ms()
    cache_key = f"corr_matrix:{pos_hash}:{bar_ms}"

    r = _get_redis()
    try:
        cached = r.get(cache_key)
    except redis_lib.RedisError as e:
        log.warning("correlation_cache: Redis GET %s failed: %s", cache_key, e)
        cached = None

    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    result = compute_correlation_matrix_and_effective_n(
        positions, client=client, binance_id_resolver=binance_id_resolver,
    )

    try:
        r.setex(cache_key, MATRIX_TTL_S, json.dumps(result, default=str))
    except redis_lib.RedisError as e:
        log.warning("correlation_cache: SETEX %s failed: %s", cache_key, e)
    return result

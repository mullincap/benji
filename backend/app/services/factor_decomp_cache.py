"""
backend/app/services/factor_decomp_cache.py
=============================================
Multivariate ridge regression of each position's 30d daily-PnL series
against (BTC factor, ALT factor) plus an intercept. Drives the Live
tab's Factor Decomposition card (Data Dictionary §9b).

Math:
  pnl_i_t = α_i + β_i_BTC × ret_BTC_t + β_i_ALT × ret_ALT_t + ε_i_t

  ALT factor = **BTC-orthogonal residual** of the equal-weighted alt
  index. Construction: build the raw equal-weighted return series over
  the alt index (config/alt_index.yml — 20 large-cap alts, ex-BTC,
  ex-ETH, ex-stables), then OLS-regress it on BTC returns over the
  same window and use the residuals as the factor. This forces BTC
  factor and ALT factor to be uncorrelated by construction, so the
  per-position β's are economically interpretable: β_BTC absorbs all
  BTC-correlated variance (including the BTC-correlated component of
  alt moves), and β_ALT picks up only genuinely alt-specific residual
  moves. Without this orthogonalization, BTC and the raw alt index
  run ~0.85+ correlated on 30d crypto data — multicollinear enough
  that ridge can't cleanly separate them on 29 daily obs, producing
  unstable per-position β's (e.g. a meme-coin long with negative
  β_BTC, which is economically nonsense). Variance attribution at the
  portfolio level is invariant to this rotation; per-position β's
  become trustworthy.

  On any given day, only alts with cached history at that bar
  contribute to the raw index, so the underlying pool is whatever
  subset has data — not a fixed-N pool.

Aggregation (portfolio level, the bar that gets rendered):
  portfolio_pnl_t = Σ pnl_i_t
  Run the same ridge regression on the portfolio series:
      portfolio_pnl_t = α + β_BTC × ret_BTC_t + β_ALT × ret_ALT_t + ε_t
  Variance attribution per §9b:
      var_BTC  = β_BTC² × Var(ret_BTC)
      var_ALT  = β_ALT² × Var(ret_ALT)
      var_IDIO = Var(ε)
      pct_x = var_x / (var_BTC + var_ALT + var_IDIO) × 100
  Cross-term β_BTC×β_ALT×Cov(BTC,ALT) is not assigned to either factor
  individually — ridge keeps β's well-behaved when BTC and ALT are
  collinear, and the three normalize to 100% even when the cross-term
  is non-zero. This matches the spec's "normalize the three to 100%".

Ridge: small λ = 0.01 (in the units of the centered design matrix).
Implemented via the closed-form 2-feature centered normal equation —
no numpy required. Intercept is recovered post-hoc and not regularized.

Cache:
  factor_decomp:{position_set_hash}:{1h_bar_close_ts} → JSON, TTL 2h.
  Per-symbol daily-close series come from `correlation_cache.get_30d_daily_closes`,
  so this module piggybacks on its `daily_closes:*` cache (same TTL,
  same daily-bar-close rotation). The alt-index symbol fetches fan out
  through a small ThreadPoolExecutor on cold cache so a fresh page
  doesn't block on 20 sequential round-trips.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import statistics
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import redis as redis_lib
import yaml

from ..core.config import settings
from .correlation_cache import (
    MIN_DAYS,
    get_30d_daily_closes,
    latest_hourly_close_ms,
)
from .exchanges.binance_market import BinanceMarketClient

log = logging.getLogger(__name__)

DECOMP_TTL_S = 2 * 60 * 60  # 2h, same as correlation matrix
RIDGE_LAMBDA = 0.01
BTC_FACTOR_SYMBOL = "BTCUSDT"

_redis: redis_lib.Redis | None = None
_alt_symbols_cache: list[str] | None = None


def _get_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = redis_lib.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


# ── Alt-index config loader ───────────────────────────────────────────

def _alt_index_path() -> Path:
    """Resolve config/alt_index.yml relative to the repo root.
    The backend container is launched from /app (the backend/ dir), so
    ../config/alt_index.yml is the canonical path. Allow an env override
    for tests."""
    env = os.environ.get("ALT_INDEX_YAML")
    if env:
        return Path(env)
    here = Path(__file__).resolve()
    # services/ → app/ → backend/ → repo_root/
    return here.parent.parent.parent.parent / "config" / "alt_index.yml"


def load_alt_symbols() -> list[str]:
    """Return the alt-index symbol list. Cached process-locally — the
    file changes only on quarterly rebalances, so re-reading on every
    request is wasteful."""
    global _alt_symbols_cache
    if _alt_symbols_cache is not None:
        return _alt_symbols_cache
    path = _alt_index_path()
    try:
        with path.open("r") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        log.error("factor_decomp: alt_index.yml not found at %s", path)
        _alt_symbols_cache = []
        return []
    except yaml.YAMLError as e:
        log.error("factor_decomp: alt_index.yml parse error: %s", e)
        _alt_symbols_cache = []
        return []
    syms = data.get("symbols") if isinstance(data, dict) else None
    if not isinstance(syms, list):
        log.error("factor_decomp: alt_index.yml missing 'symbols:' list")
        _alt_symbols_cache = []
        return []
    _alt_symbols_cache = [str(s).upper() for s in syms]
    return _alt_symbols_cache


# ── Linear algebra ────────────────────────────────────────────────────

def _ridge_2d(
    y: list[float], x1: list[float], x2: list[float], *, lam: float = RIDGE_LAMBDA,
) -> tuple[float, float, float, list[float]]:
    """Closed-form ridge regression of y on (x1, x2) plus intercept.

    Internally standardizes x1, x2 to unit variance before applying
    ridge so λ is **scale-invariant** — λ=0.01 means "add 1% to the
    diagonal of X'X in standardized units," independent of whether x1
    is daily-return-as-decimal (~0.04) or basis points (~400). Without
    this, λ=0.01 in raw return-space would dwarf s_jj for typical 30d
    crypto-return regressions and shrink β by ~50%. The coefficients
    are de-standardized before return so callers see β in raw units.

    Intercept = ȳ − β1·x̄1 − β2·x̄2 (recovered post-hoc, not
    regularized — we don't shrink the mean).

    Returns (intercept, β1, β2, residuals). When either feature has
    zero variance, the function returns the intercept-only fit.
    """
    n = len(y)
    if n < 3 or n != len(x1) or n != len(x2):
        return (0.0, 0.0, 0.0, list(y))

    my = sum(y) / n
    m1 = sum(x1) / n
    m2 = sum(x2) / n

    var1 = sum((v - m1) ** 2 for v in x1) / n
    var2 = sum((v - m2) ** 2 for v in x2) / n
    if var1 <= 0 or var2 <= 0:
        return (my, 0.0, 0.0, [yi - my for yi in y])
    sd1 = math.sqrt(var1)
    sd2 = math.sqrt(var2)

    # Standardize x1, x2 (zero mean, unit variance). y stays raw — we
    # want β in raw $-PnL-per-decimal-return units at the end.
    z1 = [(v - m1) / sd1 for v in x1]
    z2 = [(v - m2) / sd2 for v in x2]
    yc = [v - my for v in y]

    s11 = sum(v * v for v in z1)  # = n by construction
    s22 = sum(v * v for v in z2)  # = n
    s12 = sum(a * b for a, b in zip(z1, z2))
    s1y = sum(a * b for a, b in zip(z1, yc))
    s2y = sum(a * b for a, b in zip(z2, yc))

    a11 = s11 + lam * n
    a22 = s22 + lam * n
    det = a11 * a22 - s12 * s12
    if abs(det) < 1e-15:
        return (my, 0.0, 0.0, [yi - my for yi in y])

    z_b1 = (a22 * s1y - s12 * s2y) / det
    z_b2 = (a11 * s2y - s12 * s1y) / det

    beta1 = z_b1 / sd1
    beta2 = z_b2 / sd2
    intercept = my - beta1 * m1 - beta2 * m2

    residuals = [
        y[i] - intercept - beta1 * x1[i] - beta2 * x2[i] for i in range(n)
    ]
    return intercept, beta1, beta2, residuals


def _variance(xs: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    return sum((v - m) ** 2 for v in xs) / n


def _orthogonalize(target: list[float], predictor: list[float]) -> list[float]:
    """Return residuals from an OLS regression of `target` on `predictor`
    plus an intercept. The output series is uncorrelated with `predictor`
    by construction (sample correlation = 0 within rounding) and has
    mean zero.

    Used to convert the raw alt-index return series into a BTC-orthogonal
    "alt-residual" factor. See module header for why — short version:
    BTC and the raw alt index are ~0.85+ correlated on 30d windows;
    without this step, the 2-factor regression of position PnL is
    multicollinear and individual β's pick up arbitrary opposite signs
    (e.g. a meme-coin long with β_BTC < 0). After orthogonalization
    BTC absorbs all BTC-correlated variance and ALT picks up only what
    BTC can't explain.
    """
    n = len(target)
    if n < 3 or n != len(predictor):
        return list(target)
    mt = sum(target) / n
    mp = sum(predictor) / n
    var_p = sum((v - mp) ** 2 for v in predictor) / n
    if var_p <= 0:
        return [v - mt for v in target]
    cov_tp = sum((target[i] - mt) * (predictor[i] - mp) for i in range(n)) / n
    beta = cov_tp / var_p
    intercept = mt - beta * mp
    return [target[i] - intercept - beta * predictor[i] for i in range(n)]


def _returns(closes: list[float]) -> list[float]:
    """Daily returns from a closes series; len(returns) = len(closes) − 1."""
    out: list[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        out.append((closes[i] - prev) / prev if prev > 0 else 0.0)
    return out


def _pnl_series(closes: list[float], notional: float, side: str) -> list[float]:
    """Same shape as correlation_cache._pnl_series — kept local to avoid
    a private cross-module import. notional × ret × dir_sign."""
    if len(closes) < 2:
        return []
    dir_sign = 1.0 if side == "long" else -1.0
    out: list[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        if prev <= 0:
            out.append(0.0)
            continue
        out.append(notional * (closes[i] - prev) / prev * dir_sign)
    return out


# ── Alt-index return series ───────────────────────────────────────────

def _build_alt_index_returns(
    *, client: BinanceMarketClient,
) -> tuple[list[float], int]:
    """Equal-weighted mean of daily returns across the alt-index symbols
    that have ≥MIN_DAYS of cached history. Returns (returns, members_used).
    Length = TARGET_DAYS−1 if all members have full history; otherwise
    capped to the shortest series among contributors.

    Fetches fan out via a small thread pool so the cold-cache path is
    bounded by Binance round-trip latency, not 20×."""
    syms = load_alt_symbols()
    if not syms:
        return [], 0

    closes_by_sym: dict[str, list[float]] = {}

    def _fetch_one(sym: str) -> tuple[str, list[float] | None]:
        try:
            return sym, get_30d_daily_closes(sym, client=client)
        except Exception as e:
            log.warning("factor_decomp: alt fetch %s failed: %s", sym, e)
            return sym, None

    with ThreadPoolExecutor(max_workers=min(8, len(syms))) as ex:
        for sym, closes in ex.map(_fetch_one, syms):
            if closes is not None and len(closes) >= MIN_DAYS:
                closes_by_sym[sym] = closes

    if not closes_by_sym:
        return [], 0

    # Align to the shortest member series (rare, but possible if Binance
    # listed an alt mid-window).
    min_len = min(len(c) for c in closes_by_sym.values())
    aligned = {s: c[-min_len:] for s, c in closes_by_sym.items()}

    rets_by_sym = {s: _returns(c) for s, c in aligned.items()}
    n_days = len(next(iter(rets_by_sym.values())))
    n_members = len(rets_by_sym)

    avg_returns: list[float] = []
    for t in range(n_days):
        avg_returns.append(sum(r[t] for r in rets_by_sym.values()) / n_members)
    return avg_returns, n_members


# ── Compute step ──────────────────────────────────────────────────────

def compute_factor_decomposition(
    positions: list[dict],
    *, client: BinanceMarketClient | None = None,
    binance_id_resolver=None,
) -> dict[str, Any]:
    """positions: list of dicts with keys {symbol, symbol_base, side,
    notional_usd}. binance_id_resolver(symbol_base) → binance_id or None.

    Returns:
      {
        "positions": [
            {symbol, symbol_base, side, notional_usd,
             beta_btc, beta_alt, has_history,
             var_pct: {btc, alt, idio}}, ...
        ],
        "portfolio": {
            "beta_btc": $-exposure to a +1.0 BTC return,
            "beta_alt": $-exposure to a +1.0 ALT return,
            "var_btc_pct": float, "var_alt_pct": float, "var_idio_pct": float,
            "sigma_silo_usd": Σσ_i (independent risk total),
            "sigma_portfolio_usd": stdev of portfolio PnL series,
            "diversification_benefit_pct": 1−σ_port/σ_silo,
        },
        "alt_index_member_count": int,  # number of alts that contributed
        "alt_index_target_count": int,  # configured pool size
        "n_days": int,                  # days of history used in the regression
        "reasons": {symbol: 'not_listed' | 'insufficient_history', ...}
      }

    `var_pct` triplets are normalized to sum to 100% per §9b. When a
    factor's coefficient is zero (degenerate fit) its variance share is
    0 and the others absorb the mass.
    """
    if client is None:
        client = BinanceMarketClient()

    # ── Factor series ──────────────────────────────────────────────
    btc_closes = get_30d_daily_closes(BTC_FACTOR_SYMBOL, client=client)
    alt_returns, alt_members = _build_alt_index_returns(client=client)

    if not btc_closes or len(btc_closes) < MIN_DAYS or not alt_returns:
        return {
            "positions": [{
                "symbol": p["symbol"],
                "symbol_base": p["symbol_base"],
                "side": p["side"],
                "notional_usd": float(p.get("notional_usd") or 0),
                "beta_btc": None,
                "beta_alt": None,
                "has_history": False,
                "var_pct": {"btc": None, "alt": None, "idio": None},
            } for p in positions],
            "portfolio": None,
            "alt_index_member_count": alt_members,
            "alt_index_target_count": len(load_alt_symbols()),
            "n_days": 0,
            "reasons": {p["symbol"]: "factor_history_unavailable" for p in positions},
        }

    btc_returns = _returns(btc_closes)

    # Align factor lengths to the same window.
    common_factor_n = min(len(btc_returns), len(alt_returns))
    btc_returns = btc_returns[-common_factor_n:]
    alt_returns = alt_returns[-common_factor_n:]

    # Orthogonalize the alt factor against BTC. Position-PnL regressions
    # downstream now use (BTC, ALT_residual), where ALT_residual is alt
    # variance net of BTC. See module header for the multicollinearity
    # rationale.
    alt_returns = _orthogonalize(alt_returns, btc_returns)

    # ── Per-position regressions ──────────────────────────────────
    pos_rows: list[dict[str, Any]] = []
    pnl_series_by_sym: dict[str, list[float]] = {}
    reasons: dict[str, str] = {}

    for p in positions:
        sym = p["symbol"]
        sym_base = p["symbol_base"]
        side = p["side"]
        notional = float(p.get("notional_usd") or 0)
        binance_id = (
            binance_id_resolver(sym_base) if binance_id_resolver else None
        )

        closes = get_30d_daily_closes(binance_id, client=client) if binance_id else None
        if closes is None or len(closes) < MIN_DAYS:
            reasons[sym] = "not_listed" if not binance_id else "insufficient_history"
            pos_rows.append({
                "symbol": sym,
                "symbol_base": sym_base,
                "side": side,
                "notional_usd": notional,
                "beta_btc": None,
                "beta_alt": None,
                "has_history": False,
                "var_pct": {"btc": None, "alt": None, "idio": None},
            })
            continue

        pnl = _pnl_series(closes, notional, side)
        # Align to the shorter of (factor window, position window).
        n = min(len(pnl), common_factor_n)
        if n < MIN_DAYS:
            reasons[sym] = "insufficient_history"
            pos_rows.append({
                "symbol": sym,
                "symbol_base": sym_base,
                "side": side,
                "notional_usd": notional,
                "beta_btc": None,
                "beta_alt": None,
                "has_history": False,
                "var_pct": {"btc": None, "alt": None, "idio": None},
            })
            continue

        y = pnl[-n:]
        x_btc = btc_returns[-n:]
        x_alt = alt_returns[-n:]
        _alpha, beta_btc, beta_alt, residuals = _ridge_2d(y, x_btc, x_alt)

        var_btc = (beta_btc ** 2) * _variance(x_btc)
        var_alt = (beta_alt ** 2) * _variance(x_alt)
        var_idio = _variance(residuals)
        total = var_btc + var_alt + var_idio
        if total > 0:
            pct_btc = var_btc / total * 100.0
            pct_alt = var_alt / total * 100.0
            pct_idio = var_idio / total * 100.0
        else:
            pct_btc = pct_alt = 0.0
            pct_idio = 100.0

        pnl_series_by_sym[sym] = y
        pos_rows.append({
            "symbol": sym,
            "symbol_base": sym_base,
            "side": side,
            "notional_usd": notional,
            "beta_btc": beta_btc,
            "beta_alt": beta_alt,
            "has_history": True,
            "var_pct": {"btc": pct_btc, "alt": pct_alt, "idio": pct_idio},
        })

    # ── Portfolio-level regression ────────────────────────────────
    if not pnl_series_by_sym:
        return {
            "positions": pos_rows,
            "portfolio": None,
            "alt_index_member_count": alt_members,
            "alt_index_target_count": len(load_alt_symbols()),
            "n_days": 0,
            "reasons": reasons,
        }

    common_pos_n = min(len(s) for s in pnl_series_by_sym.values())
    common_n = min(common_pos_n, common_factor_n)

    portfolio_pnl: list[float] = [0.0] * common_n
    for series in pnl_series_by_sym.values():
        s = series[-common_n:]
        for t in range(common_n):
            portfolio_pnl[t] += s[t]

    x_btc = btc_returns[-common_n:]
    x_alt = alt_returns[-common_n:]
    _alpha, b_btc, b_alt, residuals = _ridge_2d(portfolio_pnl, x_btc, x_alt)

    v_btc = (b_btc ** 2) * _variance(x_btc)
    v_alt = (b_alt ** 2) * _variance(x_alt)
    v_idio = _variance(residuals)
    v_total = v_btc + v_alt + v_idio
    if v_total > 0:
        pf_pct_btc = v_btc / v_total * 100.0
        pf_pct_alt = v_alt / v_total * 100.0
        pf_pct_idio = v_idio / v_total * 100.0
    else:
        pf_pct_btc = pf_pct_alt = 0.0
        pf_pct_idio = 100.0

    # σ_silo = Σ σ_i (per-position stdev), σ_portfolio = stdev of summed series.
    sigma_silo = sum(
        statistics.pstdev(s) if len(s) >= 2 else 0.0
        for s in pnl_series_by_sym.values()
    )
    sigma_portfolio = statistics.pstdev(portfolio_pnl) if len(portfolio_pnl) >= 2 else 0.0
    if sigma_silo > 0:
        diversification_pct = (1 - sigma_portfolio / sigma_silo) * 100.0
    else:
        diversification_pct = None

    return {
        "positions": pos_rows,
        "portfolio": {
            "beta_btc": b_btc,
            "beta_alt": b_alt,
            "var_btc_pct": pf_pct_btc,
            "var_alt_pct": pf_pct_alt,
            "var_idio_pct": pf_pct_idio,
            "sigma_silo_usd": sigma_silo,
            "sigma_portfolio_usd": sigma_portfolio,
            "diversification_benefit_pct": diversification_pct,
        },
        "alt_index_member_count": alt_members,
        "alt_index_target_count": len(load_alt_symbols()),
        "n_days": common_n,
        "reasons": reasons,
    }


# ── Top-level cached entry ────────────────────────────────────────────

def _position_set_hash(positions: list[dict]) -> str:
    canon = sorted(
        (p["symbol"], p["side"], round(float(p.get("notional_usd") or 0), 2))
        for p in positions
    )
    h = hashlib.sha1(json.dumps(canon).encode()).hexdigest()
    return h[:12]


def get_factor_decomposition_cached(
    positions: list[dict],
    *, client: BinanceMarketClient | None = None,
    binance_id_resolver=None,
) -> dict[str, Any]:
    """Cache-fronted wrapper. Cache key includes the 1H bar boundary so
    decompositions auto-rotate on each hourly close."""
    if not positions:
        return {
            "positions": [],
            "portfolio": None,
            "alt_index_member_count": 0,
            "alt_index_target_count": len(load_alt_symbols()),
            "n_days": 0,
            "reasons": {},
        }

    pos_hash = _position_set_hash(positions)
    bar_ms = latest_hourly_close_ms()
    cache_key = f"factor_decomp:{pos_hash}:{bar_ms}"

    r = _get_redis()
    try:
        cached = r.get(cache_key)
    except redis_lib.RedisError as e:
        log.warning("factor_decomp: Redis GET %s failed: %s", cache_key, e)
        cached = None

    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    result = compute_factor_decomposition(
        positions, client=client, binance_id_resolver=binance_id_resolver,
    )

    try:
        r.setex(cache_key, DECOMP_TTL_S, json.dumps(result, default=str))
    except redis_lib.RedisError as e:
        log.warning("factor_decomp: SETEX %s failed: %s", cache_key, e)
    return result

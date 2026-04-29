"""
pipeline/audit_filters.py
=========================
Shared filter functions for the daily-signal pipeline.

Single source of truth for the Tail Guardrail and Dispersion gates.
Imported by:
  - daily_signal_v2.py (live path, 06:00 UTC cron)
  - pipeline/intraday_audit.py (audit verifier, 06:01 UTC cron)

These functions match the methodology in pipeline/audit.py
(build_tail_guardrail at audit.py:1950, build_dispersion_filter at
audit.py:2765) but read from the live DB / Binance FAPI rather than
parquet, so they can evaluate today's gate before the nightly ETL has
populated parquet for day N. The audit's parquet-backed implementations
remain canonical for backtesting; these implementations are the live
counterparts that produce identical decisions on identical inputs.

Extraction history: factored out of daily_signal_v2.py on 2026-04-26 as
part of the Option A1 design (see docs/strategy_specification.md). The
prior parallel-implementation pattern (audit.py + daily_signal_v2.py
each owning their own copy of these functions) was a drift hazard. After
this extraction, daily_signal_v2.py imports from here, and the audit's
parquet path should adopt these functions in a follow-up commit.
"""
from __future__ import annotations

import datetime as _dt
import logging
import math
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger("audit_filters")


# ==========================================================================
# CONFIG (canonical defaults — strategy versions override via config JSONB)
# ==========================================================================

# Tail Guardrail
TAIL_DROP_PCT = 0.04
TAIL_VOL_MULT = 1.4
TAIL_VOL_SHORT_WINDOW = 5
TAIL_VOL_LONG_WINDOW = 60

# Dispersion filter
DISPERSION_N = 40
DISPERSION_BASELINE_WIN = 33
DISPERSION_THRESHOLD = 0.66
# Minimum non-NaN return count required for cross-sectional std to be
# computed on a given day. Was 20 historically (matched the smallest
# DISPERSION_SYMBOLS_<N> floor), but with strict_dynamic mode the
# universe size now follows dispersion_n directly — N=10 (or smaller)
# is a legitimate sweep target and the 20-floor was silently stripping
# every day from the dispersion series, effectively disabling the
# filter. Floor removed (set to 0) so any operator-chosen N produces a
# real signal — pandas .std() with <2 valid values returns NaN, which
# the rolling-baseline + ratio path then handles as fail-open for that
# specific day rather than dropping the entire day from the series.
DISPERSION_MIN_SYMBOLS = 0

# Binance FAPI (used by Dispersion)
_BINANCE_FAPI = "https://fapi.binance.com"
_REST_WORKERS = 8
_REST_TIMEOUT = 10

# Binance multiplier-prefix overrides for low-priced perps. PEPE/SHIB/FLOKI/BONK
# trade as 1000-multiplied perps on Binance (1000PEPEUSDT etc.), but our
# canonical base is the un-multiplied symbol. market.symbols.binance_id is
# NULL for several of these — the symbol_registry refresh has a bug that
# doesn't detect 1000-prefix mappings. Hardcoded here; verify against
# https://fapi.binance.com/fapi/v1/exchangeInfo if extending.
_BINANCE_TICKER_OVERRIDES = {
    "PEPE":  "1000PEPEUSDT",
    "SHIB":  "1000SHIBUSDT",
    "FLOKI": "1000FLOKIUSDT",
    "BONK":  "1000BONKUSDT",
}


def _base_to_binance_ticker(base: str) -> str:
    """Map CoinGecko base ('PEPE') to actual Binance perp ticker
    ('1000PEPEUSDT'). Naive `f"{base}USDT"` silently drops 1000-prefix
    perps from any kline-fetch path that takes a base symbol — observed
    in compute_dispersion_filter where PEPE/SHIB/FLOKI/BONK were dropped
    despite ranking high in market.market_cap_daily."""
    return _BINANCE_TICKER_OVERRIDES.get(base, f"{base}USDT")


# ==========================================================================
# DB HELPER
# ==========================================================================

def _get_conn():
    """Lazy import so this module is cheap to import even when caller
    doesn't need DB access (e.g. unit tests with mocked filters)."""
    here = Path(__file__).resolve().parent
    sys.path.insert(0, str(here.parent))  # project root
    from pipeline.db.connection import get_conn
    return get_conn()


# ==========================================================================
# TAIL GUARDRAIL
# ==========================================================================

def fetch_btc_daily_closes(ref_date: _dt.date) -> list[float]:
    """Pull BTC 1d closes for the trailing TAIL_VOL_LONG_WINDOW+5 days
    from market.futures_1m. Returns chronological list of float closes.
    Empty list if coverage is insufficient — caller should fail-closed.

    Single DB round-trip; callers running per-strategy should fetch once
    and pass the result via the `closes` kwarg on compute_tail_guardrail.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        WITH btc AS (
            SELECT symbol_id FROM market.symbols WHERE binance_id = 'BTCUSDT'
        )
        SELECT DATE_TRUNC('day', f.timestamp_utc)::date AS day,
               (ARRAY_AGG(f.close ORDER BY f.timestamp_utc DESC))[1] AS day_close
          FROM market.futures_1m f
         WHERE f.symbol_id IN (SELECT symbol_id FROM btc)
           AND f.timestamp_utc >= %s::timestamptz - INTERVAL '%s days'
           AND f.timestamp_utc < %s::timestamptz
           AND f.close IS NOT NULL AND f.close > 0
         GROUP BY 1
         ORDER BY 1
        """,
        (ref_date, TAIL_VOL_LONG_WINDOW + 6, ref_date),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [float(r[1]) for r in rows]


def compute_tail_guardrail_detail(
    ref_date: _dt.date,
    closes: Optional[list[float]] = None,
    tail_drop_pct: Optional[float] = None,
    tail_vol_mult: Optional[float] = None,
) -> dict:
    """Tail Guardrail sit-flat decision with full numeric breakdown.

    Logic (matches pipeline/audit.py:1974 build_tail_guardrail):
      - prev_day_return: log-return between (ref_date-2) and (ref_date-1)
        daily closes. Fires if < -tail_drop_pct.
      - rvol ratio: stdev(short window of daily log-returns) /
        stdev(long window). Fires if > tail_vol_mult.
    Either gate firing → sit flat.

    Returns a dict with keys:
        sit_flat: bool — final gate decision
        reason: str | None
        prev_day_return: float | None — decimal (e.g. -0.052 for -5.2%)
        rvol_short: float | None — stdev of last short-window log-returns
        rvol_long: float | None — stdev of last long-window log-returns
        rvol_ratio: float | None — rvol_short / rvol_long
        tail_drop_pct: float — threshold used (decimal)
        tail_vol_mult: float — threshold used (multiplier)
        crash_fires: bool
        vol_fires: bool
        n_closes: int — count of daily closes available
        insufficient_history: bool

    `closes` may be pre-fetched via fetch_btc_daily_closes when the
    caller is computing TG for multiple strategies in one invocation —
    saves the DB round-trip.
    """
    tdp = tail_drop_pct if tail_drop_pct is not None else TAIL_DROP_PCT
    tvm = tail_vol_mult if tail_vol_mult is not None else TAIL_VOL_MULT

    log.info(f"Computing Tail Guardrail (drop={tdp*100:.1f}%  vol_mult={tvm}x)...")

    if closes is None:
        closes = fetch_btc_daily_closes(ref_date)

    base_detail: dict = {
        "tail_drop_pct": tdp,
        "tail_vol_mult": tvm,
        "tail_vol_short_window": TAIL_VOL_SHORT_WINDOW,
        "tail_vol_long_window": TAIL_VOL_LONG_WINDOW,
        "n_closes": len(closes),
        "prev_day_return": None,
        "rvol_short": None,
        "rvol_long": None,
        "rvol_ratio": None,
        "crash_fires": False,
        "vol_fires": False,
        "insufficient_history": False,
    }

    if len(closes) < TAIL_VOL_LONG_WINDOW + 1:
        log.warning(
            f"Tail Guardrail needs >={TAIL_VOL_LONG_WINDOW+1} daily closes "
            f"prior to {ref_date}; have {len(closes)}. Forcing sit-flat "
            f"(fail-closed)."
        )
        return {
            **base_detail,
            "sit_flat": True,
            "reason": "tail_guardrail_insufficient_history",
            "insufficient_history": True,
        }

    log_rets: list[float] = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            log_rets.append(math.log(closes[i] / closes[i - 1]))

    prev_day_logret = log_rets[-1] if log_rets else 0.0
    prev_day_ret = math.exp(prev_day_logret) - 1.0

    def _stdev(xs: list[float]) -> float:
        if len(xs) < 2:
            return 0.0
        m = sum(xs) / len(xs)
        return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))

    rvol_short = _stdev(log_rets[-TAIL_VOL_SHORT_WINDOW:])
    rvol_long = _stdev(log_rets[-TAIL_VOL_LONG_WINDOW:])
    ratio = (rvol_short / rvol_long) if rvol_long > 0 else 0.0

    log.info(
        f"  BTC prev-day: {prev_day_ret*100:.2f}%  "
        f"rvol_short: {rvol_short*100:.2f}%  "
        f"rvol_long: {rvol_long*100:.2f}%  "
        f"ratio: {ratio:.3f}x  threshold: {tvm}x"
    )

    crash_fires = prev_day_ret < -tdp
    vol_fires = ratio > tvm

    detail = {
        **base_detail,
        "prev_day_return": prev_day_ret,
        "rvol_short": rvol_short,
        "rvol_long": rvol_long,
        "rvol_ratio": ratio,
        "crash_fires": bool(crash_fires),
        "vol_fires": bool(vol_fires),
    }

    if crash_fires and vol_fires:
        return {**detail, "sit_flat": True, "reason": (
            f"tail_guardrail_crash_and_vol: prev_day={prev_day_ret*100:.2f}% "
            f"rvol_ratio={ratio:.3f}x"
        )}
    if crash_fires:
        return {**detail, "sit_flat": True, "reason": (
            f"tail_guardrail_crash: prev_day={prev_day_ret*100:.2f}% "
            f"< -{tdp*100:.1f}%"
        )}
    if vol_fires:
        return {**detail, "sit_flat": True,
                "reason": f"tail_guardrail_vol: rvol_ratio={ratio:.3f}x > {tvm}x"}
    return {**detail, "sit_flat": False, "reason": None}


def compute_tail_guardrail(
    ref_date: _dt.date,
    closes: Optional[list[float]] = None,
    tail_drop_pct: Optional[float] = None,
    tail_vol_mult: Optional[float] = None,
) -> tuple[bool, Optional[str]]:
    """Backward-compatible thin wrapper over compute_tail_guardrail_detail.
    Returns (sit_flat, reason|None). New callers should prefer the _detail
    variant so they have access to the underlying numeric values."""
    d = compute_tail_guardrail_detail(ref_date, closes=closes,
                                       tail_drop_pct=tail_drop_pct,
                                       tail_vol_mult=tail_vol_mult)
    return d["sit_flat"], d["reason"]


# ==========================================================================
# DISPERSION FILTER
# ==========================================================================

def compute_dispersion_filter_detail(
    ref_date: _dt.date,
    threshold: Optional[float] = None,
    baseline_win: Optional[int] = None,
    n_symbols: Optional[int] = None,
    lag_days: Optional[int] = None,
    strict_dynamic: Optional[bool] = None,
) -> dict:
    """Cross-sectional dispersion gate — DELEGATES to audit.py for full
    methodological parity.

    Calls audit.fetch_altcoin_daily_returns + _load_mcap_from_db +
    build_dynamic_symbol_mask + build_dispersion_filter (the same chain
    audit.py runs). This eliminates the prior approximation that used a
    static today's-top-N universe and no mcap lag — verified 2026-04-29:
    the static approximation diverged from audit on 6/30 borderline days,
    almost all due to audit's 1-day mcap lag (build_dynamic_symbol_mask
    line 3097: `mask[T] uses mcap[T-1]`) and per-day mask re-evaluation.

    Logic: at date T, gate fires if `disp_ratio[T-1] < threshold` where
      mask[T-1]      = top-N by mcap[T-2]    (lag in build_dynamic_symbol_mask)
      dispersion[T-1] = std of returns[T-1] over symbols where mask[T-1]
      baseline[T-1]  = rolling_median(dispersion, win) ending at T-1
      disp_ratio[T-1] = dispersion[T-1] / baseline[T-1]
    The shift(1) in build_dispersion_filter then maps that to the gate
    decision for T.

    Returns the same detail dict shape as before (yesterday_dispersion,
    baseline_median, dispersion_ratio, threshold, ...) so peek.py /
    daily_signal_v3 display logic is unchanged.
    """
    thr = threshold if threshold is not None else DISPERSION_THRESHOLD
    win = baseline_win if baseline_win is not None else DISPERSION_BASELINE_WIN
    n = n_symbols if n_symbols is not None else DISPERSION_N
    # lag_days=None means "use audit.py's DISPERSION_UNIVERSE_LAG_DAYS env
    # default" — preserve the existing behaviour for callers who don't
    # care. When daily_signal_v3 plumbs per-strategy values, it'll pass
    # an explicit int here.
    lag = lag_days
    # strict_dynamic=None means "use audit.py's
    # DISPERSION_UNIVERSE_STRICT_DYNAMIC env default (False)" — preserves
    # legacy behaviour. When True, the returns universe is built dynamically
    # from market.market_cap_daily (union of per-day top-N, lagged) instead
    # of using audit.DISPERSION_UNIVERSE.get(n) (the hardcoded list).
    sd = strict_dynamic

    log.info(
        f"Computing Dispersion filter (threshold={thr}, "
        f"baseline_win={win}d, n={n}, "
        f"lag={lag if lag is not None else 'audit-default'}, "
        f"strict_dynamic={sd if sd is not None else 'audit-default'}) — via audit.py"
    )

    base_detail: dict = {
        "threshold": thr,
        "baseline_win": win,
        "n_symbols_target": n,
        "lag_days": lag,
        "strict_dynamic": sd,
        "n_symbols_eligible": 0,
        "n_symbols_with_klines": 0,
        "n_baseline_values": 0,
        "yesterday_dispersion": None,
        "baseline_median": None,
        "dispersion_ratio": None,
        "fail_open_reason": None,
    }

    # Lazy import: audit.py has heavy module-level work + emits
    # DISPERSION_UNIVERSE_SIZE banner on import. Keep audit_filters'
    # import path cheap for callers that don't need dispersion. pandas
    # and numpy are also lazy-imported here since the existing tail-
    # guardrail path doesn't use them.
    try:
        import sys as _sys
        from pathlib import Path as _Path
        _here = _Path(__file__).resolve().parent
        if str(_here) not in _sys.path:
            _sys.path.insert(0, str(_here))   # so `import audit` works
        if str(_here.parent) not in _sys.path:
            _sys.path.insert(0, str(_here.parent))  # so audit's pipeline.* works
        import audit as _audit
        import pandas as _pd
        import numpy as _np
    except Exception as e:
        log.warning(f"  Dispersion: audit import failed ({e}); fail-open")
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": f"audit_import_failed: {e}"}

    # Window: ref_date back through enough warmup for the rolling baseline
    # to fully populate. audit's fetch_altcoin_daily_returns honours its
    # own DISPERSION_CACHE_FILE so repeated live calls within a day reuse
    # the cache.
    end_str   = (ref_date + _dt.timedelta(days=1)).isoformat()
    start_str = (ref_date - _dt.timedelta(days=win + 200)).isoformat()

    # Resolve effective strict_dynamic from audit's env default if caller
    # didn't pass an explicit bool. Done after audit is imported so the
    # env-derived module constant is available.
    eff_sd = sd if sd is not None else bool(getattr(_audit, "DISPERSION_UNIVERSE_STRICT_DYNAMIC", False))

    # Mcap from DB (matches audit's default DISPERSION_UNIVERSE_MODE='all').
    # Loaded BEFORE alt_returns now so strict_dynamic mode can derive the
    # returns universe from mcap_df via build_dynamic_returns_universe
    # instead of the hardcoded DISPERSION_UNIVERSE list.
    try:
        mcap_df = _audit._load_mcap_from_db(start_str, end_str)
    except Exception as e:
        log.warning(f"  Dispersion: mcap load failed ({e})")
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": f"mcap_load_failed: {e}"}

    # Universe selection:
    #   strict_dynamic=True  → union of per-day top-N (lagged) from mcap_df.
    #                          Universe genuinely tracks the market each day.
    #   strict_dynamic=False → audit.DISPERSION_UNIVERSE.get(n) hardcoded
    #                          list (current legacy behaviour). Kept as
    #                          A/B fallback during the strict_dynamic
    #                          rollout / tuning sweep.
    if eff_sd:
        try:
            effective_lag = lag if lag is not None else int(
                getattr(_audit, "DISPERSION_UNIVERSE_LAG_DAYS", 1)
            )
            universe_tickers = _audit.build_dynamic_returns_universe(
                mcap_df, n=n, lag_days=effective_lag,
            )
            log.info(
                f"  Dispersion[strict_dynamic]: built universe of "
                f"{len(universe_tickers)} tickers from mcap_df "
                f"(union of top-{n} over {len(mcap_df)} days, lag={effective_lag})"
            )
        except Exception as e:
            log.warning(f"  Dispersion: dynamic universe build failed ({e}); "
                        f"falling back to hardcoded list")
            universe_tickers = _audit.DISPERSION_UNIVERSE.get(n)
    else:
        universe_tickers = _audit.DISPERSION_UNIVERSE.get(n)

    if not universe_tickers:
        log.warning(
            f"  Dispersion: no universe resolved for n={n} "
            f"(strict_dynamic={eff_sd}); fail-open"
        )
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": f"no_universe_for_n_{n}"}

    try:
        # In strict_dynamic mode the universe is a function of the mcap
        # window, so the default DISPERSION_CACHE_FILE (keyed by N only)
        # would alias different runs. Derive a per-universe cache file so
        # n=40 dynamic and n=40 static don't collide and so different
        # date ranges (or lag values) get their own cache.
        if eff_sd:
            import hashlib as _hashlib
            sym_hash = _hashlib.md5(
                ",".join(universe_tickers).encode()
            ).hexdigest()[:10]
            cache_file = f"dispersion_cache_dyn_{sym_hash}.csv"
        else:
            cache_file = None  # use audit's default
        alt_returns = _audit.fetch_altcoin_daily_returns(
            symbols=universe_tickers, start=start_str, end=end_str,
            **({"cache_file": cache_file} if cache_file else {}),
        )
    except Exception as e:
        log.warning(f"  Dispersion: alt_returns fetch failed ({e})")
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": f"alt_returns_failed: {e}"}
    if alt_returns is None or alt_returns.empty:
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": "alt_returns_empty"}
    base_detail["n_symbols_with_klines"] = int(alt_returns.shape[1])

    # Dynamic mask: build_dynamic_symbol_mask applies a `.shift(lag_days)`
    # so mask[T] uses mcap[T-lag_days]. Passing lag explicitly when the
    # caller specifies it (per-strategy plumbing); otherwise None falls
    # to audit's DISPERSION_UNIVERSE_LAG_DAYS env default (=1).
    try:
        if lag is None:
            mask_df = _audit.build_dynamic_symbol_mask(mcap_df, alt_returns, n=n)
        else:
            mask_df = _audit.build_dynamic_symbol_mask(
                mcap_df, alt_returns, n=n, lag_days=lag,
            )
    except Exception as e:
        log.warning(f"  Dispersion: dynamic mask build failed ({e})")
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": f"dynamic_mask_failed: {e}"}

    base_detail["n_symbols_eligible"] = int(
        mask_df.sum(axis=1).reindex([_pd.Timestamp(ref_date)]).iloc[0]
        if _pd.Timestamp(ref_date) in mask_df.index else 0
    )

    # Compute dispersion / baseline / ratio inline (rather than calling
    # build_dispersion_filter and discarding intermediates) so we can
    # surface the underlying numerics in the detail dict for cross-checks
    # against audit's stored values.
    masked_returns = alt_returns.where(mask_df, other=_np.nan)
    valid_mask = masked_returns.notna().sum(axis=1) >= DISPERSION_MIN_SYMBOLS
    masked_returns = masked_returns[valid_mask]
    if masked_returns.empty:
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": "no_valid_dispersion_days"}
    dispersion = masked_returns.std(axis=1)
    baseline = dispersion.rolling(
        win, min_periods=max(5, win // 2),
    ).median()
    disp_ratio = dispersion / baseline.replace(0, _np.nan)

    # Gate at ref_date = "did dispersion fire at ref_date - 1 day?"
    yesterday = _pd.Timestamp(ref_date - _dt.timedelta(days=1))
    if yesterday not in disp_ratio.index or _pd.isna(disp_ratio.loc[yesterday]):
        return {**base_detail, "sit_flat": False, "reason": None,
                "fail_open_reason": "no_yesterday_dispersion"}

    yest_disp = float(dispersion.loc[yesterday])
    yest_base = float(baseline.loc[yesterday])
    yest_ratio = float(disp_ratio.loc[yesterday])
    base_detail["n_baseline_values"] = int(
        dispersion.loc[: yesterday]
                  .tail(win)
                  .notna().sum()
    )

    log.info(
        f"  Dispersion[{yesterday.date()}]={yest_disp:.5f}  "
        f"baseline_median={yest_base:.5f}  ratio={yest_ratio:.3f}  "
        f"threshold={thr}"
    )

    detail = {
        **base_detail,
        "yesterday_dispersion": yest_disp,
        "baseline_median": yest_base,
        "dispersion_ratio": yest_ratio,
    }

    if yest_ratio < thr:
        return {**detail, "sit_flat": True,
                "reason": f"dispersion_low: ratio={yest_ratio:.3f} < {thr}"}
    return {**detail, "sit_flat": False, "reason": None}


def compute_dispersion_filter(
    ref_date: _dt.date,
    threshold: Optional[float] = None,
    baseline_win: Optional[int] = None,
    n_symbols: Optional[int] = None,
    lag_days: Optional[int] = None,
    strict_dynamic: Optional[bool] = None,
) -> tuple[bool, Optional[str]]:
    """Backward-compatible thin wrapper over compute_dispersion_filter_detail.
    Returns (sit_flat, reason|None). New callers should prefer the _detail
    variant for access to numeric values."""
    d = compute_dispersion_filter_detail(ref_date, threshold=threshold,
                                          baseline_win=baseline_win,
                                          n_symbols=n_symbols,
                                          lag_days=lag_days,
                                          strict_dynamic=strict_dynamic)
    return d["sit_flat"], d["reason"]

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
DISPERSION_MIN_SYMBOLS = 20

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


def compute_tail_guardrail(
    ref_date: _dt.date,
    closes: Optional[list[float]] = None,
    tail_drop_pct: Optional[float] = None,
    tail_vol_mult: Optional[float] = None,
) -> tuple[bool, Optional[str]]:
    """Tail Guardrail sit-flat decision.

    Logic (matches pipeline/audit.py:1950 build_tail_guardrail):
      - prev_day_return: log-return between (ref_date-2) and (ref_date-1)
        daily closes. Fires if < -tail_drop_pct.
      - rvol ratio: stdev(short window of daily log-returns) /
        stdev(long window). Fires if > tail_vol_mult.
    Either gate firing → sit flat. Returns (sit_flat, reason|None).

    `closes` may be pre-fetched via fetch_btc_daily_closes when the
    caller is computing TG for multiple strategies in one invocation —
    saves the DB round-trip.
    """
    tdp = tail_drop_pct if tail_drop_pct is not None else TAIL_DROP_PCT
    tvm = tail_vol_mult if tail_vol_mult is not None else TAIL_VOL_MULT

    log.info(f"Computing Tail Guardrail (drop={tdp*100:.1f}%  vol_mult={tvm}x)...")

    if closes is None:
        closes = fetch_btc_daily_closes(ref_date)

    if len(closes) < TAIL_VOL_LONG_WINDOW + 1:
        log.warning(
            f"Tail Guardrail needs >={TAIL_VOL_LONG_WINDOW+1} daily closes "
            f"prior to {ref_date}; have {len(closes)}. Forcing sit-flat "
            f"(fail-closed)."
        )
        return True, "tail_guardrail_insufficient_history"

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

    if crash_fires and vol_fires:
        return True, (
            f"tail_guardrail_crash_and_vol: prev_day={prev_day_ret*100:.2f}% "
            f"rvol_ratio={ratio:.3f}x"
        )
    if crash_fires:
        return True, (
            f"tail_guardrail_crash: prev_day={prev_day_ret*100:.2f}% "
            f"< -{tdp*100:.1f}%"
        )
    if vol_fires:
        return True, f"tail_guardrail_vol: rvol_ratio={ratio:.3f}x > {tvm}x"
    return False, None


# ==========================================================================
# DISPERSION FILTER
# ==========================================================================

def compute_dispersion_filter(
    ref_date: _dt.date,
    threshold: Optional[float] = None,
    baseline_win: Optional[int] = None,
    n_symbols: Optional[int] = None,
) -> tuple[bool, Optional[str]]:
    """Cross-sectional dispersion gate (matches audit.py:2765).

    Lagged by 1 day:
      yesterday_dispersion = std of yesterday's daily log-returns across
                             top-N mcap symbols
      baseline             = rolling median over baseline_win days of
                             dispersion values, ending yesterday
      disp_ratio           = yesterday_dispersion / baseline
      sit_flat today       if disp_ratio < threshold

    Approximation vs the audit: live uses today's top-N mcap universe
    for the full baseline window (simpler than per-day re-querying);
    dominant effect is yesterday's dispersion which uses yesterday's
    data directly.
    """
    thr = threshold if threshold is not None else DISPERSION_THRESHOLD
    win = baseline_win if baseline_win is not None else DISPERSION_BASELINE_WIN
    n = n_symbols if n_symbols is not None else DISPERSION_N

    log.info(
        f"Computing Dispersion filter (threshold={thr}, "
        f"baseline_win={win}d, n={n})..."
    )

    # 1. Today's top-N mcap symbols.
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT base
          FROM market.market_cap_daily
         WHERE date = %s
           AND base NOT IN ('USDT','USDC','BUSD','DAI','TUSD','FDUSD','USDE',
                            'USDS','PYUSD','USD1','FRAX','USDP')
           AND base !~ '[^A-Z0-9]'
           AND base NOT IN ('XAU','XAG','XPD','XPT')
         ORDER BY market_cap_usd DESC
         LIMIT %s
        """,
        (ref_date, n),
    )
    symbols = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    if len(symbols) < DISPERSION_MIN_SYMBOLS:
        log.warning(
            f"  Dispersion: only {len(symbols)} eligible mcap symbols for "
            f"{ref_date} (< {DISPERSION_MIN_SYMBOLS}). Fail-open."
        )
        return False, None

    # 2. Fetch (win + buffer) days of 1d klines per symbol.
    fetch_days = win + 2
    end_dt = _dt.datetime.combine(ref_date, _dt.time(0, 0, 0), tzinfo=_dt.timezone.utc)
    start_dt = end_dt - _dt.timedelta(days=fetch_days)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    def _fetch_daily(sym: str):
        ticker = _base_to_binance_ticker(sym)
        try:
            r = requests.get(
                f"{_BINANCE_FAPI}/fapi/v1/klines",
                params={
                    "symbol": ticker,
                    "interval": "1d",
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": fetch_days + 5,
                },
                timeout=_REST_TIMEOUT,
            )
            if r.status_code != 200:
                return sym, None
            klines = r.json()
            if not klines:
                return sym, None
            return sym, [(k[0], float(k[4])) for k in klines]
        except Exception:
            return sym, None

    log.info(
        f"  Fetching {fetch_days}d of 1d klines for {len(symbols)} symbols..."
    )
    results: dict = {}
    with ThreadPoolExecutor(max_workers=_REST_WORKERS) as pool:
        for sym, series in pool.map(_fetch_daily, symbols):
            if series:
                results[sym] = series

    if len(results) < DISPERSION_MIN_SYMBOLS:
        log.warning(
            f"  Dispersion: only {len(results)}/{len(symbols)} symbols "
            f"returned klines (< {DISPERSION_MIN_SYMBOLS}). Fail-open."
        )
        return False, None

    # 3. Per-day cross-sectional std of log-returns.
    per_symbol_closes: dict = {}
    all_dates: set = set()
    for sym, series in results.items():
        d_map: dict = {}
        for ts_ms, close in series:
            d = _dt.datetime.fromtimestamp(
                ts_ms / 1000, tz=_dt.timezone.utc
            ).date()
            d_map[d] = close
            all_dates.add(d)
        per_symbol_closes[sym] = d_map

    sorted_dates = sorted(all_dates)
    daily_dispersion: dict = {}
    for i in range(1, len(sorted_dates)):
        d = sorted_dates[i]
        d_prev = sorted_dates[i - 1]
        rets: list[float] = []
        for sym, d_map in per_symbol_closes.items():
            c = d_map.get(d)
            c_prev = d_map.get(d_prev)
            if c is not None and c_prev is not None and c_prev > 0 and c > 0:
                rets.append(math.log(c / c_prev))
        if len(rets) >= DISPERSION_MIN_SYMBOLS:
            m = sum(rets) / len(rets)
            var = sum((x - m) ** 2 for x in rets) / (len(rets) - 1)
            daily_dispersion[d] = math.sqrt(var)

    # 4. Yesterday's dispersion vs baseline median.
    yesterday = ref_date - _dt.timedelta(days=1)
    if yesterday not in daily_dispersion:
        log.warning(
            f"  Dispersion: no value for yesterday ({yesterday}). Fail-open."
        )
        return False, None

    window_start = yesterday - _dt.timedelta(days=win - 1)
    baseline_vals = sorted(
        v for d, v in daily_dispersion.items()
        if window_start <= d <= yesterday
    )
    min_for_baseline = max(5, win // 2)
    if len(baseline_vals) < min_for_baseline:
        log.warning(
            f"  Dispersion: only {len(baseline_vals)} baseline values "
            f"(need >= {min_for_baseline}). Fail-open."
        )
        return False, None

    n_b = len(baseline_vals)
    baseline = (
        baseline_vals[n_b // 2]
        if n_b % 2 == 1
        else (baseline_vals[n_b // 2 - 1] + baseline_vals[n_b // 2]) / 2
    )
    yesterday_disp = daily_dispersion[yesterday]
    disp_ratio = (yesterday_disp / baseline) if baseline > 0 else 0.0

    log.info(
        f"  Dispersion[{yesterday}]={yesterday_disp:.5f}  "
        f"baseline_median={baseline:.5f}  "
        f"ratio={disp_ratio:.3f}  threshold={thr}"
    )

    if disp_ratio < thr:
        return True, f"dispersion_low: ratio={disp_ratio:.3f} < {thr}"
    return False, None

# ── BEFORE-COMPARISON BRANCH ─────────────────────────────────────────────────
# This file has been reverted to pre-optimisation settings for the purpose of
# reproducing the baseline audit results. Do NOT merge to main.
# Original 25 settings recorded in: before_comparison_settings.md
# ─────────────────────────────────────────────────────────────────────────────

#!/usr/bin/env python3
"""
rebuild_portfolio_matrix.py

Rebuilds the PORTFOLIO_ROI_LEV_MATRIX from raw prices, applying a symbol
eligibility gate (minimum listing age) before averaging.

Output is a drop-in replacement for PIVOT_TABLE.csv: 216 rows × N day-columns,
5-min bars 06:00–23:55 UTC, values as "X.XX%" strings, cols Bt_YYYYMMDD_060000.

Per-symbol path:
  raw[t] = (price[t] / price[0] - 1) * 100   (unleveraged)
  once raw[t] <= STOP_RAW_PCT  →  clamp at STOP_RAW_PCT forever
  leverage applied after equal-weight averaging across symbols

Portfolio path:
  equal-weight mean of eligible symbol paths at each bar

Inputs (put alongside this script or adjust paths):
  - DEPLOYS.csv
  - symbol_first_seen.csv
  - master_oi_training_table.parquet

Outputs:
  - portfolio_matrix_gated.csv
  - eligibility_gate_report.csv
  - gate_summary.txt
"""

import os
import re
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# ── CONFIG ─────────────────────────────────────────────────────────────────────
PARQUET_PATH       = os.environ.get("PARQUET_PATH", "/Users/johnmullin/Desktop/desk/import/oi_logger/backfills/1raws/oi_raw/master_data_table.parquet")
#DEPLOYS_CSV        = "DEPLOYS.tsv"   # tab-separated, same directory as script
DEPLOYS_CSV        = "deploys_overlap_top100_w20c20_snapshot_0600_0M.csv"
FIRST_SEEN_CSV     = "symbol_first_seen.csv"

OUTPUT_MATRIX      = "portfolio_matrix_gated.csv"
OUTPUT_GATE_REPORT = "eligibility_gate_report.csv"
OUTPUT_SUMMARY     = "gate_summary.txt"

# ── Deployment window ──────────────────────────────────────────────────────────
# These three variables define the 24-hour cycle structure.
# DEPLOYMENT_START_HOUR  : hour (UTC) the deployment period begins.
#   Default 6 = 06:00 UTC.  Grid search varies this across 0–23.
# SORT_LOOKBACK            : electoral window length — int hours back from deployment start,
#                            or "daily" to scan from 00:05 to deployment_start_hour
#                            (resolves to deployment_start_hour hours).
# DEPLOYMENT_RUNTIME_HOURS  : how long we are invested each day.
# N_BARS is derived so build_bar_grid() and the Binance fetch always stay in sync.
DEPLOYMENT_START_HOUR     = 6    # 06:00 UTC — independent variable for grid search
SORT_LOOKBACK             = 6               # electoral window: int hours, or "daily"
DEPLOYMENT_RUNTIME_HOURS  = "daily"  # deployment period: int hours, or "daily"
                                      # "daily" => runtime = 24 - effective_lookback
                                      # so that lookback + runtime = 24h exactly.

def _resolve_runtime_hours() -> int:
    """Return the actual integer runtime given current DEPLOYMENT_RUNTIME_HOURS,
    SORT_LOOKBACK, and DEPLOYMENT_START_HOUR.

    DEPLOYMENT_RUNTIME_HOURS can be:
      - An integer: used directly
      - "daily": runtime = 24 - effective_sort_lookback_hours
          where effective_sort_lookback_hours is:
            SORT_LOOKBACK (int)            if sort_lookback is a number
            DEPLOYMENT_START_HOUR          if sort_lookback is "daily"
              (daily mode scans 00:05 -> start_hour, so start_hour IS the lookback length)

    Example: start_hour=6, sort_lookback="daily" -> lookback=6h -> runtime=18h
    Example: start_hour=0, sort_lookback="daily" -> lookback=0h -> runtime=24h (full day)
    """
    if DEPLOYMENT_RUNTIME_HOURS != "daily":
        return int(DEPLOYMENT_RUNTIME_HOURS)
    effective_lookback = DEPLOYMENT_START_HOUR \
        if SORT_LOOKBACK == "daily" else int(SORT_LOOKBACK)
    return 24 - effective_lookback

# Cross-midnight behaviour:
#   True  (default) — when start + runtime overflows past 23:59, the bar grid
#                     and price fetch span into the next calendar day.
#                     e.g. start=10:00 + 18h → ends 03:55 next day.
#   False           — cap the session at 23:55 UTC on the same calendar day,
#                     truncating N_BARS to whatever fits. Use this if your data
#                     source does not carry bars across the UTC day boundary.
END_CROSS_MIDNIGHT     = os.environ.get("END_CROSS_MIDNIGHT", "0") == "1"

# Session — must match existing pivot table exactly
BAR_MINUTES        = 5       # bar resolution (minutes)
N_BARS             = int(_resolve_runtime_hours() * 60 // BAR_MINUTES)  # derived from runtime

# Derived session end: last bar open-time = start + (N_BARS-1) × BAR_MINUTES
# Total minutes from midnight to the last bar open-time:
_SESSION_END_MINUTES  = DEPLOYMENT_START_HOUR * 60 + (N_BARS - 1) * BAR_MINUTES
# Whether the session overflows past midnight (total minutes >= 24 * 60):
_END_CROSSES_MIDNIGHT     = _SESSION_END_MINUTES >= 24 * 60
# Clock time of the last bar (wraps within 0-23 if end crosses midnight):
DEPLOYMENT_END_HOUR   = (_SESSION_END_MINUTES % (24 * 60)) // 60
DEPLOYMENT_END_MINUTE = _SESSION_END_MINUTES % 60

# Per-symbol: raw unleveraged cum pct with -8.5% stop per symbol
#   raw[t] = (price[t] / price[0] - 1) * 100   (clamped at -8.5% once hit)
# Portfolio: equal-weight mean across symbols, THEN multiply by LEVERAGE
LEVERAGE           = float(os.environ.get("LEVERAGE", "4.0"))     # 4x per symbol
STOP_RAW_PCT       = float(os.environ.get("STOP_RAW_PCT", "-6"))    # per-symbol stop on unleveraged return (%)

# Eligibility gate
MIN_LISTING_AGE_DAYS = int(os.environ.get("MIN_LISTING_AGE", "0"))

# Max symbols per day (applied AFTER eligibility gate). None = no cap.
_max_port_env = os.environ.get("MAX_PORT", "").strip()
MAX_PORT = None if _max_port_env in ("", "None", "none", "null", "NULL") else int(_max_port_env)

# Price source: 'binance' (candle closes, matches Sheets) or 'parquet' (fast, local)
PRICE_SOURCE       = os.environ.get("PRICE_SOURCE", "parquet")

# Parquet symbol suffix
SYMBOL_SUFFIX      = "USDT"  # "SONIC" → "SONICUSDT"

# Fold windows for the summary report
FOLDS = [
    ("F1", "2025-05-01", "2025-05-30"),
    ("F2", "2025-05-31", "2025-06-29"),
    ("F3", "2025-06-30", "2025-07-29"),
    ("F4", "2025-07-30", "2025-08-28"),
    ("F5", "2025-08-29", "2025-09-27"),
    ("F6", "2025-09-28", "2025-10-27"),
    ("F7", "2025-10-28", "2025-11-26"),
    ("F8", "2025-11-27", "2025-12-26"),
]
# ── END CONFIG ─────────────────────────────────────────────────────────────────

# Pre-compiled regex for stripping numeric multiplier prefixes (e.g. 1000RATS → RATS)
_MULTIPLIER_RE = re.compile(r'^(\d+)(.*)')


def load_deploys(path):
    # Auto-detect separator: sniff first line
    with open(path, "r") as f:
        first_line = f.readline()
    sep = "\t" if "\t" in first_line else ","

    df = pd.read_csv(path, sep=sep)
    df.columns = [c.strip() for c in df.columns]

    # Support both 'timestamp_utc' and 'date' as the index column
    ts_col = "timestamp_utc" if "timestamp_utc" in df.columns else df.columns[0]
    df[ts_col] = pd.to_datetime(df[ts_col])

    # Accept any number of R-columns (R1, R2, ... R9, R10, R15, etc.)
    sym_cols = [c for c in df.columns if c.strip().startswith("R") and c.strip()[1:].isdigit()]
    sym_cols = sorted(sym_cols, key=lambda c: int(c.strip()[1:]))

    rows = []
    for _, row in df.iterrows():
        syms = [str(row[c]).strip() for c in sym_cols
                if pd.notna(row[c]) and str(row[c]).strip() not in ("", "nan")]
        rows.append({
            "session_start": row[ts_col],
            "date":          row[ts_col].date(),
            "col_name":      "Bt_" + row[ts_col].strftime("%Y%m%d") + f"_{DEPLOYMENT_START_HOUR:02d}0000",
            "symbols":       syms,
        })
    return rows


def load_first_seen(path):
    """
    Returns ({symbol_base: first_seen_date}, {symbol_base: raw_ticker}).

    raw_ticker is the original symbol string from the CSV (e.g. "1000RATSUSDT"),
    used by to_ticker() to reconstruct the correct parquet lookup key.
    """
    df = pd.read_csv(path, parse_dates=["first_seen_date"])
    dates = {}
    raw   = {}   # base → original ticker (preserves 1000x prefix etc.)
    for _, row in df.iterrows():
        sym  = str(row["symbol"])
        base = sym.replace(SYMBOL_SUFFIX, "").replace("_PERP", "")
        # Strip leading numeric multiplier to get base (e.g. 1000RATS → RATS)
        m = _MULTIPLIER_RE.match(base)
        if m:
            base = m.group(2)
        dates[base] = pd.to_datetime(row["first_seen_date"]).date()
        raw[base]   = sym   # keep original ticker for parquet lookup
    return dates, raw


def check_eligible(symbol_base, deploy_date, first_seen):
    """Returns (eligible: bool, age_days, reason_str)"""
    fs = first_seen.get(symbol_base) or first_seen.get(symbol_base + SYMBOL_SUFFIX)
    if fs is None:
        # Only gate on missing first_seen when MIN_LISTING_AGE_DAYS > 0.
        # With age=0, we have no age requirement so absence from the CSV is irrelevant.
        if MIN_LISTING_AGE_DAYS > 0:
            return False, None, "not_in_first_seen"
        else:
            return True, None, "ok_no_age_req"
    age = (deploy_date - fs).days
    if age < MIN_LISTING_AGE_DAYS:
        return False, age, f"too_young_{age}d"
    return True, age, "ok"


def build_bar_grid(session_start):
    """N_BARS × BAR_MINUTES-min bars anchored to DEPLOYMENT_START_HOUR UTC.
    Always uses only the date component of session_start so files with
    midnight timestamps (00:00:00) still produce the correct session window.

    Cross-midnight behaviour is controlled by the END_CROSS_MIDNIGHT constant:
      True  — end timestamp advances to the next calendar day when the session
              overflows past 23:59 UTC (e.g. start=10:00 + 18h → 03:55+1d).
      False — session is capped at 23:55 on the same day; N_BARS is truncated
              to however many 5-min bars fit before midnight.
    """
    date       = pd.Timestamp(session_start).date()
    start      = pd.Timestamp(date).replace(hour=DEPLOYMENT_START_HOUR, minute=0,
                                            second=0, microsecond=0)
    if _END_CROSSES_MIDNIGHT and END_CROSS_MIDNIGHT:
        # End timestamp falls on the next calendar day
        next_date  = date + pd.Timedelta(days=1)
        end        = pd.Timestamp(next_date).replace(hour=DEPLOYMENT_END_HOUR,
                                                     minute=DEPLOYMENT_END_MINUTE,
                                                     second=0, microsecond=0)
    elif _END_CROSSES_MIDNIGHT and not END_CROSS_MIDNIGHT:
        # Cap at 23:55 on the same day — truncate to whatever bars fit
        end = pd.Timestamp(date).replace(hour=23, minute=55, second=0, microsecond=0)
    else:
        # Normal same-day session (e.g. 06:00–23:55)
        end = pd.Timestamp(date).replace(hour=DEPLOYMENT_END_HOUR,
                                         minute=DEPLOYMENT_END_MINUTE,
                                         second=0, microsecond=0)
    return pd.date_range(start=start, end=end, freq=f"{BAR_MINUTES}min")


def _fetch_binance_klines(symbol: str, date: pd.Timestamp,
                          bar_minutes: int = 5) -> pd.Series:
    """
    Fetch 5-min klines from Binance public API for one symbol/date.
    Returns a Series indexed by bar open-time (tz-naive UTC), values = close price.
    Returns empty Series on any error.
    """
    import urllib.request, json, time as _time

    # Force UTC — .timestamp() uses local tz which gives wrong epoch ms
    _session_date = pd.Timestamp(date.date())
    start_ms = int(_session_date.replace(hour=DEPLOYMENT_START_HOUR)
                   .tz_localize("UTC").timestamp() * 1000)
    if _END_CROSSES_MIDNIGHT and END_CROSS_MIDNIGHT:
        _end_date = _session_date + pd.Timedelta(days=1)
        end_ms = int(_end_date.replace(hour=DEPLOYMENT_END_HOUR,
                                       minute=DEPLOYMENT_END_MINUTE)
                     .tz_localize("UTC").timestamp() * 1000)
    elif _END_CROSSES_MIDNIGHT and not END_CROSS_MIDNIGHT:
        end_ms = int(_session_date.replace(hour=23, minute=55)
                     .tz_localize("UTC").timestamp() * 1000)
    else:
        end_ms = int(_session_date.replace(hour=DEPLOYMENT_END_HOUR,
                                           minute=DEPLOYMENT_END_MINUTE)
                     .tz_localize("UTC").timestamp() * 1000)
    interval = f"{bar_minutes}m"
    url = (f"https://fapi.binance.com/fapi/v1/klines"
           f"?symbol={symbol}&interval={interval}"
           f"&startTime={start_ms}&endTime={end_ms}&limit={N_BARS}")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
            if not data:
                return pd.Series(dtype=float)
            # kline: [open_time, open, high, low, close, ...]
            idx    = pd.to_datetime([row[0] for row in data], unit="ms")
            closes = pd.to_numeric([row[4] for row in data], errors="coerce")
            return pd.Series(closes, index=idx)
        except Exception as e:
            if attempt < 2:
                _time.sleep(1)
            else:
                print(f"    [binance] FAILED {symbol} {date.date()}: {e}")
                return pd.Series(dtype=float)


def get_session_prices_binance(symbols_parquet, session_date: pd.Timestamp,
                               bar_grid: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Fetch 5-min close prices from Binance for each symbol.
    Returns pivot DataFrame: index=bar_grid, cols=symbol (USDT pair names).
    Falls back to parquet for any symbol that fails.
    """
    import time as _time
    frames = {}
    fallback_syms = []
    n_total = len(symbols_parquet)

    for idx, sym in enumerate(symbols_parquet, 1):
        # Overwrite the same line: "  fetching  3/15  SONICUSDT ..."
        print(f"    [binance] fetching {idx:>3}/{n_total}  {sym:<20}", end="\r", flush=True)
        s = _fetch_binance_klines(sym, session_date, BAR_MINUTES)
        if s.empty:
            fallback_syms.append(sym)
            continue
        # Reindex to bar_grid
        s = s.reindex(bar_grid).ffill().bfill()
        if s.isna().all():
            fallback_syms.append(sym)
        else:
            frames[sym] = s
        _time.sleep(0.05)   # ~20 req/s, well within Binance limits

    # Clear the progress line before printing day summary
    print(f"    {'':60}", end="\r", flush=True)

    if fallback_syms:
        # Exclude delisted/failed symbols — do NOT fall back to parquet.
        # Sheets builder drops these too; stale parquet data corrupts portfolio.
        print(f"    [binance] excluding {len(fallback_syms)} delisted symbols: {fallback_syms}")

    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames, index=bar_grid)


def get_session_prices_parquet(symbols_parquet, t_start, t_end):
    """Load raw tick prices from parquet, return pivot: index=timestamp, cols=symbol"""
    # Cast bounds to strings so the filter matches the large_string timestamp_utc column
    t_start_s = pd.Timestamp(t_start).strftime("%Y-%m-%d %H:%M:%S")
    t_end_s   = pd.Timestamp(t_end).strftime("%Y-%m-%d %H:%M:%S")
    filters = [
        ("timestamp_utc", ">=", t_start_s),
        ("timestamp_utc", "<=", t_end_s),
        ("symbol", "in", symbols_parquet),
    ]
    df = pd.read_parquet(PARQUET_PATH, filters=filters,
                         columns=["timestamp_utc", "symbol", "price"])
    if df.empty:
        return pd.DataFrame()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    if df["timestamp_utc"].dt.tz is not None:
        df["timestamp_utc"] = df["timestamp_utc"].dt.tz_localize(None)
    df = df.dropna(subset=["price"])
    return df.pivot_table(index="timestamp_utc", columns="symbol",
                          values="price", aggfunc="last")


def get_session_prices_db(symbols_parquet, t_start, t_end):
    """Load prices from market.futures_1m, return same pivot shape as parquet version."""
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pipeline.db.connection import get_conn
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT f.timestamp_utc, s.binance_id AS symbol, f.close AS price
        FROM market.futures_1m f
        JOIN market.symbols s ON s.symbol_id = f.symbol_id
        WHERE f.source_id = 1
          AND f.timestamp_utc >= %s
          AND f.timestamp_utc <= %s
          AND s.binance_id = ANY(%s)
          AND f.close IS NOT NULL
        ORDER BY f.timestamp_utc
        """,
        (pd.Timestamp(t_start).to_pydatetime(),
         pd.Timestamp(t_end).to_pydatetime(),
         list(symbols_parquet)),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["timestamp_utc", "symbol", "price"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    if df["timestamp_utc"].dt.tz is not None:
        df["timestamp_utc"] = df["timestamp_utc"].dt.tz_localize(None)
    return df.pivot_table(index="timestamp_utc", columns="symbol",
                          values="price", aggfunc="last")



def apply_raw_stop(prices_series):
    """
    Unleveraged cumulative % return from entry, with per-symbol stop at -8.5% (no leverage).
    Stop fires when raw return <= STOP_RAW_PCT; bars after stop are clamped at STOP_RAW_PCT.
    Leverage is applied AFTER equal-weight averaging across symbols.
    Returns np.ndarray of same length.
    """
    p0 = prices_series.iloc[0]
    if p0 == 0 or np.isnan(p0):
        return np.full(len(prices_series), np.nan)
    raw = ((prices_series.values / p0) - 1.0) * 100.0   # unleveraged %
    stopped = False
    for i in range(len(raw)):
        if stopped:
            raw[i] = STOP_RAW_PCT
        elif raw[i] <= STOP_RAW_PCT:
            raw[i] = STOP_RAW_PCT
            stopped = True
    return raw


def build_portfolio_path(symbols_parquet, session_start, debug=False):
    """Returns pd.Series (bar_grid index, float pct) or None on failure."""
    bar_grid     = build_bar_grid(session_start)
    session_date = pd.Timestamp(session_start)

    # ── Fetch prices from selected source ─────────────────────────────
    if PRICE_SOURCE == "binance":
        price_rs = get_session_prices_binance(symbols_parquet, session_date, bar_grid)
        if price_rs.empty:
            print(f"    ! Binance fetch returned empty for {symbols_parquet}")
            return None, symbols_parquet
    else:
        t_start  = bar_grid[0]
        t_end    = bar_grid[-1] + pd.Timedelta(minutes=BAR_MINUTES)
        if PRICE_SOURCE == "db":
            price_raw = get_session_prices_db(symbols_parquet, t_start, t_end)
        else:
            price_raw = get_session_prices_parquet(symbols_parquet, t_start, t_end)
        if price_raw.empty:
            print(f"    ! Parquet fetch returned empty for {symbols_parquet}")
            return None, symbols_parquet
        price_rs = (price_raw
                    .resample(f"{BAR_MINUTES}min").last()
                    .reindex(bar_grid)
                    .ffill()
                    .bfill())

    if debug:
        print(f"    [debug] price_rs shape: {price_rs.shape}")
        print(f"    [debug] price_rs index[:3]: {price_rs.index[:3].tolist()}")
        print(f"    [debug] price_rs[:3]:\n{price_rs.iloc[:3]}")

    # Drop symbols with no usable data at bar 0 — track them explicitly
    missing = [c for c in price_rs.columns if price_rs.iloc[0].isna()[c]]
    not_in_parquet = [s for s in symbols_parquet if s not in price_rs.columns]
    price_rs = price_rs.loc[:, price_rs.iloc[0].notna()]
    if price_rs.empty:
        print(f"    ! All symbols have NaN at bar 0 after resample")
        return None, symbols_parquet

    # Per-symbol raw (unleveraged) paths with stop
    sym_paths = {}
    for col in price_rs.columns:
        raw = apply_raw_stop(price_rs[col])
        if not np.all(np.isnan(raw)):
            sym_paths[col] = raw

    if not sym_paths:
        print(f"    ! No valid symbol paths after apply_raw_stop")
        return None, symbols_parquet

    paths_df  = pd.DataFrame(sym_paths, index=bar_grid)
    # Equal-weight average across symbols, THEN apply leverage
    portfolio = paths_df.mean(axis=1) * LEVERAGE
    no_data   = missing + not_in_parquet
    return portfolio, no_data


def main():
    print("=" * 70)
    print("REBUILD PORTFOLIO MATRIX WITH ELIGIBILITY GATE")
    print("=" * 70)
    print(f"  Source:    {PRICE_SOURCE.upper()}")
    if PRICE_SOURCE == "parquet":
        print(f"  Parquet:   {PARQUET_PATH}")
    print(f"  Leverage:  {LEVERAGE}x   Stop: {STOP_RAW_PCT}% (unleveraged 1x per symbol)")
    print(f"  Session:   {DEPLOYMENT_START_HOUR:02d}:00 – {DEPLOYMENT_END_HOUR:02d}:{DEPLOYMENT_END_MINUTE:02d} UTC  ({BAR_MINUTES}-min bars → {N_BARS} rows)")
    print(f"  Gate:      listing_age >= {MIN_LISTING_AGE_DAYS} days")
    if MAX_PORT is not None:
        print(f"  Max port:  {MAX_PORT} symbols per day (after gate)")
    print()

    deploys    = load_deploys(DEPLOYS_CSV)
    first_seen, first_seen_raw = load_first_seen(FIRST_SEEN_CSV)
    print(f"Deploy days:    {len(deploys)}")
    print(f"First-seen map: {len(first_seen)} symbols")
    print()

    # Canonical bar grid (same for every day — only time portion matters)
    bar_grid_template = build_bar_grid(deploys[0]["session_start"])
    bar_grid_times    = [t.strftime("%H:%M:%S") for t in bar_grid_template]  # for index formatting

    matrix_cols = {}   # col_name → list of "X.XX%" strings (N_BARS values)
    gate_rows   = []
    n_flat      = 0
    n_partial   = 0

    first_deploy_date = deploys[0]["date"]
    gate_start_date   = first_deploy_date + pd.Timedelta(days=MIN_LISTING_AGE_DAYS)
    print(f"First deploy: {first_deploy_date}  →  Gate active from: {gate_start_date}")
    print()

    cum_roi = 0.0
    for day in deploys:
        session_start = day["session_start"]
        deploy_date   = day["date"]
        col_name      = day["col_name"]
        deployed      = day["symbols"]

        # ── eligibility gate (only active after lookback warm-up) ────────────
        in_warmup = pd.Timestamp(deploy_date) < pd.Timestamp(gate_start_date)

        eligible_bases = []
        gate_detail    = []
        for s in deployed:
            if in_warmup:
                # Warmup period — accept all symbols unconditionally
                gate_detail.append({"symbol": s, "eligible": True, "age": None, "reason": "warmup"})
                eligible_bases.append(s)
            else:
                ok, age, reason = check_eligible(s, deploy_date, first_seen)
                gate_detail.append({"symbol": s, "eligible": ok, "age": age, "reason": reason})
                if ok:
                    eligible_bases.append(s)

        n_dep  = len(deployed)
        n_elig = len(eligible_bases)
        n_drop = n_dep - n_elig
        dropped_syms = [g["symbol"] for g in gate_detail if not g["eligible"]]

        # ── Cap to MAX_PORT (applied after eligibility gate) ──────────
        capped_syms = []
        if MAX_PORT is not None and n_elig > MAX_PORT:
            capped_syms    = eligible_bases[MAX_PORT:]
            eligible_bases = eligible_bases[:MAX_PORT]
            n_elig         = len(eligible_bases)

        gate_rows.append({
            "date":         str(deploy_date),
            "col_name":     col_name,
            "n_deployed":   n_dep,
            "n_eligible":   n_elig,
            "n_dropped":    n_drop,
            "deployed":     "|".join(deployed),
            "eligible":     "|".join(eligible_bases),
            "dropped":      "|".join(dropped_syms),
            "drop_reasons": "|".join([g["reason"] for g in gate_detail if not g["eligible"]]),
        })

        warmup_note = "  [warmup]" if in_warmup else ""
        drop_note   = f"  ← DROPPED {dropped_syms}" if n_drop else ""
        cap_note    = f"  ← CAPPED {capped_syms}" if capped_syms else ""
        # cumulative ROI printed after port_path is computed — placeholder for now
        _day_print = (deploy_date, n_dep, n_elig, warmup_note, drop_note, cap_note)

        if n_elig == 0:
            n_flat += 1
            matrix_cols[col_name] = ["0.00%"] * len(bar_grid_template)
            continue

        if n_drop > 0:
            n_partial += 1

        # ── build portfolio path ──────────────────────────────────────────────
        # Reconstruct the correct parquet ticker for each base symbol.
        # If the symbol was in first_seen_raw (e.g. "1000RATSUSDT"), use that directly.
        # Otherwise fall back to appending SYMBOL_SUFFIX (e.g. "RATS" → "RATSUSDT").
        def to_ticker(s):
            raw = first_seen_raw.get(s)
            if raw:
                return raw  # e.g. 1000RATSUSDT, PEPEUSDT, SHIBUSDT etc.
            if s.endswith("USDT"): return s
            if s.endswith("USD"):  return s + "T"
            return s + SYMBOL_SUFFIX
        symbols_parquet = [to_ticker(s) for s in eligible_bases]
        debug = False  # use --debug flag when needed
        port_path, no_data_syms = build_portfolio_path(symbols_parquet, session_start, debug=debug)

        # Symbols that passed the gate but had no parquet data — count as dropped
        if no_data_syms:
            no_data_bases = []
            for ticker in no_data_syms:
                # Reverse to_ticker: strip SYMBOL_SUFFIX to get base for display
                base = ticker.replace(SYMBOL_SUFFIX, "").replace("_PERP", "")
                m = _MULTIPLIER_RE.match(base)
                if m: base = m.group(2)
                no_data_bases.append(base)
            print(f"    ! No parquet data for: {no_data_bases} — excluded from portfolio average")
            # Fold into gate accounting so summary is accurate
            n_drop += len(no_data_bases)
            dropped_syms += no_data_bases
            gate_rows[-1]["n_dropped"]   = n_drop
            gate_rows[-1]["n_eligible"]  = n_elig - len(no_data_bases)
            gate_rows[-1]["dropped"]    += ("|" if gate_rows[-1]["dropped"] else "") + "|".join(no_data_bases)
            gate_rows[-1]["drop_reasons"] += ("|" if gate_rows[-1]["drop_reasons"] else "") + "|".join(["no_parquet_data"] * len(no_data_bases))
            if n_partial == 0 or gate_rows[-1]["n_dropped"] == len(no_data_bases):
                n_partial += 1  # only increment once per day

        if port_path is None:
            print(f"    ! No price data for {deploy_date} — storing zeros")
            matrix_cols[col_name] = ["0.00%"] * len(bar_grid_template)
            day_roi = 0.0
        else:
            # Drop the date-specific timestamp index → integer bar index 0..215
            vals = port_path.values
            if len(vals) < len(bar_grid_template):
                # Pad short sessions with last value
                vals = np.append(vals, [vals[-1]] * (len(bar_grid_template) - len(vals)))
            vals = vals[:len(bar_grid_template)]
            matrix_cols[col_name] = [f"{v:.2f}%" for v in vals]
            day_roi = float(vals[-1])   # last bar = session close ROI (leveraged)

        cum_roi += day_roi / LEVERAGE   # delever before cumulating
        d, nd, ne, wn, dn, cn = _day_print
        roi_str = f"  {day_roi:+.2f}%  cum(1x)={cum_roi:+.2f}%"
        print(f"  {d}  {nd}→{ne} syms{wn}{dn}{cn}{roi_str}")

    # ── Assemble and save matrix ───────────────────────────────────────────────
    print()
    print("Assembling output matrix...")

    # Build timestamp_utc index using the first day's actual date + bar times
    # (timestamp_utc column uses the session date for each bar like the original)
    first_session = deploys[0]["session_start"]
    ts_index = pd.date_range(
        start=first_session,
        periods=len(bar_grid_template),
        freq=f"{BAR_MINUTES}min"
    ).strftime("%Y-%m-%d %H:%M:%S").tolist()
    # Note: the original uses one date for all rows (the date of the first session).
    # Each column represents its own day — the row timestamps are just bar offsets.

    out_df = pd.DataFrame(matrix_cols)
    out_df.insert(0, "timestamp_utc", ts_index)
    out_df.to_csv(OUTPUT_MATRIX, index=False)
    print(f"Saved → {OUTPUT_MATRIX}  ({len(out_df)} rows × {len(out_df.columns)-1} day-cols)")

    # ── Gate report ────────────────────────────────────────────────────────────
    gate_df = pd.DataFrame(gate_rows)
    gate_df.to_csv(OUTPUT_GATE_REPORT, index=False)
    print(f"Saved → {OUTPUT_GATE_REPORT}")

    # ── Summary ────────────────────────────────────────────────────────────────
    total = len(deploys)
    lines = [
        "ELIGIBILITY GATE SUMMARY",
        "=" * 60,
        f"Total deploy days:        {total}",
        f"Days fully flat (gated):  {n_flat}  ({100*n_flat/total:.1f}%)",
        f"Days partially gated:     {n_partial}  ({100*n_partial/total:.1f}%)",
        f"Days unaffected:          {total - n_flat - n_partial}",
        "",
        f"{'Fold':<6} {'Days':>5}  {'Any Drop':>9}  {'Full Flat':>10}  Dropped symbols",
        "-" * 70,
    ]
    gate_df["date_dt"] = pd.to_datetime(gate_df["date"])
    for fold_name, f_start, f_end in FOLDS:
        mask = (gate_df["date_dt"] >= f_start) & (gate_df["date_dt"] <= f_end)
        f = gate_df[mask]
        if f.empty:
            continue
        any_drop      = (f["n_dropped"] > 0).sum()
        full_flat_n   = (f["n_eligible"] == 0).sum()
        all_dropped   = []
        for _, r in f[f["n_dropped"] > 0].iterrows():
            all_dropped.extend(r["dropped"].split("|") if r["dropped"] else [])
        unique_dropped = sorted(set(all_dropped))
        sym_str = ", ".join(unique_dropped[:8]) + ("..." if len(unique_dropped) > 8 else "")
        lines.append(f"{fold_name:<6} {len(f):>5}  {any_drop:>9}  {full_flat_n:>10}  {sym_str}")

    lines += [
        "",
        f"Gate:      MIN_LISTING_AGE_DAYS = {MIN_LISTING_AGE_DAYS}",
        f"Leverage:  {LEVERAGE}x   Stop: {STOP_RAW_PCT}% (unleveraged 1x per symbol)",
        f"Session:   {DEPLOYMENT_START_HOUR:02d}:00 – {DEPLOYMENT_END_HOUR:02d}:{DEPLOYMENT_END_MINUTE:02d} UTC  ({BAR_MINUTES}-min bars)",
    ]
    summary_str = "\n".join(lines)
    print()
    print(summary_str)
    with open(OUTPUT_SUMMARY, "w") as fh:
        fh.write(summary_str + "\n")
    print(f"\nSaved → {OUTPUT_SUMMARY}")

    # ── Final summary lines — parsed by grid_search.py ──────────────────────
    n_active = total - n_flat
    avg_syms = (
        sum(r["n_eligible"] for _, r in gate_df.iterrows()) / n_active
        if n_active > 0 else 0.0
    )
    # Compute max drawdown from per-day deleveraged returns
    daily_rets = []
    for col in [c for c in out_df.columns if c != "timestamp_utc"]:
        last_val = out_df[col].iloc[-1]
        try:
            daily_rets.append(float(str(last_val).replace("%","")) / LEVERAGE)
        except Exception:
            daily_rets.append(0.0)
    peak, max_dd = 0.0, 0.0
    running = 0.0
    for r in daily_rets:
        running += r
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    print(f"FINAL_CUM_ROI(1x):  {cum_roi:.4f}%")
    print(f"FINAL_MAX_DD(1x):   {max_dd:.4f}%")
    print(f"FINAL_ACTIVE_DAYS:  {n_active}")
    print(f"FINAL_AVG_SYMS:     {avg_syms:.2f}")
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rebuild portfolio matrix with eligibility gate.")
    parser.add_argument("--deploys", type=str, default=None,
                        help="Path to deploys CSV/TSV (overrides DEPLOYS_CSV constant).")
    parser.add_argument("--output",  type=str, default=None,
                        help="Output path for the matrix CSV (overrides OUTPUT_MATRIX constant).")
    parser.add_argument("--source", type=str, default=None,
                        choices=["binance", "parquet", "db"],
                        help="Price source: binance (candle closes, matches Sheets), "
                             "parquet (fast, local ticks), or db (market.futures_1m). "
                             "Default: parquet.")
    parser.add_argument("--min-listing-age", type=int, default=None,
                        dest="min_listing_age",
                        help="Override MIN_LISTING_AGE_DAYS (e.g. 0 to disable gate). "
                             "Default: value of MIN_LISTING_AGE_DAYS constant (30).")
    parser.add_argument("--max_port", type=int, default=None,
                        help="Max symbols per day after eligibility gate (e.g. --max_port 20). "
                             "Symbols are taken in order as they appear in the deploys file. "
                             "Default: no cap.")
    parser.add_argument("--start-hour", type=int, default=None, dest="start_hour",
                        metavar="H",
                        help="Deployment window start hour UTC (0-23). "
                             "Overrides DEPLOYMENT_START_HOUR constant (default: 6 = 06:00 UTC). "
                             "Affects bar grid, Binance fetch window, and column name suffix.")
    parser.add_argument("--deployment-runtime-hours", type=str, default=None,
                        dest="deployment_runtime_hours", metavar="H",
                        help="Deployment window length: integer hours or \"daily\". "
                             "\"daily\" sets runtime = 24 - sort_lookback so the full "
                             "24h cycle is filled exactly. Overrides DEPLOYMENT_RUNTIME_HOURS "
                             "constant (default: daily). Recomputes N_BARS and session end time.")
    parser.add_argument("--sort-lookback", type=str, default=None,
                        dest="sort_lookback", metavar="N",
                        help="Electoral window length: integer hours or 'daily'. "
                             "Overrides SORT_LOOKBACK constant (default: 6). "
                             "Stored for reference — does not affect bar grid or price fetch.")
    parser.add_argument("--end-cross-midnight", dest="end_cross_midnight",
                        action="store_true", default=None,
                        help="Allow deployment window to overflow past 23:59 UTC into the "
                             "next calendar day (default: True). Pass --no-end-cross-midnight to "
                             "cap the session at 23:55 on the same day instead.")
    parser.add_argument("--no-end-cross-midnight", dest="end_cross_midnight",
                        action="store_false",
                        help="Cap the deployment window at 23:55 UTC on the same calendar day "
                             "when start + runtime overflows midnight. Truncates N_BARS to fit.")
    args = parser.parse_args()

    if args.deploys:
        DEPLOYS_CSV = args.deploys
    if args.output:
        OUTPUT_MATRIX = args.output
    if args.max_port is not None:
        MAX_PORT = args.max_port
    if args.source is not None:
        PRICE_SOURCE = args.source
    if args.min_listing_age is not None:
        MIN_LISTING_AGE_DAYS = args.min_listing_age
    if args.end_cross_midnight is not None:
        END_CROSS_MIDNIGHT = args.end_cross_midnight
    if getattr(args, "sort_lookback", None) is not None:
        SORT_LOOKBACK = args.sort_lookback if args.sort_lookback == "daily" \
                        else int(args.sort_lookback)
    if getattr(args, "deployment_runtime_hours", None) is not None:
        # Accept "daily" or an integer string
        _rt = args.deployment_runtime_hours
        DEPLOYMENT_RUNTIME_HOURS = "daily" if str(_rt).lower() == "daily" else int(_rt)
        N_BARS = int(_resolve_runtime_hours() * 60 // BAR_MINUTES)
    if args.start_hour is not None:
        # Recompute all derived deployment window values from the new start hour
        DEPLOYMENT_START_HOUR   = args.start_hour
    if args.start_hour is not None or getattr(args, "deployment_runtime_hours", None) is not None:
        # Recompute session end whenever either start or runtime changes
        _SESSION_END_MINUTES  = DEPLOYMENT_START_HOUR * 60 + (N_BARS - 1) * BAR_MINUTES
        _END_CROSSES_MIDNIGHT = _SESSION_END_MINUTES >= 24 * 60
        DEPLOYMENT_END_HOUR   = (_SESSION_END_MINUTES % (24 * 60)) // 60
        DEPLOYMENT_END_MINUTE = _SESSION_END_MINUTES % 60

    main()

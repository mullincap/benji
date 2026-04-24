"""
overlap_analysis.py
====================
Full pipeline:
  1. Filter both price and open interest leaderboards by market cap
  2. For each date, count symbol frequency in R1-R60 between 00:00-06:00
  3. Return per-date list of symbols appearing in BOTH leaderboards

Output:
  leaderboard_price_filtered_{M}.parquet
  leaderboard_open_interest_filtered_{M}.parquet
  overlap_top60_0000_0600_{M}.parquet  — one row per date, list of overlapping symbols

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CANONICAL RUN COMMAND
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


cd /Users/johnmullin/Desktop/desk/benji3m &&

python3 overlap_analysis.py \
  --leaderboard-index 100 \
  --min-mcap 0 \
  --sort-by price \
  --mode snapshot \
  --sample-interval 5 \
  --freq-width 20 \
  --freq-cutoff 20 \
  --audit \
  --audit-script audit.py \
  2>&1 | tee audit_output.txt

python3 overlap_analysis.py --audit 2>&1 | tee audit_output.txt

GENERATE REPORT (after audit completes):

cd /Users/johnmullin/Desktop/desk/benji3m && \
  node generate_audit_report.js audit_output.txt overlap_audit_report.docx

To force a fresh filter pass (delete cached files first):
  rm leaderboard_price_top100_filtered_0M*.parquet \
     leaderboard_open_interest_top100_filtered_0M*.parquet

To enable the market cap diagnostic (~4 min extra):
  Set ENABLE_MCAP_DIAGNOSTIC = True in audit.py before running.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import re
import argparse
import subprocess
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
from collections import Counter
import os
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────

import os as _os
BASE_DIR = Path(_os.environ.get("BASE_DATA_DIR", "/Users/johnmullin/Desktop/desk/benji3m"))

# Per-metric leaderboard directory helper from pipeline/config.py.
# Used by _leaderboard_paths() below to resolve master-parquet input files
# to the canonical /mnt/quant-data/leaderboards/<metric>/ path on prod
# (or whatever LEADERBOARDS_DIR is in config.py — derived from BASE_DATA_DIR).
# Replaces the previous flat BASE_DIR / filename construction, which only
# worked when BASE_DIR happened to point at the per-metric subdirectory.
from config import leaderboard_dir_for_metric

# Index size for both leaderboards: 100, 300, or 1000
LEADERBOARD_INDEX = int(_os.environ.get("LEADERBOARD_INDEX", "100"))

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

# ── Deployment window ────────────────────────────────────────────────────────
# DEPLOYMENT_START_HOUR : UTC hour at which the deployment (invested) period begins.
#   Default 6 = 06:00 UTC.  Grid search in audit.py varies this across 0–23.
#   The electoral (frequency-measurement) window always ends at this hour.
#   Override via --deployment-start-hour CLI arg.
DEPLOYMENT_START_HOUR    = int(_os.environ.get("DEPLOYMENT_START_HOUR", "6"))    # 06:00 UTC — independent variable for grid search
_sort_lookback_env       = _os.environ.get("SORT_LOOKBACK", "6")
SORT_LOOKBACK            = "daily" if _sort_lookback_env == "daily" else int(_sort_lookback_env)  # electoral window length: int hours back from
                                        # deployment_start_hour, or "daily" to always
                                        # scan from 00:05 to deployment_start_hour

def _anchor_hhmm() -> str:
    """Return the HHMM string for the leaderboard anchor time.
    anchor = deployment_start_hour - index_lookback (wraps correctly).
    """
    return f"{(DEPLOYMENT_START_HOUR - INDEX_LOOKBACK) % 24:02d}00"


def _leaderboard_paths(leaderboard_index: int):
    """Return (price_path, oi_path) for the current INDEX_LOOKBACK setting.

    When INDEX_LOOKBACK == DEPLOYMENT_START_HOUR the anchor is midnight (00:00),
    which matches the existing files built without the anchor suffix.
    All other anchor times get an explicit _anchor{HHMM} suffix so they are
    stored as distinct cached files.

    The directory is resolved per-metric via leaderboard_dir_for_metric()
    from pipeline/config.py — which on prod points at
    /mnt/quant-data/leaderboards/<metric>/, the canonical home for master
    parquet files. Until 2026-04-08 this used a single flat BASE_DIR
    parameter (= /data, ephemeral scratch), which never matched where the
    builder actually wrote files and forced the simulator to repeatedly
    re-build the master parquet from scratch.

    The leaderboard_index parameter is preserved for call-site compatibility
    but is currently unused — the filename uses LEADERBOARD_TOP_N instead.
    """
    del leaderboard_index  # legacy parameter, kept for API compatibility
    idx    = f"top{LEADERBOARD_TOP_N}"
    anchor = _anchor_hhmm()
    suffix = f"_anchor{anchor}"  # always included, matches builder output
    price = leaderboard_dir_for_metric("price")         / f"intraday_pct_leaderboard_price_{idx}{suffix}_ALL.parquet"
    oi    = leaderboard_dir_for_metric("open_interest") / f"intraday_pct_leaderboard_open_interest_{idx}{suffix}_ALL.parquet"
    return price, oi


def _resolve_sort_lookback() -> int:
    """Return the effective sort lookback in hours.

    The leaderboard index measures % change since midnight, resetting at 00:00 UTC
    every day. A lookback window reaching before midnight crosses epochs and produces
    meaningless comparisons. The effective lookback is therefore always internal:

        effective_lookback = min(sort_lookback, deployment_start_hour)

    sort_lookback="daily" resolves to deployment_start_hour exactly, so it is
    always consistent and the cap has no effect.
    deployment_start_hour=0 always gives effective_lookback=0 (no electoral window).
    """
    raw = DEPLOYMENT_START_HOUR if SORT_LOOKBACK == "daily" else int(SORT_LOOKBACK)
    return min(raw, DEPLOYMENT_START_HOUR)

INDEX_LOOKBACK            = int(_os.environ.get("INDEX_LOOKBACK", "6"))         # hours before deployment_start_hour used as
                                      # the % change anchor in the leaderboard index.
                                      # 6 = midnight when start_hour=6 (default behaviour).
                                      # anchor = deployment_start_hour - index_lookback

# Absolute path to the canonical 1098-line builder. Computed from this file's
# own location so cwd doesn't matter when overlap_analysis.py is invoked as a
# subprocess from elsewhere (the celery worker, the simulator UI path, etc).
# Until 2026-04-08 this was the bare filename "build_intraday_leaderboard.py",
# which resolved against cwd to a STALE 414-line ancestor sitting at
# pipeline/build_intraday_leaderboard.py — a file that lacked both today's
# OOM fixes and was OOM-killing the celery worker on every simulator click.
# That stale file has now been deleted; this absolute path makes the
# correct file unambiguous.
LEADERBOARD_BUILDER       = str(Path(__file__).resolve().parent / "indexer" / "build_intraday_leaderboard.py")
# Path to the master parquet feeding the leaderboard builder. PARQUET_PATH
# must be set in the environment (.env / .env.production / shell). No
# fallback — if it's missing the script fails loudly at the first parquet
# read instead of silently pointing at a stale Mac-local path.
LEADERBOARD_PARQUET_PATH  = os.environ.get("PARQUET_PATH")
LEADERBOARD_TOP_N         = int(_os.environ.get("LEADERBOARD_TOP_N", "333"))  # TOP_N used by build_intraday_leaderboard.py
                                  # (independent of LEADERBOARD_INDEX which controls
                                  #  how many symbols are considered in the overlap)

_deployment_runtime_env   = _os.environ.get("DEPLOYMENT_RUNTIME_HOURS", "daily")
DEPLOYMENT_RUNTIME_HOURS  = "daily" if str(_deployment_runtime_env).lower() == "daily" else int(_deployment_runtime_env)

# After computing frequency across 00:00-06:00, keep only the top N most
# frequent symbols from each leaderboard before taking the intersection.
# e.g. 30 → intersect top-30 price symbols with top-30 OI symbols → max 30 overlap
FREQ_TOP_N = int(_os.environ.get("FREQ_CUTOFF", "20"))

# How to sort the overlap symbols for the deploys CSV.
# "price"         → sort by price leaderboard frequency rank
# "open_interest" → sort by OI leaderboard frequency rank
# "combined"      → sort by sum of both frequency counts
OVERLAP_SORT_BY = _os.environ.get("SORT_BY", "price")

# Downsample the 00:00-06:00 window to every N minutes before building frequency tables.
# e.g. 14 → use bars at 00:00, 00:14, 00:28 ... reducing noise from dense 1-min data.
# Must be a multiple of the underlying bar frequency (1-min bars → any integer >= 1).
SAMPLE_INTERVAL_MINUTES = int(_os.environ.get("SAMPLE_INTERVAL", "5"))

# ─────────────────────────────────────────────
# Google Sheets config
# ─────────────────────────────────────────────

GOOGLE_SHEET_ID = "19HP3wIRVbU8Wy2xGo4-uhJFK50h26GCFsK5Wk1I7WwE"
GSHEETS_SCOPES  = ["https://www.googleapis.com/auth/spreadsheets"]
GSHEETS_TOKEN_PATH       = str(BASE_DIR / "token.json")
GSHEETS_CREDENTIALS_PATH = str(BASE_DIR / "credentials.json")

# ── Output toggles ───────────────────────────────────────────────────
PRINT_FREQ_TABLE  = False   # Print full frequency table to terminal after computation
EXPORT_TO_GSHEETS = False   # Export confluence + deploys tabs to Google Sheets


# ─────────────────────────────────────────────
# Symbol normalization (same as filter script)
# ─────────────────────────────────────────────

NON_CRYPTO = {"AMZN", "TSLA", "INTC", "XAU", "XAG", "XPD", "XPT","AAPL", "GOOGL", "MSFT", "NVDA", "META",}
STABLECOINS = {"USDT", "USDC", "BUSD", "TUSD", "USDP", "FDUSD","USDS", "USDE", "FRAX", "DAI", "PYUSD", "USD1",}
MULTIPLIER_RE = re.compile(r"^(\d+)(.*)")


def normalize_symbol(raw: str) -> str | None:
    if not isinstance(raw, str):
        return None
    try:
        raw.encode("ascii")
    except UnicodeEncodeError:
        return None
    s = raw.upper()
    s = re.sub(r"_\d{6}$", "", s)
    s = re.sub(r"_PERP$", "", s)
    for quote in ["USDT", "USDC", "USD", "BUSD", "BTC", "ETH", "BNB"]:
        if s.endswith(quote) and len(s) > len(quote):
            s = s[: -len(quote)]
            break
    else:
        return None
    m = MULTIPLIER_RE.match(s)
    if m:
        s = m.group(2)
    if not s:
        return None
    if s in NON_CRYPTO or s in STABLECOINS:
        return None
    return s


# ─────────────────────────────────────────────
# Market cap lookup
# ─────────────────────────────────────────────

def build_mcap_lookup(marketcap_path: Path, min_mcap: float, max_mcap: float = 0.0) -> dict:
    log.info("Loading market cap data...")
    mc = pd.read_parquet(marketcap_path)
    mc["date"] = pd.to_datetime(mc["date"], utc=True).dt.date

    # Build coin_id -> symbol map from coins_universe.parquet
    # (the symbol column in marketcap_daily is only populated for ~999 rows)
    universe_path = marketcap_path.parent / "coins_universe.parquet"
    if universe_path.exists():
        uni = pd.read_parquet(universe_path)
        id_to_symbol = dict(zip(uni["id"], uni["symbol"].str.upper()))
        log.info(f"  Loaded {len(id_to_symbol)} coin_id→symbol mappings from universe")
    else:
        # Fallback: build from rows that do have symbol populated
        has_sym = mc[mc["symbol"].notna()][["coin_id", "symbol"]].drop_duplicates()
        id_to_symbol = dict(zip(has_sym["coin_id"], has_sym["symbol"].str.upper()))
        log.info(f"  Built {len(id_to_symbol)} coin_id→symbol mappings from marketcap rows")

    mc["base"] = mc["coin_id"].map(id_to_symbol)

    # All symbols we have ANY mcap data for, per date (coverage set)
    # Used for fail-open: unknown symbols pass, only confirmed-below-threshold fail
    coverage = {}
    for date, group in mc[mc["base"].notna()].groupby("date"):
        coverage[date] = set(group["base"])

    _max = max_mcap if max_mcap > 0 else float("inf")
    eligible = mc[
        (mc["market_cap_usd"] >= min_mcap) &
        (mc["market_cap_usd"] <= _max) &
        mc["base"].notna()
    ].copy()
    if max_mcap > 0:
        log.info(f"  Max mcap filter: ${max_mcap/1e6:.0f}M — symbols above this cap are excluded")

    lookup = {}
    for date, group in eligible.groupby("date"):
        lookup[date] = set(group["base"])
    log.info(f"Market cap lookup built for {len(lookup)} dates")
    return lookup, coverage


# ─────────────────────────────────────────────
# Filter leaderboard
# ─────────────────────────────────────────────

def filter_leaderboard(input_path: Path, output_path: Path,mcap_lookup: dict, mcap_coverage: dict, min_mcap: float, drop_unverified: bool = False, max_mcap_lookup: dict = None):
    log.info(f"Loading {input_path.name}...")
    df = pd.read_parquet(input_path)
    log.info(f"  → {len(df):,} rows loaded")

    rank_cols = sorted(
        [c for c in df.columns if c.startswith("R") and c[1:].isdigit()],
        key=lambda x: int(x[1:])
    )

    ts_col = "timestamp_utc"
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df["_date"] = df[ts_col].dt.date

    # Build norm cache
    all_syms = pd.unique(df[rank_cols].values.ravel())
    norm_cache = {s: normalize_symbol(s) for s in all_syms if isinstance(s, str)}

    result_parts = []
    for date in tqdm(sorted(df["_date"].unique()),
                     desc=f"Filtering {input_path.stem[:20]}", unit="day"):
        eligible_bases = mcap_lookup.get(date, set())
        day_df = df[df["_date"] == date].copy()
        rank_vals = day_df[rank_cols].values

        filtered = []
        for row in rank_vals:
            # Keep eligible symbols, deduplicate by base — first occurrence wins (highest rank)
            seen_bases = set()
            kept = []
            for sym in row:
                if not isinstance(sym, str):
                    continue
                base = norm_cache.get(sym)
                if base is None:
                    continue
                if min_mcap > 0 or drop_unverified:
                    known_today = mcap_coverage.get(date, set())
                    if min_mcap > 0 and base in known_today and base not in eligible_bases:
                        # Confirmed mcap data exists and is below threshold — filter out
                        continue
                    elif base not in known_today and drop_unverified:
                        # No mcap data for this symbol on this date → drop (fail closed)
                        continue
                    # else: no mcap data for this symbol on this date → fail open (pass through)
                if max_mcap_lookup is not None:
                    above_cap_today = max_mcap_lookup.get(date, set())
                    if base in above_cap_today:
                        # Confirmed mcap exists and is above ceiling — filter out
                        continue
                if base in seen_bases:
                    continue  # same base already present at a higher rank — skip variant
                seen_bases.add(base)
                kept.append(sym)
            kept += [None] * (len(rank_cols) - len(kept))
            filtered.append(kept)

        filtered_df = pd.DataFrame(filtered, columns=rank_cols, index=day_df.index)
        result_parts.append(pd.concat([day_df[[ts_col]], filtered_df], axis=1))

    result = pd.concat(result_parts, ignore_index=True)
    result.sort_values(ts_col, inplace=True)
    result.reset_index(drop=True, inplace=True)
    result.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    original_count = sum(1 for col in rank_cols for v in df[col] if isinstance(v, str))
    filtered_count = sum(1 for col in rank_cols for v in result[col] if isinstance(v, str))
    removed = original_count - filtered_count
    log.info(f"Saved filtered leaderboard → {output_path.name}")
    log.info(f"  → Kept {filtered_count:,} entries, removed {removed:,} ({removed/max(original_count,1)*100:.1f}%)")
    return result


# ─────────────────────────────────────────────
# Frequency count 00:00-06:00, top N cols
# ─────────────────────────────────────────────

def compute_daily_frequency(df: pd.DataFrame, freq_width: int) -> dict:
    """
    For each date, count symbol frequency in R1-RN across the electoral window.

    Window bounds are controlled by SORT_LOOKBACK and DEPLOYMENT_START_HOUR:

      SORT_LOOKBACK == "daily"
        From 00:05 up to (not including) DEPLOYMENT_START_HOUR on the same day.
        Resolves to deployment_start_hour hours. Window always starts just after midnight.

      SORT_LOOKBACK == N  (integer hours)
        From (DEPLOYMENT_START_HOUR - N) up to (not including) DEPLOYMENT_START_HOUR.
        If start_hour - N < 0, the window wraps to the previous calendar day:
          e.g. start=02:00, lookback=6 -> sort_start=20:00 prev day -> 20:05-01:55.
        The opening bar at sort_start:00 is excluded (same as midnight 00:00 exclusion).

    Input df is already row-sampled by get_or_create_sampled.
    Returns: date -> Counter of {base_symbol: count}
    """
    rank_cols = sorted(
        [c for c in df.columns if c.startswith("R") and c[1:].isdigit()],
        key=lambda x: int(x[1:])
    )[:freq_width]

    ts_col = "timestamp_utc"
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)

    # ── Compute electoral window ──────────────────────────────────────────────
    # Effective lookback is internal at deployment_start_hour — the leaderboard
    # resets at midnight so reaching before 00:00 crosses epochs.
    _eff_lookback    = _resolve_sort_lookback()
    _sort_start_hour = DEPLOYMENT_START_HOUR - _eff_lookback  # always >= 0 after cap

    if _eff_lookback == 0:
        # No electoral window — return empty counters (start_hour == 0)
        return {}
    elif _sort_start_hour == 0:
        # Window starts exactly at midnight — exclude the 00:00 bar
        window = df[
            (df[ts_col].dt.hour < DEPLOYMENT_START_HOUR) &
            ~((df[ts_col].dt.hour == 0) & (df[ts_col].dt.minute == 0))
        ].copy()
    else:
        # Same-day window: _sort_start_hour -> DEPLOYMENT_START_HOUR
        window = df[
            (df[ts_col].dt.hour >= _sort_start_hour) &
            (df[ts_col].dt.hour < DEPLOYMENT_START_HOUR) &
            ~((df[ts_col].dt.hour == _sort_start_hour) & (df[ts_col].dt.minute == 0))
        ].copy()

    # ── Normalize symbols ─────────────────────────────────────────────────────
    all_syms = pd.unique(window[rank_cols].values.ravel())
    norm_cache = {s: normalize_symbol(s) for s in all_syms if isinstance(s, str)}

    # ── Count frequency per deployment date ───────────────────────────────────
    groupby_key = window[ts_col].dt.date

    daily_freq = {}
    for date, group in window.groupby(groupby_key):
        counter = Counter()
        for _, bar in group.iterrows():
            # Deduplicate per bar: a symbol counts once per timestamp even if it
            # appears under multiple raw tickers (e.g. BNBUSDT and BNBUSDC)
            seen_this_bar = set()
            for col in rank_cols:
                sym = bar[col]
                if isinstance(sym, str):
                    base = norm_cache.get(sym)
                    if base and base not in seen_this_bar:
                        seen_this_bar.add(base)
                        counter[base] += 1
        daily_freq[date] = counter

    return daily_freq


def compute_daily_snapshot(df: pd.DataFrame, freq_width: int) -> dict:
    """
    For each date, take the 06:00 UTC bar (or nearest prior bar as fallback).
    Returns: date → Counter of {symbol: 1} (present/absent, no frequency)
    """
    rank_cols = sorted(
        [c for c in df.columns if c.startswith("R") and c[1:].isdigit()],
        key=lambda x: int(x[1:])
    )[:freq_width]

    ts_col = "timestamp_utc"
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df["_date"] = df[ts_col].dt.date

    daily_snap = {}
    for date, group in df.groupby("_date"):
        # Try exact DEPLOYMENT_START_HOUR bar
        target = pd.Timestamp(date, tz="UTC").replace(hour=DEPLOYMENT_START_HOUR)
        exact = group[group[ts_col] == target]

        if not exact.empty:
            row = exact.iloc[0]
        else:
            # Fallback: latest bar before DEPLOYMENT_START_HOUR
            prior = group[group[ts_col] < target]
            if prior.empty:
                continue
            row = prior.sort_values(ts_col).iloc[-1]

        # Build norm cache for this bar's symbols
        bar_syms = {row[col] for col in rank_cols if isinstance(row[col], str)}
        norm_cache = {s: normalize_symbol(s) for s in bar_syms}

        counter = Counter()
        for col in rank_cols:
            sym = row[col]
            if isinstance(sym, str):
                base = norm_cache.get(sym)
                if base:
                    counter[base] = 1
        daily_snap[date] = counter

    return daily_snap


# ─────────────────────────────────────────────
# Overlap analysis
# ─────────────────────────────────────────────

OVERLAP_DIMENSIONS_CHOICES = ("price_oi", "price_volume", "oi_volume", "price_oi_volume")


def compute_overlap(price_freq: dict, oi_freq: dict,
                    freq_cutoff: int = FREQ_TOP_N,
                    sort_by: str = OVERLAP_SORT_BY,
                    volume_freq: dict | None = None,
                    overlap_dimensions: str = "price_oi") -> pd.DataFrame:
    """
    For each date, find symbols appearing in the intersection of the
    frequency tables selected by `overlap_dimensions`.

    freq_cutoff: only the top-N most frequent symbols from each leaderboard
    are considered before intersecting.

    overlap_dimensions (Stream D-explore, candidate-exploration knob):
      'price_oi'         — top-N-price ∩ top-N-OI  (canonical, default)
      'price_volume'     — top-N-price ∩ top-N-volume
      'oi_volume'        — top-N-OI ∩ top-N-volume
      'price_oi_volume'  — three-way intersection

    `volume_freq` is required (non-None) whenever overlap_dimensions
    includes 'volume'. Caller computes it the same way price/OI are
    computed, with `metric="volume"` on _load_canonical_futures_1m_
    frequencies. Canonical 'price_oi' mode leaves volume_freq=None.

    sort_by: "price" | "open_interest" | "combined" | "price-only" | "oi-only"
      price-only/oi-only skip the intersection and use just that leaderboard;
      other values drive the ordering within the selected intersection.
      The sort_by knob is orthogonal to overlap_dimensions — it controls
      the ordering of symbols within the already-selected basket.

    Returns DataFrame with columns: date | price_symbols | oi_symbols |
    volume_symbols (empty list when volume not used) | overlap_symbols |
    overlap_count | price_count | oi_count | volume_count.
    """
    if overlap_dimensions not in OVERLAP_DIMENSIONS_CHOICES:
        raise ValueError(
            f"overlap_dimensions must be one of {OVERLAP_DIMENSIONS_CHOICES}, "
            f"got {overlap_dimensions!r}"
        )
    uses_volume = "volume" in overlap_dimensions
    if uses_volume and volume_freq is None:
        raise ValueError(
            f"overlap_dimensions={overlap_dimensions!r} requires volume_freq "
            f"to be supplied (caller should compute it via "
            f"_load_canonical_futures_1m_frequencies with metric='volume')."
        )

    all_dates = sorted(
        set(price_freq.keys()) | set(oi_freq.keys())
        | (set(volume_freq.keys()) if uses_volume else set())
    )
    rows = []

    def _top_n(counter: dict) -> set:
        return set(sym for sym, _ in sorted(counter.items(), key=lambda x: x[1], reverse=True)[:freq_cutoff])

    for date in all_dates:
        p_counter = price_freq.get(date, {})
        o_counter = oi_freq.get(date, {})
        v_counter = volume_freq.get(date, {}) if uses_volume else {}

        p_syms = _top_n(p_counter)
        o_syms = _top_n(o_counter)
        v_syms = _top_n(v_counter) if uses_volume else set()

        # ── sort_by overrides the intersection for the legacy price-only /
        # oi-only paths; otherwise overlap_dimensions drives set selection ──
        if sort_by == "price-only":
            overlap = sorted(p_syms)
            key_fn = lambda s: p_counter.get(s, 0)
        elif sort_by == "oi-only":
            overlap = sorted(o_syms)
            key_fn = lambda s: o_counter.get(s, 0)
        else:
            if overlap_dimensions == "price_oi":
                selected = p_syms & o_syms
            elif overlap_dimensions == "price_volume":
                selected = p_syms & v_syms
            elif overlap_dimensions == "oi_volume":
                selected = o_syms & v_syms
            else:  # price_oi_volume
                selected = p_syms & o_syms & v_syms

            if sort_by == "open_interest":
                key_fn = lambda s: o_counter.get(s, 0)
            elif sort_by == "combined":
                key_fn = lambda s: p_counter.get(s, 0) + o_counter.get(s, 0) + (v_counter.get(s, 0) if uses_volume else 0)
            else:  # "price" — default
                key_fn = lambda s: p_counter.get(s, 0)
            overlap = sorted(selected)
        overlap_sorted = sorted(overlap, key=key_fn, reverse=True)

        rows.append({
            "date":             pd.Timestamp(date),
            "price_symbols":    sorted(p_syms, key=lambda s: p_counter.get(s, 0), reverse=True),
            "oi_symbols":       sorted(o_syms, key=lambda s: o_counter.get(s, 0), reverse=True),
            "volume_symbols":   sorted(v_syms, key=lambda s: v_counter.get(s, 0), reverse=True) if uses_volume else [],
            "overlap_symbols":  overlap_sorted,
            "overlap_count":    len(overlap_sorted),
            "price_count":      len(p_syms),
            "oi_count":         len(o_syms),
            "volume_count":     len(v_syms) if uses_volume else 0,
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def export_deploys(overlap_df: pd.DataFrame, output_path: Path):
    """
    Export overlap_symbols to a deploys CSV matching the audit format:
      timestamp_utc  R1  R2  R3 ...
      2025-02-17 6:00:00  PEOPLE  LPT ...

    Normalizes symbols to base asset (strips USDT/USDC/_PERP/date suffixes,
    1000x prefixes etc.) and deduplicates per row.
    """
    rows = []

    for _, row in overlap_df.iterrows():
        raw_syms = row["overlap_symbols"]
        ts = pd.Timestamp(row["date"]).strftime("%-m/%-d/%Y") + f" {DEPLOYMENT_START_HOUR}:00:00"

        # Symbols are already normalized (base asset only) coming from compute_overlap.
        # Deduplicate preserving order; skip any None/empty values.
        seen = set()
        base_syms = []
        for sym in raw_syms:
            if sym and sym not in seen:
                seen.add(sym)
                base_syms.append(sym)

        entry = {"timestamp_utc": ts}
        for i, sym in enumerate(base_syms):
            entry[f"R{i+1}"] = sym
        rows.append(entry)

    max_cols = max((len(r) - 1) for r in rows) if rows else 0
    rank_cols = [f"R{i+1}" for i in range(max_cols)]
    deploys_df = pd.DataFrame(rows, columns=["timestamp_utc"] + rank_cols)
    deploys_df.to_csv(output_path, index=False)
    log.info(f"Deploys CSV saved → {output_path}")
    log.info(f"  → {len(deploys_df)} dates, avg {deploys_df[rank_cols].notna().sum(axis=1).mean():.1f} symbols/day")
    return output_path


def run_audit(deploys_path: Path, audit_script: Path, extra_args: list = None) -> dict:
    """
    Run audit.py with the generated deploys CSV.
    Streams output live to terminal and returns a dict of FINAL_* summary values
    parsed from the audit output for the end-of-run summary panel.
    """
    cmd = ["python3", str(audit_script), "--deploys", str(deploys_path)]
    if extra_args:
        cmd += extra_args
    log.info(f"Running audit: {' '.join(cmd)}")

    # finals: { tag -> { SHARPE, CAGR, MAX_DD, ACTIVE_DAYS, WF_CV } }
    finals = {}
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1)
    for line in proc.stdout:
        print(line, end="", flush=True)
        # Parse FINAL_ lines: FINAL_<METRIC>(<tag>): <value>
        # e.g. "FINAL_SHARPE(Balanced-Opt_-_Tail_Guardrail):  2.4390"
        stripped = line.strip()
        for metric in ("FINAL_SHARPE", "FINAL_CAGR", "FINAL_MAX_DD",
                       "FINAL_ACTIVE_DAYS", "FINAL_WF_CV", "FINAL_TOTAL_RETURN",
                       "FINAL_WORST_DAY", "FINAL_WORST_WEEK", "FINAL_WORST_MONTH",
                       "FINAL_DSR", "FINAL_GRADE_SCORE"):
            if stripped.startswith(metric + "("):
                try:
                    tag = stripped[len(metric)+1 : stripped.index(")")]
                    val_str = stripped.split(":", 1)[-1].strip()
                    val = float(val_str)
                    if tag not in finals:
                        finals[tag] = {}
                    finals[tag][metric] = val
                except (ValueError, IndexError):
                    pass
    proc.wait()
    if proc.returncode != 0:
        log.warning(f"audit.py exited with code {proc.returncode}")
    return finals


def get_or_create_sampled(base_path: Path, sample_interval: int) -> pd.DataFrame:
    """
    Check for a cached filtered+sampled file at the given interval.
    If it exists, load and return it. Otherwise generate from base_path,
    save alongside it, and return. Base file is never modified.
    e.g. leaderboard_price_top1000_filtered_50M.parquet → leaderboard_price_top1000_filtered_50M_5m.parquet
    """
    stem   = base_path.stem   # e.g. leaderboard_price_top1000_filtered_50M
    cached = base_path.parent / f"{stem}_{sample_interval}m.parquet"

    if cached.exists():
        log.info(f"Sampled file found ({sample_interval}m) → loading {cached.name}")
        df = pd.read_parquet(cached)
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        return df

    log.info(f"No {sample_interval}m sampled file found — generating from {base_path.name}...")
    df = pd.read_parquet(base_path)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    # Keep bars where (minutes since midnight) % sample_interval == 0.
    # This is anchored to 00:00 each day, so gaps/duplicates in the raw data
    # don't shift the grid the way iloc[::N] would.
    minutes_since_midnight = df["timestamp_utc"].dt.hour * 60 + df["timestamp_utc"].dt.minute
    df_sampled = df[minutes_since_midnight % sample_interval == 0].copy()
    df_sampled.to_parquet(cached, engine="pyarrow", compression="snappy", index=False)
    log.info(f"Saved sampled file → {cached.name}  ({len(df_sampled):,} rows, down from {len(df):,})")
    return df_sampled


def _snapshot_timestamps_at_hour(hour: int, years_back: int = 10) -> list:
    """
    Return one tz-aware UTC datetime per day at HH:00:00 covering `years_back`
    years of history up through today. Used to rewrite EXTRACT-based snapshot
    queries into `timestamp_utc = ANY(...)` lookups that the hypertable PK
    can serve via index seek instead of scanning every chunk.

    Array size is days_back × 1 ≈ 3650 at 10y — trivially small for Postgres.
    Overshooting the actual data range is harmless (missing timestamps just
    return no rows from the index seek).
    """
    import datetime as _dt
    today = _dt.datetime.now(_dt.timezone.utc).date()
    start = today - _dt.timedelta(days=years_back * 366)
    out: list = []
    d = start
    one_day = _dt.timedelta(days=1)
    while d <= today:
        out.append(_dt.datetime.combine(d, _dt.time(hour, 0, 0), tzinfo=_dt.timezone.utc))
        d += one_day
    return out


def _load_frequency_from_db(
    metric: str,
    freq_width: int,
    sample_interval: int,
    mode: str,
    min_mcap: float = 0.0,
    max_mcap: float = 0.0,
    drop_unverified: bool = False,
) -> dict:
    """
    Query market.leaderboards directly and return the same
    dict[date, Counter] that compute_daily_frequency / compute_daily_snapshot
    produce from the parquet path. ~300x less memory than loading the
    wide-format ALL parquet.

    Returns dict[datetime.date, Counter[str, int]] where keys are base
    symbols (e.g. "BTC") and values are frequency counts (or 1 for
    snapshot mode).
    """
    import sys as _sys
    from collections import Counter
    _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pipeline.db.connection import get_conn

    conn = get_conn()
    cur = conn.cursor()

    eff_lookback = _resolve_sort_lookback()
    window_start_hour = DEPLOYMENT_START_HOUR - eff_lookback
    anchor_hour = (DEPLOYMENT_START_HOUR - INDEX_LOOKBACK) % 24

    # Build mcap WHERE clauses dynamically
    mcap_join = ""
    mcap_where = ""
    mcap_params: list = []
    if min_mcap > 0 or max_mcap > 0:
        mcap_join = """
            LEFT JOIN market.market_cap_daily mcd
              ON mcd.base = s.base AND mcd.date = l.timestamp_utc::date"""
        conditions = []
        if min_mcap > 0 and not drop_unverified:
            conditions.append("(mcd.market_cap_usd >= %s OR mcd.market_cap_usd IS NULL)")
            mcap_params.append(min_mcap)
        elif min_mcap > 0 and drop_unverified:
            conditions.append("mcd.market_cap_usd IS NOT NULL AND mcd.market_cap_usd >= %s")
            mcap_params.append(min_mcap)
        if max_mcap > 0:
            conditions.append("(mcd.market_cap_usd <= %s OR mcd.market_cap_usd IS NULL)")
            mcap_params.append(max_mcap)
        mcap_where = "AND " + " AND ".join(conditions)
    elif drop_unverified:
        mcap_join = """
            JOIN market.market_cap_daily mcd
              ON mcd.base = s.base AND mcd.date = l.timestamp_utc::date"""
        mcap_where = "AND mcd.market_cap_usd IS NOT NULL"

    if mode == "snapshot":
        # Rewrite: instead of EXTRACT(HOUR)=N AND EXTRACT(MINUTE)=0 (which
        # forces a seq-scan over every chunk since expression filters can't
        # be pushed to the PK index), pass the explicit list of target UTC
        # timestamps plus an explicit range bound. The bounds let the
        # hypertable exclude chunks outside the data window; the ANY() list
        # then seeks within each remaining chunk via the PK index.
        # Verified on prod with the full 1.18B-row table:
        #   EXTRACT-based query:  ~278s per call  (4m38s)
        #   ANY + bounds rewrite:   ~0.1s per call (2800× faster)
        snapshot_ts = _snapshot_timestamps_at_hour(DEPLOYMENT_START_HOUR)
        ts_min, ts_max = snapshot_ts[0], snapshot_ts[-1]
        cur.execute(f"""
            SELECT
                l.timestamp_utc::date AS day,
                s.binance_id
            FROM market.leaderboards l
            JOIN market.symbols s ON s.symbol_id = l.symbol_id
            {mcap_join}
            WHERE l.metric = %s
              AND l.anchor_hour = %s
              AND l.variant = 'close'
              AND l.rank <= %s
              AND l.timestamp_utc >= %s
              AND l.timestamp_utc <= %s
              AND l.timestamp_utc = ANY(%s)
              {mcap_where}
            GROUP BY l.timestamp_utc::date, s.binance_id
            ORDER BY l.timestamp_utc::date
        """, (metric, anchor_hour, freq_width, ts_min, ts_max, snapshot_ts, *mcap_params))
    else:
        cur.execute(f"""
            SELECT
                l.timestamp_utc::date AS day,
                s.binance_id,
                COUNT(DISTINCT l.timestamp_utc) AS freq
            FROM market.leaderboards l
            JOIN market.symbols s ON s.symbol_id = l.symbol_id
            {mcap_join}
            WHERE l.metric = %s
              AND l.anchor_hour = %s
              AND l.variant = 'close'
              AND l.rank <= %s
              AND EXTRACT(HOUR FROM l.timestamp_utc)::int >= %s
              AND EXTRACT(HOUR FROM l.timestamp_utc)::int < %s
              AND NOT (EXTRACT(HOUR FROM l.timestamp_utc)::int = %s
                       AND EXTRACT(MINUTE FROM l.timestamp_utc)::int = 0)
              AND (EXTRACT(HOUR FROM l.timestamp_utc)::int * 60
                   + EXTRACT(MINUTE FROM l.timestamp_utc)::int) %% %s = 0
              {mcap_where}
            GROUP BY l.timestamp_utc::date, s.binance_id
            ORDER BY l.timestamp_utc::date
        """, (metric, anchor_hour, freq_width, window_start_hour, DEPLOYMENT_START_HOUR,
              window_start_hour, sample_interval, *mcap_params))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Build dict[date, Counter] — normalize symbols the same way the
    # parquet path does so that 1000PEPEUSDT → PEPE, ADAUSD_PERP → ADA, etc.
    result: dict = {}
    for row in rows:
        day = row[0]  # date
        raw_sym = row[1]  # binance_id (e.g. "BTCUSDT", "1000PEPEUSDT")
        base = normalize_symbol(raw_sym) if raw_sym else None
        if base is None:
            continue
        freq = row[2] if len(row) > 2 else 1  # snapshot mode has no freq column
        if day not in result:
            result[day] = Counter()
        result[day][base] += freq

    log.info(f"  DB query returned {len(rows):,} rows → {len(result)} dates")
    return result


# ─────────────────────────────────────────────
# Symmetric on-the-fly ranking (--ranking-metric abs_dollar)
#
# Canonical universe, no volume/BloFin filter. Ranks ALL symbols in
# market.futures_1m at the anchor + snapshot times, taking top-freq_width
# per (day, metric). Used only when --ranking-metric abs_dollar is selected
# and --live-parity is NOT set — the market.leaderboards fast path is
# pct_change-only by construction, so abs_dollar requires reading raw
# values and ranking on-the-fly.
#
# The pct_change branch of this function exists for symmetry and testing
# (identical mathematical output to market.leaderboards pct_change) but is
# NOT used in the default path — canonical pct_change always reads from
# market.leaderboards for performance.
# ─────────────────────────────────────────────


def _compute_per_day_volume_universe(
    leaderboard_index: int,
    blofin_bases: list | None,
    deployment_start_hour: int,
) -> dict:
    """Compute per-day v1-style universe: top-`leaderboard_index` USDT perps by
    SUM(volume * close) over the prior 24h ending at `deployment_start_hour`
    UTC, optionally narrowed to BloFin-listed bases.

    This reproduces the universe construction in daily_signal.py
    (LEADERBOARD_UNIVERSE = 100, BloFin-gated) and in
    `_load_live_parity_frequencies_from_db` (hardcoded LIMIT 100). Exposed as
    a shared helper so the Simulator path (apply_blofin_filter=True +
    leaderboard_index) and --live-parity share the same logic.

    Returns dict[date, list[symbol_id]]. Skips the first day (no prior 24h
    data) and any day whose universe query returns zero symbols.
    """
    import datetime as _dt
    import sys as _sys

    _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pipeline.db.connection import get_conn

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MIN(timestamp_utc)::date, MAX(timestamp_utc)::date FROM market.futures_1m")
    dmin, dmax = cur.fetchone()
    if dmin is None:
        raise RuntimeError("market.futures_1m is empty")
    first_processable = dmin + _dt.timedelta(days=1)

    bases_arr = sorted(blofin_bases) if blofin_bases else None
    result: dict = {}
    d = first_processable
    while d <= dmax:
        deployment_start = _dt.datetime.combine(d, _dt.time(deployment_start_hour, 0, 0),
                                                tzinfo=_dt.timezone.utc)
        prior_24h_start = deployment_start - _dt.timedelta(hours=24)
        cur.execute("""
            SELECT sym.symbol_id
            FROM market.futures_1m f
            JOIN market.symbols sym USING (symbol_id)
            WHERE f.timestamp_utc > %s AND f.timestamp_utc <= %s
              AND sym.binance_id LIKE '%%USDT'
              AND sym.binance_id NOT LIKE '%%\\_%%' ESCAPE '\\'
              AND (%s::text[] IS NULL OR sym.base = ANY(%s))
            GROUP BY sym.symbol_id
            HAVING SUM(f.volume * f.close) > 0
            ORDER BY SUM(f.volume * f.close) DESC
            LIMIT %s
        """, (prior_24h_start, deployment_start, bases_arr, bases_arr, leaderboard_index))
        sids = [r[0] for r in cur.fetchall()]
        if sids:
            result[d] = sids
        d += _dt.timedelta(days=1)

    cur.close()
    conn.close()
    return result


def _load_canonical_futures_1m_frequencies(
    metric: str,
    freq_width: int,
    sample_interval: int,
    mode: str,
    ranking_metric: str,
    apply_blofin_filter: bool = False,
    universe_by_day: dict | None = None,
) -> dict:
    """Compute per-day top-freq_width frequencies from raw market.futures_1m
    values. Canonical universe by default; optionally narrowed to BloFin
    instrument list and/or a per-day top-N-by-volume universe.

    metric         : 'close' (price) or 'open_interest'
    freq_width     : top-N per bar to award +1 frequency
    sample_interval: minutes between bars (frequency mode only)
    mode           : 'snapshot' | 'frequency'
    ranking_metric : 'log_return' | 'pct_change' | 'abs_dollar'
                     log_return: LN(now/anchor)  — PRICE ONLY (matches v1)
                     pct_change: (now/anchor) - 1  — canonical default
                     abs_dollar: (now - anchor)    — v1 OI methodology
    apply_blofin_filter : when True AND `universe_by_day` is None, restrict
                     the universe to symbols whose base is in today's BloFin
                     USDT-swap list (static gate, no volume cap).
    universe_by_day : optional dict[date, list[symbol_id]]. When provided,
                     restricts each day's ranking to that day's symbol_id
                     list — supersedes `apply_blofin_filter` (since the
                     per-day universe is already narrowed however the caller
                     constructed it, typically via
                     _compute_per_day_volume_universe which bundles
                     top-N-by-volume + BloFin gate).

    Returns dict[date, Counter[base, int]]. Keys are normalize_symbol()
    bases so the output shape matches _load_frequency_from_db.
    """
    if metric not in ("close", "open_interest", "volume"):
        raise ValueError(f"metric must be 'close', 'open_interest', or 'volume', got {metric!r}")
    if mode not in ("snapshot", "frequency"):
        raise ValueError(f"mode must be 'snapshot' or 'frequency', got {mode!r}")
    if ranking_metric not in ("log_return", "pct_change", "abs_dollar"):
        raise ValueError(
            f"ranking_metric must be 'log_return', 'pct_change', or 'abs_dollar', "
            f"got {ranking_metric!r}"
        )
    if ranking_metric == "log_return" and metric in ("open_interest", "volume"):
        # log_return OI == (oi/anchor_oi), which isn't a meaningful quantity.
        # v1 uses log_return for PRICE only and abs_dollar for OI.
        # Same logic for volume: log_return on volume isn't a meaningful
        # cross-sectional ranking (volume is a flow, not a stock).
        raise ValueError(
            f"ranking_metric='log_return' is valid for price (metric='close') only; "
            f"use 'pct_change' or 'abs_dollar' for metric={metric!r}."
        )

    import sys as _sys
    import datetime as _dt
    from collections import Counter as _Counter

    _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pipeline.db.connection import get_conn

    eff_lookback = _resolve_sort_lookback()
    anchor_hour = (DEPLOYMENT_START_HOUR - INDEX_LOOKBACK) % 24
    # NOTE: the snapshot is evaluated at DEPLOYMENT_START_HOUR; the electoral
    # window for frequency mode runs from (DEPLOYMENT_START_HOUR - eff_lookback)
    # to DEPLOYMENT_START_HOUR at sample_interval minute intervals.

    # Ranking formula + universe narrowing are both passed as parameters to
    # the SQL below. Kept as validated-enum strings (ranking_metric) and
    # an optional text[] (blofin_bases) so psycopg2 parameter substitution
    # is safe.

    # Fetch BloFin instrument list if the universe filter is requested AND a
    # pre-computed per-day universe was not supplied by the caller. When the
    # caller passes universe_by_day, narrowing is already baked in — the
    # BloFin fetch + static gate is skipped.
    blofin_bases = None
    if apply_blofin_filter and universe_by_day is None:
        blofin = _fetch_blofin_instruments()
        if blofin:
            blofin_bases = sorted(blofin)
        else:
            log.warning("[canonical] apply_blofin_filter=True but BloFin "
                        "fetch returned empty — proceeding without filter")

    # Build the optional WHERE clause for the BloFin narrowing.
    # Applied in the `scored` CTE (after anchor+snapshot rows are joined to
    # the symbols table), so ranking happens ONLY over symbols that survive
    # the filter. Skipped when universe_by_day supplies a per-day filter.
    blofin_clause = "AND sym.base = ANY(%s)" if blofin_bases else ""

    # `abs_dollar` on OI means delta in DOLLAR-NOTIONAL OI (OI × close).
    # See comment in the frequency branch below for the full rationale.
    # The snapshot branch mirrors the same semantic for consistency.
    is_oi_dollar = (metric == "open_interest" and ranking_metric == "abs_dollar")

    conn = get_conn()
    cur = conn.cursor()

    result: dict = {}

    if mode == "snapshot":
        # Single-query path using date_trunc('minute', ...) = ANY(minute_list)
        # to bucket sub-second timestamps into their target minute, then
        # DISTINCT ON to pick the latest sub-second row per (symbol, minute).
        # Mirrors the builder's hygiene at pipeline/indexer/build_intraday_
        # leaderboard.py:821-832 which uses `df["minute"] = timestamp_utc.dt
        # .floor("min")` + `groupby(["symbol", "minute"]).last()`.
        #
        # HISTORY (d-bug fix, 2026-04-23): the previous version used
        # `n.timestamp_utc = ANY(exact_HH_00_00_list)` — which requires rows
        # at exactly HH:00:00.000. market.futures_1m stores sub-second
        # timestamps (HH:00:00, HH:00:07, HH:00:09, ...) so only ~2 rows
        # per day matched → 158 of 432 days in the walk-forward silently
        # dropped (Test 3 acceptance test, 2026-04-23). See
        # docs/open_work_list.md § D-bug and § 11.4 of the spec for full
        # detail.
        snapshot_ts = _snapshot_timestamps_at_hour(DEPLOYMENT_START_HOUR)
        anchor_minutes = [ts - _dt.timedelta(hours=INDEX_LOOKBACK) for ts in snapshot_ts]
        # Range bounds prune hypertable chunks; `date_trunc` + ANY then
        # seeks within remaining chunks via PK index.
        ts_min = min(anchor_minutes[0], snapshot_ts[0])
        ts_max = snapshot_ts[-1] + _dt.timedelta(minutes=1)

        # Per-day universe JOIN (v1-style narrowing). Parallel arrays of
        # (day, symbol_id) are unnested into a CTE; scored CTE joins on both
        # so ranking happens only within that day's universe. Supersedes
        # blofin_clause when supplied.
        universe_clause = ""
        if universe_by_day:
            universe_clause = (
                "JOIN per_day_universe u "
                "ON u.day = s.minute::date AND u.symbol_id = s.symbol_id"
            )
            universe_cte = (
                "per_day_universe AS ("
                "  SELECT day, symbol_id "
                "  FROM ROWS FROM ("
                "    unnest(%s::date[]), unnest(%s::int[])"
                "  ) AS t(day, symbol_id)"
                "),"
            )
        else:
            universe_cte = ""

        # is_oi_dollar extras (snapshot branch mirrors the frequency branch).
        snap_anchor_select = ", a.close AS anchor_close_val" if is_oi_dollar else ""
        snap_anchor_where  = "AND a.close IS NOT NULL AND a.close > 0" if is_oi_dollar else ""
        snap_snap_select   = ", n.close AS snapshot_close_val" if is_oi_dollar else ""
        snap_snap_where    = "AND n.close IS NOT NULL AND n.close > 0" if is_oi_dollar else ""
        if is_oi_dollar:
            snap_score_case = (
                "(s.snapshot_val * s.snapshot_close_val - "
                "a.anchor_val * a.anchor_close_val)"
            )
        else:
            snap_score_case = (
                "CASE "
                "WHEN %s = 'abs_dollar' THEN (s.snapshot_val - a.anchor_val) "
                "WHEN %s = 'log_return' THEN "
                "LN(NULLIF(s.snapshot_val, 0) / NULLIF(a.anchor_val, 0)) "
                "ELSE ((s.snapshot_val / NULLIF(a.anchor_val, 0)) - 1) "
                "END"
            )

        sql = f"""
            WITH {universe_cte}
            anchor_rows AS (
                SELECT DISTINCT ON (a.symbol_id, date_trunc('minute', a.timestamp_utc))
                    a.symbol_id,
                    date_trunc('minute', a.timestamp_utc) AS minute,
                    a.{metric} AS anchor_val
                    {snap_anchor_select}
                FROM market.futures_1m a
                WHERE a.timestamp_utc >= %s
                  AND a.timestamp_utc <  %s
                  AND date_trunc('minute', a.timestamp_utc) = ANY(%s)
                  AND a.{metric} IS NOT NULL AND a.{metric} > 0
                  {snap_anchor_where}
                ORDER BY a.symbol_id,
                         date_trunc('minute', a.timestamp_utc),
                         a.timestamp_utc DESC
            ),
            snapshot_rows AS (
                SELECT DISTINCT ON (n.symbol_id, date_trunc('minute', n.timestamp_utc))
                    n.symbol_id,
                    date_trunc('minute', n.timestamp_utc) AS minute,
                    n.{metric} AS snapshot_val
                    {snap_snap_select}
                FROM market.futures_1m n
                WHERE n.timestamp_utc >= %s
                  AND n.timestamp_utc <  %s
                  AND date_trunc('minute', n.timestamp_utc) = ANY(%s)
                  AND n.{metric} IS NOT NULL
                  {snap_snap_where}
                ORDER BY n.symbol_id,
                         date_trunc('minute', n.timestamp_utc),
                         n.timestamp_utc DESC
            ),
            scored AS (
                SELECT s.minute::date AS day,
                       sym.binance_id,
                       ({snap_score_case}) AS score
                FROM snapshot_rows s
                JOIN anchor_rows a
                  ON a.symbol_id = s.symbol_id
                 AND a.minute = s.minute - (%s * interval '1 hour')
                JOIN market.symbols sym ON sym.symbol_id = s.symbol_id
                {universe_clause}
                WHERE 1=1
                  {blofin_clause}
            ),
            ranked AS (
                SELECT day, binance_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY day ORDER BY score DESC NULLS LAST
                       ) AS rnk
                FROM scored
            )
            SELECT day, binance_id
            FROM ranked
            WHERE rnk <= %s
        """
        params = []
        if universe_by_day:
            # Flatten dict[date, list[symbol_id]] into two parallel arrays.
            day_arr: list = []
            sid_arr: list = []
            for day, sids in universe_by_day.items():
                day_arr.extend([day] * len(sids))
                sid_arr.extend(sids)
            params.extend([day_arr, sid_arr])
        params.extend([
            ts_min, ts_max, anchor_minutes,          # anchor_rows bounds + minute list
            ts_min, ts_max, snapshot_ts,             # snapshot_rows bounds + minute list
        ])
        if not is_oi_dollar:
            # scored CASE has two %s placeholders (abs_dollar, log_return).
            # Skipped when is_oi_dollar since that branch uses a literal
            # expression rather than a CASE.
            params.extend([ranking_metric, ranking_metric])
        params.append(INDEX_LOOKBACK)                # lookback interval on join
        if blofin_bases:
            params.append(blofin_bases)              # BloFin filter bound param
        params.append(freq_width)                    # rnk cutoff (last)
        cur.execute(sql, params)
        for day, raw_sym in cur.fetchall():
            base = normalize_symbol(raw_sym)
            if base is None:
                continue
            if day not in result:
                result[day] = _Counter()
            result[day][base] += 1
    else:  # frequency
        # Per-day loop (simpler + matches _load_live_parity_frequencies_from_db
        # pattern). Each day: snapshot all bars in the window for the ranked
        # subset, count top-freq_width per bar.
        window_start_hour = DEPLOYMENT_START_HOUR - eff_lookback
        # BloFin filter clause applied inside the per-bar ranking CTE when
        # apply_blofin_filter=True — narrows universe before RANK() assignment.
        bf_freq_clause = "AND sym.base = ANY(%s)" if blofin_bases else ""

        cur.execute("SELECT MIN(timestamp_utc)::date, MAX(timestamp_utc)::date FROM market.futures_1m")
        dmin, dmax = cur.fetchone()
        if dmin is None:
            raise RuntimeError("market.futures_1m is empty")
        first_processable = dmin + _dt.timedelta(days=1)

        log.info(f"[canonical {ranking_metric} freq] Processing dates "
                 f"{first_processable} → {dmax} "
                 f"({(dmax - first_processable).days + 1} days)"
                 f"{'  [BloFin-filtered]' if blofin_bases else ''}"
                 f"{'  [per-day universe narrowing]' if universe_by_day else ''}")

        # Per-day universe narrowing clause (supersedes static BloFin gate).
        # Applied in the `bars` CTE so the hypertable scan is narrowed before
        # any ranking work, rather than filtering post-scan inside `scored`.
        uni_bars_clause = "AND f.symbol_id = ANY(%s)" if universe_by_day else ""

        # `abs_dollar` on OI means delta in DOLLAR-NOTIONAL OI (OI × close),
        # matching v1's `oi_usd = oi * close` in
        # _load_live_parity_frequencies_from_db. Raw-OI-contract delta is
        # barely interpretable cross-symbol (a 1-contract delta on BTC is
        # not comparable to a 1-contract delta on SHIB). When is_oi_dollar,
        # we fetch `close` alongside `open_interest` in both anchor_vals
        # and bars and swap in a dollar-notional scoring expression.
        # (`is_oi_dollar` is hoisted above the mode branch so snapshot uses
        # the same semantic.)

        # Anchor walk-forward: v1 scans the first 5 sample-interval bars per
        # symbol and picks the first non-zero value as anchor. Mirrored in
        # SQL via DISTINCT ON (symbol_id) over a windowed WHERE clause. Keeps
        # sparse-data symbols in the ranking pool rather than silently
        # dropping them, matching v1 behavior byte-for-byte.
        anchor_walk_bars = 5

        # Score expression (per-bar). Bundles ranking_metric choice +
        # dollar-notional OI. Referenced in the `scored` CTE.
        if is_oi_dollar:
            score_expr = "(b.now_val * b.now_close - b.anchor_val * b.anchor_close)"
        elif ranking_metric == "abs_dollar":
            score_expr = "(b.now_val - b.anchor_val)"
        elif ranking_metric == "log_return":
            score_expr = "LN(NULLIF(b.now_val, 0) / NULLIF(b.anchor_val, 0))"
        else:  # pct_change
            score_expr = "((b.now_val / NULLIF(b.anchor_val, 0)) - 1)"

        # Extra column clauses for is_oi_dollar: anchor_vals/bars fetch close
        # too, and both the anchor filter and bars filter require close > 0.
        cand_extra_select   = ", close AS anchor_close" if is_oi_dollar else ""
        anchor_extra_select = ", anchor_close"         if is_oi_dollar else ""
        anchor_extra_where  = "AND close IS NOT NULL AND close > 0" if is_oi_dollar else ""
        bars_extra_select   = ", f.close AS now_close" if is_oi_dollar else ""
        bars_extra_where    = "AND f.close IS NOT NULL AND f.close > 0" if is_oi_dollar else ""
        bars_propagate      = ", a.anchor_close"       if is_oi_dollar else ""

        d = first_processable
        while d <= dmax:
            # Skip days whose universe is empty (no prior-24h data) when
            # universe_by_day narrowing is active — matches the behavior of
            # _load_live_parity_frequencies_from_db.
            if universe_by_day is not None and d not in universe_by_day:
                d += _dt.timedelta(days=1)
                continue

            anchor_ts = _dt.datetime.combine(
                d, _dt.time(anchor_hour, 0, 0), tzinfo=_dt.timezone.utc,
            )
            anchor_walk_end = anchor_ts + _dt.timedelta(
                minutes=(anchor_walk_bars - 1) * sample_interval
            )
            window_end_ts = _dt.datetime.combine(
                d, _dt.time(DEPLOYMENT_START_HOUR, 0, 0), tzinfo=_dt.timezone.utc,
            )
            window_start_ts = _dt.datetime.combine(
                d, _dt.time(window_start_hour % 24, 0, 0), tzinfo=_dt.timezone.utc,
            )
            freq_sql = f"""
                WITH anchor_candidates AS (
                    SELECT symbol_id, timestamp_utc,
                           {metric} AS v
                           {cand_extra_select}
                    FROM market.futures_1m
                    WHERE timestamp_utc >= %s
                      AND timestamp_utc <= %s
                      AND MOD(EXTRACT(MINUTE FROM timestamp_utc)::int, %s) = 0
                      AND {metric} IS NOT NULL AND {metric} > 0
                      {anchor_extra_where}
                ),
                anchor_vals AS (
                    SELECT DISTINCT ON (symbol_id)
                           symbol_id, v AS anchor_val
                           {anchor_extra_select}
                    FROM anchor_candidates
                    ORDER BY symbol_id, timestamp_utc ASC
                ),
                bars AS (
                    SELECT f.timestamp_utc, f.symbol_id, f.{metric} AS now_val,
                           a.anchor_val
                           {bars_extra_select}
                           {bars_propagate}
                    FROM market.futures_1m f
                    JOIN anchor_vals a USING (symbol_id)
                    WHERE f.timestamp_utc > %s
                      AND f.timestamp_utc <= %s
                      AND MOD(EXTRACT(MINUTE FROM f.timestamp_utc)::int, %s) = 0
                      AND f.{metric} IS NOT NULL
                      {bars_extra_where}
                      {uni_bars_clause}
                ),
                scored AS (
                    SELECT b.timestamp_utc, b.symbol_id,
                           {score_expr} AS score
                    FROM bars b
                    JOIN market.symbols sym ON sym.symbol_id = b.symbol_id
                    WHERE 1=1
                      {bf_freq_clause}
                ),
                ranked AS (
                    SELECT timestamp_utc, symbol_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY timestamp_utc
                               ORDER BY score DESC NULLS LAST
                           ) AS rnk
                    FROM scored
                )
                SELECT sym.binance_id, COUNT(DISTINCT r.timestamp_utc) AS freq
                FROM ranked r
                JOIN market.symbols sym USING (symbol_id)
                WHERE r.rnk <= %s
                GROUP BY sym.binance_id
            """
            # Param order:
            #   1. anchor_candidates: anchor_ts (>=), anchor_walk_end (<=), sample_interval
            #   2. bars:              window_start_ts (>), window_end_ts (<=), sample_interval
            #   3. uni_bars_clause:   universe_by_day[d] (if present)
            #   4. bf_freq_clause:    blofin_bases (if present)
            #   5. final rnk cutoff:  freq_width
            freq_params = [anchor_ts, anchor_walk_end, sample_interval,
                           window_start_ts, window_end_ts, sample_interval]
            if universe_by_day:
                freq_params.append(universe_by_day[d])
            if blofin_bases:
                freq_params.append(blofin_bases)
            freq_params.append(freq_width)
            cur.execute(freq_sql, freq_params)
            counter: _Counter = _Counter()
            for raw_sym, freq in cur.fetchall():
                base = normalize_symbol(raw_sym)
                if base is None:
                    continue
                counter[base] += int(freq)
            if counter:
                result[d] = counter
            d += _dt.timedelta(days=1)

    cur.close()
    conn.close()
    log.info(f"[canonical {ranking_metric} {mode}] Complete: "
             f"{len(result)} dates returned for metric={metric}"
             f"{'  [BloFin-filtered]' if blofin_bases else ''}")
    return result


# ─────────────────────────────────────────────
# LIVE-PARITY mode (default-on, --live-parity / --no-live-parity)
#
# Reproduces daily_signal_v2.py basket selection on historical DB data so the
# audit's per-day basket matches what the live trader would have picked. v1
# methodology (top-100-by-volume + log_return price + abs_dollar OI + freq
# aggregation across the day) is retired — v2 is the canonical signal as of
# the 2026-04-23 cutover.
#
# v2 methodology applied here:
#   - Universe: ALL USDT perps in market.futures_1m (no quoteVolume cutoff).
#   - Anchor: 00:00 UTC bar (walk-forward up to 5m if exact bar missing).
#   - Snapshot: deployment_start_hour bar (06:00 UTC default).
#   - Price ranking: pct_change = (snap_close / anchor_close) - 1.
#   - OI ranking:    pct_change = (snap_oi / anchor_oi) - 1.
#   - Top-20 each by pct_change DESC, normalize_symbol.
#   - BloFin filter applied AFTER ranking.
#   - Counter[base] = 1 per chosen symbol so the downstream freq_cutoff
#     filter keeps the snapshot-mode picks intact.
#
# Approximations to live (immaterial drift):
#   Gap A: BloFin instrument list is today's snapshot, used as static
#          historical filter. <2-3 symbol/day lookback error.
#   Gap B: live cron pulls Binance REST at 05:58 (close of 05:55 5m bar);
#          this loader uses the 06:00 1m bar's close. ~1-5min off, basket
#          stable across that window in practice.
# ─────────────────────────────────────────────

import functools as _functools
import urllib.request as _urllib_request
import json as _json


@_functools.lru_cache(maxsize=1)
def _fetch_blofin_instruments() -> set:
    """Fetch current BloFin USDT perp universe for the static historical filter.
    Cached per-process. Returns empty set on failure (falls through to no filter —
    universe will be slightly wider than live's).

    Uses a User-Agent header — Cloudflare in front of openapi.blofin.com blocks
    urllib's default 'Python-urllib/3.x' as a bot signature, returning 403.
    """
    url = "https://openapi.blofin.com/api/v1/market/instruments?instType=SWAP"
    try:
        req = _urllib_request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; benji-audit/1.0)"},
        )
        with _urllib_request.urlopen(req, timeout=15) as r:
            data = _json.load(r)
        syms = {
            inst["instId"].replace("-USDT", "").upper()
            for inst in (data.get("data") or [])
            if inst.get("instId", "").endswith("-USDT")
        }
        log.info(f"[LIVE-PARITY] BloFin universe: {len(syms)} USDT-swap instruments (static snapshot)")
        return syms
    except Exception as e:
        log.warning(f"[LIVE-PARITY] BloFin fetch failed ({e}); proceeding without BloFin filter")
        return set()


def _load_live_parity_frequencies_from_db(
    freq_width: int = 20,
    sample_interval: int = 5,  # unused — kept for signature compat
    deployment_start_hour: int = 6,
) -> tuple:
    """Reproduce daily_signal_v2.py basket selection on historical DB data.

    v2 methodology (matches daily_signal_v2.compute_canonical_basket):
      - Universe: ALL USDT perps in market.futures_1m (no top-100 cutoff).
      - Anchor:    00:00 UTC bar (walk-forward to first available within 5m).
      - Snapshot:  deployment_start_hour UTC bar (default 06:00).
      - Price ranking: pct_change = (snap_close / anchor_close) - 1.
      - OI ranking:    pct_change = (snap_oi    / anchor_oi)    - 1.
      - Top-`freq_width` each by pct_change DESC, normalize_symbol.
      - BloFin filter applied AFTER ranking (drop top-N picks not on BloFin).
      - Single snapshot per day; Counter[base] = 1 for each chosen symbol so
        the downstream freq_cutoff filter (default = freq_width = 20) keeps
        them all in `_top_n`. Rank within the chosen set is irrelevant for
        the intersection; ties on count don't matter.

    Returns:
        (price_freq_by_date, oi_freq_by_date) where each is
        dict[date, Counter[base, int]].
    """
    import sys as _sys
    import datetime as _dt
    from collections import Counter as _Counter

    _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pipeline.db.connection import get_conn

    blofin = _fetch_blofin_instruments()
    blofin_set = set(blofin) if blofin else None

    conn = get_conn()
    cur = conn.cursor()

    # Discover date range from futures_1m. Both anchor (00:00) and snapshot
    # (deployment_start_hour) bars must be present for a date to be scored.
    cur.execute("SELECT MIN(timestamp_utc)::date, MAX(timestamp_utc)::date FROM market.futures_1m")
    dmin, dmax = cur.fetchone()
    if dmin is None:
        raise RuntimeError("market.futures_1m is empty")
    log.info(f"[LIVE-PARITY v2] Processing dates {dmin} → {dmax} "
             f"({(dmax - dmin).days + 1} days)")

    price_freq_by_date: dict = {}
    oi_freq_by_date: dict = {}

    d = dmin
    while d <= dmax:
        anchor_ts = _dt.datetime.combine(d, _dt.time(0, 0, 0), tzinfo=_dt.timezone.utc)
        snap_ts   = _dt.datetime.combine(d, _dt.time(deployment_start_hour, 0, 0),
                                         tzinfo=_dt.timezone.utc)

        # Single SQL pass: walk-forward 5-minute anchor + snapshot per symbol,
        # filter to USDT linear perps (drop BTC_*, ETH_*, etc. extension
        # quotes via NOT LIKE '%\_%'), require both bars to have non-null/
        # non-zero close and open_interest. Server-side INNER JOIN drops any
        # symbol missing either side.
        cur.execute("""
            WITH anchor_bars AS (
              SELECT DISTINCT ON (f.symbol_id)
                     f.symbol_id, f.close AS a_close, f.open_interest AS a_oi
                FROM market.futures_1m f
                JOIN market.symbols sym USING (symbol_id)
               WHERE sym.binance_id LIKE %s
                 AND sym.binance_id NOT LIKE %s ESCAPE '\\'
                 AND f.timestamp_utc >= %s
                 AND f.timestamp_utc <  %s
                 AND f.close IS NOT NULL AND f.close > 0
                 AND f.open_interest IS NOT NULL AND f.open_interest > 0
               ORDER BY f.symbol_id, f.timestamp_utc
            ),
            snap_bars AS (
              SELECT DISTINCT ON (f.symbol_id)
                     f.symbol_id, f.close AS s_close, f.open_interest AS s_oi
                FROM market.futures_1m f
                JOIN market.symbols sym USING (symbol_id)
               WHERE sym.binance_id LIKE %s
                 AND sym.binance_id NOT LIKE %s ESCAPE '\\'
                 AND f.timestamp_utc >= %s
                 AND f.timestamp_utc <  %s
                 AND f.close IS NOT NULL AND f.close > 0
                 AND f.open_interest IS NOT NULL AND f.open_interest > 0
               ORDER BY f.symbol_id, f.timestamp_utc
            )
            SELECT sym.binance_id,
                   a.a_close, a.a_oi,
                   s.s_close, s.s_oi
              FROM anchor_bars a
              JOIN snap_bars   s ON s.symbol_id = a.symbol_id
              JOIN market.symbols sym ON sym.symbol_id = a.symbol_id
        """, (
            "%USDT", r"%\_%",
            anchor_ts, anchor_ts + _dt.timedelta(minutes=5),
            "%USDT", r"%\_%",
            snap_ts,   snap_ts   + _dt.timedelta(minutes=5),
        ))
        rows = cur.fetchall()

        if not rows:
            d += _dt.timedelta(days=1)
            continue

        price_ranks = []
        oi_ranks    = []
        for binance_id, a_close, a_oi, s_close, s_oi in rows:
            base_norm = normalize_symbol(binance_id)
            if not base_norm:
                continue
            price_pct = (float(s_close) / float(a_close)) - 1.0
            oi_pct    = (float(s_oi)    / float(a_oi))    - 1.0
            price_ranks.append((base_norm, price_pct))
            oi_ranks.append((base_norm, oi_pct))

        # Top-N by pct_change DESC.
        price_top = sorted(price_ranks, key=lambda x: x[1], reverse=True)[:freq_width]
        oi_top    = sorted(oi_ranks,    key=lambda x: x[1], reverse=True)[:freq_width]

        # BloFin filter applied AFTER ranking — matches v2's
        # `basket = [s for s in basket if s in blofin]` ordering. Skipped
        # silently when the BloFin fetch failed (universe stays slightly
        # wider than live).
        if blofin_set is not None:
            price_top = [(b, p) for b, p in price_top if b in blofin_set]
            oi_top    = [(b, p) for b, p in oi_top    if b in blofin_set]

        price_freq = _Counter()
        oi_freq    = _Counter()
        for base, _ in price_top:
            price_freq[base] = 1
        for base, _ in oi_top:
            oi_freq[base] = 1

        price_freq_by_date[d] = price_freq
        oi_freq_by_date[d]    = oi_freq

        if (d - dmin).days % 30 == 0:
            log.info(f"[LIVE-PARITY v2]   processed through {d} ({len(price_freq_by_date)} days done)")
        d += _dt.timedelta(days=1)

    cur.close()
    conn.close()
    log.info(f"[LIVE-PARITY v2] Complete: {len(price_freq_by_date)} dates with price snapshot, "
             f"{len(oi_freq_by_date)} dates with OI snapshot")
    return price_freq_by_date, oi_freq_by_date


def auth_gspread():
    """
    Authenticate with Google Sheets using OAuth token.

    Token is cached at GSHEETS_TOKEN_PATH and refreshed automatically.
    If login is triggered every run:
      1. Confirm credentials.json is type "Desktop app" in Google Cloud Console
         (Web app credentials don't issue refresh tokens → re-auth every time)
      2. Delete token.json and re-run once to get a fresh token with refresh_token
    """
    creds = None
    if os.path.exists(GSHEETS_TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(GSHEETS_TOKEN_PATH, GSHEETS_SCOPES)
            # If the token was saved with different scopes, scrap it and re-auth.
            # This is the most common cause of "login every run" — scopes changed
            # between runs (e.g. during debugging) so the saved token is rejected.
            if creds.scopes and not set(GSHEETS_SCOPES).issubset(set(creds.scopes)):
                log.warning(
                    f"token.json scopes {creds.scopes} don't cover required "
                    f"{GSHEETS_SCOPES} — discarding and re-authenticating"
                )
                creds = None
        except Exception as e:
            log.warning(f"Could not load token.json: {e} — will re-authenticate")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                log.info("Google OAuth token refreshed successfully")
            except Exception as e:
                log.warning(f"Token refresh failed ({e}) — re-authenticating")
                creds = None

        if not creds or not creds.valid:
            log.info("Opening browser for Google OAuth login...")
            flow = InstalledAppFlow.from_client_secrets_file(
                GSHEETS_CREDENTIALS_PATH, GSHEETS_SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(GSHEETS_TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
        log.info(f"Token saved → {GSHEETS_TOKEN_PATH}")

    return gspread.authorize(creds)


def export_to_gsheets(overlap_df: pd.DataFrame, deploys_path: Path,run_label: str, confluence_path: Path = None):
    """
    Write two tabs to the configured Google Sheet (both named with timestamp):

      Confluence_<ts>
        timestamp_utc
        | price_R1 … price_RN      (top-N price symbols, ranked by freq)
        | oi_R1    … oi_RN         (top-N OI symbols, ranked by freq)
        | overlap_R1 … overlap_Rk  (intersection, sorted by selected rank)
        | deploy_count             (symbols sent to audit after normalisation)

      Deploys_<ts>
        Full deploys CSV (timestamp_utc, R1, R2, …)

    Each run appends new tabs — existing tabs are never overwritten.
    """
    log.info("Exporting to Google Sheets...")
    try:
        gc = auth_gspread()
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
    except Exception as e:
        log.error(f"Google Sheets auth failed: {e}")
        return

    ts = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")

    def create_tab(name, n_rows=0, n_cols=0):
        rows = max(n_rows + 30, 30)
        cols = min(max(n_cols, 1), 100)
        ws = sh.add_worksheet(title=name, rows=str(rows), cols=str(cols))
        return ws

    def write_tab(ws, rows):
        chunk_size = 500
        for i in range(0, len(rows), chunk_size):
            ws.update(values=rows[i:i+chunk_size], range_name=f"A{i+1}")

    # ── Build deploy_count lookup: date → count of symbols sent to audit ──
    deploy_count = {}
    if deploys_path.exists():
        dep_df    = pd.read_csv(deploys_path)
        rank_cols = [c for c in dep_df.columns if c != "timestamp_utc"]
        sym_counts = dep_df[rank_cols].notna().sum(axis=1)
        for idx, drow in dep_df.iterrows():
            try:
                dkey = pd.to_datetime(drow["timestamp_utc"]).date()
            except Exception:
                continue
            deploy_count[dkey] = int(sym_counts.iloc[idx])

    # ── Determine max widths for headers ──
    max_price   = max((len(r["price_symbols"])   for _, r in overlap_df.iterrows()), default=0)
    max_oi      = max((len(r["oi_symbols"])      for _, r in overlap_df.iterrows()), default=0)
    max_overlap = max((len(r["overlap_symbols"]) for _, r in overlap_df.iterrows()), default=0)

    price_hdrs   = [f"price_R{i+1}"   for i in range(max_price)]
    oi_hdrs      = [f"oi_R{i+1}"      for i in range(max_oi)]
    overlap_hdrs = [f"overlap_R{i+1}" for i in range(max_overlap)]
    header = ["timestamp_utc"] + price_hdrs + oi_hdrs + overlap_hdrs + ["deploy_count"]

    # ── Build rows ──
    conf_rows = [header]
    for _, row in overlap_df.iterrows():
        date_key = row["date"].date()
        p_syms   = list(row["price_symbols"])
        o_syms   = list(row["oi_symbols"])
        ov_syms  = list(row["overlap_symbols"])
        dc       = deploy_count.get(date_key, "")

        data_row = (
            [str(date_key)]
            + p_syms  + [""] * (max_price   - len(p_syms))
            + o_syms  + [""] * (max_oi      - len(o_syms))
            + ov_syms + [""] * (max_overlap - len(ov_syms))
            + [dc]
        )
        conf_rows.append(data_row)

    conf_tab = create_tab(f"Confluence_{ts}", n_rows=len(conf_rows), n_cols=len(header))
    write_tab(conf_tab, conf_rows)
    log.info(f"  → Confluence_{ts} tab written ({len(conf_rows)-1} rows, "
             f"{max_price}p + {max_oi}oi + {max_overlap}overlap cols)")

    # ── Tab 2: Deploys_<timestamp> ──
    dep_df2      = pd.read_csv(deploys_path)
    deploys_tab  = create_tab(f"Deploys_{ts}", n_rows=len(dep_df2)+1, n_cols=len(dep_df2.columns))
    deploys_rows = [dep_df2.columns.tolist()] + dep_df2.fillna("").values.tolist()
    write_tab(deploys_tab, deploys_rows)
    log.info(f"  → Deploys_{ts} tab written ({len(deploys_rows)-1} rows)")

    sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"
    log.info(f"  → Sheet: {sheet_url}")


def print_freq_table(freq: dict, label: str) -> None:
    """Print the full frequency table to terminal, one date per block."""
    dates = sorted(freq.keys())
    print()
    print("=" * 70)
    print(f"FREQUENCY TABLE — {label}  ({len(dates)} dates)")
    print("=" * 70)
    for date in dates:
        ranked = sorted(freq[date].items(), key=lambda x: x[1], reverse=True)
        syms   = "  ".join(f"{sym}({cnt})" for sym, cnt in ranked)
        print(f"  {date}  [{len(ranked)}]  {syms}")
    print("=" * 70)
    print()


def export_freq_table(freq: dict, output_path: Path) -> None:
    """
    Write a frequency table to CSV.
    Rows = dates, columns = symbols ranked by frequency count (R1 = most frequent).
    Each cell contains the symbol name; a separate _count column follows each symbol
    so the raw count is preserved alongside the rank.

    Schema: date | R1 | R1_count | R2 | R2_count | ... | RN | RN_count
    """
    rows = []
    max_width = 0
    for date in sorted(freq.keys()):
        counter = freq[date]
        ranked  = sorted(counter.items(), key=lambda x: x[1], reverse=True)
        max_width = max(max_width, len(ranked))
        row = {"date": str(date)}
        for i, (sym, cnt) in enumerate(ranked):
            row[f"R{i+1}"]       = sym
            row[f"R{i+1}_count"] = cnt
        rows.append(row)

    rank_cols = []
    for i in range(max_width):
        rank_cols += [f"R{i+1}", f"R{i+1}_count"]
    df = pd.DataFrame(rows, columns=["date"] + rank_cols)
    df.to_csv(output_path, index=False)
    log.info(f"Frequency table saved → {output_path.name}  ({len(df)} dates, {max_width} symbols max)")


def run(min_mcap: float, freq_width: int, marketcap_dir: Path,
        mode: str = "frequency", freq_cutoff: int = FREQ_TOP_N, sort_by: str = OVERLAP_SORT_BY,
        sample_interval: int = SAMPLE_INTERVAL_MINUTES, leaderboard_index: int = LEADERBOARD_INDEX,
        audit: bool = False, audit_script: Path = None, audit_args: list = None,
        confluence_path: Path = None, drop_unverified: bool = False, max_mcap: float = 0.0,
        force: bool = False, source: str = "parquet",
        live_parity: bool = False,
        price_ranking_metric: str = "pct_change",
        oi_ranking_metric: str = "pct_change",
        apply_blofin_filter: bool = False,
        overlap_dimensions: str = "price_oi",
):
    mcap_label      = f"{int(min_mcap / 1_000_000)}M"
    marketcap_path  = marketcap_dir / "marketcap_daily.parquet"
    idx_label       = f"top{leaderboard_index}"
    price_input, oi_input = _leaderboard_paths(leaderboard_index)

    # ── Validate sort_lookback ≤ index_lookback ───────────────────────────────
    # sort_lookback scans the leaderboard going back from deployment_start_hour.
    # index_lookback defines the anchor from which % changes are measured.
    # Scanning bars before the anchor produces % change values that are measured
    # from a baseline that hasn't occurred yet — meaningless by construction.
    _eff_sl = _resolve_sort_lookback()
    if _eff_sl > INDEX_LOOKBACK:
        raise ValueError(
            f"sort_lookback ({_eff_sl}h effective) > index_lookback ({INDEX_LOOKBACK}h). "
            f"The electoral window reaches before the leaderboard anchor point, "
            f"where % change values are not yet valid. "
            f"Set sort_lookback ≤ index_lookback. "
            f"Current: start={DEPLOYMENT_START_HOUR:02d}:00, "
            f"index_lookback={INDEX_LOOKBACK}h → anchor={_anchor_hhmm()[:2]}:00, "
            f"sort_lookback={SORT_LOOKBACK} → effective={_eff_sl}h."
        )
    for _lb_path, _metric in [(price_input, "price"),
                              (oi_input,    "open_interest")]:
        if not _lb_path.exists():
            log.info(f"Leaderboard not found: {_lb_path.name}")
            log.info(f"  Building with anchor={_anchor_hhmm()} "
                     f"(start={DEPLOYMENT_START_HOUR:02d}:00, "
                     f"index_lookback={INDEX_LOOKBACK}h)...")
            import subprocess as _sp
            _builder = Path(LEADERBOARD_BUILDER)
            if not _builder.exists():
                raise FileNotFoundError(
                    f"Leaderboard builder not found: {_builder}. "
                    f"Set LEADERBOARD_BUILDER constant or run the builder manually."
                )
            _build_cmd = [
                "python3", str(_builder),
                "--metric",                  _metric,
                "--deployment-start-hour",   str(DEPLOYMENT_START_HOUR),
                "--index-lookback",          str(INDEX_LOOKBACK),
                # Write to the canonical per-metric directory so the master
                # parquet ends up where _leaderboard_paths() looks for it
                # (and where the indexer cron also writes its outputs).
                # Until 2026-04-08 this was str(BASE_DIR) (= /data, ephemeral
                # scratch), which meant the build subprocess wrote to one
                # place and the input lookup checked another, breaking
                # idempotency on every run.
                "--output-dir",              str(leaderboard_dir_for_metric(_metric)),
                "--parquet-path",            LEADERBOARD_PARQUET_PATH,
            ]
            # Auto-build (file missing) does NOT pass --force — that's the
            # whole point of the checkpoint. The user-driven --force flag is
            # propagated only for explicit rebuild requests from the UI.
            if force:
                _build_cmd.append("--force")
            _cmd_str = " ".join(_build_cmd)
            log.info(f"  CMD: {_cmd_str}")
            _sp.run(_build_cmd, check=True)
            if not _lb_path.exists():
                raise FileNotFoundError(
                    f"Builder ran but output not found: {_lb_path}"
                )

    _unver_suffix       = "_dropunverified" if drop_unverified else ""
    _maxcap_suffix      = f"_max{int(max_mcap/1_000_000)}M" if max_mcap > 0 else ""
    _run_suffix         = _unver_suffix + _maxcap_suffix
    _anchor_suffix      = "" if _anchor_hhmm() == "0000" else f"_anchor{_anchor_hhmm()}"
    price_filtered_path = BASE_DIR / f"leaderboard_price_{idx_label}_filtered_{mcap_label}{_anchor_suffix}{_run_suffix}.parquet"
    oi_filtered_path    = BASE_DIR / f"leaderboard_open_interest_{idx_label}_filtered_{mcap_label}{_anchor_suffix}{_run_suffix}.parquet"
    _start_hhmm     = f"{DEPLOYMENT_START_HOUR:02d}00"
    _eff_lookback_run = _resolve_sort_lookback()
    _lookback_hhmm    = f"{(DEPLOYMENT_START_HOUR - _eff_lookback_run):02d}00"
    # Output label: live-parity has a distinct prefix; non-live-parity runs
    # that deviate from (pct_change price, pct_change OI, no BloFin filter)
    # get a suffix describing the deviation so artifacts don't collide with
    # canonical default runs of the same mode.
    _non_canonical_ranking = (
        price_ranking_metric != "pct_change"
        or oi_ranking_metric != "pct_change"
        or apply_blofin_filter
    )
    if _non_canonical_ranking:
        _rm_parts = []
        if price_ranking_metric != "pct_change":
            _rm_parts.append(f"p{price_ranking_metric[:4]}")
        if oi_ranking_metric != "pct_change":
            _rm_parts.append(f"o{oi_ranking_metric[:4]}")
        if apply_blofin_filter:
            _rm_parts.append("bf")
        _rm_suffix = "_" + "".join(_rm_parts)
    else:
        _rm_suffix = ""
    if live_parity:
        mode_label = f"live_parity_{_start_hhmm}"
    else:
        mode_label = (
            f"snapshot_{_start_hhmm}{_rm_suffix}" if mode == "snapshot"
            else f"freq_{_lookback_hhmm}_{_start_hhmm}{_rm_suffix}"
        )
    overlap_path        = BASE_DIR / f"overlap_{idx_label}_w{freq_width}c{freq_cutoff}_{mode_label}_{mcap_label}{_run_suffix}.csv"
    output_files        = []   # collect all generated output paths for final summary

    # ── Step 1+2: DB mode — bypass parquet load/filter/downsample entirely ──
    if source == "db":
        from collections import Counter as _Counter
        _raw_lookback = DEPLOYMENT_START_HOUR if SORT_LOOKBACK == "daily" else int(SORT_LOOKBACK)
        _cap_note     = f", internal from {_raw_lookback}h" if _eff_lookback_run < _raw_lookback else ""
        _window_label = f"{_lookback_hhmm[:2]}:00-{DEPLOYMENT_START_HOUR:02d}:00 ({_eff_lookback_run}h{_cap_note})"

        if live_parity:
            log.info("[LIVE-PARITY MODE] Reproducing daily_signal_v2 basket selection on historical DB data "
                     "(unfiltered universe, pct_change ranking on both price + OI, "
                     "single 06:00 UTC snapshot, BloFin filter applied AFTER ranking). "
                     "Audits using this flag pick the basket the live trader would have picked each day.")
            price_freq, oi_freq = _load_live_parity_frequencies_from_db(
                freq_width=freq_width,
                sample_interval=sample_interval,
                deployment_start_hour=DEPLOYMENT_START_HOUR,
            )
            log.info(f"  → {len(price_freq)} dates with price snapshot (live-parity v2)")
            log.info(f"  → {len(oi_freq)} dates with OI snapshot (live-parity v2)")
        elif _non_canonical_ranking:
            # Any non-default ranking axis (log_return/abs_dollar on price OR
            # abs_dollar on OI OR BloFin filter) reads raw values from
            # market.futures_1m and ranks on-the-fly (market.leaderboards is
            # pct_change-only and unfiltered by construction). Opt-in axes
            # for candidate methodology exploration per spec § 3.1.
            #
            # When apply_blofin_filter=True, we compute the v1-style per-day
            # universe once (top-`leaderboard_index` by 24h volume, BloFin-
            # gated) and reuse it for both price and OI rankings. This
            # bundles the BloFin gate with v1's top-100-by-volume narrowing
            # so `leaderboard_index=100 + apply_blofin_filter=True + (log_
            # return price, abs_dollar OI, mode=frequency)` reproduces v1
            # --live-parity exactly.
            log.info(f"[CANONICAL + split ranking] price={price_ranking_metric} "
                     f"oi={oi_ranking_metric} "
                     f"blofin_filter={apply_blofin_filter} mode={mode}")
            universe_by_day = None
            if apply_blofin_filter:
                blofin = _fetch_blofin_instruments()
                blofin_bases = sorted(blofin) if blofin else None
                if blofin_bases is None:
                    log.warning("[canonical] apply_blofin_filter=True but "
                                "BloFin fetch returned empty — per-day "
                                "universe will reflect volume only")
                universe_by_day = _compute_per_day_volume_universe(
                    leaderboard_index=leaderboard_index,
                    blofin_bases=blofin_bases,
                    deployment_start_hour=DEPLOYMENT_START_HOUR,
                )
                _avg = (sum(len(v) for v in universe_by_day.values())
                        / max(len(universe_by_day), 1))
                log.info(f"[canonical] Per-day universe (top-{leaderboard_index} "
                         f"by 24h volume"
                         f"{', BloFin-gated' if blofin_bases else ''}): "
                         f"{len(universe_by_day)} days, avg {_avg:.1f} symbols/day")
            price_freq = _load_canonical_futures_1m_frequencies(
                metric="close", freq_width=freq_width,
                sample_interval=sample_interval, mode=mode,
                ranking_metric=price_ranking_metric,
                apply_blofin_filter=apply_blofin_filter,
                universe_by_day=universe_by_day,
            )
            log.info(f"  → {len(price_freq)} dates with price data "
                     f"(ranking={price_ranking_metric})")
            oi_freq = _load_canonical_futures_1m_frequencies(
                metric="open_interest", freq_width=freq_width,
                sample_interval=sample_interval, mode=mode,
                ranking_metric=oi_ranking_metric,
                apply_blofin_filter=apply_blofin_filter,
                universe_by_day=universe_by_day,
            )
            log.info(f"  → {len(oi_freq)} dates with OI data "
                     f"(ranking={oi_ranking_metric})")
        else:
            log.info("[DB MODE] Computing frequency tables from market.leaderboards...")
            if mode == "snapshot":
                log.info(f"Computing price snapshot from DB at {DEPLOYMENT_START_HOUR:02d}:00 (freq_width={freq_width})...")
            else:
                log.info(f"Computing price frequency from DB (freq_width={freq_width}, {_window_label})...")
            price_freq = _load_frequency_from_db(
                metric="price", freq_width=freq_width,
                sample_interval=sample_interval, mode=mode,
                min_mcap=min_mcap, max_mcap=max_mcap,
                drop_unverified=drop_unverified,
            )
            log.info(f"  → {len(price_freq)} dates with price data")

            if mode == "snapshot":
                log.info(f"Computing OI snapshot from DB at {DEPLOYMENT_START_HOUR:02d}:00 (freq_width={freq_width})...")
            else:
                log.info(f"Computing OI frequency from DB (freq_width={freq_width}, {_window_label})...")
            oi_freq = _load_frequency_from_db(
                metric="open_interest", freq_width=freq_width,
                sample_interval=sample_interval, mode=mode,
                min_mcap=min_mcap, max_mcap=max_mcap,
                drop_unverified=drop_unverified,
            )
            log.info(f"  → {len(oi_freq)} dates with OI data")

        # Export freq tables (same as parquet path)
        freq_label = f"{idx_label}_w{freq_width}_{mode_label}_{mcap_label}{_run_suffix}"
        price_freq_path = BASE_DIR / f"freq_price_{freq_label}.csv"
        oi_freq_path    = BASE_DIR / f"freq_oi_{freq_label}.csv"
        if PRINT_FREQ_TABLE:
            print_freq_table(price_freq, f"PRICE  {freq_label}")
            print_freq_table(oi_freq,    f"OI     {freq_label}")
        export_freq_table(price_freq, price_freq_path)
        export_freq_table(oi_freq,    oi_freq_path)
        output_files += [price_freq_path, oi_freq_path]

    else:
        # ── Step 1: Filter (auto-skip if filtered files already exist) ──
        if price_filtered_path.exists() and oi_filtered_path.exists():
            log.info(f"Filtered files found for {mcap_label} — skipping filter step...")
            log.info(f"  Price : {price_filtered_path.name}")
            log.info(f"  OI    : {oi_filtered_path.name}")
            price_df = pd.read_parquet(price_filtered_path)
            oi_df    = pd.read_parquet(oi_filtered_path)
            price_df["timestamp_utc"] = pd.to_datetime(price_df["timestamp_utc"], utc=True)
            oi_df["timestamp_utc"]    = pd.to_datetime(oi_df["timestamp_utc"], utc=True)
        else:
            if min_mcap == 0 and not drop_unverified:
                # No market cap filter and not dropping unverified — copy base files directly
                log.warning("=" * 60)
                log.warning("NO MARKET CAP FILTER APPLIED (--min-mcap 0)")
                log.warning("ALL symbols are being passed forward including")
                log.warning("low-cap, illiquid, and unverified assets.")
                log.warning("=" * 60)
                import shutil
                shutil.copy2(price_input, price_filtered_path)
                shutil.copy2(oi_input,    oi_filtered_path)
                price_df = pd.read_parquet(price_filtered_path)
                oi_df    = pd.read_parquet(oi_filtered_path)
                price_df["timestamp_utc"] = pd.to_datetime(price_df["timestamp_utc"], utc=True)
                oi_df["timestamp_utc"]    = pd.to_datetime(oi_df["timestamp_utc"], utc=True)
            else:
                log.info(f"No filtered files found for {mcap_label} — running filter...")
                mcap_lookup, mcap_coverage = build_mcap_lookup(marketcap_path, min_mcap, max_mcap=max_mcap)
                _above_cap_lookup = None
                if max_mcap > 0:
                    import pandas as _pd
                    _mc_raw = _pd.read_parquet(marketcap_path)
                    _mc_raw["date"] = _pd.to_datetime(_mc_raw["date"], utc=True).dt.date
                    _uni_path = marketcap_path.parent / "coins_universe.parquet"
                    if _uni_path.exists():
                        _uni = _pd.read_parquet(_uni_path)
                        _id2sym = dict(zip(_uni["id"], _uni["symbol"].str.upper()))
                    else:
                        _has = _mc_raw[_mc_raw["symbol"].notna()][["coin_id","symbol"]].drop_duplicates()
                        _id2sym = dict(zip(_has["coin_id"], _has["symbol"].str.upper()))
                    _mc_raw["base"] = _mc_raw["coin_id"].map(_id2sym)
                    _above = _mc_raw[(_mc_raw["market_cap_usd"] > max_mcap) & _mc_raw["base"].notna()]
                    _above_cap_lookup = {}
                    for _date, _grp in _above.groupby("date"):
                        _above_cap_lookup[_date] = set(_grp["base"])
                    log.info(f"  Above-cap lookup built: {len(_above_cap_lookup)} dates with symbols > ${max_mcap/1e6:.0f}M")
                price_df = filter_leaderboard(price_input, price_filtered_path, mcap_lookup, mcap_coverage, min_mcap, drop_unverified=drop_unverified, max_mcap_lookup=_above_cap_lookup)
                oi_df    = filter_leaderboard(oi_input,    oi_filtered_path,    mcap_lookup, mcap_coverage, min_mcap, drop_unverified=drop_unverified, max_mcap_lookup=_above_cap_lookup)

        # ── Step 1b: Apply sample interval downsampling (cached per interval) ──
        log.info(f"Applying {sample_interval}m downsampling to frequency window...")
        price_df = get_or_create_sampled(price_filtered_path, sample_interval)
        oi_df    = get_or_create_sampled(oi_filtered_path,    sample_interval)

        # ── Step 2: Frequency counts or snapshot ──
        freq_label = f"{idx_label}_w{freq_width}_{mode_label}_{mcap_label}{_run_suffix}"
        _raw_lookback = DEPLOYMENT_START_HOUR if SORT_LOOKBACK == "daily" else int(SORT_LOOKBACK)
        _cap_note     = f", internal from {_raw_lookback}h" if _eff_lookback_run < _raw_lookback else ""
        _window_label = f"{_lookback_hhmm[:2]}:00-{DEPLOYMENT_START_HOUR:02d}:00 ({_eff_lookback_run}h{_cap_note})"
        if mode == "snapshot":
            log.info(f"Computing price snapshot at {DEPLOYMENT_START_HOUR:02d}:00 (freq_width={freq_width} cols)...")
            price_freq = compute_daily_snapshot(price_df, freq_width)
            log.info(f"  → {len(price_freq)} dates with price snapshot data")
            log.info(f"Computing OI snapshot at {DEPLOYMENT_START_HOUR:02d}:00 (freq_width={freq_width} cols)...")
            oi_freq    = compute_daily_snapshot(oi_df,    freq_width)
            log.info(f"  → {len(oi_freq)} dates with OI snapshot data")
        else:
            log.info(f"Computing price frequency counts (freq_width={freq_width} cols, {_window_label})...")
            price_freq = compute_daily_frequency(price_df, freq_width)
            log.info(f"  → {len(price_freq)} dates with price frequency data")
            log.info(f"Computing OI frequency counts (freq_width={freq_width} cols, {_window_label})...")
            oi_freq    = compute_daily_frequency(oi_df,    freq_width)
            log.info(f"  → {len(oi_freq)} dates with OI frequency data")

        # ── Print + export full frequency tables ──
        price_freq_path = BASE_DIR / f"freq_price_{freq_label}.csv"
        oi_freq_path    = BASE_DIR / f"freq_oi_{freq_label}.csv"
        if PRINT_FREQ_TABLE:
            print_freq_table(price_freq, f"PRICE  {freq_label}")
            print_freq_table(oi_freq,    f"OI     {freq_label}")
        export_freq_table(price_freq, price_freq_path)
        export_freq_table(oi_freq,    oi_freq_path)
        output_files += [price_freq_path, oi_freq_path]

    # ── Step 2b (conditional): Volume frequency table for Stream D-explore ──
    # Only computed when overlap_dimensions includes 'volume'. Reads
    # market.futures_1m directly (volume isn't in the pre-built
    # market.leaderboards table). Uses the same ranking metric and universe
    # narrowing as price/OI for symmetry with the canonical intersection.
    volume_freq: dict | None = None
    if "volume" in overlap_dimensions:
        if source != "db":
            raise RuntimeError(
                f"overlap_dimensions={overlap_dimensions!r} requires source=db; "
                f"got source={source!r}. Volume is not in the pre-built "
                f"market.leaderboards tables — must be computed on-the-fly "
                f"from market.futures_1m."
            )
        if live_parity:
            raise RuntimeError(
                "overlap_dimensions with 'volume' is incompatible with "
                "--live-parity (live-parity reproduces v1 which uses only "
                "price ∩ OI). Drop --live-parity to explore volume overlaps."
            )
        # Volume shares price's ranking_metric (not OI's) because volume is
        # more naturally a flow-like quantity, like price changes. When the
        # user has overridden price ranking, volume follows.
        _vol_ranking = price_ranking_metric
        # Note: when apply_blofin_filter=True with non-canonical ranking,
        # the `_non_canonical_ranking` branch above computes a per-day
        # `universe_by_day` and passes it to price/OI loaders. Volume is
        # computed here separately and re-derives its own per-day universe
        # inside `_load_canonical_futures_1m_frequencies` via the
        # apply_blofin_filter path. For the default canonical call (pct_change
        # both, no BloFin filter) this is a no-op — universe is unfiltered
        # across all three axes. Hoisting universe_by_day to share across all
        # three loaders is a future refinement; for the acceptance-test
        # scope (canonical pct_change, no BloFin filter) it's equivalent.
        log.info(f"[D-explore] Computing volume frequencies "
                 f"(ranking={_vol_ranking}, mode={mode})...")
        volume_freq = _load_canonical_futures_1m_frequencies(
            metric="volume", freq_width=freq_width,
            sample_interval=sample_interval, mode=mode,
            ranking_metric=_vol_ranking,
            apply_blofin_filter=apply_blofin_filter,
            universe_by_day=None,
        )
        log.info(f"  → {len(volume_freq)} dates with volume data")

    # ── Step 3: Overlap ──
    log.info(f"Computing overlap (freq_cutoff={freq_cutoff}, dimensions={overlap_dimensions})...")
    overlap_df = compute_overlap(
        price_freq, oi_freq,
        freq_cutoff=freq_cutoff, sort_by=sort_by,
        volume_freq=volume_freq,
        overlap_dimensions=overlap_dimensions,
    )
    log.info(f"  → {len(overlap_df)} dates processed")

    # ── Export overlap diagnostic CSV ──
    diagnostic_path = BASE_DIR / f"overlap_diagnostic_{idx_label}_w{freq_width}c{freq_cutoff}_{mode_label}_{mcap_label}.csv"
    diag_rows = []
    for _, row in overlap_df.iterrows():
        diag_rows.append({
            "date":               row["date"].date(),
            "price_count":        row["price_count"],
            "oi_count":           row["oi_count"],
            "overlap_count":      row["overlap_count"],
            "price_symbols":      "|".join(row["price_symbols"]),
            "oi_symbols":         "|".join(row["oi_symbols"]),
            "overlap_symbols":    "|".join(row["overlap_symbols"]),
        })
    diag_df = pd.DataFrame(diag_rows)
    diag_df.to_csv(diagnostic_path, index=False)
    log.info(f"Overlap diagnostic CSV saved → {diagnostic_path.name}")
    output_files.append(diagnostic_path)

    overlap_df.to_csv(overlap_path, index=False)
    output_files.append(overlap_path)

    log.info("=" * 60)
    log.info(f"Overlap analysis complete")
    log.info(f"  Min market cap  : ${min_mcap:,.0f}")
    log.info(f"  Freq width      : R1-R{freq_width}")
    log.info(f"  Mode            : {mode} (freq_width={freq_width}, {f'{DEPLOYMENT_START_HOUR:02d}:00 snapshot' if mode == 'snapshot' else _window_label})")
    log.info(f"  Dates processed : {len(overlap_df)}")
    log.info(f"  Avg overlap/day : {overlap_df['overlap_count'].mean():.1f} symbols")
    log.info(f"  Output          : {overlap_path.name}")
    log.info("=" * 60)

    # Print sample
    print("\nSample output (first 5 dates):")
    for _, row in overlap_df.head(5).iterrows():
        print(f"  {row['date'].date()} — {row['overlap_count']} overlapping: "
              f"{row['overlap_symbols'][:10]}{'...' if row['overlap_count'] > 10 else ''}")

    # ── Export deploys CSV ──
    deploys_path = BASE_DIR / f"deploys_overlap_{idx_label}_w{freq_width}c{freq_cutoff}_{mode_label}_{mcap_label}{_run_suffix}.csv"
    export_deploys(overlap_df, deploys_path)
    output_files.append(deploys_path)

    # ── Optionally run audit ──
    audit_finals = {}
    if audit and audit_script:
        # Pass the run config so audit.py can derive the freq CSV path dynamically.
        # This means audit.py never needs a hardcoded filename — it reconstructs it
        # from the same parameters overlap_analysis.py used to produce the file.
        mcap_audit_args = [
            "--base-dir",              str(BASE_DIR),
            "--leaderboard-index", str(leaderboard_index),
            "--freq-width",        str(freq_width),
            "--mode",              mode,
            "--min-mcap",               str(min_mcap),
        ]
        # Also pass the mcap parquet path explicitly if we know where it is
        _mcap_parquet_path = marketcap_dir / "marketcap_daily.parquet"
        if _mcap_parquet_path.exists():
            mcap_audit_args += ["--mcap-parquet", str(_mcap_parquet_path)]

        audit_finals = run_audit(
            deploys_path, audit_script,
            (audit_args or []) + mcap_audit_args,
        )

    # ── Append no-filter warning to gate_summary.txt if applicable ──
    if min_mcap == 0:
        gate_summary_path = BASE_DIR / "gate_summary.txt"
        if gate_summary_path.exists():
            existing = gate_summary_path.read_text()
            warning = (
                "=" * 60 + "\n"
                "WARNING: NO MARKET CAP FILTER APPLIED\n"
                "  --min-mcap was set to 0. All symbols were passed forward\n"
                "  including low-cap, illiquid, and unverified assets.\n"
                "  Results should be interpreted with caution.\n"
                + "=" * 60 + "\n\n"
            )
            gate_summary_path.write_text(warning + existing)
            log.warning("No-filter warning appended to gate_summary.txt")
            output_files.append(gate_summary_path)

    # ── Export to Google Sheets ──
    run_label = f"{idx_label}_w{freq_width}_c{freq_cutoff}_{sample_interval}m_{mcap_label}_sort={sort_by}"
    if EXPORT_TO_GSHEETS:
        export_to_gsheets(overlap_df, deploys_path, run_label, confluence_path=confluence_path)
    else:
        log.info("EXPORT_TO_GSHEETS=False — skipping Google Sheets export")

    # ── Audit outputs (portfolio matrix + gate report) ──
    portfolio_matrix = BASE_DIR / "portfolio_matrix_gated.csv"
    gate_report      = BASE_DIR / "eligibility_gate_report.csv"
    gate_summary     = BASE_DIR / "gate_summary.txt"
    for p in [portfolio_matrix, gate_report, gate_summary]:
        if p.exists() and p not in output_files:
            output_files.append(p)

    # ── Print all generated output files ──
    print()
    print("=" * 60)
    print("OUTPUT FILES")
    print("=" * 60)
    for p in output_files:
        exists = "✓" if Path(p).exists() else "✗"
        print(f"  {exists}  {p}")
    print("=" * 60)

    # ── END-OF-RUN SUMMARY ────────────────────────────────────────
    W = 148
    print()
    print("█" * W)
    print("  RUN SUMMARY")
    print("█" * W)
    print(f"  Config          : {idx_label}  w{freq_width}c{freq_cutoff}  {mode}  {mcap_label}  sort={sort_by}")
    print(f"  Dates processed : {len(overlap_df)}")
    _avg_overlap = overlap_df["overlap_count"].mean()
    print(f"  Avg overlap/day : {_avg_overlap:.1f} symbols")
    if audit_finals:
        # audit_finals = { tag -> { FINAL_SHARPE, FINAL_CAGR, ... } }
        # Determine winner dynamically: tag with highest FINAL_SHARPE
        _winner_tag = max(
            audit_finals,
            key=lambda t: audit_finals[t].get("FINAL_SHARPE", float("-inf")),
        )
        _COL = {"FINAL_SHARPE": ("Sharpe", False, ".3f"),
                "FINAL_CAGR":   ("CAGR%",  True,  ".1f"),
                "FINAL_MAX_DD": ("MaxDD%", True,  ".2f"),
                "FINAL_ACTIVE_DAYS": ("Active", False, "d"),
                "FINAL_WF_CV":  ("WF-CV",  False, ".3f"),
                "FINAL_TOTAL_RETURN": ("TotRet%", True, ".1f")}
        _hdr = f"  {'Filter':<38}  {'Sharpe':>7}  {'CAGR%':>8}  {'MaxDD%':>8}  {'Active':>7}  {'WF-CV':>7}  {'TotRet%':>9}  {'Eq':>6}  {'Wst1D%':>7}  {'Wst1W%':>7}  {'Wst1M%':>7}  {'DSR%':>6}  {'Grd':>5}"
        print("─" * W)
        print(_hdr)
        print("─" * W)
        for _tag, _vals in audit_finals.items():
            _sh  = _vals.get("FINAL_SHARPE",        float("nan"))
            _ca  = _vals.get("FINAL_CAGR",          float("nan"))
            _md  = _vals.get("FINAL_MAX_DD",        float("nan"))
            _act = _vals.get("FINAL_ACTIVE_DAYS",   float("nan"))
            _cv  = _vals.get("FINAL_WF_CV",         float("nan"))
            _tr  = _vals.get("FINAL_TOTAL_RETURN",  float("nan"))
            _eq  = 1.0 + _tr / 100.0 if not (isinstance(_tr, float) and _tr != _tr) else float("nan")
            _wd  = _vals.get("FINAL_WORST_DAY",     float("nan"))
            _ww  = _vals.get("FINAL_WORST_WEEK",    float("nan"))
            _wm  = _vals.get("FINAL_WORST_MONTH",   float("nan"))
            _dsr = _vals.get("FINAL_DSR",           float("nan"))
            _gs  = _vals.get("FINAL_GRADE_SCORE",   float("nan"))
            _label = _tag.replace("_", " ").replace("Balanced Opt   ", "").strip()
            _star = " ◄" if _tag == _winner_tag else "  "
            _sh_s  = f"{_sh:>7.3f}"    if not (isinstance(_sh,  float) and _sh  != _sh)  else "    n/a"
            _ca_s  = f"{_ca:>7.1f}%"   if not (isinstance(_ca,  float) and _ca  != _ca)  else "    n/a"
            _md_s  = f"{_md:>7.2f}%"   if not (isinstance(_md,  float) and _md  != _md)  else "    n/a"
            _act_s = f"{int(_act):>7d}" if not (isinstance(_act, float) and _act != _act) else "    n/a"
            _cv_s  = f"{_cv:>7.3f}"    if not (isinstance(_cv,  float) and _cv  != _cv)  else "    n/a"
            _tr_s  = f"{_tr:>8.1f}%"   if not (isinstance(_tr,  float) and _tr  != _tr)  else "     n/a"
            _eq_s  = f"{_eq:>5.2f}×"   if not (isinstance(_eq,  float) and _eq  != _eq)  else "   n/a"
            _wd_s  = f"{_wd:>6.2f}%"   if not (isinstance(_wd,  float) and _wd  != _wd)  else "   n/a"
            _ww_s  = f"{_ww:>6.2f}%"   if not (isinstance(_ww,  float) and _ww  != _ww)  else "   n/a"
            _wm_s  = f"{_wm:>6.2f}%"   if not (isinstance(_wm,  float) and _wm  != _wm)  else "   n/a"
            _dsr_s = f"{_dsr:>5.1f}%"  if not (isinstance(_dsr, float) and _dsr != _dsr) else "   n/a"
            _gs_s  = f"{_gs:>4.0f}"    if not (isinstance(_gs,  float) and _gs  != _gs)  else "  n/a"
            print(f"  {_label:<38}  {_sh_s}  {_ca_s}  {_md_s}  {_act_s}  {_cv_s}  {_tr_s}  {_eq_s}  {_wd_s}  {_ww_s}  {_wm_s}  {_dsr_s}  {_gs_s}{_star}")
        print("─" * W)
    else:
        print("─" * W)
        print("  (run with --audit to see performance metrics)")
    print("█" * W)
    print()

    return overlap_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-mcap",      type=float, default=float(_os.environ.get("MIN_MCAP", "0")))
    parser.add_argument("--freq-width",    type=int,   default=int(_os.environ.get("FREQ_WIDTH", "20")),
                        help="Number of rank columns R1-RN to scan for frequency (default: 60)")
    parser.add_argument("--leaderboard-index", type=int, default=LEADERBOARD_INDEX,
                        choices=[100, 300, 333, 1000],
                        help=f"Index size for both leaderboards: 100, 300, or 1000 (default: {LEADERBOARD_INDEX})")
    parser.add_argument("--sample-interval", type=int, default=SAMPLE_INTERVAL_MINUTES,
                        help=f"Downsample frequency window to every N minutes (default: {SAMPLE_INTERVAL_MINUTES})")
    parser.add_argument("--capital-mode",   type=str,   default=None,
                        choices=["compounding", "fixed"],
                        help="Capital allocation mode passed to audit.py: "
                             "compounding (default) or fixed notional per day.")
    parser.add_argument("--fixed-notional-cap", type=str, default=None,
                        choices=["external", "internal"],
                        help="Only used with --capital-mode fixed. "
                             "external (default): always trade full starting notional. "
                             "internal: cap position at current equity if below starting capital.")
    parser.add_argument("--sort-by",        type=str,   default=OVERLAP_SORT_BY,
                        choices=["price", "open_interest", "combined", "price-only", "oi-only"],
                        help="Sort overlap symbols by rank. "
                             "price/open_interest/combined: intersect both leaderboards. "
                             "price-only: use price leaderboard only. "
                             "oi-only: use OI leaderboard only. (default: price)")
    parser.add_argument("--freq-cutoff",   type=int,   default=FREQ_TOP_N,
                        help=f"After computing frequency, keep only top-N symbols from each "
                             f"Top-N cutoff after frequency sort, before intersecting (default: {FREQ_TOP_N})")
    parser.add_argument("--marketcap-dir", type=str,   default=str(BASE_DIR / "binetl/data/marketcap"))
    parser.add_argument("--mode",           type=str, default=_os.environ.get("MODE", "snapshot"),
                        choices=["frequency", "snapshot"],
                        help="frequency: count symbols in R1-RN across 00:00-06:00 | "
                             "snapshot: take the 06:00 bar only (default: frequency)")
    parser.add_argument("--audit",          action="store_true",
                        help="Run audit.py after generating deploys CSV")
    parser.add_argument("--audit-script",   type=str, default="audit.py",
                        help="Path to audit.py (default: ./audit.py)")
    parser.add_argument("--quick",          action="store_true",
                        help="Pass --quick flag to audit.py (faster run)")
    parser.add_argument("--audit-source",   type=str, default=None,
                        choices=["binance", "parquet", "db"],
                        help="Price source for audit rebuild (passed to audit.py)")
    parser.add_argument("--source",         type=str,
                        default=_os.environ.get("OVERLAP_SOURCE", "parquet"),
                        choices=["parquet", "db"],
                        help="Data source for leaderboard frequency computation. "
                             "'parquet' loads wide-format ALL parquets (default). "
                             "'db' queries market.leaderboards directly (low memory).")
    parser.add_argument("--max-mcap",        type=float, default=float(_os.environ.get("MAX_MCAP", "0.0")),
                        help="Maximum market cap in USD (e.g. 500000000 for $500M). Symbols above this are excluded. Default: no ceiling.")
    parser.add_argument("--drop-unverified", action="store_true", default=_os.environ.get("DROP_UNVERIFIED", "0") == "1",
                        help="Drop symbols with no mcap data (fail closed). Default: pass through (fail open).")
    parser.add_argument("--confluence",     type=str, default=None,
                        help="Path to confluence CSV (e.g. CTABLE_30_*.csv) — exports "
                             "R1-R30 OI, R1-R30 price, and shared_symbs to a Confluence tab in Google Sheets")
    parser.add_argument("--deployment-start-hour", type=int, default=None,
                        dest="deployment_start_hour", metavar="H",
                        help="Electoral window end / deployment start hour UTC (0-23). "
                             "Default: 6 (= 06:00 UTC). Overrides DEPLOYMENT_START_HOUR constant. "
                             "Affects snapshot target, frequency filter window, deploys timestamp, "
                             "and mode_label in all output filenames. Passed through to audit.py "
                             "and rebuild_portfolio_matrix.py as --start-hour.")
    parser.add_argument("--index-lookback", type=int, default=None,
                        dest="index_lookback", metavar="H",
                        help="Hours before deployment_start_hour to use as the "
                             "leaderboard %% change anchor. "
                             "0 = midnight (default behaviour). "
                             "e.g. start=6, index_lookback=6 anchors at 00:00; "
                             "start=6, index_lookback=4 anchors at 02:00. "
                             "If the required leaderboard file does not exist, "
                             "build_intraday_leaderboard.py is called automatically.")
    parser.add_argument("--deployment-runtime-hours", type=str, default=None,
                        dest="deployment_runtime_hours", metavar="H",
                        help="Deployment window length: integer hours or \"daily\". "
                             "\"daily\" sets runtime = 24 - sort_lookback. "
                             "Default: \"daily\". Passed through to audit.py "
                             "and rebuild_portfolio_matrix.py.")
    parser.add_argument("--end-cross-midnight", dest="end_cross_midnight",
                        action="store_true", default=None,
                        help="Allow deployment window to overflow past 23:59 UTC into the "
                             "next calendar day (default: True). "
                             "Passed through to audit.py and rebuild_portfolio_matrix.py.")
    parser.add_argument("--no-end-cross-midnight", dest="end_cross_midnight",
                        action="store_false",
                        help="Cap deployment window at 23:55 UTC same day. "
                             "Passed through to audit.py and rebuild_portfolio_matrix.py.")
    parser.add_argument("--sort-lookback", type=str, default=None,
                        dest="sort_lookback", metavar="N",
                        help="Electoral window length. Either an integer number of hours "
                             "(e.g. 6 = scan the 6h before deployment_start_hour) or the "
                             "string \"daily\" to scan from 00:05 to deployment_start_hour "
                             "(resolves to deployment_start_hour hours). "
                             "If start_hour - N < 0, the window wraps to the previous "
                             "calendar day. Default: 6.")
    parser.add_argument("--force", action="store_true", default=False,
                        help="Force a leaderboard rebuild even if the parquet exists. "
                             "Passes --force through to build_intraday_leaderboard.py "
                             "so it bypasses its own DB checkpoint as well.")
    parser.add_argument("--live-parity", dest="live_parity", action="store_true", default=False,
                        help="OPT-IN: simulate daily_signal.py (live v1) basket methodology for "
                             "comparison audits. When set: universe = top-100 USDT perps by 24h "
                             "USD volume ending 06:00 UTC, filtered to BloFin instrument list; "
                             "ASYMMETRIC ranking (log-return on price, absolute $ delta on OI — "
                             "matches daily_signal.py v1 exactly); freq count over 72 5-min bars. "
                             "Mutually exclusive with --price-ranking-metric, --oi-ranking-metric, "
                             "and --apply-blofin-filter. Does NOT affect canonical audit output "
                             "unless explicitly set. Requires --source db.")
    # ── Individual ranking-metric knobs (Stream D-medium-split, 2026-04-23) ──
    # Replaces --ranking-metric (deprecated below). Split into price + OI so
    # Simulator UI can reproduce live v1 asymmetry (log_return price + abs_dollar OI)
    # from individual dropdowns, without needing the --live-parity preset.
    parser.add_argument("--price-ranking-metric", dest="price_ranking_metric", type=str,
                        default="pct_change", choices=["log_return", "pct_change", "abs_dollar"],
                        help="OPT-IN (default: pct_change). Ranking formula for PRICE. "
                             "log_return = LN(close/anchor) (v1 methodology, identical ordering "
                             "to pct_change for positive prices). pct_change = (close/anchor) − 1 "
                             "(canonical default). abs_dollar = (close − anchor) (exploratory — "
                             "tends to dominate with mega-caps on canonical universe).")
    parser.add_argument("--oi-ranking-metric", dest="oi_ranking_metric", type=str,
                        default="pct_change", choices=["pct_change", "abs_dollar"],
                        help="OPT-IN (default: pct_change). Ranking formula for OPEN INTEREST. "
                             "pct_change = (oi/anchor) − 1 (canonical default). abs_dollar = "
                             "(oi − anchor) (matches live v1 methodology). log_return is NOT "
                             "valid for OI — use pct_change if relative OI moves matter.")
    parser.add_argument("--apply-blofin-filter", dest="apply_blofin_filter",
                        action="store_true", default=False,
                        help="OPT-IN (default: off). When set, universe is narrowed to symbols "
                             "currently listed on BloFin's USDT-swap instrument list BEFORE "
                             "ranking. Matches live trading universe constraints. Fetched once "
                             "per audit-run via BloFin REST API.")
    parser.add_argument("--overlap-dimensions", dest="overlap_dimensions", type=str,
                        default="price_oi", choices=list(OVERLAP_DIMENSIONS_CHOICES),
                        help="Which axes must agree for a symbol to enter the basket. "
                             "'price_oi' (default) is canonical: intersection of top-N-by-price "
                             "with top-N-by-OI. Stream D-explore adds 'price_volume' (drop OI), "
                             "'oi_volume' (drop price), and 'price_oi_volume' (three-way). "
                             "Requires --source db since volume is not in the pre-built "
                             "market.leaderboards tables. Mutually exclusive with --live-parity.")
    # ── Deprecated: --ranking-metric (symmetric single knob) ──
    # Mapped to both --price-ranking-metric and --oi-ranking-metric for one release
    # of backward compatibility. Emits a deprecation warning when used. Remove in
    # next cycle once no callers remain.
    parser.add_argument("--ranking-metric", dest="ranking_metric", type=str,
                        default=None, choices=["pct_change", "abs_dollar"],
                        help="DEPRECATED — use --price-ranking-metric and --oi-ranking-metric "
                             "individually. Kept for one release cycle for backward compat. "
                             "Maps to both new flags with the same value; emits a deprecation "
                             "warning.")
    args = parser.parse_args()

    # Backward compat: --ranking-metric → both new flags.
    if args.ranking_metric is not None:
        log.warning(
            "[DEPRECATION] --ranking-metric is deprecated and will be removed in a future "
            "release. Use --price-ranking-metric and --oi-ranking-metric individually. "
            "Mapping %r to both new flags.", args.ranking_metric
        )
        # Only override if the user didn't also set the new flags explicitly.
        if args.price_ranking_metric == "pct_change":
            args.price_ranking_metric = args.ranking_metric
        if args.oi_ranking_metric == "pct_change":
            args.oi_ranking_metric = args.ranking_metric

    # Mutual exclusion: --live-parity bundles its own ranking + universe setup
    # internally. Combining with any individual ranking/universe knob creates
    # ambiguity, so we reject at CLI parse time.
    if args.live_parity and (
        args.price_ranking_metric != "pct_change"
        or args.oi_ranking_metric != "pct_change"
        or args.apply_blofin_filter
        or args.ranking_metric is not None
        or args.overlap_dimensions != "price_oi"
    ):
        parser.error(
            "--live-parity is mutually exclusive with --price-ranking-metric, "
            "--oi-ranking-metric, --apply-blofin-filter, --ranking-metric, and "
            "--overlap-dimensions (must stay 'price_oi'). "
            "--live-parity reproduces daily_signal.py v1 exactly (asymmetric: log-return "
            "on price, absolute $ delta on OI; top-100 by 24h volume; BloFin-filtered; "
            "price ∩ OI only). Use the individual knobs OR --live-parity, not both, per "
            "docs/strategy_specification.md § 3.1."
        )

    audit_args = []
    if args.quick:
        audit_args.append("--quick")
    if args.audit_source:
        audit_args += ["--source", args.audit_source]
    if args.sort_lookback is not None:
        # Accept either an integer string ("6") or the literal "daily"
        SORT_LOOKBACK = args.sort_lookback if args.sort_lookback == "daily" \
                        else int(args.sort_lookback)
        # Pass the resolved (internal) integer downstream so audit.py and
        # rebuild_portfolio_matrix.py use the correct effective lookback.
        audit_args += ["--sort-lookback", str(_resolve_sort_lookback())]
    if getattr(args, "capital_mode", None):
        audit_args += ["--capital-mode", args.capital_mode]

    if getattr(args, "fixed_notional_cap", None):
        audit_args += ["--fixed-notional-cap", args.fixed_notional_cap]

    if getattr(args, "index_lookback", None) is not None:
        INDEX_LOOKBACK = args.index_lookback
        # index_lookback is an overlap_analysis concern only — it determines
        # which leaderboard file is read. It does not need to pass to audit.py.
    if getattr(args, "deployment_runtime_hours", None) is not None:
        # Pass through to audit.py -> rebuild_portfolio_matrix.py
        audit_args += ["--deployment-runtime-hours", str(args.deployment_runtime_hours)]
    if getattr(args, "end_cross_midnight", None) is not None:
        audit_args += ["--end-cross-midnight" if args.end_cross_midnight
                       else "--no-end-cross-midnight"]
    if args.deployment_start_hour is not None:
        # Override module-level constant so all functions use the new hour
        DEPLOYMENT_START_HOUR = args.deployment_start_hour
        # Pass through to audit.py -> rebuild_portfolio_matrix.py
        audit_args += ["--deployment-start-hour", str(args.deployment_start_hour)]

    import time as _time
    _start = _time.time()
    log.info(f"Starting overlap_analysis.py at {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Prominent top-of-run header so audits are visibly distinguishable by
    # universe-selection mode. Mismatches between audit and live trading
    # (observed 2026-04-23 TAC-vs-GENIUS) were invisible without this.
    log.info("=" * 60)
    log.info(f"LIVE-PARITY: {'ON' if args.live_parity else 'OFF'}"
             f"  ({'daily_signal_v2 reproduction — unfiltered universe, pct_change rank on price + OI, 06:00 UTC snapshot, BloFin AFTER' if args.live_parity else 'market.leaderboards fast path'})")
    log.info("=" * 60)

    if args.live_parity and args.source != "db":
        log.warning("[LIVE-PARITY] --live-parity requires --source db; overriding source=db")
        args.source = "db"

    run(
        min_mcap      = args.min_mcap,
        freq_width    = args.freq_width,
        freq_cutoff   = args.freq_cutoff,
        sort_by            = args.sort_by,
        sample_interval    = args.sample_interval,
        leaderboard_index  = args.leaderboard_index,
        marketcap_dir = Path(args.marketcap_dir),
        mode          = args.mode,
        audit         = args.audit,
        audit_script  = Path(args.audit_script) if args.audit else None,
        audit_args    = audit_args if audit_args else None,
        confluence_path = Path(args.confluence) if args.confluence else None,
        drop_unverified = args.drop_unverified,
        max_mcap        = args.max_mcap,
        force           = args.force,
        source          = args.source,
        live_parity          = args.live_parity,
        price_ranking_metric = args.price_ranking_metric,
        oi_ranking_metric    = args.oi_ranking_metric,
        apply_blofin_filter  = args.apply_blofin_filter,
        overlap_dimensions   = args.overlap_dimensions,
    )

    elapsed = _time.time() - _start
    mins, secs = divmod(int(elapsed), 60)

    log.info("=" * 60)
    log.info("RUN INPUTS SUMMARY")
    log.info("=" * 60)
    log.info(f"  --leaderboard-index  : {args.leaderboard_index}")
    log.info(f"  --min-mcap           : ${args.min_mcap:,.0f}{' ⚠ NO FILTER APPLIED — all symbols passed forward' if args.min_mcap == 0 else ''}")
    log.info(f"  --freq-width         : {args.freq_width}")
    log.info(f"  --freq-cutoff        : {args.freq_cutoff}")
    log.info(f"  --sample-interval    : {args.sample_interval}m")
    log.info(f"  --sort-by            : {args.sort_by}")
    log.info(f"  --mode                      : {args.mode}{'  [OVERRIDDEN by --live-parity]' if args.live_parity else ''}")
    log.info(f"  --live-parity               : {args.live_parity}")
    log.info(f"  --price-ranking-metric      : {args.price_ranking_metric}"
             f"{'  [IGNORED under --live-parity — internally log_return]' if args.live_parity else ''}")
    log.info(f"  --oi-ranking-metric         : {args.oi_ranking_metric}"
             f"{'  [IGNORED under --live-parity — internally abs_dollar]' if args.live_parity else ''}")
    log.info(f"  --apply-blofin-filter       : {args.apply_blofin_filter}"
             f"{'  [IGNORED under --live-parity — internally True]' if args.live_parity else ''}")
    log.info(f"  --deployment-start-hour : {DEPLOYMENT_START_HOUR:02d}:00 UTC")
    log.info(f"  --index-lookback        : {INDEX_LOOKBACK}h  "
             f"(anchor={_anchor_hhmm()[:2]}:{_anchor_hhmm()[2:]} UTC)")
    log.info(f"  --deployment-runtime-hours : {DEPLOYMENT_RUNTIME_HOURS}")
    log.info(f"  --sort-lookback      : {SORT_LOOKBACK}")
    log.info(f"  --end-cross-midnight : {getattr(args, 'end_cross_midnight', None)}")
    log.info(f"  --drop-unverified    : {args.drop_unverified}")
    log.info(f"  --max-mcap           : ${args.max_mcap:,.0f}" if args.max_mcap > 0 else "  --max-mcap           : no ceiling")
    log.info("=" * 60)
    log.info(f"Finished in {mins}m {secs}s")

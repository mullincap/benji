#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
daily_signal_v2.py
==================
Canonical daily-signal generator for the Alpha Tail Guardrail family
(Alpha Low / Main / Max).

Implements the CANONICAL methodology documented in
docs/strategy_specification.md, which matches the methodology used by
the published audit (overlap_analysis.py --mode snapshot) that
produces the Sharpe 3.5 landing-page metric.

This replaces the methodology in the legacy daily_signal.py, which
was discovered via the 2026-04-22 comparison audit (Part 1b) to
backtest at Sharpe -0.55 over 432 days — net-negative EV and
materially different from what published metrics describe. See
docs/strategy_specification.md § Incident log.

Canonical pipeline (this file):
  1. Query BloFin instrument list (exchange-availability gate).
  2. Snapshot at 06:00 UTC: query market.futures_1m for per-symbol
     pct_change from the 00:00 UTC anchor, for `close` (price) and
     `open_interest`. Rank globally for each metric; take top-20 each.
     Basket = intersection of the two top-20 sets, normalized.
  3. BloFin filter: drop basket symbols not on BloFin.
  4. Tail Guardrail: BTC prev-day return < -4% OR 5d rvol > 1.4x 60d
     baseline → sit flat.
  5. Write to live_deploys_signal.csv (trader reads this).
  6. Write to user_mgmt.daily_signals + daily_signal_items DB, one row
     per published+active strategy_version_id (Alpha Low / Main / Max).
     The trader at 06:05 UTC reads these DB rows, not the CSV.

CUTOVER STATUS (2026-04-23)
---------------------------
v2 is the LIVE signal generator as of 05:58 UTC 2026-04-23, replacing
daily_signal.py v1. The DB write path (step 6 above) is ported verbatim
from host-v1 (see archive/daily_signal_v1_host_snapshot_20260423.py) —
it queries all published+active strategy_version_ids and writes one
daily_signals row per version, so allocation-keyed queries in manager.py
and spawn_traders.py find signals matching their strategy_version_id.

NOTE: v2 computes the 06:00 snapshot on-the-fly from market.futures_1m
rather than reading from market.leaderboards. This is a deliberate
design choice because the nightly indexer cron (01:00 UTC) populates
market.leaderboards for day D-1, not day D — so the canonical
leaderboard row for today's 06:00 snapshot does not exist yet at
05:58 UTC when v2 runs. The formula and hygiene match the builder
(pipeline/indexer/build_intraday_leaderboard.py):
   anchor = futures_1m.(close|open_interest) at 00:00 UTC
   pct_change = value_now / anchor - 1
   drop where anchor == 0 or null (builder line ~1043: .replace(0, pd.NA))
   rank by pct_change DESC; take top-20.

Usage (cron entry):
  58 5 * * *  /root/benji/pipeline/.venv/bin/python /root/benji/daily_signal_v2.py \\
              >> /mnt/quant-data/logs/signal/cron.log 2>&1
"""

import csv
import datetime as _dt
import logging
import math
import re
import sys
import time
from pathlib import Path

import requests

# ==========================================================================
# CONFIG (shared with canonical audit spec)
# ==========================================================================

DEPLOYMENT_START_HOUR = 6      # 06:00 UTC snapshot
FREQ_WIDTH            = 20     # top-20 per metric
ANCHOR_HOUR           = 0      # 00:00 UTC anchor

# -- Tail Guardrail (unchanged from v1, verified against audit --------------
TAIL_DROP_PCT         = 0.04
TAIL_VOL_MULT         = 1.4
TAIL_VOL_SHORT_WINDOW = 5
TAIL_VOL_LONG_WINDOW  = 60

# -- Dispersion filter (matches audit.py build_dispersion_filter) ----------
# Cross-sectional dispersion of yesterday's daily log-returns across top-N
# mcap symbols. Low dispersion (alts moving together) = momentum edge
# suppressed → sit flat today.
#
# Lagged by 1 day: yesterday's disp_ratio gates today's trade.
# Static universe in the live path (top-N mcap *today*) used for the full
# baseline window — a minor approximation vs the audit's per-day universe.
DISPERSION_N            = 40
DISPERSION_BASELINE_WIN = 33
DISPERSION_THRESHOLD    = 0.66
DISPERSION_MIN_SYMBOLS  = 20

FILTER_NAME           = "Tail Guardrail"

# -- Output paths -----------------------------------------------------------
# Post-cutover (2026-04-23): v2 writes to the production filename
# `live_deploys_signal.csv` that trader-blofin.py has historically read.
# Legacy v1 output file is preserved at live_deploys_signal_v1_archive.csv
# by the cutover step if needed for rollback reference.
_SCRIPT_DIR           = Path(__file__).resolve().parent
DEPLOYS_CSV           = _SCRIPT_DIR / "live_deploys_signal.csv"
DEPLOYS_RETAIN_DAYS   = 90
LOG_FILE              = _SCRIPT_DIR / "daily_signal_v2.log"


# ==========================================================================
# NORMALIZATION (verbatim from pipeline/overlap_analysis.py)
# ==========================================================================

NON_CRYPTO = {
    "AMZN", "TSLA", "INTC", "XAU", "XAG", "XPD", "XPT",
    "AAPL", "GOOGL", "MSFT", "NVDA", "META",
}
STABLECOINS = {
    "USDT", "USDC", "BUSD", "TUSD", "USDP", "FDUSD",
    "USDS", "USDE", "FRAX", "DAI", "PYUSD", "USD1",
}
_MULTIPLIER_RE = re.compile(r"^(\d+)(.*)")


def normalize_symbol(raw):
    """Convert a Binance-style instrument id (e.g. '1000PEPEUSDT', 'BTCUSDC')
    into the canonical base symbol ('PEPE', 'BTC'). Returns None if the
    symbol should be rejected (non-crypto, stablecoin, non-ASCII, unknown
    quote currency). Matches overlap_analysis.py.normalize_symbol byte-for-byte
    so baskets produced by v2 normalize identically to audit baskets."""
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
            s = s[:-len(quote)]
            break
    else:
        return None
    m = _MULTIPLIER_RE.match(s)
    if m:
        s = m.group(2)
    if not s:
        return None
    if s in NON_CRYPTO or s in STABLECOINS:
        return None
    return s


# ==========================================================================
# LOGGING
# ==========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("daily_signal_v2")


# ==========================================================================
# DB CONNECTION HELPER
# ==========================================================================

def _get_db_conn():
    """Import get_conn via the pipeline package. Prepends the script's parent
    directory so pipeline.db.connection resolves when v2 is invoked from cron
    with cwd=/root."""
    sys.path.insert(0, str(_SCRIPT_DIR))
    from pipeline.db.connection import get_conn
    return get_conn()


# ==========================================================================
# STEP 1 — BLOFIN UNIVERSE GATE (unchanged from v1)
# ==========================================================================

def get_blofin_symbols():
    """Fetch current BloFin USDT perp universe. Returns set of bare base
    symbols (e.g. {'BTC', 'ETH', ...}). Empty set on failure → no filter
    applied."""
    try:
        url = "https://openapi.blofin.com/api/v1/market/instruments"
        resp = requests.get(url, params={"instType": "SWAP"}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        syms = {
            inst["instId"].replace("-USDT", "").upper()
            for inst in (data.get("data") or [])
            if inst.get("instId", "").endswith("-USDT")
        }
        log.info(f"BloFin universe: {len(syms)} instruments")
        return syms
    except Exception as e:
        log.warning(f"BloFin gate unavailable: {e} -- no filtering applied")
        return set()


# ==========================================================================
# STEP 2 — CANONICAL BASKET (REST source, snapshot at ~05:55 UTC)
# ==========================================================================
#
# HISTORY: the original v2 (shipped at cutover 2026-04-23 05:58 UTC)
# read market.futures_1m to build the snapshot. That failed on its first
# live run — futures_1m is populated by the nightly metl cron which lags
# ~24h, so bars for TODAY do not exist at 05:58 UTC. v2 aborted with "No
# price leaderboard rows at 06:00 UTC", wrote an empty basket to CSV + DB
# for all 3 published strategies, and trader spawn at 06:05 UTC read zero
# symbols. Incident recorded at docs/strategy_specification.md §11.6.
#
# Replacement (this version, 2026-04-23 post-incident): fetch 5m klines +
# openInterestHist directly from Binance FAPI for all USDT perps. At cron
# time 05:58 UTC, the latest closed 5m bar is the one with open_time=05:55
# (close at 05:59:59). This is an off-canonical snapshot by ~1 second from
# the canonical 06:00 UTC definition — functionally identical for
# daily-frequency ranking (top-20 baskets are extremely stable on 5-min
# windows). Shifting cron to 06:02 UTC would give the exact 06:00 bar but
# requires a crontab coordination with trader spawn (06:05); deferred as a
# future refinement.
#
# The canonical methodology axes (unfiltered universe, pct_change ranking,
# normalize_symbol, top-20 intersection, BloFin filter applied AFTER
# ranking) are preserved exactly.
_BINANCE_FAPI   = "https://fapi.binance.com"

# Binance multiplier-prefix overrides for low-priced perps.
# market.symbols.binance_id is NULL for PEPE/SHIB and missing FLOKI/BONK
# entirely — the symbol_registry refresh has a bug that doesn't detect
# 1000-prefix mappings (PEPE listed as 1000PEPEUSDT, etc.). Hardcoded
# here until that registry refresh is patched. Same set the audit
# corrected via PR #6 (commit a204113), which fixed _load_mcap_from_db
# to map mcap rows by coin_id instead of naive base+USDT concat.
# Verify against: curl https://fapi.binance.com/fapi/v1/exchangeInfo
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
    in compute_dispersion_filter, where PEPE/SHIB/FLOKI/BONK were dropped
    despite ranking high in market.market_cap_daily."""
    return _BINANCE_TICKER_OVERRIDES.get(base, f"{base}USDT")
_OI_HIST_URL    = "https://fapi.binance.com/futures/data/openInterestHist"
_REST_WORKERS   = 8
_REST_TIMEOUT   = 10


def _fetch_all_futures_symbols():
    """Return list of TRADING USDT perpetual symbols on Binance futures."""
    r = requests.get(f"{_BINANCE_FAPI}/fapi/v1/exchangeInfo",
                     timeout=_REST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return [
        s["symbol"] for s in data.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("contractType") == "PERPETUAL"
        and s["symbol"].endswith("USDT")
    ]


def _fetch_symbol_snapshot(args):
    """Fetch 1m price klines + 5m OI history for one symbol, extract anchor
    close (first bar at start_ms) and snapshot close (latest CLOSED bar ≤
    end_ms). Returns (symbol, price_pct_change, oi_pct_change). Any failure
    or missing data maps to None so the symbol is silently dropped.

    Why 1m for price + 5m for OI: the audit reads market.leaderboards rows
    at timestamp=06:00, which were built by the indexer from market.futures_1m
    (1-minute bars). To exactly match the audit's snapshot bar, price must
    be the 1m bar at 06:00 UTC (closes at 06:00:59). Binance's
    openInterestHist endpoint only supports period=5m+ (no 1m OI from the
    public API), so OI uses the 5m bar at 05:55 UTC (closes at 05:59:59) —
    1 second off the audit's 06:00:59 reference, best-effort given the API
    constraint. Limit bumped to 380 for 1m klines to cover the 6h+ window.
    """
    symbol, start_ms, end_ms = args
    try:
        kr = requests.get(
            f"{_BINANCE_FAPI}/fapi/v1/klines",
            params={"symbol": symbol, "interval": "1m",
                    "startTime": start_ms, "endTime": end_ms, "limit": 380},
            timeout=_REST_TIMEOUT,
        )
        if kr.status_code != 200:
            return symbol, None, None
        klines = kr.json()
        if not klines:
            return symbol, None, None

        # Drop any in-progress bar from the tail so klines[-1] is always the
        # latest CLOSED bar regardless of when this script ran. Paired with
        # the 65s sleep at main() entry — baskets are now reproducible across
        # runs (was the wall-clock-non-determinism bug per
        # docs/open_work_list.md "Live trader basket — wall-clock non-determinism").
        now_ms = int(time.time() * 1000)
        BAR_MS_1M = 60 * 1000
        BAR_MS_5M = 5 * 60 * 1000
        while klines and (int(klines[-1][0]) + BAR_MS_1M > now_ms):
            klines.pop()
        if not klines:
            return symbol, None, None

        or_ = requests.get(
            _OI_HIST_URL,
            params={"symbol": symbol, "period": "5m",
                    "startTime": start_ms, "endTime": end_ms, "limit": 80},
            timeout=_REST_TIMEOUT,
        )
        oi_hist = or_.json() if or_.status_code == 200 else []
        while oi_hist and isinstance(oi_hist, list):
            try:
                tail_ts = int(oi_hist[-1].get("timestamp", 0))
                if tail_ts + BAR_MS_5M > now_ms:
                    oi_hist.pop()
                    continue
            except (KeyError, ValueError, TypeError, AttributeError):
                pass
            break

        # klines row: [open_time, open, high, low, close, volume, ...]
        anchor_close = float(klines[0][4])
        snap_close   = float(klines[-1][4])
        price_pct = ((snap_close / anchor_close) - 1) if anchor_close > 0 else None

        # OI item: {"sumOpenInterest": "...", "timestamp": ...}
        oi_pct = None
        if isinstance(oi_hist, list) and oi_hist:
            try:
                anchor_oi = float(oi_hist[0]["sumOpenInterest"])
                snap_oi   = float(oi_hist[-1]["sumOpenInterest"])
                if anchor_oi > 0:
                    oi_pct = (snap_oi / anchor_oi) - 1
            except (KeyError, ValueError, TypeError):
                oi_pct = None

        return symbol, price_pct, oi_pct
    except Exception:
        return symbol, None, None


def compute_canonical_basket(ref_date):
    """Compute top-20-by-pct_change for price and OI at today's 05:55 UTC
    snapshot (closest closed 5m bar before 06:00 UTC at cron time 05:58).
    Intersect them, apply normalize_symbol, return the canonical basket.

    Data source: Binance FAPI REST (`/fapi/v1/klines` + `/futures/data/
    openInterestHist`). Universe = all USDT perps with status=TRADING,
    contractType=PERPETUAL.

    Returns (basket_list, price_top_rows, oi_top_rows).
        basket_list     = sorted list of normalized base symbols
        price_top_rows  = [(binance_id, pct_change), ...]  (length ≤ 20)
        oi_top_rows     = [(binance_id, pct_change), ...]  (length ≤ 20)
    """
    from concurrent.futures import ThreadPoolExecutor

    anchor_ts = _dt.datetime.combine(ref_date, _dt.time(ANCHOR_HOUR, 0, 0),
                                     tzinfo=_dt.timezone.utc)
    target_snap = _dt.datetime.combine(ref_date,
                                       _dt.time(DEPLOYMENT_START_HOUR, 0, 0),
                                       tzinfo=_dt.timezone.utc)
    log.info(f"Computing canonical basket (REST source) "
             f"anchor={anchor_ts.isoformat()} "
             f"snapshot_target={target_snap.isoformat()}")

    start_ms = int(anchor_ts.timestamp() * 1000)
    # endTime on Binance is inclusive of open_time; add 5 min so if cron
    # runs at/after 06:00, the 06:00 bar (if closed) is returned.
    end_ms = int((target_snap + _dt.timedelta(minutes=5)).timestamp() * 1000)

    t0 = time.time()
    symbols = _fetch_all_futures_symbols()
    log.info(f"Binance futures universe: {len(symbols)} USDT perps")

    log.info(f"Fetching klines + OI for {len(symbols)} symbols "
             f"({_REST_WORKERS} workers)...")
    args_list = [(s, start_ms, end_ms) for s in symbols]
    results: list = []
    with ThreadPoolExecutor(max_workers=_REST_WORKERS) as pool:
        for res in pool.map(_fetch_symbol_snapshot, args_list):
            results.append(res)
    log.info(f"  Fetched in {time.time() - t0:.1f}s "
             f"(price={sum(1 for _, p, _ in results if p is not None)}, "
             f"oi={sum(1 for _, _, o in results if o is not None)})")

    price_ranks = sorted(
        [(s, p) for s, p, _ in results if p is not None],
        key=lambda x: x[1], reverse=True,
    )[:FREQ_WIDTH]
    oi_ranks = sorted(
        [(s, o) for s, _, o in results if o is not None],
        key=lambda x: x[1], reverse=True,
    )[:FREQ_WIDTH]

    if not price_ranks:
        log.error("No price data returned from REST — aborting basket.")
        return [], price_ranks, oi_ranks
    if not oi_ranks:
        log.warning("No OI data returned from REST — basket will be empty.")

    price_bases = {normalize_symbol(s) for s, _ in price_ranks}
    oi_bases    = {normalize_symbol(s) for s, _ in oi_ranks}
    price_bases.discard(None)
    oi_bases.discard(None)
    basket = sorted(price_bases & oi_bases)

    log.info(f"  Price top-{len(price_ranks)}: {[s for s, _ in price_ranks]}")
    log.info(f"  OI top-{len(oi_ranks)}:       {[s for s, _ in oi_ranks]}")
    log.info(f"  Intersection ({len(basket)}): {basket}")
    return basket, price_ranks, oi_ranks


# ==========================================================================
# STEP 3 — TAIL GUARDRAIL (BTC daily returns + rvol from futures_1m)
# ==========================================================================

def _fetch_btc_daily_closes(ref_date):
    """Pull BTC 1d closes for the trailing TAIL_VOL_LONG_WINDOW+5 days.

    Extracted from compute_tail_guardrail so the DB round-trip happens
    ONCE per daily_signal_v2 run even when multiple strategies (each with
    their own threshold config) compute Tail Guardrail independently.

    Returns a list of float closes in chronological order. Empty list if
    coverage is insufficient — caller should fail-closed.
    """
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("""
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
    """, (ref_date, TAIL_VOL_LONG_WINDOW + 6, ref_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [float(r[1]) for r in rows]


def compute_tail_guardrail(
    ref_date,
    closes: list | None = None,
    tail_drop_pct: float | None = None,
    tail_vol_mult: float | None = None,
):
    """Compute Tail Guardrail sit-flat decision for a given set of thresholds.

    Logic matches daily_signal.py (v1):
      - prev_day_return: BTC close at (ref_date-1) 23:59 / BTC close at
        (ref_date-1) 00:00 - 1. Fires if < -tail_drop_pct.
      - 5d rvol / 60d rvol ratio: std of daily log returns over the trailing
        TAIL_VOL_SHORT_WINDOW days divided by std over the trailing
        TAIL_VOL_LONG_WINDOW days. Fires if > tail_vol_mult.

    Either gate firing → sit flat. Returns (sit_flat: bool, reason: str|None).

    Thresholds fall through to the module-level defaults when the caller
    passes None; this preserves the existing canonical-CSV write path
    (main() uses the canonical strategy's thresholds by default). For
    per-strategy computation in write_to_db, pass the strategy's config
    values explicitly.

    `closes` may be pre-fetched (via _fetch_btc_daily_closes) when the
    caller is computing Tail Guardrail for multiple strategies in one
    invocation — saves the DB round-trip. When None, fetches on demand.
    """
    tdp = tail_drop_pct if tail_drop_pct is not None else TAIL_DROP_PCT
    tvm = tail_vol_mult if tail_vol_mult is not None else TAIL_VOL_MULT

    log.info(f"Computing Tail Guardrail (drop={tdp*100:.1f}%  vol_mult={tvm}x)...")

    if closes is None:
        closes = _fetch_btc_daily_closes(ref_date)

    if len(closes) < TAIL_VOL_LONG_WINDOW + 1:
        log.warning(f"Tail Guardrail needs ≥{TAIL_VOL_LONG_WINDOW+1} daily closes "
                    f"prior to {ref_date}; have {len(closes)}. Forcing sit-flat "
                    f"(fail-closed) to avoid trading under uncertain guardrail "
                    f"state.")
        return True, "tail_guardrail_insufficient_history"

    # Log returns (day N close / day N-1 close)
    log_rets = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            log_rets.append(math.log(closes[i] / closes[i-1]))

    # Prev-day return = last full day's log return (ref_date - 1)
    prev_day_logret = log_rets[-1] if log_rets else 0.0
    prev_day_ret = math.exp(prev_day_logret) - 1.0

    # Rvol ratio
    def _stdev(xs):
        if len(xs) < 2:
            return 0.0
        m = sum(xs) / len(xs)
        return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))

    rvol_5d  = _stdev(log_rets[-TAIL_VOL_SHORT_WINDOW:])
    rvol_60d = _stdev(log_rets[-TAIL_VOL_LONG_WINDOW:])
    ratio = (rvol_5d / rvol_60d) if rvol_60d > 0 else 0.0

    log.info(f"  BTC prev-day: {prev_day_ret*100:.2f}%  "
             f"5d rvol: {rvol_5d*100:.2f}%  "
             f"60d baseline: {rvol_60d*100:.2f}%  "
             f"ratio: {ratio:.3f}x  threshold: {tvm}x")

    crash_fires = prev_day_ret < -tdp
    vol_fires   = ratio > tvm

    if crash_fires and vol_fires:
        reason = (f"tail_guardrail_crash_and_vol: prev_day={prev_day_ret*100:.2f}% "
                  f"rvol_ratio={ratio:.3f}x")
        log.info(f"  Tail Guardrail: FIRE (both gates) -- SIT FLAT")
        return True, reason
    if crash_fires:
        reason = f"tail_guardrail_crash: prev_day={prev_day_ret*100:.2f}% < -{tdp*100:.1f}%"
        log.info(f"  Tail Guardrail: FIRE (crash gate) -- SIT FLAT")
        return True, reason
    if vol_fires:
        reason = f"tail_guardrail_vol: rvol_ratio={ratio:.3f}x > {tvm}x"
        log.info(f"  Tail Guardrail: FIRE (vol gate) -- SIT FLAT")
        return True, reason

    log.info(f"  Tail Guardrail: PASS -- both gates clear")
    return False, None


# ==========================================================================
# STEP 3b — DISPERSION FILTER (matches audit.py build_dispersion_filter)
# ==========================================================================
#
# Live implementation of the cross-sectional dispersion gate. Port of the
# audit's logic in pipeline/audit.py:2723 (build_dispersion_filter), adapted
# for live same-day evaluation via Binance REST klines.
#
# Semantic (lagged by 1 day):
#   yesterday_dispersion = std of yesterday's daily log-returns across the
#                          top-N mcap symbols
#   baseline             = rolling median over DISPERSION_BASELINE_WIN days
#                          of dispersion values, ending yesterday
#   disp_ratio           = yesterday_dispersion / baseline
#   sit_flat today       if disp_ratio < threshold
#
# Approximation vs audit: the audit uses per-day top-N mcap universe. Live
# uses today's top-N for the full baseline window (simpler + within a few
# symbols of per-day). Dominant effect is yesterday's dispersion value,
# which uses yesterday's data directly; the baseline median is more robust
# to symbol churn.
#
# Returns (sit_flat, reason) matching compute_tail_guardrail shape.


def compute_dispersion_filter(ref_date,
                              threshold: float | None = None,
                              baseline_win: int | None = None,
                              n_symbols: int | None = None):
    thr = threshold    if threshold    is not None else DISPERSION_THRESHOLD
    win = baseline_win if baseline_win is not None else DISPERSION_BASELINE_WIN
    n   = n_symbols    if n_symbols    is not None else DISPERSION_N

    log.info(f"Computing Dispersion filter (threshold={thr}, baseline_win={win}d, n={n})...")

    # 1. Today's top-N mcap symbols from DB (excludes stablecoins + non-ASCII)
    conn = _get_db_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT base
        FROM market.market_cap_daily
        WHERE date = %s
          AND base NOT IN ('USDT','USDC','BUSD','DAI','TUSD','FDUSD','USDE',
                           'USDS','PYUSD','USD1','FRAX','USDP')
          AND base !~ '[^A-Z0-9]'
          AND base NOT IN ('XAU','XAG','XPD','XPT')  -- non-crypto tickers
        ORDER BY market_cap_usd DESC
        LIMIT %s
    """, (ref_date, n))
    symbols = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    if len(symbols) < DISPERSION_MIN_SYMBOLS:
        log.warning(f"  Dispersion: only {len(symbols)} eligible mcap symbols "
                    f"for {ref_date} (< {DISPERSION_MIN_SYMBOLS} min). "
                    f"Fail-open (not firing).")
        return False, None

    # 2. Fetch (win + buffer) days of 1d klines per symbol from Binance FAPI
    from concurrent.futures import ThreadPoolExecutor
    fetch_days = win + 2
    end_dt    = _dt.datetime.combine(ref_date, _dt.time(0, 0, 0), tzinfo=_dt.timezone.utc)
    start_dt  = end_dt - _dt.timedelta(days=fetch_days)
    start_ms  = int(start_dt.timestamp() * 1000)
    end_ms    = int(end_dt.timestamp()   * 1000)

    def _fetch_daily(sym):
        ticker = _base_to_binance_ticker(sym)
        try:
            r = requests.get(
                f"{_BINANCE_FAPI}/fapi/v1/klines",
                params={"symbol": ticker, "interval": "1d",
                        "startTime": start_ms, "endTime": end_ms,
                        "limit": fetch_days + 5},
                timeout=_REST_TIMEOUT,
            )
            if r.status_code != 200:
                return sym, None
            klines = r.json()
            if not klines:
                return sym, None
            return sym, [(k[0], float(k[4])) for k in klines]  # (open_time_ms, close)
        except Exception:
            return sym, None

    log.info(f"  Fetching {fetch_days}d of 1d klines for {len(symbols)} symbols...")
    results: dict = {}
    with ThreadPoolExecutor(max_workers=_REST_WORKERS) as pool:
        for sym, series in pool.map(_fetch_daily, symbols):
            if series:
                results[sym] = series

    if len(results) < DISPERSION_MIN_SYMBOLS:
        log.warning(f"  Dispersion: only {len(results)}/{len(symbols)} symbols "
                    f"returned klines (< {DISPERSION_MIN_SYMBOLS} min). "
                    f"Fail-open.")
        return False, None

    # 3. Build per-symbol {date: close} then compute per-day cross-sectional
    #    std of log-returns.
    per_symbol_closes: dict = {}
    all_dates: set = set()
    for sym, series in results.items():
        d_map = {}
        for ts_ms, close in series:
            d = _dt.datetime.fromtimestamp(ts_ms / 1000,
                                           tz=_dt.timezone.utc).date()
            d_map[d] = close
            all_dates.add(d)
        per_symbol_closes[sym] = d_map

    sorted_dates = sorted(all_dates)
    daily_dispersion: dict = {}
    for i in range(1, len(sorted_dates)):
        d = sorted_dates[i]
        d_prev = sorted_dates[i - 1]
        rets = []
        for sym, d_map in per_symbol_closes.items():
            c = d_map.get(d)
            c_prev = d_map.get(d_prev)
            if c is not None and c_prev is not None and c_prev > 0 and c > 0:
                rets.append(math.log(c / c_prev))
        if len(rets) >= DISPERSION_MIN_SYMBOLS:
            m = sum(rets) / len(rets)
            var = sum((r - m) ** 2 for r in rets) / (len(rets) - 1)
            daily_dispersion[d] = math.sqrt(var)

    # 4. Yesterday's dispersion + baseline median
    yesterday = ref_date - _dt.timedelta(days=1)
    if yesterday not in daily_dispersion:
        log.warning(f"  Dispersion: no value for yesterday ({yesterday}). "
                    f"Fail-open.")
        return False, None

    window_start = yesterday - _dt.timedelta(days=win - 1)
    baseline_vals = sorted(v for d, v in daily_dispersion.items()
                           if window_start <= d <= yesterday)
    min_for_baseline = max(5, win // 2)
    if len(baseline_vals) < min_for_baseline:
        log.warning(f"  Dispersion: only {len(baseline_vals)} baseline values "
                    f"(need ≥ {min_for_baseline}). Fail-open.")
        return False, None

    n_b = len(baseline_vals)
    baseline = (baseline_vals[n_b // 2]
                if n_b % 2 == 1
                else (baseline_vals[n_b // 2 - 1] + baseline_vals[n_b // 2]) / 2)
    yesterday_disp = daily_dispersion[yesterday]
    disp_ratio = (yesterday_disp / baseline) if baseline > 0 else 0.0

    log.info(f"  Dispersion[{yesterday}]={yesterday_disp:.5f}  "
             f"baseline_median={baseline:.5f}  "
             f"ratio={disp_ratio:.3f}  threshold={thr}")

    if disp_ratio < thr:
        reason = f"dispersion_low: ratio={disp_ratio:.3f} < {thr}"
        log.info(f"  Dispersion: FIRE -- SIT FLAT")
        return True, reason

    log.info(f"  Dispersion: PASS")
    return False, None


# ==========================================================================
# STEP 4 — DEPLOYS CSV WRITER (parallel to v1's writer, different output file)
# ==========================================================================

def write_deploys_csv(date_str, filter_entries, overlap_pool):
    """Write/update live_deploys_signal.csv.

    `filter_entries` is a list of dicts, one per distinct filter_name used
    by any published strategy today:
        [{"filter_name": str, "sit_flat": bool, "filter_reason": str}, ...]

    Writes ONE row per entry (e.g. two rows on days where Alpha Main uses
    "Tail Guardrail" and ALTS MAIN uses "Tail + Dispersion"). The trader's
    get_today_symbols() matches on `filter` column against its config's
    active_filter — so multi-filter days need multi-row CSVs.

    Additive: preserves prior rows up to DEPLOYS_RETAIN_DAYS. Drops any
    rows with today's date before appending new ones (idempotent re-run).
    """
    fieldnames = ["date", "filter", "symbols", "sit_flat", "filter_reason"]

    # Load existing rows (if file exists) and drop the target date if already present
    existing = []
    if DEPLOYS_CSV.exists():
        try:
            with open(DEPLOYS_CSV, newline="") as f:
                reader = csv.DictReader(f)
                existing = [r for r in reader if r.get("date") != date_str]
        except Exception as e:
            log.warning(f"Could not read existing deploys CSV ({e}); starting fresh")
            existing = []

    # Prune rows older than DEPLOYS_RETAIN_DAYS
    cutoff = (_dt.date.today() - _dt.timedelta(days=DEPLOYS_RETAIN_DAYS)).isoformat()
    existing = [r for r in existing if r.get("date", "") >= cutoff]

    new_rows = []
    for entry in filter_entries:
        fn       = entry["filter_name"]
        sflat    = entry["sit_flat"]
        freason  = entry.get("filter_reason") or ("pass" if not sflat else "")
        new_rows.append({
            "date":          date_str,
            "filter":        fn,
            "symbols":       " ".join(overlap_pool) if overlap_pool and not sflat else "",
            "sit_flat":      "True" if sflat else "False",
            "filter_reason": freason,
        })
    rows = existing + new_rows
    rows.sort(key=lambda r: r["date"])

    with open(DEPLOYS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log.info(
        f"Deploys CSV v2 written -> {DEPLOYS_CSV}  "
        f"(kept {len(existing)} prior rows, pruned before {cutoff})"
    )
    for r in new_rows:
        log.info(f"  date={r['date']}  filter={r['filter']}  "
                 f"sit_flat={r['sit_flat']}  "
                 f"symbols={r['symbols'] or '(none)'}")


# ==========================================================================
# STEP 6 — WRITE TO DATABASE (ported verbatim from host-v1 / archive/)
#
# Writes one row per published+active strategy_version_id to
# user_mgmt.daily_signals + daily_signal_items. The trader at 06:05 UTC
# reads these rows (via allocation → strategy_version_id join); without
# them, allocations find no signal and sit flat.
#
# Source: archive/daily_signal_v1_host_snapshot_20260423.py lines 643-745
# (the production host version, which had this multi-version logic added
# on 2026-04-20 per commit e1522db but never propagated to the repo).
# Ported byte-for-byte during the 2026-04-23 cutover to preserve the
# exact DB-write behavior that the trader has been relying on.
# ==========================================================================

def fetch_published_strategy_configs() -> list:
    """Query all published+active strategy versions, return list of
    dicts: [{sv_id, display_name, config}]. Shared by main() (CSV
    build-up) and write_to_db (DB inserts) to avoid duplicated work.
    """
    sys.path.insert(0, str(_SCRIPT_DIR))
    from pipeline.db.connection import get_conn
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT sv.strategy_version_id::text, s.display_name, sv.config
            FROM audit.strategy_versions sv
            JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
            WHERE sv.is_active = TRUE AND s.is_published = TRUE
            ORDER BY sv.strategy_version_id
        """)
        out = [{"sv_id": r[0], "display_name": r[1], "config": (r[2] or {})}
               for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    return out


def compute_per_strategy_decisions(
    today,
    strategies: list,
    btc_closes: list,
    disp_decision: tuple,
    canonical_tg_decision: tuple,
    canonical_filter_reason: str,
) -> list:
    """For each strategy, compute the final (sit_flat, reason, filter_label)
    using the strategy's own TG thresholds + shared Disp decision, resolved
    via its active_filter.

    Returns list of dicts ready for both the CSV writer and the DB writer:
        [{sv_id, display_name, config, sit_flat, filter_reason,
          filter_name, tg_decision}]
    """
    out = []
    fallback = (canonical_tg_decision[0], canonical_filter_reason)
    for s in strategies:
        sv_id = s["sv_id"]
        cfg   = s["config"]
        name  = s["display_name"]

        # Per-strategy TG (fallback to canonical if thresholds missing)
        tg_decision = canonical_tg_decision
        sv_tdp = cfg.get("tail_drop_pct")
        sv_tvm = cfg.get("tail_vol_mult")
        if sv_tdp is not None and sv_tvm is not None:
            try:
                tg_decision = compute_tail_guardrail(
                    today, closes=btc_closes,
                    tail_drop_pct=float(sv_tdp),
                    tail_vol_mult=float(sv_tvm),
                )
            except (TypeError, ValueError):
                pass

        flat, reason, label = _resolve_strategy_filter(
            cfg, tg_decision, disp_decision, fallback=fallback,
        )
        out.append({
            "sv_id":          sv_id,
            "display_name":   name,
            "config":         cfg,
            "sit_flat":       flat,
            "filter_reason":  reason,
            "filter_name":    label,
            "tg_decision":    tg_decision,
        })
    return out


def _resolve_strategy_filter(
    cfg: dict,
    tg_decision: tuple,            # (sit_flat, reason) at strategy thresholds
    disp_decision: tuple,          # (sit_flat, reason), shared across strategies
    fallback: tuple,               # (sit_flat, reason) if active_filter unhandled
) -> tuple:
    """Combine per-strategy filter components into a single (sit_flat,
    reason, filter_name) decision based on the strategy's active_filter.

    Supported active_filters (matches audit.py's REGIME FILTER COMPARISON
    labels that live signal can realistically implement):
        "A - No Filter"           → never sit flat (trade every day)
        "A - Tail Guardrail"      → TG only
        "A - Dispersion"          → Dispersion only
        "A - Tail + Dispersion"   → TG OR Dispersion (either fires → flat)

    Unhandled filters (Calendar, Vol, Tail+Disp+Vol, etc.) fall back to
    caller's fallback decision — caller should pass TG-only as the safe
    default.
    """
    af = (cfg.get("active_filter") or "").strip()
    tg_flat, tg_reason = tg_decision
    dp_flat, dp_reason = disp_decision

    if af == "A - No Filter":
        return False, "pass", "No Filter"
    if af == "A - Tail Guardrail":
        return tg_flat, (tg_reason or "pass"), "Tail Guardrail"
    if af == "A - Dispersion":
        return dp_flat, (dp_reason or "pass"), "Dispersion"
    if af == "A - Tail + Dispersion":
        # EITHER fires → sit flat
        if tg_flat and dp_flat:
            reason = f"tail+disp_both: {tg_reason} | {dp_reason}"
        elif tg_flat:
            reason = f"tail+disp_tail_only: {tg_reason}"
        elif dp_flat:
            reason = f"tail+disp_disp_only: {dp_reason}"
        else:
            reason = "pass"
        return (tg_flat or dp_flat), reason, "Tail + Dispersion"
    # Unhandled — fall back to caller's decision
    fb_flat, fb_reason = fallback
    return fb_flat, (fb_reason or "pass"), "Tail Guardrail"


def write_to_db(date_str, filter_name, overlap_pool, sit_flat, filter_reason,
                btc_closes: list | None = None, today=None,
                disp_decision: tuple | None = None):
    """Insert today's signal into user_mgmt.daily_signals + daily_signal_items.

    Writes one row per published+active strategy_version_id. Each strategy's
    Tail Guardrail decision is computed using THAT strategy's own
    tail_drop_pct / tail_vol_mult (read from `audit.strategy_versions.config`)
    — critical for correctness when strategies run with divergent thresholds
    (e.g. ALTS MAIN's 0.03 vs Alpha Main's 0.04). The caller-supplied
    `sit_flat` + `filter_reason` are used as FALLBACKS when a strategy's
    config doesn't specify thresholds or when BTC closes can't be fetched.

    `btc_closes` avoids re-fetching from DB when main() already has the
    series in hand. Pass today=ref_date (datetime.date) to enable per-
    strategy compute; omit both to use the caller's sit_flat across all
    strategies (legacy behavior).

    A DB failure is logged but never fatal (CSV write already succeeded).
    """
    try:
        sys.path.insert(0, str(_SCRIPT_DIR))
        from pipeline.db.connection import get_conn

        conn = get_conn()
        cur = conn.cursor()

        # Query all published + active strategy versions WITH their config
        # JSONB so we can pull per-strategy tail_drop_pct / tail_vol_mult.
        # Fallback to secrets.env STRATEGY_VERSION_ID if the query returns
        # nothing (defensive — should never fire in production).
        cur.execute("""
            SELECT sv.strategy_version_id::text,
                   sv.config,
                   s.display_name
            FROM audit.strategy_versions sv
            JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
            WHERE sv.is_active = TRUE AND s.is_published = TRUE
            ORDER BY sv.strategy_version_id
        """)
        version_rows = cur.fetchall()
        version_ids = [r[0] for r in version_rows]
        version_configs = {r[0]: (r[1] or {}) for r in version_rows}
        version_names   = {r[0]: r[2] for r in version_rows}

        if not version_ids:
            # Legacy fallback: read single STRATEGY_VERSION_ID from secrets.env.
            secrets_path = Path("/mnt/quant-data/credentials/secrets.env")
            if secrets_path.exists():
                for line in secrets_path.read_text().splitlines():
                    line = line.strip()
                    if line.startswith("STRATEGY_VERSION_ID="):
                        version_ids = [line.split("=", 1)[1].strip()]
                        break
            if not version_ids:
                log.warning("No published+active strategy versions and no "
                            "STRATEGY_VERSION_ID fallback — skipping DB write")
                conn.close()
                return

        # Look up symbol_ids once (shared across all strategy versions).
        sym_map = {}
        if overlap_pool:
            cur.execute(
                "SELECT base, symbol_id FROM market.symbols WHERE base = ANY(%s)",
                ([sym for sym in overlap_pool],)
            )
            sym_map = dict(cur.fetchall())

        from psycopg2.extras import execute_values

        # Default Dispersion decision when caller didn't compute it (e.g.
        # legacy caller path that predates the filter). Treated as
        # "don't fire" — safe fallback for strategies whose active_filter
        # references Dispersion; the _resolve_strategy_filter function
        # handles the absent-Dispersion case by defaulting to Tail-only.
        disp_fallback = disp_decision if disp_decision is not None else (False, None)

        rows_written = 0
        for sv_id in version_ids:
            cfg = version_configs.get(sv_id, {})
            sv_name = version_names.get(sv_id, sv_id[:8])

            # Per-strategy Tail Guardrail decision (each strategy's own
            # tail_drop_pct / tail_vol_mult from config JSONB). Falls back
            # to caller decision when per-strategy inputs are missing.
            tg_decision = (sit_flat, filter_reason)
            sv_tdp = cfg.get("tail_drop_pct")
            sv_tvm = cfg.get("tail_vol_mult")
            if (today is not None and btc_closes is not None
                    and sv_tdp is not None and sv_tvm is not None):
                try:
                    tg_decision = compute_tail_guardrail(
                        today, closes=btc_closes,
                        tail_drop_pct=float(sv_tdp),
                        tail_vol_mult=float(sv_tvm),
                    )
                    log.info(f"  [per-strategy TG] {sv_name}: "
                             f"drop={float(sv_tdp)*100:.1f}% "
                             f"vol_mult={float(sv_tvm)}x "
                             f"→ sit_flat={tg_decision[0]}")
                except (TypeError, ValueError) as e:
                    log.warning(f"  [per-strategy TG] {sv_name}: invalid "
                                f"thresholds ({e}); fallback to caller")

            # Resolve per-strategy active_filter into final sit_flat decision
            sv_sit_flat, sv_reason, sv_filter_label = _resolve_strategy_filter(
                cfg, tg_decision, disp_fallback, fallback=(sit_flat, filter_reason),
            )
            log.info(f"  [{sv_name}] active_filter={cfg.get('active_filter')!r} "
                     f"→ sit_flat={sv_sit_flat}  reason={sv_reason!r}")

            # filter_name column tracks the resolved label (maps 1:1 to the
            # trader's `active_filter` match path). For canonical strategies
            # this stays "Tail Guardrail"; ALTS MAIN writes "Tail + Dispersion".
            sv_filter_name = sv_filter_label

            cur.execute("""
                INSERT INTO user_mgmt.daily_signals
                    (signal_date, strategy_version_id, computed_at,
                     sit_flat, filter_name, filter_reason)
                VALUES (%s, %s, NOW(), %s, %s, %s)
                ON CONFLICT (signal_date, strategy_version_id) DO NOTHING
                RETURNING signal_batch_id
            """, (date_str, sv_id, sv_sit_flat, sv_filter_name, sv_reason))

            row = cur.fetchone()
            if row is None:
                log.info(f"DB write: signal already exists for {sv_id[:8]} "
                         f"on {date_str} — skipped")
                continue

            signal_batch_id = row[0]
            rows_written += 1

            if overlap_pool and not sv_sit_flat and sym_map:
                item_rows = []
                for rank, sym in enumerate(overlap_pool, start=1):
                    sid = sym_map.get(sym)
                    if sid is None:
                        continue
                    item_rows.append((signal_batch_id, sid, rank, None, True))

                if item_rows:
                    execute_values(
                        cur,
                        """INSERT INTO user_mgmt.daily_signal_items
                               (signal_batch_id, symbol_id, rank, weight, is_selected)
                           VALUES %s
                           ON CONFLICT DO NOTHING""",
                        item_rows,
                    )

        conn.commit()
        cur.close()
        conn.close()
        log.info(f"DB write: {rows_written}/{len(version_ids)} strategy versions, "
                 f"{len(overlap_pool)} symbols")

    except Exception as e:
        log.warning(f"DB write failed (non-fatal): {e}")


# ==========================================================================
# MAIN
# ==========================================================================

def main():
    t_start = time.time()
    # Sleep at script start so the 06:00 UTC 1m bar is fully closed before
    # we fetch klines. Cron fires at 06:00:00 UTC; the 1m bar at 06:00 closes
    # at 06:00:59 UTC. 65s sleep lands us at ~06:01:05 — past 06:00:59 close
    # with a small buffer for clock skew + Binance API latency. The
    # _fetch_symbol_snapshot function below uses interval="1m" + closed-bar
    # filter so klines[-1] is always the closed 06:00 1m bar, exactly
    # matching the audit's market.leaderboards reference at timestamp=06:00.
    # Discovered 2026-04-25: prior 05:58 cron fired before the 06:00 bar
    # opened, so klines[-1] returned the in-progress 05:55-05:59 5m bar.
    # Two runs minutes apart produced different baskets (MAGIC vs SAND swap)
    # because the in-progress bar's close moved with wall-clock. See
    # docs/open_work_list.md "Live trader basket — wall-clock non-determinism".
    time.sleep(65)
    today = _dt.datetime.now(_dt.timezone.utc).date()
    log.info("=" * 65)
    log.info(f"  DAILY SIGNAL v2 -- {today} (canonical methodology, LIVE)")
    log.info("=" * 65)

    # 1. BloFin universe
    blofin = get_blofin_symbols()

    # 2. Canonical basket from 06:00 UTC snapshot
    basket, _price_top, _oi_top = compute_canonical_basket(today)

    # 3. BloFin filter (exchange availability)
    if blofin:
        basket_pre_filter = list(basket)
        basket = [s for s in basket if s in blofin]
        dropped = sorted(set(basket_pre_filter) - set(basket))
        if dropped:
            log.info(f"BloFin filter dropped {len(dropped)} symbols: {dropped}")

    # 4a. Tail Guardrail. Fetch BTC closes once, compute with canonical
    #     defaults for the CSV writer + pass closes to write_to_db so
    #     each strategy evaluates with its OWN config thresholds (critical
    #     when ALTS MAIN's 0.03 tail_drop_pct coexists with Alpha Main's
    #     0.04).
    btc_closes = _fetch_btc_daily_closes(today)
    sit_flat, tg_reason = compute_tail_guardrail(today, closes=btc_closes)

    # 4b. Dispersion filter. Computed ONCE (shared across strategies since
    #     DISPERSION_THRESHOLD / DISPERSION_N / DISPERSION_BASELINE_WIN are
    #     currently uniform across published strategy configs; per-strategy
    #     override support can be added later by moving the call inside the
    #     write_to_db loop). Result passed to write_to_db so strategies
    #     whose active_filter includes Dispersion can combine it with Tail.
    disp_flat, disp_reason = compute_dispersion_filter(today)

    # 5. Final decision for CSV — CSV writer uses the CANONICAL strategy's
    #    methodology (Alpha Main = Tail Guardrail only). The DB write path
    #    handles per-strategy combinations.
    final_basket = [] if sit_flat else basket
    if sit_flat:
        log.info(f"SIT FLAT TODAY — canonical Tail Guardrail fired (reason: {tg_reason})")
    else:
        log.info(f"TRADE TODAY — canonical TG clear, basket: {final_basket}")
    if disp_flat:
        log.info(f"[Dispersion status] FIRED — will sit out strategies with active_filter=Dispersion/Tail+Dispersion  (reason: {disp_reason})")
    else:
        log.info(f"[Dispersion status] clear")

    # 6. Pre-compute per-strategy decisions (used by both CSV writer and DB
    #    writer). This resolves each published strategy's active_filter into
    #    a final (sit_flat, reason, filter_label) using that strategy's own
    #    TG thresholds + the shared Dispersion decision.
    filter_reason = tg_reason or ("pass" if not sit_flat else "")
    strategies   = fetch_published_strategy_configs()
    decisions    = compute_per_strategy_decisions(
        today, strategies, btc_closes,
        disp_decision=(disp_flat, disp_reason),
        canonical_tg_decision=(sit_flat, tg_reason),
        canonical_filter_reason=filter_reason,
    )

    # Build filter_entries list, deduped by filter_name. Each unique filter
    # label gets one CSV row; the trader matches CSV rows to its config's
    # active_filter by label. If two strategies share a label but disagree
    # on sit_flat (can't happen with current semantics — same inputs), the
    # first wins.
    seen = {}
    for d in decisions:
        fn = d["filter_name"]
        if fn not in seen:
            seen[fn] = {
                "filter_name":   fn,
                "sit_flat":      d["sit_flat"],
                "filter_reason": d["filter_reason"],
            }
    filter_entries = list(seen.values())

    # 7. Write CSV — one row per unique filter_name. Multi-row on days where
    #    published strategies disagree (e.g. Alpha Main's Tail Guardrail +
    #    ALTS MAIN's Tail + Dispersion both resolve independently). Pass
    #    the raw (post-BloFin) basket rather than final_basket; each row's
    #    sit_flat controls whether symbols are emitted for that filter.
    write_deploys_csv(today.isoformat(), filter_entries, basket)

    # 8. Write DB: one row per published+active strategy_version_id.
    #    Trader spawn at 06:05 UTC reads these — without them, allocations
    #    find no signal and sit flat. Per-strategy sit_flat computed inside
    #    write_to_db by combining TG + Dispersion per active_filter.
    write_to_db(today.isoformat(), FILTER_NAME, final_basket,
                sit_flat, filter_reason,
                btc_closes=btc_closes, today=today,
                disp_decision=(disp_flat, disp_reason))

    elapsed = time.time() - t_start
    log.info(f"Done. ({elapsed:.1f}s) — v2 LIVE at 05:58 UTC (post-cutover 2026-04-23)")
    log.info("=" * 65)


if __name__ == "__main__":
    main()

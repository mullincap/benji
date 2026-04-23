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
# STEP 2 — CANONICAL BASKET (snapshot at 06:00 UTC, on-the-fly from futures_1m)
# ==========================================================================

def compute_canonical_basket(ref_date):
    """Compute top-20-by-pct_change for price and OI at the 06:00 UTC snapshot
    on ref_date, intersect them, and return the normalized basket.

    Formula + hygiene exactly mirror build_intraday_leaderboard.py:
        anchor_val  = futures_1m.{col} at ref_date 00:00 UTC
        pct_change  = futures_1m.{col} at ref_date 06:00 UTC / anchor_val - 1
        drop rows where anchor_val == 0 or NULL     (builder line ~1043)
        rank by pct_change DESC
        keep top FREQ_WIDTH (=20) per metric

    Returns (basket_list, price_top_rows, oi_top_rows).
        basket_list     = sorted list of normalized base symbols (canonical basket)
        price_top_rows  = [(binance_id, pct_change), ...]  (length ≤ 20)
        oi_top_rows     = [(binance_id, pct_change), ...]  (length ≤ 20)
    """
    anchor_ts = _dt.datetime.combine(ref_date, _dt.time(ANCHOR_HOUR, 0, 0),
                                     tzinfo=_dt.timezone.utc)
    snapshot_ts = _dt.datetime.combine(ref_date, _dt.time(DEPLOYMENT_START_HOUR, 0, 0),
                                       tzinfo=_dt.timezone.utc)
    log.info(f"Computing canonical basket anchor={anchor_ts.isoformat()} "
             f"snapshot={snapshot_ts.isoformat()}")

    conn = _get_db_conn()
    cur = conn.cursor()

    def _top_n(metric_col, n=FREQ_WIDTH):
        # Self-join futures_1m at the two target timestamps, filter symbols
        # with valid anchor (>0), compute pct_change, rank DESC, take top-n.
        cur.execute(f"""
            SELECT sym.binance_id,
                   (n.{metric_col} / a.{metric_col}) - 1 AS pct_change
            FROM market.futures_1m a
            JOIN market.futures_1m n
              ON a.symbol_id = n.symbol_id
            JOIN market.symbols sym ON sym.symbol_id = a.symbol_id
            WHERE a.timestamp_utc = %s
              AND n.timestamp_utc = %s
              AND a.{metric_col} IS NOT NULL AND a.{metric_col} > 0
              AND n.{metric_col} IS NOT NULL
            ORDER BY pct_change DESC NULLS LAST
            LIMIT %s
        """, (anchor_ts, snapshot_ts, n))
        return [(r[0], float(r[1]) if r[1] is not None else None)
                for r in cur.fetchall()]

    price_top = _top_n("close")
    oi_top    = _top_n("open_interest")

    cur.close()
    conn.close()

    if not price_top:
        log.error("No price leaderboard rows at 06:00 UTC — DB may be missing "
                  "today's bar data. Aborting basket computation.")
        return [], price_top, oi_top
    if not oi_top:
        log.warning("No OI leaderboard rows at 06:00 UTC — basket will be empty.")

    # Normalize and intersect
    price_bases = {normalize_symbol(bid) for bid, _ in price_top}
    oi_bases    = {normalize_symbol(bid) for bid, _ in oi_top}
    price_bases.discard(None)
    oi_bases.discard(None)
    basket = sorted(price_bases & oi_bases)

    log.info(f"  Price top-{len(price_top)}: {[b for b, _ in price_top]}")
    log.info(f"  OI top-{len(oi_top)}:       {[b for b, _ in oi_top]}")
    log.info(f"  Intersection ({len(basket)}): {basket}")
    return basket, price_top, oi_top


# ==========================================================================
# STEP 3 — TAIL GUARDRAIL (BTC daily returns + rvol from futures_1m)
# ==========================================================================

def compute_tail_guardrail(ref_date):
    """Compute Tail Guardrail sit-flat decision from BTC 1-min bars in DB.

    Logic matches daily_signal.py (v1):
      - prev_day_return: BTC close at (ref_date-1) 23:59 / BTC close at
        (ref_date-1) 00:00 - 1. Fires if < -TAIL_DROP_PCT (=-4%).
      - 5d rvol / 60d rvol ratio: std of daily log returns over the trailing
        TAIL_VOL_SHORT_WINDOW days divided by std over the trailing
        TAIL_VOL_LONG_WINDOW days. Fires if > TAIL_VOL_MULT (=1.4x).

    Either gate firing → sit flat. Returns (sit_flat: bool, reason: str|None).
    """
    log.info("Computing Tail Guardrail (BTC prev-day return + 5d/60d rvol)...")
    conn = _get_db_conn()
    cur = conn.cursor()

    # Pull BTC daily closes for the last TAIL_VOL_LONG_WINDOW + 5 days.
    # Use the last bar of each UTC day (23:59 close, or latest before midnight).
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

    if len(rows) < TAIL_VOL_LONG_WINDOW + 1:
        log.warning(f"Tail Guardrail needs ≥{TAIL_VOL_LONG_WINDOW+1} daily closes "
                    f"prior to {ref_date}; have {len(rows)}. Forcing sit-flat "
                    f"(fail-closed) to avoid trading under uncertain guardrail "
                    f"state.")
        return True, "tail_guardrail_insufficient_history"

    closes = [float(r[1]) for r in rows]

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
             f"ratio: {ratio:.3f}x  threshold: {TAIL_VOL_MULT}x")

    crash_fires = prev_day_ret < -TAIL_DROP_PCT
    vol_fires   = ratio > TAIL_VOL_MULT

    if crash_fires and vol_fires:
        reason = (f"tail_guardrail_crash_and_vol: prev_day={prev_day_ret*100:.2f}% "
                  f"rvol_ratio={ratio:.3f}x")
        log.info(f"  Tail Guardrail: FIRE (both gates) -- SIT FLAT")
        return True, reason
    if crash_fires:
        reason = f"tail_guardrail_crash: prev_day={prev_day_ret*100:.2f}% < -{TAIL_DROP_PCT*100:.0f}%"
        log.info(f"  Tail Guardrail: FIRE (crash gate) -- SIT FLAT")
        return True, reason
    if vol_fires:
        reason = f"tail_guardrail_vol: rvol_ratio={ratio:.3f}x > {TAIL_VOL_MULT}x"
        log.info(f"  Tail Guardrail: FIRE (vol gate) -- SIT FLAT")
        return True, reason

    log.info(f"  Tail Guardrail: PASS -- both gates clear")
    return False, None


# ==========================================================================
# STEP 4 — DEPLOYS CSV WRITER (parallel to v1's writer, different output file)
# ==========================================================================

def write_deploys_csv(date_str, filter_name, overlap_pool, sit_flat, sit_flat_reason):
    """Write/update live_deploys_signal_v2.csv. Additive: preserves prior rows
    up to DEPLOYS_RETAIN_DAYS. Format matches live_deploys_signal.csv (v1)
    exactly (column names + order + sit_flat capitalization) so v2 is a
    drop-in replacement at cutover time."""
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

    # Add today's row (column names + order match v1 for drop-in cutover)
    new_row = {
        "date":          date_str,
        "filter":        filter_name,
        "symbols":       " ".join(overlap_pool) if overlap_pool and not sit_flat else "",
        "sit_flat":      "True" if sit_flat else "False",
        "filter_reason": sit_flat_reason or ("pass" if not sit_flat else ""),
    }
    rows = existing + [new_row]
    rows.sort(key=lambda r: r["date"])

    with open(DEPLOYS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log.info(
        f"Deploys CSV v2 written -> {DEPLOYS_CSV}  "
        f"(kept {len(existing)} prior rows, pruned before {cutoff})\n"
        f"  date={date_str}  filter={filter_name}  "
        f"sit_flat={sit_flat}  symbols={new_row['symbols'] or '(none)'}"
    )


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

def write_to_db(date_str, filter_name, overlap_pool, sit_flat, filter_reason):
    """Insert today's signal into user_mgmt.daily_signals + daily_signal_items.

    Writes one row per published+active strategy_version_id (Alpha Low / Main /
    Max). Alpha variants are derivative of the shared filter — they use the
    same overlap_pool / sit_flat / filter_name but need their own signal rows
    so allocation-keyed queries (manager overview, trader subprocesses) find
    signals matching their strategy_version_id.

    A DB failure is logged but never fatal (CSV write already succeeded).
    """
    try:
        sys.path.insert(0, str(_SCRIPT_DIR))
        from pipeline.db.connection import get_conn

        conn = get_conn()
        cur = conn.cursor()

        # Query all published + active strategy versions. One signal row per
        # version. Fallback to secrets.env STRATEGY_VERSION_ID if the query
        # returns nothing (defensive — should never fire in production).
        cur.execute("""
            SELECT sv.strategy_version_id::text
            FROM audit.strategy_versions sv
            JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
            WHERE sv.is_active = TRUE AND s.is_published = TRUE
            ORDER BY sv.strategy_version_id
        """)
        version_ids = [r[0] for r in cur.fetchall()]

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
        if overlap_pool and not sit_flat:
            cur.execute(
                "SELECT base, symbol_id FROM market.symbols WHERE base = ANY(%s)",
                ([sym for sym in overlap_pool],)
            )
            sym_map = dict(cur.fetchall())

        from psycopg2.extras import execute_values

        rows_written = 0
        for sv_id in version_ids:
            cur.execute("""
                INSERT INTO user_mgmt.daily_signals
                    (signal_date, strategy_version_id, computed_at,
                     sit_flat, filter_name, filter_reason)
                VALUES (%s, %s, NOW(), %s, %s, %s)
                ON CONFLICT (signal_date, strategy_version_id) DO NOTHING
                RETURNING signal_batch_id
            """, (date_str, sv_id, sit_flat, filter_name, filter_reason))

            row = cur.fetchone()
            if row is None:
                log.info(f"DB write: signal already exists for {sv_id[:8]} "
                         f"on {date_str} — skipped")
                continue

            signal_batch_id = row[0]
            rows_written += 1

            if overlap_pool and not sit_flat and sym_map:
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

    # 4. Tail Guardrail
    sit_flat, tg_reason = compute_tail_guardrail(today)

    # 5. Final decision
    final_basket = [] if sit_flat else basket
    if sit_flat:
        log.info(f"SIT FLAT TODAY (reason: {tg_reason})")
    else:
        log.info(f"TRADE TODAY -- {len(final_basket)} symbols: {final_basket}")

    # 6. Write CSV (the trader-blofin executable path still reads this file;
    #    allocator-spawned traders read the DB rows written in step 7).
    filter_reason = tg_reason or ("pass" if not sit_flat else "")
    write_deploys_csv(today.isoformat(), FILTER_NAME, final_basket,
                      sit_flat, filter_reason)

    # 7. Write DB: one row per published+active strategy_version_id.
    #    Trader spawn at 06:05 UTC reads these — without them, allocations
    #    find no signal and sit flat.
    write_to_db(today.isoformat(), FILTER_NAME, final_basket,
                sit_flat, filter_reason)

    elapsed = time.time() - t_start
    log.info(f"Done. ({elapsed:.1f}s) — v2 LIVE at 05:58 UTC (post-cutover 2026-04-23)")
    log.info("=" * 65)


if __name__ == "__main__":
    main()

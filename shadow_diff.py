#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shadow_diff.py
==============
Daily basket diff for the daily_signal v1 → v2 migration shadow period.

Reads three same-day baskets and logs their pairwise overlap + Jaccard
distances. Also logs a rolling 7-day mean of J(v2, canonical) — the
cutover gate metric. Standalone (no v2 import); mirrors the canonical
computation independently for failure-isolation from v2.

Three basket sources per day:
  1. v1  — live_deploys_signal.csv       (what live money actually traded)
  2. v2  — live_deploys_signal_v2.csv    (what v2 would trade if wired)
  3. CAN — canonical basket computed on-the-fly from market.futures_1m
           (= what the audit would select; ground-truth reference)

Three pairwise Jaccards per day:
  - J(v1, CAN): baseline of how far legacy methodology drifts from
                canonical. Expected low (~0.6-0.7 per 2026-04-13/04-16
                observations). Informational, not a gate.
  - J(v2, CAN): CUTOVER GATE. Should be ≈1.0 every day since v2 uses
                the same math as CAN. Rolling 7-day mean ≥ 0.95 for ≥7
                consecutive days is the cutover precondition.
  - J(v1, v2):  informational. Shows how much v1 drifts from v2 daily.

Cross-check (lagged 1 day): also log J(v2_yesterday, leaderboard_DB_yesterday)
where leaderboard_DB_yesterday is a direct query against
market.leaderboards (which only contains day D-1 at 06:15 UTC on day D —
the nightly indexer builds D-1 at 01:00 UTC on day D). If this daily
cross-check ever drifts from 1.0, v2's on-the-fly computation has
diverged from the builder's output, which is a bug.

Outputs:
  - daily_signal_shadow_diff.log       (human-readable append log)
  - daily_signal_shadow_diff_history.csv  (rolling Jaccard history for
                                           cutover-gate computation)

Cron entry:
  15 6 * * *  /usr/bin/python3 /root/benji/shadow_diff.py >> \\
              /root/benji/daily_signal_shadow_diff.log 2>&1

Runs after v1 (05:58 UTC) and v2 (06:02 UTC) have both written their
CSVs for the day. 06:15 UTC chosen to leave a buffer.

Read-only. Never triggers trades, mutates v1/v2 CSVs, or modifies the
leaderboards DB. Safe to run any time.
"""

import csv
import datetime as _dt
import functools
import logging
import re
import sys
from pathlib import Path

import requests

# ==========================================================================
# CONFIG
# ==========================================================================

DEPLOYMENT_START_HOUR = 6
ANCHOR_HOUR           = 0
FREQ_WIDTH            = 20

# Rolling cutover-gate window
CUTOVER_JACCARD_MIN    = 0.95   # minimum J(v2, CAN) to count a day as "pass"
CUTOVER_CONSEC_DAYS    = 7      # consecutive pass-days required for cutover

_SCRIPT_DIR          = Path(__file__).resolve().parent
V1_CSV               = _SCRIPT_DIR / "live_deploys_signal.csv"
V2_CSV               = _SCRIPT_DIR / "live_deploys_signal_v2.csv"
LOG_FILE             = _SCRIPT_DIR / "daily_signal_shadow_diff.log"
HISTORY_CSV          = _SCRIPT_DIR / "daily_signal_shadow_diff_history.csv"


# ==========================================================================
# NORMALIZATION (verbatim from pipeline/overlap_analysis.py + daily_signal_v2)
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
    """Binance-style instrument id → canonical base. Matches overlap_analysis.py
    and daily_signal_v2 byte-for-byte so all three basket sources normalize
    the same way."""
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
log = logging.getLogger("shadow_diff")


# ==========================================================================
# DB HELPER
# ==========================================================================

def _get_db_conn():
    sys.path.insert(0, str(_SCRIPT_DIR))
    from pipeline.db.connection import get_conn
    return get_conn()


# ==========================================================================
# BLOFIN FETCH (cached per-process — mirrors daily_signal_v2 logic)
# ==========================================================================

@functools.lru_cache(maxsize=1)
def get_blofin_symbols():
    """Fetch current BloFin USDT-perp instrument list. Shadow-diff applies
    the same filter v2 applies so canonical baskets in shadow_diff are
    directly comparable to v2 output (post-BloFin-filter both sides).

    Uses `requests` (not urllib) to match daily_signal_v2's UA — BloFin
    returns HTTP 403 to urllib's default UA.

    Returns set of bare base symbols. Empty set on failure → no filter."""
    url = "https://openapi.blofin.com/api/v1/market/instruments"
    try:
        resp = requests.get(url, params={"instType": "SWAP"}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        syms = {
            inst["instId"].replace("-USDT", "").upper()
            for inst in (data.get("data") or [])
            if inst.get("instId", "").endswith("-USDT")
        }
        log.info(f"BloFin universe: {len(syms)} USDT-swap instruments "
                 "(applied to canonical baskets for apples-to-apples J with v2)")
        return syms
    except Exception as e:
        log.warning(f"BloFin fetch failed ({e}); canonical baskets will NOT be "
                    "BloFin-filtered — Jaccards vs v2 may under-report parity.")
        return set()


def _apply_blofin(basket, blofin):
    """Drop symbols not in BloFin. If blofin is empty (fetch failed), no filter."""
    if not blofin or basket is None:
        return basket
    return [s for s in basket if s in blofin]


# ==========================================================================
# CSV BASKET READER
# ==========================================================================

def csv_basket_for_date(csv_path, date_str):
    """Return the normalized basket for `date_str` from a deploys CSV.

    Handles both v1's format (date, filter, symbols, sit_flat, filter_reason)
    and v2's matching format (same columns). Returns None if the row is
    missing, [] if the row exists but was sit-flat.
    """
    if not csv_path.exists():
        return None
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("date") != date_str:
                    continue
                # v1 uses "True"/"False"; v2 matches
                if str(row.get("sit_flat", "False")).lower() == "true":
                    return []
                raw_syms = (row.get("symbols") or "").strip()
                if not raw_syms:
                    return []
                # v1 + v2 write space-separated bare symbols (already normalized
                # to base). But in case someone wrote a full binance_id, we
                # normalize defensively.
                syms = []
                for tok in raw_syms.split():
                    tok = tok.strip().upper()
                    if not tok:
                        continue
                    # If it looks like a bare base already (no known quote suffix),
                    # accept as-is; else try normalize_symbol.
                    if any(tok.endswith(q) for q in ("USDT", "USDC", "_PERP")):
                        norm = normalize_symbol(tok)
                        if norm:
                            syms.append(norm)
                    else:
                        syms.append(tok)
                return sorted(set(syms))
    except Exception as e:
        log.warning(f"Error reading {csv_path} for {date_str}: {e}")
        return None
    return None


# ==========================================================================
# CANONICAL BASKET (on-the-fly from market.futures_1m)
# ==========================================================================

def compute_canonical_from_futures_1m(ref_date, blofin=None):
    """Same algorithm as daily_signal_v2.compute_canonical_basket. Returns a
    tuple (basket_post_blofin, basket_pre_blofin). When `blofin` is None or
    empty, both elements of the tuple are identical (no filter applied).

    The post-filter basket is what shadow_diff compares against v2's CSV
    output (both sides post-filter → apples-to-apples). The pre-filter
    basket is also returned so we can log the true-canonical intersection
    as an informational metric."""
    anchor_ts = _dt.datetime.combine(ref_date, _dt.time(ANCHOR_HOUR, 0, 0),
                                     tzinfo=_dt.timezone.utc)
    snapshot_ts = _dt.datetime.combine(ref_date, _dt.time(DEPLOYMENT_START_HOUR, 0, 0),
                                       tzinfo=_dt.timezone.utc)
    conn = _get_db_conn()
    cur = conn.cursor()

    def _top(metric_col):
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
        """, (anchor_ts, snapshot_ts, FREQ_WIDTH))
        return [r[0] for r in cur.fetchall()]

    price_top = _top("close")
    oi_top    = _top("open_interest")
    cur.close()
    conn.close()

    if not price_top or not oi_top:
        return (None, None)

    p = {normalize_symbol(b) for b in price_top} - {None}
    o = {normalize_symbol(b) for b in oi_top}    - {None}
    pre_filter = sorted(p & o)
    post_filter = sorted(_apply_blofin(pre_filter, blofin)) if blofin else list(pre_filter)
    return (post_filter, pre_filter)


def compute_canonical_from_leaderboards(ref_date, blofin=None):
    """Cross-check: pull the basket directly from market.leaderboards for
    ref_date. Only populated after the nightly indexer cron runs (01:00 UTC
    on day ref_date+1) — so when called for 'today' at 06:15 UTC, returns
    (None, None). When called for 'yesterday' after the cron has run,
    returns the basket that the builder computed overnight.

    Returns (basket_post_blofin, basket_pre_blofin), same shape as
    compute_canonical_from_futures_1m.

    If the futures_1m-based canonical and the DB-leaderboards-based
    canonical ever disagree on the pre-filter basket, v2's on-the-fly
    computation has drifted from the builder's output. This is the
    daily bug-check."""
    snapshot_ts = _dt.datetime.combine(ref_date, _dt.time(DEPLOYMENT_START_HOUR, 0, 0),
                                       tzinfo=_dt.timezone.utc)
    conn = _get_db_conn()
    cur = conn.cursor()

    def _top(metric):
        cur.execute("""
            SELECT sym.binance_id
            FROM market.leaderboards lb
            JOIN market.symbols sym USING (symbol_id)
            WHERE timestamp_utc = %s
              AND metric = %s
              AND anchor_hour = %s
              AND variant = 'close'
              AND rank <= %s
            ORDER BY rank
        """, (snapshot_ts, metric, ANCHOR_HOUR, FREQ_WIDTH))
        return [r[0] for r in cur.fetchall()]

    price_top = _top("price")
    oi_top    = _top("open_interest")
    cur.close()
    conn.close()

    if not price_top or not oi_top:
        return (None, None)

    p = {normalize_symbol(b) for b in price_top} - {None}
    o = {normalize_symbol(b) for b in oi_top}    - {None}
    pre_filter = sorted(p & o)
    post_filter = sorted(_apply_blofin(pre_filter, blofin)) if blofin else list(pre_filter)
    return (post_filter, pre_filter)


# ==========================================================================
# JACCARD + HISTORY
# ==========================================================================

def jaccard(a, b):
    """|A ∩ B| / |A ∪ B|. Empty ∩ empty = 1.0. None sets treated as empty."""
    sa = set(a) if a else set()
    sb = set(b) if b else set()
    union = sa | sb
    if not union:
        return 1.0
    return len(sa & sb) / len(union)


def append_history(date_str, j_v1_can, j_v2_can, j_v1_v2, j_v2_lb_yest,
                   v1_n, v2_n, can_n):
    """Append one daily row to HISTORY_CSV for rolling computations + audit."""
    fieldnames = [
        "date", "j_v1_canonical", "j_v2_canonical", "j_v1_v2",
        "j_v2_leaderboard_yesterday", "n_v1", "n_v2", "n_canonical",
    ]
    rows = []
    if HISTORY_CSV.exists():
        try:
            with open(HISTORY_CSV, newline="") as f:
                for r in csv.DictReader(f):
                    if r.get("date") != date_str:  # replace today's row if rerun
                        rows.append(r)
        except Exception:
            pass

    rows.append({
        "date":                       date_str,
        "j_v1_canonical":             f"{j_v1_can:.4f}" if j_v1_can is not None else "",
        "j_v2_canonical":             f"{j_v2_can:.4f}" if j_v2_can is not None else "",
        "j_v1_v2":                    f"{j_v1_v2:.4f}"  if j_v1_v2  is not None else "",
        "j_v2_leaderboard_yesterday": f"{j_v2_lb_yest:.4f}" if j_v2_lb_yest is not None else "",
        "n_v1":                       str(v1_n) if v1_n is not None else "",
        "n_v2":                       str(v2_n) if v2_n is not None else "",
        "n_canonical":                str(can_n) if can_n is not None else "",
    })
    rows.sort(key=lambda r: r.get("date", ""))

    with open(HISTORY_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def compute_cutover_gate():
    """Read HISTORY_CSV and report cutover-gate state.

    Gate passes if the last CUTOVER_CONSEC_DAYS (=7) entries all have
    j_v2_canonical ≥ CUTOVER_JACCARD_MIN (=0.95)."""
    if not HISTORY_CSV.exists():
        return "PENDING — no history yet", 0, 0

    with open(HISTORY_CSV, newline="") as f:
        rows = list(csv.DictReader(f))

    # Keep only rows with a non-empty j_v2_canonical value
    scored = [r for r in rows if r.get("j_v2_canonical", "") != ""]
    scored.sort(key=lambda r: r["date"])

    # Count consecutive trailing pass-days
    consec = 0
    for r in reversed(scored):
        try:
            val = float(r["j_v2_canonical"])
        except ValueError:
            break
        if val >= CUTOVER_JACCARD_MIN:
            consec += 1
        else:
            break

    total = len(scored)
    if consec >= CUTOVER_CONSEC_DAYS:
        return (f"PASS — {consec} consecutive days at J(v2,canonical) ≥ "
                f"{CUTOVER_JACCARD_MIN}", consec, total)
    if consec > 0:
        return (f"PENDING — {consec}/{CUTOVER_CONSEC_DAYS} days at ≥"
                f"{CUTOVER_JACCARD_MIN}", consec, total)
    if total == 0:
        return ("PENDING — no scored days yet", 0, 0)
    return (f"FAIL — last day below {CUTOVER_JACCARD_MIN}; counter reset",
            0, total)


# ==========================================================================
# MAIN
# ==========================================================================

def _fmt_basket(b):
    if b is None:
        return "(unavailable)"
    if not b:
        return "(empty / sit_flat)"
    return f"[{len(b):>2}] " + " ".join(b)


def main():
    today     = _dt.datetime.now(_dt.timezone.utc).date()
    yesterday = today - _dt.timedelta(days=1)
    today_str     = today.isoformat()
    yesterday_str = yesterday.isoformat()

    log.info("=" * 74)
    log.info(f"  SHADOW-DIFF — {today_str}")
    log.info("=" * 74)

    # BloFin filter: applied to canonical baskets so shadow_diff compares
    # apples-to-apples with v2 (which filters BloFin post-basket).
    blofin = get_blofin_symbols()

    # --- Three baskets for today ---
    v1 = csv_basket_for_date(V1_CSV, today_str)
    v2 = csv_basket_for_date(V2_CSV, today_str)
    canonical_post, canonical_pre = compute_canonical_from_futures_1m(today, blofin=blofin)

    log.info(f"  v1 basket                   : {_fmt_basket(v1)}")
    log.info(f"  v2 basket                   : {_fmt_basket(v2)}")
    log.info(f"  canonical intersection      : {_fmt_basket(canonical_pre)}  [pre-BloFin, informational]")
    log.info(f"  canonical (post-BloFin)     : {_fmt_basket(canonical_post)}  [compared below]")

    # --- Pairwise Jaccards (None source → None result) ---
    def _j(a, b):
        return jaccard(a, b) if (a is not None and b is not None) else None

    j_v1_can = _j(v1, canonical_post)
    j_v2_can = _j(v2, canonical_post)
    j_v1_v2  = _j(v1, v2)

    def _fmt(x):
        return f"{x:.3f}" if x is not None else "n/a (source missing)"

    log.info(f"  J(v1, canonical) = {_fmt(j_v1_can)}   [informational — expect low; v1 drift]")
    log.info(f"  J(v2, canonical) = {_fmt(j_v2_can)}   [CUTOVER GATE — target ≈1.0]")
    log.info(f"  J(v1, v2)        = {_fmt(j_v1_v2)}    [informational — v1/v2 divergence]")

    # --- Cross-check: yesterday's v2 vs yesterday's DB leaderboard ---
    v2_yest = csv_basket_for_date(V2_CSV, yesterday_str)
    canonical_lb_post_yest, canonical_lb_pre_yest = compute_canonical_from_leaderboards(
        yesterday, blofin=blofin,
    )
    j_v2_lb_yest = _j(v2_yest, canonical_lb_post_yest)

    log.info("")
    log.info("  Cross-check (lagged 1 day, validates v2 ≡ builder):")
    log.info(f"    v2 ({yesterday_str})                 : {_fmt_basket(v2_yest)}")
    log.info(f"    leaderboards ({yesterday_str}) pre   : {_fmt_basket(canonical_lb_pre_yest)}  [informational]")
    log.info(f"    leaderboards ({yesterday_str}) post  : {_fmt_basket(canonical_lb_post_yest)}  [compared]")
    log.info(f"    J(v2_yest, leaderboard_DB_yest) = {_fmt(j_v2_lb_yest)}   "
             "[should be ≈1.0 daily; bug-catcher]")

    # --- Append to rolling history + compute cutover gate ---
    append_history(
        today_str, j_v1_can, j_v2_can, j_v1_v2, j_v2_lb_yest,
        v1_n=(len(v1) if v1 is not None else None),
        v2_n=(len(v2) if v2 is not None else None),
        can_n=(len(canonical_post) if canonical_post is not None else None),
    )
    gate_msg, consec, total = compute_cutover_gate()
    log.info("")
    log.info(f"  Cutover gate: {gate_msg}")
    log.info(f"  History rows: {total} scored days")
    log.info("=" * 74)


if __name__ == "__main__":
    main()

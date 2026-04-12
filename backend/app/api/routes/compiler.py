"""
backend/app/api/routes/compiler.py
==================================
Read-only FastAPI router for the Compiler admin page.

All endpoints query market.* tables in TimescaleDB through the shared
get_cursor() dependency. No write operations. No POST/trigger endpoints
this round — those will be added in a follow-up phase.

Constraints (per build doc):
  - Always filter by source_id = 1 (amberdata_binance) unless ?source= is given
  - Use symbol_id (integer FK) for joins, never string symbol matching
  - Coverage queries must use time_bucket('1 day', ...) for chunk-aware grouping
  - Default lookback windows are bounded to keep queries fast
"""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from ...db import get_cursor
from .admin import require_admin

# All compiler endpoints require an admin session. The dependency raises
# HTTP 401 if the admin_session cookie is missing or invalid. The
# frontend layout uses GET /api/admin/whoami to detect this and redirect
# to /compiler/login before rendering the protected pages.
router = APIRouter(
    prefix="/api/compiler",
    tags=["compiler"],
    dependencies=[Depends(require_admin)],
)


# ─── Per-symbol-day endpoint columns (used by /symbols/{symbol}, /days/{date}) ─
# Data columns in market.futures_1m that represent distinct Amberdata
# endpoints. Used to compute per-endpoint completeness for the Symbol Inspector
# and Day Detail pages. Order roughly matches data importance / cost (L1 first).
FUTURES_ENDPOINT_COLS = [
    # L1 — core market data (metl.py FETCH_PRICE / FETCH_OI / FETCH_FUNDING / FETCH_LS)
    "close", "volume", "open_interest", "funding_rate", "long_short_ratio",
    # L2 — trades + liquidations (metl.py FETCH_TRADES / FETCH_LIQUIDATIONS)
    "trade_delta", "long_liqs", "short_liqs",
    # L3 — orderbook depth + ticker (metl.py FETCH_TICKER / FETCH_ORDERBOOK)
    "last_bid_depth", "last_ask_depth", "last_depth_imbalance", "last_spread_pct",
    "spread_pct", "bid_ask_imbalance", "basis_pct",
]

# market_cap_usd is no longer in futures_1m. It lives in
# market.market_cap_daily as the single source of truth (one row per
# (date, base)). The per-symbol-day endpoint shape still surfaces it so
# the frontend keeps a single uniform completeness column — we synthesize
# 1440 ("complete") if a row exists in the daily table, else 0.
DAILY_ENDPOINT_COLS = ["market_cap_usd"]

ENDPOINT_COLS = FUTURES_ENDPOINT_COLS + DAILY_ENDPOINT_COLS

# Endpoints the day-health KPI is gated on. min over these per-endpoint
# pcts decides complete vs partial. The simulator/audit pipeline only
# strictly requires price (close) and open interest, so volume is no
# longer in the gate. mcap is excluded for the same reason as before:
# the CoinGecko-side mapping caps below 95 pct so gating on it produces
# zero complete days. Both volume and mcap remain visible in the
# per-endpoint breakdown.
CRITICAL_ENDPOINTS = ["close", "open_interest"]
COMPLETE_THRESHOLD_PCT = 90.0


def _serialize_job(r: dict) -> dict[str, Any]:
    """Convert a RealDictCursor row from market.compiler_jobs into JSON-safe dict."""
    return {
        "job_id":            str(r["job_id"]),
        "source_id":         r["source_id"],
        "status":            r["status"],
        "date_from":         r["date_from"].isoformat() if r["date_from"] else None,
        "date_to":           r["date_to"].isoformat()   if r["date_to"]   else None,
        "endpoints_enabled": r["endpoints_enabled"],
        "symbols_total":     r["symbols_total"],
        "symbols_done":      r["symbols_done"],
        "rows_written":      r["rows_written"],
        "started_at":        r["started_at"].isoformat()    if r["started_at"]    else None,
        "completed_at":      r["completed_at"].isoformat()  if r["completed_at"]  else None,
        "last_heartbeat":    r["last_heartbeat"].isoformat() if r["last_heartbeat"] else None,
        "error_msg":         r["error_msg"],
        "triggered_by":      r["triggered_by"],
        "run_tag":           r["run_tag"],
        "created_at":        r["created_at"].isoformat() if r["created_at"] else None,
        "is_stale":          bool(r["is_stale"]),
    }


# ─── /coverage ────────────────────────────────────────────────────────────────

@router.get("/coverage")
def coverage(
    days: int = Query(90, ge=1, le=10000, description="Lookback window in days (1-10000, default 90; frontend uses a large value to represent ALL)"),
    source_id: int = Query(1, ge=1, description="market.sources.source_id (default 1 = amberdata_binance)"),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Per-day symbol completeness for the last N days.

    Denominator semantics: each day uses its OWN expected symbol count,
    joined from market.compiler_jobs (symbols_total = what metl.py's
    get_symbols_for_date(day) returned at run time). This is the honest
    ground truth for "how many symbols did the pipeline try to fetch."

    For pre-job-tracking days (historical master-parquet backfill), no
    compiler_jobs row exists, so we fall back to symbols_with_data — the
    count of symbols that produced at least one row. Reads as 100% of
    whatever actually arrived, which is the best we can do without the
    original run's expected count.

    Returns one row per day with:
      - symbols_complete:    distinct symbols with >= 1440 rows that day
      - symbols_partial:     distinct symbols with 1-1439 rows that day
      - symbols_missing:     expected_symbols - symbols_with_data
      - expected_symbols:    per-day denominator (job ground truth or fallback)
      - has_job_truth:       bool — True if expected_symbols came from
                             compiler_jobs, False if fallback
      - completeness_pct:    symbols_complete / expected_symbols
    """
    interval_str = f"{days} days"

    cur.execute(
        """
        -- Per-(day, symbol) endpoint counts from the continuous aggregate
        -- market.futures_1m_daily_symbol_count, joined to per-day mcap
        -- coverage from market.market_cap_daily (the dedicated daily-grained
        -- table that replaced jamming mcap into futures_1m at 1m grain).
        --
        -- Per-endpoint complete check uses `>= 1440` so days with duplicate
        -- row sets still report correctly. Mcap "complete" means a row
        -- exists in market_cap_daily for (date, base) joined to a known
        -- symbol — there's no per-minute count for daily data.
        -- Per-symbol "complete" = both close and open_interest have at
        -- least 1296 non-null rows (1296 = 0.9 x 1440). The simulator
        -- only strictly requires price + OI, so volume/funding/LS are
        -- shown but not gated.
        WITH day_stats AS (
            SELECT
                day::date AS day,
                COUNT(DISTINCT symbol_id) AS symbols_with_data,
                COUNT(DISTINCT symbol_id) FILTER (
                    WHERE close_count >= 1296
                      AND oi_count    >= 1296
                ) AS symbols_complete,
                COUNT(DISTINCT symbol_id) FILTER (
                    WHERE row_count >= 1
                      AND NOT (close_count >= 1296 AND oi_count >= 1296)
                ) AS symbols_partial,
                COUNT(DISTINCT symbol_id) FILTER (WHERE close_count    >= 1296) AS symbols_complete_close,
                COUNT(DISTINCT symbol_id) FILTER (WHERE volume_count   >= 1296) AS symbols_complete_volume,
                COUNT(DISTINCT symbol_id) FILTER (WHERE oi_count       >= 1296) AS symbols_complete_oi,
                COUNT(DISTINCT symbol_id) FILTER (WHERE funding_count  >= 1296) AS symbols_complete_funding,
                COUNT(DISTINCT symbol_id) FILTER (WHERE ls_count       >= 1296) AS symbols_complete_ls
            FROM market.futures_1m_daily_symbol_count
            WHERE source_id = %s
              AND day >= NOW() - %s::interval
            GROUP BY day
        ),
        mcap_stats AS (
            -- Bound the mcap join to symbols that ALSO had futures_1m data
            -- on the same date. Without this, market.symbols can grow over
            -- time (new listings, retired bases that stayed in the table)
            -- and the join finds more matches than the day's expected
            -- symbols, producing nonsense like 127.3 pct. The cagg join makes
            -- mcap_done a strict subset of sym_with_data.
            SELECT
                fdc.day::date AS day,
                COUNT(DISTINCT s.symbol_id) AS symbols_complete_mcap
            FROM market.market_cap_daily mcd
            JOIN market.symbols s ON s.base = mcd.base
            JOIN market.futures_1m_daily_symbol_count fdc
              ON fdc.symbol_id = s.symbol_id
             AND fdc.day::date = mcd.date
             AND fdc.source_id = %s
            WHERE mcd.date >= (NOW() - %s::interval)::date
            GROUP BY fdc.day::date
        ),
        -- Ground truth for how many symbols metl.py actually tried to fetch
        -- on each day. DISTINCT ON picks the most recent successful run per
        -- date_from (cron + backfill scripts always invoke metl.py with a
        -- single-day range, so date_from uniquely identifies the day).
        job_totals AS (
            SELECT DISTINCT ON (date_from::date)
                date_from::date AS day,
                symbols_total
            FROM market.compiler_jobs
            WHERE status = 'complete'
              AND source_id = %s
              AND symbols_total IS NOT NULL
              AND symbols_total > 0
              AND date_from = date_to
            ORDER BY date_from::date, completed_at DESC NULLS LAST
        )
        SELECT
            d.day,
            d.symbols_with_data,
            d.symbols_complete,
            d.symbols_partial,
            d.symbols_complete_close,
            d.symbols_complete_volume,
            d.symbols_complete_oi,
            d.symbols_complete_funding,
            d.symbols_complete_ls,
            COALESCE(m.symbols_complete_mcap, 0) AS symbols_complete_mcap,
            COALESCE(j.symbols_total, d.symbols_with_data) AS expected_symbols,
            (j.symbols_total IS NOT NULL) AS has_job_truth
        FROM day_stats d
        LEFT JOIN mcap_stats m ON m.day = d.day
        LEFT JOIN job_totals j ON j.day = d.day
        ORDER BY d.day DESC
        """,
        (source_id, interval_str, source_id, interval_str, source_id),
    )
    day_rows = cur.fetchall()

    cur.execute("SELECT COUNT(*) AS total FROM market.symbols WHERE active = TRUE")
    total_active = cur.fetchone()["total"]

    def _pct(num: int, denom: int) -> float:
        return round(num / denom * 100, 1) if denom > 0 else 0.0

    days_payload = []
    for r in day_rows:
        expected = int(r["expected_symbols"]) if r["expected_symbols"] is not None else 0
        symbols_complete = int(r["symbols_complete"])
        symbols_with_data = int(r["symbols_with_data"])
        endpoints = {
            "close":            _pct(int(r["symbols_complete_close"]),   expected),
            "volume":           _pct(int(r["symbols_complete_volume"]),  expected),
            "open_interest":    _pct(int(r["symbols_complete_oi"]),      expected),
            "funding_rate":     _pct(int(r["symbols_complete_funding"]), expected),
            "long_short_ratio": _pct(int(r["symbols_complete_ls"]),      expected),
            "market_cap_usd":   _pct(int(r["symbols_complete_mcap"]),    expected),
        }
        # Unified day health: min over the critical endpoints. A day is
        # only "complete" when every critical endpoint clears the
        # threshold; "missing" when zero data; "partial" otherwise. This
        # makes the KPI counts add up to days_returned and propagates
        # mcap gaps into the heatmap + gap table.
        min_critical_pct = min(endpoints[c] for c in CRITICAL_ENDPOINTS)
        if symbols_with_data == 0:
            status = "missing"
        elif min_critical_pct > COMPLETE_THRESHOLD_PCT:
            status = "complete"
        else:
            status = "partial"
        days_payload.append({
            "date":                 r["day"].isoformat(),
            "symbols_complete":     symbols_complete,
            "symbols_partial":      int(r["symbols_partial"]),
            "symbols_with_data":    symbols_with_data,
            "symbols_missing":      max(0, expected - symbols_with_data),
            "expected_symbols":     expected,
            "has_job_truth":        bool(r["has_job_truth"]),
            # Legacy field — symbols_complete / expected_symbols. Kept for
            # backwards compatibility but the frontend should prefer
            # min_critical_pct + status, which factor in mcap.
            "completeness_pct":     _pct(symbols_complete, expected),
            "min_critical_pct":     round(min_critical_pct, 1),
            "status":               status,
            # Per-endpoint coverage — symbols where the named column has
            # >= 1440 non-null values that day, divided by expected_symbols.
            # mcap is sourced from market.market_cap_daily; the rest from
            # the futures_1m daily-symbol-count cagg.
            "endpoints":            endpoints,
        })

    # Most recent day's expected count — used by the frontend as the
    # "Expected Today" KPI (replacing the old stale fetched_universe).
    expected_today = days_payload[0]["expected_symbols"] if days_payload else total_active

    return {
        "source_id": source_id,
        "lookback_days": days,
        "total_active_symbols": total_active,
        "expected_today": expected_today,
        "days_returned": len(days_payload),
        "days": days_payload,
    }


# ─── /gaps ────────────────────────────────────────────────────────────────────

@router.get("/gaps")
def gaps(
    days: int = Query(90, ge=1, le=10000, description="Lookback window in days (1-10000, default 90)"),
    source_id: int = Query(1, ge=1),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Days within the lookback window where ANY critical endpoint
    (close / volume / open_interest / market_cap_usd) is below the
    completeness threshold (95%). Reuses /coverage so the definition
    stays in one place.

    Status mirrors the per-day status from /coverage:
      - 'missing' — symbols_with_data == 0
      - 'partial' — has data but min_critical_pct < 95%
    Days at "complete" status are excluded. Ordered by date DESC.
    """
    cov = coverage(days=days, source_id=source_id, cur=cur)

    gap_days = []
    for d in cov["days"]:
        if d["status"] == "complete":
            continue
        gap_days.append({
            "date":             d["date"],
            "symbols_complete": d["symbols_complete"],
            "symbols_total":    d["expected_symbols"],
            "completeness_pct": d["min_critical_pct"],
            "has_job_truth":    d["has_job_truth"],
            "status":           d["status"],
            "endpoints":        d["endpoints"],
        })

    return {
        "source_id": source_id,
        "lookback_days": days,
        "total_active_symbols": cov["total_active_symbols"],
        "expected_today":       cov["expected_today"],
        "gaps_returned": len(gap_days),
        "gaps": gap_days,
    }


# ─── /days/{date} ─────────────────────────────────────────────────────────────

@router.get("/days/{target_date}")
def day_detail(
    target_date: str,
    source_id: int = Query(1, ge=1),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Drill-down view of a single day. Returns:

    - date, expected_symbols (from compiler_jobs when available, else
      symbols_with_data), symbols_complete/partial/missing counts
    - Optional compiler_jobs metadata (job_id, status, triggered_by,
      run_tag, started_at, completed_at, symbols_total, etc.)
    - symbols[] — one row per symbol with per-endpoint row counts so the
      frontend can visually render "this symbol got close + volume but
      no orderbook" style hints

    Layout of symbols[] mirrors the symbol inspector's per-endpoint
    shape so the two pages share rendering logic.
    """
    # Validate date format
    try:
        parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {target_date}. Use YYYY-MM-DD.")

    # Per-symbol per-endpoint counts for the target day. FUTURES_ENDPOINT_COLS
    # is hardcoded above so direct interpolation is safe. Daily mcap is
    # joined separately from market.market_cap_daily — futures_1m no longer
    # carries it.
    col_selects = ",\n            ".join([f"COUNT(f.{c}) AS {c}_cnt" for c in FUTURES_ENDPOINT_COLS])
    cur.execute(
        f"""
        SELECT
            s.symbol_id,
            s.base,
            s.binance_id,
            COUNT(*) AS total_rows,
            {col_selects}
        FROM market.futures_1m f
        JOIN market.symbols s ON s.symbol_id = f.symbol_id
        WHERE f.source_id = %s
          AND f.timestamp_utc >= %s::date
          AND f.timestamp_utc <  (%s::date + INTERVAL '1 day')
        GROUP BY s.symbol_id, s.base, s.binance_id
        ORDER BY total_rows DESC, s.base
        """,
        (source_id, parsed_date, parsed_date),
    )
    symbol_rows = cur.fetchall()

    # Bases that have a daily mcap row for the target date — used to
    # synthesize a "1440" count for the market_cap_usd endpoint column so
    # the per-symbol shape stays uniform with the futures_1m endpoints.
    cur.execute(
        "SELECT base FROM market.market_cap_daily WHERE date = %s",
        (parsed_date,),
    )
    mcap_bases_for_day = {r["base"] for r in cur.fetchall()}

    # Per-symbol payload. A symbol is "complete" iff close AND open
    # interest both have at least 1296 rows (1296 = 0.9 x 1440). Same
    # definition as the /coverage handler. Volume / funding / LS are
    # shown in the per-row breakdown but not part of the gate.
    L1_KEYS = ("close", "open_interest")
    L1_THRESHOLD = 1296
    symbols_payload = []
    complete_count = 0
    partial_count = 0
    for r in symbol_rows:
        total = int(r["total_rows"])
        rows_per_endpoint = {c: int(r[f"{c}_cnt"]) for c in FUTURES_ENDPOINT_COLS}
        rows_per_endpoint["market_cap_usd"] = 1440 if r["base"] in mcap_bases_for_day else 0
        l1_complete = all(rows_per_endpoint[k] >= L1_THRESHOLD for k in L1_KEYS)
        if l1_complete:
            status = "complete"
            complete_count += 1
        elif total > 0:
            status = "partial"
            partial_count += 1
        else:
            status = "missing"
        symbols_payload.append({
            "symbol_id":         r["symbol_id"],
            "base":              r["base"],
            "binance_id":        r["binance_id"],
            "total_rows":        total,
            "completeness_pct":  round(min(total / 1440 * 100, 100), 1),
            "status":            status,
            "rows_per_endpoint": rows_per_endpoint,
        })

    # Compiler job metadata for this date (most recent successful run).
    cur.execute(
        """
        SELECT job_id, status, symbols_total, symbols_done, rows_written,
               started_at, completed_at, triggered_by, run_tag, error_msg
        FROM market.compiler_jobs
        WHERE source_id = %s
          AND date_from = %s::date
          AND date_to   = %s::date
        ORDER BY
          CASE status WHEN 'complete' THEN 0 ELSE 1 END,
          completed_at DESC NULLS LAST
        LIMIT 1
        """,
        (source_id, parsed_date, parsed_date),
    )
    job_row = cur.fetchone()

    job_payload = None
    expected_symbols = None
    if job_row:
        job_payload = {
            "job_id":         str(job_row["job_id"]),
            "status":         job_row["status"],
            "symbols_total":  job_row["symbols_total"],
            "symbols_done":   job_row["symbols_done"],
            "rows_written":   int(job_row["rows_written"]) if job_row["rows_written"] is not None else 0,
            "started_at":     job_row["started_at"].isoformat() if job_row["started_at"] else None,
            "completed_at":   job_row["completed_at"].isoformat() if job_row["completed_at"] else None,
            "triggered_by":   job_row["triggered_by"],
            "run_tag":        job_row["run_tag"],
            "error_msg":      job_row["error_msg"],
        }
        if job_row["symbols_total"] and job_row["symbols_total"] > 0:
            expected_symbols = int(job_row["symbols_total"])

    # Fall back to symbols_with_data when no job record exists (historical
    # master-parquet era).
    symbols_with_data = len(symbols_payload)
    if expected_symbols is None:
        expected_symbols = symbols_with_data

    missing_count = max(0, expected_symbols - symbols_with_data)

    total_complete = complete_count
    total_expected = expected_symbols
    day_completeness_pct = (
        round(total_complete / total_expected * 100, 1)
        if total_expected > 0 else 0.0
    )

    # Dupe-detection guard. A normal day has total_rows ≤ symbols × 1440.
    # If the day's actual total exceeds 1.3× that ceiling, we likely have
    # two row sets coexisting from different ingestion sources (e.g. the
    # original parquet at sub-minute timestamps + a metl.py reload at
    # minute-aligned). This silently bloats storage and confuses the
    # row-count completeness math. Surface it as a warning so the user
    # sees it on the day detail page.
    total_rows_on_day = sum(s["total_rows"] for s in symbols_payload)
    expected_max_rows = max(symbols_with_data, 1) * 1440
    suspected_duplicates = total_rows_on_day > expected_max_rows * 1.3
    duplicate_factor = (
        round(total_rows_on_day / expected_max_rows, 2)
        if expected_max_rows > 0 else 0.0
    )

    return {
        "date":                  parsed_date.isoformat(),
        "source_id":             source_id,
        "expected_symbols":      expected_symbols,
        "symbols_with_data":     symbols_with_data,
        "symbols_complete":      complete_count,
        "symbols_partial":       partial_count,
        "symbols_missing":       missing_count,
        "has_job_truth":         job_payload is not None,
        "day_completeness_pct":  day_completeness_pct,
        "job":                   job_payload,
        "endpoint_cols":         ENDPOINT_COLS,
        "symbols":               symbols_payload,
        "total_rows":            total_rows_on_day,
        "suspected_duplicates":  suspected_duplicates,
        "duplicate_factor":      duplicate_factor,
    }


# ─── /marketcap/coverage ──────────────────────────────────────────────────────

@router.get("/marketcap/coverage")
def marketcap_coverage(
    days: int = Query(90, ge=1, le=10000, description="Lookback window (1-10000)"),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Per-day market.market_cap_daily coverage stats. Mirrors the futures
    /coverage shape so the frontend can reuse the same heatmap layout.

    A day is "complete" if it has any mcap rows; "missing" if zero. There's
    no per-symbol rollup here because mcap is daily-grained — either the
    CoinGecko snapshot landed for that day or it didn't.
    """
    interval_str = f"{days} days"
    cur.execute(
        """
        SELECT
            date,
            COUNT(*)                AS mcap_rows,
            COUNT(rank_num)         AS ranked_rows,
            SUM(market_cap_usd)     AS total_mcap_usd
        FROM market.market_cap_daily
        WHERE date >= (NOW() - %s::interval)::date
        GROUP BY date
        ORDER BY date DESC
        """,
        (interval_str,),
    )
    rows = cur.fetchall()

    days_payload = []
    for r in rows:
        mcap_rows = int(r["mcap_rows"])
        days_payload.append({
            "date":           r["date"].isoformat(),
            "mcap_rows":      mcap_rows,
            "ranked_rows":    int(r["ranked_rows"]),
            "total_mcap_usd": float(r["total_mcap_usd"]) if r["total_mcap_usd"] is not None else None,
            "status":         "missing" if mcap_rows == 0 else "complete",
        })

    return {
        "lookback_days":     days,
        "days_returned":     len(days_payload),
        "days_with_data":    sum(1 for d in days_payload if d["status"] == "complete"),
        "days_missing":      sum(1 for d in days_payload if d["status"] == "missing"),
        "days":              days_payload,
    }


# ─── /marketcap/days/{date} ───────────────────────────────────────────────────

@router.get("/marketcap/days/{target_date}")
def marketcap_day_detail(
    target_date: str,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    All market_cap_daily rows for one day, sorted by rank_num. Each row is
    flagged with `has_futures` = whether its base also exists in
    market.symbols (i.e. whether the simulator can use this mcap).
    """
    try:
        parsed_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {target_date}. Use YYYY-MM-DD.")

    cur.execute(
        """
        SELECT
            mcd.base,
            mcd.coin_id,
            mcd.name,
            mcd.market_cap_usd,
            mcd.price_usd,
            mcd.volume_usd,
            mcd.rank_num,
            (s.symbol_id IS NOT NULL) AS has_futures
        FROM market.market_cap_daily mcd
        LEFT JOIN market.symbols s ON s.base = mcd.base
        WHERE mcd.date = %s
        ORDER BY COALESCE(mcd.rank_num, 999999), mcd.market_cap_usd DESC NULLS LAST
        """,
        (parsed_date,),
    )
    rows = cur.fetchall()

    payload = []
    matched = 0
    for r in rows:
        if r["has_futures"]:
            matched += 1
        payload.append({
            "base":            r["base"],
            "coin_id":         r["coin_id"],
            "name":            r["name"],
            "market_cap_usd":  float(r["market_cap_usd"]) if r["market_cap_usd"] is not None else None,
            "price_usd":       float(r["price_usd"])      if r["price_usd"]      is not None else None,
            "volume_usd":      float(r["volume_usd"])     if r["volume_usd"]     is not None else None,
            "rank_num":        int(r["rank_num"])         if r["rank_num"]       is not None else None,
            "has_futures":     bool(r["has_futures"]),
        })

    return {
        "date":               parsed_date.isoformat(),
        "total_rows":         len(payload),
        "matched_to_futures": matched,
        "rows":               payload,
    }


# ─── /jobs ────────────────────────────────────────────────────────────────────

_JOB_SELECT = """
    SELECT
        job_id, source_id, status, date_from, date_to,
        endpoints_enabled, symbols_total, symbols_done, rows_written,
        started_at, completed_at, last_heartbeat, error_msg,
        triggered_by, run_tag, created_at,
        (status = 'running' AND (
            (last_heartbeat IS NOT NULL AND last_heartbeat < NOW() - INTERVAL '2 hours')
            OR
            (last_heartbeat IS NULL AND started_at < NOW() - INTERVAL '2 hours')
        )) AS is_stale
    FROM market.compiler_jobs
"""


@router.get("/jobs")
def list_jobs(
    limit: int = Query(50, ge=1, le=200),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """List recent compiler jobs ordered by created_at DESC."""
    cur.execute(
        _JOB_SELECT + " ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )
    rows = cur.fetchall()
    return {
        "jobs_returned": len(rows),
        "jobs":          [_serialize_job(r) for r in rows],
    }


@router.get("/jobs/{job_id}")
def get_job(job_id: str, cur=Depends(get_cursor)) -> dict[str, Any]:
    """Single job by UUID."""
    cur.execute(_JOB_SELECT + " WHERE job_id = %s", (job_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return _serialize_job(row)


# ─── /runs (UI-triggered script runs via market.run_jobs) ──────────────────

class RunRequest(BaseModel):
    script: str           # one of SCRIPT_REGISTRY keys: metl | coingecko_marketcap | backfill_futures_1m
    params: dict[str, Any] = {}


def _serialize_run(r: dict) -> dict[str, Any]:
    return {
        "run_id":         str(r["run_id"]),
        "script_name":    r["script_name"],
        "module":         r["module"],
        "status":         r["status"],
        "triggered_by":   r["triggered_by"],
        "params":         r["params"],
        "started_at":     r["started_at"].isoformat()    if r["started_at"]    else None,
        "completed_at":   r["completed_at"].isoformat()  if r["completed_at"]  else None,
        "last_heartbeat": r["last_heartbeat"].isoformat() if r["last_heartbeat"] else None,
        "exit_code":      r["exit_code"],
        "rows_written":   int(r["rows_written"]) if r["rows_written"] is not None else 0,
        "error_msg":      r["error_msg"],
        "stdout_tail":    r["stdout_tail"],
        "stderr_tail":    r["stderr_tail"],
        "created_at":     r["created_at"].isoformat() if r["created_at"] else None,
    }


@router.post("/runs")
def create_run(body: RunRequest) -> dict[str, Any]:
    """Enqueue a celery task that runs the requested registered script.
    Returns the celery task_id immediately. Progress is tracked in
    market.run_jobs and visible via GET /api/compiler/runs (polled by the
    Compiler Jobs page)."""
    from app.workers.run_jobs_worker import SCRIPT_REGISTRY, run_script
    if body.script not in SCRIPT_REGISTRY:
        raise HTTPException(
            status_code=400,
            detail=f"unknown script {body.script!r}, must be one of {list(SCRIPT_REGISTRY.keys())}",
        )
    async_result = run_script.delay(body.script, body.params, "ui")
    return {
        "ok": True,
        "script": body.script,
        "celery_task_id": async_result.id,
    }


@router.get("/runs")
def list_runs(
    limit: int = Query(20, ge=1, le=200),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """List recent UI-triggered runs from market.run_jobs filtered to
    module='compiler'. Polled by the Compiler Jobs page."""
    cur.execute(
        """
        SELECT run_id, script_name, module, status, triggered_by, params,
               started_at, completed_at, last_heartbeat, exit_code,
               rows_written, error_msg, stdout_tail, stderr_tail, created_at
        FROM market.run_jobs
        WHERE module = 'compiler'
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    return {
        "runs_returned": len(rows),
        "runs": [_serialize_run(r) for r in rows],
    }


# ─── /symbols (list) ──────────────────────────────────────────────────────────

@router.get("/symbols")
def list_symbols(
    source_id: int = Query(1, ge=1),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Coverage summary for every active symbol — one row per symbol with
    most-recent-day completeness for price (close), open_interest, and
    volume, plus a 90-day count of distinct days with any data.

    Backed by `market.symbol_day_counts` continuous aggregate (created
    2026-04-08) which pre-aggregates per-day-per-symbol-per-source row
    counts. The cagg refresh policy runs every 1 hour, so the displayed
    "latest day" can be up to 1h stale relative to live ingest. ~223k
    rows in the cagg vs ~558M in the underlying futures_1m hypertable.

    Without the cagg, the equivalent query against compressed futures_1m
    chunks took ~152s (decompression overhead per symbol × 658 symbols).
    With the cagg this query is sub-50ms.
    """
    cur.execute(
        """
        WITH latest_per_symbol AS (
            SELECT
                symbol_id,
                MAX(day)::date AS latest_date,
                COUNT(DISTINCT day) AS days_with_data
            FROM market.symbol_day_counts
            WHERE source_id = %s
              AND day >= NOW() - INTERVAL '90 days'
            GROUP BY symbol_id
        )
        SELECT
            s.symbol_id,
            s.base AS symbol,
            COALESCE(lps.days_with_data, 0) AS days_with_data,
            lps.latest_date,
            COALESCE(sdc.total_rows,  0) AS total_rows,
            COALESCE(sdc.price_rows,  0) AS price_rows,
            COALESCE(sdc.oi_rows,     0) AS oi_rows,
            COALESCE(sdc.volume_rows, 0) AS volume_rows
        FROM market.symbols s
        LEFT JOIN latest_per_symbol lps ON lps.symbol_id = s.symbol_id
        LEFT JOIN market.symbol_day_counts sdc
          ON sdc.symbol_id = lps.symbol_id
         AND sdc.source_id = %s
         AND sdc.day::date = lps.latest_date
        WHERE s.active = TRUE
        ORDER BY days_with_data DESC, s.base ASC
        """,
        (source_id, source_id),
    )
    rows = cur.fetchall()

    def _pct(num: int, denom: int) -> float:
        if denom <= 0:
            return 0.0
        return round(num / denom * 100, 1)

    def _status(price_pct: float, oi_pct: float, volume_pct: float, total_rows: int) -> str:
        if total_rows == 0:
            return "missing"
        if price_pct >= 95 and oi_pct >= 95 and volume_pct >= 95:
            return "complete"
        return "partial"

    payload = []
    for r in rows:
        total = int(r["total_rows"])
        price_pct  = _pct(int(r["price_rows"]),  total)
        oi_pct     = _pct(int(r["oi_rows"]),     total)
        volume_pct = _pct(int(r["volume_rows"]), total)
        payload.append({
            "symbol":        r["symbol"],
            "symbol_id":     r["symbol_id"],
            "days_with_data": int(r["days_with_data"]),
            "latest_date":   r["latest_date"].isoformat() if r["latest_date"] else None,
            "price_pct":     price_pct,
            "oi_pct":        oi_pct,
            "volume_pct":    volume_pct,
            "status":        _status(price_pct, oi_pct, volume_pct, total),
        })

    return {
        "source_id":        source_id,
        "lookback_days":    90,
        "symbols_returned": len(payload),
        "symbols":          payload,
    }


# ─── /symbols/{symbol} ────────────────────────────────────────────────────────

@router.get("/symbols/{symbol}")
def symbol_inspector(
    symbol: str,
    source_id: int = Query(1, ge=1),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Symbol Inspector — per-endpoint completeness for the most recent day with
    data, plus a 30-day sparkline of total row counts.
    """
    base = symbol.upper().strip()

    cur.execute("SELECT symbol_id, base FROM market.symbols WHERE base = %s", (base,))
    sym_row = cur.fetchone()
    if not sym_row:
        raise HTTPException(status_code=404, detail=f"Symbol '{base}' not found in market.symbols")
    symbol_id = sym_row["symbol_id"]

    # Latest day with any data for this symbol
    cur.execute(
        """
        SELECT MAX(time_bucket('1 day', timestamp_utc))::date AS latest_date
        FROM market.futures_1m
        WHERE symbol_id = %s AND source_id = %s
        """,
        (symbol_id, source_id),
    )
    latest_row = cur.fetchone()
    latest_date = latest_row["latest_date"] if latest_row else None

    if latest_date is None:
        # Symbol exists but no data yet — return an empty shape rather than 404
        return {
            "symbol":            base,
            "symbol_id":         symbol_id,
            "source_id":         source_id,
            "date":              None,
            "total_rows":        0,
            "rows_per_endpoint": {c: 0 for c in ENDPOINT_COLS},
            "sparkline":         [],
        }

    # Per-column non-null counts for the latest day. FUTURES_ENDPOINT_COLS
    # is hardcoded so direct interpolation here is safe.
    col_selects = ",\n            ".join([f"COUNT({c}) AS {c}_cnt" for c in FUTURES_ENDPOINT_COLS])
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            {col_selects}
        FROM market.futures_1m
        WHERE symbol_id = %s
          AND source_id = %s
          AND timestamp_utc >= %s::date
          AND timestamp_utc <  (%s::date + INTERVAL '1 day')
        """,
        (symbol_id, source_id, latest_date, latest_date),
    )
    endpoint_row = cur.fetchone()

    rows_per_endpoint = {c: int(endpoint_row[f"{c}_cnt"]) for c in FUTURES_ENDPOINT_COLS}

    # Daily mcap is in market.market_cap_daily, not futures_1m. Synthesize
    # 1440 ("complete") if a row exists for this base on the latest day.
    cur.execute(
        "SELECT 1 FROM market.market_cap_daily WHERE base = %s AND date = %s LIMIT 1",
        (base, latest_date),
    )
    rows_per_endpoint["market_cap_usd"] = 1440 if cur.fetchone() else 0

    # 30-day sparkline of total row counts
    cur.execute(
        """
        SELECT
            time_bucket('1 day', timestamp_utc)::date AS day,
            COUNT(*) AS cnt
        FROM market.futures_1m
        WHERE symbol_id = %s
          AND source_id = %s
          AND timestamp_utc >= NOW() - INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 1
        """,
        (symbol_id, source_id),
    )
    sparkline = [{"date": r["day"].isoformat(), "rows": int(r["cnt"])} for r in cur.fetchall()]

    return {
        "symbol":            base,
        "symbol_id":         symbol_id,
        "source_id":         source_id,
        "date":              latest_date.isoformat(),
        "total_rows":        int(endpoint_row["total_rows"]),
        "rows_per_endpoint": rows_per_endpoint,
        "sparkline":         sparkline,
    }

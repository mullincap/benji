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

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from ...db import get_cursor

router = APIRouter(prefix="/api/compiler", tags=["compiler"])


# ─── Per-symbol-day endpoint columns (used by /symbols/{symbol}) ──────────────
# These are the 15 data columns in market.futures_1m that represent distinct
# Amberdata endpoints. Used to compute per-endpoint completeness for the
# Symbol Inspector page. Order chosen to roughly match data importance / cost.
ENDPOINT_COLS = [
    "close", "volume", "open_interest", "funding_rate", "long_short_ratio",
    "trade_delta", "long_liqs", "short_liqs",
    "last_bid_depth", "last_ask_depth", "last_depth_imbalance", "last_spread_pct",
    "spread_pct", "bid_ask_imbalance", "basis_pct",
]


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
    days: int = Query(90, ge=1, le=365, description="Lookback window in days (1-365, default 90)"),
    source_id: int = Query(1, ge=1, description="market.sources.source_id (default 1 = amberdata_binance)"),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Per-day symbol completeness for the last N days.

    Returns one row per day with:
      - symbols_complete: count of distinct symbols with >= 1440 rows that day
      - symbols_partial:  count of distinct symbols with 1-1439 rows that day
      - symbols_missing:  total_active - symbols_with_data
      - total_active_symbols: market.symbols WHERE active = TRUE
    """
    interval_str = f"{days} days"

    cur.execute(
        """
        SELECT day::date AS day, symbols_with_data, symbols_complete, symbols_partial
        FROM (
            SELECT
                day,
                COUNT(DISTINCT symbol_id) AS symbols_with_data,
                COUNT(DISTINCT symbol_id) FILTER (WHERE cnt >= 1440) AS symbols_complete,
                COUNT(DISTINCT symbol_id) FILTER (WHERE cnt BETWEEN 1 AND 1439) AS symbols_partial
            FROM (
                SELECT time_bucket('1 day', timestamp_utc) AS day, symbol_id, COUNT(*) AS cnt
                FROM market.futures_1m
                WHERE source_id = %s
                  AND timestamp_utc >= NOW() - %s::interval
                GROUP BY 1, 2
            ) sub
            GROUP BY day
            ORDER BY day DESC
        ) coverage
        """,
        (source_id, interval_str),
    )
    day_rows = cur.fetchall()

    cur.execute("SELECT COUNT(*) AS total FROM market.symbols WHERE active = TRUE")
    total_active = cur.fetchone()["total"]

    return {
        "source_id": source_id,
        "lookback_days": days,
        "total_active_symbols": total_active,
        "days_returned": len(day_rows),
        "days": [
            {
                "date":                  r["day"].isoformat(),
                "symbols_complete":      r["symbols_complete"],
                "symbols_partial":       r["symbols_partial"],
                "symbols_missing":       max(0, total_active - r["symbols_with_data"]),
                "total_active_symbols":  total_active,
            }
            for r in day_rows
        ],
    }


# ─── /gaps ────────────────────────────────────────────────────────────────────

@router.get("/gaps")
def gaps(
    days: int = Query(90, ge=1, le=365, description="Lookback window in days (1-365, default 90)"),
    source_id: int = Query(1, ge=1),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Days within the lookback window where coverage is incomplete.

    A day appears in this list if symbols_complete < total_active_symbols.
    Status:
      - 'missing' = completeness_pct == 0 (no symbols hit 1440 rows)
      - 'partial' = completeness_pct > 0 but < 100
    Ordered by date DESC.
    """
    # Reuse the coverage logic so the math stays in one place.
    cov = coverage(days=days, source_id=source_id, cur=cur)
    total = cov["total_active_symbols"]

    gap_days = []
    for d in cov["days"]:
        if total > 0 and d["symbols_complete"] >= total:
            continue
        completeness_pct = round((d["symbols_complete"] / total * 100), 2) if total > 0 else 0.0
        gap_days.append({
            "date":             d["date"],
            "symbols_complete": d["symbols_complete"],
            "symbols_total":    total,
            "completeness_pct": completeness_pct,
            "status":           "missing" if completeness_pct == 0 else "partial",
        })

    return {
        "source_id": source_id,
        "lookback_days": days,
        "total_active_symbols": total,
        "gaps_returned": len(gap_days),
        "gaps": gap_days,
    }


# ─── /jobs ────────────────────────────────────────────────────────────────────

_JOB_SELECT = """
    SELECT
        job_id, source_id, status, date_from, date_to,
        endpoints_enabled, symbols_total, symbols_done, rows_written,
        started_at, completed_at, last_heartbeat, error_msg,
        triggered_by, run_tag, created_at,
        (status = 'running' AND last_heartbeat IS NOT NULL
            AND last_heartbeat < NOW() - INTERVAL '2 hours') AS is_stale
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

    # Per-column non-null counts for the latest day. ENDPOINT_COLS is hardcoded
    # so direct interpolation here is safe (no SQL injection surface).
    col_selects = ",\n            ".join([f"COUNT({c}) AS {c}_cnt" for c in ENDPOINT_COLS])
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

    rows_per_endpoint = {c: int(endpoint_row[f"{c}_cnt"]) for c in ENDPOINT_COLS}

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

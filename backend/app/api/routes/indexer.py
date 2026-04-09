"""
backend/app/api/routes/indexer.py
=================================
Read-only FastAPI router for the Indexer admin page.

All endpoints query market.* and user_mgmt.* and audit.* tables in
TimescaleDB through the shared get_cursor() dependency. No write
operations. No POST/trigger endpoints this round — those will be added
in a follow-up phase.

Constraints (per build doc):
  - Coverage: COUNT(*) = 1440 × 333 strict per (metric, day)
  - Honest empty state: market.leaderboards is currently price-only and
    frozen at 2026-03-19. Coverage will surface this honestly.
  - All endpoints require admin auth via the same require_admin
    dependency the compiler router uses (cookie name: admin_session)
"""

from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from ...db import get_cursor
from .admin import require_admin

# All indexer endpoints require an admin session. The dependency raises
# HTTP 401 if the admin_session cookie is missing or invalid. Frontend
# uses GET /api/admin/whoami to detect this and redirect to /indexer/login
# (or /compiler/login — same auth, same cookie).
router = APIRouter(
    prefix="/api/indexer",
    tags=["indexer"],
    dependencies=[Depends(require_admin)],
)


# ─── Coverage math constants ────────────────────────────────────────────────
# A "complete" day for a metric has every minute fully ranked from R1 to R333.
# 1440 minutes/day × 333 ranks/minute = 479,520 rows per day per metric.
ROWS_PER_FULL_DAY = 1440 * 333  # 479,520

# Three metrics the indexer produces, in display order.
METRICS = ["price", "open_interest", "volume"]


def _serialize_indexer_job(r: dict) -> dict[str, Any]:
    """Convert a RealDictCursor row from market.indexer_jobs into JSON-safe dict."""
    return {
        "job_id":         str(r["job_id"]),
        "job_type":       r["job_type"],
        "status":         r["status"],
        "metric":         r["metric"],
        "date_from":      r["date_from"].isoformat() if r["date_from"] else None,
        "date_to":        r["date_to"].isoformat()   if r["date_to"]   else None,
        "params":         r["params"],
        "symbols_total":  r["symbols_total"],
        "symbols_done":   r["symbols_done"],
        "rows_written":   r["rows_written"],
        "started_at":     r["started_at"].isoformat()    if r["started_at"]    else None,
        "completed_at":   r["completed_at"].isoformat()  if r["completed_at"]  else None,
        "last_heartbeat": r["last_heartbeat"].isoformat() if r["last_heartbeat"] else None,
        "error_msg":      r["error_msg"],
        "triggered_by":   r["triggered_by"],
        "run_tag":        r["run_tag"],
        "created_at":     r["created_at"].isoformat() if r["created_at"] else None,
        "is_stale":       bool(r["is_stale"]),
    }


# ─── /coverage ────────────────────────────────────────────────────────────────

@router.get("/coverage")
def coverage(
    days: int = Query(90, ge=1, le=10000, description="Lookback window in days (1-10000, default 90; the frontend uses a large value to represent ALL)"),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Per-day, per-metric leaderboard completeness for the last N days.

    For each (day, metric) returns:
      - rows_actual: COUNT(*) for that day+metric in market.leaderboards
      - rows_expected: 1440 × 333 = 479520
      - completeness_pct: round(rows_actual / rows_expected * 100, 1)

    The expected count assumes a full TOP_N=333 leaderboard for every
    minute of the day. Days where the indexer never ran will show 0%
    rows_actual; days where it ran with a smaller TOP_N (e.g. the
    historical backfill at TOP_N=240) will show ~67% completeness.

    The current state of market.leaderboards (as of build time):
      - metric='price' has data from 2025-02-13 to 2026-03-19 at ~80,640
        rows/day from the historical backfill (TOP_N≈240, not 333), which
        will read as ~16.8% complete by the strict 1440×333 math
      - metric='open_interest' has zero rows
      - metric='volume' has zero rows
    This honest "mostly empty" view is the intended Phase 3+ Coverage page UX.
    """
    interval_str = f"{days} days"

    # Query the continuous aggregate `market.leaderboards_daily_count` instead
    # of the raw hypertable. The cagg is a pre-materialized per-day per-metric
    # row count maintained by a TimescaleDB refresh policy (every 1 hour). It
    # contains ~1k rows for ~400 days × 3 metrics, so even the [ALL] preset
    # responds in <100ms instead of scanning ~570M rows.
    cur.execute(
        """
        SELECT
            day::date AS day,
            metric,
            rows_actual
        FROM market.leaderboards_daily_count
        WHERE day >= NOW() - %s::interval
          AND anchor_hour = 0
          AND variant = 'close'
        ORDER BY day DESC, metric
        """,
        (interval_str,),
    )
    rows = cur.fetchall()

    days_payload = []
    for r in rows:
        rows_actual = int(r["rows_actual"])
        completeness_pct = round(rows_actual / ROWS_PER_FULL_DAY * 100, 1) if ROWS_PER_FULL_DAY > 0 else 0.0
        # The cagg's `day` column comes back as a python date already
        day_val = r["day"]
        day_iso = day_val.isoformat() if hasattr(day_val, "isoformat") else str(day_val)
        days_payload.append({
            "date":             day_iso,
            "metric":           r["metric"],
            "rows_actual":      rows_actual,
            "rows_expected":    ROWS_PER_FULL_DAY,
            "completeness_pct": completeness_pct,
        })

    return {
        "lookback_days":   days,
        "rows_per_full_day": ROWS_PER_FULL_DAY,
        "metrics":         METRICS,
        "days_returned":   len(days_payload),
        "days":            days_payload,
    }


# ─── /jobs ────────────────────────────────────────────────────────────────────

_JOB_SELECT = """
    SELECT
        job_id, job_type, status, metric, date_from, date_to,
        params, symbols_total, symbols_done, rows_written,
        started_at, completed_at, last_heartbeat, error_msg,
        triggered_by, run_tag, created_at,
        (status = 'running' AND (
            (last_heartbeat IS NOT NULL AND last_heartbeat < NOW() - INTERVAL '2 hours')
            OR
            (last_heartbeat IS NULL AND started_at < NOW() - INTERVAL '2 hours')
        )) AS is_stale
    FROM market.indexer_jobs
"""


@router.get("/jobs")
def list_jobs(
    limit: int = Query(50, ge=1, le=200),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """List recent indexer jobs ordered by created_at DESC.

    The table is currently empty (no script writes to it yet — see the
    Phase 0 cron diagnostic in the build doc). The frontend Jobs page
    must handle the empty state gracefully on day one.
    """
    cur.execute(
        _JOB_SELECT + " ORDER BY created_at DESC LIMIT %s",
        (limit,),
    )
    rows = cur.fetchall()
    return {
        "jobs_returned": len(rows),
        "jobs":          [_serialize_indexer_job(r) for r in rows],
    }


@router.get("/jobs/{job_id}")
def get_job(job_id: str, cur=Depends(get_cursor)) -> dict[str, Any]:
    """Single job by UUID."""
    cur.execute(_JOB_SELECT + " WHERE job_id = %s", (job_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return _serialize_indexer_job(row)


# ─── /signals ────────────────────────────────────────────────────────────────

@router.get("/signals")
def list_signals(
    days: int = Query(90, ge=1, le=365),
    source: Optional[str] = Query(
        None,
        pattern="^(live|backtest|research)$",
        description="Filter by signal_source. Omit to return all sources.",
    ),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Recent daily signals joined to their items + symbol base names.

    Returns one entry per signal_batch_id (newest first), each containing
    the full symbol list as an array. Filterable by source. Default
    lookback 90 days.

    Symbols are pre-aggregated server-side using a LATERAL subquery so the
    response is one row per signal batch, not one row per (batch, symbol).
    """
    where_source = "AND ds.signal_source = %s" if source else ""
    params: list[Any] = [f"{days} days"]
    if source:
        params.append(source)

    cur.execute(
        f"""
        SELECT
            ds.signal_batch_id,
            ds.signal_date,
            ds.strategy_version_id,
            ds.signal_source,
            ds.sit_flat,
            ds.filter_name,
            ds.filter_reason,
            ds.computed_at,
            COALESCE(items.symbol_count, 0) AS symbol_count,
            COALESCE(items.symbols, '[]'::jsonb) AS symbols
        FROM user_mgmt.daily_signals ds
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) AS symbol_count,
                jsonb_agg(
                    jsonb_build_object(
                        'rank',   dsi.rank,
                        'base',   s.base,
                        'weight', dsi.weight
                    )
                    ORDER BY dsi.rank
                ) AS symbols
            FROM user_mgmt.daily_signal_items dsi
            JOIN market.symbols s ON s.symbol_id = dsi.symbol_id
            WHERE dsi.signal_batch_id = ds.signal_batch_id
              AND dsi.is_selected = TRUE
        ) items ON TRUE
        WHERE ds.signal_date >= (NOW() - %s::interval)::date
        {where_source}
        ORDER BY ds.signal_date DESC, ds.computed_at DESC
        LIMIT 200
        """,
        tuple(params),
    )
    rows = cur.fetchall()

    return {
        "lookback_days":     days,
        "source_filter":     source,
        "signals_returned":  len(rows),
        "signals": [
            {
                "signal_batch_id":     str(r["signal_batch_id"]),
                "signal_date":         r["signal_date"].isoformat() if r["signal_date"] else None,
                "strategy_version_id": str(r["strategy_version_id"]) if r["strategy_version_id"] else None,
                "signal_source":       r["signal_source"],
                "sit_flat":            bool(r["sit_flat"]),
                "filter_name":         r["filter_name"],
                "filter_reason":       r["filter_reason"],
                "computed_at":         r["computed_at"].isoformat() if r["computed_at"] else None,
                "symbol_count":        int(r["symbol_count"] or 0),
                "symbols":             r["symbols"] or [],
            }
            for r in rows
        ],
    }


@router.get("/signals/{signal_batch_id}")
def get_signal(signal_batch_id: str, cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Single signal batch with full symbol list. Useful for the click-row
    detail view on the Signals page.
    """
    cur.execute(
        """
        SELECT
            ds.signal_batch_id,
            ds.signal_date,
            ds.strategy_version_id,
            ds.signal_source,
            ds.sit_flat,
            ds.filter_name,
            ds.filter_reason,
            ds.computed_at,
            COALESCE(items.symbol_count, 0) AS symbol_count,
            COALESCE(items.symbols, '[]'::jsonb) AS symbols
        FROM user_mgmt.daily_signals ds
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*) AS symbol_count,
                jsonb_agg(
                    jsonb_build_object(
                        'rank',   dsi.rank,
                        'base',   s.base,
                        'weight', dsi.weight
                    )
                    ORDER BY dsi.rank
                ) AS symbols
            FROM user_mgmt.daily_signal_items dsi
            JOIN market.symbols s ON s.symbol_id = dsi.symbol_id
            WHERE dsi.signal_batch_id = ds.signal_batch_id
              AND dsi.is_selected = TRUE
        ) items ON TRUE
        WHERE ds.signal_batch_id = %s
        """,
        (signal_batch_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Signal batch {signal_batch_id} not found")

    return {
        "signal_batch_id":     str(row["signal_batch_id"]),
        "signal_date":         row["signal_date"].isoformat() if row["signal_date"] else None,
        "strategy_version_id": str(row["strategy_version_id"]) if row["strategy_version_id"] else None,
        "signal_source":       row["signal_source"],
        "sit_flat":            bool(row["sit_flat"]),
        "filter_name":         row["filter_name"],
        "filter_reason":       row["filter_reason"],
        "computed_at":         row["computed_at"].isoformat() if row["computed_at"] else None,
        "symbol_count":        int(row["symbol_count"] or 0),
        "symbols":             row["symbols"] or [],
    }


# ─── /strategies ─────────────────────────────────────────────────────────────

@router.get("/strategies")
def list_strategies(cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    All strategies with their versions nested under each.

    Reads audit.strategies and audit.strategy_versions. Returns a flat
    array of strategies, each with a `versions` array. The `config_excerpt`
    field is the first 200 chars of the JSONB config as a string — full
    config is intentionally NOT exposed in the list view to keep payloads
    small. A future endpoint can return the full config for a single
    version when needed.
    """
    cur.execute(
        """
        SELECT
            s.strategy_id,
            s.name,
            s.display_name,
            s.description,
            s.filter_mode,
            s.is_published,
            s.capital_cap_usd,
            s.created_at,
            s.updated_at,
            COALESCE(v.versions, '[]'::jsonb) AS versions
        FROM audit.strategies s
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'strategy_version_id', sv.strategy_version_id,
                    'version_label',       sv.version_label,
                    'is_active',           sv.is_active,
                    'published_at',        sv.published_at,
                    'created_at',          sv.created_at,
                    'config_excerpt',      LEFT(sv.config::text, 200)
                )
                ORDER BY sv.created_at DESC
            ) AS versions
            FROM audit.strategy_versions sv
            WHERE sv.strategy_id = s.strategy_id
        ) v ON TRUE
        ORDER BY s.strategy_id
        """,
    )
    rows = cur.fetchall()

    return {
        "strategies_returned": len(rows),
        "strategies": [
            {
                "strategy_id":     r["strategy_id"],
                "name":            r["name"],
                "display_name":    r["display_name"],
                "description":     r["description"],
                "filter_mode":     r["filter_mode"],
                "is_published":    bool(r["is_published"]),
                "capital_cap_usd": float(r["capital_cap_usd"]) if r["capital_cap_usd"] is not None else None,
                "created_at":      r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at":      r["updated_at"].isoformat() if r["updated_at"] else None,
                "versions":        r["versions"] or [],
            }
            for r in rows
        ],
    }

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
from pydantic import BaseModel

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


# ─── /coverage/refresh ────────────────────────────────────────────────────────

@router.post("/coverage/refresh")
def refresh_coverage(
    days: int = Query(7, ge=1, le=90,
                      description="Refresh the last N days of cagg data."),
) -> dict[str, Any]:
    """Manually refresh the continuous aggregate the indexer coverage map
    reads from (`market.leaderboards_daily_count`).

    The nightly cron at 01:15 UTC normally keeps this aggregate current.
    But a manual leaderboard rebuild (e.g. `--force` after an anchor
    fallback fix or a metl topup) lands AFTER the cron and won't appear
    in the indexer coverage UI until tomorrow's cron — unless this
    endpoint is called.

    Uses an autocommit connection because `CALL refresh_continuous_aggregate(...)`
    cannot run inside a transaction block on some TimescaleDB versions.
    """
    import time as _time
    from ...db import get_conn  # local import to avoid pulling at module load

    conn = get_conn()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        t0 = _time.time()
        try:
            cur.execute(
                f"CALL refresh_continuous_aggregate(%s, "
                f"NOW() - INTERVAL '{int(days)} days', NOW())",
                ("market.leaderboards_daily_count",),
            )
        finally:
            cur.close()
        return {
            "refreshed": ["market.leaderboards_daily_count"],
            "days": days,
            "elapsed_seconds": round(_time.time() - t0, 2),
        }
    finally:
        conn.close()


# ─── /coverage/fill-missing ──────────────────────────────────────────────────
# Mirrors the compiler page's fill-missing pattern (see compiler.py).
# Detects partial days in market.leaderboards (any metric < 95% of
# 1440 × 333 = 479,520 rows), spawns a worker that runs
# build_intraday_leaderboard.py --force per (date, metric) tuple, then
# refreshes leaderboards_daily_count.

_FILL_MISSING_DIR_INDEXER = "/mnt/quant-data/jobs/fill_missing_indexer"


def _fill_missing_indexer_status_path(job_id: str) -> str:
    return f"{_FILL_MISSING_DIR_INDEXER}/{job_id}.json"


def _detect_partial_dates_indexer(lookback_days: int = 30) -> list[str]:
    """Return ISO dates in the last `lookback_days` where any of
    (price, open_interest, volume) has < 95% of expected rows
    (479,520 = 1440 × 333). Excludes today (UTC) — see compiler's
    _detect_partial_dates for why."""
    import datetime as _dt
    from ...db import get_conn

    today = _dt.datetime.now(_dt.timezone.utc).date()
    start = today - _dt.timedelta(days=lookback_days)
    expected_full = 1440 * 333  # 479,520
    threshold = int(expected_full * 0.95)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT day::text
            FROM (
                SELECT
                    timestamp_utc::date AS day,
                    metric,
                    COUNT(*) AS rows_in_lb
                FROM market.leaderboards
                WHERE timestamp_utc >= %s::timestamptz
                  AND timestamp_utc <  %s::timestamptz
                  AND anchor_hour = 0 AND variant = 'close'
                GROUP BY timestamp_utc::date, metric
            ) per_day_metric
            WHERE rows_in_lb < %s
              AND day < (timezone('UTC', NOW()))::date  -- never today
            GROUP BY day
            ORDER BY day DESC
        """, (start, today, threshold))
        return [r[0] for r in cur.fetchall()]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@router.post("/coverage/fill-missing")
def fill_missing_indexer(
    days_back: int = Query(30, ge=1, le=90,
                            description="Lookback window in days for "
                                        "auto-detect."),
    dates: str | None = Query(None,
                               description="Optional comma-separated "
                                           "YYYY-MM-DD list to fill instead "
                                           "of auto-detecting."),
) -> dict[str, Any]:
    """Detect partial days in market.leaderboards, spawn a background
    worker that runs `build_intraday_leaderboard.py --force` for each
    (date, metric) tuple, then refreshes leaderboards_daily_count.

    Zero-gap fast path: if no partial dates found, refreshes the cagg
    inline (~0.5-1s) and returns state=done immediately.
    """
    import os as _os
    import subprocess as _sp
    import sys as _sys
    import uuid as _uuid
    import time as _time
    import json as _json
    from ...db import get_conn

    if dates:
        target_dates = [d.strip() for d in dates.split(",") if d.strip()]
    else:
        target_dates = _detect_partial_dates_indexer(lookback_days=days_back)

    job_id = str(_uuid.uuid4())
    _os.makedirs(_FILL_MISSING_DIR_INDEXER, exist_ok=True)
    status_path = _fill_missing_indexer_status_path(job_id)

    # Zero-gap fast path
    if not target_dates:
        t0 = _time.time()
        conn = get_conn()
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                "CALL refresh_continuous_aggregate(%s, "
                "NOW() - INTERVAL '7 days', NOW())",
                ("market.leaderboards_daily_count",),
            )
            cur.close()
        finally:
            conn.close()
        elapsed = round(_time.time() - t0, 1)
        synthetic = {
            "job_id": job_id,
            "page": "indexer",
            "state": "done",
            "started_at": t0,
            "finished_at": _time.time(),
            "dates_total": 0,
            "dates_completed": 0,
            "dates_failed": 0,
            "dates": [],
            "current_date": None,
            "current_state": None,
            "elapsed_seconds": int(elapsed),
            "errors": [],
            "summary": f"No partial days found; cagg refreshed in {elapsed}s",
        }
        with open(status_path, "w") as f:
            _json.dump(synthetic, f, indent=2)
        return {
            "job_id": job_id,
            "state": "done",
            "dates_total": 0,
            "summary": synthetic["summary"],
        }

    # Spawn detached worker subprocess
    cmd = [
        _sys.executable, "-m", "app.cli.fill_missing_indexer",
        "--job-id", job_id,
        "--status-file", status_path,
        "--dates", ",".join(target_dates),
    ]
    log_path = status_path.replace(".json", ".spawn.log")
    with open(log_path, "w") as logf:
        logf.write(f"spawning: {' '.join(cmd)}\n")
        _sp.Popen(
            cmd,
            stdout=logf, stderr=_sp.STDOUT,
            stdin=_sp.DEVNULL,
            cwd="/app",
            start_new_session=True,
        )

    return {
        "job_id": job_id,
        "state": "queued",
        "dates_total": len(target_dates),
        "dates": target_dates,
    }


@router.get("/coverage/fill-missing/status")
def fill_missing_indexer_status(job_id: str) -> dict[str, Any]:
    """Read the JSON status file written by the indexer fill-missing worker."""
    import json as _json
    import os as _os

    path = _fill_missing_indexer_status_path(job_id)
    if not _os.path.exists(path):
        raise HTTPException(status_code=404, detail="status not yet written")
    try:
        with open(path) as f:
            return _json.load(f)
    except _json.JSONDecodeError:
        raise HTTPException(status_code=503, detail="status mid-write, retry")


@router.get("/coverage/fill-missing/active")
def fill_missing_indexer_active() -> dict[str, Any]:
    """Most recent in-progress indexer fill-missing job; for page-load
    rehydration. Returns {job_id: null} if none."""
    import json as _json
    import glob as _glob
    import os as _os

    if not _os.path.isdir(_FILL_MISSING_DIR_INDEXER):
        return {"job_id": None}
    candidates: list[tuple[float, dict]] = []
    for path in _glob.glob(f"{_FILL_MISSING_DIR_INDEXER}/*.json"):
        try:
            with open(path) as f:
                doc = _json.load(f)
        except Exception:
            continue
        if doc.get("page") != "indexer":
            continue
        if doc.get("state") not in ("running", "queued"):
            continue
        candidates.append((doc.get("started_at", 0), doc))
    if not candidates:
        return {"job_id": None}
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


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


# ─── POST /runs — UI-triggered backfill ─────────────────────────────────────

class RunRequest(BaseModel):
    metric: str  # "price" | "open_interest" | "volume"


@router.post("/runs")
def create_run(body: RunRequest) -> dict[str, Any]:
    """Enqueue a celery task that runs backfill_leaderboards_bulk.py for the
    given metric. Returns the celery task_id immediately. Progress is tracked
    in market.indexer_jobs and visible on the Jobs page (which polls every
    10s).

    No params beyond metric — the bulk script handles its own staging
    table, conflict logic, and clean-up. Each call is independent and safe
    to re-run idempotently."""
    if body.metric not in METRICS:
        raise HTTPException(
            status_code=400,
            detail=f"invalid metric {body.metric!r}, must be one of {METRICS}",
        )
    # Lazy import so importing this route module doesn't pull in celery+psycopg2
    # at FastAPI startup time.
    from app.workers.indexer_backfill_worker import backfill_metric
    async_result = backfill_metric.delay(body.metric, "ui")
    return {
        "ok": True,
        "metric": body.metric,
        "celery_task_id": async_result.id,
        "message": f"Backfill enqueued for {body.metric}. Watch the Jobs page for progress.",
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
            ds.conviction_roi_x AS stored_conviction,
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

    # ── Conviction gate: compute roi_x for each signal date ────────────
    # roi_x = equal-weight avg return from 06:00 to 06:35 UTC across
    # the signal's selected symbols.
    #
    # Resolution order (each falls through on NULL):
    #   1. daily_signals.conviction_roi_x — populated by the per-allocation
    #      trader at gate-evaluation time (or legacy master trader).
    #   2. allocation_returns.conviction_roi_x — written by the trader at
    #      session close / early-exit paths. Covers historical rows that
    #      predate the per-allocation daily_signals UPDATE.
    #   3. Compute from market.futures_1m (06:00 open → 06:35 close) —
    #      fallback only used when futures_1m has the bars; for today's
    #      signal_date this bucket isn't in futures_1m until tomorrow's
    #      00:15 UTC metl run, so this path is effectively
    #      yesterday-or-earlier.
    #
    # Keying: strategy_versions with different selected symbols compute
    # different conviction values on the same date. Key by
    # (signal_date, strategy_version_id) so each row resolves against
    # its own value rather than collapsing to a shared per-date value.
    KILL_Y = 0.3  # conviction threshold (%)

    ConvKey = tuple[str, str]  # (signal_date_iso, strategy_version_id)

    def _row_key(r) -> ConvKey | None:
        if not r["signal_date"] or not r["strategy_version_id"]:
            return None
        return (r["signal_date"].isoformat(), str(r["strategy_version_id"]))

    conviction_by_key: dict[ConvKey, float | None] = {}

    # Step 1: per-row stored_conviction from daily_signals.
    for r in rows:
        k = _row_key(r)
        if k and r["stored_conviction"] is not None:
            conviction_by_key[k] = round(float(r["stored_conviction"]), 4)

    # Step 2: allocation_returns fallback. Per-strategy-version JOIN so
    # each daily_signals row pulls conviction from an allocation on its
    # own version. Pick MAX arbitrarily when multiple allocations share
    # a version (conviction values agree across allocations of the same
    # version — they're computed from the same signal symbols).
    missing_keys: set[ConvKey] = {
        k for k in (_row_key(r) for r in rows)
        if k is not None and k not in conviction_by_key
    }
    if missing_keys:
        missing_dates = list({k[0] for k in missing_keys})
        cur.execute("""
            SELECT ds.signal_date,
                   ds.strategy_version_id,
                   MAX(ar.conviction_roi_x) AS conviction_roi_x
            FROM user_mgmt.daily_signals ds
            JOIN user_mgmt.allocations a
              ON a.strategy_version_id = ds.strategy_version_id
            JOIN user_mgmt.allocation_returns ar
              ON ar.allocation_id = a.allocation_id
             AND ar.session_date  = ds.signal_date
            WHERE ds.signal_date = ANY(%s::date[])
              AND ar.conviction_roi_x IS NOT NULL
            GROUP BY ds.signal_date, ds.strategy_version_id
        """, (missing_dates,))
        # allocation_returns.conviction_roi_x is stored as a fraction
        # (0.01187 = +1.187%) whereas daily_signals.conviction_roi_x is
        # stored as a percent (1.187). Scale to percent here so the
        # frontend's ConvictionBadge (which renders `${roiX.toFixed(2)}%`)
        # shows a sensible magnitude regardless of source.
        for row in cur.fetchall():
            k = (row["signal_date"].isoformat(), str(row["strategy_version_id"]))
            if k in missing_keys and k not in conviction_by_key:
                conviction_by_key[k] = round(float(row["conviction_roi_x"]) * 100, 4)

    # Step 3: compute from futures_1m for rows still missing conviction.
    # Same symbol-set per row → conviction differs only by symbol_set, so
    # group by signal_date for the SQL (cheap) then apply to every
    # (date, version) key whose symbols match. In practice strategy_versions
    # sharing a date usually share signal symbols too, so one compute
    # satisfies multiple keys.
    signal_dates = list({
        r["signal_date"] for r in rows
        if r["signal_date"]
        and int(r["symbol_count"] or 0) > 0
        and _row_key(r) not in conviction_by_key
    })
    # Legacy dict for the futures_1m fallback — keyed by date only since
    # the query aggregates across all symbols of that date's signals.
    conviction_by_date: dict[str, float | None] = {}

    if signal_dates:
        # Lightweight query: only conviction (open + bar6), no session close
        cur.execute("""
            WITH signal_syms AS (
                SELECT ds.signal_date, dsi.symbol_id
                FROM user_mgmt.daily_signals ds
                JOIN user_mgmt.daily_signal_items dsi
                  ON dsi.signal_batch_id = ds.signal_batch_id
                WHERE ds.signal_date = ANY(%s::date[])
                  AND dsi.is_selected = TRUE
            ),
            open_px AS (
                SELECT ss.signal_date, ss.symbol_id, f.close AS px
                FROM signal_syms ss
                JOIN market.futures_1m f ON f.symbol_id = ss.symbol_id
                WHERE f.source_id = 1
                  AND f.timestamp_utc >= (ss.signal_date + TIME '06:00')
                  AND f.timestamp_utc <  (ss.signal_date + TIME '06:01')
            ),
            bar6_px AS (
                SELECT ss.signal_date, ss.symbol_id, f.close AS px
                FROM signal_syms ss
                JOIN market.futures_1m f ON f.symbol_id = ss.symbol_id
                WHERE f.source_id = 1
                  AND f.timestamp_utc >= (ss.signal_date + TIME '06:35')
                  AND f.timestamp_utc <  (ss.signal_date + TIME '06:36')
            )
            SELECT o.signal_date,
                   AVG((b.px / NULLIF(o.px, 0) - 1) * 100) AS roi_x
            FROM open_px o
            JOIN bar6_px b ON b.signal_date = o.signal_date
                          AND b.symbol_id = o.symbol_id
            GROUP BY o.signal_date
        """, (signal_dates,))
        for row in cur.fetchall():
            d = row["signal_date"].isoformat()
            conviction_by_date[d] = (
                round(float(row["roi_x"]), 4) if row["roi_x"] is not None else None
            )

    def _conviction_for(r) -> float | None:
        """Resolve conviction for one row via the per-key chain, with the
        futures_1m per-date result as a last-resort fallback."""
        k = _row_key(r)
        if k is not None and k in conviction_by_key:
            return conviction_by_key[k]
        if r["signal_date"]:
            return conviction_by_date.get(r["signal_date"].isoformat())
        return None

    return {
        "lookback_days":     days,
        "source_filter":     source,
        "signals_returned":  len(rows),
        "conviction_kill_y": KILL_Y,
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
                "conviction_roi_x":    _conviction_for(r),
            }
            for r in rows
        ],
    }


@router.get("/signals/raw-roi")
def signals_raw_roi(
    days: int = Query(90, ge=1, le=365),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Compute raw (unleveraged) session return for each signal date.
    Separate endpoint because the 3-price-point query is slow.
    """
    interval_str = f"{days} days"
    cur.execute("""
        WITH signal_syms AS (
            SELECT ds.signal_date, dsi.symbol_id
            FROM user_mgmt.daily_signals ds
            JOIN user_mgmt.daily_signal_items dsi
              ON dsi.signal_batch_id = ds.signal_batch_id
            WHERE ds.signal_date >= (NOW() - %s::interval)::date
              AND dsi.is_selected = TRUE
              AND ds.sit_flat = FALSE
        ),
        open_px AS (
            SELECT ss.signal_date, ss.symbol_id, f.close AS px
            FROM signal_syms ss
            JOIN market.futures_1m f ON f.symbol_id = ss.symbol_id
            WHERE f.source_id = 1
              AND f.timestamp_utc >= (ss.signal_date + TIME '06:00')
              AND f.timestamp_utc <  (ss.signal_date + TIME '06:01')
        ),
        close_px AS (
            SELECT ss.signal_date, ss.symbol_id, f.close AS px
            FROM signal_syms ss
            JOIN market.futures_1m f ON f.symbol_id = ss.symbol_id
            WHERE f.source_id = 1
              AND f.timestamp_utc >= (ss.signal_date + TIME '23:55')
              AND f.timestamp_utc <  (ss.signal_date + TIME '23:56')
        )
        SELECT o.signal_date,
               AVG((c.px / NULLIF(o.px, 0) - 1) * 100) AS session_return
        FROM open_px o
        JOIN close_px c ON c.signal_date = o.signal_date AND c.symbol_id = o.symbol_id
        GROUP BY o.signal_date
    """, (interval_str,))

    result: dict[str, float] = {}
    for row in cur.fetchall():
        result[row["signal_date"].isoformat()] = (
            round(float(row["session_return"]), 4) if row["session_return"] is not None else 0.0
        )
    return {"raw_roi": result}


@router.get("/signals/strat-roi")
def signals_strat_roi(
    days: int = Query(90, ge=1, le=365),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Compute strategy-simulated return for each signal date. Separate
    endpoint because the bar-by-bar query is heavy (~5-10s). The
    frontend calls this after the main /signals response loads.
    """
    from collections import defaultdict

    # Constants mirror the BloFin allocation running Alpha Tail Guardrail -
    # High lev. Values lifted from audit.strategy_versions.config so the
    # strat_roi column on /indexer/signals rehydrates the same leverage +
    # exit rules + fee model the live trader uses.
    #
    # Known gap: the live trader also scales l_high by a daily vol-boost
    # (vol_lev_scaling, target_vol=0.02, max_boost=2.0, 30-day window),
    # which here would require rolling historical strat returns + a DD
    # regime filter. Without it strat_roi matches the "vol_lev off"
    # Simulator run exactly. Enabling the dynamic boost is a follow-up
    # change (~60 LOC); with it we'd match the actual eff_lev per day
    # that live BloFin uses (today ≈ 2.95× vs. our 2.0×).
    KILL_Y = 0.3                            # conviction gate at bar 6 (06:35 UTC)
    STRAT_LEVERAGE             = 2.0        # was 1.33; config.l_high
    STRAT_SL                   = -7.5       # was -6.0;  config.port_sl × 100
    STRAT_TSL                  = -9.5       # was -7.5;  config.port_tsl × 100
    STRAT_EARLY_KILL_BAR       = 35         # config.early_kill_x
    STRAT_EARLY_KILL_Y         = 0.3        # config.early_kill_y × 100
    STRAT_EARLY_FILL_Y         = 9.0        # config.early_fill_y × 100
    TAKER_FEE_PCT_PER_LEV      = 0.08       # round-trip 0.04% × 2; config.taker_fee_pct × 100
    FUNDING_DRAG_PCT_PER_LEV   = 0.02       # 2 windows/day; config.funding_rate_daily_pct × 100

    interval_str = f"{days} days"

    # Get signal dates with selected symbols
    cur.execute("""
        SELECT ds.signal_date, ds.sit_flat
        FROM user_mgmt.daily_signals ds
        WHERE ds.signal_date >= (NOW() - %s::interval)::date
    """, (interval_str,))
    sig_rows = cur.fetchall()
    active_dates = [r["signal_date"] for r in sig_rows if not r["sit_flat"]]

    if not active_dates:
        return {"strat_roi": {}}

    # Conviction values (needed for no-entry check)
    cur.execute("""
        WITH signal_syms AS (
            SELECT ds.signal_date, dsi.symbol_id
            FROM user_mgmt.daily_signals ds
            JOIN user_mgmt.daily_signal_items dsi
              ON dsi.signal_batch_id = ds.signal_batch_id
            WHERE ds.signal_date = ANY(%s::date[])
              AND dsi.is_selected = TRUE
        ),
        open_px AS (
            SELECT ss.signal_date, ss.symbol_id, f.close AS px
            FROM signal_syms ss
            JOIN market.futures_1m f ON f.symbol_id = ss.symbol_id
            WHERE f.source_id = 1
              AND f.timestamp_utc >= (ss.signal_date + TIME '06:00')
              AND f.timestamp_utc <  (ss.signal_date + TIME '06:01')
        ),
        bar6_px AS (
            SELECT ss.signal_date, ss.symbol_id, f.close AS px
            FROM signal_syms ss
            JOIN market.futures_1m f ON f.symbol_id = ss.symbol_id
            WHERE f.source_id = 1
              AND f.timestamp_utc >= (ss.signal_date + TIME '06:35')
              AND f.timestamp_utc <  (ss.signal_date + TIME '06:36')
        )
        SELECT o.signal_date,
               AVG((b.px / NULLIF(o.px, 0) - 1) * 100) AS roi_x
        FROM open_px o
        JOIN bar6_px b ON b.signal_date = o.signal_date AND b.symbol_id = o.symbol_id
        GROUP BY o.signal_date
    """, (active_dates,))
    conviction_by_date = {}
    for row in cur.fetchall():
        conviction_by_date[row["signal_date"].isoformat()] = (
            round(float(row["roi_x"]), 4) if row["roi_x"] is not None else None
        )

    # Fetch all bars 06:00→23:55 for active signal dates
    cur.execute("""
        WITH signal_syms AS (
            SELECT ds.signal_date, dsi.symbol_id
            FROM user_mgmt.daily_signals ds
            JOIN user_mgmt.daily_signal_items dsi
              ON dsi.signal_batch_id = ds.signal_batch_id
            WHERE ds.signal_date = ANY(%s::date[])
              AND dsi.is_selected = TRUE
        )
        SELECT ss.signal_date, f.timestamp_utc, ss.symbol_id, f.close
        FROM signal_syms ss
        JOIN market.futures_1m f ON f.symbol_id = ss.symbol_id
        WHERE f.source_id = 1
          AND f.timestamp_utc >= (ss.signal_date + TIME '06:00')
          AND f.timestamp_utc <= (ss.signal_date + TIME '23:55')
        ORDER BY ss.signal_date, f.timestamp_utc
    """, (active_dates,))
    bar_rows = cur.fetchall()

    bars_by_date: dict = defaultdict(lambda: defaultdict(list))
    for br in bar_rows:
        d = br["signal_date"].isoformat()
        bars_by_date[d][br["symbol_id"]].append(
            (br["timestamp_utc"], float(br["close"]))
        )

    result: dict[str, dict] = {}
    for d, syms in bars_by_date.items():
        open_prices = {}
        for sid, bars in syms.items():
            if bars:
                open_prices[sid] = bars[0][1]
        if not open_prices:
            continue

        all_ts = sorted({ts for bars in syms.values() for ts, _ in bars})
        bar_returns = []
        for ts in all_ts:
            rets = []
            for sid, bars in syms.items():
                op = open_prices.get(sid)
                if op is None or op == 0:
                    continue
                px = None
                for bts, bpx in bars:
                    if bts <= ts:
                        px = bpx
                    else:
                        break
                if px is not None:
                    rets.append((px / op - 1) * 100)
            if rets:
                bar_returns.append((ts, sum(rets) / len(rets)))

        if not bar_returns:
            continue

        conviction_roi = conviction_by_date.get(d)
        if conviction_roi is not None and conviction_roi < KILL_Y:
            result[d] = {"return_pct": 0.0, "exit_reason": "no_entry"}
            continue

        peak_ret = 0.0
        exit_ret = None
        exit_reason = "held"

        for bar_idx, (ts, ret) in enumerate(bar_returns):
            if bar_idx == STRAT_EARLY_KILL_BAR and ret < STRAT_EARLY_KILL_Y:
                exit_ret = ret
                exit_reason = "early_kill"
                break
            if ret <= STRAT_SL:
                exit_ret = STRAT_SL
                exit_reason = "stop_loss"
                break
            peak_ret = max(peak_ret, ret)
            if peak_ret > 0 and (ret - peak_ret) <= STRAT_TSL:
                exit_ret = ret
                exit_reason = "trailing_stop"
                break
            if ret >= STRAT_EARLY_FILL_Y:
                exit_ret = ret
                exit_reason = "profit_take"
                break

        if exit_ret is None:
            exit_ret = bar_returns[-1][1] if bar_returns else 0.0

        # Apply leverage, then deduct round-trip taker fees + 2-window funding
        # drag — both scaled by leverage to match the live trader's net-return
        # accounting (trader_blofin._log_allocation_return / audit simulate).
        # "Held to close" positions pay fees the same as any other exit;
        # only the conviction no-entry path above is fee-free.
        leveraged_ret = exit_ret * STRAT_LEVERAGE
        fee_drag = (TAKER_FEE_PCT_PER_LEV + FUNDING_DRAG_PCT_PER_LEV) * STRAT_LEVERAGE
        net_ret = leveraged_ret - fee_drag
        result[d] = {"return_pct": round(net_ret, 4), "exit_reason": exit_reason}

    # Also mark sit-flat dates
    for r in sig_rows:
        if r["sit_flat"]:
            result[r["signal_date"].isoformat()] = {"return_pct": 0.0, "exit_reason": "sit_flat"}

    return {"strat_roi": result}


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

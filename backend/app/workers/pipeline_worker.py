"""
Celery task that orchestrates the full audit pipeline chain:
  1. run_audit() — prestage + overlap_analysis.py --audit + metrics scrape
  2. Persist results to job store + audit.jobs row
"""

import json
import logging
from pathlib import Path

from celery import Celery

from app.core.config import settings
from app.db import get_worker_conn
from app.services.audit.pipeline_runner import JobCancelled, run_audit
from app.services.job_store import get_job, update_job

_worker_log = logging.getLogger("pipeline_worker")

celery_app = Celery("pipeline_worker", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
)


def _is_cancelled(job_id: str) -> bool:
    job = get_job(job_id)
    if not job:
        return False
    return str(job.get("status", "")).lower() in {"cancelled", "canceled"}


@celery_app.task(bind=True, name="pipeline_worker.run_pipeline")
def run_pipeline(self, job_id: str, params: dict) -> dict:
    """
    Execute the full audit pipeline for a job.

    run_audit() handles: prestage parquets (if needed) + overlap_analysis.py --audit
    + metrics scrape. This function owns the job-store orchestration and the
    audit.jobs row insert.
    """
    job_dir       = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    audit_output_path = job_dir / "audit_output.txt"

    update_job(job_id, status="running", stage="overlap", progress=5)

    # Pulse progress slowly while the pipeline runs (cap at 75).
    progress = [5]

    def _bump_progress(_line: bytes) -> None:
        if progress[0] < 75:
            progress[0] += 1
            if progress[0] % 5 == 0:
                update_job(job_id, progress=progress[0])

    try:
        metrics = run_audit(
            params,
            output_path=audit_output_path,
            progress_cb=_bump_progress,
            cancellation_cb=lambda: _is_cancelled(job_id),
            on_rebuild_start=lambda: update_job(
                job_id, status="running", stage="leaderboard_refresh", progress=2,
            ),
        )
    except JobCancelled as exc:
        update_job(job_id, status="cancelled", stage="done", error=str(exc))
        return {}
    except Exception as exc:
        update_job(job_id, status="failed", stage="overlap", error=str(exc))
        raise

    if _is_cancelled(job_id):
        update_job(job_id, status="cancelled", stage="done", error="Cancelled by user.")
        return {}

    results = {
        "metrics":            metrics,
        "audit_output_path":  str(audit_output_path),
        "starting_capital":   params.get("starting_capital", 100000.0),
        "fees_tables_by_filter": metrics.get("fees_tables_by_filter"),
        "fees_table":         metrics.get("fees_table"),
    }

    update_job(job_id, status="complete", stage="done", progress=100, results=results)

    # Best-effort: also persist a lightweight audit.jobs row. This seeds the
    # allocator-visible history without populating audit.results (promotion is
    # a separate explicit action). Any failure here is logged but must not
    # disrupt the JSON-file write above — that's still the source of truth
    # for in-progress jobs and crash recovery.
    _persist_audit_job_row(job_id, params, metrics)

    return results


def _persist_audit_job_row_at_cursor(
    cur,
    job_id: str,
    params: dict,
    metrics: dict,
    *,
    strategy_version_id: str | None = None,
) -> bool:
    """Issue the audit.jobs INSERT/UPSERT against an external cursor.

    Returns True if the row was written; False if required data (fees_table
    date range) is missing — in which case the caller should NOT commit on
    our behalf. Caller owns the enclosing transaction and must commit.

    strategy_version_id defaults to NULL (user-driven audits; promote fills
    it later). The nightly CLI passes the version it's refreshing so the
    audit.jobs row is linked from birth.
    """
    fees_table = metrics.get("fees_table") or []
    if not fees_table:
        _worker_log.warning(
            "audit.jobs insert skipped for %s: metrics.fees_table empty "
            "(cannot derive date_from/date_to)", job_id,
        )
        return False
    try:
        date_from = fees_table[0].get("date")
        date_to   = fees_table[-1].get("date")
    except (AttributeError, IndexError, TypeError):
        _worker_log.warning(
            "audit.jobs insert skipped for %s: fees_table entries lack a date key",
            job_id,
        )
        return False
    if not date_from or not date_to:
        _worker_log.warning(
            "audit.jobs insert skipped for %s: date_from=%r date_to=%r",
            job_id, date_from, date_to,
        )
        return False

    cur.execute(
        """
        INSERT INTO audit.jobs
            (job_id, strategy_version_id, status,
             completed_at, date_from, date_to, config_overrides)
        VALUES (%s, %s, 'complete', NOW(), %s, %s, %s::jsonb)
        ON CONFLICT (job_id) DO UPDATE SET
            status           = 'complete',
            completed_at     = EXCLUDED.completed_at,
            date_from        = EXCLUDED.date_from,
            date_to          = EXCLUDED.date_to,
            config_overrides = EXCLUDED.config_overrides
        """,
        (job_id, strategy_version_id, date_from, date_to, json.dumps(params)),
    )
    return True


def _persist_audit_job_row(job_id: str, params: dict, metrics: dict) -> None:
    """
    INSERT (or re-assert) the audit.jobs row at finalize. strategy_version_id
    is NULL until the admin explicitly promotes this audit as a strategy via
    POST /api/simulator/audits/{job_id}/promote.

    Date range is derived from metrics["fees_table"] (first + last row's
    `date` key). If fees_table is empty or missing both dates, we skip the
    insert — date_from/date_to are NOT NULL in the schema and we refuse to
    fabricate values.

    Thin wrapper over `_persist_audit_job_row_at_cursor` that owns its own
    connection + commit. Used by the worker's best-effort finalize hook.
    """
    try:
        conn = get_worker_conn()
        try:
            with conn.cursor() as cur:
                if _persist_audit_job_row_at_cursor(cur, job_id, params, metrics):
                    conn.commit()
        finally:
            conn.close()
    except Exception as e:
        _worker_log.warning("audit.jobs insert failed for %s: %s", job_id, e)


# ─── Side-effect import: register additional task modules ───────────────────
# `app.workers.indexer_backfill_worker` defines @celery_app.task(...) functions
# against the SAME celery_app instance from this file. Importing it here at
# module load time ensures the celery worker process picks up those task
# definitions when it starts via:
#   celery -A app.workers.pipeline_worker.celery_app worker
# Without this import the new tasks would only be discovered if the worker
# command was changed to load multiple modules. Keep this at the bottom of
# the file so it runs after `celery_app` is fully constructed.
import app.workers.indexer_backfill_worker  # noqa: E402,F401
import app.workers.run_jobs_worker  # noqa: E402,F401

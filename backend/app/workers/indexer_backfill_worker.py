"""
backend/app/workers/indexer_backfill_worker.py
==============================================
Celery task that runs `pipeline/db/backfill_leaderboards_bulk.py` for one
metric, with progress tracked in `market.indexer_jobs` so the Indexer
Jobs page (which polls /api/indexer/jobs) can show live status.

Triggered by POST /api/indexer/runs (see backend/app/api/routes/indexer.py).

This worker is intentionally separate from `pipeline_worker.py` because:
  - it talks directly to `market.indexer_jobs` (postgres) instead of the
    flat-file `app.services.job_store` used by the simulator pipeline
  - it wraps a single, parametrised script (one metric per call) rather
    than the full audit chain
  - keeping it isolated avoids complicating the existing simulator path
"""

import subprocess
from pathlib import Path

from app.core.config import settings
from app.db import get_worker_conn
# Register tasks against the SHARED celery_app from pipeline_worker so the
# existing `celery -A app.workers.pipeline_worker.celery_app worker` picks
# them up without a docker-compose change.
from app.workers.pipeline_worker import celery_app

# Path to the bulk backfill script. Resolved relative to PIPELINE_DIR
# (which inside the celery container is /app/pipeline).
_PIPELINE_DIR = Path(settings.PIPELINE_DIR)
_BACKFILL_SCRIPT = _PIPELINE_DIR / "db" / "backfill_leaderboards_bulk.py"

VALID_METRICS = ("price", "open_interest", "volume")


def _job_create(metric: str, triggered_by: str = "ui") -> str:
    """Insert a new row in market.indexer_jobs and return its job_id."""
    conn = get_worker_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO market.indexer_jobs
            (job_type, status, metric, params, triggered_by, run_tag, started_at, last_heartbeat)
        VALUES ('leaderboard', 'running', %s, '{}'::jsonb, %s, 'ui-backfill', NOW(), NOW())
        RETURNING job_id
        """,
        (metric, triggered_by),
    )
    job_id = str(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()
    return job_id


def _job_complete(job_id: str, rows_written: int) -> None:
    conn = get_worker_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE market.indexer_jobs
        SET status='complete', completed_at=NOW(), last_heartbeat=NOW(), rows_written=%s
        WHERE job_id=%s
        """,
        (rows_written, job_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def _job_fail(job_id: str, error_msg: str) -> None:
    conn = get_worker_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE market.indexer_jobs
        SET status='failed', completed_at=NOW(), last_heartbeat=NOW(), error_msg=%s
        WHERE job_id=%s
        """,
        (str(error_msg)[:2000], job_id),
    )
    conn.commit()
    cur.close()
    conn.close()


@celery_app.task(bind=True, name="indexer_backfill_worker.backfill_metric")
def backfill_metric(self, metric: str, triggered_by: str = "ui") -> dict:
    """Run backfill_leaderboards_bulk.py --metric <metric>, track in
    market.indexer_jobs. Returns the job_id and exit info."""
    if metric not in VALID_METRICS:
        raise ValueError(f"invalid metric: {metric!r}, must be one of {VALID_METRICS}")
    if not _BACKFILL_SCRIPT.exists():
        raise FileNotFoundError(f"backfill script not found at {_BACKFILL_SCRIPT}")

    job_id = _job_create(metric, triggered_by=triggered_by)

    cmd = [
        "python3",
        str(_BACKFILL_SCRIPT),
        "--metric", metric,
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            _job_fail(job_id, f"exit={proc.returncode} stderr={proc.stderr[-1000:]}")
            return {
                "job_id": job_id,
                "status": "failed",
                "exit_code": proc.returncode,
                "stderr_tail": proc.stderr[-500:],
            }

        # Parse rows inserted from stdout if available — the bulk script
        # prints "Rows inserted:  <N>" near the end. Best-effort.
        rows_written = 0
        for line in reversed(proc.stdout.splitlines()):
            if "Rows inserted" in line:
                try:
                    rows_written = int(line.split(":")[-1].strip().replace(",", ""))
                except (ValueError, IndexError):
                    pass
                break

        _job_complete(job_id, rows_written)
        return {
            "job_id": job_id,
            "status": "complete",
            "rows_written": rows_written,
        }
    except Exception as exc:
        _job_fail(job_id, str(exc))
        raise

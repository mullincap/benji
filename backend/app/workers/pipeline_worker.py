"""
Celery task that orchestrates the full audit pipeline chain:
  1. run_audit() — prestage + overlap_analysis.py --audit + metrics scrape
  2. Ingest per-day baskets from eligibility_gate_report.csv into
     audit.daily_baskets (added 2026-04-26 — closes the gap where the
     audit pipeline produced the trusted symbol-per-day list but never
     persisted it to a queryable table)
  3. Persist results to job store + audit.jobs row
"""

import csv
import json
import logging
import shutil
from pathlib import Path
from typing import Optional

from celery import Celery
from celery.schedules import crontab

from app.core.config import settings
from app.db import get_worker_conn
from app.services.audit.pipeline_runner import (
    JobCancelled,
    run_audit_with_blofin_variants,
)
from app.services.job_store import get_job, update_job

_worker_log = logging.getLogger("pipeline_worker")

celery_app = Celery("pipeline_worker", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    timezone="UTC",
)

# Beat schedule — pre-warm the EMA20 cache for every active-position
# symbol on each timeframe's bar-close boundary. Without this the Live
# tab's first MA Alignment render after a bar close adds ~1.15s of
# Binance round-trip latency; with it, the first request hits a hot
# cache and returns in tens of ms. See ema_warmer.warm_ema_cache.
#
# Each entry fires AT the bar boundary (00m for 5m bars, 00m+15m+30m+45m
# for 15m, etc.). The cache key in compute_and_cache_ema20 already
# includes latest_closed_bar_open_ms, so any minor drift between the
# beat tick and the actual bar close is harmless — the warmer always
# writes the right key.
celery_app.conf.beat_schedule = {
    "ema-warm-5m":  {"task": "ema_warmer.warm_ema_cache", "args": ("5m",),  "schedule": crontab(minute="*/5")},
    "ema-warm-15m": {"task": "ema_warmer.warm_ema_cache", "args": ("15m",), "schedule": crontab(minute="*/15")},
    "ema-warm-30m": {"task": "ema_warmer.warm_ema_cache", "args": ("30m",), "schedule": crontab(minute="*/30")},
    "ema-warm-1h":  {"task": "ema_warmer.warm_ema_cache", "args": ("1h",),  "schedule": crontab(minute=0)},
    "ema-warm-4h":  {"task": "ema_warmer.warm_ema_cache", "args": ("4h",),  "schedule": crontab(minute=0, hour="*/4")},
    "ema-warm-8h":  {"task": "ema_warmer.warm_ema_cache", "args": ("8h",),  "schedule": crontab(minute=0, hour="*/8")},
    "ema-warm-1d":  {"task": "ema_warmer.warm_ema_cache", "args": ("1d",),  "schedule": crontab(minute=0, hour=0)},
}


def _is_cancelled(job_id: str) -> bool:
    job = get_job(job_id)
    if not job:
        return False
    return str(job.get("status", "")).lower() in {"cancelled", "canceled"}


# ---------------------------------------------------------------------------
# Daily basket ingestion
# ---------------------------------------------------------------------------
#
# rebuild_portfolio_matrix.py writes eligibility_gate_report.csv to
# $BASE_DATA_DIR (= /mnt/quant-data in container) at the end of the
# audit subprocess. It contains per-day:
#   date, col_name, n_deployed, n_eligible, n_dropped,
#   deployed (pipe-joined), eligible (pipe-joined),
#   dropped (pipe-joined), drop_reasons
#
# IMPORTANT: the `eligible` column string is set BEFORE the no-data
# filter pass (rebuild_portfolio_matrix.py:597-598). After that pass,
# `n_eligible` is decremented and `dropped` is appended with no-data
# bases — but the `eligible` STRING is not updated. To get the
# actually-traded basket we must compute  eligible_set - dropped_set.
#
# rebuild_portfolio_matrix.py writes the file with a relative path
# (`OUTPUT_GATE_REPORT = "eligibility_gate_report.csv"`), so it lands
# in the cwd of the audit subprocess — which is settings.PIPELINE_DIR
# (= /app/pipeline in container). Despite overlap_analysis.py:2264
# referencing BASE_DIR / "eligibility_gate_report.csv", that path is
# only used as an "expected location" for output_files reporting; the
# actual write target is the script's cwd. Confirmed empirically on
# 2026-04-26 with audit job d4df15e1.
#
# The file is shared across audits in pipeline_dir; we snapshot it
# into job_dir immediately after the subprocess returns so a
# concurrent audit can't overwrite it before we ingest.

_GATE_REPORT_NAME = "eligibility_gate_report.csv"

# Image extensions to copy from the audit's run_dir into job_dir/charts/
_CHART_EXTS = {".png", ".jpg", ".jpeg", ".svg"}
# Marker line audit.py prints at startup so the worker can locate run_dir
_AUDIT_RUN_DIR_MARKER = "[AUDIT_RUN_DIR]"


def _snapshot_audit_charts(job_id: str, job_dir: Path) -> int:
    """Copy chart PNG/JPG files from the audit's timestamped run_dir(s)
    into job_dir/charts/ so the frontend can serve them.

    The audit prints a `[AUDIT_RUN_DIR] <abs path>` line at startup
    (audit.py main()). For BloFin variants=both we get two such lines
    (one per pass). Charts from both passes are merged into job_dir/
    charts/, with BloFin-pass files prefixed `blofin_` to avoid name
    collisions with the vanilla pass.

    Returns the number of files copied. Best-effort — any error is
    logged and swallowed.
    """
    out_dir = job_dir / "charts"
    audit_log = job_dir / "audit_output.txt"
    if not audit_log.exists():
        return 0
    try:
        run_dirs: list[Path] = []
        with audit_log.open() as fh:
            for line in fh:
                if _AUDIT_RUN_DIR_MARKER in line:
                    parts = line.strip().split(_AUDIT_RUN_DIR_MARKER, 1)
                    if len(parts) != 2:
                        continue
                    p = Path(parts[1].strip())
                    if p.is_dir():
                        if p not in run_dirs:
                            run_dirs.append(p)
                        continue
                    # audit.py renames run_dir at end-of-audit by appending
                    # `_sh<sharpe>` (e.g. run_20260430_040142 →
                    # run_20260430_040142_sh3.646). The marker line above
                    # was printed at startup with the pre-rename path, so
                    # fall back to a glob on `<basename>*` in the parent.
                    parent = p.parent
                    if not parent.is_dir():
                        continue
                    matches = sorted(parent.glob(f"{p.name}*"))
                    for m in matches:
                        if m.is_dir() and m not in run_dirs:
                            run_dirs.append(m)
                            break  # first match — same start timestamp
        if not run_dirs:
            return 0
        out_dir.mkdir(parents=True, exist_ok=True)
        copied = 0
        for idx, run_dir in enumerate(run_dirs):
            # First run = vanilla / single-mode; second run = blofin variant.
            # Prefix blofin files so they don't collide with vanilla.
            prefix = "blofin_" if idx >= 1 else ""
            # rglob walks subdirectories. audit.py writes parameter-sweep
            # plots into run_dir/parameter_sweeps/, ridge-map / plateau-
            # detector grids included. Preserve the relative subpath in
            # the snapshot so the gallery can group by category and so
            # filenames stay unique across categories.
            for src in run_dir.rglob("*"):
                if not src.is_file() or src.suffix.lower() not in _CHART_EXTS:
                    continue
                rel = src.relative_to(run_dir)
                # Apply the blofin_ prefix to the filename only, not to
                # any parent dirs — the prefix disambiguates files of
                # the same name across passes, and parent dirs already
                # group by category.
                rel_dir = rel.parent
                dst = out_dir / rel_dir / f"{prefix}{rel.name}"
                try:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                    copied += 1
                except Exception as e:
                    _worker_log.warning(
                        "charts: failed to copy %s for job %s: %s",
                        rel, job_id, e,
                    )
        _worker_log.info(
            "charts: copied %d files from %d run dir(s) into %s for job %s",
            copied, len(run_dirs), out_dir, job_id,
        )
        return copied
    except Exception as e:
        _worker_log.warning(
            "charts: snapshot failed for job %s: %s", job_id, e,
        )
        return 0


def _split_pipe(value: Optional[str]) -> list[str]:
    """Split a pipe-joined string into a list of bases, dropping empties."""
    if not value:
        return []
    return [b for b in value.split("|") if b]


def _read_baskets(gate_report_path: Path) -> list[tuple[str, list[str]]]:
    """Parse eligibility_gate_report.csv and return [(date, basket), ...].

    basket = sorted(set(eligible) - set(dropped)) so the no-data drop
    that's NOT reflected in the `eligible` string is excluded.
    """
    rows: list[tuple[str, list[str]]] = []
    with gate_report_path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            date_str = (r.get("date") or "").strip()
            if not date_str:
                continue
            eligible = set(_split_pipe(r.get("eligible")))
            dropped = set(_split_pipe(r.get("dropped")))
            traded = sorted(eligible - dropped)
            rows.append((date_str, traded))
    return rows


def _ingest_daily_baskets(job_id: str, gate_report_path: Path) -> int:
    """UPSERT one row per (job_id, date) into audit.daily_baskets.

    Returns the number of rows written. Best-effort: any DB error is
    logged and swallowed so the audit job is still marked complete
    (basket persistence is observability, not load-bearing for the
    audit's own metrics).
    """
    try:
        baskets = _read_baskets(gate_report_path)
    except Exception as e:
        _worker_log.warning(
            "audit.daily_baskets: failed reading %s for job %s: %s",
            gate_report_path, job_id, e,
        )
        return 0
    if not baskets:
        _worker_log.warning(
            "audit.daily_baskets: %s for job %s contained 0 rows",
            gate_report_path, job_id,
        )
        return 0

    written = 0
    try:
        conn = get_worker_conn()
        try:
            with conn.cursor() as cur:
                for date_str, basket in baskets:
                    cur.execute(
                        """
                        INSERT INTO audit.daily_baskets (job_id, date, basket)
                        VALUES (%s::uuid, %s::date, %s)
                        ON CONFLICT (job_id, date) DO UPDATE
                           SET basket     = EXCLUDED.basket,
                               created_at = NOW()
                        """,
                        (job_id, date_str, basket),
                    )
                    written += 1
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        _worker_log.warning(
            "audit.daily_baskets: insert failed for job %s after %d rows: %s",
            job_id, written, e,
        )
        return written

    _worker_log.info(
        "audit.daily_baskets: wrote %d row(s) for job %s", written, job_id,
    )
    return written


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

    update_job(job_id, status="running", stage="overlap", progress=5)

    # Pulse progress slowly while the pipeline runs (cap at 75).
    progress = [5]

    def _bump_progress(_line: bytes) -> None:
        if progress[0] < 75:
            progress[0] += 1
            if progress[0] % 5 == 0:
                update_job(job_id, progress=progress[0])

    try:
        # Delegates BloFin variant orchestration:
        #   blofin_variants="off"          → single run as before
        #   blofin_variants="blofin_only"  → single run with BloFin universe applied
        #   blofin_variants="both"         → two runs (vanilla + BloFin), merged
        metrics = run_audit_with_blofin_variants(
            params,
            output_dir=job_dir,
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

    # Persist the audit.jobs row FIRST — audit.daily_baskets has a FK to
    # audit.jobs.job_id, so any later ingest needs the parent row in
    # place. Previously this lived AFTER _ingest_daily_baskets, which
    # caused every JobRequest run since the FK was added (commit f8f3a4e)
    # to silently lose its per-day basket detail (FK violation logged +
    # swallowed). Discovered 2026-04-29 during the dispersion-lag sweep.
    # Best-effort: failure here is logged but doesn't disrupt anything.
    _persist_audit_job_row(job_id, params, metrics)

    # Snapshot the gate report into job_dir BEFORE ingestion so a
    # concurrent audit can't overwrite the shared file at
    # $PIPELINE_DIR/eligibility_gate_report.csv between when this
    # subprocess finished and when we read it. The job-dir copy is
    # also retained for diagnostics.
    src_gate_report = Path(settings.PIPELINE_DIR) / _GATE_REPORT_NAME
    if src_gate_report.exists():
        try:
            job_gate_report = job_dir / _GATE_REPORT_NAME
            shutil.copy2(src_gate_report, job_gate_report)
            _ingest_daily_baskets(job_id, job_gate_report)
        except Exception as e:
            _worker_log.warning(
                "audit.daily_baskets: snapshot/ingest failed for %s: %s",
                job_id, e,
            )
    else:
        _worker_log.warning(
            "audit.daily_baskets: %s missing after subprocess for job %s — "
            "skipping basket ingestion", src_gate_report, job_id,
        )

    # Snapshot the audit's chart PNGs into job_dir/charts/ so the
    # frontend's Full Report tab can serve them. Pipeline writes them
    # to a timestamped run_<TS>/ subdir at $PIPELINE_DIR/audit_outputs_*/.
    # We scrape "[AUDIT_RUN_DIR] <abs path>" lines from the captured
    # stdout (one per run for vanilla, one for BloFin) and copy *.png /
    # *.jpg from each into job_dir/charts/. Best-effort — chart absence
    # doesn't disrupt the rest of the result write.
    _snapshot_audit_charts(job_id, job_dir)

    # Pre-compute the BO/BF attribution analysis so the Summary tab's
    # Attribution panel loads instantly instead of waiting on a ~30-130s
    # synchronous compute the first time a user opens the audit. Result
    # is written to job_dir/attribution.json — the GET endpoint reads
    # cache-first. Best-effort: failure is logged, doesn't disrupt the
    # rest of the result write.
    try:
        from app.api.routes.jobs import _compute_attribution
        from app.services.job_store import get_job as _get_job
        _live_job = _get_job(job_id)
        if _live_job is not None:
            _attr = _compute_attribution(job_id, _live_job)
            (job_dir / "attribution.json").write_text(json.dumps(_attr))
            _worker_log.info(
                "attribution: precomputed and cached for job %s in %.1fs",
                job_id, _attr.get("elapsed_sec", 0),
            )
    except Exception as e:
        _worker_log.warning(
            "attribution: precompute failed for job %s: %s", job_id, e,
        )

    # audit_output.txt is written by the wrapper:
    #   off / blofin_only → directly at this path
    #   both              → concatenated from audit_output_vanilla.txt
    #                       and audit_output_blofin.txt at end of wrapper
    results = {
        "metrics":            metrics,
        "audit_output_path":  str(job_dir / "audit_output.txt"),
        "starting_capital":   params.get("starting_capital", 100000.0),
        "fees_tables_by_filter": metrics.get("fees_tables_by_filter"),
        "fees_table":         metrics.get("fees_table"),
    }

    update_job(job_id, status="complete", stage="done", progress=100, results=results)

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
import app.workers.ema_warmer  # noqa: E402,F401

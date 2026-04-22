"""Nightly refresh of current_metrics for active published strategies.

For each (is_active=TRUE, is_published=TRUE) strategy_version:
  1. Load config from audit.strategy_versions, strategy.filter_mode from audit.strategies
  2. Run the audit pipeline (run_audit handles prestage + subprocess + parse)
  3. Metrics are returned from run_audit (same shape as the pre-refactor _parse_metrics)
  4. Insert a fresh audit.jobs + audit.results row (audit trail preserved)
  5. Update strategy_version.current_metrics JSONB with the picked-filter payload

Invoked daily by host cron at 01:30 UTC via `docker compose exec`.

CLI
---
  python -m app.cli.refresh_strategy_metrics
  python -m app.cli.refresh_strategy_metrics --dry-run
  python -m app.cli.refresh_strategy_metrics --strategy-version-id <uuid>
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.db import get_worker_conn
from app.services.audit.current_metrics import build_current_metrics
from app.services.audit.pipeline_runner import run_audit
from app.services.audit.vol_boost import compute_strategy_vol_boost
# Reuse the existing worker's job-row upsert — don't duplicate logic.
from app.workers.pipeline_worker import _persist_audit_job_row_at_cursor
# _build_result_row + column maps live in the promote route.
from app.api.routes.simulator import _build_result_row

log = logging.getLogger("refresh_strategy_metrics")

# Simulator UI's Audit History panel reads from the disk job_store
# (/app/backend/jobs/{id}/job.json), not audit.jobs DB. Without a disk
# breadcrumb, nightly refreshes stay invisible in the UI even though
# the trader is already consuming the fresh metrics. Mirror the
# Celery worker's completion write here so each night's fresh fees_table
# is browsable in the Simulator view.
_PUBLISHED_FOLDER_ID = "b24a9618-2759-4277-b01f-7479da731db7"


def _write_nightly_job_json(
    job_id: str,
    sv: dict[str, Any],
    metrics: dict[str, Any],
    params: dict[str, Any],
    audit_output_path: Path,
) -> None:
    """Persist a disk job.json breadcrumb so the Simulator UI can see
    this nightly refresh in its Audit History panel.

    Overwrite-in-place: deletes the previous nightly's disk job for this
    strategy_version first, so Audit History shows exactly one
    "Nightly — <display_name>" entry per strategy. DB audit trail is
    untouched — audit.jobs / audit.results keep full history.
    """
    jobs_dir = Path(settings.JOBS_DIR)
    nightly_tag = f"nightly:{sv['strategy_version_id']}"

    for path in jobs_dir.glob("*/job.json"):
        try:
            old = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if old.get("nightly_tag") == nightly_tag:
            shutil.rmtree(path.parent, ignore_errors=True)

    job_dir = jobs_dir / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "metrics":               metrics,
        "audit_output_path":     str(audit_output_path),
        "starting_capital":      params.get("starting_capital", 100000.0),
        "fees_tables_by_filter": metrics.get("fees_tables_by_filter"),
        "fees_table":            metrics.get("fees_table"),
    }

    now = time.time()
    label_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    job_json = {
        "id":           job_id,
        "display_name": f"Nightly {label_date} — {sv['strategy_display_name']}",
        "folder_id":    _PUBLISHED_FOLDER_ID,
        "status":       "complete",
        "stage":        "done",
        "progress":     100,
        "params":       params,
        "results":      results,
        "error":        None,
        "created_at":   now,
        "updated_at":   now,
        "nightly_tag":  nightly_tag,
    }
    (job_dir / "job.json").write_text(json.dumps(job_json, indent=2))


def _fetch_active_published_versions() -> list[dict[str, Any]]:
    """Query the set of strategy versions eligible for nightly refresh.

    Eligibility: sv.is_active AND strategies.is_published. Inactive published
    versions keep their last-known metrics frozen.
    """
    conn = get_worker_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    sv.strategy_version_id,
                    sv.strategy_id,
                    sv.version_label,
                    sv.config,
                    sv.config_hash,
                    s.name          AS strategy_name,
                    s.display_name  AS strategy_display_name,
                    s.filter_mode   AS strategy_filter_mode
                FROM audit.strategy_versions sv
                JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
                WHERE sv.is_active = TRUE
                  AND s.is_published = TRUE
                ORDER BY s.name, sv.version_label
                """
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def _run_audit_for_version(sv: dict[str, Any]) -> tuple[dict, Path, str]:
    """Run prestage + audit subprocess for one version.

    Returns (metrics, audit_output_path, job_id). Caller writes DB rows.
    Raises if the audit subprocess fails.
    """
    short_id = str(sv["strategy_version_id"])[:8]
    job_id = str(uuid.uuid4())
    params = dict(sv["config"] or {})

    # Scratch dir for this run's stdout. Audit trail is in audit.jobs/results,
    # not the raw log — tempdir is acceptable.
    job_dir = Path(tempfile.mkdtemp(prefix=f"nightly_refresh_{short_id}_"))
    audit_output_path = job_dir / "audit_output.txt"

    log.info(f"  [{short_id}] job_id={job_id}  output={audit_output_path}")

    metrics = run_audit(
        params,
        output_path=audit_output_path,
        on_rebuild_start=lambda: log.info(
            f"  [{short_id}] leaderboard parquets stale; rebuilding"
        ),
    )
    return metrics, audit_output_path, job_id


def refresh_one(sv: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    """Refresh one strategy version. Returns an outcome dict.

    Raises on failure; caller's try/except scopes per-version isolation.
    """
    short_id = str(sv["strategy_version_id"])[:8]
    picked_filter_name = sv["strategy_filter_mode"]
    log.info(
        f"{sv['strategy_name']} {sv['version_label']} [{short_id}]: start "
        f"filter_mode={picked_filter_name!r}"
    )

    if dry_run:
        params = dict(sv["config"] or {})
        log.info(f"  [{short_id}] [dry-run] config keys ({len(params)}): {sorted(params.keys())[:10]}...")
        log.info(f"  [{short_id}] [dry-run] would scrape filter row for: {picked_filter_name!r}")
        return {"status": "dry-run", "job_id": None}

    metrics, audit_output_path, job_id = _run_audit_for_version(sv)

    filter_rows = metrics.get("filters") or []
    picked_entry = next(
        (f for f in filter_rows if f.get("filter") == picked_filter_name),
        None,
    )
    if picked_entry is None:
        available = [f.get("filter") for f in filter_rows]
        raise ValueError(
            f"Audit output has no filter row matching "
            f"strategy.filter_mode={picked_filter_name!r}. "
            f"Available filters: {available}. audit_output: {audit_output_path}"
        )

    starting_capital = float(
        (sv["config"] or {}).get("starting_capital") or 100000.0
    )
    is_best = metrics.get("best_filter") == picked_filter_name
    result_row = _build_result_row(
        picked_entry, metrics, starting_capital, is_best_filter=is_best,
    )

    # All writes in a single transaction: audit.jobs row, audit.results row,
    # and the denormalized current_metrics JSONB. audit.jobs/results stay
    # append-only; current_metrics is a read-cache for the allocator card.
    params_for_audit = dict(sv["config"] or {})

    # Prefer the picked-filter's fees_table (= strategy.filter_mode) so
    # vol_boost is computed from the same simulated returns we publish
    # metrics for. Falls back to metrics["fees_table"] (best_filter's) if
    # absent — a no-op for current production where best_filter ==
    # filter_mode for all three alpha strategies.
    fees_tables_by_filter = metrics.get("fees_tables_by_filter") or {}
    fees_table = fees_tables_by_filter.get(picked_filter_name) or (
        metrics.get("fees_table") or []
    )
    data_through = None
    if fees_table and fees_table[-1].get("date"):
        data_through = fees_table[-1]["date"]

    # ret_net is in percent (e.g. 25.11 = 25.11%); the sibling expects
    # decimals. Flat-day handling is documented in the function's
    # docstring — flats ARE included as 0.0.
    daily_rets = [
        float(f["ret_net"]) / 100.0
        for f in fees_table
        if f.get("ret_net") is not None
    ]
    vol_boost = compute_strategy_vol_boost(daily_rets)
    log.info(
        f"  [{short_id}] vol_boost={vol_boost:.4f} "
        f"(from {len(daily_rets)} daily returns)"
    )

    conn = get_worker_conn()
    try:
        with conn.cursor() as cur:
            wrote_jobs_row = _persist_audit_job_row_at_cursor(
                cur, job_id, params_for_audit, metrics,
                strategy_version_id=str(sv["strategy_version_id"]),
            )
            if not wrote_jobs_row:
                raise RuntimeError(
                    f"audit.jobs row could not be written for {job_id} "
                    f"(fees_table empty/malformed). Refusing to insert audit.results "
                    f"without a linked jobs row."
                )

            columns = ["job_id", "filter_mode"] + list(result_row.keys())
            placeholders = ["%s"] * len(columns)
            values: list[Any] = [job_id, picked_filter_name] + list(result_row.values())
            cur.execute(
                f"""
                INSERT INTO audit.results ({", ".join(columns)})
                VALUES ({", ".join(placeholders)})
                RETURNING result_id
                """,
                values,
            )
            result_id = str(cur.fetchone()[0])

            current_metrics = build_current_metrics(result_row)
            current_metrics["vol_boost"] = round(vol_boost, 4)
            cur.execute(
                """
                UPDATE audit.strategy_versions
                SET current_metrics       = %s::jsonb,
                    metrics_updated_at    = NOW(),
                    metrics_data_through  = %s
                WHERE strategy_version_id = %s
                """,
                (json.dumps(current_metrics), data_through, sv["strategy_version_id"]),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # Best-effort: write a disk job.json so the Simulator UI sees this refresh.
    # DB writes above are the source of truth for the trader — a failure here
    # should never surface a refresh as failed. Log and move on.
    try:
        _write_nightly_job_json(
            job_id, sv, metrics, params_for_audit, audit_output_path,
        )
    except Exception as e:
        log.warning(f"  [{short_id}] disk job.json write failed: {e}")

    return {
        "status":       "ok",
        "job_id":       job_id,
        "result_id":    result_id,
        "is_best":      is_best,
        "data_through": str(data_through) if data_through else None,
        "preview": {
            "total_return_pct": result_row.get("total_return_pct"),
            "sharpe":           result_row.get("sharpe"),
            "cagr_pct":         result_row.get("cagr_pct"),
            "max_dd_pct":       result_row.get("max_dd_pct"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="List eligible versions and the filter they'd scrape; do not run the audit.")
    parser.add_argument("--strategy-version-id", type=str, default=None,
                        help="Refresh a single strategy_version_id (UUID) instead of all eligible.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log.info(f"refresh_strategy_metrics start  utc={datetime.now(timezone.utc).isoformat()}")

    try:
        versions = _fetch_active_published_versions()
    except Exception as e:
        log.exception(f"Failed to fetch eligible versions: {e}")
        return 1

    if args.strategy_version_id:
        versions = [v for v in versions if str(v["strategy_version_id"]) == args.strategy_version_id]
        if not versions:
            log.error(f"No eligible (active+published) version matches id={args.strategy_version_id}")
            return 1

    if not versions:
        log.info("No eligible strategy versions. Exiting.")
        return 0

    log.info(f"Found {len(versions)} eligible version(s)")

    ok = 0
    fail = 0
    for sv in versions:
        try:
            result = refresh_one(sv, dry_run=args.dry_run)
            log.info(f"  {sv['strategy_name']} {sv['version_label']}: {result}")
            ok += 1
        except Exception as e:
            log.exception(
                f"  FAIL {sv['strategy_name']} {sv['version_label']}: {e}"
            )
            fail += 1

    log.info(f"refresh_strategy_metrics done. ok={ok} fail={fail}")
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

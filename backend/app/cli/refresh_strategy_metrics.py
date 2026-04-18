"""Nightly refresh of current_metrics for active published strategies.

For each (is_active=TRUE, is_published=TRUE) strategy_version:
  1. Load config from audit.strategy_versions, strategy.filter_mode from audit.strategies
  2. Run the audit pipeline (prestage_parquet + run_audit_subprocess)
  3. Parse metrics from audit_output.txt (same _parse_metrics as pipeline_worker)
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
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import settings
from app.db import get_worker_conn
from app.services.audit.current_metrics import build_current_metrics
from app.services.audit.pipeline_runner import (
    build_cli_args,
    build_pipeline_env,
    prestage_parquet,
    run_audit_subprocess,
)
# Reuse the existing worker's parser + job-row upsert — don't duplicate logic.
from app.workers.pipeline_worker import (
    _parse_metrics,
    _persist_audit_job_row_at_cursor,
)
# _build_result_row + column maps live in the promote route.
from app.api.routes.simulator import _build_result_row

log = logging.getLogger("refresh_strategy_metrics")


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

    pipeline_env = build_pipeline_env(params)
    pipeline_dir = Path(settings.PIPELINE_DIR)
    overlap_script = pipeline_dir / "overlap_analysis.py"
    cmd = [settings.PIPELINE_PYTHON, str(overlap_script)] + build_cli_args(params)

    prestage_parquet(
        params,
        pipeline_env=pipeline_env,
        pipeline_dir=pipeline_dir,
        on_rebuild_start=lambda: log.info(f"  [{short_id}] leaderboard parquets stale; rebuilding"),
    )

    run_audit_subprocess(
        cmd=cmd,
        output_path=audit_output_path,
        cwd=pipeline_dir,
        env=pipeline_env,
        on_line=None,       # no progress reporting for nightly
        cancelled=None,     # runs to completion
    )

    metrics = _parse_metrics(audit_output_path)
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
    fees_table = metrics.get("fees_table") or []
    data_through = None
    if fees_table and fees_table[-1].get("date"):
        data_through = fees_table[-1]["date"]

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

#!/usr/bin/env python3
"""fill_missing_indexer.py — background worker that fixes incomplete days
in market.leaderboards by re-running build_intraday_leaderboard.py with
--force for each (metric, date), then refreshing the leaderboards_daily_count
continuous aggregate.

Spawned by POST /api/indexer/coverage/fill-missing as a detached subprocess.
Writes progress to a JSON status file so the FastAPI status endpoint and the
frontend's polling loop can show live progress.

Usage:
    python -m app.cli.fill_missing_indexer \\
        --job-id <uuid> \\
        --status-file /mnt/quant-data/jobs/fill_missing_indexer/<uuid>.json \\
        --dates 2026-04-25,2026-04-22
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Allow `from pipeline.db.connection import get_conn` to resolve when this
# script runs from the backend container.
sys.path.insert(0, "/app")

PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", "/app/pipeline"))
INDEXER_SCRIPT = PIPELINE_DIR / "indexer" / "build_intraday_leaderboard.py"

CAGG_NAME = "market.leaderboards_daily_count"
METRICS = ("price", "open_interest", "volume")


def _now() -> float:
    return time.time()


def _write_status(status_file: Path, status: dict) -> None:
    """Atomic-ish write: write to .tmp then rename."""
    tmp = status_file.with_suffix(status_file.suffix + ".tmp")
    status["updated_at"] = _now()
    with open(tmp, "w") as f:
        json.dump(status, f, indent=2)
    os.replace(tmp, status_file)


def _run_indexer(metric: str, date_str: str, log_path: Path) -> int:
    """Run build_intraday_leaderboard.py --force for one (metric, date).
    The --force flag wipes existing leaderboard rows for the (metric,
    anchor_hour, date) tuple before INSERTing fresh ranks (added in commit
    3bdce9b). Returns subprocess exit code."""
    cmd = [
        sys.executable, str(INDEXER_SCRIPT),
        "--metric", metric,
        "--source", "db",
        "--start", date_str,
        "--end", date_str,
        "--force",
        "--triggered-by", "cli",
        "--run-tag", "fill_missing_indexer",
    ]
    with open(log_path, "a") as logf:
        logf.write(f"\n\n=== indexer {metric} {date_str} starting at "
                   f"{datetime.now(timezone.utc).isoformat()} ===\n")
        logf.flush()
        proc = subprocess.run(
            cmd, cwd=str(PIPELINE_DIR), stdout=logf, stderr=subprocess.STDOUT,
            check=False,
        )
    return proc.returncode


def _refresh_cagg(log_path: Path) -> bool:
    """Refresh leaderboards_daily_count cagg over the last 30 days."""
    from pipeline.db.connection import get_conn
    conn = get_conn()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute(
                "CALL refresh_continuous_aggregate(%s, "
                "NOW() - INTERVAL '30 days', NOW())",
                (CAGG_NAME,),
            )
            return True
        except Exception as e:
            with open(log_path, "a") as logf:
                logf.write(f"\n⚠ cagg refresh failed for {CAGG_NAME}: {e}\n")
            return False
        finally:
            cur.close()
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--status-file", required=True)
    ap.add_argument("--dates", required=True,
                    help="Comma-separated YYYY-MM-DD list")
    args = ap.parse_args()

    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    status_file = Path(args.status_file)
    log_path = status_file.with_suffix(".log")
    status_file.parent.mkdir(parents=True, exist_ok=True)

    started = _now()
    status: dict = {
        "job_id": args.job_id,
        "page": "indexer",
        # Worker PID — read by DELETE /coverage/fill-missing/{job_id}.
        "pid": os.getpid(),
        "state": "running",
        "started_at": started,
        "finished_at": None,
        "dates_total": len(dates),
        "dates_completed": 0,
        "dates_failed": 0,
        "dates": [{"date": d, "state": "pending",
                   "metrics_done": [], "metrics_failed": []}
                  for d in dates],
        "current_date": None,
        "current_state": None,
        "elapsed_seconds": 0,
        "errors": [],
        "log_path": str(log_path),
        "summary": None,
    }
    _write_status(status_file, status)

    try:
        for i, d in enumerate(dates):
            status["current_date"] = d
            status["dates"][i]["state"] = "running"
            status["elapsed_seconds"] = int(_now() - started)
            _write_status(status_file, status)

            day_failed = False
            for metric in METRICS:
                status["current_state"] = f"rebuilding {metric}"
                status["elapsed_seconds"] = int(_now() - started)
                _write_status(status_file, status)

                rc = _run_indexer(metric, d, log_path)
                if rc != 0:
                    status["errors"].append(
                        f"{d} {metric}: indexer exited rc={rc}"
                    )
                    status["dates"][i]["metrics_failed"].append(metric)
                    day_failed = True
                else:
                    status["dates"][i]["metrics_done"].append(metric)
                _write_status(status_file, status)

            if day_failed:
                status["dates"][i]["state"] = "failed"
                status["dates_failed"] += 1
            else:
                status["dates"][i]["state"] = "done"
                status["dates_completed"] += 1
            status["elapsed_seconds"] = int(_now() - started)
            _write_status(status_file, status)

        # Always refresh cagg at the end (defensive)
        status["current_date"] = None
        status["current_state"] = "refreshing_cagg"
        status["elapsed_seconds"] = int(_now() - started)
        _write_status(status_file, status)
        _refresh_cagg(log_path)

        status["state"] = "done" if status["dates_failed"] == 0 \
                          else "completed_with_errors"
        status["finished_at"] = _now()
        status["current_state"] = None
        status["elapsed_seconds"] = int(_now() - started)
        elapsed = status["elapsed_seconds"]
        status["summary"] = (
            f"Rebuilt {status['dates_completed']}/{status['dates_total']} days "
            f"in {elapsed // 60}m {elapsed % 60}s"
            + (f" ({status['dates_failed']} failed)" if status['dates_failed'] else "")
        )
        _write_status(status_file, status)
        return 0
    except Exception as e:
        with open(log_path, "a") as logf:
            logf.write(f"\n\n=== FATAL ===\n{traceback.format_exc()}\n")
        status["state"] = "failed"
        status["errors"].append(f"fatal: {e}")
        status["finished_at"] = _now()
        status["elapsed_seconds"] = int(_now() - started)
        status["summary"] = f"Fatal error: {e}"
        _write_status(status_file, status)
        return 1


if __name__ == "__main__":
    sys.exit(main())

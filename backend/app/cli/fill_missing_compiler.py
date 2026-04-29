#!/usr/bin/env python3
"""fill_missing_compiler.py — background worker that fixes incomplete days
in market.futures_1m by deleting + re-running metl, then refreshing the
relevant continuous aggregates.

Spawned by POST /api/compiler/coverage/fill-missing as a detached
subprocess. Writes progress to a JSON status file so the FastAPI status
endpoint (and the frontend's polling loop) can show live progress.

Usage:
    python -m app.cli.fill_missing_compiler \\
        --job-id <uuid> \\
        --status-file /mnt/quant-data/jobs/fill_missing/<uuid>.json \\
        --dates 2026-04-25,2026-04-22,2026-04-19
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
_HERE = Path(__file__).resolve()
sys.path.insert(0, "/app")

PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", "/app/pipeline"))
METL_SCRIPT = PIPELINE_DIR / "compiler" / "metl.py"

CAGG_NAMES = [
    "market.futures_1m_daily_symbol_count",
    "market.symbol_day_counts",
]

# metl's CSV cache locations. metl's AUTO_RESUME mode skips fetch when a
# CSV exists — so to actually re-fetch a partial day from Amberdata we
# need to remove the stale CSV first. Two paths because metl falls back
# to a Mac-style path when CSV_BACKUP_DIR isn't exported. We don't pre-
# DELETE DB rows (that was the 2026-04-29 data-loss vector); ON CONFLICT
# DO NOTHING in metl's loader tops up missing rows safely. Net effect:
# CSV cache busted → metl re-fetches → new rows insert; existing rows
# stay if fetch fails partway through.
_CSV_BACKUP_DIRS = [
    Path(os.environ.get("CSV_BACKUP_DIR", "/mnt/quant-data/raw/amberdata/csv/")),
    Path("/Users/johnmullin/Desktop/desk/import/oi_logger/ob-backfills/"),
]


def _delete_cached_csvs(date_str: str) -> list[str]:
    """Wipe the day's CSV from both candidate locations so metl falls
    out of AUTO_RESUME and re-fetches from Amberdata. Silent on missing
    files. Returns paths that were actually removed."""
    deleted: list[str] = []
    fname = f"oi_{date_str.replace('-', '')}.csv"
    for d in _CSV_BACKUP_DIRS:
        p = d / fname
        if p.exists():
            try:
                p.unlink()
                deleted.append(str(p))
            except Exception:
                pass
    return deleted


def _now() -> float:
    return time.time()


def _write_status(status_file: Path, status: dict) -> None:
    """Atomic-ish write: write to .tmp then rename. Survives kills mid-write."""
    tmp = status_file.with_suffix(status_file.suffix + ".tmp")
    status["updated_at"] = _now()
    with open(tmp, "w") as f:
        json.dump(status, f, indent=2)
    os.replace(tmp, status_file)


def _run_metl(date_str: str, log_path: Path) -> int:
    """Run metl --start date --end date, append output to log_path. Returns
    the subprocess exit code."""
    cmd = [
        sys.executable, str(METL_SCRIPT),
        "--start", date_str, "--end", date_str,
        "--triggered-by", "cli",
        "--run-tag", "fill_missing",
    ]
    with open(log_path, "a") as logf:
        logf.write(f"\n\n=== metl {date_str} starting at {datetime.now(timezone.utc).isoformat()} ===\n")
        logf.flush()
        proc = subprocess.run(
            cmd, cwd=str(PIPELINE_DIR), stdout=logf, stderr=subprocess.STDOUT,
            check=False,
        )
    return proc.returncode


def _refresh_caggs(log_path: Path) -> list[str]:
    """Run CALL refresh_continuous_aggregate(...) for each compiler-page cagg.
    Uses an autocommit connection (some Timescale versions disallow inside
    txn block). Returns list of refreshed cagg names."""
    from pipeline.db.connection import get_conn
    conn = get_conn()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        refreshed: list[str] = []
        for name in CAGG_NAMES:
            try:
                cur.execute(
                    "CALL refresh_continuous_aggregate(%s, "
                    "NOW() - INTERVAL '7 days', NOW())",
                    (name,),
                )
                refreshed.append(name)
            except Exception as e:
                with open(log_path, "a") as logf:
                    logf.write(f"\n⚠ cagg refresh failed for {name}: {e}\n")
        cur.close()
        return refreshed
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
        "page": "compiler",
        # Worker PID — read by the cancel endpoint (DELETE
        # /coverage/fill-missing/{job_id}) to SIGTERM this process.
        "pid": os.getpid(),
        "state": "running",
        "started_at": started,
        "finished_at": None,
        "dates_total": len(dates),
        "dates_completed": 0,
        "dates_failed": 0,
        "dates": [{"date": d, "state": "pending"} for d in dates],
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
            # Step 1: bust the CSV cache so metl falls out of
            # AUTO_RESUME and re-fetches the day from Amberdata. We do
            # NOT delete DB rows — the 2026-04-29 incident wiped 11
            # days of good data because the worker DELETEd before
            # confirming metl's re-fetch would succeed. Now: metl
            # writes a fresh CSV + INSERT ON CONFLICT DO NOTHING tops
            # up the day's rows. If the fetch fails partway, existing
            # rows stay intact and the next run can resume.
            csvs = _delete_cached_csvs(d)
            with open(log_path, "a") as logf:
                if csvs:
                    logf.write(f"\n[{d}] cleared cached CSV(s) so metl re-fetches: {csvs}\n")
                else:
                    logf.write(f"\n[{d}] no cached CSV present; metl will fetch fresh\n")

            # Step 2: run metl. fetching = network-bound Amberdata
            # download + DB load.
            status["dates"][i]["state"] = "fetching"
            status["current_state"] = "fetching"
            status["elapsed_seconds"] = int(_now() - started)
            _write_status(status_file, status)

            rc = _run_metl(d, log_path)
            if rc != 0:
                status["errors"].append(f"{d}: metl exited rc={rc}")
                status["dates"][i]["state"] = "failed"
                status["dates_failed"] += 1
            else:
                status["dates"][i]["state"] = "done"
                status["dates_completed"] += 1
            status["elapsed_seconds"] = int(_now() - started)
            _write_status(status_file, status)

        # Always refresh caggs at the end (defensive — even if some dates
        # failed, those that succeeded should be visible)
        status["current_date"] = None
        status["current_state"] = "refreshing_caggs"
        status["elapsed_seconds"] = int(_now() - started)
        _write_status(status_file, status)
        refreshed = _refresh_caggs(log_path)
        with open(log_path, "a") as logf:
            logf.write(f"\nCagg refresh complete: {refreshed}\n")

        status["state"] = "done" if status["dates_failed"] == 0 else "completed_with_errors"
        status["finished_at"] = _now()
        status["current_state"] = None
        status["elapsed_seconds"] = int(_now() - started)
        elapsed = status["elapsed_seconds"]
        status["summary"] = (
            f"Filled {status['dates_completed']}/{status['dates_total']} days "
            f"in {elapsed // 60}m {elapsed % 60}s"
            + (f" ({status['dates_failed']} failed)" if status['dates_failed'] else "")
        )
        _write_status(status_file, status)
        return 0
    except Exception as e:
        # Catastrophic failure — record it and mark job failed
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

#!/usr/bin/env python3
"""nightly_etl_catchup.py — self-healing wrapper for the nightly ETL +
indexer crons.

Replaces the legacy `--start $(date -d "yesterday")` cron pattern with
"scan the last N days, fill any gap." After a cutover (binetl 2026-05-09),
a transient fetch failure, or an operator forgetting to top up "today"
mid-day, the next nightly run automatically catches up instead of
silently leaving a hole.

Two modes:
  --module compiler  → detect partial/missing days in market.futures_1m
                       and run the active ETL_BACKEND for each
  --module indexer   → detect partial/missing (date, metric) combos in
                       market.leaderboards and run the indexer for each

Idempotency:
  - ETL writers use ON CONFLICT DO NOTHING (see pipeline/db/load_csv_to_futures_1m.py).
  - The indexer's `date_already_in_db` guard short-circuits any (date,
    metric, variant, anchor_hour) slot that is already ≥95% complete
    (pipeline/indexer/build_intraday_leaderboard.py:830).
  Both are safe to re-run on the same date.

Detection (compiler):
  Reuses backend.app.api.routes.compiler._detect_partial_dates_compiler —
  the same query that powers /api/compiler/coverage's "partial" badge.
  Plus an explicit yesterday probe so a fully-missing yesterday gets
  picked up even when no prior partial run wrote any rows. We do NOT
  auto-fill arbitrary historical missing days (that's the manual
  /coverage/fill-missing flow's job, after operator review).

Detection (indexer):
  Per-(day, metric) row count from market.leaderboards over the lookback
  window. Expected = 1440 * TOP_N (≈479,520 for TOP_N=333). A slot is
  flagged when its count < 95% of expected, mirroring the indexer's own
  skip threshold so we don't fight ourselves.

Exit code:
  0 if every detected gap was filled successfully (or none detected).
  Non-zero if any subprocess failed — surfaces in cron logs and lets a
  future supervisor alert hook into the same signal.

Cron usage (host crontab — ops/crontab.txt):
  15 0 * * * ... docker compose exec -T celery python -m \\
      app.cli.nightly_etl_catchup --module compiler --lookback-days 7
  0  1 * * * ... docker compose exec -T celery python -m \\
      app.cli.nightly_etl_catchup --module indexer --lookback-days 7
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow `from app.db import ...` and the route helper import below to
# resolve when run via `python -m app.cli.nightly_etl_catchup`.
sys.path.insert(0, "/app/backend")
sys.path.insert(0, "/app")

from app.db import get_worker_conn  # noqa: E402

# Mirrors run_jobs_worker.py / pipeline_runner.py wiring so the same env
# variables drive both the on-demand and the nightly paths.
PIPELINE_DIR = Path(os.environ.get("PIPELINE_DIR", "/app/pipeline"))
PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", sys.executable)

_ETL_BACKEND = os.environ.get("ETL_BACKEND", "metl").lower()
if _ETL_BACKEND not in ("metl", "binetl"):
    _ETL_BACKEND = "metl"

INDEXER_METRICS = ("price", "open_interest", "volume")
INDEXER_TOP_N = 333  # matches build_intraday_leaderboard.py default
INDEXER_EXPECTED_PER_DAY = 1440 * INDEXER_TOP_N  # 479,520
INDEXER_COMPLETE_THRESHOLD = int(INDEXER_EXPECTED_PER_DAY * 0.95)

CAGG_NAMES = (
    "market.futures_1m_daily_symbol_count",
    "market.symbol_day_counts",
    "market.leaderboards_daily_count",
)


def _emit(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}",
          flush=True)


def _yesterday_iso() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()


# ─── Compiler detection ─────────────────────────────────────────────────────

def _detect_compiler_gaps(lookback_days: int, source_id: int) -> list[str]:
    """Return ISO dates that need the ETL re-run.

    Includes:
      1. Any "partial" day inside the lookback window (per the same query
         /api/compiler/coverage uses to flag the "partial" badge).
      2. Yesterday, if futures_1m has zero rows for it. A fully-missing
         yesterday won't show up in the partial detector (which excludes
         zero-data days), but it's the one missing day we always want to
         backfill from cron.

    Older zero-data days are intentionally left for the manual fill
    flow — see _detect_partial_dates_compiler's docstring for the
    2026-04-29 incident that motivated that guard.
    """
    # Reuse the canonical detector from the routes module so the cron and
    # the UI agree on what "partial" means.
    from app.api.routes.compiler import _detect_partial_dates_compiler
    partials = _detect_partial_dates_compiler(lookback_days, source_id=source_id)

    yesterday = _yesterday_iso()
    candidates = set(partials)

    # Probe yesterday explicitly for the fully-missing case.
    conn = get_worker_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM market.futures_1m "
            "WHERE timestamp_utc::date = %s::date AND source_id = %s",
            (yesterday, source_id),
        )
        if int(cur.fetchone()[0]) == 0:
            candidates.add(yesterday)
        cur.close()
    finally:
        conn.close()

    return sorted(candidates, reverse=True)


def _run_compiler_for_date(date_str: str) -> int:
    """Invoke the active ETL backend for a single date. Returns exit code."""
    script = PIPELINE_DIR / "compiler" / f"{_ETL_BACKEND}.py"
    cmd = [
        PIPELINE_PYTHON, "-u", str(script),
        "--start", date_str, "--end", date_str,
        "--triggered-by", "scheduler",
        "--run-tag", "nightly_catchup",
    ]
    _emit(f"  → {_ETL_BACKEND} {date_str}")
    proc = subprocess.run(cmd, cwd=str(PIPELINE_DIR), check=False)
    return proc.returncode


# ─── Indexer detection ──────────────────────────────────────────────────────

def _detect_indexer_gaps(lookback_days: int) -> list[tuple[str, str]]:
    """Return (date, metric) tuples that need the indexer re-run.

    A slot is flagged when its leaderboards row count for that
    (date, metric) is below the indexer's own 95% skip threshold —
    mirroring `date_already_in_db` so the wrapper and the indexer
    don't disagree about what "complete" means.

    Excludes today (incomplete by definition) and dates where the
    underlying futures_1m has no data (nothing to build from).
    """
    today = datetime.now(timezone.utc).date()
    earliest = today - timedelta(days=lookback_days)
    conn = get_worker_conn()
    try:
        cur = conn.cursor()
        # Build the (date, metric) Cartesian product of "days that
        # futures_1m has data for" × "the three canonical metrics",
        # then LEFT JOIN actual leaderboard counts. A NULL or below-
        # threshold count means we need to (re)build.
        cur.execute(
            """
            WITH source_days AS (
                SELECT DISTINCT timestamp_utc::date AS day
                FROM market.futures_1m
                WHERE timestamp_utc >= %s::date
                  AND timestamp_utc <  %s::date
                  AND source_id = 1
            ),
            metrics AS (
                SELECT unnest(%s::text[]) AS metric
            ),
            existing AS (
                SELECT timestamp_utc::date AS day,
                       metric,
                       COUNT(*) AS row_count
                FROM market.leaderboards
                WHERE timestamp_utc >= %s::date
                  AND timestamp_utc <  %s::date
                GROUP BY 1, 2
            )
            SELECT sd.day::text, m.metric,
                   COALESCE(e.row_count, 0) AS row_count
            FROM source_days sd
            CROSS JOIN metrics m
            LEFT JOIN existing e ON e.day = sd.day AND e.metric = m.metric
            WHERE COALESCE(e.row_count, 0) < %s
            ORDER BY sd.day DESC, m.metric
            """,
            (
                earliest, today,
                list(INDEXER_METRICS),
                earliest, today,
                INDEXER_COMPLETE_THRESHOLD,
            ),
        )
        gaps = [(r[0], r[1]) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    return gaps


def _run_indexer_for(date_str: str, metric: str) -> int:
    """Invoke the indexer for a single (date, metric). Returns exit code."""
    script = PIPELINE_DIR / "indexer" / "build_intraday_leaderboard.py"
    cmd = [
        PIPELINE_PYTHON, "-u", str(script),
        "--metric", metric,
        "--source", "db",
        "--start", date_str, "--end", date_str,
        "--triggered-by", "scheduler",
        "--run-tag", "nightly_catchup",
    ]
    _emit(f"  → indexer {date_str} ({metric})")
    proc = subprocess.run(cmd, cwd=str(PIPELINE_DIR), check=False)
    return proc.returncode


# ─── Cagg refresh ───────────────────────────────────────────────────────────

def _refresh_caggs(window_days: int = 7) -> None:
    """Refresh the caggs that the compiler/indexer/coverage UIs depend on.

    The hourly built-in policy only refreshes the last 7 days, so an
    older partial-fill won't surface on /compiler/coverage until tomorrow
    unless we nudge it manually. Mirrors POST /api/compiler/coverage/refresh.
    """
    conn = get_worker_conn()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        for name in CAGG_NAMES:
            try:
                cur.execute(
                    "CALL refresh_continuous_aggregate(%s, "
                    "NOW() - INTERVAL %s, NOW())",
                    (name, f"{window_days} days"),
                )
                _emit(f"  ✓ refreshed {name}")
            except Exception as e:
                _emit(f"  ⚠ cagg refresh failed for {name}: {e}")
        cur.close()
    finally:
        conn.close()


# ─── Mode dispatch ──────────────────────────────────────────────────────────

def run_compiler(lookback_days: int) -> int:
    _emit(f"compiler catchup: ETL_BACKEND={_ETL_BACKEND}, "
          f"lookback={lookback_days}d")
    t0 = time.time()
    dates = _detect_compiler_gaps(lookback_days, source_id=1)
    if not dates:
        _emit("no gaps detected; nothing to do")
        _refresh_caggs(window_days=lookback_days)
        return 0
    _emit(f"detected {len(dates)} day(s) needing fill: {dates}")
    failures: list[str] = []
    for d in dates:
        rc = _run_compiler_for_date(d)
        if rc != 0:
            failures.append(f"{d} (rc={rc})")
    _refresh_caggs(window_days=lookback_days)
    elapsed = int(time.time() - t0)
    if failures:
        _emit(f"completed with {len(failures)} failure(s) in {elapsed}s: "
              f"{failures}")
        return 1
    _emit(f"completed cleanly in {elapsed}s")
    return 0


def run_indexer(lookback_days: int) -> int:
    _emit(f"indexer catchup: lookback={lookback_days}d, "
          f"threshold={INDEXER_COMPLETE_THRESHOLD:,} rows")
    t0 = time.time()
    gaps = _detect_indexer_gaps(lookback_days)
    if not gaps:
        _emit("no (date, metric) gaps detected; nothing to do")
        _refresh_caggs(window_days=lookback_days)
        return 0
    _emit(f"detected {len(gaps)} (date, metric) gap(s): {gaps}")
    failures: list[str] = []
    for date_str, metric in gaps:
        rc = _run_indexer_for(date_str, metric)
        if rc != 0:
            failures.append(f"{date_str}/{metric} (rc={rc})")
    _refresh_caggs(window_days=lookback_days)
    elapsed = int(time.time() - t0)
    if failures:
        _emit(f"completed with {len(failures)} failure(s) in {elapsed}s: "
              f"{failures}")
        return 1
    _emit(f"completed cleanly in {elapsed}s")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--module", required=True, choices=("compiler", "indexer"))
    p.add_argument("--lookback-days", type=int, default=7,
                   help="How many days back to scan for gaps (default: 7)")
    args = p.parse_args()
    if args.module == "compiler":
        return run_compiler(args.lookback_days)
    return run_indexer(args.lookback_days)


if __name__ == "__main__":
    sys.exit(main())

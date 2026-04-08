#!/usr/bin/env python3
# ============================================================
# Intraday Leaderboard Builder — VECTORIZED
# Metric toggle: price / open_interest / volume
#
# Outputs (in order of authority, all per day):
#   1. INSERT INTO market.leaderboards   ← canonical, source of truth
#   2. Wide parquet master file          ← cache for the simulator
#   3. Per-day CSV files                 ← optional debugging output
#
# Re-runs are checkpointed against market.leaderboards: a date is skipped
# if it already has rows for the current (metric, anchor_hour) combination.
# Use --force to bypass the checkpoint and reprocess every date.
#
# Each invocation creates a row in market.indexer_jobs and updates it
# with progress (symbols_done, rows_written, last_heartbeat) as the loop
# advances. Per-day failures increment a counter; the final job status
# is "complete" if all dates succeeded, otherwise "failed" with an
# error_msg listing the failures.
# ============================================================

import os
import sys
import json
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import argparse

# Two sys.path entries are needed:
#   - parents[1] = pipeline/      → for the legacy `from config import ...`
#                                   (config.py lives at pipeline/config.py).
#                                   The original script relied on this being
#                                   CWD-relative, which silently broke when
#                                   the cron invoked the script via absolute
#                                   path. Adding it explicitly fixes that bug.
#   - parents[2] = project root   → for `from pipeline.db.connection import`
_PIPELINE_DIR = str(Path(__file__).resolve().parents[1])
_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config import (
    COMPILED_DIR, LOG_INDEXER, ensure_dirs,
    leaderboard_dir_for_metric,
)
ensure_dirs()

# ============================================================
# CONFIG
# ============================================================

PARQUET_DIR = Path(
    os.environ.get("LEADERBOARD_PARQUET_DIR", str(COMPILED_DIR))
)

# OUTPUT_DIR is resolved per metric via leaderboard_dir_for_metric() after args are parsed

TOP_N = int(os.environ.get("LEADERBOARD_TOP_N", "333"))

# ============================================================
# OUTPUT SETTINGS
# ============================================================

SAVE_DAILY_FILES = os.environ.get("SAVE_DAILY_FILES", "0") == "1"     # True = write per-day CSVs
BUILD_MASTER_FILE = os.environ.get("BUILD_MASTER_FILE", "1") == "1"    # True = build merged master

# Streaming-write state for the master parquet. The writer is opened lazily
# on the first day that produces a frame so we can pin the schema from real
# data, then closed in a finally block after the per-day loop. Days are
# written one row group at a time — each day's wide-format DataFrame is
# ~1440 rows and ~5 MB on disk, so memory stays bounded regardless of how
# many days are in the run. Replaces the previous master_frames list which
# accumulated every day's DataFrame in RAM and OOM-killed on full backlogs.
_master_writer: pq.ParquetWriter | None = None
_master_path: Path | None = None
_master_rows_written = 0

# ============================================================
# CLI ARGUMENTS
# ============================================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--metric",
    type=str,
    required=True,
    help="Ranking metric: price | open_interest | volume"
)
parser.add_argument(
    "--parquet-path",
    type=str,
    default=None,
    dest="parquet_path",
    help=(
        "Path to a single master parquet file (e.g. master_oi_training_table.parquet). "
        "When supplied, the builder reads this file grouped by date instead of "
        "scanning PARQUET_DIR for date=* partitions. "
        "Required columns: timestamp_utc, symbol, <metric>."
    )
)
parser.add_argument(
    "--output-dir",
    type=str,
    default=None,
    dest="output_dir",
    help="Directory to write output parquet files. Overrides OUTPUT_DIR constant."
)

parser.add_argument(
    "--index-lookback",
    type=int,
    default=0,
    dest="index_lookback",
    help=(
        "Hours before deployment_start_hour to use as the % change anchor. "
        "Default: 0 = midnight (first bar of the day, current behaviour). "
        "e.g. --deployment-start-hour 6 --index-lookback 6 anchors at 00:00; "
        "     --deployment-start-hour 6 --index-lookback 4 anchors at 02:00; "
        "     --deployment-start-hour 6 --index-lookback 8 anchors at 22:00 prev day."
    )
)
parser.add_argument(
    "--deployment-start-hour",
    type=int,
    default=6,
    dest="deployment_start_hour",
    help="Deployment window start hour UTC (0-23). Used with --index-lookback to "
         "compute the anchor time: anchor = deployment_start_hour - index_lookback. "
         "Default: 6."
)

# ─── Date range filter ──────────────────────────────────────────────────────
# Optional. If supplied, only dates in [start, end] inclusive are processed.
# Without these, every date in the parquet (or every date=* partition) is
# processed, which is the legacy behaviour.
parser.add_argument(
    "--start",
    type=str,
    default=None,
    help="Optional start date YYYY-MM-DD. If supplied, dates before this are skipped."
)
parser.add_argument(
    "--end",
    type=str,
    default=None,
    help="Optional end date YYYY-MM-DD inclusive. If supplied, dates after this are skipped."
)

# ─── Checkpointing + DB write toggles ───────────────────────────────────────
parser.add_argument(
    "--force",
    action="store_true",
    default=False,
    help="Reprocess dates even if rows already exist in market.leaderboards. "
         "Default: skip dates that already have rows for (metric, anchor_hour)."
)
parser.add_argument(
    "--no-db-write",
    action="store_true",
    default=False,
    dest="no_db_write",
    help="Skip writing to market.leaderboards. Parquet/CSV output still happens. "
         "Useful for testing the pivot logic without touching the DB."
)

# ─── Job tracking (mirrors metl.py shape) ───────────────────────────────────
parser.add_argument(
    "--job-id",
    type=str,
    default=None,
    dest="job_id",
    help="UUID of an existing market.indexer_jobs row to update. "
         "If not provided, a new job row is created automatically."
)
parser.add_argument(
    "--triggered-by",
    type=str,
    default="cli",
    choices=["ui", "cli", "scheduler"],
    dest="triggered_by",
    help="Who triggered this run. Recorded in indexer_jobs.triggered_by."
)
parser.add_argument(
    "--run-tag",
    type=str,
    default=None,
    dest="run_tag",
    help="Optional human label e.g. 'nightly' or 'backfill-march'. "
         "Recorded in indexer_jobs.run_tag."
)

args = parser.parse_args()
RANK_METRIC = args.metric.lower()
if args.output_dir:
    OUTPUT_DIR = Path(args.output_dir)
else:
    OUTPUT_DIR = leaderboard_dir_for_metric(RANK_METRIC)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── DB write toggle ────────────────────────────────────────────────────────
WRITE_TO_DB = not args.no_db_write
# Variant column on market.leaderboards. Currently the only variant in use
# is "close" — see backfill_leaderboards.py for the same constant. If we
# ever add e.g. "vwap" or "twap", they would be separate variants.
VARIANT = "close"


# ============================================================
# DB JOB TRACKING — market.indexer_jobs
# All operations are best-effort: a DB failure must never interrupt the
# existing parquet/CSV write path.
# ============================================================
def job_create(date_from, date_to, triggered_by="cli", run_tag=None,
               params: dict | None = None):
    """Insert a new indexer job row, return job_id UUID string."""
    try:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO market.indexer_jobs
                (job_type, status, metric, date_from, date_to,
                 params, triggered_by, run_tag, started_at, last_heartbeat)
            VALUES ('leaderboard', 'running', %s, %s, %s, %s::jsonb, %s, %s, NOW(), NOW())
            RETURNING job_id
        """, (
            RANK_METRIC,
            date_from,
            date_to,
            json.dumps(params or {}),
            triggered_by,
            run_tag,
        ))
        job_id = str(cur.fetchone()[0])
        conn.commit()
        cur.close()
        conn.close()
        print(f"   📋 Job created → {job_id}")
        return job_id
    except Exception as e:
        print(f"⚠️ job_create failed → {e}")
        return None


def job_set_total(job_id, total_dates):
    """Set symbols_total once the date list is known. We use this column to
    record the number of dates in the run, since the indexer_jobs table has
    no dedicated 'dates_total' column."""
    if not job_id:
        return
    try:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE market.indexer_jobs SET symbols_total=%s, last_heartbeat=NOW() WHERE job_id=%s",
            (total_dates, job_id),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ job_set_total failed → {e}")


def job_increment(job_id, rows_added):
    """Atomically increment symbols_done by 1 and rows_written by rows_added,
    and bump last_heartbeat. Called after each successful date OR each skipped
    date — both count as 'progress' from the job's perspective."""
    if not job_id:
        return
    try:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE market.indexer_jobs
            SET symbols_done   = symbols_done + 1,
                rows_written   = rows_written + %s,
                last_heartbeat = NOW()
            WHERE job_id = %s
        """, (rows_added, job_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ job_increment failed → {e}")


def job_complete(job_id):
    """Mark job as complete. Called only when all dates succeeded."""
    if not job_id:
        return
    try:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE market.indexer_jobs
            SET status='complete', completed_at=NOW(), last_heartbeat=NOW()
            WHERE job_id=%s
        """, (job_id,))
        conn.commit()
        cur.close()
        conn.close()
        print(f"   ✅ Job complete → {job_id}")
    except Exception as e:
        print(f"⚠️ job_complete failed → {e}")


def job_fail(job_id, error_msg):
    """Mark job as failed with error message. Called when one or more dates
    failed (per-day failures are aggregated into the message by the caller)
    or when something catastrophic happened outside the loop."""
    if not job_id:
        return
    try:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE market.indexer_jobs
            SET status='failed', completed_at=NOW(), last_heartbeat=NOW(), error_msg=%s
            WHERE job_id=%s
        """, (str(error_msg)[:2000], job_id))
        conn.commit()
        cur.close()
        conn.close()
        print(f"   ❌ Job failed → {job_id}")
    except Exception as e:
        print(f"⚠️ job_fail failed → {e}")


# ============================================================
# DB CHECKPOINT + LEADERBOARD WRITE
# ============================================================
# Module-level cache: built lazily on first need, reused across days.
# binance_id (e.g. "BTCUSDT") → symbol_id (integer FK).
_SYMBOL_MAP_CACHE: dict[str, int] | None = None


def _build_symbol_map() -> dict[str, int]:
    global _SYMBOL_MAP_CACHE
    if _SYMBOL_MAP_CACHE is not None:
        return _SYMBOL_MAP_CACHE
    try:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
        _SYMBOL_MAP_CACHE = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        conn.close()
        print(f"   🔑 Symbol map cached: {len(_SYMBOL_MAP_CACHE)} entries")
    except Exception as e:
        print(f"⚠️ _build_symbol_map failed → {e}")
        _SYMBOL_MAP_CACHE = {}
    return _SYMBOL_MAP_CACHE


def date_already_in_db(date_str: str) -> int:
    """Return the count of rows already in market.leaderboards for this
    date + RANK_METRIC + ANCHOR_HOUR. Returns -1 on DB failure (which the
    caller treats as 'fall through and process anyway')."""
    try:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM market.leaderboards
            WHERE metric = %s
              AND variant = %s
              AND anchor_hour = %s
              AND timestamp_utc >= %s::date
              AND timestamp_utc <  (%s::date + INTERVAL '1 day')
        """, (RANK_METRIC, VARIANT, ANCHOR_HOUR, date_str, date_str))
        existing_rows = cur.fetchone()[0]
        cur.close()
        conn.close()
        return existing_rows
    except Exception as e:
        tqdm.write(f"⚠ checkpoint DB check failed ({e}) — processing anyway")
        return -1


def write_leaders_to_db(leaders_long: pd.DataFrame, date_str: str) -> int:
    """Insert the long-format leaders dataframe into market.leaderboards.
    Called BEFORE the pivot so we still have pct_change_midnight available.

    Expected columns on `leaders_long`:
        - minute (timestamp)
        - symbol (string, binance_id format e.g. 'BTCUSDT')
        - rank (int)
        - pct_change_midnight (float)

    Returns the number of rows successfully inserted (after ON CONFLICT
    DO NOTHING dedupe). Returns 0 on any failure — does NOT raise.
    """
    try:
        from pipeline.db.connection import get_conn
        from psycopg2.extras import execute_values
    except Exception as e:
        tqdm.write(f"⚠ DB write skipped for {date_str}: import failed ({e})")
        return 0

    sym_map = _build_symbol_map()
    if not sym_map:
        tqdm.write(f"⚠ DB write skipped for {date_str}: empty symbol map")
        return 0

    rows = []
    skipped_no_symbol_id = 0
    for r in leaders_long.itertuples(index=False):
        sid = sym_map.get(getattr(r, "symbol"))
        if sid is None:
            skipped_no_symbol_id += 1
            continue
        rows.append((
            r.minute,
            RANK_METRIC,
            VARIANT,
            ANCHOR_HOUR,
            int(r.rank),
            sid,
            None if pd.isna(getattr(r, "pct_change_midnight", None)) else float(r.pct_change_midnight),
        ))

    if not rows:
        tqdm.write(f"⚠ DB write for {date_str}: 0 rows after symbol mapping (skipped {skipped_no_symbol_id})")
        return 0

    try:
        conn = get_conn()
        cur = conn.cursor()
        result = execute_values(cur, """
            INSERT INTO market.leaderboards
                (timestamp_utc, metric, variant, anchor_hour, rank, symbol_id, pct_change)
            VALUES %s
            ON CONFLICT (timestamp_utc, metric, variant, anchor_hour, rank) DO NOTHING
            RETURNING timestamp_utc
        """, rows, page_size=10_000, fetch=True)
        inserted = len(result)
        conn.commit()
        cur.close()
        conn.close()
        if skipped_no_symbol_id > 0:
            tqdm.write(f"  💾 DB: {inserted:,} rows inserted ({skipped_no_symbol_id:,} skipped — symbol not in market.symbols)")
        else:
            tqdm.write(f"  💾 DB: {inserted:,} rows inserted")
        return inserted
    except Exception as e:
        tqdm.write(f"⚠ DB write failed for {date_str}: {e}")
        return 0


# ============================================================
# VALIDATE METRIC
# ============================================================

VALID_METRICS = ["price", "open_interest", "volume"]

if RANK_METRIC not in VALID_METRICS:
    raise ValueError(
        f"Invalid RANK_METRIC → {RANK_METRIC} | "
        f"Valid: {VALID_METRICS}"
    )

print(f"\n📊 Ranking Metric → {RANK_METRIC}")

# ── Anchor time ──────────────────────────────────────────────────────────
# anchor_offset = how many hours before deployment_start_hour the % change
# baseline is measured from. 0 = midnight (current behaviour).
INDEX_LOOKBACK       = args.index_lookback
DEPLOYMENT_START_HOUR = args.deployment_start_hour
_anchor_total         = DEPLOYMENT_START_HOUR - INDEX_LOOKBACK  # may be negative
ANCHOR_CROSS_MIDNIGHT = _anchor_total < 0
ANCHOR_HOUR           = _anchor_total % 24   # clock hour (wraps correctly)
ANCHOR_HHMM           = f"{ANCHOR_HOUR:02d}00"

if INDEX_LOOKBACK == 0:
    print(f"📍 Anchor → midnight (index_lookback=0, current behaviour)")
elif ANCHOR_CROSS_MIDNIGHT:
    print(f"📍 Anchor → {ANCHOR_HOUR:02d}:00 UTC previous day "
          f"(start={DEPLOYMENT_START_HOUR:02d}:00, lookback={INDEX_LOOKBACK}h — crosses midnight)")
else:
    print(f"📍 Anchor → {ANCHOR_HOUR:02d}:00 UTC same day "
          f"(start={DEPLOYMENT_START_HOUR:02d}:00, lookback={INDEX_LOOKBACK}h)")

# ============================================================
# START
# ============================================================

script_start = datetime.now()

print(f"\n🚀 Building intraday leaderboards")
print(f"Start → {script_start}\n")

# ── Helper: stable schema for the streamed master parquet ───────────────
# Pinned up-front so every day's row group writes against the same schema.
# Without this, pyarrow infers each column's dtype from the first day's
# data — and a day where (e.g.) R333 is all-null would lock that column
# to the null type and fail when a later day tries to write strings into
# it. The reindex above guarantees R1..R{TOP_N} all exist on every day.

def _master_schema(top_n: int) -> pa.Schema:
    fields = [pa.field("timestamp_utc", pa.timestamp("ns"))]
    for i in range(1, top_n + 1):
        fields.append(pa.field(f"R{i}", pa.string()))
    return pa.schema(fields)


# ── Helpers: row-group streaming for the single-file mode ────────────────
# Avoids loading the entire master parquet into RAM (was OOM-killing the
# server at 312M rows). Same iteration pattern as
# pipeline/db/backfill_futures_1m.py.

def _build_date_to_rg_map(pf: pq.ParquetFile) -> tuple[dict, list]:
    """Pass 1: scan ONLY the timestamp_utc column from every row group and
    build a map of date → list of row group indices that contain rows for
    that date. Also returns the sorted list of unique dates.

    Memory cost per row group: one column of timestamps (~3 MB for ~393K
    rows). Discarded immediately after extracting unique dates. Total
    pass-1 footprint is bounded by one row group, not the whole file.
    """
    date_to_rg: dict = {}
    for rg_idx in range(pf.num_row_groups):
        tbl = pf.read_row_group(rg_idx, columns=["timestamp_utc"])
        ts = pd.to_datetime(tbl.column("timestamp_utc").to_pandas())
        for d in ts.dt.date.unique():
            date_to_rg.setdefault(d, []).append(rg_idx)
    return date_to_rg, sorted(date_to_rg.keys())


def _load_day_from_parquet(
    pf: pq.ParquetFile,
    date_to_rg_map: dict,
    target_date,
    columns: list[str],
) -> pd.DataFrame:
    """Read all rows for `target_date` by iterating only the row groups
    that contain it (lookup via date_to_rg_map). Returns a DataFrame with
    the requested columns plus a `timestamp_utc` column converted to
    pandas datetime. Returns an empty DataFrame if the date isn't in the
    map.

    Per-day memory: 1-2 row groups × 3 columns ≈ 25-50 MB peak. Far below
    the ~2-4 GB the all-at-once read used to require.
    """
    row_groups = date_to_rg_map.get(target_date, [])
    if not row_groups:
        return pd.DataFrame(columns=columns)
    frames = []
    for rg_idx in row_groups:
        tbl = pf.read_row_group(rg_idx, columns=columns)
        df = tbl.to_pandas()
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
        df = df[df["timestamp_utc"].dt.date == target_date]
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


# ── Determine data source ─────────────────────────────────────────────────
# Single-file mode: row-group streaming (memory-bounded, OOM-safe)
# Partition mode:   scan PARQUET_DIR for date=* subdirectories (original)
USE_SINGLE_FILE = args.parquet_path is not None

# Module-level handles populated only in single-file mode. Both stay None
# in partition mode and are not referenced from that branch.
_PF: pq.ParquetFile | None = None
_DATE_TO_RG: dict = {}

if USE_SINGLE_FILE:
    print(f"📂 Single-file mode (row-group streaming): {args.parquet_path}")
    _PF = pq.ParquetFile(args.parquet_path)
    print(
        f"   row groups: {_PF.num_row_groups:,} | "
        f"total rows: {_PF.metadata.num_rows:,}"
    )
    print("   pass 1: scanning timestamps to build date → row-group map ...")
    _DATE_TO_RG, _date_groups = _build_date_to_rg_map(_PF)
    print(f"📂 Dates found: {len(_date_groups)}\n")
else:
    partitions = sorted(PARQUET_DIR.glob("date=*"))
    print(f"📂 Partitions found: {len(partitions)}\n")
    _date_groups = partitions

# ─── Apply --start / --end filter to _date_groups ───────────────────────────
# Both bounds are inclusive. Filtering happens before the main loop so the
# tqdm progress bar reflects only the dates we'll actually process.
def _date_of(group) -> "datetime.date":
    """Extract a python date from a _date_groups entry, regardless of mode."""
    if USE_SINGLE_FILE:
        return group  # already a python date from .dt.date.unique()
    # Partition mode: group is a Path like date=2025-04-05
    return datetime.strptime(group.name.split("=")[1], "%Y-%m-%d").date()

if args.start or args.end:
    _start_date = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
    _end_date   = datetime.strptime(args.end,   "%Y-%m-%d").date() if args.end   else None
    _before = len(_date_groups)
    _date_groups = [
        g for g in _date_groups
        if (_start_date is None or _date_of(g) >= _start_date)
           and (_end_date is None or _date_of(g) <= _end_date)
    ]
    print(f"📅 Date filter: {args.start or 'beginning'} → {args.end or 'end'} "
          f"({len(_date_groups)} of {_before} dates kept)\n")

# ============================================================
# JOB TRACKING — create row in market.indexer_jobs for this run
# ============================================================
# Either reuse the user-supplied --job-id (e.g. when invoked by the UI)
# or create a fresh row. Failures are tolerated — job_id may be None and
# all subsequent helpers no-op when given None.
_first_date = _date_of(_date_groups[0]) if _date_groups else None
_last_date  = _date_of(_date_groups[-1]) if _date_groups else None

JOB_ID = args.job_id
if JOB_ID is None:
    JOB_ID = job_create(
        date_from=_first_date,
        date_to=_last_date,
        triggered_by=args.triggered_by,
        run_tag=args.run_tag,
        params={
            "metric": RANK_METRIC,
            "anchor_hour": ANCHOR_HOUR,
            "deployment_start_hour": DEPLOYMENT_START_HOUR,
            "index_lookback": INDEX_LOOKBACK,
            "force": bool(args.force),
            "no_db_write": bool(args.no_db_write),
            "parquet_path": str(args.parquet_path) if args.parquet_path else None,
        },
    )

job_set_total(JOB_ID, len(_date_groups))

# Per-day failure tracking. Used to decide final job status (complete vs failed)
# and to populate error_msg with up to the first 5 failure summaries.
_failure_count = 0
_failure_summaries: list[str] = []

# ============================================================
# LOOP DATES
# ============================================================

for _day_key in tqdm(_date_groups, desc="Processing days"):

    # ─── Resolve date_str up-front so the checkpoint and per-iteration
    # diagnostics have it available before any processing happens. ────
    if USE_SINGLE_FILE:
        date_str = str(_day_key)
    else:
        date_str = _day_key.name.split("=")[1]

    # ─── DB checkpoint ─────────────────────────────────────────────────
    # If this date already has rows in market.leaderboards for the current
    # (metric, variant, anchor_hour) combination, skip it unless --force is
    # set. The DB is the canonical source of truth — file existence is not
    # checked. A DB error in the checkpoint query falls through and processes
    # the date anyway (better to do duplicate work than skip a real gap).
    if not args.force and WRITE_TO_DB:
        existing_rows = date_already_in_db(date_str)
        if existing_rows > 0:
            tqdm.write(f"⏭  {date_str} → skipping ({existing_rows:,} rows already in DB, use --force to reprocess)")
            job_increment(JOB_ID, 0)  # progress without rows
            continue

    try:

        if USE_SINGLE_FILE:
            df = _load_day_from_parquet(
                _PF, _DATE_TO_RG, _day_key,
                columns=["timestamp_utc", "symbol", RANK_METRIC],
            )
        else:
            df = pd.read_parquet(
                _day_key,
                columns=["timestamp_utc", "symbol", RANK_METRIC]
            )

        # Keep only required cols
        df = df[["timestamp_utc", "symbol", RANK_METRIC]]

        # ----------------------------------------------------
        # Minute normalization (collapse sub-minute prints)
        # ----------------------------------------------------

        # Bucket timestamps to the minute
        df["minute"] = df["timestamp_utc"].dt.floor("min")

        # Keep last observation per symbol per minute
        df = (
            df.sort_values("timestamp_utc")
              .groupby(["symbol", "minute"])
              .last()
              .reset_index()
        )

        # ----------------------------------------------------
        # Volume: replace per-bar value with cumulative sum
        # from anchor forward (Option B).
        #
        # For price / OI the ranking value is the close of
        # each minute bar.  For volume it is the running total
        # of all bars from the anchor bar through that minute.
        # The anchor bar itself is used as the denominator so
        # the ratio at T=anchor is always 1 (0 % change).
        #
        # All three anchor modes are handled: midnight (first
        # bar), same-day ANCHOR_HOUR, and cross-midnight.
        # ----------------------------------------------------

        if RANK_METRIC == "volume":

            df = df.sort_values(["symbol", "minute"])
            _cross_midnight_handled = False

            if ANCHOR_CROSS_MIDNIGHT:
                # Cross-midnight: anchor bar lives in previous day
                prev_date_str = (
                    pd.Timestamp(date_str) - pd.Timedelta(days=1)
                ).strftime("%Y-%m-%d")
                _prev_date_key = pd.Timestamp(prev_date_str).date()
                _prev_exists = (
                    (USE_SINGLE_FILE and _prev_date_key in _DATE_TO_RG) or
                    (not USE_SINGLE_FILE and
                     (PARQUET_DIR / f"date={prev_date_str}").exists())
                )
                if _prev_exists:
                    if USE_SINGLE_FILE:
                        prev_df = _load_day_from_parquet(
                            _PF, _DATE_TO_RG, _prev_date_key,
                            columns=["timestamp_utc", "symbol", "volume"],
                        )
                    else:
                        prev_df = pd.read_parquet(
                            PARQUET_DIR / f"date={prev_date_str}",
                            columns=["timestamp_utc", "symbol", "volume"]
                        )
                    prev_df["minute"] = prev_df["timestamp_utc"].dt.floor("min")
                    prev_df = (
                        prev_df.sort_values("timestamp_utc")
                               .groupby(["symbol", "minute"])
                               .last()
                               .reset_index()
                    )
                    anchor_ts = pd.Timestamp(prev_date_str).replace(
                        hour=ANCHOR_HOUR, minute=0
                    )
                    prev_anchor_vol = (
                        prev_df[prev_df["minute"] <= anchor_ts]
                               .groupby("symbol")["volume"]
                               .last()
                    )
                    df["cumvol"] = df.groupby("symbol")["volume"].cumsum()
                    anchor_vol = df["symbol"].map(prev_anchor_vol).replace(0, pd.NA)
                    df["pct_change_midnight"] = df["cumvol"] / anchor_vol - 1
                    df = df.replace([float("inf"), -float("inf")], pd.NA)
                    df = df.dropna(subset=["pct_change_midnight"])
                    _cross_midnight_handled = True
                else:
                    tqdm.write(
                        f"  ⚠ No prev partition for {prev_date_str} "
                        f"— falling back to midnight anchor for volume"
                    )
                    # fall through to same-day path with midnight anchor
                    anchor_minute_map = df.groupby("symbol")["minute"].first()

            if not _cross_midnight_handled:
                # ── Resolve anchor minute (midnight or same-day hour) ──
                if INDEX_LOOKBACK == 0:
                    anchor_minute_map = df.groupby("symbol")["minute"].first()
                elif not ANCHOR_CROSS_MIDNIGHT:
                    anchor_ts = pd.Timestamp(date_str).replace(
                        hour=ANCHOR_HOUR, minute=0
                    )
                    anchor_minute_map = (
                        df[df["minute"] <= anchor_ts]
                          .groupby("symbol")["minute"]
                          .last()
                    )
                # else: anchor_minute_map already set in fallback above

                # Map each row to its anchor minute
                df["anchor_minute"] = df["symbol"].map(anchor_minute_map)

                # Anchor volume = single bar at the anchor minute
                anchor_vol_map = (
                    df[df["minute"] == df["anchor_minute"]]
                      .groupby("symbol")["volume"]
                      .last()
                )
                anchor_vol = df["symbol"].map(anchor_vol_map).replace(0, pd.NA)

                # Full cumsum then subtract pre-anchor volume
                df["cumvol"] = df.groupby("symbol")["volume"].cumsum()
                pre_anchor_cumvol = (
                    df[df["minute"] < df["anchor_minute"]]
                      .groupby("symbol")["volume"]
                      .sum()
                )
                df["pre_anchor_cumvol"] = (
                    df["symbol"].map(pre_anchor_cumvol).fillna(0)
                )
                df["cumvol_from_anchor"] = df["cumvol"] - df["pre_anchor_cumvol"]

                df["pct_change_midnight"] = (
                    df["cumvol_from_anchor"] / anchor_vol - 1
                )
                df = df.replace([float("inf"), -float("inf")], pd.NA)
                df = df.dropna(subset=["pct_change_midnight"])

        # ----------------------------------------------------
        # Anchor-based % change — price and open_interest only
        # (volume has already computed pct_change_midnight above)
        # ----------------------------------------------------

        else:

            if INDEX_LOOKBACK == 0:
                # Legacy midnight anchor: first bar of the day
                anchor_vals = (
                    df.sort_values("minute")
                      .groupby("symbol")[RANK_METRIC]
                      .transform("first")
                )
            elif not ANCHOR_CROSS_MIDNIGHT:
                # Same-day anchor: find the bar closest to ANCHOR_HOUR:00
                anchor_ts = pd.Timestamp(date_str).replace(
                    hour=ANCHOR_HOUR, minute=0
                )
                anchor_df = (
                    df[df["minute"] <= anchor_ts]
                      .sort_values("minute")
                      .groupby("symbol")[RANK_METRIC]
                      .last()
                )
                anchor_vals = df["symbol"].map(anchor_df)
            else:
                # Cross-midnight anchor: value lives in previous day's partition
                prev_date_str = (
                    pd.Timestamp(date_str) - pd.Timedelta(days=1)
                ).strftime("%Y-%m-%d")
                _prev_date_key = pd.Timestamp(prev_date_str).date()
                _prev_exists = (
                    (USE_SINGLE_FILE and _prev_date_key in _DATE_TO_RG) or
                    (not USE_SINGLE_FILE and
                     (PARQUET_DIR / f"date={prev_date_str}").exists())
                )
                if _prev_exists:
                    if USE_SINGLE_FILE:
                        prev_df = _load_day_from_parquet(
                            _PF, _DATE_TO_RG, _prev_date_key,
                            columns=["timestamp_utc", "symbol", RANK_METRIC],
                        )
                    else:
                        prev_df = pd.read_parquet(
                            PARQUET_DIR / f"date={prev_date_str}",
                            columns=["timestamp_utc", "symbol", RANK_METRIC]
                        )
                    prev_df["minute"] = prev_df["timestamp_utc"].dt.floor("min")
                    prev_df = (
                        prev_df.sort_values("timestamp_utc")
                               .groupby(["symbol", "minute"])
                               .last()
                               .reset_index()
                    )
                    anchor_ts = pd.Timestamp(prev_date_str).replace(
                        hour=ANCHOR_HOUR, minute=0
                    )
                    prev_anchor = (
                        prev_df[prev_df["minute"] <= anchor_ts]
                               .sort_values("minute")
                               .groupby("symbol")[RANK_METRIC]
                               .last()
                    )
                    anchor_vals = df["symbol"].map(prev_anchor)
                else:
                    tqdm.write(
                        f"  ⚠ No prev partition for {prev_date_str} "
                        f"— falling back to midnight anchor"
                    )
                    anchor_vals = (
                        df.sort_values("minute")
                          .groupby("symbol")[RANK_METRIC]
                          .transform("first")
                    )

            anchor_vals = anchor_vals.replace(0, pd.NA)

            df["pct_change_midnight"] = (
                df[RANK_METRIC] / anchor_vals - 1
            )

            df = df.replace([float("inf"), -float("inf")], pd.NA)
            df = df.dropna(subset=["pct_change_midnight"])

        # ----------------------------------------------------
        # VECTOR RANKING
        # ----------------------------------------------------

        df["rank"] = (
            df.groupby("minute")["pct_change_midnight"]
              .rank(method="first", ascending=False)
              .astype("int16")
        )

        leaders = df[df["rank"] <= TOP_N]

        # ----------------------------------------------------
        # WRITE TO DB (long format, before the pivot)
        # ----------------------------------------------------
        # The pivot below collapses long → wide and discards pct_change_midnight.
        # We write the long-format rows to market.leaderboards here while
        # everything is still available. Failures are non-fatal — the parquet/CSV
        # write below still happens regardless.
        _rows_inserted_today = 0
        if WRITE_TO_DB:
            _rows_inserted_today = write_leaders_to_db(leaders, date_str)

        # ----------------------------------------------------
        # Pivot leaderboard
        # ----------------------------------------------------

        leaders = leaders.sort_values(
            ["minute", "rank"]
        )

        leaders["rank_col"] = (
            "R" + leaders["rank"].astype(int).astype(str)
        )

        out_df = (
            leaders
            .pivot(
                index="minute",
                columns="rank_col",
                values="symbol"
            )
            .reset_index()
        )

        # Rename minute → timestamp_utc
        out_df = out_df.rename(columns={"minute": "timestamp_utc"})

        # Sort columns R1 → R100
        out_df = out_df.reindex(
            sorted(out_df.columns, key=lambda x: (
                0 if x == "timestamp_utc" else int(x[1:])
            )),
            axis=1
        )

        rank_cols = [f"R{i}" for i in range(1, TOP_N+1)]

        for col in rank_cols:
            if col not in out_df.columns:
                out_df[col] = pd.NA

        # ----------------------------------------------------
        # Force full 1,440 minute index
        # ----------------------------------------------------

        day_start = pd.to_datetime(date_str)

        full_index = pd.date_range(
            start=day_start,
            periods=1440,
            freq="min"
        )

        out_df = (
            out_df
            .set_index("timestamp_utc")
            .reindex(full_index)
            .reset_index()
            .rename(columns={"index": "timestamp_utc"})
        )

        # ----------------------------------------------------
        # SAVE
        # ----------------------------------------------------

        if SAVE_DAILY_FILES:

            out_file = OUTPUT_DIR / (
                f"wide_intraday_pct_leaderboard_{RANK_METRIC}_top{TOP_N}_{date_str}.csv"
            )

            out_df.to_csv(out_file, index=False)

        if BUILD_MASTER_FILE:
            # Lazy-open the writer on the first day so the path is known
            # and the schema is fixed once. After that, every day writes
            # one row group (1440 rows) directly to disk.
            if _master_writer is None:
                _master_path = OUTPUT_DIR / (
                    f"intraday_pct_leaderboard_{RANK_METRIC}_top{TOP_N}_anchor{ANCHOR_HHMM}_ALL.parquet"
                )
                _master_writer = pq.ParquetWriter(
                    _master_path,
                    _master_schema(TOP_N),
                    compression="snappy",
                )
            # Cast the day's wide-format DataFrame to the pinned schema.
            # preserve_index=False keeps the writer aligned with the column
            # list, and the schema= arg coerces nullable rank columns to
            # the canonical string type even if a day has all-null entries.
            _day_table = pa.Table.from_pandas(
                out_df,
                schema=_master_schema(TOP_N),
                preserve_index=False,
            )
            _master_writer.write_table(_day_table)
            _master_rows_written += _day_table.num_rows

        tqdm.write(
            f"✔ {date_str} → "
            f"{df['symbol'].nunique()} symbols | "
            f"{len(out_df)} minutes"
        )

        # Per-day success — bump job progress with rows actually inserted
        # this iteration (0 if --no-db-write or write_leaders_to_db failed).
        job_increment(JOB_ID, _rows_inserted_today)

    except Exception as e:
        _day_label = date_str if 'date_str' in dir() else str(_day_key)
        tqdm.write(f"❌ Failed → {_day_label} | {e}")
        _failure_count += 1
        if len(_failure_summaries) < 5:
            _failure_summaries.append(f"{_day_label}: {type(e).__name__}: {e}")
        # Still call job_increment so the progress bar advances honestly —
        # the job knows this date was attempted, just didn't write any rows.
        job_increment(JOB_ID, 0)


# ============================================================
# CLOSE MASTER FILE
# ============================================================
# The master parquet was streamed one day at a time inside the loop above,
# so all that remains is to close the writer. The previous all-at-once
# pd.concat + to_parquet path OOM-killed at ~10 GB on full-backlog runs.

if BUILD_MASTER_FILE:
    if _master_writer is not None:
        _master_writer.close()
        print(
            f"\n✅ Master file built → {_master_path.name}\n"
            f"Rows: {_master_rows_written:,}"
        )
    else:
        # No frame ever produced — every day either failed or was skipped
        # by the checkpoint. The file does NOT exist on disk; downstream
        # consumers (overlap_analysis.py) must check for its presence.
        print("⚠️  No dataframes produced — master parquet was not written.")

# ============================================================
# JOB FINAL STATUS — option (c): counter-based decision
# ============================================================
# If any per-day failures happened, the job is marked failed with up to
# the first 5 failures listed in error_msg. Otherwise it's complete.
# This is a more honest signal than "any failure = fatal" or "swallow all
# failures" — the master file may still have been built with the surviving
# days, and the user needs to know whether there's anything to investigate.
if _failure_count > 0:
    _summary = (
        f"{_failure_count} of {len(_date_groups)} dates failed. "
        f"First failures: " + " | ".join(_failure_summaries)
    )
    job_fail(JOB_ID, _summary)
else:
    job_complete(JOB_ID)

# ============================================================
# DONE
# ============================================================

end_time = datetime.now()

print("\n✅ Leaderboard build complete")
print("Start :", script_start)
print("End   :", end_time)
print("Elapsed:", end_time - script_start)
if _failure_count > 0:
    print(f"⚠️  {_failure_count} of {len(_date_groups)} dates failed — see indexer_jobs row {JOB_ID} for details")

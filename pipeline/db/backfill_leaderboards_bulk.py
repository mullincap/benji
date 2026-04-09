#!/usr/bin/env python3
"""
pipeline/db/backfill_leaderboards_bulk.py
==========================================
Fast bulk backfill of market.leaderboards from wide-format parquet files.

This is the production backfill path. It supersedes (but does not replace)
backfill_leaderboards.py, which uses row-by-row execute_values and is too
slow for additive backfills against a populated hypertable. The slow script
is kept as a documented fallback.

Strategy
--------
For each metric:
  1. Extract every (timestamp_utc, rank, symbol_id) triple from the parquet
     by iterating one rank column at a time. Memory bounded per column
     (~5 MB), no full file load.
  2. Stream the triples directly into a plain staging table via psycopg2's
     copy_expert (CSV TEXT mode, no temp file on disk). The staging table
     has NO indexes and NO constraints, so the COPY runs at raw I/O speed
     (~50-100k rows/sec on commodity disk).
  3. Run a single
       INSERT INTO market.leaderboards (...)
       SELECT timestamp_utc, %s::text, %s::text, %s::smallint, rank, symbol_id, NULL
       FROM staging
       ON CONFLICT DO NOTHING
     The conflict check uses the existing unique index leaderboards_pkey
     and runs as one sorted-merge instead of N individual lookups, which
     is what makes this 50-100× faster than the row-by-row approach.
  4. Drop the staging table.

Compared to backfill_leaderboards.py
------------------------------------
- Same input format (wide parquet files with R1..R333 columns)
- Same idempotency guarantees (ON CONFLICT DO NOTHING)
- Same correctness (uses the same symbol_map)
- ~50-100× faster on additive backfills against a populated table
- Slightly more memory (one whole column at a time instead of one row at
  a time), but bounded — peak ~50 MB for the largest columns

Usage
-----
    # All metrics with parquet files present
    python3 pipeline/db/backfill_leaderboards_bulk.py

    # One specific metric
    python3 pipeline/db/backfill_leaderboards_bulk.py --metric price

    # Custom parquet root
    LEADERBOARD_BASE=/some/path python3 pipeline/db/backfill_leaderboards_bulk.py
"""

import argparse
import io
import os
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

LEADERBOARD_BASE = Path(os.environ.get("LEADERBOARD_BASE", "/mnt/quant-data/leaderboards"))

METRIC_DIRS = {
    "price":         LEADERBOARD_BASE / "price",
    "open_interest": LEADERBOARD_BASE / "open_interest",
    "volume":        LEADERBOARD_BASE / "volume",
}

VARIANT     = "close"
ANCHOR_HOUR = 0


def build_symbol_map(cur) -> dict:
    cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
    return {row[0]: row[1] for row in cur.fetchall()}


def stream_triples_for_column(parquet_path: Path, rank_col: str, symbol_map: dict, csv_buffer: io.StringIO) -> tuple[int, int]:
    """Read one rank column from the parquet, write `timestamp_utc,rank,symbol_id`
    rows to csv_buffer in CSV TEXT mode. Returns (written, skipped) counts."""
    pf = pq.ParquetFile(parquet_path)
    df = pf.read(columns=["timestamp_utc", rank_col]).to_pandas()
    df = df.dropna(subset=[rank_col])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    rank_num = int(rank_col[1:])

    written = 0
    skipped = 0
    # Build the CSV lines via a list join — faster than per-row csv_buffer.write
    lines = []
    for ts, sym in zip(df["timestamp_utc"].values, df[rank_col].values):
        sid = symbol_map.get(sym)
        if sid is None:
            skipped += 1
            continue
        # CSV TEXT mode, comma-separated, ISO 8601 timestamp
        # numpy.datetime64 → ISO via pd.Timestamp
        ts_str = pd.Timestamp(ts).isoformat()
        lines.append(f"{ts_str},{rank_num},{sid}")
        written += 1

    csv_buffer.write("\n".join(lines))
    if lines:
        csv_buffer.write("\n")
    return written, skipped


def process_metric(metric: str, parquet_path: Path, symbol_map: dict, conn) -> tuple[int, int, int]:
    """Bulk-load all rank columns from parquet_path into market.leaderboards
    for the given metric. Returns (rows_in_staging, rows_inserted, rows_skipped_no_symbol)."""
    cur = conn.cursor()

    pf = pq.ParquetFile(parquet_path)
    schema_names = pf.schema_arrow.names
    rank_cols = sorted(
        [c for c in schema_names if c.startswith("R") and c[1:].isdigit()],
        key=lambda x: int(x[1:])
    )

    if not rank_cols:
        print(f"  ⚠ No rank columns in {parquet_path.name} — skipping")
        return 0, 0, 0

    print(f"  Source: {parquet_path.name}")
    print(f"          {len(rank_cols)} rank columns ({rank_cols[0]}..{rank_cols[-1]}), {pf.metadata.num_rows:,} rows in file")

    # Step 1: create staging table (plain table, no indexes, no constraints)
    staging = f"_staging_leaderboards_{metric}_{int(time.time())}"
    print(f"  → Creating staging table {staging}")
    cur.execute(f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS public.{staging} (
            timestamp_utc timestamptz NOT NULL,
            rank          smallint    NOT NULL,
            symbol_id     integer     NOT NULL
        )
    """)
    conn.commit()

    # Step 2: stream all rank columns into a single in-memory CSV buffer,
    # then COPY in one shot. For 333 cols × ~573k rows × ~99% fill, that's
    # ~190M rows × ~30 bytes per CSV line ≈ ~5.7 GB in memory. Too much.
    # Instead, COPY one rank column at a time so memory stays bounded at
    # ~17 MB per column (one column's worth of CSV).
    total_written = 0
    total_skipped = 0
    start = time.time()

    for i, rank_col in enumerate(rank_cols, 1):
        buf = io.StringIO()
        written, skipped = stream_triples_for_column(parquet_path, rank_col, symbol_map, buf)
        total_written += written
        total_skipped += skipped

        # Rewind and stream into staging via COPY
        buf.seek(0)
        cur.copy_expert(
            f"COPY public.{staging} (timestamp_utc, rank, symbol_id) FROM STDIN WITH (FORMAT csv)",
            buf,
        )
        conn.commit()
        buf.close()

        elapsed = time.time() - start
        rate = total_written / elapsed if elapsed > 0 else 0
        eta_sec = (len(rank_cols) - i) * (elapsed / i) if i > 0 else 0
        print(
            f"    [{i:3d}/{len(rank_cols)}] {rank_col:>5s} → "
            f"+{written:>7,} (skipped {skipped:>5,}) "
            f"| total {total_written:>10,} | {rate:>6,.0f} rows/s | ETA {eta_sec:.0f}s"
        )

    copy_elapsed = time.time() - start
    print(f"  ✓ Staging COPY done: {total_written:,} rows in {copy_elapsed:.1f}s")

    # Step 3: insert from staging into the real table with conflict handling
    print(f"  → INSERT INTO market.leaderboards FROM {staging} (ON CONFLICT DO NOTHING)")
    insert_start = time.time()
    cur.execute(f"""
        INSERT INTO market.leaderboards
            (timestamp_utc, metric, variant, anchor_hour, rank, symbol_id, pct_change)
        SELECT timestamp_utc, %s, %s, %s::smallint, rank, symbol_id, NULL
        FROM public.{staging}
        ON CONFLICT (timestamp_utc, metric, variant, anchor_hour, rank) DO NOTHING
    """, (metric, VARIANT, ANCHOR_HOUR))
    inserted = cur.rowcount
    conn.commit()
    insert_elapsed = time.time() - insert_start
    print(f"  ✓ Inserted {inserted:,} new rows in {insert_elapsed:.1f}s")

    # Step 4: drop staging table
    cur.execute(f"DROP TABLE public.{staging}")
    conn.commit()
    print(f"  ✓ Dropped staging table {staging}")

    cur.close()
    return total_written, inserted, total_skipped


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    parser.add_argument(
        "--metric",
        choices=list(METRIC_DIRS.keys()),
        default=None,
        help="Run only one metric. Default: all metrics with parquet files present.",
    )
    args = parser.parse_args()

    metrics = [args.metric] if args.metric else list(METRIC_DIRS.keys())

    conn = get_conn()
    cur  = conn.cursor()
    symbol_map = build_symbol_map(cur)
    cur.close()
    print(f"Symbol map: {len(symbol_map)} entries")

    grand_written = 0
    grand_inserted = 0
    grand_skipped = 0
    grand_start = time.time()

    for metric in metrics:
        directory = METRIC_DIRS[metric]
        if not directory.exists():
            print(f"\n⚠ {metric}: directory {directory} does not exist — skipping")
            continue

        # Pick the canonical anchor0000 master file. (Other files in the dir
        # are filtered/derived intermediates that overlap_analysis.py writes.)
        candidates = sorted(directory.glob(f"intraday_pct_leaderboard_{metric}_top*_anchor0000_ALL.parquet"))
        if not candidates:
            print(f"\n⚠ {metric}: no canonical *_anchor0000_ALL.parquet in {directory} — skipping")
            continue
        if len(candidates) > 1:
            print(f"\n⚠ {metric}: multiple candidate parquets, picking the largest:")
            for c in candidates:
                print(f"    {c.stat().st_size / 1024 / 1024:>7.0f} MB  {c.name}")
            candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
        parquet_path = candidates[0]

        print(f"\n{'=' * 60}")
        print(f"METRIC: {metric}")
        print(f"{'=' * 60}")
        written, inserted, skipped = process_metric(metric, parquet_path, symbol_map, conn)
        grand_written  += written
        grand_inserted += inserted
        grand_skipped  += skipped

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM market.leaderboards")
    db_total = cur.fetchone()[0]
    cur.close()

    grand_elapsed = time.time() - grand_start
    print(f"\n{'=' * 60}")
    print(f"✅ Bulk backfill complete in {grand_elapsed:.1f}s ({grand_elapsed/60:.1f} min)")
    print(f"   Rows staged:    {grand_written:,}")
    print(f"   Rows inserted:  {grand_inserted:,} (after ON CONFLICT dedup)")
    print(f"   Rows skipped:   {grand_skipped:,} (no symbol_id match)")
    print(f"   Total in market.leaderboards: {db_total:,}")

    conn.close()


if __name__ == "__main__":
    main()

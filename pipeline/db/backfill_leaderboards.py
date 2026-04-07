#!/usr/bin/env python3
"""
pipeline/db/backfill_leaderboards.py
======================================
Backfills market.leaderboards from wide-format parquet files.

Memory strategy: reads ONE rank column at a time (e.g. just R1, then R2...).
Peak memory = timestamp_utc column + one symbol column = ~10MB regardless
of file size or number of ranks. No melt, no full file load.

Safe to re-run — uses ON CONFLICT DO NOTHING.

Usage:
    python3 pipeline/db/backfill_leaderboards.py
"""

import gc
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

LEADERBOARD_BASE = Path("/mnt/quant-data/leaderboards")
METRIC_DIRS = {
    "price":         LEADERBOARD_BASE / "price",
    "open_interest": LEADERBOARD_BASE / "open_interest",
    "volume":        LEADERBOARD_BASE / "volume",
}

VARIANT     = "close"
ANCHOR_HOUR = 0
BATCH_SIZE  = 50_000


def build_symbol_map(cur) -> dict:
    cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
    return {row[0]: row[1] for row in cur.fetchall()}


def process_file(parquet_path: Path, metric: str, symbol_map: dict, cur, conn) -> tuple[int, int]:
    """
    Process one rank column at a time — reads only timestamp_utc + R1,
    inserts, then reads timestamp_utc + R2, etc.
    Peak memory is ~10MB regardless of file size.
    """
    print(f"  Opening {parquet_path.name} (column-by-column mode)...")
    pf = pq.ParquetFile(parquet_path)

    schema_names = pf.schema_arrow.names
    rank_cols = sorted(
        [c for c in schema_names if c.startswith("R") and c[1:].isdigit()],
        key=lambda x: int(x[1:])
    )

    if "timestamp_utc" not in schema_names:
        print(f"    ⚠ No timestamp_utc column — skipping")
        return 0, 0
    if not rank_cols:
        print(f"    ⚠ No rank columns found — skipping")
        return 0, 0

    print(f"    {len(rank_cols)} rank columns — processing one at a time...")

    inserted = 0
    skipped  = 0
    start    = time.time()

    for i, rank_col in enumerate(rank_cols):
        rank_num = int(rank_col[1:])

        # Read only timestamp_utc + this one rank column — tiny memory footprint
        df = pf.read(columns=["timestamp_utc", rank_col]).to_pandas()
        df = df.dropna(subset=[rank_col])
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

        batch_rows = []
        for row in df.itertuples(index=False):
            symbol_id = symbol_map.get(getattr(row, rank_col))
            if symbol_id is None:
                skipped += 1
                continue
            batch_rows.append((
                row.timestamp_utc,
                metric,
                VARIANT,
                ANCHOR_HOUR,
                rank_num,
                symbol_id,
                None,
            ))

            if len(batch_rows) >= BATCH_SIZE:
                result = execute_values(cur, """
                    INSERT INTO market.leaderboards
                        (timestamp_utc, metric, variant, anchor_hour, rank, symbol_id, pct_change)
                    VALUES %s
                    ON CONFLICT (timestamp_utc, metric, variant, anchor_hour, rank) DO NOTHING
                    RETURNING timestamp_utc
                """, batch_rows, fetch=True)
                inserted += len(result)
                conn.commit()
                batch_rows = []

        if batch_rows:
            result = execute_values(cur, """
                INSERT INTO market.leaderboards
                    (timestamp_utc, metric, variant, anchor_hour, rank, symbol_id, pct_change)
                VALUES %s
                ON CONFLICT (timestamp_utc, metric, variant, anchor_hour, rank) DO NOTHING
                RETURNING timestamp_utc
            """, batch_rows, fetch=True)
            inserted += len(result)
            conn.commit()

        del df, batch_rows
        gc.collect()

        elapsed = time.time() - start
        pct = (i + 1) / len(rank_cols) * 100
        print(f"    R{rank_num} [{i+1}/{len(rank_cols)}] ({pct:.0f}%) — {inserted:,} inserted, {skipped:,} skipped — {elapsed:.0f}s")

    return inserted, skipped


def main():
    conn = get_conn()
    cur  = conn.cursor()

    symbol_map = build_symbol_map(cur)
    print(f"Symbol map: {len(symbol_map)} entries\n")

    total_inserted = 0
    total_skipped  = 0

    for metric, directory in METRIC_DIRS.items():
        if not directory.exists():
            print(f"⚠ Directory not found, skipping: {directory}")
            continue

        parquet_files = sorted(directory.glob("*.parquet"))
        if not parquet_files:
            print(f"⚠ No parquet files in {directory} — skipping")
            continue

        print(f"\n{'='*60}")
        print(f"Metric: {metric} — {len(parquet_files)} file(s) in {directory}")
        print(f"{'='*60}")

        metric_inserted = 0
        metric_skipped  = 0
        start = time.time()

        for pf_path in parquet_files:
            ins, skp = process_file(pf_path, metric, symbol_map, cur, conn)
            metric_inserted += ins
            metric_skipped  += skp
            print(f"    ✓ {pf_path.name}: {ins:,} inserted, {skp:,} skipped")

        elapsed = time.time() - start
        print(f"  Metric total: {metric_inserted:,} inserted, {metric_skipped:,} skipped ({elapsed:.1f}s)")
        total_inserted += metric_inserted
        total_skipped  += metric_skipped

    cur.execute("SELECT COUNT(*) FROM market.leaderboards")
    db_total = cur.fetchone()[0]

    print(f"\n{'='*60}")
    print(f"✅ Leaderboard backfill complete")
    print(f"   Total inserted: {total_inserted:,}")
    print(f"   Total skipped (no symbol match): {total_skipped:,}")
    print(f"   Total rows in market.leaderboards: {db_total:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

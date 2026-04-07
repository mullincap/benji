#!/usr/bin/env python3
"""
pipeline/db/backfill_leaderboards.py
======================================
Backfills market.leaderboards from the wide-format parquet files written
by build_intraday_leaderboard.py.

Parquet format (wide):
  Rows:    one per timestamp_utc (minute)
  Columns: timestamp_utc, R1, R2, ..., R100
  Values:  symbol string (e.g. 'BTCUSDT') at each rank position

Uses pyarrow row-group reading to avoid OOM — never loads more than one
row group (~100k rows) into memory at once regardless of file size.

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
    """binance_id → symbol_id"""
    cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
    return {row[0]: row[1] for row in cur.fetchall()}


def process_file(parquet_path: Path, metric: str, symbol_map: dict, cur, conn) -> tuple[int, int]:
    """
    Read parquet one row group at a time using pyarrow.
    Peak memory = one row group (~100k rows) not the full file.
    Returns (inserted, skipped).
    """
    print(f"  Opening {parquet_path.name} (pyarrow row-group mode)...")
    pf = pq.ParquetFile(parquet_path)
    num_groups = pf.num_row_groups

    # Detect columns from schema — no full file load needed
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

    cols_to_read = ["timestamp_utc"] + rank_cols
    print(f"    {num_groups} row groups × {len(rank_cols)} ranks")

    inserted = 0
    skipped  = 0
    start    = time.time()

    for g in range(num_groups):
        # Read one row group only — typically ~100k rows
        chunk = pf.read_row_group(g, columns=cols_to_read).to_pandas()
        chunk["timestamp_utc"] = pd.to_datetime(chunk["timestamp_utc"], utc=True)

        # Melt wide → long for this chunk only
        df_long = chunk.melt(
            id_vars="timestamp_utc",
            value_vars=rank_cols,
            var_name="rank_col",
            value_name="symbol"
        )
        df_long = df_long.dropna(subset=["symbol"])
        df_long["rank"] = df_long["rank_col"].str[1:].astype(int)

        batch_rows = []
        for row in df_long.itertuples(index=False):
            symbol_id = symbol_map.get(row.symbol)
            if symbol_id is None:
                skipped += 1
                continue
            batch_rows.append((
                row.timestamp_utc,
                metric,
                VARIANT,
                ANCHOR_HOUR,
                row.rank,
                symbol_id,
                None,  # pct_change — not in leaderboard parquets
            ))

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

        elapsed = time.time() - start
        pct = (g + 1) / num_groups * 100
        print(f"    Group {g+1}/{num_groups} ({pct:.0f}%) — {inserted:,} inserted, {skipped:,} skipped — {elapsed:.0f}s")

        del chunk, df_long, batch_rows
        gc.collect()

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

        for pf in parquet_files:
            ins, skp = process_file(pf, metric, symbol_map, cur, conn)
            metric_inserted += ins
            metric_skipped  += skp
            print(f"    ✓ {pf.name}: {ins:,} inserted, {skp:,} skipped")

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

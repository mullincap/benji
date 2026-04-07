#!/usr/bin/env python3
"""
pipeline/db/backfill_futures_1m.py
====================================
Backfills market.futures_1m from the master parquet file using pyarrow
row-group streaming (never loads the full 6GB file into memory).

Source:  /mnt/quant-data/raw/amberdata/master_data_table.parquet
         793 row groups, ~312M rows
         Columns: timestamp_utc, symbol, price, volume,
                  open_interest, funding_rate, long_short_ratio

Column mapping:
  price         -> close
  symbol (TEXT) -> symbol_id via market.symbols.binance_id
  source_id     = 1 (amberdata_binance) for all rows
  open/high/low = NULL (not in source data)

Safe to re-run -- uses ON CONFLICT DO NOTHING.

Usage:
    python3 pipeline/db/backfill_futures_1m.py
"""

import sys
import time
from pathlib import Path

import pyarrow.parquet as pq
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

PARQUET_PATH = Path("/mnt/quant-data/raw/amberdata/master_data_table.parquet")
SOURCE_ID    = 1  # amberdata_binance
PAGE_SIZE    = 10_000

# Parquet columns that map directly to futures_1m columns (same name)
DIRECT_COLS = ["volume", "open_interest", "funding_rate", "long_short_ratio"]

INSERT_COLS = ["timestamp_utc", "symbol_id", "source_id", "close"] + DIRECT_COLS

INSERT_SQL = f"""
    INSERT INTO market.futures_1m ({', '.join(INSERT_COLS)})
    VALUES %s
    ON CONFLICT (timestamp_utc, symbol_id, source_id) DO NOTHING
"""


def build_symbol_map(cur) -> dict:
    """Build binance_id TEXT -> symbol_id integer lookup."""
    cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
    return {row[0]: row[1] for row in cur.fetchall()}


def main():
    if not PARQUET_PATH.exists():
        print(f"Parquet file not found: {PARQUET_PATH}")
        sys.exit(1)

    pf = pq.ParquetFile(PARQUET_PATH)
    num_rg    = pf.metadata.num_row_groups
    total_rows = pf.metadata.num_rows
    print(f"Parquet: {num_rg} row groups, {total_rows:,} rows")

    conn = get_conn()
    cur  = conn.cursor()

    symbol_map = build_symbol_map(cur)
    print(f"Symbol map: {len(symbol_map)} entries")

    cur.execute("SELECT COUNT(*) FROM market.futures_1m WHERE source_id = %s", (SOURCE_ID,))
    already = cur.fetchone()[0]
    if already > 0:
        print(f"Already in futures_1m: {already:,} rows (duplicates will be skipped)")

    read_cols = ["timestamp_utc", "symbol", "price"] + DIRECT_COLS

    inserted_total = 0
    skipped_total  = 0
    rows_read      = 0
    start          = time.time()

    print(f"\nStarting backfill from {PARQUET_PATH.name} ...")

    for rg_idx in range(num_rg):
        table = pf.read_row_group(rg_idx, columns=read_cols)
        rg_rows = table.num_rows
        rows_read += rg_rows

        # Convert to column arrays for fast access
        ts_col    = table.column("timestamp_utc")
        sym_col   = table.column("symbol")
        price_col = table.column("price")
        direct_arrays = [table.column(c) for c in DIRECT_COLS]

        batch = []
        rg_skipped = 0

        for i in range(rg_rows):
            symbol = sym_col[i].as_py()
            symbol_id = symbol_map.get(symbol)
            if symbol_id is None:
                rg_skipped += 1
                continue

            row = (
                ts_col[i].as_py(),       # timestamp_utc (string -> pg casts to timestamptz)
                symbol_id,
                SOURCE_ID,
                price_col[i].as_py(),    # price -> close
            ) + tuple(arr[i].as_py() for arr in direct_arrays)

            batch.append(row)

            if len(batch) >= PAGE_SIZE:
                execute_values(cur, INSERT_SQL, batch, page_size=PAGE_SIZE)
                inserted_total += len(batch)
                batch = []

        # Flush remaining
        if batch:
            execute_values(cur, INSERT_SQL, batch, page_size=PAGE_SIZE)
            inserted_total += len(batch)

        skipped_total += rg_skipped
        conn.commit()

        elapsed = time.time() - start
        rate    = rows_read / elapsed if elapsed > 0 else 0
        eta     = (total_rows - rows_read) / rate if rate > 0 else 0
        pct     = rows_read / total_rows * 100

        print(
            f"  Row group {rg_idx + 1}/{num_rg} ({pct:.1f}%) "
            f"| read {rows_read:,} | inserted {inserted_total:,} | skipped {skipped_total:,} "
            f"| {rate:,.0f} rows/s | ETA {eta:.0f}s"
        )

    elapsed = time.time() - start
    cur.execute("SELECT COUNT(*) FROM market.futures_1m")
    db_total = cur.fetchone()[0]

    print(f"\nBackfill complete in {elapsed:.1f}s")
    print(f"  Rows read:    {rows_read:,}")
    print(f"  Rows sent:    {inserted_total:,}")
    print(f"  Rows skipped: {skipped_total:,} (no symbol match)")
    print(f"  Total in market.futures_1m: {db_total:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

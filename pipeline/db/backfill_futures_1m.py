#!/usr/bin/env python3
"""
pipeline/db/backfill_futures_1m.py
====================================
Migrates market_data_1m → market.futures_1m.

Column mapping:
  price         → close
  symbol (TEXT) → symbol_id via market.symbols lookup
  source_id     = 1 (amberdata_binance) for all rows
  open/high/low = NULL (not in source data)

Safe to re-run — uses ON CONFLICT DO NOTHING.

Usage:
    python3 pipeline/db/backfill_futures_1m.py
"""

import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

BATCH_SIZE  = 50_000
SOURCE_ID   = 1  # amberdata_binance

# All columns that exist in both tables (excluding price→close and symbol→symbol_id)
DIRECT_COLS = [
    "volume", "quote_volume", "trades",
    "taker_buy_base_vol", "taker_buy_quote_vol",
    "open_interest", "funding_rate", "long_short_ratio",
    "trade_delta", "long_liqs", "short_liqs",
    "last_bid_depth", "last_ask_depth", "last_depth_imbalance",
    "last_spread_pct", "spread_pct", "bid_ask_imbalance",
    "basis_pct", "market_cap_usd", "market_cap_rank",
]


def get_existing_cols(cur) -> set:
    """Return columns that actually exist in market_data_1m."""
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'market_data_1m'
    """)
    return {row[0] for row in cur.fetchall()}


def build_symbol_map(cur) -> dict:
    """Build symbol TEXT → symbol_id integer lookup."""
    cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
    return {row[0]: row[1] for row in cur.fetchall()}


def main():
    conn = get_conn()
    cur  = conn.cursor()

    # Check old table exists
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'market_data_1m'
        )
    """)
    if not cur.fetchone()[0]:
        print("market_data_1m does not exist — nothing to migrate.")
        cur.close()
        conn.close()
        return

    # Get actual columns in old table
    existing_cols = get_existing_cols(cur)
    use_cols = [c for c in DIRECT_COLS if c in existing_cols]
    missing_cols = [c for c in DIRECT_COLS if c not in existing_cols]
    if missing_cols:
        print(f"  Note: {len(missing_cols)} columns not in source (will be NULL): {missing_cols}")

    # Build symbol map
    symbol_map = build_symbol_map(cur)
    print(f"  Symbol map: {len(symbol_map)} entries")

    # Count total rows
    cur.execute("SELECT COUNT(*) FROM market_data_1m")
    total_rows = cur.fetchone()[0]
    print(f"  Source rows: {total_rows:,}")

    # Check already migrated
    cur.execute("SELECT COUNT(*) FROM market.futures_1m WHERE source_id = %s", (SOURCE_ID,))
    already_done = cur.fetchone()[0]
    if already_done > 0:
        print(f"  Already migrated: {already_done:,} rows (will skip duplicates via ON CONFLICT)")

    # Build SELECT query
    col_select = ", ".join(use_cols)
    query = f"SELECT timestamp_utc, symbol, price, {col_select} FROM market_data_1m ORDER BY timestamp_utc OFFSET %s LIMIT %s"

    # Build INSERT columns list
    insert_cols = ["timestamp_utc", "symbol_id", "source_id", "close"] + use_cols
    # Add NULL placeholders for missing direct cols (open, high, low not in source)
    placeholders = "%s" + ", %s" * (len(insert_cols) - 1)

    insert_sql = f"""
        INSERT INTO market.futures_1m ({', '.join(insert_cols)})
        VALUES %s
        ON CONFLICT (timestamp_utc, symbol_id, source_id) DO NOTHING
    """

    inserted    = 0
    skipped_sym = 0
    offset      = 0
    start       = time.time()
    batch_num   = 0

    print(f"\nStarting migration in batches of {BATCH_SIZE:,}...")

    while True:
        cur.execute(query, (offset, BATCH_SIZE))
        rows = cur.fetchall()
        if not rows:
            break

        batch_num += 1
        batch_rows = []
        batch_skip = 0

        for row in rows:
            ts      = row[0]
            symbol  = row[1]
            price   = row[2]
            rest    = row[3:]

            symbol_id = symbol_map.get(symbol)
            if symbol_id is None:
                batch_skip += 1
                continue

            batch_rows.append((ts, symbol_id, SOURCE_ID, price) + rest)

        if batch_rows:
            result = execute_values(cur, insert_sql, batch_rows, fetch=True, page_size=10_000)
            inserted += len(result)
        skipped_sym += batch_skip
        conn.commit()

        offset  += BATCH_SIZE
        elapsed  = time.time() - start
        pct      = min(offset / total_rows * 100, 100)
        rate     = offset / elapsed if elapsed > 0 else 0
        eta      = (total_rows - offset) / rate if rate > 0 else 0

        print(
            f"  Batch {batch_num}: {offset:,}/{total_rows:,} ({pct:.1f}%) "
            f"| inserted {inserted:,} | skipped {skipped_sym:,} "
            f"| {rate:,.0f} rows/s | ETA {eta:.0f}s"
        )

        if len(rows) < BATCH_SIZE:
            break

    elapsed = time.time() - start
    cur.execute("SELECT COUNT(*) FROM market.futures_1m")
    db_total = cur.fetchone()[0]

    print(f"\n✅ Migration complete in {elapsed:.1f}s")
    print(f"   Rows inserted this run: {inserted:,}")
    print(f"   Rows skipped (no symbol match): {skipped_sym:,}")
    print(f"   Total rows in market.futures_1m: {db_total:,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

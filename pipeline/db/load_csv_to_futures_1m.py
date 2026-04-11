#!/usr/bin/env python3
"""
pipeline/db/load_csv_to_futures_1m.py
======================================
Load a metl.py daily CSV into market.futures_1m.

The metl.py compiler script writes daily CSVs to CSV_BACKUP_DIR but never
inserts the data into the DB itself. This loader closes that gap. It is:

  - Importable: `load_csv(csv_path)` is called by metl.py at the end of each
    day's processing so the job's "complete" status reflects DB state.
  - Re-runnable: ON CONFLICT DO NOTHING means it's safe to call against the
    same CSV repeatedly. Used for one-off historical backfills.

CSV format (canonical metl.py output):
    timestamp_utc, symbol, price, volume, open_interest, funding_rate,
    long_short_ratio, trade_delta, long_liqs, short_liqs, last_bid_depth,
    last_ask_depth, last_depth_imbalance, last_spread_pct, spread_pct,
    bid_ask_imbalance, basis_pct, market_cap_usd, market_cap_rank

Mapping:
    symbol (TEXT) -> symbol_id INTEGER via market.symbols.binance_id
    price         -> close
    source_id     = 1 (amberdata_binance)
    open/high/low/quote_volume/trades/taker_buy_*  = NULL (not in CSV)

Usage (CLI for one-off backfills):
    python pipeline/db/load_csv_to_futures_1m.py /path/to/oi_20260410.csv
    python pipeline/db/load_csv_to_futures_1m.py /path/to/dir/oi_20260408.csv /path/to/oi_20260409.csv
"""

from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

SOURCE_ID = 1  # amberdata_binance
PAGE_SIZE = 5_000

# CSV columns that map directly to futures_1m columns of the same name
DIRECT_COLS = [
    "volume", "open_interest", "funding_rate", "long_short_ratio",
    "trade_delta", "long_liqs", "short_liqs",
    "last_bid_depth", "last_ask_depth", "last_depth_imbalance",
    "last_spread_pct", "spread_pct", "bid_ask_imbalance", "basis_pct",
    "market_cap_usd",
]

# market_cap_rank is INTEGER in the schema but FLOAT in the CSV — handled separately
INSERT_COLS = ["timestamp_utc", "symbol_id", "source_id", "close"] + DIRECT_COLS + ["market_cap_rank"]

INSERT_SQL = f"""
    INSERT INTO market.futures_1m ({', '.join(INSERT_COLS)})
    VALUES %s
    ON CONFLICT (timestamp_utc, symbol_id, source_id) DO NOTHING
"""


def _build_symbol_map(cur) -> dict:
    cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
    return {row[0]: row[1] for row in cur.fetchall()}


def _derive_base(binance_id: str) -> str:
    """Derive the `base` primary key for market.symbols from a Binance id.

    `*USDT` symbols use the stripped base (BTCUSDT -> BTC) to match the
    existing convention in seed_symbols.py. Other suffixes (USDC,
    USD_PERP, dated futures, etc.) keep the full binance_id as base to
    avoid collisions with the USDT variant (e.g., BTCUSDT and BTCUSDC
    both have the same stripped base "BTC", which would break the
    UNIQUE constraint).
    """
    if binance_id.endswith("USDT"):
        return binance_id[:-4]
    return binance_id


def _auto_upsert_symbol(cur, binance_id: str) -> int | None:
    """Insert a new market.symbols row for an unknown Binance id and
    return its symbol_id. Uses ON CONFLICT DO NOTHING on the UNIQUE base
    column so two concurrent loader runs won't race. If the row already
    exists (from a race or a prior base collision), do a follow-up
    SELECT to recover the existing symbol_id. Returns None only if the
    INSERT was skipped AND no existing row could be matched — which
    shouldn't happen in practice.
    """
    base = _derive_base(binance_id)
    cur.execute(
        """
        INSERT INTO market.symbols (base, binance_id, active)
        VALUES (%s, %s, TRUE)
        ON CONFLICT (base) DO NOTHING
        RETURNING symbol_id
        """,
        (base, binance_id),
    )
    row = cur.fetchone()
    if row is not None:
        return row[0]
    # ON CONFLICT skipped — fetch the existing row.
    cur.execute(
        "SELECT symbol_id FROM market.symbols WHERE binance_id = %s LIMIT 1",
        (binance_id,),
    )
    existing = cur.fetchone()
    return existing[0] if existing else None


def _parse_float(val: str) -> float | None:
    if val == "" or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_int(val: str) -> int | None:
    f = _parse_float(val)
    if f is None:
        return None
    try:
        return int(f)
    except (ValueError, OverflowError):
        return None


def load_csv(csv_path: Path | str, conn=None) -> dict:
    """
    Load a single metl.py CSV into market.futures_1m.

    Returns a dict: {file, rows_read, rows_inserted, rows_skipped, elapsed_sec}.
    Idempotent — re-running on the same file is a no-op (ON CONFLICT DO NOTHING).

    Pass an existing psycopg2 connection to participate in a larger transaction;
    omit it to open and commit a fresh connection.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    cur = conn.cursor()
    symbol_map = _build_symbol_map(cur)

    rows_read = 0
    rows_skipped = 0
    rows_inserted = 0
    symbols_auto_created = 0
    batch: list[tuple] = []
    start = time.time()

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_read += 1
            sym = row.get("symbol", "") or ""
            symbol_id = symbol_map.get(sym)

            # Self-heal: if metl.py fetched a symbol we've never seen
            # before (Binance listed a new instrument), auto-upsert it
            # into market.symbols instead of silently dropping its rows.
            if symbol_id is None and sym:
                symbol_id = _auto_upsert_symbol(cur, sym)
                if symbol_id is not None:
                    symbol_map[sym] = symbol_id
                    symbols_auto_created += 1

            if symbol_id is None:
                rows_skipped += 1
                continue

            ts = row.get("timestamp_utc")
            close_val = _parse_float(row.get("price", ""))
            if not ts or close_val is None:
                rows_skipped += 1
                continue

            record = (
                ts,
                symbol_id,
                SOURCE_ID,
                close_val,
                *[_parse_float(row.get(c, "")) for c in DIRECT_COLS],
                _parse_int(row.get("market_cap_rank", "")),
            )
            batch.append(record)

            if len(batch) >= PAGE_SIZE:
                execute_values(cur, INSERT_SQL, batch, page_size=PAGE_SIZE)
                rows_inserted += len(batch)
                batch.clear()

    if batch:
        execute_values(cur, INSERT_SQL, batch, page_size=PAGE_SIZE)
        rows_inserted += len(batch)
        batch.clear()

    if own_conn:
        conn.commit()
        cur.close()
        conn.close()

    elapsed = time.time() - start
    return {
        "file": str(csv_path),
        "rows_read": rows_read,
        "rows_inserted": rows_inserted,
        "rows_skipped": rows_skipped,
        "symbols_auto_created": symbols_auto_created,
        "elapsed_sec": round(elapsed, 2),
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: load_csv_to_futures_1m.py <csv_path> [<csv_path> ...]")
        sys.exit(1)

    for arg in sys.argv[1:]:
        try:
            result = load_csv(arg)
            auto_note = (
                f"  auto_created={result['symbols_auto_created']}"
                if result.get("symbols_auto_created") else ""
            )
            print(
                f"OK  {result['file']}  "
                f"read={result['rows_read']:,}  "
                f"inserted={result['rows_inserted']:,}  "
                f"skipped={result['rows_skipped']:,}  "
                f"elapsed={result['elapsed_sec']}s"
                f"{auto_note}"
            )
        except Exception as e:
            print(f"ERR {arg}  {type(e).__name__}: {e}")
            sys.exit(2)


if __name__ == "__main__":
    main()

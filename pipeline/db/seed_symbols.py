#!/usr/bin/env python3
"""
pipeline/db/seed_symbols.py
============================
Populates market.symbols from two sources:
  1. Distinct symbols already in market_data_1m (the old table)
  2. The 40 known dispersion symbols from daily_signal.py

Safe to re-run — uses ON CONFLICT DO NOTHING throughout.

Usage:
    python3 pipeline/db/seed_symbols.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

# The 40 dispersion symbols from daily_signal.py (lines 76-87)
DISPERSION_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT",
    "TRXUSDT", "DOGEUSDT", "HYPEUSDT", "BCHUSDT", "ADAUSDT",
    "LINKUSDT", "XLMUSDT", "LTCUSDT", "AVAXUSDT", "HBARUSDT",
    "ZECUSDT", "SUIUSDT", "1000SHIBUSDT", "TONUSDT", "TAOUSDT",
    "DOTUSDT", "MNTUSDT", "UNIUSDT", "NEARUSDT", "AAVEUSDT",
    "1000PEPEUSDT", "ICPUSDT", "ETCUSDT", "ONDOUSDT", "KASUSDT",
    "POLUSDT", "WLDUSDT", "QNTUSDT", "ATOMUSDT", "RENDERUSDT",
    "ENAUSDT", "APTUSDT", "FILUSDT", "STXUSDT", "ARBUSDT",
]


def binance_id_to_base(binance_id: str) -> str:
    """Strip USDT suffix to get base symbol. e.g. BTCUSDT -> BTC"""
    if binance_id.endswith("USDT"):
        return binance_id[:-4]
    return binance_id


def main():
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    # ── Step 1: seed from market_data_1m if it exists ────────────────────────
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'market_data_1m'
        )
    """)
    old_table_exists = cur.fetchone()[0]

    if old_table_exists:
        print("Reading distinct symbols from market_data_1m...")
        cur.execute("SELECT DISTINCT symbol FROM market_data_1m ORDER BY symbol")
        old_symbols = [row[0] for row in cur.fetchall()]
        print(f"  Found {len(old_symbols)} distinct symbols in market_data_1m")

        rows = []
        for sym in old_symbols:
            base = binance_id_to_base(sym)
            binance_id = sym if sym.endswith("USDT") else sym + "USDT"
            blofin_id = base + "-USDT"
            rows.append((base, binance_id, blofin_id))

        from psycopg2.extras import execute_values
        result = execute_values(cur, """
            INSERT INTO market.symbols (base, binance_id, blofin_id)
            VALUES %s
            ON CONFLICT (base) DO NOTHING
            RETURNING symbol_id
        """, rows, fetch=True)
        inserted += len(result)
        skipped += len(rows) - len(result)
        conn.commit()
        print(f"  From market_data_1m: {len(result)} inserted, {len(rows) - len(result)} already existed")
    else:
        print("market_data_1m not found — skipping old table seed")

    # ── Step 2: seed dispersion symbols ──────────────────────────────────────
    print(f"\nSeeding {len(DISPERSION_SYMBOLS)} dispersion symbols...")
    from psycopg2.extras import execute_values

    rows = []
    for sym in DISPERSION_SYMBOLS:
        base = binance_id_to_base(sym)
        blofin_id = base + "-USDT"
        rows.append((base, sym, blofin_id))

    result = execute_values(cur, """
        INSERT INTO market.symbols (base, binance_id, blofin_id)
        VALUES %s
        ON CONFLICT (base) DO NOTHING
        RETURNING symbol_id
    """, rows, fetch=True)
    inserted += len(result)
    skipped += len(rows) - len(result)
    conn.commit()
    print(f"  From dispersion list: {len(result)} inserted, {len(rows) - len(result)} already existed")

    # ── Summary ───────────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM market.symbols")
    total = cur.fetchone()[0]
    print(f"\n✅ Done — {inserted} new symbols inserted, {skipped} already existed")
    print(f"   Total symbols in market.symbols: {total}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

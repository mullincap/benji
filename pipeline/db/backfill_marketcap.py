#!/usr/bin/env python3
"""
pipeline/db/backfill_marketcap.py
===================================
Backfills market.marketcap_daily from the CoinGecko parquet file.

CoinGecko parquet schema (from coingecko_marketcap.py):
  coin_id, date, price_usd, market_cap_usd, volume_usd, rank_num

The parquet uses CoinGecko coin_id (e.g. 'bitcoin') not Binance symbol.
We join via market.symbols.coingecko_id where populated, and fall back
to a best-effort name match for common symbols.

Safe to re-run — uses ON CONFLICT DO NOTHING.

Usage:
    python3 pipeline/db/backfill_marketcap.py
"""

import sys
import time
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

COINGECKO_DIR = Path("/mnt/quant-data/raw/coingecko")
PARQUET_FILE  = COINGECKO_DIR / "marketcap_daily.parquet"
BATCH_SIZE    = 10_000

# Best-effort CoinGecko coin_id → Binance symbol mapping for common coins
# This covers the case where coingecko_id is not yet populated in market.symbols
COINGECKO_TO_BINANCE = {
    "bitcoin":        "BTCUSDT",
    "ethereum":       "ETHUSDT",
    "binancecoin":    "BNBUSDT",
    "ripple":         "XRPUSDT",
    "solana":         "SOLUSDT",
    "tron":           "TRXUSDT",
    "dogecoin":       "DOGEUSDT",
    "bitcoin-cash":   "BCHUSDT",
    "cardano":        "ADAUSDT",
    "chainlink":      "LINKUSDT",
    "stellar":        "XLMUSDT",
    "litecoin":       "LTCUSDT",
    "avalanche-2":    "AVAXUSDT",
    "hedera-hashgraph": "HBARUSDT",
    "zcash":          "ZECUSDT",
    "sui":            "SUIUSDT",
    "shiba-inu":      "1000SHIBUSDT",
    "the-open-network": "TONUSDT",
    "bittensor":      "TAOUSDT",
    "polkadot":       "DOTUSDT",
    "uniswap":        "UNIUSDT",
    "near":           "NEARUSDT",
    "aave":           "AAVEUSDT",
    "pepe":           "1000PEPEUSDT",
    "internet-computer": "ICPUSDT",
    "ethereum-classic": "ETCUSDT",
    "ondo-finance":   "ONDOUSDT",
    "kaspa":          "KASUSDT",
    "matic-network":  "POLUSDT",
    "worldcoin-wld":  "WLDUSDT",
    "quant-network":  "QNTUSDT",
    "cosmos":         "ATOMUSDT",
    "render-token":   "RENDERUSDT",
    "ethena":         "ENAUSDT",
    "aptos":          "APTUSDT",
    "filecoin":       "FILUSDT",
    "stacks":         "STXUSDT",
    "arbitrum":       "ARBUSDT",
    "mantle":         "MNTUSDT",
}


def load_symbol_map(cur) -> dict:
    """Build coin_id → symbol_id lookup from market.symbols."""
    # Primary: use coingecko_id column where populated
    cur.execute("SELECT coingecko_id, symbol_id FROM market.symbols WHERE coingecko_id IS NOT NULL")
    mapping = {row[0]: row[1] for row in cur.fetchall()}

    # Secondary: use binance_id with our hardcoded map
    cur.execute("SELECT binance_id, symbol_id FROM market.symbols WHERE binance_id IS NOT NULL")
    binance_map = {row[0]: row[1] for row in cur.fetchall()}

    for coin_id, binance_id in COINGECKO_TO_BINANCE.items():
        if coin_id not in mapping and binance_id in binance_map:
            mapping[coin_id] = binance_map[binance_id]

    return mapping


def main():
    if not PARQUET_FILE.exists():
        # Try finding any parquet in the dir
        candidates = list(COINGECKO_DIR.glob("*.parquet")) if COINGECKO_DIR.exists() else []
        if not candidates:
            print(f"❌ No parquet file found at {PARQUET_FILE}")
            print(f"   Searched: {COINGECKO_DIR}")
            sys.exit(1)
        parquet_path = candidates[0]
        print(f"  Using: {parquet_path}")
    else:
        parquet_path = PARQUET_FILE

    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    print(f"  Loaded {len(df):,} rows, columns: {list(df.columns)}")
    print(f"  Date range: {df['date'].min()} → {df['date'].max()}")
    print(f"  Unique coins: {df['coin_id'].nunique()}")

    conn = get_conn()
    cur  = conn.cursor()

    symbol_map = load_symbol_map(cur)
    print(f"\n  Symbol map: {len(symbol_map)} coin_id → symbol_id entries")

    # Normalise date column
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Map rank column (may be rank_num or market_cap_rank)
    rank_col = "rank_num" if "rank_num" in df.columns else "market_cap_rank"

    inserted = 0
    skipped  = 0
    start    = time.time()

    rows_buffer = []
    total = len(df)

    for i, row in enumerate(df.itertuples(index=False)):
        coin_id = row.coin_id
        symbol_id = symbol_map.get(coin_id)
        if symbol_id is None:
            skipped += 1
            continue

        rank = getattr(row, rank_col, None)
        rows_buffer.append((
            row.date,
            symbol_id,
            coin_id,
            getattr(row, "market_cap_usd", None),
            int(rank) if rank and not pd.isna(rank) else None,
            getattr(row, "price_usd", None),
            getattr(row, "volume_usd", None),
        ))

        if len(rows_buffer) >= BATCH_SIZE:
            result = execute_values(cur, """
                INSERT INTO market.marketcap_daily
                    (date, symbol_id, coin_id, market_cap_usd, market_cap_rank, price_usd, volume_usd)
                VALUES %s
                ON CONFLICT (date, symbol_id) DO NOTHING
                RETURNING date
            """, rows_buffer, fetch=True)
            inserted += len(result)
            conn.commit()
            rows_buffer = []
            elapsed = time.time() - start
            print(f"  {i+1:,}/{total:,} rows processed — {inserted:,} inserted, {skipped:,} skipped ({elapsed:.0f}s)")

    # Final batch
    if rows_buffer:
        result = execute_values(cur, """
            INSERT INTO market.marketcap_daily
                (date, symbol_id, coin_id, market_cap_usd, market_cap_rank, price_usd, volume_usd)
            VALUES %s
            ON CONFLICT (date, symbol_id) DO NOTHING
            RETURNING date
        """, rows_buffer, fetch=True)
        inserted += len(result)
        conn.commit()

    elapsed = time.time() - start
    cur.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM market.marketcap_daily")
    db_min, db_max, db_count = cur.fetchone()

    print(f"\n✅ Done in {elapsed:.1f}s")
    print(f"   Inserted: {inserted:,} rows")
    print(f"   Skipped (no symbol match): {skipped:,} rows")
    print(f"   DB now has: {db_count:,} rows ({db_min} → {db_max})")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

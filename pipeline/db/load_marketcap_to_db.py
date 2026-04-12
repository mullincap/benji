#!/usr/bin/env python3
"""
pipeline/db/load_marketcap_to_db.py
=====================================
Load CoinGecko marketcap data from a parquet file into
market.market_cap_daily — the canonical daily-grained mcap table.

Source parquet schema (from coingecko_marketcap.py):
    date, coin_id, symbol, name, market_cap_usd, price_usd,
    volume_usd, rank_num, filled, imputed

Target table schema (market.market_cap_daily):
    date, base, coin_id, name, market_cap_usd, price_usd,
    volume_usd, rank_num, filled, imputed
    PRIMARY KEY (date, base)

Mapping rules:
  - parquet `symbol` (lowercase coingecko ticker, e.g. "btc") is uppercased
    to `base` (e.g. "BTC") to match market.symbols.base
  - For (date, base) duplicates in the parquet (different coin_ids sharing
    a ticker — e.g. "ETH" the ethereum vs an ETH-named clone), the row
    with the highest market_cap_usd wins. The smaller-cap clones lose.
  - ON CONFLICT (date, base) DO UPDATE — re-runs are idempotent and
    pick up newer data when CoinGecko revises historical values.

Usage:
    # Load the standard cron-output parquet
    python pipeline/db/load_marketcap_to_db.py

    # Load a specific file
    python pipeline/db/load_marketcap_to_db.py /path/to/marketcap_daily.parquet
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn

DEFAULT_PARQUET = Path("/mnt/quant-data/raw/coingecko/marketcap_daily.parquet")
DEFAULT_COINS_UNIVERSE = Path("/mnt/quant-data/raw/coingecko/coins_universe.parquet")
PAGE_SIZE = 5_000

INSERT_COLS = [
    "date", "base", "coin_id", "name",
    "market_cap_usd", "price_usd", "volume_usd", "rank_num",
    "filled", "imputed",
]

INSERT_SQL = f"""
    INSERT INTO market.market_cap_daily ({', '.join(INSERT_COLS)})
    VALUES %s
    ON CONFLICT (date, base) DO NOTHING
"""


def _safe_int(val) -> int | None:
    if pd.isna(val):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    if pd.isna(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_bool(val) -> bool | None:
    if pd.isna(val):
        return None
    return bool(val)


def _safe_str(val) -> str | None:
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _build_coin_id_to_symbol_map(coins_universe_path: Path) -> dict:
    """Read coins_universe.parquet and return {coin_id: (symbol, name)}.
    Used as a fallback when marketcap_daily.parquet rows have NaN symbols
    (a regression in coingecko_marketcap.py --mode daily that left those
    fields unpopulated for months — the fix is now landed but historical
    rows in the parquet still need this fallback to be loadable)."""
    if not coins_universe_path.exists():
        return {}
    u = pd.read_parquet(coins_universe_path)
    out = {}
    for r in u.itertuples(index=False):
        cid = getattr(r, "id", None)
        if cid:
            out[cid] = (
                _safe_str(getattr(r, "symbol", None)),
                _safe_str(getattr(r, "name", None)),
            )
    return out


def load_parquet_to_db(
    parquet_path: Path,
    conn=None,
    coins_universe_path: Path | None = None,
) -> dict:
    """
    Load the marketcap parquet into market.market_cap_daily.
    Returns a dict: {file, rows_read, rows_loaded, unique_dates,
                     unique_bases, dropped_dupes, fallback_filled,
                     elapsed_sec}.

    coins_universe_path is used to fill in missing symbol/name when
    those fields are NaN in the marketcap parquet (broken daily-mode
    rows). Defaults to the standard production path.
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet not found: {parquet_path}")

    if coins_universe_path is None:
        coins_universe_path = DEFAULT_COINS_UNIVERSE

    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    start = time.time()
    df = pd.read_parquet(parquet_path)
    coin_id_map = _build_coin_id_to_symbol_map(coins_universe_path)

    # Normalize date
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.date

    # Fill NaN symbol/name from coins_universe lookup. This patches the
    # daily-mode regression that wrote rows with NaN symbol for months.
    # Track how many we filled so the report tells the truth.
    fallback_filled = 0
    if coin_id_map:
        def _fill_symbol(row):
            sym = row.get("symbol")
            if pd.isna(sym) or sym is None or str(sym).strip() == "":
                lookup = coin_id_map.get(row.get("coin_id"))
                if lookup and lookup[0]:
                    return lookup[0]
            return sym
        def _fill_name(row):
            nm = row.get("name")
            if pd.isna(nm) or nm is None or str(nm).strip() == "":
                lookup = coin_id_map.get(row.get("coin_id"))
                if lookup and lookup[1]:
                    return lookup[1]
            return nm
        # Vectorize the symbol fill so we can count what was patched
        before_null = df["symbol"].isna().sum()
        df["symbol"] = df.apply(_fill_symbol, axis=1)
        df["name"]   = df.apply(_fill_name, axis=1)
        after_null = df["symbol"].isna().sum()
        fallback_filled = int(before_null - after_null)

    # Drop rows that STILL have no symbol after the fallback — they're
    # delisted coins not in coins_universe. Can't be keyed in the table.
    df = df[df["symbol"].notna() & (df["symbol"].astype(str).str.strip() != "")]

    df["base"] = df["symbol"].astype(str).str.upper().str.strip()

    # Backfill rank_num where missing. The broken daily-mode run wrote rows
    # without market_cap_rank for months — compute it from market_cap_usd
    # within each date so every row lands with a rank.
    if "rank_num" not in df.columns:
        df["rank_num"] = pd.NA
    needs_rank = df["rank_num"].isna()
    if needs_rank.any():
        computed = (
            df[needs_rank]
            .groupby("date")["market_cap_usd"]
            .rank(method="first", ascending=False)
        )
        df.loc[needs_rank, "rank_num"] = computed

    rows_read = len(df)

    # Dedupe by (date, base) keeping the row with the largest market_cap_usd.
    # This collapses CoinGecko ticker collisions (e.g., two coins both named
    # "ETH" — the smaller one loses).
    df_sorted = df.sort_values("market_cap_usd", ascending=False, na_position="last")
    df_dedup = df_sorted.drop_duplicates(subset=["date", "base"], keep="first")
    dropped_dupes = rows_read - len(df_dedup)

    rows = []
    for r in df_dedup.itertuples(index=False):
        rows.append((
            r.date,
            r.base,
            _safe_str(getattr(r, "coin_id", None)),
            _safe_str(getattr(r, "name", None)),
            _safe_float(getattr(r, "market_cap_usd", None)),
            _safe_float(getattr(r, "price_usd", None)),
            _safe_float(getattr(r, "volume_usd", None)),
            _safe_int(getattr(r, "rank_num", None)),
            _safe_bool(getattr(r, "filled", None)),
            _safe_str(getattr(r, "imputed", None)),
        ))

    cur = conn.cursor()
    rows_loaded = 0
    for i in range(0, len(rows), PAGE_SIZE):
        batch = rows[i : i + PAGE_SIZE]
        execute_values(cur, INSERT_SQL, batch, page_size=PAGE_SIZE)
        rows_loaded += len(batch)

    if own_conn:
        conn.commit()
        cur.close()
        conn.close()

    elapsed = time.time() - start
    return {
        "file":            str(parquet_path),
        "rows_read":       int(rows_read),
        "rows_loaded":     int(rows_loaded),
        "dropped_dupes":   int(dropped_dupes),
        "fallback_filled": int(fallback_filled),
        "unique_dates":    int(df_dedup["date"].nunique()),
        "unique_bases":    int(df_dedup["base"].nunique()),
        "elapsed_sec":     round(elapsed, 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Load CoinGecko parquet into market.market_cap_daily")
    parser.add_argument(
        "parquet",
        nargs="?",
        type=Path,
        default=DEFAULT_PARQUET,
        help=f"Parquet path (default {DEFAULT_PARQUET})",
    )
    args = parser.parse_args()

    try:
        result = load_parquet_to_db(args.parquet)
        print(
            f"OK  {result['file']}\n"
            f"    rows_read={result['rows_read']:,}  "
            f"rows_loaded={result['rows_loaded']:,}  "
            f"dropped_dupes={result['dropped_dupes']:,}  "
            f"fallback_filled={result['fallback_filled']:,}\n"
            f"    unique_dates={result['unique_dates']:,}  "
            f"unique_bases={result['unique_bases']:,}\n"
            f"    elapsed={result['elapsed_sec']}s"
        )
    except Exception as e:
        print(f"ERR {args.parquet}  {type(e).__name__}: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()

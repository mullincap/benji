#!/usr/bin/env python3
"""
pipeline/db/backfill_mcap_gap.py
================================
Targeted historical gap-fill for market.market_cap_daily. Used to plug
date ranges where the CoinGecko daily cron was broken or not running.

Unlike coingecko_marketcap.py --mode historical, this script:
  - Does NOT rewrite the master parquet
  - Does NOT recompute ranks for the entire history
  - Does NOT filter to top-1000 per day globally
  - Only fetches coins whose (uppercased) symbol matches a base in
    market.symbols — we don't care about bases we don't trade
  - Writes a small intermediate parquet in /tmp and loads only that

For each selected coin, calls /coins/{id}/market_chart with days=N
where N spans the gap start. Demo-tier returns hourly points for <90
day lookbacks, so we keep the earliest hourly point per date (same
convention as run_historical's drop_duplicates keep='first'). Rank is
computed within the gap batch only.

Usage:
    # Fill a specific date range (inclusive)
    python backfill_mcap_gap.py --start 2026-04-03 --end 2026-04-08

    # Dry run (fetch + build parquet, but don't load to DB)
    python backfill_mcap_gap.py --start 2026-04-03 --end 2026-04-08 --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.compiler.coingecko_marketcap import (
    MARKET_CHART_EP,
    RATE_LIMIT_SLEEP,
    api_get,
)
from pipeline.db.connection import get_conn
from pipeline.db.load_marketcap_to_db import load_parquet_to_db

DEFAULT_COINS_UNIVERSE = Path("/mnt/quant-data/raw/coingecko/coins_universe.parquet")


def _load_symbol_bases(conn) -> set[str]:
    """Return the set of bases (uppercase) from market.symbols."""
    cur = conn.cursor()
    cur.execute("SELECT base FROM market.symbols WHERE base IS NOT NULL")
    rows = cur.fetchall()
    cur.close()
    return {r[0].upper() for r in rows if r[0]}


def _pick_coin_ids(universe: pd.DataFrame, bases: set[str]) -> list[tuple[str, str, str]]:
    """For each base we care about, pick the coin_id from coins_universe.

    coins_universe is already sorted by market_cap_desc at fetch time, so
    when the same symbol appears multiple times (e.g. 'ETH' the real one
    + an ETH-named clone), the FIRST occurrence is the canonical one.

    Returns a list of (coin_id, symbol, name) tuples, one per matched base.
    """
    # Uppercase symbol for matching
    u = universe.copy()
    u["symbol_upper"] = u["symbol"].astype(str).str.upper().str.strip()
    # Drop duplicates keeping first (highest mcap at fetch time)
    u = u.drop_duplicates(subset=["symbol_upper"], keep="first")
    matched = u[u["symbol_upper"].isin(bases)]
    out = []
    for r in matched.itertuples(index=False):
        out.append((r.id, r.symbol_upper, getattr(r, "name", None)))
    return out


def _fetch_coin_range(
    coin_id: str,
    start_date: datetime,
    end_date: datetime,
    api_key: str,
) -> pd.DataFrame | None:
    """Fetch market_chart for one coin covering the gap range.

    Demo API auto-picks granularity by `days`: <90 returns hourly. We
    dedup to one row per date (earliest hourly point, same as the main
    historical script) and filter to [start_date, end_date] inclusive.
    """
    now = datetime.now(UTC)
    days_back = (now.date() - start_date.date()).days + 1
    if days_back < 1:
        return None

    data = api_get(
        MARKET_CHART_EP.format(id=coin_id),
        {"vs_currency": "usd", "days": days_back, "interval": "daily"},
        api_key,
    )
    if not data or not data.get("prices"):
        return None

    df = pd.DataFrame({
        "timestamp_ms":   [p[0] for p in data["prices"]],
        "price_usd":      [p[1] for p in data["prices"]],
        "market_cap_usd": [m[1] for m in data.get("market_caps", [])],
        "volume_usd":     [v[1] for v in data.get("total_volumes", [])],
    })
    df["date"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True).dt.normalize()
    df = df.sort_values("timestamp_ms").drop_duplicates(subset=["date"], keep="first")

    # Filter to the gap window (inclusive)
    start_ts = pd.Timestamp(start_date.date(), tz="UTC")
    end_ts = pd.Timestamp(end_date.date(), tz="UTC")
    df = df[(df["date"] >= start_ts) & (df["date"] <= end_ts)]
    if df.empty:
        return None

    df["coin_id"] = coin_id
    return df[["coin_id", "date", "price_usd", "market_cap_usd", "volume_usd"]]


def backfill(
    start_date: datetime,
    end_date: datetime,
    api_key: str,
    coins_universe_path: Path,
    dry_run: bool = False,
) -> dict:
    """Run the gap fill. Returns a summary dict."""
    if not coins_universe_path.exists():
        raise FileNotFoundError(f"coins_universe not found: {coins_universe_path}")

    universe = pd.read_parquet(coins_universe_path)

    conn = get_conn()
    try:
        bases = _load_symbol_bases(conn)
        print(f"market.symbols: {len(bases)} active bases")

        coins_to_fetch = _pick_coin_ids(universe, bases)
        print(f"coins_universe matched: {len(coins_to_fetch)} coin_ids")

        all_rows: list[pd.DataFrame] = []
        empty = 0
        with tqdm(total=len(coins_to_fetch), desc="Fetching history", unit="coin") as pbar:
            for coin_id, symbol_upper, name in coins_to_fetch:
                df = _fetch_coin_range(coin_id, start_date, end_date, api_key)
                if df is None or df.empty:
                    empty += 1
                else:
                    df = df.copy()
                    df["symbol"] = symbol_upper.lower()  # match main parquet convention
                    df["name"] = name
                    df["rank_num"] = None  # recomputed by loader
                    df["filled"] = False
                    df["imputed"] = None
                    all_rows.append(df)
                pbar.set_postfix(ok=len(all_rows), empty=empty)
                pbar.update(1)

        if not all_rows:
            print("No data fetched. Nothing to load.")
            return {"coins_fetched": 0, "rows_written": 0}

        batch = pd.concat(all_rows, ignore_index=True)
        print(f"Batch rows: {len(batch):,}  (unique dates: {batch['date'].nunique()}, unique coins: {batch['coin_id'].nunique()})")

        # Write to a temp parquet matching the main parquet's schema so the
        # existing loader can pick it up unmodified.
        tmp = tempfile.NamedTemporaryFile(
            prefix="mcap_gap_", suffix=".parquet", delete=False, dir="/tmp"
        )
        tmp.close()
        tmp_path = Path(tmp.name)
        batch.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
        print(f"Wrote temp parquet: {tmp_path}  size={tmp_path.stat().st_size / 1024:.1f} KB")

        if dry_run:
            print("Dry run — skipping DB load.")
            return {
                "coins_fetched": len(all_rows),
                "rows_written": int(len(batch)),
                "temp_parquet": str(tmp_path),
                "dry_run": True,
            }

        result = load_parquet_to_db(tmp_path, conn=conn, coins_universe_path=coins_universe_path)
        conn.commit()
        os.unlink(tmp_path)
        print(
            f"DB load — rows_loaded={result['rows_loaded']:,} "
            f"dropped_dupes={result['dropped_dupes']:,} "
            f"fallback_filled={result['fallback_filled']:,} "
            f"unique_dates={result['unique_dates']:,} "
            f"unique_bases={result['unique_bases']:,}"
        )
        return {
            "coins_fetched": len(all_rows),
            "rows_written": int(len(batch)),
            **result,
        }
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Targeted gap fill for market.market_cap_daily")
    parser.add_argument("--start", type=str, required=True, help="Gap start date (YYYY-MM-DD, inclusive)")
    parser.add_argument("--end", type=str, required=True, help="Gap end date (YYYY-MM-DD, inclusive)")
    parser.add_argument("--api-key", type=str, default=os.environ.get("COINGECKO_API_KEY"))
    parser.add_argument("--coins-universe", type=Path, default=DEFAULT_COINS_UNIVERSE)
    parser.add_argument("--dry-run", action="store_true", help="Fetch + write temp parquet but don't load to DB")
    args = parser.parse_args()

    if not args.api_key:
        print("ERR: missing --api-key (or COINGECKO_API_KEY env var)")
        sys.exit(2)

    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC)
    if end_dt < start_dt:
        print("ERR: --end must be >= --start")
        sys.exit(2)

    num_days = (end_dt.date() - start_dt.date()).days + 1
    print(f"Backfilling {num_days} day(s): {start_dt.date()} → {end_dt.date()}")
    print(f"Estimated API time (at {RATE_LIMIT_SLEEP}s/coin): ~{(RATE_LIMIT_SLEEP * 600) / 60:.0f} min for ~600 coins")

    result = backfill(
        start_date=start_dt,
        end_date=end_dt,
        api_key=args.api_key,
        coins_universe_path=args.coins_universe,
        dry_run=args.dry_run,
    )
    print("Done.")
    print(result)


if __name__ == "__main__":
    main()

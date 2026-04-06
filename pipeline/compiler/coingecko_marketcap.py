"""
CoinGecko Market Cap Downloader (historically accurate)
=========================================================
Downloads daily historical market cap, price, and volume data.

FIX 1 — API key sent as header (not query param), using pro-api.coingecko.com
FIX 2 — Historically accurate: fetches a wide universe (2000 coins) and
         recomputes daily rank from actual per-date market cap values.
         Coins that were top-1000 in early 2025 but have since fallen out
         are still captured correctly.

Usage:
    python coingecko_marketcap.py --api-key YOUR_KEY --mode historical --start 2025-01-01
    python coingecko_marketcap.py --api-key YOUR_KEY --mode daily

Requirements:
    pip install requests pandas pyarrow tqdm
"""

import time
import logging
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, UTC
from tqdm import tqdm

BASE_URL         = "https://api.coingecko.com/api/v3"
COINS_MARKETS_EP = "/coins/markets"
MARKET_CHART_EP  = "/coins/{id}/market_chart"

RATE_LIMIT_SLEEP = 2.1
RETRY_SLEEP      = 60
MAX_RETRIES      = 3
COINS_PER_PAGE   = 250

# Fetch a wider universe than 1000 so that coins which have fallen out
# of the current top 1000 since Jan 2025 are still included.
# Daily rank is recomputed from actual per-date market caps at the end.
UNIVERSE_SIZE    = 2000

DEFAULT_START    = "2025-01-01"


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            tqdm.write(self.format(record))
        except Exception:
            self.handleError(record)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("coingecko_marketcap.log"), TqdmLoggingHandler()],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# API client — key sent as HEADER, not query param
# ─────────────────────────────────────────────

def api_get(endpoint: str, params: dict, api_key: str) -> dict | list | None:
    url     = BASE_URL + endpoint
    headers = {"Accept": "application/json"}
    params  = {**params, "x_cg_demo_api_key": api_key}  # key as query param
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            resp = requests.get(url, params=params, headers=headers, timeout=20)

            if resp.status_code == 429:
                wait = RETRY_SLEEP * (attempt + 1)
                log.warning(f"Rate limit (429). Sleeping {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                raise RuntimeError(
                    "401 Unauthorized — check your API key is correct and active. "
                    "Test with: curl -H 'x-cg-demo-api-key: YOUR_KEY' "
                    "https://pro-api.coingecko.com/api/v3/ping"
                )

            if resp.status_code == 404:
                return None

            resp.raise_for_status()
            return resp.json()

        except RuntimeError:
            raise
        except requests.exceptions.RequestException as e:
            wait = 10 * (attempt + 1)
            log.warning(f"Request error (attempt {attempt+1}): {e}. Retry in {wait}s...")
            time.sleep(wait)

    log.error(f"All retries failed for {endpoint}")
    return None


# ─────────────────────────────────────────────
# Fetch coin universe (wide — 2000 coins)
# ─────────────────────────────────────────────

def fetch_coin_universe(api_key: str) -> pd.DataFrame:
    pages     = (UNIVERSE_SIZE + COINS_PER_PAGE - 1) // COINS_PER_PAGE
    all_coins = []
    log.info(f"Fetching top {UNIVERSE_SIZE} coin universe ({pages} pages)...")

    for page in range(1, pages + 1):
        log.info(f"  Page {page}/{pages}...")
        data = api_get(COINS_MARKETS_EP, {
            "vs_currency": "usd",
            "order":       "market_cap_desc",
            "per_page":    COINS_PER_PAGE,
            "page":        page,
            "sparkline":   "false",
        }, api_key)
        if not data:
            log.warning(f"  No data on page {page}.")
            continue
        all_coins.extend(data)

    df = pd.DataFrame(all_coins)[[
        "id", "symbol", "name", "market_cap_rank", "current_price",
        "market_cap", "total_volume", "circulating_supply",
        "total_supply", "last_updated",
    ]]
    df["fetched_at"] = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Sanity check
    btc_rows = df[df["symbol"].str.upper() == "BTC"]
    if not btc_rows.empty:
        btc_cap = btc_rows.iloc[0]["market_cap"]
        if btc_cap and btc_cap < 500_000_000_000:
            raise RuntimeError(f"BTC cap (${btc_cap:,.0f}) looks wrong — aborting.")
        log.info(f"  BTC sanity check passed: ${btc_cap:,.0f}")

    log.info(f"Universe fetched: {len(df)} coins.")
    return df


# ─────────────────────────────────────────────
# Fetch history for one coin
# ─────────────────────────────────────────────

def fetch_coin_history(coin_id: str, start_date: str, api_key: str) -> pd.DataFrame | None:
    start_dt  = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
    days_back = (datetime.now(UTC) - start_dt).days + 1

    data = api_get(MARKET_CHART_EP.format(id=coin_id), {
        "vs_currency": "usd",
        "days":        days_back,
        "interval":    "daily",
    }, api_key)

    if not data or not data.get("prices"):
        return None

    df = pd.DataFrame({
        "timestamp_ms":   [p[0] for p in data["prices"]],
        "price_usd":      [p[1] for p in data["prices"]],
        "market_cap_usd": [m[1] for m in data["market_caps"]],
        "volume_usd":     [v[1] for v in data["total_volumes"]],
    })
    df["date"]    = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True).dt.normalize()
    df["coin_id"] = coin_id
    df = df[["coin_id", "date", "price_usd", "market_cap_usd", "volume_usd"]]
    df = df[df["date"] >= pd.Timestamp(start_date, tz="UTC")]
    df.sort_values("date", inplace=True)
    df.drop_duplicates(subset=["coin_id", "date"], inplace=True)
    return df


# ─────────────────────────────────────────────
# Daily snapshot (for --mode daily)
# ─────────────────────────────────────────────

def fetch_daily_snapshot(coin_ids: list, api_key: str) -> pd.DataFrame:
    today   = pd.Timestamp(datetime.now(UTC)).normalize()
    rows    = []
    batches = [coin_ids[i:i+50] for i in range(0, len(coin_ids), 50)]
    log.info(f"Daily snapshot: {len(coin_ids)} coins, {len(batches)} batches...")

    for batch in tqdm(batches, desc="Daily snapshot", unit="batch"):
        data = api_get(COINS_MARKETS_EP, {
            "vs_currency": "usd",
            "ids":         ",".join(batch),
            "order":       "market_cap_desc",
            "per_page":    len(batch),
            "page":        1,
            "sparkline":   "false",
        }, api_key)
        if not data:
            continue
        for coin in data:
            rows.append({
                "coin_id":       coin["id"],
                "date":          today,
                "price_usd":     coin.get("current_price"),
                "market_cap_usd": coin.get("market_cap"),
                "volume_usd":    coin.get("total_volume"),
            })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# Parquet helpers
# ─────────────────────────────────────────────

def load_parquet(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as e:
        log.warning(f"Could not read {path.name}: {e}")
        return None


def save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)


def merge_and_save(new_df: pd.DataFrame, path: Path) -> pd.DataFrame:
    existing = load_parquet(path)
    combined = pd.concat([existing, new_df], ignore_index=True) if existing is not None else new_df.copy()
    combined["date"] = pd.to_datetime(combined["date"], utc=True)
    combined.drop_duplicates(subset=["coin_id", "date"], keep="last", inplace=True)
    combined.sort_values(["coin_id", "date"], inplace=True)
    save_parquet(combined, path)
    return combined


# ─────────────────────────────────────────────
# Checkpoint helpers
# ─────────────────────────────────────────────

def load_checkpoint(path: Path) -> set:
    return set(path.read_text().splitlines()) if path.exists() else set()

def save_checkpoint(path: Path, coin_id: str):
    with open(path, "a") as f:
        f.write(coin_id + "\n")


# ─────────────────────────────────────────────
# Historical mode
# ─────────────────────────────────────────────

def run_historical(api_key: str, start_date: str, output_dir: Path):
    marketcap_path  = output_dir / "marketcap_daily.parquet"
    universe_path   = output_dir / "coins_universe.parquet"
    checkpoint_path = output_dir / "checkpoint_historical.txt"

    universe_df = fetch_coin_universe(api_key)
    save_parquet(universe_df, universe_path)
    coin_ids = universe_df["id"].tolist()

    completed = load_checkpoint(checkpoint_path)
    remaining = [c for c in coin_ids if c not in completed]
    log.info(f"Coins remaining: {len(remaining)} ({len(completed)} already done)")
    log.info(f"Est. time: ~{len(remaining) * RATE_LIMIT_SLEEP / 60:.0f} minutes")

    stats    = {"ok": 0, "empty": 0}
    new_rows = []

    with tqdm(total=len(remaining), desc="Historical download", unit="coin") as pbar:
        for coin_id in remaining:
            df = fetch_coin_history(coin_id, start_date, api_key)
            if df is None or df.empty:
                stats["empty"] += 1
            else:
                new_rows.append(df)
                stats["ok"] += 1

            save_checkpoint(checkpoint_path, coin_id)

            if len(new_rows) >= 50:
                merge_and_save(pd.concat(new_rows, ignore_index=True), marketcap_path)
                new_rows = []
                log.info("  Flushed 50 coins to disk.")

            pbar.set_postfix(ok=stats["ok"], empty=stats["empty"])
            pbar.update(1)

    if new_rows:
        merge_and_save(pd.concat(new_rows, ignore_index=True), marketcap_path)

    # ── Recompute historically accurate daily ranks ──────────────────
    # This is the key step: rank each coin against all others using only
    # that date's actual market cap — not today's standing.
    log.info("Recomputing historically accurate daily ranks...")
    df = load_parquet(marketcap_path)
    df["rank_num"] = (
        df.groupby("date")["market_cap_usd"]
          .rank(method="first", ascending=False)
          .astype(int)
    )

    log.info("Filtering to historically accurate top-1000 per day...")
    df = df[df["rank_num"] <= 1000].sort_values(["date", "rank_num"])
    save_parquet(df, marketcap_path)

    checkpoint_path.unlink(missing_ok=True)

    log.info("=" * 60)
    log.info(f"Done — {stats['ok']} coins saved, {stats['empty']} empty")
    log.info(f"Rows : {len(df):,}")
    log.info(f"Dates: {df['date'].nunique():,}")
    log.info(f"Output: {marketcap_path.resolve()}")
    log.info("=" * 60)


# ─────────────────────────────────────────────
# Daily mode
# ─────────────────────────────────────────────

def run_daily(api_key: str, output_dir: Path):
    universe_path  = output_dir / "coins_universe.parquet"
    marketcap_path = output_dir / "marketcap_daily.parquet"

    universe_df = fetch_coin_universe(api_key)
    save_parquet(universe_df, universe_path)
    snapshot_df = fetch_daily_snapshot(universe_df["id"].tolist(), api_key)

    if snapshot_df.empty:
        log.warning("No data returned.")
        return

    final = merge_and_save(snapshot_df, marketcap_path)
    log.info("=" * 60)
    log.info(f"Daily update done — {len(snapshot_df)} rows added, {len(final)} total")
    log.info(f"Output: {marketcap_path.resolve()}")
    log.info("=" * 60)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key",    type=str, required=True,
                        help="CoinGecko Demo API key")
    parser.add_argument("--mode",       choices=["historical", "daily"], default="historical")
    parser.add_argument("--start",      type=str, default=DEFAULT_START)
    parser.add_argument("--output-dir", type=str, default="data/marketcap")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    if args.mode == "historical":
        log.info(f"Mode: HISTORICAL | Start: {args.start} | Output: {output_dir}")
        run_historical(args.api_key, args.start, output_dir)
    else:
        log.info(f"Mode: DAILY | Output: {output_dir}")
        run_daily(args.api_key, output_dir)

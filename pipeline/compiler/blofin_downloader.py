"""
BloFin Historical OHLCV Downloader
=====================================
Downloads candlestick (OHLCV) data from the BloFin REST API for all
USDT-margined perpetual futures instruments.

Key differences vs Binance downloader:
  - No bulk data portal — must paginate through REST API
  - Max 300 bars per request (vs 1000 on Binance)
  - Rate limits enforced — exceeding them causes 5-minute bans
  - Instrument IDs use hyphens: BTC-USDT (not BTCUSDT)
  - Data returned newest-first, so we paginate backwards in time
  - BloFin launched ~2023 so history is shallower than Binance

Usage:
    python blofin_downloader.py --symbols 200 --years 2 --interval 5m --threads 3
    python blofin_downloader.py --symbol-list BTC-USDT,ETH-USDT --years 1 --interval 1H

Supported intervals:
    1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W

Requirements:
    pip install requests pandas pyarrow tqdm
"""

import io
import time
import logging
import argparse
import requests
import pandas as pd
from uuid import uuid4
from pathlib import Path
from datetime import datetime, UTC, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

from config import RAW_BLOFIN_DIR, LOG_BLOFIN, ensure_dirs
ensure_dirs()

BASE_URL       = "https://openapi.blofin.com"
INSTRUMENTS_EP = "/api/v1/market/instruments"
CANDLES_EP     = "/api/v1/market/candles"

MAX_BARS_PER_REQUEST = 300   # BloFin hard limit
RATE_LIMIT_SLEEP     = 0.25  # seconds between requests per thread (conservative)
# With 3 threads × 4 req/s = 12 req/s — well within BloFin limits

INTERVAL_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1H": 60, "2H": 120, "4H": 240, "6H": 360, "12H": 720,
    "1D": 1440, "1W": 10080,
}

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close",
    "volume", "quote_volume", "taker_buy_quote_volume", "confirmed",
]

NUMERIC_COLS = ["open", "high", "low", "close", "volume",
                "quote_volume", "taker_buy_quote_volume"]


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            tqdm.write(self.format(record))
            self.flush()
        except Exception:
            self.handleError(record)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_BLOFIN),
        TqdmLoggingHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Instrument Fetching
# ─────────────────────────────────────────────

def fetch_instruments(top_n: int) -> list[str]:
    """Fetch all active USDT-margined perpetual swap instruments."""
    log.info("Fetching instrument list from BloFin...")
    resp = requests.get(
        BASE_URL + INSTRUMENTS_EP,
        params={"instType": "SWAP"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != "0":
        raise RuntimeError(f"BloFin API error: {data.get('msg')}")

    instruments = []
    for inst in data["data"]:
        inst_id = inst.get("instId", "")
        state   = inst.get("state", "")
        if state != "live":
            continue
        if not inst_id.endswith("-USDT"):
            continue
        instruments.append(inst_id)

    log.info(f"Found {len(instruments)} active USDT perpetual instruments.")
    selected = instruments[:top_n]
    log.info(f"Selected top {len(selected)} instruments.")
    return selected


# ─────────────────────────────────────────────
# Pagination Helper
# ─────────────────────────────────────────────

def fetch_candles_page(
    inst_id: str,
    interval: str,
    before_ms: int | None = None,
) -> list[list]:
    """
    Fetch one page of up to 300 candles ending before `before_ms`.
    BloFin returns data newest-first. We paginate backwards using `before`.
    Returns list of raw rows, or empty list on failure.
    """
    params = {
        "instId": inst_id,
        "bar":    interval,
        "limit":  MAX_BARS_PER_REQUEST,
    }
    if before_ms is not None:
        params["before"] = before_ms

    for attempt in range(3):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            resp = requests.get(
                BASE_URL + CANDLES_EP,
                params=params,
                timeout=20,
            )

            if resp.status_code == 403:
                log.warning(f"Rate limit hit (403) for {inst_id}. Sleeping 60s...")
                time.sleep(60)
                continue

            resp.raise_for_status()
            data = resp.json()

            if data.get("code") != "0":
                log.warning(f"API error for {inst_id}: {data.get('msg')}")
                return []

            return data.get("data", [])

        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            log.warning(f"Request failed {inst_id} (attempt {attempt+1}): {e}. Retry in {wait}s")
            time.sleep(wait)

    return []


# ─────────────────────────────────────────────
# Full Symbol Download
# ─────────────────────────────────────────────

def download_symbol(
    inst_id: str,
    interval: str,
    start_ms: int,
    output_dir: Path,
) -> dict:
    """
    Download all available candles for one instrument from start_ms to now.
    Paginates backwards from current time until we pass start_ms.
    Saves to Parquet.
    """
    interval_ms = INTERVAL_MINUTES[interval] * 60 * 1000
    all_rows = []
    before_ms = None  # Start from most recent
    pages_fetched = 0

    while True:
        rows = fetch_candles_page(inst_id, interval, before_ms)

        if not rows:
            break

        # Filter to only confirmed candles (confirmed == "1")
        # and only those within our requested date range
        for row in rows:
            ts_ms = int(row[0])
            if ts_ms < start_ms:
                continue
            all_rows.append(row)

        # Check if oldest candle in this page is before our start
        oldest_ts = int(rows[-1][0])
        if oldest_ts <= start_ms:
            break

        # Set before to oldest timestamp in this page to get next page
        before_ms = oldest_ts
        pages_fetched += 1

        # Safety limit — 10,000 pages = 3M bars max
        if pages_fetched > 10_000:
            log.warning(f"{inst_id}: hit page limit, stopping early")
            break

    if not all_rows:
        return {"inst_id": inst_id, "status": "empty", "rows": 0}

    # Build DataFrame
    df = pd.DataFrame(all_rows, columns=KLINE_COLUMNS)

    # Parse timestamps
    df["open_time"] = pd.to_datetime(
        pd.to_numeric(df["open_time"], errors="coerce"),
        unit="ms", utc=True
    )

    # Cast numerics
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop unconfirmed candles and the confirmed column
    df = df[df["confirmed"] == "1"].drop(columns=["confirmed"])

    df.set_index("open_time", inplace=True)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep="last")]

    # Save — use inst_id without hyphen for filename (BTC-USDT → BTCUSDT)
    filename = inst_id.replace("-", "") + ".parquet"
    parquet_path = output_dir / filename

    # Merge with existing if present
    if parquet_path.exists():
        existing = pd.read_parquet(parquet_path)
        df = pd.concat([existing, df])
        df = df[~df.index.duplicated(keep="last")]
        df.sort_index(inplace=True)

    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")

    return {"inst_id": inst_id, "status": "ok", "rows": len(df)}


# ─────────────────────────────────────────────
# Checkpointing
# ─────────────────────────────────────────────

def load_checkpoint(path: Path) -> set:
    if not path.exists():
        return set()
    return set(line.strip() for line in path.read_text().splitlines() if line.strip())


def save_checkpoint(path: Path, key: str):
    with open(path, "a") as f:
        f.write(key + "\n")


# ─────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────

def run_downloader(
    top_n: int,
    years_back: int,
    interval: str,
    threads: int,
    output_dir: Path,
    symbol_list: list[str] | None = None,
):
    if interval not in INTERVAL_MINUTES:
        raise ValueError(f"Invalid interval '{interval}'. Choose from: {list(INTERVAL_MINUTES)}")

    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = output_dir / f"checkpoint_blofin_{interval}.txt"

    # Symbols
    if symbol_list:
        instruments = [s.strip().upper() for s in symbol_list]
        log.info(f"Using provided symbol list: {len(instruments)} instruments.")
    else:
        instruments = fetch_instruments(top_n)

    # Start timestamp
    start_dt = datetime.now(UTC) - timedelta(days=365 * years_back)
    start_ms  = int(start_dt.timestamp() * 1000)

    log.info(f"Interval        : {interval}")
    log.info(f"Start date      : {start_dt.strftime('%Y-%m-%d')} ({years_back} years back)")
    log.info(f"Threads         : {threads}")
    log.info(f"Output dir      : {output_dir.resolve()}")

    completed = load_checkpoint(checkpoint_file)
    log.info(f"Checkpoint: {len(completed)} instruments already completed.")

    tasks = [i for i in instruments if i not in completed]
    log.info(f"Instruments to download: {len(tasks)} (skipping {len(completed)} done)")

    if not tasks:
        log.info("Nothing to do.")
        return

    stats = {"ok": 0, "empty": 0, "error": 0, "total_rows": 0}

    # NOTE: Keep threads LOW (2-4) for BloFin to avoid rate limit bans
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_map = {
            executor.submit(download_symbol, inst_id, interval, start_ms, output_dir): inst_id
            for inst_id in tasks
        }

        with tqdm(total=len(tasks), desc="Downloading BloFin", unit="symbol", dynamic_ncols=True) as pbar:
            for future in as_completed(future_map):
                inst_id = future_map[future]
                try:
                    result = future.result()
                    status = result["status"]

                    if status == "ok":
                        stats["ok"] += 1
                        stats["total_rows"] += result["rows"]
                        save_checkpoint(checkpoint_file, inst_id)
                    elif status == "empty":
                        stats["empty"] += 1
                        save_checkpoint(checkpoint_file, inst_id)

                except Exception as e:
                    stats["error"] += 1
                    log.debug(f"Failed {inst_id}: {e}")

                pbar.set_postfix(
                    ok=stats["ok"],
                    empty=stats["empty"],
                    errors=stats["error"],
                )
                pbar.update(1)

    log.info("=" * 60)
    log.info("Download complete — BloFin")
    log.info(f"  Symbols downloaded : {stats['ok']}")
    log.info(f"  Symbols empty      : {stats['empty']} (no data in range)")
    log.info(f"  Errors             : {stats['error']}")
    log.info(f"  Total rows stored  : {stats['total_rows']:,}")
    log.info(f"  Output directory   : {output_dir.resolve()}")
    log.info("=" * 60)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download BloFin historical OHLCV data to Parquet files via REST API."
    )
    parser.add_argument(
        "--symbols", type=int, default=100,
        help="Number of top USDT perpetual symbols to download. Default: 100"
    )
    parser.add_argument(
        "--symbol-list", type=str, default=None,
        help="Comma-separated instrument IDs e.g. BTC-USDT,ETH-USDT. Overrides --symbols."
    )
    parser.add_argument(
        "--years", type=int, default=2,
        help="Years of history to fetch. Default: 2 (BloFin history is shallower than Binance)"
    )
    parser.add_argument(
        "--interval", type=str, default="5m",
        choices=list(INTERVAL_MINUTES.keys()),
        help="Candle interval. Default: 5m"
    )
    parser.add_argument(
        "--threads", type=int, default=3,
        help="Parallel threads. Keep at 2-4 to avoid BloFin rate limit bans. Default: 3"
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory for Parquet files. Defaults to config RAW_BLOFIN_DIR."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args   = parse_args()
    output = Path(args.output_dir) if args.output_dir else RAW_BLOFIN_DIR
    syms   = args.symbol_list.split(",") if args.symbol_list else None

    run_downloader(
        top_n=args.symbols,
        years_back=args.years,
        interval=args.interval,
        threads=args.threads,
        output_dir=output,
        symbol_list=syms,
    )

"""
Binance Historical OHLCV Downloader
====================================
Downloads 1-minute kline data from data.binance.vision for Spot or Futures markets.
Features:
  - Parallel downloads (configurable thread count)
  - Checkpointing (resume interrupted downloads)
  - Automatic Parquet storage with per-symbol files
  - Progress tracking with tqdm
  - Graceful 404 handling (symbols with shorter history)

Usage:
    python binance_downloader.py --market spot --symbols 1000 --years 5 --threads 8
    python binance_downloader.py --market futures --symbols 1000 --years 5 --interval 5m --threads 8
    python binance_downloader.py --market spot --symbol-list BTCUSDT,ETHUSDT --years 3 --interval 15m

Requirements:
    pip install requests pandas pyarrow tqdm
"""

import os
import io
import time
import zipfile
import logging
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, UTC
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

from config import RAW_BINANCE_SPOT, RAW_BINANCE_FUT, LOG_BINANCE, ensure_dirs
ensure_dirs()

BASE_URL = "https://data.binance.vision/data"

MARKET_CONFIG = {
    "spot": {
        "info_url": "https://api.binance.com/api/v3/exchangeInfo",
        "kline_path": "spot/monthly/klines",
        "quote_assets": ["USDT", "BUSD", "USDC"],  # Filter to USD-denominated pairs
    },
    "futures": {
        "info_url": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "kline_path": "futures/um/monthly/klines",
        "quote_assets": ["USDT", "USDC"],
    },
}

KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
]

NUMERIC_COLS = ["open", "high", "low", "close", "volume",
                "quote_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

class TqdmLoggingHandler(logging.Handler):
    """Logging handler that writes through tqdm so log lines don't break the progress bar."""
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_BINANCE),
        TqdmLoggingHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Symbol Fetching
# ─────────────────────────────────────────────

def fetch_top_symbols(market: str, top_n: int) -> list[str]:
    """
    Fetch actively trading symbols from Binance exchange info,
    filtered to USD-denominated pairs, sorted by quoteVolume (approximate rank).
    Falls back to alphabetical order if volume unavailable.
    """
    config = MARKET_CONFIG[market]
    log.info(f"Fetching symbol list from Binance ({market})...")

    resp = requests.get(config["info_url"], timeout=15)
    resp.raise_for_status()
    data = resp.json()

    quote_assets = config["quote_assets"]
    symbols = []

    for s in data["symbols"]:
        status = s.get("status") or s.get("contractStatus", "")
        if status != "TRADING":
            continue
        quote = s.get("quoteAsset", "")
        if quote not in quote_assets:
            continue
        symbols.append(s["symbol"])

    log.info(f"Found {len(symbols)} active {market} symbols (USD-denominated).")

    # Prioritise USDT pairs, then others
    usdt = [s for s in symbols if s.endswith("USDT")]
    others = [s for s in symbols if not s.endswith("USDT")]
    ranked = usdt + others

    selected = ranked[:top_n]
    log.info(f"Selected top {len(selected)} symbols.")
    return selected


# ─────────────────────────────────────────────
# Date Range Generation
# ─────────────────────────────────────────────

def generate_months(years_back: int = None, days_back: int = None,
                    start_month: str = None, end_month: str = None) -> list[str]:
    """Generate list of YYYY-MM strings covering the requested history window."""
    if start_month and end_month:
        start = datetime.strptime(start_month, "%Y-%m").replace(tzinfo=UTC)
        end   = datetime.strptime(end_month,   "%Y-%m").replace(tzinfo=UTC)
    else:
        end = datetime.now(UTC).replace(day=1) - timedelta(days=1)
        end = end.replace(day=1)
        if days_back is not None:
            start = datetime.now(UTC) - timedelta(days=days_back)
            start = start.replace(day=1)
        else:
            start = end - relativedelta(years=years_back)

    months = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        current += relativedelta(months=1)
    return months


# ─────────────────────────────────────────────
# Checkpointing
# ─────────────────────────────────────────────

def load_checkpoint(checkpoint_file: Path) -> set:
    """Load set of already-completed (symbol, month) keys."""
    if not checkpoint_file.exists():
        return set()
    with open(checkpoint_file) as f:
        return set(line.strip() for line in f if line.strip())


def save_checkpoint(checkpoint_file: Path, key: str):
    """Append a completed key to the checkpoint file."""
    with open(checkpoint_file, "a") as f:
        f.write(key + "\n")


# ─────────────────────────────────────────────
# Download & Parse
# ─────────────────────────────────────────────

def build_url(market: str, symbol: str, month: str, interval: str = "1m") -> str:
    path = MARKET_CONFIG[market]["kline_path"]
    filename = f"{symbol}-{interval}-{month}.zip"
    return f"{BASE_URL}/{path}/{symbol}/{interval}/{filename}"


def download_and_parse(url: str, retries: int = 3, backoff: float = 2.0) -> pd.DataFrame | None:
    """
    Download a zip file from Binance data portal and parse the CSV inside.
    Returns None if file doesn't exist (404) or on persistent failure.
    """
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=60)

            if resp.status_code == 404:
                return None  # File simply doesn't exist for this symbol/month

            resp.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as csv_file:
                    df = pd.read_csv(csv_file, header=None, names=KLINE_COLUMNS)

            # Drop header row if Binance included it (open_time will be the string "open_time")
            if df.iloc[0]["open_time"] == "open_time":
                df = df.iloc[1:].reset_index(drop=True)

            # Parse timestamps — handle both integer ms and string formats
            def parse_timestamps(series):
                # Try integer milliseconds first
                numeric = pd.to_numeric(series, errors="coerce")
                if numeric.notna().all():
                    return pd.to_datetime(numeric, unit="ms", utc=True)
                # Fall back to string parsing
                return pd.to_datetime(series, utc=True, infer_datetime_format=True)

            df["open_time"] = parse_timestamps(df["open_time"])
            df["close_time"] = parse_timestamps(df["close_time"])

            # Cast numerics
            for col in NUMERIC_COLS:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["trades"] = pd.to_numeric(df["trades"], errors="coerce").astype("Int64")

            df.drop(columns=["ignore"], inplace=True)
            df.set_index("open_time", inplace=True)
            df.sort_index(inplace=True)

            return df

        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait = backoff ** attempt
                log.warning(f"Attempt {attempt+1} failed for {url}: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"All retries failed for {url}: {e}")
                return None


# ─────────────────────────────────────────────
# Parquet Storage
# ─────────────────────────────────────────────

def append_to_parquet(parquet_path: Path, df: pd.DataFrame):
    """
    Append new data to an existing Parquet file, or create it if it doesn't exist.
    Deduplicates on open_time index after merging.
    Handles corrupted files from interrupted writes by deleting and recreating them.
    """
    if parquet_path.exists():
        # Guard against corrupted files from previously interrupted writes
        try:
            existing = pd.read_parquet(parquet_path)
            combined = pd.concat([existing, df])
        except Exception as e:
            log.warning(f"Corrupted Parquet file detected ({parquet_path.name}), recreating: {e}")
            parquet_path.unlink()  # Delete the corrupted file
            combined = df

        combined = combined[~combined.index.duplicated(keep="last")]
        combined.sort_index(inplace=True)
    else:
        combined = df

    # Ensure index is tz-aware UTC datetime64[us] before writing.
    # Without explicit casting, PyArrow may store ns-precision timestamps that
    # read back as microseconds, shifting dates by ~56,000 years.
    combined.index = pd.DatetimeIndex(
        combined.index.astype("int64") // 1000,  # ns → us
        dtype="datetime64[us, UTC]",
    ) if combined.index.dtype == "datetime64[ns, UTC]" else combined.index

    # Also fix close_time column if present
    if "close_time" in combined.columns and pd.api.types.is_datetime64_ns_dtype(combined["close_time"]):
        combined["close_time"] = combined["close_time"].astype("datetime64[us, UTC]")

    combined.to_parquet(parquet_path, engine="pyarrow", compression="snappy")


# ─────────────────────────────────────────────
# Worker Function
# ─────────────────────────────────────────────

def process_task(task: dict) -> dict:
    """
    Download and store one (symbol, month) task.
    Returns a result dict for reporting.
    Uses a per-symbol lock to prevent concurrent writes corrupting Parquet files.
    """
    symbol = task["symbol"]
    month = task["month"]
    market = task["market"]
    output_dir = task["output_dir"]
    symbol_locks = task["symbol_locks"]

    interval = task.get("interval", "1m")
    url = build_url(market, symbol, month, interval=interval)
    df = download_and_parse(url)

    if df is None or df.empty:
        return {"symbol": symbol, "month": month, "status": "skipped", "rows": 0}

    parquet_path = output_dir / f"{symbol}.parquet"

    # Acquire per-symbol lock before writing — prevents concurrent months for the
    # same symbol from corrupting the Parquet file via simultaneous read-merge-write
    with symbol_locks[symbol]:
        append_to_parquet(parquet_path, df)

    return {"symbol": symbol, "month": month, "status": "ok", "rows": len(df)}


# ─────────────────────────────────────────────
# Main Orchestrator
# ─────────────────────────────────────────────

def run_downloader(
    market: str,
    top_n: int,
    years_back: int,
    threads: int,
    output_dir: Path,
    symbol_list: list[str] | None = None,
    interval: str = "1m",
    days_back: int = None,
    start_month: str = None,
    end_month: str = None,
):
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_file = output_dir / f"checkpoint_{market}_{interval}.txt"

    # Get symbols
    if symbol_list:
        symbols = [s.strip().upper() for s in symbol_list]
        log.info(f"Using provided symbol list: {len(symbols)} symbols.")
    else:
        symbols = fetch_top_symbols(market, top_n)

    months = generate_months(years_back=years_back, days_back=days_back,
                             start_month=start_month, end_month=end_month)
    log.info(f"Interval        : {interval}")
    log.info(f"Date range: {months[0]} → {months[-1]} ({len(months)} months)")

    completed = load_checkpoint(checkpoint_file)
    log.info(f"Checkpoint: {len(completed)} tasks already completed.")

    # Per-symbol locks — prevent concurrent months for same symbol corrupting Parquet files
    symbol_locks = {symbol: Lock() for symbol in symbols}

    # Build task list
    tasks = []
    for symbol in symbols:
        for month in months:
            key = f"{symbol}:{month}"
            if key not in completed:
                tasks.append({
                    "symbol": symbol,
                    "month": month,
                    "market": market,
                    "interval": interval,
                    "output_dir": output_dir,
                    "symbol_locks": symbol_locks,
                    "key": key,
                })

    total = len(tasks)
    log.info(f"Tasks to process: {total} (skipping {len(completed)} already done)")

    if total == 0:
        log.info("Nothing to do. All tasks already completed.")
        return

    # Stats tracking
    stats = {"ok": 0, "skipped": 0, "error": 0, "total_rows": 0}

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {executor.submit(process_task, task): task for task in tasks}

        with tqdm(total=total, desc=f"Downloading {market}", unit="file", dynamic_ncols=True) as pbar:
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                    status = result["status"]

                    if status == "ok":
                        stats["ok"] += 1
                        stats["total_rows"] += result["rows"]
                        save_checkpoint(checkpoint_file, task["key"])
                    elif status == "skipped":
                        stats["skipped"] += 1
                        save_checkpoint(checkpoint_file, task["key"])  # Don't re-attempt 404s

                except Exception as e:
                    stats["error"] += 1
                    log.debug(f"Task failed {task['symbol']} {task['month']}: {e}")

                pbar.set_postfix(
                    ok=stats["ok"],
                    skipped=stats["skipped"],
                    errors=stats["error"],
                )
                pbar.update(1)

    # Final summary
    log.info("=" * 60)
    log.info(f"Download complete for market: {market.upper()}")
    log.info(f"  Files downloaded : {stats['ok']}")
    log.info(f"  Files skipped    : {stats['skipped']} (404 / no data)")
    log.info(f"  Errors           : {stats['error']}")
    log.info(f"  Total rows stored: {stats['total_rows']:,}")
    log.info(f"  Output directory : {output_dir.resolve()}")
    log.info("=" * 60)


# ─────────────────────────────────────────────
# Parquet Reader Utility
# ─────────────────────────────────────────────

def load_symbol(symbol: str, output_dir: Path, start: str = None, end: str = None) -> pd.DataFrame:
    """
    Load a symbol's Parquet file into a DataFrame.
    Optionally filter by date range (YYYY-MM-DD strings).

    Example:
        from config import RAW_BINANCE_SPOT, RAW_BINANCE_FUT
        df = load_symbol("BTCUSDT", RAW_BINANCE_FUT)
        df = load_symbol("ETHUSDT", RAW_BINANCE_SPOT, start="2023-01-01", end="2023-12-31")
    """
    path = output_dir / f"{symbol}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"No data found for {symbol} at {path}")

    df = pd.read_parquet(path)

    if start:
        df = df[df.index >= pd.Timestamp(start, tz="UTC")]
    if end:
        df = df[df.index <= pd.Timestamp(end, tz="UTC")]

    return df


# ─────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Binance historical 1-minute OHLCV data to Parquet files."
    )
    parser.add_argument(
        "--market", choices=["spot", "futures"], default="spot",
        help="Market type: 'spot' or 'futures' (USD-M perpetuals). Default: spot"
    )
    parser.add_argument(
        "--symbols", type=int, default=100,
        help="Number of top symbols to download (by USDT pair priority). Default: 100"
    )
    parser.add_argument(
        "--symbol-list", type=str, default=None,
        help="Comma-separated list of specific symbols, e.g. BTCUSDT,ETHUSDT. Overrides --symbols."
    )
    parser.add_argument(
        "--years", type=int, default=None,
        help="Years of historical data to fetch. Default: 5 (ignored if --days is set)"
    )
    parser.add_argument(
        "--days", type=int, default=None,
        help="Days of historical data to fetch. Overrides --years if provided."
    )
    parser.add_argument(
        "--start-month", type=str, default=None,
        help="Start month YYYY-MM. Use with --end-month to fetch a specific range."
    )
    parser.add_argument(
        "--end-month", type=str, default=None,
        help="End month YYYY-MM. Use with --start-month to fetch a specific range."
    )
    parser.add_argument(
        "--threads", type=int, default=8,
        help="Number of parallel download threads. Default: 8"
    )
    parser.add_argument(
        "--interval", type=str, default="1m",
        choices=["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"],
        help="Kline interval. Default: 1m"
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory for Parquet files. Defaults to config RAW_BINANCE_SPOT or RAW_BINANCE_FUT based on --market."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif args.market == "spot":
        output_dir = RAW_BINANCE_SPOT
    else:
        output_dir = RAW_BINANCE_FUT
    symbol_list = args.symbol_list.split(",") if args.symbol_list else None

    years_back = args.years if not args.days else None
    if years_back is None and args.days is None:
        years_back = 5  # default

    run_downloader(
        market=args.market,
        top_n=args.symbols,
        years_back=years_back,
        days_back=args.days,
        start_month=args.start_month,
        end_month=args.end_month,
        threads=args.threads,
        output_dir=output_dir,
        symbol_list=symbol_list,
        interval=args.interval,
    )

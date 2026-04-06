"""
config.py — Shared path and environment configuration
======================================================
Single source of truth for all data directories used by the
compiler and indexer scripts. Set DATA_ROOT in your environment
to relocate the entire data store without touching any script.

    export DATA_ROOT=/data          # production
    export DATA_ROOT=/Volumes/SSD   # external drive
    export DATA_ROOT=~/quant        # local dev

Default: ~/quant-data
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# Root — override with DATA_ROOT environment variable
# ─────────────────────────────────────────────────────────────
DATA_ROOT = Path(os.environ.get("DATA_ROOT", Path.home() / "quant-data"))

# ─────────────────────────────────────────────────────────────
# Raw layer — data as it arrives from each source, never modified
# ─────────────────────────────────────────────────────────────
RAW_DIR            = DATA_ROOT / "raw"
RAW_AMBERDATA_DIR  = RAW_DIR / "amberdata"       # daily CSVs from metl.py
RAW_BINANCE_SPOT   = RAW_DIR / "binance" / "spot"
RAW_BINANCE_FUT    = RAW_DIR / "binance" / "futures"
RAW_BLOFIN_DIR     = RAW_DIR / "blofin"
RAW_COINGECKO_DIR  = RAW_DIR / "coingecko"

# ─────────────────────────────────────────────────────────────
# Compiled layer — unified, normalized parquet partitions
# ready for TimescaleDB ingestion and indexer consumption
# ─────────────────────────────────────────────────────────────
COMPILED_DIR       = DATA_ROOT / "compiled"      # date=YYYY-MM-DD/ partitions

# ─────────────────────────────────────────────────────────────
# Leaderboard layer — pre-materialized indexer output
# consumed directly by the backtester
# ─────────────────────────────────────────────────────────────
LEADERBOARDS_DIR         = DATA_ROOT / "leaderboards"
LEADERBOARD_PRICE_DIR    = LEADERBOARDS_DIR / "price"
LEADERBOARD_OI_DIR       = LEADERBOARDS_DIR / "open_interest"
LEADERBOARD_VOLUME_DIR   = LEADERBOARDS_DIR / "volume"

# ─────────────────────────────────────────────────────────────
# Logs — one subdirectory per script
# ─────────────────────────────────────────────────────────────
LOGS_DIR           = DATA_ROOT / "logs"
LOG_AMBERDATA      = LOGS_DIR / "amberdata" / "metl.log"
LOG_BINANCE        = LOGS_DIR / "binance" / "binance_downloader.log"
LOG_BLOFIN         = LOGS_DIR / "blofin" / "blofin_downloader.log"
LOG_COINGECKO      = LOGS_DIR / "coingecko" / "coingecko_marketcap.log"
LOG_INDEXER        = LOGS_DIR / "indexer" / "build_intraday_leaderboard.log"

# ─────────────────────────────────────────────────────────────
# CoinGecko specific file paths
# ─────────────────────────────────────────────────────────────
COINGECKO_MARKETCAP_PARQUET = RAW_COINGECKO_DIR / "marketcap_daily.parquet"
COINGECKO_UNIVERSE_PARQUET  = RAW_COINGECKO_DIR / "coins_universe.parquet"
COINGECKO_CHECKPOINT        = RAW_COINGECKO_DIR / "checkpoint_historical.txt"

# ─────────────────────────────────────────────────────────────
# Google Sheets credentials (metl.py)
# ─────────────────────────────────────────────────────────────
GSHEETS_TOKEN_PATH       = DATA_ROOT / "credentials" / "token.json"
GSHEETS_CREDENTIALS_PATH = DATA_ROOT / "credentials" / "credentials.json"


def ensure_dirs():
    """Create all directories that don't exist yet. Call once at script startup."""
    dirs = [
        RAW_AMBERDATA_DIR,
        RAW_BINANCE_SPOT,
        RAW_BINANCE_FUT,
        RAW_BLOFIN_DIR,
        RAW_COINGECKO_DIR,
        COMPILED_DIR,
        LEADERBOARD_PRICE_DIR,
        LEADERBOARD_OI_DIR,
        LEADERBOARD_VOLUME_DIR,
        LOG_AMBERDATA.parent,
        LOG_BINANCE.parent,
        LOG_BLOFIN.parent,
        LOG_COINGECKO.parent,
        LOG_INDEXER.parent,
        GSHEETS_TOKEN_PATH.parent,
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def amberdata_csv_path(date_obj) -> Path:
    """Return the raw CSV path for a given date. e.g. raw/amberdata/2025-01-15.csv"""
    return RAW_AMBERDATA_DIR / f"{date_obj.strftime('%Y-%m-%d')}.csv"


def amberdata_summary_csv_path(date_obj) -> Path:
    """Return the summary CSV path for a given date."""
    return RAW_AMBERDATA_DIR / f"{date_obj.strftime('%Y-%m-%d')}_summary.csv"


def compiled_partition_dir(date_obj) -> Path:
    """Return the compiled parquet partition dir for a date. e.g. compiled/date=2025-01-15/"""
    return COMPILED_DIR / f"date={date_obj.strftime('%Y-%m-%d')}"


def leaderboard_dir_for_metric(metric: str) -> Path:
    """Return the output directory for a given leaderboard metric."""
    mapping = {
        "price":         LEADERBOARD_PRICE_DIR,
        "open_interest": LEADERBOARD_OI_DIR,
        "volume":        LEADERBOARD_VOLUME_DIR,
    }
    if metric not in mapping:
        raise ValueError(f"Unknown metric '{metric}'. Valid: {list(mapping)}")
    return mapping[metric]


if __name__ == "__main__":
    # Print the resolved directory layout — useful for verifying a new environment
    ensure_dirs()
    print(f"\n📂 DATA_ROOT  →  {DATA_ROOT}\n")
    sections = [
        ("Raw",       [RAW_AMBERDATA_DIR, RAW_BINANCE_SPOT, RAW_BINANCE_FUT, RAW_BLOFIN_DIR, RAW_COINGECKO_DIR]),
        ("Compiled",  [COMPILED_DIR]),
        ("Leaderboards", [LEADERBOARD_PRICE_DIR, LEADERBOARD_OI_DIR, LEADERBOARD_VOLUME_DIR]),
        ("Logs",      [LOG_AMBERDATA.parent, LOG_BINANCE.parent, LOG_BLOFIN.parent, LOG_COINGECKO.parent, LOG_INDEXER.parent]),
        ("Credentials", [GSHEETS_TOKEN_PATH.parent]),
    ]
    for section, paths in sections:
        print(f"  {section}")
        for p in paths:
            exists = "✓" if p.exists() else "+"
            print(f"    {exists}  {p}")
        print()

#!/usr/bin/env python3
"""
metl_v2.py — Binance futures 1-minute backfill from Amberdata.

Fetches OI / price / funding / L-S / trades / liquidations / ticker per symbol
per day in parallel, merges on the OHLCV spine, forward-fills sparse feeds,
writes a per-day CSV backup, and loads the CSV into market.futures_1m.

Refactor of metl.py: dead Google Sheets scaffolding removed, fetch boilerplate
unified through a single _amber_fetch helper, DB job tracking uses a shared
context manager. Behavior on the live CSV→Postgres path is unchanged.
"""
import os
import sys
import csv
import time
import argparse
import threading
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from requests.adapters import HTTPAdapter

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn
from pipeline.db.load_csv_to_futures_1m import load_csv as load_csv_to_futures_1m

SCRIPT_START_TIME = time.time()

# ============================================================
# CONFIG
# ============================================================
BASE_URL = "https://api.amberdata.com/markets/futures"
TIMEOUT = 30
CSV_FLUSH_CHUNK = 100_000
CSV_BACKUP_ENABLED = True
CSV_BACKUP_DIR = os.environ.get(
    "CSV_BACKUP_DIR",
    "/Users/johnmullin/Desktop/desk/import/oi_logger/ob-backfills/",
)

# Parallelization
RATE_LIMIT_RPS = 15
MAX_WORKERS = 15
ENDPOINT_WORKERS = 6
PAGE_WORKERS = 8
VERBOSE = False

# Endpoint toggles
FETCH_OI           = True
FETCH_PRICE        = True
FETCH_FUNDING      = True
FETCH_LS           = True
FETCH_TICKER       = True
FETCH_LIQUIDATIONS = True
FETCH_TRADES       = True
FETCH_ORDERBOOK    = False   # Amberdata OB page discovery stalls on ~5 symbols/day
FETCH_MARKETCAP    = True    # CoinGecko daily, loaded from parquet (not live)

AUTO_RESUME = True

MARKETCAP_PARQUET = "data/marketcap/marketcap_daily.parquet"

# ============================================================
# AMBER API KEY
# ============================================================
def _load_amber_api_key():
    key = os.environ.get("AMBER_API_KEY")
    if key:
        return key
    secrets_path = Path("/mnt/quant-data/credentials/secrets.env")
    if secrets_path.exists():
        for line in secrets_path.read_text().splitlines():
            line = line.strip()
            if line.startswith("AMBER_API_KEY="):
                return line.split("=", 1)[1].strip()
    return None


AMBER_API_KEY = _load_amber_api_key()
if not AMBER_API_KEY:
    raise RuntimeError("AMBER_API_KEY not set — add to secrets.env")

# ============================================================
# SESSION + RATE LIMIT
# ============================================================
thread_local = threading.local()
_rl_lock = threading.Lock()
_next_allowed = 0.0


def get_session():
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update({
            "x-api-key": AMBER_API_KEY,
            "Accept-Encoding": "gzip, deflate, br",
        })
        adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=3)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        thread_local.session = s
    return thread_local.session


def rl_sleep():
    global _next_allowed
    with _rl_lock:
        now = time.time()
        if now < _next_allowed:
            time.sleep(_next_allowed - now)
        _next_allowed = max(_next_allowed, now) + (1.0 / RATE_LIMIT_RPS)


# ============================================================
# SMALL HELPERS
# ============================================================
def log(*a, **k):
    if VERBOSE:
        print(*a, **k)


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def safe_result(futures, key, default):
    if key not in futures:
        return default
    try:
        return futures[key].result()
    except Exception as e:
        print(f"⚠️ Endpoint failed → {key}: {e}")
        return default


def normalize_ts(ts):
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


def parse_amber_ts(value):
    if isinstance(value, int):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _fmt_range(start_ts, end_ts):
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    return (
        start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
    )


# ============================================================
# DB JOB TRACKING — market.compiler_jobs
# Best-effort: a DB failure must never interrupt the CSV/parquet write path.
# ============================================================
@contextmanager
def _db_conn():
    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            yield conn, cur
            conn.commit()
        finally:
            cur.close()
    finally:
        conn.close()


def _get_enabled_endpoints():
    return [name for name, ep in ENDPOINTS.items() if ep.get("enabled")]


def job_create(date_from, date_to, triggered_by='cli', run_tag=None):
    enabled = _get_enabled_endpoints()
    try:
        with _db_conn() as (_conn, cur):
            cur.execute("""
                INSERT INTO market.compiler_jobs
                    (source_id, status, date_from, date_to, endpoints_enabled,
                     triggered_by, run_tag, started_at)
                VALUES (1, 'running', %s, %s, %s, %s, %s, NOW())
                RETURNING job_id
            """, (date_from, date_to, enabled, triggered_by, run_tag))
            job_id = str(cur.fetchone()[0])
        print(f"   📋 Job created → {job_id}")
        return job_id
    except Exception as e:
        print(f"⚠️ job_create failed → {e}")
        return None


def job_set_total(job_id, total):
    if not job_id:
        return
    try:
        with _db_conn() as (_conn, cur):
            cur.execute(
                "UPDATE market.compiler_jobs SET symbols_total=%s WHERE job_id=%s",
                (total, job_id),
            )
    except Exception as e:
        print(f"⚠️ job_set_total failed → {e}")


def job_increment(job_id, rows_added):
    if not job_id:
        return
    try:
        with _db_conn() as (_conn, cur):
            cur.execute("""
                UPDATE market.compiler_jobs
                SET symbols_done   = symbols_done + 1,
                    rows_written   = rows_written + %s,
                    last_heartbeat = NOW()
                WHERE job_id = %s
            """, (rows_added, job_id))
    except Exception as e:
        print(f"⚠️ job_increment failed → {e}")


def job_complete(job_id):
    if not job_id:
        return
    try:
        with _db_conn() as (_conn, cur):
            cur.execute("""
                UPDATE market.compiler_jobs
                SET status='complete', completed_at=NOW()
                WHERE job_id=%s
            """, (job_id,))
        print(f"   ✅ Job complete → {job_id}")
    except Exception as e:
        print(f"⚠️ job_complete failed → {e}")


def job_fail(job_id, error_msg):
    if not job_id:
        return
    try:
        with _db_conn() as (_conn, cur):
            cur.execute("""
                UPDATE market.compiler_jobs
                SET status='failed', completed_at=NOW(), error_msg=%s
                WHERE job_id=%s
            """, (str(error_msg)[:2000], job_id))
        print(f"   ❌ Job failed → {job_id}")
    except Exception as e:
        print(f"⚠️ job_fail failed → {e}")


# ============================================================
# MARKETCAP (CoinGecko daily parquet)
# ============================================================
_mcap_sym_cache = {}   # date_str → {base_symbol → {market_cap_usd, market_cap_rank}}


def load_marketcap_day(date_obj):
    """
    Returns a dict keyed by base symbol (e.g. 'BTC') →
        {'market_cap_usd': float, 'market_cap_rank': int}
    Cached per date.
    """
    date_str = date_obj.strftime("%Y-%m-%d")
    if date_str in _mcap_sym_cache:
        return _mcap_sym_cache[date_str]
    if not os.path.exists(MARKETCAP_PARQUET):
        print(f"⚠️  marketcap parquet not found → {MARKETCAP_PARQUET}")
        _mcap_sym_cache[date_str] = {}
        return {}
    try:
        import pandas as pd
        df = pd.read_parquet(MARKETCAP_PARQUET)
        df["date"] = pd.to_datetime(df["date"], utc=True)
        target = pd.Timestamp(date_str, tz="UTC")
        day_df = df[df["date"] == target]
        result = {}
        for _, row in day_df.iterrows():
            cg_sym = str(row.get("symbol", row["coin_id"])).upper()
            result[cg_sym] = {
                "market_cap_usd":  float(row["market_cap_usd"]) if row["market_cap_usd"] else 0.0,
                "market_cap_rank": int(row["rank_num"]) if row.get("rank_num") else 0,
            }
        _mcap_sym_cache[date_str] = result
        print(f"   📊 Marketcap loaded for {date_str} → {len(result)} coins")
        return result
    except Exception as e:
        print(f"⚠️  Failed to load marketcap parquet: {e}")
        _mcap_sym_cache[date_str] = {}
        return {}


# ============================================================
# AMBER HTTP HELPER
# ============================================================
def _amber_fetch(path, symbol, start_ts, end_ts, retries=1, extra_query=""):
    """
    Single-endpoint Amber GET with optional retry. Returns payload['data'] list
    or None on failure. `path` is the endpoint suffix after /markets/futures/.
    """
    start_str, end_str = _fmt_range(start_ts, end_ts)
    url = (
        f"{BASE_URL}/{path}/{symbol}"
        f"?exchange=binance&startDate={start_str}&endDate={end_str}{extra_query}"
    )
    for attempt in range(retries):
        rl_sleep()
        resp = get_session().get(url, timeout=TIMEOUT)
        if resp.status_code == 200:
            return resp.json().get("payload", {}).get("data", [])
        print(f"⚠️ {symbol} {path} error → {resp.status_code} (attempt {attempt+1}/{retries})")
        if attempt < retries - 1:
            time.sleep(2 ** (attempt + 1))
    return None


# ============================================================
# FETCHES
# ============================================================
def fetch_ohlcv_data(symbol, start_ts, end_ts):
    """OHLCV spine: {bucket: {price, volume}}."""
    data = _amber_fetch("ohlcv", symbol, start_ts, end_ts,
                        extra_query="&timeInterval=minutes")
    if data is None:
        return {}
    px_map = {}
    for r in data:
        ts = normalize_ts(r.get("exchangeTimestamp") or r.get("timestamp"))
        if ts is None:
            continue
        px_map[int(ts // 60000)] = {
            "price":  float(r.get("close", 0)),
            "volume": float(r.get("volume", 0)),
        }
    return px_map


def fetch_oi_data(symbol, start_ts, end_ts):
    """Open interest: {bucket: value}."""
    data = _amber_fetch("open-interest", symbol, start_ts, end_ts)
    if data is None:
        return {}
    oi_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts is None:
            continue
        oi_map[int(ts // 60000)] = float(r.get("value", 0))
    return oi_map


def fetch_funding_data(symbol, start_ts, end_ts):
    """Funding rates: {bucket: funding_rate}."""
    data = _amber_fetch("funding-rates", symbol, start_ts, end_ts)
    if data is None:
        return {}
    funding_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts:
            funding_map[int(ts // 60000)] = float(r.get("fundingRate", 0))
    return funding_map


def fetch_long_short_data(symbol, start_ts, end_ts):
    """Long/short ratio: {bucket: ratio}."""
    data = _amber_fetch("long-short-ratio", symbol, start_ts, end_ts)
    if data is None:
        return {}
    ls_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts:
            ls_map[int(ts // 60000)] = float(r.get("ratio", 0))
    return ls_map


def fetch_trades_data(symbol, start_ts, end_ts):
    """Taker buy/sell delta: {bucket: net_volume}. Retried — heavy endpoint."""
    data = _amber_fetch("trades", symbol, start_ts, end_ts, retries=5)
    if data is None:
        return {}
    taker_map = {}
    for t in data:
        ts = normalize_ts(t.get("exchangeTimestamp") or t.get("timestamp"))
        if ts is None:
            continue
        bucket = int(ts // 60000)
        volume = float(t.get("volume", 0))
        if bucket not in taker_map:
            taker_map[bucket] = 0
        if t.get("isBuySide", False):
            taker_map[bucket] += volume
        else:
            taker_map[bucket] -= volume
    return taker_map


def fetch_liquidations_data(symbol, start_ts, end_ts):
    """Liquidations: {bucket: {long, short}}."""
    data = _amber_fetch("liquidations", symbol, start_ts, end_ts)
    if data is None:
        return {}
    liq_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts is None:
            continue
        liq_map[int(ts // 60000)] = {
            "long":  float(r.get("longLiquidations", 0)),
            "short": float(r.get("shortLiquidations", 0)),
        }
    return liq_map


def fetch_ticker_data(symbol, start_ts, end_ts):
    """Ticker: {bucket: {spread_pct, imbalance, basis_pct}}. Retried — heavy."""
    data = _amber_fetch("tickers", symbol, start_ts, end_ts, retries=5)
    if data is None:
        return {}
    ticker_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts is None:
            continue
        bid = f(r.get("bid"))
        ask = f(r.get("ask"))
        mid = f(r.get("mid"))
        bid_vol = f(r.get("bidVolume"))
        ask_vol = f(r.get("askVolume"))
        spread_pct = ((ask - bid) / mid * 100 if mid else 0)
        imbalance = ((bid_vol - ask_vol) / (bid_vol + ask_vol) if (bid_vol + ask_vol) > 0 else 0)
        mark = float(r.get("markPrice", 0))
        index = float(r.get("indexPrice", 0))
        basis_pct = ((mark - index) / index * 100 if index else 0)
        ticker_map[int(ts // 60000)] = {
            "spread_pct": spread_pct,
            "imbalance": imbalance,
            "basis_pct": basis_pct,
        }
    return ticker_map


def fetch_orderbook_snapshots_batch(symbol, start_ts, end_ts):
    """
    Order book L1 snapshots aggregated per minute bucket. Two-phase:
    (1) walk pagination to collect page URLs, (2) fan out page GETs.
    """
    start_str, end_str = _fmt_range(start_ts, end_ts)
    max_level = 1
    base_url = (
        f"{BASE_URL}/order-book-snapshots/{symbol}"
        f"?exchange=binance&startDate={start_str}&endDate={end_str}&maxLevel={max_level}"
    )
    page_urls = []
    url = base_url
    page_cap = 500
    page_count = 0
    while url and page_count < page_cap:
        rl_sleep()
        for attempt in range(5):
            resp = get_session().get(url, timeout=TIMEOUT)
            if resp.status_code == 200:
                break
            print(f"⚠️ {symbol} OB page discovery error → {resp.status_code} (attempt {attempt+1}/5)")
            if attempt < 4:
                time.sleep(2 ** (attempt + 1))
        else:
            print(f"❌ {symbol} OB page discovery failed — skipping")
            return {}
        payload = resp.json()["payload"]
        page_urls.append(url)
        url = payload["metadata"].get("next")
        page_count += 1
    if page_count == page_cap:
        print(f"⚠️ {symbol} OB page cap hit")

    def fetch_page(page_url):
        rl_sleep()
        r = get_session().get(page_url, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        return r.json()["payload"]["data"]

    ob_map = {}
    with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as ex:
        futures = {ex.submit(fetch_page, u): u for u in page_urls}
        for future in as_completed(futures):
            for snap in future.result():
                ts = normalize_ts(snap.get("timestamp") or snap.get("exchangeTimestamp"))
                if ts is None:
                    continue
                bids = snap.get("bid", [])
                asks = snap.get("ask", [])
                if not bids or not asks:
                    continue
                bid_depth = sum(float(b["volume"]) for b in bids)
                ask_depth = sum(float(a["volume"]) for a in asks)
                total = bid_depth + ask_depth
                imbalance = (bid_depth - ask_depth) / total if total else 0
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                mid = (best_bid + best_ask) / 2
                spread_pct = ((best_ask - best_bid) / mid * 100 if mid else 0)
                ob_map[int(ts // 60000)] = {
                    "bid_depth": bid_depth,
                    "ask_depth": ask_depth,
                    "imbalance": imbalance,
                    "spread_pct": spread_pct,
                }
    return ob_map


def get_symbols_for_date(date_obj):
    """Walk Amber's instrument directory and return symbols active on date_obj."""
    url = f"{BASE_URL}/open-interest/information"
    params = {"exchange": "binance"}
    symbols = []
    while True:
        rl_sleep()
        r = get_session().get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        resp = r.json()["payload"]
        data = resp["data"]
        metadata = resp["metadata"]
        if not data:
            break
        for row in data:
            symb = row["instrument"]
            start = parse_amber_ts(row["startDate"])
            end   = parse_amber_ts(row["endDate"])
            if start.date() <= date_obj.date() <= end.date():
                symbols.append(symb)
        next_url = metadata.get("next")
        if not next_url:
            break
        url = next_url
        params = None
    return symbols


# ============================================================
# ENDPOINT REGISTRY
# ============================================================
ENDPOINTS = {
    "ohlcv": {
        "enabled": FETCH_PRICE,
        "fetch": fetch_ohlcv_data,
        "columns": ["price", "volume"],
        "type": "map",
    },
    "oi": {
        "enabled": FETCH_OI,
        "fetch": fetch_oi_data,
        "columns": ["open_interest"],
        "type": "map",
    },
    "funding": {
        "enabled": FETCH_FUNDING,
        "fetch": fetch_funding_data,
        "columns": ["funding_rate"],
        "type": "map",
    },
    "ls": {
        "enabled": FETCH_LS,
        "fetch": fetch_long_short_data,
        "columns": ["long_short_ratio"],
        "type": "map",
    },
    "trades": {
        "enabled": FETCH_TRADES,
        "fetch": fetch_trades_data,
        "columns": ["trade_delta"],
        "type": "map",
    },
    "liquidations": {
        "enabled": FETCH_LIQUIDATIONS,
        "fetch": fetch_liquidations_data,
        "columns": ["long_liqs", "short_liqs"],
        "type": "map",
    },
    "orderbook": {
        "enabled": FETCH_ORDERBOOK,
        "fetch": fetch_orderbook_snapshots_batch,
        "columns": [
            "last_bid_depth",
            "last_ask_depth",
            "last_depth_imbalance",
            "last_spread_pct",
        ],
        "type": "map",
    },
    "ticker": {
        "enabled": FETCH_TICKER,
        "fetch": fetch_ticker_data,
        "columns": ["spread_pct", "bid_ask_imbalance", "basis_pct"],
        "type": "map",
    },
    "marketcap": {
        "enabled": FETCH_MARKETCAP,
        "fetch": None,   # loaded from parquet via load_marketcap_day()
        "columns": ["market_cap_usd", "market_cap_rank"],
        "type": "daily",
    },
}


# ============================================================
# MERGE — build per-minute rows for one symbol/day
# ============================================================
def fetch_market_data(symbol, start_ts, end_ts, mcap_day=None):
    rows_out = []
    start_str_short = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    log(f"       FETCHES - {symbol}:")

    with ThreadPoolExecutor(max_workers=ENDPOINT_WORKERS) as ex:
        futures = {}
        for name, ep in ENDPOINTS.items():
            if ep["enabled"] and ep["fetch"] is not None:
                futures[name] = ex.submit(ep["fetch"], symbol, start_ts, end_ts)
        results = {}
        for name in ENDPOINTS.keys():
            if ENDPOINTS[name]["fetch"] is None:
                results[name] = {}   # daily join — not fetched here
            else:
                results[name] = safe_result(futures, name, {})

    log(f"       DATASETS - {start_str_short}:")
    for name, data in results.items():
        if isinstance(data, (dict, list)):
            log(f"           {name} rows:", len(data))

    # Forward-fill state across the OHLCV spine.
    last_vals = {
        "oi": 0, "price": 0, "volume": 0, "funding": 0, "ls": 0, "trades": 0,
        "spread": 0, "imbalance": 0, "basis": 0,
        "bid_depth": 0, "ask_depth": 0, "depth_imb": 0, "depth_spread": 0,
        "market_cap_usd": 0, "market_cap_rank": 0,
    }

    # Resolve base symbol for CoinGecko lookup (e.g. "BTCUSDT" → "BTC").
    base_sym = symbol.upper().replace("USDT", "").replace("BUSD", "").replace("USDC", "")
    if mcap_day and base_sym in mcap_day:
        mc = mcap_day[base_sym]
        last_vals["market_cap_usd"]  = mc.get("market_cap_usd", 0)
        last_vals["market_cap_rank"] = mc.get("market_cap_rank", 0)

    ohlcv_data = results.get("ohlcv", {})
    for bucket in sorted(ohlcv_data.keys()):
        dt = datetime.fromtimestamp(bucket * 60, tz=timezone.utc)
        row = [dt.strftime("%Y-%m-%d %H:%M:%S"), symbol]

        if ENDPOINTS["ohlcv"]["enabled"]:
            px = ohlcv_data[bucket]
            last_vals["price"]  = px.get("price", 0)
            last_vals["volume"] = px.get("volume", 0)
            row.extend([last_vals["price"], last_vals["volume"]])

        if ENDPOINTS["oi"]["enabled"]:
            val = results["oi"].get(bucket, last_vals["oi"])
            last_vals["oi"] = val
            row.append(val)

        if ENDPOINTS["funding"]["enabled"]:
            val = results["funding"].get(bucket, last_vals["funding"])
            last_vals["funding"] = val
            row.append(val)

        if ENDPOINTS["ls"]["enabled"]:
            val = results["ls"].get(bucket, last_vals["ls"])
            last_vals["ls"] = val
            row.append(val)

        if ENDPOINTS["trades"]["enabled"]:
            val = results["trades"].get(bucket, last_vals["trades"])
            last_vals["trades"] = val
            row.append(val)

        if ENDPOINTS["liquidations"]["enabled"]:
            liq = results["liquidations"].get(bucket, {"long": 0, "short": 0})
            row.extend([liq["long"], liq["short"]])

        if ENDPOINTS["orderbook"]["enabled"]:
            ob = results["orderbook"].get(bucket)
            if ob:
                last_vals["bid_depth"]    = ob["bid_depth"]
                last_vals["ask_depth"]    = ob["ask_depth"]
                last_vals["depth_imb"]    = ob["imbalance"]
                last_vals["depth_spread"] = ob["spread_pct"]
            row.extend([
                last_vals["bid_depth"],
                last_vals["ask_depth"],
                last_vals["depth_imb"],
                last_vals["depth_spread"],
            ])

        if ENDPOINTS["ticker"]["enabled"]:
            tick = results["ticker"].get(bucket)
            if tick:
                last_vals["spread"]    = tick["spread_pct"]
                last_vals["imbalance"] = tick["imbalance"]
                last_vals["basis"]     = tick["basis_pct"]
            row.extend([last_vals["spread"], last_vals["imbalance"], last_vals["basis"]])

        if ENDPOINTS["marketcap"]["enabled"]:
            row.extend([last_vals["market_cap_usd"], last_vals["market_cap_rank"]])

        rows_out.append(row)
    return rows_out


def build_header():
    header = ["timestamp_utc", "symbol"]
    for ep in ENDPOINTS.values():
        if ep["enabled"]:
            header.extend(ep["columns"])
    return header


# ============================================================
# CSV I/O
# ============================================================
def _day_csv_path(date_obj):
    return os.path.join(CSV_BACKUP_DIR, f"oi_{date_obj.strftime('%Y%m%d')}.csv")


def csv_exists_for_day(date_obj):
    return os.path.exists(_day_csv_path(date_obj))


def append_day_csv(date_obj, header, rows, write_header=False):
    if not CSV_BACKUP_ENABLED:
        return
    os.makedirs(CSV_BACKUP_DIR, exist_ok=True)
    file_path = _day_csv_path(date_obj)
    mode = "a" if os.path.exists(file_path) else "w"
    with open(file_path, mode, newline="") as fh:
        writer = csv.writer(fh)
        if write_header or mode == "w":
            writer.writerow(header)
        writer.writerows(rows)
        fh.flush()
        os.fsync(fh.fileno())
    size_mb = os.path.getsize(file_path) / (1024 ** 2)
    print(f"💾 CSV chunk written → {len(rows):,} rows | {size_mb:.2f} MB")


# ============================================================
# RUNTIME PRINTS
# ============================================================
def print_runtime():
    total = time.time() - SCRIPT_START_TIME
    print("\n========================================")
    print("⏱ SCRIPT RUNTIME")
    print("========================================")
    print(f"Seconds : {total:,.2f}")
    print(f"Minutes : {total/60:,.2f}")
    print(f"Hours   : {total/3600:,.2f}")
    print("========================================\n")


def print_day_runtime(day_label, start_time):
    seconds = time.time() - start_time
    print("\n----------------------------------------")
    print(f"⏱ Runtime for {day_label}")
    print("----------------------------------------")
    print(f"Seconds : {seconds:,.2f}")
    print(f"Minutes : {seconds/60:,.2f}")
    print(f"Hours   : {seconds/3600:,.2f}")
    print("----------------------------------------\n")


# ============================================================
# MAIN (parallelized per symbol)
# ============================================================
def process_symbol(symb, start_ts, end_ts, mcap_day=None):
    """Worker wrapper for parallel symbol processing."""
    try:
        start = datetime.now()
        rows = fetch_market_data(symb, start_ts, end_ts, mcap_day=mcap_day)
        elap = datetime.now() - start
        return {"symbol": symb, "rows": rows, "elapsed": elap, "success": True}
    except Exception as e:
        return {"symbol": symb, "rows": [], "elapsed": 0, "success": False, "error": str(e)}


def _load_day_into_db(day_csv_path, job_id, failure_label):
    """Load a day's CSV into market.futures_1m. Idempotent via ON CONFLICT DO NOTHING."""
    try:
        print(f"\n💽 Loading CSV into market.futures_1m → {day_csv_path}")
        result = load_csv_to_futures_1m(day_csv_path)
        print(
            f"   ✅ DB load done → "
            f"read={result['rows_read']:,} "
            f"sent={result['rows_inserted']:,} "
            f"skipped={result['rows_skipped']:,} "
            f"in {result['elapsed_sec']}s"
        )
    except Exception as load_err:
        print(f"   ⚠️ DB load failed → {load_err}")
        job_fail(job_id, f"{failure_label}: {load_err}")
        raise


def main(start_date, end_date, job_id=None, triggered_by='cli', run_tag=None):
    if job_id is None:
        job_id = job_create(start_date.date(), end_date.date(), triggered_by, run_tag)

    try:
        for day in daterange(start_date, end_date):
            expected_header = build_header()
            day_start_time = time.time()
            print(f"\n📅 {day.date()}")

            # Resume path: CSV already exists → still run DB load to cover the
            # case where an earlier run was killed mid-insert. load_csv is
            # idempotent, so re-running is a cheap correctness guarantee.
            if AUTO_RESUME and csv_exists_for_day(day):
                print(f"\n⏭ Skipping fetch for {day.date()} → CSV already exists")
                _load_day_into_db(_day_csv_path(day), job_id,
                                  "Skip-path DB sync failed")
                continue

            day_utc = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
            start_ts = int(day_utc.timestamp() * 1000)
            end_ts   = int((day_utc + timedelta(days=1) - timedelta(seconds=1)).timestamp() * 1000)

            print("   🔎 Fetching instrument universe for date...")
            symbols = get_symbols_for_date(day)
            total_symbs = len(symbols)
            job_set_total(job_id, total_symbs)
            print(f"   ✅ {total_symbs} symbols active\n")
            print("  Batch write mode ENABLED (streaming CSV)")

            # Load CoinGecko market cap once per day — shared across all symbols.
            mcap_day = load_marketcap_day(day) if FETCH_MARKETCAP else {}

            all_rows_day = []
            script_start = datetime.now()
            print("  script_start:", script_start, "\n")
            print(f"\n⚡ Parallel mode ENABLED ({MAX_WORKERS} workers)\n")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_symbol, symb, start_ts, end_ts, mcap_day): symb
                    for symb in symbols
                }
                completed = 0
                for future in as_completed(futures):
                    result = future.result()
                    completed += 1
                    symb = result["symbol"]
                    progress = round((completed / total_symbs) * 100, 2)
                    if result["success"]:
                        print(f"   ✅ {symb} done → {result['elapsed']} ({progress}%)")
                        all_rows_day.extend(result["rows"])
                        job_increment(job_id, len(result["rows"]))
                        if len(all_rows_day) >= CSV_FLUSH_CHUNK:
                            append_day_csv(
                                day, expected_header, all_rows_day,
                                write_header=not os.path.exists(_day_csv_path(day)),
                            )
                            all_rows_day.clear()
                    else:
                        print(f"   ❌ {symb} failed → {result['error']}")

            if all_rows_day:
                print(f"\n💾 Final CSV flush → {len(all_rows_day):,} rows")
                append_day_csv(
                    day, expected_header, all_rows_day,
                    write_header=not os.path.exists(_day_csv_path(day)),
                )
                all_rows_day.clear()

            # Load the day's CSV into market.futures_1m. Compiler job "complete"
            # status reflects DB state, not just CSV state.
            if os.path.exists(_day_csv_path(day)):
                _load_day_into_db(_day_csv_path(day), job_id,
                                  "CSV written but DB load failed")

            print_day_runtime(str(day.date()), day_start_time)
            print_runtime()
    except Exception as e:
        job_fail(job_id, str(e))
        raise

    job_complete(job_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=False)
    parser.add_argument("--job-id", type=str, default=None, dest="job_id")
    parser.add_argument("--triggered-by", type=str, default="cli",
                        choices=["ui", "cli", "scheduler"], dest="triggered_by")
    parser.add_argument("--run-tag", type=str, default=None, dest="run_tag")
    args = parser.parse_args()
    start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_date = (datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if args.end else start_date)
    print(f"\n📆 Backfill Range: {start_date.date()} → {end_date.date()}")
    main(start_date, end_date,
         job_id=args.job_id,
         triggered_by=args.triggered_by,
         run_tag=args.run_tag)

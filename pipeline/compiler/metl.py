#!/usr/bin/env python3
""" amber_oi_backfill_to_sheets.py - Backfill 1-minute Binance Open Interest from Amberdata → Google Sheets """
import os
import sys
import csv
import time
import argparse
import requests
import gspread
from datetime import datetime, timedelta, timezone
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from requests.adapters import HTTPAdapter
from bisect import bisect_left

# DB connection helper for compiler_jobs tracking
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.db.connection import get_conn

SCRIPT_START_TIME = time.time()


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
BASE_URL = "https://api.amberdata.com/markets/futures/open-interest"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
SHEET_ID = "1fhBWdMx9TsntKK8kVkAsTkax3Fwatg28xWCj_hGWgzI"

AUTO_RESUME = True

REQUEST_SLEEP = 0 #0.25
PAGE_SIZE = 1000
BATCH_MODE = True
TIMEOUT = 30

NEW_SHEET_COLS = 11
NEW_SHEET_ROWS = 700000
SHEETS_CHUNK = 1000
WRITE_SLEEP = 0.35
CSV_FLUSH_CHUNK = 100000   # rows per flush


PRINT_SUMMARY_SHEETS = False  # only works for FAST MODE
PRINT_RAW_SHEETS = False    #
CSV_BACKUP_ENABLED = True

# ============================================================
# MODE
# ============================================================
MODE = "FULL"  # FULL |  FAST
CSV_BACKUP_DIR = "/Users/johnmullin/Desktop/desk/import/oi_logger/ob-backfills/"
if MODE == "FULL": heavy_bool = True
else: heavy_bool = False

# ============================================================
# PARALLELIZATION
# ============================================================
RATE_LIMIT_RPS = 15
MAX_WORKERS = 15
ENDPOINT_WORKERS = 6
PAGE_WORKERS = 8
VERBOSE = False
thread_local = threading.local()
_rl_lock = threading.Lock()
_next_allowed = 0.0

# ============================================================
# Toggles
# ============================================================
FETCH_OI           = True   # done

#L1
FETCH_PRICE        = True # done
FETCH_FUNDING      = True # done
FETCH_LS           = True # done                               2.5min / day

#L2
FETCH_TICKER       = True   # heavy - bid, ask, mid, index,    2min / day
FETCH_LIQUIDATIONS = True   # heacy - long liqs, short liqs

#L3
FETCH_TRADES       = True   # HEAVIEST - trade delta
FETCH_ORDERBOOK    = True   # HEAVIEST - order imbalance

#DAILY (CoinGecko join — loaded from parquet, not fetched live)
FETCH_MARKETCAP    = True   # market_cap_usd, market_cap_rank


full_header_options = [
 "timestamp_utc",           # 1
 "symbol",                  # 2
 "price",                   # 3
 "volume",                  # 4
 "open_interest",           # 5
 "funding_rate",            # 6
 "long_short_ratio",        # 7
 "trade_delta",             # 8
 "long_liqs",               # 9
 "short_liqs",              # 10
 "last_bid_depth",          # 11
 "last_ask_depth",          # 12
 "last_depth_imbalance",    # 13
 "last_spread_pct",         # 14
 "spread_pct",              # 15
 "bid_ask_imbalance",       # 16
 "basis_pct",               # 17
 "market_cap_usd",          # 18  — CoinGecko daily, forward-filled per minute
 "market_cap_rank",         # 19  — historically accurate daily rank (recomputed from actual per-date mcap)
]

# ============================================================
# HELPERS
# ============================================================
def connect_sheets():
    creds = None
    WORKSHEET_NAME = "New Sheet Made"
    if os.path.exists("token.json"):creds = Credentials.from_authorized_user_file("token.json",SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json",SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)
    try:ws = sheet.worksheet(WORKSHEET_NAME)
    except:ws = sheet.add_worksheet(title=WORKSHEET_NAME,rows=1,cols=1)
    return ws
def connect_daily_sheet(date_obj):
    """ Creates / connects to a DAILY spreadsheet, Example: OI_RAW_20250212, Inside it creates worksheet: OI_20250212"""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    client = gspread.authorize(creds)

    date_str = date_obj.strftime("%Y%m%d")
    spreadsheet_name = f"OI_RAW_{date_str}"
    worksheet_name   = f"OI_{date_str}"

    try:
        sh = client.open(spreadsheet_name)
        print(f"📊 Using existing spreadsheet → {spreadsheet_name}")

    except gspread.SpreadsheetNotFound:
        print(f"🆕 Creating spreadsheet → {spreadsheet_name}")
        sh = client.create(spreadsheet_name)
        # Optional — move to same folder as master sheet
        try:sh.share(None,perm_type="anyone",role="writer")
        except: pass
    try:
        ws = sh.worksheet(worksheet_name)
        print(f"📄 Using existing worksheet → {worksheet_name}")
    except:
        print(f"🆕 Creating worksheet → {worksheet_name}")
        ws = sh.add_worksheet(title=worksheet_name,rows=NEW_SHEET_ROWS,cols=NEW_SHEET_COLS)
    return ws
def parse_amber_ts(value):
    if isinstance(value, int):return datetime.fromtimestamp(value / 1000,tz=timezone.utc) # ms epoch
    return datetime.strptime(value[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
def get_symbols_for_date(date_obj):
    url = "https://api.amberdata.com/markets/futures/open-interest/information"
    params = {"exchange": "binance"}
    symbols = []
    while True:
        rl_sleep()
        r = get_session().get(url, params=params, timeout=TIMEOUT,  )
        r.raise_for_status()
        resp = r.json()["payload"]
        data = resp["data"]
        metadata = resp["metadata"]
        if not data:break
        for row in data:
            symb = row["instrument"]
            start = parse_amber_ts(row["startDate"])
            end   = parse_amber_ts(row["endDate"])
            if start.date() <= date_obj.date() <= end.date():symbols.append(symb)
        next_url = metadata.get("next")
        if not next_url:break
        url = next_url
        params = None
        rl_sleep()
    return symbols
def log(*a, **k):
    if VERBOSE: print(*a, **k)
def rl_sleep():
    global _next_allowed
    with _rl_lock:
        now = time.time()
        if now < _next_allowed:
            time.sleep(_next_allowed - now)
        _next_allowed = max(_next_allowed, now) + (1.0 / RATE_LIMIT_RPS)
def f(x):
    try: return float(x)
    except: return 0.0
def safe_result(futures, key, default):
    if key not in futures: return default
    try: return futures[key].result()
    except Exception as e:
        print(f"⚠️ Endpoint failed → {key}: {e}")
        return default
def pct(a,b):
    if a == 0: return None   # or np.nan
    return (b-a)/a*100
# ============================================================
# DB JOB TRACKING — market.compiler_jobs
# All operations are best-effort: a DB failure must never interrupt
# the existing CSV/parquet write path.
# ============================================================
def _get_enabled_endpoints():
    """Return list of enabled endpoint names based on FETCH_* flags."""
    enabled = []
    for name, ep in ENDPOINTS.items():
        if ep.get("enabled"):
            enabled.append(name)
    return enabled

def job_create(date_from, date_to, triggered_by='cli', run_tag=None):
    """Insert a new compiler job row, return job_id UUID string."""
    enabled = _get_enabled_endpoints()
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO market.compiler_jobs
                (source_id, status, date_from, date_to, endpoints_enabled,
                 triggered_by, run_tag, started_at)
            VALUES (1, 'running', %s, %s, %s, %s, %s, NOW())
            RETURNING job_id
        """, (date_from, date_to, enabled, triggered_by, run_tag))
        job_id = str(cur.fetchone()[0])
        conn.commit()
        cur.close()
        conn.close()
        print(f"   📋 Job created → {job_id}")
        return job_id
    except Exception as e:
        print(f"⚠️ job_create failed → {e}")
        return None

def job_set_total(job_id, total):
    """Set symbols_total once symbol list is known."""
    if not job_id:
        return
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE market.compiler_jobs SET symbols_total=%s WHERE job_id=%s",
                    (total, job_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ job_set_total failed → {e}")

def job_increment(job_id, rows_added):
    """Atomically increment symbols_done by 1 and rows_written by rows_added."""
    if not job_id:
        return
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE market.compiler_jobs
            SET symbols_done = symbols_done + 1,
                rows_written  = rows_written  + %s
            WHERE job_id = %s
        """, (rows_added, job_id))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ job_increment failed → {e}")

def job_complete(job_id):
    """Mark job as complete."""
    if not job_id:
        return
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE market.compiler_jobs
            SET status='complete', completed_at=NOW()
            WHERE job_id=%s
        """, (job_id,))
        conn.commit()
        cur.close()
        conn.close()
        print(f"   ✅ Job complete → {job_id}")
    except Exception as e:
        print(f"⚠️ job_complete failed → {e}")

def job_fail(job_id, error_msg):
    """Mark job as failed with error message."""
    if not job_id:
        return
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE market.compiler_jobs
            SET status='failed', completed_at=NOW(), error_msg=%s
            WHERE job_id=%s
        """, (str(error_msg)[:2000], job_id))
        conn.commit()
        cur.close()
        conn.close()
        print(f"   ❌ Job failed → {job_id}")
    except Exception as e:
        print(f"⚠️ job_fail failed → {e}")


def compute_slope(series):
    """Linear regression slope of a time series, series = list[float]"""
    n = len(series)
    if n < 2:return 0
    x = list(range(n))
    sum_x  = sum(x)
    sum_y  = sum(series)
    sum_xy = sum(i*j for i, j in zip(x, series))
    sum_x2 = sum(i*i for i in x)
    denom = (n * sum_x2 - sum_x**2)
    if denom == 0: return 0
    slope = (n * sum_xy - sum_x * sum_y) / denom
    return slope
def r5(value):
    """Round to 5 decimals safely, Handles None / NaN / inf """
    if value is None:return None
    try: return round(float(value), 5)
    except: return None
def first_after(rows, target):
    ts_list = [r["ts"] for r in rows]
    idx = bisect_left(ts_list, target)
    return rows[idx] if idx < len(rows) else rows[-1]
def safe_append(ws, rows, retries=5):
    for attempt in range(retries):
        try:
            ws.append_rows(rows)
            return True
        except Exception as e:
            print(f"⚠️ Write retry {attempt+1} → {e}")
            time.sleep(2 ** attempt)
    print(f"❌ Failed chunk permanently ({len(rows)} rows)")
    return False
def normalize_ts(ts):
    if isinstance(ts, (int, float)): return int(ts)
    if isinstance(ts, str):
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return None

MARKETCAP_PARQUET = "data/marketcap/marketcap_daily.parquet"
_mcap_cache = {}   # date_str → {coin_id: {market_cap_usd, market_cap_rank}}
_mcap_sym_cache = {}  # date_str → {binance_symbol (upper, no USDT) → {...}}

def load_marketcap_day(date_obj):
    """
    Load CoinGecko market cap data for a given date from the parquet file.
    Returns a dict keyed by normalised base symbol (e.g. 'BTC') →
        {'market_cap_usd': float, 'market_cap_rank': int}
    Caches per date to avoid re-reading parquet on every symbol.
    """
    import os
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
            # CoinGecko symbol is lowercase (e.g. 'btc'); normalise to upper
            sym = str(row["coin_id"]).upper()  # coin_id often matches symbol
            # Also index by the 'symbol' column if present
            cg_sym = str(row.get("symbol", row["coin_id"])).upper()
            entry = {
                "market_cap_usd":  float(row["market_cap_usd"]) if row["market_cap_usd"] else 0.0,
                "market_cap_rank": int(row["rank_num"]) if row.get("rank_num") else 0,
            }
            result[cg_sym] = entry
        _mcap_sym_cache[date_str] = result
        print(f"   📊 Marketcap loaded for {date_str} → {len(result)} coins")
        return result
    except Exception as e:
        print(f"⚠️  Failed to load marketcap parquet: {e}")
        _mcap_sym_cache[date_str] = {}
        return {}


# lowered max_level from 10 to 1 (rate limit and stronger signal)

# ============================================================
# FETCHES
# ============================================================
def fetch_orderbook_snapshots_batch(symbol, start_ts, end_ts):
    fetch_start = datetime.now()
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    max_level = 1
    base_url = (f"https://api.amberdata.com/markets/futures/order-book-snapshots/{symbol}?exchange=binance&startDate={start_str}&endDate={end_str}&maxLevel={max_level}")
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
            print(f"❌ {symbol} OB page discovery failed after 3 attempts → skipping")
            return {}
        payload = resp.json()["payload"]
        page_urls.append(url)
        url = payload["metadata"].get("next")
        page_count += 1
    if page_count == page_cap: print(f"⚠️ {symbol} OB page cap hit")
    ob_map = {}
    def fetch_page(page_url):
        rl_sleep()
        r = get_session().get(page_url, timeout=TIMEOUT)
        if r.status_code != 200:return []
        return r.json()["payload"]["data"]
    with ThreadPoolExecutor(max_workers=PAGE_WORKERS) as ex:
        futures = {ex.submit(fetch_page, u): u for u in page_urls}
        for future in as_completed(futures):
            data = future.result()
            for snap in data:
                ts = normalize_ts(snap.get("timestamp") or snap.get("exchangeTimestamp"))
                if ts is None:continue
                bucket = int(ts // 60000)
                bids = snap.get("bid", [])
                asks = snap.get("ask", [])
                if not bids or not asks:continue
                bid_depth = sum(float(b["volume"]) for b in bids)
                ask_depth = sum(float(a["volume"]) for a in asks)
                if (bid_depth + ask_depth) == 0: imbalance = 0
                else:imbalance = ( (bid_depth - ask_depth) / (bid_depth + ask_depth) )
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                mid = (best_bid + best_ask) / 2
                spread_pct = ((best_ask - best_bid) / mid * 100 if mid else 0)
                ob_map[bucket] = {"bid_depth": bid_depth, "ask_depth": ask_depth, "imbalance": imbalance,"spread_pct": spread_pct}
    fetch_end  = datetime.now()
    fetch_elap = fetch_end - fetch_start
    return ob_map
def fetch_funding_data(symbol, start_ts, end_ts):
    # log("           getting funding data...")
    """ Fetch historical funding rates, Returns: funding_map[bucket] = funding_rate"""
    fetch_start = datetime.now()
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    url = (f"https://api.amberdata.com/markets/futures/funding-rates/{symbol}?exchange=binance&startDate={start_str}&endDate={end_str}")
    rl_sleep()
    resp = get_session().get(url, timeout=TIMEOUT,  )
    if resp.status_code != 200:
        print(f"⚠️ {symbol} Funding error → {resp.status_code}")
        return {}
    data = resp.json()["payload"]["data"]
    funding_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts:funding_map[int(ts // 60000)] = float(r.get("fundingRate", 0))
    fetch_end  = datetime.now()
    fetch_elap = fetch_end - fetch_start
    # log(f"                ", fetch_elap)
    return funding_map # funding rate
def fetch_long_short_data(symbol, start_ts, end_ts):
    # log("           getting S/L data...")
    """ Fetch historical long/short ratio, Returns: ls_map[bucket] = ratio """
    fetch_start = datetime.now()
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    url = (f"https://api.amberdata.com/markets/futures/long-short-ratio/{symbol}?exchange=binance&startDate={start_str}&endDate={end_str}")
    rl_sleep()
    resp = get_session().get(url, timeout=TIMEOUT,  )
    if resp.status_code != 200:
        print(f"⚠️ {symbol} L/S error → {resp.status_code}")
        return {}
    data = resp.json()["payload"]["data"]
    ls_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts:ls_map[int(ts // 60000)] = float(r.get("ratio", 0))
    fetch_end  = datetime.now()
    fetch_elap = fetch_end - fetch_start
    # log(f"                ", fetch_elap)
    return ls_map # long/short ratio
def fetch_trades_data(symbol, start_ts, end_ts):
    # log("           getting trades data...")
    fetch_start = datetime.now()
    """ Fetch taker buy/sell delta from Amberdata trades endpoint, Returns: taker_map[bucket] = net taker volume"""
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    url = (f"https://api.amberdata.com/markets/futures/trades/{symbol}?exchange=binance&startDate={start_str}&endDate={end_str}")
    for attempt in range(5):
        rl_sleep()
        resp = get_session().get(url, timeout=TIMEOUT)
        if resp.status_code == 200:
            break
        print(f"⚠️ {symbol} trades error → {resp.status_code} (attempt {attempt+1}/5)")
        if attempt < 4:
            time.sleep(2 ** (attempt + 1))
    else:
        print(f"❌ {symbol} trades failed after 5 attempts → skipping")
        return {}
    trades = resp.json()["payload"]["data"]
    taker_map = {}
    # ───────────── Trade Delta Calc ─────────────
    for t in trades:
        ts = normalize_ts(t.get("exchangeTimestamp") or t.get("timestamp"))
        if ts is None:continue
        bucket = int(ts // 60000)
        volume = float(t.get("volume", 0))
        is_buy = t.get("isBuySide", False)
        if bucket not in taker_map:taker_map[bucket] = 0
        if is_buy:taker_map[bucket] += volume
        else:taker_map[bucket] -= volume
    fetch_end = datetime.now()
    fetch_elap = fetch_end - fetch_start
    # log(f"                ", fetch_elap)
    return taker_map # buy trades, sell trades
def fetch_liquidations_data(symbol, start_ts, end_ts): # short liqs, long liqs
    # log("           getting liquidation data...")
    """ Fetch long/short liquidation volumes, Returns: liq_map[bucket] = { long: x, short: y } """
    fetch_start = datetime.now()
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    url = (f"https://api.amberdata.com/markets/futures/liquidations/{symbol}?exchange=binance&startDate={start_str}&endDate={end_str}")
    rl_sleep()
    resp = get_session().get(url, timeout=TIMEOUT,  )
    if resp.status_code != 200:
        print(f"⚠️ {symbol} liquidation error → {resp.status_code}")
        return {}
    liq_data = resp.json()["payload"]["data"]
    liq_map = {}
    # ───────────── Liquidation Aggregation ─────────────
    for r in liq_data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts is None:continue
        bucket = int(ts // 60000)
        liq_map[bucket] = {"long":  float(r.get("longLiquidations", 0)),"short": float(r.get("shortLiquidations", 0))}
    fetch_end = datetime.now()
    fetch_elap = fetch_end - fetch_start
    # log(f"                ", fetch_elap)
    return liq_map
def fetch_ticker_data(symbol, start_ts, end_ts):
    """ Historical ticker data: Spread, imbalance, basis (instances vary) """
    fetch_start = datetime.now()
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    url = (f"https://api.amberdata.com/markets/futures/tickers/{symbol}?exchange=binance&startDate={start_str}&endDate={end_str}")
    for attempt in range(5):
        rl_sleep()
        resp = get_session().get(url, timeout=TIMEOUT)
        if resp.status_code == 200:
            break
        print(f"⚠️ {symbol} ticker error → {resp.status_code} (attempt {attempt+1}/5)")
        if attempt < 4:
            time.sleep(2 ** (attempt + 1))
    else:
        print(f"❌ {symbol} ticker failed after 5 attempts → skipping")
        return {}
    data = resp.json()["payload"]["data"]
    ticker_map = {}
    for r in data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts is None:continue
        bucket = int(ts // 60000)

        bid = f(r.get("bid"))
        ask = f(r.get("ask"))
        mid = f(r.get("mid"))
        bid_vol = f(r.get("bidVolume"))
        ask_vol = f(r.get("askVolume"))

        # Spread
        spread_pct = ((ask - bid) / mid * 100 if mid else 0)
        imbalance = ((bid_vol - ask_vol) / (bid_vol + ask_vol) if (bid_vol + ask_vol) > 0 else 0)
        mark = float(r.get("markPrice", 0))
        index = float(r.get("indexPrice", 0))
        basis_pct = ( (mark - index) / index * 100 if index else 0)
        ticker_map[bucket] = {"spread_pct": spread_pct, "imbalance": imbalance, "basis_pct": basis_pct}
    fetch_end = datetime.now()
    fetch_elap = fetch_end - fetch_start
    return ticker_map #bids, asks, spread, imbalance, basis
def fetch_oi_data(symbol, start_ts, end_ts):
    """ Fetch:  Open Interest history, Returns: oi_map (dict[bucket] = open_interest) """
    fetch_start = datetime.now()
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    oi_url = (f"https://api.amberdata.com/markets/futures/open-interest/{symbol}?exchange=binance&startDate={start_str}&endDate={end_str}")
    rl_sleep()
    resp = get_session().get(oi_url, timeout=TIMEOUT,  )
    if resp.status_code != 200:
        print(f"⚠️ {symbol} OI error → {resp.text}")
        return {}
    oi_data = resp.json().get("payload", {}).get("data", [])
    oi_map = {}
    for r in oi_data:
        ts = normalize_ts(r.get("timestamp") or r.get("exchangeTimestamp"))
        if ts is None: continue
        oi_map[int(ts // 60000)] = float(r.get("value", 0))
    fetch_end  = datetime.now()
    fetch_elap = fetch_end - fetch_start
    return oi_map
def fetch_ohlcv_data(symbol, start_ts, end_ts):
    """ Fetch: Price close, Volume, Returns: px_map (dict[bucket] = {price, volume}) """
    fetch_start = datetime.now()
    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt   = datetime.fromtimestamp(end_ts   / 1000, tz=timezone.utc)
    start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
    end_str   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
    ohlcv_url = (f"https://api.amberdata.com/markets/futures/ohlcv/{symbol}?exchange=binance&timeInterval=minutes&startDate={start_str}&endDate={end_str}")
    rl_sleep()
    resp = get_session().get(ohlcv_url, timeout=TIMEOUT,  )
    if resp.status_code != 200:
        print(f"⚠️ {symbol} OHLCV error → {resp.text}")
        return {}
    px_data = resp.json().get("payload", {}).get("data", [])
    px_map = {}
    for r in px_data:
        ts_raw = r.get("exchangeTimestamp") or r.get("timestamp")
        ts = normalize_ts(ts_raw)
        if ts is None:continue
        bucket = int(ts // 60000)
        px_map[bucket] = {"price":  float(r.get("close", 0)),"volume": float(r.get("volume", 0))}
    fetch_end  = datetime.now()
    fetch_elap = fetch_end - fetch_start
    return px_map
def fetch_market_data(symbol, start_ts, end_ts, mcap_day=None):
    rows_out = []
    start_str_short = (datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d"))
    log(f"       FETCHES - {symbol}:")
    with ThreadPoolExecutor(max_workers=ENDPOINT_WORKERS) as ex:
        futures = {}
        for name, ep in ENDPOINTS.items():
            if ep["enabled"] and ep["fetch"] is not None:
                futures[name] = ex.submit(ep["fetch"], symbol, start_ts, end_ts)
        results = {}
        for name in ENDPOINTS.keys():
            if ENDPOINTS[name]["fetch"] is None:
                results[name] = {}  # daily join — not fetched here
            else:
                results[name] = safe_result(futures, name, {})

    log(f"       DATASETS - {start_str_short}:")
    for name, data in results.items():
        if isinstance(data, dict): log(f"           {name} rows:", len(data))
        elif isinstance(data, list): log(f"           {name} rows:", len(data))
    # ============================================================
    # MERGE  (spine: OHLCV — guaranteed complete 1-min series)
    # ============================================================
    last_vals = {
        "oi": 0,
        "price": 0,
        "volume": 0,
        "funding": 0,
        "ls": 0,
        "trades": 0,
        "spread": 0,
        "imbalance": 0,
        "basis": 0,
        "bid_depth": 0,
        "ask_depth": 0,
        "depth_imb": 0,
        "depth_spread": 0,
        "market_cap_usd": 0,
        "market_cap_rank": 0,
    }
    # Resolve base symbol for CoinGecko lookup (e.g. "BTCUSDT" → "BTC")
    base_sym = symbol.upper().replace("USDT", "").replace("BUSD", "").replace("USDC", "")
    if mcap_day and base_sym in mcap_day:
        mc = mcap_day[base_sym]
        last_vals["market_cap_usd"]  = mc.get("market_cap_usd", 0)
        last_vals["market_cap_rank"] = mc.get("market_cap_rank", 0)
    ohlcv_data = results.get("ohlcv", {})
    for bucket in sorted(ohlcv_data.keys()):
        dt = datetime.fromtimestamp(bucket * 60, tz=timezone.utc)
        row = [dt.strftime("%Y-%m-%d %H:%M:%S"), symbol]

        # ============================
        # OHLCV (spine)
        # ============================
        if ENDPOINTS["ohlcv"]["enabled"]:
            px = ohlcv_data[bucket]
            last_vals["price"]  = px.get("price", 0)
            last_vals["volume"] = px.get("volume", 0)
            row.extend([last_vals["price"], last_vals["volume"]])

        # ============================
        # OI (forward-filled map join)
        # ============================
        if ENDPOINTS["oi"]["enabled"]:
            val = results["oi"].get(bucket, last_vals["oi"])
            last_vals["oi"] = val
            row.append(val)

        # ============================
        # Funding
        # ============================
        if ENDPOINTS["funding"]["enabled"]:
            val = results["funding"].get(bucket, last_vals["funding"])
            last_vals["funding"] = val
            row.append(val)

        # ============================
        # Long / Short
        # ============================
        if ENDPOINTS["ls"]["enabled"]:
            val = results["ls"].get(bucket, last_vals["ls"])
            last_vals["ls"] = val
            row.append(val)

        # ============================
        # Trades
        # ============================
        if ENDPOINTS["trades"]["enabled"]:
            val = results["trades"].get(bucket, last_vals["trades"])
            last_vals["trades"] = val
            row.append(val)

        # ============================
        # Liquidations
        # ============================
        if ENDPOINTS["liquidations"]["enabled"]:
            liq = results["liquidations"].get(bucket, {"long": 0, "short": 0})
            row.extend([liq["long"], liq["short"]])

        # ============================
        # Orderbook
        # ============================
        if ENDPOINTS["orderbook"]["enabled"]:
            ob = results["orderbook"].get(bucket)
            if ob:
                last_vals["bid_depth"]   = ob["bid_depth"]
                last_vals["ask_depth"]   = ob["ask_depth"]
                last_vals["depth_imb"]   = ob["imbalance"]
                last_vals["depth_spread"] = ob["spread_pct"]
            row.extend([last_vals["bid_depth"], last_vals["ask_depth"], last_vals["depth_imb"], last_vals["depth_spread"]])

        # ============================
        # Ticker
        # ============================
        if ENDPOINTS["ticker"]["enabled"]:
            tick = results["ticker"].get(bucket)
            if tick:
                last_vals["spread"]    = tick["spread_pct"]
                last_vals["imbalance"] = tick["imbalance"]
                last_vals["basis"]     = tick["basis_pct"]
            row.extend([last_vals["spread"], last_vals["imbalance"], last_vals["basis"]])

        # ============================
        # Market Cap (CoinGecko daily)
        # ============================
        if ENDPOINTS["marketcap"]["enabled"]:
            row.extend([last_vals["market_cap_usd"], last_vals["market_cap_rank"]])

        rows_out.append(row)
    return rows_out

# ============================================================
# DYNAMIC BUILDS
# ============================================================
def build_column_indexes(header):
    col_index = {}
    for metric in SUMMARY_METRICS.values():
        col = metric["column"]
        if col in header: col_index[col] = header.index(col)
    return col_index
def build_header():
    header = ["timestamp_utc", "symbol"]
    for ep in ENDPOINTS.values():
        if ep["enabled"]: header.extend(ep["columns"])
    return header
def build_summary_header_basic():
    expected_header = build_header()
    header = ["symbol"]
    for metric in SUMMARY_METRICS.values():
        col = metric["column"]
        label = metric["label"]
        if col not in expected_header: continue
        if col == "volume":
            header.extend([f"{label}_pre_slope",f"{label}_post_slope"])
        else:
            header.extend([f"{label}_pre_%",f"{label}_post_%"])
    return header
def build_summary_header():
    return [

        # Keys
        "date",
        "symbol",

        # TARGET
        "price_post_pct",

        # OI
        "oi_pre_pct",
        "oi_pre_mean",
        "oi_pre_std",
        "oi_pre_min",
        "oi_pre_max",

        # PRICE
        "price_pre_pct",
        "price_pre_std",

        # VOLUME
        "volume_pre_sum",
        "volume_pre_mean",
        "volume_pre_spike",
        "volume_pre_trend_ratio",

        # FUNDING
        "funding_pre_mean",
        "funding_pre_std",

        # LONG/SHORT
        "ls_pre_mean",
        "ls_pre_last",
    ]

# ============================================================
# PRINTS
# ============================================================
def csv_exists_for_day(date_obj):
    """Check if CSV backup already exists for this day"""
    file_path = os.path.join(CSV_BACKUP_DIR,f"oi_{date_obj.strftime('%Y%m%d')}.csv")
    return os.path.exists(file_path)
def write_day_csv(date_obj, header, rows):
    """Write full day dataset to CSV backup"""
    if not CSV_BACKUP_ENABLED:return
    os.makedirs(CSV_BACKUP_DIR, exist_ok=True)
    file_path = os.path.join(CSV_BACKUP_DIR,f"oi_{date_obj.strftime('%Y%m%d')}.csv")
    print(f"\n💾 Writing CSV backup → {file_path}")
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"✅ CSV saved ({len(rows):,} rows)")
def append_day_csv(date_obj, header, rows, write_header=False):
    if not CSV_BACKUP_ENABLED: return
    os.makedirs(CSV_BACKUP_DIR, exist_ok=True)
    file_path = os.path.join(CSV_BACKUP_DIR,f"oi_{date_obj.strftime('%Y%m%d')}.csv")
    mode = "a" if os.path.exists(file_path) else "w"
    with open(file_path, mode, newline="") as f:
        writer = csv.writer(f)
        if write_header or mode == "w": writer.writerow(header)
        writer.writerows(rows)
        f.flush()
        os.fsync(f.fileno())
    size_mb = os.path.getsize(file_path) / (1024**2)
    print(f"💾 CSV chunk written → {len(rows):,} rows | {size_mb:.2f} MB")
def write_summary_csv(date_obj, header, summary_rows):
    """Write summary CSV backup"""
    if not CSV_BACKUP_ENABLED:return
    os.makedirs(CSV_BACKUP_DIR, exist_ok=True)
    file_path = os.path.join(CSV_BACKUP_DIR,f"oi_summary_{date_obj.strftime('%Y%m%d')}.csv")
    print(f"\n💾 Writing SUMMARY CSV → {file_path}")
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(summary_rows)
    print(f"✅ Summary CSV saved ({len(summary_rows):,} symbols)")
def print_runtime():
    end_time = time.time()
    total_seconds = end_time - SCRIPT_START_TIME
    minutes = total_seconds / 60
    hours   = minutes / 60
    print("\n========================================")
    print("⏱ SCRIPT RUNTIME")
    print("========================================")
    print(f"Seconds : {total_seconds:,.2f}")
    print(f"Minutes : {minutes:,.2f}")
    print(f"Hours   : {hours:,.2f}")
    print("========================================\n")
    return 1
def print_day_runtime(day_label, start_time):
    end_time = time.time()
    seconds = end_time - start_time
    minutes = seconds / 60
    hours   = minutes / 60
    print("\n----------------------------------------")
    print(f"⏱ Runtime for {day_label}")
    print("----------------------------------------")
    print(f"Seconds : {seconds:,.2f}")
    print(f"Minutes : {minutes:,.2f}")
    print(f"Hours   : {hours:,.2f}")
    print("----------------------------------------\n")
    return 1
def create_daily_summary(ws_raw, date_str):
    print(f"\n📊 Building summary for {date_str}...")
    print("⏳    Waiting for Sheets sync...")
    time.sleep(5)
    rows = ws_raw.get_all_values()
    header = rows[0]
    data   = rows[1:]
    ts_i  = header.index("timestamp_utc")
    sym_i = header.index("symbol")
    col_index = build_column_indexes(header)
    hist = {}

    for r in data:
        if not r[ts_i].startswith(date_str): continue
        ts  = datetime.strptime(r[ts_i], "%Y-%m-%d %H:%M:%S")
        sym = r[sym_i]
        row_data = {"ts": ts}
        for col, idx in col_index.items():
            try: row_data[col] = float(r[idx])
            except: row_data[col] = 0
        hist.setdefault(sym, []).append(row_data)

    pre_start = datetime.strptime(f"{date_str} 00:00:00","%Y-%m-%d %H:%M:%S")
    pre_end   = datetime.strptime(f"{date_str} 06:00:00","%Y-%m-%d %H:%M:%S")
    post_end  = datetime.strptime(f"{date_str} 23:59:00","%Y-%m-%d %H:%M:%S")

    summary_rows = []
    for sym, rows in hist.items():
        rows.sort(key=lambda x: x["ts"])
        def nearest(target): return min(rows, key=lambda x: abs(x["ts"] - target))
        r0  = nearest(pre_start)
        r6  = nearest(pre_end)
        r24 = nearest(post_end)
        row_out = [sym]
        for metric in SUMMARY_METRICS.values():
            col = metric["column"]
            if col not in r0:continue   # endpoint disabled
            row_out.extend([pct(r0[col], r6[col]),pct(r6[col], r24[col])])
        summary_rows.append(row_out)

    print(f"✅ Summary computed for {len(summary_rows)} symbols")
    return summary_rows
def create_daily_summary_from_rows_basic(rows, header, date_str):
    print(f"\n📊 Building summary for {date_str} (memory mode)...")
    ts_i  = header.index("timestamp_utc")
    sym_i = header.index("symbol")
    col_index = build_column_indexes(header)
    hist = {}
    for r in rows:
        if not r[ts_i].startswith(date_str): continue
        ts  = datetime.strptime(r[ts_i], "%Y-%m-%d %H:%M:%S")
        sym = r[sym_i]
        row_data = {"ts": ts}
        for col, idx in col_index.items():
            try: row_data[col] = float(r[idx])
            except: row_data[col] = 0
        hist.setdefault(sym, []).append(row_data)
    pre_start = datetime.strptime(f"{date_str} 00:00:00","%Y-%m-%d %H:%M:%S")
    pre_end = datetime.strptime(f"{date_str} 06:00:00","%Y-%m-%d %H:%M:%S")
    post_end = datetime.strptime(f"{date_str} 23:59:00","%Y-%m-%d %H:%M:%S")
    summary_rows = []
    for sym, rows_sym in hist.items():
        rows_sym.sort(key=lambda x: x["ts"])
        r0  = first_after(rows_sym, pre_start)
        r6  = first_after(rows_sym, pre_end)
        r24 = first_after(rows_sym, post_end)
        row_out = [sym]
        for metric in SUMMARY_METRICS.values():
            col = metric["column"]
            if col not in r0: continue
            if col == "volume":
                pre_series = [ r[col] for r in rows_sym if pre_start <= r["ts"] <= pre_end ]
                post_series = [ r[col] for r in rows_sym if pre_end <= r["ts"] <= post_end ]
                pre_slope  = compute_slope(pre_series)
                post_slope = compute_slope(post_series)
                row_out.extend([r5(pre_slope), r5(post_slope)])
            else: row_out.extend([ r5(pct(r0[col], r6[col])), r5(pct(r6[col], r24[col])) ])
        summary_rows.append(row_out)
    print(f"✅ Summary computed for {len(summary_rows)} symbols")
    return summary_rows
def create_daily_summary_from_rows(rows, header, date_str):

    print(f"\n📊 Building training summary for {date_str}...")

    ts_i  = header.index("timestamp_utc")
    sym_i = header.index("symbol")
    col_index = build_column_indexes(header)
    hist = {}
    for r in rows:
        if not r[ts_i].startswith(date_str): continue
        ts  = datetime.strptime(r[ts_i], "%Y-%m-%d %H:%M:%S")
        sym = r[sym_i]
        row_data = {"ts": ts}
        for col, idx in col_index.items():
            try: row_data[col] = float(r[idx])
            except: row_data[col] = 0.0
        hist.setdefault(sym, []).append(row_data)

    # ─────────────────────────────
    # Time anchors
    # ─────────────────────────────
    pre_start = datetime.strptime(f"{date_str} 00:00:00","%Y-%m-%d %H:%M:%S")
    pre_end   = datetime.strptime(f"{date_str} 06:00:00","%Y-%m-%d %H:%M:%S")
    post_end  = datetime.strptime(f"{date_str} 23:59:00","%Y-%m-%d %H:%M:%S")

    summary_rows = []

    for sym, rows_sym in hist.items():

        rows_sym.sort(key=lambda x: x["ts"])

        r0  = first_after(rows_sym, pre_start)
        r6  = first_after(rows_sym, pre_end)
        r24 = first_after(rows_sym, post_end)

        pre_rows = [r for r in rows_sym if pre_start <= r["ts"] <= pre_end]

        if not pre_rows: continue

        # ───────────── Helpers ─────────────
        def series(col): return [r[col] for r in pre_rows if col in r]
        def mean(x): return sum(x)/len(x) if x else 0

        def std(x):
            if len(x) < 2: return 0
            m = mean(x)
            return (sum((v-m)**2 for v in x)/len(x))**0.5

        # ───────────── Series ─────────────
        oi_s   = series("open_interest")
        px_s   = series("price")
        vol_s  = series("volume")
        fr_s   = series("funding_rate")
        ls_s   = series("long_short_ratio")

        # ───────────── Volume features ─────────────
        vol_mean = mean(vol_s)
        vol_spike = (max(vol_s) / vol_mean) if vol_mean else 0

        if len(vol_s) >= 12:
            first = mean(vol_s[:12])
            last  = mean(vol_s[-12:])
            vol_trend = last / first if first else 0
        else:
            vol_trend = 0

        # ───────────── Target ─────────────
        price_post_pct = pct(
            r6.get("price",0),
            r24.get("price",0)
        )

        # ───────────── Row build ─────────────
        row_out = [

            # Keys
            date_str,
            sym,

            # TARGET
            r5(price_post_pct),

            # OI
            r5(pct(r0["open_interest"], r6["open_interest"])),
            r5(mean(oi_s)),
            r5(std(oi_s)),
            r5(min(oi_s) if oi_s else 0),
            r5(max(oi_s) if oi_s else 0),

            # PRICE
            r5(pct(r0["price"], r6["price"])),
            r5(std(px_s)),

            # VOLUME
            r5(sum(vol_s)),
            r5(vol_mean),
            r5(vol_spike),
            r5(vol_trend),

            # FUNDING
            r5(mean(fr_s)),
            r5(std(fr_s)),

            # LONG / SHORT
            r5(mean(ls_s)),
            r5(ls_s[-1] if ls_s else 0),
        ]

        summary_rows.append(row_out)

    print(f"✅ Training rows built → {len(summary_rows)}")

    return summary_rows

def write_summary_sheet(date_str, summary_rows):
    gc = gspread.authorize(Credentials.from_authorized_user_file("token.json",["https://www.googleapis.com/auth/spreadsheets"]))
    sh = gc.open_by_key(SHEET_ID)
    tab_name = f"SUMMARY_{date_str.replace('-', '')}"
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
    except: ws = sh.add_worksheet(title=tab_name,rows=1000,cols=10)
    total_symbols = len(summary_rows)
    print(f"🔢 Total symbols in summary: {total_symbols}")
    header = build_summary_header()
    ws.append_row(header)
    ws.append_rows(summary_rows)
    ws.append_row([])  # spacer
    ws.append_row([f"SUMMARY DATE: {date_str}"])
    ws.append_row([f"TOTAL SYMBOLS: {total_symbols}"])
    ws.append_row([])  # spacer
    print(f"📄 Summary written → {tab_name}")

# ============================================================
# REGISTRY
# ============================================================
ENDPOINTS = {
    "ohlcv": {
        "enabled": FETCH_PRICE,
        "fetch": fetch_ohlcv_data,
        "columns": ["price", "volume"],
        "type": "map"
    },
    "oi": {
        "enabled": FETCH_OI,
        "fetch": fetch_oi_data,
        "columns": ["open_interest"],
        "type": "map"
    },
    "funding": {
        "enabled": FETCH_FUNDING,
        "fetch": fetch_funding_data,
        "columns": ["funding_rate"],
        "type": "map"
    },
    "ls": {
        "enabled": FETCH_LS,
        "fetch": fetch_long_short_data,
        "columns": ["long_short_ratio"],
        "type": "map"
    },
    "trades": {
        "enabled": FETCH_TRADES,   # 🔁 toggle here
        "fetch": fetch_trades_data,
        "columns": ["trade_delta"],
        "type": "map"
    },
    "liquidations": {
        "enabled": FETCH_LIQUIDATIONS,   # 🔁 toggle here
        "fetch": fetch_liquidations_data,
        "columns": ["long_liqs", "short_liqs"],
        "type": "map"
    },
    "orderbook": {
        "enabled": FETCH_ORDERBOOK,
        "fetch": fetch_orderbook_snapshots_batch,
        "columns": [
            "last_bid_depth",
            "last_ask_depth",
            "last_depth_imbalance",
            "last_spread_pct"
        ],
        "type": "map"
    },
    "ticker": {
        "enabled": FETCH_TICKER,
        "fetch": fetch_ticker_data,
        "columns": [
            "spread_pct",
            "bid_ask_imbalance",
            "basis_pct"
        ],
        "type": "map"
    },
    "marketcap": {
        "enabled": FETCH_MARKETCAP,
        "fetch": None,   # not fetched live — loaded from CoinGecko parquet via load_marketcap_day()
        "columns": ["market_cap_usd", "market_cap_rank"],
        "type": "daily"  # daily granularity, constant across all minute rows for the day
    }
}
SUMMARY_METRICS = {
    "price": {
        "column": "price",
        "label": "price"
    },
    "volume": {
        "column": "volume",
        "label": "volume"
    },
    "open_interest": {
        "column": "open_interest",
        "label": "oi"
    },
    "funding_rate": {
        "column": "funding_rate",
        "label": "funding"
    },
    "long_short_ratio": {
        "column": "long_short_ratio",
        "label": "ls"
    },
    "trade_delta": {
        "column": "trade_delta",
        "label": "taker"
    },
    "long_liqs": {
        "column": "long_liqs",
        "label": "longliq"
    },
    "short_liqs": {
        "column": "short_liqs",
        "label": "shortliq"
    },
    "spread_pct": {
        "column": "spread_pct",
        "label": "spread"
    },
    "bid_ask_imbalance": {
        "column": "bid_ask_imbalance",
        "label": "imbalance"
    },
    "basis_pct": {
        "column": "basis_pct",
        "label": "basis"
    }
}

# ============================================================
# MAIN (Parrallelized)
# ============================================================
def get_session():
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update({"x-api-key": AMBER_API_KEY,"Accept-Encoding": "gzip, deflate, br"})
        adapter = HTTPAdapter(pool_connections=200,pool_maxsize=200,max_retries=3)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        thread_local.session = s
    return thread_local.session
def process_symbol(symb, start_ts, end_ts, mcap_day=None):
    """ Worker wrapper for parallel symbol processing """
    try:
        start = datetime.now()
        rows = fetch_market_data(symb, start_ts, end_ts, mcap_day=mcap_day)
        elap = datetime.now() - start
        return {"symbol": symb,"rows": rows,"elapsed": elap,"success": True}
    except Exception as e: return {"symbol": symb,"rows": [],"elapsed": 0,"success": False,"error": str(e)}
def main(start_date, end_date, job_id=None, triggered_by='cli', run_tag=None):
    if job_id is None:
        job_id = job_create(start_date.date(), end_date.date(), triggered_by, run_tag)

    try:
        for day in daterange(start_date, end_date):
            ws = None
            if PRINT_RAW_SHEETS:ws = connect_daily_sheet(day)
            expected_header = build_header()
            if PRINT_RAW_SHEETS:
                current = ws.row_values(1)
                if not current:ws.append_row(expected_header)
                elif current != expected_header:
                    print("⚠️ Header mismatch — clearing and rewriting")
                    ws.clear()
                    ws.append_row(expected_header)
            day_start_time = time.time()
            print(f"\n📅 {day.date()}")

            if AUTO_RESUME and csv_exists_for_day(day):
                print(f"\n⏭ Skipping {day.date()} → CSV already exists")
                continue

            day_utc = datetime(year=day.year,month=day.month,day=day.day,tzinfo=timezone.utc)
            start_ts = int(day_utc.timestamp() * 1000)
            end_ts = int((day_utc + timedelta(days=1) - timedelta(seconds=1)).timestamp() * 1000)

            print("   🔎 Fetching instrument universe for date...")
            symbols = get_symbols_for_date(day)
            total_symbs = len(symbols)
            job_set_total(job_id, total_symbs)
            print(f"   ✅ {total_symbs} symbols active\n")
            print("  Batch write mode ENABLED (streaming CSV)")

            # Load CoinGecko market cap for this day once — shared across all symbols
            mcap_day = load_marketcap_day(day) if FETCH_MARKETCAP else {}

            all_rows_day = []
            script_start = datetime.now()
            print("  script_start:", script_start, "\n")

            print(f"\n⚡ Parallel mode ENABLED ({MAX_WORKERS} workers)\n")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(process_symbol,symb,start_ts,end_ts,mcap_day): symb for symb in symbols}
                completed = 0
                for future in as_completed(futures):
                    result = future.result()
                    completed += 1
                    symb = result["symbol"]
                    rows = result["rows"]
                    elap = result["elapsed"]
                    progress = round((completed / total_symbs) * 100,2)
                    if result["success"]:
                        print(f"   ✅ {symb} done → {elap} ({progress}%)")
                        all_rows_day.extend(rows)
                        job_increment(job_id, len(rows))
                        if len(all_rows_day) >= CSV_FLUSH_CHUNK:
                            file_path = os.path.join(CSV_BACKUP_DIR, f"oi_{day.strftime('%Y%m%d')}.csv")
                            append_day_csv(day,expected_header,all_rows_day,write_header=not os.path.exists(file_path))
                            all_rows_day.clear()   # frees RAM
                    else: print(f"   ❌ {symb} failed → {result['error']}")

                    total_elap = datetime.now() - script_start
                    avg_cycle = (total_elap / completed if completed else timedelta(0))
                    rem_syms = total_symbs - completed
                    rem_time = rem_syms * avg_cycle
                    # print(f"                total elap: {total_elap}")
                    # print(f"                remaining: {rem_time}\n")

            if all_rows_day:
                print(f"\n💾 Final CSV flush → {len(all_rows_day):,} rows")
                file_path = os.path.join(CSV_BACKUP_DIR, f"oi_{day.strftime('%Y%m%d')}.csv")
                append_day_csv(day,expected_header,all_rows_day,write_header=not os.path.exists(file_path))
                all_rows_day.clear()

            if PRINT_SUMMARY_SHEETS:
                date_str = day.strftime("%Y-%m-%d")
                print("\n📊 Building summary from CSV...")
                csv_path = os.path.join(CSV_BACKUP_DIR,f"oi_{day.strftime('%Y%m%d')}.csv")
                if not os.path.exists(csv_path):
                    print("⚠️ CSV missing — summary skipped")
                    continue
                rows = []
                with open(csv_path, newline="") as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    for r in reader:rows.append(r)
                summary = create_daily_summary_from_rows(rows,header,date_str)
                summary_header = build_summary_header()
                write_summary_csv(day,summary_header,summary)

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
    end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc) \
               if args.end else start_date
    print(f"\n📆 Backfill Range: {start_date.date()} → {end_date.date()}")
    main(start_date, end_date,
         job_id=args.job_id,
         triggered_by=args.triggered_by,
         run_tag=args.run_tag)

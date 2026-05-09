#!/usr/bin/env python3
"""
binetl.py — direct-Binance public-REST ETL.

Drop-in replacement for metl.py. Same CLI args, same DB writes (source_id=1),
same CSV format (so pipeline/db/load_csv_to_futures_1m.py works unchanged).

Why: Amberdata subscription expiring; we collect the same 6 series directly
from Binance public REST endpoints (no API key needed). Liquidations are
dropped — Binance pulled the public REST endpoint in 2021. Existing
historical liquidations rows are preserved; new rows have NULL.

Endpoint mapping:
  Price (klines 1m)        /fapi/v1/klines                              weight=2
  OI 5m → ffill to 1m      /futures/data/openInterestHist               weight=1
  Funding rates            /fapi/v1/fundingRate                         weight=1
  Top L/S 5m → ffill 1m    /futures/data/topLongShortAccountRatio       weight=1
  Taker buy/sell 5m → 1m   /futures/data/takerlongshortRatio            weight=1
  24h ticker (per-symbol)  /fapi/v1/ticker/24hr (with symbol param)     weight=1

Rate-limit budget per symbol per day: ~7 weight.
  ~600 symbols × 7 = ~4,200 weight/day total.
  IP cap: 2400 weight/min — comfortably fits in <3 min wall time at
  concurrency 8 (peak ~150 weight/sec well under 40/sec cap).

Originally tried per-trade aggTrades for 1m signed taker delta — even
on normal-volume hours, BTCUSDT exceeded the 50-page (50K trades)
pagination cap, and the runtime ballooned to ~17h for a full day.
takerlongshortRatio gives 5m aggregated buy/sell volumes — same
forward-fill pattern as OI/LS, sum-over-day matches the per-trade
total within rounding.

Shadow / validation:
  --csv-suffix VALUE writes oi_YYYYMMDD_VALUE.csv instead of oi_YYYYMMDD.csv,
  letting binetl run alongside metl on the same date without overwriting.
  The validation tool (_etl_compare.py) diffs the two CSVs.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

# DB connection helper for compiler_jobs tracking
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn
from pipeline.db.load_csv_to_futures_1m import load_csv as load_csv_to_futures_1m

# ─── Constants ──────────────────────────────────────────────────────────────

SCRIPT_START_TIME = time.time()

# Binance endpoints (public, no auth)
FAPI_BASE = "https://fapi.binance.com"   # USDT-M perpetuals
DAPI_BASE = "https://dapi.binance.com"   # COIN-M perpetuals

# Rate-limit guardrails. Binance IP cap is 2400 weight/min on fapi.
# Concurrency 4 keeps the burst-rate moderate — with 8 workers, all 8
# would sleep simultaneously when the cap was hit, creating a 60s
# stall every cycle. 4 workers is a smoother throughput curve.
MAX_WORKERS = 4
RATE_LIMIT_THRESHOLD = 1900     # back off when X-MBX-USED-WEIGHT-1M ≥ this
RATE_LIMIT_HARD_CAP = 2400      # what Binance enforces
RATE_LIMIT_BACKOFF_SEC = 5      # short sleep on throttle hit; rolling
                                # 1-min window will dip below cap within
                                # this even under concurrent load
TIMEOUT = 30
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 1.5

# Output dirs. Mirrors metl's `/mnt/quant-data/raw/amberdata/csv/` pattern
# but namespaced under `binance/` so binetl-produced CSVs are visually
# distinguishable from metl-produced ones during the cutover window.
CSV_BACKUP_DIR = os.environ.get("BINETL_CSV_DIR", "/mnt/quant-data/raw/binance/csv")
LOG_DIR = "/mnt/quant-data/logs/compiler"

# CSV flush frequency (rows per flush — same as metl)
CSV_FLUSH_CHUNK = 100_000

# Toggles — mirror metl's enabled set, except liquidations / orderbook / mcap
FETCH_PRICE        = True
FETCH_OI           = True
FETCH_FUNDING      = True
FETCH_LS           = True
FETCH_TICKER       = True
FETCH_TRADES       = True
FETCH_LIQUIDATIONS = False  # No public REST equivalent on Binance (since 2021)
FETCH_ORDERBOOK    = False  # Disabled in metl too — no historical depth on Binance
FETCH_MARKETCAP    = False  # CoinGecko path, not relevant here

# CSV header: must match metl's build_header() output exactly so
# load_csv_to_futures_1m maps columns identically. Liquidation columns
# are present (empty values) so the CSV column order is stable.
CSV_HEADER = [
    "timestamp_utc", "symbol",
    "price", "volume",            # ohlcv
    "open_interest",              # oi
    "funding_rate",               # funding
    "long_short_ratio",           # ls
    "trade_delta",                # trades
    "long_liqs", "short_liqs",    # liquidations (always empty for binetl)
    "spread_pct", "bid_ask_imbalance", "basis_pct",  # ticker
]

# ─── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [binetl] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("binetl")

# ─── Thread-local HTTP session ──────────────────────────────────────────────

_thread_local = threading.local()
_weight_lock = threading.Lock()
_current_weight = {"value": 0, "ts": 0.0}  # most recent X-MBX-USED-WEIGHT-1M

def get_session():
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"Accept-Encoding": "gzip, deflate, br"})
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0)
        s.mount("https://", adapter)
        _thread_local.session = s
    return s

def _maybe_throttle():
    """Block briefly when nearing the per-IP weight cap. Reads
    X-MBX-USED-WEIGHT-1M from the last response (refreshed on every
    call). Short sleep + retry — Binance's window is rolling 1-min,
    so weight can age out within a few seconds under concurrent load.
    """
    with _weight_lock:
        weight = _current_weight["value"]
    if weight >= RATE_LIMIT_THRESHOLD:
        log.debug(f"weight {weight} ≥ {RATE_LIMIT_THRESHOLD}, sleeping {RATE_LIMIT_BACKOFF_SEC}s")
        time.sleep(RATE_LIMIT_BACKOFF_SEC)

def _binance_get(url: str, params: dict | None = None, base: str = FAPI_BASE) -> dict | list:
    """GET with retry + rate-limit awareness. Returns parsed JSON or raises."""
    full_url = base + url
    for attempt in range(MAX_RETRIES):
        _maybe_throttle()
        try:
            r = get_session().get(full_url, params=params, timeout=TIMEOUT)
            # Update cached weight
            w = r.headers.get("X-MBX-USED-WEIGHT-1M")
            if w is not None:
                with _weight_lock:
                    _current_weight["value"] = int(w)
                    _current_weight["ts"] = time.time()
            if r.status_code == 429 or r.status_code == 418:
                # Hard rate-limit hit. Back off via Retry-After if present.
                retry_after = int(r.headers.get("Retry-After", "30"))
                log.error(f"429/418 {full_url} → sleeping {retry_after}s")
                time.sleep(retry_after)
                continue
            if r.status_code >= 500:
                wait = RETRY_BACKOFF_BASE ** attempt
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            wait = RETRY_BACKOFF_BASE ** attempt
            log.warning(f"{full_url} attempt {attempt+1}/{MAX_RETRIES} → {e}; retry in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"{full_url} failed after {MAX_RETRIES} attempts")

# ─── Universe ───────────────────────────────────────────────────────────────

def get_symbols_for_date(_date_obj) -> list[str]:
    """Symbol universe = (metl-historical last 30 days) ∩ (Binance currently TRADING).

    Sourcing from metl-historical preserves continuity — binetl will only
    collect symbols that already appear in market.futures_1m. New Binance
    products (equity perps, commodity perps) that metl never tracked are
    excluded by intersection. Drift events (delistings) are logged.
    """
    # metl-historical
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT s.binance_id
          FROM market.futures_1m f
          JOIN market.symbols s ON s.symbol_id = f.symbol_id
         WHERE f.timestamp_utc >= NOW() - INTERVAL '30 days'
           AND s.binance_id IS NOT NULL
    """)
    metl_set = {row[0] for row in cur.fetchall()}
    conn.close()

    # Binance current — fapi (USDT-M / USDC-M) only.
    #
    # COIN-margined perps (dapi, *USD_PERP) are intentionally excluded:
    # dapi /klines returns volume in CONTRACTS (a contract represents a
    # fixed USD notional, varies per symbol). Amberdata converted these
    # to base-asset volume automatically; binetl would need a contract-
    # size lookup to match. The strategy framework is USDT-M only per
    # CLAUDE.md, so COIN-M was auxiliary historical data — we stop
    # adding new rows for those symbols rather than write incorrect
    # volume values. Existing rows in market.futures_1m stay unchanged.
    fapi = _binance_get("/fapi/v1/exchangeInfo")
    binance_set = {s["symbol"] for s in fapi["symbols"] if s.get("status") == "TRADING"}

    intersection = sorted(metl_set & binance_set)
    metl_only = sorted(metl_set - binance_set)
    binance_only = sorted(binance_set - metl_set)

    log.info(f"universe: metl-historical={len(metl_set)} binance-active={len(binance_set)} "
             f"intersection={len(intersection)}")
    if metl_only:
        log.warning(f"metl-only (delisted on Binance, dropped): {len(metl_only)}: {metl_only[:10]}...")
    if binance_only:
        log.info(f"binance-only (new since metl, not collecting): {len(binance_only)}: {binance_only[:10]}...")
    return intersection

# ─── Endpoint helpers ───────────────────────────────────────────────────────

def _is_coin_margined(symbol: str) -> bool:
    """COIN-M symbols on Binance match the pattern *USD_PERP or *USD_<DATE>.
    They live on dapi (delivery API) instead of fapi (futures API)."""
    return "USD_" in symbol

def _api_base_for(symbol: str) -> str:
    return DAPI_BASE if _is_coin_margined(symbol) else FAPI_BASE

# ─── Fetchers ───────────────────────────────────────────────────────────────

def fetch_klines_1m(symbol: str, start_ts: int, end_ts: int) -> dict[int, tuple[float, float]]:
    """Returns: {minute_ms: (close_price, base_volume)}.

    /klines returns up to 1500 rows per call. A full day = 1440 minutes —
    one call covers it.
    """
    base = _api_base_for(symbol)
    out: dict[int, tuple[float, float]] = {}
    cursor = start_ts
    while cursor <= end_ts:
        data = _binance_get("/fapi/v1/klines" if base == FAPI_BASE else "/dapi/v1/klines", {
            "symbol": symbol, "interval": "1m",
            "startTime": cursor, "endTime": end_ts, "limit": 1500,
        }, base=base)
        if not data:
            break
        for row in data:
            # row = [openTime, open, high, low, close, volume, closeTime, ...]
            ts = int(row[0])
            close = float(row[4])
            volume = float(row[5])
            out[ts] = (close, volume)
        last_ts = int(data[-1][0])
        if len(data) < 1500 or last_ts >= end_ts:
            break
        cursor = last_ts + 60_000  # advance past the last minute we got
    return out

def fetch_oi_5m(symbol: str, start_ts: int, end_ts: int) -> dict[int, float]:
    """Returns: {minute_ms: open_interest}, forward-filled from 5m to 1m.

    /futures/data/openInterestHist:
      - period min = 5m (no 1m option)
      - max 500 rows per call (5m × 500 = ~41hr — one call covers a day)
      - Window cap = 30 days
      - dapi has /futures/data/openInterestHist too with same shape
    """
    base = _api_base_for(symbol)
    path = "/futures/data/openInterestHist" if base == FAPI_BASE else "/futures/data/openInterestHist"
    # Note: dapi uses "pair" + "contractType", not "symbol", but for *_PERP
    # symbols passing symbol works on the redirect. If a coin-margined
    # quarterly is ever in the universe, we'd need to split on contract.
    # Today's universe contains BTCUSD_PERP-style perps which work via symbol.
    params = {
        "symbol": symbol, "period": "5m",
        "startTime": start_ts, "endTime": end_ts, "limit": 500,
    }
    if base == DAPI_BASE:
        # dapi requires pair + contractType for the OI hist endpoint
        if symbol.endswith("_PERP"):
            params = {
                "pair": symbol.replace("_PERP", ""),
                "contractType": "PERPETUAL",
                "period": "5m",
                "startTime": start_ts, "endTime": end_ts, "limit": 500,
            }
        else:
            # Quarterly future (BTCUSD_260626 etc.) — skip OI for now
            return {}
    data = _binance_get(path, params, base=base)
    if not data:
        return {}
    # Sort by timestamp, then forward-fill to 1m by carrying each 5m value
    # forward across the next 5 minute slots.
    sorted_pts = sorted(data, key=lambda d: int(d.get("timestamp", 0)))
    out: dict[int, float] = {}
    for entry in sorted_pts:
        ts5 = int(entry["timestamp"])
        oi_field = "sumOpenInterest" if base == FAPI_BASE else "sumOpenInterest"
        try:
            oi = float(entry.get(oi_field, 0) or 0)
        except (ValueError, TypeError):
            continue
        for i in range(5):
            slot = ts5 + i * 60_000
            if start_ts <= slot <= end_ts:
                out[slot] = oi
    return out

def fetch_funding_rates(symbol: str, start_ts: int, end_ts: int) -> dict[int, float]:
    """Returns: {minute_ms: funding_rate}. Funding rate at funding-event
    timestamps only (00:00, 08:00, 16:00 UTC); other minutes have no entry.
    """
    base = _api_base_for(symbol)
    path = "/fapi/v1/fundingRate" if base == FAPI_BASE else "/dapi/v1/fundingRate"
    data = _binance_get(path, {
        "symbol": symbol, "startTime": start_ts, "endTime": end_ts, "limit": 1000,
    }, base=base)
    out: dict[int, float] = {}
    for entry in (data or []):
        ts = int(entry["fundingTime"])
        # Snap to the nearest minute boundary
        ts_min = ts - (ts % 60_000)
        try:
            out[ts_min] = float(entry["fundingRate"])
        except (ValueError, TypeError):
            continue
    return out

def fetch_long_short_5m(symbol: str, start_ts: int, end_ts: int) -> dict[int, float]:
    """Returns: {minute_ms: long_short_ratio}, forward-filled 5m→1m."""
    if _is_coin_margined(symbol):
        return {}  # dapi doesn't expose this endpoint
    data = _binance_get("/futures/data/topLongShortAccountRatio", {
        "symbol": symbol, "period": "5m",
        "startTime": start_ts, "endTime": end_ts, "limit": 500,
    })
    if not data:
        return {}
    out: dict[int, float] = {}
    for entry in sorted(data, key=lambda d: int(d.get("timestamp", 0))):
        ts5 = int(entry["timestamp"])
        try:
            lsr = float(entry["longShortRatio"])
        except (ValueError, TypeError, KeyError):
            continue
        for i in range(5):
            slot = ts5 + i * 60_000
            if start_ts <= slot <= end_ts:
                out[slot] = lsr
    return out

def fetch_ticker_snapshot(symbol: str, start_ts: int, end_ts: int) -> dict[int, dict]:
    """Returns: {minute_ms: {"spread_pct": ..., "bid_ask_imbalance": ...}}.

    /ticker/24hr is a snapshot endpoint — there's no historical
    intra-day ticker. We replicate the same value across all minutes of
    the day. Good-enough since metl's `spread_pct`/`basis_pct` columns
    were computed similarly from end-of-day Amberdata snapshots.
    """
    base = _api_base_for(symbol)
    path = "/fapi/v1/ticker/24hr" if base == FAPI_BASE else "/dapi/v1/ticker/24hr"
    data = _binance_get(path, {"symbol": symbol}, base=base)
    if isinstance(data, list):
        if not data:
            return {}
        data = data[0]
    try:
        last = float(data.get("lastPrice", 0) or 0)
        weighted = float(data.get("weightedAvgPrice", 0) or 0)
        if last and weighted:
            spread_pct = abs(last - weighted) / weighted * 100
        else:
            spread_pct = 0.0
        # Binance doesn't surface bid/ask in ticker24h consistently; leave
        # bid_ask_imbalance = 0. basis_pct also requires a spot reference,
        # which we don't have without a separate spot price call. Set 0.
        out_row = {
            "spread_pct": spread_pct,
            "bid_ask_imbalance": 0.0,
            "basis_pct": 0.0,
        }
    except Exception:
        out_row = {"spread_pct": 0.0, "bid_ask_imbalance": 0.0, "basis_pct": 0.0}
    out: dict[int, dict] = {}
    cursor = start_ts
    while cursor <= end_ts:
        out[cursor] = out_row
        cursor += 60_000
    return out

def fetch_taker_delta(symbol: str, start_ts: int, end_ts: int) -> dict[int, float]:
    """Returns: {minute_ms: signed_taker_volume}, forward-filled 5m→1m.

    Signed = buyVol - sellVol (reported by Binance as taker-aggressive
    buy and sell volumes in 5-min buckets). Within each 5m bucket the
    SAME signed value is forward-filled to all 5 minute slots — same
    pattern as OI and L/S ratio.

    Earlier per-trade aggTrades approach was abandoned: BTCUSDT in 1 hour
    can exceed 50,000 trades (the practical pagination cap), causing
    truncation. takerlongshortRatio at 5m granularity gives the same
    daily-sum signal and fits the rate-limit budget.

    Coin-margined: dapi doesn't expose this endpoint — return empty
    (matches the L/S behavior).
    """
    if _is_coin_margined(symbol):
        return {}
    data = _binance_get("/futures/data/takerlongshortRatio", {
        "symbol": symbol, "period": "5m",
        "startTime": start_ts, "endTime": end_ts, "limit": 500,
    })
    if not data:
        return {}
    out: dict[int, float] = {}
    for entry in sorted(data, key=lambda d: int(d.get("timestamp", 0))):
        ts5 = int(entry["timestamp"])
        try:
            buy = float(entry.get("buyVol", 0) or 0)
            sell = float(entry.get("sellVol", 0) or 0)
        except (ValueError, TypeError):
            continue
        signed = buy - sell
        for i in range(5):
            slot = ts5 + i * 60_000
            if start_ts <= slot <= end_ts:
                out[slot] = signed
    return out

# ─── Per-symbol assembly ────────────────────────────────────────────────────

def fetch_symbol_day(symbol: str, start_ts: int, end_ts: int) -> list[list]:
    """Pull every enabled series for one symbol over [start_ts, end_ts] and
    return a list of CSV-ready row arrays, ordered by CSV_HEADER.
    """
    klines = fetch_klines_1m(symbol, start_ts, end_ts) if FETCH_PRICE else {}
    oi = fetch_oi_5m(symbol, start_ts, end_ts) if FETCH_OI else {}
    funding = fetch_funding_rates(symbol, start_ts, end_ts) if FETCH_FUNDING else {}
    ls = fetch_long_short_5m(symbol, start_ts, end_ts) if FETCH_LS else {}
    ticker = fetch_ticker_snapshot(symbol, start_ts, end_ts) if FETCH_TICKER else {}
    taker = fetch_taker_delta(symbol, start_ts, end_ts) if FETCH_TRADES else {}

    # Walk every minute in the window; emit a row per minute we have a price for.
    # (Symbols without trade activity in a given minute don't get a row, matching
    # metl's behavior — load_csv treats absence as "no trading".)
    rows = []
    for ts in sorted(klines.keys()):
        ts_iso = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00:00")
        close, volume = klines[ts]
        row = [
            ts_iso, symbol,
            close, volume,
            oi.get(ts, ""),
            funding.get(ts, ""),
            ls.get(ts, ""),
            taker.get(ts, ""),
            "", "",  # long_liqs, short_liqs (always empty for binetl)
            ticker.get(ts, {}).get("spread_pct", ""),
            ticker.get(ts, {}).get("bid_ask_imbalance", ""),
            ticker.get(ts, {}).get("basis_pct", ""),
        ]
        rows.append(row)
    return rows

# ─── Job tracking (mirrors metl helpers) ────────────────────────────────────

def job_create(date_from, date_to, triggered_by="cli", run_tag=None) -> str | None:
    """Insert a new compiler job row, return job_id UUID string. Mirrors
    metl.job_create exactly — same source_id (1) so cron/UI rollups
    aggregate metl + binetl jobs as a single 'compiler' surface."""
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO market.compiler_jobs
                (source_id, status, date_from, date_to, endpoints_enabled,
                 triggered_by, run_tag, started_at)
            VALUES (1, 'running', %s, %s, %s, %s, %s, NOW())
            RETURNING job_id
        """, (date_from, date_to, list(_enabled_endpoints()), triggered_by, run_tag))
        job_id = str(cur.fetchone()[0])
        conn.commit(); cur.close(); conn.close()
        log.info(f"job_id={job_id}")
        return job_id
    except Exception as e:
        log.warning(f"job_create failed: {e}")
        return None

def job_set_total(job_id, total):
    if not job_id: return
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE market.compiler_jobs SET symbols_total=%s WHERE job_id=%s", (total, job_id))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        log.warning(f"job_set_total failed: {e}")

def job_increment(job_id, rows_added):
    """Atomically increment symbols_done by 1 and rows_written by rows_added."""
    if not job_id: return
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""
            UPDATE market.compiler_jobs
            SET symbols_done   = symbols_done + 1,
                rows_written   = rows_written + %s,
                last_heartbeat = NOW()
            WHERE job_id = %s
        """, (rows_added, job_id))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        log.warning(f"job_increment failed: {e}")

def job_complete(job_id):
    if not job_id: return
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE market.compiler_jobs SET status='complete', completed_at=NOW() WHERE job_id=%s", (job_id,))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        log.warning(f"job_complete failed: {e}")

def job_fail(job_id, error_msg):
    if not job_id: return
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("UPDATE market.compiler_jobs SET status='failed', error_msg=%s, completed_at=NOW() WHERE job_id=%s",
                    ((error_msg or "")[:1000], job_id))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        log.warning(f"job_fail failed: {e}")

def _enabled_endpoints() -> list[str]:
    eps = []
    if FETCH_PRICE: eps.append("ohlcv")
    if FETCH_OI: eps.append("oi")
    if FETCH_FUNDING: eps.append("funding")
    if FETCH_LS: eps.append("ls")
    if FETCH_TICKER: eps.append("ticker")
    if FETCH_TRADES: eps.append("trades")
    return eps

# ─── CSV helpers ────────────────────────────────────────────────────────────

def csv_path_for(date_obj: datetime, suffix: str | None) -> str:
    base = f"oi_{date_obj.strftime('%Y%m%d')}"
    if suffix:
        base = f"{base}_{suffix}"
    return os.path.join(CSV_BACKUP_DIR, f"{base}.csv")

def append_day_csv(path: str, rows: list[list], write_header: bool):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(CSV_HEADER)
        w.writerows(rows)

# ─── Main loop ──────────────────────────────────────────────────────────────

def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)

def process_symbol(symbol: str, start_ts: int, end_ts: int) -> dict:
    """Worker wrapper — same shape as metl's process_symbol."""
    try:
        t0 = datetime.now()
        rows = fetch_symbol_day(symbol, start_ts, end_ts)
        return {"symbol": symbol, "rows": rows, "elapsed": datetime.now() - t0, "success": True}
    except Exception as e:
        return {"symbol": symbol, "rows": [], "elapsed": timedelta(0), "success": False, "error": str(e)}

def main(start_date: datetime, end_date: datetime, *,
         job_id: str | None, triggered_by: str, run_tag: str | None,
         csv_suffix: str | None, skip_db_load: bool):
    if job_id is None:
        job_id = job_create(start_date.date(), end_date.date(), triggered_by, run_tag)
    log.info(f"job_id={job_id} range={start_date.date()}→{end_date.date()} suffix={csv_suffix or '<none>'}")

    try:
        for day in daterange(start_date, end_date):
            log.info(f"=== {day.date()} ===")
            day_utc = datetime(year=day.year, month=day.month, day=day.day, tzinfo=timezone.utc)
            start_ts = int(day_utc.timestamp() * 1000)
            end_ts = int((day_utc + timedelta(days=1) - timedelta(seconds=1)).timestamp() * 1000)

            symbols = get_symbols_for_date(day)
            total = len(symbols)
            job_set_total(job_id, total)

            csv_path = csv_path_for(day, csv_suffix)
            # Idempotent re-run: if CSV already exists, just sync to DB and skip.
            if os.path.exists(csv_path) and not skip_db_load and not csv_suffix:
                log.info(f"CSV exists, syncing to DB: {csv_path}")
                _r = load_csv_to_futures_1m(csv_path)
                log.info(f"DB sync: read={_r['rows_read']:,} sent={_r['rows_inserted']:,} skipped={_r['rows_skipped']:,}")
                continue
            # Fresh run for this date — ensure no stale CSV
            if os.path.exists(csv_path):
                log.warning(f"removing existing CSV before fresh run: {csv_path}")
                os.unlink(csv_path)

            buffer: list[list] = []
            wrote_header = False
            t_day = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = {ex.submit(process_symbol, s, start_ts, end_ts): s for s in symbols}
                done = 0
                for fut in as_completed(futs):
                    res = fut.result()
                    done += 1
                    if res["success"]:
                        buffer.extend(res["rows"])
                        job_increment(job_id, len(res["rows"]))
                        if done % 50 == 0 or done == total:
                            with _weight_lock:
                                w = _current_weight["value"]
                            log.info(f"  {done}/{total} ({100*done//total}%) weight={w}")
                        if len(buffer) >= CSV_FLUSH_CHUNK:
                            append_day_csv(csv_path, buffer, write_header=not wrote_header)
                            wrote_header = True
                            buffer.clear()
                    else:
                        log.error(f"  FAIL {res['symbol']}: {res['error']}")

            if buffer:
                append_day_csv(csv_path, buffer, write_header=not wrote_header)
                wrote_header = True
            log.info(f"  CSV written: {csv_path} ({time.time()-t_day:.1f}s)")

            if not skip_db_load and not csv_suffix:
                _r = load_csv_to_futures_1m(csv_path)
                log.info(f"  DB load: read={_r['rows_read']:,} sent={_r['rows_inserted']:,} skipped={_r['rows_skipped']:,}")
            elif csv_suffix:
                log.info(f"  shadow run (csv_suffix={csv_suffix}) — skipping DB load")
    except Exception as e:
        log.exception("run failed")
        job_fail(job_id, str(e))
        raise

    job_complete(job_id)
    log.info(f"DONE in {time.time()-SCRIPT_START_TIME:.1f}s")

# ─── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=False)
    p.add_argument("--job-id", type=str, default=None, dest="job_id")
    p.add_argument("--triggered-by", default="cli", choices=["ui", "cli", "scheduler"], dest="triggered_by")
    p.add_argument("--run-tag", default=None, dest="run_tag")
    p.add_argument("--csv-suffix", default=None, dest="csv_suffix",
                   help="If set, write to oi_YYYYMMDD_SUFFIX.csv and skip DB load. Used for shadow validation.")
    p.add_argument("--skip-db-load", action="store_true", dest="skip_db_load")
    args = p.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.end else start
    main(start, end,
         job_id=args.job_id, triggered_by=args.triggered_by, run_tag=args.run_tag,
         csv_suffix=args.csv_suffix, skip_db_load=args.skip_db_load)

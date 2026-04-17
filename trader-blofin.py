#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trader-blofin.py
================
Live execution for the Overlap Strategy on BloFin.

Full session lifecycle (single persistent process per day):

  06:00 UTC  -> snapshot session-open prices
  06:35 UTC  -> BAR-6 CONVICTION CHECK
                  roi_x < 0.3%  -> B: No Entry (0%, no fees)
                  roi_x >= 0.3% -> Enter at L_HIGH x VOL_boost
  06:40-18:00 -> 5-min loop (bars 7-143):
                  1st PORT_SL   : incr <= -6%          -> C: Hard Stop
                  2nd PORT_TSL  : (incr-peak) <= -7.5% -> D: Trail Stop
                  3rd EARLY_FILL: sess_ret >= 9%        -> E: Early Fill
  18:00-23:55 -> 5-min loop (bars 144-216, FILL gate closed)
  23:55 UTC  -> F: Session Close

Usage:
  python3 trader-blofin.py                   # normal daily session
  python3 trader-blofin.py --dry-run         # simulate, no orders sent
  python3 trader-blofin.py --resume          # resume after crash (restores from state)
  python3 trader-blofin.py --status          # print state file and exit
  python3 trader-blofin.py --seed-returns FILE  # seed returns log from CSV

Cron (invoked by run_daily.sh at 05:58 UTC after daily_signal.py):
  58 5 * * *  /home/ubuntu/benji3m/run_daily.sh >> /home/ubuntu/benji3m/cron.log 2>&1
"""

import os, sys, json, math, time, logging, datetime, argparse, csv, requests
from urllib.parse import urlencode
import numpy as np
import pandas as pd
from pathlib import Path

# Ensure project root is on sys.path so pipeline imports resolve
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline.allocator.blofin_auth import get_headers as _shared_get_headers

# Self-load secrets.env so the script works regardless of how it's invoked
# (cron without `set -a`, manual shell, systemd, etc). Plain KEY=VALUE lines
# only — no `export` prefix support, no shell interpolation. Existing env
# vars take precedence (setdefault), so callers can still override per-run.
_SECRETS = Path("/mnt/quant-data/credentials/secrets.env")
if _SECRETS.exists():
    for _line in _SECRETS.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())


# ==========================================================================
# CONFIGURATION
# ==========================================================================

# -- API credentials -------------------------------------------------------
API_KEY    = os.environ.get("BLOFIN_API_KEY",    "YOUR_API_KEY")
API_SECRET = os.environ.get("BLOFIN_API_SECRET", "YOUR_API_SECRET")
PASSPHRASE = os.environ.get("BLOFIN_PASSPHRASE", "YOUR_PASSPHRASE")

if "YOUR_" in API_KEY or "YOUR_" in API_SECRET or not PASSPHRASE:
    raise RuntimeError("BloFin API credentials not set — check secrets.env")

DEMO_MODE  = False   # True -> BloFin paper trading environment

# -- Session timing (must match backtest) ----------------------------------
SESSION_START_HOUR = 6
BAR_MINUTES        = 5
CONVICTION_BAR     = 6        # bar-6 closes at 35 min (06:35 UTC)
FILL_MAX_BAR       = 143      # EARLY_FILL gate closes at bar 143 (18:00 UTC)

# -- Strategy parameters (must match backtest exactly) ---------------------
L_HIGH       = 1.33    # base leverage floor; L_BASE=0 never selected
KILL_Y       = 0.003   # 0.3%  -- bar-6 conviction threshold
CONVICTION_EXEC_BUFFER_MIN = 20 # minutes after 06:35 within which we still execute so 6:45 UTC
                                 # if conviction passes. After 06:46, log but don't trade.
PORT_SL_PCT  = -0.06   # -6%   -- hard stop (unleveraged, from entry)
PORT_TSL_PCT = -0.075  # -7.5% -- trailing stop (from peak, unleveraged)
EARLY_FILL_Y = 0.09    # 9%    -- profit-take threshold (from session open)
MARGIN_MODE  = "isolated"
POSITION_SIDE = "net"

# -- Exchange-native stop-loss (optional, belt-and-suspenders) -------------
# Set EXCHANGE_SL_ENABLED = True to attach a stop-loss order to every
# position at entry. This fires at the exchange level independently of the
# software-operated symbol/portfolio stops -- a backstop in case of
# connectivity loss or script crash.

# EXCHANGE_SL_PCT is the 1x unleveraged drop from entry price that triggers.
# Default: -8.5% (wider than PORT_SL_PCT=-6% so the software stop acts first
# under normal conditions; exchange SL only fires if software stop fails).
EXCHANGE_SL_ENABLED = True
EXCHANGE_SL_PCT     = -0.085  # -8.5% from entry price (1x, unleveraged)

# -- VOL-Target Leverage Engine (Figure 2.4) -------------------------------
VOL_LEV_TARGET_VOL   = 0.02
VOL_LEV_WINDOW       = 30
VOL_LEV_SHARPE_REF   = 3.3
VOL_LEV_MAX_BOOST    = 2.0     # 1.33x floor to 2.66x ceiling
VOL_LEV_DD_THRESHOLD = -0.15
VOL_LEV_DD_SCALE     = 1.0     # inactive (structurally present)

# -- Capital allocation ----------------------------------------------------
# CAPITAL_MODE = "pct_balance" + CAPITAL_VALUE = 1.0 matches the audit exactly:
# the audit deploys 100% of compounded equity each session (equity_running * 1.0).
# CAPITAL_VALUE = 1.0 means 100% of available USDT balance.
CAPITAL_MODE  = "pct_balance"
CAPITAL_VALUE = 1.0            # 100% of account balance -- matches audit equity compounding

# -- Transaction costs (must match audit.py exactly) -----------------------
# fee_drag     = TAKER_FEE_PCT * lev_used   (round-trip taker fee on total position)
# funding_drag = FUNDING_RATE_DAILY_PCT * lev_used  (2 funding windows per day)
# Applied to net return: r_net = r_gross - fee_drag - funding_drag
# These are deducted from equity_running after each session in the audit.
# In the live trader we log gross return; actual fees are deducted by BloFin.
# These constants are kept here for reference and VOL boost log accuracy.
TAKER_FEE_PCT          = 0.0008   # 0.04% per side x 2 = 0.08% round-trip
FUNDING_RATE_DAILY_PCT = 0.0002   # ~0.02% per day (2 windows x ~0.01%)

# -- Signal files ----------------------------------------------------------
DEPLOYS_CSV   = Path("live_deploys_signal.csv")
ACTIVE_FILTER = "Tail Guardrail"
SYMBOL_SUFFIX = "-USDT"

# -- Price source for session monitoring (conviction, stops, fills) ----------
# "binance" -> matches backtest data source (Binance Futures last price)
#              use this for consistency with audit.py / portfolio matrix
# "blofin"  -> BloFin mark price (exchange where orders are placed)
# The two are very close in practice but Binance is the correct choice
# since that is what the backtest roi_x, PORT_SL, PORT_TSL are computed from.
PRICE_SOURCE  = "blofin"   # "binance" | "blofin"
BINANCE_BASE  = "https://fapi.binance.com"

# -- Alerting --------------------------------------------------------------
# Set ALERT_EMAIL to receive email on critical events (failed closes, crashes).
# Requires 'mail' to be configured on the server (e.g. via postfix + SES relay).
# Leave empty to disable email alerts (log-file alerts always active).
ALERT_EMAIL = os.environ.get("ALERT_EMAIL", "")

# -- Persistence -----------------------------------------------------------
STATE_FILE  = Path("blofin_executor_state.json")
RETURNS_LOG = Path("blofin_returns_log.csv")
ALERTS_LOG  = Path("blofin_alerts.log")
LOCK_FILE   = Path(".trader_session.lock")
LOG_FILE    = Path("blofin_executor.log")


# ==========================================================================
# LOGGING
# ==========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("trader_blofin")


# ==========================================================================
# ALERTING
# ==========================================================================

def alert(msg: str, subject: str = "trader-blofin ALERT"):
    """
    Write a critical alert to the alerts log and optionally send email.
    Called on: failed closes, crash recovery start, unclosed positions.
    """
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {subject}: {msg}"
    log.error(f"ALERT -- {msg}")
    try:
        with open(ALERTS_LOG, "a") as f:
            f.write(line + "\n")
    except OSError as e:
        log.debug("Could not write alert log: %s", e)
    if ALERT_EMAIL:
        try:
            import subprocess
            subprocess.run(
                ["mail", "-s", subject, ALERT_EMAIL],
                input=msg.encode(), timeout=10, check=False,
            )
        except Exception as e:
            log.warning(f"Alert email failed: {e}")


# ==========================================================================
# LOCKFILE  (prevents double-launch from cron or manual run)
# ==========================================================================

def acquire_lock():
    """
    Create a lockfile. Exits with error if one already exists.
    Prevents two instances of the trader running simultaneously.
    """
    if LOCK_FILE.exists():
        try:
            pid = LOCK_FILE.read_text().strip()
        except Exception:
            pid = "unknown"
        alert(
            f"Lock file {LOCK_FILE} exists (PID {pid}). "
            "Previous session may still be running. "
            "Delete the lock file manually if the previous session has ended.",
            subject="trader-blofin DOUBLE LAUNCH PREVENTED"
        )
        log.error("Aborting -- lockfile exists. If the previous session is done, "
                  f"delete {LOCK_FILE} and retry.")
        sys.exit(1)
    LOCK_FILE.write_text(str(os.getpid()))

def release_lock():
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except OSError as e:
        log.debug("Could not release lock file: %s", e)


# ==========================================================================
# UTC TIME HELPERS
# ==========================================================================

def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

def utc_today() -> str:
    return utcnow().strftime("%Y-%m-%d")

def bar_index() -> int:
    elapsed = utcnow().hour * 60 + utcnow().minute - SESSION_START_HOUR * 60
    return max(-1, int(elapsed // BAR_MINUTES))

def sleep_until_next_bar():
    n = utcnow()
    next_min = (n.minute // BAR_MINUTES + 1) * BAR_MINUTES
    if next_min >= 60:
        target = n.replace(minute=0, second=2, microsecond=0) + datetime.timedelta(hours=1)
    else:
        target = n.replace(minute=next_min, second=2, microsecond=0)
    secs = (target - n).total_seconds()
    time.sleep(max(secs, 1))

def sleep_until(target: datetime.datetime, label: str = ""):
    secs = (target - utcnow()).total_seconds()
    if secs > 0:
        log.info(f"Waiting {secs/60:.1f} min until {target.strftime('%H:%M')} UTC"
                 + (f" ({label})" if label else ""))
        time.sleep(secs)


# ==========================================================================
# STATE
# ==========================================================================

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


# ==========================================================================
# SIGNAL READER
# ==========================================================================

def get_today_symbols(date_str: str) -> list:
    if not DEPLOYS_CSV.exists():
        raise FileNotFoundError(
            f"Deploys CSV not found: {DEPLOYS_CSV}\n"
            "daily_signal.py must run before this script."
        )
    with open(DEPLOYS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))

    today_rows = [r for r in rows if r.get("date", "").strip() == date_str]
    if not today_rows:
        log.warning(f"No row for {date_str} in deploys CSV -- flat day.")
        return []

    if ACTIVE_FILTER:
        today_rows = [r for r in today_rows
                      if r.get("filter", "").strip().lower() == ACTIVE_FILTER.strip().lower()]
        if not today_rows:
            log.info(f"Filter '{ACTIVE_FILTER}' blocked today -> A: Filtered (0%, no fees)")
            return []

    symbols = set()
    for row in today_rows:
        for sym in row.get("symbols", "").replace(",", " ").split():
            s = sym.strip().upper()
            if s:
                symbols.add(s)

    log.info(f"Signal: {len(symbols)} symbols -> {sorted(symbols)}")
    return sorted(symbols)


# ==========================================================================
# BLOFIN API HELPERS
# ==========================================================================

class BlofinREST:
    """
    Direct BloFin REST client using the auth pattern from mtrader2.py.
    No SDK required -- pure requests + HMAC-SHA256.
    """
    LIVE_URL = "https://openapi.blofin.com"
    DEMO_URL = "https://demo-trading-openapi.blofin.com"

    def __init__(self, api_key: str, api_secret: str, passphrase: str, demo: bool = False):
        self._key        = api_key
        self._secret     = api_secret
        self._passphrase = passphrase
        self._base       = self.DEMO_URL if demo else self.LIVE_URL
        if demo:
            log.info("DEMO MODE -- using BloFin paper trading environment")

    def _headers(self, path: str, method: str, body: dict = None) -> dict:
        body_str = (json.dumps(body, separators=(",", ":"), ensure_ascii=False)
                    if body and method.upper() != "GET" else "")
        return _shared_get_headers(method, path, body_str)

    def request(self, method: str, path: str,
                params: dict = None, body: dict = None) -> dict:
        method = method.upper()
        qs     = ("?" + urlencode(params, doseq=True)) if params else ""
        full_path = path + qs
        headers   = self._headers(full_path, method, body)
        url       = self._base + full_path

        try:
            if method == "GET":
                resp = requests.get(url, headers=headers, timeout=15)
            else:
                resp = requests.post(
                    url, headers=headers,
                    data=json.dumps(body or {}, separators=(",", ":")),
                    timeout=15,
                )
            resp.raise_for_status()
            data = resp.json()
            code = str(data.get("code", ""))
            if code not in ("0", ""):
                log.warning(f"BloFin API: code={code} msg={data.get('msg','')} path={path}")
            return data
        except requests.exceptions.RequestException as e:
            log.error(f"BloFin request failed: {method} {path} -- {e}")
            return {"code": "error", "data": None}

    # ── Public market endpoints (no auth needed) ──────────────────────────

    def get_tickers(self, inst_id: str) -> dict:
        return self.request("GET", "/api/v1/market/tickers",
                            params={"instId": inst_id})

    def get_instruments(self, inst_id: str = None, inst_type: str = "SWAP") -> dict:
        p = {"instType": inst_type}
        if inst_id:
            p["instId"] = inst_id
        return self.request("GET", "/api/v1/market/instruments", params=p)

    # ── Private account endpoints ─────────────────────────────────────────

    def get_balance(self) -> dict:
        return self.request("GET", "/api/v1/account/balance")

    def get_positions(self, inst_id: str = None) -> dict:
        p = {"instId": inst_id} if inst_id else {}
        return self.request("GET", "/api/v1/account/positions", params=p)

    def set_leverage(self, inst_id: str, leverage: int, margin_mode: str) -> dict:
        return self.request("POST", "/api/v1/account/set-leverage", body={
            "instId": inst_id, "leverage": str(leverage),
            "marginMode": margin_mode,
        })

    # ── Private trading endpoints ─────────────────────────────────────────

    def place_order(self, inst_id: str, margin_mode: str, position_side: str,
                    side: str, order_type: str, size: str,
                    reduce_only: bool = False,
                    sl_trigger_price: str = None,
                    sl_order_price: str = None) -> dict:
        body = {
            "instId":       inst_id,
            "marginMode":   margin_mode,
            "positionSide": position_side,
            "side":         side,
            "orderType":    order_type,
            "size":         size,
            "reduceOnly":   "true" if reduce_only else "false",
            "clientOrderId": "",
        }
        if sl_trigger_price is not None:
            body["slTriggerPrice"] = sl_trigger_price
            body["slOrderPrice"]   = sl_order_price if sl_order_price is not None else "-1"
        return self.request("POST", "/api/v1/trade/order", body=body)

    def close_position(self, inst_id: str, margin_mode: str,
                       position_side: str) -> dict:
        return self.request("POST", "/api/v1/trade/close-position", body={
            "instId":       inst_id,
            "marginMode":   margin_mode,
            "positionSide": position_side,
            "clientOrderId": "",
        })


def build_api() -> BlofinREST:
    return BlofinREST(
        api_key    = API_KEY,
        api_secret = API_SECRET,
        passphrase = PASSPHRASE,
        demo       = DEMO_MODE,
    )

def get_account_balance_usdt(api: BlofinREST) -> float:
    """
    Confirmed field structure from working mtrader2.py:
      resp.data.totalEquity       (top-level)
      resp.data.details[0].availableEquity  (per-currency)
    Falls back through multiple field names for robustness.
    """
    resp = api.get_balance()
    data = resp.get("data") or {}

    # Structure 1: {data: {totalEquity, details:[{availableEquity, currency}]}}
    if isinstance(data, dict):
        details = data.get("details") or []
        for item in details:
            ccy = item.get("currency", "") or item.get("ccy", "")
            if ccy.upper() == "USDT":
                val = (item.get("availableEquity")
                       or item.get("available")
                       or item.get("availBal") or 0)
                avail = float(val)
                if avail > 0:
                    log.info(f"USDT available equity: ${avail:,.2f}")
                    return avail
        # fallback: totalEquity if no detail found
        total = data.get("totalEquity") or data.get("totalEq") or 0
        if total:
            log.warning(f"availableEquity not found -- using totalEquity ${float(total):,.2f}")
            return float(total)

    # Structure 2: {data: [{currency, available}]}  (SDK wrapper format)
    if isinstance(data, list):
        for item in data:
            ccy = item.get("currency", "") or item.get("ccy", "")
            if ccy.upper() == "USDT":
                val = (item.get("availableEquity")
                       or item.get("available")
                       or item.get("availBal") or 0)
                return float(val)

    log.warning("USDT balance not found in any known response structure -- returning 0")
    return 0.0

def get_mark_prices(api: BlofinREST, inst_ids: list) -> dict:
    """
    Fetch prices for monitoring (conviction check, stops, fills).

    PRICE_SOURCE = "binance" (default):
        Uses Binance Futures last price -- matches the data source used
        in the backtest portfolio matrix (master_data_table.parquet is
        Binance data). Ensures roi_x, PORT_SL, PORT_TSL are computed
        on the same price series as the backtest.
        Falls back to BloFin if Binance returns no data for a symbol.

    PRICE_SOURCE = "blofin":
        Uses BloFin mark price -- the exchange where orders execute.
        Small basis difference from Binance, could affect marginal
        conviction gate decisions.

    Symbols with no price from either source are excluded from
    equal_weight_return (denominator adjusts automatically).
    """
    if PRICE_SOURCE == "binance":
        return _get_prices_binance(inst_ids)
    return _get_prices_blofin(api, inst_ids)


def _get_prices_binance(inst_ids: list) -> dict:
    """Fetch last prices from Binance Futures. inst_ids are BloFin format (BTC-USDT)."""
    prices = {}
    for inst_id in inst_ids:
        # Convert BloFin format BTC-USDT -> Binance format BTCUSDT
        binance_sym = inst_id.replace("-USDT", "USDT").replace("-", "")
        try:
            resp = requests.get(
                f"{BINANCE_BASE}/fapi/v1/ticker/price",
                params={"symbol": binance_sym},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            price = float(data.get("price", 0))
            if price > 0:
                prices[inst_id] = price
            else:
                log.warning(f"  {inst_id}: Binance price=0 -- excluded")
        except Exception as e:
            log.warning(f"  {inst_id}: Binance price fetch failed: {e} -- excluded")
    return prices


def _get_prices_blofin(api: BlofinREST, inst_ids: list) -> dict:
    """Fetch last prices from BloFin tickers."""
    print("getting prices from blofin")
    prices = {}
    for inst_id in inst_ids:
        try:
            resp = api.get_tickers(inst_id=inst_id)
            data = resp.get("data") or []
            if data:
                price = float(data[0].get("last") or data[0].get("markPrice") or 0)
                if price > 0:
                    prices[inst_id] = price
                else:
                    log.warning(f"  {inst_id}: BloFin price=0 -- possible delisting, excluded")
            else:
                log.warning(f"  {inst_id}: no BloFin price data -- excluded")
        except Exception as e:
            log.warning(f"  {inst_id}: BloFin price fetch error: {e} -- excluded")
    return prices

def get_instrument_info(api: BlofinREST, inst_id: str) -> dict:
    """
    Fetch instrument constraints from BloFin.
    Returns dict with: contractValue, minSize, lotSize, maxMarketSize, state.
    Confirmed fields from working mtrader2.py.
    """
    try:
        resp = api.get_instruments(inst_id=inst_id, inst_type="SWAP")
        data = resp.get("data") or []
        if data:
            row = data[0]
            return {
                "contractValue": _safe_float(row.get("contractValue"), 1.0),
                "minSize":       _safe_float(row.get("minSize"),       1.0),
                "lotSize":       _safe_float(row.get("lotSize"),       1.0),
                "maxMarketSize": _safe_float(row.get("maxMarketSize"), None),
                "state":         row.get("state", "live"),
            }
    except Exception as e:
        log.warning(f"Instrument info failed for {inst_id}: {e}")
    return {"contractValue": 1.0, "minSize": 1.0, "lotSize": 1.0,
            "maxMarketSize": None, "state": "live"}

def _get_prices_at_timestamp(inst_ids: list,
                              target_dt: datetime.datetime) -> dict:
    """
    Fetch the closing price of each symbol at a specific UTC timestamp
    using Binance 1-min klines. Returns {inst_id: price}.

    Uses the 1-min kline whose open_time == target_dt (or the nearest
    prior bar within 2 minutes as tolerance for rounding).

    This ensures roi_x is always computed from the correct 06:00 / 06:35
    UTC prices regardless of when the script is invoked.
    """
    import calendar as _cal
    prices = {}
    ts_ms  = _cal.timegm(target_dt.timetuple()) * 1000
    # Fetch 3 bars around the target to handle minor timestamp offsets
    start_ms = ts_ms - 60_000   # 1 min before
    end_ms   = ts_ms + 60_000   # 1 min after

    for inst_id in inst_ids:
        binance_sym = inst_id.replace("-USDT", "USDT").replace("-", "")
        try:
            resp = requests.get(
                f"{BINANCE_BASE}/fapi/v1/klines",
                params={
                    "symbol":    binance_sym,
                    "interval":  "1m",
                    "startTime": start_ms,
                    "endTime":   end_ms,
                    "limit":     3,
                },
                timeout=10,
            )
            resp.raise_for_status()
            bars = resp.json()
            if not bars:
                continue
            # Pick the bar whose open_time is closest to target_dt
            best = min(bars, key=lambda b: abs(b[0] - ts_ms))
            price = float(best[4])   # close price
            if price > 0:
                prices[inst_id] = price
        except Exception as e:
            log.debug(f"  {inst_id}: kline at {target_dt.strftime('%H:%M')} failed: {e}")

    return prices


def _safe_float(val, default):
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default

def _round_to_lot(value: float, lot_size: float) -> float:
    """Floor value to nearest lot_size multiple (confirmed from mtrader2.py)."""
    if not lot_size or lot_size <= 0:
        return value
    inv = 1.0 / lot_size
    return int(value * inv) / inv

def get_actual_positions(api: BlofinREST, inst_ids: list) -> dict:
    """
    Fetch actual open positions from BloFin for the given instruments.
    Returns {inst_id: contracts} for any position with size > 0.
    Uses a single bulk call (no instId filter) to avoid triggering BloFin's
    rate-limit firewall with rapid per-symbol requests.
    If inst_ids is empty, returns ALL open positions (no filtering).
    """
    actual = {}
    inst_id_set = set(inst_ids)
    try:
        resp = api.get_positions()   # fetch ALL positions in one call
        for pos in (resp.get("data") or []):
            iid = pos.get("instId", "")
            if inst_id_set and iid not in inst_id_set:
                continue
            size = float(pos.get("positions", 0) or pos.get("pos", 0) or 0)
            if size > 0:
                actual[iid] = size
                log.info(f"  BloFin position: {iid} = {size} contracts")
    except Exception as e:
        log.warning(f"Could not fetch actual positions from BloFin: {e}")
    return actual


# ==========================================================================
# VOL-TARGET LEVERAGE ENGINE  (Figure 2.4)
# ==========================================================================

def compute_vol_boost() -> float:
    """
    Four-stage VOL-target leverage boost scalar.

    Stage 1: vc = clip(target_vol / realized_vol, 1.0, max_boost)  -- boosts only
    Stage 2: sc = clip(sharpe_ref / rolling_sharpe, 0.5, 2.0)      -- contrarian
    Stage 3: dg = DD_SCALE if running_DD < DD_THRESHOLD else 1.0   -- guard (inactive)
    Stage 4: boost = clip(vc x sc x dg, 1.0, max_boost)
             effective_lev = L_HIGH x boost  ->  1.33x to 2.66x

    NOTE: returns log records approximate net return (gross x lev, before fees).
    Over time, the small fee discrepancy has negligible effect on vol estimation.
    """
    if not RETURNS_LOG.exists():
        log.warning("Returns log not found -- boost=1.0 (L_HIGH floor)")
        return 1.0
    try:
        df = pd.read_csv(RETURNS_LOG, parse_dates=["date"])
        df = df.sort_values("date")
        rets = df["net_return_pct"].values / 100.0

        window = min(VOL_LEV_WINDOW, len(rets))
        if window < 5:
            log.warning(f"Only {window} return days available -- boost=1.0")
            return 1.0
        rets_w = rets[-window:]

        realized_vol = max(float(np.std(rets_w)), 1e-8)
        vc = float(np.clip(VOL_LEV_TARGET_VOL / realized_vol, 1.0, VOL_LEV_MAX_BOOST))

        mean_ret = float(np.mean(rets_w))
        rolling_sharpe = max(
            mean_ret / realized_vol * math.sqrt(365) if realized_vol > 0
            else VOL_LEV_SHARPE_REF,
            1e-8
        )
        sc = float(np.clip(VOL_LEV_SHARPE_REF / rolling_sharpe, 0.5, 2.0))

        equity     = np.cumprod(1.0 + rets)
        running_dd = float(equity[-1] / np.max(equity) - 1.0)
        dg = VOL_LEV_DD_SCALE if running_dd < VOL_LEV_DD_THRESHOLD else 1.0

        boost = float(np.clip(vc * sc * dg, 1.0, VOL_LEV_MAX_BOOST))
        log.info(
            f"VOL boost: rvol={realized_vol*100:.2f}%  vc={vc:.3f}  "
            f"sharpe={rolling_sharpe:.2f}  sc={sc:.3f}  dg={dg:.3f}  "
            f"-> boost={boost:.3f}x  (eff={L_HIGH*boost:.3f}x)"
        )
        return boost
    except Exception as e:
        log.warning(f"VOL boost failed: {e} -- boost=1.0")
        return 1.0

def log_daily_return(net_return_pct: float, exit_reason: str,
                     session_date: str = None, eff_lev: float = 0.0):
    """
    Append session return to returns log for VOL boost computation.

    Deducts fees matching audit.py simulate():
      fee_drag     = TAKER_FEE_PCT * lev_used
      funding_drag = FUNDING_RATE_DAILY_PCT * lev_used
      r_net        = r_gross - fee_drag - funding_drag

    This ensures the returns log used by the VOL boost engine reflects
    the same net-of-fees returns as the audit equity_running series.
    Flat days (no trade) have no fees deducted -- matches audit behaviour.
    """
    if eff_lev > 0 and exit_reason not in ("filtered", "no_entry_conviction",
                                            "missed_window", "stale_closed",
                                            "stale_close_failed"):
        fee_drag     = TAKER_FEE_PCT          * eff_lev
        funding_drag = FUNDING_RATE_DAILY_PCT * eff_lev
        net_return_pct = net_return_pct - (fee_drag + funding_drag) * 100
    row = {
        "date": session_date or utc_today(),
        "net_return_pct": round(net_return_pct, 4),
        "exit_reason": exit_reason,
    }
    df_new = pd.DataFrame([row])
    if RETURNS_LOG.exists():
        df = pd.read_csv(RETURNS_LOG)
        df = df[df["date"] != row["date"]]
        df_out = pd.concat([df, df_new], ignore_index=True)
    else:
        df_out = df_new
    df_out.to_csv(RETURNS_LOG, index=False)
    log.info(f"Return logged: {net_return_pct:+.4f}% ({exit_reason})")


# ==========================================================================
# PORTFOLIO RETURN HELPERS
# ==========================================================================

def equal_weight_return(current: dict, ref: dict) -> float:
    """
    Equal-weight 1x return: mean((price/ref_price) - 1) across symbols.
    Symbols missing from current (delisted/fetch failure) are excluded;
    the denominator shrinks automatically -- no fabricated 0 returns.
    """
    rets = [
        current[k] / ref[k] - 1.0
        for k in ref
        if k in current and ref[k] > 0
    ]
    if not rets:
        return 0.0
    return float(np.mean(rets))


# ==========================================================================
# ORDER EXECUTION
# ==========================================================================

def enter_positions(api: BlofinREST, inst_ids, entry_prices,
                    eff_lev, balance, dry_run) -> list:
    """
    Equal-weight market buy. No BloFin TPSL orders -- stops managed at portfolio
    level by the monitoring loop.

    Sizing follows confirmed mtrader2.py patterns:
      - Leverage must be integer (BloFin requirement)
      - Contracts rounded DOWN to lotSize increment
      - minSize checked before placing
      - maxMarketSize respected via chunked orders
      - instrument state checked (must be "live")
    Also guards against balance=0 which would place 1 contract per symbol.
    """
    usdt_total = (balance * CAPITAL_VALUE if CAPITAL_MODE == "pct_balance"
                  else min(CAPITAL_VALUE, balance))

    # Hard guard: refuse to trade if balance is 0 or near-0
    if usdt_total < 10:
        alert(
            f"Capital allocation is ${usdt_total:.2f} -- too small to trade safely. "
            "Check USDT balance fetch. Aborting session.",
            subject="trader-blofin CAPITAL ERROR"
        )
        log.error(f"Capital ${usdt_total:.2f} is below $10 minimum -- aborting enter_positions")
        return []

    tradeable    = [i for i in inst_ids if entry_prices.get(i, 0) > 0]
    n_tradeable  = max(len(tradeable), 1)
    MARGIN_BUFFER = 0.10
    usdt_deployable = usdt_total * (1 - MARGIN_BUFFER)
    usdt_per_sym    = usdt_deployable / n_tradeable

    # BloFin requires integer leverage
    lev_int = max(1, int(math.ceil(eff_lev)))

    log.info(
        f"Entering {len(tradeable)} positions  "
        f"${usdt_total:,.0f} total  ${usdt_deployable:,.0f} deployable (10% buffer)  ${usdt_per_sym:,.0f}/symbol  "
        f"lev={eff_lev:.3f}x (set as {lev_int}x integer)  "
        f"{'[DRY RUN]' if dry_run else '[LIVE]'}"
    )

    opened = []
    failed = []
    for inst_id in tradeable:
        price = entry_prices[inst_id]

        # Fetch instrument constraints (confirmed from mtrader2.py)
        info    = get_instrument_info(api, inst_id)
        ctval   = info["contractValue"]
        lot     = info["lotSize"]
        min_sz  = info["minSize"]
        max_mkt = info["maxMarketSize"]
        state   = info["state"]

        # Skip suspended/delisted instruments
        if str(state).lower() not in ("live", ""):
            log.warning(f"  {inst_id}: state={state} -- skipping")
            continue

        # Compute and lot-round contracts
        # Use eff_lev (e.g. 1.33x) for notional sizing -- this is the true
        # target leverage. lev_int (e.g. 2x) is only the exchange setting,
        # which must be an integer >= eff_lev but should NOT drive sizing.
        raw_contracts = usdt_per_sym * eff_lev / (price * ctval)
        contracts     = _round_to_lot(raw_contracts, lot)

        if contracts < min_sz:
            log.warning(
                f"  {inst_id}: computed {contracts} contracts < minSize {min_sz} "
                f"(price=${price:,.4f} ctval={ctval} lot={lot}) -- skipping"
            )
            continue

        notional = contracts * price * ctval
        log.info(
            f"  {inst_id}: {contracts}ct  price=${price:,.4f}  "
            f"ctval={ctval}  lot={lot}  notional=${notional:,.0f}"
        )

        if dry_run:
            opened.append({"inst_id": inst_id, "contracts": contracts,
                           "entry_price": price, "order_id": "DRY_RUN",
                           "lev_int": lev_int,
                           "marginMode": MARGIN_MODE,
                           "positionSide": POSITION_SIDE})
            continue

        # Set leverage (integer, hard-fail -- wrong leverage = wrong risk exposure)
        try:
            lev_resp = api.set_leverage(inst_id=inst_id, leverage=lev_int, margin_mode=MARGIN_MODE)
            lev_code = str(lev_resp.get("code", ""))
            if lev_code not in ("0", ""):
                log.error(
                    f"  {inst_id}: set_leverage rejected (code={lev_code} "
                    f"msg={lev_resp.get('msg','')}) -- skipping to avoid "
                    f"wrong leverage exposure"
                )
                continue
        except Exception as le:
            log.error(f"  {inst_id}: set_leverage exception: {le} -- skipping")
            continue

        # Place order(s), chunking if maxMarketSize is set.
        # Optionally attach an exchange-native stop-loss at entry.
        sl_trigger = None
        if EXCHANGE_SL_ENABLED:
            sl_trigger = f"{price * (1 + EXCHANGE_SL_PCT):.8g}"
            log.info(f"  {inst_id}: exchange SL set at ${float(sl_trigger):,.4f} "
                     f"({EXCHANGE_SL_PCT*100:.1f}% from entry)")

        ok, order_id = _place_order_chunked(
            api, inst_id, contracts, lot, min_sz, max_mkt,
            sl_trigger_price=sl_trigger,
        )
        if ok:
            log.info(f"  ✅ {inst_id}  orderId={order_id}")
            opened.append({"inst_id": inst_id, "contracts": contracts,
                           "entry_price": price, "order_id": order_id,
                           "lev_int": lev_int,
                           "marginMode": MARGIN_MODE,
                           "positionSide": POSITION_SIDE})
        else:
            log.error(f"  ❌ {inst_id}: order failed -- queued for retry")
            failed.append({
                "inst_id":  inst_id,
                "contracts": contracts,
                "lot":      lot,
                "min_sz":   min_sz,
                "max_mkt":  max_mkt,
                "price":    price,
                "lev_int":  lev_int,
                "ctval":    ctval,
            })

    MAX_ENTRY_RETRIES = 3
    retry_round = 0
    while failed and not dry_run and retry_round < MAX_ENTRY_RETRIES:
        retry_round += 1
        time.sleep(5)
        log.info(f"RETRY round {retry_round}/{MAX_ENTRY_RETRIES} -- {len(failed)} symbol(s)")
        still_failed = []
        for item in failed:
            inst_id = item["inst_id"]
            target  = item["contracts"]
            lot     = item["lot"]
            min_sz  = item["min_sz"]
            max_mkt = item["max_mkt"]
            price   = item["price"]
            ctval   = item["ctval"]

            half = _round_to_lot(target / 2.0, lot)
            if half < min_sz:
                log.error(f"  {inst_id}: retry half {half} < minSize {min_sz} -- giving up")
                continue

            sl_trigger = None
            if EXCHANGE_SL_ENABLED:
                sl_trigger = f"{price * (1 + EXCHANGE_SL_PCT):.8g}"

            filled_total = 0.0
            ok1, order_id1 = _place_order_chunked(
                api, inst_id, half, lot, min_sz, max_mkt,
                sl_trigger_price=sl_trigger,
            )
            if ok1:
                log.info(f"  ✅ {inst_id} retry half-1 {half}ct orderId={order_id1}")
                filled_total += half
                time.sleep(1)
                remainder_first = _round_to_lot(target - half, lot)
                if remainder_first >= min_sz:
                    ok2, order_id2 = _place_order_chunked(
                        api, inst_id, remainder_first, lot, min_sz, max_mkt,
                        sl_trigger_price=None,
                    )
                    if ok2:
                        log.info(f"  ✅ {inst_id} retry half-2 {remainder_first}ct orderId={order_id2}")
                        filled_total += remainder_first
                    else:
                        log.error(f"  ❌ {inst_id}: retry half-2 failed")
            else:
                log.error(f"  ❌ {inst_id}: retry half-1 failed")

            if filled_total > 0:
                opened.append({"inst_id": inst_id, "contracts": filled_total,
                               "entry_price": price, "order_id": order_id1,
                               "lev_int": item["lev_int"],
                               "marginMode": MARGIN_MODE,
                               "positionSide": POSITION_SIDE})
                unfilled = _round_to_lot(target - filled_total, lot)
                if unfilled >= min_sz:
                    next_item = dict(item)
                    next_item["contracts"] = unfilled
                    still_failed.append(next_item)
            else:
                still_failed.append(item)

        failed = still_failed

    for item in failed:
        log.error(f"RETRY EXHAUSTED {item['inst_id']}: {item['contracts']}ct unfilled after {MAX_ENTRY_RETRIES} rounds")

    return opened

def _place_order_chunked(trading_api, inst_id: str, total_contracts: float,
                          lot: float, min_sz: float, max_mkt_sz,
                          sl_trigger_price: str = None) -> tuple:
    """
    Place one or more market buy orders to fill total_contracts,
    respecting maxMarketSize by splitting into chunks.
    If sl_trigger_price is set, attaches an exchange-native stop-loss to the
    first chunk only (BloFin attaches SL to the position, not per-chunk).
    Returns (success: bool, last_order_id: str).
    Confirmed pattern from mtrader2.py.
    """
    chunk_cap  = float(max_mkt_sz) if max_mkt_sz else float("inf")
    remaining  = float(total_contracts)
    last_id    = "unknown"
    chunk_num  = 0

    while remaining >= min_sz - 1e-9:
        chunk_num += 1
        chunk = _round_to_lot(min(remaining, chunk_cap), lot)

        if chunk < min_sz:
            break

        if chunk_num > 1:
            log.info(f"  {inst_id}: chunk {chunk_num} -- placing {chunk}ct (remaining {remaining}ct)")

        # Attach SL only on the first chunk; subsequent chunks are additions
        # to the same position and the SL is already set at the exchange level.
        sl_price = sl_trigger_price if chunk_num == 1 else None

        try:
            resp = trading_api.place_order(
                inst_id=inst_id, margin_mode=MARGIN_MODE, position_side=POSITION_SIDE,
                side="buy", order_type="market", size=str(chunk),
                sl_trigger_price=sl_price,
            )
        except Exception as e:
            log.error(f"  {inst_id}: place_order exception: {e}")
            return False, "exception"

        data = resp.get("data") or [{}]
        code = str(resp.get("code", ""))

        # Check for nested error code 102015 (exceeds maxMarketSize)
        inner_code = ""
        if isinstance(data, list) and data:
            inner_code = str(data[0].get("code", ""))
        if inner_code == "102015" or code == "102015":
            # Reduce chunk cap by 20% and retry
            new_cap = _round_to_lot(max(min_sz, chunk_cap * 0.8), lot)
            if new_cap >= chunk_cap or new_cap < min_sz:
                log.error(f"  {inst_id}: cannot reduce chunk further (cap={chunk_cap})")
                return False, "max_size_error"
            log.warning(f"  {inst_id}: maxMarketSize hit -- reducing cap {chunk_cap} -> {new_cap}")
            chunk_cap = new_cap
            continue

        if code not in ("0", ""):
            msg = resp.get("msg", "")
            inner_msg = ""
            if isinstance(data, list) and data:
                inner_msg = data[0].get("msg", "")
            log.error(f"  {inst_id}: order error code={code} msg={msg}"
                      + (f" | detail={inner_msg}" if inner_msg and inner_msg != msg else ""))
            return False, f"error_{code}"

        last_id   = data[0].get("orderId", "unknown") if isinstance(data, list) and data else "unknown"
        remaining = _round_to_lot(remaining - chunk, lot)

    if remaining > min_sz / 2.0:
        log.warning(f"  {inst_id}: unfilled remainder {remaining}ct -- partial fill")

    return True, last_id

def close_all_positions(api: BlofinREST, positions, reason, dry_run) -> list:
    """
    Close all tracked positions with position reconciliation.

    Before closing, fetches actual BloFin positions to catch:
      - Positions already closed (liquidation, manual close) -> skip sell
      - State file / BloFin size mismatch -> log warning
    Returns list of positions that FAILED to close (for state persistence).
    """
    log.info(f"CLOSE ALL -- reason: {reason}")
    if not positions:
        return []

    inst_ids = [p["inst_id"] for p in positions]

    # Reconcile with actual BloFin positions
    if not dry_run:
        actual = get_actual_positions(api, inst_ids)
        reconciled = []
        for pos in positions:
            iid = pos["inst_id"]
            if iid not in actual:
                log.warning(
                    f"  {iid}: not found in BloFin positions -- "
                    "already closed (liquidated or manual). Skipping sell."
                )
            else:
                blofin_size = actual[iid]
                if abs(blofin_size - pos["contracts"]) > 0.5:
                    log.warning(
                        f"  {iid}: size mismatch -- state={pos['contracts']} "
                        f"BloFin={blofin_size}. Closing BloFin size."
                    )
                    pos = dict(pos)
                    pos["contracts"] = int(blofin_size)
                reconciled.append(pos)
        positions = reconciled

    failed = []
    for pos in positions:
        inst_id   = pos["inst_id"]
        contracts = pos["contracts"]

        if dry_run:
            log.info(f"  [DRY RUN] sell {contracts}x {inst_id}")
            continue

        try:
            # Use the dedicated close-position endpoint (confirmed from mtrader2.py)
            # This is more reliable than a reduce-only market sell and handles
            # marginMode/positionSide automatically from the open position.
            resp = api.close_position(
                inst_id=inst_id,
                margin_mode=pos.get("marginMode", MARGIN_MODE),
                position_side=pos.get("positionSide", POSITION_SIDE),
            )
            code = str(resp.get("code", ""))
            if code == "0":
                log.info(f"  ✅ {inst_id}  closed via close-position endpoint")
            else:
                # Fallback: reduce-only market sell
                log.warning(
                    f"  {inst_id}: close-position returned code={code} "
                    f"-- falling back to reduce-only market sell"
                )
                resp2 = api.place_order(
                    inst_id=inst_id, margin_mode=MARGIN_MODE,
                    position_side=POSITION_SIDE,
                    side="sell", order_type="market",
                    size=str(contracts), reduce_only=True,
                )
                data2 = resp2.get("data") or [{}]
                log.info(f"  ✅ {inst_id}  fallback orderId={data2[0].get('orderId','?')}")
        except Exception as e:
            log.error(f"  ❌ {inst_id}: {e} -- position may still be open!")
            failed.append(pos)

    if failed:
        alert(
            f"{len(failed)} position(s) FAILED to close on {utc_today()}: "
            f"{[p['inst_id'] for p in failed]}. Check BloFin manually.",
            subject="trader-blofin UNCLOSED POSITIONS"
        )
    return failed


# ==========================================================================
# MAIN SESSION
# ==========================================================================

def run_session(dry_run: bool = False, resume: bool = False):
    today = utc_today()
    start_ts = utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log.info("=" * 70)
    log.info(f"  SESSION {today}  "
             f"{'[DRY RUN]' if dry_run else '[LIVE]'}"
             f"{'  [RESUME]' if resume else ''}")
    log.info(f"  Started: {start_ts}")
    log.info("=" * 70)

    # ======================================================================
    # CRASH RECOVERY: restore from state file if --resume
    # ======================================================================
    if resume:
        state = load_state()
        if state.get("date") != today or state.get("phase") != "active":
            log.error(
                f"--resume requested but state shows "
                f"date={state.get('date')} phase={state.get('phase')}. "
                "Nothing to resume."
            )
            return

        log.info("Resuming from saved state ...")
        alert(
            f"Crash recovery triggered on {today}. Resuming monitoring loop from state.",
            subject="trader-blofin CRASH RECOVERY"
        )

        # Restore all session variables from state
        open_prices  = {k: float(v) for k, v in state["open_prices"].items()}
        entry_prices = {k: float(v) for k, v in state["entry_prices"].items()}
        entry_1x     = float(state["entry_1x"])
        eff_lev      = float(state["effective_leverage"])
        peak         = float(state.get("peak", 0.0))
        positions    = state["positions"]
        inst_ids     = list(open_prices.keys())
        today_date   = utcnow().date()

        api = build_api()

        log.info(
            f"Restored: {len(positions)} positions  "
            f"peak={peak*100:.3f}%  entry_1x={entry_1x*100:.4f}%  lev={eff_lev}x"
        )
        # Jump directly to monitoring loop
        _run_monitoring_loop(
            today, today_date, api, inst_ids,
            open_prices, entry_prices, entry_1x, eff_lev, peak,
            positions, dry_run
        )
        return

    # ======================================================================
    # NORMAL SESSION START
    # ======================================================================

    # Load signal (pre-session tail/dispersion filter already applied)
    symbols = get_today_symbols(today)
    if not symbols:
        log.info("A -- Filtered: return=0%  fees=none")
        log_daily_return(0.0, "filtered", session_date=today)
        save_state({"date": today, "phase": "filtered", "positions": []})
        return

    inst_ids = [s + SYMBOL_SUFFIX for s in symbols]

    api = build_api()

    # ── Phase 1 & 2: session-open snapshot + bar-6 conviction check ──────
    #
    # Prices are always fetched anchored to the correct UTC timestamps:
    #   open_prices : 06:00 UTC kline close (Binance 1-min historical)
    #   bar6_prices : 06:35 UTC kline close (Binance 1-min historical)
    #
    # This means roi_x is always the true 35-minute window return regardless
    # of when the script is invoked -- running at 08:00 gives the same result
    # as running at 06:01.
    #
    # Execution buffer (CONVICTION_EXEC_BUFFER_MIN = 5):
    #   If current time > 06:40 UTC, conviction is still computed and logged
    #   but the trade does NOT execute. The session window has passed.
    #
    # Timing:
    #   Before 06:35 UTC  -> sleep until 06:35, then fetch both timestamps
    #   At/after 06:35 UTC -> fetch both timestamps immediately

    _pre_sleep_date = utcnow().date()
    session_open  = datetime.datetime.combine(
        _pre_sleep_date, datetime.time(SESSION_START_HOUR, 0, 0)
    )
    conviction_dt = session_open + datetime.timedelta(
        minutes=CONVICTION_BAR * BAR_MINUTES + BAR_MINUTES  # 06:35 UTC
    )
    exec_cutoff_dt = conviction_dt + datetime.timedelta(
        minutes=CONVICTION_EXEC_BUFFER_MIN                  # 06:40 UTC
    )

    # Sleep until 06:35 if we haven't reached it yet
    sleep_until(conviction_dt, "bar-6 conviction 06:35 UTC")
    today_date = utcnow().date()
    now        = utcnow()

    # Check execution buffer before spending time on price fetches
    past_cutoff = now > exec_cutoff_dt
    if past_cutoff:
        log.warning(
            f"Started at {now.strftime('%H:%M:%S')} UTC -- "
            f"past execution cutoff (06:{35 + CONVICTION_EXEC_BUFFER_MIN:02d} UTC). "
            "Conviction will be computed and logged but trade will NOT execute."
        )

    # Fetch open prices at 06:00 UTC from Binance 1-min kline history
    log.info("Fetching 06:00 UTC open prices from Binance kline history ...")
    open_prices = _get_prices_at_timestamp(inst_ids, session_open)

    # Fetch bar-6 prices at 06:35 UTC from Binance 1-min kline history
    log.info("Fetching 06:35 UTC bar-6 prices from Binance kline history ...")
    bar6_prices = _get_prices_at_timestamp(inst_ids, conviction_dt)

    # Fall back to live prices for any symbol missing from kline history
    # (very new listings may not have history at the exact timestamp)
    missing = [i for i in inst_ids if i not in open_prices or i not in bar6_prices]
    if missing:
        log.warning(f"  {len(missing)} symbols missing from kline history -- "
                    f"falling back to live prices: {missing}")
        live = get_mark_prices(api, missing)
        for i in missing:
            if i not in open_prices  and i in live: open_prices[i]  = live[i]
            if i not in bar6_prices  and i in live: bar6_prices[i]  = live[i]

    if not open_prices:
        log.error("Could not fetch session-open prices -- aborting.")
        return

    # Narrow inst_ids to symbols with both prices (required for roi_x)
    inst_ids = [i for i in inst_ids if i in open_prices and i in bar6_prices]
    log.info(f"Open  prices ({len(open_prices)}): "
             f"{ {k: f'${v:.4f}' for k, v in open_prices.items()} }")
    log.info(f"Bar-6 prices ({len(bar6_prices)}): "
             f"{ {k: f'${v:.4f}' for k, v in bar6_prices.items()} }")

    roi_x = equal_weight_return(bar6_prices, open_prices)
    log.info(f"  roi_x={roi_x*100:.4f}%  KILL_Y={KILL_Y*100:.1f}%")

    # Store conviction result on the daily_signals row for the indexer UI
    try:
        import psycopg2 as _pg2
        _cconn = _pg2.connect(
            host=os.environ.get("DB_HOST", "127.0.0.1"),
            user=os.environ.get("DB_USER", "quant"),
            password=os.environ.get("DB_PASSWORD", ""),
            dbname=os.environ.get("DB_NAME", "marketdata"),
        )
        _ccur = _cconn.cursor()
        _ccur.execute("""
            UPDATE user_mgmt.daily_signals
            SET conviction_roi_x = %s,
                conviction_kill_y = %s,
                conviction_passed = %s
            WHERE signal_date = %s
        """, (round(roi_x * 100, 4), round(KILL_Y * 100, 4), roi_x >= KILL_Y, today))
        _cconn.commit()
        _ccur.close()
        _cconn.close()
    except Exception as _ce:
        log.warning(f"  Failed to store conviction: {_ce}")

    if past_cutoff:
        log.info(
            f"  MISSED WINDOW: roi_x={roi_x*100:.4f}%  conviction={'PASS' if roi_x >= KILL_Y else 'FAIL'}  "
            f"but execution window closed at 06:{35 + CONVICTION_EXEC_BUFFER_MIN:02d} UTC -- no trade"
        )
        log_daily_return(0.0, "missed_window", session_date=today)
        save_state({"date": today, "phase": "missed_window",
                    "roi_x": round(roi_x, 6), "positions": []})
        return

    if roi_x < KILL_Y:
        log.info(
            f"  B -- No Entry: roi_x={roi_x*100:.4f}% < {KILL_Y*100:.1f}%  "
            f"return=0%  fees=none  sentinel: margin=-1.0"
        )
        log_daily_return(0.0, "no_entry_conviction", session_date=today)
        save_state({"date": today, "phase": "no_entry",
                    "roi_x": round(roi_x, 6), "positions": []})
        return

    log.info(f"  Conviction passed -- entering at L_HIGH={L_HIGH} x VOL_boost")

    # ── Phase 3: pre-entry checks ─────────────────────────────────────────

    # Check for existing open positions before entry.
    # Stale positions from a previous session (failed close, crash) are closed
    # automatically before entering today's positions.
    # Note: --resume is handled earlier and never reaches this point.
    log.info("Checking for existing open positions on BloFin ...")
    existing = get_actual_positions(api, inst_ids)
    if existing:
        alert(
            f"{len(existing)} stale position(s) found before entry: "
            f"{list(existing.keys())}. Closing them now before entering today's session.",
            subject="trader-blofin STALE POSITIONS CLOSED"
        )
        log.warning(f"Stale positions found -- closing before entry: {list(existing.keys())}")
        stale = [{"inst_id": iid, "contracts": int(sz),
                  "marginMode": MARGIN_MODE, "positionSide": POSITION_SIDE}
                 for iid, sz in existing.items()]
        failed_stale = close_all_positions(api, stale, "pre_entry_cleanup", dry_run)
        if failed_stale:
            alert(
                f"Could not close stale positions: {[p['inst_id'] for p in failed_stale]}. "
                "Aborting entry to avoid double exposure.",
                subject="trader-blofin PRE-ENTRY ABORT"
            )
            log.error("Aborting -- could not close all stale positions.")
            save_state({
                "date": today, "phase": "stale_close_failed",
                "positions": failed_stale,
                "unclosed_count": len(failed_stale),
            })
            return
        log.info("  Stale positions closed -- proceeding with today's entry.")
        save_state({"date": today, "phase": "stale_closed", "positions": []})
    else:
        log.info("  Pre-entry check passed -- no existing positions found.")

    # Check state file -- if today's session already has active positions,
    # something is wrong (double-launch, cron misfire, etc.)
    _existing_state = load_state()
    if (_existing_state.get("date") == today
            and _existing_state.get("phase") == "active"
            and _existing_state.get("positions")):
        alert(
            f"State file shows active positions for {today} but --resume was not passed. "
            "Possible double-launch. Aborting to avoid duplication.",
            subject="trader-blofin DOUBLE-LAUNCH DETECTED"
        )
        log.error("State file shows active session -- use --resume or check for duplicate process.")
        return

    log.info("  Pre-entry check passed -- no existing positions found.")

    # ── Phase 3: compute leverage and enter ───────────────────────────────
    boost   = compute_vol_boost()
    eff_lev = round(L_HIGH * boost, 4)
    log.info(f"Effective leverage: {L_HIGH} x {boost:.3f} = {eff_lev:.3f}x")

    balance = get_account_balance_usdt(api)
    log.info(f"USDT available: ${balance:,.2f}")

    entry_prices = bar6_prices
    entry_1x     = roi_x
    peak         = 0.0

    positions = enter_positions(
        api, inst_ids, entry_prices, eff_lev, balance, dry_run
    )
    if not positions:
        log.error("No positions entered -- aborting.")
        return

    save_state({
        "date": today, "phase": "active",
        "entry_1x": round(entry_1x, 6), "effective_leverage": eff_lev, "peak": peak,
        "open_prices":  {k: v for k, v in open_prices.items()},
        "entry_prices": {k: v for k, v in entry_prices.items()},
        "positions": positions,
    })

    # ── Phase 4+5: monitoring loop ────────────────────────────────────────
    _run_monitoring_loop(
        today, today_date, api, inst_ids,
        open_prices, entry_prices, entry_1x, eff_lev, peak,
        positions, dry_run
    )


def _run_monitoring_loop(today, today_date, api: BlofinREST, inst_ids,
                         open_prices, entry_prices, entry_1x, eff_lev,
                         peak, positions, dry_run):
    """
    Intraday monitoring loop (bars 7-216).

    Matches rebuild_portfolio_matrix.py + audit.py stop structure exactly:

    Per-symbol stop (from rebuild_portfolio_matrix.py apply_raw_stop):
      STOP_RAW_PCT = -6% unleveraged per symbol.
      When a symbol's individual return hits <= -6%, it is closed immediately
      and its contribution to the portfolio average is clamped at -6% for all
      subsequent bars. This matches the matrix builder exactly.

    Per-bar portfolio check order (from audit.py simulate()):
      1st PORT_SL   : portfolio incr <= -6%       -> C: Hard Stop (close all)
      2nd PORT_TSL  : (incr-peak) <= -7.5%        -> D: Trail Stop (close all)
      3rd EARLY_FILL: sess_ret >= 9% (bars 7-143) -> E: Early Fill (close all)

    incr is computed over the ACTIVE pool only, with stopped symbols
    contributing their clamped -6% value -- matching the matrix builder.
    """
    session_open = datetime.datetime.combine(
        today_date, datetime.time(SESSION_START_HOUR, 0, 0)
    )
    session_close_dt = datetime.datetime.combine(today_date, datetime.time(23, 55, 0))
    fill_gate_dt     = session_open + datetime.timedelta(
        minutes=FILL_MAX_BAR * BAR_MINUTES + BAR_MINUTES   # 720 min = 18:00 UTC
    )

    exit_reason     = None
    final_return_1x = 0.0

    # Per-symbol stop tracking (matches apply_raw_stop in rebuild_portfolio_matrix.py)
    # sym_stopped: {inst_id: clamped_return}  -- symbols closed by per-symbol stop
    # Once stopped, a symbol contributes PORT_SL_PCT (-6%) to the portfolio average.
    sym_stopped = {}   # {inst_id: PORT_SL_PCT}
    active_positions = list(positions)   # shrinks as per-symbol stops fire

    log.info("Entering 5-min monitoring loop ...")

    while utcnow() < session_close_dt:
        sleep_until_next_bar()
        if utcnow() >= session_close_dt:
            break

        b       = bar_index()
        current = get_mark_prices(api, list(inst_ids))
        if not current:
            log.warning(f"Bar {b}: all price fetches failed -- skipping bar")
            continue

        # ── Per-symbol stop check ──────────────────────────────────────────
        # Check each still-active symbol against its individual -6% stop.
        # Matches apply_raw_stop: once raw return <= STOP_RAW_PCT, close and clamp.
        newly_stopped = []
        for pos in list(active_positions):
            iid   = pos["inst_id"]
            ref   = entry_prices.get(iid, 0)
            price = current.get(iid, 0)
            if not ref or not price:
                continue
            sym_ret = price / ref - 1.0
            if sym_ret <= PORT_SL_PCT:
                log.warning(
                    f"  SYM STOP: {iid} ret={sym_ret*100:.3f}% <= {PORT_SL_PCT*100:.0f}%"
                    f" -- closing symbol and clamping at {PORT_SL_PCT*100:.0f}%"
                )
                sym_stopped[iid] = PORT_SL_PCT
                newly_stopped.append(pos)
                active_positions = [p for p in active_positions if p["inst_id"] != iid]

        if newly_stopped and not dry_run:
            failed = close_all_positions(api, newly_stopped, "sym_stop", dry_run)
            if failed:
                # If close failed, keep in active pool and don't clamp
                for p in failed:
                    del sym_stopped[p["inst_id"]]
                    active_positions.append(p)
                    log.warning(f"  {p['inst_id']}: sym stop close failed -- keeping in pool")

        # ── Portfolio return (clamped + active symbols) ────────────────────
        # Mirrors rebuild_portfolio_matrix.py: stopped symbols contribute
        # their clamped value; active symbols use current live return.
        sym_returns = []
        for iid in inst_ids:
            if iid in sym_stopped:
                sym_returns.append(sym_stopped[iid])   # clamped at -6%
            elif iid in current and entry_prices.get(iid, 0) > 0:
                sym_returns.append(current[iid] / entry_prices[iid] - 1.0)

        incr = float(np.mean(sym_returns)) if sym_returns else 0.0

        # session_ret uses open_prices anchor (for EARLY_FILL threshold)
        session_ret = equal_weight_return(current, open_prices)

        peak     = max(peak, incr)
        tsl_dist = incr - peak
        fill_open = utcnow() <= fill_gate_dt

        log.info(
            f"Bar {b:3d} | incr={incr*100:+.3f}%  peak={peak*100:.3f}%  "
            f"tsl={tsl_dist*100:+.3f}%  sess={session_ret*100:+.3f}%  "
            f"active={len(active_positions)}/{len(positions)}  "
            f"stopped={len(sym_stopped)}  "
            f"fill={'open' if fill_open else 'closed'}"
        )

        # ── 1st: PORT_SL -- portfolio hard floor ───────────────────────────
        if incr <= PORT_SL_PCT:
            log.warning(
                f"  C -- HARD STOP: portfolio incr={incr*100:.3f}% <= {PORT_SL_PCT*100:.0f}%"
            )
            exit_reason = "port_sl"
            final_return_1x = incr
            break

        # ── 2nd: PORT_TSL -- trailing stop ─────────────────────────────────
        if tsl_dist <= PORT_TSL_PCT:
            tsl_exit = peak + PORT_TSL_PCT
            log.warning(
                f"  D -- TRAIL STOP: (incr-peak)={tsl_dist*100:.3f}%  "
                f"exit at {tsl_exit*100:.3f}%  "
                f"({'loss' if tsl_exit < 0 else 'profit'})"
            )
            exit_reason = "port_tsl"
            final_return_1x = tsl_exit
            break

        # ── 3rd: EARLY_FILL (bars 7-143 only) ─────────────────────────────
        if fill_open and session_ret >= EARLY_FILL_Y:
            fill_ret = session_ret - entry_1x
            log.info(
                f"  E -- EARLY FILL: sess={session_ret*100:.3f}% >= {EARLY_FILL_Y*100:.0f}%  "
                f"return from entry={fill_ret*100:.3f}%"
            )
            exit_reason = "early_fill"
            final_return_1x = fill_ret
            break

        save_state({
            "date": today, "phase": "active", "bar": b,
            "incr": round(incr, 6), "peak": round(peak, 6),
            "session_ret": round(session_ret, 6),
            "sym_stopped": list(sym_stopped.keys()),
            "effective_leverage": eff_lev, "entry_1x": round(entry_1x, 6),
            "open_prices":  {k: v for k, v in open_prices.items()},
            "entry_prices": {k: v for k, v in entry_prices.items()},
            "positions": active_positions,
        })

    # ── Phase 5: Exit ─────────────────────────────────────────────────────
    if exit_reason is None:
        current = get_mark_prices(api, inst_ids)
        if current:
            final_return_1x = equal_weight_return(current, entry_prices)
            log.info(
                f"F -- SESSION CLOSE (23:55 UTC)  "
                f"return from entry={final_return_1x*100:+.3f}%  (unbounded)"
            )
        else:
            final_return_1x = float("nan")
            log.warning("F -- SESSION CLOSE: price fetch failed -- not logging return")
        exit_reason = "session_close"

    # Close remaining active positions (sym_stopped symbols already closed above)
    failed_closes = close_all_positions(api, active_positions, exit_reason, dry_run)

    net_pct = final_return_1x * eff_lev * 100
    log.info(
        f"Session done -- {exit_reason}  "
        f"return_1x={final_return_1x*100 if not math.isnan(final_return_1x) else 'nan':+.3f}%  "
        f"lev={eff_lev:.3f}x  net~{net_pct if not math.isnan(net_pct) else 'nan':+.3f}%"
    )

    if not math.isnan(final_return_1x):
        log_daily_return(net_pct, exit_reason, session_date=today, eff_lev=eff_lev)
    else:
        log.warning("Return not logged (NaN -- price unavailable at close)")

    save_state({
        "date": today, "phase": "closed",
        "exit_reason": exit_reason,
        "final_return_1x_pct": round(final_return_1x * 100, 4) if not math.isnan(final_return_1x) else None,
        "effective_leverage": eff_lev,
        "net_return_approx_pct": round(net_pct, 4) if not math.isnan(net_pct) else None,
        "positions": failed_closes,
        "unclosed_count": len(failed_closes),
    })
    log.info("=" * 70)


# ==========================================================================
# RETURNS LOG SEEDING  (--seed-returns)
# ==========================================================================

def seed_returns_log(filepath: str):
    """
    Seed blofin_returns_log.csv from a CSV file containing historical
    daily returns. Allows the VOL boost engine to calibrate correctly
    from day 1 instead of warming up over 30 live sessions.

    Expected CSV columns: date (YYYY-MM-DD), net_return_pct (float)
    Optional column:      exit_reason (string)
    """
    src = Path(filepath)
    if not src.exists():
        log.error(f"Seed file not found: {src}")
        sys.exit(1)

    df = pd.read_csv(src)
    required = {"date", "net_return_pct"}
    missing  = required - set(df.columns)
    if missing:
        log.error(f"Seed file missing required columns: {missing}")
        sys.exit(1)

    if "exit_reason" not in df.columns:
        df["exit_reason"] = "seeded"

    df = df[["date", "net_return_pct", "exit_reason"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    df.to_csv(RETURNS_LOG, index=False)
    log.info(
        f"Returns log seeded from {src}: {len(df)} rows  "
        f"({df['date'].iloc[0]} to {df['date'].iloc[-1]})\n"
        f"  Saved to: {RETURNS_LOG}"
    )


# ==========================================================================
# ENTRY POINT
# ==========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BloFin live executor -- Overlap Strategy")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Simulate full session without sending orders")
    parser.add_argument("--resume",       action="store_true",
                        help="Resume monitoring loop from saved state after a crash")
    parser.add_argument("--status",       action="store_true",
                        help="Print current state file and exit")
    parser.add_argument("--seed-returns", metavar="FILE",
                        help="Seed returns log from a CSV to warm up VOL boost engine")
    parser.add_argument("--close-all",    action="store_true",
                        help="Force-close all open positions on BloFin and exit")
    args = parser.parse_args()

    if args.status:
        print(json.dumps(load_state(), indent=2, default=str))
        sys.exit(0)

    if args.seed_returns:
        seed_returns_log(args.seed_returns)
        sys.exit(0)

    if args.close_all:
        api = build_api()
        log.info("--close-all: fetching all open positions ...")
        actual = get_actual_positions(api, [])   # empty list = fetch all, filter none
        if not actual:
            log.info("No open positions found.")
            sys.exit(0)
        log.info(f"Found {len(actual)} open position(s): {list(actual.keys())}")
        positions = [{"inst_id": iid, "contracts": int(sz),
                      "marginMode": MARGIN_MODE, "positionSide": POSITION_SIDE}
                     for iid, sz in actual.items()]
        failed = close_all_positions(api, positions, "manual_close_all", dry_run=False)
        if failed:
            log.error(f"Failed to close: {[p['inst_id'] for p in failed]}")
            sys.exit(1)
        log.info("All positions closed successfully.")
        sys.exit(0)

    acquire_lock()
    try:
        run_session(dry_run=args.dry_run, resume=args.resume)
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
        alert("Session interrupted by KeyboardInterrupt. Check for open positions.",
              subject="trader-blofin INTERRUPTED")
    except Exception as e:
        log.exception(f"Unhandled exception: {e}")
        alert(f"Unhandled exception: {e}. Session may have open positions.",
              subject="trader-blofin CRASH")
        raise
    finally:
        release_lock()

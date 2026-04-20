#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backend/app/cli/trader_blofin.py
================================
Containerized copy of /root/benji/trader-blofin.py. Part 1 of the multi-tenant
refactor: same execution logic, adapted for the backend container (no secrets
self-load, paths anchored to /mnt/quant-data/trader, vendored BloFin auth,
credential guard deferred to run_session entry).

Master-account host cron still runs the original /root/benji/trader-blofin.py;
this copy currently exists only as groundwork for Part 2's per-allocation loop.
Running it live against the master account would duplicate trading activity —
--dry-run only for now.

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
import hashlib
from contextlib import contextmanager
from urllib.parse import urlencode
import numpy as np
import pandas as pd
from pathlib import Path

# Vendored BloFin auth — lives in the backend package so the container doesn't
# need the pipeline venv on sys.path. Reads env vars at call time, so it's
# safe to import before creds are present.
from app.services.trading.blofin_auth import get_headers as _shared_get_headers

# Multi-tenant allocation mode — only used when --allocation-id is set.
# Master-account execution path doesn't touch any of these.
from app.services.trading.trader_config import TraderConfig
from app.services.trading.credential_loader import (
    load_credentials,
    CredentialDecryptError,
)

# ==========================================================================
# CONFIGURATION
# ==========================================================================

# -- API credentials -------------------------------------------------------
# Read at module load for visibility, but the "are these real" guard is
# deferred to run_session() — that way --status, --close-all, and import
# succeed in environments (e.g. the backend container) where BloFin creds
# aren't set. The host-cron script still fails loudly before any HTTP call
# because run_session is what every trade-touching mode enters.
API_KEY    = os.environ.get("BLOFIN_API_KEY",    "YOUR_API_KEY")
API_SECRET = os.environ.get("BLOFIN_API_SECRET", "YOUR_API_SECRET")
PASSPHRASE = os.environ.get("BLOFIN_PASSPHRASE", "YOUR_PASSPHRASE")


def _require_blofin_credentials() -> None:
    """Validate API credentials are real (not placeholder). Raises at the
    same logical point the original module-level guard did in the host
    script: before any trade-path HTTP call."""
    if "YOUR_" in API_KEY or "YOUR_" in API_SECRET or not PASSPHRASE:
        raise RuntimeError("BloFin API credentials not set — check secrets.env")

DEMO_MODE  = False   # True -> BloFin paper trading environment

# -- Session timing (must match backtest) ----------------------------------
SESSION_START_HOUR = 6
BAR_MINUTES        = 5
CONVICTION_BAR     = 6        # bar-6 closes at 35 min (06:35 UTC)
FILL_MAX_BAR       = 143      # EARLY_FILL gate closes at bar 143 (18:00 UTC)

# -- Strategy parameters (must match backtest exactly) ---------------------
L_HIGH       = 2.3     # base leverage floor; L_BASE=0 never selected
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
# Containerized copy: artifacts live under /mnt/quant-data/trader/ (bind-
# mounted into the backend service). Keeps them operator-readable via the
# same path pattern the host script uses for its own files.
# The signal CSV is still produced by daily_signal.py on the host; the
# containerized copy reads it via the bind mount.
_TRADER_DATA_DIR = Path("/mnt/quant-data/trader")
_TRADER_DATA_DIR.mkdir(parents=True, exist_ok=True)
# Host daily_signal.py writes to /root/benji/live_deploys_signal.csv, read-only
# bind-mounted into the container at /host_trader/.
DEPLOYS_CSV   = Path("/host_trader") / "live_deploys_signal.csv"
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
# All trader artifacts land in /mnt/quant-data/trader/ (bind-mounted into
# the backend container). This is a parallel space to the host script's
# /root/benji/ artifacts — they coexist without collision.
STATE_FILE    = _TRADER_DATA_DIR / "blofin_executor_state.json"
RETURNS_LOG   = _TRADER_DATA_DIR / "blofin_returns_log.csv"
ALERTS_LOG    = _TRADER_DATA_DIR / "blofin_alerts.log"
LOCK_FILE     = _TRADER_DATA_DIR / ".trader_session.lock"
REPORTS_DIR   = _TRADER_DATA_DIR / "blofin_execution_reports"
PORTFOLIO_DIR = REPORTS_DIR / "portfolios"

_SESSION_ALERT_COUNT = 0
LOG_FILE      = _TRADER_DATA_DIR / "blofin_executor.log"


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
    global _SESSION_ALERT_COUNT
    _SESSION_ALERT_COUNT += 1
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


def write_execution_report(report: dict):
    """
    Persist a per-session execution report as
    blofin_execution_reports/YYYY-MM-DD.json.
    Best-effort: swallows IOError and logs a warning so reporting
    never blocks the trading session.
    """
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        date = report.get("date") or utc_today()
        path = REPORTS_DIR / f"{date}.json"
        with open(path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        log.info(f"  Execution report written: {path}")
    except Exception as e:
        log.warning(f"  Could not write execution report: {e}")


# ── Portfolio time series (bar-by-bar NDJSON) ───────────────────────────────
# Schema: first line is {"type":"meta", ...}, then per-bar lines
# {"type":"bar", ...}, then a final {"type":"meta_update", status:"closed", ...}
# at session end. Reader merges meta_update fields into the initial meta.
# Append-only writes ensure the file is always parseable mid-session — the UI
# can poll while session.status == "active".

def _portfolio_path(date: str) -> Path:
    return PORTFOLIO_DIR / f"{date}.ndjson"


def init_portfolio_record(date: str, symbols: list, entered: list,
                          session_start_utc: str,
                          eff_lev: float, lev_int: int) -> None:
    """
    Write the initial meta line for a new portfolio record. Idempotent: if the
    file already exists (e.g. --resume after crash), preserves the existing
    timeline and meta — no overwrite.

    `symbols` is the full signaled+priceable universe; `entered` is the subset
    that actually got positions on the exchange (the difference = failed entry).
    """
    try:
        PORTFOLIO_DIR.mkdir(parents=True, exist_ok=True)
        path = _portfolio_path(date)
        if path.exists():
            return
        meta = {
            "type":              "meta",
            "date":              date,
            "status":            "active",
            "session_start_utc": session_start_utc,
            "exit_time_utc":     None,
            "symbols":           list(symbols),
            "entered":           list(entered),
            "eff_lev":           eff_lev,
            "lev_int":           lev_int,
            "exit_reason":       None,
        }
        with open(path, "w") as f:
            f.write(json.dumps(meta, default=str) + "\n")
    except Exception as e:
        log.warning(f"  Could not init portfolio record: {e}")


def append_portfolio_bar(date: str, bar: int, ts: str,
                         incr: float, peak: float,
                         sym_returns: dict, stopped: list) -> None:
    """
    Append one bar's snapshot. Best-effort — never raises so a write hiccup
    can't crash the monitoring loop.
    """
    try:
        line = {
            "type":        "bar",
            "bar":         bar,
            "ts":          ts,
            "incr":        round(incr, 6),
            "peak":        round(peak, 6),
            "sym_returns": {k: round(float(v), 6) for k, v in sym_returns.items()},
            "stopped":     list(stopped),
        }
        with open(_portfolio_path(date), "a") as f:
            f.write(json.dumps(line, default=str) + "\n")
    except Exception as e:
        log.warning(f"  Could not append portfolio bar: {e}")


def update_portfolio_meta(date: str, **fields) -> None:
    """
    Append a meta_update line. The reader merges these fields into the initial
    meta in-order, so the latest value wins per field.
    """
    try:
        line = {"type": "meta_update", **fields}
        with open(_portfolio_path(date), "a") as f:
            f.write(json.dumps(line, default=str) + "\n")
    except Exception as e:
        log.warning(f"  Could not update portfolio meta: {e}")


# ── Portfolio time series (SQL mirror, primary store) ───────────────────────
# The trader writes to both NDJSON (above) and Postgres for every hook point.
# These run INDEPENDENTLY and are both best-effort: a DB outage does not block
# the NDJSON write, and an NDJSON I/O error does not block the DB write. The
# Manager UI reads only from SQL; NDJSON remains a local backup.

def _trader_db_connect():
    """
    Short-lived DB connection for trader-side writes. Matches the existing
    conviction-write pattern already used in run_session. 5s connect timeout
    so a DB outage can't hang the 5-minute bar loop.
    """
    import psycopg2
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        user=os.environ.get("DB_USER", "quant"),
        password=os.environ.get("DB_PASSWORD", ""),
        dbname=os.environ.get("DB_NAME", "marketdata"),
        connect_timeout=5,
    )


def init_portfolio_session_sql(date: str, symbols: list, entered: list,
                               session_start_utc: str,
                               eff_lev: float, lev_int: int) -> None:
    """
    Insert the session row with status='active'. Idempotent via
    ON CONFLICT (signal_date) DO NOTHING so --resume after a crash is safe.
    session_start_utc is a "YYYY-MM-DD HH:MM:SS" UTC string.
    """
    try:
        start_dt = datetime.datetime.strptime(
            session_start_utc, "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
        conn = _trader_db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_mgmt.portfolio_sessions
                        (signal_date, session_start_utc, status,
                         symbols, entered, eff_lev, lev_int)
                    VALUES (%s, %s, 'active', %s, %s, %s, %s)
                    ON CONFLICT (signal_date) DO NOTHING
                """, (date, start_dt, list(symbols), list(entered),
                      eff_lev, lev_int))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"  Could not init portfolio session in DB: {e}")


def append_portfolio_bar_sql(date: str, bar: int, ts: str,
                             incr: float, peak: float,
                             sym_returns: dict, stopped: list) -> None:
    """
    INSERT the bar and UPDATE the session's cached summary fields in a single
    transaction. If the session row doesn't exist (init failed earlier), both
    statements silently no-op — NDJSON remains the backup-of-record.
    """
    try:
        bar_dt = datetime.datetime.strptime(
            ts, "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=datetime.timezone.utc)
        stopped_list = list(stopped)
        conn = _trader_db_connect()
        try:
            with conn.cursor() as cur:
                # Insert the bar row (idempotent on --resume via PK conflict).
                cur.execute("""
                    INSERT INTO user_mgmt.portfolio_bars
                        (portfolio_session_id, bar_number, bar_timestamp_utc,
                         portfolio_return, peak_return, symbol_returns, stopped)
                    SELECT portfolio_session_id, %s, %s, %s, %s, %s::jsonb, %s
                    FROM user_mgmt.portfolio_sessions
                    WHERE signal_date = %s
                    ON CONFLICT (portfolio_session_id, bar_number) DO NOTHING
                """, (bar, bar_dt, incr, peak,
                      json.dumps(sym_returns), stopped_list, date))

                # Refresh cached summary from truth table (idempotent).
                cur.execute("""
                    UPDATE user_mgmt.portfolio_sessions
                    SET bars_count             = (
                            SELECT COUNT(*)
                            FROM user_mgmt.portfolio_bars b
                            WHERE b.portfolio_session_id =
                                  portfolio_sessions.portfolio_session_id
                        ),
                        final_portfolio_return = %s,
                        peak_portfolio_return  = GREATEST(
                            COALESCE(peak_portfolio_return, %s), %s
                        ),
                        max_dd_from_peak       = LEAST(
                            COALESCE(max_dd_from_peak, 0), %s
                        ),
                        sym_stops              = ARRAY(
                            SELECT DISTINCT unnest(sym_stops || %s::text[])
                        ),
                        updated_at             = NOW()
                    WHERE signal_date = %s
                """, (incr, peak, peak, incr - peak, stopped_list, date))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"  Could not append portfolio bar in DB: {e}")


def update_portfolio_meta_sql(date: str, **fields) -> None:
    """
    UPDATE portfolio_sessions with the given fields. Only known columns are
    persisted — unknown keys are silently ignored so the caller can share the
    same kwargs it passes to the NDJSON update_portfolio_meta.
    """
    ALLOWED = {
        "status":        "status",
        "exit_reason":   "exit_reason",
        "exit_time_utc": "exit_time_utc",
    }
    updates = []
    params: list = []
    for k, v in fields.items():
        if k not in ALLOWED:
            continue
        if k == "exit_time_utc" and isinstance(v, str):
            v = datetime.datetime.strptime(
                v, "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=datetime.timezone.utc)
        updates.append(f"{ALLOWED[k]} = %s")
        params.append(v)
    if not updates:
        return
    updates.append("updated_at = NOW()")
    sql = f"""
        UPDATE user_mgmt.portfolio_sessions
        SET {", ".join(updates)}
        WHERE signal_date = %s
    """
    params.append(date)
    try:
        conn = _trader_db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"  Could not update portfolio meta in DB: {e}")


# ==========================================================================
# SIGNAL READER
# ==========================================================================

def get_today_symbols(date_str: str, active_filter: str | None = None) -> list:
    # Master path passes no active_filter → use module constant. Allocation
    # mode passes its per-strategy filter from TraderConfig.active_filter.
    # Behavior on the master path is bit-for-bit identical when called with
    # one positional arg, since active_filter falls through to ACTIVE_FILTER.
    filter_name = active_filter if active_filter is not None else ACTIVE_FILTER

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

    if filter_name:
        today_rows = [r for r in today_rows
                      if r.get("filter", "").strip().lower() == filter_name.strip().lower()]
        if not today_rows:
            log.info(f"Filter '{filter_name}' blocked today -> A: Filtered (0%, no fees)")
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

    def get_fills_history(self, inst_type: str = "SWAP") -> dict:
        """
        Fetch recent trade fills. Used to reconcile exit fill prices after
        close_all_positions, when positions are no longer visible via
        /api/v1/account/positions.
        BloFin supports /api/v1/trade/fills-history (historical) and
        /api/v1/trade/fills (recent). We try fills-history first.
        """
        resp = self.request("GET", "/api/v1/trade/fills-history",
                            params={"instType": inst_type})
        code = str(resp.get("code", ""))
        if code not in ("0", ""):
            # Fallback to /api/v1/trade/fills if -history is not available
            resp = self.request("GET", "/api/v1/trade/fills",
                                params={"instType": inst_type})
        return resp


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
                    eff_lev, balance, dry_run) -> tuple:
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

    Returns (positions_list, fill_report_dict). fill_report_dict has:
      - usdt_total, usdt_deployable, usdt_per_symbol, margin_buffer_pct
      - fills: {total_symbols, filled_first_pass, filled_via_retry, failed,
                fill_rate_pct, symbols[]}
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
        return [], {
            "usdt_total": round(usdt_total, 2),
            "usdt_deployable": 0.0,
            "usdt_per_symbol": 0.0,
            "margin_buffer_pct": 10,
            "fills": {
                "total_symbols": 0, "filled_first_pass": 0,
                "filled_via_retry": 0, "failed": 0,
                "fill_rate_pct": 0.0, "symbols": [],
            },
        }

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

    # Per-symbol fill tracking. Keyed by inst_id so partial retries across
    # rounds aggregate into one entry (prevents duplicate close attempts).
    symbol_status = {}   # inst_id -> per-symbol report dict
    opened_by_sym = {}   # inst_id -> position dict (unique per symbol)
    failed = []
    filled_first_pass = 0
    filled_via_retry_syms = set()

    for inst_id in tradeable:
        price = entry_prices[inst_id]
        symbol_status[inst_id] = {
            "inst_id":            inst_id,
            "target_contracts":   0,
            "filled_contracts":   0,
            "fill_pct":           0.0,
            "est_entry_price":    round(price, 6),
            "fill_entry_price":   None,
            "entry_slippage_bps": None,
            "order_ids":          [],
            "retry_rounds":       0,
            "skipped_reason":     None,
            "lev_int":            lev_int,
            "eff_lev":            eff_lev,
            "ctval":              0.0,
            "notional_usd":       0.0,
        }

        # Fetch instrument constraints (confirmed from mtrader2.py)
        info    = get_instrument_info(api, inst_id)
        ctval   = info["contractValue"]
        lot     = info["lotSize"]
        min_sz  = info["minSize"]
        max_mkt = info["maxMarketSize"]
        state   = info["state"]
        symbol_status[inst_id]["ctval"] = ctval

        # Skip suspended/delisted instruments
        if str(state).lower() not in ("live", ""):
            log.warning(f"  {inst_id}: state={state} -- skipping")
            symbol_status[inst_id]["skipped_reason"] = f"state_{state}"
            continue

        # Compute and lot-round contracts
        # Use eff_lev (e.g. 1.33x) for notional sizing -- this is the true
        # target leverage. lev_int (e.g. 2x) is only the exchange setting,
        # which must be an integer >= eff_lev but should NOT drive sizing.
        raw_contracts = usdt_per_sym * eff_lev / (price * ctval)
        contracts     = _round_to_lot(raw_contracts, lot)
        symbol_status[inst_id]["target_contracts"] = contracts

        if contracts < min_sz:
            log.warning(
                f"  {inst_id}: computed {contracts} contracts < minSize {min_sz} "
                f"(price=${price:,.4f} ctval={ctval} lot={lot}) -- skipping"
            )
            symbol_status[inst_id]["skipped_reason"] = (
                f"below_min_size ({contracts}<{min_sz})"
            )
            continue

        notional = contracts * price * ctval
        log.info(
            f"  {inst_id}: {contracts}ct  price=${price:,.4f}  "
            f"ctval={ctval}  lot={lot}  notional=${notional:,.0f}"
        )

        if dry_run:
            opened_by_sym[inst_id] = {
                "inst_id": inst_id, "contracts": contracts,
                "entry_price": price, "order_id": "DRY_RUN",
                "lev_int": lev_int,
                "marginMode": MARGIN_MODE,
                "positionSide": POSITION_SIDE,
                "target_contracts": contracts,
                "retry_rounds": 0,
            }
            symbol_status[inst_id]["filled_contracts"] = contracts
            symbol_status[inst_id]["fill_pct"] = 100.0
            symbol_status[inst_id]["order_ids"].append("DRY_RUN")
            filled_first_pass += 1
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
                symbol_status[inst_id]["skipped_reason"] = (
                    f"set_leverage_rejected_{lev_code}"
                )
                continue
        except Exception as le:
            log.error(f"  {inst_id}: set_leverage exception: {le} -- skipping")
            symbol_status[inst_id]["skipped_reason"] = "set_leverage_exception"
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
            opened_by_sym[inst_id] = {
                "inst_id": inst_id, "contracts": contracts,
                "entry_price": price, "order_id": order_id,
                "lev_int": lev_int,
                "marginMode": MARGIN_MODE,
                "positionSide": POSITION_SIDE,
                "target_contracts": contracts,
                "retry_rounds": 0,
            }
            symbol_status[inst_id]["filled_contracts"] = contracts
            symbol_status[inst_id]["fill_pct"] = 100.0
            symbol_status[inst_id]["order_ids"].append(order_id)
            filled_first_pass += 1
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
                if symbol_status[inst_id]["skipped_reason"] is None:
                    symbol_status[inst_id]["skipped_reason"] = "retry_half_below_min_size"
                continue

            sl_trigger = None
            if EXCHANGE_SL_ENABLED:
                sl_trigger = f"{price * (1 + EXCHANGE_SL_PCT):.8g}"

            filled_total = 0.0
            round_order_ids = []
            ok1, order_id1 = _place_order_chunked(
                api, inst_id, half, lot, min_sz, max_mkt,
                sl_trigger_price=sl_trigger,
            )
            if ok1:
                log.info(f"  ✅ {inst_id} retry half-1 {half}ct orderId={order_id1}")
                filled_total += half
                round_order_ids.append(order_id1)
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
                        round_order_ids.append(order_id2)
                    else:
                        log.error(f"  ❌ {inst_id}: retry half-2 failed")
            else:
                log.error(f"  ❌ {inst_id}: retry half-1 failed")

            if filled_total > 0:
                filled_via_retry_syms.add(inst_id)
                if inst_id in opened_by_sym:
                    opened_by_sym[inst_id]["contracts"] += filled_total
                    opened_by_sym[inst_id]["retry_rounds"] = retry_round
                else:
                    opened_by_sym[inst_id] = {
                        "inst_id": inst_id, "contracts": filled_total,
                        "entry_price": price, "order_id": round_order_ids[0],
                        "lev_int": item["lev_int"],
                        "marginMode": MARGIN_MODE,
                        "positionSide": POSITION_SIDE,
                        "target_contracts": symbol_status[inst_id]["target_contracts"],
                        "retry_rounds": retry_round,
                    }
                symbol_status[inst_id]["filled_contracts"] += filled_total
                symbol_status[inst_id]["retry_rounds"] = retry_round
                symbol_status[inst_id]["order_ids"].extend(round_order_ids)

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
        if symbol_status[item["inst_id"]]["skipped_reason"] is None:
            symbol_status[item["inst_id"]]["skipped_reason"] = (
                f"retry_exhausted_{MAX_ENTRY_RETRIES}_rounds"
            )

    # Finalize per-symbol fill_pct and notional (using est_entry_price as the
    # basis; reconcile_fill_prices will overwrite notional_usd once actual fill
    # prices are known).
    for st in symbol_status.values():
        tgt = st["target_contracts"]
        if tgt > 0:
            st["fill_pct"] = round(st["filled_contracts"] / tgt * 100, 2)
        if st["filled_contracts"] > 0 and st["ctval"] and st["est_entry_price"]:
            st["notional_usd"] = round(
                st["filled_contracts"] * st["est_entry_price"] * st["ctval"], 2
            )

    failed_count = sum(
        1 for st in symbol_status.values()
        if st["filled_contracts"] == 0 and st["target_contracts"] > 0
    )
    total_syms = len(tradeable)
    filled_any = sum(
        1 for st in symbol_status.values() if st["filled_contracts"] > 0
    )
    fill_rate_pct = (filled_any / total_syms * 100) if total_syms > 0 else 0.0

    fill_report = {
        "usdt_total":        round(usdt_total, 2),
        "usdt_deployable":   round(usdt_deployable, 2),
        "usdt_per_symbol":   round(usdt_per_sym, 2),
        "margin_buffer_pct": 10,
        "fills": {
            "total_symbols":     total_syms,
            "filled_first_pass": filled_first_pass,
            "filled_via_retry":  len(filled_via_retry_syms),
            "failed":            failed_count,
            "fill_rate_pct":     round(fill_rate_pct, 2),
            "symbols":           list(symbol_status.values()),
        },
    }

    return list(opened_by_sym.values()), fill_report

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


def reconcile_fill_prices(api: BlofinREST, positions: list) -> list:
    """
    Fetch actual average-fill prices from BloFin positions and attach to
    each position dict. Computes slippage in basis points vs est_entry_price.
    Called once after all orders are placed (bulk, not per-symbol).
    Leaves fill_entry_price and entry_slippage_bps as None for any symbol
    that cannot be matched.
    """
    if not positions:
        return positions
    try:
        resp = api.get_positions()
    except Exception as e:
        log.warning(f"Could not fetch positions for fill reconciliation: {e}")
        for pos in positions:
            pos.setdefault("fill_entry_price", None)
            pos.setdefault("entry_slippage_bps", None)
        return positions

    by_inst = {}
    for row in (resp.get("data") or []):
        iid = row.get("instId", "")
        avg = (row.get("averagePrice")
               or row.get("avgPx")
               or row.get("averagePx"))
        if iid and avg not in (None, "", "0"):
            try:
                by_inst[iid] = float(avg)
            except (TypeError, ValueError):
                pass

    summary = []
    for pos in positions:
        iid  = pos["inst_id"]
        est  = pos.get("entry_price")
        fill = by_inst.get(iid)
        if fill is None or not est or est <= 0:
            pos["fill_entry_price"]   = None
            pos["entry_slippage_bps"] = None
            log.warning(f"  {iid}: entry fill price not found on BloFin -- leaving null")
            continue
        pos["fill_entry_price"]   = round(fill, 6)
        slip_bps                  = (fill / est - 1.0) * 10000
        pos["entry_slippage_bps"] = round(slip_bps, 2)
        summary.append(f"{iid} est=${est:,.4f} fill=${fill:,.4f} slip={slip_bps:+.2f}bps")

    if summary:
        log.info("  Fill prices: " + "  |  ".join(summary))

    return positions


def reconcile_exit_prices(api: BlofinREST, positions: list,
                          est_exit_prices: dict) -> list:
    """
    Fetch actual exit fill prices from BloFin trade-fills history and attach
    to each position dict. Computes exit slippage in basis points vs the
    monitoring loop's last price snapshot for that symbol.

    Matches by inst_id + side=sell, picking the most recent matching fill.
    Negative exit_slippage_bps is favorable (sold higher than estimated).
    """
    if not positions:
        return positions
    try:
        resp = api.get_fills_history()
    except Exception as e:
        log.warning(f"Could not fetch fills history for exit reconciliation: {e}")
        for pos in positions:
            pos.setdefault("fill_exit_price", None)
            pos.setdefault("exit_slippage_bps", None)
        return positions

    # Group latest sell fills per instId. Fill rows may include fields named
    # fillPrice/price/fillPx, side = "buy"/"sell", ts = epoch ms.
    fills_by_inst = {}
    for row in (resp.get("data") or []):
        iid  = row.get("instId", "")
        side = (row.get("side") or "").lower()
        if not iid or side != "sell":
            continue
        price_raw = (row.get("fillPrice")
                     or row.get("price")
                     or row.get("fillPx"))
        try:
            price = float(price_raw) if price_raw not in (None, "") else None
        except (TypeError, ValueError):
            price = None
        if price is None:
            continue
        ts_raw = row.get("ts") or row.get("fillTime") or row.get("cTime") or 0
        try:
            ts = int(ts_raw)
        except (TypeError, ValueError):
            ts = 0
        cur = fills_by_inst.get(iid)
        if cur is None or ts > cur[0]:
            fills_by_inst[iid] = (ts, price)

    summary = []
    for pos in positions:
        iid  = pos["inst_id"]
        est  = est_exit_prices.get(iid)
        hit  = fills_by_inst.get(iid)
        fill = hit[1] if hit else None
        if fill is None or not est or est <= 0:
            pos["fill_exit_price"]   = None
            pos["exit_slippage_bps"] = None
            log.warning(f"  {iid}: exit fill price not found in BloFin fills history -- leaving null")
            continue
        pos["fill_exit_price"]   = round(fill, 6)
        slip_bps                 = (fill / est - 1.0) * 10000
        pos["exit_slippage_bps"] = round(slip_bps, 2)
        summary.append(f"{iid} est=${est:,.4f} fill=${fill:,.4f} slip={slip_bps:+.2f}bps")

    if summary:
        log.info("  Exit prices: " + "  |  ".join(summary))

    return positions


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
    # Credential guard: fail loudly before any trade-path HTTP call. Matches
    # the logical position of the original module-level guard in the host
    # script — just moved into this function so that import-only entry
    # points (--status, --close-all with no creds, module tests) don't
    # require BloFin env set.
    _require_blofin_credentials()
    global _SESSION_ALERT_COUNT
    _SESSION_ALERT_COUNT = 0
    today = utc_today()
    session_start_utc = utcnow().strftime("%Y-%m-%d %H:%M:%S")
    start_ts = session_start_utc + " UTC"
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
            positions, dry_run,
            report_base=None, session_start_utc=session_start_utc,
            balance_pre_entry=None,
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
        write_execution_report({
            "date": today,
            "session_start_utc": session_start_utc,
            "session_end_utc":   utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "signal": {"symbols_signaled": [], "filter": ACTIVE_FILTER, "count": 0},
            "conviction": None,
            "exit": {"reason": "filtered"},
            "alerts_fired": _SESSION_ALERT_COUNT,
        })
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
        write_execution_report({
            "date": today,
            "session_start_utc": session_start_utc,
            "session_end_utc":   utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "signal": {"symbols_signaled": symbols,
                       "filter": ACTIVE_FILTER, "count": len(symbols)},
            "conviction": {
                "roi_x_pct":  round(roi_x * 100, 4),
                "kill_y_pct": round(KILL_Y * 100, 4),
                "passed":     roi_x >= KILL_Y,
            },
            "exit": {"reason": "missed_window"},
            "alerts_fired": _SESSION_ALERT_COUNT,
        })
        return

    if roi_x < KILL_Y:
        log.info(
            f"  B -- No Entry: roi_x={roi_x*100:.4f}% < {KILL_Y*100:.1f}%  "
            f"return=0%  fees=none  sentinel: margin=-1.0"
        )
        log_daily_return(0.0, "no_entry_conviction", session_date=today)
        save_state({"date": today, "phase": "no_entry",
                    "roi_x": round(roi_x, 6), "positions": []})
        write_execution_report({
            "date": today,
            "session_start_utc": session_start_utc,
            "session_end_utc":   utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "signal": {"symbols_signaled": symbols,
                       "filter": ACTIVE_FILTER, "count": len(symbols)},
            "conviction": {
                "roi_x_pct":  round(roi_x * 100, 4),
                "kill_y_pct": round(KILL_Y * 100, 4),
                "passed":     False,
            },
            "exit": {"reason": "no_entry_conviction"},
            "alerts_fired": _SESSION_ALERT_COUNT,
        })
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
    lev_int = max(1, int(math.ceil(eff_lev)))
    log.info(f"Effective leverage: {L_HIGH} x {boost:.3f} = {eff_lev:.3f}x")

    balance = get_account_balance_usdt(api)
    log.info(f"USDT available: ${balance:,.2f}")

    entry_prices = bar6_prices
    entry_1x     = roi_x
    peak         = 0.0

    balance_pre_entry = balance

    positions, fill_report = enter_positions(
        api, inst_ids, entry_prices, eff_lev, balance, dry_run
    )

    # Reconcile actual fill prices vs est_entry_price. Skip in dry-run since
    # BloFin won't have any positions to read.
    if positions and not dry_run:
        reconcile_fill_prices(api, positions)

    # Sync fill prices + slippage + notional into fill_report's per-symbol
    # records; compute avg entry slippage across filled symbols.
    sym_rec_by_id = {s["inst_id"]: s for s in fill_report["fills"]["symbols"]}
    entry_slips = []
    for pos in positions:
        iid = pos["inst_id"]
        st  = sym_rec_by_id.get(iid)
        if not st:
            continue
        fill_price = pos.get("fill_entry_price")
        slip       = pos.get("entry_slippage_bps")
        st["fill_entry_price"]   = fill_price
        st["entry_slippage_bps"] = slip
        # Use fill price for notional when known, else fall back to est.
        price_for_notional = fill_price if fill_price else st.get("est_entry_price") or 0
        if st.get("ctval") and st.get("filled_contracts"):
            st["notional_usd"] = round(
                st["filled_contracts"] * price_for_notional * st["ctval"], 2
            )
        if slip is not None:
            entry_slips.append(slip)
    avg_entry_slip = (sum(entry_slips) / len(entry_slips)) if entry_slips else None
    fill_report["fills"]["avg_entry_slippage_bps"] = (
        round(avg_entry_slip, 2) if avg_entry_slip is not None else None
    )

    report_base = {
        "date": today,
        "session_start_utc": session_start_utc,
        "signal": {
            "symbols_signaled": symbols,
            "filter":           ACTIVE_FILTER,
            "count":            len(symbols),
        },
        "conviction": {
            "roi_x_pct":  round(roi_x * 100, 4),
            "kill_y_pct": round(KILL_Y * 100, 4),
            "passed":     True,
        },
        "leverage": {
            "l_high":    L_HIGH,
            "vol_boost": round(boost, 4),
            "eff_lev":   eff_lev,
            "lev_int":   lev_int,
        },
        "capital": {
            "account_balance":   round(balance, 2),
            "balance_pre_entry": round(balance_pre_entry, 2),
            "balance_post_exit": None,
            "usdt_total":        fill_report["usdt_total"],
            "margin_buffer_pct": fill_report["margin_buffer_pct"],
            "usdt_deployable":   fill_report["usdt_deployable"],
            "usdt_per_symbol":   fill_report["usdt_per_symbol"],
        },
        "fills": fill_report["fills"],
    }

    if not positions:
        log.error("No positions entered -- aborting.")
        report = dict(report_base)
        report["session_end_utc"] = utcnow().strftime("%Y-%m-%d %H:%M:%S")
        report["exit"] = {"reason": "no_positions"}
        report["alerts_fired"] = _SESSION_ALERT_COUNT
        write_execution_report(report)
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
        positions, dry_run,
        report_base=report_base, session_start_utc=session_start_utc,
        balance_pre_entry=balance_pre_entry,
    )


def _run_monitoring_loop(today, today_date, api: BlofinREST, inst_ids,
                         open_prices, entry_prices, entry_1x, eff_lev,
                         peak, positions, dry_run,
                         report_base=None, session_start_utc=None,
                         balance_pre_entry=None,
                         *,
                         allocation_id: str | None = None,
                         config: "TraderConfig | None" = None,
                         session_id: str | None = None,
                         capital_deployed_usd: float = 0.0):
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

    Multi-tenant mode (allocation_id set): uses per-call `config` for strategy
    constants instead of module-level constants, writes to allocation-aware
    storage (runtime_state JSONB + allocation_returns + portfolio_session_id)
    instead of filesystem state + signal_date-keyed rows, and skips the
    master-only execution_report scaffold. The per-bar check order and
    portfolio-return math are a single source of truth shared by both modes.
    """
    cfg = config if config is not None else TraderConfig.master_defaults()
    log_prefix = f"[alloc {allocation_id[:8]}] " if allocation_id else ""

    session_open = datetime.datetime.combine(
        today_date, datetime.time(cfg.session_start_hour, 0, 0)
    )
    session_close_dt = datetime.datetime.combine(today_date, datetime.time(23, 55, 0))
    fill_gate_dt     = session_open + datetime.timedelta(
        minutes=cfg.fill_max_bar * cfg.bar_minutes + cfg.bar_minutes   # 720 min = 18:00 UTC
    )

    exit_reason     = None
    final_return_1x = 0.0

    # Per-symbol stop tracking (matches apply_raw_stop in rebuild_portfolio_matrix.py)
    # sym_stopped: {inst_id: clamped_return}  -- symbols closed by per-symbol stop
    # Once stopped, a symbol contributes PORT_SL_PCT (-6%) to the portfolio average.
    sym_stopped = {}   # {inst_id: PORT_SL_PCT}
    active_positions = list(positions)   # shrinks as per-symbol stops fire
    bars_monitored = 0
    # Exit-price snapshots for slippage reconciliation.
    #   sym_exit_prices: price at the bar that tripped a per-symbol stop
    #   final_exit_snapshot: last `current` dict before final close_all
    sym_exit_prices    = {}
    final_exit_snapshot = {}

    # Initialize the bar-by-bar portfolio NDJSON + SQL row. Each write is
    # independent and best-effort; a DB outage won't block the NDJSON backup
    # and vice versa. Both are idempotent on --resume.
    _portfolio_lev_int = max(1, int(math.ceil(eff_lev)))
    _portfolio_symbols = list(inst_ids)
    _portfolio_entered = [p["inst_id"] for p in positions]
    _portfolio_session_start = session_start_utc or utcnow().strftime("%Y-%m-%d %H:%M:%S")
    if allocation_id is None:
        # Master path: initialize both NDJSON + master-schema session row.
        # Allocation mode already persisted its session row via
        # _init_portfolio_session_for_allocation_inline before entering this
        # function; skip these writes here.
        init_portfolio_record(
            today, _portfolio_symbols, _portfolio_entered,
            _portfolio_session_start, eff_lev, _portfolio_lev_int,
        )
        init_portfolio_session_sql(
            today, _portfolio_symbols, _portfolio_entered,
            _portfolio_session_start, eff_lev, _portfolio_lev_int,
        )

    log.info(f"{log_prefix}Entering 5-min monitoring loop ...")

    while utcnow() < session_close_dt:
        sleep_until_next_bar()
        if utcnow() >= session_close_dt:
            break

        bars_monitored += 1
        b       = bar_index()
        current = get_mark_prices(api, list(inst_ids))
        if not current:
            log.warning(f"{log_prefix}Bar {b}: all price fetches failed -- skipping bar")
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
            if sym_ret <= cfg.port_sl_pct:
                log.warning(
                    f"{log_prefix}  SYM STOP: {iid} ret={sym_ret*100:.3f}% <= {cfg.port_sl_pct*100:.0f}%"
                    f" -- closing symbol and clamping at {cfg.port_sl_pct*100:.0f}%"
                )
                sym_stopped[iid] = cfg.port_sl_pct
                sym_exit_prices[iid] = price
                newly_stopped.append(pos)
                active_positions = [p for p in active_positions if p["inst_id"] != iid]

        if newly_stopped and not dry_run:
            failed = close_all_positions(api, newly_stopped, "sym_stop", dry_run)
            if failed:
                # If close failed, keep in active pool and don't clamp
                for p in failed:
                    del sym_stopped[p["inst_id"]]
                    sym_exit_prices.pop(p["inst_id"], None)
                    active_positions.append(p)
                    log.warning(f"{log_prefix}  {p['inst_id']}: sym stop close failed -- keeping in pool")

        # ── Portfolio return (clamped + active symbols) ────────────────────
        # Mirrors rebuild_portfolio_matrix.py: stopped symbols contribute
        # their clamped value; active symbols use current live return.
        sym_returns_map = {}
        for iid in inst_ids:
            if iid in sym_stopped:
                sym_returns_map[iid] = sym_stopped[iid]   # clamped at -6%
            elif iid in current and entry_prices.get(iid, 0) > 0:
                sym_returns_map[iid] = current[iid] / entry_prices[iid] - 1.0
        sym_returns = list(sym_returns_map.values())

        incr = float(np.mean(sym_returns)) if sym_returns else 0.0

        # session_ret uses open_prices anchor (for EARLY_FILL threshold)
        session_ret = equal_weight_return(current, open_prices)

        peak     = max(peak, incr)
        tsl_dist = incr - peak
        fill_open = utcnow() <= fill_gate_dt

        log.info(
            f"{log_prefix}Bar {b:3d} | incr={incr*100:+.3f}%  peak={peak*100:.3f}%  "
            f"tsl={tsl_dist*100:+.3f}%  sess={session_ret*100:+.3f}%  "
            f"active={len(active_positions)}/{len(positions)}  "
            f"stopped={len(sym_stopped)}  "
            f"fill={'open' if fill_open else 'closed'}"
        )

        # Persist this bar's snapshot before the break checks so the bar that
        # tripped an exit is included in the timeline. Both writes are
        # independent + best-effort.
        _bar_ts   = utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _bar_stop = list(sym_stopped.keys())
        if allocation_id is None:
            # Master: NDJSON + master-schema SQL row (matched by signal_date).
            append_portfolio_bar(
                today, b, _bar_ts, incr, peak, sym_returns_map, _bar_stop,
            )
            append_portfolio_bar_sql(
                today, b, _bar_ts, incr, peak, sym_returns_map, _bar_stop,
            )
        else:
            # Allocation: session row is FK-identified by session_id; no NDJSON.
            _append_portfolio_bar_for_session(
                session_id=session_id, bar=b,
                ts_utc=utcnow().replace(tzinfo=datetime.timezone.utc),
                incr=incr, peak=peak,
                sym_returns=sym_returns_map, stopped=_bar_stop,
            )

        # ── 1st: PORT_SL -- portfolio hard floor ───────────────────────────
        if incr <= cfg.port_sl_pct:
            log.warning(
                f"{log_prefix}  C -- HARD STOP: portfolio incr={incr*100:.3f}% <= {cfg.port_sl_pct*100:.0f}%"
            )
            exit_reason = "port_sl"
            final_return_1x = incr
            final_exit_snapshot = dict(current)
            break

        # ── 2nd: PORT_TSL -- trailing stop ─────────────────────────────────
        if tsl_dist <= cfg.port_tsl_pct:
            tsl_exit = peak + cfg.port_tsl_pct
            log.warning(
                f"{log_prefix}  D -- TRAIL STOP: (incr-peak)={tsl_dist*100:.3f}%  "
                f"exit at {tsl_exit*100:.3f}%  "
                f"({'loss' if tsl_exit < 0 else 'profit'})"
            )
            exit_reason = "port_tsl"
            final_return_1x = tsl_exit
            final_exit_snapshot = dict(current)
            break

        # ── 3rd: EARLY_FILL (bars 7-143 only) ─────────────────────────────
        if fill_open and session_ret >= cfg.early_fill_y:
            fill_ret = session_ret - entry_1x
            log.info(
                f"{log_prefix}  E -- EARLY FILL: sess={session_ret*100:.3f}% >= {cfg.early_fill_y*100:.0f}%  "
                f"return from entry={fill_ret*100:.3f}%"
            )
            exit_reason = "early_fill"
            final_return_1x = fill_ret
            final_exit_snapshot = dict(current)
            break

        _state = {
            "date": today, "phase": "active", "bar": b,
            "incr": round(incr, 6), "peak": round(peak, 6),
            "session_ret": round(session_ret, 6),
            "sym_stopped": list(sym_stopped.keys()),
            "effective_leverage": eff_lev, "entry_1x": round(entry_1x, 6),
            "open_prices":  {k: v for k, v in open_prices.items()},
            "entry_prices": {k: v for k, v in entry_prices.items()},
            "positions": active_positions,
        }
        if allocation_id is None:
            save_state(_state)
        else:
            _state["session_id"] = session_id
            _state["symbols"] = list(inst_ids)
            _state["capital_deployed_usd"] = float(capital_deployed_usd)
            _mark_runtime_state(allocation_id, _state)

    # ── Phase 5: Exit ─────────────────────────────────────────────────────
    if exit_reason is None:
        current = get_mark_prices(api, inst_ids)
        if current:
            final_return_1x = equal_weight_return(current, entry_prices)
            final_exit_snapshot = dict(current)
            log.info(
                f"{log_prefix}F -- SESSION CLOSE (23:55 UTC)  "
                f"return from entry={final_return_1x*100:+.3f}%  (unbounded)"
            )
        else:
            final_return_1x = float("nan")
            log.warning(f"{log_prefix}F -- SESSION CLOSE: price fetch failed -- not logging return")
        exit_reason = "session_close"

    # Close remaining active positions (sym_stopped symbols already closed above)
    failed_closes = close_all_positions(api, active_positions, exit_reason, dry_run)

    net_pct = final_return_1x * eff_lev * 100
    log.info(
        f"{log_prefix}Session done -- {exit_reason}  "
        f"return_1x={final_return_1x*100 if not math.isnan(final_return_1x) else 'nan':+.3f}%  "
        f"lev={eff_lev:.3f}x  net~{net_pct if not math.isnan(net_pct) else 'nan':+.3f}%"
    )

    if not math.isnan(final_return_1x):
        if allocation_id is None:
            log_daily_return(net_pct, exit_reason, session_date=today, eff_lev=eff_lev)
        else:
            # Mirror of log_daily_return — applies same fee + funding drag internally.
            _log_allocation_return(
                allocation_id, today,
                net_return_pct=net_pct,
                exit_reason=exit_reason,
                effective_leverage=eff_lev,
                capital_deployed_usd=float(capital_deployed_usd),
                config=cfg,
            )
    else:
        log.warning(f"{log_prefix}Return not logged (NaN -- price unavailable at close)")

    sym_stops_fired = list(sym_stopped.keys())

    # Build per-symbol est_exit_price: sym-stop price for stopped symbols,
    # final snapshot for the rest. Then reconcile against BloFin fill history.
    # Both master and allocation paths use this for exit slippage reconciliation.
    est_exit_prices = {}
    for pos in positions:
        iid = pos["inst_id"]
        if iid in sym_exit_prices:
            est_exit_prices[iid] = sym_exit_prices[iid]
        elif iid in final_exit_snapshot:
            est_exit_prices[iid] = final_exit_snapshot[iid]

    if allocation_id is None:
        # ── Master-only: execution report scaffold ────────────────────────
        # Per-symbol exit slippage tracking, balance-delta P&L, exit_block +
        # monitoring_block, JSON execution report. Allocation mode skips this
        # — it records aggregates via _log_allocation_return and runtime_state.
        exit_symbols = []
        exit_slips   = []
        if not dry_run:
            reconcile_exit_prices(api, positions, est_exit_prices)
            for pos in positions:
                iid = pos["inst_id"]
                est = est_exit_prices.get(iid)
                exit_symbols.append({
                    "inst_id":           iid,
                    "est_exit_price":    round(est, 6) if est else None,
                    "fill_exit_price":   pos.get("fill_exit_price"),
                    "exit_slippage_bps": pos.get("exit_slippage_bps"),
                })
                if pos.get("exit_slippage_bps") is not None:
                    exit_slips.append(pos["exit_slippage_bps"])
        else:
            for pos in positions:
                iid = pos["inst_id"]
                est = est_exit_prices.get(iid)
                exit_symbols.append({
                    "inst_id":           iid,
                    "est_exit_price":    round(est, 6) if est else None,
                    "fill_exit_price":   None,
                    "exit_slippage_bps": None,
                })
        avg_exit_slip = (sum(exit_slips) / len(exit_slips)) if exit_slips else None

        # Actual P&L from balance delta (skipped on dry-run and on --resume where
        # balance_pre_entry is unknown).
        balance_post_exit   = None
        actual_pnl_usd      = None
        actual_return_pct   = None
        pnl_vs_est_pct      = None
        if not dry_run and balance_pre_entry is not None:
            try:
                balance_post_exit = get_account_balance_usdt(api)
                actual_pnl_usd    = balance_post_exit - balance_pre_entry
                if balance_pre_entry > 0:
                    actual_return_pct = (actual_pnl_usd / balance_pre_entry) * 100.0
                    if not math.isnan(net_pct):
                        pnl_vs_est_pct = actual_return_pct - net_pct
            except Exception as e:
                log.warning(f"  Could not capture balance_post_exit: {e}")

        exit_block = {
            "reason":                exit_reason,
            "est_return_1x_pct":     round(final_return_1x * 100, 4) if not math.isnan(final_return_1x) else None,
            "eff_lev":               eff_lev,
            "est_net_return_pct":    round(net_pct, 4) if not math.isnan(net_pct) else None,
            "close_failures":        [p["inst_id"] for p in failed_closes],
            "close_failure_count":   len(failed_closes),
            "symbols":               exit_symbols,
            "avg_exit_slippage_bps": round(avg_exit_slip, 2) if avg_exit_slip is not None else None,
            "actual_pnl_usd":        round(actual_pnl_usd, 2) if actual_pnl_usd is not None else None,
            "actual_return_pct":     round(actual_return_pct, 4) if actual_return_pct is not None else None,
            "pnl_vs_est_pct":        round(pnl_vs_est_pct, 4) if pnl_vs_est_pct is not None else None,
        }
        monitoring_block = {
            "bars_monitored":  bars_monitored,
            "peak_pct":        round(peak * 100, 4),
            "sym_stops_fired": sym_stops_fired,
            "sym_stops_count": len(sym_stops_fired),
        }

        if report_base is not None:
            report = dict(report_base)
            if balance_post_exit is not None and isinstance(report.get("capital"), dict):
                report["capital"] = dict(report["capital"])
                report["capital"]["balance_post_exit"] = round(balance_post_exit, 2)
        else:
            # --resume path: no pre-built report_base. Write minimal report
            # with what we have from restored state.
            report = {
                "date":              today,
                "session_start_utc": session_start_utc,
                "resumed":           True,
                "leverage":          {"eff_lev": eff_lev},
                "capital":           {
                    "balance_pre_entry": None,
                    "balance_post_exit": round(balance_post_exit, 2) if balance_post_exit is not None else None,
                },
            }
        report["session_end_utc"] = utcnow().strftime("%Y-%m-%d %H:%M:%S")
        report["monitoring"]      = monitoring_block
        report["exit"]            = exit_block
        report["alerts_fired"]    = _SESSION_ALERT_COUNT
        write_execution_report(report)

        # Mark the portfolio closed in both stores so the UI stops polling.
        _close_kwargs = {
            "status":        "closed",
            "exit_time_utc": report["session_end_utc"],
            "exit_reason":   exit_reason,
        }
        update_portfolio_meta(today, **_close_kwargs)
        update_portfolio_meta_sql(today, **_close_kwargs)

        save_state({
            "date": today, "phase": "closed",
            "exit_reason": exit_reason,
            "final_return_1x_pct": round(final_return_1x * 100, 4) if not math.isnan(final_return_1x) else None,
            "effective_leverage": eff_lev,
            "net_return_approx_pct": round(net_pct, 4) if not math.isnan(net_pct) else None,
            "positions": failed_closes,
            "unclosed_count": len(failed_closes),
        })
    else:
        # ── Allocation-mode close ─────────────────────────────────────────
        # Exit-price reconcile still fires (same data, different surface).
        # No execution report. Session row + runtime_state closed via
        # allocation-aware writers.
        if not dry_run and est_exit_prices:
            try:
                reconcile_exit_prices(api, positions, est_exit_prices)
            except Exception as e:
                log.warning(f"{log_prefix}Exit-price reconcile failed: {e}")

        _close_portfolio_session_for_allocation(
            session_id=session_id,
            exit_reason=exit_reason,
            exit_time_utc=utcnow().replace(tzinfo=datetime.timezone.utc),
        )
        _mark_runtime_state(allocation_id, {
            "phase": "closed",
            "exit_reason": exit_reason,
            "final_return_1x_pct": round(final_return_1x * 100, 4) if not math.isnan(final_return_1x) else None,
            "net_return_approx_pct": round(net_pct, 4) if not math.isnan(net_pct) else None,
            "effective_leverage": float(eff_lev),
            "session_id": session_id,
            "unclosed_count": len(failed_closes),
            "bars_monitored": bars_monitored,
            "sym_stops_fired": sym_stops_fired,
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
# ALLOCATION MODE (multi-tenant)
# ==========================================================================
# Everything below this banner is only reached when --allocation-id is set.
# The master-account execution path above does not call any of this.
#
# High-level flow:
#   1. _acquire_allocation_lock — atomic CAS on allocations.lock_acquired_at
#      (24h staleness threshold, same semantic as the filesystem LOCK_FILE)
#   2. Load allocation row → TraderConfig via strategy_version.config JSONB
#   3. Decrypt per-user BloFin creds via credential_loader
#   4. Build a per-call BlofinREST instance with those creds
#   5. Branch on runtime_state.phase: resume active / already-finished / fresh
#   6. Persist runtime_state JSONB (replaces filesystem STATE_FILE) at the
#      same logical save points as the master script's save_state() calls
#   7. Write per-allocation portfolio_sessions row + allocation_returns on close

# ─── Helpers ────────────────────────────────────────────────────────────────

def _acquire_allocation_lock(allocation_id: str) -> bool:
    """Atomic CAS on allocations.lock_acquired_at.

    Returns True if the lock was acquired (we own this allocation's session
    and should proceed). False if another subprocess holds a lock that's
    less than 24 hours old — we back off and exit cleanly.
    """
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE user_mgmt.allocations
                SET lock_acquired_at = NOW(), updated_at = NOW()
                WHERE allocation_id = %s::uuid
                  AND status = 'active'
                  AND (lock_acquired_at IS NULL
                       OR lock_acquired_at < NOW() - INTERVAL '24 hours')
                RETURNING allocation_id
                """,
                (allocation_id,),
            )
            got_lock = cur.fetchone() is not None
        conn.commit()
        return got_lock
    finally:
        conn.close()


def _release_allocation_lock(allocation_id: str) -> None:
    """Release the lock unconditionally. Called from finally-block."""
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE user_mgmt.allocations
                SET lock_acquired_at = NULL, updated_at = NOW()
                WHERE allocation_id = %s::uuid
                """,
                (allocation_id,),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"  Could not release allocation lock {allocation_id}: {e}")
    finally:
        conn.close()


def _account_lock_key(connection_id: str) -> int:
    """Stable bigint key from a connection UUID for pg_advisory_lock.

    PostgreSQL advisory locks take int8 (bigint) keys. Hash the UUID string
    with SHA-256 and take the first 8 bytes as a signed integer (matches
    int8 range). Deterministic: same UUID -> same key across processes.
    """
    h = hashlib.sha256(str(connection_id).encode()).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=True)


@contextmanager
def _account_advisory_lock(connection_id: str):
    """Session-level advisory lock keyed on exchange connection UUID.

    Serializes the balance-read + enter-positions critical section across
    concurrent per-allocation subprocesses that share the same exchange
    account. Enforces first-come-wins at execution time: spawn_traders
    orders subprocess spawn by created_at ASC; whichever reaches this lock
    first wins it, and later subprocesses block until the earlier one
    releases (i.e. finishes entering positions).

    Lock is session-scoped, not transaction-scoped, so it persists across
    the commit implicit in pg_advisory_lock. Released on context exit via
    pg_advisory_unlock. If the subprocess dies mid-lock (SIGKILL/OOM), the
    DB session drops and the lock auto-releases — no stuck-lock scenario.
    """
    key = _account_lock_key(connection_id)
    conn = _trader_db_connect()
    acquired = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (key,))
        conn.commit()
        acquired = True
        yield
    finally:
        if acquired:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
                conn.commit()
            except Exception as e:
                log.warning(f"  advisory_unlock failed for {connection_id}: {e}")
        try:
            conn.close()
        except Exception:
            pass


def _compute_allocation_capital(
    allocation_id: str, requested: float, balance: float,
) -> tuple[float, str | None]:
    """Size per-allocation capital against available account balance.

    Returns (usdt_for_allocation, warning_message_or_None). Caller owns
    logging — helper returns the warning string so tests can assert
    on it directly without installing a logging capture.

    Decision 10.1 (Session B pre-commit): capital_usd is NOT NULL at schema
    level; `requested` is always a real number, no NULL fallback needed.
    Decision 10.2: if requested > balance, size down to balance with
    warning and continue (not abort).
    """
    if requested > balance:
        msg = (
            f"Allocation {allocation_id}: requested capital "
            f"${requested:,.2f} exceeds available balance "
            f"${balance:,.2f} — sizing down to ${balance:,.2f}. "
            "Likely cause: another allocation has already deployed capital "
            "against this account."
        )
        return balance, msg
    return requested, None


def _fetch_allocation(allocation_id: str) -> dict:
    """Load allocation row as a dict. Raises ValueError if not found."""
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT allocation_id, user_id, strategy_version_id, connection_id,
                       capital_usd, status, runtime_state
                FROM user_mgmt.allocations
                WHERE allocation_id = %s::uuid
                """,
                (allocation_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(f"No allocation: {allocation_id}")
    cols = ["allocation_id", "user_id", "strategy_version_id", "connection_id",
            "capital_usd", "status", "runtime_state"]
    return dict(zip(cols, row))


def _fetch_strategy_version(strategy_version_id: str) -> dict:
    """Load strategy_version row (for config JSONB + identity)."""
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT strategy_version_id, version_label, config, is_active
                FROM audit.strategy_versions
                WHERE strategy_version_id = %s::uuid
                """,
                (strategy_version_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(f"No strategy_version: {strategy_version_id}")
    cols = ["strategy_version_id", "version_label", "config", "is_active"]
    return dict(zip(cols, row))


def _mark_runtime_state(allocation_id: str, state: dict) -> None:
    """Merge + persist runtime_state JSONB.

    Replaces the filesystem save_state() call for allocation mode. Adds today's
    UTC date + an updated_at timestamp so resume logic can detect stale state.
    Best-effort — logs on failure but doesn't raise (mirrors master's
    save_state error handling pattern: a DB outage must not block the loop).
    """
    payload = {**state, "date": utc_today(),
               "updated_at": utcnow().strftime("%Y-%m-%d %H:%M:%S")}
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE user_mgmt.allocations
                SET runtime_state = %s::jsonb, updated_at = NOW()
                WHERE allocation_id = %s::uuid
                """,
                (json.dumps(payload), allocation_id),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"  Could not persist runtime_state for {allocation_id}: {e}")
    finally:
        conn.close()


_FLAT_EXIT_REASONS = (
    "filtered", "no_entry_conviction", "missed_window",
    "stale_closed", "stale_close_failed", "no_entry", "errored",
    "entry_failed",
)


def _log_allocation_return(
    allocation_id: str, session_date: str,
    net_return_pct: float,                # gross × lev × 100, BEFORE fees (same
                                          # shape master's log_daily_return takes)
    exit_reason: str,
    effective_leverage: float,
    capital_deployed_usd: float,
    config: "TraderConfig",
) -> None:
    """Mirror of log_daily_return for user allocations — applies the same fee +
    funding drag internally so net P&L accounting stays identical across master
    and allocation paths.

    net_return_pct signature intentionally matches master's log_daily_return:
    caller passes `final_return_1x * effective_leverage * 100` (gross leveraged
    percent before fees). This helper subtracts fee_drag + funding_drag and
    records both gross and net.

    Upsert semantics so --resume or re-runs don't duplicate. Called for every
    terminal outcome — including filtered / conviction-kill paths that deploy
    no capital — so the return history is dense.
    """
    gross_pct = net_return_pct
    if effective_leverage > 0 and exit_reason not in _FLAT_EXIT_REASONS:
        fee_drag = config.taker_fee_pct * effective_leverage
        funding_drag = config.funding_rate_daily_pct * effective_leverage
        net_return_pct = net_return_pct - (fee_drag + funding_drag) * 100

    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_mgmt.allocation_returns
                    (allocation_id, session_date, net_return_pct,
                     gross_return_pct, exit_reason, effective_leverage,
                     capital_deployed_usd)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (allocation_id, session_date) DO UPDATE SET
                    net_return_pct       = EXCLUDED.net_return_pct,
                    gross_return_pct     = EXCLUDED.gross_return_pct,
                    exit_reason          = EXCLUDED.exit_reason,
                    effective_leverage   = EXCLUDED.effective_leverage,
                    capital_deployed_usd = EXCLUDED.capital_deployed_usd,
                    logged_at            = NOW()
                """,
                (allocation_id, session_date, net_return_pct, gross_pct,
                 exit_reason, effective_leverage, capital_deployed_usd),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"  Could not log allocation_returns for {allocation_id}: {e}")
    finally:
        conn.close()


def _init_portfolio_session_for_allocation_inline(
    *,
    allocation_id: str,
    signal_date: str,
    symbols: list,
    entered: list,
    session_start_utc: datetime.datetime,
    eff_lev: float,
    lev_int: int,
) -> str:
    """Temporary inline INSERT for portfolio_sessions with allocation_id.

    Part 2a.5 will merge this into the shared init_portfolio_session_sql.
    Uses the new UNIQUE (signal_date, allocation_id) constraint from the
    schema migration. Returns the new portfolio_session_id as a string.
    """
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_mgmt.portfolio_sessions
                    (allocation_id, signal_date, session_start_utc, status,
                     symbols, entered, eff_lev, lev_int)
                VALUES (%s::uuid, %s, %s, 'active', %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT portfolio_sessions_signal_date_allocation_key
                DO UPDATE SET
                    session_start_utc = EXCLUDED.session_start_utc,
                    symbols           = EXCLUDED.symbols,
                    entered           = EXCLUDED.entered,
                    eff_lev           = EXCLUDED.eff_lev,
                    lev_int           = EXCLUDED.lev_int,
                    updated_at        = NOW()
                RETURNING portfolio_session_id
                """,
                (allocation_id, signal_date, session_start_utc,
                 list(symbols), list(entered),
                 float(eff_lev), int(lev_int)),
            )
            session_id = cur.fetchone()[0]
        conn.commit()
        return str(session_id)
    finally:
        conn.close()


def _run_fresh_session_for_allocation(
    allocation_id: str,
    config: TraderConfig,
    api: BlofinREST,
    connection_id: str,
    dry_run: bool = False,
) -> None:
    """Entry + monitoring-loop-handoff for one user allocation.

    Mirrors master's run_session() happy path. Substitutes per-allocation
    DB-backed state for filesystem, allocation-aware SQL writes for master's,
    and per-allocation credentials/api for module-level build_api(). At every
    master save_state() checkpoint, calls _mark_runtime_state() with the
    equivalent dict. At terminal-state exits, also calls _log_allocation_return()
    so the trader dashboard has a row to render for filtered/no-entry days.
    """
    today = utc_today()  # "YYYY-MM-DD" string
    today_date = utcnow().date()

    # ── Phase 1: load signal scoped to this allocation's filter ──────────
    # TODO(open-work-list): replace CSV with per-strategy-version signal
    # source once a second strategy version exists. All published versions
    # today route through the same CSV filter-column path.
    symbols = get_today_symbols(today, active_filter=config.active_filter)
    if not symbols:
        log.info(
            f"Allocation {allocation_id}: A -- Filtered, no symbols for "
            f"{today} (filter={config.active_filter!r})"
        )
        _mark_runtime_state(allocation_id, {"phase": "filtered", "positions": []})
        _log_allocation_return(
            allocation_id, today,
            net_return_pct=0.0,
            exit_reason="filtered",
            effective_leverage=0.0, capital_deployed_usd=0.0,
            config=config,
        )
        return

    inst_ids = [s + config.symbol_suffix for s in symbols]

    # ── Phase 2: sleep to conviction bar, then fetch 06:00 + 06:35 prices ─
    session_open = datetime.datetime.combine(
        today_date, datetime.time(config.session_start_hour, 0, 0)
    )
    conviction_dt = session_open + datetime.timedelta(
        minutes=config.conviction_bar * config.bar_minutes + config.bar_minutes
    )
    exec_cutoff_dt = conviction_dt + datetime.timedelta(
        minutes=config.conviction_exec_buffer_min
    )

    sleep_until(conviction_dt, f"conviction bar (allocation {allocation_id})")
    now = utcnow()
    past_cutoff = now > exec_cutoff_dt

    log.info(
        f"Allocation {allocation_id}: fetching 06:00 + {conviction_dt.strftime('%H:%M')} "
        "UTC prices from Binance kline history"
    )
    open_prices = _get_prices_at_timestamp(inst_ids, session_open)
    bar6_prices = _get_prices_at_timestamp(inst_ids, conviction_dt)

    # Fallback to live marks for symbols missing from kline history
    missing = [i for i in inst_ids if i not in open_prices or i not in bar6_prices]
    if missing:
        log.warning(f"  {len(missing)} symbols missing from kline history — falling back to live")
        live = get_mark_prices(api, missing)
        for i in missing:
            if i not in open_prices and i in live: open_prices[i] = live[i]
            if i not in bar6_prices and i in live: bar6_prices[i] = live[i]

    if not open_prices:
        log.error(f"Allocation {allocation_id}: could not fetch session-open prices — aborting")
        _mark_runtime_state(allocation_id, {"phase": "errored", "reason": "open_prices_fetch_failed"})
        _log_allocation_return(
            allocation_id, today,
            net_return_pct=0.0,
            exit_reason="errored",
            effective_leverage=0.0, capital_deployed_usd=0.0,
            config=config,
        )
        return

    inst_ids = [i for i in inst_ids if i in open_prices and i in bar6_prices]

    # Compute conviction. Order matches master line 1829: (current, ref).
    roi_x = equal_weight_return(bar6_prices, open_prices)
    log.info(
        f"Allocation {allocation_id}: roi_x={roi_x * 100:.4f}%  "
        f"kill_y={config.kill_y * 100:.2f}%"
    )

    # ── Phase 2a: missed execution window ────────────────────────────────
    if past_cutoff:
        log.warning(
            f"Allocation {allocation_id}: past conviction exec cutoff "
            f"(now={now.strftime('%H:%M:%S')} UTC, cutoff={exec_cutoff_dt.strftime('%H:%M:%S')} UTC). "
            "No trade this session."
        )
        _mark_runtime_state(allocation_id, {
            "phase": "missed_window",
            "roi_x": round(float(roi_x), 6),
            "positions": [],
        })
        _log_allocation_return(
            allocation_id, today,
            net_return_pct=0.0,
            exit_reason="missed_window",
            effective_leverage=0.0, capital_deployed_usd=0.0,
            config=config,
        )
        return

    # ── Phase 2b: conviction gate ────────────────────────────────────────
    if roi_x < config.kill_y:
        log.info(
            f"Allocation {allocation_id}: B -- No Entry  roi_x < kill_y  "
            "(return=0%, no fees)"
        )
        _mark_runtime_state(allocation_id, {
            "phase": "no_entry",
            "roi_x": round(float(roi_x), 6),
            "positions": [],
        })
        _log_allocation_return(
            allocation_id, today,
            net_return_pct=0.0,
            exit_reason="no_entry_conviction",
            effective_leverage=0.0, capital_deployed_usd=0.0,
            config=config,
        )
        return

    log.info(f"Allocation {allocation_id}: conviction passed — entering at {config.l_high}x")

    # ── Phase 3: stale-position sweep (this account only) ────────────────
    # Mirrors master lines 1904-1939. Uses per-allocation api so only this
    # user's BloFin account is touched.
    existing = get_actual_positions(api, inst_ids)
    if existing:
        log.warning(
            f"Allocation {allocation_id}: {len(existing)} stale position(s) "
            f"found before entry: {list(existing.keys())}. Closing them first."
        )
        stale = [{"inst_id": iid, "contracts": int(sz),
                  "marginMode": config.margin_mode,
                  "positionSide": config.position_side}
                 for iid, sz in existing.items()]
        failed_stale = close_all_positions(api, stale, "pre_entry_cleanup", dry_run)
        if failed_stale:
            log.error(
                f"Allocation {allocation_id}: could not close all stale positions. "
                "Aborting entry to avoid double exposure."
            )
            _mark_runtime_state(allocation_id, {
                "phase": "stale_close_failed",
                "positions": failed_stale,
                "unclosed_count": len(failed_stale),
            })
            _log_allocation_return(
                allocation_id, today,
                net_return_pct=0.0,
                exit_reason="stale_close_failed",
                effective_leverage=0.0, capital_deployed_usd=0.0,
                config=config,
            )
            return
        log.info(f"Allocation {allocation_id}: stale positions closed")

    # ── Phase 4: compute leverage (no VOL boost this release) ────────────
    # Open work list: VOL boost from strategy_version.vol_boost_config.
    eff_lev = float(config.l_high)
    lev_int = max(1, int(math.ceil(eff_lev)))

    # ── Phase 5: balance + enter positions (advisory-lock serialized) ────
    # Lock prevents two allocations on the same exchange account from
    # racing the balance read -> over-committing capital. See
    # _account_advisory_lock for lifecycle details.
    entry_prices = bar6_prices
    entry_1x = roi_x
    peak = 0.0

    with _account_advisory_lock(connection_id):
        account_balance = get_account_balance_usdt(api)
        log.info(
            f"Allocation {allocation_id}: USDT available=${account_balance:,.2f}"
        )

        # Test scaffolding — exercise the lock under concurrency in dry-run.
        # Set TRADER_LOCK_TEST_SLEEP_S to a positive float to hold the lock
        # for that many seconds after the balance read. No-op when unset or
        # when not in dry-run. Intended for operator scenarios only.
        _test_sleep = os.environ.get("TRADER_LOCK_TEST_SLEEP_S")
        if dry_run and _test_sleep:
            try:
                _t = float(_test_sleep)
                if _t > 0:
                    log.info(f"  [dry-run lock-test] sleeping {_t}s while holding lock")
                    time.sleep(_t)
            except ValueError:
                pass

        # Per-allocation sizing. _compute_allocation_capital encapsulates the
        # size-down-with-warning conditional so the unit is testable without
        # a logging capture. Caller owns the log.warning() call.
        requested = float(config.capital_value)
        usdt_for_allocation, warn_msg = _compute_allocation_capital(
            allocation_id, requested, account_balance,
        )
        if warn_msg:
            log.warning(warn_msg)

        # enter_positions's existing 10% MARGIN_BUFFER is applied inside,
        # on top of usdt_for_allocation. No other sizing logic here.
        positions, fill_report = enter_positions(
            api, inst_ids, entry_prices, eff_lev, usdt_for_allocation, dry_run,
        )

    if not positions:
        log.error(f"Allocation {allocation_id}: enter_positions returned no fills")
        _mark_runtime_state(allocation_id, {
            "phase": "errored",
            "reason": "entry_failed",
            "fill_report": fill_report,
        })
        _log_allocation_return(
            allocation_id, today,
            net_return_pct=0.0,
            exit_reason="entry_failed",
            effective_leverage=float(eff_lev), capital_deployed_usd=0.0,
            config=config,
        )
        return

    # ── Phase 5b: reconcile fills (mutates `positions` in place) ─────────
    if not dry_run:
        reconcile_fill_prices(api, positions)

    capital_deployed_usd = float(fill_report.get("usdt_deployable") or 0.0)

    # ── Phase 6: persist portfolio_sessions row with allocation_id ───────
    session_start_dt = utcnow()
    entered_inst_ids = [p["inst_id"] for p in positions]
    session_id = _init_portfolio_session_for_allocation_inline(
        allocation_id=allocation_id,
        signal_date=today,
        symbols=list(inst_ids),
        entered=entered_inst_ids,
        session_start_utc=session_start_dt.replace(tzinfo=datetime.timezone.utc),
        eff_lev=eff_lev,
        lev_int=lev_int,
    )
    log.info(f"Allocation {allocation_id}: portfolio_session_id={session_id}")

    # ── Phase 7: mark runtime_state=active, handoff to monitoring loop ───
    _mark_runtime_state(allocation_id, {
        "phase": "active",
        "session_id": session_id,
        "entry_1x": round(float(entry_1x), 6),
        "effective_leverage": float(eff_lev),
        "peak": float(peak),
        "open_prices": {k: float(v) for k, v in open_prices.items()},
        "entry_prices": {k: float(v) for k, v in entry_prices.items()},
        "positions": positions,
        "symbols": list(inst_ids),
        "capital_deployed_usd": capital_deployed_usd,
    })

    # ── Phase 8: monitoring loop — parameterized shared function ─────────
    # Master's _run_monitoring_loop accepts allocation-mode kwargs (config,
    # session_id, allocation_id, capital_deployed_usd) and branches at the
    # write sites. Per-bar check order + portfolio-return math are shared.
    _run_monitoring_loop(
        today, today_date, api, inst_ids,
        open_prices, entry_prices, entry_1x, eff_lev,
        peak, positions, dry_run,
        allocation_id=allocation_id,
        config=config,
        session_id=session_id,
        capital_deployed_usd=capital_deployed_usd,
    )


def _append_portfolio_bar_for_session(
    *,
    session_id: str,
    bar: int,
    ts_utc: datetime.datetime,
    incr: float,
    peak: float,
    sym_returns: dict,
    stopped: list,
) -> None:
    """Temporary inline per-bar INSERT for allocation mode.

    Part 2a.5 will extend the shared append_portfolio_bar_sql to accept an
    allocation_id. For now, write directly by portfolio_session_id (which
    already FK-encodes the allocation via the session row).

    Idempotent on (portfolio_session_id, bar_number) PK. Best-effort —
    a DB outage must not block the 5-min loop.
    """
    stopped_list = list(stopped)
    try:
        conn = _trader_db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_mgmt.portfolio_bars
                        (portfolio_session_id, bar_number, bar_timestamp_utc,
                         portfolio_return, peak_return, symbol_returns, stopped)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (portfolio_session_id, bar_number) DO NOTHING
                    """,
                    (session_id, bar, ts_utc, float(incr), float(peak),
                     json.dumps(sym_returns), stopped_list),
                )
                cur.execute(
                    """
                    UPDATE user_mgmt.portfolio_sessions
                    SET bars_count             = (
                            SELECT COUNT(*) FROM user_mgmt.portfolio_bars b
                            WHERE b.portfolio_session_id = %s::uuid
                        ),
                        final_portfolio_return = %s,
                        peak_portfolio_return  = GREATEST(
                            COALESCE(peak_portfolio_return, %s), %s
                        ),
                        max_dd_from_peak       = LEAST(
                            COALESCE(max_dd_from_peak, 0), %s
                        ),
                        sym_stops              = ARRAY(
                            SELECT DISTINCT unnest(sym_stops || %s::text[])
                        ),
                        updated_at             = NOW()
                    WHERE portfolio_session_id = %s::uuid
                    """,
                    (session_id, float(incr), float(peak), float(peak),
                     float(incr) - float(peak), stopped_list, session_id),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"  Could not append portfolio bar (session {session_id}): {e}")


def _close_portfolio_session_for_allocation(
    *,
    session_id: str,
    exit_reason: str,
    exit_time_utc: datetime.datetime,
) -> None:
    """Mark the per-allocation portfolio_sessions row closed. Best-effort."""
    try:
        conn = _trader_db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_mgmt.portfolio_sessions
                    SET status        = 'closed',
                        exit_reason   = %s,
                        exit_time_utc = %s,
                        updated_at    = NOW()
                    WHERE portfolio_session_id = %s::uuid
                    """,
                    (exit_reason, exit_time_utc, session_id),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"  Could not close portfolio session {session_id}: {e}")



def run_session_for_allocation(allocation_id: str, dry_run: bool = False) -> None:
    """Execute one session for one user allocation.

    Replaces module-level credentials, filesystem state, and hardcoded strategy
    constants with per-allocation DB-backed equivalents. Otherwise follows the
    same phase sequence as master's run_session().
    """
    # 1. Acquire per-allocation lock (24-hour staleness threshold).
    if not _acquire_allocation_lock(allocation_id):
        log.warning(
            f"Allocation {allocation_id} already locked or stale-but-recent. "
            "Exiting without running a session."
        )
        return

    try:
        # 2. Load allocation context.
        alloc = _fetch_allocation(allocation_id)
        if alloc["status"] != "active":
            log.info(
                f"Allocation {allocation_id} status={alloc['status']!r}; skipping."
            )
            return

        # 3. Build TraderConfig from the frozen strategy_version.config JSONB.
        strategy_version = _fetch_strategy_version(alloc["strategy_version_id"])
        config = TraderConfig.from_strategy_version(
            strategy_version["config"],
            capital_usd=float(alloc["capital_usd"]),
        )
        log.info(
            f"Allocation {allocation_id}  "
            f"strategy_version={strategy_version['version_label']}  "
            f"capital=${float(alloc['capital_usd']):,.2f}  "
            f"l_high={config.l_high}  kill_y={config.kill_y}  "
            f"port_sl={config.port_sl_pct}  port_tsl={config.port_tsl_pct}  "
            f"filter={config.active_filter!r}"
        )

        # 4. Decrypt exchange credentials (Fernet via the encryption service).
        try:
            creds = load_credentials(alloc["connection_id"])
        except (ValueError, CredentialDecryptError) as e:
            log.error(
                f"Allocation {allocation_id}: credential load failed ({e}). "
                "Marking runtime_state errored and exiting."
            )
            _mark_runtime_state(
                allocation_id,
                {"phase": "errored", "reason": f"credential load: {e}"},
            )
            return

        if creds.exchange != "blofin":
            log.warning(
                f"Allocation {allocation_id} uses {creds.exchange!r} — only "
                "blofin is supported in this release. Skipping."
            )
            _mark_runtime_state(
                allocation_id,
                {"phase": "skipped",
                 "reason": f"exchange {creds.exchange} not supported"},
            )
            return

        # 5. Build a per-allocation BlofinREST instance with the decrypted keys.
        # Note: NOT build_api() — that reads module-level creds for the master
        # account. Each allocation gets its own instance with its own keys.
        api = BlofinREST(
            api_key=creds.api_key,
            api_secret=creds.api_secret,
            passphrase=creds.passphrase,
            demo=False,
        )

        # 6. Resume vs fresh branch on runtime_state.
        state = alloc.get("runtime_state") or {}
        today = utc_today()

        if state.get("date") == today and state.get("phase") == "active":
            log.info(
                f"Allocation {allocation_id}: resuming active session from "
                "runtime_state."
            )
            # Rehydrate the positional args the shared loop expects.
            _open_prices  = {k: float(v) for k, v in state["open_prices"].items()}
            _entry_prices = {k: float(v) for k, v in state["entry_prices"].items()}
            _inst_ids     = list(state.get("symbols", _open_prices.keys()))
            _today_date   = utcnow().date()
            _run_monitoring_loop(
                today, _today_date, api, _inst_ids,
                _open_prices, _entry_prices,
                float(state.get("entry_1x", 0.0)),
                float(state.get("effective_leverage", config.l_high)),
                float(state.get("peak", 0.0)),
                state["positions"], dry_run,
                allocation_id=allocation_id,
                config=config,
                session_id=state.get("session_id"),
                capital_deployed_usd=float(state.get("capital_deployed_usd", 0.0)),
            )
            return

        if state.get("date") == today and state.get("phase") in (
            "closed", "filtered", "no_entry", "missed_window",
            "skipped", "errored", "stale_close_failed",
        ):
            log.info(
                f"Allocation {allocation_id}: already finished today "
                f"(phase={state['phase']}). Exiting."
            )
            return

        # 7. Fresh session — entry + monitoring + close.
        _run_fresh_session_for_allocation(
            allocation_id, config, api,
            connection_id=str(alloc["connection_id"]),
            dry_run=dry_run,
        )

    finally:
        _release_allocation_lock(allocation_id)


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
    parser.add_argument(
        "--allocation-id", type=str, default=None,
        help="UUID of user_mgmt.allocations row to execute. When set, runs in "
             "multi-tenant mode: decrypts per-user credentials, uses per-"
             "allocation runtime_state, writes with allocation_id. When unset, "
             "runs master account mode (legacy, unchanged).",
    )
    args = parser.parse_args()

    if args.status:
        print(json.dumps(load_state(), indent=2, default=str))
        sys.exit(0)

    if args.seed_returns:
        seed_returns_log(args.seed_returns)
        sys.exit(0)

    if args.close_all:
        _require_blofin_credentials()
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

    if args.allocation_id:
        # Multi-tenant mode: per-allocation lock (DB-backed) instead of the
        # filesystem LOCK_FILE. Master-account path below is unchanged.
        try:
            run_session_for_allocation(args.allocation_id, dry_run=args.dry_run)
        except KeyboardInterrupt:
            log.warning(f"Allocation {args.allocation_id}: interrupted by user.")
        except Exception as e:
            log.exception(
                f"Allocation {args.allocation_id}: unhandled exception: {e}"
            )
            raise
    else:
        # Master account mode — legacy, behavior bit-for-bit identical.
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

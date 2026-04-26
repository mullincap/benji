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

Cron (invoked by run_daily.sh at 05:58 UTC after daily_signal.py):
  58 5 * * *  /home/ubuntu/benji3m/run_daily.sh >> /home/ubuntu/benji3m/cron.log 2>&1
"""

import os, sys, json, math, time, logging, datetime, argparse, csv, requests
import hashlib
from contextlib import contextmanager
from urllib.parse import urlencode
import numpy as np
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

# Exchange-agnostic trade surface. `adapter_for(creds)` dispatches to
# BloFinAdapter or BinanceMarginAdapter based on creds.exchange. Per-allocation
# path uses adapter_for(); legacy master path via build_api() returns a
# BloFinAdapter directly so helper functions see the same ABC surface on
# both paths.
from app.services.exchanges.adapter import (
    ExchangeAdapter,
    InstrumentInfo,
    PositionInfo,
    adapter_for,
)
from app.services.exchanges.blofin_adapter import BloFinAdapter

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
#
# DISABLED 2026-04-23 post-VELVET-fill incident: BloFin rejects atomic-with-
# entry SL on low-liquidity pairs (msg="Trading for this low-liquidity pair
# is temporarily unavailable due to risk control restrictions"), blocking
# the entry fill entirely. Bare market entry without embedded SL clears
# risk control (verified via manual UI fill on VELVET-USDT 2026-04-23 06:40
# UTC). Per-symbol software SL via the port_sl/port_tsl monitoring loop
# remains the primary stop — the exchange SL was always a backstop for
# connectivity loss. Order filling takes priority over backstop SL.
EXCHANGE_SL_ENABLED = False
EXCHANGE_SL_PCT     = -0.085  # -8.5% from entry price (1x, unleveraged)

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

def get_today_symbols(
    date_str: str,
    active_filter: str | None = None,
    *,
    bypass_sit_flat: bool = False,
) -> list:
    # Master path passes no active_filter → use module constant. Allocation
    # mode passes its per-strategy filter from TraderConfig.active_filter.
    # Behavior on the master path is bit-for-bit identical when called with
    # one positional arg, since active_filter falls through to ACTIVE_FILTER.
    #
    # bypass_sit_flat=True is for operator-initiated late-entry overrides:
    # the CSV writer emits the candidate basket even on sit_flat days
    # (so the UI can render a preview portfolio), but normal trader spawns
    # must still respect sit_flat = sit out. Only --late-entry sets this.
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

    # Normal spawn path: gate on sit_flat. Late-entry overrides bypass.
    if not bypass_sit_flat:
        sit_flat_rows = [r for r in today_rows
                         if str(r.get("sit_flat", "")).strip().lower() == "true"]
        if sit_flat_rows:
            reasons = ", ".join(
                r.get("filter_reason", "") for r in sit_flat_rows
            ) or "(no reason given)"
            log.info(
                f"Filter '{filter_name}' sit_flat=True for {date_str} "
                f"({reasons}) -> A: Filtered (0%, no fees)"
            )
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
        # Pass instance credentials explicitly so per-allocation subprocesses
        # sign requests with their own Fernet-decrypted keys instead of the
        # process-level BLOFIN_API_KEY env var (which is unset in the backend
        # container after the master cron retirement on 2026-04-20).
        return _shared_get_headers(
            method, path, body_str,
            api_key=self._key,
            api_secret=self._secret,
            passphrase=self._passphrase,
        )

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

    def get_deposit_history(
        self,
        before_ms: int | None = None,
        after_ms: int | None = None,
        limit: int = 100,
    ) -> dict:
        """Fetch deposit history. BloFin docs:
        GET /api/v1/asset/deposit-history
            ?after=<ms>&before=<ms>&limit=<n>
        Pagination is by `after`/`before` (epoch-ms cursors); `limit` caps
        per-page count (max 100). Returns oldest-first within the window.
        """
        p: dict = {"limit": str(min(limit, 100))}
        if before_ms is not None:
            p["before"] = str(before_ms)
        if after_ms is not None:
            p["after"] = str(after_ms)
        return self.request("GET", "/api/v1/asset/deposit-history", params=p)

    def get_withdrawal_history(
        self,
        before_ms: int | None = None,
        after_ms: int | None = None,
        limit: int = 100,
    ) -> dict:
        """Fetch withdrawal history. BloFin docs:
        GET /api/v1/asset/withdrawal-history
            ?after=<ms>&before=<ms>&limit=<n>
        Same pagination semantics as get_deposit_history.
        """
        p: dict = {"limit": str(min(limit, 100))}
        if before_ms is not None:
            p["before"] = str(before_ms)
        if after_ms is not None:
            p["after"] = str(after_ms)
        return self.request("GET", "/api/v1/asset/withdrawal-history", params=p)

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


def build_api() -> BloFinAdapter:
    """Return a BloFinAdapter wrapping BlofinREST with module-level master
    credentials. Used by the legacy run_session master path + --close-all CLI.
    Per-allocation path uses adapter_for(creds) at dispatch instead.
    """
    return BloFinAdapter(
        api_key=API_KEY,
        api_secret=API_SECRET,
        passphrase=PASSPHRASE,
    )

def get_account_balance_usdt(api: ExchangeAdapter) -> float:
    """Return USDT available balance on the exchange.

    Thin wrapper preserving the operator-visible log line. Field-fallback
    logic lives in each concrete adapter's get_balance().
    """
    balance = api.get_balance()
    if balance.available_usdt > 0:
        log.info(f"USDT available equity: ${balance.available_usdt:,.2f}")
    else:
        log.warning("USDT balance is zero or not found -- returning 0")
    return balance.available_usdt

def get_mark_prices(api: ExchangeAdapter, inst_ids: list) -> dict:
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


def _get_prices_blofin(api: ExchangeAdapter, inst_ids: list) -> dict:
    """Fetch last prices from the exchange via the adapter.

    Name retained for call-site stability; now exchange-agnostic. Field-fallback
    + zero-price-exclusion logic lives in each adapter's get_price().
    """
    prices = {}
    for inst_id in inst_ids:
        price = api.get_price(inst_id)
        if price is None:
            log.warning(f"  {inst_id}: no price from {api.exchange_name} -- excluded")
        else:
            prices[inst_id] = price
    return prices

def get_instrument_info(api: ExchangeAdapter, inst_id: str) -> InstrumentInfo:
    """Fetch instrument constraints from the exchange via the adapter.

    Returns the InstrumentInfo dataclass directly (previously returned a dict).
    Field-fallback and _safe_float defaults live in each adapter.
    """
    return api.get_instrument_info(inst_id)

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

def get_actual_positions(
    api: ExchangeAdapter, inst_ids: list,
) -> list[PositionInfo]:
    """Fetch actual open positions from the exchange via the adapter.

    Returns list[PositionInfo] (previously returned {inst_id: contracts} dict).
    Used for pre-entry stale-position detection and state reconciliation.
    Empty list = no positions found. If inst_ids is empty, all open positions
    are returned (no filtering).

    Field-fallback (positions vs pos) and size>0 filter live in each adapter.
    Bulk-call semantics preserved; BloFinAdapter passes the adapter-side filter
    as None when inst_ids is empty.
    """
    positions = api.get_positions(inst_ids if inst_ids else None)
    for p in positions:
        log.info(f"  Position: {p.inst_id} = {p.contracts} contracts")
    return positions


# ==========================================================================
# PORTFOLIO RETURN HELPERS
# ==========================================================================

def equal_weight_return(current: dict, ref: dict,
                        sym_stopped: dict | None = None) -> float:
    """
    Equal-weight 1x return: mean((price/ref_price) - 1) across symbols.
    Symbols missing from current (delisted/fetch failure) are excluded;
    the denominator shrinks automatically -- no fabricated 0 returns.

    sym_stopped (optional): {inst_id: clamped_return_decimal} — symbols whose
    portfolio stop has fired. For these, the clamped value (typically -0.06)
    is substituted for the live (current/ref - 1) calculation. This mirrors
    the audit's apply_raw_stop in rebuild_portfolio_matrix.py — once a
    symbol crosses STOP_RAW_PCT, its contribution to the portfolio mean is
    locked at the stop level for the rest of the session, regardless of
    how its market price moves afterwards. Audit-vs-live alignment for the
    early-fill (sess) trigger relies on this clamp being applied here.
    Pass None to disable clamping (legacy behaviour).
    """
    sym_stopped = sym_stopped or {}
    rets = []
    for k in ref:
        if k in sym_stopped:
            rets.append(float(sym_stopped[k]))
        elif k in current and ref[k] > 0:
            rets.append(current[k] / ref[k] - 1.0)
    if not rets:
        return 0.0
    return float(np.mean(rets))


# ==========================================================================
# ORDER EXECUTION
# ==========================================================================

def enter_positions(api: ExchangeAdapter, inst_ids, entry_prices,
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

    # Rate-limit avoidance: BloFin imposes a temporary 5-min ban on
    # high-frequency request bursts against low-liquidity pairs (per
    # their docs; observed 2026-04-24 ZEREBRO-USDT). Prior behavior
    # fired one order every ~1s (4.5s total for 5 symbols) which is
    # in the burst-detect zone. Space each placement with a base
    # delay + random jitter to flatten the rate and look less
    # bot-like to the rate-limit heuristic.
    import random as _random
    ENTRY_SPACING_BASE_S   = 5.0
    ENTRY_SPACING_JITTER_S = 3.0
    _entry_order_idx = 0

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
        ctval   = info.contract_value
        lot     = info.lot_size
        min_sz  = info.min_size
        max_mkt = info.max_market_size
        state   = info.state
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

        # Set leverage (integer, hard-fail -- wrong leverage = wrong risk exposure).
        # Adapter handles margin_mode internally (BloFin: per-symbol POST;
        # Binance margin: verification via get_max_margin_loan).
        lev_result = api.set_leverage(inst_id=inst_id, leverage=lev_int)
        if not lev_result.success:
            log.error(
                f"  {inst_id}: set_leverage rejected (code={lev_result.error_code} "
                f"msg={lev_result.error_msg}) -- skipping to avoid wrong leverage exposure"
            )
            symbol_status[inst_id]["skipped_reason"] = (
                f"set_leverage_rejected_{lev_result.error_code}"
            )
            continue

        # Place order(s), chunking if maxMarketSize is set.
        # Optionally attach an exchange-native stop-loss at entry. Only on
        # adapters where the exchange supports atomic-with-entry SL (BloFin
        # yes; Binance margin no — client-side SL runs via the port_sl /
        # port_tsl monitoring loop on those paths). Belt+suspenders: adapter
        # also raises NotImplementedError if a non-None SL arrives.
        sl_trigger = None
        if EXCHANGE_SL_ENABLED and api.native_sl_supported:
            sl_trigger = price * (1 + EXCHANGE_SL_PCT)
            log.info(f"  {inst_id}: exchange SL set at ${sl_trigger:,.4f} "
                     f"({EXCHANGE_SL_PCT*100:.1f}% from entry)")

        # Space out order placements (rate-limit avoidance). Skip the
        # delay on the first order since there's no preceding request
        # to flatten against. `_entry_order_idx` resets per entry phase
        # (enter_positions call), so retry rounds don't inherit this.
        if _entry_order_idx > 0 and not dry_run:
            _delay = ENTRY_SPACING_BASE_S + _random.uniform(0, ENTRY_SPACING_JITTER_S)
            log.info(f"  {inst_id}: spacing delay {_delay:.2f}s before placement")
            time.sleep(_delay)
        _entry_order_idx += 1

        ok, order_id, err_msg = _place_order_chunked(
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
                "ctval": ctval,
            }
            symbol_status[inst_id]["filled_contracts"] = contracts
            symbol_status[inst_id]["fill_pct"] = 100.0
            symbol_status[inst_id]["order_ids"].append(order_id)
            filled_first_pass += 1
        elif _is_risk_control_error(err_msg):
            # BloFin risk-control is sticky; retrying in the next 16s won't
            # help. Mark skipped, skip the retry loop, and let operator
            # manual fill + per-bar reconcile pick it up in the monitoring
            # loop (feedback 2026-04-24 ZEREBRO incident).
            log.warning(
                f"  ⚠️  {inst_id}: risk-control rejection — skipping retries, "
                f"monitoring-loop reconcile will adopt any manual fill. "
                f"msg={err_msg}"
            )
            symbol_status[inst_id]["skipped_reason"] = "exchange_risk_control"
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

    # Retry cadence: 5.5 min (330s) between rounds. Rationale — BloFin
    # imposes a temporary 5-min ban on burst requests against
    # low-liquidity pairs; retrying inside that window guarantees
    # re-rejection and prolongs the ban. 5.5 min adds a 30s safety
    # margin past the documented ban expiry. Risk-control errors are
    # already classified as non-retriable upstream (see
    # _is_risk_control_error) — this retry handles only OTHER transient
    # errors (network, auth hiccup, etc.), which at 5.5 min cadence
    # should resolve on the first retry or not at all. MAX_ENTRY_RETRIES
    # is therefore 1: multiple retries at this cadence would push the
    # entry phase past monitoring bar 1 at +5:00 min and delay coverage
    # on the symbols that successfully filled.
    MAX_ENTRY_RETRIES = 1
    RETRY_SPACING_S = 330
    retry_round = 0
    while failed and not dry_run and retry_round < MAX_ENTRY_RETRIES:
        retry_round += 1
        time.sleep(RETRY_SPACING_S)
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
            if EXCHANGE_SL_ENABLED and api.native_sl_supported:
                sl_trigger = price * (1 + EXCHANGE_SL_PCT)

            filled_total = 0.0
            round_order_ids = []
            ok1, order_id1, err_msg1 = _place_order_chunked(
                api, inst_id, half, lot, min_sz, max_mkt,
                sl_trigger_price=sl_trigger,
            )
            # If we hit risk control on a retry, stop retrying this symbol
            # for the rest of the entry phase. BloFin's 5-min ban is
            # triggered by rapid-fire requests; continuing to retry
            # prolongs the ban and doesn't help.
            if not ok1 and _is_risk_control_error(err_msg1):
                log.warning(
                    f"  ⚠️  {inst_id}: risk-control rejection on retry — "
                    f"abandoning retries for this symbol"
                )
                if symbol_status[inst_id]["skipped_reason"] is None:
                    symbol_status[inst_id]["skipped_reason"] = "exchange_risk_control"
                continue  # skip this item in the retry loop
            if ok1:
                log.info(f"  ✅ {inst_id} retry half-1 {half}ct orderId={order_id1}")
                filled_total += half
                round_order_ids.append(order_id1)
                time.sleep(1)
                remainder_first = _round_to_lot(target - half, lot)
                if remainder_first >= min_sz:
                    ok2, order_id2, _err_msg2 = _place_order_chunked(
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
                        "ctval": ctval,
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

def _is_risk_control_error(err_msg: str) -> bool:
    """BloFin's low-liquidity / risk-control rejection is sticky for N
    minutes — retrying inside the entry phase (burst of 4 in 16s) doesn't
    help and burns API budget. Callers match this to skip the retry loop
    and let the per-bar monitoring-loop reconcile pick up any operator
    manual fill instead."""
    low = (err_msg or "").lower()
    return "risk control" in low or "low-liquidity pair" in low


def _place_order_chunked(trading_api: ExchangeAdapter, inst_id: str,
                          total_contracts: float, lot: float, min_sz: float,
                          max_mkt_sz,
                          sl_trigger_price: float | None = None) -> tuple:
    """
    Place one or more market buy orders to fill total_contracts,
    respecting maxMarketSize by splitting into chunks.
    If sl_trigger_price is set AND the adapter supports native SL, attaches
    an exchange-native stop-loss to the first chunk only. On adapters without
    native SL (e.g., Binance margin), SL is monitored client-side by the
    port_sl / port_tsl loop instead; callers should pass sl_trigger_price=None
    for those paths.
    Returns (success: bool, last_order_id_or_tag: str, error_msg: str).
    error_msg is "" on success, the BloFin-surfaced rejection message
    otherwise (or the exception string for transport-level failures).
    Callers use error_msg to skip retries for known non-retriable
    errors (e.g. risk control).

    Retry-on-102015 (exceeds maxMarketSize) lives HERE, not in the adapter.
    Adapter surfaces error_code="102015"; trader reduces chunk cap by 20%
    and retries. Keeps retry responsibility single-source so we don't get
    multiplicative retry-on-retry under cascading failures.
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

        # Attach SL only on the first chunk AND only on exchanges with native
        # SL support; subsequent chunks are additions to the same position
        # and the SL is already set at the exchange level.
        sl_price = sl_trigger_price if (chunk_num == 1 and trading_api.native_sl_supported) else None

        result = trading_api.place_entry_order(
            inst_id=inst_id, size=chunk, sl_trigger_price=sl_price,
        )

        # 102015 (exceeds maxMarketSize) — reduce chunk cap by 20% and retry.
        if result.error_code == "102015":
            new_cap = _round_to_lot(max(min_sz, chunk_cap * 0.8), lot)
            if new_cap >= chunk_cap or new_cap < min_sz:
                log.error(f"  {inst_id}: cannot reduce chunk further (cap={chunk_cap})")
                return False, "max_size_error", result.error_msg or ""
            log.warning(f"  {inst_id}: maxMarketSize hit -- reducing cap {chunk_cap} -> {new_cap}")
            chunk_cap = new_cap
            continue

        if not result.success:
            if result.error_code == "exception":
                log.error(f"  {inst_id}: place_entry_order exception: {result.error_msg}")
                return False, "exception", result.error_msg or ""
            log.error(
                f"  {inst_id}: order error code={result.error_code} "
                f"msg={result.error_msg}"
            )
            return False, f"error_{result.error_code}", result.error_msg or ""

        last_id   = result.order_id or "unknown"
        remaining = _round_to_lot(remaining - chunk, lot)

    if remaining > min_sz / 2.0:
        log.warning(f"  {inst_id}: unfilled remainder {remaining}ct -- partial fill")

    return True, last_id, ""


def reconcile_fill_prices(api: ExchangeAdapter, positions: list) -> list:
    """
    Fetch actual average-fill prices from BloFin positions and attach to
    each position dict. Computes slippage in basis points vs est_entry_price.
    Called once after all orders are placed (bulk, not per-symbol).
    Leaves fill_entry_price and entry_slippage_bps as None for any symbol
    that cannot be matched.
    """
    if not positions:
        return positions

    inst_ids = [p["inst_id"] for p in positions]
    adapter_positions = api.get_positions(inst_ids)
    by_inst = {
        p.inst_id: p.average_price
        for p in adapter_positions
        if p.average_price is not None
    }

    if not by_inst and adapter_positions == []:
        # Distinguish "exchange returned no data" from "positions found but no
        # average_price available on them"; the former is a fetch failure, the
        # latter is data-shape.
        log.warning("Could not fetch positions for fill reconciliation")
        for pos in positions:
            pos.setdefault("fill_entry_price", None)
            pos.setdefault("entry_slippage_bps", None)
        return positions

    summary = []
    for pos in positions:
        iid  = pos["inst_id"]
        est  = pos.get("entry_price")
        fill = by_inst.get(iid)
        if fill is None or not est or est <= 0:
            pos["fill_entry_price"]   = None
            pos["entry_slippage_bps"] = None
            log.warning(f"  {iid}: entry fill price not found on {api.exchange_name} -- leaving null")
            continue
        pos["fill_entry_price"]   = round(fill, 6)
        slip_bps                  = (fill / est - 1.0) * 10000
        pos["entry_slippage_bps"] = round(slip_bps, 2)
        summary.append(f"{iid} est=${est:,.4f} fill=${fill:,.4f} slip={slip_bps:+.2f}bps")

    if summary:
        log.info("  Fill prices: " + "  |  ".join(summary))

    return positions


def reconcile_exit_prices(api: ExchangeAdapter, positions: list,
                          est_exit_prices: dict) -> list:
    """
    Fetch actual exit fill prices from the exchange's fill history and attach
    to each position dict. Computes exit slippage in basis points vs the
    monitoring loop's last price snapshot for that symbol.

    Matches by inst_id + side=sell, picking the most recent matching fill.
    Negative exit_slippage_bps is favorable (sold higher than estimated).

    NOTE: just-closed positions aren't in api.get_positions() anymore, so we
    MUST pass inst_ids explicitly — caller-responsibility clause in
    BinanceMarginAdapter.get_recent_fills docstring. BloFin adapter post-filters
    client-side; same semantic either way.
    """
    if not positions:
        return positions

    inst_ids = [p["inst_id"] for p in positions]
    fills = api.get_recent_fills(inst_ids=inst_ids)

    if not fills:
        log.warning(f"Could not fetch fills history for exit reconciliation on {api.exchange_name}")
        for pos in positions:
            pos.setdefault("fill_exit_price", None)
            pos.setdefault("exit_slippage_bps", None)
        return positions

    # Group latest sell fills per inst_id.
    fills_by_inst = {}
    for f in fills:
        if f.side != "sell":
            continue
        cur = fills_by_inst.get(f.inst_id)
        if cur is None or f.ts_ms > cur[0]:
            fills_by_inst[f.inst_id] = (f.ts_ms, f.price)

    summary = []
    for pos in positions:
        iid  = pos["inst_id"]
        est  = est_exit_prices.get(iid)
        hit  = fills_by_inst.get(iid)
        fill = hit[1] if hit else None
        if fill is None or not est or est <= 0:
            pos["fill_exit_price"]   = None
            pos["exit_slippage_bps"] = None
            log.warning(f"  {iid}: exit fill price not found on {api.exchange_name} -- leaving null")
            continue
        pos["fill_exit_price"]   = round(fill, 6)
        slip_bps                 = (fill / est - 1.0) * 10000
        pos["exit_slippage_bps"] = round(slip_bps, 2)
        summary.append(f"{iid} est=${est:,.4f} fill=${fill:,.4f} slip={slip_bps:+.2f}bps")

    if summary:
        log.info("  Exit prices: " + "  |  ".join(summary))

    return positions


def close_all_positions(api: ExchangeAdapter, positions, reason, dry_run) -> list:
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
        actual_list = get_actual_positions(api, inst_ids)
        actual = {p.inst_id: p.contracts for p in actual_list}
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
            # Primary: native close endpoint (BloFin /trade/close-position;
            # Binance margin: MARKET SELL of free balance with AUTO_REPAY).
            # More reliable than reduce-only for BloFin; adapter handles
            # margin_mode/position_side internally.
            result = api.close_position(inst_id)
            if result.success:
                log.info(f"  ✅ {inst_id}  closed via {api.exchange_name} close-position")
            else:
                # Fallback: reduce-only market sell
                log.warning(
                    f"  {inst_id}: close-position returned code={result.error_code} "
                    f"msg={result.error_msg} -- falling back to reduce-only market sell"
                )
                result2 = api.place_reduce_order(inst_id, contracts)
                if result2.success:
                    log.info(f"  ✅ {inst_id}  fallback orderId={result2.order_id}")
                else:
                    log.error(
                        f"  ❌ {inst_id}: reduce-only fallback also failed "
                        f"code={result2.error_code} msg={result2.error_msg}"
                    )
                    failed.append(pos)
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
    # DEPRECATED: the containerized master run_session is legacy. Production
    # invocation is always per-allocation via --allocation-id; host master
    # runs a separate file at /root/benji/trader-blofin.py. Path kept functional
    # for --close-all and manual debugging, but no cron hits this.
    log.warning(
        "run_session is the deprecated master path in the container. "
        "Production uses --allocation-id; host cron uses /root/benji/trader-blofin.py."
    )
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
        existing_ids = [p.inst_id for p in existing]
        alert(
            f"{len(existing)} stale position(s) found before entry: "
            f"{existing_ids}. Closing them now before entering today's session.",
            subject="trader-blofin STALE POSITIONS CLOSED"
        )
        log.warning(f"Stale positions found -- closing before entry: {existing_ids}")
        stale = [{"inst_id": p.inst_id, "contracts": int(p.contracts),
                  "marginMode": MARGIN_MODE, "positionSide": POSITION_SIDE}
                 for p in existing]
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
    # DEPRECATED master-account path: vol_boost defaults to 1.0 (L_HIGH floor).
    # The live per-allocation path reads boost from strategy_version.
    # current_metrics.vol_boost (populated nightly by refresh_strategy_metrics).
    boost   = 1.0
    eff_lev = round(L_HIGH * boost, 4)
    lev_int = max(1, int(math.ceil(eff_lev)))
    log.info(f"Effective leverage: {L_HIGH} x {boost:.3f} = {eff_lev:.3f}x "
             f"(deprecated path — no vol_boost)")

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


def _append_portfolio_session_entered(session_id: str | None, iid: str) -> None:
    """Best-effort: append `iid` to portfolio_sessions.entered[].

    Called when _reconcile_positions_with_exchange adopts a manual UI fill
    into the live session. Without this, portfolio_sessions.entered stays
    stuck at the programmatic-fill set (e.g. 4) while positions in memory
    grows to include the reconciled symbol (5). The Manager Portfolios tab
    + Execution per-symbol count then disagree with reality.

    Idempotent via array_append + NOT ANY check so double-reconcile on
    subsequent bars doesn't duplicate the entry.
    """
    if not session_id:
        return
    try:
        conn = _trader_db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE user_mgmt.portfolio_sessions
                       SET entered    = array_append(entered, %s),
                           updated_at = NOW()
                     WHERE portfolio_session_id = %s::uuid
                       AND NOT (%s = ANY(entered))
                    """,
                    (iid, session_id, iid),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning(f"Could not append {iid} to portfolio_session {session_id[:8]}: {e}")


def _reconcile_positions_with_exchange(
    api: ExchangeAdapter,
    positions: list,
    inst_ids: list,
    entry_prices: dict,
    eff_lev: float,
    log_prefix: str = "",
    session_id: str | None = None,
) -> list:
    """Sync runtime_state.positions[] against live exchange positions.

    At monitoring-loop entry, query the exchange for actual open positions
    across `inst_ids` (today's basket symbols) and merge any positions that
    exist on the exchange but are missing from the trader's internal
    `positions` list. This handles:

      - Manual fills by the operator after the programmatic entry failed
        (e.g. BloFin low-liquidity rejection → operator fills via UI).
      - Positions opened during a prior subprocess that crashed before
        persisting to runtime_state.

    Reconciled positions join the monitoring loop with the same schema as
    programmatically-placed ones, so the software SL, per-symbol stop, and
    session-end close logic treat them identically. They're flagged with
    order_id="RECONCILED_MANUAL" for observability.

    Does NOT remove positions that exist in internal state but are absent on
    the exchange (already-closed case) — the per-bar loop detects that via
    its own positions query.

    Returns a new positions list (original is not mutated).
    """
    try:
        live_positions = api.get_positions(inst_ids) if inst_ids else []
    except Exception as e:
        log.warning(f"{log_prefix}Reconcile fetch failed ({e}); monitoring "
                    f"loop starts with runtime_state positions only")
        return positions

    known_iids = {p["inst_id"] for p in positions}
    live_by_iid = {p.inst_id: p for p in live_positions if p.contracts > 0}

    added: list = []
    for iid in inst_ids:
        if iid in known_iids:
            continue
        lp = live_by_iid.get(iid)
        if lp is None:
            continue
        avg = lp.average_price
        if avg is None or avg <= 0:
            # Fall back to today's canonical basket anchor price if the
            # exchange doesn't report averagePrice on this position.
            avg = entry_prices.get(iid)
            if avg is None or avg <= 0:
                log.warning(
                    f"{log_prefix}Reconcile: {iid} open on exchange "
                    f"({lp.contracts}ct) but no entry price available — "
                    f"skipping (monitoring loop would have no basis for "
                    f"return calculation)"
                )
                continue
        # Fetch contract_value so downstream pnl_usd math works (the
        # reconcile-add path previously omitted ctval, leaving pnl_usd
        # NULL in allocation_execution_symbols for reconciled symbols).
        try:
            _info = api.get_instrument_info(iid)
            _ctval = float(_info.contract_value) if _info.contract_value else None
        except Exception as _e:
            log.warning(f"{log_prefix}Reconcile: get_instrument_info({iid}) failed ({_e}); ctval unavailable")
            _ctval = None
        added.append({
            "inst_id":            iid,
            "lev_int":            max(1, int(math.ceil(eff_lev))),
            "order_id":           "RECONCILED_MANUAL",
            "contracts":          lp.contracts,
            "marginMode":         MARGIN_MODE,
            "entry_price":        avg,
            "positionSide":       POSITION_SIDE,
            "retry_rounds":       0,
            # Round to match reconcile_fill_prices (6 decimals) so UI doesn't
            # render the raw 18-decimal float from the exchange response.
            "fill_entry_price":   round(float(avg), 6),
            "target_contracts":   lp.contracts,
            # Null rather than 0.0 — we didn't place a programmatic order
            # with an estimated price, so there's no meaningful slippage
            # to report. 0.0 would wrongly factor into avg_entry_slip_bps.
            "entry_slippage_bps": None,
            "reconciled":         True,
            "ctval":              _ctval,
        })
        # Mirror the adopt into portfolio_sessions.entered so the
        # per-symbol count the Manager/Portfolios tabs surface stays
        # consistent with the in-memory positions list.
        _append_portfolio_session_entered(session_id, iid)

    if added:
        log.info(
            f"{log_prefix}Reconcile: picked up {len(added)} live position(s) "
            f"missing from runtime_state: "
            f"{[p['inst_id'] for p in added]} "
            f"— will be managed by the monitoring loop alongside "
            f"programmatically-placed positions"
        )

    return list(positions) + added


def _run_monitoring_loop(today, today_date, api: ExchangeAdapter, inst_ids,
                         open_prices, entry_prices, entry_1x, eff_lev,
                         peak, positions, dry_run,
                         report_base=None, session_start_utc=None,
                         balance_pre_entry=None,
                         *,
                         allocation_id: str | None = None,
                         config: "TraderConfig | None" = None,
                         session_id: str | None = None,
                         capital_deployed_usd: float = 0.0,
                         fill_report: dict | None = None,
                         conviction_roi_x: float | None = None,
                         signal_count: int | None = None,
                         resume_sym_stopped: "list[str] | dict[str, float] | None" = None,
                         resume_sym_observed: dict[str, float] | None = None,
                         pre_stopped_clamps: dict[str, float] | None = None):
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
        minutes=cfg.early_fill_x   # audit's EARLY_FILL_X — minutes from session open
    )                              # past which early-fill triggers stop firing

    exit_reason     = None
    final_return_1x = 0.0

    # Per-symbol stop tracking (matches apply_raw_stop in rebuild_portfolio_matrix.py)
    # sym_stopped: {inst_id: clamped_return}  -- symbols closed by per-symbol stop
    # Once stopped, a symbol contributes PORT_SL_PCT (-7.5%) to the portfolio average.
    #
    # On resume, rehydrate sym_stopped from runtime_state.sym_stopped (list of
    # instrument IDs) so the incr = mean(sym_returns) calculation continues
    # including the clamped -7.5% contribution for previously-stopped symbols.
    # Without this, a mid-session trader restart would drop those symbols
    # from the portfolio_avg entirely and distort incr upward (less negative),
    # which in turn distorts expected = incr × eff_lev and the delta vs
    # actual_roi. stop_raw_pct is the canonical clamp value — the stored list
    # only records which symbols stopped, not their individual clamp values,
    # because all per-symbol stops clamp at the same cfg threshold. Was
    # previously port_sl_pct (-7.5% for ALTS MAIN) by mistake; now correctly
    # uses stop_raw_pct (-6%) matching audit's apply_raw_stop.
    sym_stopped: dict[str, float] = {}
    # Display-only mirror of sym_stopped that holds the *observed* return
    # at the crossing bar (vs the audit threshold value in sym_stopped).
    # Read by sym_returns_map_open for portfolio_bars.symbol_returns.
    sym_observed: dict[str, float] = {}
    if resume_sym_stopped:
        # Newest format: sym_stopped is dict {iid: cfg.stop_raw_pct} (math),
        # paired with resume_sym_observed dict {iid: observed} (display).
        # Older format: sym_stopped persisted observed value into the same
        # dict (no separate sym_observed). Legacy: list of iids only.
        if isinstance(resume_sym_stopped, dict):
            for _iid, _clamp_val in resume_sym_stopped.items():
                sym_stopped[_iid] = float(_clamp_val)
        else:
            _clamp = float(cfg.stop_raw_pct)
            for _iid in resume_sym_stopped:
                sym_stopped[_iid] = _clamp
        # Load display clamps if present. Falls back to sym_stopped value
        # for any iid missing from the observed dict (handles older state).
        if resume_sym_observed:
            for _iid, _obs in resume_sym_observed.items():
                sym_observed[_iid] = float(_obs)
        log.info(
            f"{log_prefix}Rehydrated {len(sym_stopped)} stopped symbol(s) from "
            f"runtime_state: math={ {k: f'{v*100:+.3f}%' for k,v in sym_stopped.items()} }"
            f"  display={ {k: f'{v*100:+.3f}%' for k,v in sym_observed.items()} }"
        )

    # Late-entry pre-stopped symbols: split clamp values across math + display.
    #   sym_stopped[iid]  = cfg.stop_raw_pct  → math (audit-consistent incr)
    #   sym_observed[iid] = the observed pre-entry return  → display
    # Without the split, late-entry incr math would diverge from the audit's
    # apply_raw_stop semantics (audit always clamps at threshold).
    if pre_stopped_clamps:
        for _iid, _clamp_val in pre_stopped_clamps.items():
            sym_stopped[_iid] = float(cfg.stop_raw_pct)
            sym_observed[_iid] = float(_clamp_val)
        log.info(
            f"{log_prefix}Seeded {len(pre_stopped_clamps)} pre-stopped symbol(s) "
            f"(late-entry pre-filter): observed clamps "
            f"{ {k: f'{v*100:+.3f}%' for k,v in pre_stopped_clamps.items()} }"
            f"  math clamp at {cfg.stop_raw_pct*100:.1f}% (audit-consistent)"
        )

    # Sync runtime_state.positions[] against live exchange positions before
    # the monitoring loop starts. Picks up manual fills and any positions
    # opened out-of-band from the programmatic entry phase, so the software
    # SL / session-end close logic manages them alongside the rest. Symbols
    # already stopped in a prior run are excluded to avoid rehydrating
    # positions that were intentionally closed.
    positions = _reconcile_positions_with_exchange(
        api=api,
        positions=positions,
        inst_ids=[iid for iid in inst_ids if iid not in sym_stopped],
        entry_prices=entry_prices,
        eff_lev=eff_lev,
        log_prefix=log_prefix,
        session_id=session_id,
    )

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

    # ── Actual-account ROI baseline ───────────────────────────────────────
    # Captures total account equity at session start so each bar can log
    # the real account ROI + session PnL alongside the strategy's 1x
    # incr / peak values.
    #
    # Resolution order (each falls through on None/failure):
    #   1. balance_pre_entry (fresh-session caller passes pre-entry total)
    #   2. runtime_state.session_start_equity_usdt (prior bar persisted it)
    #   3. exchange_snapshots — earliest snapshot today at/before session
    #      start hour; this is the fallback for a mid-session rescue where
    #      the trader crashed BEFORE the first bar wrote runtime_state.
    #      Cleaner than "current equity at resume" because today's price
    #      movement has already made current equity inaccurate as a
    #      session-start baseline.
    #   4. current equity — last-resort approximation if all else fails.
    session_start_equity_usdt = balance_pre_entry
    if allocation_id is not None and session_start_equity_usdt is None:
        try:
            _cur_alloc = _fetch_allocation(allocation_id)
            _cur_state = _cur_alloc.get("runtime_state") or {}
            if _cur_state.get("session_start_equity_usdt") is not None:
                session_start_equity_usdt = float(_cur_state["session_start_equity_usdt"])
        except Exception as _e:
            log.warning(f"{log_prefix}runtime_state read for equity baseline failed: {_e}")
    if allocation_id is not None and session_start_equity_usdt is None:
        # Query exchange_snapshots for the earliest today-UTC snapshot on
        # this allocation's connection at or before the session start hour.
        try:
            _baseline_from_snaps = _fetch_session_start_equity_from_snapshots(
                allocation_id, session_start_hour=cfg.session_start_hour,
            )
            if _baseline_from_snaps is not None:
                session_start_equity_usdt = _baseline_from_snaps
                log.info(
                    f"{log_prefix}Captured session-start equity baseline: "
                    f"${session_start_equity_usdt:,.2f} "
                    "(from exchange_snapshots — trader crashed before first bar write)"
                )
        except Exception as _e:
            log.warning(f"{log_prefix}exchange_snapshots baseline lookup failed: {_e}")
    if session_start_equity_usdt is None and allocation_id is not None:
        try:
            _bal0 = api.get_balance()
            session_start_equity_usdt = float(_bal0.total_usdt)
            log.warning(
                f"{log_prefix}Using current equity ${session_start_equity_usdt:,.2f} "
                "as session-start baseline (no prior capture found). "
                "actual-ROI will be relative to restart time, not session start."
            )
        except Exception as _e:
            log.warning(
                f"{log_prefix}get_balance for ROI baseline failed: {_e} — "
                "actual-ROI will show 0 until a successful fetch."
            )
            session_start_equity_usdt = 0.0
    if session_start_equity_usdt is None:
        session_start_equity_usdt = 0.0

    # Periodic reconcile cadence: every bar. Re-query the exchange for
    # positions missing from active_positions so operator mid-session
    # fills (e.g. BloFin risk-control rejection → manual mobile-app fill)
    # are adopted within ~5 min. One extra get_positions call per 5-min
    # bar is cheap; the near-immediate adoption is the difference between
    # "sym_stop protected for the full remaining session" and "unmanaged
    # for up to an hour."
    RECONCILE_BAR_INTERVAL = 1

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

        # Periodic reconcile. Best-effort — any failure logs a WARN but
        # doesn't interrupt the bar. `active_positions` only grows here
        # (never shrinks), so this can't accidentally drop a position the
        # trader is already managing.
        if bars_monitored > 0 and bars_monitored % RECONCILE_BAR_INTERVAL == 0:
            try:
                before_n = len(active_positions)
                reconciled = _reconcile_positions_with_exchange(
                    api=api,
                    positions=active_positions,
                    inst_ids=[iid for iid in inst_ids if iid not in sym_stopped],
                    entry_prices=entry_prices,
                    eff_lev=eff_lev,
                    log_prefix=log_prefix,
                    session_id=session_id,
                )
                if len(reconciled) > before_n:
                    active_positions = reconciled
                    # Keep `positions` (the outer full list) in sync so the
                    # close-out phase sees the adopted symbols too.
                    for p in reconciled[before_n:]:
                        if p not in positions:
                            positions.append(p)
            except Exception as _e:
                log.warning(f"{log_prefix}Periodic reconcile failed: {_e}")

        # ── Per-symbol stop check ──────────────────────────────────────────
        # Check each still-active symbol against its individual stop_raw_pct
        # threshold (canonical -6%). Mirrors audit's apply_raw_stop in
        # rebuild_portfolio_matrix.py: anchor at OPEN price (06:00 UTC,
        # matrix's bar 0), trigger + clamp at stop_raw_pct. Earlier this
        # used cfg.port_sl_pct (-7.5% for ALTS MAIN) by mistake — symbols
        # held 1.5pp longer + clamped 1.5pp lower than audit. Fixed
        # 2026-04-25.
        newly_stopped = []
        for pos in list(active_positions):
            iid   = pos["inst_id"]
            ref   = open_prices.get(iid, 0)  # 06:00 anchor (audit parity)
            price = current.get(iid, 0)
            if not ref or not price:
                continue
            sym_ret = price / ref - 1.0
            if sym_ret <= cfg.stop_raw_pct:
                # Audit-consistent semantics — TWO separate clamp values:
                #
                #   sym_stopped[iid]  = cfg.stop_raw_pct (-6%)
                #     → drives sym_returns_map → incr → port_sl, port_tsl,
                #       early_fill checks. MUST match the audit's
                #       apply_raw_stop, which clamps at the threshold.
                #       Otherwise live triggers fire at different bars
                #       than the backtest predicted, breaking the
                #       audit-vs-live comparison.
                #
                #   sym_observed[iid] = sym_ret (e.g. -6.76%)
                #     → drives sym_returns_map_open → display in
                #       portfolio_bars.symbol_returns. Reflects the actual
                #       fill price at the crossing bar so the matrix
                #       shows what BloFin executed, not a synthetic -6%.
                log.warning(
                    f"{log_prefix}  SYM STOP: {iid} ret={sym_ret*100:.3f}% "
                    f"<= {cfg.stop_raw_pct*100:.1f}% -- closing symbol; "
                    f"math clamp={cfg.stop_raw_pct*100:.1f}% (audit), "
                    f"display clamp={sym_ret*100:.3f}% (observed)"
                )
                sym_stopped[iid] = float(cfg.stop_raw_pct)
                sym_observed[iid] = float(sym_ret)
                sym_exit_prices[iid] = price
                newly_stopped.append(pos)
                active_positions = [p for p in active_positions if p["inst_id"] != iid]

        if newly_stopped and not dry_run:
            failed = close_all_positions(api, newly_stopped, "sym_stop", dry_run)
            if failed:
                # If close failed, keep in active pool and don't clamp
                for p in failed:
                    del sym_stopped[p["inst_id"]]
                    sym_observed.pop(p["inst_id"], None)
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

        # Open-anchored per-symbol returns for the Portfolio UI matrix.
        # The per-symbol stop check above (cfg.stop_raw_pct) anchors on
        # open_prices (audit's apply_raw_stop p0 = 06:00 UTC bar). Storing
        # entry-anchored returns in portfolio_bars made the matrix display
        # values 3pp+ worse than the trigger threshold whenever fills slipped
        # against open, so a row at -8% in the UI looked like a missed stop
        # even though the open-based ret was still above -6%. Same denominator
        # on both sides removes that confusion.
        sym_returns_map_open = {}
        for iid in inst_ids:
            if iid in sym_stopped:
                # Display: observed return at crossing bar; falls back to
                # threshold for legacy/resume sessions where observed
                # value wasn't recorded.
                sym_returns_map_open[iid] = sym_observed.get(iid, sym_stopped[iid])
            elif iid in current and open_prices.get(iid, 0) > 0:
                sym_returns_map_open[iid] = current[iid] / open_prices[iid] - 1.0

        # session_ret uses open_prices anchor (06:00 UTC) AND clamps stopped
        # symbols at their realized stop value — mirrors the audit's path_1x
        # in rebuild_portfolio_matrix.py:apply_raw_stop. This is what the
        # EARLY_FILL_Y trigger compares against. Only difference vs `incr`
        # above: anchor (06:00 vs entry @ 06:35). Both apply the same -6%
        # clamp to stopped symbols, matching audit semantics.
        session_ret = equal_weight_return(current, open_prices, sym_stopped)

        peak     = max(peak, incr)
        tsl_dist = incr - peak
        fill_open = utcnow() <= fill_gate_dt

        # Actual account ROI — live fetch per bar, compared against the 1x
        # strategy return (incr). Leveraged-expected = incr × eff_lev; delta
        # surfaces fees / slippage drift between strategy assumption and the
        # real account. Network failures here don't affect SL/TSL decisions.
        current_equity_usdt = 0.0
        actual_roi = 0.0
        session_pnl_usd = 0.0
        try:
            _bal = api.get_balance()
            current_equity_usdt = float(_bal.total_usdt)
            if session_start_equity_usdt > 0:
                actual_roi = current_equity_usdt / session_start_equity_usdt - 1.0
                session_pnl_usd = current_equity_usdt - session_start_equity_usdt
        except Exception as _e:
            log.warning(f"{log_prefix}Bar {b:3d}: equity fetch failed: {_e}")

        # Scale expected_roi by the structural deploy fraction. enter_positions
        # holds back a 10% MARGIN_BUFFER (trader_blofin.py:1046) so only ~90%
        # of total equity goes into leveraged positions. Hardcoding 0.90
        # rather than computing from runtime_state.capital_deployed_usd
        # because the persisted value reflects "this allocation's new orders"
        # and can be artificially low when other positions on the same BloFin
        # account pre-empt available margin (observed 2026-04-25: persisted
        # $1,464 vs effective ~92% of account, because pre-existing long-crypto
        # positions moved with this allocation's basket). The 0.90 constant
        # captures the buffer drag — the residual fee/slippage signal lives
        # in delta after this scaling.
        DEPLOY_RATIO = 0.90
        expected_roi = incr * eff_lev * DEPLOY_RATIO
        roi_delta    = actual_roi - expected_roi

        log.info(
            f"{log_prefix}Bar {b:3d} | incr={incr*100:+.3f}%  peak={peak*100:.3f}%  "
            f"tsl={tsl_dist*100:+.3f}%  sess={session_ret*100:+.3f}%  "
            # Denominator uses inst_ids (the stable full portfolio) rather
            # than positions (which is only active_positions at loop start
            # → on resume becomes the post-stops active count, making the
            # "X/Y" display shrink incorrectly after a restart).
            f"active={len(active_positions)}/{len(inst_ids)}  "
            f"stopped={len(sym_stopped)}  "
            f"fill={'open' if fill_open else 'closed'}"
        )
        log.info(
            f"{log_prefix}         | actual_roi={actual_roi*100:+.3f}%  "
            f"equity=${current_equity_usdt:,.2f}  "
            f"pnl=${session_pnl_usd:+,.2f}  "
            f"expected={expected_roi*100:+.3f}% (incr×lev×0.90)  "
            f"delta={roi_delta*100:+.3f}%"
        )

        # Persist this bar's snapshot before the break checks so the bar that
        # tripped an exit is included in the timeline. Both writes are
        # independent + best-effort.
        _bar_ts   = utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _bar_stop = list(sym_stopped.keys())
        if allocation_id is None:
            # Master: NDJSON + master-schema SQL row (matched by signal_date).
            append_portfolio_bar(
                today, b, _bar_ts, incr, peak, sym_returns_map_open, _bar_stop,
            )
            append_portfolio_bar_sql(
                today, b, _bar_ts, incr, peak, sym_returns_map_open, _bar_stop,
            )
        else:
            # Allocation: session row is FK-identified by session_id; no NDJSON.
            _append_portfolio_bar_for_session(
                session_id=session_id, bar=b,
                ts_utc=utcnow().replace(tzinfo=datetime.timezone.utc),
                incr=incr, peak=peak,
                sym_returns=sym_returns_map_open, stopped=_bar_stop,
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
            # Math clamp values per-symbol — used by the resumed loop's incr
            # calc. Audit-consistent (cfg.stop_raw_pct). Legacy list format
            # still supported in the resume path via isinstance check.
            "sym_stopped": {k: float(v) for k, v in sym_stopped.items()},
            # Display clamp values — observed return at the crossing bar
            # (or at the late-entry pre-filter check). Used by sym_returns_
            # map_open for portfolio_bars.symbol_returns. Empty when no
            # symbols have stopped yet.
            "sym_observed": {k: float(v) for k, v in sym_observed.items()},
            "effective_leverage": eff_lev, "entry_1x": round(entry_1x, 6),
            "open_prices":  {k: v for k, v in open_prices.items()},
            "entry_prices": {k: v for k, v in entry_prices.items()},
            "positions": active_positions,
            "session_start_equity_usdt": round(session_start_equity_usdt, 2),
            "current_equity_usdt": round(current_equity_usdt, 2),
            "session_pnl_usd": round(session_pnl_usd, 2),
            "actual_roi": round(actual_roi, 6),
        }
        if allocation_id is None:
            save_state(_state)
        else:
            _state["session_id"] = session_id
            _state["symbols"] = list(inst_ids)
            _state["capital_deployed_usd"] = float(capital_deployed_usd)
            # Preserve fill_report so a mid-session respawn (and the final
            # close-time _log_allocation_return write) still sees it. Per-bar
            # state writes otherwise replace the whole JSONB blob, dropping
            # the session-start fill_report key.
            if fill_report is not None:
                _state["fill_report"] = fill_report
            # Persist pre_stopped_clamps unconditionally — _mark_runtime_state
            # REPLACES the entire JSONB blob, so omitting the key when empty
            # causes it to be silently dropped on every bar. Always-write keeps
            # resume-path semantics deterministic (key present = authoritative
            # state; key absent ONLY for legacy pre-fix sessions).
            _state["pre_stopped_clamps"] = pre_stopped_clamps or {}
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

    # Allocation path calls _log_allocation_return() AFTER reconcile_exit_prices
    # runs (below, in the allocation-mode close block) so the writer captures
    # post-reconcile exit_slippage_bps. Master-path CSV logging was removed
    # with the CSV-based compute_vol_boost (now sourced from nightly audit
    # refresh instead of live returns log).
    if math.isnan(final_return_1x):
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
        # Reconcile exit prices FIRST so _log_allocation_return() captures
        # post-reconcile exit_slippage_bps. Previously the writer ran in the
        # earlier return-logging block with un-reconciled positions, which
        # persisted NULL slip/fill values every session.
        if not dry_run and est_exit_prices:
            try:
                reconcile_exit_prices(api, positions, est_exit_prices)
            except Exception as e:
                log.warning(f"{log_prefix}Exit-price reconcile failed: {e}")

        # Now write allocation_returns with reconciled telemetry. NaN-guarded
        # and wrapped in a one-retry try/except so a transient DB hiccup
        # doesn't drop the row.
        # A WARN fires when positions are non-empty but slip is null — surfaces
        # future reconcile misses before they become silent data gaps.
        if not math.isnan(final_return_1x):
            post_exit_equity_usd = None
            if not dry_run:
                try:
                    post_exit_equity_usd = get_account_balance_usdt(api)
                except Exception as e:
                    log.warning(
                        f"{log_prefix}Could not fetch post-exit equity for "
                        f"performance_daily: {e}"
                    )

            # Telemetry completeness check — log a WARN if reconcile ran but
            # exit_slippage_bps is missing on every non-dry-run position. That
            # combination means the writer is about to persist NULL despite
            # having the opportunity to capture real values.
            if not dry_run and positions:
                n_with_exit_slip = sum(
                    1 for p in positions
                    if p.get("exit_slippage_bps") is not None
                )
                if n_with_exit_slip == 0:
                    log.warning(
                        f"{log_prefix}Writer called with {len(positions)} positions but "
                        f"zero have exit_slippage_bps — avg_exit_slip_bps will be NULL"
                    )

            last_err: Exception | None = None
            for attempt in (1, 2):
                try:
                    _log_allocation_return(
                        allocation_id, today,
                        net_return_pct=net_pct,
                        exit_reason=exit_reason,
                        effective_leverage=eff_lev,
                        capital_deployed_usd=float(capital_deployed_usd),
                        config=cfg,
                        equity_usd=post_exit_equity_usd,
                        signal_count=signal_count,
                        conviction_roi_x=conviction_roi_x,
                        fill_report=fill_report,
                        positions=positions,
                    )
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    log.warning(
                        f"{log_prefix}_log_allocation_return attempt {attempt}/2 failed: {e}"
                    )
            if last_err is not None:
                log.error(
                    f"{log_prefix}_log_allocation_return failed twice; "
                    f"allocation_returns row will be incomplete until the "
                    f"nightly backfill cron runs"
                )

            # Per-symbol execution telemetry (Gap 7). Best-effort — logs a
            # WARN on failure but doesn't raise. Additive telemetry; a miss
            # only degrades the Manager Execution per-symbol expand for
            # that session.
            try:
                _log_allocation_execution_symbols(
                    allocation_id, today,
                    positions=positions,
                    sym_stops_fired=sym_stops_fired,
                    session_exit_reason=exit_reason,
                    fill_report=fill_report,
                    est_exit_prices=est_exit_prices,
                )
            except Exception as e:
                log.warning(
                    f"{log_prefix}_log_allocation_execution_symbols failed: {e}"
                )

            # ── Compounding mode: roll session-close equity into capital_usd ─
            # When allocations.compounding_mode = 'compound', capital_usd
            # auto-updates to the post-exit wallet equity so the next
            # session sizes off realized gains/losses without a manual
            # edit. Fixed mode leaves capital_usd alone; profits accumulate
            # in the wallet as idle capital. Runs once per session, guarded
            # by the same NaN-check as the writer. Best-effort — on DB
            # failure we log a WARN and leave capital_usd as-is rather
            # than destabilizing the next session start.
            if not dry_run and post_exit_equity_usd is not None:
                try:
                    _compound_capital_if_enabled(
                        allocation_id, post_exit_equity_usd, log_prefix,
                    )
                except Exception as e:
                    log.warning(
                        f"{log_prefix}Compound update skipped: {e}"
                    )
            elif not dry_run and post_exit_equity_usd is None:
                log.warning(
                    f"{log_prefix}Compound update skipped for allocation "
                    f"{allocation_id}: no session close equity captured"
                )

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
# ALLOCATION MODE (multi-tenant)
# ==========================================================================
# Everything below this banner is only reached when --allocation-id is set.
# The master-account execution path above does not call any of this.
#
# High-level flow:
#   1. _acquire_allocation_lock — atomic CAS on allocations.lock_acquired_at
#      (24h staleness threshold, same semantic as the filesystem LOCK_FILE)
#   2. Load allocation row → TraderConfig via strategy_version.config JSONB
#   3. Decrypt per-user exchange creds via credential_loader
#   4. Dispatch to the right ExchangeAdapter via adapter_for(creds)
#      (BloFin perps or Binance cross-margin; see app.services.exchanges.*)
#   5. Branch on runtime_state.phase: resume active / already-finished / fresh
#   6. Persist runtime_state JSONB (replaces filesystem STATE_FILE) at the
#      same logical save points as the master script's save_state() calls
#   7. Write per-allocation portfolio_sessions row + allocation_returns on close

# ─── Helpers ────────────────────────────────────────────────────────────────

# Globally-tracked "I currently hold this allocation's lock" reference. Read
# by the SIGTERM handler + atexit hook below so a graceful container shutdown
# (SIGTERM from `docker compose up --force-recreate`) releases the lock before
# the process dies. Prevents next-morning's spawn from bouncing off a
# stale-but-recent lock (see 2026-04-24 06:05 UTC incident).
_HELD_ALLOCATION_LOCK: str | None = None
# Mirror of the active portfolio_session row owned by this process. Set when
# the INSERT (fresh) or state-read (resume) hands us a session_id; cleared on
# normal close. On SIGTERM the shutdown handler closes it with
# exit_reason='subprocess_died' so the Portfolios tab doesn't render a phantom
# LIVE row after a container rebuild mid-session (2026-04-23 incident).
_HELD_PORTFOLIO_SESSION_ID: str | None = None


def _track_active_portfolio_session(session_id: str | None) -> None:
    """Record the session this process currently owns so shutdown can close it."""
    global _HELD_PORTFOLIO_SESSION_ID
    if session_id:
        _HELD_PORTFOLIO_SESSION_ID = str(session_id)


def _release_held_lock_on_shutdown() -> None:
    """Best-effort lock release + portfolio-session close on Python process exit.

    Fires from two paths:
      1. SIGTERM / SIGINT signal handler (below)
      2. atexit — any normal or SystemExit termination

    SIGKILL doesn't run either path — the supervisor's orphan-PID detection
    (follow-up) is the belt-and-suspenders defense for that case. Most
    container rebuilds use SIGTERM with a 10s grace period, which this
    catches.
    """
    global _HELD_ALLOCATION_LOCK, _HELD_PORTFOLIO_SESSION_ID
    sid = _HELD_PORTFOLIO_SESSION_ID
    if sid:
        try:
            conn = _trader_db_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE user_mgmt.portfolio_sessions
                           SET status        = 'closed',
                               exit_reason   = 'subprocess_died',
                               exit_time_utc = NOW(),
                               updated_at    = NOW()
                         WHERE portfolio_session_id = %s::uuid
                           AND status = 'active'
                        """,
                        (sid,),
                    )
                conn.commit()
            finally:
                conn.close()
            log.info(f"Shutdown handler: closed portfolio_session {sid[:8]} as subprocess_died")
        except Exception as e:
            log.warning(f"Shutdown handler session close failed for {sid[:8]}: {e}")
        _HELD_PORTFOLIO_SESSION_ID = None
    aid = _HELD_ALLOCATION_LOCK
    if aid:
        try:
            _release_allocation_lock(aid)
            log.info(f"Shutdown handler: released allocation lock {aid[:8]}")
        except Exception as e:
            log.warning(f"Shutdown handler lock release failed for {aid[:8]}: {e}")
        _HELD_ALLOCATION_LOCK = None


def _sigterm_handler(signum, _frame):
    """Graceful shutdown: release lock, then exit."""
    import signal as _sig
    name = {_sig.SIGTERM: "SIGTERM", _sig.SIGINT: "SIGINT"}.get(signum, f"signal {signum}")
    log.warning(f"Received {name} — releasing allocation lock and exiting")
    _release_held_lock_on_shutdown()
    sys.exit(0)


# Wire up handlers once at module import. atexit covers normal exits;
# signal handlers cover SIGTERM/SIGINT (which otherwise bypass finally
# blocks in long sleep/IO). Only register signal handlers in the main
# thread — non-main threads can't install them. Wrapped in try/except
# so unexpected platform issues can't block trader startup.
try:
    import atexit as _atexit
    import signal as _signal
    import threading as _threading
    if _threading.current_thread() is _threading.main_thread():
        _signal.signal(_signal.SIGTERM, _sigterm_handler)
        _signal.signal(_signal.SIGINT, _sigterm_handler)
    _atexit.register(_release_held_lock_on_shutdown)
except Exception as _sig_init_err:
    # log may not exist yet at import time in some call paths
    print(f"[WARN] trader signal handler registration failed: {_sig_init_err}", flush=True)


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
        if got_lock:
            # Track the acquired lock so the shutdown handlers can release
            # it even if the main code path is interrupted by SIGTERM.
            global _HELD_ALLOCATION_LOCK
            _HELD_ALLOCATION_LOCK = allocation_id
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
        # Clear the global so atexit doesn't double-release on normal exit.
        global _HELD_ALLOCATION_LOCK
        if _HELD_ALLOCATION_LOCK == allocation_id:
            _HELD_ALLOCATION_LOCK = None
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


def _fetch_session_start_equity_from_snapshots(
    allocation_id: str,
    session_start_hour: int = 6,
) -> float | None:
    """Return total_equity_usd from the snapshot closest to session start.

    Every session opens at `session_start_hour:35` UTC. Equity at
    `session_start_hour:30` UTC (5 min pre-open, last pre-positions tick)
    is the correct session-start baseline for actual-ROI tracking.

    Joins allocations.connection_id → exchange_snapshots, filters to
    today UTC, picks the latest snapshot with `snapshot_at <= hh:30 UTC`.
    Returns None if no qualifying snapshot exists (fresh connection, or
    sync_exchange_snapshots cron was down).

    Used as a fallback in the monitoring loop when runtime_state lacks
    session_start_equity_usdt (trader crashed before first bar write).
    """
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT es.total_equity_usd
                FROM user_mgmt.exchange_snapshots es
                JOIN user_mgmt.allocations a
                  ON a.connection_id = es.connection_id
                WHERE a.allocation_id = %s::uuid
                  AND es.snapshot_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
                  AND es.snapshot_at <= date_trunc('day', NOW() AT TIME ZONE 'UTC')
                                        + make_interval(hours => %s, mins => 30)
                  AND es.fetch_ok = TRUE
                  AND es.total_equity_usd IS NOT NULL
                ORDER BY es.snapshot_at DESC
                LIMIT 1
                """,
                (allocation_id, session_start_hour),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    val = row[0] if not isinstance(row, dict) else row.get("total_equity_usd")
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _fetch_strategy_version(strategy_version_id: str) -> dict:
    """Load strategy_version row (config JSONB + identity + live vol_boost)."""
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT strategy_version_id, version_label, config, is_active,
                       current_metrics
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
    cols = ["strategy_version_id", "version_label", "config", "is_active",
            "current_metrics"]
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


def _compound_capital_if_enabled(
    allocation_id: str,
    session_close_equity: float,
    log_prefix: str,
) -> None:
    """If allocations.compounding_mode = 'compound', update capital_usd to
    the post-exit wallet equity captured at session close. Called once per
    session from the allocation-mode close path.

    No-op for 'fixed' mode. Emits an INFO log on actual update so the
    compounding audit trail is visible in the session log. The UPDATE
    only touches rows currently in compound mode — if the user flips to
    fixed mid-session, this runs as a no-op at close.
    """
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            # Read mode + current capital in a single round-trip to produce
            # a clean log message without a second SELECT.
            cur.execute(
                """
                SELECT capital_usd, compounding_mode
                FROM user_mgmt.allocations
                WHERE allocation_id = %s::uuid
                """,
                (allocation_id,),
            )
            row = cur.fetchone()
            if not row:
                return
            current_capital = float(row[0] or 0)
            mode = row[1]
            if mode != "compound":
                return

            cur.execute(
                """
                UPDATE user_mgmt.allocations
                SET capital_usd = %s, updated_at = NOW()
                WHERE allocation_id = %s::uuid
                  AND compounding_mode = 'compound'
                """,
                (round(float(session_close_equity), 2), allocation_id),
            )
        conn.commit()
        log.info(
            f"{log_prefix}Compound mode — capital_usd "
            f"${current_capital:,.2f} -> ${float(session_close_equity):,.2f} "
            f"(session close equity)"
        )
    finally:
        conn.close()


_FLAT_EXIT_REASONS = (
    "filtered", "no_entry_conviction", "missed_window",
    "stale_closed", "stale_close_failed", "no_entry", "errored",
    "entry_failed",
)


def _log_allocation_return(
    allocation_id: str, session_date: str,
    net_return_pct: float,                # gross × lev × 100, BEFORE fees
                                          # (fee + funding drag subtracted below)
    exit_reason: str,
    effective_leverage: float,
    capital_deployed_usd: float,
    config: "TraderConfig",
    equity_usd: float | None = None,      # post-session balance (None for pre-
                                          # trade terminal exits; used for the
                                          # performance_daily chart write)
    signal_count: int | None = None,      # count of symbols after Decision-C1 pre-filter
    conviction_roi_x: float | None = None, # roi_x value compared against kill_y at gate
    fill_report: dict | None = None,      # from enter_positions; derives fill_rate + retries
    positions: list[dict] | None = None,  # post-reconcile positions; derives avg slip bps
    alerts_fired: list[str] | None = None, # list of alert strings fired during session (TBD)
) -> None:
    """Write a daily_allocation_returns row for one user allocation.

    Caller passes `final_return_1x * effective_leverage * 100` (gross leveraged
    percent before fees). This helper subtracts fee_drag + funding_drag and
    records both gross and net.

    Upsert semantics so --resume or re-runs don't duplicate. Called for every
    terminal outcome — including filtered / conviction-kill paths that deploy
    no capital — so the return history is dense.

    equity_usd: when provided, ALSO writes a row into performance_daily
    (allocation_id, date, equity_usd, daily_return) for the allocator
    dashboard's performance chart. Callers at pre-trade terminal states
    (filtered / no_entry / errored before any position) pass None since no
    balance fetch was performed; performance_daily gets no row for those days
    (chart shows a gap). Post-trading close paths pass the post-exit balance
    from get_account_balance_usdt(api).
    """
    gross_pct = net_return_pct
    if effective_leverage > 0 and exit_reason not in _FLAT_EXIT_REASONS:
        fee_drag = config.taker_fee_pct * effective_leverage
        funding_drag = config.funding_rate_daily_pct * effective_leverage
        net_return_pct = net_return_pct - (fee_drag + funding_drag) * 100

    # ── Derive execution-quality telemetry from in-memory session state ──
    # None-safe: any missing source → None → NULL in DB. Pre-trade terminal
    # exits pass fill_report=None and positions=None.
    fill_rate: float | None = None
    retries_used: int | None = None
    if fill_report and isinstance(fill_report.get("fills"), dict):
        fr = fill_report["fills"].get("fill_rate_pct")
        fill_rate = float(fr) if fr is not None else None
        # Count symbols that had at least one retry_round > 0 (matches
        # master's filled_via_retry semantic). fill_report.fills.symbols[]
        # is the per-symbol status list from enter_positions.
        syms = fill_report["fills"].get("symbols") or []
        retries_used = sum(
            1 for s in syms if (s.get("retry_rounds") or 0) > 0
        )

    def _mean_slip(positions_list: list[dict] | None, key: str) -> float | None:
        if not positions_list:
            return None
        vals = [
            float(p[key]) for p in positions_list
            if p.get(key) is not None
        ]
        return (sum(vals) / len(vals)) if vals else None

    avg_entry_slip_bps = _mean_slip(positions, "entry_slippage_bps")
    avg_exit_slip_bps  = _mean_slip(positions, "exit_slippage_bps")

    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_mgmt.allocation_returns
                    (allocation_id, session_date, net_return_pct,
                     gross_return_pct, exit_reason, effective_leverage,
                     capital_deployed_usd,
                     fill_rate, avg_entry_slip_bps, avg_exit_slip_bps,
                     retries_used, signal_count, conviction_roi_x, alerts_fired)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (allocation_id, session_date) DO UPDATE SET
                    net_return_pct       = EXCLUDED.net_return_pct,
                    gross_return_pct     = EXCLUDED.gross_return_pct,
                    exit_reason          = EXCLUDED.exit_reason,
                    effective_leverage   = EXCLUDED.effective_leverage,
                    capital_deployed_usd = EXCLUDED.capital_deployed_usd,
                    fill_rate            = COALESCE(EXCLUDED.fill_rate, allocation_returns.fill_rate),
                    avg_entry_slip_bps   = COALESCE(EXCLUDED.avg_entry_slip_bps, allocation_returns.avg_entry_slip_bps),
                    avg_exit_slip_bps    = COALESCE(EXCLUDED.avg_exit_slip_bps, allocation_returns.avg_exit_slip_bps),
                    retries_used         = COALESCE(EXCLUDED.retries_used, allocation_returns.retries_used),
                    signal_count         = COALESCE(EXCLUDED.signal_count, allocation_returns.signal_count),
                    conviction_roi_x     = COALESCE(EXCLUDED.conviction_roi_x, allocation_returns.conviction_roi_x),
                    alerts_fired         = COALESCE(EXCLUDED.alerts_fired, allocation_returns.alerts_fired),
                    logged_at            = NOW()
                """,
                (allocation_id, session_date, net_return_pct, gross_pct,
                 exit_reason, effective_leverage, capital_deployed_usd,
                 fill_rate, avg_entry_slip_bps, avg_exit_slip_bps,
                 retries_used, signal_count, conviction_roi_x, alerts_fired),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"  Could not log allocation_returns for {allocation_id}: {e}")
    finally:
        conn.close()

    # Performance chart row — separate connection + try/except so a
    # performance_daily failure cannot corrupt the allocation_returns write.
    # Only writes when a post-session equity balance was captured; pre-trade
    # terminal exits (filtered / no_entry / etc.) pass equity_usd=None and
    # skip this path, leaving the chart with a gap for non-trading days.
    if equity_usd is None:
        return

    # TODO(session-e+): compute drawdown as rolling peak-to-trough from prior
    # performance_daily.equity_usd rows for this allocation_id, computed at
    # write time. NULL = 'not yet computed', NOT 'drawdown is zero'.
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_mgmt.performance_daily
                    (allocation_id, date, equity_usd, daily_return, drawdown)
                VALUES (%s::uuid, %s, %s, %s, NULL)
                ON CONFLICT (allocation_id, date) DO UPDATE SET
                    equity_usd   = EXCLUDED.equity_usd,
                    daily_return = EXCLUDED.daily_return,
                    updated_at   = NOW()
                """,
                # session_date is a UTC date string (matches utc_today()) —
                # matches system convention, avoids TZ drift for non-UTC operators.
                (allocation_id, session_date, equity_usd, net_return_pct),
            )
        conn.commit()
    except Exception as e:
        log.warning(f"  Could not log performance_daily for {allocation_id}: {e}")
    finally:
        conn.close()


def _log_allocation_execution_symbols(
    allocation_id: str,
    session_date: str,
    positions: list[dict],
    sym_stops_fired: set | list | None,
    session_exit_reason: str,
    fill_report: dict | None = None,
    est_exit_prices: dict | None = None,
) -> None:
    """Write one row per symbol into user_mgmt.allocation_execution_symbols.

    Called after _log_allocation_return in the allocation-mode close. Uses
    the same post-reconcile `positions` list (so entry + exit slippage are
    populated) plus the sym_stops_fired set (so per-symbol exit_reason can
    distinguish sym_stop vs session_close vs port_sl/port_tsl).

    Best-effort — any exception is swallowed with a WARN so this never
    prevents allocation_returns from being written. The table is additive
    telemetry; a missing row degrades the Manager Execution per-symbol
    expand for that session but does not affect trading.
    """
    if not positions:
        return
    sym_stops_set = set(sym_stops_fired or [])
    est_exit_lookup = est_exit_prices or {}

    rows: list[tuple] = []
    for p in positions:
        iid = p.get("inst_id")
        if not iid:
            continue
        # est_exit_price is stored on a separate dict by the caller (master-
        # path uses the same pattern at line ~2497), NOT on the position
        # itself. Pull it from the lookup; fall back to whatever's on the
        # position for forward-compat in case a future code path stashes it.
        est_exit_price = est_exit_lookup.get(iid) or p.get("est_exit_price")
        # Per-symbol exit reason: sym_stop wins; else fall back to the
        # session-level reason (session_close / port_sl / port_tsl).
        exit_reason = "sym_stop" if iid in sym_stops_set else session_exit_reason

        # pnl from fills when both are available.  Side not yet tracked
        # per symbol (strategy is long-only today); set to "long" as a
        # placeholder so the column isn't always NULL.
        side = p.get("side") or "long"
        fill_entry = p.get("fill_entry_price")
        fill_exit  = p.get("fill_exit_price")
        contracts  = p.get("filled_contracts") or p.get("contracts")
        ctval      = p.get("ctval") or p.get("ct_val")

        # PnL fallback: reconcile-add symbols (operator manual fills picked up
        # by the per-bar monitoring loop) often don't get a sell fill back from
        # api.get_recent_fills at session close, so fill_exit_price is NULL on
        # those rows. Fall back to est_exit_price so pnl_pct + pnl_usd still
        # render — the est is the per-bar mark price snapshot at exit time, a
        # close-enough proxy when the actual fill record is missing. Doesn't
        # affect normally-filled symbols (fill_exit is populated for those).
        exit_for_pnl = fill_exit if fill_exit is not None else est_exit_price
        pnl_pct = None
        pnl_usd = None
        try:
            if fill_entry is not None and exit_for_pnl is not None and float(fill_entry) > 0:
                raw = (float(exit_for_pnl) - float(fill_entry)) / float(fill_entry) * 100.0
                # strategy is long-only; negate for shorts in the future.
                pnl_pct = -raw if side == "short" else raw
                if contracts and ctval:
                    pnl_usd = (float(exit_for_pnl) - float(fill_entry)) * float(contracts) * float(ctval)
                    if side == "short":
                        pnl_usd = -pnl_usd
        except (TypeError, ValueError):
            pass

        # retry_rounds: pulled from fill_report when available (symbol-scoped).
        retry_rounds = 0
        if fill_report and isinstance(fill_report.get("fills"), dict):
            for s in (fill_report["fills"].get("symbols") or []):
                if s.get("inst_id") == iid:
                    retry_rounds = int(s.get("retry_rounds") or 0)
                    break

        # est_entry_price column reads `entry_price` as a fallback because
        # position dicts built by enter_positions store the canonical anchor
        # price under the `entry_price` key, never `est_entry_price`.
        # Without the fallback, every symbol writes NULL for est_entry_price
        # and the Manager Execution per-symbol expand renders ENTRY EST as —.
        rows.append((
            allocation_id, session_date, iid,
            side,
            p.get("target_contracts"),
            p.get("filled_contracts") or p.get("contracts"),
            p.get("fill_pct"),
            p.get("est_entry_price") or p.get("entry_price"),
            p.get("fill_entry_price"),
            p.get("entry_slippage_bps"),
            est_exit_price,
            p.get("fill_exit_price"),
            p.get("exit_slippage_bps"),
            pnl_usd,
            pnl_pct,
            exit_reason,
            retry_rounds,
            iid in sym_stops_set,
            ctval,
        ))

    if not rows:
        return
    conn = _trader_db_connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO user_mgmt.allocation_execution_symbols
                    (allocation_id, session_date, inst_id, side,
                     target_contracts, filled_contracts, fill_pct,
                     est_entry_price, fill_entry_price, entry_slippage_bps,
                     est_exit_price, fill_exit_price, exit_slippage_bps,
                     pnl_usd, pnl_pct, exit_reason, retry_rounds, sym_stopped,
                     ctval)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (allocation_id, session_date, inst_id) DO UPDATE SET
                    side                = EXCLUDED.side,
                    target_contracts    = EXCLUDED.target_contracts,
                    filled_contracts    = EXCLUDED.filled_contracts,
                    fill_pct            = EXCLUDED.fill_pct,
                    est_entry_price     = EXCLUDED.est_entry_price,
                    fill_entry_price    = EXCLUDED.fill_entry_price,
                    entry_slippage_bps  = EXCLUDED.entry_slippage_bps,
                    est_exit_price      = EXCLUDED.est_exit_price,
                    fill_exit_price     = EXCLUDED.fill_exit_price,
                    exit_slippage_bps   = EXCLUDED.exit_slippage_bps,
                    pnl_usd             = EXCLUDED.pnl_usd,
                    pnl_pct             = EXCLUDED.pnl_pct,
                    exit_reason         = EXCLUDED.exit_reason,
                    retry_rounds        = EXCLUDED.retry_rounds,
                    ctval               = COALESCE(EXCLUDED.ctval, allocation_execution_symbols.ctval),
                    sym_stopped         = EXCLUDED.sym_stopped,
                    updated_at          = NOW()
                """,
                rows,
            )
        conn.commit()
        log.info(f"  Wrote {len(rows)} allocation_execution_symbols rows for {allocation_id[:8]}/{session_date}")
    except Exception as e:
        log.warning(f"  Could not log allocation_execution_symbols for {allocation_id}: {e}")
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
                    -- Clear preview markers so the row transitions cleanly
                    -- from "preview" → "live session". Existing portfolio_bars
                    -- stay attached via the unchanged portfolio_session_id.
                    status            = 'active',
                    exit_reason       = NULL,
                    exit_time_utc     = NULL,
                    updated_at        = NOW()
                RETURNING portfolio_session_id
                """,
                (allocation_id, signal_date, session_start_utc,
                 list(symbols), list(entered),
                 float(eff_lev), int(lev_int)),
            )
            session_id = cur.fetchone()[0]
        conn.commit()
        _track_active_portfolio_session(str(session_id))
        return str(session_id)
    finally:
        conn.close()


def _run_fresh_session_for_allocation(
    allocation_id: str,
    config: TraderConfig,
    api: ExchangeAdapter,
    connection_id: str,
    vol_boost: float,
    dry_run: bool = False,
    late_entry: bool = False,
    filter_override: str | None = None,
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
    # Late-entry override: replace the strategy's default filter for this run
    # only. The strategy_version config stays untouched on disk.
    active_filter = filter_override or config.active_filter
    if filter_override:
        log.info(
            f"Allocation {allocation_id}: LATE ENTRY filter override — "
            f"using {active_filter!r} instead of {config.active_filter!r}"
        )
    symbols = get_today_symbols(today, active_filter=active_filter, bypass_sit_flat=late_entry)
    if not symbols:
        log.info(
            f"Allocation {allocation_id}: A -- Filtered, no symbols for "
            f"{today} (filter={active_filter!r})"
        )
        _mark_runtime_state(allocation_id, {"phase": "filtered", "positions": []})
        _log_allocation_return(
            allocation_id, today,
            net_return_pct=0.0,
            exit_reason="filtered",
            effective_leverage=0.0, capital_deployed_usd=0.0,
            config=config,
            signal_count=0,
        )
        return

    inst_ids = [s + config.symbol_suffix for s in symbols]

    # ── Phase 1b: pre-filter against exchange's supported-symbol universe ──
    # Decision C1 (Session D): strict pre-filter. Some symbols in the daily
    # signal may not be tradable on the allocation's exchange (notably Binance
    # margin, which lists a smaller subset than BloFin perps). Drop unsupported
    # symbols BEFORE sizing so capital-per-symbol isn't diluted across symbols
    # that would fail at order time.
    pre_filter_count = len(inst_ids)
    supported_set = {i for i in inst_ids if api.supports_symbol(i)}
    dropped = [i for i in inst_ids if i not in supported_set]
    if dropped:
        log.warning(
            f"Allocation {allocation_id}: {len(dropped)}/{pre_filter_count} "
            f"symbols unsupported on {api.exchange_name}, dropped: {dropped}"
        )
    inst_ids = [i for i in inst_ids if i in supported_set]
    if not inst_ids:
        log.info(
            f"Allocation {allocation_id}: A -- Filtered, zero symbols supported "
            f"on {api.exchange_name} after pre-filter (pre-filter list: {pre_filter_count})"
        )
        _mark_runtime_state(allocation_id, {
            "phase": "filtered",
            "positions": [],
            "pre_filter_dropped": dropped,
        })
        _log_allocation_return(
            allocation_id, today,
            net_return_pct=0.0,
            exit_reason="filtered",
            effective_leverage=0.0, capital_deployed_usd=0.0,
            config=config,
            signal_count=0,
        )
        return

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

    if late_entry:
        log.info(
            f"Allocation {allocation_id}: LATE ENTRY — skipping sleep to "
            f"{conviction_dt.strftime('%H:%M')} UTC (now={utcnow().strftime('%H:%M:%S')} UTC)"
        )
    else:
        sleep_until(conviction_dt, f"conviction bar (allocation {allocation_id})")
    now = utcnow()
    past_cutoff = (now > exec_cutoff_dt) and not late_entry

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
            signal_count=len(inst_ids),
        )
        return

    inst_ids = [i for i in inst_ids if i in open_prices and i in bar6_prices]

    # Compute conviction. Order matches master line 1829: (current, ref).
    roi_x = equal_weight_return(bar6_prices, open_prices)
    log.info(
        f"Allocation {allocation_id}: roi_x={roi_x * 100:.4f}%  "
        f"kill_y={config.kill_y * 100:.2f}%"
    )

    # Persist conviction to user_mgmt.daily_signals so the Indexer →
    # Signals view shows it as soon as the gate evaluates — matches the
    # master trader's behavior at trader_blofin.py:1826. Scoped by the
    # allocation's strategy_version_id so multi-tenant allocations on
    # different versions each update their own row. Best-effort: failure
    # here doesn't block trading.
    try:
        _c = _trader_db_connect()
        try:
            with _c.cursor() as _cur:
                _cur.execute("""
                    UPDATE user_mgmt.daily_signals
                    SET conviction_roi_x  = %s,
                        conviction_kill_y = %s,
                        conviction_passed = %s
                    WHERE signal_date = %s
                      AND strategy_version_id = (
                          SELECT strategy_version_id
                          FROM user_mgmt.allocations
                          WHERE allocation_id = %s::uuid
                      )
                """, (
                    round(float(roi_x) * 100, 4),
                    round(float(config.kill_y) * 100, 4),
                    float(roi_x) >= float(config.kill_y),
                    today,
                    allocation_id,
                ))
            _c.commit()
        finally:
            _c.close()
    except Exception as _e:
        log.warning(
            f"Allocation {allocation_id}: could not persist conviction "
            f"to daily_signals: {_e}"
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
            signal_count=len(inst_ids),
            conviction_roi_x=float(roi_x),
        )
        return

    # ── Phase 2b: conviction gate ────────────────────────────────────────
    if late_entry and roi_x < config.kill_y:
        log.warning(
            f"Allocation {allocation_id}: LATE ENTRY — bypassing conviction "
            f"gate (roi_x={roi_x*100:+.4f}% < kill_y={config.kill_y*100:.2f}%). "
            "Operator override; entering anyway."
        )
    elif roi_x < config.kill_y:
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
            signal_count=len(inst_ids),
            conviction_roi_x=float(roi_x),
        )
        return

    log.info(f"Allocation {allocation_id}: conviction passed — entering at {config.l_high}x")

    # ── Late-entry pre-filter: track symbols already past the per-symbol
    # stop threshold as "pre-stopped" (clamp at observed return, no entry
    # order, but stay in the basket for matrix/portfolio observability) ────
    # Without this guard, a late-entry on a basket where one symbol has
    # already drawn down past stop_raw_pct (e.g. MIRA at -6.76% vs -6%
    # threshold) would immediately liquidate that position at session start.
    #
    # Behaviour: keep the symbol in inst_ids so the bar writer + UI matrix
    # render the full original basket. Skip placing entry orders for them
    # (no real position, $0 capital allocation). Seed sym_stopped at clamp
    # value so subsequent bars show them held at -clamp%.
    pre_stopped_clamps: dict[str, float] = {}
    if late_entry:
        try:
            current_marks = get_mark_prices(api, inst_ids)
            stop_thr = float(config.stop_raw_pct)
            for inst in inst_ids:
                ref = bar6_prices.get(inst)
                cur_p = current_marks.get(inst)
                if ref is None or cur_p is None or float(ref) <= 0:
                    continue
                ret = (float(cur_p) - float(ref)) / float(ref)
                if ret <= stop_thr:
                    pre_stopped_clamps[inst] = round(float(ret), 6)
            if pre_stopped_clamps:
                log.warning(
                    f"Allocation {allocation_id}: LATE ENTRY pre-filter — "
                    f"{len(pre_stopped_clamps)} symbol(s) already past "
                    f"stop threshold ({stop_thr*100:.1f}%); keeping in basket "
                    "for observability, skipping entry orders, clamping at observed return:"
                )
                for inst, clamp in pre_stopped_clamps.items():
                    log.warning(f"  {inst}: clamp={clamp*100:+.3f}%")
            # When EVERY basket symbol is past stop, abort cleanly
            if pre_stopped_clamps and len(pre_stopped_clamps) >= len(inst_ids):
                log.error(
                    f"Allocation {allocation_id}: LATE ENTRY — all symbols past "
                    "stop threshold, no entry."
                )
                _mark_runtime_state(allocation_id, {
                    "phase": "late_entry_aborted",
                    "reason": "all_symbols_past_stop",
                    "positions": [],
                })
                _log_allocation_return(
                    allocation_id, today,
                    net_return_pct=0.0,
                    exit_reason="late_entry_no_eligible_symbols",
                    effective_leverage=0.0, capital_deployed_usd=0.0,
                    config=config,
                    signal_count=len(inst_ids),
                    conviction_roi_x=float(roi_x),
                )
                return
        except Exception as e:
            log.warning(
                f"Allocation {allocation_id}: LATE ENTRY pre-filter failed "
                f"({e}); proceeding without exclusion"
            )

    # ── Phase 3: stale-position sweep (this account only) ────────────────
    # Mirrors master lines 1904-1939. Uses per-allocation api so only this
    # user's BloFin account is touched.
    #
    # LATE ENTRY: skip the sweep. On a normal 06:00 spawn, "stale" means
    # leftover from a prior session (real cleanup). On a late-entry spawn
    # initiated by the operator, those positions are NOT stale — they are
    # the operator's intentional manual longs/shorts on the same symbols,
    # placed precisely BECAUSE the operator wanted them. Closing them is
    # destructive (forced exit + double slippage on re-entry). For symbols
    # where the operator already has a position, the trader will skip
    # placing a new entry order (handled in Phase 5 below).
    # Operator-held basket symbols on late-entry: skip the fresh-entry
    # order for them but KEEP them in the basket (inst_ids). Mirrors the
    # pre_stopped_clamps pattern — zero the entry price, enter_positions's
    # `entry_prices.get(i, 0) > 0` filter skips them, restore after. Keeping
    # inst_ids = full basket means runtime_state.symbols + portfolio_sessions
    # .symbols always carry the 06:00 UTC index basket, so the portfolio
    # detail page renders all symbols (per operator's "always show full view"
    # requirement).
    preserved_ids: set[str] = set()
    if late_entry:
        existing = get_actual_positions(api, inst_ids)
        if existing:
            existing_ids = [p.inst_id for p in existing]
            log.warning(
                f"Allocation {allocation_id}: LATE ENTRY — preserving "
                f"{len(existing)} pre-existing position(s) on this account: "
                f"{existing_ids}. Skipping fresh-entry orders for these symbols."
            )
            preserved_ids = {p.inst_id for p in existing}
            # If every basket symbol is already held by operator AND none
            # are pre-stopped, there's nothing for the trader to do. Same
            # abort as before but checks the post-pre-stopped subset.
            entry_targets = [i for i in inst_ids
                             if i not in preserved_ids
                             and i not in pre_stopped_clamps]
            if not entry_targets:
                log.warning(
                    f"Allocation {allocation_id}: LATE ENTRY — all basket "
                    "symbols already held by operator (or pre-stopped); "
                    "nothing to add."
                )
                _mark_runtime_state(allocation_id, {
                    "phase": "late_entry_all_preserved",
                    "preserved_symbols": existing_ids,
                    "symbols": list(inst_ids),
                    "pre_stopped_clamps": pre_stopped_clamps or {},
                    "positions": [],
                })
                _log_allocation_return(
                    allocation_id, today,
                    net_return_pct=0.0,
                    exit_reason="late_entry_all_preserved",
                    effective_leverage=0.0, capital_deployed_usd=0.0,
                    config=config,
                    signal_count=0,
                    conviction_roi_x=float(roi_x),
                )
                return
    else:
        existing = get_actual_positions(api, inst_ids)
        if existing:
            existing_ids = [p.inst_id for p in existing]
            log.warning(
                f"Allocation {allocation_id}: {len(existing)} stale position(s) "
                f"found before entry: {existing_ids}. Closing them first."
            )
            stale = [{"inst_id": p.inst_id, "contracts": int(p.contracts),
                      "marginMode": config.margin_mode,
                      "positionSide": config.position_side}
                     for p in existing]
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
                    signal_count=len(inst_ids),
                    conviction_roi_x=float(roi_x),
                )
                return
            log.info(f"Allocation {allocation_id}: stale positions closed")

    # ── Phase 4: compute leverage (vol_boost from nightly refresh) ───────
    # vol_boost is read once at session entry by the caller and passed in
    # as a fixed-for-session value. Caller defaults to 1.0 when
    # strategy_version.current_metrics.vol_boost is NULL, matching the host
    # trader fallback when blofin_returns_log.csv is missing. NULL state
    # clears on the next 01:30 UTC nightly tick.
    eff_lev = round(float(config.l_high) * vol_boost, 4)
    lev_int = max(1, int(math.ceil(eff_lev)))
    log.info(
        f"Allocation {allocation_id}: l_high={config.l_high} × "
        f"vol_boost={vol_boost:.4f} = eff_lev={eff_lev:.4f}x"
    )

    # ── Phase 5: balance + enter positions (advisory-lock serialized) ────
    # Lock prevents two allocations on the same exchange account from
    # racing the balance read -> over-committing capital. See
    # _account_advisory_lock for lifecycle details.
    entry_prices = bar6_prices
    entry_1x = roi_x
    peak = 0.0

    with _account_advisory_lock(connection_id):
        # Fetch balance once, keep both values:
        #   - available_usdt drives sizing (legacy `account_balance`)
        #   - total_usdt is the pre-entry account equity baseline for
        #     actual-ROI tracking; captured BEFORE orders fire so the
        #     monitoring-loop baseline matches the session start (not a
        #     post-entry approximation).
        _balance_snapshot = api.get_balance()
        account_balance = _balance_snapshot.available_usdt
        total_equity_pre_entry = _balance_snapshot.total_usdt
        if account_balance > 0:
            log.info(
                f"Allocation {allocation_id}: USDT available=${account_balance:,.2f} "
                f"(total equity=${total_equity_pre_entry:,.2f})"
            )
        else:
            log.warning(
                f"Allocation {allocation_id}: USDT available is zero; aborting may be required"
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
        #
        # Excluded from the entry-orders pass (without being dropped from the
        # basket inst_ids): pre-stopped symbols (late-entry pre-filter) AND
        # operator-preserved positions (already held on this account at
        # late-entry). Both are excluded by zeroing their entry_price — the
        # enter_positions tradeable filter `entry_prices.get(i, 0) > 0` skips
        # them, so capital divides cleanly across the remaining symbols. The
        # original entry_prices are restored after for runtime_state +
        # ROI-math persistence (incr clamping needs the real reference price).
        _excluded = set(pre_stopped_clamps) | preserved_ids
        _saved_entry_prices = {}
        for inst in _excluded:
            if inst in entry_prices:
                _saved_entry_prices[inst] = entry_prices[inst]
                entry_prices[inst] = 0.0
        positions, fill_report = enter_positions(
            api, inst_ids, entry_prices, eff_lev, usdt_for_allocation, dry_run,
        )
        for inst, p in _saved_entry_prices.items():
            entry_prices[inst] = p

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
            signal_count=len(inst_ids),
            conviction_roi_x=float(roi_x),
            fill_report=fill_report,
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
    # fill_report persisted so a mid-session subprocess respawn (SIGTERM
    # from docker rebuild, crash, etc.) keeps fill_rate + retries_used
    # intact for the eventual _log_allocation_return() write at close.
    # Without this, a respawned trader reaches close with fill_report=None
    # and those columns come up NULL in allocation_returns.
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
        "fill_report": fill_report,
        # Late-entry pre-stopped clamp values; the monitoring loop seeds
        # sym_stopped from this so every bar's symbol_returns + stopped[]
        # include the pre-stopped symbols. Empty {} on normal spawns.
        "pre_stopped_clamps": pre_stopped_clamps,
    })

    # ── Phase 8: monitoring loop — parameterized shared function ─────────
    # Master's _run_monitoring_loop accepts allocation-mode kwargs (config,
    # session_id, allocation_id, capital_deployed_usd) and branches at the
    # write sites. Per-bar check order + portfolio-return math are shared.
    # balance_pre_entry is the total account equity captured BEFORE any
    # orders were placed (line ~3258, inside the advisory lock). Using the
    # pre-entry value ensures the actual-ROI baseline matches session
    # start — a post-entry fetch would include order fees/slippage and
    # understate session PnL.
    _run_monitoring_loop(
        today, today_date, api, inst_ids,
        open_prices, entry_prices, entry_1x, eff_lev,
        peak, positions, dry_run,
        balance_pre_entry=float(total_equity_pre_entry) if total_equity_pre_entry else None,
        allocation_id=allocation_id,
        config=config,
        session_id=session_id,
        capital_deployed_usd=capital_deployed_usd,
        # Execution-quality telemetry forwarded into _log_allocation_return
        # at the post-close write site. None-safe for pre-trade exits (not
        # reached here — those terminal paths return before this call).
        fill_report=fill_report,
        conviction_roi_x=float(roi_x),
        signal_count=len(inst_ids),
        pre_stopped_clamps=pre_stopped_clamps,
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
        global _HELD_PORTFOLIO_SESSION_ID
        if _HELD_PORTFOLIO_SESSION_ID == str(session_id):
            _HELD_PORTFOLIO_SESSION_ID = None
    except Exception as e:
        log.warning(f"  Could not close portfolio session {session_id}: {e}")



def run_session_for_allocation(
    allocation_id: str,
    dry_run: bool = False,
    late_entry: bool = False,
    filter_override: str | None = None,
) -> None:
    """Execute one session for one user allocation.

    Replaces module-level credentials, filesystem state, and hardcoded strategy
    constants with per-allocation DB-backed equivalents. Otherwise follows the
    same phase sequence as master's run_session().

    late_entry=True is operator-initiated: skips conviction sleep + past-cutoff
    check + conviction gate. filter_override (with late_entry) replaces the
    strategy_version's active_filter for this single run.
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

        if creds.exchange not in ("blofin", "binance"):
            log.warning(
                f"Allocation {allocation_id} uses {creds.exchange!r} — only "
                "blofin + binance are supported in this release. Skipping."
            )
            _mark_runtime_state(
                allocation_id,
                {"phase": "skipped",
                 "reason": f"exchange {creds.exchange} not supported"},
            )
            return

        # 5. Dispatch to the right adapter. adapter_for reads creds.exchange
        # and returns BloFinAdapter or BinanceMarginAdapter. Each allocation
        # gets its own adapter instance scoped to its own credentials — no
        # cross-allocation state leakage. Legacy run_session path uses
        # build_api() instead (module-level master credentials).
        api = adapter_for(creds)

        # 6. Resume vs fresh branch on runtime_state.
        state = alloc.get("runtime_state") or {}
        today = utc_today()

        if state.get("date") == today and state.get("phase") == "active":
            log.info(
                f"Allocation {allocation_id}: resuming active session from "
                "runtime_state."
            )
            # Clear any stale crash markers on the portfolio_sessions row.
            # The previous trader instance set status='closed' +
            # exit_reason='subprocess_died' when SIGTERM hit; this resume
            # is reopening the session so the row should reflect that.
            # Without this, the manager UI keeps showing "subprocess died
            # exit" even while bars are being written.
            _resume_session_id = state.get("session_id")
            if _resume_session_id:
                try:
                    _conn = _trader_db_connect()
                    try:
                        with _conn.cursor() as _cur:
                            _cur.execute(
                                """
                                UPDATE user_mgmt.portfolio_sessions
                                   SET status = 'active',
                                       exit_reason = NULL,
                                       exit_time_utc = NULL,
                                       updated_at = NOW()
                                 WHERE portfolio_session_id = %s::uuid
                                   AND exit_reason IN ('subprocess_died',
                                                       'stale_close_failed',
                                                       'errored')
                                """,
                                (_resume_session_id,),
                            )
                        _conn.commit()
                    finally:
                        _conn.close()
                except Exception as _e:
                    log.warning(
                        f"Allocation {allocation_id}: failed to clear stale "
                        f"portfolio_session crash marker: {_e}"
                    )
            # Rehydrate the positional args the shared loop expects.
            _open_prices  = {k: float(v) for k, v in state["open_prices"].items()}
            _entry_prices = {k: float(v) for k, v in state["entry_prices"].items()}
            _inst_ids     = list(state.get("symbols", _open_prices.keys()))
            _today_date   = utcnow().date()
            _resume_baseline = state.get("session_start_equity_usdt")
            # Persisted sym_stopped is a list of inst_ids that hit the
            # per-symbol stop earlier in the session; rehydrate so incr
            # math keeps including their clamped contribution and the
            # active=X/Y display stays truthful across restarts.
            # sym_stopped persists math clamps (or legacy observed-as-math).
            # sym_observed (post-display-split) persists display clamps.
            # Both fall back to {} on pre-fix sessions; the monitoring loop
            # handles missing/empty observed dict gracefully.
            _resume_stopped = state.get("sym_stopped") or {}
            if not isinstance(_resume_stopped, (list, dict)):
                _resume_stopped = {}
            _resume_observed = state.get("sym_observed") or {}
            if not isinstance(_resume_observed, dict):
                _resume_observed = {}
            # Rehydrate late-entry pre-stopped clamps so resumed bars also
            # include the pre-stopped symbols in symbol_returns + stopped[].
            # Pre-fix sessions won't have this key; {} keeps the bar writer
            # behaviour identical to before for normal-spawn sessions.
            _resume_pre_stopped = state.get("pre_stopped_clamps") or {}
            if not isinstance(_resume_pre_stopped, dict):
                _resume_pre_stopped = {}
            # Self-heal: re-derive pre-stopped state from basket vs positions
            # vs current prices. Defends against runtime_state corruption
            # (operator hand-craft, partial write, etc.) so any basket symbol
            # missing from positions whose live return is past stop_raw_pct
            # gets reinjected as pre-stopped at its observed return.
            #
            # Fires only when pre_stopped_clamps + sym_stopped together miss
            # an orphan that should clearly be marked stopped — never
            # downgrades existing entries (resume values always win).
            try:
                _stop_thr = float(config.stop_raw_pct)
                _position_iids = {p.get("inst_id") for p in state.get("positions", []) or []}
                _known_stopped = (
                    set(_resume_pre_stopped.keys())
                    | (set(_resume_stopped.keys()) if isinstance(_resume_stopped, dict)
                       else set(_resume_stopped or []))
                )
                _orphans = [iid for iid in _inst_ids
                            if iid not in _position_iids
                            and iid not in _known_stopped]
                if _orphans:
                    _live = get_mark_prices(api, _orphans)
                    _healed: dict[str, float] = {}
                    for iid in _orphans:
                        ref = _open_prices.get(iid)
                        cur_p = _live.get(iid)
                        if not ref or not cur_p or float(ref) <= 0:
                            continue
                        _ret = (float(cur_p) - float(ref)) / float(ref)
                        if _ret <= _stop_thr:
                            _healed[iid] = round(float(_ret), 6)
                    if _healed:
                        log.warning(
                            f"Allocation {allocation_id}: RESUME self-heal — "
                            f"{len(_healed)} basket symbol(s) had no position "
                            f"and current return past stop threshold "
                            f"({_stop_thr*100:.1f}%); reinjecting as pre-stopped:"
                        )
                        for iid, clamp in _healed.items():
                            log.warning(f"  {iid}: clamp={clamp*100:+.3f}%")
                        _resume_pre_stopped = {**_resume_pre_stopped, **_healed}
            except Exception as _e:
                # Self-heal is best-effort — never block resume on failure.
                log.warning(
                    f"Allocation {allocation_id}: RESUME self-heal failed "
                    f"({_e}); proceeding with persisted clamps as-is"
                )
            _track_active_portfolio_session(state.get("session_id"))
            _run_monitoring_loop(
                today, _today_date, api, _inst_ids,
                _open_prices, _entry_prices,
                float(state.get("entry_1x", 0.0)),
                float(state.get("effective_leverage", config.l_high)),
                float(state.get("peak", 0.0)),
                state["positions"], dry_run,
                balance_pre_entry=(float(_resume_baseline)
                                   if _resume_baseline is not None else None),
                allocation_id=allocation_id,
                config=config,
                session_id=state.get("session_id"),
                capital_deployed_usd=float(state.get("capital_deployed_usd", 0.0)),
                # Pass dict (post-fix) or list (legacy) directly; the loop
                # handles both shapes via isinstance check.
                resume_sym_stopped=_resume_stopped,
                resume_sym_observed={k: float(v) for k, v in _resume_observed.items()},
                # Rehydrate fill_report from runtime_state so fill_rate +
                # retries_used survive a subprocess respawn. Pre-fix
                # sessions won't have this key; .get() returns None which
                # matches the old behavior (NULL in DB).
                fill_report=state.get("fill_report"),
                pre_stopped_clamps={k: float(v) for k, v in _resume_pre_stopped.items()},
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
        # vol_boost read once here, fixed for session (decision 6.4).
        # NULL → 1.0: matches host's "returns log missing → boost=1.0"
        # fallback; clears on next 01:30 UTC nightly refresh tick.
        cm = strategy_version.get("current_metrics") or {}
        vol_boost = float(cm.get("vol_boost") or 1.0)
        _run_fresh_session_for_allocation(
            allocation_id, config, api,
            connection_id=str(alloc["connection_id"]),
            vol_boost=vol_boost,
            dry_run=dry_run,
            late_entry=late_entry,
            filter_override=filter_override,
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
    parser.add_argument("--close-all",    action="store_true",
                        help="Force-close all open positions on BloFin and exit")
    parser.add_argument(
        "--allocation-id", type=str, default=None,
        help="UUID of user_mgmt.allocations row to execute. When set, runs in "
             "multi-tenant mode: decrypts per-user credentials, uses per-"
             "allocation runtime_state, writes with allocation_id. When unset, "
             "runs master account mode (legacy, unchanged).",
    )
    parser.add_argument(
        "--late-entry", action="store_true",
        help="Operator-initiated late entry. Skips the conviction sleep, "
             "skips the past-cutoff check, and bypasses the conviction gate "
             "(roi_x < kill_y). Conviction values are still computed and "
             "logged for the audit trail. Use when the original session was "
             "filter-flat or the trader missed the spawn window.",
    )
    parser.add_argument(
        "--filter-override", type=str, default=None,
        help="Override the strategy_version's active_filter for this run. "
             "E.g. --filter-override 'Tail Guardrail' when the strategy's "
             "default filter sat the day out. Only takes effect with "
             "--late-entry; otherwise the strategy_version's filter wins.",
    )
    args = parser.parse_args()

    if args.status:
        print(json.dumps(load_state(), indent=2, default=str))
        sys.exit(0)

    if args.close_all:
        _require_blofin_credentials()
        api = build_api()
        log.info("--close-all: fetching all open positions ...")
        actual = get_actual_positions(api, [])   # empty list = fetch all, filter none
        if not actual:
            log.info("No open positions found.")
            sys.exit(0)
        actual_ids = [p.inst_id for p in actual]
        log.info(f"Found {len(actual)} open position(s): {actual_ids}")
        positions = [{"inst_id": p.inst_id, "contracts": int(p.contracts),
                      "marginMode": MARGIN_MODE, "positionSide": POSITION_SIDE}
                     for p in actual]
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
            run_session_for_allocation(
                args.allocation_id,
                dry_run=args.dry_run,
                late_entry=args.late_entry,
                filter_override=args.filter_override,
            )
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

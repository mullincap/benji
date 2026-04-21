"""
backend/app/cli/monitor_blofin_positions.py
============================================
Lean stop-loss / trailing-stop monitor for BloFin allocation
f87fe130-a90c-4e60-908a-14f4065b415c.

BACKGROUND
----------
On 2026-04-21 the BloFin per-allocation trader crashed at 06:35 UTC with
KeyError: 'BLOFIN_API_KEY' (fixed in commit 9850fec). Positions were
opened manually on the exchange UI, so runtime_state is NULL and the
trader's normal resume path can't attach. This script provides
SL/TSL-only coverage for the session until normal telemetry resumes
tomorrow.

WHAT THIS DOES
--------------
- Every invocation: fetch current BloFin positions + mark prices,
  compute equal-weighted per-symbol return (mark/avgPx - 1), update
  running peak, compare vs port_sl (-7.5%) and port_tsl (-9.5% from peak).
- If breach: close all positions via /trade/close-position.
- Logs to the allocation's session log file for UI visibility.
- Peak persisted to /mnt/quant-data/logs/trader/monitor_peak_<alloc>.json
  so the trailing stop survives across cron invocations.

WHAT THIS DOES NOT DO
---------------------
- Does NOT re-enter or modify positions.
- Does NOT touch runtime_state, allocation_returns, or portfolio_bars.
- Does NOT do per-symbol stops (simplification — portfolio-level coverage
  only for the session).
- Does NOT reconstruct the full trader session telemetry.

Run via:
  docker compose exec -T backend python -m app.cli.monitor_blofin_positions

Host cron schedule: */5 * * * * (every 5 min).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

import requests

from app.db import get_conn
from app.services.trading.credential_loader import load_credentials

ALLOCATION_ID = "f87fe130-a90c-4e60-908a-14f4065b415c"
PORT_SL = -0.075   # -7.5% portfolio return (1x, from entry) → HARD stop
PORT_TSL = -0.095  # -9.5% from peak (1x) → TRAIL stop
BLOFIN_BASE = "https://openapi.blofin.com"

_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
LOG_PATH = Path(f"/mnt/quant-data/logs/trader/allocation_{ALLOCATION_ID}_{_today}.log")
PEAK_STATE_PATH = Path(f"/mnt/quant-data/logs/trader/monitor_peak_{ALLOCATION_ID}.json")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(str(LOG_PATH), mode="a"),
        logging.StreamHandler(),
    ],
    force=True,
)
log = logging.getLogger("monitor")


def _sign(method: str, path: str, api_secret: str, body_str: str = "") -> dict:
    ts = str(int(time.time() * 1000))
    nonce = str(uuid4())
    body_in_sig = body_str if method.upper() != "GET" else ""
    prehash = f"{path}{method.upper()}{ts}{nonce}{body_in_sig}"
    sig_hex = hmac.new(
        api_secret.encode(), prehash.encode(), hashlib.sha256
    ).hexdigest().encode()
    return {
        "ACCESS-SIGN": base64.b64encode(sig_hex).decode(),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-NONCE": nonce,
    }


def _request(creds, method: str, path: str, params=None, body=None) -> dict:
    qs = "?" + urlencode(params) if params else ""
    full = path + qs
    body_str = json.dumps(body, separators=(",", ":")) if body else ""
    headers = _sign(method, full, creds.api_secret, body_str)
    headers["ACCESS-KEY"] = creds.api_key
    headers["ACCESS-PASSPHRASE"] = creds.passphrase
    headers["Content-Type"] = "application/json"
    url = BLOFIN_BASE + full
    if method.upper() == "GET":
        r = requests.get(url, headers=headers, timeout=15)
    else:
        r = requests.post(url, headers=headers, data=body_str, timeout=15)
    return r.json()


def _load_peak() -> float:
    if not PEAK_STATE_PATH.exists():
        return 0.0
    try:
        return float(json.loads(PEAK_STATE_PATH.read_text()).get("peak", 0.0))
    except Exception:
        return 0.0


def _save_peak(peak: float) -> None:
    PEAK_STATE_PATH.write_text(json.dumps({
        "peak": peak,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }))


def _close_all(creds, positions: list) -> None:
    for p in positions:
        iid = p.get("instId", "?")
        try:
            resp = _request(creds, "POST", "/api/v1/trade/close-position", body={
                "instId": iid,
                "marginMode": "isolated",
                "positionSide": "net",
            })
            code = str(resp.get("code", ""))
            msg = resp.get("msg", "") or ""
            if code == "0":
                log.warning(f"  CLOSED: {iid}")
            else:
                log.error(f"  CLOSE FAILED: {iid}  code={code}  msg={msg}")
        except Exception as e:
            log.error(f"  CLOSE EXCEPTION: {iid}  {e}")


def _load_creds():
    with get_conn() as cx, cx.cursor() as cur:
        cur.execute(
            "SELECT connection_id FROM user_mgmt.allocations WHERE allocation_id = %s",
            (ALLOCATION_ID,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Allocation {ALLOCATION_ID} not found")
    return load_credentials(str(row[0]))


def main() -> int:
    creds = _load_creds()

    pos_resp = _request(creds, "GET", "/api/v1/account/positions")
    if str(pos_resp.get("code", "")) not in ("0", ""):
        log.error(f"Positions fetch failed: {pos_resp.get('code')} {pos_resp.get('msg')}")
        return 1

    positions = pos_resp.get("data") or []
    if not positions:
        log.info("Monitor: no open positions. Exiting.")
        return 0

    sym_rows = []
    for p in positions:
        iid = p.get("instId", "?")
        avg = float(p.get("averagePrice") or p.get("avgPx") or 0)
        mark = float(p.get("markPrice") or 0)
        if avg > 0 and mark > 0:
            sym_ret = mark / avg - 1.0
            sym_rows.append((iid, avg, mark, sym_ret))

    if not sym_rows:
        log.warning("Monitor: no valid positions after filter. Skipping.")
        return 0

    incr = sum(r[3] for r in sym_rows) / len(sym_rows)
    prev_peak = _load_peak()
    peak = max(prev_peak, incr)
    tsl_dist = incr - peak

    log.info(
        f"Monitor | n={len(sym_rows)} positions  "
        f"incr={incr*100:+.3f}%  peak={peak*100:+.3f}%  "
        f"tsl_dist={tsl_dist*100:+.3f}%  "
        f"[port_sl={PORT_SL*100:.1f}%  port_tsl={PORT_TSL*100:.1f}%]"
    )
    for iid, avg, mark, r in sym_rows:
        log.info(f"  {iid:<16} avg={avg:<12.6g} mark={mark:<12.6g} ret={r*100:+.3f}%")

    if incr <= PORT_SL:
        log.warning(
            f"HARD STOP: portfolio incr={incr*100:.3f}% <= port_sl={PORT_SL*100:.1f}% "
            "-- closing all positions"
        )
        _close_all(creds, positions)
        _save_peak(peak)
        return 0

    if tsl_dist <= PORT_TSL:
        log.warning(
            f"TRAIL STOP: tsl_dist={tsl_dist*100:.3f}% <= port_tsl={PORT_TSL*100:.1f}% "
            "-- closing all positions"
        )
        _close_all(creds, positions)
        _save_peak(peak)
        return 0

    _save_peak(peak)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log.exception(f"Monitor failed: {e}")
        sys.exit(1)

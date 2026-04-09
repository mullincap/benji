#!/usr/bin/env python3
"""
pipeline/allocator/blofin_logger.py
====================================
Snapshots BloFin account balance and open positions into
user_mgmt.exchange_snapshots every 5 minutes (cron).

Usage:
  python blofin_logger.py               # log all active BloFin connections
  python blofin_logger.py --dry-run     # fetch + print, no DB write
  python blofin_logger.py --connection-id <uuid>  # target one connection
"""

import os
import sys
import json
import time
import argparse
import requests
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode

# ── Project root on sys.path so pipeline imports resolve ─────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.db.connection import get_conn
from pipeline.allocator.blofin_auth import get_headers

# ── Self-load secrets.env ────────────────────────────────────────────────────
_SECRETS = Path("/mnt/quant-data/credentials/secrets.env")
if _SECRETS.exists():
    for _line in _SECRETS.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

# ── Constants ────────────────────────────────────────────────────────────────
BLOFIN_BASE_URL = os.environ.get("BLOFIN_BASE_URL", "https://openapi.blofin.com")

INSERT_SQL = """\
INSERT INTO user_mgmt.exchange_snapshots
    (connection_id, total_equity_usd, available_usd, used_margin_usd,
     unrealized_pnl, positions, fetch_ok, error_msg)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


# ── BloFin REST helpers ─────────────────────────────────────────────────────

def _request(method: str, path: str, params: dict = None) -> dict:
    method = method.upper()
    qs = ("?" + urlencode(params, doseq=True)) if params else ""
    full_path = path + qs
    headers = get_headers(method, full_path)
    url = BLOFIN_BASE_URL + full_path
    resp = requests.request(method, url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_balance() -> dict:
    return _request("GET", "/api/v1/account/balance")


def fetch_positions() -> dict:
    return _request("GET", "/api/v1/account/positions")


# ── Parsing helpers ──────────────────────────────────────────────────────────

def _safe_decimal(val) -> Decimal | None:
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def parse_balance(resp: dict) -> dict:
    """Extract balance fields using the same field names as trader-blofin.py."""
    data = resp.get("data") or {}
    total_equity = None
    available = None
    used_margin = None
    unrealized_pnl = None

    if isinstance(data, dict):
        total_equity = _safe_decimal(
            data.get("totalEquity") or data.get("totalEq")
        )
        details = data.get("details") or []
        for item in details:
            ccy = (item.get("currency") or item.get("ccy") or "").upper()
            if ccy == "USDT":
                available = _safe_decimal(
                    item.get("availableEquity")
                    or item.get("available")
                    or item.get("availBal")
                )
                break
        # frozenBal as used_margin proxy
        if isinstance(data, dict):
            used_margin = _safe_decimal(data.get("frozenBal"))
        unrealized_pnl = _safe_decimal(data.get("upl"))
    elif isinstance(data, list):
        for item in data:
            ccy = (item.get("currency") or item.get("ccy") or "").upper()
            if ccy == "USDT":
                total_equity = _safe_decimal(
                    item.get("totalEquity") or item.get("totalEq")
                )
                available = _safe_decimal(
                    item.get("availableEquity")
                    or item.get("available")
                    or item.get("availBal")
                )
                break

    return {
        "total_equity": total_equity,
        "available": available,
        "used_margin": used_margin,
        "unrealized_pnl": unrealized_pnl,
    }


def parse_positions(resp: dict) -> list[dict]:
    """Parse open positions into the JSONB array format."""
    out = []
    for pos in (resp.get("data") or []):
        size = float(pos.get("positions", 0) or pos.get("pos", 0) or 0)
        if size == 0:
            continue
        out.append({
            "symbol": (pos.get("instId") or "").replace("-", ""),
            "side": (pos.get("posSide") or "net").lower(),
            "size": size,
            "entry_price": float(pos.get("avgPx") or pos.get("avgPrice") or 0),
            "mark_price": float(pos.get("markPx") or pos.get("markPrice") or 0),
            "unrealized_pnl": float(pos.get("upl") or pos.get("unrealizedPnl") or 0),
            "leverage": int(float(pos.get("lever") or pos.get("leverage") or 0)),
            "margin_mode": (pos.get("marginMode") or "cross").lower(),
        })
    return out


# ── Main logic ───────────────────────────────────────────────────────────────

def get_active_connections(conn, connection_id: str = None) -> list[dict]:
    sql = """
        SELECT connection_id, label
        FROM user_mgmt.exchange_connections
        WHERE exchange = 'blofin' AND status = 'active'
    """
    params = []
    if connection_id:
        sql += " AND connection_id = %s"
        params.append(connection_id)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [{"connection_id": r[0], "label": r[1]} for r in rows]


def snapshot_connection(conn, cxn: dict, dry_run: bool = False):
    cid = cxn["connection_id"]
    label = cxn["label"] or ""

    try:
        bal_resp = fetch_balance()
        time.sleep(0.5)
        pos_resp = fetch_positions()

        bal = parse_balance(bal_resp)
        positions = parse_positions(pos_resp)
        upnl_from_positions = sum(p["unrealized_pnl"] for p in positions)
        unrealized_pnl = bal["unrealized_pnl"] if bal["unrealized_pnl"] is not None else _safe_decimal(upnl_from_positions)

        if dry_run:
            symbols = ", ".join(p["symbol"] for p in positions) or "flat"
            print(f"Connection: {cid} \u00b7 {label}")
            print(f"Balance:    equity=${bal['total_equity']} available=${bal['available']} margin=${bal['used_margin']} upnl={unrealized_pnl}")
            print(f"Positions:  {len(positions)} open \u2014 {symbols}")
            print(f"Dry run: would insert 1 row for connection {cid}")
            return

        with conn.cursor() as cur:
            cur.execute(INSERT_SQL, (
                str(cid),
                bal["total_equity"],
                bal["available"],
                bal["used_margin"],
                unrealized_pnl,
                json.dumps(positions),
                True,
                None,
            ))
        conn.commit()
        print(f"[OK] {cid} \u2014 equity=${bal['total_equity']}  positions={len(positions)}")

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        print(f"[ERR] {cid} \u2014 {error_msg}")
        if not dry_run:
            try:
                with conn.cursor() as cur:
                    cur.execute(INSERT_SQL, (
                        str(cid),
                        None, None, None, None,
                        None,
                        False,
                        error_msg,
                    ))
                conn.commit()
            except Exception as db_err:
                print(f"[FATAL] Could not write error row for {cid}: {db_err}")


def main():
    parser = argparse.ArgumentParser(description="BloFin exchange balance/position logger")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print, do not write to DB")
    parser.add_argument("--connection-id", type=str, default=None, help="Target a specific connection UUID")
    args = parser.parse_args()

    conn = get_conn()
    try:
        connections = get_active_connections(conn, args.connection_id)
        if not connections:
            print("WARNING: no active BloFin connection found in exchange_connections \u2014 nothing to log")
            sys.exit(1)

        for cxn in connections:
            snapshot_connection(conn, cxn, dry_run=args.dry_run)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

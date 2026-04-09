"""
backend/app/api/routes/allocator.py
====================================
Allocator endpoints — live exchange data from the BloFin snapshot logger.

Endpoints:
  GET  /api/allocator/snapshots         — most recent snapshot per active connection
  POST /api/allocator/snapshots/refresh — fetch live from all exchanges, write fresh snapshots
"""

from __future__ import annotations

import os
import json
import time
import hmac
import base64
import hashlib
import logging
from decimal import Decimal
from typing import Any
from uuid import uuid4
from pathlib import Path
import requests as http_requests
from fastapi import APIRouter, Depends

from ...db import get_cursor
from .admin import require_admin

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/allocator",
    tags=["allocator"],
    dependencies=[Depends(require_admin)],
)

# ── Self-load secrets for BloFin API keys ────────────────────────────────────
_SECRETS = Path("/mnt/quant-data/credentials/secrets.env")
if _SECRETS.exists():
    for _line in _SECRETS.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

BLOFIN_BASE_URL = os.environ.get("BLOFIN_BASE_URL", "https://openapi.blofin.com")


# ── BloFin API helpers ───────────────────────────────────────────────────────

def _blofin_headers(method: str, path: str, body: str = "") -> dict:
    """Build BloFin auth headers using the same HMAC-SHA256 pattern as blofin_auth.py."""
    api_key = os.environ.get("BLOFIN_API_KEY", "")
    api_secret = os.environ.get("BLOFIN_API_SECRET", "")
    passphrase = os.environ.get("BLOFIN_PASSPHRASE", "")

    ts = str(int(time.time() * 1000))
    nonce = str(uuid4())
    body_str = body if method.upper() != "GET" else ""
    prehash = f"{path}{method.upper()}{ts}{nonce}{body_str}"

    hex_sig = hmac.new(
        api_secret.encode(), prehash.encode(), hashlib.sha256
    ).hexdigest().encode()
    signature = base64.b64encode(hex_sig).decode()

    return {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-NONCE": nonce,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }


def _blofin_get(path: str) -> dict:
    headers = _blofin_headers("GET", path)
    resp = http_requests.get(BLOFIN_BASE_URL + path, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _safe_decimal(val) -> Decimal | None:
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def _fetch_live_blofin() -> dict:
    """Fetch live balance + positions from BloFin API. Returns parsed dict."""
    bal_resp = _blofin_get("/api/v1/account/balance")
    time.sleep(0.5)
    pos_resp = _blofin_get("/api/v1/account/positions")

    # Parse balance
    data = bal_resp.get("data") or {}
    total_equity = None
    available = None
    unrealized_pnl = None

    if isinstance(data, dict):
        total_equity = _safe_decimal(data.get("totalEquity") or data.get("totalEq"))
        unrealized_pnl = _safe_decimal(data.get("upl"))
        details = data.get("details") or []
        for item in details:
            ccy = (item.get("currency") or item.get("ccy") or "").upper()
            if ccy == "USDT":
                available = _safe_decimal(
                    item.get("availableEquity") or item.get("available") or item.get("availBal")
                )
                break

    # Parse positions
    positions = []
    for pos in (pos_resp.get("data") or []):
        size = float(pos.get("positions", 0) or pos.get("pos", 0) or 0)
        if size == 0:
            continue
        positions.append({
            "symbol": (pos.get("instId") or "").replace("-", ""),
            "side": (pos.get("posSide") or "net").lower(),
            "size": size,
            "entry_price": float(pos.get("avgPx") or pos.get("avgPrice") or 0),
            "mark_price": float(pos.get("markPx") or pos.get("markPrice") or 0),
            "unrealized_pnl": float(pos.get("upl") or pos.get("unrealizedPnl") or 0),
            "leverage": int(float(pos.get("lever") or pos.get("leverage") or 0)),
            "margin_mode": (pos.get("marginMode") or "cross").lower(),
        })

    return {
        "total_equity": total_equity,
        "available": available,
        "used_margin": _safe_decimal(data.get("frozenBal")) if isinstance(data, dict) else None,
        "unrealized_pnl": unrealized_pnl,
        "positions": positions,
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _decimal_or_none(val: Any) -> float | None:
    if val is None:
        return None
    return float(val)


INSERT_SQL = """
    INSERT INTO user_mgmt.exchange_snapshots
        (connection_id, total_equity_usd, available_usd, used_margin_usd,
         unrealized_pnl, positions, fetch_ok, error_msg)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def refresh_snapshots(cur) -> None:
    """Fetch live data from all active exchanges and write fresh snapshot rows."""
    cur.execute("""
        SELECT connection_id, exchange
        FROM user_mgmt.exchange_connections
        WHERE status = 'active'
    """)
    connections = cur.fetchall()

    for conn in connections:
        cid = conn["connection_id"]
        exchange = conn["exchange"]

        try:
            if exchange == "blofin":
                data = _fetch_live_blofin()
            else:
                log.warning(f"Unsupported exchange '{exchange}' for live fetch — skipping")
                continue

            cur.execute(INSERT_SQL, (
                str(cid),
                data["total_equity"],
                data["available"],
                data["used_margin"],
                data["unrealized_pnl"],
                json.dumps(data["positions"]),
                True,
                None,
            ))
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            log.error(f"Live fetch failed for {cid}: {error_msg}")
            cur.execute(INSERT_SQL, (
                str(cid),
                None, None, None, None, None,
                False,
                error_msg,
            ))


# ── Routes ───────────────────────────────────────────────────────────────────

@router.get("/snapshots")
def snapshots(cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Return the most recent exchange_snapshots row per active connection,
    joined to exchange_connections for exchange name and label.
    """
    cur.execute("""
        SELECT DISTINCT ON (ec.connection_id)
            ec.connection_id,
            ec.exchange,
            ec.label,
            es.snapshot_at,
            es.total_equity_usd,
            es.available_usd,
            es.used_margin_usd,
            es.unrealized_pnl,
            es.positions,
            es.fetch_ok,
            es.error_msg
        FROM user_mgmt.exchange_connections ec
        LEFT JOIN user_mgmt.exchange_snapshots es
            ON es.connection_id = ec.connection_id
            AND es.fetch_ok = TRUE
        WHERE ec.status = 'active'
        ORDER BY ec.connection_id, es.snapshot_at DESC NULLS LAST
    """)
    rows = cur.fetchall()

    snaps = []
    total_equity = Decimal(0)
    total_upnl = Decimal(0)
    has_any = False

    for r in rows:
        snap = {
            "connection_id": str(r["connection_id"]),
            "exchange": r["exchange"],
            "label": r["label"],
            "snapshot_at": r["snapshot_at"].isoformat() if r["snapshot_at"] else None,
            "total_equity_usd": _decimal_or_none(r["total_equity_usd"]),
            "available_usd": _decimal_or_none(r["available_usd"]),
            "used_margin_usd": _decimal_or_none(r["used_margin_usd"]),
            "unrealized_pnl": _decimal_or_none(r["unrealized_pnl"]),
            "positions": r["positions"] if r["positions"] is not None else [],
            "fetch_ok": r["fetch_ok"] if r["fetch_ok"] is not None else None,
            "error_msg": r["error_msg"],
        }
        snaps.append(snap)
        if r["total_equity_usd"] is not None:
            has_any = True
            total_equity += r["total_equity_usd"]
        if r["unrealized_pnl"] is not None:
            total_upnl += r["unrealized_pnl"]

    return {
        "snapshots": snaps,
        "total_live_equity_usd": float(total_equity) if has_any else None,
        "total_unrealized_pnl": float(total_upnl) if has_any else None,
    }


@router.post("/snapshots/refresh")
def snapshots_refresh(cur=Depends(get_cursor)) -> dict[str, Any]:
    """Fetch live from all active exchanges, write fresh snapshots, return latest."""
    refresh_snapshots(cur)
    # Now read back what we just wrote (plus any other connections)
    return snapshots(cur)

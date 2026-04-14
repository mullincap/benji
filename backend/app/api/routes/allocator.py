"""
backend/app/api/routes/allocator.py
====================================
Allocator endpoints — live exchange data, strategies, allocations, exchanges.

Endpoints:
  GET  /api/allocator/snapshots           — most recent snapshot per active connection
  POST /api/allocator/snapshots/refresh   — fetch live from all exchanges, write fresh snapshots
  GET  /api/allocator/strategies          — published strategies with latest audit metrics
  GET  /api/allocator/exchanges           — active exchange connections (masked keys)
  POST /api/allocator/exchanges/keys      — store new exchange API keys
  DELETE /api/allocator/exchanges/{id}    — remove an exchange connection
  GET  /api/allocator/allocations         — active allocations with strategy + exchange info
  POST /api/allocator/allocations         — create or update an allocation
  DELETE /api/allocator/allocations/{id}  — deactivate an allocation
  GET  /api/allocator/trader/{id}/balance-history — daily equity for an allocation
  GET  /api/allocator/trader/{id}/pnl     — P&L summary for an allocation
  GET  /api/allocator/trader/{id}/positions — open positions from latest snapshot

All endpoints are scoped by user_id via the get_current_user() dependency.
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
import requests as http_requests
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ...db import get_cursor
from ...core.config import load_secrets
from ...services.encryption import encrypt_key, decrypt_key
from .auth import get_current_user

log = logging.getLogger(__name__)

load_secrets()

router = APIRouter(
    prefix="/api/allocator",
    tags=["allocator"],
)

BLOFIN_BASE_URL = os.environ.get("BLOFIN_BASE_URL", "https://openapi.blofin.com")


# ── BloFin API helpers ───────────────────────────────────────────────────────

def _blofin_headers(
    method: str, path: str, body: str = "",
    *, api_key: str, api_secret: str, passphrase: str,
) -> dict:
    """Build BloFin auth headers using HMAC-SHA256."""
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


def _blofin_get(
    path: str, *, api_key: str, api_secret: str, passphrase: str,
) -> dict:
    headers = _blofin_headers(
        "GET", path,
        api_key=api_key, api_secret=api_secret, passphrase=passphrase,
    )
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


def _fetch_live_blofin(
    *, api_key: str, api_secret: str, passphrase: str,
) -> dict:
    """Fetch live balance + positions from BloFin API using the given credentials."""
    creds = dict(api_key=api_key, api_secret=api_secret, passphrase=passphrase)
    bal_resp = _blofin_get("/api/v1/account/balance", **creds)
    time.sleep(0.5)
    pos_resp = _blofin_get("/api/v1/account/positions", **creds)

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


def refresh_snapshots(cur, *, user_id: str | None = None) -> None:
    """
    Fetch live data from active exchanges and write fresh snapshot rows.
    If user_id is provided, only refresh that user's connections.
    """
    if user_id:
        cur.execute("""
            SELECT connection_id, exchange, api_key_enc, api_secret_enc, passphrase_enc
            FROM user_mgmt.exchange_connections
            WHERE status = 'active' AND user_id = %s::uuid
        """, (user_id,))
    else:
        cur.execute("""
            SELECT connection_id, exchange, api_key_enc, api_secret_enc, passphrase_enc
            FROM user_mgmt.exchange_connections
            WHERE status = 'active'
        """)
    connections = cur.fetchall()

    for conn in connections:
        cid = conn["connection_id"]
        exchange = conn["exchange"]

        try:
            if exchange == "blofin":
                # Decrypt per-connection keys
                api_key = decrypt_key(conn["api_key_enc"]) if conn["api_key_enc"] else None
                api_secret = decrypt_key(conn["api_secret_enc"]) if conn["api_secret_enc"] else None
                passphrase = decrypt_key(conn["passphrase_enc"]) if conn["passphrase_enc"] else None

                # Fallback to env keys for legacy/seed connections with null keys
                # TODO: remove after migration — all connections should have encrypted keys
                if not api_key:
                    api_key = os.environ.get("BLOFIN_API_KEY", "")
                if not api_secret:
                    api_secret = os.environ.get("BLOFIN_API_SECRET", "")
                if not passphrase:
                    passphrase = os.environ.get("BLOFIN_PASSPHRASE", "")

                if not api_key or not api_secret:
                    log.warning(f"No API credentials for connection {cid} — skipping")
                    continue

                data = _fetch_live_blofin(
                    api_key=api_key, api_secret=api_secret, passphrase=passphrase,
                )
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
def snapshots(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Return the most recent exchange_snapshots row per active connection
    belonging to the authenticated user.
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
        WHERE ec.status = 'active' AND ec.user_id = %s::uuid
        ORDER BY ec.connection_id, es.snapshot_at DESC NULLS LAST
    """, (user_id,))
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
def snapshots_refresh(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Fetch live from this user's active exchanges, write fresh snapshots, return latest."""
    refresh_snapshots(cur, user_id=user_id)
    return snapshots(user_id=user_id, cur=cur)


# ── Strategies ──────────────────────────────────────────────────────────────

@router.get("/strategies")
def get_strategies(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Return published strategies with their latest audit metrics.
    Joins strategies → strategy_versions → latest audit results.
    """
    cur.execute("""
        SELECT
            s.strategy_id, s.name, s.display_name, s.description,
            s.filter_mode, s.capital_cap_usd,
            sv.strategy_version_id, sv.version_label, sv.is_active
        FROM audit.strategies s
        JOIN audit.strategy_versions sv ON sv.strategy_id = s.strategy_id
        WHERE s.is_published = TRUE AND sv.is_active = TRUE
        ORDER BY s.strategy_id
    """)
    strategies = cur.fetchall()

    version_ids = [s["strategy_version_id"] for s in strategies]

    # Fetch latest audit results per version
    metrics_by_version: dict[str, dict] = {}
    if version_ids:
        cur.execute("""
            SELECT DISTINCT ON (j.strategy_version_id)
                j.strategy_version_id,
                r.sharpe, r.sortino, r.max_dd_pct, r.cagr_pct,
                r.total_return_pct, r.profit_factor,
                r.win_rate_daily, r.active_days,
                r.avg_daily_ret_pct, r.best_month_pct, r.worst_month_pct,
                r.equity_r2,
                r.starting_capital, r.ending_capital,
                r.scorecard_score, r.grade
            FROM audit.results r
            JOIN audit.jobs j ON r.job_id = j.job_id
            WHERE j.strategy_version_id = ANY(%s::uuid[])
              AND j.status = 'complete'
            ORDER BY j.strategy_version_id, j.completed_at DESC NULLS LAST
        """, (version_ids,))
        for row in cur.fetchall():
            metrics_by_version[str(row["strategy_version_id"])] = {
                "sharpe": _decimal_or_none(row["sharpe"]),
                "sortino": _decimal_or_none(row["sortino"]),
                "max_dd_pct": _decimal_or_none(row["max_dd_pct"]),
                "cagr_pct": _decimal_or_none(row["cagr_pct"]),
                "total_return_pct": _decimal_or_none(row["total_return_pct"]),
                "profit_factor": _decimal_or_none(row["profit_factor"]),
                "win_rate_daily": _decimal_or_none(row["win_rate_daily"]),
                "active_days": row["active_days"],
                "avg_daily_ret_pct": _decimal_or_none(row["avg_daily_ret_pct"]),
                "best_month_pct": _decimal_or_none(row["best_month_pct"]),
                "worst_month_pct": _decimal_or_none(row["worst_month_pct"]),
                "scorecard_score": _decimal_or_none(row["scorecard_score"]),
                "grade": row["grade"],
            }

    # Fetch allocation counts and deployed capital per strategy version
    capacity_by_version: dict[str, dict] = {}
    if version_ids:
        cur.execute("""
            SELECT strategy_version_id,
                   COUNT(*) AS allocator_count,
                   COALESCE(SUM(capital_usd), 0) AS deployed_usd
            FROM user_mgmt.allocations
            WHERE status = 'active'
              AND strategy_version_id = ANY(%s::uuid[])
            GROUP BY strategy_version_id
        """, (version_ids,))
        for row in cur.fetchall():
            capacity_by_version[str(row["strategy_version_id"])] = {
                "allocators": row["allocator_count"],
                "deployed_usd": float(row["deployed_usd"]),
            }

    result = []
    for s in strategies:
        vid = str(s["strategy_version_id"])
        metrics = metrics_by_version.get(vid, {})
        cap_info = capacity_by_version.get(vid, {"allocators": 0, "deployed_usd": 0})
        result.append({
            "strategy_id": s["strategy_id"],
            "strategy_version_id": vid,
            "name": s["name"],
            "display_name": s["display_name"] or s["name"],
            "description": s["description"],
            "filter_mode": s["filter_mode"],
            "capital_cap_usd": s["capital_cap_usd"],
            "version_label": s["version_label"],
            "metrics": metrics,
            "capacity": {
                "allocators": cap_info["allocators"],
                "deployed_usd": cap_info["deployed_usd"],
                "capacity_usd": s["capital_cap_usd"] or 1_000_000,
            },
        })

    return {"strategies": result}


# ── Exchanges ───────────────────────────────────────────────────────────────

def _mask_key(enc_value: str | None) -> str:
    """
    Return a masked version of an API key. Never expose raw keys.
    Attempts to decrypt first (Fernet); falls back to masking the raw value
    for legacy plaintext rows.
    """
    if not enc_value or len(enc_value) < 8:
        return "••••••••"
    # Try to decrypt to get the real key for masking
    plaintext = decrypt_key(enc_value)
    if plaintext and len(plaintext) >= 8:
        return plaintext[:4] + "••••••" + plaintext[-4:]
    # Legacy plaintext value — mask directly
    return enc_value[:4] + "••••••" + enc_value[-4:]


@router.get("/exchanges")
def get_exchanges(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return active exchange connections with masked keys for the authenticated user."""
    cur.execute("""
        SELECT connection_id, exchange, label, status,
               api_key_enc, last_validated_at, created_at
        FROM user_mgmt.exchange_connections
        WHERE status = 'active' AND user_id = %s::uuid
        ORDER BY created_at
    """, (user_id,))
    rows = cur.fetchall()

    exchanges = []
    for r in rows:
        exchanges.append({
            "connection_id": str(r["connection_id"]),
            "exchange": r["exchange"],
            "label": r["label"],
            "masked_key": _mask_key(r["api_key_enc"]),
            "status": r["status"],
            "last_validated_at": r["last_validated_at"].isoformat() if r["last_validated_at"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })

    return {"exchanges": exchanges}


class ExchangeKeysRequest(BaseModel):
    exchange: str
    label: str
    api_key: str
    api_secret: str
    passphrase: str | None = None


@router.post("/exchanges/keys")
def store_exchange_keys(body: ExchangeKeysRequest, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Store exchange API keys for a new connection.
    Keys are Fernet-encrypted before INSERT into api_key_enc / api_secret_enc.
    """
    if not body.api_key or not body.api_secret:
        raise HTTPException(status_code=400, detail="api_key and api_secret are required")

    connection_id = str(uuid4())
    cur.execute("""
        INSERT INTO user_mgmt.exchange_connections
            (connection_id, user_id, exchange, label, api_key_enc, api_secret_enc,
             passphrase_enc, status, created_at, updated_at)
        VALUES (%s, %s::uuid, %s, %s, %s, %s, %s, 'active', NOW(), NOW())
        RETURNING connection_id
    """, (
        connection_id,
        user_id,
        body.exchange.lower(),
        body.label,
        encrypt_key(body.api_key),
        encrypt_key(body.api_secret),
        encrypt_key(body.passphrase) if body.passphrase else None,
    ))
    # TODO: Existing rows in exchange_connections may contain plaintext keys
    # from before encryption was added. Do NOT auto-migrate — run a manual
    # one-time script to re-encrypt legacy values after verifying FERNET_KEY.

    log.info("Stored exchange connection %s for %s (keys need encryption)", connection_id, body.exchange)
    # Never return raw keys — only masked
    return {
        "connection_id": connection_id,
        "exchange": body.exchange.lower(),
        "label": body.label,
        "masked_key": _mask_key(body.api_key),
        "status": "active",
    }


@router.delete("/exchanges/{connection_id}")
def remove_exchange(connection_id: str, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Soft-delete an exchange connection. Verifies ownership before delete."""
    # Verify ownership
    cur.execute("""
        SELECT user_id FROM user_mgmt.exchange_connections
        WHERE connection_id = %s::uuid AND status = 'active'
    """, (connection_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Exchange connection not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your exchange connection")

    # Check for active allocations using this connection
    cur.execute("""
        SELECT COUNT(*) AS cnt
        FROM user_mgmt.allocations
        WHERE connection_id = %s::uuid AND status = 'active'
    """, (connection_id,))
    alloc_row = cur.fetchone()
    if alloc_row and alloc_row["cnt"] > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot remove: {alloc_row['cnt']} active allocation(s) use this exchange"
        )

    cur.execute("""
        UPDATE user_mgmt.exchange_connections
        SET status = 'removed', updated_at = NOW()
        WHERE connection_id = %s::uuid AND user_id = %s::uuid AND status = 'active'
    """, (connection_id, user_id))
    return {"removed": True, "connection_id": connection_id}


# ── Allocations ─────────────────────────────────────────────────────────────

@router.get("/allocations")
def get_allocations(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return active allocations for the authenticated user."""
    cur.execute("""
        SELECT
            a.allocation_id, a.strategy_version_id, a.connection_id,
            a.capital_usd, a.status, a.created_at,
            sv.version_label, sv.strategy_id,
            s.display_name AS strategy_display_name, s.name AS strategy_name,
            s.filter_mode,
            ec.exchange, ec.label AS connection_label
        FROM user_mgmt.allocations a
        JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
        JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
        JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
        WHERE a.status = 'active' AND a.user_id = %s::uuid
        ORDER BY a.created_at
    """, (user_id,))
    allocations = cur.fetchall()

    alloc_ids = [a["allocation_id"] for a in allocations]

    # Fetch today's performance
    today_return: dict[str, float] = {}
    today_equity: dict[str, float] = {}
    if alloc_ids:
        cur.execute("""
            SELECT allocation_id, equity_usd, daily_return
            FROM user_mgmt.performance_daily
            WHERE allocation_id = ANY(%s::uuid[])
              AND date = CURRENT_DATE
        """, (alloc_ids,))
        for row in cur.fetchall():
            aid = str(row["allocation_id"])
            today_return[aid] = float(row["daily_return"] or 0)
            today_equity[aid] = float(row["equity_usd"] or 0)

    result = []
    for a in allocations:
        aid = str(a["allocation_id"])
        capital = float(a["capital_usd"] or 0)
        equity = today_equity.get(aid, capital)
        result.append({
            "allocation_id": aid,
            "strategy_version_id": str(a["strategy_version_id"]),
            "connection_id": str(a["connection_id"]),
            "capital_usd": capital,
            "status": a["status"],
            "strategy_name": a["strategy_display_name"] or a["strategy_name"],
            "strategy_slug": a["strategy_name"],
            "filter_mode": a["filter_mode"],
            "exchange": a["exchange"],
            "connection_label": a["connection_label"],
            "equity_usd": equity,
            "daily_return_pct": today_return.get(aid, 0.0),
            "daily_pnl_usd": round(capital * today_return.get(aid, 0) / 100, 2),
        })

    return {"allocations": result}


class AllocationRequest(BaseModel):
    strategy_version_id: str
    connection_id: str
    capital_usd: float


@router.post("/allocations")
def create_allocation(body: AllocationRequest, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Create a new allocation (strategy instance) for the authenticated user."""
    if body.capital_usd <= 0:
        raise HTTPException(status_code=400, detail="capital_usd must be positive")

    allocation_id = str(uuid4())
    cur.execute("""
        INSERT INTO user_mgmt.allocations
            (allocation_id, user_id, strategy_version_id, connection_id,
             capital_usd, status, created_at, updated_at)
        VALUES (%s, %s::uuid, %s::uuid, %s::uuid, %s, 'active', NOW(), NOW())
        RETURNING allocation_id
    """, (allocation_id, user_id, body.strategy_version_id, body.connection_id, body.capital_usd))

    return {"allocation_id": allocation_id, "status": "active"}


class AllocationUpdateRequest(BaseModel):
    capital_usd: float | None = None
    status: str | None = None


@router.patch("/allocations/{allocation_id}")
def update_allocation(allocation_id: str, body: AllocationUpdateRequest, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Update an allocation's capital or status. Verifies ownership."""
    # Ownership check
    cur.execute("""
        SELECT user_id FROM user_mgmt.allocations WHERE allocation_id = %s::uuid
    """, (allocation_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")

    updates = []
    params: list = []

    if body.capital_usd is not None:
        if body.capital_usd <= 0:
            raise HTTPException(status_code=400, detail="capital_usd must be positive")
        updates.append("capital_usd = %s")
        params.append(body.capital_usd)
    if body.status is not None:
        if body.status not in ("active", "paused", "closed"):
            raise HTTPException(status_code=400, detail="Invalid status")
        updates.append("status = %s")
        params.append(body.status)
        if body.status == "closed":
            updates.append("closed_at = NOW()")

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append("updated_at = NOW()")
    params.extend([allocation_id, user_id])

    cur.execute(
        f"UPDATE user_mgmt.allocations SET {', '.join(updates)} WHERE allocation_id = %s::uuid AND user_id = %s::uuid",
        params,
    )
    return {"updated": True, "allocation_id": allocation_id}


@router.delete("/allocations/{allocation_id}")
def delete_allocation(allocation_id: str, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Close an allocation. Verifies ownership before delete."""
    # Ownership check
    cur.execute("""
        SELECT user_id FROM user_mgmt.allocations WHERE allocation_id = %s::uuid
    """, (allocation_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")

    cur.execute("""
        UPDATE user_mgmt.allocations
        SET status = 'closed', closed_at = NOW(), updated_at = NOW()
        WHERE allocation_id = %s::uuid AND user_id = %s::uuid AND status IN ('active', 'paused')
    """, (allocation_id, user_id))
    return {"closed": True, "allocation_id": allocation_id}


# ── Trader data (per-allocation) ───────────────────────────────────────────

@router.get("/trader/{allocation_id}/balance-history")
def trader_balance_history(allocation_id: str, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return daily equity curve for an allocation. Verifies ownership."""
    cur.execute("SELECT user_id FROM user_mgmt.allocations WHERE allocation_id = %s::uuid", (allocation_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")

    cur.execute("""
        SELECT date, equity_usd, daily_return, drawdown
        FROM user_mgmt.performance_daily
        WHERE allocation_id = %s::uuid
        ORDER BY date
    """, (allocation_id,))
    rows = cur.fetchall()

    return {
        "allocation_id": allocation_id,
        "history": [
            {
                "date": r["date"].isoformat(),
                "equity_usd": float(r["equity_usd"] or 0),
                "daily_return": float(r["daily_return"] or 0),
                "drawdown": float(r["drawdown"] or 0),
            }
            for r in rows
        ],
    }


@router.get("/trader/{allocation_id}/pnl")
def trader_pnl(allocation_id: str, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return P&L summary for an allocation. Verifies ownership."""
    cur.execute("""
        SELECT capital_usd, user_id FROM user_mgmt.allocations
        WHERE allocation_id = %s::uuid
    """, (allocation_id,))
    alloc = cur.fetchone()
    if not alloc:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(alloc["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")

    capital = float(alloc["capital_usd"] or 0)

    # Get latest equity and today's return
    cur.execute("""
        SELECT date, equity_usd, daily_return, drawdown
        FROM user_mgmt.performance_daily
        WHERE allocation_id = %s::uuid
        ORDER BY date DESC
        LIMIT 1
    """, (allocation_id,))
    latest = cur.fetchone()

    if latest:
        equity = float(latest["equity_usd"] or 0)
        daily_return = float(latest["daily_return"] or 0)
        drawdown = float(latest["drawdown"] or 0)
    else:
        equity = capital
        daily_return = 0
        drawdown = 0

    all_time_pnl = equity - capital
    daily_pnl_usd = capital * daily_return / 100

    return {
        "allocation_id": allocation_id,
        "capital_usd": capital,
        "equity_usd": equity,
        "all_time_pnl": round(all_time_pnl, 2),
        "daily_return_pct": daily_return,
        "daily_pnl_usd": round(daily_pnl_usd, 2),
        "drawdown": drawdown,
    }


@router.get("/trader/{allocation_id}/positions")
def trader_positions(allocation_id: str, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return open positions from the latest snapshot. Verifies ownership."""
    cur.execute("""
        SELECT a.connection_id, a.user_id, ec.exchange, ec.label
        FROM user_mgmt.allocations a
        JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
        WHERE a.allocation_id = %s::uuid
    """, (allocation_id,))
    alloc = cur.fetchone()
    if not alloc:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(alloc["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")

    # Get latest successful snapshot for this connection
    cur.execute("""
        SELECT positions, snapshot_at
        FROM user_mgmt.exchange_snapshots
        WHERE connection_id = %s::uuid AND fetch_ok = TRUE
        ORDER BY snapshot_at DESC
        LIMIT 1
    """, (str(alloc["connection_id"]),))
    snap = cur.fetchone()

    positions = snap["positions"] if snap and snap["positions"] else []
    snapshot_at = snap["snapshot_at"].isoformat() if snap and snap["snapshot_at"] else None

    return {
        "allocation_id": allocation_id,
        "connection_id": str(alloc["connection_id"]),
        "exchange": alloc["exchange"],
        "snapshot_at": snapshot_at,
        "positions": positions,
    }

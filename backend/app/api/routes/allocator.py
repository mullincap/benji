"""
backend/app/api/routes/allocator.py
====================================
Allocator endpoints — live exchange data from the BloFin snapshot logger.

Endpoints:
  GET /api/allocator/snapshots — most recent snapshot per active connection
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends

from ...db import get_cursor
from .admin import require_admin

router = APIRouter(
    prefix="/api/allocator",
    tags=["allocator"],
    dependencies=[Depends(require_admin)],
)


def _decimal_or_none(val: Any) -> float | None:
    if val is None:
        return None
    return float(val)


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

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
  GET  /api/allocator/account-balance-series — aggregate equity across all user connections
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
import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4
import requests as http_requests
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from ...core.config import settings
from ...db import get_cursor
from ...core.config import load_secrets
from ...services.admin_sessions import validate_session as _validate_admin_session
from ...services.encryption import encrypt_key, decrypt_key
from ...services.exchanges.binance import (
    BinanceClient,
    BinanceError,
)
from ...services.exchanges.permissions import (
    fetch_permissions,
    validate_permissions,
    PermissionAuthError,
    PermissionNetworkError,
    PermissionUnsupportedExchange,
    PermissionProbeError,
)
from .auth import get_current_user
from .admin import require_admin, COOKIE_NAME as _ADMIN_COOKIE_NAME

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


# In-process cache of BloFin contract values. Needed for notional = size ×
# mark × contractValue since BloFin position sizes are in contracts, not
# coins. Instrument list is near-static; a 30-min TTL is plenty.
_BLOFIN_CONTRACT_CACHE: dict[str, float] = {}
_BLOFIN_CONTRACT_CACHE_TS: float = 0.0
_BLOFIN_CONTRACT_TTL_S = 1800


def _blofin_contract_values() -> dict[str, float]:
    """Return {instId → contractValue} for BloFin SWAP instruments.

    Public endpoint (no auth). Cached per-process with a 30-min TTL so
    bursts of /positions calls don't hammer the exchange.
    """
    global _BLOFIN_CONTRACT_CACHE_TS
    if (
        _BLOFIN_CONTRACT_CACHE
        and time.time() - _BLOFIN_CONTRACT_CACHE_TS < _BLOFIN_CONTRACT_TTL_S
    ):
        return _BLOFIN_CONTRACT_CACHE
    try:
        resp = http_requests.get(
            BLOFIN_BASE_URL + "/api/v1/market/instruments",
            params={"instType": "SWAP"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("data") or []
        fresh: dict[str, float] = {}
        for row in data:
            iid = row.get("instId")
            if not iid:
                continue
            try:
                fresh[iid] = float(row.get("contractValue") or 1.0)
            except (TypeError, ValueError):
                fresh[iid] = 1.0
        if fresh:
            _BLOFIN_CONTRACT_CACHE.clear()
            _BLOFIN_CONTRACT_CACHE.update(fresh)
            _BLOFIN_CONTRACT_CACHE_TS = time.time()
    except Exception as e:
        log.warning(f"BloFin contract-value fetch failed: {e}")
    return _BLOFIN_CONTRACT_CACHE


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
    # BloFin's actual response fields (verified against live API):
    #   averagePrice, markPrice, unrealizedPnl, leverage, positionSide,
    #   marginMode, positions (size in contracts).
    # avgPx/markPx/upl/posSide/lever/pos are legacy aliases — kept as
    # fallbacks so any schema drift doesn't silently zero fields.
    contract_values = _blofin_contract_values()
    positions = []
    for pos in (pos_resp.get("data") or []):
        size = float(pos.get("positions", 0) or pos.get("pos", 0) or 0)
        if size == 0:
            continue
        inst_id = pos.get("instId") or ""
        entry_price = float(pos.get("averagePrice") or pos.get("avgPx") or pos.get("avgPrice") or 0)
        mark_price = float(pos.get("markPrice") or pos.get("markPx") or 0)
        contract_value = contract_values.get(inst_id, 1.0)
        notional_usd = size * mark_price * contract_value
        positions.append({
            "symbol": inst_id.replace("-", ""),
            "side": (pos.get("positionSide") or pos.get("posSide") or "net").lower(),
            "size": size,
            "entry_price": entry_price,
            "mark_price": mark_price,
            "contract_value": contract_value,
            "notional_usd": round(notional_usd, 2),
            "unrealized_pnl": float(pos.get("unrealizedPnl") or pos.get("upl") or 0),
            "leverage": int(float(pos.get("leverage") or pos.get("lever") or 0)),
            "margin_mode": (pos.get("marginMode") or "cross").lower(),
        })

    return {
        "total_equity": total_equity,
        "available": available,
        "used_margin": _safe_decimal(data.get("frozenBal")) if isinstance(data, dict) else None,
        "unrealized_pnl": unrealized_pnl,
        "positions": positions,
    }


# ── Binance live fetch ───────────────────────────────────────────────────────

def _get_binance_btc_price() -> float:
    """Unsigned BTC/USDT spot ticker — used by the margin fallback for
    BTC-denominated equity conversion. No caching; called once per sync cycle."""
    resp = http_requests.get(
        "https://api.binance.com/api/v3/ticker/price",
        params={"symbol": "BTCUSDT"},
        timeout=5,
    )
    resp.raise_for_status()
    return float(resp.json()["price"])


def _get_binance_spot_price(symbol: str) -> float | None:
    """Unsigned spot ticker for arbitrary symbol (e.g. 'BTCUSDT').

    Used to enrich cross-margin position rows with live mark_price. Returns
    None on failure — caller falls back to 0.0 with a log warning rather
    than crashing the snapshot cycle.
    """
    try:
        resp = http_requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=5,
        )
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception:
        return None


def _fetch_live_binance(*, api_key: str, api_secret: str) -> dict:
    """Fetch balance + positions from Binance.

    Futures-primary: /fapi/v2/account is the authoritative source (USDT-native,
    matches BloFin's unified-account model). Falls back to cross-margin
    (/sapi/v1/margin/account, BTC-denominated) only if futures is empty.

    Returns the same dict shape as _fetch_live_blofin:
        {total_equity, available, used_margin, unrealized_pnl, positions}
    Raises BinanceError / BinanceNetworkError on hard failure — the caller's
    except block writes a fail row to exchange_snapshots.
    """
    client = BinanceClient(api_key=api_key, api_secret=api_secret)

    # Futures positionRisk is authoritative for entry prices on live positions.
    # We query it regardless of which balance path runs below, so that UTA /
    # portfolio-margin accounts (where /fapi/v2/account.totalMarginBalance
    # reports 0 and we fall through to the margin path) still get the real
    # entryPrice rather than 0.0. Maps symbol -> {entry, mark, leverage,
    # margin_mode}; callers look up by symbol when building the positions list.
    risk_by_symbol: dict[str, dict] = {}
    try:
        risk_rows = client.get_futures_position_risk() or []
        for r in risk_rows:
            try:
                amt = float(r.get("positionAmt", 0) or 0)
            except (TypeError, ValueError):
                amt = 0.0
            if amt == 0:
                continue
            sym = r.get("symbol", "")
            try:
                risk_by_symbol[sym] = {
                    "entry":       float(r.get("entryPrice", 0) or 0),
                    "mark":        float(r.get("markPrice", 0) or 0),
                    "upl":         float(r.get("unRealizedProfit", 0) or 0),
                    "leverage":    int(float(r.get("leverage") or 0)),
                    "margin_mode": (r.get("marginType") or "").lower(),
                    "size":        abs(amt),
                    "side":        "long" if amt > 0 else "short",
                }
            except (TypeError, ValueError):
                continue
    except BinanceError:
        # positionRisk fetch failed — proceed with the balance path and
        # best-effort dust-only positions (no entry prices). Better than
        # raising; the UI can still display equity and flag missing entry.
        pass

    # ── Futures primary ─────────────────────────────────────────────────
    futures = client.get_futures_account()
    futures_equity = _safe_decimal(futures.get("totalMarginBalance")) if futures else None
    futures_has_funds = bool(
        futures and futures_equity is not None and float(futures_equity) > 0
    )

    if futures_has_funds:
        total_equity = _safe_decimal(futures.get("totalMarginBalance"))
        available = _safe_decimal(futures.get("availableBalance"))
        used_margin = _safe_decimal(futures.get("totalInitialMargin"))
        unrealized_pnl = _safe_decimal(futures.get("totalUnrealizedProfit"))

        # /fapi/v2/account.positions has entryPrice but no markPrice.
        # positionRisk has both. Prefer positionRisk when available, fall
        # back to account payload otherwise.
        positions = []
        for p in futures.get("positions") or []:
            try:
                amt = float(p.get("positionAmt", 0) or 0)
            except (TypeError, ValueError):
                amt = 0.0
            if amt == 0:
                continue
            sym = p.get("symbol", "")
            risk = risk_by_symbol.get(sym)
            if risk:
                entry = risk["entry"]
                mark = risk["mark"]
                upl = risk["upl"]
                leverage = risk["leverage"]
                margin_mode = risk["margin_mode"]
            else:
                try:
                    entry = float(p.get("entryPrice", 0) or 0)
                except (TypeError, ValueError):
                    entry = 0.0
                try:
                    upl = float(p.get("unRealizedProfit", 0) or 0)
                except (TypeError, ValueError):
                    upl = 0.0
                mark = 0.0
                leverage = int(float(p.get("leverage") or 0))
                margin_mode = (p.get("marginType") or "").lower()

            # Dust filter: drop positions whose notional is < $1. Binance
            # UTA / cross-margin keeps microscopic leftover balances around
            # indefinitely; they clutter the allocator view without being
            # tradable. Threshold in USD assumes mark_price is USDT-quoted.
            notional = abs(amt) * (mark or entry or 0)
            if notional and notional < 1.0:
                continue

            positions.append({
                "symbol":         sym,
                "side":           "long" if amt > 0 else "short",
                "size":           abs(amt),
                "entry_price":    entry,
                "mark_price":     mark,
                "unrealized_pnl": upl,
                "leverage":       leverage,
                "margin_mode":    margin_mode,
                "notional_usd":   round(notional, 2) if notional else 0.0,
            })

        return {
            "total_equity": total_equity,
            "available": available,
            "used_margin": used_margin,
            "unrealized_pnl": unrealized_pnl,
            "positions": positions,
        }

    # ── Margin fallback ────────────────────────────────────────────────
    margin = client.get_margin_account()
    if margin is None:
        raise BinanceError(
            "No tradable account balance found — check that the key has access "
            "to futures or cross-margin."
        )

    try:
        net_btc = float(margin.get("totalNetAssetOfBtc", 0) or 0)
        asset_btc = float(margin.get("totalAssetOfBtc", 0) or 0)
        liability_btc = float(margin.get("totalLiabilityOfBtc", 0) or 0)
    except (TypeError, ValueError) as e:
        raise BinanceError(f"Binance margin response malformed: {e}") from e

    if asset_btc == 0 and net_btc == 0:
        raise BinanceError(
            "No tradable account balance found — check that the key has access "
            "to futures or cross-margin."
        )

    btc_price = _get_binance_btc_price()

    # Synthesize per-asset positions from userAssets where netAsset != 0.
    # Cross-margin doesn't have a positions endpoint, so "position" = any
    # non-stablecoin asset with a non-zero net (long = owned, short = borrowed).
    # Enrich each with live spot price via /api/v3/ticker/price.
    #
    # When positionRisk has entry data for this symbol (common in UTA where
    # futures positions show up both as userAssets AND on positionRisk), we
    # prefer positionRisk values — entry_price, leverage, real margin_mode,
    # and live UPL. Pure spot holdings (no positionRisk row) keep the
    # entry_price=0 / leverage=0 cross defaults.
    STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "FDUSD"}
    positions = []
    for item in margin.get("userAssets", []) or []:
        asset = item.get("asset")
        if not asset or asset in STABLECOINS:
            continue
        try:
            net = float(item.get("netAsset", 0) or 0)
        except (TypeError, ValueError):
            continue
        if net == 0:
            continue
        symbol = f"{asset}USDT"
        risk = risk_by_symbol.get(symbol)
        if risk:
            entry = risk["entry"]
            mark = risk["mark"]
            upl = risk["upl"]
            leverage = risk["leverage"]
            margin_mode = risk["margin_mode"]
            # Prefer positionRisk's positionAmt when present — userAssets'
            # netAsset is an asset-level balance that can differ from the
            # futures contract count (e.g. if user has spot + futures of
            # the same coin).
            size = risk["size"]
            side = risk["side"]
        else:
            mark = _get_binance_spot_price(symbol)
            if mark is None:
                log.warning(f"Binance margin: no spot price for {symbol}, leaving mark_price=0")
                mark = 0.0
            entry = 0.0
            upl = 0.0
            leverage = 0
            margin_mode = "cross"
            size = abs(net)
            side = "long" if net > 0 else "short"

        # Dust filter: drop positions worth < $1. UTA / cross-margin keeps
        # microscopic leftover asset balances around indefinitely and they
        # clutter the allocator view without being tradable.
        notional = size * (mark or entry or 0)
        if notional and notional < 1.0:
            continue

        positions.append({
            "symbol":         symbol,
            "side":           side,
            "size":           size,
            "entry_price":    entry,
            "mark_price":     mark,
            "unrealized_pnl": upl,
            "leverage":       leverage,
            "margin_mode":    margin_mode,
            "notional_usd":   round(notional, 2) if notional else 0.0,
        })

    return {
        "total_equity": _safe_decimal(net_btc * btc_price),
        "available": _safe_decimal(net_btc * btc_price),  # net-of-liabilities
        "used_margin": _safe_decimal(liability_btc * btc_price),
        "unrealized_pnl": _safe_decimal(0),  # not separately exposed on margin
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


def _enrich_positions_from_runtime_state(
    cur, connection_id: str, positions: list[dict],
) -> list[dict]:
    """Fill in entry_price / leverage / UPL from the trader's runtime_state
    when the exchange read path couldn't supply them (Binance keys without
    /fapi read permission fall into this case — Binance returns -2015 on
    /fapi/v2/account and /fapi/v2/positionRisk, so we only see cross-margin
    userAssets with no entry prices).

    Runtime_state is authoritative for the strategy's own positions because
    the trader persists fill_entry_price straight from the order-confirm
    response. Non-strategy holdings (dust / pre-existing coins not opened
    by any active allocation) keep whatever defaults the fetch path set.
    """
    cur.execute(
        """
        SELECT runtime_state
        FROM user_mgmt.allocations
        WHERE connection_id = %s AND status = 'active'
          AND runtime_state IS NOT NULL
        """,
        (connection_id,),
    )
    entry_by_sym: dict[str, dict] = {}
    for row in cur.fetchall():
        rs = row["runtime_state"] or {}
        for p in rs.get("positions") or []:
            inst_id = p.get("inst_id") or ""
            fill = p.get("fill_entry_price")
            if not inst_id or fill is None:
                continue
            # runtime_state uses "PENGU-USDT"; Binance snapshot uses "PENGUUSDT".
            sym_nodash = inst_id.replace("-", "")
            try:
                entry_by_sym[sym_nodash] = {
                    "entry":       float(fill),
                    "leverage":    int(p.get("lev_int") or 0),
                    "margin_mode": p.get("marginMode") or "cross",
                }
            except (TypeError, ValueError):
                continue

    if not entry_by_sym:
        return positions

    enriched: list[dict] = []
    for pos in positions:
        sym = pos.get("symbol", "")
        rs = entry_by_sym.get(sym)
        if rs and not pos.get("entry_price"):
            pos = dict(pos)
            pos["entry_price"] = rs["entry"]
            if not pos.get("leverage"):
                pos["leverage"] = rs["leverage"]
            if pos.get("margin_mode") in (None, "", "cross"):
                pos["margin_mode"] = rs["margin_mode"]
            # Compute UPL from mark we already have + entry we just merged.
            mark = pos.get("mark_price") or 0
            size = pos.get("size") or 0
            side = pos.get("side") or "long"
            if mark and rs["entry"] and size:
                sign = 1 if side == "long" else -1
                pos["unrealized_pnl"] = (mark - rs["entry"]) * size * sign
        enriched.append(pos)
    return enriched


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
            elif exchange == "binance":
                api_key = decrypt_key(conn["api_key_enc"]) if conn["api_key_enc"] else None
                api_secret = decrypt_key(conn["api_secret_enc"]) if conn["api_secret_enc"] else None
                if not api_key or not api_secret:
                    log.warning(f"No API credentials for connection {cid} — skipping")
                    continue

                data = _fetch_live_binance(
                    api_key=api_key, api_secret=api_secret,
                )
                # Binance cross-margin has no per-symbol entry price, and keys
                # with trade-only perms can't hit /fapi/v2/positionRisk either.
                # The trader's runtime_state persists fill_entry_price from the
                # order confirm — use that as the authoritative source for
                # positions owned by this account's active allocations.
                data["positions"] = _enrich_positions_from_runtime_state(
                    cur, str(cid), data["positions"],
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

def _caller_is_admin(request: Request) -> bool:
    """Best-effort admin check using the admin_session cookie. Mirrors
    require_admin() but never raises — used to gate optional query params."""
    from .admin import INTERNAL_API_TOKEN
    internal_token = request.headers.get("X-Internal-Token")
    if internal_token and INTERNAL_API_TOKEN and internal_token == INTERNAL_API_TOKEN:
        return True
    token = request.cookies.get(_ADMIN_COOKIE_NAME)
    return bool(_validate_admin_session(settings.ADMIN_SESSIONS_FILE, token))


@router.get("/strategies")
def get_strategies(
    request: Request,
    include_retired: bool = False,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Return strategies with their latest audit metrics. By default only
    published + active strategies are returned. Pass `include_retired=true`
    (requires a valid admin session) to include retired strategies.
    """
    if include_retired and not _caller_is_admin(request):
        raise HTTPException(
            status_code=403,
            detail="Admin session required to include retired strategies",
        )

    if include_retired:
        cur.execute("""
            SELECT
                s.strategy_id, s.name, s.display_name, s.description,
                s.filter_mode, s.capital_cap_usd, s.is_published, s.is_canonical,
                sv.strategy_version_id, sv.version_label, sv.is_active
            FROM audit.strategies s
            JOIN audit.strategy_versions sv ON sv.strategy_id = s.strategy_id
            WHERE sv.is_active = TRUE
            ORDER BY s.is_published DESC, s.strategy_id
        """)
    else:
        cur.execute("""
            SELECT
                s.strategy_id, s.name, s.display_name, s.description,
                s.filter_mode, s.capital_cap_usd, s.is_published, s.is_canonical,
                sv.strategy_version_id, sv.version_label, sv.is_active
            FROM audit.strategies s
            JOIN audit.strategy_versions sv ON sv.strategy_id = s.strategy_id
            WHERE s.is_published = TRUE AND sv.is_active = TRUE
            ORDER BY s.strategy_id
        """)
    strategies = cur.fetchall()

    version_ids = [s["strategy_version_id"] for s in strategies]

    # Fetch audit metrics from the denormalized current_metrics JSONB. This
    # is refreshed nightly by app.cli.refresh_strategy_metrics and on every
    # promote via POST /api/simulator/audits/{job_id}/promote.
    # Source of truth for the metric values remains audit.results (append-only);
    # this is a read-cache to avoid the audit.results JOIN on every card render.
    metrics_by_version: dict[str, dict] = {}
    metrics_meta_by_version: dict[str, dict] = {}
    if version_ids:
        cur.execute("""
            SELECT sv.strategy_version_id,
                   (sv.current_metrics->>'sharpe')::numeric            AS sharpe,
                   (sv.current_metrics->>'sortino')::numeric           AS sortino,
                   (sv.current_metrics->>'max_dd_pct')::numeric        AS max_dd_pct,
                   (sv.current_metrics->>'cagr_pct')::numeric          AS cagr_pct,
                   (sv.current_metrics->>'total_return_pct')::numeric  AS total_return_pct,
                   (sv.current_metrics->>'profit_factor')::numeric     AS profit_factor,
                   (sv.current_metrics->>'win_rate_daily')::numeric    AS win_rate_daily,
                   (sv.current_metrics->>'active_days')::integer       AS active_days,
                   (sv.current_metrics->>'avg_daily_ret_pct')::numeric AS avg_daily_ret_pct,
                   (sv.current_metrics->>'best_month_pct')::numeric    AS best_month_pct,
                   (sv.current_metrics->>'worst_month_pct')::numeric   AS worst_month_pct,
                   (sv.current_metrics->>'equity_r2')::numeric         AS equity_r2,
                   (sv.current_metrics->>'starting_capital')::numeric  AS starting_capital,
                   (sv.current_metrics->>'ending_capital')::numeric    AS ending_capital,
                   (sv.current_metrics->>'scorecard_score')::numeric   AS scorecard_score,
                    sv.current_metrics->>'grade'                       AS grade,
                    sv.metrics_updated_at,
                    sv.metrics_data_through
            FROM audit.strategy_versions sv
            WHERE sv.strategy_version_id = ANY(%s::uuid[])
              AND sv.current_metrics IS NOT NULL
        """, (version_ids,))
        for row in cur.fetchall():
            svid = str(row["strategy_version_id"])
            metrics_by_version[svid] = {
                "sharpe":            _decimal_or_none(row["sharpe"]),
                "sortino":           _decimal_or_none(row["sortino"]),
                "max_dd_pct":        _decimal_or_none(row["max_dd_pct"]),
                "cagr_pct":          _decimal_or_none(row["cagr_pct"]),
                "total_return_pct":  _decimal_or_none(row["total_return_pct"]),
                "profit_factor":     _decimal_or_none(row["profit_factor"]),
                "win_rate_daily":    _decimal_or_none(row["win_rate_daily"]),
                "active_days":       row["active_days"],
                "avg_daily_ret_pct": _decimal_or_none(row["avg_daily_ret_pct"]),
                "best_month_pct":    _decimal_or_none(row["best_month_pct"]),
                "worst_month_pct":   _decimal_or_none(row["worst_month_pct"]),
                "scorecard_score":   _decimal_or_none(row["scorecard_score"]),
                "grade":             row["grade"],
            }
            metrics_meta_by_version[svid] = {
                "metrics_updated_at":   row["metrics_updated_at"].isoformat() if row["metrics_updated_at"] else None,
                "metrics_data_through": row["metrics_data_through"].isoformat() if row["metrics_data_through"] else None,
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
        meta = metrics_meta_by_version.get(vid, {"metrics_updated_at": None, "metrics_data_through": None})
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
            "is_published": bool(s["is_published"]),
            "is_canonical": bool(s["is_canonical"]),
            "metrics": metrics,
            "metrics_updated_at":   meta["metrics_updated_at"],
            "metrics_data_through": meta["metrics_data_through"],
            "capacity": {
                "allocators": cap_info["allocators"],
                "deployed_usd": cap_info["deployed_usd"],
                "capacity_usd": s["capital_cap_usd"] or 1_000_000,
            },
        })

    return {"strategies": result}


# ── Strategy publish / unpublish (admin-only) ──────────────────────────────
# Toggles audit.strategies.is_published — the filter the list query above
# applies. Admin-gated because controlling what regular allocator users can
# attach capital to is a governance action.

def _set_strategy_published(
    strategy_id: int, publish: bool, cur,
) -> dict[str, Any]:
    cur.execute(
        """
        UPDATE audit.strategies
        SET is_published = %s
        WHERE strategy_id = %s
        RETURNING strategy_id, name, display_name, is_published
        """,
        (publish, strategy_id),
    )
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {
        "strategy_id":   row["strategy_id"],
        "name":          row["name"],
        "display_name":  row["display_name"],
        "is_published":  row["is_published"],
    }


@router.post(
    "/strategies/{strategy_id}/publish",
    dependencies=[Depends(require_admin)],
)
def publish_strategy(strategy_id: int, cur=Depends(get_cursor)) -> dict[str, Any]:
    return _set_strategy_published(strategy_id, True, cur)


@router.post(
    "/strategies/{strategy_id}/unpublish",
    dependencies=[Depends(require_admin)],
)
def unpublish_strategy(strategy_id: int, cur=Depends(get_cursor)) -> dict[str, Any]:
    return _set_strategy_published(strategy_id, False, cur)


# ── Strategy rename (admin-only) ───────────────────────────────────────────
# Updates audit.strategies.display_name only. The slug (name) stays immutable;
# changing it is a bigger migration (UNIQUE constraint, log/history references).
# Soft duplicate check (case-insensitive): if another strategy already uses the
# requested display_name, returns 409. Admin can force via allow_duplicate=True.

class StrategyRenameRequest(BaseModel):
    display_name: str
    allow_duplicate: bool = False


@router.post(
    "/strategies/{strategy_id}/rename",
    dependencies=[Depends(require_admin)],
)
def rename_strategy(
    strategy_id: int,
    body: StrategyRenameRequest,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    new_name = body.display_name.strip()
    if not new_name:
        raise HTTPException(
            status_code=400,
            detail="display_name cannot be empty after trimming whitespace",
        )
    if len(new_name) > 200:
        raise HTTPException(
            status_code=400,
            detail="display_name exceeds 200 character limit",
        )

    if not body.allow_duplicate:
        cur.execute(
            """
            SELECT strategy_id, name, display_name
            FROM audit.strategies
            WHERE LOWER(display_name) = LOWER(%s) AND strategy_id <> %s
            LIMIT 1
            """,
            (new_name, strategy_id),
        )
        dup = cur.fetchone()
        if dup is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "display_name_conflict",
                    "conflict": {
                        "strategy_id":  dup["strategy_id"],
                        "name":         dup["name"],
                        "display_name": dup["display_name"],
                    },
                },
            )

    cur.execute(
        """
        UPDATE audit.strategies
        SET display_name = %s
        WHERE strategy_id = %s
        RETURNING strategy_id, name, display_name, is_published
        """,
        (new_name, strategy_id),
    )
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return {
        "strategy_id":  row["strategy_id"],
        "name":         row["name"],
        "display_name": row["display_name"],
        "is_published": row["is_published"],
    }


# ── Strategy promote-canonical (admin-only) ────────────────────────────────
# Atomically demotes the currently-canonical strategy and promotes the target.
# The partial unique index `strategies_one_canonical` guarantees at most one
# row can carry is_canonical=TRUE at any time; running demote+promote in a
# single transaction keeps the unique invariant holding throughout.
#
# Governance: spec §5 requires promotion only after a comparison audit shows
# the candidate beats the current canonical. That gate is honor-system /
# client-side today (confirmation modal on the Strategies page). Server-side
# enforcement (stored comparison audit IDs, admin two-key) is deferred —
# this endpoint is trust-the-admin for now.

@router.post(
    "/strategies/{strategy_id}/promote-canonical",
    dependencies=[Depends(require_admin)],
)
def promote_canonical(strategy_id: int, cur=Depends(get_cursor)) -> dict[str, Any]:
    # Target must exist. Published status required — promoting a retired
    # strategy to canonical would silently break the Simulator compare card
    # (the canonical-reference endpoint filters on sv.is_active + the
    # retired strategy's version would usually be inactive too).
    cur.execute(
        """
        SELECT strategy_id, name, display_name, is_published, is_canonical
        FROM audit.strategies
        WHERE strategy_id = %s
        """,
        (strategy_id,),
    )
    target = cur.fetchone()
    if target is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if not target["is_published"]:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "target_not_published",
                "message": "Cannot promote a retired strategy to canonical.",
            },
        )
    if target["is_canonical"]:
        # Idempotent: already canonical, return current state.
        return {
            "strategy_id":  target["strategy_id"],
            "name":         target["name"],
            "display_name": target["display_name"],
            "is_canonical": True,
            "demoted":      None,
        }

    # Demote current canonical (if any) and promote target in one statement
    # so the partial unique index never sees two rows with is_canonical=TRUE.
    # CTE returns the demoted row so we can report it back to the caller.
    cur.execute(
        """
        WITH demoted AS (
            UPDATE audit.strategies
               SET is_canonical = FALSE
             WHERE is_canonical = TRUE
            RETURNING strategy_id, display_name
        ),
        promoted AS (
            UPDATE audit.strategies
               SET is_canonical = TRUE
             WHERE strategy_id = %s
            RETURNING strategy_id, name, display_name
        )
        SELECT
            (SELECT strategy_id  FROM demoted)  AS demoted_strategy_id,
            (SELECT display_name FROM demoted)  AS demoted_display_name,
            (SELECT strategy_id  FROM promoted) AS promoted_strategy_id,
            (SELECT name         FROM promoted) AS promoted_name,
            (SELECT display_name FROM promoted) AS promoted_display_name
        """,
        (strategy_id,),
    )
    row = cur.fetchone()
    return {
        "strategy_id":  row["promoted_strategy_id"],
        "name":         row["promoted_name"],
        "display_name": row["promoted_display_name"],
        "is_canonical": True,
        "demoted": None if row["demoted_strategy_id"] is None else {
            "strategy_id":  row["demoted_strategy_id"],
            "display_name": row["demoted_display_name"],
        },
    }


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


_PUBLIC_PERMISSION_FIELDS = ("read", "spot_trade", "futures_trade", "withdrawals")


def _extract_public_permissions(last_permissions: Any, exchange: str) -> dict | None:
    """
    Convert the raw exchange payload stored in `last_permissions` into the
    UI-safe {read, spot_trade, futures_trade, withdrawals} shape. Returns None
    for grandfathered rows where last_permissions is NULL.
    """
    if not last_permissions:
        return None
    # Re-parse the raw payload rather than cache the cooked form — keeps a
    # single source of truth (the parser) and survives parser upgrades.
    from ...services.exchanges.permissions import _parse_binance, _parse_blofin
    try:
        if exchange == "binance":
            ps = _parse_binance(last_permissions)
        elif exchange == "blofin":
            ps = _parse_blofin(last_permissions)
        else:
            return None
        return ps.to_public_dict()
    except Exception:
        return None


@router.get("/exchanges")
def get_exchanges(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Return all non-revoked exchange connections for the authenticated user,
    enriched with permissions (from last validation) and balance (from latest
    snapshot). Grandfathered rows (last_permissions NULL) return permissions:null
    — the frontend should treat them as unvalidated.
    """
    cur.execute("""
        SELECT
            ec.connection_id, ec.exchange, ec.label, ec.status,
            ec.api_key_enc,
            ec.last_validated_at, ec.last_error_msg, ec.last_permissions,
            ec.created_at,
            snap.total_equity_usd AS balance
        FROM user_mgmt.exchange_connections ec
        LEFT JOIN LATERAL (
            SELECT total_equity_usd
            FROM user_mgmt.exchange_snapshots
            WHERE connection_id = ec.connection_id
            ORDER BY snapshot_at DESC
            LIMIT 1
        ) snap ON TRUE
        WHERE ec.user_id = %s::uuid
          AND ec.status <> 'revoked'
        ORDER BY ec.created_at
    """, (user_id,))
    rows = cur.fetchall()

    exchanges = []
    for r in rows:
        exchanges.append({
            "connection_id":     str(r["connection_id"]),
            "exchange":          r["exchange"],
            "label":             r["label"],
            "status":            r["status"],
            "masked_key":        _mask_key(r["api_key_enc"]),
            "last_validated_at": r["last_validated_at"].isoformat() if r["last_validated_at"] else None,
            "last_error_msg":    r["last_error_msg"],
            "permissions":       _extract_public_permissions(r["last_permissions"], r["exchange"]),
            "balance":           float(r["balance"]) if r["balance"] is not None else 0.0,
            "created_at":        r["created_at"].isoformat() if r["created_at"] else None,
        })

    return {"exchanges": exchanges}


SUPPORTED_EXCHANGES = {"binance", "blofin"}


class ExchangeKeysRequest(BaseModel):
    exchange: str
    label: str | None = None
    api_key: str
    api_secret: str
    passphrase: str | None = None


def _update_connection_status(
    cur, connection_id: str, *,
    status: str,
    error_msg: str | None = None,
    mark_validated: bool = False,
    last_permissions: dict | None = None,
) -> None:
    """Persist a state-machine transition for a connection. Commits immediately
    so the row reflects reality even if a later step raises."""
    cur.execute(
        """
        UPDATE user_mgmt.exchange_connections
        SET status             = %s,
            last_error_msg     = %s,
            last_error_at      = CASE WHEN %s IS NULL THEN NULL ELSE NOW() END,
            last_validated_at  = CASE WHEN %s THEN NOW() ELSE last_validated_at END,
            last_permissions   = COALESCE(%s::jsonb, last_permissions),
            updated_at         = NOW()
        WHERE connection_id = %s::uuid
        """,
        (
            status,
            error_msg,
            error_msg,
            mark_validated,
            json.dumps(last_permissions) if last_permissions is not None else None,
            connection_id,
        ),
    )
    cur.connection.commit()


@router.post("/exchanges/keys")
def store_exchange_keys(
    body: ExchangeKeysRequest,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Store + validate exchange API keys.

    Flow: INSERT with status='pending_validation' → call fetch_permissions →
    enforce read-only policy → UPDATE status accordingly. Each DB write is
    committed immediately so the state machine reflects reality across the
    exchange HTTP call.
    """
    # ── Step 1: validate body ────────────────────────────────────────────
    exchange = (body.exchange or "").lower()
    if exchange not in SUPPORTED_EXCHANGES:
        raise HTTPException(
            status_code=400,
            detail=f"Exchange '{body.exchange}' is not supported. Supported: {sorted(SUPPORTED_EXCHANGES)}",
        )
    if not body.api_key or not body.api_secret:
        raise HTTPException(status_code=400, detail="api_key and api_secret are required")
    if exchange == "blofin" and not body.passphrase:
        raise HTTPException(status_code=400, detail="BloFin requires a passphrase")

    # ── Step 2: encrypt ──────────────────────────────────────────────────
    api_key_enc = encrypt_key(body.api_key)
    api_secret_enc = encrypt_key(body.api_secret)
    passphrase_enc = encrypt_key(body.passphrase) if body.passphrase else None

    # ── Step 3: INSERT pending_validation (separate transaction) ─────────
    connection_id = str(uuid4())
    cur.execute(
        """
        INSERT INTO user_mgmt.exchange_connections
            (connection_id, user_id, exchange, label,
             api_key_enc, api_secret_enc, passphrase_enc,
             status, created_at, updated_at)
        VALUES (%s, %s::uuid, %s, %s, %s, %s, %s,
                'pending_validation', NOW(), NOW())
        """,
        (
            connection_id, user_id, exchange, body.label,
            api_key_enc, api_secret_enc, passphrase_enc,
        ),
    )
    cur.connection.commit()
    log.info("Inserted pending exchange connection %s (%s)", connection_id, exchange)

    # ── Step 4: probe permissions ────────────────────────────────────────
    try:
        perms = fetch_permissions(
            exchange=exchange,
            api_key_enc=api_key_enc,
            api_secret_enc=api_secret_enc,
            passphrase_enc=passphrase_enc,
        )
    except PermissionAuthError as e:
        _update_connection_status(
            cur, connection_id,
            status="invalid",
            error_msg=f"Authentication failed: {e}",
        )
        raise HTTPException(status_code=400, detail=f"Authentication failed: {e}")
    except PermissionUnsupportedExchange as e:
        # Shouldn't happen — already validated above — but defensive.
        _update_connection_status(
            cur, connection_id, status="invalid", error_msg=str(e),
        )
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionNetworkError as e:
        _update_connection_status(
            cur, connection_id, status="errored",
            error_msg=f"Exchange unreachable: {e}",
        )
        raise HTTPException(status_code=503, detail=f"Exchange unreachable: {e}")
    except PermissionProbeError as e:
        _update_connection_status(
            cur, connection_id, status="errored", error_msg=str(e),
        )
        raise HTTPException(status_code=502, detail=f"Exchange returned an error: {e}")

    # ── Step 5: enforce trade-capable + no-withdrawals policy ───────────
    is_valid, reason = validate_permissions(exchange, perms)
    if not is_valid:
        _update_connection_status(
            cur, connection_id,
            status="invalid",
            error_msg=reason,
            last_permissions=perms.raw,
        )
        raise HTTPException(status_code=400, detail=reason)

    # ── Step 6: mark active ──────────────────────────────────────────────
    _update_connection_status(
        cur, connection_id,
        status="active",
        error_msg=None,
        mark_validated=True,
        last_permissions=perms.raw,
    )

    # ── Step 7: immediate snapshot so UI can render balance without a 2nd rt ─
    try:
        refresh_snapshots(cur, user_id=user_id)
        cur.connection.commit()
    except Exception as e:
        # Non-fatal — the connection is valid; balance will fill on next sync.
        log.warning("Initial snapshot fetch failed for %s: %s", connection_id, e)

    return {
        "connection_id": connection_id,
        "exchange": exchange,
        "label": body.label,
        "masked_key": _mask_key(body.api_key),
        "status": "active",
        "permissions": perms.to_public_dict(),
    }


@router.delete("/exchanges/{connection_id}")
def remove_exchange(connection_id: str, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Soft-delete an exchange connection — any non-revoked row is removable.

    Returns 404 both for unknown rows and for rows owned by a different user
    (don't reveal cross-user existence). 409 if the row is still bound to an
    active allocation (only possible when status='active', since allocations
    reference active connections).
    """
    cur.execute(
        """
        SELECT user_id, status FROM user_mgmt.exchange_connections
        WHERE connection_id = %s::uuid AND status <> 'revoked'
        """,
        (connection_id,),
    )
    row = cur.fetchone()
    if not row or str(row["user_id"]) != user_id:
        raise HTTPException(status_code=404, detail="Exchange connection not found")

    # Allocation check — only relevant if the row is currently active.
    if row["status"] == "active":
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM user_mgmt.allocations
            WHERE connection_id = %s::uuid AND status = 'active'
            """,
            (connection_id,),
        )
        alloc_row = cur.fetchone()
        if alloc_row and alloc_row["cnt"] > 0:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot remove: {alloc_row['cnt']} active allocation(s) use this exchange",
            )

    cur.execute(
        """
        UPDATE user_mgmt.exchange_connections
        SET status = 'revoked', updated_at = NOW()
        WHERE connection_id = %s::uuid AND user_id = %s::uuid AND status <> 'revoked'
        """,
        (connection_id, user_id),
    )
    return {"removed": True, "connection_id": connection_id}


# ── Allocations ─────────────────────────────────────────────────────────────

@router.get("/allocations")
def get_allocations(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return active allocations for the authenticated user."""
    cur.execute("""
        SELECT
            a.allocation_id, a.strategy_version_id, a.connection_id,
            a.capital_usd, a.status, a.compounding_mode, a.created_at,
            sv.version_label, sv.strategy_id,
            s.display_name AS strategy_display_name, s.name AS strategy_name,
            s.filter_mode,
            ec.exchange, ec.label AS connection_label
        FROM user_mgmt.allocations a
        JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
        JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
        JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
        WHERE a.status IN ('active', 'paused') AND a.user_id = %s::uuid
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
            "compounding_mode": a["compounding_mode"],
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
    compounding_mode: str | None = None  # 'compound' | 'fixed'
    # Principal anchor — lets the operator pin the "official start date"
    # used by /pnl's total_pnl_usd + total_return_pct computation. Setting
    # to literal NULL (body sends null, not omitted) clears the override
    # and falls back to created_at + capital_usd.
    principal_anchor_at: str | None = None     # ISO 8601 or null
    principal_baseline_usd: float | None = None
    # Sentinel flags so caller can explicitly clear (vs leave unchanged).
    # When true, sets the corresponding column to NULL irrespective of
    # the accompanying value field.
    clear_principal_anchor: bool = False
    clear_principal_baseline: bool = False


@router.patch("/allocations/{allocation_id}")
def update_allocation(allocation_id: str, body: AllocationUpdateRequest, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Update an allocation's capital, status, or compounding mode.
    Verifies ownership. Emits a WARN when capital_usd is edited while a
    session is active — the compounding feature's whole point is to
    eliminate that pattern, so manual edits during live trading are
    tracked as an exception path.
    """
    # Ownership + session-state check in one round-trip.
    cur.execute("""
        SELECT user_id, compounding_mode, runtime_state
        FROM user_mgmt.allocations WHERE allocation_id = %s::uuid
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
        # WARN if editing capital during an active session — baseline drift
        # risk (mitigated by the P&L baseline fix but still worth logging).
        runtime_state = row["runtime_state"] or {}
        if runtime_state.get("phase") == "active":
            log.warning(
                "Manual capital_usd change during active session "
                f"(allocation_id={allocation_id}, mode={row['compounding_mode']}); "
                "capital tracking may be inconsistent until next session close."
            )
        updates.append("capital_usd = %s")
        params.append(body.capital_usd)
    if body.status is not None:
        if body.status not in ("active", "paused", "closed"):
            raise HTTPException(status_code=400, detail="Invalid status")
        updates.append("status = %s")
        params.append(body.status)
        if body.status == "closed":
            updates.append("closed_at = NOW()")
    if body.compounding_mode is not None:
        if body.compounding_mode not in ("compound", "fixed"):
            raise HTTPException(status_code=400, detail="Invalid compounding_mode")
        updates.append("compounding_mode = %s")
        params.append(body.compounding_mode)

    # Principal anchor semantics:
    #   clear_principal_anchor=True    → SET principal_anchor_at = NULL
    #   anchor_at provided             → SET to that timestamp
    #   both omitted                   → no change
    if body.clear_principal_anchor:
        updates.append("principal_anchor_at = NULL")
    elif body.principal_anchor_at is not None:
        updates.append("principal_anchor_at = %s::timestamptz")
        params.append(body.principal_anchor_at)

    if body.clear_principal_baseline:
        updates.append("principal_baseline_usd = NULL")
    elif body.principal_baseline_usd is not None:
        if body.principal_baseline_usd <= 0:
            raise HTTPException(status_code=400, detail="principal_baseline_usd must be positive")
        updates.append("principal_baseline_usd = %s")
        params.append(body.principal_baseline_usd)

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


@router.post("/allocations/{allocation_id}/close-positions")
def close_allocation_positions(
    allocation_id: str,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Flatten all open positions for this allocation on the real exchange.

    Reads runtime_state.positions for the symbols/sizes the trader is tracking,
    dispatches to the right ExchangeAdapter (BloFin or Binance), and calls the
    same close_all_positions() routine the trader's end-of-session path uses.
    Also flips allocation.status to 'paused' so no new session spawns while the
    user still holds the connection.

    Race-safety: the trader subprocess polls every 5 minutes and reconciles
    against the exchange; if it sees positions missing it treats them as
    "already closed" and skips redundant sells (trader_blofin.py:1576-1580).
    So concurrent manual close + trader subprocess is non-destructive.
    """
    cur.execute("""
        SELECT user_id, connection_id::text AS connection_id, runtime_state
        FROM user_mgmt.allocations
        WHERE allocation_id = %s::uuid
    """, (allocation_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")

    runtime_state = row["runtime_state"] or {}
    positions = runtime_state.get("positions") or []
    if not positions:
        # No tracked positions — just flip status to paused for consistency.
        cur.execute("""
            UPDATE user_mgmt.allocations
            SET status = 'paused', updated_at = NOW()
            WHERE allocation_id = %s::uuid AND user_id = %s::uuid
              AND status IN ('active', 'paused')
        """, (allocation_id, user_id))
        return {
            "closed": True,
            "allocation_id": allocation_id,
            "attempted": 0,
            "closed_ok": 0,
            "failed": [],
            "note": "No tracked positions — status set to paused.",
        }

    # Lazy import the close helper + credential loader + adapter factory
    # so routes that never hit this path don't pay the import cost.
    from app.services.trading.credential_loader import (
        load_credentials, CredentialDecryptError,
    )
    from app.services.exchanges.adapter import adapter_for
    from app.cli.trader_blofin import close_all_positions

    try:
        creds = load_credentials(row["connection_id"])
    except CredentialDecryptError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        api = adapter_for(creds)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # close_all_positions() handles reconciliation (skips positions already
    # gone from the exchange), primary close via close_position(), and
    # reduce-only fallback. Returns list of positions that failed to close.
    try:
        failed = close_all_positions(
            api, positions, reason=f"manual close via UI (user {user_id})", dry_run=False,
        )
    except Exception as e:
        log.exception(f"close_all_positions raised for allocation {allocation_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Close routine raised: {type(e).__name__}: {e}",
        )

    attempted = len(positions)
    closed_ok = attempted - len(failed)

    # On full success, mark state as exited and pause the allocation. On
    # partial failure, leave runtime_state alone so the trader's next poll
    # can reconcile — but still pause to block future spawns.
    new_runtime_state = dict(runtime_state)
    if not failed:
        new_runtime_state["positions"] = []
        new_runtime_state["phase"] = "exited_manual"
        new_runtime_state["updated_at"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    cur.execute(
        """
        UPDATE user_mgmt.allocations
        SET status = 'paused',
            runtime_state = %s::jsonb,
            updated_at = NOW()
        WHERE allocation_id = %s::uuid AND user_id = %s::uuid
        """,
        (json.dumps(new_runtime_state), allocation_id, user_id),
    )

    return {
        "closed": not failed,
        "allocation_id": allocation_id,
        "attempted": attempted,
        "closed_ok": closed_ok,
        "failed": [p.get("inst_id") for p in failed],
    }


# ── Trader data (per-allocation) ───────────────────────────────────────────

@router.get("/trader/{allocation_id}/balance-history")
def trader_balance_history(
    allocation_id: str,
    range: str | None = None,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Return account-equity curve for an allocation. Verifies ownership.

    Sourced from `user_mgmt.exchange_snapshots` (every-5-min account
    snapshot from `sync_exchange_snapshots` cron) so the curve is visible
    live, not just after session close at 23:55 UTC. For 1D: intraday
    5-min resolution; for 1W/1M/ALL: last snapshot per UTC day.

    Optional `range` query param filters server-side. Allowed values:
      - "1D"  → intraday, last 24h (every 5 min)
      - "1W"  → daily close, last 7 days
      - "1M"  → daily close, last 30 days
      - "ALL" / None / unknown → daily close, full history

    NOTE: exchange_snapshots captures TOTAL account equity keyed by
    connection_id, not allocation_id. If one connection backs multiple
    allocations (hypothetical — today each allocation has its own key),
    the curve here reflects the aggregate account, not an allocation
    slice. This matches the "Total Account Equity" card semantic.
    """
    cur.execute(
        """
        SELECT a.user_id, a.connection_id
        FROM user_mgmt.allocations a
        WHERE a.allocation_id = %s::uuid
        """,
        (allocation_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")
    connection_id = row["connection_id"]

    # Bucket size tuned per-range so every window yields roughly the same
    # number of points (~200-300), keeping chart rendering cost bounded
    # and label density readable. Snapshots are taken every 5 min; the
    # DISTINCT ON (bucket) query picks the latest snapshot within each
    # bucket, giving "bucket-close" equity.
    #
    #   1D  → 5-min buckets × 24h   = 288 points  (raw resolution)
    #   1W  → 30-min buckets × 7d   = 336 points
    #   1M  → 3-hour buckets × 30d  = 240 points
    #   ALL → 1-day buckets         = N days of history
    RANGE_SPECS = {
        "1D":  {"bucket_seconds": 300,    "lookback_interval": "24 hours"},
        "1W":  {"bucket_seconds": 1800,   "lookback_interval": "7 days"},
        "1M":  {"bucket_seconds": 10800,  "lookback_interval": "30 days"},
        "ALL": {"bucket_seconds": 86400,  "lookback_interval": None},
    }
    range_up = (range or "").upper()
    spec = RANGE_SPECS.get(range_up, RANGE_SPECS["ALL"])
    bucket_s = spec["bucket_seconds"]
    lookback = spec["lookback_interval"]

    # floor(epoch / bucket_s) * bucket_s gives the bucket-start timestamp;
    # parameterized via %(bucket)s (int). Lookback clamps the range, None
    # returns full history.
    if lookback:
        query = """
            SELECT DISTINCT ON (bucket)
                   bucket AS ts,
                   equity_usd
            FROM (
              SELECT snapshot_at,
                     to_timestamp(
                       floor(extract(epoch from snapshot_at) / %(bucket)s) * %(bucket)s
                     ) AS bucket,
                     total_equity_usd AS equity_usd
              FROM user_mgmt.exchange_snapshots
              WHERE connection_id = %(cid)s::uuid
                AND snapshot_at >= NOW() - %(lookback)s::interval
                AND fetch_ok = TRUE
                AND total_equity_usd IS NOT NULL
            ) t
            ORDER BY bucket, snapshot_at DESC
        """
        params = {"cid": connection_id, "bucket": bucket_s, "lookback": lookback}
    else:
        query = """
            SELECT DISTINCT ON (bucket)
                   bucket AS ts,
                   equity_usd
            FROM (
              SELECT snapshot_at,
                     to_timestamp(
                       floor(extract(epoch from snapshot_at) / %(bucket)s) * %(bucket)s
                     ) AS bucket,
                     total_equity_usd AS equity_usd
              FROM user_mgmt.exchange_snapshots
              WHERE connection_id = %(cid)s::uuid
                AND fetch_ok = TRUE
                AND total_equity_usd IS NOT NULL
            ) t
            ORDER BY bucket, snapshot_at DESC
        """
        params = {"cid": connection_id, "bucket": bucket_s}

    cur.execute(query, params)
    rows = cur.fetchall()

    return {
        "allocation_id": allocation_id,
        "range": range_up or "ALL",
        "bucket_seconds": bucket_s,
        "history": [
            {
                "date": r["ts"].isoformat(),
                "equity_usd": float(r["equity_usd"] or 0),
                "daily_return": 0.0,
                "drawdown": None,
            }
            for r in rows
        ],
    }


@router.get("/account-balance-series")
def account_balance_series(
    range: str | None = None,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Aggregate equity curve across ALL of the user's exchange connections.

    Sums total_equity_usd across Binance + BloFin (and any future venues)
    per time bucket. Used on the allocator overview to show total account
    balance — distinct from per-allocation equity on the trader detail page.

    Bucketing pattern mirrors trader_balance_history so the frontend can
    reuse PerformanceChart without adapter code. Per-bucket equity =
    sum(DISTINCT ON (connection_id, bucket) latest snapshot) — this
    matches the intraday-equity pattern from the manager.py fix and avoids
    the naive-AVG-across-snapshots bug.
    """
    RANGE_SPECS = {
        "1D":  {"bucket_seconds": 300,    "lookback_interval": "24 hours"},
        "1W":  {"bucket_seconds": 1800,   "lookback_interval": "7 days"},
        "1M":  {"bucket_seconds": 10800,  "lookback_interval": "30 days"},
        "ALL": {"bucket_seconds": 86400,  "lookback_interval": None},
    }
    range_up = (range or "").upper()
    spec = RANGE_SPECS.get(range_up, RANGE_SPECS["ALL"])
    bucket_s = spec["bucket_seconds"]
    lookback = spec["lookback_interval"]

    # Latest snapshot per (connection, bucket), then sum across connections
    # per bucket. Only include snapshots from connections owned by the user.
    base_select = """
        SELECT s.connection_id,
               s.snapshot_at,
               to_timestamp(
                 floor(extract(epoch from s.snapshot_at) / %(bucket)s) * %(bucket)s
               ) AS bucket,
               s.total_equity_usd AS equity
        FROM user_mgmt.exchange_snapshots s
        JOIN user_mgmt.exchange_connections ec
          ON ec.connection_id = s.connection_id
        WHERE ec.user_id = %(uid)s::uuid
          AND s.fetch_ok = TRUE
          AND s.total_equity_usd IS NOT NULL
    """
    if lookback:
        base_select += " AND s.snapshot_at >= NOW() - %(lookback)s::interval"

    query = f"""
        WITH bucketed AS ({base_select}),
        latest_per_conn AS (
          SELECT DISTINCT ON (connection_id, bucket)
                 connection_id, bucket, equity
          FROM bucketed
          ORDER BY connection_id, bucket, snapshot_at DESC
        )
        SELECT bucket AS ts,
               SUM(equity) AS equity_usd,
               COUNT(*) AS connections_in_bucket
        FROM latest_per_conn
        GROUP BY bucket
        ORDER BY bucket
    """
    params: dict[str, Any] = {"uid": user_id, "bucket": bucket_s}
    if lookback:
        params["lookback"] = lookback

    cur.execute(query, params)
    rows = cur.fetchall()

    # Count distinct connections that contributed at least one bucket
    cur.execute(
        """
        SELECT COUNT(DISTINCT ec.connection_id) AS n
        FROM user_mgmt.exchange_connections ec
        WHERE ec.user_id = %s::uuid
          AND EXISTS (
            SELECT 1 FROM user_mgmt.exchange_snapshots s
            WHERE s.connection_id = ec.connection_id
              AND s.fetch_ok = TRUE
              AND s.total_equity_usd IS NOT NULL
          )
        """,
        (user_id,),
    )
    count_row = cur.fetchone()
    conn_count = int(count_row["n"]) if count_row and count_row["n"] is not None else 0

    return {
        "range": range_up or "ALL",
        "bucket_seconds": bucket_s,
        "connections_included": int(conn_count or 0),
        "history": [
            {
                "date": r["ts"].isoformat(),
                "equity_usd": float(r["equity_usd"] or 0),
                "daily_return": 0.0,
                "drawdown": None,
            }
            for r in rows
        ],
    }


@router.get("/trader/{allocation_id}/pnl")
def trader_pnl(allocation_id: str, user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return live P&L summary for an allocation. Verifies ownership.

    Baselines are decoupled from allocation.capital_usd so mid-session or
    mid-day allocation-size edits don't shift a computed P&L value — the
    user's allocator configuration is not a trading outcome.

    Sources:
      - equity_usd        : latest row in user_mgmt.exchange_snapshots
                            for this allocation's connection.
      - session_pnl_usd   : equity_usd - session_start_equity.
                            Only populated when runtime_state.phase='active'
                            AND runtime_state.date == today UTC. Between
                            sessions the field returns null (frontend
                            renders em-dash) — "session" is defined by the
                            trader's daily loop, not by allocation edits.
      - session_start     : runtime_state.session_start_equity_usdt
                            (written by the trader's monitoring loop at
                            session open); fallback chain picks the
                            latest pre-06:30 UTC snapshot only when
                            runtime_state hasn't captured it yet.
      - total_pnl_usd     : equity_usd - initial_equity_usd.
                            initial_equity_usd = earliest exchange_snapshot
                            for this connection at or after the allocation's
                            created_at (immutable once the first snapshot
                            lands). Falls back to capital_usd only for
                            brand-new allocations with no snapshot history.

    Trade-off: total_pnl baselines on WALLET equity, which may include
    other allocations or idle balance on the same connection. Still
    immutable under allocation.capital_usd edits (the target of this fix)
    and good enough until the Session E+ capital_events table ships — see
    docs/open_work_list.md.
    """
    cur.execute("""
        SELECT capital_usd, user_id, connection_id, runtime_state,
               created_at,
               principal_anchor_at, principal_baseline_usd
        FROM user_mgmt.allocations
        WHERE allocation_id = %s::uuid
    """, (allocation_id,))
    alloc = cur.fetchone()
    if not alloc:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(alloc["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")

    capital = float(alloc["capital_usd"] or 0)
    connection_id = alloc["connection_id"]
    runtime_state = alloc["runtime_state"] or {}
    created_at = alloc["created_at"]

    # Principal anchor (migration 007). Defaults back to created_at +
    # capital_usd when the operator hasn't set an explicit anchor/baseline.
    principal_anchor_at = alloc["principal_anchor_at"] or created_at
    principal_baseline = (
        float(alloc["principal_baseline_usd"])
        if alloc["principal_baseline_usd"] is not None
        else capital
    )

    # Current equity: latest exchange_snapshot for this connection.
    cur.execute("""
        SELECT total_equity_usd
        FROM user_mgmt.exchange_snapshots
        WHERE connection_id = %s::uuid
          AND fetch_ok = TRUE
          AND total_equity_usd IS NOT NULL
        ORDER BY snapshot_at DESC
        LIMIT 1
    """, (connection_id,))
    latest_snap = cur.fetchone()
    equity = float(latest_snap["total_equity_usd"]) if latest_snap else capital

    # ── Session P&L ─────────────────────────────────────────────────────────
    # Only meaningful during an active session. Between sessions (post-close
    # at 23:55 UTC through pre-open at 06:35 UTC next day), there's no
    # session to measure and the previous session's P&L has already been
    # persisted via allocation_returns. Returning null here prevents the
    # chart/card from displaying a stale or misleading number.
    today_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    session_phase = runtime_state.get("phase")
    session_date = runtime_state.get("date")
    session_active = session_phase == "active" and session_date == today_utc

    session_start: float | None = None
    if session_active:
        raw = runtime_state.get("session_start_equity_usdt")
        try:
            session_start = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            session_start = None

        if session_start is None:
            # Rescue path: if the trader crashed before first-bar write but
            # session is nominally active, use the latest pre-06:30 snapshot.
            cur.execute("""
                SELECT total_equity_usd
                FROM user_mgmt.exchange_snapshots
                WHERE connection_id = %s::uuid
                  AND snapshot_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
                  AND snapshot_at <= date_trunc('day', NOW() AT TIME ZONE 'UTC')
                                     + INTERVAL '6 hours 30 minutes'
                  AND fetch_ok = TRUE
                  AND total_equity_usd IS NOT NULL
                ORDER BY snapshot_at DESC
                LIMIT 1
            """, (connection_id,))
            baseline_row = cur.fetchone()
            if baseline_row:
                session_start = float(baseline_row["total_equity_usd"])

    session_pnl_usd = None
    session_return_pct = None
    if session_active and session_start is not None and session_start > 0:
        session_pnl_usd = equity - session_start
        session_return_pct = session_pnl_usd / session_start * 100

    # ── Total P&L ───────────────────────────────────────────────────────────
    # Baseline = earliest exchange_snapshot for this connection at or after
    # the allocation's created_at. Immutable across allocation.capital_usd
    # edits (the bug this fix targets). Falls back to capital_usd only for
    # brand-new allocations that haven't captured any snapshot yet.
    initial_equity = None
    if created_at is not None:
        cur.execute("""
            SELECT total_equity_usd
            FROM user_mgmt.exchange_snapshots
            WHERE connection_id = %s::uuid
              AND snapshot_at >= %s
              AND fetch_ok = TRUE
              AND total_equity_usd IS NOT NULL
            ORDER BY snapshot_at ASC
            LIMIT 1
        """, (connection_id, created_at))
        initial_row = cur.fetchone()
        if initial_row:
            initial_equity = float(initial_row["total_equity_usd"])

    # ── Principal computation (migration 007) ───────────────────────────────
    # principal_now = principal_baseline + SUM(deposits − withdrawals since
    #                 principal_anchor_at AND NOT soft-deleted)
    # Defaults (both anchor + baseline NULL): anchor = allocation.created_at,
    # baseline = allocation.capital_usd. This reproduces the pre-migration-007
    # behavior so upgrades don't silently change anyone's displayed numbers.
    cur.execute("""
        SELECT COALESCE(SUM(CASE kind
                              WHEN 'deposit'    THEN amount_usd
                              WHEN 'withdrawal' THEN -amount_usd
                            END), 0)::float AS net_since_anchor
        FROM user_mgmt.allocation_capital_events
        WHERE allocation_id = %s::uuid
          AND deleted_at IS NULL
          AND event_at >= %s::timestamptz
    """, (allocation_id, principal_anchor_at))
    net_since_anchor = float(cur.fetchone()["net_since_anchor"] or 0)
    principal_now = principal_baseline + net_since_anchor

    # Trading PnL = equity − principal_now. Cleaner than the old baseline-
    # adjustment math: anchor pins the left edge of the tracked track record,
    # all subsequent inflows/outflows are principal changes not profit, and
    # the delta is pure trading outcome.
    total_pnl_usd = equity - principal_now
    total_return_pct = (
        (total_pnl_usd / principal_now * 100) if principal_now > 0 else 0.0
    )

    # Legacy "lifetime_capital_net_usd" field is preserved in the response
    # (subscribers may be rendering it). Scope = lifetime now as before, for
    # backwards compatibility.
    cur.execute("""
        SELECT COALESCE(SUM(CASE kind
                              WHEN 'deposit'    THEN amount_usd
                              WHEN 'withdrawal' THEN -amount_usd
                            END), 0)::float AS lifetime_net
        FROM user_mgmt.allocation_capital_events
        WHERE allocation_id = %s::uuid
          AND deleted_at IS NULL
    """, (allocation_id,))
    lifetime_capital_net = float(cur.fetchone()["lifetime_net"] or 0)

    session_capital_net = 0.0
    if session_active and session_start is not None and session_date:
        # Session open window: session_date at session_start_hour UTC.
        # Conservative: events on/after today 00:00 UTC. The live trader
        # runs its session between 06:00 and 23:55 UTC; any capital event
        # within the session date is assumed to be within the session
        # window (cross-midnight sessions are out of scope for this v1).
        cur.execute("""
            SELECT COALESCE(SUM(CASE kind
                                  WHEN 'deposit'    THEN amount_usd
                                  WHEN 'withdrawal' THEN -amount_usd
                                END), 0)::float AS session_net
            FROM user_mgmt.allocation_capital_events
            WHERE allocation_id = %s::uuid
              AND event_at >= (%s::date)::timestamptz
              AND deleted_at IS NULL
        """, (allocation_id, session_date))
        session_capital_net = float(cur.fetchone()["session_net"] or 0)
        if session_pnl_usd is not None:
            session_pnl_usd -= session_capital_net
            if session_start > 0:
                session_return_pct = (
                    session_pnl_usd / (session_start + session_capital_net) * 100
                    if (session_start + session_capital_net) > 0
                    else 0.0
                )

    return {
        "allocation_id": allocation_id,
        "capital_usd": round(capital, 2),
        "equity_usd": round(equity, 2),
        "session_start_equity_usd": round(session_start, 2) if session_start is not None else None,
        "session_pnl_usd": round(session_pnl_usd, 2) if session_pnl_usd is not None else None,
        "session_return_pct": round(session_return_pct, 4) if session_return_pct is not None else None,
        "session_capital_net_usd": round(session_capital_net, 2),
        "initial_equity_usd": round(initial_equity, 2) if initial_equity is not None else None,
        "total_pnl_usd": round(total_pnl_usd, 2),
        "total_return_pct": round(total_return_pct, 4),
        "lifetime_capital_net_usd": round(lifetime_capital_net, 2),
        # Principal anchor (migration 007)
        "principal_usd":          round(principal_now, 2),
        "principal_baseline_usd": round(principal_baseline, 2),
        "principal_anchor_at":    (
            principal_anchor_at.isoformat()
            if principal_anchor_at else None
        ),
        "principal_anchor_explicit": alloc["principal_anchor_at"] is not None,
        "principal_baseline_explicit": alloc["principal_baseline_usd"] is not None,
        "net_since_anchor_usd":   round(net_since_anchor, 2),
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


# ── Capital Events (manual deposits/withdrawals) ────────────────────────────
# Operator-recorded out-of-band capital movements. Subtracted from PnL math
# in /pnl so trading returns are isolated from principal moves. Until
# automated import via exchange income APIs ships, these are entered by hand
# from /trader/settings.

class CapitalEventCreateRequest(BaseModel):
    # allocation_id may be NULL to create an unmapped event tied only to a
    # connection (e.g., a deposit landing on a connection with multiple
    # allocations where the operator hasn't yet decided which to credit).
    # Either allocation_id OR connection_id must be present so ownership
    # can be verified.
    allocation_id: str | None = None
    connection_id: str | None = None
    amount_usd: float
    kind: str                      # 'deposit' | 'withdrawal'
    event_at: str | None = None    # ISO 8601; defaults to NOW() server-side
    notes: str | None = None


class CapitalEventUpdateRequest(BaseModel):
    allocation_id: str | None = None  # set to remap an unmapped event
    amount_usd: float | None = None
    kind: str | None = None
    event_at: str | None = None
    notes: str | None = None


def _verify_allocation_owned_by(cur, allocation_id: str, user_id: str) -> None:
    cur.execute(
        "SELECT user_id FROM user_mgmt.allocations WHERE allocation_id = %s::uuid",
        (allocation_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Allocation not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your allocation")


def _verify_connection_owned_by(cur, connection_id: str, user_id: str) -> None:
    cur.execute(
        "SELECT user_id FROM user_mgmt.exchange_connections "
        "WHERE connection_id = %s::uuid",
        (connection_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Connection not found")
    if str(row["user_id"]) != user_id:
        raise HTTPException(status_code=403, detail="Not your connection")


def _verify_capital_event_owned_by(cur, event_id: str, user_id: str) -> dict:
    """Resolve ownership through whichever path the event has populated:
    allocation_id (mapped) OR connection_id (unmapped/auto). Returns the
    row dict with owner_user_id resolved."""
    cur.execute("""
        SELECT ce.event_id::text,
               ce.allocation_id::text,
               ce.connection_id::text,
               COALESCE(a.user_id, ec.user_id)::text AS owner_user_id,
               ce.is_manually_overridden,
               ce.deleted_at
          FROM user_mgmt.allocation_capital_events ce
          LEFT JOIN user_mgmt.allocations a
                 ON a.allocation_id = ce.allocation_id
          LEFT JOIN user_mgmt.exchange_connections ec
                 ON ec.connection_id = ce.connection_id
         WHERE ce.event_id = %s::uuid
    """, (event_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Capital event not found")
    if row["owner_user_id"] is None or row["owner_user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your capital event")
    return dict(row)


@router.get("/capital-events")
def list_capital_events(
    allocation_id: str | None = None,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """List capital events. Restricted to events owned by the caller via
    allocation_id (mapped) OR connection_id (unmapped — auto-poll surfaced
    a transfer that needs operator-assignment to a specific allocation).
    Soft-deleted rows are excluded.
    """
    if allocation_id:
        _verify_allocation_owned_by(cur, allocation_id, user_id)
        cur.execute("""
            SELECT ce.event_id::text, ce.allocation_id::text,
                   ce.connection_id::text, ce.event_at,
                   ce.amount_usd, ce.kind, ce.notes, ce.created_at,
                   ce.source, ce.exchange_event_id, ce.is_manually_overridden,
                   ec.exchange AS exchange_name
              FROM user_mgmt.allocation_capital_events ce
              LEFT JOIN user_mgmt.exchange_connections ec
                     ON ec.connection_id = ce.connection_id
             WHERE ce.allocation_id = %s::uuid
               AND ce.deleted_at IS NULL
             ORDER BY ce.event_at DESC, ce.created_at DESC
        """, (allocation_id,))
    else:
        # All events for allocations OR connections owned by this user.
        # The COALESCE-on-ownership pattern surfaces unmapped (NULL alloc)
        # auto-poll events to the connection's owner, so they show up in
        # the UI for assignment to a specific allocation.
        cur.execute("""
            SELECT ce.event_id::text, ce.allocation_id::text,
                   ce.connection_id::text, ce.event_at,
                   ce.amount_usd, ce.kind, ce.notes, ce.created_at,
                   ce.source, ce.exchange_event_id, ce.is_manually_overridden,
                   ec.exchange AS exchange_name
              FROM user_mgmt.allocation_capital_events ce
              LEFT JOIN user_mgmt.allocations a
                     ON a.allocation_id = ce.allocation_id
              LEFT JOIN user_mgmt.exchange_connections ec
                     ON ec.connection_id = ce.connection_id
             WHERE COALESCE(a.user_id, ec.user_id) = %s::uuid
               AND ce.deleted_at IS NULL
             ORDER BY ce.event_at DESC, ce.created_at DESC
        """, (user_id,))

    rows = cur.fetchall()
    events = [
        {
            "event_id":               r["event_id"],
            "allocation_id":          r["allocation_id"],
            "connection_id":          r["connection_id"],
            "event_at":               r["event_at"].isoformat() if r["event_at"] else None,
            "amount_usd":             float(r["amount_usd"]),
            "kind":                   r["kind"],
            "notes":                  r["notes"],
            "created_at":             r["created_at"].isoformat() if r["created_at"] else None,
            "source":                 r["source"],
            "exchange_event_id":      r["exchange_event_id"],
            "is_manually_overridden": r["is_manually_overridden"],
            "exchange_name":          r["exchange_name"],
        }
        for r in rows
    ]
    return {"events": events}


@router.post("/capital-events")
def create_capital_event(
    body: CapitalEventCreateRequest,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Record a new capital event. Owner-gated via either allocation_id
    or connection_id (at least one must be present).

    Manual creation always sets source='manual' + is_manually_overridden=TRUE
    so the auto-poller will never silently overwrite operator intent.
    """
    if body.amount_usd <= 0:
        raise HTTPException(status_code=400, detail="amount_usd must be positive")
    if body.kind not in ("deposit", "withdrawal"):
        raise HTTPException(status_code=400, detail="kind must be 'deposit' or 'withdrawal'")
    if not body.allocation_id and not body.connection_id:
        raise HTTPException(
            status_code=400,
            detail="allocation_id or connection_id required",
        )

    if body.allocation_id:
        _verify_allocation_owned_by(cur, body.allocation_id, user_id)
    if body.connection_id:
        _verify_connection_owned_by(cur, body.connection_id, user_id)

    cur.execute("""
        INSERT INTO user_mgmt.allocation_capital_events
            (allocation_id, connection_id, event_at, amount_usd, kind, notes,
             source, is_manually_overridden)
        VALUES (
            %s::uuid, %s::uuid,
            COALESCE(%s::timestamptz, NOW()),
            %s, %s, %s,
            'manual', TRUE
        )
        RETURNING event_id::text
    """, (
        body.allocation_id, body.connection_id,
        body.event_at,
        body.amount_usd, body.kind, body.notes,
    ))

    event_id = cur.fetchone()["event_id"]
    return {"event_id": event_id, "allocation_id": body.allocation_id}


@router.patch("/capital-events/{event_id}")
def update_capital_event(
    event_id: str,
    body: CapitalEventUpdateRequest,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Partial update. Sets is_manually_overridden=TRUE so subsequent
    auto-polls won't overwrite. Allows setting allocation_id to map an
    auto-detected (allocation_id=NULL) event to a specific allocation.
    """
    _verify_capital_event_owned_by(cur, event_id, user_id)

    updates: list[str] = ["is_manually_overridden = TRUE"]
    params: list = []
    if body.allocation_id is not None:
        # Verify caller owns the target allocation before remap.
        _verify_allocation_owned_by(cur, body.allocation_id, user_id)
        updates.append("allocation_id = %s::uuid")
        params.append(body.allocation_id)
    if body.amount_usd is not None:
        if body.amount_usd <= 0:
            raise HTTPException(status_code=400, detail="amount_usd must be positive")
        updates.append("amount_usd = %s")
        params.append(body.amount_usd)
    if body.kind is not None:
        if body.kind not in ("deposit", "withdrawal"):
            raise HTTPException(status_code=400, detail="kind must be 'deposit' or 'withdrawal'")
        updates.append("kind = %s")
        params.append(body.kind)
    if body.event_at is not None:
        updates.append("event_at = %s::timestamptz")
        params.append(body.event_at)
    if body.notes is not None:
        updates.append("notes = %s")
        params.append(body.notes)

    if len(updates) == 1:
        # Only the override marker — no actual field changes — refuse so
        # callers don't accidentally lock a row by sending an empty patch.
        raise HTTPException(status_code=400, detail="No fields to update")

    params.append(event_id)
    cur.execute(
        f"UPDATE user_mgmt.allocation_capital_events "
        f"SET {', '.join(updates)} WHERE event_id = %s::uuid",
        params,
    )
    return {"updated": True, "event_id": event_id}


@router.delete("/capital-events/{event_id}")
def delete_capital_event(
    event_id: str,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Soft-delete. Sets deleted_at + is_manually_overridden=TRUE so the
    row stays present (auto-poller won't re-create it on the next tick)
    but is excluded from list reads + PnL math (deleted_at IS NULL filter).
    """
    _verify_capital_event_owned_by(cur, event_id, user_id)
    cur.execute("""
        UPDATE user_mgmt.allocation_capital_events
           SET deleted_at = NOW(),
               is_manually_overridden = TRUE
         WHERE event_id = %s::uuid
    """, (event_id,))
    return {"deleted": True, "event_id": event_id}


@router.post("/capital-events/reset-defaults")
def reset_capital_events_to_defaults(
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Wipe all operator-authored capital events (manual entries + manual
    overrides on auto rows) and re-sync from the exchange. Semantic: "show
    me exchange truth only."

    Steps:
      1. Hard-delete EVERY row owned by the caller, regardless of source:
         - source='manual'                    (operator-created records)
         - source IN ('auto','auto-anomaly') (auto-detected — will be
           re-inserted on step 2 with their original exchange-reported
           values, minus any edits/deletes the operator had layered on)
      2. Re-poll each affected connection via poll_connection with
         source='auto'. Only auto rows come back; manual entries are gone
         until the operator re-enters them.

    Scope: caller's own allocations + connections. Ownership resolved
    through allocation_id → allocation.user_id OR connection_id →
    exchange_connection.user_id. One user's reset can never touch
    another user's rows.
    """
    # Collect connections to re-poll BEFORE deleting (we need the linkage).
    # Union across all events the user owns (either mapping chain).
    cur.execute("""
        SELECT DISTINCT ce.connection_id::text AS connection_id
          FROM user_mgmt.allocation_capital_events ce
          LEFT JOIN user_mgmt.allocations a
                 ON a.allocation_id = ce.allocation_id
          LEFT JOIN user_mgmt.exchange_connections ec
                 ON ec.connection_id = ce.connection_id
         WHERE COALESCE(a.user_id, ec.user_id) = %s::uuid
           AND ce.connection_id IS NOT NULL
    """, (user_id,))
    affected_connections = [r["connection_id"] for r in cur.fetchall()]

    # Delete by connection-ownership path.
    cur.execute("""
        DELETE FROM user_mgmt.allocation_capital_events ce
         USING user_mgmt.exchange_connections ec
         WHERE ce.connection_id = ec.connection_id
           AND ec.user_id = %s::uuid
    """, (user_id,))
    deleted_count = cur.rowcount

    # Sweep any remaining rows whose allocation-ownership chain says
    # user-owned but connection_id was NULL (very rare — only happens if
    # a manual entry was created with allocation_id only, which the
    # POST path doesn't explicitly prevent). Covers that edge.
    cur.execute("""
        DELETE FROM user_mgmt.allocation_capital_events ce
         USING user_mgmt.allocations a
         WHERE ce.allocation_id = a.allocation_id
           AND a.user_id = %s::uuid
    """, (user_id,))
    deleted_count += cur.rowcount

    # Re-poll each affected connection so auto events re-appear with
    # their exchange-reported defaults. Import lazily to avoid a module-
    # import cycle when the route file is imported by the celery worker.
    from app.cli.poll_capital_events import poll_connection
    repolled = 0
    for cid in affected_connections:
        try:
            # poll_connection commits internally so failures on one
            # connection don't block the others.
            poll_connection(cur.connection, cur, cid, source="auto")
            repolled += 1
        except Exception as e:
            log.warning(f"reset: re-poll failed for {cid[:8]}: {e}")

    return {
        "reset": True,
        "deleted_rows": deleted_count,
        "connections_repolled": repolled,
    }

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

        positions = []
        for p in futures.get("positions") or []:
            try:
                amt = float(p.get("positionAmt", 0) or 0)
            except (TypeError, ValueError):
                amt = 0.0
            if amt == 0:
                continue
            try:
                entry = float(p.get("entryPrice", 0) or 0)
            except (TypeError, ValueError):
                entry = 0.0
            try:
                upl = float(p.get("unRealizedProfit", 0) or 0)
            except (TypeError, ValueError):
                upl = 0.0
            positions.append({
                "symbol": p.get("symbol", ""),
                "side": "long" if amt > 0 else "short",
                "size": abs(amt),
                "entry_price": entry,
                "mark_price": 0.0,           # not in /fapi/v2/account payload
                "unrealized_pnl": upl,
                "leverage": int(float(p.get("leverage") or 0)),
                "margin_mode": (p.get("marginType") or "").lower(),
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
        mark = _get_binance_spot_price(symbol)
        if mark is None:
            # Leave mark_price=0.0 with a log so UI can show "—" rather than
            # crashing the entire snapshot. UPL also uncomputable without mark.
            log.warning(f"Binance margin: no spot price for {symbol}, leaving mark_price=0")
            mark = 0.0
        positions.append({
            "symbol":         symbol,
            "side":           "long" if net > 0 else "short",
            "size":           abs(net),
            "entry_price":    0.0,           # cross-margin: no stored entry price
            "mark_price":     mark,
            "unrealized_pnl": 0.0,           # requires entry_price — defer until writer persists it
            "leverage":       0,             # cross-margin: per-account, not per-symbol
            "margin_mode":    "cross",
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
                s.filter_mode, s.capital_cap_usd, s.is_published,
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
                s.filter_mode, s.capital_cap_usd, s.is_published,
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
            a.capital_usd, a.status, a.created_at,
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

    Sources:
      - equity_usd        : latest row in user_mgmt.exchange_snapshots
                            for this allocation's connection
      - session_pnl_usd   : equity_usd - session_start_equity
      - session_start     : runtime_state.session_start_equity_usdt
                            (populated by the trader's monitoring loop);
                            falls back to the latest exchange_snapshot at
                            or before today's 06:30 UTC if runtime_state
                            hasn't captured it yet.
      - total_pnl_usd     : equity_usd - capital_usd (cumulative since
                            allocation creation)

    Returns None for session_pnl / session_return_pct when no baseline is
    available (e.g. fresh allocation with no snapshots yet); frontend
    should render an em-dash in that case.
    """
    cur.execute("""
        SELECT capital_usd, user_id, connection_id, runtime_state
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

    # Session-start baseline: prefer runtime_state (written per bar by the
    # trader's monitoring loop); fall back to today's pre-session snapshot.
    session_start = runtime_state.get("session_start_equity_usdt")
    try:
        session_start = float(session_start) if session_start is not None else None
    except (TypeError, ValueError):
        session_start = None

    if session_start is None:
        # Same helper-equivalent query as trader_blofin.py uses to rescue
        # a crashed-before-first-bar baseline. Session opens at 06:35 UTC,
        # so the latest snapshot <= 06:30 UTC is the pre-positions tick.
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
    if session_start is not None and session_start > 0:
        session_pnl_usd = equity - session_start
        session_return_pct = session_pnl_usd / session_start * 100

    total_pnl_usd = equity - capital
    total_return_pct = (total_pnl_usd / capital * 100) if capital > 0 else 0.0

    return {
        "allocation_id": allocation_id,
        "capital_usd": round(capital, 2),
        "equity_usd": round(equity, 2),
        "session_start_equity_usd": round(session_start, 2) if session_start is not None else None,
        "session_pnl_usd": round(session_pnl_usd, 2) if session_pnl_usd is not None else None,
        "session_return_pct": round(session_return_pct, 4) if session_return_pct is not None else None,
        "total_pnl_usd": round(total_pnl_usd, 2),
        "total_return_pct": round(total_return_pct, 4),
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

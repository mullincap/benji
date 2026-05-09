"""
backend/app/services/exchanges/binance.py
==========================================
Binance REST client for account introspection + USDⓈ-M futures trading.

Scope:
  • Spot/sapi: permissions probe, spot balance, cross-margin account/positions
    (read-only).
  • USDⓈ-M futures (fapi): account, positions, instrument metadata, tickers,
    user trades, income history, leverage/marginType setters, order placement,
    one-way / hedge mode probe.

HMAC-SHA256 signing via stdlib to avoid adding a dependency.

Usage:
    from app.services.exchanges.binance import BinanceClient, BinanceAuthError
    c = BinanceClient(api_key="...", api_secret="...")
    perms = c.get_permissions()   # /sapi/v1/account/apiRestrictions
    pos   = c.get_futures_position_risk()  # /fapi/v2/positionRisk
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any
from urllib.parse import urlencode

import requests


# ─── Endpoints ────────────────────────────────────────────────────────────────

SPOT_BASE_PROD = "https://api.binance.com"
SPOT_BASE_TEST = "https://testnet.binance.vision"
FAPI_BASE_PROD = "https://fapi.binance.com"
FAPI_BASE_TEST = "https://testnet.binancefuture.com"

DEFAULT_TIMEOUT_S = 10
DEFAULT_RECV_WINDOW_MS = 5000


# ─── Exceptions ───────────────────────────────────────────────────────────────

class BinanceError(Exception):
    """Base class for all Binance client errors."""


class BinanceAuthError(BinanceError):
    """Invalid API key / signature. Maps to HTTP 401/403 from Binance."""


class BinancePermissionError(BinanceError):
    """Key valid but lacks permission for the requested endpoint."""


class BinanceNetworkError(BinanceError):
    """Transport-level failure: timeout, DNS, connection refused, etc."""


class BinanceRateLimitError(BinanceError):
    """Weight ceiling reached or 418/429 response from Binance.

    Raised both pre-flight (when an outgoing call would exceed the
    weight ceiling per the most recent X-MBX-USED-WEIGHT-1M header) and
    in-flight (HTTP 418 Banned / 429 Too Many Requests). Distinct from
    BinanceNetworkError because the network worked — the API said no.
    """


# ─── Client ───────────────────────────────────────────────────────────────────

class BinanceClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool = False):
        if not api_key or not api_secret:
            raise BinanceAuthError("api_key and api_secret are required")
        self._api_key = api_key
        self._api_secret = api_secret.encode()
        self._spot_base = SPOT_BASE_TEST if testnet else SPOT_BASE_PROD
        self._fapi_base = FAPI_BASE_TEST if testnet else FAPI_BASE_PROD

    # ── signing ──

    def _sign(self, query: str) -> str:
        return hmac.new(self._api_secret, query.encode(), hashlib.sha256).hexdigest()

    def _signed_query(self, params: dict[str, Any] | None = None) -> str:
        """Build query string with timestamp + recvWindow + signature."""
        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = DEFAULT_RECV_WINDOW_MS
        qs = urlencode(params)
        sig = self._sign(qs)
        return f"{qs}&signature={sig}"

    def _get_signed(self, base: str, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._signed_request("GET", base, path, params)

    def _signed_request(
        self, method: str, base: str, path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Generalized signed-request helper covering GET / POST / DELETE.

        Binance accepts the signed query string in the URL for all verbs on
        signed endpoints (the docs use `?queryString` form for POST too) — no
        request body. Returns parsed JSON which can be a dict OR a list
        (e.g. /fapi/v2/positionRisk returns a JSON array).
        """
        method = method.upper()
        url = f"{base}{path}?{self._signed_query(params)}"
        headers = {"X-MBX-APIKEY": self._api_key}
        try:
            resp = requests.request(
                method, url, headers=headers, timeout=DEFAULT_TIMEOUT_S,
            )
        except requests.Timeout as e:
            raise BinanceNetworkError(f"Binance request timed out: {e}") from e
        except requests.ConnectionError as e:
            raise BinanceNetworkError(f"Binance connection error: {e}") from e
        except requests.RequestException as e:
            raise BinanceNetworkError(f"Binance request failed: {e}") from e

        # Map HTTP status codes before JSON parsing — Binance sometimes returns
        # non-JSON bodies on auth failures (WAF, CloudFront).
        if resp.status_code in (401, 403):
            raise BinanceAuthError(f"Binance auth rejected ({resp.status_code}): {resp.text[:200]}")
        if resp.status_code == 418 or resp.status_code == 429:
            raise BinanceNetworkError(f"Binance rate-limited ({resp.status_code})")

        try:
            body = resp.json()
        except ValueError as e:
            raise BinanceError(f"Binance returned non-JSON ({resp.status_code}): {resp.text[:200]}") from e

        # Binance returns {"code": <int>, "msg": <str>} on errors even with 200.
        # Lists never carry an error code, so the dict-only check is correct.
        if resp.status_code >= 400 or (
            isinstance(body, dict) and body.get("code") and body.get("code") != 200
        ):
            code = body.get("code") if isinstance(body, dict) else None
            msg = body.get("msg") if isinstance(body, dict) else resp.text[:200]
            # Binance error code -2015: "Invalid API-key, IP, or permissions for action"
            # -2014: "API-key format invalid"
            # -1022: "Signature for this request is not valid"
            if code in (-2014, -1022):
                raise BinanceAuthError(f"Binance: {msg} (code={code})")
            if code == -2015:
                raise BinancePermissionError(f"Binance: {msg} (code={code})")
            raise BinanceError(f"Binance error: {msg} (code={code}, http={resp.status_code})")

        return body

    def _get_public(self, base: str, path: str, params: dict[str, Any] | None = None) -> Any:
        """Unsigned GET for public endpoints (exchangeInfo, ticker)."""
        url = f"{base}{path}"
        if params:
            from urllib.parse import urlencode as _ue
            url = f"{url}?{_ue(params)}"
        try:
            resp = requests.get(url, timeout=DEFAULT_TIMEOUT_S)
        except requests.Timeout as e:
            raise BinanceNetworkError(f"Binance request timed out: {e}") from e
        except requests.ConnectionError as e:
            raise BinanceNetworkError(f"Binance connection error: {e}") from e
        except requests.RequestException as e:
            raise BinanceNetworkError(f"Binance request failed: {e}") from e

        if resp.status_code in (418, 429):
            raise BinanceNetworkError(f"Binance rate-limited ({resp.status_code})")
        try:
            body = resp.json()
        except ValueError as e:
            raise BinanceError(f"Binance returned non-JSON ({resp.status_code}): {resp.text[:200]}") from e
        if resp.status_code >= 400:
            code = body.get("code") if isinstance(body, dict) else None
            msg = body.get("msg") if isinstance(body, dict) else resp.text[:200]
            raise BinanceError(f"Binance error: {msg} (code={code}, http={resp.status_code})")
        return body

    # ── endpoints ──

    def get_permissions(self) -> dict:
        """GET /sapi/v1/account/apiRestrictions — permission flags."""
        return self._get_signed(self._spot_base, "/sapi/v1/account/apiRestrictions")

    def get_spot_account(self) -> dict:
        """GET /api/v3/account — spot balances + account-level flags."""
        return self._get_signed(self._spot_base, "/api/v3/account")

    def get_futures_account(self) -> dict | None:
        """GET /fapi/v2/account — futures balances. None if the key lacks futures perm."""
        try:
            return self._get_signed(self._fapi_base, "/fapi/v2/account")
        except BinancePermissionError:
            return None
        except BinanceAuthError:
            # Futures base sometimes returns 401 for keys without futures enabled.
            return None

    def get_margin_account(self) -> dict | None:
        """GET /sapi/v1/margin/account — cross-margin balances, BTC-denominated totals.

        Returns None if the key lacks margin permission. Other errors (network,
        5xx) propagate as BinanceNetworkError / BinanceError.
        """
        try:
            return self._get_signed(self._spot_base, "/sapi/v1/margin/account")
        except BinancePermissionError:
            return None
        except BinanceAuthError:
            return None

    def get_futures_position_risk(self) -> list[dict] | None:
        """GET /fapi/v2/positionRisk — per-symbol futures positions with entry price.

        Unlike /fapi/v2/account (aggregate balances), this endpoint always
        returns the live positions array with entryPrice, markPrice, leverage,
        marginType. Works in both classic and UTA / portfolio-margin account
        modes, so it's the authoritative source for "what futures positions
        are open RIGHT NOW".

        Returns None if the key lacks futures permission. Callers merge the
        entry prices from this response into whatever balance path they took
        (futures-primary or cross-margin fallback).
        """
        try:
            return self._get_signed(self._fapi_base, "/fapi/v2/positionRisk")
        except BinancePermissionError:
            return None
        except BinanceAuthError:
            return None

    # ── Futures (fapi) — read endpoints used by BinanceFuturesAdapter ──

    def get_futures_balance(self) -> list[dict]:
        """GET /fapi/v2/balance — list of per-asset futures wallet balances.

        Distinct from /fapi/v2/account (aggregate). Each row carries
        {asset, balance, crossWalletBalance, availableBalance, ...}. The
        adapter reads the USDT row for available + total.
        """
        return self._get_signed(self._fapi_base, "/fapi/v2/balance")

    def get_futures_position_side_dual(self) -> dict:
        """GET /fapi/v1/positionSide/dual — returns {"dualSidePosition": bool}.

        True = hedge mode (long+short positions per symbol).
        False = one-way mode (single net position per symbol).

        Our trader assumes one-way mode; the adapter refuses to construct if
        hedge mode is on.
        """
        return self._get_signed(self._fapi_base, "/fapi/v1/positionSide/dual")

    def get_futures_exchange_info(self) -> dict:
        """GET /fapi/v1/exchangeInfo — all-symbol metadata. Public, unsigned.

        Returns full payload {timezone, serverTime, rateLimits, symbols:[…]}
        with status, contractType, pricePrecision, quantityPrecision, filters,
        and onboardDate per symbol. Caller caches; this is several-hundred KB.
        """
        return self._get_public(self._fapi_base, "/fapi/v1/exchangeInfo")

    def get_futures_ticker_price(self, symbol: str) -> dict:
        """GET /fapi/v1/ticker/price?symbol=… — last trade price. Public."""
        return self._get_public(
            self._fapi_base, "/fapi/v1/ticker/price", {"symbol": symbol},
        )

    def get_futures_user_trades(
        self, symbol: str, since_ms: int | None = None, limit: int = 500,
    ) -> list[dict]:
        """GET /fapi/v1/userTrades — per-symbol fills with realized PnL + commission.

        Per-symbol query is mandatory (no all-symbol form). Caller iterates
        across the inst_ids it cares about and unions results.
        """
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if since_ms is not None:
            params["startTime"] = int(since_ms)
        return self._get_signed(self._fapi_base, "/fapi/v1/userTrades", params)

    def get_futures_income(
        self, since_ms: int | None = None,
        income_type: str | None = None, limit: int = 1000,
    ) -> list[dict]:
        """GET /fapi/v1/income — wallet flows (TRANSFER, REALIZED_PNL, etc.).

        For capital-events tracking, query with income_type='TRANSFER' to
        capture deposits + withdrawals into the futures wallet. Negative
        income = withdrawal, positive = deposit (per Binance convention).
        """
        params: dict[str, Any] = {"limit": limit}
        if since_ms is not None:
            params["startTime"] = int(since_ms)
        if income_type:
            params["incomeType"] = income_type
        return self._get_signed(self._fapi_base, "/fapi/v1/income", params)

    # ── Futures (fapi) — trade endpoints used by BinanceFuturesAdapter ──

    def set_futures_leverage(self, symbol: str, leverage: int) -> dict:
        """POST /fapi/v1/leverage — set initial leverage for a symbol.

        Echoes back {leverage, maxNotionalValue, symbol}. Per-symbol setting,
        persists on the account until next change.
        """
        return self._signed_request(
            "POST", self._fapi_base, "/fapi/v1/leverage",
            {"symbol": symbol, "leverage": int(leverage)},
        )

    def set_futures_margin_type(self, symbol: str, margin_type: str) -> dict:
        """POST /fapi/v1/marginType — 'ISOLATED' or 'CROSSED'.

        Returns {"code": 200, "msg": "success"} on change, or raises with
        code -4046 ("No need to change margin type") if already set.
        Adapter swallows -4046 since it's idempotent-equivalent.
        """
        if margin_type not in ("ISOLATED", "CROSSED"):
            raise ValueError(f"margin_type must be ISOLATED or CROSSED, got {margin_type!r}")
        return self._signed_request(
            "POST", self._fapi_base, "/fapi/v1/marginType",
            {"symbol": symbol, "marginType": margin_type},
        )

    def place_futures_order(
        self, *, symbol: str, side: str, order_type: str,
        quantity: float | None = None,
        reduce_only: bool = False,
        close_position: bool = False,
        stop_price: float | None = None,
        working_type: str | None = None,
        time_in_force: str | None = None,
        new_client_order_id: str | None = None,
    ) -> dict:
        """POST /fapi/v1/order.

        Caller supplies Binance-form symbol ("BTCUSDT"). For MARKET entry/exit
        passes order_type="MARKET" + quantity. For native stop-loss, the
        adapter sends a separate STOP_MARKET with reduce_only + stop_price
        + closePosition=true.
        """
        params: dict[str, Any] = {
            "symbol":      symbol,
            "side":        side.upper(),
            "type":        order_type.upper(),
        }
        if quantity is not None:
            params["quantity"] = quantity
        if reduce_only:
            params["reduceOnly"] = "true"
        if close_position:
            params["closePosition"] = "true"
        if stop_price is not None:
            params["stopPrice"] = stop_price
        if working_type:
            params["workingType"] = working_type   # "MARK_PRICE" or "CONTRACT_PRICE"
        if time_in_force:
            params["timeInForce"] = time_in_force
        if new_client_order_id:
            params["newClientOrderId"] = new_client_order_id
        return self._signed_request(
            "POST", self._fapi_base, "/fapi/v1/order", params,
        )

    def cancel_all_futures_orders(self, symbol: str) -> dict:
        """DELETE /fapi/v1/allOpenOrders — cancel all open orders for a symbol.

        Used by close_position cleanup to drop dangling stops before issuing
        the closing market order.
        """
        return self._signed_request(
            "DELETE", self._fapi_base, "/fapi/v1/allOpenOrders",
            {"symbol": symbol},
        )

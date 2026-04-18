"""
backend/app/services/exchanges/binance.py
==========================================
Minimal read-only Binance REST client for account introspection.

Scope: permissions probe + spot balance + optional futures balance. No trade
endpoints. HMAC-SHA256 signing via stdlib to avoid adding a dependency.

Usage:
    from app.services.exchanges.binance import BinanceClient, BinanceAuthError
    c = BinanceClient(api_key="...", api_secret="...")
    perms = c.get_permissions()   # dict from /sapi/v1/account/apiRestrictions
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

    def _get_signed(self, base: str, path: str, params: dict[str, Any] | None = None) -> dict:
        url = f"{base}{path}?{self._signed_query(params)}"
        headers = {"X-MBX-APIKEY": self._api_key}
        try:
            resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT_S)
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

        # Binance returns {"code": <int>, "msg": <str>} on errors even with 200
        if resp.status_code >= 400 or (isinstance(body, dict) and body.get("code") and body.get("code") != 200):
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

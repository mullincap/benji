"""
backend/app/services/exchanges/permissions.py
===============================================
Exchange-agnostic permissions probe + validator.

fetch_permissions() dispatches to the right client based on the exchange name
and decrypts credentials from the exchange_connections row. validate_permissions()
enforces the read-only MVP policy.

Policy (2026-04-18 reversal): keys must be trade-capable because the allocator
executes strategies. Withdrawals remain the security boundary.

  • Binance: requires read + spot/margin trading, rejects withdrawals.
  • BloFin:  requires trade-capable (readOnly=0). Cannot distinguish TRADE from
             TRANSFER via query-apikey — we accept and rely on UI guidance.

PermissionSet fields are `bool | None`; None means "could not confirm". BloFin's
query-apikey logs its raw response at WARN on first call with the marker
`BLOFIN_APIKEY_SCHEMA_CAPTURE` for schema debugging.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from typing import Any

from ..encryption import decrypt_key
from .binance import (
    BinanceClient,
    BinanceAuthError,
    BinancePermissionError,
    BinanceNetworkError,
    BinanceError,
)

log = logging.getLogger(__name__)


# ─── Model ────────────────────────────────────────────────────────────────────

@dataclass
class PermissionSet:
    """Parsed permission flags. `None` means 'could not confirm from response'
    — distinct from False ('confirmed disabled')."""
    read: bool | None
    spot_trade: bool | None
    futures_trade: bool | None
    withdrawals: bool | None
    raw: dict = field(default_factory=dict)

    def to_public_dict(self) -> dict[str, Any]:
        """Return a UI-safe dict (excludes raw payload)."""
        d = asdict(self)
        d.pop("raw", None)
        return d


# ─── Exchange-specific parsers ────────────────────────────────────────────────

def _parse_binance(raw: dict) -> PermissionSet:
    """
    Parse /sapi/v1/account/apiRestrictions response.

    Known fields: enableReading, enableSpotAndMarginTrading, enableFutures,
    enableWithdrawals, ipRestrict, createTime, enableMargin, etc.
    """
    def b(key: str) -> bool | None:
        v = raw.get(key)
        return v if isinstance(v, bool) else None

    return PermissionSet(
        read=b("enableReading"),
        spot_trade=b("enableSpotAndMarginTrading"),
        futures_trade=b("enableFutures"),
        withdrawals=b("enableWithdrawals"),
        raw=raw,
    )


def _parse_blofin(raw: dict) -> PermissionSet:
    """
    Parse BloFin /api/v1/user/query-apikey.

    Real response schema (captured from prod, 2026-04-17):
        {"code":"0","msg":"success","data":{
            "uid":"...","apiName":"...","apiKey":"...",
            "readOnly": 0,           # int: 0 = tradable, 1 = strictly read-only
            "ips": [], "type": 1,
            "expireTime":"<ms>", "createTime":"<ms>",
            "referralCode":"...", "parentUid":"0"
        }}

    BloFin does NOT break permissions into READ / TRADE / TRANSFER on this
    endpoint — only a single `readOnly` flag. We interpret:
      • readOnly=1 → key is read-only. spot/futures/withdrawals all False.
      • readOnly=0 → key has trading permissions. Map to spot_trade=True +
        futures_trade=True (BloFin is futures-native, a trade-enabled key can
        do both). withdrawals stays None because this endpoint doesn't
        expose transfer/withdraw granularity — but the policy rejects on
        spot/futures anyway so we never fall through to the "cannot confirm"
        path for trade-enabled keys.
      • readOnly missing/unrecognized → all non-read fields None → rejected.
    """
    data = raw.get("data") if isinstance(raw, dict) else None
    if not isinstance(data, dict):
        return PermissionSet(read=True, spot_trade=None, futures_trade=None, withdrawals=None, raw=raw)

    ro = data.get("readOnly")
    # Accept int or str form ("0"/"1") defensively.
    if ro in (1, "1", True):
        return PermissionSet(
            read=True, spot_trade=False, futures_trade=False, withdrawals=False, raw=raw,
        )
    if ro in (0, "0", False):
        return PermissionSet(
            read=True, spot_trade=True, futures_trade=True, withdrawals=None, raw=raw,
        )

    # readOnly absent or unrecognized → admit read (call succeeded), reject the rest.
    return PermissionSet(read=True, spot_trade=None, futures_trade=None, withdrawals=None, raw=raw)


# ─── Dispatch ─────────────────────────────────────────────────────────────────

class PermissionProbeError(Exception):
    """Raised when fetch_permissions fails. HTTP-mappable subclasses below."""


class PermissionAuthError(PermissionProbeError):
    """Exchange rejected the key as invalid."""


class PermissionNetworkError(PermissionProbeError):
    """Transport-level failure reaching the exchange."""


class PermissionUnsupportedExchange(PermissionProbeError):
    """The connection's `exchange` value isn't one we support."""


def fetch_permissions(
    *, exchange: str, api_key_enc: str, api_secret_enc: str,
    passphrase_enc: str | None = None, testnet: bool = False,
) -> PermissionSet:
    """
    Decrypt credentials, probe the exchange, return a PermissionSet.

    Raises:
        PermissionAuthError — invalid key (maps to HTTP 400).
        PermissionNetworkError — transport failure (maps to HTTP 503).
        PermissionUnsupportedExchange — unknown exchange (maps to HTTP 400).
    """
    exchange = (exchange or "").lower()
    api_key = decrypt_key(api_key_enc) if api_key_enc else None
    api_secret = decrypt_key(api_secret_enc) if api_secret_enc else None
    passphrase = decrypt_key(passphrase_enc) if passphrase_enc else None

    if not api_key or not api_secret:
        raise PermissionAuthError("Stored credentials could not be decrypted")

    if exchange == "binance":
        client = BinanceClient(api_key=api_key, api_secret=api_secret, testnet=testnet)
        try:
            raw = client.get_permissions()
        except BinanceAuthError as e:
            raise PermissionAuthError(str(e)) from e
        except BinancePermissionError as e:
            # Key authenticates but can't hit apiRestrictions — treat as auth issue.
            raise PermissionAuthError(f"Key lacks permission to read its own restrictions: {e}") from e
        except BinanceNetworkError as e:
            raise PermissionNetworkError(str(e)) from e
        except BinanceError as e:
            raise PermissionProbeError(str(e)) from e
        return _parse_binance(raw)

    if exchange == "blofin":
        if not passphrase:
            raise PermissionAuthError("BloFin requires a passphrase")
        # Call the existing BloFin helper — kept in allocator.py to avoid
        # circular imports when exchanges/ is used from other services too.
        from ...api.routes.allocator import _blofin_get  # local import on purpose
        try:
            raw = _blofin_get(
                "/api/v1/user/query-apikey",
                api_key=api_key, api_secret=api_secret, passphrase=passphrase,
            )
        except Exception as e:  # requests.HTTPError / ConnectionError / etc.
            # Distinguish auth vs network by string match — BloFin returns code
            # "50113" for bad keys. Not perfect but good enough for MVP.
            msg = str(e)
            if "401" in msg or "403" in msg or "50113" in msg or "invalid" in msg.lower():
                raise PermissionAuthError(f"BloFin auth rejected: {msg}") from e
            raise PermissionNetworkError(f"BloFin request failed: {msg}") from e

        # Verify envelope. BloFin returns {"code": "0", "msg": "success", "data": ...}
        code = raw.get("code") if isinstance(raw, dict) else None
        if str(code) not in ("0", "00000"):
            raise PermissionAuthError(f"BloFin returned code={code}, msg={raw.get('msg')}")

        # Amendment 1: log raw response on first call so we can capture the real
        # schema from prod logs. Marker makes it greppable.
        log.warning("BLOFIN_APIKEY_SCHEMA_CAPTURE raw=%r", raw)

        return _parse_blofin(raw)

    raise PermissionUnsupportedExchange(f"Exchange '{exchange}' is not supported")


# ─── Validator ────────────────────────────────────────────────────────────────

def validate_permissions(
    exchange: str, perms: PermissionSet,
) -> tuple[bool, str | None]:
    """
    Validate that an exchange key has the permissions required to execute
    strategies (trade-capable, no withdrawals).

    Binance: requires read + spot/margin trading, rejects withdrawals.
    BloFin:  requires trade-capable (readOnly=0). BloFin's query-apikey does
             not break out TRADE vs TRANSFER — we accept any non-read-only key
             and steer users via UI guidance to enable only Trade.
    """
    exchange = (exchange or "").lower()

    if exchange == "binance":
        if perms.read is not True:
            return False, (
                "Key is missing read permission. Enable 'Enable Reading' in your "
                "Binance API key settings."
            )
        if perms.spot_trade is not True:
            return False, (
                "Key is missing margin trading permission. Enable 'Enable Spot & "
                "Margin Trading' in your Binance API key settings — required to "
                "execute strategies."
            )
        if perms.withdrawals is True:
            return False, (
                "Withdrawals must be disabled. Create a new Binance API key "
                "without 'Enable Withdrawals' checked."
            )
        return True, None

    if exchange == "blofin":
        # BloFin's query-apikey exposes only a single `readOnly` int. Our parser
        # maps readOnly=1 → spot/futures/withdrawals all False, readOnly=0 →
        # spot/futures True + withdrawals None (cannot distinguish).
        if perms.read is not True:
            return False, (
                "Cannot confirm read access on BloFin key — please contact support."
            )
        if perms.spot_trade is None or perms.futures_trade is None:
            return False, (
                "Cannot confirm trade permission on BloFin key — please contact support."
            )
        if perms.spot_trade is False and perms.futures_trade is False:
            return False, (
                "Key is read-only. Create a new BloFin API key with 'Trade' "
                "permission enabled — required to execute strategies."
            )
        return True, None

    return False, f"Unsupported exchange: {exchange}"

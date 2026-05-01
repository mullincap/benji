"""
backend/app/cli/binance_perm_audit.py
======================================
Runnable as: python -m app.cli.binance_perm_audit

One-shot permissions audit for the Binance USDM Futures key — exercised
against every endpoint the Manager Live tab will read, plus the listenKey
endpoint that gates the user-data WebSocket stream.

Pulls the encrypted key for the active Binance connection from
user_mgmt.exchange_connections, decrypts via the project's Fernet helper,
calls each endpoint once with minimum-friction params, and prints a
PASS/FAIL line per endpoint with the Binance error code (if any) so
permission gaps surface as -2015 rather than silent "None" responses.

Exits 0 if every endpoint passes. Exits 1 if any endpoint is blocked or
errors — the printed summary lists exactly which ones, so the build can
proceed only after the key is widened.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sys
import time
from typing import Any
from urllib.parse import urlencode

import requests

from ..db import get_worker_conn
from ..services.encryption import decrypt_key

# ── Endpoints ───────────────────────────────────────────────────────────────

FAPI = "https://fapi.binance.com"
DAPI_DATA = "https://fapi.binance.com"  # /futures/data/* lives under fapi.binance.com
SPOT = "https://api.binance.com"  # /sapi/v1/account/apiRestrictions for permission flags
TIMEOUT = 10
RECV_WINDOW = 5000

# (label, method, base, path, params, signed)
# Signed endpoints add timestamp + recvWindow + signature; public endpoints don't.
PROBES: list[tuple[str, str, str, str, dict, bool]] = [
    ("apiRestrictions",       "GET",  SPOT, "/sapi/v1/account/apiRestrictions",        {},                                       True),
    ("fapi/v2/account",       "GET",  FAPI, "/fapi/v2/account",                         {},                                       True),
    ("fapi/v2/positionRisk",  "GET",  FAPI, "/fapi/v2/positionRisk",                    {},                                       True),
    ("fapi/v1/openOrders",    "GET",  FAPI, "/fapi/v1/openOrders",                      {},                                       True),
    ("fapi/v1/income",        "GET",  FAPI, "/fapi/v1/income",                          {"limit": 1},                             True),
    ("fapi/v1/userTrades",    "GET",  FAPI, "/fapi/v1/userTrades",                      {"symbol": "BTCUSDT", "limit": 1},        True),
    ("fapi/v1/klines",        "GET",  FAPI, "/fapi/v1/klines",                          {"symbol": "BTCUSDT", "interval": "1m", "limit": 1}, False),
    ("fapi/v1/premiumIndex",  "GET",  FAPI, "/fapi/v1/premiumIndex",                    {"symbol": "BTCUSDT"},                    False),
    ("fapi/v1/openInterest",  "GET",  FAPI, "/fapi/v1/openInterest",                    {"symbol": "BTCUSDT"},                    False),
    ("openInterestHist",      "GET",  FAPI, "/futures/data/openInterestHist",           {"symbol": "BTCUSDT", "period": "1d", "limit": 2}, False),
    ("topLongShortRatio",     "GET",  FAPI, "/futures/data/topLongShortAccountRatio",   {"symbol": "BTCUSDT", "period": "1h", "limit": 1}, False),
    ("listenKey",             "POST", FAPI, "/fapi/v1/listenKey",                       {},                                       "header_only"),  # type: ignore[list-item]
]

# ── Signing ─────────────────────────────────────────────────────────────────

def sign_query(secret: bytes, params: dict[str, Any]) -> str:
    qs = urlencode(params)
    sig = hmac.new(secret, qs.encode(), hashlib.sha256).hexdigest()
    return f"{qs}&signature={sig}"


# ── Single-probe runner ─────────────────────────────────────────────────────

def probe(*, api_key: str, secret: bytes, label: str, method: str, base: str, path: str,
          params: dict, signed: Any) -> dict:
    """Execute one probe. Returns {label, status, http, code, msg, weight}."""
    headers = {"X-MBX-APIKEY": api_key} if (signed is True or signed == "header_only") else {}
    url = f"{base}{path}"

    if signed is True:
        # GET with full HMAC signature
        p = dict(params)
        p["timestamp"] = int(time.time() * 1000)
        p["recvWindow"] = RECV_WINDOW
        url = f"{url}?{sign_query(secret, p)}"
        kwargs: dict[str, Any] = {}
    elif signed == "header_only":
        # listenKey on UM Futures: API key header, no signature, no body
        kwargs = {}
    else:
        # Public endpoint
        url = f"{url}?{urlencode(params)}" if params else url
        kwargs = {}

    try:
        if method == "GET":
            resp = requests.get(url, headers=headers, timeout=TIMEOUT, **kwargs)
        else:
            resp = requests.post(url, headers=headers, timeout=TIMEOUT, **kwargs)
    except requests.RequestException as e:
        return {"label": label, "status": "NETWORK", "http": None, "code": None,
                "msg": f"{type(e).__name__}: {e}", "weight": None}

    weight = resp.headers.get("X-MBX-USED-WEIGHT-1M") or resp.headers.get("X-MBX-USED-WEIGHT")

    try:
        body = resp.json()
    except ValueError:
        return {"label": label, "status": "NON_JSON", "http": resp.status_code,
                "code": None, "msg": resp.text[:160], "weight": weight}

    code = body.get("code") if isinstance(body, dict) else None
    msg = body.get("msg") if isinstance(body, dict) else None

    if resp.status_code == 200 and code in (None, 200):
        return {"label": label, "status": "PASS", "http": 200, "code": code,
                "msg": None, "weight": weight}

    # Classify failure modes
    if code == -2015:
        bucket = "PERM_DENIED"
    elif code in (-2014, -1022):
        bucket = "AUTH_FAIL"
    elif code in (-1021,):
        bucket = "TIMESTAMP_SKEW"
    elif resp.status_code in (401, 403):
        bucket = "AUTH_FAIL"
    elif resp.status_code in (418, 429):
        bucket = "RATE_LIMITED"
    else:
        bucket = "FAIL"

    return {"label": label, "status": bucket, "http": resp.status_code, "code": code,
            "msg": msg, "weight": weight}


# ── Connection lookup ──────────────────────────────────────────────────────

def fetch_active_binance_creds() -> tuple[str, str, str]:
    """Find the active Binance connection and decrypt its key/secret.

    Returns (connection_id, api_key, api_secret). Raises RuntimeError if
    no active connection exists or if decryption fails.
    """
    with get_worker_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT connection_id, api_key_enc, api_secret_enc
                  FROM user_mgmt.exchange_connections
                 WHERE exchange = 'binance' AND status = 'active'
                 ORDER BY created_at DESC
                 LIMIT 1
            """)
            row = cur.fetchone()
    if not row:
        raise RuntimeError("No active Binance connection in user_mgmt.exchange_connections")
    cid, key_enc, secret_enc = row[0], row[1], row[2]
    if not key_enc or not secret_enc:
        raise RuntimeError(f"Connection {cid}: encrypted credentials are NULL")
    api_key = decrypt_key(key_enc)
    api_secret = decrypt_key(secret_enc)
    if not api_key or not api_secret:
        raise RuntimeError(f"Connection {cid}: decryption returned None")
    return str(cid), api_key, api_secret


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    print("=" * 78)
    print("Binance USDM Futures — permissions audit")
    print("=" * 78)

    try:
        conn_id, api_key, api_secret = fetch_active_binance_creds()
    except RuntimeError as e:
        print(f"\n✗ ABORT: {e}")
        return 1
    secret_bytes = api_secret.encode()
    print(f"connection_id : {conn_id}")
    print(f"api_key       : {api_key[:6]}…{api_key[-4:]} ({len(api_key)} chars)")
    print()

    results: list[dict] = []
    for label, method, base, path, params, signed in PROBES:
        r = probe(api_key=api_key, secret=secret_bytes, label=label, method=method,
                  base=base, path=path, params=params, signed=signed)
        results.append(r)
        # Per-row line
        sym = {
            "PASS": "✓",
            "PERM_DENIED": "✗",
            "AUTH_FAIL": "✗",
            "RATE_LIMITED": "·",
            "TIMESTAMP_SKEW": "·",
            "NETWORK": "·",
            "NON_JSON": "·",
            "FAIL": "✗",
        }.get(r["status"], "?")
        wt = f"w={r['weight']}" if r.get("weight") else ""
        line = f"  {sym} {r['status']:14}  {label:24}  http={r['http']}  code={r['code']}  {wt}"
        if r.get("msg"):
            line += f"\n      msg: {r['msg']}"
        print(line)
        # Tiny gap between calls — be polite to the rate limiter even on a healthy key.
        time.sleep(0.1)

    # Summary
    blocked = [r for r in results if r["status"] in ("PERM_DENIED", "AUTH_FAIL", "FAIL")]
    other_failures = [r for r in results if r["status"] in ("RATE_LIMITED", "TIMESTAMP_SKEW", "NETWORK", "NON_JSON")]
    print()
    print("-" * 78)
    print(f"PASS    : {len([r for r in results if r['status'] == 'PASS'])} / {len(results)}")
    print(f"BLOCKED : {len(blocked)}")
    if blocked:
        print("          " + ", ".join(r["label"] for r in blocked))
    if other_failures:
        print(f"NOISE   : {len(other_failures)}  ({', '.join(r['label'] for r in other_failures)})")
    print("-" * 78)

    # JSON dump for machine consumption
    print("\nJSON:")
    print(json.dumps(results, indent=2, default=str))

    return 0 if not blocked and not other_failures else 1


if __name__ == "__main__":
    sys.exit(main())

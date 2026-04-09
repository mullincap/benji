"""
pipeline/allocator/blofin_auth.py
=================================
Shared BloFin HMAC-SHA256 signing logic.
Extracted from trader-blofin.py so multiple scripts can authenticate
without duplicating the signing implementation.
"""

import os
import hmac
import json
import time
import base64
import hashlib
from uuid import uuid4


def get_headers(method: str, path: str, body: str = "") -> dict:
    """
    Build the complete dict of BloFin auth headers.

    Parameters
    ----------
    method : str   – HTTP method, e.g. "GET" or "POST"
    path   : str   – Full request path including query string,
                      e.g. "/api/v1/account/balance" or
                      "/api/v1/account/positions?instId=BTC-USDT"
    body   : str   – JSON-encoded request body for POST requests.
                      Empty string for GET requests.

    Returns
    -------
    dict with keys: ACCESS-KEY, ACCESS-SIGN, ACCESS-TIMESTAMP,
                    ACCESS-NONCE, ACCESS-PASSPHRASE, Content-Type
    """
    api_key    = os.environ["BLOFIN_API_KEY"]
    api_secret = os.environ["BLOFIN_API_SECRET"]
    passphrase = os.environ["BLOFIN_PASSPHRASE"]

    ts    = str(int(time.time() * 1000))
    nonce = str(uuid4())

    # Body is only included in the prehash for non-GET requests
    body_str = body if method.upper() != "GET" else ""
    prehash  = f"{path}{method.upper()}{ts}{nonce}{body_str}"

    hex_sig   = hmac.new(
        api_secret.encode(), prehash.encode(), hashlib.sha256
    ).hexdigest().encode()
    signature = base64.b64encode(hex_sig).decode()

    return {
        "ACCESS-KEY":        api_key,
        "ACCESS-SIGN":       signature,
        "ACCESS-TIMESTAMP":  ts,
        "ACCESS-NONCE":      nonce,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type":      "application/json",
    }

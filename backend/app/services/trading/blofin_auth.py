"""
backend/app/services/trading/blofin_auth.py
============================================
BloFin HMAC-SHA256 signing. Vendored from pipeline/allocator/blofin_auth.py so
the backend container doesn't depend on the pipeline venv's sys.path.

Reads BLOFIN_API_KEY / BLOFIN_API_SECRET / BLOFIN_PASSPHRASE from the process
environment at call time (not module load) — so the module imports cleanly in
environments where BloFin creds aren't present, and only fails when a signed
request is actually attempted.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from uuid import uuid4


def get_headers(method: str, path: str, body: str = "") -> dict:
    """Build the complete dict of BloFin auth headers.

    Reads BLOFIN_API_KEY / BLOFIN_API_SECRET / BLOFIN_PASSPHRASE from
    os.environ at call time. Raises KeyError if any is unset.
    """
    api_key = os.environ["BLOFIN_API_KEY"]
    api_secret = os.environ["BLOFIN_API_SECRET"]
    passphrase = os.environ["BLOFIN_PASSPHRASE"]

    ts = str(int(time.time() * 1000))
    nonce = str(uuid4())
    body_str = body if method.upper() != "GET" else ""
    prehash = f"{path}{method.upper()}{ts}{nonce}{body_str}"

    hex_sig = hmac.new(
        api_secret.encode(), prehash.encode(), hashlib.sha256,
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

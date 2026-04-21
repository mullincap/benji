"""
backend/app/services/trading/blofin_auth.py
============================================
BloFin HMAC-SHA256 signing. Vendored from pipeline/allocator/blofin_auth.py so
the backend container doesn't depend on the pipeline venv's sys.path.

Creds resolution order:
  1. Explicit kwargs (api_key / api_secret / passphrase) — used by the
     per-allocation BlofinREST client, which stores Fernet-decrypted
     credentials from exchange_connections.api_key_enc on instance.
  2. BLOFIN_API_KEY / BLOFIN_API_SECRET / BLOFIN_PASSPHRASE env vars —
     fallback for --close-all CLI and any legacy master paths.

Env vars are read at call time (not module load) so the module imports
cleanly in environments where BloFin creds aren't present.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from uuid import uuid4


def get_headers(
    method: str,
    path: str,
    body: str = "",
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
    passphrase: str | None = None,
) -> dict:
    """Build the complete dict of BloFin auth headers.

    Explicit kwargs take precedence; any missing kwarg falls back to the
    corresponding env var read at call time. Raises KeyError if a required
    credential is neither passed nor in the environment.
    """
    if api_key is None:
        api_key = os.environ["BLOFIN_API_KEY"]
    if api_secret is None:
        api_secret = os.environ["BLOFIN_API_SECRET"]
    if passphrase is None:
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

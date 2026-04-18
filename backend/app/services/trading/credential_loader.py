"""
backend/app/services/trading/credential_loader.py
==================================================
Load decrypted exchange credentials for a given connection_id.

Thin wrapper around the existing encryption service — isolated here so the
trader execution path has a clear interface and doesn't reach into allocator
routing code.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..encryption import decrypt_key
from ...db import get_conn


class CredentialDecryptError(Exception):
    """Stored ciphertext couldn't be recovered. Likely legacy plaintext or
    wrong FERNET_KEY."""


@dataclass
class ExchangeCredentials:
    exchange: str                 # 'blofin' | 'binance'
    api_key: str
    api_secret: str
    passphrase: str | None        # BloFin-specific; None for Binance


def load_credentials(connection_id: str) -> ExchangeCredentials:
    """Fetch + decrypt credentials for a connection.

    Raises ValueError if the connection doesn't exist or is not active.
    Raises CredentialDecryptError if the stored ciphertext is unrecoverable.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT exchange, api_key_enc, api_secret_enc, passphrase_enc, status
                FROM user_mgmt.exchange_connections
                WHERE connection_id = %s::uuid
                """,
                (connection_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise ValueError(f"No exchange connection found: {connection_id}")

    exchange, ak_enc, as_enc, pp_enc, status = row
    if status != "active":
        raise ValueError(
            f"Connection {connection_id} status is {status!r}, not 'active'"
        )

    ak = decrypt_key(ak_enc)
    as_ = decrypt_key(as_enc)
    pp = decrypt_key(pp_enc) if pp_enc else None

    # Each encrypted field that was stored must decrypt back to a string.
    # A None from decrypt_key signals either legacy plaintext (pre-encryption
    # rows) or a FERNET_KEY mismatch — either way, unusable.
    if ak is None or as_ is None or (pp_enc and pp is None):
        raise CredentialDecryptError(
            f"Stored credentials for connection {connection_id} could not be "
            "decrypted. Row may contain legacy plaintext or wrong FERNET_KEY."
        )

    return ExchangeCredentials(
        exchange=exchange, api_key=ak, api_secret=as_, passphrase=pp,
    )

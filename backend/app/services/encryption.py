"""
backend/app/services/encryption.py
===================================
Fernet symmetric encryption for exchange API keys at rest.

Usage:
    from app.services.encryption import encrypt_key, decrypt_key

    ciphertext = encrypt_key("my-api-key")   # → base64 Fernet token string
    plaintext  = decrypt_key(ciphertext)      # → "my-api-key"

Setup:
    1. Generate a key:
       python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    2. Add to secrets.env (or .env):
       # REQUIRED — losing this key makes all stored exchange keys unrecoverable.
       FERNET_KEY=<paste-key-here>

    The key is loaded from settings.FERNET_KEY at import time. If missing,
    encrypt_key() raises ValueError — decrypt_key() returns None so that
    legacy unencrypted rows don't crash reads.
"""

from __future__ import annotations

import logging

from cryptography.fernet import Fernet, InvalidToken

from ..core.config import settings

log = logging.getLogger(__name__)

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    """Lazy-init the Fernet instance from settings."""
    global _fernet
    if _fernet is None:
        key = settings.FERNET_KEY
        if not key:
            raise ValueError(
                "FERNET_KEY is not set. Generate one with: "
                'python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"'
            )
        _fernet = Fernet(key.encode())
    return _fernet


def encrypt_key(plaintext: str) -> str:
    """Encrypt a plaintext string. Returns a Fernet token (base64 string)."""
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt_key(ciphertext: str) -> str | None:
    """
    Decrypt a Fernet token back to plaintext.
    Returns None if decryption fails (wrong key, corrupted, or plaintext legacy value).
    """
    if not ciphertext:
        return None
    try:
        f = _get_fernet()
        return f.decrypt(ciphertext.encode()).decode()
    except (InvalidToken, ValueError):
        # Could be a legacy unencrypted value or wrong key.
        # Log at debug — callers decide how to handle None.
        log.debug("decrypt_key failed — possibly legacy plaintext or wrong FERNET_KEY")
        return None

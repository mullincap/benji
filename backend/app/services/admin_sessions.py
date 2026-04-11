"""
backend/app/services/admin_sessions.py
======================================
Flat-file admin session store backing the Compiler/Indexer admin pages.

Why flat file (not DB, not Redis):
  - Single admin user, low traffic — DB connection overhead would dwarf the work
  - Survives uvicorn --reload (DB tables are stable, Redis would lose state)
  - Zero new dependencies
  - Easy to inspect/wipe by hand (just edit/delete the JSON file)
  - Single-process model — concurrent writes are rare enough (one admin) that
    we rely on os.replace's atomic rename for crash safety, no advisory locks

File format (backend/data/admin_sessions.json):
  {
    "tokens": {
      "<random-32-byte-hex>": {
        "created_at":  "2026-04-08T01:23:45+00:00",
        "expires_at":  "2026-04-09T01:23:45+00:00",
        "last_seen_at":"2026-04-08T01:30:12+00:00"
      },
      ...
    }
  }

The token itself is opaque — generated via secrets.token_hex(32) which gives
64 hex chars of cryptographic randomness. The token NEVER encodes anything
about the passphrase. Knowing the passphrase does NOT let you craft a valid
token without first hitting POST /api/admin/login.

Sessions auto-expire after 24 hours (configurable via SESSION_TTL_HOURS).
Expired tokens are pruned lazily on every access — no background job needed.
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# 24-hour session lifetime — matches the spec ("24h expiry")
SESSION_TTL_HOURS = 24


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_file(path: Path) -> None:
    """Create the sessions file with an empty {tokens: {}} body if missing."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"tokens": {}}, indent=2))


def _load(path: Path) -> dict:
    """Read sessions file. Returns {tokens: {...}} dict."""
    _ensure_file(path)
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or "tokens" not in data:
        return {"tokens": {}}
    return data


def _save(path: Path, data: dict) -> None:
    """
    Atomic write with a per-call unique temp file. Two concurrent writers
    would race on a shared `<name>.tmp` filename — the first one's
    os.replace consumes the tmp, the second one's os.replace blows up
    with FileNotFoundError. Using tempfile.mkstemp in the same directory
    gives each writer its own source file while keeping the rename atomic.
    """
    import os
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_str, path)
    except Exception:
        # Clean up our tmp file if the rename failed
        try:
            os.unlink(tmp_str)
        except FileNotFoundError:
            pass
        raise


def _prune_expired(data: dict) -> dict:
    """Remove tokens whose expires_at is in the past. Mutates+returns data."""
    now = _now()
    keep = {}
    for token, meta in data.get("tokens", {}).items():
        try:
            expires_at = datetime.fromisoformat(meta["expires_at"])
        except (KeyError, ValueError, TypeError):
            continue  # malformed → drop
        if expires_at > now:
            keep[token] = meta
    data["tokens"] = keep
    return data


# ─── Public API ──────────────────────────────────────────────────────────────

def create_session(sessions_file: Path) -> str:
    """
    Generate a new session token, persist it, and return the token string.
    Pruning of expired tokens happens as a side-effect on every write.
    """
    token = secrets.token_hex(32)  # 64 hex chars = 256 bits of entropy
    now = _now()
    meta = {
        "created_at":   now.isoformat(),
        "expires_at":   (now + timedelta(hours=SESSION_TTL_HOURS)).isoformat(),
        "last_seen_at": now.isoformat(),
    }

    data = _load(sessions_file)
    data = _prune_expired(data)
    data["tokens"][token] = meta
    _save(sessions_file, data)
    return token


def validate_session(sessions_file: Path, token: Optional[str]) -> bool:
    """
    Return True if `token` exists and is not expired. Updates last_seen_at as
    a side-effect on hits. Returns False for None, missing, expired, or
    malformed tokens.
    """
    if not token:
        return False

    data = _load(sessions_file)
    data = _prune_expired(data)

    meta = data["tokens"].get(token)
    if not meta:
        # Token not in store (could have just been pruned). Persist the prune.
        _save(sessions_file, data)
        return False

    # Update last_seen_at — this is a write but it's small and rare enough
    # that taking the exclusive lock per validate is fine for this scale.
    meta["last_seen_at"] = _now().isoformat()
    data["tokens"][token] = meta
    _save(sessions_file, data)
    return True


def delete_session(sessions_file: Path, token: Optional[str]) -> bool:
    """Remove a single token from the store. Returns True if it existed."""
    if not token:
        return False
    data = _load(sessions_file)
    data = _prune_expired(data)
    existed = token in data["tokens"]
    if existed:
        del data["tokens"][token]
    _save(sessions_file, data)
    return existed

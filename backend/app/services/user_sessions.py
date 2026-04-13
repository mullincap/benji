"""
backend/app/services/user_sessions.py
=====================================
DB-backed per-user session store for the allocator module.

Sessions are stored in user_mgmt.user_sessions (Postgres) instead of a
flat-file JSON. The public interface is unchanged so auth.py callers
don't need modification — the `sessions_file` parameter is accepted
for signature compatibility but ignored.

Table schema:
  CREATE TABLE user_mgmt.user_sessions (
      token      TEXT        PRIMARY KEY,
      user_id    UUID        NOT NULL REFERENCES user_mgmt.users(user_id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      expires_at TIMESTAMPTZ NOT NULL
  );

TTL: 30 days. get_user_id (validate_user_session) checks expires_at on
every call — expired tokens return None immediately without relying on
background cleanup.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from ..core.config import settings

SESSION_TTL_DAYS = 30
# Kept for backward-compat with auth.py which imports this name
SESSION_TTL_HOURS = SESSION_TTL_DAYS * 24


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _get_conn():
    """Open a short-lived DB connection for session operations."""
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        connect_timeout=5,
    )


# ─── Public API ──────────────────────────────────────────────────────────────
# Signatures keep the `sessions_file: Path` first arg for caller compat
# with auth.py (which passes settings.USER_SESSIONS_FILE). It's ignored.


def create_user_session(sessions_file: Path, user_id: str) -> str:
    """Create a session token for a specific user. Returns the token string."""
    token = secrets.token_hex(32)  # 64 hex chars = 256 bits
    expires_at = _now() + timedelta(days=SESSION_TTL_DAYS)

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_mgmt.user_sessions (token, user_id, created_at, expires_at)
                VALUES (%s, %s::uuid, NOW(), %s)
            """, (token, user_id, expires_at))
        conn.commit()
    finally:
        conn.close()
    return token


def validate_user_session(sessions_file: Path, token: Optional[str]) -> Optional[str]:
    """
    Return the user_id if token is valid and not expired.
    Returns None for missing, invalid, or expired tokens.
    Expiry is checked inline — no reliance on background cleanup.
    """
    if not token:
        return None

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id FROM user_mgmt.user_sessions
                WHERE token = %s AND expires_at > NOW()
            """, (token,))
            row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()

    if not row:
        return None
    return str(row["user_id"])


def delete_user_session(sessions_file: Path, token: Optional[str]) -> bool:
    """Remove a session token. Returns True if it existed."""
    if not token:
        return False

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM user_mgmt.user_sessions WHERE token = %s
            """, (token,))
            deleted = cur.rowcount > 0
        conn.commit()
    finally:
        conn.close()
    return deleted


def cleanup_expired_sessions() -> int:
    """
    Delete all expired session rows. Returns the number of rows removed.
    Intended to be called from a background/cron task (weekly cadence is fine).
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM user_mgmt.user_sessions WHERE expires_at < NOW()")
            count = cur.rowcount
        conn.commit()
    finally:
        conn.close()
    return count

"""
backend/app/api/routes/auth.py
==============================
Per-user authentication for the allocator module.

Endpoints:
  POST /api/auth/register — create a new user account
  POST /api/auth/login    — authenticate and set session cookie
  POST /api/auth/logout   — invalidate session, clear cookie
  GET  /api/auth/me       — return current user info

Dependency:
  get_current_user(request, cur) — extracts user_id from the session cookie,
  verifies the user exists and is active. Returns user_id as a string.
  Use on allocator routes in place of require_admin.

DB migration required:
  ALTER TABLE user_mgmt.users ADD COLUMN IF NOT EXISTS password_hash TEXT;
  ALTER TABLE user_mgmt.users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;
"""

from __future__ import annotations

import os
import logging
from typing import Any

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

from ...core.config import settings
from ...db import get_cursor
from ...services.rate_limit import RateLimit
from ...services.user_sessions import (
    create_user_session,
    validate_user_session,
    delete_user_session,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])

COOKIE_NAME = "user_session"
# DB session row TTL is 30d (SESSION_TTL_DAYS). The cookie max-age is shorter
# (14d) so the UI can truthfully label the "Remember device" checkbox; the
# user logs in again ~14d later and receives a fresh token. The DB row's
# remaining 16d acts as a buffer for stale-cookie cleanup.
REMEMBER_COOKIE_DAYS = 14
COOKIE_MAX_AGE = REMEMBER_COOKIE_DAYS * 24 * 3600

# ─── Login lockout policy ───────────────────────────────────────────────────
# 5 consecutive failed logins → 15-minute account lock. The window is the
# lifetime of a single bad-password streak; a successful login resets the
# counter and clears any lock. Returns 423 with a retry_after seconds field
# while locked.
LOCKOUT_THRESHOLD = 5
LOCKOUT_DURATION_MINUTES = 15


def _cookie_secure() -> bool:
    return os.environ.get("COOKIE_SECURE", "false").lower() in ("1", "true", "yes")


def _hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def _verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


# ─── Auth dependency (replaces require_admin on allocator routes) ───────────

def get_current_user(request: Request, cur=Depends(get_cursor)) -> str:
    """
    FastAPI dependency: extracts user_id from the user_session cookie.
    Returns the user_id string. Raises 401 if not authenticated.
    """
    token = request.cookies.get(COOKIE_NAME)
    user_id = validate_user_session(settings.USER_SESSIONS_FILE, token)
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )

    # Verify the user still exists and is active
    cur.execute("""
        SELECT user_id, is_active FROM user_mgmt.users
        WHERE user_id = %s::uuid
    """, (user_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="User not found")
    if not row["is_active"]:
        raise HTTPException(status_code=403, detail="Account deactivated")

    return user_id


# ─── Request models ─────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str
    # remember=true (default) → 30-day persistent cookie; remember=false →
    # session cookie that expires when the browser closes. The DB session
    # row TTL is unchanged either way.
    remember: bool = True


# ─── Routes ─────────────────────────────────────────────────────────────────

@router.post("/register")
def register(body: RegisterRequest, cur=Depends(get_cursor)) -> dict[str, Any]:
    """Create a new user account."""
    if not body.email or not body.password:
        raise HTTPException(status_code=400, detail="email and password required")
    if len(body.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    # Check for duplicate email
    cur.execute("SELECT user_id FROM user_mgmt.users WHERE email = %s", (body.email,))
    if cur.fetchone():
        raise HTTPException(status_code=409, detail="Email already registered")

    password_hash = _hash_password(body.password)
    cur.execute("""
        INSERT INTO user_mgmt.users (email, password_hash, is_active, created_at, updated_at)
        VALUES (%s, %s, TRUE, NOW(), NOW())
        RETURNING user_id
    """, (body.email, password_hash))
    row = cur.fetchone()
    user_id = str(row["user_id"])

    log.info("Registered user %s (%s)", user_id, body.email)
    return {"user_id": user_id, "email": body.email}


@router.post("/login", dependencies=[Depends(RateLimit("login"))])
def login(body: LoginRequest, response: Response, cur=Depends(get_cursor)) -> dict[str, Any]:
    """Authenticate user and set session cookie."""
    if not body.email or not body.password:
        raise HTTPException(status_code=400, detail="email and password required")

    cur.execute("""
        SELECT user_id, email, password_hash, is_active,
               failed_login_count, locked_until, first_login
        FROM user_mgmt.users WHERE email = %s
    """, (body.email,))
    row = cur.fetchone()

    # Generic 401 if the account doesn't exist OR has no password set.
    # Don't leak account existence via differing error messages.
    if not row or not row["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not row["is_active"]:
        raise HTTPException(status_code=403, detail="Account deactivated")

    user_id = str(row["user_id"])

    # Honor an active lockout before checking the password — prevents the
    # password check itself from being used as a timing oracle while
    # locked.
    if row["locked_until"]:
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (locked_until - NOW()))::int AS retry_after
            FROM user_mgmt.users WHERE user_id = %s::uuid
        """, (user_id,))
        lock_row = cur.fetchone()
        retry_after = int(lock_row["retry_after"]) if lock_row else 0
        if retry_after > 0:
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail={"error": "account_locked", "retry_after": retry_after},
                headers={"Retry-After": str(retry_after)},
            )

    if not _verify_password(body.password, row["password_hash"]):
        # Failed-attempt accounting; trigger lockout at the threshold.
        new_count = (row["failed_login_count"] or 0) + 1
        if new_count >= LOCKOUT_THRESHOLD:
            cur.execute("""
                UPDATE user_mgmt.users
                   SET failed_login_count = %s,
                       locked_until = NOW() + INTERVAL %s
                 WHERE user_id = %s::uuid
            """, (new_count, f"{LOCKOUT_DURATION_MINUTES} minutes", user_id))
            log.warning("Account locked: user=%s (%s) after %s failed attempts",
                        user_id, body.email, new_count)
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail={"error": "account_locked", "retry_after": LOCKOUT_DURATION_MINUTES * 60},
                headers={"Retry-After": str(LOCKOUT_DURATION_MINUTES * 60)},
            )
        cur.execute("""
            UPDATE user_mgmt.users SET failed_login_count = %s WHERE user_id = %s::uuid
        """, (new_count, user_id))
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Successful login — reset counters and refresh last_login.
    cur.execute("""
        UPDATE user_mgmt.users
           SET last_login = NOW(),
               failed_login_count = 0,
               locked_until = NULL
         WHERE user_id = %s::uuid
    """, (user_id,))

    token = create_user_session(settings.USER_SESSIONS_FILE, user_id)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        # remember=False → session cookie (max_age omitted via None)
        max_age=COOKIE_MAX_AGE if body.remember else None,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        path="/",
    )
    return {
        "ok": True,
        "user_id": user_id,
        "email": row["email"],
        "first_login": bool(row["first_login"]),
    }


@router.post("/logout")
def logout(request: Request, response: Response) -> dict[str, Any]:
    """Invalidate session and clear cookie."""
    token = request.cookies.get(COOKIE_NAME)
    deleted = delete_user_session(settings.USER_SESSIONS_FILE, token)
    response.delete_cookie(key=COOKIE_NAME, path="/")
    return {"ok": True, "session_existed": deleted}


@router.get("/me")
def me(user_id: str = Depends(get_current_user), cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return current authenticated user info."""
    cur.execute("""
        SELECT user_id, email, created_at, first_login,
               first_name, last_name, firm, role
        FROM user_mgmt.users WHERE user_id = %s::uuid
    """, (user_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "user_id": str(row["user_id"]),
        "email": row["email"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "first_login": bool(row["first_login"]),
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "firm": row["firm"],
        "role": row["role"],
    }

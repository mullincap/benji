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
from ...services.user_sessions import (
    SESSION_TTL_HOURS,
    create_user_session,
    validate_user_session,
    delete_user_session,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["auth"])

COOKIE_NAME = "user_session"
COOKIE_MAX_AGE = SESSION_TTL_HOURS * 3600


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


@router.post("/login")
def login(body: LoginRequest, response: Response, cur=Depends(get_cursor)) -> dict[str, Any]:
    """Authenticate user and set session cookie."""
    if not body.email or not body.password:
        raise HTTPException(status_code=400, detail="email and password required")

    cur.execute("""
        SELECT user_id, email, password_hash, is_active
        FROM user_mgmt.users WHERE email = %s
    """, (body.email,))
    row = cur.fetchone()

    if not row or not row["password_hash"]:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not row["is_active"]:
        raise HTTPException(status_code=403, detail="Account deactivated")
    if not _verify_password(body.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user_id = str(row["user_id"])

    # Update last_login
    cur.execute("UPDATE user_mgmt.users SET last_login = NOW() WHERE user_id = %s::uuid", (user_id,))

    token = create_user_session(settings.USER_SESSIONS_FILE, user_id)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        path="/",
    )
    return {"ok": True, "user_id": user_id, "email": row["email"]}


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
    cur.execute("SELECT user_id, email, created_at FROM user_mgmt.users WHERE user_id = %s::uuid", (user_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "user_id": str(row["user_id"]),
        "email": row["email"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
    }

"""
backend/app/api/routes/admin.py
===============================
Admin auth routes and the require_admin dependency used by other admin
routers (compiler, indexer).

Endpoints:
  POST /api/admin/login    — body {passphrase}; sets admin_session cookie on match
  POST /api/admin/logout   — invalidates current session, clears cookie
  GET  /api/admin/whoami   — returns {authenticated: bool, expires_at?: ...}

Cookie:
  name:     admin_session
  value:    64-hex-char random token (NOT a passphrase derivative)
  httpOnly: true
  secure:   false in dev (localhost), true in production via env var
  samesite: lax
  max-age:  86400 (24h)

Dependency:
  require_admin(request) — raises 401 if cookie missing/invalid, returns
  the session token on success. Routes that need protection use:
      @router.get("...", dependencies=[Depends(require_admin)])
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel

from ...core.config import settings
from ...services.admin_sessions import (
    SESSION_TTL_HOURS,
    create_session,
    delete_session,
    validate_session,
)

# ── Self-load secrets.env for INTERNAL_API_TOKEN on the server ────────────────
_SECRETS = Path("/mnt/quant-data/credentials/secrets.env")
if _SECRETS.exists():
    for _line in _SECRETS.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

INTERNAL_API_TOKEN = settings.INTERNAL_API_TOKEN or os.environ.get("INTERNAL_API_TOKEN", "")

router = APIRouter(prefix="/api/admin", tags=["admin"])

COOKIE_NAME = "admin_session"
COOKIE_MAX_AGE = SESSION_TTL_HOURS * 3600


def _cookie_secure() -> bool:
    """True in production (HTTPS only), false in local dev."""
    return os.environ.get("COOKIE_SECURE", "false").lower() in ("1", "true", "yes")


# ─── Request models ──────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    passphrase: str


# ─── Auth dependency ─────────────────────────────────────────────────────────

def require_admin(request: Request) -> str:
    """
    FastAPI dependency: enforces a valid admin session cookie OR a valid
    X-Internal-Token header (for server-to-server calls like the briefing cron).
    Returns the session token (or "internal") on success. Raises 401 otherwise.

    Use as: @router.get(..., dependencies=[Depends(require_admin)])
    or:     def my_route(token: str = Depends(require_admin), ...)
    """
    # Check X-Internal-Token header first (server-to-server auth)
    internal_token = request.headers.get("X-Internal-Token")
    if internal_token and INTERNAL_API_TOKEN and internal_token == INTERNAL_API_TOKEN:
        return "internal"

    token = request.cookies.get(COOKIE_NAME)
    if not validate_session(settings.ADMIN_SESSIONS_FILE, token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin session required",
        )
    return token  # type: ignore[return-value]  # validated above


# ─── Routes ──────────────────────────────────────────────────────────────────

@router.post("/login")
def login(body: LoginRequest, response: Response) -> dict[str, Any]:
    """
    Validate passphrase against ADMIN_PASSPHRASE config. On match, generate
    a fresh random session token, persist it, and set the httpOnly cookie.
    """
    if not settings.ADMIN_PASSPHRASE:
        raise HTTPException(
            status_code=503,
            detail="Admin login is not configured. Set ADMIN_PASSPHRASE in backend .env.",
        )

    if body.passphrase != settings.ADMIN_PASSPHRASE:
        raise HTTPException(status_code=401, detail="Invalid passphrase")

    token = create_session(settings.ADMIN_SESSIONS_FILE)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        path="/",
    )
    return {"ok": True, "expires_in_seconds": COOKIE_MAX_AGE}


@router.post("/logout")
def logout(request: Request, response: Response) -> dict[str, Any]:
    """Invalidate the current session token and clear the cookie."""
    token = request.cookies.get(COOKIE_NAME)
    deleted = delete_session(settings.ADMIN_SESSIONS_FILE, token)
    response.delete_cookie(key=COOKIE_NAME, path="/")
    return {"ok": True, "session_existed": deleted}


@router.get("/whoami")
def whoami(request: Request) -> dict[str, Any]:
    """
    Return authentication state. Used by the frontend layout to decide
    whether to redirect to /compiler/login. Never raises 401 — returns
    {authenticated: false} for unauth callers.
    """
    token = request.cookies.get(COOKIE_NAME)
    authed = validate_session(settings.ADMIN_SESSIONS_FILE, token)
    return {"authenticated": bool(authed)}

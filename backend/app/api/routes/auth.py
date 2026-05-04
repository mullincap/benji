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

import hashlib
import os
import logging
import secrets
from datetime import datetime, timedelta, timezone
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


# ─── Password strength validation ───────────────────────────────────────────
# Mirrors frontend/app/(auth)/_components/PasswordStrengthMeter.tsx exactly.
# v1a public minimum is score >= 3 ("Good") — corresponds to the
# "≥12 chars, mixed case, number, symbol" requirement on accept-invite.
PASSWORD_MIN_SCORE = 3

_COMMON_PASSWORD_SUBSTRINGS = (
    "password", "qwerty", "admin", "123456", "letmein",
    "welcome", "monkey", "dragon", "master", "login",
    "abc123", "iloveyou", "sunshine", "princess", "football",
    "baseball", "shadow", "superman", "mullincap", "3m3m",
)


def _password_score(password: str) -> int:
    """Returns 0–4. Mirror of the frontend scorer; keep in sync."""
    if not password:
        return 0
    lower = password.lower()
    if any(c in lower for c in _COMMON_PASSWORD_SUBSTRINGS):
        return 1

    classes = (
        any("a" <= c <= "z" for c in password)
        + any("A" <= c <= "Z" for c in password)
        + any(c.isdigit() for c in password)
        + any(not c.isalnum() for c in password)
    )

    if len(password) < 8 or classes < 2:
        return 1
    if len(password) < 12:
        return 2
    if len(password) < 16:
        return 4 if classes >= 4 else 3
    return 4 if classes >= 3 else 3


def _validate_password_or_400(password: str) -> None:
    if _password_score(password) < PASSWORD_MIN_SCORE:
        raise HTTPException(
            status_code=400,
            detail=(
                "Password too weak. Minimum 12 characters with mixed case, "
                "a number, and a symbol; avoid common substrings."
            ),
        )


# ─── Invite token helpers ──────────────────────────────────────────────────
# Tokens are minted via secrets.token_urlsafe(32) and live ONLY in the
# invite URL. The DB stores the SHA-256 hex digest. Same convention reset
# tokens will use in Phase 1b.

def _hash_invite_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


# ─── Post-login default landing path ───────────────────────────────────────
# Server-side rule: users with at least one active allocation land on
# /manager/overview (the surface that surfaces live state); users with
# none land on /trader/overview (the workspace where they'd configure
# their first allocation). Explicit ?next= on signin always overrides
# this — the rule only applies when the user opens /auth/signin
# directly with no return-to context.

def _default_landing_for(cur, user_id: str) -> str:
    cur.execute(
        "SELECT count(*) AS n FROM user_mgmt.allocations "
        "WHERE user_id = %s::uuid AND status = 'active'",
        (user_id,),
    )
    row = cur.fetchone()
    if row and (row["n"] or 0) > 0:
        return "/manager/overview"
    return "/trader/overview"


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


class AcceptInviteRequest(BaseModel):
    first_name: str
    last_name: str
    # firm is optional — many invitees (especially solo traders) don't have
    # a firm to enter, and the previous "all profile fields required" gate
    # blocked them at the form. Frontend shows it as "Firm (optional)";
    # null/empty here gets persisted as NULL on user_mgmt.users.firm
    # (column is already nullable, no migration needed).
    firm: str | None = None
    role: str
    password: str


class ChangePasswordRequest(BaseModel):
    new_password: str
    # Optional only when the user is currently using a temporary password
    # (admin-issued). For self-initiated changes, current_password is
    # required — verified server-side.
    current_password: str | None = None


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
        # The UPDATEs persist even though we raise HTTPException — see
        # the transaction-lifecycle contract on get_cursor (db.py).
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
        "default_landing": _default_landing_for(cur, user_id),
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
    """Return current authenticated user info.

    has_exchange is computed via a cheap EXISTS subquery so the trader
    (protected) layout and /trader/get-started page can decide their
    redirect direction without a second round-trip to /onboarding/state.
    The fuller onboarding state (selected strategy, active allocation,
    etc.) lives at /api/onboarding/state — keeping /me lean since it's
    polled on every page mount."""
    cur.execute("""
        SELECT u.user_id, u.email, u.created_at, u.first_login,
               u.first_name, u.last_name, u.firm, u.role,
               u.is_admin, u.password_is_temporary,
               EXISTS(
                 SELECT 1 FROM user_mgmt.exchange_connections ec
                  WHERE ec.user_id = u.user_id AND ec.status != 'revoked'
               ) AS has_exchange
        FROM user_mgmt.users u WHERE u.user_id = %s::uuid
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
        "is_admin": bool(row["is_admin"]),
        "password_is_temporary": bool(row["password_is_temporary"]),
        "has_exchange": bool(row["has_exchange"]),
    }


# ─── Invitations (public, token-gated) ─────────────────────────────────────

@router.get("/invite/{token}")
def get_invite(token: str, cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return invitation details for the accept-invite screen.

    Public endpoint — the token itself is the auth. Returns 404 for
    missing, expired, or already-accepted invitations (don't differentiate
    states publicly so the token can't be probed for state).
    """
    token_hash = _hash_invite_token(token)
    cur.execute("""
        SELECT inviter_name, inviter_firm, invited_email, expires_at,
               accepted_at, suggested_firm, suggested_role
        FROM user_mgmt.invitations
        WHERE token_hash = %s
    """, (token_hash,))
    row = cur.fetchone()
    if not row or row["accepted_at"] is not None or row["expires_at"] <= _now_utc():
        raise HTTPException(status_code=404, detail="Invitation not found or expired")
    return {
        "inviter_name": row["inviter_name"],
        "inviter_firm": row["inviter_firm"],
        "invited_email": row["invited_email"],
        "expires_at": row["expires_at"].isoformat(),
        # Non-binding hints from the admin's "New Invitation" form.
        # Acceptance form prefills from these; invitee can override.
        # NULL for invitations issued before migration 025 OR via the
        # CLI tool (which doesn't capture these fields).
        "suggested_firm": row["suggested_firm"],
        "suggested_role": row["suggested_role"],
    }


@router.post("/invite/{token}/accept", dependencies=[Depends(RateLimit("invite_accept"))])
def accept_invite(
    token: str,
    body: AcceptInviteRequest,
    response: Response,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Atomically validate token, create user, mark invitation accepted, sign in."""
    # firm dropped from the required check — see AcceptInviteRequest
    # docstring. Other profile fields stay required.
    if not all([body.first_name.strip(), body.last_name.strip(), body.role.strip()]):
        raise HTTPException(status_code=400, detail="first_name, last_name, and role are required")
    _validate_password_or_400(body.password)

    token_hash = _hash_invite_token(token)

    # Lock the invitation row for the duration of the transaction. The
    # cursor is shared via Depends(get_cursor), so we're inside an
    # implicit transaction already; SELECT FOR UPDATE prevents two
    # concurrent accepts on the same token.
    cur.execute("""
        SELECT invitation_id, invited_email, expires_at, accepted_at
        FROM user_mgmt.invitations
        WHERE token_hash = %s
        FOR UPDATE
    """, (token_hash,))
    invite = cur.fetchone()
    if not invite or invite["accepted_at"] is not None or invite["expires_at"] <= _now_utc():
        raise HTTPException(status_code=404, detail="Invitation not found or expired")

    invited_email = invite["invited_email"]

    # Reject if a user with the invited email already exists. The unique
    # constraint on users.email would catch this on insert, but checking
    # explicitly gives a cleaner error.
    cur.execute("SELECT user_id FROM user_mgmt.users WHERE email = %s", (invited_email,))
    if cur.fetchone():
        raise HTTPException(status_code=409, detail="An account with that email already exists")

    password_hash = _hash_password(body.password)
    # Normalize firm: null / empty / whitespace-only all collapse to NULL
    # in the DB so downstream consumers don't need to distinguish the
    # three "no firm" representations.
    firm_normalized = body.firm.strip() if body.firm and body.firm.strip() else None

    cur.execute("""
        INSERT INTO user_mgmt.users
            (email, password_hash, is_active, email_verified, first_login,
             first_name, last_name, firm, role,
             created_at, updated_at)
        VALUES (%s, %s, TRUE, TRUE, TRUE, %s, %s, %s, %s, NOW(), NOW())
        RETURNING user_id
    """, (
        invited_email, password_hash,
        body.first_name.strip(), body.last_name.strip(),
        firm_normalized, body.role.strip(),
    ))
    new_user_id = str(cur.fetchone()["user_id"])

    cur.execute("""
        UPDATE user_mgmt.invitations
           SET accepted_at = NOW(), accepted_user_id = %s::uuid
         WHERE invitation_id = %s
    """, (new_user_id, invite["invitation_id"]))

    log.info("Invite accepted: user=%s email=%s", new_user_id, invited_email)

    # Sign in the new user immediately. Persistent cookie (remember=True
    # default) — invite accepters are unlikely to want session-only.
    #
    # Insert the session row using the REQUEST cursor (not create_user_session)
    # so it lands in the same transaction as the user INSERT. Otherwise the
    # session row's FK to users would fail — create_user_session opens its own
    # connection which can't see the uncommitted user row.
    token_value = secrets.token_hex(32)
    session_expires = datetime.now(timezone.utc) + timedelta(days=30)
    cur.execute(
        """
        INSERT INTO user_mgmt.user_sessions (token, user_id, created_at, expires_at)
        VALUES (%s, %s::uuid, NOW(), %s)
        """,
        (token_value, new_user_id, session_expires),
    )
    response.set_cookie(
        key=COOKIE_NAME,
        value=token_value,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=_cookie_secure(),
        samesite="lax",
        path="/",
    )
    return {
        "ok": True,
        "user_id": new_user_id,
        "email": invited_email,
        "first_login": True,
    }


def _now_utc():
    """Lazy import: keeps the top-level import surface terse."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc)


# ─── First-run / welcome ───────────────────────────────────────────────────

@router.post("/welcome/complete")
def welcome_complete(
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Clear the first_login flag. Called when the user clicks 'Enter platform'
    on /auth/welcome. Idempotent — repeated calls just keep first_login=false.

    Also returns default_landing so the frontend can route the user to
    the same place a regular signin would (/trader/overview for fresh
    users with no allocations yet, /manager/overview otherwise)."""
    cur.execute(
        "UPDATE user_mgmt.users SET first_login = FALSE WHERE user_id = %s::uuid",
        (user_id,),
    )
    return {
        "ok": True,
        "default_landing": _default_landing_for(cur, user_id),
    }


# ─── User-facing password change ────────────────────────────────────────────

@router.post("/change-password")
def change_password(
    body: ChangePasswordRequest,
    request: Request,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """User-initiated password change. Two paths:

      1. password_is_temporary = true → current_password NOT required
         (the temp was already verified at login; admin-issued temps
         are intentionally one-shot to drive change-on-first-use)
      2. password_is_temporary = false → current_password REQUIRED and
         must verify before the new password is accepted

    On success: hash + UPDATE, clear password_is_temporary, set
    password_set_at = now(), null out password_changed_by (self-change).
    Writes a password_changed_self audit row.
    Other devices' sessions are NOT revoked — distinct from
    admin-reset behavior, since the user themselves is opting in.
    """
    from ...services.admin_audit import _client_ip, log_admin_action

    cur.execute(
        """
        SELECT password_hash, password_is_temporary
        FROM user_mgmt.users WHERE user_id = %s::uuid
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row or not row["password_hash"]:
        raise HTTPException(status_code=400, detail="Password not set on this account")

    if not row["password_is_temporary"]:
        if not body.current_password:
            raise HTTPException(status_code=400, detail="Current password required")
        if not _verify_password(body.current_password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Current password incorrect")

    _validate_password_or_400(body.new_password)

    # Reject same-as-current — bcrypt.checkpw is the source of truth since
    # bcrypt.hashpw with a fresh salt produces a different ciphertext for
    # the same plaintext, so a string-equality check on the hash wouldn't
    # work. Skipped on the temporary-password path: the temp is admin-issued
    # and the user has no canonical "current" they're trying to keep.
    if not row["password_is_temporary"]:
        if _verify_password(body.new_password, row["password_hash"]):
            raise HTTPException(
                status_code=400,
                detail="New password must be different from current password",
            )

    new_hash = _hash_password(body.new_password)
    cur.execute(
        """
        UPDATE user_mgmt.users
           SET password_hash = %s,
               password_is_temporary = FALSE,
               password_changed_by = NULL,
               password_set_at = NOW(),
               failed_login_count = 0,
               locked_until = NULL
         WHERE user_id = %s::uuid
        """,
        (new_hash, user_id),
    )

    log_admin_action(
        cur,
        admin_user_id=user_id,
        action_type="password_changed_self",
        subject_user_id=user_id,
        metadata={"was_temporary": bool(row["password_is_temporary"])},
        ip=_client_ip(request),
    )

    log.info("User %s changed their own password", user_id)
    return {"ok": True}

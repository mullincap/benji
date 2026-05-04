"""
backend/app/api/routes/account.py
==================================
Self-service account profile endpoints.

Endpoints:
  GET   /api/account  — return current user's full profile (incl. inviter)
  PATCH /api/account  — update first_name, last_name, firm

Password change still lives on the auth router at POST /api/auth/change-password
since it predates this module and is invoked from both invite-acceptance
and self-service flows. The frontend account page calls it directly; we
do NOT add a duplicate /api/account/change-password — single source of
truth for the password mutation path.

The /api/auth/me endpoint stays lean (used by AuthProvider on every page
mount). /api/account is the heavier read with the invitations join, only
fetched when the user opens the account page.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from ...db import get_cursor
from ...services.admin_audit import _client_ip, log_admin_action
from .auth import get_current_user

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/account", tags=["account"])


# ─── Request models ─────────────────────────────────────────────────────────

class UpdateProfileRequest(BaseModel):
    first_name: str
    last_name: str
    # Optional — solo traders without a firm leave this blank. Empty/whitespace
    # strings are normalized to NULL on persist (mirrors PR #36 invite-accept
    # handling so the column stays clean).
    firm: str | None = None


# ─── Helpers ────────────────────────────────────────────────────────────────

def _normalize_firm(firm: str | None) -> str | None:
    if firm is None:
        return None
    stripped = firm.strip()
    return stripped if stripped else None


def _profile_response(cur, user_id: str) -> dict[str, Any]:
    """Build the GET /api/account response shape from the current DB state.

    Joins user_mgmt.invitations on accepted_user_id to surface the inviter's
    email. CLI-bootstrapped users have no invitation row → invited_by is null.
    Multiple invitation rows for the same user (edge case: re-invitation
    after revoke) resolve to the most recent accepted_at.
    """
    cur.execute(
        """
        SELECT u.user_id, u.email, u.first_name, u.last_name, u.firm,
               u.role, u.is_admin, u.created_at, u.last_login,
               u.password_is_temporary,
               (SELECT inviter.email
                  FROM user_mgmt.invitations inv
                  JOIN user_mgmt.users inviter
                    ON inviter.user_id = inv.inviter_user_id
                 WHERE inv.accepted_user_id = u.user_id
                 ORDER BY inv.accepted_at DESC NULLS LAST
                 LIMIT 1) AS invited_by_email
          FROM user_mgmt.users u
         WHERE u.user_id = %s::uuid
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "user_id": str(row["user_id"]),
        "email": row["email"],
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "firm": row["firm"],
        "role": row["role"],
        "is_admin": bool(row["is_admin"]),
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "last_login": row["last_login"].isoformat() if row["last_login"] else None,
        "invited_by": row["invited_by_email"],
        "password_is_temporary": bool(row["password_is_temporary"]),
    }


# ─── Endpoints ──────────────────────────────────────────────────────────────

@router.get("")
def get_account(
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Return the authenticated user's full profile + inviter context."""
    return _profile_response(cur, user_id)


@router.patch("")
def update_profile(
    body: UpdateProfileRequest,
    request: Request,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Update first_name, last_name, firm on the authenticated user.

    Server-enforced field allowlist via the Pydantic model — email, role,
    is_admin, user_id are NOT accepted (Pydantic drops unknown fields by
    default for BaseModel without extra='allow', so silently ignored).

    Writes a profile_updated_self audit row enumerating which fields
    changed (not the values — values aren't sensitive but tracking the
    set is enough for the audit trail).
    """
    first = (body.first_name or "").strip()
    last = (body.last_name or "").strip()
    firm = _normalize_firm(body.firm)

    if not first:
        raise HTTPException(status_code=400, detail="First name is required")
    if not last:
        raise HTTPException(status_code=400, detail="Last name is required")

    cur.execute(
        "SELECT first_name, last_name, firm FROM user_mgmt.users WHERE user_id = %s::uuid",
        (user_id,),
    )
    current = cur.fetchone()
    if not current:
        raise HTTPException(status_code=404, detail="User not found")

    fields_changed: list[str] = []
    if (current["first_name"] or "") != first:
        fields_changed.append("first_name")
    if (current["last_name"] or "") != last:
        fields_changed.append("last_name")
    if (current["firm"] or None) != firm:
        fields_changed.append("firm")

    if not fields_changed:
        return _profile_response(cur, user_id)

    cur.execute(
        """
        UPDATE user_mgmt.users
           SET first_name = %s,
               last_name = %s,
               firm = %s,
               updated_at = NOW()
         WHERE user_id = %s::uuid
        """,
        (first, last, firm, user_id),
    )

    log_admin_action(
        cur,
        admin_user_id=user_id,
        action_type="profile_updated_self",
        subject_user_id=user_id,
        metadata={"fields_changed": fields_changed},
        ip=_client_ip(request),
    )

    log.info("User %s updated profile fields: %s", user_id, fields_changed)
    return _profile_response(cur, user_id)

"""
backend/app/services/admin_audit.py
====================================
Admin gate + append-only audit log.

`require_admin` is the FastAPI dep used on every /api/admin/* endpoint.
It depends on `get_current_user` (from auth.py) so authentication runs
first; if `is_admin = false` it writes a `admin_login_attempt_denied`
audit row and raises 403.

`log_admin_action` is the canonical write path for the audit table.
Every state-mutating admin endpoint must call it before returning
success — non-negotiable per Phase 1 admin spec.

Transaction note: both write via the request cursor (`Depends(get_cursor)`),
so writes land in the same transaction as the route handler. Per the
get_cursor contract (db.py), HTTPException raises still commit, so the
audit row persists even on the 403-deny path.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from fastapi import Depends, HTTPException, Request, status

from ..db import get_cursor
from ..api.routes.auth import get_current_user

log = logging.getLogger(__name__)


# ─── Action types written by application code ──────────────────────────────
# Keep this list authoritative — anything not here is a typo to be caught
# in code review.
ACTION_TYPES = frozenset({
    "password_reset_admin",
    "password_changed_self",
    "invitation_issued",
    "invitation_revoked",
    "sessions_revoked",
    "admin_login_attempt_denied",
    "user_locked",
    "user_unlocked",
    "admin_granted",
    "admin_revoked",
})


def log_admin_action(
    cur,
    admin_user_id: str,
    action_type: str,
    subject_user_id: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    ip: Optional[str] = None,
) -> None:
    """Insert one row into user_mgmt.admin_actions.

    Caller passes the request cursor — the row commits with the rest of
    the request transaction. Do not open a separate connection.
    """
    if action_type not in ACTION_TYPES:
        # Catch typos at runtime — better than silently writing garbage.
        raise ValueError(f"unknown admin action_type: {action_type!r}")

    cur.execute(
        """
        INSERT INTO user_mgmt.admin_actions
            (admin_user_id, subject_user_id, action_type, action_metadata, ip_address)
        VALUES (%s::uuid, %s, %s, %s::jsonb, %s::inet)
        """,
        (
            admin_user_id,
            subject_user_id,
            action_type,
            json.dumps(metadata or {}),
            ip,
        ),
    )


def _client_ip(request: Request) -> Optional[str]:
    """Best-effort client IP. Honors X-Forwarded-For (nginx prepends the
    original client) and falls back to request.client.host."""
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        # First entry is the original client per RFC 7239 convention
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else None


def require_admin(
    request: Request,
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> str:
    """FastAPI dep. Returns user_id if admin. Writes a denial audit row
    and raises 403 otherwise.

    Auth happens first via get_current_user (raises 401 if not signed
    in). This dep only runs for authenticated users — non-admin denials
    are a different signal than missing-auth denials.
    """
    cur.execute(
        "SELECT is_admin FROM user_mgmt.users WHERE user_id = %s::uuid",
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        # Race: user got deleted between auth check and admin check.
        raise HTTPException(status_code=401, detail="User not found")

    if not row["is_admin"]:
        # Persist the denial. The get_cursor contract commits on
        # HTTPException, so this row survives.
        log_admin_action(
            cur,
            admin_user_id=user_id,
            action_type="admin_login_attempt_denied",
            metadata={"path": str(request.url.path)},
            ip=_client_ip(request),
        )
        log.warning(
            "Admin access denied: user=%s path=%s ip=%s",
            user_id, request.url.path, _client_ip(request),
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )

    return user_id

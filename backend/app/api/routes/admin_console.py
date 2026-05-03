"""
backend/app/api/routes/admin_console.py
=========================================
Admin Console API — per-user admin module shipped in admin-phase-1.

Mounted at /api/admin/*. Routes here cover user management, invitations,
and the audit log; gating is by `is_admin = true` on the user row,
verified via the Phase 1a session cookie.

NAMING NOTE:
  This file is intentionally distinct from `admin.py` in the same
  directory. `admin.py` is the LEGACY passphrase-based admin router
  used by compiler/indexer/manager admin pages — it uses an
  `admin_session` cookie and a shared `ADMIN_PASSPHRASE` secret. Both
  routers mount at /api/admin/*, but their routes do not overlap:

    /api/admin/login,  /logout,  /whoami       → admin.py (passphrase)
    /api/admin/users,  /invitations,  /audit   → this file (user-based)

  The two `require_admin` functions live in different modules:
    backend/app/api/routes/admin.py        — passphrase
    backend/app/services/admin_audit.py    — user-based (used here)

  Importers must be explicit about which one they want.

Transaction note:
  All mutating endpoints write the audit row as the LAST cursor.execute()
  before returning. The Phase 1a get_cursor contract commits on
  HTTPException too, so audit rows persist even when a route raises 4xx
  after a partial mutation. Do NOT add manual conn.commit() calls.

last_ip caveat:
  The spec asks for the user's last IP on the detail page. user_sessions
  doesn't store an IP column today; surfacing this would need a
  migration 024 + a write on every login. For Phase 1 commit 2 we
  return last_ip = null and TODO it for a follow-up commit.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from ...db import get_cursor
from ...services.admin_audit import _client_ip, log_admin_action, require_admin
from ...services.temp_password import generate_temp_password
from .auth import _hash_password

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin-console"])


# ─── Helpers ───────────────────────────────────────────────────────────────

def _user_status(row: dict) -> str:
    """Compute the status pill string from a user row + computed fields.

    Precedence (high to low): admin > locked > pending > idle > no_activity > active.

    Frontend expects one of: 'admin', 'locked', 'pending', 'idle',
    'no_activity', 'active'.
    """
    if row.get("is_admin"):
        return "admin"
    if row.get("locked_until_active"):
        return "locked"
    if row.get("last_login") is None:
        return "pending"
    if row.get("days_since_login") is not None and row["days_since_login"] > 7:
        return "idle"
    if row.get("allocations_count", 0) == 0:
        return "no_activity"
    return "active"


def _format_user_summary(row: dict) -> dict[str, Any]:
    """Shape a user list row for the API response."""
    return {
        "user_id": str(row["user_id"]),
        "email": row["email"],
        "first_name": row["first_name"],
        "last_name": row["last_name"],
        "firm": row["firm"],
        "role": row["role"],
        "is_admin": bool(row["is_admin"]),
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "last_login": row["last_login"].isoformat() if row["last_login"] else None,
        "locked_until": row["locked_until"].isoformat() if row["locked_until"] else None,
        "allocations_count": int(row["allocations_count"] or 0),
        "capital_deployed_usd": float(row["capital_deployed_usd"] or 0),
        "status": _user_status(row),
    }


def _ensure_user_exists(cur, user_id: str) -> dict:
    """Fetch the user row or 404."""
    cur.execute(
        """
        SELECT user_id, email, first_name, last_name, firm, role,
               is_admin, is_active, email_verified,
               password_is_temporary, password_set_at,
               created_at, updated_at, last_login,
               locked_until, failed_login_count
        FROM user_mgmt.users WHERE user_id = %s::uuid
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return row


# ─── Users — list, detail, tabs ───────────────────────────────────────────

@router.get("/users")
def list_users(
    search: Optional[str] = Query(None, description="email/name/firm substring"),
    status: Optional[str] = Query(None, description="active|locked|idle|pending|no_activity|admin"),
    sort: str = Query("last_login", description="last_login|joined|email"),
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Users list with computed allocations/capital stats and a derived
    status. Always returns the full set in one shot — Phase 1 user counts
    are <100, no pagination needed yet."""
    sort_clause = {
        "last_login": "u.last_login DESC NULLS LAST",
        "joined":     "u.created_at DESC",
        "email":      "u.email ASC",
    }.get(sort, "u.last_login DESC NULLS LAST")

    sql = f"""
        SELECT
          u.user_id, u.email, u.first_name, u.last_name, u.firm, u.role,
          u.is_admin, u.is_active, u.created_at, u.last_login, u.locked_until,
          (u.locked_until IS NOT NULL AND u.locked_until > NOW()) AS locked_until_active,
          CASE WHEN u.last_login IS NULL THEN NULL
               ELSE EXTRACT(EPOCH FROM (NOW() - u.last_login)) / 86400.0
          END AS days_since_login,
          COALESCE(a.alloc_count, 0)   AS allocations_count,
          COALESCE(a.capital, 0)       AS capital_deployed_usd
        FROM user_mgmt.users u
        LEFT JOIN (
          SELECT user_id, count(*) AS alloc_count, sum(capital_usd) AS capital
          FROM user_mgmt.allocations
          WHERE status = 'active'
          GROUP BY user_id
        ) a USING (user_id)
        WHERE
          (%(q)s IS NULL
            OR u.email      ILIKE '%%' || %(q)s || '%%'
            OR COALESCE(u.first_name, '') ILIKE '%%' || %(q)s || '%%'
            OR COALESCE(u.last_name, '')  ILIKE '%%' || %(q)s || '%%'
            OR COALESCE(u.firm, '')       ILIKE '%%' || %(q)s || '%%')
        ORDER BY {sort_clause}
    """
    cur.execute(sql, {"q": search})
    rows = cur.fetchall()

    summaries = [_format_user_summary(r) for r in rows]

    if status:
        summaries = [s for s in summaries if s["status"] == status]

    # Aggregates for the page-head KPI strip — computed AFTER status
    # filter so the strip reflects whatever is currently shown.
    total_users = len(summaries)
    active_30d = sum(1 for s in summaries
                     if s["last_login"]
                     and (s["status"] in ("active", "admin", "no_activity")))
    pending_count = sum(1 for s in summaries if s["status"] == "pending")
    locked_count = sum(1 for s in summaries if s["status"] == "locked")

    return {
        "users": summaries,
        "total": total_users,
        "stats": {
            "active_30d": active_30d,
            "pending": pending_count,
            "locked": locked_count,
            "allocations_total": sum(s["allocations_count"] for s in summaries),
            "capital_deployed_total_usd": sum(s["capital_deployed_usd"] for s in summaries),
        },
    }


@router.get("/users/{user_id}")
def get_user_detail(
    user_id: str,
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Full user detail for the admin user page. Includes counts, the
    most-recent session timestamp, and identity meta. last_ip is null —
    user_sessions doesn't carry IP today (TODO migration 024)."""
    user_row = _ensure_user_exists(cur, user_id)

    # Sessions count (active only) + most-recent login session
    cur.execute(
        """
        SELECT count(*) AS sessions_active,
               max(created_at) AS last_session_at
        FROM user_mgmt.user_sessions
        WHERE user_id = %s::uuid AND expires_at > NOW()
        """,
        (user_id,),
    )
    sess = cur.fetchone()

    # Allocation summary
    cur.execute(
        """
        SELECT count(*) FILTER (WHERE status = 'active') AS active_count,
               count(*) AS total_count,
               COALESCE(sum(capital_usd) FILTER (WHERE status = 'active'), 0) AS capital_active
        FROM user_mgmt.allocations
        WHERE user_id = %s::uuid
        """,
        (user_id,),
    )
    alloc_summary = cur.fetchone()

    # Connections count
    cur.execute(
        "SELECT count(*) AS n FROM user_mgmt.exchange_connections WHERE user_id = %s::uuid",
        (user_id,),
    )
    conn_count = cur.fetchone()["n"]

    # Resolve password_changed_by → email if set
    password_changed_by_email = None
    cur.execute(
        "SELECT password_changed_by FROM user_mgmt.users WHERE user_id = %s::uuid",
        (user_id,),
    )
    pcb_row = cur.fetchone()
    if pcb_row and pcb_row["password_changed_by"]:
        cur.execute(
            "SELECT email FROM user_mgmt.users WHERE user_id = %s::uuid",
            (str(pcb_row["password_changed_by"]),),
        )
        admin_row = cur.fetchone()
        if admin_row:
            password_changed_by_email = admin_row["email"]

    return {
        "user_id": str(user_row["user_id"]),
        "email": user_row["email"],
        "first_name": user_row["first_name"],
        "last_name": user_row["last_name"],
        "firm": user_row["firm"],
        "role": user_row["role"],
        "is_admin": bool(user_row["is_admin"]),
        "is_active": bool(user_row["is_active"]),
        "email_verified": bool(user_row["email_verified"]),
        "password_is_temporary": bool(user_row["password_is_temporary"]),
        "password_set_at": user_row["password_set_at"].isoformat() if user_row["password_set_at"] else None,
        "password_changed_by_email": password_changed_by_email,
        "created_at": user_row["created_at"].isoformat() if user_row["created_at"] else None,
        "updated_at": user_row["updated_at"].isoformat() if user_row["updated_at"] else None,
        "last_login": user_row["last_login"].isoformat() if user_row["last_login"] else None,
        "last_ip": None,  # TODO: requires user_sessions.ip column (migration 024)
        "locked_until": user_row["locked_until"].isoformat() if user_row["locked_until"] else None,
        "failed_login_count": int(user_row["failed_login_count"] or 0),
        "sessions_active": int(sess["sessions_active"] or 0),
        "last_session_at": sess["last_session_at"].isoformat() if sess["last_session_at"] else None,
        "allocations_active": int(alloc_summary["active_count"] or 0),
        "allocations_total": int(alloc_summary["total_count"] or 0),
        "capital_active_usd": float(alloc_summary["capital_active"] or 0),
        "connections_total": int(conn_count or 0),
    }


@router.get("/users/{user_id}/allocations")
def get_user_allocations(
    user_id: str,
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """All allocations for the user, with current equity from the latest
    exchange_snapshots row for each connection."""
    _ensure_user_exists(cur, user_id)
    cur.execute(
        """
        SELECT
          a.allocation_id, a.status, a.created_at, a.closed_at,
          a.capital_usd, a.compounding_mode, a.runtime_state,
          ec.exchange, ec.label AS connection_label,
          sv.version_label, s.name AS strategy_name,
          (SELECT total_equity_usd
             FROM user_mgmt.exchange_snapshots
             WHERE connection_id = a.connection_id
             ORDER BY snapshot_at DESC
             LIMIT 1) AS current_equity_usd,
          (SELECT snapshot_at
             FROM user_mgmt.exchange_snapshots
             WHERE connection_id = a.connection_id
             ORDER BY snapshot_at DESC
             LIMIT 1) AS equity_at
        FROM user_mgmt.allocations a
        JOIN user_mgmt.exchange_connections ec USING (connection_id)
        JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
        LEFT JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
        WHERE a.user_id = %s::uuid
        ORDER BY a.created_at DESC
        """,
        (user_id,),
    )
    rows = cur.fetchall()

    out = []
    for r in rows:
        capital = float(r["capital_usd"] or 0)
        equity = float(r["current_equity_usd"]) if r["current_equity_usd"] is not None else None
        return_pct = None
        if equity is not None and capital > 0:
            return_pct = (equity - capital) / capital * 100.0

        out.append({
            "allocation_id": str(r["allocation_id"]),
            "status": r["status"],
            "strategy_name": r["strategy_name"],
            "version_label": r["version_label"],
            "exchange": r["exchange"],
            "connection_label": r["connection_label"],
            "capital_usd": capital,
            "current_equity_usd": equity,
            "return_pct": return_pct,
            "equity_at": r["equity_at"].isoformat() if r["equity_at"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "closed_at": r["closed_at"].isoformat() if r["closed_at"] else None,
            "compounding_mode": r["compounding_mode"],
            "runtime_phase": (r["runtime_state"] or {}).get("phase"),
        })
    return {"allocations": out}


@router.get("/users/{user_id}/capital-events")
def get_user_capital_events(
    user_id: str,
    limit: int = Query(100, ge=1, le=500),
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Capital deposits/withdrawals across all of the user's allocations."""
    _ensure_user_exists(cur, user_id)
    cur.execute(
        """
        SELECT ce.event_id, ce.event_at, ce.kind, ce.amount_usd,
               ce.source, ce.notes, ce.exchange_event_id,
               ce.allocation_id, ce.connection_id,
               ec.exchange, ec.label AS connection_label
        FROM user_mgmt.allocation_capital_events ce
        LEFT JOIN user_mgmt.exchange_connections ec USING (connection_id)
        LEFT JOIN user_mgmt.allocations a ON a.allocation_id = ce.allocation_id
        WHERE (a.user_id = %s::uuid OR ec.user_id = %s::uuid)
          AND ce.deleted_at IS NULL
        ORDER BY ce.event_at DESC
        LIMIT %s
        """,
        (user_id, user_id, limit),
    )
    rows = cur.fetchall()
    out = [{
        "event_id": str(r["event_id"]),
        "event_at": r["event_at"].isoformat() if r["event_at"] else None,
        "kind": r["kind"],
        "amount_usd": float(r["amount_usd"] or 0),
        "source": r["source"],
        "notes": r["notes"],
        "exchange_event_id": r["exchange_event_id"],
        "allocation_id": str(r["allocation_id"]) if r["allocation_id"] else None,
        "exchange": r["exchange"],
        "connection_label": r["connection_label"],
    } for r in rows]
    return {"events": out}


@router.get("/users/{user_id}/sessions")
def get_user_sessions(
    user_id: str,
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """All active sessions for the user. user_sessions doesn't carry IP
    or user-agent today, so we surface what's available (created_at +
    expires_at). Token values themselves never leave the DB."""
    _ensure_user_exists(cur, user_id)
    cur.execute(
        """
        SELECT created_at, expires_at,
               expires_at > NOW() AS is_active
        FROM user_mgmt.user_sessions
        WHERE user_id = %s::uuid
        ORDER BY created_at DESC
        """,
        (user_id,),
    )
    rows = cur.fetchall()
    return {
        "sessions": [{
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None,
            "is_active": bool(r["is_active"]),
        } for r in rows]
    }


@router.get("/users/{user_id}/connections")
def get_user_connections(
    user_id: str,
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Exchange connection metadata only. Encrypted credentials never
    leave the DB."""
    _ensure_user_exists(cur, user_id)
    cur.execute(
        """
        SELECT connection_id, exchange, label, testnet, status,
               last_validated_at, last_error_at, last_error_msg,
               principal_baseline_usd, principal_anchor_at,
               created_at, updated_at
        FROM user_mgmt.exchange_connections
        WHERE user_id = %s::uuid
        ORDER BY created_at DESC
        """,
        (user_id,),
    )
    rows = cur.fetchall()
    return {
        "connections": [{
            "connection_id": str(r["connection_id"]),
            "exchange": r["exchange"],
            "label": r["label"],
            "testnet": bool(r["testnet"]),
            "status": r["status"],
            "last_validated_at": r["last_validated_at"].isoformat() if r["last_validated_at"] else None,
            "last_error_at": r["last_error_at"].isoformat() if r["last_error_at"] else None,
            "last_error_msg": r["last_error_msg"],
            "principal_baseline_usd": float(r["principal_baseline_usd"]) if r["principal_baseline_usd"] is not None else None,
            "principal_anchor_at": r["principal_anchor_at"].isoformat() if r["principal_anchor_at"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
        } for r in rows]
    }


# ─── Users — mutating actions ──────────────────────────────────────────────

@router.post("/users/{user_id}/reset-password")
def admin_reset_password(
    user_id: str,
    request: Request,
    admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Mint a temp password for the target user, hash + store, mark
    password_is_temporary=true, revoke all of their sessions, and write
    one audit row. Atomic via the request cursor — get_cursor commits
    the whole thing on return.

    Returns the temp password ONCE in plaintext. The admin sends it
    out-of-band (Signal, encrypted email). Subsequent reads of this
    user must NOT return it again.

    Side-effect: clears any active lockout (failed_login_count=0,
    locked_until=NULL). An admin reset implicitly trusts the operator's
    judgment on who should be allowed back in.
    """
    target = _ensure_user_exists(cur, user_id)

    temp_pw = generate_temp_password()
    pw_hash = _hash_password(temp_pw)

    cur.execute(
        """
        UPDATE user_mgmt.users
           SET password_hash = %s,
               password_is_temporary = TRUE,
               password_changed_by = %s::uuid,
               password_set_at = NOW(),
               failed_login_count = 0,
               locked_until = NULL
         WHERE user_id = %s::uuid
        """,
        (pw_hash, admin_id, user_id),
    )

    # Revoke all of the target's sessions atomically
    cur.execute(
        "DELETE FROM user_mgmt.user_sessions WHERE user_id = %s::uuid",
        (user_id,),
    )
    revoked = cur.rowcount

    log_admin_action(
        cur,
        admin_user_id=admin_id,
        action_type="password_reset_admin",
        subject_user_id=user_id,
        metadata={
            "target_email": target["email"],
            "sessions_revoked": revoked,
        },
        ip=_client_ip(request),
    )

    log.info(
        "Admin %s reset password for user %s (%s); revoked %s sessions",
        admin_id, user_id, target["email"], revoked,
    )

    return {
        "ok": True,
        "temp_password": temp_pw,
        "sessions_revoked": revoked,
    }


@router.post("/users/{user_id}/revoke-sessions")
def admin_revoke_sessions(
    user_id: str,
    request: Request,
    admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Delete all of the target user's session rows. Their next request
    on every device will 401."""
    target = _ensure_user_exists(cur, user_id)
    cur.execute(
        "DELETE FROM user_mgmt.user_sessions WHERE user_id = %s::uuid",
        (user_id,),
    )
    revoked = cur.rowcount
    log_admin_action(
        cur,
        admin_user_id=admin_id,
        action_type="sessions_revoked",
        subject_user_id=user_id,
        metadata={"target_email": target["email"], "sessions_revoked": revoked},
        ip=_client_ip(request),
    )
    return {"ok": True, "sessions_revoked": revoked}


class _LockBody:
    """Pydantic-free body shape — we accept duration_hours via Body(...)."""


@router.post("/users/{user_id}/lock")
def admin_lock_user(
    user_id: str,
    request: Request,
    duration_hours: int = Body(24, embed=True, ge=1, le=720),
    admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Set locked_until = now() + duration_hours. Default 24h, max 30d.
    Does NOT revoke sessions — they expire naturally; rapid revocation
    use the dedicated endpoint."""
    target = _ensure_user_exists(cur, user_id)
    cur.execute(
        """
        UPDATE user_mgmt.users
           SET locked_until = NOW() + (%s || ' hours')::interval
         WHERE user_id = %s::uuid
         RETURNING locked_until
        """,
        (duration_hours, user_id),
    )
    row = cur.fetchone()
    log_admin_action(
        cur,
        admin_user_id=admin_id,
        action_type="user_locked",
        subject_user_id=user_id,
        metadata={"target_email": target["email"], "duration_hours": duration_hours},
        ip=_client_ip(request),
    )
    return {
        "ok": True,
        "locked_until": row["locked_until"].isoformat() if row else None,
    }


@router.post("/users/{user_id}/unlock")
def admin_unlock_user(
    user_id: str,
    request: Request,
    admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Clear locked_until + reset failed_login_count to 0."""
    target = _ensure_user_exists(cur, user_id)
    cur.execute(
        """
        UPDATE user_mgmt.users
           SET locked_until = NULL, failed_login_count = 0
         WHERE user_id = %s::uuid
        """,
        (user_id,),
    )
    log_admin_action(
        cur,
        admin_user_id=admin_id,
        action_type="user_unlocked",
        subject_user_id=user_id,
        metadata={"target_email": target["email"]},
        ip=_client_ip(request),
    )
    return {"ok": True}


# ─── Invitations ───────────────────────────────────────────────────────────
# Mirrors the CLI flow shipped in Phase 1a (backend/app/cli/issue_invite.py)
# but exposed as HTTP endpoints for the admin UI. Same token convention:
# secrets.token_urlsafe(32) for the URL value, sha256 hex digest stored.

def _hash_invite_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _now_utc():
    return datetime.now(timezone.utc)


class IssueInviteRequest(BaseModel):
    email: str
    # firm is optional — matches the acceptance form's symmetric change
    # (PR #36). Currently dead code on this endpoint (the inviter_firm
    # stored on the invitation row comes from the calling admin's own
    # user record, not from body.firm); see the issue_invitation
    # handler for context.
    firm: str | None = None
    role: str
    expires_in_days: int = Field(default=7, ge=1, le=30)


def _format_invitation(row: dict) -> dict[str, Any]:
    """Common shape for invitation list rows. Token value never exposed —
    plaintext only ever exists in the issue_invitation response."""
    accepted_at = row["accepted_at"]
    expires_at = row["expires_at"]
    now = _now_utc()
    if accepted_at is not None:
        status = "accepted"
    elif expires_at <= now:
        status = "expired"
    elif (expires_at - now) < timedelta(hours=24):
        status = "expiring"
    else:
        status = "pending"
    return {
        "invitation_id": str(row["invitation_id"]),
        "invited_email": row["invited_email"],
        "inviter_name": row["inviter_name"],
        "inviter_firm": row["inviter_firm"],
        "inviter_email": row.get("inviter_email"),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "accepted_at": accepted_at.isoformat() if accepted_at else None,
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "status": status,
    }


@router.get("/invitations")
def list_invitations(
    status: Optional[str] = Query(None, description="pending|expiring|accepted|expired"),
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Lists invitations with computed status (pending|expiring|accepted|expired).

    `expiring` is a sub-state of pending: same row, but expires_at is
    within 24h. The frontend filter pills surface 'pending' as the
    union of pending + expiring; 'expiring' is also queryable directly
    for the at-risk view."""
    cur.execute(
        """
        SELECT i.invitation_id, i.invited_email, i.inviter_user_id,
               i.inviter_name, i.inviter_firm,
               i.created_at, i.expires_at, i.accepted_at,
               u.email AS inviter_email
        FROM user_mgmt.invitations i
        LEFT JOIN user_mgmt.users u ON u.user_id = i.inviter_user_id
        ORDER BY i.created_at DESC
        """
    )
    rows = cur.fetchall()
    items = [_format_invitation(r) for r in rows]
    if status:
        # 'pending' filter folds in 'expiring' rows since they're the
        # same admin-action set (still revocable, still copyable).
        if status == "pending":
            items = [it for it in items if it["status"] in ("pending", "expiring")]
        else:
            items = [it for it in items if it["status"] == status]

    stats = {
        "pending": sum(1 for it in items if it["status"] in ("pending", "expiring")),
        "expiring": sum(1 for it in items if it["status"] == "expiring"),
        "accepted": sum(1 for it in items if it["status"] == "accepted"),
        "expired": sum(1 for it in items if it["status"] == "expired"),
    }
    total_decisions = stats["accepted"] + stats["expired"]
    acceptance_rate = (stats["accepted"] / total_decisions) if total_decisions > 0 else None

    return {
        "invitations": items,
        "total": len(items),
        "stats": {**stats, "acceptance_rate": acceptance_rate},
    }


@router.post("/invitations")
def issue_invitation(
    body: IssueInviteRequest,
    request: Request,
    admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Mint a fresh invitation. Returns the invite URL ONCE — token
    plaintext is never persisted, only the SHA-256 hash. Admin sends
    the URL out-of-band.

    Validates: invitee email isn't already a user, no pending invite
    for that email already exists. Inviter context (name + firm) comes
    from the calling admin's own user row."""
    invited_email = body.email.strip().lower()
    if not invited_email or "@" not in invited_email:
        raise HTTPException(status_code=400, detail="Valid email required")
    # firm dropped from required check — it's optional on the admin form
    # and currently dead code anyway (inviter_firm uses admin_row["firm"]
    # below, not body.firm). Role still required since the form always
    # sends a non-empty value from its dropdown.
    if not body.role.strip():
        raise HTTPException(status_code=400, detail="Role required")

    # Ensure no existing user with that email
    cur.execute("SELECT user_id FROM user_mgmt.users WHERE lower(email) = %s", (invited_email,))
    if cur.fetchone():
        raise HTTPException(status_code=409, detail="A user with that email already exists")

    # Reject duplicate pending invite (matches CLI behavior)
    cur.execute(
        """
        SELECT invitation_id FROM user_mgmt.invitations
         WHERE lower(invited_email) = %s
           AND accepted_at IS NULL
           AND expires_at > NOW()
        """,
        (invited_email,),
    )
    if cur.fetchone():
        raise HTTPException(status_code=409, detail="A pending invitation for that email already exists")

    # Resolve inviter context. Falls back to email if name/firm fields
    # are NULL on the admin user row (e.g. legacy admins predating the
    # accept-invite flow that populates first_name etc.).
    cur.execute(
        "SELECT email, first_name, last_name, firm FROM user_mgmt.users WHERE user_id = %s::uuid",
        (admin_id,),
    )
    admin_row = cur.fetchone()
    inviter_name = (
        " ".join(filter(None, [admin_row["first_name"], admin_row["last_name"]]))
        or admin_row["email"]
    )
    inviter_firm = (admin_row["firm"] or "Mullincap").strip()

    # Mint
    token = secrets.token_urlsafe(32)
    token_hash = _hash_invite_token(token)
    expires_at = _now_utc() + timedelta(days=body.expires_in_days)

    cur.execute(
        """
        INSERT INTO user_mgmt.invitations
            (token_hash, invited_email, inviter_user_id,
             inviter_name, inviter_firm, expires_at)
        VALUES (%s, %s, %s::uuid, %s, %s, %s)
        RETURNING invitation_id
        """,
        (token_hash, invited_email, admin_id, inviter_name, inviter_firm, expires_at),
    )
    invitation_id = str(cur.fetchone()["invitation_id"])

    # Audit
    log_admin_action(
        cur,
        admin_user_id=admin_id,
        action_type="invitation_issued",
        metadata={
            "invitation_id": invitation_id,
            "invited_email": invited_email,
            "firm": body.firm.strip(),
            "role": body.role.strip(),
            "expires_in_days": body.expires_in_days,
        },
        ip=_client_ip(request),
    )

    # Build the URL using request scheme + host so the admin gets a
    # link that works on whichever environment they're on. Falls back
    # to mullincap.com if forwarding headers are missing.
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or "mullincap.com"
    invite_url = f"{proto}://{host}/auth/invite?token={token}"

    log.info("Admin %s issued invite %s for %s", admin_id, invitation_id, invited_email)

    return {
        "ok": True,
        "invitation_id": invitation_id,
        "invite_url": invite_url,
        "expires_at": expires_at.isoformat(),
    }


@router.get("/audit")
def list_audit_events(
    action_type: Optional[str] = Query(None),
    subject_email: Optional[str] = Query(None),
    actor_email: Optional[str] = Query(None),
    since: Optional[str] = Query(None, description="ISO timestamp lower bound"),
    limit: int = Query(100, ge=1, le=500),
    _admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Read the audit log. Supports server-side filter by action_type,
    actor email, subject email, and a `since` lower bound. Default 100
    rows, max 500 — admin views are operator workflows, not bulk export."""
    where: list[str] = []
    params: list[Any] = []

    if action_type:
        where.append("a.action_type = %s")
        params.append(action_type)
    if since:
        where.append("a.created_at >= %s")
        params.append(since)
    if actor_email:
        where.append("au.email ILIKE %s")
        params.append(f"%{actor_email}%")
    if subject_email:
        where.append("(su.email ILIKE %s OR a.action_metadata->>'target_email' ILIKE %s OR a.action_metadata->>'invited_email' ILIKE %s)")
        params.extend([f"%{subject_email}%"] * 3)

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT a.action_id, a.admin_user_id, a.subject_user_id,
               a.action_type, a.action_metadata, a.ip_address, a.created_at,
               au.email AS actor_email,
               su.email AS subject_email
        FROM user_mgmt.admin_actions a
        LEFT JOIN user_mgmt.users au ON au.user_id = a.admin_user_id
        LEFT JOIN user_mgmt.users su ON su.user_id = a.subject_user_id
        {where_clause}
        ORDER BY a.created_at DESC
        LIMIT %s
    """
    cur.execute(sql, [*params, limit])
    rows = cur.fetchall()

    out = []
    for r in rows:
        meta = r["action_metadata"] or {}
        # Subject prefers an actual user join; falls back to metadata
        # fields for actions that name a target by string (invitation
        # issuance, denied admin attempts).
        subject = (
            r["subject_email"]
            or meta.get("target_email")
            or meta.get("invited_email")
            or None
        )
        out.append({
            "action_id": str(r["action_id"]),
            "action_type": r["action_type"],
            "actor_email": r["actor_email"],
            "subject_email": subject,
            "metadata": meta,
            "ip_address": str(r["ip_address"]) if r["ip_address"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })

    return {"events": out, "total": len(out)}


@router.post("/invitations/{invitation_id}/revoke")
def revoke_invitation(
    invitation_id: str,
    request: Request,
    admin_id: str = Depends(require_admin),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Force the invitation to expire immediately. Used when the
    invited person bounces, the email was wrong, or we simply changed
    our mind. Already-accepted invitations are 409s — they're settled
    history."""
    cur.execute(
        """
        SELECT invitation_id, invited_email, accepted_at, expires_at
        FROM user_mgmt.invitations
        WHERE invitation_id = %s::uuid
        """,
        (invitation_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")
    if row["accepted_at"] is not None:
        raise HTTPException(status_code=409, detail="Cannot revoke an already-accepted invitation")

    cur.execute(
        "UPDATE user_mgmt.invitations SET expires_at = NOW() WHERE invitation_id = %s::uuid",
        (invitation_id,),
    )
    log_admin_action(
        cur,
        admin_user_id=admin_id,
        action_type="invitation_revoked",
        metadata={
            "invitation_id": invitation_id,
            "invited_email": row["invited_email"],
        },
        ip=_client_ip(request),
    )
    return {"ok": True}

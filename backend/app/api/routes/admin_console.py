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

import logging
from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

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

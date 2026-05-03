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
  routers mount at `/api/admin/*` but their routes do not overlap:

    /api/admin/login,  /logout,  /whoami       → admin.py (passphrase)
    /api/admin/users,  /invitations,  /audit   → this file (user-based)

  The two `require_admin` functions live in different modules:
    backend/app/api/routes/admin.py        — passphrase
    backend/app/services/admin_audit.py    — user-based (used here)

  Importers must be explicit about which one they want.

Phase 1 Commit 1 ships only a stub /users endpoint to validate router
mount + admin gate behavior. Real endpoints land in subsequent commits.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends

from ...services.admin_audit import require_admin

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin-console"])


@router.get("/users")
def list_users(_admin_id: str = Depends(require_admin)) -> dict[str, Any]:
    """Stub — returns empty list. Real implementation lands in commit 2."""
    return {"users": [], "total": 0}

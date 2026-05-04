"""
backend/app/api/routes/onboarding.py
=====================================
Onboarding state for the 1-step get-started flow.

Surfaces three booleans + a snapshot of the user's selected strategy
(if any) so the frontend can decide:
  - which nudge banner to render on /trader/overview
  - whether to redirect from /trader/overview to /trader/get-started
    (or vice versa)

The state is computed live on every call (no caching) — small joins,
fast queries, and onboarding state is too transactional to risk
staleness. If this becomes a hot path later we can add a bounded
cache; not warranted in v1.

Mounted at /api/onboarding/* and gated by get_current_user (Phase 1a
session cookie).

Mutations:
  POST /api/onboarding/select-strategy  — set the user's
        selected_strategy_id. Validates the strategy_version_id
        exists and is_active=true.
  POST /api/onboarding/clear-strategy   — clear the selection.
        Also called automatically by allocator.py:create_allocation
        on a successful allocation insert (atomic with the allocation
        row), so the "you have a strategy selected" nudge stops
        showing the moment the user actually allocates.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException

from ...db import get_cursor
from .auth import get_current_user

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/onboarding", tags=["onboarding"])


# ─── State ─────────────────────────────────────────────────────────────────

@router.get("/state")
def get_onboarding_state(
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Returns the user's onboarding progression. Lightweight enough to
    poll-on-mount from the dashboard without caching."""
    # has_exchange: any non-revoked exchange_connections row for this user
    cur.execute(
        """
        SELECT EXISTS(
            SELECT 1 FROM user_mgmt.exchange_connections
             WHERE user_id = %s::uuid AND status != 'revoked'
        ) AS has_exchange
        """,
        (user_id,),
    )
    has_exchange = bool(cur.fetchone()["has_exchange"])

    # has_active_allocation: any active allocation row for this user
    cur.execute(
        """
        SELECT EXISTS(
            SELECT 1 FROM user_mgmt.allocations
             WHERE user_id = %s::uuid AND status = 'active'
        ) AS has_active_allocation
        """,
        (user_id,),
    )
    has_active_allocation = bool(cur.fetchone()["has_active_allocation"])

    # selected_strategy + denormalized slug/display_name + sharpe extracted
    # from current_metrics jsonb. NULL-safe so a user with no selection
    # just gets nulls back.
    #
    # `audit.strategies.name` is the catalog slug (e.g. "alts_main") used
    # for /trader/strategies/<slug> deep links. `audit.strategies.display_name`
    # is the human-readable name (e.g. "ALTS MAIN") for banner copy.
    # Surfacing both under dedicated API field names keeps slug/display
    # responsibilities cleanly separated and matches what the catalog
    # cards already use elsewhere.
    cur.execute(
        """
        SELECT
            u.selected_strategy_id,
            sv.version_label,
            s.name AS strategy_slug,
            s.display_name AS strategy_display_name,
            (sv.current_metrics->>'sharpe')::numeric AS sharpe
        FROM user_mgmt.users u
        LEFT JOIN audit.strategy_versions sv
               ON sv.strategy_version_id = u.selected_strategy_id
        LEFT JOIN audit.strategies s
               ON s.strategy_id = sv.strategy_id
        WHERE u.user_id = %s::uuid
        """,
        (user_id,),
    )
    sel = cur.fetchone()
    selected_strategy_id = str(sel["selected_strategy_id"]) if sel and sel["selected_strategy_id"] else None
    has_selected_strategy = selected_strategy_id is not None
    strategy_slug = sel["strategy_slug"] if sel else None
    strategy_display_name = (sel["strategy_display_name"] if sel else None) or strategy_slug

    # When the user has an active allocation, fetch its strategy name for
    # the "you're live" banner copy. Distinct from selected_strategy_*
    # fields above — those are cleared the moment an allocation is
    # created (atomically with the insert in allocator.py:create_allocation),
    # so by the time the live banner needs to render, selected_strategy_id
    # is already null. The live banner reads from the allocation directly.
    # Most-recent active row by created_at — covers the multi-allocation
    # case (banner mentions the latest one, which is usually the most
    # relevant context for a "you just deployed" celebration).
    active_strategy_name: str | None = None
    if has_active_allocation:
        cur.execute(
            """
            SELECT s.display_name AS strategy_display_name,
                   s.name         AS strategy_slug
              FROM user_mgmt.allocations a
              JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
              JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
             WHERE a.user_id = %s::uuid AND a.status = 'active'
             ORDER BY a.created_at DESC
             LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()
        # Prefer display_name (human-readable like "ALTS MAIN"); fall back
        # to slug if display_name is null on legacy rows so the banner
        # still has something meaningful to show.
        if row:
            active_strategy_name = row["strategy_display_name"] or row["strategy_slug"]

    return {
        "has_exchange": has_exchange,
        "has_selected_strategy": has_selected_strategy,
        "selected_strategy_id": selected_strategy_id,
        "selected_strategy_slug": strategy_slug,
        "selected_strategy_name": strategy_display_name,
        "selected_strategy_version": sel["version_label"] if sel else None,
        "selected_strategy_sharpe": float(sel["sharpe"]) if sel and sel["sharpe"] is not None else None,
        "has_active_allocation": has_active_allocation,
        "active_allocation_strategy_name": active_strategy_name,
    }


# ─── Mutations ─────────────────────────────────────────────────────────────

@router.post("/select-strategy")
def select_strategy(
    strategy_version_id: str = Body(..., embed=True),
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Set the user's selected_strategy_id. Idempotent — overwriting
    a previous selection is fine. Validates the strategy exists and
    is the canonical (is_active) version of its strategy_id; rejects
    non-canonical or unknown IDs with 400."""
    cur.execute(
        """
        SELECT strategy_version_id, is_active
        FROM audit.strategy_versions
        WHERE strategy_version_id = %s::uuid
        """,
        (strategy_version_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=400, detail="Unknown strategy_version_id")
    if not row["is_active"]:
        # Allow non-canonical selections too — operators may want to
        # pin to a specific historical version. Loosen this if Phase 1
        # turns out to need it.
        raise HTTPException(status_code=400, detail="Selected strategy version is not active")

    cur.execute(
        """
        UPDATE user_mgmt.users
           SET selected_strategy_id = %s::uuid,
               selected_strategy_at = NOW()
         WHERE user_id = %s::uuid
        """,
        (strategy_version_id, user_id),
    )
    log.info("User %s selected strategy_version_id=%s", user_id, strategy_version_id)

    # Refetch the full state so the caller doesn't need a second round-trip
    return get_onboarding_state(user_id=user_id, cur=cur)


@router.post("/clear-strategy")
def clear_strategy(
    user_id: str = Depends(get_current_user),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Clear the user's selected_strategy_id. Idempotent — clearing a
    null selection is a no-op."""
    cur.execute(
        """
        UPDATE user_mgmt.users
           SET selected_strategy_id = NULL,
               selected_strategy_at = NULL
         WHERE user_id = %s::uuid
        """,
        (user_id,),
    )
    return get_onboarding_state(user_id=user_id, cur=cur)

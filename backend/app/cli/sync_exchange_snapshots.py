"""
backend/app/cli/sync_exchange_snapshots.py
============================================
Runnable as: python -m app.cli.sync_exchange_snapshots

Dual responsibility per-connection:

  status = 'pending_validation':
    call fetch_permissions() + validate_permissions()
      pass  → promote to 'active', set last_validated_at + last_permissions
      fail  → flip to 'invalid' with reason in last_error_msg
      netw. → leave 'pending_validation', bump last_error_* (retried next run)

  status = 'active':
    fetch a fresh snapshot via the existing refresh_snapshots() path,
    write exchange_snapshots row. Failures bump last_error_* but do NOT
    flip status — a one-off network blip shouldn't demote an active key.

  status IN ('invalid', 'revoked', 'errored'):
    skip. These need manual intervention.

Exits 0 always (cron-friendly). One log line per connection. No partial work
persists across row boundaries — each connection commits independently.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from ..db import get_worker_conn
from ..services.exchanges.permissions import (
    fetch_permissions,
    validate_permissions,
    PermissionAuthError,
    PermissionNetworkError,
    PermissionUnsupportedExchange,
    PermissionProbeError,
)
from ..api.routes.allocator import refresh_snapshots


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sync_exchange_snapshots")


# ─── Per-row handlers ─────────────────────────────────────────────────────────

def _validate_pending(conn, cur, row: dict[str, Any]) -> str:
    """Run one-shot validation on a pending_validation row. Returns status tag."""
    cid = str(row["connection_id"])
    exchange = row["exchange"]
    try:
        perms = fetch_permissions(
            exchange=exchange,
            api_key_enc=row["api_key_enc"],
            api_secret_enc=row["api_secret_enc"],
            passphrase_enc=row["passphrase_enc"],
        )
    except PermissionAuthError as e:
        cur.execute(
            """
            UPDATE user_mgmt.exchange_connections
            SET status='invalid', last_error_msg=%s, last_error_at=NOW(), updated_at=NOW()
            WHERE connection_id=%s::uuid
            """,
            (f"Authentication failed: {e}", cid),
        )
        conn.commit()
        return f"invalid (auth): {e}"
    except PermissionUnsupportedExchange as e:
        cur.execute(
            """
            UPDATE user_mgmt.exchange_connections
            SET status='invalid', last_error_msg=%s, last_error_at=NOW(), updated_at=NOW()
            WHERE connection_id=%s::uuid
            """,
            (str(e), cid),
        )
        conn.commit()
        return f"invalid (unsupported): {e}"
    except PermissionNetworkError as e:
        # Stay pending_validation — will retry next run.
        cur.execute(
            """
            UPDATE user_mgmt.exchange_connections
            SET last_error_msg=%s, last_error_at=NOW(), updated_at=NOW()
            WHERE connection_id=%s::uuid
            """,
            (f"Exchange unreachable: {e}", cid),
        )
        conn.commit()
        return f"pending (network): {e}"
    except PermissionProbeError as e:
        cur.execute(
            """
            UPDATE user_mgmt.exchange_connections
            SET status='errored', last_error_msg=%s, last_error_at=NOW(), updated_at=NOW()
            WHERE connection_id=%s::uuid
            """,
            (str(e), cid),
        )
        conn.commit()
        return f"errored: {e}"

    is_valid, reason = validate_permissions(exchange, perms)
    if not is_valid:
        cur.execute(
            """
            UPDATE user_mgmt.exchange_connections
            SET status='invalid',
                last_error_msg=%s, last_error_at=NOW(),
                last_permissions=%s::jsonb, updated_at=NOW()
            WHERE connection_id=%s::uuid
            """,
            (reason, json.dumps(perms.raw), cid),
        )
        conn.commit()
        return f"invalid (policy): {reason}"

    cur.execute(
        """
        UPDATE user_mgmt.exchange_connections
        SET status='active',
            last_error_msg=NULL, last_error_at=NULL,
            last_validated_at=NOW(),
            last_permissions=%s::jsonb,
            updated_at=NOW()
        WHERE connection_id=%s::uuid
        """,
        (json.dumps(perms.raw), cid),
    )
    conn.commit()
    return "promoted → active"


def _snapshot_active(conn, cur, row: dict[str, Any]) -> str:
    """Fetch a fresh snapshot for an active row. Returns status tag."""
    cid = str(row["connection_id"])
    try:
        # refresh_snapshots scopes by user_id. To run per-row we reuse it with
        # the owning user_id, then ignore other rows belonging to that user —
        # acceptable because the cost is one DB row per extra connection and
        # a handful of extra HTTP calls. The alternative is duplicating the
        # per-connection dispatch logic here.
        refresh_snapshots(cur, user_id=str(row["user_id"]))
        conn.commit()
        return "snapshot ok"
    except Exception as e:
        cur.execute(
            """
            UPDATE user_mgmt.exchange_connections
            SET last_error_msg=%s, last_error_at=NOW(), updated_at=NOW()
            WHERE connection_id=%s::uuid
            """,
            (f"Snapshot fetch failed: {e}", cid),
        )
        conn.commit()
        return f"snapshot failed: {e}"


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    conn = get_worker_conn()
    try:
        from psycopg2.extras import RealDictCursor
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            """
            SELECT connection_id, user_id, exchange, label, status,
                   api_key_enc, api_secret_enc, passphrase_enc
            FROM user_mgmt.exchange_connections
            WHERE status IN ('pending_validation', 'active')
            ORDER BY status DESC, created_at
            """
        )
        rows = cur.fetchall()
        log.info("Processing %d connection(s)", len(rows))

        # Track which user_ids we've already snapshotted so we don't fetch
        # the same user's data multiple times per run.
        snapshotted_users: set[str] = set()
        promoted_users: set[str] = set()

        for r in rows:
            cid = str(r["connection_id"])
            tag = f"{r['exchange']}/{r['label'] or '—'} [{cid[:8]}]"
            try:
                if r["status"] == "pending_validation":
                    outcome = _validate_pending(conn, cur, r)
                    if outcome.startswith("promoted"):
                        promoted_users.add(str(r["user_id"]))
                elif r["status"] == "active":
                    uid = str(r["user_id"])
                    if uid in snapshotted_users:
                        outcome = "snapshot skipped (already run for this user)"
                    else:
                        outcome = _snapshot_active(conn, cur, r)
                        if outcome.startswith("snapshot ok"):
                            snapshotted_users.add(uid)
                else:
                    outcome = f"skipped (status={r['status']})"
                log.info("%s → %s", tag, outcome)
            except Exception as e:
                # Per-row safety net — don't let one bad row kill the job.
                log.exception("%s → unhandled error: %s", tag, e)
                conn.rollback()

        # Any just-promoted users also need an initial snapshot.
        for uid in promoted_users - snapshotted_users:
            try:
                refresh_snapshots(cur, user_id=uid)
                conn.commit()
                log.info("initial snapshot for newly-promoted user %s", uid)
            except Exception as e:
                log.warning("initial snapshot for user %s failed: %s", uid, e)
                conn.rollback()

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())

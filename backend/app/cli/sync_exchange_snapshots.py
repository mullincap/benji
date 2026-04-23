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


# Anomaly threshold: a snapshot equity delta exceeding the larger of $50
# OR 2% of the prior equity triggers an out-of-cycle capital-events poll.
# Tuned to be liberal — false positives just cost one extra HTTP call per
# exchange (cheap), false negatives leave a transfer un-recorded until the
# nightly cron at 01:45 UTC.
_ANOMALY_USD_FLOOR    = 50.0
_ANOMALY_PCT_OF_PRIOR = 0.02


def _check_capital_anomaly(conn, cur, connection_id: str) -> None:
    """Compare the two most-recent successful snapshots on this connection.
    If equity delta exceeds (max($50, 2% of prior equity)) AFTER netting
    out any auto-detected capital events in that window, fire an
    out-of-band poll tagged source='auto-anomaly'. Cheap fail: any error
    just logs and continues — the nightly cron will catch what we miss.
    """
    try:
        cur.execute("""
            SELECT total_equity_usd::float AS equity, snapshot_at
              FROM user_mgmt.exchange_snapshots
             WHERE connection_id = %s::uuid
               AND fetch_ok = TRUE
               AND total_equity_usd IS NOT NULL
             ORDER BY snapshot_at DESC
             LIMIT 2
        """, (connection_id,))
        snaps = cur.fetchall()
        if len(snaps) < 2:
            return  # no prior to compare against

        cur_equity, cur_at = snaps[0]["equity"], snaps[0]["snapshot_at"]
        prv_equity, prv_at = snaps[1]["equity"], snaps[1]["snapshot_at"]
        delta = cur_equity - prv_equity

        # Subtract any capital events ALREADY recorded in this window so a
        # legitimate manual entry doesn't trip the anomaly detector again.
        cur.execute("""
            SELECT COALESCE(SUM(CASE kind
                                  WHEN 'deposit'    THEN amount_usd
                                  WHEN 'withdrawal' THEN -amount_usd
                                END), 0)::float AS net
              FROM user_mgmt.allocation_capital_events
             WHERE connection_id = %s::uuid
               AND event_at > %s::timestamptz
               AND event_at <= %s::timestamptz
               AND deleted_at IS NULL
        """, (connection_id, prv_at, cur_at))
        already_recorded = float(cur.fetchone()["net"] or 0)
        unexplained = delta - already_recorded

        threshold = max(_ANOMALY_USD_FLOOR, _ANOMALY_PCT_OF_PRIOR * prv_equity)
        if abs(unexplained) < threshold:
            return  # within normal trading-PnL range

        log.info(
            "anomaly: connection %s equity %.2f → %.2f (delta %+.2f, "
            "already_recorded %+.2f, unexplained %+.2f, threshold %.2f) — "
            "triggering out-of-band capital-events poll",
            connection_id[:8], prv_equity, cur_equity, delta,
            already_recorded, unexplained, threshold,
        )
        try:
            from .poll_capital_events import poll_connection
            summary = poll_connection(
                conn, cur, connection_id, source="auto-anomaly",
            )
            log.info("anomaly poll done: %s", summary)
        except Exception as e:
            log.warning("anomaly poll failed for %s: %s", connection_id[:8], e)
    except Exception as e:
        log.warning("anomaly check failed for %s: %s", connection_id[:8], e)


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
        # Post-snapshot: anomaly check on this specific connection. Runs
        # AFTER commit so the new snapshot row is visible to the comparison
        # query.
        _check_capital_anomaly(conn, cur, cid)
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

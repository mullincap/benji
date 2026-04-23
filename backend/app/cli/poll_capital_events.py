"""
backend/app/cli/poll_capital_events.py
=======================================
Auto-detect deposits + withdrawals from exchange income APIs and insert
them into user_mgmt.allocation_capital_events with source='auto'.

Two entry points:

  python -m app.cli.poll_capital_events
      Default cron path. Iterates ALL active exchange_connections, calls
      adapter.get_capital_events(since_ms=last_polled_at), dedup-inserts
      into the table. Used by the nightly cron at ~01:45 UTC.

  python -m app.cli.poll_capital_events --connection-id <uuid> --source auto-anomaly
      On-demand path. Polls a single connection out-of-cycle, used by
      sync_exchange_snapshots when the equity-jump anomaly detector fires
      mid-session. Same dedup semantics, different `source` tag.

  python -m app.cli.poll_capital_events --backfill
      First-deploy backfill. Polls each active connection back to its
      created_at (capped at API limits — BloFin ~90d, Binance ~365d).

Allocation mapping policy:
  - If the connection has exactly ONE active allocation at the event_at
    timestamp, credit that allocation.
  - If it has 0 or 2+ active allocations, insert with allocation_id=NULL.
    The list endpoint surfaces unmapped events to the connection's
    owning user so they can assign in the UI.

Manual-override safety:
  - The dedup query checks `(connection_id, exchange_event_id)` regardless
    of `is_manually_overridden` or `deleted_at`. Any pre-existing row
    blocks insert. So:
      - User edited an auto event → next poll skips (row exists, override
        flag set; we never touch it again)
      - User soft-deleted an auto event → next poll skips (row still
        present, deleted_at set, we never re-create it)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any

from psycopg2.extras import RealDictCursor

from ..db import get_worker_conn
from ..services.exchanges.adapter import CapitalEventInfo, adapter_for
from ..services.trading.credential_loader import (
    load_credentials,
    CredentialDecryptError,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("poll_capital_events")


# Default lookback when polling a connection that has no prior auto rows
# AND we're not doing a backfill. Keeps the API call cheap for daily cron
# runs (only catches events from the last week, which is more than enough
# given we run nightly).
_DEFAULT_LOOKBACK_DAYS = 7

# Backfill lookback caps per exchange (per their API retention).
_BACKFILL_DAYS = {
    "blofin":  90,
    "binance": 365,
}


def _last_auto_event_ms(cur, connection_id: str) -> int | None:
    """Most recent auto-detected event_at on this connection, in epoch-ms.
    Includes soft-deleted rows so we don't re-poll the same window for
    something an operator already explicitly deleted.
    """
    cur.execute("""
        SELECT EXTRACT(EPOCH FROM MAX(event_at)) * 1000 AS last_ms
          FROM user_mgmt.allocation_capital_events
         WHERE connection_id = %s::uuid
           AND source IN ('auto', 'auto-anomaly')
    """, (connection_id,))
    row = cur.fetchone()
    if not row or row.get("last_ms") is None:
        return None
    return int(row["last_ms"])


def _resolve_allocation_for_connection(
    cur,
    connection_id: str,
    event_at_ms: int,
) -> str | None:
    """Pick the allocation to credit for this event, or None if ambiguous.

    Policy: among the connection's allocations whose lifecycle window
    includes event_at (created_at <= event_at AND (closed_at IS NULL OR
    closed_at >= event_at)), if there's EXACTLY ONE, credit it.
    Otherwise return None and let the operator assign in the UI.
    """
    from datetime import datetime, timezone
    event_at_dt = datetime.fromtimestamp(event_at_ms / 1000, tz=timezone.utc)
    cur.execute("""
        SELECT allocation_id::text
          FROM user_mgmt.allocations
         WHERE connection_id = %s::uuid
           AND created_at <= %s::timestamptz
           AND (closed_at IS NULL OR closed_at >= %s::timestamptz)
    """, (connection_id, event_at_dt, event_at_dt))
    candidates = cur.fetchall()
    if len(candidates) == 1:
        return candidates[0]["allocation_id"]
    return None


def _insert_event(
    cur,
    connection_id: str,
    event: CapitalEventInfo,
    source: str,
) -> bool:
    """Insert one auto-detected event. Returns True if a row was created,
    False if the (connection_id, exchange_event_id) was already present.
    Idempotent via the partial UNIQUE index added in migration 006.
    """
    allocation_id = _resolve_allocation_for_connection(
        cur, connection_id, event.ts_ms,
    )
    cur.execute("""
        INSERT INTO user_mgmt.allocation_capital_events
            (allocation_id, connection_id, event_at, amount_usd,
             kind, notes, source, exchange_event_id, is_manually_overridden)
        VALUES (
            %s::uuid, %s::uuid,
            to_timestamp(%s / 1000.0),
            %s, %s, %s, %s, %s, FALSE
        )
        ON CONFLICT (connection_id, exchange_event_id)
            WHERE exchange_event_id IS NOT NULL
            DO NOTHING
        RETURNING event_id::text
    """, (
        allocation_id, connection_id,
        event.ts_ms,
        event.amount_usd, event.kind, event.notes,
        source, event.event_id,
    ))
    return cur.fetchone() is not None


def poll_connection(
    conn,
    cur,
    connection_id: str,
    *,
    source: str = "auto",
    backfill: bool = False,
) -> dict[str, Any]:
    """Poll one connection. Decrypts credentials, builds adapter, fetches
    events, dedup-inserts. Returns a summary dict for logging.
    """
    cur.execute("""
        SELECT connection_id, exchange, label, created_at,
               api_key_enc, api_secret_enc, passphrase_enc
          FROM user_mgmt.exchange_connections
         WHERE connection_id = %s::uuid
           AND status = 'active'
    """, (connection_id,))
    row = cur.fetchone()
    if not row:
        return {"connection_id": connection_id, "skipped": "not_active"}

    try:
        creds = load_credentials(
            exchange=row["exchange"],
            api_key_enc=row["api_key_enc"],
            api_secret_enc=row["api_secret_enc"],
            passphrase_enc=row["passphrase_enc"],
        )
    except CredentialDecryptError as e:
        return {"connection_id": connection_id, "skipped": f"creds_error: {e}"}

    adapter = adapter_for(creds)

    # Determine since_ms.
    if backfill:
        days = _BACKFILL_DAYS.get(row["exchange"], 90)
        since_ms = int(time.time() * 1000) - (days * 86_400_000)
    else:
        last_ms = _last_auto_event_ms(cur, connection_id)
        if last_ms is not None:
            # Walk a small overlap window in case the previous run captured
            # only partial events at the boundary; dedup will collapse
            # duplicates.
            since_ms = last_ms - (60 * 1000)
        else:
            since_ms = int(time.time() * 1000) - (_DEFAULT_LOOKBACK_DAYS * 86_400_000)

    try:
        events = adapter.get_capital_events(since_ms=since_ms)
    except Exception as e:
        log.exception("get_capital_events failed for %s: %s", row["exchange"], e)
        return {"connection_id": connection_id, "skipped": f"fetch_error: {e}"}

    inserted = 0
    skipped_dup = 0
    for ev in events:
        try:
            if _insert_event(cur, connection_id, ev, source=source):
                inserted += 1
            else:
                skipped_dup += 1
        except Exception as e:
            log.exception("insert failed for event %s: %s", ev.event_id, e)
            conn.rollback()
    conn.commit()
    return {
        "connection_id": connection_id,
        "exchange":      row["exchange"],
        "fetched":       len(events),
        "inserted":      inserted,
        "skipped_dup":   skipped_dup,
        "since_ms":      since_ms,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--connection-id", type=str,
                   help="Poll one specific connection (default: all active).")
    p.add_argument("--source", type=str, default="auto",
                   choices=("auto", "auto-anomaly"),
                   help="Tag inserted rows with this source (default: auto).")
    p.add_argument("--backfill", action="store_true",
                   help="Walk back to API limits (BloFin 90d, Binance 365d) "
                        "instead of using the incremental cursor.")
    args = p.parse_args()

    conn = get_worker_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if args.connection_id:
            connections = [args.connection_id]
        else:
            cur.execute("""
                SELECT connection_id::text AS connection_id
                  FROM user_mgmt.exchange_connections
                 WHERE status = 'active'
                 ORDER BY created_at
            """)
            connections = [r["connection_id"] for r in cur.fetchall()]

        log.info("Polling %d connection(s) source=%s backfill=%s",
                 len(connections), args.source, args.backfill)
        for cid in connections:
            try:
                summary = poll_connection(
                    conn, cur, cid,
                    source=args.source,
                    backfill=args.backfill,
                )
                log.info("conn %s [%s] fetched=%s inserted=%s dup=%s",
                         cid[:8],
                         summary.get("exchange") or summary.get("skipped"),
                         summary.get("fetched"),
                         summary.get("inserted"),
                         summary.get("skipped_dup"))
            except Exception as e:
                log.exception("poll_connection %s failed: %s", cid[:8], e)
                conn.rollback()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())

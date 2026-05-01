"""
backend/app/cli/account_anchor_writer.py
==========================================
Daily 00:00 UTC writer for user_mgmt.account_daily_anchors. One row per
(venue, connection_id, anchor_date) with the connection's equity +
margin + position-count state at the moment of capture. The Manager
Live tab reads from this table for "today's PnL" math:
   today_pnl = current_equity − latest_anchor.total_equity_usd

Idempotent: re-running on the same (venue, connection_id, anchor_date)
overwrites the row via ON CONFLICT … DO UPDATE.

Run paths:
  python -m app.cli.account_anchor_writer
      → today UTC, every active connection.
  python -m app.cli.account_anchor_writer --date 2026-04-30
      → write the 2026-04-30 anchor using current state. If the date is
        not today UTC, raw_payload.synthesized=true marks the row as
        a back-fill rather than a true 00:00 capture.
  python -m app.cli.account_anchor_writer --connection <uuid>
      → restrict to one connection.

On per-connection failure: 3 retries with 5s/30s/120s back-off, refresh
+ read latest snapshot. After exhaustion, log loudly (cron-mailer
pickup) and continue to the next connection. Job overall exits 0 if
at least one anchor wrote — only exits 1 if every connection failed,
keeping cron noise low while still surfacing total-outage events.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
import time

from psycopg2.extras import RealDictCursor

from ..db import get_worker_conn
from ..api.routes.allocator import refresh_snapshots


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("account_anchor_writer")


# Retry schedule for refresh failures: 5s, 30s, 120s (then give up).
# Tuned for transient network blips and exchange brown-outs — Binance/
# BloFin recover within seconds; backing off past 2 minutes wastes cron
# time without raising success probability.
RETRY_DELAYS_S = [5, 30, 120]


# ─── Per-connection writer ─────────────────────────────────────────────────

def write_anchor_for_connection(
    conn, cur, *,
    connection_id: str,
    exchange: str,
    anchor_date: datetime.date,
    synthesized: bool,
) -> str:
    """Refresh + read latest snapshot + UPSERT to account_daily_anchors.
    Returns a status tag for the run log. Never raises — callers continue
    to the next connection on any error."""
    cur.execute(
        "SELECT user_id FROM user_mgmt.exchange_connections WHERE connection_id=%s::uuid",
        (connection_id,),
    )
    row = cur.fetchone()
    if not row:
        return f"connection not found"
    user_id = str(row["user_id"])

    # Refresh + check fetch_ok up to 4 attempts (initial + 3 retries).
    # refresh_snapshots writes a fetch_ok=FALSE row on failure rather
    # than raising, so we re-check the snapshot row after each call.
    snap = None
    last_err_msg = None
    for attempt in range(len(RETRY_DELAYS_S) + 1):
        if attempt > 0:
            delay = RETRY_DELAYS_S[attempt - 1]
            log.warning(
                "retry %d/%d for %s after %ds (last error: %s)",
                attempt, len(RETRY_DELAYS_S), connection_id[:8], delay, last_err_msg,
            )
            time.sleep(delay)

        try:
            refresh_snapshots(cur, user_id=user_id)
            conn.commit()
        except Exception as e:
            log.warning(
                "refresh raised on attempt %d for %s: %s",
                attempt + 1, connection_id[:8], e,
            )
            conn.rollback()
            last_err_msg = str(e)
            continue

        cur.execute("""
            SELECT total_equity_usd, available_usd, used_margin_usd,
                   unrealized_pnl, positions, snapshot_at, fetch_ok, error_msg
              FROM user_mgmt.exchange_snapshots
             WHERE connection_id = %s::uuid
             ORDER BY snapshot_at DESC
             LIMIT 1
        """, (connection_id,))
        latest = cur.fetchone()
        if latest and latest["fetch_ok"]:
            snap = latest
            break
        last_err_msg = (latest and latest["error_msg"]) or "fetch_ok=FALSE"

    if not snap:
        # Loud log — cron mailer picks up via stderr-on-error pattern. The
        # writer doesn't dial out itself; it relies on the existing log-
        # tail observability that the rest of the project uses.
        log.error(
            "ALERT: anchor write failed for %s/%s after %d attempts: %s",
            exchange, connection_id[:8], len(RETRY_DELAYS_S) + 1, last_err_msg,
        )
        return f"failed after retries: {last_err_msg}"

    # Build raw_payload. The position list comes straight from the
    # snapshot — already normalized. The synthesized flag is the audit
    # trail for back-filled rows.
    positions = snap["positions"] or []
    raw_payload: dict = {
        "snapshot_at": snap["snapshot_at"].isoformat() if snap["snapshot_at"] else None,
        "positions": positions,
    }
    if synthesized:
        raw_payload["synthesized"] = True

    cur.execute("""
        INSERT INTO user_mgmt.account_daily_anchors
            (venue, connection_id, anchor_date, total_equity_usd,
             available_usd, used_margin_usd, unrealized_pnl_usd,
             open_position_count, raw_payload)
        VALUES (%s, %s::uuid, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (venue, connection_id, anchor_date) DO UPDATE SET
            captured_at         = NOW(),
            total_equity_usd    = EXCLUDED.total_equity_usd,
            available_usd       = EXCLUDED.available_usd,
            used_margin_usd     = EXCLUDED.used_margin_usd,
            unrealized_pnl_usd  = EXCLUDED.unrealized_pnl_usd,
            open_position_count = EXCLUDED.open_position_count,
            raw_payload         = EXCLUDED.raw_payload
    """, (
        exchange, connection_id, anchor_date,
        snap["total_equity_usd"], snap["available_usd"],
        snap["used_margin_usd"], snap["unrealized_pnl"],
        len(positions), json.dumps(raw_payload, default=str),
    ))

    # Per-position anchor rows in user_mgmt.position_snapshots — one per
    # open position at this anchor moment. Drives the Live tab waterfall:
    # today_pnl = current_unrealized_pnl − this_anchor.unrealized_pnl_usd.
    #
    # Position identifier: BloFin's snapshot-stored symbol form
    # ("BTCUSDT", dash already stripped). One position per symbol in net
    # mode; sufficient as a stable id for v1. When hedge mode or
    # multiple-position-per-symbol cases arise, switch to a synthesized
    # `{symbol}|{side}|{margin_mode}` key.
    #
    # snapshot_at is set to anchor_date at midnight UTC so the row is a
    # true "00:00 anchor" regardless of when the cron actually fires
    # (typically 00:00:00–00:01:00 with jitter). Idempotent on
    # (venue, connection_id, position_id, snapshot_at).
    anchor_ts = datetime.datetime.combine(
        anchor_date, datetime.time(0, 0, 0, tzinfo=datetime.timezone.utc),
    )
    pos_rows_written = 0
    for p in positions:
        sym_full = (p.get("symbol") or "").upper()
        if not sym_full:
            continue
        size = float(p.get("size") or 0)
        if size == 0:
            continue
        mark = float(p.get("mark_price") or 0)
        notional = float(p.get("notional_usd") or 0)
        upl = float(p.get("unrealized_pnl") or 0)
        upl_pct = (upl / notional * 100.0) if notional > 0 else 0.0
        cur.execute("""
            INSERT INTO user_mgmt.position_snapshots
                (venue, connection_id, position_id, snapshot_at,
                 mark_price, size, notional_usd, unrealized_pnl_usd,
                 unrealized_pct, funding_paid_usd)
            VALUES (%s, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (venue, connection_id, position_id, snapshot_at) DO UPDATE SET
                mark_price         = EXCLUDED.mark_price,
                size               = EXCLUDED.size,
                notional_usd       = EXCLUDED.notional_usd,
                unrealized_pnl_usd = EXCLUDED.unrealized_pnl_usd,
                unrealized_pct     = EXCLUDED.unrealized_pct,
                funding_paid_usd   = EXCLUDED.funding_paid_usd
        """, (
            exchange, connection_id, sym_full, anchor_ts,
            mark, abs(size), notional, upl, upl_pct, None,
        ))
        pos_rows_written += 1

    conn.commit()
    return (
        f"wrote anchor: equity={snap['total_equity_usd']}, "
        f"positions={len(positions)}, position_anchors={pos_rows_written}"
        + ("  [SYNTHESIZED]" if synthesized else "")
    )


# ─── CLI ──────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Write daily account anchor rows. Idempotent on (venue, connection_id, anchor_date).",
    )
    p.add_argument(
        "--date",
        help="UTC date for the anchor (YYYY-MM-DD). Default = today UTC. "
             "If different from today, raw_payload.synthesized=true is set.",
    )
    p.add_argument(
        "--connection",
        help="Single connection_id (UUID). Default = every active connection.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    today_utc = datetime.datetime.now(datetime.timezone.utc).date()
    if args.date:
        try:
            anchor_date = datetime.date.fromisoformat(args.date)
        except ValueError:
            log.error("Invalid --date: %r (expected YYYY-MM-DD)", args.date)
            return 1
        synthesized = anchor_date != today_utc
    else:
        anchor_date = today_utc
        synthesized = False

    conn = get_worker_conn()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if args.connection:
            cur.execute("""
                SELECT connection_id, exchange
                  FROM user_mgmt.exchange_connections
                 WHERE connection_id = %s::uuid AND status = 'active'
            """, (args.connection,))
        else:
            cur.execute("""
                SELECT connection_id, exchange
                  FROM user_mgmt.exchange_connections
                 WHERE status = 'active'
                 ORDER BY exchange, created_at
            """)
        connections = cur.fetchall()

        log.info(
            "Anchor date %s (synthesized=%s) — %d active connection(s)",
            anchor_date, synthesized, len(connections),
        )
        if not connections:
            log.warning("No active connections — nothing to write")
            return 0

        successes = 0
        for c in connections:
            cid = str(c["connection_id"])
            tag = f"{c['exchange']}/[{cid[:8]}]"
            try:
                outcome = write_anchor_for_connection(
                    conn, cur,
                    connection_id=cid,
                    exchange=c["exchange"],
                    anchor_date=anchor_date,
                    synthesized=synthesized,
                )
                log.info("%s → %s", tag, outcome)
                if outcome.startswith("wrote anchor"):
                    successes += 1
            except Exception as e:
                # Per-connection safety net — never let one bad row kill the job.
                log.exception("%s → unhandled error: %s", tag, e)
                try:
                    conn.rollback()
                except Exception:
                    pass

        log.info(
            "Anchor write complete: %d/%d connection(s) succeeded for %s",
            successes, len(connections), anchor_date,
        )
        # Exit 0 if at least one wrote (avoids cron-mail spam on transient
        # single-connection failures); exit 1 only on total outage so the
        # operator gets paged when it actually matters.
        return 0 if successes > 0 else 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

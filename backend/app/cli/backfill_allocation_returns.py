"""
backend/app/cli/backfill_allocation_returns.py
===============================================
Post-session backfill for user_mgmt.allocation_returns telemetry fields
(fill_rate, avg_entry_slip_bps, avg_exit_slip_bps) that the live writer
missed because of the pre-Session-F ordering bug where
_log_allocation_return() ran BEFORE reconcile_exit_prices() on the
allocation path.

Strategy: parse the per-allocation session log file for that date,
extract whatever telemetry the logs captured (typically the "Exit
prices:" line that follows reconcile_exit_prices), and UPDATE only
the currently-NULL columns — never overwrite a value the writer may
have persisted in a later code path.

The log file format is defined by trader_blofin's allocation logger.
Relevant lines this parser targets:

    <ts> [INFO]   Exit prices: SYM-USDT est=$X.XXXX fill=$X.XXXX slip=-XX.XXbps  |  ...

Entry slippage and fill_rate are not typically recoverable from the
main session log (they're computed inside enter_positions() and only
logged in debug/summary form). We leave those columns alone and let
the nightly cron re-run this script — the live writer fix in the
same commit prevents NEW sessions from needing backfill.

Usage:
    python -m app.cli.backfill_allocation_returns \\
        --allocation-id <uuid> \\
        --date 2026-04-21 \\
        [--dry-run]

    # Or batch mode: scan all allocation_returns rows for a date and
    # backfill any that have NULL slip/fill_rate.
    python -m app.cli.backfill_allocation_returns --date 2026-04-21 --all

Exits 0 on success (including no-op), 1 on error. Safe to re-run —
COALESCE-style update never clobbers a non-null value.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

log = logging.getLogger("backfill_allocation_returns")


# ── DB connection (mirrors spawn_traders._connect) ──────────────────────────
def _connect():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        user=os.environ.get("DB_USER", "quant"),
        password=os.environ.get("DB_PASSWORD", ""),
        dbname=os.environ.get("DB_NAME", "marketdata"),
        connect_timeout=5,
    )


# ── Log location ────────────────────────────────────────────────────────────
LOG_DIR = Path(os.environ.get(
    "TRADER_LOG_DIR",
    "/mnt/quant-data/logs/trader",
))


def log_path_for(allocation_id: str, date: str) -> Path:
    return LOG_DIR / f"allocation_{allocation_id}_{date}.log"


# ── Parsers ─────────────────────────────────────────────────────────────────
# "Exit prices: IP-USDT est=$0.5170 fill=$0.5150 slip=-38.68bps  |  ..."
# Each per-symbol chunk is: <SYM>-USDT est=$... fill=$... slip=<N>bps
_EXIT_CHUNK_RE = re.compile(
    r"([A-Z0-9]+-USDT)\s+est=\$([0-9.]+)\s+fill=\$([0-9.]+)\s+slip=([-+]?[0-9.]+)bps"
)


def parse_exit_slips(log_text: str) -> list[float]:
    """Return every per-symbol slip_bps value that reconcile_exit_prices
    printed in the "Exit prices:" summary line. Missing symbols (logged
    as "exit fill price not found on blofin") are absent from that line
    and therefore excluded — matches _mean_slip's null-safe behavior.
    """
    slips: list[float] = []
    # Scan only lines containing "Exit prices:" for isolation; the
    # chunk regex can then pick off each symbol block.
    for line in log_text.splitlines():
        if "Exit prices:" not in line:
            continue
        for m in _EXIT_CHUNK_RE.finditer(line):
            try:
                slips.append(float(m.group(4)))
            except ValueError:
                continue
    return slips


def parse_fill_rate_from_log(log_text: str) -> float | None:
    """Best-effort parse. The live writer derives fill_rate from
    fill_report["fills"]["fill_rate_pct"] — there's no public log line
    that carries the same value. Return None so the UPDATE leaves the
    column alone. Keep this stub to document the gap for future parsers.
    """
    return None


def parse_entry_slips(log_text: str) -> list[float]:
    """Same situation as fill_rate — entry slippage is computed in
    enter_positions() and not printed to the per-allocation log in a
    consistent machine-readable form. Return [] so avg_entry_slip_bps
    stays untouched.
    """
    return []


# ── Backfill ────────────────────────────────────────────────────────────────
def backfill_one(
    cur,
    allocation_id: str,
    date: str,
    dry_run: bool,
) -> dict:
    """Parse this allocation's log for `date`, compute whatever metrics
    the parser can extract, and UPDATE only the NULL columns on the
    matching allocation_returns row. Returns a dict summarizing what
    was parsed and what was applied.
    """
    path = log_path_for(allocation_id, date)
    if not path.exists():
        return {"allocation_id": allocation_id, "date": date, "skipped": "log_not_found", "path": str(path)}

    text = path.read_text(encoding="utf-8", errors="replace")
    exit_slips = parse_exit_slips(text)
    entry_slips = parse_entry_slips(text)
    fill_rate = parse_fill_rate_from_log(text)

    avg_exit = (sum(exit_slips) / len(exit_slips)) if exit_slips else None
    avg_entry = (sum(entry_slips) / len(entry_slips)) if entry_slips else None

    cur.execute(
        """
        SELECT allocation_id, session_date,
               fill_rate, avg_entry_slip_bps, avg_exit_slip_bps,
               net_return_pct, exit_reason
        FROM user_mgmt.allocation_returns
        WHERE allocation_id = %s::uuid AND session_date = %s
        """,
        (allocation_id, date),
    )
    row = cur.fetchone()
    if not row:
        return {"allocation_id": allocation_id, "date": date, "skipped": "no_row_to_update"}

    before = {
        "fill_rate": row["fill_rate"],
        "avg_entry_slip_bps": row["avg_entry_slip_bps"],
        "avg_exit_slip_bps": row["avg_exit_slip_bps"],
    }
    planned = {
        "fill_rate":
            fill_rate if row["fill_rate"] is None and fill_rate is not None else None,
        "avg_entry_slip_bps":
            avg_entry if row["avg_entry_slip_bps"] is None and avg_entry is not None else None,
        "avg_exit_slip_bps":
            avg_exit if row["avg_exit_slip_bps"] is None and avg_exit is not None else None,
    }
    # Only include columns that actually need updating.
    set_cols = {k: v for k, v in planned.items() if v is not None}

    if not set_cols:
        return {
            "allocation_id": allocation_id, "date": date, "skipped": "no_null_cells_to_fill",
            "before": before,
            "parsed": {"avg_exit_slip_bps": avg_exit, "avg_entry_slip_bps": avg_entry, "fill_rate": fill_rate},
        }

    # Build UPDATE statement.
    set_clause = ", ".join(f"{col} = %s" for col in set_cols)
    params = list(set_cols.values()) + [allocation_id, date]
    sql = f"""
        UPDATE user_mgmt.allocation_returns
        SET {set_clause}, logged_at = NOW()
        WHERE allocation_id = %s::uuid AND session_date = %s
    """

    if dry_run:
        return {
            "allocation_id": allocation_id, "date": date, "applied": False,
            "dry_run": True,
            "before": before,
            "would_set": set_cols,
            "sql": sql.strip(),
            "parsed": {"avg_exit_slip_bps": avg_exit, "avg_entry_slip_bps": avg_entry, "fill_rate": fill_rate, "n_exit_slips": len(exit_slips)},
        }

    cur.execute(sql, params)
    return {
        "allocation_id": allocation_id, "date": date, "applied": True,
        "before": before,
        "set": set_cols,
        "parsed": {"avg_exit_slip_bps": avg_exit, "avg_entry_slip_bps": avg_entry, "fill_rate": fill_rate, "n_exit_slips": len(exit_slips)},
    }


def find_rows_with_nulls(cur, date: str) -> list[str]:
    """Batch mode helper: every allocation_returns row for `date` that
    has NULL fill_rate, avg_entry_slip_bps, OR avg_exit_slip_bps.
    Skips rows with no capital deployed (terminal pre-trade exits are
    expected to have null telemetry — they never traded).
    """
    cur.execute(
        """
        SELECT allocation_id::text AS allocation_id
        FROM user_mgmt.allocation_returns
        WHERE session_date = %s
          AND capital_deployed_usd > 0
          AND (fill_rate IS NULL
               OR avg_entry_slip_bps IS NULL
               OR avg_exit_slip_bps IS NULL)
        """,
        (date,),
    )
    return [r["allocation_id"] for r in cur.fetchall()]


# ── CLI ─────────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--allocation-id", help="Single allocation UUID to backfill")
    parser.add_argument("--date", required=True, help="Session date YYYY-MM-DD (UTC)")
    parser.add_argument("--all", action="store_true", help="Backfill every allocation_returns row for --date that has NULL telemetry and non-zero capital")
    parser.add_argument("--dry-run", action="store_true", help="Print the UPDATE that WOULD run; don't execute")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not args.allocation_id and not args.all:
        log.error("Must pass either --allocation-id <uuid> or --all")
        return 1

    conn = _connect()
    conn.autocommit = False
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if args.all:
                ids = find_rows_with_nulls(cur, args.date)
                log.info(f"[batch] {len(ids)} row(s) with null telemetry on {args.date}")
            else:
                ids = [args.allocation_id]

            results = []
            for aid in ids:
                result = backfill_one(cur, aid, args.date, args.dry_run)
                results.append(result)
                log.info(f"  {result}")

            if args.dry_run:
                log.info("[dry-run] nothing committed")
                conn.rollback()
            else:
                conn.commit()
                applied = sum(1 for r in results if r.get("applied"))
                log.info(f"[done] {applied}/{len(results)} row(s) updated")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

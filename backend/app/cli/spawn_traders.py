"""
backend/app/cli/spawn_traders.py
=================================
Parent spawner for per-allocation BloFin traders.

Invoked daily by host cron INSIDE the long-lived backend container (via
`docker compose exec`, NOT `docker compose run --rm`). Queries active BloFin
allocations, spawns one detached subprocess per allocation running
`python -m app.cli.trader_blofin --allocation-id <uuid>`, redirects each
subprocess's stdout/stderr to its own per-day log file, then exits.

Parent exits within seconds. Subprocesses run for up to ~17 hours each
inside the same backend container — they survive the parent's exit because
`start_new_session=True` puts them in their own process group, and the
backend container keeps running for the life of the compose stack.

CRITICAL: this MUST run via `docker compose exec` against the persistent
backend service. If you instead use `docker compose run --rm`, the ephemeral
container exits when the parent exits and Linux tears down all processes in
its PID namespace — subprocesses included. Detachment via start_new_session
does not protect across container teardown. See Part 2c cron line.

Master account path is NOT touched — host cron at /root/benji/trader-blofin.py
owns the master account; this spawner only picks up user allocations.

Binance allocations are filtered out — the trader executor's per-allocation
path only supports BloFin in this release. Open work list.

Usage:
    python -m app.cli.spawn_traders                # spawn all eligible (fresh session)
    python -m app.cli.spawn_traders --dry-run      # log what would spawn, don't actually spawn
    python -m app.cli.spawn_traders --verbose      # verbose logging
    python -m app.cli.spawn_traders --resume-stuck # supervisor tick — detect stale
                                                     active sessions and respawn.
                                                     NEVER creates new sessions.

The --resume-stuck flag is cron-driven every 5 min; the default (fresh
spawn) runs once at 06:05 UTC. Both modes are safe to run concurrently —
the supervisor only touches allocations whose runtime_state is already
phase=active for today, while fresh-spawn only touches allocations
without a today-runtime_state.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

LOG_DIR = Path("/mnt/quant-data/logs/trader")

# Trader executor dispatches per-exchange via ExchangeAdapter at
# trader_blofin.py:3312. Both BloFin (perps) and Binance (cross-margin) are
# supported. Adding a new exchange = new adapter + add slug here.
SUPPORTED_EXCHANGES = {"blofin", "binance"}

# Phases that mean "this allocation already finished its session for `date`".
# Subprocess for an already-finished allocation would just hit the orchestrator's
# "already finished today" branch and exit; pre-filtering avoids the spawn cost.
TERMINAL_PHASES = {
    "closed", "filtered", "no_entry", "missed_window",
    "errored", "stale_close_failed", "entry_failed", "skipped",
}

log = logging.getLogger("spawn_traders")


def _connect():
    """Direct psycopg2 connection. Mirrors trader_blofin._trader_db_connect."""
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        user=os.environ.get("DB_USER", "quant"),
        password=os.environ.get("DB_PASSWORD", ""),
        dbname=os.environ.get("DB_NAME", "marketdata"),
        connect_timeout=5,
    )


def fetch_eligible_allocations() -> list[dict]:
    """Query active allocations on supported exchanges with active connections.

    Returns rows as dicts. Pre-filters out allocations whose runtime_state shows
    they already finished today.
    """
    today_str = datetime.now(timezone.utc).date().isoformat()
    conn = _connect()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    a.allocation_id,
                    a.user_id,
                    a.connection_id,
                    a.strategy_version_id,
                    a.capital_usd,
                    ec.exchange,
                    a.runtime_state->>'phase' AS runtime_phase,
                    a.runtime_state->>'date'  AS runtime_date,
                    a.lock_acquired_at
                FROM user_mgmt.allocations a
                JOIN user_mgmt.exchange_connections ec
                  ON ec.connection_id = a.connection_id
                WHERE a.status = 'active'
                  AND ec.status = 'active'
                  AND ec.exchange = ANY(%s)
                ORDER BY a.created_at
                """,
                (list(SUPPORTED_EXCHANGES),),
            )
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    eligible: list[dict] = []
    for r in rows:
        cid = str(r["allocation_id"])[:8]
        if r["runtime_date"] == today_str and r["runtime_phase"] in TERMINAL_PHASES:
            log.info(
                f"Skipping allocation {cid}: already finished today "
                f"(phase={r['runtime_phase']})"
            )
            continue
        eligible.append(r)
    return eligible


def spawn_allocation(allocation_id, *, dry_run: bool = False) -> int | None:
    """Spawn one detached subprocess for an allocation.

    Returns the subprocess PID, or None on dry-run. Detachment via
    start_new_session=True; subprocess runs inside the same long-lived backend
    container as the parent and survives the parent's exit.
    """
    today_str = datetime.now(timezone.utc).date().isoformat()
    short_id = str(allocation_id)[:8]
    log_path = LOG_DIR / f"allocation_{allocation_id}_{today_str}.log"

    cmd = [
        sys.executable, "-m", "app.cli.trader_blofin",
        "--allocation-id", str(allocation_id),
    ]

    if dry_run:
        log.info(f"[dry-run] would spawn: {' '.join(cmd)}  →  {log_path}")
        return None

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "a", buffering=1)  # line-buffered
    log_file.write(
        f"\n{'=' * 70}\n"
        f"Spawn at {datetime.now(timezone.utc).isoformat()} "
        f"for allocation {allocation_id}\n"
        f"{'=' * 70}\n"
    )

    # start_new_session=True creates a new process group so SIGHUP from the
    # parent's exit doesn't kill the subprocess. Within the same long-lived
    # container, this is sufficient for ~17h survival.
    p = subprocess.Popen(
        cmd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        cwd="/app/backend",   # PYTHONPATH=/app/backend per Dockerfile.backend
    )
    log.info(f"Spawned allocation {short_id}: pid={p.pid}, log={log_path}")
    return p.pid


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Spawn per-allocation BloFin traders for the daily session."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Log what would spawn without actually spawning.")
    parser.add_argument("--verbose", action="store_true",
                        help="Verbose logging.")
    parser.add_argument("--resume-stuck", action="store_true",
                        help="Supervisor mode: detect stale active sessions "
                             "(runtime_state.updated_at > 15m) and respawn "
                             "their traders. Never creates new sessions.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --resume-stuck branches to the trader supervisor. Kept in this
    # CLI so there's a single cron entry point and a single place to
    # configure trader-lifecycle logging.
    if args.resume_stuck:
        from app.cli.trader_supervisor import supervisor_pass
        return supervisor_pass()

    log.info(f"spawn_traders starting at {datetime.now(timezone.utc).isoformat()}")

    try:
        allocations = fetch_eligible_allocations()
    except Exception as e:
        log.exception(f"Failed to fetch allocations: {e}")
        return 1

    if not allocations:
        log.info("No eligible allocations to spawn. Exiting.")
        return 0

    log.info(f"Found {len(allocations)} eligible allocation(s).")

    spawned = 0
    failed = 0
    for alloc in allocations:
        try:
            pid = spawn_allocation(alloc["allocation_id"], dry_run=args.dry_run)
            if pid is not None or args.dry_run:
                spawned += 1
        except Exception as e:
            failed += 1
            log.exception(
                f"Failed to spawn allocation "
                f"{str(alloc['allocation_id'])[:8]}: {e}"
            )

    log.info(
        f"spawn_traders done. spawned={spawned}, failed={failed}, "
        f"skipped={len(allocations) - spawned - failed}"
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

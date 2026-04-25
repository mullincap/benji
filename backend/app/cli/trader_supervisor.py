"""
backend/app/cli/trader_supervisor.py
=====================================
Trader durability layer — supervisor + startup recovery.

Two invocation paths, both sharing the same resume-only respawn logic:

1. supervisor_pass(): triggered every 5 min by the host cron
   (`python -m app.cli.spawn_traders --resume-stuck`). Detects trader
   subprocesses that have gone silent mid-session (runtime_state.updated_at
   older than STALE_THRESHOLD_MIN) and respawns them, guarded by a
   two-tick confirmation, per-allocation exponential backoff, and a
   corrupted-runtime_state refuse path.

2. startup_recovery(): triggered on backend container boot via FastAPI's
   @app.on_event("startup"). Scans for sessions that were mid-flight
   when the container restarted and respawns them immediately (no
   two-tick delay — container restart is deterministic, not a heartbeat
   hiccup), with a 2-min buffer against false-positives on fast restarts.

Guardrails (shared between both paths):
  - Resume-only: never creates new sessions. Skipped unless
    runtime_state.date == today AND runtime_state.phase == 'active'.
    Fresh-spawn remains exclusive to the 06:05 UTC cron.
  - Backoff: BACKOFF_MAX_RESPAWNS within BACKOFF_WINDOW_MIN → refuse
    further attempts; surfaces to supervisor_state.last_error.
  - Corrupted-state guard: _validate_runtime_state checks required
    fields and invariants. Refuses respawn if anything's malformed —
    unacceptable failure mode would be re-entering positions the
    original trader was supposed to be monitoring.

Persistence: user_mgmt.trader_supervisor_state. Created automatically
via CREATE TABLE IF NOT EXISTS on first run.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

# Reuses the existing subprocess spawn machinery — keeps a single
# source of truth for how a trader subprocess is launched (detached,
# stdout/stderr to per-day log file, same cwd / env).
from app.cli.spawn_traders import _connect, spawn_allocation

log = logging.getLogger("trader_supervisor")


# ── Tuning constants ────────────────────────────────────────────────────────
# Heartbeat threshold: runtime_state.updated_at older than this → stale.
# 15 min is 3 missed bar-writes; a single 5-min gap is within one heartbeat
# of a healthy trader, so two consecutive ticks are required (below) before
# we act on staleness.
STALE_THRESHOLD_MIN = 15

# Two-tick confirmation window: supervisor runs every 5 min, so a
# previous stale marker must be within ~1-2 ticks to count as a
# "consecutive" observation. Past this, treat as a new first-detection
# rather than respawning on stale data carried over from an earlier
# session or a day-old marker.
STALE_CONFIRMATION_MAX_MIN = 12

# Exponential backoff: N respawns in WINDOW → refuse further respawns.
# Persistent failure = human intervention, not infinite loop.
BACKOFF_MAX_RESPAWNS = 3
BACKOFF_WINDOW_MIN = 60

# Startup recovery: how stale runtime_state must be vs the container's
# start time before we respawn.
#
# History: was 2 min. Caused a dup-spawn on 2026-04-25 19:05 UTC during a
# mid-session backend rebuild. The trader at PID 144 in benji-celery-1 was
# alive and sleeping between bar 156 (19:00:04) and bar 157 (19:05:04).
# When backend container booted at 19:05:02, runtime_state.updated_at was
# 5 min stale (the gap between bars), which exceeded the 2-min buffer ->
# startup_recovery respawned, creating PID 8 in benji-backend-1. Both
# processes then double-logged bar 158.
#
# Set to STALE_THRESHOLD_MIN so startup matches the supervisor cron's own
# notion of staleness — never respawn on a gap shorter than what the
# supervisor would itself act on. The supervisor's 5-min cadence catches
# any genuine orphans within minutes anyway, so being conservative at
# startup costs little and prevents the cross-container dup-spawn race.
STARTUP_STALE_BUFFER_MIN = STALE_THRESHOLD_MIN


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS user_mgmt.trader_supervisor_state (
    allocation_id     uuid PRIMARY KEY
        REFERENCES user_mgmt.allocations(allocation_id) ON DELETE CASCADE,
    stale_detected_at timestamptz,
    respawn_count     int         NOT NULL DEFAULT 0,
    last_respawn_at   timestamptz,
    last_error        text,
    last_error_at     timestamptz,
    updated_at        timestamptz NOT NULL DEFAULT NOW()
);
"""


def ensure_schema(conn) -> None:
    """Idempotent schema creation. Called on first use of each entry point."""
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.commit()


# ── Invariant validation ────────────────────────────────────────────────────

def _validate_runtime_state(state) -> str | None:
    """Return error message describing why runtime_state is unsafe to
    resume, or None if it passes all invariant checks.

    Checks match the subset of fields the trader's resume path at
    trader_blofin.py:3685+ reads without fallbacks. Missing these
    would cause the respawned trader to either crash immediately or
    enter a degenerate state.
    """
    if not state or not isinstance(state, dict):
        return "runtime_state is empty or not a dict"
    for f in ("date", "phase", "session_id"):
        if f not in state:
            return f"missing required field: {f}"
    if state.get("phase") != "active":
        return f"phase is {state.get('phase')!r}, expected 'active'"
    if not isinstance(state.get("symbols"), list) or not state.get("symbols"):
        return "symbols must be a non-empty list"
    if not isinstance(state.get("entry_prices"), dict) or not state.get("entry_prices"):
        return "entry_prices must be a non-empty dict"
    if not isinstance(state.get("open_prices"), dict) or not state.get("open_prices"):
        return "open_prices must be a non-empty dict"
    positions = state.get("positions")
    if not isinstance(positions, list) or not positions:
        return "positions must be a non-empty list"
    for i, p in enumerate(positions):
        if not isinstance(p, dict):
            return f"positions[{i}] is not a dict: {p!r}"
        if not p.get("inst_id"):
            return f"positions[{i}] missing inst_id"
        if "contracts" not in p:
            return f"positions[{i}] missing contracts field"
    return None


# ── Supervisor-state helpers ────────────────────────────────────────────────

def _release_lock(conn, allocation_id: str) -> None:
    """Clear the advisory lock so the respawned trader can acquire."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE user_mgmt.allocations SET lock_acquired_at = NULL "
            "WHERE allocation_id = %s::uuid",
            (allocation_id,),
        )
    conn.commit()


def _should_backoff(row: dict) -> bool:
    count = int(row.get("respawn_count") or 0)
    last = row.get("last_respawn_at")
    if count < BACKOFF_MAX_RESPAWNS:
        return False
    if last is None:
        return False
    return (datetime.now(timezone.utc) - last) < timedelta(minutes=BACKOFF_WINDOW_MIN)


def _upsert_supervisor_state(conn, allocation_id: str, **cols) -> None:
    """Upsert supervisor state columns for one allocation.

    `cols` is a dict of column → value. `None` inserts / updates NULL.
    updated_at is always refreshed.
    """
    if not cols:
        return
    col_names = list(cols.keys())
    insert_cols = ", ".join(["allocation_id", *col_names, "updated_at"])
    placeholders = ", ".join(["%s"] * (len(col_names) + 2))
    set_clauses = ", ".join(f"{c} = EXCLUDED.{c}" for c in col_names)
    sql = f"""
        INSERT INTO user_mgmt.trader_supervisor_state ({insert_cols})
        VALUES ({placeholders})
        ON CONFLICT (allocation_id) DO UPDATE SET
          {set_clauses},
          updated_at = EXCLUDED.updated_at
    """
    params = [allocation_id, *[cols[c] for c in col_names], datetime.now(timezone.utc)]
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def _clear_stale_marker(conn, allocation_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE user_mgmt.trader_supervisor_state "
            "SET stale_detected_at = NULL, updated_at = NOW() "
            "WHERE allocation_id = %s::uuid",
            (allocation_id,),
        )
    conn.commit()


# ── Target fetch ────────────────────────────────────────────────────────────

def _fetch_stale_rows(conn) -> list[dict]:
    """Supervisor mode: allocations with active today-session AND
    runtime_state.updated_at older than STALE_THRESHOLD_MIN.

    Staleness is computed inside the DB query against NOW() so cron and
    DB share the same clock — no wall-clock drift between supervisor
    and the trader's per-bar writes.
    """
    today_str = datetime.now(timezone.utc).date().isoformat()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT a.allocation_id::text AS allocation_id,
                   a.runtime_state,
                   ss.stale_detected_at,
                   ss.respawn_count,
                   ss.last_respawn_at
            FROM user_mgmt.allocations a
            LEFT JOIN user_mgmt.trader_supervisor_state ss
              ON ss.allocation_id = a.allocation_id
            WHERE a.status = 'active'
              AND a.runtime_state->>'phase' = 'active'
              AND a.runtime_state->>'date'  = %s
              AND (NOW() - (a.runtime_state->>'updated_at')::timestamptz)
                  > make_interval(mins => %s)
            """,
            (today_str, STALE_THRESHOLD_MIN),
        )
        return [dict(r) for r in cur.fetchall()]


def _fetch_orphans_at_startup(conn, threshold_ts: datetime) -> list[dict]:
    """Startup mode: allocations with active today-session whose last
    runtime_state write predates the container's start time by at
    least STARTUP_STALE_BUFFER_MIN minutes.
    """
    today_str = datetime.now(timezone.utc).date().isoformat()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT a.allocation_id::text AS allocation_id,
                   a.runtime_state,
                   ss.respawn_count,
                   ss.last_respawn_at
            FROM user_mgmt.allocations a
            LEFT JOIN user_mgmt.trader_supervisor_state ss
              ON ss.allocation_id = a.allocation_id
            WHERE a.status = 'active'
              AND a.runtime_state->>'phase' = 'active'
              AND a.runtime_state->>'date'  = %s
              AND (a.runtime_state->>'updated_at')::timestamptz < %s
            """,
            (today_str, threshold_ts),
        )
        return [dict(r) for r in cur.fetchall()]


# ── Respawn core (shared between supervisor + startup) ──────────────────────

def _attempt_respawn(conn, row: dict, *, source: str) -> bool:
    """Validate, release lock, respawn. Returns True on success.

    Every path through this function writes a decision record to
    trader_supervisor_state so post-hoc reconstruction of what the
    supervisor did is always possible from the DB.
    """
    aid = row["allocation_id"]
    short = aid[:8]
    state = row["runtime_state"] or {}
    now = datetime.now(timezone.utc)

    # Backoff check — identical guardrail for both paths.
    if _should_backoff(row):
        err = (f"BACKOFF: {row['respawn_count']} respawns within last "
               f"{BACKOFF_WINDOW_MIN}m, last at {row['last_respawn_at']}. "
               f"Manual intervention required.")
        _upsert_supervisor_state(conn, aid, last_error=err, last_error_at=now)
        log.error(f"  [{source}] {short}: {err}")
        return False

    # Corrupted-state guard — identical for both paths.
    err = _validate_runtime_state(state)
    if err:
        msg = f"CORRUPTED runtime_state: {err}. Refusing respawn."
        _upsert_supervisor_state(conn, aid, last_error=msg, last_error_at=now)
        log.error(f"  [{source}] {short}: {msg}")
        return False

    try:
        _release_lock(conn, aid)
        pid = spawn_allocation(aid)
    except Exception as e:
        msg = f"respawn failed: {e!r}"
        _upsert_supervisor_state(conn, aid, last_error=msg, last_error_at=now)
        log.exception(f"  [{source}] {short}: {msg}")
        return False

    new_count = int(row.get("respawn_count") or 0) + 1
    _upsert_supervisor_state(
        conn, aid,
        respawn_count=new_count,
        last_respawn_at=now,
        stale_detected_at=None,
        last_error=None,
        last_error_at=None,
    )
    log.warning(
        f"  [{source}] {short}: RESPAWNED pid={pid} respawn_count={new_count}"
    )
    return True


# ── Public entry points ─────────────────────────────────────────────────────

def supervisor_pass() -> int:
    """Cron-driven supervisor tick. Logs every decision so post-hoc
    reconstruction is possible from the log alone.
    """
    log.info("supervisor: starting pass")
    conn = _connect()
    try:
        ensure_schema(conn)

        stale_rows = _fetch_stale_rows(conn)
        log.info(f"supervisor: {len(stale_rows)} stale allocation(s) detected")

        for row in stale_rows:
            aid = row["allocation_id"]
            short = aid[:8]
            now = datetime.now(timezone.utc)
            prev_mark = row.get("stale_detected_at")

            # Two-tick confirmation:
            #   prev_mark None           → first detection; mark + skip
            #   prev_mark fresh (<12m)   → confirmed; attempt respawn
            #   prev_mark stale (≥12m)   → treat as new first-detection
            #                              (intervening healthy period lost
            #                              the confirmation chain)
            if prev_mark is None:
                _upsert_supervisor_state(conn, aid, stale_detected_at=now)
                log.info(f"  [tick1] {short}: first stale detection, "
                         f"will confirm on next tick")
                continue

            mark_age_min = (now - prev_mark).total_seconds() / 60
            if mark_age_min > STALE_CONFIRMATION_MAX_MIN:
                _upsert_supervisor_state(conn, aid, stale_detected_at=now)
                log.info(f"  [tick1] {short}: prior mark stale "
                         f"({mark_age_min:.1f}m old), resetting to first detection")
                continue

            # Confirmed — attempt respawn.
            log.info(f"  [tick2] {short}: confirmed stale, attempting respawn")
            _attempt_respawn(conn, row, source="supervisor")

        # Self-heal: clear stale_detected_at for any allocation that wrote
        # a bar between the stale-detection tick and this tick. Prevents
        # a false-positive respawn on the NEXT tick if the trader came
        # back to life organically.
        _clear_self_healed(conn)

        # Belt-and-suspenders: close portfolio_sessions rows left LIVE by a
        # SIGKILL (where the SIGTERM handler couldn't run) or any prior
        # mid-session death.
        swept = _sweep_orphaned_portfolio_sessions(conn)
        if swept:
            log.info(f"supervisor: swept {swept} orphan portfolio_session row(s)")

        log.info("supervisor: pass complete")
        return 0
    finally:
        conn.close()


def startup_recovery() -> int:
    """Backend-container boot hook. Respawns sessions whose trader
    subprocess was killed by the container restart.

    No two-tick confirmation — container restart is a known-deterministic
    event, not an ambiguous heartbeat hiccup. Uses STARTUP_STALE_BUFFER_MIN
    to avoid touching traders that are about to tick on a fast restart.
    Failures are swallowed and logged so a hung recovery pass never
    blocks the backend from coming up.
    """
    try:
        log.info("startup_recovery: starting")
        threshold_ts = (datetime.now(timezone.utc)
                        - timedelta(minutes=STARTUP_STALE_BUFFER_MIN))

        conn = _connect()
        try:
            ensure_schema(conn)
            orphans = _fetch_orphans_at_startup(conn, threshold_ts)
            log.info(f"startup_recovery: {len(orphans)} orphaned session(s) "
                     f"(runtime_state last updated before {threshold_ts.isoformat()})")

            for row in orphans:
                _attempt_respawn(conn, row, source="startup")

            # Close portfolio_sessions rows left LIVE by the prior process's
            # SIGKILL / hard kill, so the Portfolios tab reflects reality on
            # container boot.
            swept = _sweep_orphaned_portfolio_sessions(conn)
            if swept:
                log.info(f"startup_recovery: swept {swept} orphan portfolio_session row(s)")

            log.info("startup_recovery: complete")
            return 0
        finally:
            conn.close()
    except Exception as e:
        # Never block container startup on supervisor error.
        log.exception(f"startup_recovery: failed (non-blocking): {e!r}")
        return 1


# ── Orphaned portfolio_session sweep ────────────────────────────────────────

def _sweep_orphaned_portfolio_sessions(conn) -> int:
    """Close portfolio_sessions rows left as status='active' with no live trader.

    Two orphan classes:
      1. signal_date < today — trader moved past this day without a graceful
         close. Always orphan (the day is over; no trader is still writing).
      2. signal_date = today — runtime_state is stale (>15m) AND phase is not
         'active'. Catches SIGKILL + crash-after-finalize cases where the
         SIGTERM handler couldn't run.

    Matches the SIGTERM handler's exit_reason so both paths write the same
    label. Idempotent (WHERE status='active' guards re-runs).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            UPDATE user_mgmt.portfolio_sessions ps
               SET status        = 'closed',
                   exit_reason   = 'subprocess_died',
                   exit_time_utc = NOW(),
                   updated_at    = NOW()
              FROM user_mgmt.allocations a
             WHERE ps.allocation_id = a.allocation_id
               AND ps.status        = 'active'
               AND (
                     ps.signal_date < CURRENT_DATE
                  OR (
                       ps.signal_date = CURRENT_DATE
                       AND (a.runtime_state->>'phase') IS DISTINCT FROM 'active'
                       AND (NOW() - COALESCE((a.runtime_state->>'updated_at')::timestamptz, ps.session_start_utc))
                           > make_interval(mins => %s)
                     )
               )
          RETURNING ps.portfolio_session_id::text AS sid,
                    ps.signal_date,
                    ps.allocation_id::text        AS aid
            """,
            (STALE_THRESHOLD_MIN,),
        )
        closed = cur.fetchall()
    conn.commit()
    for r in closed:
        log.warning(
            f"  [sweep] closed orphan portfolio_session {r['sid'][:8]} "
            f"allocation={r['aid'][:8]} signal_date={r['signal_date']}"
        )
    return len(closed)


# ── Self-heal helper ────────────────────────────────────────────────────────

def _clear_self_healed(conn) -> None:
    """Clear stale_detected_at markers for allocations whose runtime_state
    has been updated within the last STALE_THRESHOLD_MIN (trader came
    alive organically between ticks)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT ss.allocation_id::text AS allocation_id
            FROM user_mgmt.trader_supervisor_state ss
            JOIN user_mgmt.allocations a ON a.allocation_id = ss.allocation_id
            WHERE ss.stale_detected_at IS NOT NULL
              AND a.runtime_state->>'updated_at' IS NOT NULL
              AND (NOW() - (a.runtime_state->>'updated_at')::timestamptz)
                  <= make_interval(mins => %s)
            """,
            (STALE_THRESHOLD_MIN,),
        )
        healed = [r["allocation_id"] for r in cur.fetchall()]
    for aid in healed:
        _clear_stale_marker(conn, aid)
        log.info(f"  [self-heal] {aid[:8]}: runtime_state writes resumed, "
                 f"clearing stale marker")


# ── Module entrypoint (for ad-hoc invocation / testing) ─────────────────────

def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return supervisor_pass()


if __name__ == "__main__":
    sys.exit(main())

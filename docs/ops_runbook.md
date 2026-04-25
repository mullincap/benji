# Ops Runbook

Short reference for managing the mcap host's production state. Pairs with
`docs/strategy_specification.md` (canonical strategy definition) and
`docs/open_work_list.md` (active + queued work items).

## Crontab

**Source of truth**: `ops/crontab.txt` in the repo. Host crontab should match
this file byte-for-byte. Drift between the two is an incident; reconcile by
copying the repo version to the host via the pattern below.

### View current host crontab

```bash
ssh mcap "crontab -l"
```

### Diff repo vs host

```bash
diff <(ssh mcap "crontab -l") ops/crontab.txt
```

### Apply repo crontab to host (with backup)

```bash
ssh mcap "TS=\$(date +%Y%m%d_%H%M%S) && cp <(crontab -l) /root/crontab_backup_\${TS}.bak"
ssh mcap "crontab" < ops/crontab.txt
ssh mcap "crontab -l" > /tmp/post_apply.txt
diff ops/crontab.txt /tmp/post_apply.txt  # should be empty
```

### Rollback crontab to a specific backup

```bash
ssh mcap "ls -la /root/crontab_backup_*.bak"   # find the backup
ssh mcap "crontab /root/crontab_backup_<TS>.bak"
```

## Signal generator

**Current**: `daily_signal_v2.py` (LIVE since 2026-04-23 05:58 UTC). Writes
both the deploys CSV and `user_mgmt.daily_signals` DB rows.

**Archived**: `archive/daily_signal_v1_host_snapshot_20260423.py` (the host
version as of cutover) and `archive/daily_signal_v1_archived_2026-04-23.py`
(same file, on mcap host).

### Verify v2 ran cleanly overnight

```bash
# Log output for today
ssh mcap "tail -40 /mnt/quant-data/logs/signal/cron.log | head -40"

# DB rows written for today
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \
  \"SELECT ds.signal_date, s.display_name, ds.sit_flat, ds.filter_name, ds.filter_reason, ds.computed_at \
    FROM user_mgmt.daily_signals ds \
    JOIN audit.strategy_versions sv USING (strategy_version_id) \
    JOIN audit.strategies s USING (strategy_id) \
    WHERE signal_date = CURRENT_DATE ORDER BY s.display_name;\""

# Expected: 3 rows (Alpha Low / Main / Max), all with same sit_flat + computed_at ~05:58 UTC.
```

### Emergency rollback from v2 to v1

If v2 fails at 05:58 UTC and allocations would miss their 06:05 UTC trade
session, restore v1:

```bash
# 1. Restore crontab (disables v2, re-enables v1)
ssh mcap "crontab /root/crontab_backup_cutover_20260423_043317.bak"

# 2. Restore v1 file on host
ssh mcap "cp /root/benji/archive/daily_signal_v1_archived_2026-04-23.py /root/benji/daily_signal.py"

# 3. Manually trigger v1 if it's past 05:58 UTC already (otherwise cron fires normally tomorrow)
ssh mcap ". /mnt/quant-data/credentials/secrets.env && /root/benji/pipeline/.venv/bin/python /root/benji/daily_signal.py"

# 4. Verify trader at 06:05 UTC has signals to work with
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \"SELECT COUNT(*) FROM user_mgmt.daily_signals WHERE signal_date = CURRENT_DATE;\""
```

Rollback should complete in < 5 minutes. Document failure mode in
`docs/strategy_specification.md § 11` and update `ops/crontab.txt`.

## Shadow diff (regression detector)

Runs daily at 06:15 UTC. Compares v2's live basket against a direct
`market.leaderboards` query for the same day (post-cutover, this is the
bug-catcher that fires if v2's on-the-fly computation ever drifts from the
builder's output). Also continues to log v1 vs v2 deltas (v1 no longer
active, so this side will be "unavailable" daily — harmless).

### View today's shadow diff

```bash
ssh mcap "tail -40 /mnt/quant-data/logs/signal/shadow_diff.log"
```

### Jaccard history

```bash
ssh mcap "cat /root/benji/daily_signal_shadow_diff_history.csv"
```

Expected post-cutover: `J(v2_yest, leaderboard_DB_yest) ≈ 1.000` every day.
If it drops below 0.95, investigate immediately — v2 has drifted from
canonical methodology, or a data-source issue has surfaced.

## Traders

**Current**: allocator-based spawn at 06:05 UTC via `spawn_traders`
(inside `benji-backend-1` container). Legacy master `trader-blofin.py` is
DISABLED since 2026-04-20.

### Check traders ran + didn't error

```bash
ssh mcap "tail -40 /mnt/quant-data/logs/trader/spawn.log"
ssh mcap "ls /mnt/quant-data/logs/trader/allocation_*.log | xargs -I {} tail -5 {}"
```

### Health endpoints (poll these from UptimeRobot)

- `https://mullincap.com/health` — basic backend liveness (HTTP 200 + `{"status":"ok"}`)
- `https://mullincap.com/health/trader` — returns 503 if any DB-active allocation has `runtime_state.phase='active'` but `updated_at` > 20 min ago. Catches dead trader subprocesses, BACKOFF state, stuck-lock recoveries.

## Trader incident playbooks

Trader subprocesses run inside the **same container as the process that
spawned them**, because `spawn_allocation()` uses `subprocess.Popen` (see
[spawn_traders.py:228-269](../backend/app/cli/spawn_traders.py#L228-L269),
docstring: "subprocess runs inside the same long-lived backend container as
the parent and survives the parent's exit"). Concretely:

| Spawn path | Container the trader lives in |
|---|---|
| `06:05` cron — `docker compose exec celery python -m app.cli.spawn_traders` | `benji-celery-1` |
| `*/5` supervisor cron — `docker compose exec celery python -m app.cli.trader_supervisor` | `benji-celery-1` |
| FastAPI startup hook — `app/main.py:resume_orphaned_traders` calls `startup_recovery()` | `benji-backend-1` (child of uvicorn) |

So the trader can be in **either** container depending on who last spawned
it. Verified 2026-04-25 18:50 UTC: PID 144 in benji-celery-1 (06:05 cron's
spawn). Verified 2026-04-25 19:05 UTC: PID 8 in benji-backend-1 (FastAPI
startup hook spawn after a backend rebuild).

**Critical implication — dup-spawn race:** when a backend rebuild fires
mid-session, the FastAPI startup hook's `startup_recovery()` runs, sees
`runtime_state.updated_at` older than `STARTUP_STALE_BUFFER_MIN`, and Popens
a NEW trader inside the backend container. If a celery-resident trader is
also alive (06:05 cron's spawn), both run in parallel, double-logging every
bar and double-calling exchange APIs. Mitigation: see
[trader_supervisor.py:74](../backend/app/cli/trader_supervisor.py#L74) — the
buffer was raised to match `STALE_THRESHOLD_MIN` (15 min) so a sleeping trader
between 5-min bars is no longer mistaken for orphaned.

The supervisor auto-respawns dead traders, but several failure modes need
manual SQL. Each playbook is a self-contained recipe; copy-paste verbatim.

### Playbook A: `/health/trader` returns 503 with `stale` entries

Surfaced when trader subprocess died and supervisor either hasn't respawned yet
OR is stuck in BACKOFF. Check supervisor state first:

```bash
ssh mcap "docker compose -f ~/benji/docker-compose.yml exec -T celery python3 -c '
import sys; sys.path.insert(0,\"/app/backend\")
from app.db import get_worker_conn
with get_worker_conn() as c:
    cur = c.cursor()
    cur.execute(\"SELECT respawn_count, last_respawn_at, last_error FROM user_mgmt.trader_supervisor_state\")
    for r in cur.fetchall(): print(r)
'"
```

- **`respawn_count >= 10` and `last_error` says BACKOFF**: do Playbook C
- **`respawn_count < 10` and `last_error` is None**: supervisor will respawn within 5–10 min; wait, then re-check `/health/trader`
- **`respawn_count < 10` but `last_error` mentions lock/CORRUPTED**: do Playbook B

### Playbook B: stuck DB lock blocking respawn

Symptom: supervisor keeps trying to respawn but every spawn exits immediately.
`last_error` mentions lock acquisition. Cause is usually a previous trader PID
died holding the lock (deploy or SIGKILL).

```bash
ssh mcap "docker compose -f ~/benji/docker-compose.yml exec -T celery python3 -c '
import sys; sys.path.insert(0,\"/app/backend\")
from app.db import get_worker_conn
ALLOC = \"<allocation_id>\"
with get_worker_conn() as c:
    cur = c.cursor()
    cur.execute(\"UPDATE user_mgmt.allocations SET lock_acquired_at=NULL WHERE allocation_id=%s\", (ALLOC,))
    c.commit()
    print(\"lock cleared\")
'"
```

Replace `<allocation_id>` with the affected allocation_id (from `/health/trader` body).
After this, supervisor's next 5-min tick will respawn cleanly.

### Playbook C: BACKOFF — supervisor refuses to respawn

Symptom: `respawn_count >= 10` AND `last_error: BACKOFF: N respawns within last 60m...`.
The supervisor has given up. Need to reset counter, clear lock, and manually spawn.

```bash
ssh mcap "docker compose -f ~/benji/docker-compose.yml exec -T celery python3 << 'PYEOF'
import sys; sys.path.insert(0, '/app/backend')
from app.db import get_worker_conn
from app.cli.spawn_traders import spawn_allocation

ALLOC = '<allocation_id>'

with get_worker_conn() as c:
    cur = c.cursor()
    cur.execute('UPDATE user_mgmt.trader_supervisor_state SET respawn_count=0, stale_detected_at=NULL, last_error=NULL, last_error_at=NULL WHERE allocation_id=%s', (ALLOC,))
    cur.execute('UPDATE user_mgmt.allocations SET lock_acquired_at=NULL WHERE allocation_id=%s', (ALLOC,))
    c.commit()
    print('cleared BACKOFF + lock')

pid = spawn_allocation(ALLOC)
print(f'spawned pid={pid}')
PYEOF
"
```

Verify recovery within 30 sec:

```bash
curl -s https://mullincap.com/health/trader | python3 -m json.tool
# Expect: status=ok, allocation in `healthy`, age_min < 5
```

### Playbook D: backend container down (`/health` 503 or no response)

Symptom: UptimeRobot alerts on `/health`, not just `/health/trader`. Backend
container died, network issue, or host down.

First diagnose:

```bash
ssh mcap "docker compose -f ~/benji/docker-compose.yml ps"
# Look for benji-backend-1 status. If "Exit" or missing, container died.
```

If container died:

```bash
# Restart backend ONLY — do not rebuild (rebuild kills trader subprocesses).
ssh mcap "docker compose -f ~/benji/docker-compose.yml start backend"
```

This brings the container back without recreating it (preserves any in-flight
trader subprocesses if they survived). After backend is up, check:

```bash
curl -s https://mullincap.com/health
curl -s https://mullincap.com/health/trader | python3 -m json.tool
```

If `/health/trader` now shows stale, do Playbook A.

### Playbook E: dup-spawn (trader running in BOTH containers)

Symptom: bar entries appear twice in the per-allocation log within ~50ms of
each other, both with identical `incr/peak/sess/active/stopped` values.
Confirmed via `grep -c "Bar N "` on the log file.

Cause: both `benji-celery-1` and `benji-backend-1` have a live trader
process for the same allocation. Most commonly happens after a backend
container restart (or rebuild) on pre-fix code where
`STARTUP_STALE_BUFFER_MIN = 2` — the FastAPI startup hook respawns
inside backend even though the celery-resident trader is alive.
Post-fix (`STARTUP_STALE_BUFFER_MIN = STALE_THRESHOLD_MIN = 15`), this
should not occur unless something else is concurrently spawning.

Recovery:

```bash
# 1. Confirm dup by listing trader processes in both containers
ssh mcap 'for c in benji-backend-1 benji-celery-1; do echo "=== $c ==="; docker exec $c sh -c "for f in /proc/[0-9]*/cmdline; do c=\$(tr \"\\0\" \" \" < \$f 2>/dev/null); case \"\$c\" in *trader_blofin*allocation-id*) p=\${f#/proc/}; p=\${p%/cmdline}; echo \"PID=\$p :: \$c\";; esac; done"; done'

# 2. Identify which to kill. Prefer keeping the OLDER process (it has the
#    full in-memory state — peak, sym_stopped, positions tracked across the
#    whole session). Kill the newer one. Compare wall-clock start times:
ssh mcap 'docker exec <CONTAINER> sh -c "awk \"{print \$22}\" /proc/<PID>/stat | awk -v b=\$(grep ^btime /proc/stat | awk \"{print \\\$2}\") \"{printf \\\"%d\\n\\\", b + \$1/100}\""'
# Compare the two; lower number = older.

# 3. SIGKILL the duplicate. Use SIGKILL not SIGTERM:
#    SIGTERM would fire _release_held_lock_on_shutdown, which marks the
#    portfolio_session as 'subprocess_died' — that closes the live session
#    in the DB even though the surviving trader is still running.
ssh mcap "docker exec <CONTAINER> sh -c 'kill -9 <DUP_PID>'"

# 4. Verify only one process remains
ssh mcap 'for c in benji-backend-1 benji-celery-1; do echo "=== $c ==="; docker exec $c sh -c "for f in /proc/[0-9]*/cmdline; do c=\$(tr \"\\0\" \" \" < \$f 2>/dev/null); case \"\$c\" in *trader_blofin*allocation-id*) echo \$c;; esac; done"; done'

# 5. Audit the exchange for duplicate orders placed during the dup window.
#    Both processes ran position_reconcile + balance fetch every bar; if a
#    SYM_STOP fired during the window, both would have tried to close it.
#    Check BloFin's recent fills and SL orders for the affected symbols
#    around the dup-window timeframe.
```

After SIGKILL, the surviving trader keeps running normally. `lock_acquired_at`
is whatever the killed process last set it to (harmless — neither process
re-checks during operation). `runtime_state` continues being updated by the
survivor. No further action needed unless step 5 reveals duplicate orders.

### Playbook F: emergency flatten all positions on an allocation

For when you need to exit cleanly without waiting for trader. Uses the same
close routine as the trader's port_sl path (idempotent, reconciles against
exchange).

Via UI: Manager → Allocations → click allocation → "Close Positions" button.

Via CLI (if UI down):

```bash
ssh mcap "docker compose -f ~/benji/docker-compose.yml exec -T celery python3 << 'PYEOF'
import sys; sys.path.insert(0, '/app/backend')
from app.db import get_worker_conn
from app.services.trading.credential_loader import load_credentials
from app.services.exchanges.adapter import adapter_for
from app.cli.trader_blofin import close_all_positions

ALLOC = '<allocation_id>'

with get_worker_conn() as c:
    cur = c.cursor()
    cur.execute('SELECT connection_id::text, runtime_state FROM user_mgmt.allocations WHERE allocation_id=%s', (ALLOC,))
    conn_id, rs = cur.fetchone()

positions = rs.get('positions') or []
print(f'positions to close: {len(positions)}')

if positions:
    creds = load_credentials(conn_id)
    api = adapter_for(creds)
    failed = close_all_positions(api, positions, reason='manual flatten via runbook', dry_run=False)
    print(f'closed_ok={len(positions)-len(failed)}  failed={len(failed)}')

# Pause the allocation so spawn_traders skips it tomorrow
with get_worker_conn() as c:
    cur = c.cursor()
    cur.execute(\"UPDATE user_mgmt.allocations SET status='paused' WHERE allocation_id=%s\", (ALLOC,))
    c.commit()
PYEOF
"
```

To resume trading after a flatten: change `status='paused'` back to `'active'`
in the DB. Next 06:05 UTC spawn picks it up.

## Deploy safety

Container-level deploy impact (assumes the post-2026-04-25 fix to
`STARTUP_STALE_BUFFER_MIN` has shipped — see "dup-spawn race" note in Trader
incident playbooks above; before that fix, `--no-deps backend` was also unsafe):

| Command | Trader impact |
|---|---|
| `docker compose up -d --build --no-deps frontend` | Safe anytime |
| `docker compose up -d --build --no-deps nginx` / `restart nginx` | Safe anytime |
| `docker compose up -d --build --no-deps backend` | Safe anytime *(post-fix)*; triggers `startup_recovery()` but the 15-min staleness threshold prevents false-positive respawn on a sleeping trader |
| `docker compose up -d --build --no-deps celery` | **KILLS TRADER** if it lives in celery (06:05 / supervisor cron spawn) |
| `./redeploy.sh` (full `--force-recreate`) | **KILLS TRADER** + dup-spawn risk: recreates celery (kills celery-resident trader) AND fires backend startup hook (spawns in backend) |
| `git pull` only (cron-driven host scripts) | Safe |

Hard rules:

1. **Never run `./redeploy.sh` mid-session** (06:00 UTC → session close, typically
   23:00 UTC). The full recreation kills any celery-resident trader AND fires
   backend's startup hook, which on the *old* `STARTUP_STALE_BUFFER_MIN=2`
   code respawns the celery-spawned trader inside backend before celery comes
   back up — root cause of the 2026-04-25 19:05 UTC dup-spawn.
2. **`--no-deps celery` is unsafe mid-session if a celery-resident trader is
   running.** Always check first (see "Pre-flight: locate the trader" below).
   If celery code MUST ship, follow the "Deploying with running traders"
   procedure.
3. **`--no-deps backend` is safe post-fix.** The startup hook's
   `STARTUP_STALE_BUFFER_MIN = STALE_THRESHOLD_MIN` (15 min) is wider than
   the 5-min bar interval, so a sleeping trader is never mistaken for orphaned.
4. **`--no-deps frontend` and `restart nginx` are unconditionally safe.**

### Pre-flight: locate the trader

Before any rebuild during 06:05–close, find which container holds the trader
so you know whether the rebuild will kill it:

```bash
ssh mcap 'for c in benji-backend-1 benji-celery-1; do
  echo "=== $c ===";
  docker exec $c sh -c "for f in /proc/[0-9]*/cmdline; do c=\$(tr \"\\0\" \" \" < \$f 2>/dev/null); case \"\$c\" in *trader_blofin*allocation-id*) p=\${f#/proc/}; p=\${p%/cmdline}; echo \"PID=\$p :: \$c\";; esac; done";
done'
```

Possible outcomes:
- Trader in `benji-celery-1` only → backend rebuild safe (post-fix); celery
  rebuild requires Approach B
- Trader in `benji-backend-1` only → celery rebuild safe; backend rebuild
  requires Approach B
- Trader in BOTH → dup-spawn already in progress; SIGKILL the duplicate
  (typically the newer one; see Playbook D below) before any further action
- Trader in neither → check `/health/trader` and supervisor state; either
  session is genuinely closed or both spawn paths have failed

If you MUST deploy celery mid-session: be ready to run Playbook A/B/C if the
respawn race deadlocks.

### Deploying with running traders

For backend / frontend / nginx changes, no special procedure — those containers
are independent of the trader. This section is **only for celery rebuilds**,
which kill the trader subprocess.

Two locks matter when the celery container restarts:

1. **`pg_advisory_lock`** (session-scoped, keyed on connection_id) — auto-releases
   when the trader's DB connection drops. No manual cleanup ever needed.
2. **`allocations.lock_acquired_at`** (DB column) — the stuck-prone one. The
   trader's SIGTERM handler at `trader_blofin.py:_release_held_lock_on_shutdown`
   clears it on graceful shutdown (added after 2026-04-24 06:05 UTC incident).
   `docker compose` sends SIGTERM with a 10s grace before SIGKILL, so the happy
   path is clean. SIGKILL / OOM bypass the handler — that's when the lock sticks.
   The CAS at `trader_blofin.py:_acquire_allocation_lock` self-heals after 24h
   (`lock_acquired_at < NOW() - INTERVAL '24 hours'`), but that's too long mid-session.

#### Approach A — defer celery to off-hours (recommended)

For most celery code changes (audit pipeline edits, supervisor tweaks, trader
logic): ship after session close (~23:00 UTC) or before 06:00 UTC the next day.

```bash
git push origin main
ssh mcap "cd ~/benji && git pull && docker compose up -d --build --no-deps celery"
```

Backend / frontend changes can ship anytime independently:

```bash
# Backend route or schema change — safe mid-session
ssh mcap "cd ~/benji && git pull && docker compose up -d --build --no-deps backend"

# Frontend change — safe mid-session, restart nginx if you want to clear cache
ssh mcap "cd ~/benji && docker compose up -d --build --no-deps frontend"
ssh mcap "cd ~/benji && docker compose restart nginx"
```

#### Approach B — celery rebuild mid-session (SIGKILL + manual respawn)

Verified working multiple times on 2026-04-25 with ~30s of trader downtime
per rebuild and **zero unintended portfolio_session closures**. The key
trick: SIGKILL the trader BEFORE `docker compose up` rather than letting
docker's SIGTERM hit it. SIGTERM fires the trader's
`_release_held_lock_on_shutdown` handler which marks the active
portfolio_session as `subprocess_died` and closes it — bad mid-session
because the UI will show the day's session as ended even though the
trader resumes via runtime_state. SIGKILL bypasses the handler, leaving
the portfolio_session as `active` and the lock as-is; we then clear the
lock manually and let the new spawn re-acquire.

The whole sequence is one ssh command — fast and atomic:

```bash
# 1. Pre-flight snapshot (optional but recommended)
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \
  \"SELECT allocation_id, lock_acquired_at, runtime_state->>'phase' AS phase, \
           runtime_state->>'bar' AS bar \
    FROM user_mgmt.allocations WHERE runtime_state->>'phase' = 'active';\""

# 2. Push the code first
git push origin main

# 3. SIGKILL → clear lock → pull → rebuild celery → manual spawn (single ssh).
#    Replace <PID> with the trader PID inside benji-celery-1 (from /proc scan)
#    and <ALLOC_ID> with the allocation_id from step 1.
ssh mcap "
  docker exec benji-celery-1 sh -c 'kill -9 <PID>' &&
  PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \
    \"UPDATE user_mgmt.allocations SET lock_acquired_at = NULL
      WHERE allocation_id='<ALLOC_ID>'::uuid;\" &&
  cd ~/benji && git pull &&
  docker compose up -d --build --no-deps celery &&
  docker compose exec -T celery python -m app.cli.spawn_traders
"

# 4. Verify new trader is alive and the new code shipped
ssh mcap 'docker exec benji-celery-1 sh -c "for f in /proc/[0-9]*/cmdline; do c=\$(tr \"\\0\" \" \" < \$f 2>/dev/null); case \"\$c\" in *trader_blofin*allocation-id*) p=\${f#/proc/}; p=\${p%/cmdline}; echo \"PID=\$p :: \$c\";; esac; done"'

# 5. Wait for the next 5-min bar boundary, confirm bar advances
ssh mcap "tail -5 /mnt/quant-data/logs/trader/allocation_<ALLOC_ID>_$(date -u +%Y-%m-%d).log"
```

Expected outcomes after step 3:
- `kill -9 <PID>` → silent, no output if successful
- `UPDATE user_mgmt.allocations` → `UPDATE 1`
- `git pull` → fast-forwards
- `docker compose up -d --build --no-deps celery` → image build (~45s) + container restart
- `docker compose exec -T celery python -m app.cli.spawn_traders` →
  `Found 1 eligible allocation(s).` followed by `Spawned allocation <ID>: pid=<NEW_PID>`

Total elapsed: ~60-90s. **Trader downtime: ~30-35s** (PID kill → new PID
entering monitoring loop). One bar typically missed; positions remain
managed by exchange-side SL during the gap.

Failure modes:
- Step 3 spawn returns "Skipping allocation: already finished today" →
  runtime_state.phase is in TERMINAL_PHASES (closed/errored). Manually
  reset to 'active' first: `UPDATE allocations SET runtime_state = jsonb_set(runtime_state, '{phase}', '"active"'::jsonb) WHERE allocation_id='<ID>'::uuid`.
- New spawn exits "already locked or stale-but-recent" → step 3's
  lock-clear didn't apply. Re-run step 3's UPDATE then call
  `spawn_traders` again.
- Supervisor `last_error` mentions BACKOFF → Playbook C.
- No `Spawn at …` block in log after step 5 → spawn never reached the
  monitoring loop. Check the log file for tracebacks (credential
  decrypt errors, exchange connection failures, etc.).

#### Verified deploy patterns (2026-04-25 session log)

Empirical data from the 2026-04-25 session — useful as a reference for
"what should this take?" and "is anything anomalous about this run?":

| Type | Code-to-deployed wall-clock | Trader downtime | Bars missed |
|---|---:|---:|---:|
| `--no-deps frontend` | ~90s | **0** | 0 |
| `--no-deps backend frontend` (parallel) | ~3min | **0** *(post-fix)* | 0 |
| `--no-deps celery` (Approach B) | ~90s | ~30-35s | 1 |
| `--no-deps backend frontend` w/ bar-boundary timing | ~8min | **0** *(pre-fix)* | 0 |

Pre-fix vs post-fix refers to commit `ff36f59`
(`STARTUP_STALE_BUFFER_MIN: 2 → 15`). With pre-fix code in production,
backend rebuilds had to be timed within 2min of a 5-min bar boundary
to avoid the FastAPI startup hook spuriously respawning. Post-fix,
backend rebuilds are safe at any moment.

Total session: 9 mid-session deploys, ~96s aggregate trader downtime,
3 bars missed across the entire trading day, 0 lost portfolio sessions,
0 duplicate orders placed. One dup-spawn caught and resolved within 5
min via Playbook E.

## Database checks

### Today's signals + basket items

```bash
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \"
SELECT ds.signal_date, s.display_name, dsi.rank, sym.base
FROM user_mgmt.daily_signals ds
JOIN audit.strategy_versions sv USING (strategy_version_id)
JOIN audit.strategies s USING (strategy_id)
LEFT JOIN user_mgmt.daily_signal_items dsi ON dsi.signal_batch_id = ds.signal_batch_id
LEFT JOIN market.symbols sym USING (symbol_id)
WHERE signal_date = CURRENT_DATE
ORDER BY s.display_name, dsi.rank;\""
```

### Latest leaderboards coverage

```bash
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \
  \"SELECT MAX(timestamp_utc) FROM market.leaderboards WHERE metric='price' AND anchor_hour=0;\""
```

Canonical expectation: within 24h of now. Older means the nightly indexer
(01:00 UTC) is broken.

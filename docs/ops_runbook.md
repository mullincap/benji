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

The trader subprocess lives inside `benji-celery-1` (spawned by `*/5` supervisor
cron via `docker compose exec celery python -m app.cli.trader_supervisor` and
by celery's startup hook). Verified 2026-04-25 18:50 UTC: `PID 144` was running
`python3 -m app.cli.trader_blofin --allocation-id …` inside the celery container,
not the backend container.

Implication: **celery rebuilds kill the trader; backend / frontend rebuilds do
not.** This is the inverse of what an earlier version of this runbook claimed.

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

### Playbook E: emergency flatten all positions on an allocation

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

Trader subprocesses live in `benji-celery-1`. Container-level deploy impact:

| Command | Trader impact | Notes |
|---|---|---|
| `docker compose up -d --build --no-deps backend` | **Safe** anytime | Restarts uvicorn only |
| `docker compose up -d --build --no-deps frontend` | **Safe** anytime | Different container |
| `docker compose up -d --build --no-deps nginx` / `restart nginx` | **Safe** anytime | Reverse proxy only |
| `docker compose up -d --build --no-deps celery` | **KILLS TRADER** | Mid-session: avoid |
| `./redeploy.sh` (full `--force-recreate`) | **KILLS TRADER** | Recreates celery |
| `git pull` only (cron-driven host scripts) | Safe | No container change |

Hard rules:

1. **Never run `./redeploy.sh` mid-session** (06:00 UTC → session close, typically
   23:00 UTC). It recreates the celery container, kills trader subprocesses,
   and triggers the dup-spawn race that has caused every BACKOFF incident to date.
2. **`--no-deps celery` is also unsafe mid-session** — same kill mechanism as
   redeploy.sh, just narrower in scope. If celery code MUST ship mid-session,
   follow the "Deploying with running traders" procedure below.
3. **`--no-deps backend` and `--no-deps frontend` are both safe anytime.** A
   2026-04-25 mid-session deploy of `manager.py` rebuilt the backend container
   end-to-end and the trader at PID 144 in celery never noticed.

If you MUST deploy celery mid-session: be ready to run Playbook A/B/C if the
respawn race deadlocks (it usually does).

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

#### Approach B — celery rebuild mid-session with lock sweep

Higher risk; only when an in-flight bug fix MUST land before session close.
Wraps the celery rebuild with a pre-flight snapshot, post-rebuild lock sweep,
and respawn verification:

```bash
# (a) Snapshot trader state before rebuild
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \
  \"SELECT allocation_id, lock_acquired_at, runtime_state->>'phase' AS phase, \
           runtime_state->>'bar' AS bar \
    FROM user_mgmt.allocations \
    WHERE runtime_state->>'phase' = 'active' \
    ORDER BY allocation_id;\""

# Pre-flight: count live trader subprocesses inside celery
ssh mcap 'docker exec benji-celery-1 sh -c "for f in /proc/[0-9]*/cmdline; do c=\$(tr \"\\0\" \" \" < \$f 2>/dev/null); case \"\$c\" in *trader_blofin*allocation-id*) echo \$c;; esac; done | wc -l"'

# (b) Push + rebuild celery (SIGTERM → 10s grace → SIGKILL → restart)
git push origin main
ssh mcap "cd ~/benji && git pull && docker compose up -d --build --no-deps celery"

# (c) Wait for SIGTERM handler + container start + supervisor first tick
sleep 30

# (d) Sweep stuck locks — the > 5min guard avoids yanking freshly-acquired locks
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \
  \"UPDATE user_mgmt.allocations SET lock_acquired_at = NULL \
    WHERE runtime_state->>'phase' = 'active' \
      AND lock_acquired_at IS NOT NULL \
      AND lock_acquired_at < NOW() - INTERVAL '5 minutes';\""

# (e) Verify each trader respawned and resumed (bar should be ≥ pre-flight bar)
ssh mcap "PGPASSWORD=A psql -h 127.0.0.1 -U quant -d marketdata -c \
  \"SELECT allocation_id, runtime_state->>'phase' AS phase, \
           runtime_state->>'bar' AS bar, \
           NOW() - (runtime_state->>'updated_at')::timestamptz AS staleness \
    FROM user_mgmt.allocations \
    WHERE runtime_state->>'phase' = 'active';\""

# Confirm a Spawn block landed in the per-allocation log post-rebuild
ssh mcap "tail -50 /mnt/quant-data/logs/trader/allocation_<aid>_$(date -u +%Y-%m-%d).log | grep -E 'Spawn at|Entering 5-min monitoring'"

# Process count should match pre-flight
ssh mcap 'docker exec benji-celery-1 sh -c "for f in /proc/[0-9]*/cmdline; do c=\$(tr \"\\0\" \" \" < \$f 2>/dev/null); case \"\$c\" in *trader_blofin*allocation-id*) echo \$c;; esac; done | wc -l"'

curl -s https://mullincap.com/health/trader | python3 -m json.tool
# ↑ status=ok; if 503 with stale entries, do Playbook A/B/C
```

Failure modes after step (b):
- Supervisor logs `last_error` mentioning lock acquisition → Playbook B
- `respawn_count >= 10` with BACKOFF → Playbook C
- Trader process count short of pre-flight after step (e) → check
  `trader_supervisor_state.last_error` and route to the matching playbook
- No `Spawn at …` block in the per-allocation log → trader was never respawned;
  the supervisor cron will retry within 5 min, OR run Playbook B/C immediately

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

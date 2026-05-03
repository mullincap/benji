# Incident: Ghost Trader Log Writer + Sub-Account Drain

**Date:** 2026-05-03
**Branch label:** `incident-2026-05-03-trader-forensics`
**Status:** forensic investigation complete; remediation deferred
**Allocation:** `f87fe130-a90c-4e60-908a-14f4065b415c` (alts_main v1, status=`active`)
**Connection:** `5f51a294-4254-4af0-91bb-e36cf38a498b` (BloFin)
**Capital exposure:** $0 — proven (sub-account drained 2026-05-02 19:07; `total_equity_usd=0` across all 159 snapshots on 2026-05-03)

## Forensic preservation rules

Failure state is preserved for a future scheduled debug session. Do **not**:

- close the allocation row
- kill the ghost log writer
- clear/refresh `trader_supervisor_state`
- restart any service tied to this allocation

J withdrew the remaining BloFin balance and reset the principal anchor to May-2 (backfills $0 for 7 prior days for a clean visual). Allocation is dormant via the strategy filter; the platform-level state mismatch is what we want to study.

---

## C1 — Locate the log writer

**Tools attempted:**

- `docker top` across all 7 containers (mid-bar at 12:42, between 12:40 and 12:45 ticks): only uvicorn / celery worker / celery-beat / nginx visible. No trader process anywhere.
- Host `ps auxf`: only zombies (`[python] <defunct>`, ppid=uvicorn) for previously spawned trader PIDs 1113265 (yesterday) and 1577290 (today 06:35).
- `lsof <log>` and `fuser <log>` on host: no holders.
- `find /proc -inum 29360189` on host and inside every running container: zero matches.
- `inotifywait` on host: **not installed** (`command not found`). Per instruction not to install, technique unavailable at time of report.
- `auditctl`: skipped per instruction.
- `strace -p 1017117 -p 884221 -p 720564 -f -e trace=openat,write,close,creat,rename,renameat -t -y -s 200` on uvicorn + celery-worker + celery-beat for 360s: launched at 12:50:47, no matching events captured at point of forensics close.

**Mechanical verification of writes:** between 12:42:24 and 12:47:22 (5min window), file size grew from 23472 → 23772 bytes, mtime advanced from 12:40:04 → 12:45:04, inode unchanged at 29360189. So writes are in-place appends — **not** an atomic-rename pattern. Yet no process ever holds an fd to that inode at any moment sampled.

**Writes are still happening** — Bar 81 at 12:45:00 confirmed:

```
2026-05-03 12:45:00,000 [INFO] [alloc f87fe130] Bar  81 | incr=-2.418% ... active=2/4  stopped=2
2026-05-03 12:45:00,000 [INFO] [alloc f87fe130] | actual_roi=-6.529%  equity=$93.47 ...
```

`actual_roi` matches `incr×lev×0.90` exactly with `delta=+0.000%` on every bar — confirming the log values are computed from a model, not from BloFin equity.

**C1: GHOST_CONFIRMED** (with caveat: inotifywait not available at time of investigation, and strace results inconclusive within the bounded window. Every standard-tool observation says no live writer process exists.)

---

## C2 — BloFin's view of the sub-account

**Connection record** (key fields, credentials redacted):

```
connection_id          | 5f51a294-4254-4af0-91bb-e36cf38a498b
exchange               | blofin
testnet                | f
status                 | active
last_validated_at      | 2026-04-18 01:58:32  ← never re-validated since creation
last_error_at          | (null)
last_error_msg         | (null)
principal_anchor_at    | 2026-05-02 22:35:00
principal_baseline_usd | 1036.7413
created_at             | 2026-04-18 01:58:31
last_permissions       | {readOnly: 0, apiName: "3mweb", expireTime: 1783512081301}
```

API key permissions: `readOnly=0` → **trading scope enabled**. Expires `1783512081301` ms = **2026-05-04** (1 day after incident).

**Most recent 3 snapshot payloads — `fetch_ok=true`, `positions=[]`, no `error_msg`:**

```
snapshot_at      | 2026-05-03 12:40:03  | total=$0  available=$0  positions=[]  fetch_ok=t  error_msg=(null)
snapshot_at      | 2026-05-03 12:35:02  | total=$0  available=$0  positions=[]  fetch_ok=t  error_msg=(null)
snapshot_at      | 2026-05-03 12:30:02  | total=$0  available=$0  positions=[]  fetch_ok=t  error_msg=(null)
```

Empty `positions` JSONB array (`[\n]`), not null. This is BloFin returning a **real empty-account response**, not an auth-failure-misreported-as-empty.

**14-day daily snapshot summary:**

| Day | Rows | Rows w/ Equity | Max Equity | Errors |
|---|---|---|---|---|
| 2026-04-20 | 108 | 108 | $2,971 | 0 |
| 2026-04-21 | 427 | 427 | $3,517 | 0 |
| 2026-04-22 | 497 | 497 | $3,419 | 0 |
| 2026-04-23 | 320 | 320 | $4,490 | 0 |
| 2026-04-24 | 491 | 491 | $4,876 | 0 |
| 2026-04-25 | 417 | 417 | $5,802 | 0 |
| 2026-04-26 | 308 | 308 | $5,870 | 0 |
| 2026-04-27 | 294 | 294 | $4,803 | 0 |
| 2026-04-28 | 297 | 297 | $4,800 | 0 |
| 2026-04-29 | 302 | 302 | $4,436 | 0 |
| 2026-04-30 | 293 | 293 | $4,333 | 0 |
| 2026-05-01 | 591 | 591 | $3,493 | 0 |
| **2026-05-02** | **304** | **246** | **$1,037** | **0** |
| **2026-05-03** | **159** | **0** | **$0** | **0** |

**C2 finding:** Connection is alive and authenticating fine. BloFin honestly reports the sub-account holds $0. The transition is sharp: 2026-05-02 had 58 zero-equity rows (drainage during the day), 2026-05-03 has been zero from row 1.

---

## C3 — Allocation lifecycle

**Allocation row** (current state):

```
allocation_id       | f87fe130-a90c-4e60-908a-14f4065b415c
user_id             | ea44b15d-bd76-4bfa-bad7-012e8c17d8f4
strategy_version_id | 5cb04dc8-053e-4808-aaf8-c14a451c06af
connection_id       | 5f51a294-4254-4af0-91bb-e36cf38a498b
capital_usd         | 100.00
status              | active                          ← still flagged active
created_at          | 2026-04-20 08:14:58
updated_at          | 2026-05-03 06:35:02             ← today's trader start
closed_at           | (null)
runtime_state       | {"phase": "filtered",
                       "positions": [],
                       "date": "2026-05-03",
                       "updated_at": "2026-05-03 06:35:02"}
```

`runtime_state.phase = "filtered"` — today's trader **opened, evaluated, decided not to trade, set phase=filtered, exited at 06:35:02**. No positions opened in DB. The 80+ ghost-bars in the log file are not from today's trader.

**Capital events — the smoking gun:**

```
event_at              | kind        | amount_usd | source | tx
----------------------+-------------+------------+--------+------
2026-05-02 19:07:23   | WITHDRAWAL  |  $1036.87  | auto   | Polygon POS 0x802eed6b...
2026-05-01 22:18:35   | withdrawal  |  $199.97   | auto   | Polygon POS 0x71172a5a...
2026-04-30 03:13:09   | withdrawal  |  $199.97   | auto   | Polygon POS 0x11cfd069...
2026-04-23 08:43:20   | deposit     |  $1021.79  | auto   | Polygon POS 0xb2d10ab6...
2026-04-23 03:06:49   | withdrawal  |  $179.97   | auto   | Polygon POS 0xcfc8d5de...
2026-04-20 17:08:39   | withdrawal  |  $979.97   | auto   | Polygon POS 0x41d867bc...
```

The **2026-05-02 19:07 auto-withdrawal of $1,036.87** matches `principal_baseline_usd=1036.74` (off by ~$0.13 in fees). This is the capital-events auto-poll cron picking up an on-chain Polygon withdrawal that drained the sub-account.

**Cross-table presence:**

```
src                 | rows | most_recent
--------------------+------+-----------------------
capital_events      |    6 | 2026-05-02 19:07:23
returns             |   13 | 2026-05-03 06:35:02   ← today, exit_reason='filtered', net=0%, capital_deployed=$0
deployments         |    0 | (none ever)
portfolio_sessions  |   11 | 2026-05-03 06:35:00
execution_symbols   |   22 | 2026-05-01 07:35:05
performance_daily   |    6 | 2026-05-01 07:35:05
```

**Recent sessions / failure pattern:**

```
signal_date  | status | exit_reason     | bars | duration         | final_return | eff_lev
-------------+--------+-----------------+------+------------------+--------------+--------
2026-05-03   | closed | subprocess_died |    2 | 06:35:00–06:44   | -0.19%       | 3.00
2026-05-02   | closed | subprocess_died |    1 | 06:35:00–06:44   | +0.60%       | 3.00
2026-05-01   | closed | early_fill      |   10 | 06:35:13–07:35   | +4.17%       | 2.14
2026-04-30   | closed | session_close   |  204 | 06:35:00–23:55   | -2.99%       | 1.50
2026-04-29   | closed | subprocess_died |    3 | 06:35:00–06:46   | -0.36%       | 3.00
2026-04-28   | closed | session_close   |  207 | 06:35:36–23:55   | -3.07%       | 3.00
2026-04-27   | closed | filtered        |    1 | 06:35:00–06:36   |  0.00%       | 3.00
2026-04-25   | closed | session_close   |  187 | 06:36:23–23:55   | +6.29%       | 3.00
```

**`trader_supervisor_state`:**

```
allocation_id     | f87fe130-...
stale_detected_at | (null)
respawn_count     | 0
last_respawn_at   | (null)
last_error        | (null)
updated_at        | 2026-05-02 01:45:03   ← stale by 35 hours
```

**Supervisor chronology** (selected):

- 2026-04-25 14:47–15:55: 11 respawns within 60min, BACKOFF triggered, manual intervention required, then self-heal.
- 2026-04-30 08:28: subprocess_died, respawned successfully → ran till session_close.
- 2026-05-01 07:20: subprocess_died, respawned → exited via early_fill at 07:35.
- 2026-05-02 06:44: orphan portfolio_session sweep (subprocess died ~9min in, no respawn).
- 2026-05-03 06:44: same — orphan sweep, no respawn.

**C3 timeline:**

- 2026-04-18: Connection created.
- 2026-04-20: Allocation created at $100 capital.
- **2026-04-20 → 2026-04-30:** healthy daily sessions (full-day session_close runs, peak equity $5,870).
- 2026-05-01 onwards: **chronic `subprocess_died` after ~9 min** every morning. Strategy filter has been killing entries (today, 5/2, 4/29, 4/27 all "filtered" or 1–3 bar exits).
- **Last real heartbeat:** `trader_supervisor_state.updated_at = 2026-05-02 01:45` (>35h ago).
- **2026-05-02 19:07:** auto-withdrawal of $1,037 from BloFin sub-account drained capital.
- **2026-05-03 06:35:** today's trader started, immediately filtered, exited cleanly with `phase=filtered`. Supervisor swept orphan session at 06:44.
- **2026-05-03 06:35 → present:** ghost log writer continues writing fake bars. `allocation.status` remains "active" despite the underlying connection being drained.

**What changed:** capital exited via auto-withdrawal on 2026-05-02 19:07 — but the allocation status was never closed and the strategy is still scheduled to spawn each morning. The ghost log writer is an orthogonal mystery.

---

## C4 — Strategy version classification

```
strategy_version_id  | 5cb04dc8-053e-4808-aaf8-c14a451c06af
strategy_id          | 5
strategy_name        | alts_main
version_label        | v1
config_hash          | 4970027dadc802b70e76eb330125b48b8fd716881ef0b004f62977f411ae2930
is_active            | TRUE
published_at         | 2026-04-23 11:10:40
sv_created           | 2026-04-23 11:10:40
metrics_data_through | 2026-05-02
```

```
sibling_versions_active | 1   ← only one active version for strategy_id=5
```

Only one version of `alts_main` ever created (v1, 2026-04-23). It is the active/published version. Per `idx_one_active_version_per_strategy` partial unique index, this is **the canonical promoted version** of the strategy, not a test artifact.

**C4: CANONICAL** — `alts_main` v1, published 2026-04-23, only active version of `strategy_id=5`, metrics fresh through 2026-05-02.

---

## Bugs identified for follow-up

### 1. GHOST_LOG_WRITER (priority: low)

The allocation log file `/mnt/quant-data/logs/trader/allocation_f87fe130-..._2026-05-03.log` is being appended every 5 minutes on the dot, but no process across host or any container ever holds an open fd to the file at moment of inspection. Inode is unchanged (no atomic-rename pattern). Bar entries claim `active=2/4 stopped=2 equity=$93` while `runtime_state.phase="filtered"` and `position_history` has zero rows for this allocation ever. The log's `actual_roi` matches `incr×lev×0.90` exactly with `delta=+0.000%` on every bar — confirming the log values are computed from a model, not from BloFin equity.

**Likely candidates:** stray dev-session trader from a previous deployment held in memory by some still-running parent; custom Python logging handler that opens-writes-closes per record (default `FileHandler` keeps fd open, so this would have to be a custom handler).

**Resolution:** requires `apt-get install inotify-tools` then directory-level `inotifywait -m -e create -e moved_to -e modify /mnt/quant-data/logs/trader/` for one bar cycle, capturing CREATE/MOVED_TO events that file-level watches miss. Alternative: `auditctl -w <path> -p wa -k ghostlog`. Defer until prioritized.

### 2. CAPITAL_EVENT_DOES_NOT_CLOSE_ALLOCATION (priority: HIGH)

The capital_events auto-poll detected an on-chain Polygon withdrawal of $1,036.87 from connection `5f51a294` at 2026-05-02 19:07:23 UTC, draining the sub-account to ~$0. The event was correctly recorded in `user_mgmt.allocation_capital_events` (event_id=`cf00cd11-1378-49fb-99c1-7177f5003add`, source=auto). But:

- `user_mgmt.allocations.status` stayed `"active"`
- `allocations.runtime_state.phase` continued cycling normally each morning (`filtered` today)
- The trader continued spawning each day at 06:35 against an empty connection
- The Manager Overview UI presumably continues to show this allocation as live

No downstream handler closes the allocation when `principal_baseline_usd → 0` or when sustained snapshot equity is `$0` for >24h. **This is the highest-impact of the three findings** — a real-money allocation can be drained out-of-band and the platform doesn't notice or react.

### 3. SUPERVISOR_STALE_CHECK_OWN_STATE (priority: medium)

`user_mgmt.trader_supervisor_state` row for allocation `f87fe130` has `updated_at = 2026-05-02 01:45:03` — stale by >35 hours at time of investigation. Supervisor cron runs `*/2 * * * *` and every cycle reports `"0 stale allocation(s) detected"` — but the supervisor's stale-detection logic uses `runtime_state.last_heartbeat`, which is not being written by the ghost writer. The supervisor's own state row is not being refreshed by the supervisor itself, so it cannot detect that it is operating on stale or missing trader state.

Effectively: the supervisor only refreshes `trader_supervisor_state` when it intervenes. If a trader exits cleanly and a ghost writer takes over (or a process dies silently), the supervisor sees "everything fine" indefinitely.

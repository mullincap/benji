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

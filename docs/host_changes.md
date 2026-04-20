# Host-side changes log

The host at mcap runs files outside the repo (notably `/root/benji/trader-blofin.py`
and `/root/benji/daily_signal.py`). Changes to those files don't flow through
`git push` + `./redeploy.sh` — they're edited in place on the host. This log
records each host-side change so future debugging can trace what happened.

---

## 2026-04-20 — daily_signal.py write_to_db: per-strategy-version INSERTs

**File**: `/root/benji/daily_signal.py`
**Backup**: `/root/benji/daily_signal.py.bak-20260420-172540`
**Change**: `write_to_db` function rewritten. Previously read a single
`STRATEGY_VERSION_ID` from `/mnt/quant-data/credentials/secrets.env` and wrote
one row per day to `user_mgmt.daily_signals`. Now queries
`audit.strategy_versions WHERE is_active=TRUE JOIN audit.strategies WHERE
is_published=TRUE` and writes one row per published-active strategy_version_id
per day. Each row gets its own `signal_batch_id` and per-symbol
`daily_signal_items` rows.

**Why**: Manager overview's pipeline-status query filters `daily_signals` by
active allocations' strategy_version_ids. Pre-fix, the cron only wrote under
the legacy v1.0 id (`d023dc1e-…`, `is_published=FALSE`). Active allocations
reference alpha Med/High lev ids that never got signal rows → Manager showed
"Daily Signal UNKNOWN / never".

**Fallback**: If the query returns zero rows (no published+active versions),
falls back to reading `STRATEGY_VERSION_ID` from secrets.env so the cron still
writes something. Defensive, should never trigger in production.

**Verification** (after next 05:58 UTC cron tick on 2026-04-21):
```
docker exec timescaledb psql -U quant -d marketdata -c "
  SELECT strategy_version_id, COUNT(*), MAX(computed_at)
  FROM user_mgmt.daily_signals
  WHERE computed_at >= NOW() - INTERVAL '1 day'
  GROUP BY strategy_version_id;"
```
Expected: 3+ rows (v1.0 `d023dc1e`, Med lev `6b6168b0`, High lev `987312fd`).
Manager overview's "Daily Signal" should flip from UNKNOWN to a real timestamp
automatically on next page load.

**Deploy pattern**: edit in place on host, no redeploy needed (cron reads the
file fresh each run).

-- Migration 005: drop allocation_returns.capital_events_reconciled_at
--
-- Migration 004 added this column to gate the 23:56 UTC reconcile cron's
-- idempotence (each row reconciled exactly once). The cron has been
-- retired in favor of compute-on-read: manager.py and /pnl now subtract
-- allocation_capital_events live each call, so manual edits/adds/deletes
-- propagate instantly without needing a stored "already-processed" flag.
--
-- The trader-written net_return_pct + gross_return_pct on
-- allocation_returns are now the SOURCE OF TRUTH — never overwritten by
-- batch jobs. Capital-event subtraction happens inside read queries via
-- a JOIN to allocation_capital_events grouped by (allocation_id, date)
-- and a CASE that re-denominators on the capital-adjusted baseline.
--
-- Pre-migration check: at the time this migration is deployed, no rows
-- in allocation_returns have a non-NULL capital_events_reconciled_at
-- (the cron only ran one smoke-test that produced 0 updates). Verified
-- via:
--   SELECT COUNT(*) FROM user_mgmt.allocation_returns
--    WHERE capital_events_reconciled_at IS NOT NULL;
-- Expected: 0. If non-zero on your env, those rows have stored
-- already-adjusted net values — see incident note in commit message
-- before deploying.
--
-- Safe to re-run: guarded by IF EXISTS.

ALTER TABLE user_mgmt.allocation_returns
    DROP COLUMN IF EXISTS capital_events_reconciled_at;

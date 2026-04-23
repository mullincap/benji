-- Migration 004: allocation_returns.capital_events_reconciled_at
--
-- Tracks whether a given (allocation_id, session_date) row in
-- allocation_returns has already had its net_return_pct + gross_return_pct
-- adjusted to net out same-day capital events (deposits / withdrawals).
--
-- Required by the permanent nightly reconcile cron (ops/reconcile_
-- allocation_returns_nightly.sql) for idempotence: the adjustment math
-- (end_equity = capital_deployed * (1 + stored_net/100)) reads the
-- already-stored value, so re-running without a guard would
-- double-adjust. Filtering on `capital_events_reconciled_at IS NULL`
-- ensures each row is corrected exactly once across the cron's lifetime.
--
-- Historical rows: NULL on existing rows is the correct initial state.
-- The first nightly run after this migration deploys will scan all
-- historical allocation_returns and reconcile any that have matching
-- capital_events — a free retroactive backfill.
--
-- Supersedes the one-shot ops/reconcile_session_capital_events.sql
-- (committed in 75e2bd9 with a hardcoded allocation_id) — that file is
-- preserved for rollback / manual diagnostic use but the cron line
-- pointing at it is replaced in ops/crontab.txt.
--
-- Safe to re-run: guarded by IF NOT EXISTS on the column.

ALTER TABLE user_mgmt.allocation_returns
    ADD COLUMN IF NOT EXISTS capital_events_reconciled_at TIMESTAMPTZ;

COMMENT ON COLUMN user_mgmt.allocation_returns.capital_events_reconciled_at IS
    'Set by the nightly reconcile cron when net_return_pct + gross_return_pct '
    'have been adjusted to subtract same-day allocation_capital_events. '
    'NULL = not yet processed (cron will pick it up on next run if matching '
    'capital events exist).';

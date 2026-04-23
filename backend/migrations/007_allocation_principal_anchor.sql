-- Migration 007: principal_anchor_at + principal_baseline_usd on allocations
--
-- Lets the operator pin the "official account start date" per allocation.
-- Principal is computed live as:
--
--   anchor_at = COALESCE(principal_anchor_at, created_at)
--   baseline  = COALESCE(principal_baseline_usd, capital_usd)
--   principal = baseline
--             + SUM(deposits)    where event_at >= anchor_at AND deleted_at IS NULL
--             - SUM(withdrawals) where event_at >= anchor_at AND deleted_at IS NULL
--
-- Leaving both NULL preserves the existing behavior (principal = capital_usd
-- at allocation creation + all subsequent capital events). Setting them lets
-- the operator declare "the real track record started on 2026-04-01 at
-- $5,000" and have all PnL % on /pnl compute against that baseline.
--
-- Safe to re-run: guarded by IF NOT EXISTS.

ALTER TABLE user_mgmt.allocations
    ADD COLUMN IF NOT EXISTS principal_anchor_at TIMESTAMPTZ;

ALTER TABLE user_mgmt.allocations
    ADD COLUMN IF NOT EXISTS principal_baseline_usd NUMERIC(18, 4);

COMMENT ON COLUMN user_mgmt.allocations.principal_anchor_at IS
    'Operator-set "principal tracking starts here" date. NULL = use allocation.created_at. '
    'Capital events with event_at >= anchor_at are netted into the live principal computation.';

COMMENT ON COLUMN user_mgmt.allocations.principal_baseline_usd IS
    'Operator-set USD value at principal_anchor_at. NULL = use allocation.capital_usd. '
    'Principal = baseline + SUM(deposits − withdrawals since anchor_at).';

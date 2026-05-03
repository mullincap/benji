-- 024_user_strategy_selection.sql
-- ===========================================================================
-- Onboarding Phase 1: track which strategy a user has explicitly selected
-- but not yet allocated to.
--
-- The 1-step get-started flow drives users through:
--   1. Link exchange  →  has_exchange becomes true
--   2. Select a strategy (visit /trader/strategies/[id], click "Select this
--      strategy")  →  selected_strategy_id populated
--   3. Deploy capital  →  allocation row created; selected_strategy_id
--      cleared atomically with the allocation insert (in
--      backend/app/api/routes/allocator.py:create_allocation)
--
-- The OnboardingNudge banner reads these fields plus exchange_connections /
-- allocations counts via GET /api/onboarding/state to decide which of the
-- three nudges to show.
-- ===========================================================================

BEGIN;

ALTER TABLE user_mgmt.users
  ADD COLUMN IF NOT EXISTS selected_strategy_id  uuid REFERENCES audit.strategy_versions(strategy_version_id),
  ADD COLUMN IF NOT EXISTS selected_strategy_at  timestamptz;

-- Partial index — only meaningful when a selection exists. Most users will
-- have NULL here once they've allocated, so the partial keeps the index
-- compact.
CREATE INDEX IF NOT EXISTS idx_users_selected_strategy
  ON user_mgmt.users (selected_strategy_id)
  WHERE selected_strategy_id IS NOT NULL;

COMMIT;

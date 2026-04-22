-- Migration 001: add compounding_mode to user_mgmt.allocations
--
-- Motivates the per-allocation Compounding setting:
--   'compound' (default) — capital_usd auto-updates to wallet equity at
--                          session close, so profits/losses compound into
--                          the next session's sizing without manual edits.
--   'fixed'              — capital_usd stays at the user's configured value;
--                          profits/losses accumulate as idle wallet balance.
--
-- Existing rows default to 'compound' since that matches the manual
-- behavior users were performing before this feature (adjusting capital
-- between sessions to carry forward gains).
--
-- Safe to re-run: guarded by IF NOT EXISTS.

ALTER TABLE user_mgmt.allocations
    ADD COLUMN IF NOT EXISTS compounding_mode TEXT NOT NULL DEFAULT 'compound';

-- Add the CHECK only on first apply; PostgreSQL doesn't support
-- IF NOT EXISTS on ADD CONSTRAINT, so guard with a catalog lookup.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'allocations_compounding_mode_check'
          AND conrelid = 'user_mgmt.allocations'::regclass
    ) THEN
        ALTER TABLE user_mgmt.allocations
            ADD CONSTRAINT allocations_compounding_mode_check
            CHECK (compounding_mode IN ('compound', 'fixed'));
    END IF;
END $$;

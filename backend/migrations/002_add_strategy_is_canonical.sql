-- Migration 002: add is_canonical to audit.strategies
--
-- Moves the "which strategy is the canonical reference for Simulator
-- compare-to-canonical" source of truth from a hardcoded Python constant
-- (backend/app/api/routes/simulator.py:CANONICAL_REFERENCE_STRATEGY)
-- into the DB, so promotion becomes a data operation instead of a code
-- change + redeploy.
--
-- Constraints:
--   - Column is NOT NULL DEFAULT false; existing rows auto-populate to
--     false, then the seed UPDATE below promotes the current canonical.
--   - A partial unique index enforces "at most one row can have
--     is_canonical=true at any time". Concurrent promote/demote must
--     run inside a single transaction (the /promote-canonical endpoint
--     does this).
--
-- Seed choice: 'Alpha Main' is the current canonical per spec §5 and
-- per the pre-migration hardcoded value. Sharpe is leverage-invariant
-- across the Alpha family, so picking Main as the arbitrary
-- representative gives the same Sharpe comparison regardless of which
-- leverage tier a candidate is testing against.
--
-- Safe to re-run: column guarded by IF NOT EXISTS; unique index
-- guarded by IF NOT EXISTS; seed is idempotent.

ALTER TABLE audit.strategies
    ADD COLUMN IF NOT EXISTS is_canonical BOOLEAN NOT NULL DEFAULT FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS strategies_one_canonical
    ON audit.strategies (is_canonical)
    WHERE is_canonical = TRUE;

UPDATE audit.strategies
   SET is_canonical = TRUE
 WHERE display_name = 'Alpha Main'
   AND is_canonical = FALSE;

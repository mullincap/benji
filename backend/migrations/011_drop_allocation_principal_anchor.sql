-- Migration 011: drop allocation-level principal_anchor_at + _baseline_usd
--
-- Deprecated by migration 010, which moved these to exchange_connections.
-- Run AFTER 010 has successfully executed and the data move completed.
-- Rollback requires re-adding the columns + copying back from
-- exchange_connections — do not run until 010 is proven stable on prod.
--
-- Safe to re-run: guarded by IF EXISTS.

ALTER TABLE user_mgmt.allocations DROP COLUMN IF EXISTS principal_anchor_at;
ALTER TABLE user_mgmt.allocations DROP COLUMN IF EXISTS principal_baseline_usd;

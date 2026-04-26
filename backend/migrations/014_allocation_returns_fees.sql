-- 014_allocation_returns_fees.sql
-- Add per-session fee aggregates to allocation_returns so the daily
-- execution summary surface can show order fees + funding alongside the
-- session's PnL and return percentages without requiring a per-symbol
-- aggregation at query time.

ALTER TABLE user_mgmt.allocation_returns
  ADD COLUMN IF NOT EXISTS order_fees_usd   NUMERIC,
  ADD COLUMN IF NOT EXISTS funding_fees_usd NUMERIC;

COMMENT ON COLUMN user_mgmt.allocation_returns.order_fees_usd IS
  'Sum of |fee| across the session''s filled orders for this allocation, '
  'aggregated from allocation_execution_symbols.order_fees_usd. Populated '
  'by the trader writer at session close; NULL on historical rows '
  'predating the 2026-04-25 backfill.';

COMMENT ON COLUMN user_mgmt.allocation_returns.funding_fees_usd IS
  'Estimated funding cost for the session: notional_lev × FUNDING_RATE_DAILY '
  '× duration_days (mirrors trader_blofin.py:2557 calc). BloFin does not '
  'expose historical per-symbol funding at the active API tier; this '
  'estimate matches the trader''s per-bar log accounting.';

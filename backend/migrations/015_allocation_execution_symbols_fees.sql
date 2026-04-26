-- 015_allocation_execution_symbols_fees.sql
-- Add per-symbol fee breakdown columns. Order fees come from BloFin's
-- |fee| field on each filled order. Funding fees are estimated from
-- duration × FUNDING_RATE_DAILY × leveraged notional, since BloFin
-- does not expose per-symbol historical funding at the active API tier.

ALTER TABLE user_mgmt.allocation_execution_symbols
  ADD COLUMN IF NOT EXISTS order_fees_usd   NUMERIC,
  ADD COLUMN IF NOT EXISTS funding_fees_usd NUMERIC;

COMMENT ON COLUMN user_mgmt.allocation_execution_symbols.order_fees_usd IS
  'Sum of |fee| across all today''s filled orders for this symbol on this '
  'allocation. ~0.06% × notional per filled leg on BloFin SWAP. Populated '
  'by the trader writer at session close; NULL on historical rows predating '
  'the 2026-04-25 backfill.';

COMMENT ON COLUMN user_mgmt.allocation_execution_symbols.funding_fees_usd IS
  'Estimated funding cost: filled_contracts × fill_entry_price × ctval × '
  'leverage × FUNDING_RATE_DAILY (0.0002) × duration_days. Matches the '
  'trader''s per-bar expected_roi accounting (trader_blofin.py:2557).';

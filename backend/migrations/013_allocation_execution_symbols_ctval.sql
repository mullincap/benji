-- 013_allocation_execution_symbols_ctval.sql
-- Add ctval (exchange contract value) to allocation_execution_symbols so
-- notional_usd and pnl_usd can be computed correctly for symbols whose
-- contract_value ≠ 1 (e.g. INX=100, KAT=10, SKR=100 on BloFin).
--
-- Prior behavior: the trader writer omitted ctval from position dicts,
-- leaving downstream math either wrong (notional = contracts × price,
-- ignoring ctval) or null (pnl_usd skipped when ctval missing).
-- Both surfaces now read this column.

ALTER TABLE user_mgmt.allocation_execution_symbols
  ADD COLUMN IF NOT EXISTS ctval numeric;

COMMENT ON COLUMN user_mgmt.allocation_execution_symbols.ctval IS
  'Exchange contract value (BloFin: contractValue; Binance margin: 1.0). '
  'Required to compute notional_usd = contracts × price × ctval and '
  'pnl_usd = (exit - entry) × contracts × ctval correctly. Populated by '
  'the trader writer; NULL for rows written before migration 013.';

-- 016_allocation_execution_symbols_operator_intervened.sql
-- Mark symbol-rows where the operator manually placed orders mid-session
-- (added contracts, partial closes, etc.). When TRUE, downstream consumers
-- can warn that pnl_usd / fill_exit_price reflect the operator's blended
-- activity rather than just the trader's deliberate entry-and-exit.

ALTER TABLE user_mgmt.allocation_execution_symbols
  ADD COLUMN IF NOT EXISTS operator_intervened BOOLEAN DEFAULT false;

COMMENT ON COLUMN user_mgmt.allocation_execution_symbols.operator_intervened IS
  'TRUE when the operator placed manual orders on this symbol during the '
  'session beyond the trader''s entry/close. Detected by the trader writer '
  '(or set by hand on backfilled rows). UI shows an asterisk next to the '
  'symbol so allocators know the per-symbol stats blend trader + operator '
  'activity.';

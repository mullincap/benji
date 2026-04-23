-- Migration 008: relax allocation_capital_events.amount_usd > 0 to >= 0
--
-- Migration 003's CHECK (amount_usd > 0) was correct when all rows came
-- from operator entry (manual entries always have a known positive USD
-- value). Auto-detected non-stablecoin deposits may legitimately have
-- amount_usd=0 when the historical-price fetch fails (e.g., obscure
-- altcoin without a {asset}-USDT pair, BloFin klines endpoint missing
-- historical data for the deposit timestamp, network error). The row
-- still carries the asset + native amount in `notes`, so the operator
-- can manually set the correct USD value via the UI Update modal.
--
-- Negative amounts remain forbidden; the kind column ('deposit' /
-- 'withdrawal') already encodes sign for PnL math.
--
-- Safe to re-run: drops the old constraint by name, adds the new one.

ALTER TABLE user_mgmt.allocation_capital_events
    DROP CONSTRAINT IF EXISTS allocation_capital_events_amount_usd_check;

ALTER TABLE user_mgmt.allocation_capital_events
    ADD CONSTRAINT allocation_capital_events_amount_usd_check
        CHECK (amount_usd >= 0);

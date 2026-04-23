-- Migration 003: allocation_capital_events
--
-- Records out-of-band capital movements into/out of an allocation so the
-- Trader P&L endpoint (and any downstream PnL-aware component) can subtract
-- them and attribute only trading returns to the strategy.
--
-- Motivates the Trader-page fix on 2026-04-23: operator moved $1,003.75
-- from a dormant Binance connection into the active BloFin allocation. The
-- /pnl endpoint's baseline was the earliest exchange_snapshot for the
-- connection post-allocation.created_at — an immutable pre-deposit value —
-- so the +$1k deposit showed up as +$1k of Total P&L (and Session P&L,
-- since the session_start_equity captured earlier in the day also didn't
-- include it).
--
-- Convention:
--   - amount_usd is always positive
--   - kind ∈ ('deposit', 'withdrawal') determines sign when netted
--   - event_at is the moment the capital change was actually observable
--     on the exchange (e.g., deposit receipt timestamp). Used for session-
--     scoped subtraction (only events within the active session window
--     are subtracted from session PnL).
--
-- NOT a complete audit log — this is for PnL correction only. More
-- granular capital accounting (wire references, fees, timing assertions)
-- would warrant a separate cashflow table.
--
-- Safe to re-run: guarded by IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS user_mgmt.allocation_capital_events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    allocation_id   UUID NOT NULL REFERENCES user_mgmt.allocations(allocation_id),
    event_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    amount_usd      NUMERIC(18, 4) NOT NULL CHECK (amount_usd > 0),
    kind            TEXT NOT NULL CHECK (kind IN ('deposit', 'withdrawal')),
    notes           TEXT,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_allocation_capital_events_alloc_time
    ON user_mgmt.allocation_capital_events (allocation_id, event_at);

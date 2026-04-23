-- Migration 006: auto-detection support on allocation_capital_events
--
-- Adds the columns needed to ingest capital events from exchange income
-- APIs (deposits + withdrawals) without losing any operator overrides:
--
--   source                  Provenance: where this row came from.
--                             'manual'        Operator entered via UI / SQL.
--                             'auto'          Nightly poller picked it up.
--                             'auto-anomaly'  Same poller path, but fired
--                                             out-of-cycle by the equity-jump
--                                             anomaly detector.
--   connection_id           Exchange connection that produced this event.
--                           NULL on legacy / pre-migration rows. For
--                           auto-poll dedup, paired with exchange_event_id.
--   exchange_event_id       The exchange's own ID for this transfer
--                           (BloFin: transferId; Binance: id from
--                            /sapi/v1/capital/deposit/hisrec). NULL on
--                           manual rows since operators don't have one.
--                           Dedup key when present.
--   is_manually_overridden  TRUE = operator has touched this row (edit
--                           OR soft-delete). Auto-poller MUST skip any
--                           (connection_id, exchange_event_id) pair that
--                           already has an overridden row, deleted or not.
--                           This is what makes "manual override sticks
--                           even across re-polls" work.
--   deleted_at              Soft-delete timestamp. Auto-poller dedup
--                           reads with deleted_at OR NOT deleted_at
--                           (any existing row blocks re-insert).
--                           Read paths (manager.py + /pnl) filter
--                           deleted_at IS NULL so soft-deleted events
--                           are excluded from PnL math.
--
-- Idempotence: poller MUST query by (connection_id, exchange_event_id)
-- and skip insert if any row exists. The partial unique index below
-- enforces this at the DB layer too.
--
-- Safe to re-run: each ALTER guarded by IF NOT EXISTS.

ALTER TABLE user_mgmt.allocation_capital_events
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'
        CHECK (source IN ('manual', 'auto', 'auto-anomaly'));

ALTER TABLE user_mgmt.allocation_capital_events
    ADD COLUMN IF NOT EXISTS connection_id UUID
        REFERENCES user_mgmt.exchange_connections(connection_id);

ALTER TABLE user_mgmt.allocation_capital_events
    ADD COLUMN IF NOT EXISTS exchange_event_id TEXT;

ALTER TABLE user_mgmt.allocation_capital_events
    ADD COLUMN IF NOT EXISTS is_manually_overridden BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE user_mgmt.allocation_capital_events
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Allocation_id can now be NULL — the auto-poller may not be able to
-- map an exchange-level event to a specific allocation when the
-- connection has multiple active allocations. Such rows surface in
-- admin review.
ALTER TABLE user_mgmt.allocation_capital_events
    ALTER COLUMN allocation_id DROP NOT NULL;

-- Dedup index: at most one event per (connection, exchange-event-id)
-- when the poller produced one. Manual rows (exchange_event_id IS NULL)
-- are unconstrained — operators may have multiple manual entries with
-- the same connection on the same date.
CREATE UNIQUE INDEX IF NOT EXISTS idx_capital_events_exchange_dedup
    ON user_mgmt.allocation_capital_events (connection_id, exchange_event_id)
    WHERE exchange_event_id IS NOT NULL;

-- Hot path: poller filters by connection + event_at recency
CREATE INDEX IF NOT EXISTS idx_capital_events_connection_time
    ON user_mgmt.allocation_capital_events (connection_id, event_at DESC);

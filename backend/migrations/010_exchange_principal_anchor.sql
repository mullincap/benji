-- Migration 010: exchange-level principal anchor + baseline
--
-- Moves principal_anchor_at + principal_baseline_usd from allocations
-- to exchange_connections. Rationale: the account start date is an
-- EXCHANGE-level concept (one wallet, one history), not a per-strategy
-- concept. Multiple allocations on the same connection share the
-- anchor; each allocation's track record is computed from its own
-- daily allocation_returns rows since that anchor, not from a
-- separate stored baseline.
--
-- Data move: for each connection, pick the anchor/baseline from the
-- oldest allocation that had explicit values. If two allocations on
-- one connection had conflicting overrides, the oldest one wins (this
-- can only happen if the operator explicitly diverged them; no
-- production data today has this state).
--
-- Migration 011 drops the allocation-level columns as a follow-up —
-- kept in a separate file so this migration can be rolled back if the
-- data move surfaces an unexpected edge case. Run 010 and 011
-- together during normal deploy.
--
-- Safe to re-run: guarded by IF NOT EXISTS on column adds and a
-- "only UPDATE when connection's anchor is still NULL" idempotence
-- guard.

ALTER TABLE user_mgmt.exchange_connections
    ADD COLUMN IF NOT EXISTS principal_anchor_at TIMESTAMPTZ;

ALTER TABLE user_mgmt.exchange_connections
    ADD COLUMN IF NOT EXISTS principal_baseline_usd NUMERIC(18, 4);

COMMENT ON COLUMN user_mgmt.exchange_connections.principal_anchor_at IS
    'Operator-set "principal tracking starts here" date for this exchange. '
    'NULL = no anchor set; UI falls back to the earliest event date or to '
    'the connection creation date. Capital events with event_at >= anchor '
    'are netted into the exchange principal; earlier events are ignored '
    'by default display.';

COMMENT ON COLUMN user_mgmt.exchange_connections.principal_baseline_usd IS
    'Operator-set USD value of the exchange wallet at principal_anchor_at. '
    'Principal = baseline + SUM(capital events since anchor). NULL = no '
    'explicit baseline; treated as 0 for principal-arithmetic purposes.';

-- Data move: transfer any existing allocation-level overrides to the
-- connection. Only runs when the connection doesn't already have values
-- (idempotent on re-run).
UPDATE user_mgmt.exchange_connections ec
   SET principal_anchor_at     = sub.anchor,
       principal_baseline_usd  = sub.baseline
  FROM (
        SELECT DISTINCT ON (connection_id)
               connection_id,
               principal_anchor_at    AS anchor,
               principal_baseline_usd AS baseline
          FROM user_mgmt.allocations
         WHERE principal_anchor_at IS NOT NULL
            OR principal_baseline_usd IS NOT NULL
         ORDER BY connection_id, created_at
       ) sub
 WHERE ec.connection_id = sub.connection_id
   AND (ec.principal_anchor_at IS NULL AND ec.principal_baseline_usd IS NULL);

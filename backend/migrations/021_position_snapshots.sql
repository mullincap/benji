-- 021_position_snapshots.sql
-- Per-position intraday state at 1-minute cadence, used by the Live
-- tab waterfall (§6) for arbitrary-window PnL deltas (today / 7D /
-- 30D / since-open) and by the drill-down for MFE/MAE update inputs.
--
-- Cadence: 1m. Cheaper than every-tick (2s would balloon storage),
-- denser than 5m (allows tighter "today since 06:35" windowing). At
-- ~1440 rows per position per day with ~10 open positions, that's
-- ~14k rows/day — well within Timescale's comfort zone.
--
-- Hypertable, not a regular table:
--   * Range scans by snapshot_at dominate every read (waterfall window
--     selection, MFE/MAE search).
--   * Compression after 7 days reclaims storage on stale snapshots
--     that are still needed for SINCE-OPEN windowing on long-held
--     positions.
--   * Per-position retention can be aligned with position_history
--     close events later (drop snapshots > 90 days post-close).
--
-- Multi-venue keying: UNIQUE (venue, connection_id, position_id,
-- snapshot_at). venue=''blofin'' for every row in v1.
--
-- No FK constraints on the hypertable: TimescaleDB has historically
-- restricted FKs to/from hypertables, and application-level integrity
-- (writer enforces venue/account/position_id consistency with
-- position_history) is sufficient here.

CREATE TABLE IF NOT EXISTS user_mgmt.position_snapshots (
    venue              TEXT         NOT NULL,
    connection_id         UUID         NOT NULL,
    position_id        TEXT         NOT NULL,
    snapshot_at        TIMESTAMPTZ  NOT NULL,
    -- live state at snapshot time
    mark_price         NUMERIC(20, 10) NOT NULL,
    size               NUMERIC(20, 10) NOT NULL,
    notional_usd       NUMERIC(20, 8),
    unrealized_pnl_usd NUMERIC(20, 8),
    unrealized_pct     NUMERIC(10, 4),
    -- cumulative since position open (running totals, not deltas)
    funding_paid_usd   NUMERIC(20, 8),
    -- constraints
    CONSTRAINT position_snapshots_unique
        UNIQUE (venue, connection_id, position_id, snapshot_at),
    CONSTRAINT position_snapshots_venue_chk
        CHECK (venue IN ('blofin', 'binance'))
);

-- Convert to hypertable. 7-day chunks: ~100k rows per chunk at 10
-- positions × 1440 m/d × 7 d, comfortably under Timescale's
-- recommended chunk-size sweet spot of ~25% of working memory.
SELECT create_hypertable(
    'user_mgmt.position_snapshots',
    'snapshot_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Per-position retrieval ordered by recency — the drill-down's
-- "trajectory since open" + MFE/MAE update inputs.
CREATE INDEX IF NOT EXISTS idx_position_snapshots_pos_time
    ON user_mgmt.position_snapshots (venue, connection_id, position_id, snapshot_at DESC);

-- Account-wide time slice — waterfall windowing aggregates across
-- every open position for the same account at a fixed time bound.
CREATE INDEX IF NOT EXISTS idx_position_snapshots_conn_time
    ON user_mgmt.position_snapshots (venue, connection_id, snapshot_at DESC);

-- Compression after 30 days. Strategy positions are intra-day (close
-- by 23:55 UTC same day) so they never read from compressed chunks.
-- Manual positions routinely run 1–3 weeks; compressing them at 7
-- days would hit SINCE-OPEN waterfall recomputes on still-open
-- positions. 30 days covers the vast majority of manual holds while
-- keeping cold storage trim.
ALTER TABLE user_mgmt.position_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'venue, connection_id, position_id'
);

SELECT add_compression_policy(
    'user_mgmt.position_snapshots',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

COMMENT ON TABLE user_mgmt.position_snapshots IS
    'Per-position intraday state at 1m cadence. Drives waterfall '
    'window-anchored PnL deltas (§6) and MFE/MAE maintenance for '
    'position_history. TimescaleDB hypertable; chunks compress after '
    '7 days.';

COMMENT ON COLUMN user_mgmt.position_snapshots.position_id IS
    'Matches user_mgmt.position_history.position_id for the same '
    'venue + account. Application enforces referential integrity — '
    'no FK due to Timescale hypertable limits.';

COMMENT ON COLUMN user_mgmt.position_snapshots.funding_paid_usd IS
    'Cumulative funding paid (negative = received) since position '
    'opened, NOT a per-snapshot delta. Stored cumulatively so the '
    'drill-down can read "current funding" with one row instead of '
    'a window sum.';

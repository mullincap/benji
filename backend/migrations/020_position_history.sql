-- 020_position_history.sql
-- Lifecycle row per position: open → close, with entry context, source
-- attribution, and excursion peaks (MFE/MAE) for the Live tab drill-
-- down. Augments — does not replace — exchange_snapshots.positions
-- (which captures point-in-time state but discards lifecycle).
--
-- Multi-venue keying: UNIQUE (venue, connection_id, position_id) where
-- position_id is the venue's native identifier (BloFin posId in v1).
-- For venues that don't expose a stable position id (e.g. Binance
-- USDM with hedge-mode disabled — positions are implied from
-- positionRisk rows, no server-side id), the writer synthesizes one
-- of the form '{symbol}|{margin_mode}|{position_side}|{open_ms}'.
--
-- Open vs closed: closed_at IS NULL → still open. The MFE/MAE/bar
-- counts continue updating on every T0 tick while open; lock in at
-- close.
--
-- Written by:
--   * trader-blofin.py at order-confirm time (entry insert)
--   * the same trader at session close or stop-out (exit update)
--   * the position-snapshot worker on every snapshot tick
--     (mfe/mae/bars_in_profit/bars_total maintenance)

CREATE TABLE IF NOT EXISTS user_mgmt.position_history (
    history_id          BIGSERIAL    PRIMARY KEY,
    venue               TEXT         NOT NULL,
    connection_id          UUID         NOT NULL,
    position_id         TEXT         NOT NULL,
    -- identification
    symbol              TEXT         NOT NULL,
    symbol_base         TEXT         NOT NULL,
    side                TEXT         NOT NULL,
    -- entry
    opened_at           TIMESTAMPTZ  NOT NULL,
    entry_price         NUMERIC(20, 10) NOT NULL,
    entry_size          NUMERIC(20, 10) NOT NULL,
    leverage            NUMERIC(8, 2),
    margin_mode         TEXT,
    -- close (NULL while open)
    closed_at           TIMESTAMPTZ,
    exit_price          NUMERIC(20, 10),
    realized_pnl_usd    NUMERIC(20, 8),
    -- source attribution (§14)
    source              TEXT         NOT NULL DEFAULT 'manual',
    strategy_name       TEXT,
    allocation_id       UUID,
    signal_session_id   UUID,
    signal_grade        TEXT,
    signal_conviction_px NUMERIC(20, 10),
    -- excursions (sampled at T0 cadence, 0% precision lost on close)
    mark_at_open        NUMERIC(20, 10),
    mfe_pct             NUMERIC(10, 4),
    mfe_at              TIMESTAMPTZ,
    mae_pct             NUMERIC(10, 4),
    mae_at              TIMESTAMPTZ,
    bars_in_profit      INTEGER      NOT NULL DEFAULT 0,
    bars_total          INTEGER      NOT NULL DEFAULT 0,
    -- free-form annotation: tags, manual notes, why-I-took-it. Empty
    -- by default; the column exists so future schema needs (note
    -- threading, tagging UI) don't require another migration.
    metadata            JSONB        NOT NULL DEFAULT '{}'::jsonb,
    -- audit
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- constraints
    CONSTRAINT position_history_unique UNIQUE (venue, connection_id, position_id),
    CONSTRAINT position_history_connection_fk
        FOREIGN KEY (connection_id) REFERENCES user_mgmt.exchange_connections(connection_id),
    CONSTRAINT position_history_allocation_fk
        FOREIGN KEY (allocation_id) REFERENCES user_mgmt.allocations(allocation_id),
    CONSTRAINT position_history_venue_chk
        CHECK (venue IN ('blofin', 'binance')),
    CONSTRAINT position_history_side_chk
        CHECK (side IN ('long', 'short')),
    CONSTRAINT position_history_source_chk
        CHECK (source IN ('manual', 'strategy')),
    CONSTRAINT position_history_close_consistency_chk
        CHECK (
            (closed_at IS NULL AND exit_price IS NULL AND realized_pnl_usd IS NULL)
            OR
            (closed_at IS NOT NULL AND exit_price IS NOT NULL)
        ),
    CONSTRAINT position_history_strategy_consistency_chk
        CHECK (
            (source = 'strategy' AND allocation_id IS NOT NULL)
            OR
            (source = 'manual')
        )
);

-- Recent activity scan (table view, ordered by recency)
CREATE INDEX IF NOT EXISTS idx_position_history_venue_conn_open
    ON user_mgmt.position_history (venue, connection_id, opened_at DESC);

-- "What's currently open" — partial index, the dominant Live-tab read.
-- Smaller than a full index; the closed_at IS NULL clause filters out
-- the historical bulk that grows unbounded over time.
CREATE INDEX IF NOT EXISTS idx_position_history_venue_conn_open_only
    ON user_mgmt.position_history (venue, connection_id)
    WHERE closed_at IS NULL;

-- Cross-venue lookups by base symbol (e.g. "show me every BTC position
-- I've ever held") — useful for chat context + the drill-down history.
CREATE INDEX IF NOT EXISTS idx_position_history_symbol_base
    ON user_mgmt.position_history (symbol_base, opened_at DESC);

-- Strategy session lookup (§14 attribution backfill)
CREATE INDEX IF NOT EXISTS idx_position_history_signal_session
    ON user_mgmt.position_history (signal_session_id)
    WHERE signal_session_id IS NOT NULL;

-- updated_at trigger so writers don't have to maintain it manually.
CREATE OR REPLACE FUNCTION user_mgmt._position_history_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS position_history_updated_at ON user_mgmt.position_history;
CREATE TRIGGER position_history_updated_at
    BEFORE UPDATE ON user_mgmt.position_history
    FOR EACH ROW EXECUTE FUNCTION user_mgmt._position_history_set_updated_at();

COMMENT ON TABLE user_mgmt.position_history IS
    'Per-position lifecycle row: open → close, with entry context, '
    'source attribution (manual vs strategy), and excursion peaks. '
    'Drives the Live tab drill-down (§13a) and AGE column (§12).';

COMMENT ON COLUMN user_mgmt.position_history.position_id IS
    'Venue-native position identifier. BloFin: posId. Binance USDM '
    '(when added later): synthesized from {symbol}|{margin_mode}|'
    '{position_side}|{open_ms} since the venue does not expose a '
    'stable id.';

COMMENT ON COLUMN user_mgmt.position_history.symbol IS
    'Venue-native symbol form: BloFin ''BTC-USDT'', Binance ''BTCUSDT''.';

COMMENT ON COLUMN user_mgmt.position_history.symbol_base IS
    'Quote-stripped base symbol — joins to market.symbols.base for '
    'cross-venue market-data lookups (Binance kline pulls).';

COMMENT ON COLUMN user_mgmt.position_history.source IS
    'Strategy attribution per §14: ''strategy'' if symbol matched the '
    'active strategy session basket at open time AND side matches the '
    'session signal; ''manual'' otherwise.';

COMMENT ON COLUMN user_mgmt.position_history.bars_total IS
    'Total T0 ticks observed since position open. Together with '
    'bars_in_profit, drives the "N / M bars in profit" stat in the '
    'drill-down. Tick cadence is the live-data writer''s cadence — '
    '2s in the spec, throttled to 60s when document.hidden.';

-- 019_account_daily_anchors.sql
-- 00:00 UTC daily snapshot of account state per (venue, account). Sole
-- purpose is to anchor "today's PnL" math on the Manager Live tab —
-- "today" = (latest equity − this row's total_equity_usd). Required
-- because exchange_snapshots is sampled at 5-min cadence and does not
-- guarantee a row exactly at midnight; without a stable anchor, the
-- KPI flickers with every fetch.
--
-- Multi-venue from day one (Live build prereq #1):
--   * In v1, every row will have venue='blofin' (account data lives
--     there per the venue split — Binance is reference market data
--     only).
--   * No migration needed when Binance trading lands later — just a
--     new venue value.
--
-- Written by a daily Celery beat job at 00:00 UTC; one row per
-- (venue, account, anchor_date). Idempotent by UNIQUE constraint —
-- a re-run on the same UTC day overwrites the existing row via
-- INSERT ... ON CONFLICT in the writer.

CREATE TABLE IF NOT EXISTS user_mgmt.account_daily_anchors (
    anchor_id           BIGSERIAL    PRIMARY KEY,
    venue               TEXT         NOT NULL,
    connection_id          UUID         NOT NULL,
    anchor_date         DATE         NOT NULL,
    captured_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    total_equity_usd    NUMERIC(20, 8)  NOT NULL,
    available_usd       NUMERIC(20, 8),
    used_margin_usd     NUMERIC(20, 8),
    unrealized_pnl_usd  NUMERIC(20, 8),
    open_position_count INTEGER      NOT NULL DEFAULT 0,
    raw_payload         JSONB,
    CONSTRAINT account_daily_anchors_unique UNIQUE (venue, connection_id, anchor_date),
    CONSTRAINT account_daily_anchors_connection_fk
        FOREIGN KEY (connection_id) REFERENCES user_mgmt.exchange_connections(connection_id),
    CONSTRAINT account_daily_anchors_venue_chk
        CHECK (venue IN ('blofin', 'binance'))
);

-- "Latest anchor for this account" is the dominant query (today's-PnL
-- math runs on every Live tab refresh), so index DESC by anchor_date.
CREATE INDEX IF NOT EXISTS idx_account_daily_anchors_venue_conn_date
    ON user_mgmt.account_daily_anchors (venue, connection_id, anchor_date DESC);

COMMENT ON TABLE user_mgmt.account_daily_anchors IS
    'Daily 00:00 UTC snapshot of account equity + margin state, used as '
    'the anchor point for today-PnL computation on the Manager Live tab. '
    'Distinct from exchange_snapshots (5-min cadence, intraday) — this '
    'table guarantees one and only one row per UTC day per account.';

COMMENT ON COLUMN user_mgmt.account_daily_anchors.venue IS
    'Exchange identifier — ''blofin'' in v1; ''binance'' reserved for when '
    'Binance account-data trading lands.';

COMMENT ON COLUMN user_mgmt.account_daily_anchors.connection_id IS
    'FK to user_mgmt.exchange_connections(connection_id). One connection '
    '= one venue-side account; the column name matches the existing '
    'naming convention rather than introducing an account_id alias.';

COMMENT ON COLUMN user_mgmt.account_daily_anchors.anchor_date IS
    'UTC calendar day this anchor represents. anchor_date = N means '
    'today-PnL on day N is computed as (latest equity − this row).';

COMMENT ON COLUMN user_mgmt.account_daily_anchors.captured_at IS
    'Wall-clock time the snapshot job ran. Usually within a few seconds '
    'of 00:00 UTC; jitter is tolerated.';

COMMENT ON COLUMN user_mgmt.account_daily_anchors.raw_payload IS
    'Full balance response from the venue, retained for audit and for '
    'fields not yet promoted to first-class columns.';

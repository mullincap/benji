-- =============================================================================
-- Benji3m — Full Database Schema v2
-- =============================================================================
-- Three PostgreSQL schemas:
--   market     — compiler + indexer output (raw data, leaderboards)
--   audit      — simulator (jobs, results, equity curves, charts)
--   user_mgmt  — allocator + manager (users, keys, trades, performance)
--
-- Changes from v1 (based on external review):
--   1. audit.strategy_versions — immutable versioning; jobs/allocations point
--      to strategy_version_id instead of strategy_id directly
--   2. user_mgmt.deployments — session lifecycle table between allocations
--      and session_returns; trader-blofin.py writes here per session
--   3. user_mgmt.daily_signal_items — normalized per-symbol signal rows;
--      daily_signals becomes the header, items hold rank/weight/reason
--   4. exchange_connections — expanded lifecycle: status enum, label,
--      last_validated_at, last_error_at, testnet flag
--   5. audit.strategy_performance — moved from user_mgmt to audit schema
--      (it is platform-level aggregate data, not user-specific data)
--
-- Apply with:
--   docker exec -i timescaledb psql -U quant -d marketdata < schema.sql
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SCHEMA IF NOT EXISTS market;
CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS user_mgmt;


-- =============================================================================
-- MARKET SCHEMA — Compiler + Indexer
-- =============================================================================

-- ─── Sources registry ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market.sources (
    source_id   SMALLINT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    type        TEXT NOT NULL,          -- 'futures' | 'spot' | 'options' | 'macro'
    exchange    TEXT,                   -- NULL for cross-exchange aggregates
    notes       TEXT
);

INSERT INTO market.sources (source_id, name, type, exchange, notes) VALUES
    (1, 'amberdata_binance', 'futures', 'binance', 'Primary futures source — metl.py'),
    (2, 'binance_direct',   'futures', 'binance', 'Direct Binance downloader — OHLCV backfill'),
    (3, 'blofin_direct',    'futures', 'blofin',  'Direct BloFin downloader — supplementary'),
    (4, 'coingecko',        'macro',   NULL,       'Daily market cap + rank'),
    (5, 'amberdata_spot',   'spot',    NULL,       'Future: Amberdata spot endpoints'),
    (6, 'amberdata_options','options', NULL,       'Future: Amberdata options endpoints')
ON CONFLICT DO NOTHING;

-- ─── Symbol registry ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market.symbols (
    symbol_id    SERIAL PRIMARY KEY,
    base         TEXT NOT NULL UNIQUE,
    binance_id   TEXT,
    blofin_id    TEXT,
    coingecko_id TEXT,
    active       BOOLEAN DEFAULT TRUE,
    listed_at    DATE,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Futures 1-minute data — primary hypertable ───────────────────────────────
DROP TABLE IF EXISTS market_data_1m;

CREATE TABLE IF NOT EXISTS market.futures_1m (
    timestamp_utc        TIMESTAMPTZ      NOT NULL,
    symbol_id            INTEGER          NOT NULL REFERENCES market.symbols(symbol_id),
    source_id            SMALLINT         NOT NULL REFERENCES market.sources(source_id),
    open                 DOUBLE PRECISION,
    high                 DOUBLE PRECISION,
    low                  DOUBLE PRECISION,
    close                DOUBLE PRECISION    NOT NULL,
    volume               DOUBLE PRECISION,
    quote_volume         DOUBLE PRECISION,
    trades               INTEGER,
    taker_buy_base_vol   DOUBLE PRECISION,
    taker_buy_quote_vol  DOUBLE PRECISION,
    open_interest        DOUBLE PRECISION,
    funding_rate         DOUBLE PRECISION,
    long_short_ratio     DOUBLE PRECISION,
    trade_delta          DOUBLE PRECISION,
    long_liqs            DOUBLE PRECISION,
    short_liqs           DOUBLE PRECISION,
    last_bid_depth       DOUBLE PRECISION,
    last_ask_depth       DOUBLE PRECISION,
    last_depth_imbalance DOUBLE PRECISION,
    last_spread_pct      DOUBLE PRECISION,
    spread_pct           DOUBLE PRECISION,
    bid_ask_imbalance    DOUBLE PRECISION,
    basis_pct            DOUBLE PRECISION,
    market_cap_usd       DOUBLE PRECISION,
    market_cap_rank      INTEGER,
    PRIMARY KEY (timestamp_utc, symbol_id, source_id)
);

SELECT create_hypertable('market.futures_1m', 'timestamp_utc', if_not_exists => TRUE);

ALTER TABLE market.futures_1m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol_id, source_id',
    timescaledb.compress_orderby   = 'timestamp_utc DESC'
);

SELECT add_compression_policy('market.futures_1m', INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_futures_1m_symbol_ts
    ON market.futures_1m (symbol_id, timestamp_utc DESC);

CREATE INDEX IF NOT EXISTS idx_futures_1m_source_symbol_ts
    ON market.futures_1m (source_id, symbol_id, timestamp_utc DESC);

-- ─── Continuous aggregate: per-(day, symbol) row counts ──────────────────────
-- Pre-computed denominator for the compiler coverage page. The raw query
-- against futures_1m would scan ~330M rows on the ALL preset (5-15s).
-- This cagg materializes COUNT(*) per (day, source_id, symbol_id) with an
-- hourly refresh policy and ~300K rows total, dropping coverage queries to
-- <100ms. materialized_only = false means the planner also merges the
-- latest unmaterialized futures_1m rows into the result, so today's data
-- is always accurate even though the materialized portion lags by ~1 hour.
--
-- DO blocks because TimescaleDB caggs and policies aren't fully idempotent
-- via plain CREATE/IF NOT EXISTS in older versions.
CREATE MATERIALIZED VIEW IF NOT EXISTS market.futures_1m_daily_symbol_count
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', timestamp_utc) AS day,
    source_id,
    symbol_id,
    COUNT(*) AS row_count
FROM market.futures_1m
GROUP BY 1, 2, 3
WITH NO DATA;

-- One-time backfill (run manually after first creation):
--   CALL refresh_continuous_aggregate('market.futures_1m_daily_symbol_count', NULL, NULL);

-- Hourly refresh policy. start_offset=7 days catches late-arriving rows
-- from metl.py reloads; end_offset=1 hour leaves the most recent hour
-- to the real-time aggregation path.
DO $$
BEGIN
    PERFORM add_continuous_aggregate_policy('market.futures_1m_daily_symbol_count',
        start_offset      => INTERVAL '7 days',
        end_offset        => INTERVAL '1 hour',
        schedule_interval => INTERVAL '1 hour'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

ALTER MATERIALIZED VIEW market.futures_1m_daily_symbol_count
SET (timescaledb.materialized_only = false);

-- ─── Cross-exchange derivatives analytics ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS market.derivatives_analytics (
    timestamp_utc         TIMESTAMPTZ      NOT NULL,
    symbol_id             INTEGER          NOT NULL REFERENCES market.symbols(symbol_id),
    granularity           TEXT             NOT NULL DEFAULT 'daily',
    oi_total_coin         DOUBLE PRECISION,
    oi_total_usd          DOUBLE PRECISION,
    liq_buy_usd           DOUBLE PRECISION,
    liq_sell_usd          DOUBLE PRECISION,
    volume_total_usd      DOUBLE PRECISION,
    funding_realized      DOUBLE PRECISION,
    funding_accumulated   DOUBLE PRECISION,
    apr_basis_30d         DOUBLE PRECISION,
    apr_basis_90d         DOUBLE PRECISION,
    realized_vol_1d       DOUBLE PRECISION,
    realized_vol_7d       DOUBLE PRECISION,
    realized_vol_30d      DOUBLE PRECISION,
    bid_ask_spread_bps    DOUBLE PRECISION,
    order_book_depth_usd  DOUBLE PRECISION,
    buy_pressure          DOUBLE PRECISION,
    sell_pressure         DOUBLE PRECISION,
    insurance_fund_units  DOUBLE PRECISION,
    PRIMARY KEY (timestamp_utc, symbol_id, granularity)
);

SELECT create_hypertable('market.derivatives_analytics', 'timestamp_utc', if_not_exists => TRUE);

-- ─── Daily market cap — CoinGecko ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market.marketcap_daily (
    date            DATE    NOT NULL,
    symbol_id       INTEGER NOT NULL REFERENCES market.symbols(symbol_id),
    coin_id         TEXT,
    market_cap_usd  DOUBLE PRECISION,
    market_cap_rank INTEGER,
    price_usd       DOUBLE PRECISION,
    volume_usd      DOUBLE PRECISION,
    PRIMARY KEY (date, symbol_id)
);

-- ─── Leaderboards — indexer output ────────────────────────────────────────────
-- Long format in DB for API queries; indexer also writes wide parquet files
-- as a performance cache for the simulator.
CREATE TABLE IF NOT EXISTS market.leaderboards (
    timestamp_utc  TIMESTAMPTZ NOT NULL,
    metric         TEXT        NOT NULL,
    variant        TEXT        NOT NULL,
    anchor_hour    SMALLINT    NOT NULL,
    rank           SMALLINT    NOT NULL,
    symbol_id      INTEGER     NOT NULL REFERENCES market.symbols(symbol_id),
    pct_change     DOUBLE PRECISION,
    PRIMARY KEY (timestamp_utc, metric, variant, anchor_hour, rank)
);

SELECT create_hypertable('market.leaderboards', 'timestamp_utc', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_leaderboards_metric
    ON market.leaderboards (metric, variant, timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_leaderboards_symbol_metric_ts
    ON market.leaderboards (symbol_id, metric, timestamp_utc DESC);

-- ─── Compiler jobs — ETL run tracking ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market.compiler_jobs (
    job_id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id           SMALLINT    NOT NULL REFERENCES market.sources(source_id),
    status              TEXT        NOT NULL DEFAULT 'queued'
                            CHECK (status IN ('queued','running','complete','failed','cancelled')),
    date_from           DATE        NOT NULL,
    date_to             DATE        NOT NULL,
    endpoints_enabled   TEXT[]      NOT NULL,
    symbols_total       INTEGER,
    symbols_done        INTEGER     DEFAULT 0,
    rows_written        BIGINT      DEFAULT 0,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    last_heartbeat      TIMESTAMPTZ,
    error_msg           TEXT,
    triggered_by        TEXT        DEFAULT 'cli'
                            CHECK (triggered_by IN ('ui', 'cli', 'scheduler')),
    run_tag             TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_compiler_jobs_status
    ON market.compiler_jobs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_compiler_jobs_dates
    ON market.compiler_jobs (date_from, date_to);

-- ─── Indexer jobs — leaderboard + overlap run tracking ───────────────────────
CREATE TABLE IF NOT EXISTS market.indexer_jobs (
    job_id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type            TEXT        NOT NULL
                            CHECK (job_type IN ('leaderboard', 'overlap', 'full')),
    status              TEXT        NOT NULL DEFAULT 'queued'
                            CHECK (status IN ('queued','running','complete','failed','cancelled')),
    metric              TEXT,
    date_from           DATE        NOT NULL,
    date_to             DATE        NOT NULL,
    params              JSONB,
    symbols_total       INTEGER,
    symbols_done        INTEGER     DEFAULT 0,
    rows_written        BIGINT      DEFAULT 0,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    last_heartbeat      TIMESTAMPTZ,
    error_msg           TEXT,
    triggered_by        TEXT        DEFAULT 'ui'
                            CHECK (triggered_by IN ('ui', 'cli', 'scheduler')),
    run_tag             TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_indexer_jobs_status
    ON market.indexer_jobs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_indexer_jobs_type_dates
    ON market.indexer_jobs (job_type, date_from, date_to);

-- ─── Spot 1-minute data — future layer ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market.spot_1m (
    timestamp_utc   TIMESTAMPTZ      NOT NULL,
    symbol_id       INTEGER          NOT NULL REFERENCES market.symbols(symbol_id),
    source_id       SMALLINT         NOT NULL REFERENCES market.sources(source_id),
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          DOUBLE PRECISION,
    quote_volume    DOUBLE PRECISION,
    trades          INTEGER,
    twap            DOUBLE PRECISION,
    vwap            DOUBLE PRECISION,
    PRIMARY KEY (timestamp_utc, symbol_id, source_id)
);

SELECT create_hypertable('market.spot_1m', 'timestamp_utc', if_not_exists => TRUE);

-- ─── Options quotes — future layer ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market.options_quotes (
    timestamp_utc    TIMESTAMPTZ      NOT NULL,
    symbol_id        INTEGER          NOT NULL REFERENCES market.symbols(symbol_id),
    exchange         TEXT             NOT NULL,
    expiry           DATE             NOT NULL,
    strike           DOUBLE PRECISION NOT NULL,
    option_type      CHAR(1)          NOT NULL,
    bid              DOUBLE PRECISION,
    ask              DOUBLE PRECISION,
    bid_size         DOUBLE PRECISION,
    ask_size         DOUBLE PRECISION,
    iv               DOUBLE PRECISION,
    delta            DOUBLE PRECISION,
    gamma            DOUBLE PRECISION,
    vega             DOUBLE PRECISION,
    theta            DOUBLE PRECISION,
    underlying_price DOUBLE PRECISION,
    PRIMARY KEY (timestamp_utc, symbol_id, exchange, expiry, strike, option_type)
);

SELECT create_hypertable('market.options_quotes', 'timestamp_utc', if_not_exists => TRUE);


-- =============================================================================
-- AUDIT SCHEMA — Simulator
-- =============================================================================

-- ─── Strategy registry — admin-controlled ─────────────────────────────────────
-- Identity of each strategy. Config lives in strategy_versions (immutable).
-- is_published and capital_cap_usd are mutable admin controls on the identity row.
CREATE TABLE IF NOT EXISTS audit.strategies (
    strategy_id      SERIAL PRIMARY KEY,
    name             TEXT    NOT NULL UNIQUE,
    display_name     TEXT    NOT NULL,
    description      TEXT,
    filter_mode      TEXT    NOT NULL,        -- canonical FilterMode from audit.py
    is_published     BOOLEAN DEFAULT FALSE,   -- admin toggles to expose in allocator
    capital_cap_usd  DOUBLE PRECISION,        -- NULL = no cap
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Strategy versions — IMMUTABLE config snapshots ───────────────────────────
-- Every time a strategy config changes, a new version row is created.
-- jobs and allocations point to strategy_version_id — never to strategy_id directly.
-- This guarantees backtest results are always tied to the exact config that ran.
-- published_at is set when the version is made available to users.
-- is_active = TRUE means this is the current live version for that strategy.
CREATE TABLE IF NOT EXISTS audit.strategy_versions (
    strategy_version_id  UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id          INTEGER NOT NULL REFERENCES audit.strategies(strategy_id),
    version_label        TEXT    NOT NULL,          -- e.g. 'v1.0', 'v1.1-tail-disp'
    config               JSONB   NOT NULL,          -- FROZEN parameter set from audit.py
    notes                TEXT,                      -- what changed in this version
    is_active            BOOLEAN DEFAULT FALSE,     -- TRUE = current live version
    published_at         TIMESTAMPTZ,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (strategy_id, version_label)
);

-- Enforces only one active version per strategy at the DB level.
-- Without this, multiple active versions can silently exist.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_version_per_strategy
    ON audit.strategy_versions (strategy_id)
    WHERE is_active = TRUE;

-- ─── Audit jobs ────────────────────────────────────────────────────────────────
-- One row per simulation run. Points to strategy_version_id for exact reproducibility.
CREATE TABLE IF NOT EXISTS audit.jobs (
    job_id               UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_version_id  UUID    NOT NULL REFERENCES audit.strategy_versions(strategy_version_id),
    status               TEXT    NOT NULL DEFAULT 'queued',  -- queued|running|complete|failed
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    error_msg            TEXT,
    date_from            DATE    NOT NULL,
    date_to              DATE    NOT NULL,
    -- Runtime overrides applied on top of version config (optional).
    -- NULL = ran with version config unchanged.
    config_overrides     JSONB,
    run_tag              TEXT,   -- e.g. 'baseline', 'sensitivity-test', 'pre-launch'
    -- Path to raw audit_output.txt on /mnt/quant-data volume.
    -- Frontend reads this directly for full drill-down; DB stores queryable scalars.
    output_path          TEXT
);

-- ─── Audit results ─────────────────────────────────────────────────────────────
-- One row per (job, filter_mode). Stores all queryable scalar metrics from the
-- institutional audit scorecard. The raw audit_output.txt (via jobs.output_path)
-- remains the source of truth for non-scalar outputs.
CREATE TABLE IF NOT EXISTS audit.results (
    result_id              UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                 UUID    NOT NULL REFERENCES audit.jobs(job_id),
    filter_mode            TEXT    NOT NULL,

    -- ── Core performance ──────────────────────────────────────────────────────
    sharpe                 DOUBLE PRECISION,
    sortino                DOUBLE PRECISION,
    calmar                 DOUBLE PRECISION,
    omega                  DOUBLE PRECISION,
    ulcer_index            DOUBLE PRECISION,
    profit_factor          DOUBLE PRECISION,
    max_dd_pct             DOUBLE PRECISION,
    cagr_pct               DOUBLE PRECISION,
    total_return_pct       DOUBLE PRECISION,
    equity_multiplier      DOUBLE PRECISION,
    starting_capital       DOUBLE PRECISION,
    ending_capital         DOUBLE PRECISION,

    -- ── Activity ──────────────────────────────────────────────────────────────
    active_days            INTEGER,
    flat_days              INTEGER,
    total_days             INTEGER,

    -- ── Walk-forward ──────────────────────────────────────────────────────────
    cv                     DOUBLE PRECISION,
    fa_oos_sharpe          DOUBLE PRECISION,
    fa_wf_cv               DOUBLE PRECISION,
    wf_pct_folds_positive  DOUBLE PRECISION,
    unstable_folds         INTEGER,
    wf_mean_sortino        DOUBLE PRECISION,
    wf_mean_r2             DOUBLE PRECISION,
    wf_mean_oos_dsr        DOUBLE PRECISION,

    -- ── Statistical validity ──────────────────────────────────────────────────
    dsr_pct                DOUBLE PRECISION,
    dsr_benchmark_sharpe   DOUBLE PRECISION,
    min_track_record_days  INTEGER,
    skewness               DOUBLE PRECISION,
    excess_kurtosis        DOUBLE PRECISION,
    jarque_bera_stat       DOUBLE PRECISION,
    ljung_box_q10          DOUBLE PRECISION,
    ljung_box_pval         DOUBLE PRECISION,
    mc_pct_losing          DOUBLE PRECISION,

    -- ── Risk ──────────────────────────────────────────────────────────────────
    var_5pct               DOUBLE PRECISION,
    cvar_5pct              DOUBLE PRECISION,
    var_1pct               DOUBLE PRECISION,
    cvar_1pct              DOUBLE PRECISION,
    weekly_cvar_1pct       DOUBLE PRECISION,
    ruin_33_pct            DOUBLE PRECISION,
    ruin_50_pct            DOUBLE PRECISION,
    ruin_75_pct            DOUBLE PRECISION,
    kelly_full             DOUBLE PRECISION,
    kelly_half             DOUBLE PRECISION,
    max_consec_losing_days INTEGER,
    drawdown_episodes      INTEGER,
    avg_dd_depth_pct       DOUBLE PRECISION,
    avg_dd_duration_days   DOUBLE PRECISION,
    pct_time_underwater    DOUBLE PRECISION,

    -- ── Win rates ─────────────────────────────────────────────────────────────
    win_rate_daily         DOUBLE PRECISION,
    win_rate_weekly        DOUBLE PRECISION,
    win_rate_monthly       DOUBLE PRECISION,
    avg_daily_ret_pct      DOUBLE PRECISION,
    avg_win_pct            DOUBLE PRECISION,
    avg_loss_pct           DOUBLE PRECISION,
    best_day_pct           DOUBLE PRECISION,
    worst_day_pct          DOUBLE PRECISION,
    best_week_pct          DOUBLE PRECISION,
    worst_week_pct         DOUBLE PRECISION,
    best_month_pct         DOUBLE PRECISION,
    worst_month_pct        DOUBLE PRECISION,

    -- ── Regime robustness ─────────────────────────────────────────────────────
    regime_is_sharpe        DOUBLE PRECISION,
    regime_oos_sharpe       DOUBLE PRECISION,
    regime_sharpe_decay_pct DOUBLE PRECISION,
    regime_cagr_ratio       DOUBLE PRECISION,
    equity_r2               DOUBLE PRECISION,

    -- ── Parameter robustness ──────────────────────────────────────────────────
    neighbor_plateau_ratio  DOUBLE PRECISION,
    neighbor_sharpe_median  DOUBLE PRECISION,
    neighbor_sharpe_p10     DOUBLE PRECISION,
    neighbor_sharpe_p25     DOUBLE PRECISION,
    neighbor_sharpe_std     DOUBLE PRECISION,
    slippage_sharpe_2x      DOUBLE PRECISION,
    cost_elasticity         DOUBLE PRECISION,

    -- ── Vol-target leverage ───────────────────────────────────────────────────
    vol_mean_boost          DOUBLE PRECISION,
    vol_min_boost           DOUBLE PRECISION,
    vol_max_boost           DOUBLE PRECISION,
    vol_days_at_floor       INTEGER,

    -- ── Scorecard ─────────────────────────────────────────────────────────────
    scorecard_score         INTEGER,
    scorecard_total         INTEGER,
    grade                   TEXT,

    created_at              TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Daily equity curves ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.equity_curves (
    result_id    UUID    NOT NULL REFERENCES audit.results(result_id),
    date         DATE    NOT NULL,
    equity       DOUBLE PRECISION,
    daily_return DOUBLE PRECISION,
    drawdown     DOUBLE PRECISION,
    is_active    BOOLEAN,
    PRIMARY KEY (result_id, date)
);

-- ─── Walk-forward fold results ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.wf_folds (
    result_id   UUID     NOT NULL REFERENCES audit.results(result_id),
    fold_index  SMALLINT NOT NULL,
    train_from  DATE,
    train_to    DATE,
    test_from   DATE,
    test_to     DATE,
    test_days   SMALLINT,
    sharpe      DOUBLE PRECISION,
    cagr_pct    DOUBLE PRECISION,
    max_dd_pct  DOUBLE PRECISION,
    sortino     DOUBLE PRECISION,
    r_squared   DOUBLE PRECISION,
    dsr_pct     DOUBLE PRECISION,
    fp_pct      DOUBLE PRECISION,
    is_stable   BOOLEAN,
    PRIMARY KEY (result_id, fold_index)
);

-- ─── Parameter sweep surface results ──────────────────────────────────────────
-- sweep_name identifies which sweep produced this row e.g. 'L_HIGH', 'TrailWide'
CREATE TABLE IF NOT EXISTS audit.param_sweeps (
    sweep_id        UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID    NOT NULL REFERENCES audit.jobs(job_id),
    sweep_name      TEXT    NOT NULL,
    param_x         TEXT    NOT NULL,
    param_y         TEXT,
    value_x         DOUBLE PRECISION,
    value_y         DOUBLE PRECISION,
    sharpe          DOUBLE PRECISION,
    max_dd_pct      DOUBLE PRECISION,
    cagr_pct        DOUBLE PRECISION,
    wf_cv           DOUBLE PRECISION,
    unstable_folds  DOUBLE PRECISION,
    is_baseline     BOOLEAN DEFAULT FALSE
);

-- ─── Sensitivity curves ───────────────────────────────────────────────────────
-- Multi-row structured test outputs: slippage, leverage, capacity, shock injection,
-- capped return, top-N removal. One row per (result, curve_type, x_value).
-- curve_type: 'slippage' | 'leverage' | 'capacity' | 'shock' | 'cap_return' | 'topn_remove'
CREATE TABLE IF NOT EXISTS audit.sensitivity_curves (
    curve_id    UUID             PRIMARY KEY DEFAULT gen_random_uuid(),
    result_id   UUID             NOT NULL REFERENCES audit.results(result_id),
    curve_type  TEXT             NOT NULL,
    x_label     TEXT             NOT NULL,
    x_value     DOUBLE PRECISION NOT NULL,
    x_value2    DOUBLE PRECISION,
    sharpe      DOUBLE PRECISION,
    cagr_pct    DOUBLE PRECISION,
    max_dd_pct  DOUBLE PRECISION,
    y_extra     DOUBLE PRECISION,
    flag        TEXT
);

-- ─── Chart file references ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.charts (
    chart_id   UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    result_id  UUID    NOT NULL REFERENCES audit.results(result_id),
    chart_type TEXT    NOT NULL,
    file_path  TEXT    NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Strategy public performance ───────────────────────────────────────────────
-- Platform-level aggregate performance per published strategy version per day.
-- Uses strategy_version_id to preserve version lineage — consistent with how
-- jobs and allocations both reference the frozen version, not the identity row.
-- Visible to all users in allocator and manager modules.
-- Does NOT expose any individual user equity or returns.
CREATE TABLE IF NOT EXISTS audit.strategy_performance (
    strategy_version_id  UUID    NOT NULL REFERENCES audit.strategy_versions(strategy_version_id),
    date                 DATE    NOT NULL,
    total_aum_usd        NUMERIC(18,2),
    n_allocations        INTEGER,
    daily_return_pct     DOUBLE PRECISION,
    cumulative_return    DOUBLE PRECISION,
    drawdown             DOUBLE PRECISION,
    PRIMARY KEY (strategy_version_id, date)
);


-- =============================================================================
-- USER_MGMT SCHEMA — Allocator + Manager
-- =============================================================================

-- ─── Users ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_mgmt.users (
    user_id       UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT    NOT NULL UNIQUE,
    password_hash TEXT,
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    last_login    TIMESTAMPTZ
);

-- Migration for existing deployments:
ALTER TABLE user_mgmt.users ADD COLUMN IF NOT EXISTS password_hash TEXT;
ALTER TABLE user_mgmt.users ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- ─── User sessions (DB-backed, replaces flat-file JSON) ──────────────────────
CREATE TABLE IF NOT EXISTS user_mgmt.user_sessions (
    token      TEXT        PRIMARY KEY,
    user_id    UUID        NOT NULL REFERENCES user_mgmt.users(user_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user
    ON user_mgmt.user_sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires
    ON user_mgmt.user_sessions (expires_at);

-- ─── Exchange connections ──────────────────────────────────────────────────────
-- API keys stored AES-256-GCM encrypted at the application layer.
-- The database NEVER holds plaintext keys.
-- status lifecycle: active | pending_validation | invalid | revoked | errored
CREATE TABLE IF NOT EXISTS user_mgmt.exchange_connections (
    connection_id      UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id            UUID    NOT NULL REFERENCES user_mgmt.users(user_id),
    exchange           TEXT    NOT NULL,         -- 'blofin' | 'binance'
    label              TEXT,                     -- user-defined name e.g. 'My BloFin Main'
    api_key_enc        TEXT    NOT NULL,
    api_secret_enc     TEXT    NOT NULL,
    passphrase_enc     TEXT,                     -- BloFin-specific; NULL for other exchanges
    testnet            BOOLEAN DEFAULT FALSE,
    status             TEXT    NOT NULL DEFAULT 'pending_validation'
                            CHECK (status IN ('pending_validation','active','invalid','revoked','errored')),
    last_validated_at  TIMESTAMPTZ,
    last_error_at      TIMESTAMPTZ,
    last_error_msg     TEXT,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Strategy allocations ──────────────────────────────────────────────────────
-- Points to strategy_version_id so the user is always running a specific
-- frozen config — not a moving target.
CREATE TABLE IF NOT EXISTS user_mgmt.allocations (
    allocation_id        UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id              UUID    NOT NULL REFERENCES user_mgmt.users(user_id),
    strategy_version_id  UUID    NOT NULL REFERENCES audit.strategy_versions(strategy_version_id),
    connection_id        UUID    NOT NULL REFERENCES user_mgmt.exchange_connections(connection_id),
    capital_usd          NUMERIC(18,2) NOT NULL,
    status               TEXT    NOT NULL DEFAULT 'active',  -- active|paused|closed
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW(),
    closed_at            TIMESTAMPTZ,
    -- Composite unique key so deployments can FK-enforce strategy_version consistency.
    UNIQUE (allocation_id, strategy_version_id)
);

-- ─── Daily signals — header ────────────────────────────────────────────────────
-- signal_batch_id is a surrogate UUID PK for extensibility — supports reruns,
-- alternate signal modes, and intraday batches without changing the FK structure.
-- (signal_date, strategy_version_id) has a UNIQUE constraint for the normal case.
CREATE TABLE IF NOT EXISTS user_mgmt.daily_signals (
    signal_batch_id      UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_date          DATE    NOT NULL,
    strategy_version_id  UUID    NOT NULL REFERENCES audit.strategy_versions(strategy_version_id),
    computed_at          TIMESTAMPTZ,
    sit_flat             BOOLEAN NOT NULL DEFAULT FALSE,
    filter_name          TEXT,
    filter_reason        TEXT,
    signal_source        TEXT    NOT NULL DEFAULT 'live'
                            CHECK (signal_source IN ('live', 'backtest', 'research')),
    UNIQUE (signal_date, strategy_version_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_signals_source
    ON user_mgmt.daily_signals (signal_source, signal_date DESC);

-- ─── Daily signal items — per-symbol rows ─────────────────────────────────────
-- Normalized from the JSONB symbols array in v1.
-- References signal_batch_id so reruns and alternate modes are supported cleanly.
CREATE TABLE IF NOT EXISTS user_mgmt.daily_signal_items (
    signal_batch_id  UUID     NOT NULL REFERENCES user_mgmt.daily_signals(signal_batch_id),
    symbol_id        INTEGER  NOT NULL REFERENCES market.symbols(symbol_id),
    rank             SMALLINT NOT NULL,   -- 1 = top-ranked symbol
    weight           DOUBLE PRECISION,   -- NULL = equal weight
    reason           TEXT,               -- optional per-symbol annotation
    is_selected      BOOLEAN  NOT NULL DEFAULT TRUE,
    PRIMARY KEY (signal_batch_id, symbol_id)
);

-- Each rank position must be unique within a batch — no two symbols share rank 1.
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_rank_per_batch
    ON user_mgmt.daily_signal_items (signal_batch_id, rank);

-- ─── Deployments — session lifecycle ──────────────────────────────────────────
-- One row per (allocation, trading session date).
-- Bridges the gap between allocations (standing capital assignment) and
-- session_returns (realized outcome). trader-blofin.py writes lifecycle
-- events here throughout the session.
-- status: pending | entered | stopped | filled | closed | failed | skipped
CREATE TABLE IF NOT EXISTS user_mgmt.deployments (
    deployment_id        UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    allocation_id        UUID    NOT NULL REFERENCES user_mgmt.allocations(allocation_id),
    signal_date          DATE    NOT NULL,
    -- strategy_version_id stored explicitly for audit integrity.
    -- Records the exact version that ran on this deployment, even if the
    -- allocation is later migrated or cloned to a different version.
    strategy_version_id  UUID    NOT NULL REFERENCES audit.strategy_versions(strategy_version_id),
    status               TEXT    NOT NULL DEFAULT 'pending',
    -- Session timing
    session_open_at      TIMESTAMPTZ,
    entry_at             TIMESTAMPTZ,
    exit_at              TIMESTAMPTZ,
    -- Config snapshot used for this deployment
    strategy_params      JSONB,
    -- Entry state
    entry_leverage       DOUBLE PRECISION,
    vol_boost            DOUBLE PRECISION,
    capital_deployed_usd NUMERIC(18,2),
    -- Signal reference
    n_symbols            INTEGER,
    -- Notes
    notes                TEXT,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (allocation_id, signal_date),
    -- Enforces that deployment version matches allocation version declaratively.
    -- Cleaner than a trigger: same guarantee, no PL/pgSQL maintenance.
    CONSTRAINT fk_deployment_strategy_match
        FOREIGN KEY (allocation_id, strategy_version_id)
        REFERENCES user_mgmt.allocations (allocation_id, strategy_version_id)
);

-- ─── Session returns — output of trader-blofin.py ─────────────────────────────
-- One row per (deployment). Replaces blofin_returns_log.csv.
-- References deployment_id so every return is traceable to the exact
-- session lifecycle and config that produced it.
-- exit_reason: 'port_sl'|'port_tsl'|'early_fill'|'session_close'|
--              'filtered'|'no_entry_conviction'|'missed_window'|'stale_closed'
-- signal_date derived through deployment_id -> deployments.signal_date
CREATE TABLE IF NOT EXISTS user_mgmt.session_returns (
    deployment_id    UUID    PRIMARY KEY REFERENCES user_mgmt.deployments(deployment_id),
    net_return_pct   DOUBLE PRECISION,
    gross_return_pct DOUBLE PRECISION,
    exit_reason      TEXT,
    effective_lev    DOUBLE PRECISION,
    logged_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Daily performance snapshots ──────────────────────────────────────────────
-- Private per-user equity curve. Updated nightly.
CREATE TABLE IF NOT EXISTS user_mgmt.performance_daily (
    allocation_id   UUID    NOT NULL REFERENCES user_mgmt.allocations(allocation_id),
    date            DATE    NOT NULL,
    equity_usd      NUMERIC(18,2),
    daily_return    DOUBLE PRECISION,
    drawdown        DOUBLE PRECISION,
    PRIMARY KEY (allocation_id, date)
);


-- ─── Live-trader portfolio sessions ───────────────────────────────────────────
-- One row per traded day; bar-by-bar timeline lives in portfolio_bars.
-- Written live by trader-blofin.py every 5 minutes. The trader also writes an
-- NDJSON copy at blofin_execution_reports/portfolios/YYYY-MM-DD.ndjson as a
-- local backup in case of DB connectivity issues. The Manager UI reads only
-- from these tables. Flat days (filtered / no_entry_conviction / missed_window)
-- do NOT produce a row — there is no bar data to capture.
CREATE TABLE IF NOT EXISTS user_mgmt.portfolio_sessions (
    portfolio_session_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_date             DATE         NOT NULL,
    session_start_utc       TIMESTAMPTZ  NOT NULL,
    exit_time_utc           TIMESTAMPTZ,                                -- set at close
    status                  TEXT         NOT NULL DEFAULT 'active'
                                CHECK (status IN ('active', 'closed')),
    exit_reason             TEXT,                                       -- null while active
    symbols                 TEXT[]       NOT NULL,                      -- signaled+priceable universe
    entered                 TEXT[]       NOT NULL,                      -- subset with actual positions
    eff_lev                 NUMERIC(6,4) NOT NULL,
    lev_int                 SMALLINT     NOT NULL,

    -- Cached summary fields maintained by the trader on each bar append so
    -- the list endpoint returns without aggregating bars per row.
    bars_count              INTEGER      NOT NULL DEFAULT 0,
    final_portfolio_return  NUMERIC(10,8),
    peak_portfolio_return   NUMERIC(10,8),
    max_dd_from_peak        NUMERIC(10,8),
    sym_stops               TEXT[]       NOT NULL DEFAULT '{}',

    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (signal_date)
);

-- One row per 5-min bar. Not a hypertable — ~210 bars/day × 250 trading days
-- = ~52k rows/year, well within regular-PG territory. Composite PK gives
-- clustered reads for "all bars for this session in order".
CREATE TABLE IF NOT EXISTS user_mgmt.portfolio_bars (
    portfolio_session_id  UUID          NOT NULL
                              REFERENCES user_mgmt.portfolio_sessions(portfolio_session_id)
                              ON DELETE CASCADE,
    bar_number            INTEGER       NOT NULL,                       -- 7..216 per session
    bar_timestamp_utc     TIMESTAMPTZ   NOT NULL,
    portfolio_return      NUMERIC(10,8) NOT NULL,                       -- audit "incr"
    peak_return           NUMERIC(10,8) NOT NULL,
    symbol_returns        JSONB         NOT NULL,                       -- {"BTC-USDT": 0.0015, ...}
    stopped               TEXT[]        NOT NULL DEFAULT '{}',          -- sym-stop snapshot as of this bar
    logged_at             TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (portfolio_session_id, bar_number)
);


-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy
    ON audit.strategy_versions (strategy_id);
CREATE INDEX IF NOT EXISTS idx_jobs_strategy_version
    ON audit.jobs (strategy_version_id);
CREATE INDEX IF NOT EXISTS idx_results_job
    ON audit.results (job_id);
CREATE INDEX IF NOT EXISTS idx_results_filter_mode
    ON audit.results (filter_mode);
CREATE INDEX IF NOT EXISTS idx_allocations_user
    ON user_mgmt.allocations (user_id);
CREATE INDEX IF NOT EXISTS idx_allocations_version
    ON user_mgmt.allocations (strategy_version_id);
-- UNIQUE (allocation_id, signal_date) covers composite lookups.
-- Separate single-column index covers WHERE allocation_id = ? queries
-- (user dashboards, allocation history) without scanning the composite index.
CREATE INDEX IF NOT EXISTS idx_deployments_allocation
    ON user_mgmt.deployments (allocation_id);
CREATE INDEX IF NOT EXISTS idx_deployments_signal_date
    ON user_mgmt.deployments (signal_date);
-- idx_session_returns_date removed: signal_date lives on deployments, join there
CREATE INDEX IF NOT EXISTS idx_signal_items_batch
    ON user_mgmt.daily_signal_items (signal_batch_id);
CREATE INDEX IF NOT EXISTS idx_perf_daily_date
    ON user_mgmt.performance_daily (date);
CREATE INDEX IF NOT EXISTS idx_strategy_perf_date
    ON audit.strategy_performance (strategy_version_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_connections_user
    ON user_mgmt.exchange_connections (user_id);
CREATE INDEX IF NOT EXISTS idx_portfolio_sessions_status_date
    ON user_mgmt.portfolio_sessions (status, signal_date DESC);

-- ─── Exchange snapshots (balance + position logger) ───────────────────────────
CREATE TABLE IF NOT EXISTS user_mgmt.exchange_snapshots (
    snapshot_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id    UUID        NOT NULL REFERENCES user_mgmt.exchange_connections(connection_id),
    snapshot_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_equity_usd NUMERIC(18,4),
    available_usd    NUMERIC(18,4),
    used_margin_usd  NUMERIC(18,4),
    unrealized_pnl   NUMERIC(18,4),
    positions        JSONB,
    fetch_ok         BOOLEAN     NOT NULL DEFAULT TRUE,
    error_msg        TEXT
);

CREATE INDEX IF NOT EXISTS idx_exchange_snapshots_connection_time
    ON user_mgmt.exchange_snapshots (connection_id, snapshot_at DESC);

CREATE INDEX IF NOT EXISTS idx_exchange_snapshots_time
    ON user_mgmt.exchange_snapshots (snapshot_at DESC);

-- ─── Manager conversations + messages ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_mgmt.manager_conversations (
    conversation_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title            TEXT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_mgmt.manager_messages (
    message_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id  UUID NOT NULL REFERENCES user_mgmt.manager_conversations(conversation_id) ON DELETE CASCADE,
    role             TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content          TEXT NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_manager_messages_conversation
    ON user_mgmt.manager_messages (conversation_id, created_at ASC);


-- =============================================================================
-- MIGRATION: market_data_1m -> market.futures_1m
-- =============================================================================
-- Uncomment and run once after verifying the new schema is correct.
--
-- INSERT INTO market.symbols (base, binance_id)
-- SELECT DISTINCT symbol, symbol FROM market_data_1m ON CONFLICT DO NOTHING;
--
-- INSERT INTO market.futures_1m
-- SELECT
--     timestamp_utc,
--     s.symbol_id,
--     1 AS source_id,
--     NULL AS open, NULL AS high, NULL AS low,
--     price AS close,
--     volume, quote_volume, trades,
--     taker_buy_base_vol, taker_buy_quote_vol,
--     open_interest, funding_rate, long_short_ratio,
--     trade_delta, long_liqs, short_liqs,
--     last_bid_depth, last_ask_depth, last_depth_imbalance,
--     last_spread_pct, spread_pct, bid_ask_imbalance, basis_pct,
--     market_cap_usd, market_cap_rank
-- FROM market_data_1m m
-- JOIN market.symbols s ON s.base = m.symbol
-- ON CONFLICT DO NOTHING;

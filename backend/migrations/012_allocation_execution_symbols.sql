-- Per-symbol execution telemetry for each (allocation_id, session_date).
-- Populated by the trader at session close (see backend/app/cli/trader_blofin.py
-- _run_fresh_session_for_allocation + _run_monitoring_loop). Consumed by the
-- Manager Execution view's per-row expand.

CREATE TABLE IF NOT EXISTS user_mgmt.allocation_execution_symbols (
    allocation_id        UUID        NOT NULL,
    session_date         DATE        NOT NULL,
    inst_id              TEXT        NOT NULL,

    side                 TEXT,
    target_contracts     NUMERIC,
    filled_contracts     NUMERIC,
    fill_pct             NUMERIC,

    est_entry_price      NUMERIC,
    fill_entry_price     NUMERIC,
    entry_slippage_bps   NUMERIC,

    est_exit_price       NUMERIC,
    fill_exit_price      NUMERIC,
    exit_slippage_bps    NUMERIC,

    pnl_usd              NUMERIC,
    pnl_pct              NUMERIC,

    exit_reason          TEXT,       -- sym_stop | session_close | port_sl | port_tsl | early_fill | other
    retry_rounds         INTEGER     DEFAULT 0,
    sym_stopped          BOOLEAN     DEFAULT FALSE,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (allocation_id, session_date, inst_id),
    FOREIGN KEY (allocation_id)
        REFERENCES user_mgmt.allocations(allocation_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aes_session_date
    ON user_mgmt.allocation_execution_symbols (session_date DESC);

CREATE INDEX IF NOT EXISTS idx_aes_alloc_date
    ON user_mgmt.allocation_execution_symbols (allocation_id, session_date DESC);

-- 018_audit_daily_baskets.sql
-- Per-day basket persistence for audit jobs. Closes the gap surfaced
-- 2026-04-26: the audit pipeline does not currently persist the
-- per-day list of symbols traded in any structured table or file —
-- only the gate decision (audit.equity_curves.is_active) and the
-- aggregate metrics (audit.results) are stored. The deploys CSV that
-- the audit consumes (`/mnt/quant-data/deploys_overlap_*.csv`) is
-- regenerated nightly by overlap_analysis, so the basket inputs to
-- past audits are overwritten and unrecoverable.
--
-- This table holds one row per (job_id, date). Baskets are
-- filter-independent at the audit level (the same basket is evaluated
-- under each filter_mode; only the trade/sit-flat decision differs)
-- so the foreign key is to audit.jobs rather than audit.results.

CREATE TABLE IF NOT EXISTS audit.daily_baskets (
    daily_basket_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id           UUID         NOT NULL REFERENCES audit.jobs(job_id),
    date             DATE         NOT NULL,
    basket           TEXT[]       NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (job_id, date)
);

CREATE INDEX IF NOT EXISTS idx_audit_daily_baskets_job_date
    ON audit.daily_baskets (job_id, date);

COMMENT ON TABLE audit.daily_baskets IS
    'Per-day basket of base symbols evaluated by an audit job. '
    'Filter-independent — the basket is the same across all filter '
    'modes for a given (job, date); only the sit_flat decision in '
    'audit.equity_curves differs across filters. Written by audit.py '
    'at end-of-run from df_4x.columns.';

COMMENT ON COLUMN audit.daily_baskets.basket IS
    'Base symbols (no quote currency suffix), e.g. {"BTC","ETH","SOL"}. '
    'Order is the deploys-CSV order (R1, R2, ...).';

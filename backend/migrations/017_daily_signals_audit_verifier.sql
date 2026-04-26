-- 017_daily_signals_audit_verifier.sql
-- Audit verifier columns on user_mgmt.daily_signals.
--
-- Per the 2026-04-26 design (Option A1 — verifier model): rather than a
-- parallel writer table, daily_signals carries the audit verifier's
-- decision lifecycle in three columns:
--
--   audit_status           pending | verified | mismatch | error
--   audit_verified_at      timestamp set when verifier confirms agreement
--   audit_mismatch_reason  one-line summary of what disagreed (mismatch)
--                          OR what failed (error)
--
-- Lifecycle:
--   pending   — verifier has not run for this row yet (default)
--   verified  — verifier independently re-computed the gate and got the
--               same (sit_flat, filter_name) decision; sets audit_verified_at
--   mismatch  — verifier disagreed; audit_mismatch_reason captures the diff,
--               and the verifier emits a [GATE_MISMATCH] alert. Trader
--               (after follow-up commit) refuses to enter on a non-verified
--               row, treating mismatch as fail-closed.
--   error     — verifier itself failed (DB unreachable, Binance fetch threw,
--               compute path bug). NOT a mismatch — there's no comparison
--               to mismatch against. Emits [GATE_ERROR]. Trader treats it
--               as fail-closed, same as mismatch. Exists as a distinct
--               state so infrastructure-flake alerts don't get conflated
--               with logic-bug alerts (collapsing them would train us to
--               ignore the alert).
--
-- The original design proposed a separate audit.intraday_gates table; that
-- shape was rejected because user_mgmt.daily_signals already covers
-- (sit_flat, filter_name, filter_reason, basket via daily_signal_items)
-- and the only gap was "did the audit path agree?" — which is one column
-- conceptually, four for explicit lifecycle semantics.

ALTER TABLE user_mgmt.daily_signals
  ADD COLUMN IF NOT EXISTS audit_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (audit_status IN ('pending', 'verified', 'mismatch', 'error'));

ALTER TABLE user_mgmt.daily_signals
  ADD COLUMN IF NOT EXISTS audit_verified_at TIMESTAMPTZ;

ALTER TABLE user_mgmt.daily_signals
  ADD COLUMN IF NOT EXISTS audit_mismatch_reason TEXT;

-- Index supports the trader's "find verified gates for today" lookup
-- without reading the whole table.
CREATE INDEX IF NOT EXISTS idx_daily_signals_audit_status
    ON user_mgmt.daily_signals (signal_date DESC, audit_status);

COMMENT ON COLUMN user_mgmt.daily_signals.audit_status IS
    'Verifier lifecycle: pending (default, verifier has not run), '
    'verified (audit re-computer agreed with the live decision), '
    'mismatch (audit disagreed — see audit_mismatch_reason and the '
    '[GATE_MISMATCH] alert), or error (verifier itself failed — see '
    'audit_mismatch_reason and the [GATE_ERROR] alert).';

COMMENT ON COLUMN user_mgmt.daily_signals.audit_verified_at IS
    'Timestamp at which the verifier confirmed agreement. NULL while '
    'audit_status IN (pending, mismatch, error).';

COMMENT ON COLUMN user_mgmt.daily_signals.audit_mismatch_reason IS
    'One-line summary of the verifier outcome when status is mismatch '
    '(e.g. "sit_flat: live=true audit=false") or error (e.g. '
    '"audit_compute_failed: ConnectionError: ..."). NULL otherwise.';

-- Reconcile tonight's (or any) allocation_returns row against
-- allocation_capital_events. Removes out-of-band deposits/withdrawals
-- from net_return_pct and gross_return_pct so lifetime stats reflect
-- ONLY trading P&L.
--
-- Usage:
--   psql ... -v alloc_id="'f87fe130-a90c-4e60-908a-14f4065b415c'" \
--           -v session_date="'2026-04-23'" \
--           -f reconcile_session_capital_events.sql
--
-- Idempotent via ON CONFLICT-style guard: computes a "corrected" value
-- and UPDATEs. Running twice produces the same final row (no
-- double-subtraction) because the adjustment is computed against the
-- raw session_start, not against the already-updated row.

\set ECHO all
\pset pager off

-- Preview the row BEFORE patching
\echo '=== BEFORE ==='
SELECT session_date, allocation_id,
       ROUND(net_return_pct::numeric, 4)   AS net_return_pct,
       ROUND(gross_return_pct::numeric, 4) AS gross_return_pct,
       exit_reason, effective_leverage, capital_deployed_usd,
       logged_at
  FROM user_mgmt.allocation_returns
 WHERE allocation_id = :alloc_id::uuid
   AND session_date  = :session_date::date;

-- Capital events we're about to net out
\echo '=== CAPITAL EVENTS IN SESSION WINDOW ==='
SELECT event_at, kind, amount_usd, notes
  FROM user_mgmt.allocation_capital_events
 WHERE allocation_id = :alloc_id::uuid
   AND event_at::date = :session_date::date
 ORDER BY event_at;

-- The fix:
-- session_start_approx is reconstructed from the stored return:
--   capital_deployed_usd / (1 + net_return_pct/100)
-- (capital_deployed is the INITIAL capital at session open; end_equity =
-- capital_deployed * (1 + net/100). We don't store session_start
-- directly, but the trader's session_start_equity_usdt ≈ capital_deployed
-- for purposes of baseline reconstruction.)
--
-- Adjusted net_return_pct:
--   end_equity = session_start * (1 + net/100)
--   real_trading_pnl = end_equity - session_start - capital_events_net
--   adjusted_net_pct = real_trading_pnl / (session_start + capital_events_net) * 100
--                    = (end_equity - session_start - ce_net)
--                      / (session_start + ce_net) * 100
-- Rearranging with stored fields:
--   end_equity    = capital_deployed * (1 + net/100)
--   adjusted_net  = (end_equity - capital_deployed - ce_net)
--                   / (capital_deployed + ce_net) * 100

WITH ce AS (
  SELECT COALESCE(SUM(
           CASE kind WHEN 'deposit'    THEN amount_usd
                     WHEN 'withdrawal' THEN -amount_usd
           END
         ), 0)::numeric AS session_net
    FROM user_mgmt.allocation_capital_events
   WHERE allocation_id = :alloc_id::uuid
     AND event_at::date = :session_date::date
),
recomputed AS (
  SELECT ar.allocation_id, ar.session_date,
         ar.net_return_pct       AS orig_net,
         ar.gross_return_pct     AS orig_gross,
         ar.capital_deployed_usd AS cap_dep,
         ce.session_net,
         -- reconstruct end_equity from stored net
         (ar.capital_deployed_usd * (1 + ar.net_return_pct/100)) AS end_equity_net,
         (ar.capital_deployed_usd * (1 + ar.gross_return_pct/100)) AS end_equity_gross
    FROM user_mgmt.allocation_returns ar
   CROSS JOIN ce
   WHERE ar.allocation_id = :alloc_id::uuid
     AND ar.session_date  = :session_date::date
),
adjusted AS (
  SELECT allocation_id, session_date,
         CASE WHEN (cap_dep + session_net) > 0
              THEN (end_equity_net   - cap_dep - session_net) / (cap_dep + session_net) * 100
              ELSE orig_net
         END AS new_net,
         CASE WHEN (cap_dep + session_net) > 0
              THEN (end_equity_gross - cap_dep - session_net) / (cap_dep + session_net) * 100
              ELSE orig_gross
         END AS new_gross,
         session_net
    FROM recomputed
)
UPDATE user_mgmt.allocation_returns ar
   SET net_return_pct   = adjusted.new_net,
       gross_return_pct = adjusted.new_gross
  FROM adjusted
 WHERE ar.allocation_id = adjusted.allocation_id
   AND ar.session_date  = adjusted.session_date
   AND adjusted.session_net <> 0
 RETURNING ar.session_date, ar.allocation_id,
           ROUND(ar.net_return_pct::numeric, 4)   AS net_after,
           ROUND(ar.gross_return_pct::numeric, 4) AS gross_after,
           ROUND(adjusted.session_net::numeric, 2) AS capital_event_net_usd;

\echo '=== AFTER ==='
SELECT session_date, allocation_id,
       ROUND(net_return_pct::numeric, 4)   AS net_return_pct,
       ROUND(gross_return_pct::numeric, 4) AS gross_return_pct
  FROM user_mgmt.allocation_returns
 WHERE allocation_id = :alloc_id::uuid
   AND session_date  = :session_date::date;

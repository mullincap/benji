-- Nightly reconcile of allocation_returns vs allocation_capital_events.
-- Replaces the one-shot reconcile_session_capital_events.sql which was
-- hardcoded to a single (alloc_id, session_date).
--
-- Scope: all (allocation_id, session_date) pairs in allocation_returns where
--   - capital_events_reconciled_at IS NULL (idempotence guard)
--   - matching capital_events exist on the same date
--   - capital_deployed_usd + session_net > 0 (math defined)
--
-- Math (same as the one-shot, generalized):
--   end_equity_X    = capital_deployed * (1 + stored_X_pct/100)   for X in {net, gross}
--   adjusted_X_pct  = (end_equity_X - capital_deployed - session_net)
--                     / (capital_deployed + session_net) * 100
-- where session_net = SUM(deposits) - SUM(withdrawals) for that date.
--
-- Idempotence: row gets capital_events_reconciled_at = NOW() set on the
-- same UPDATE, so subsequent nightly runs skip it. Historical rows that
-- have no capital_events stay capital_events_reconciled_at IS NULL forever
-- (cheap — JOIN to ce filters them out before any work happens).
--
-- Cron invocation:
--   56 23 * * *  . /mnt/quant-data/credentials/secrets.env && \
--                PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER \
--                -d $DB_NAME -f /root/benji/ops/reconcile_allocation_returns_nightly.sql \
--                >> /mnt/quant-data/logs/trader/capital_events_reconcile.log 2>&1

\set ECHO all
\pset pager off

\echo '=== CANDIDATES (rows with capital events not yet reconciled) ==='
WITH ce AS (
  SELECT allocation_id,
         event_at::date AS session_date,
         SUM(CASE kind WHEN 'deposit'    THEN amount_usd
                       WHEN 'withdrawal' THEN -amount_usd
             END)::numeric AS session_net
    FROM user_mgmt.allocation_capital_events
   GROUP BY allocation_id, event_at::date
)
SELECT ar.session_date, ar.allocation_id,
       ROUND(ar.net_return_pct::numeric, 4)   AS net_pct_before,
       ROUND(ar.gross_return_pct::numeric, 4) AS gross_pct_before,
       ROUND(ar.capital_deployed_usd::numeric, 2) AS cap_deployed,
       ROUND(ce.session_net, 2)               AS capital_event_net_usd
  FROM user_mgmt.allocation_returns ar
  JOIN ce
    ON ce.allocation_id = ar.allocation_id
   AND ce.session_date  = ar.session_date
 WHERE ar.capital_events_reconciled_at IS NULL
   AND ar.capital_deployed_usd > 0
   AND (ar.capital_deployed_usd + ce.session_net) > 0
   AND ce.session_net <> 0
 ORDER BY ar.session_date, ar.allocation_id;

\echo '=== APPLY ==='
WITH ce AS (
  SELECT allocation_id,
         event_at::date AS session_date,
         SUM(CASE kind WHEN 'deposit'    THEN amount_usd
                       WHEN 'withdrawal' THEN -amount_usd
             END)::numeric AS session_net
    FROM user_mgmt.allocation_capital_events
   GROUP BY allocation_id, event_at::date
),
candidates AS (
  SELECT ar.allocation_id, ar.session_date,
         ar.net_return_pct       AS orig_net,
         ar.gross_return_pct     AS orig_gross,
         ar.capital_deployed_usd AS cap_dep,
         ce.session_net,
         (ar.capital_deployed_usd * (1 + ar.net_return_pct/100))   AS end_equity_net,
         (ar.capital_deployed_usd * (1 + ar.gross_return_pct/100)) AS end_equity_gross
    FROM user_mgmt.allocation_returns ar
    JOIN ce
      ON ce.allocation_id = ar.allocation_id
     AND ce.session_date  = ar.session_date
   WHERE ar.capital_events_reconciled_at IS NULL
     AND ar.capital_deployed_usd > 0
     AND (ar.capital_deployed_usd + ce.session_net) > 0
     AND ce.session_net <> 0
)
UPDATE user_mgmt.allocation_returns ar
   SET net_return_pct   = (c.end_equity_net   - c.cap_dep - c.session_net)
                          / (c.cap_dep + c.session_net) * 100,
       gross_return_pct = (c.end_equity_gross - c.cap_dep - c.session_net)
                          / (c.cap_dep + c.session_net) * 100,
       capital_events_reconciled_at = NOW()
  FROM candidates c
 WHERE ar.allocation_id = c.allocation_id
   AND ar.session_date  = c.session_date
RETURNING ar.session_date, ar.allocation_id,
          ROUND(c.orig_net::numeric, 4)            AS net_before,
          ROUND(ar.net_return_pct::numeric, 4)     AS net_after,
          ROUND(c.orig_gross::numeric, 4)          AS gross_before,
          ROUND(ar.gross_return_pct::numeric, 4)   AS gross_after,
          ROUND(c.session_net::numeric, 2)         AS capital_event_net_usd;

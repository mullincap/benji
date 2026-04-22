# Open Work List

## Track 3 — Group B implementation (decisions locked, see session_handoff_2026-04-18.md)

- ✅ Item 5: Audit convention sweep (scope: `IDENTITY_FIELDS`) — SHIPPED in `9b14233` (docs-only, zero convention mismatches)
- ✅ Item 4: `audit.py` refactor to `run_audit(params) → dict` — SHIPPED in `1199708`
- ✅ Item 10: Per-allocation capital sizing — SHIPPED in `5a7bdc7`
- ✅ Item 6: VOL boost publication — SHIPPED in `f26d460`
- Item 9: Binance margin executor — **INVESTIGATION COMPLETE (Session C); implementation + architectural ratification OUTSTANDING**

  **Investigation findings (see [session_handoff_2026-04-20.md](session_handoff_2026-04-20.md) for full report):**
  - `backend/app/services/exchanges/binance.py` is **READ-ONLY by design** — no trade endpoints. `BinanceClient` has only: `get_permissions`, `get_spot_account`, `get_futures_account`, `get_margin_account`. Compare to `BlofinREST` at `trader_blofin.py:606-727` which has the full `set_leverage` + `place_order` + `close_position` surface.
  - Two exchange gates currently block the $20 Binance allocation: `spawn_traders.py:53` SQL filter + `trader_blofin.py:3323` hardcoded `creds.exchange != "blofin"` check. Lifting both without other changes makes a BloFin-client-with-Binance-creds call to `openapi.blofin.com` — unsafe.
  - Scope: **12-20h implementation** across Binance trade-endpoint extension (~3-5h), adapter pattern in trader (~2-4h), symbol + metadata translation (~2-3h), margin mode semantics (~1-2h), stop-loss synthesis + rollback (~2h), gate lifts (~30m), testing (~2-4h).

  **Six architectural decisions to ratify in Session D (ratification-only session; no code):**
  1. Adapter pattern: `ExchangeAdapter` ABC with concretes, OR sibling `trader_binance.py`, OR inline branching
  2. Binance trade-endpoint scope + signing (stdlib HMAC-SHA256 stays, or adopt `python-binance` SDK?)
  3. Stop-loss synthesis semantics (rollback when entry fills but SL placement fails; ordering)
  4. Margin mode lifecycle (per-spawn warming vs per-session setup)
  5. Symbol / metadata translation ownership (config vs adapter vs per-call)
  6. Testnet vs real-money minimum-trade verification strategy

  **Session D = scope + decisions. Session E or later = implementation.** Pattern mirrors Item 10's A/B split.

  **4-path BloFin integration gate — HARD PREREQUISITE, independent of Item 9 decisions:** before any Binance live-money deploy, exercise these four paths against a real BloFin allocation with today's signals (conviction passing):
    - `connection_id` threading through `run_session_for_allocation` → `_run_fresh_session_for_allocation` → `_account_advisory_lock` (Item 10)
    - `TRADER_LOCK_TEST_SLEEP_S` env-var scaffolding activation inside the live CLI flow (Item 10)
    - Phase 5's integration with the surrounding CLI phases: signal load, conviction check, credential load, monitoring loop handoff (Item 10)
    - Phase 4's `vol_boost` read + `l_high × vol_boost = eff_lev` log format: the caller-side read of `strategy_version.current_metrics["vol_boost"]` threading through `_run_fresh_session_for_allocation`'s new `vol_boost: float` param (Item 6)

  This gate can be exercised on any day BloFin conviction passes — does not need to co-occur with Item 9 implementation. Small-session work (~1-2h) using a scratch BloFin allocation under j@mullincap.com's BloFin connection. Document results in that session's handoff.

## Operationally gated

- Retire `blofin_logger.py` cron after multi-tenant executor stable ≥ 7 days (starts counting from `spawn_traders` first cron tick)
- Resolve plaintext BloFin row under `admin@mullincap.com` (Option A delete, after Binance executor confirms live)

### Master-cron removal — **COMPLETED 2026-04-20** (early, collision-driven)

> **COMPLETED 2026-04-20** — master cron retired early due to same-account
> collision risk with user BloFin allocation (`f87fe130-a90c-4e60-908a-14f4065b415c`).
> Both used the same API key (`116a…3734`, len 32) against the same BloFin
> sub-account; at tomorrow's 06:05 UTC spawn, running both would cause
> order/margin collisions in a single wallet.
>
> **Retirement actions taken 2026-04-20 ~18:17 UTC**:
> - Crontab lines for `trader-blofin.py` (06:00 UTC) and `blofin_logger.py`
>   (*/5) commented with `# DISABLED 2026-04-20` prefix
> - Host files archived to `/root/benji-archive-20260420/`:
>   - `trader-blofin.py`
>   - `blofin_executor.log`
>   - `blofin_executor_state.json`
>   - `blofin_returns_log.csv`
>   - `blofin_execution_reports/` directory
> - Originals at `/root/benji/trader-blofin.py` etc. left in place as
>   second rollback layer
> - Crontab backup at `/root/crontab_backup_20260420_181714.bak`
>
> **7-day stability gate is N/A** for master retirement — retirement was
> collision-driven, not gate-driven. The separate 7-day gate for
> `trader_blofin_fallback.py` deletion (code rollback safety net, commit
> `592206a`) is UNCHANGED and remains in effect until 2026-04-28.
>
> **Rollback procedure if needed**: `ssh mcap 'crontab /root/crontab_backup_20260420_181714.bak'` and master resumes its 06:00 UTC cron the following day.
>
> The historical Phase-2 gate plan below is preserved for reference but no
> longer active.

### Master-cron removal — PHASE 2 operational task (superseded by completion above)

Retire the host cron `0 6 * * *` at `/root/benji/trader-blofin.py` (master BloFin account, ~$3,951) only after the multi-tenant path has proven out. **Earliest possible retirement date: 2026-04-28** (assuming first activation 2026-04-21 fires clean and every day after).

**Gate: 7 consecutive days of multi-tenant operation with ALL of the following**

- Both allocations spawn at 06:05 UTC (no missing subprocesses vs. eligible-allocations query)
- Zero crashes in `_run_fresh_session_for_allocation` across either exchange
- `runtime_state.phase` transitions complete cleanly (`active → monitoring → closed/errored`) on every session — no stuck-active overnight, no missing phase field
- Entry + exit reconciliation populates `fill_entry_price` / `fill_exit_price` on the BloFin allocation (can be blank on Binance due to `get_margin_trades` indexing lag — known follow-up)
- No advisory-lock timeout or collision events in `allocation_*.log` files (`_account_advisory_lock` sits on the connection_id keyspace; contention would show as wait-time or timeout)
- `port_sl` / `port_tsl` trigger correctly if any session hits a threshold (harder to verify: absence of error, not presence of event — document intent to verify on any day either threshold actually fires)

**Counter reset condition**: any crash, missed spawn, unrecovered error state, or unexplained phase stall resets the 7-day counter to zero. Counter restarts only after the underlying cause is identified + fixed.

**Decision fork at gate-pass**

- **A) Consolidate** — transfer $3,951 master BloFin → j@mullincap.com's BloFin connection → allocate fully via UI → retire `/root/benji/trader-blofin.py` cron + `blofin_logger.py` cron. All capital flows under the multi-tenant model. Clean mental model going forward.
- **B) Keep master running** — preserve master's `execution_report` telemetry stream OR preserve the account-separation (master vs. user) for any compliance/operational reason. Document the rationale in that session's handoff.

**Pre-retirement checklist (when executing fork A)**

1. Decide: transfer master $3,951 to user's BloFin connection, OR leave the account dormant (no strategy running, balance preserved).
2. Back up `/root/benji/*.state` files + last 30 days of `/mnt/quant-data/logs/trader/cron.log` for audit continuity.
3. Confirm allocation-path telemetry (`portfolio_sessions` + `allocation_returns` + `runtime_state`) covers what master's `execution_report` does — spot-check one master run vs one allocation run side-by-side.
4. Remove two cron lines via `crontab -e`:
    - `0 6 * * * ... /root/benji/trader-blofin.py ...` (master trader)
    - `*/5 * * * * ... /root/benji/pipeline/allocator/blofin_logger.py ...` (legacy logger — retirement-gated on the same 7-day criterion)
5. Verify host has no leftover references in other scripts (grep `trader-blofin.py` across `/root/benji/`) before finalizing.
6. **Verify `_LIVE_BASELINE` still matches master's constants.** `backend/app/services/trading/trader_config.py:195-234` inlines master's module constants (lines 55-159 of `/root/benji/trader-blofin.py`) as a value-copy reference for the `master_defaults()` self-check. If master's constants have changed between Session D (2026-04-20) and retirement day, `_LIVE_BASELINE` drifts silently and the self-check passes against stale values. Cheap insurance: diff `_LIVE_BASELINE` against the current host file right before cron removal; reconcile any drift into `trader_config.py` as a separate commit, or update the source-of-truth reference to `TraderConfig.master_defaults()` directly.
7. **Decide fate of `/root/benji/live_deploys_signal.csv` write path.** `daily_signal.py` writes this file into `/root/benji/` (host), and the containerized trader reads it via a mounted path. If Fork A and you delete `/root/benji/trader-blofin.py` + master artifacts, the signal file must continue existing at this path or the container loses its signal source. Two paths: (a) keep `/root/benji/` as a signal-only directory (delete master trader + artifacts but retain the dir + `daily_signal.py` + `live_deploys_signal.csv`), or (b) relocate `daily_signal.py` output to `/mnt/quant-data/signals/` (or similar) and update the container mount + reader. Option (a) is minimal-change; (b) is cleaner long-term. Decide at retirement day; document the choice in that session's handoff.

## Small polish (any time)

- Frontend "Last refreshed N ago" label on Allocator cards (Track 1 exposes `metrics_updated_at` in API)
- $25K allocation slider UI clamp (display-only, server-side enforcement will land in Track 3 item 10)
- Avg lev column on Allocator strategy cards (UI follow-up — surfacing effective leverage per strategy at-a-glance)

## Environment / infrastructure

- BASE_DATA_DIR drift between backend and celery services (see `docs/deferred_work.md`)

## Multi-tenant assumption audit — 2026-04-20

Swept `backend/app/api/routes/` + `backend/app/cli/` + `backend/app/services/`
for patterns that silently assume a single allocation:

- `fetchone()` / `.first()` / `LIMIT 1`: 50+ hits, all classified (a) —
  legitimately single-row. Auth lookups, specific-ID PK fetches, scalar
  counts, LATERAL latest-snapshot patterns. Zero bugs.
- `AVG()` in SQL: 4 hits in `indexer.py` (strategy-level backtest aggregations
  across symbols — correct semantic) + 1 comment reference in `manager.py`.
  Zero new bugs.
- `WHERE status = 'active'` queries: all aggregate-aware (no `LIMIT 1` applied
  to multi-row result sets). Correct.

**Previously-known multi-allocation latent bugs** (all fixed today):
- manager intraday AVG-not-SUM → commit `578072c`
- portfolios fetchone() collapse → commit `d6acfae`

Audit conclusion: no further code changes needed for multi-allocation
correctness in the API layer. Future additions should default to
allocation-id projection in SELECT + optional filter param, following the
pattern established in execution-summary (`fd9fad3`) and portfolios (`d6acfae`).

## Session E+ follow-up: Capital change tracking

Trigger event: 2026-04-20 ~17:05-17:20 UTC manual capital transfer between
BloFin ($3,950 → $2,970) and Binance ($20 → $999) that bypassed the
allocator UI. Stale pre-transfer snapshots in `exchange_snapshots` caused
Today/WTD/MTD/MaxDD queries to compute nonsense returns (+5083% on Binance,
-24.80% on BloFin). Resolved via DELETE of pre-17:21:00 UTC rows
(2549 rows) for the two connections. Backup SQL preserved at
`/root/pre_reset_backup_20260420_180127.sql` on mcap (1.1 MB).

Need: `capital_events` table with `(allocation_id, event_ts,
capital_before, capital_after, source='ui'|'external')`. All
Today/WTD/MTD/MaxDD queries use `max(event_ts, period_start)` as the
baseline reference to prevent stale-baseline bug recurrence. Either:
  (a) UI writes `capital_events` whenever `allocations.capital_usd` changes
  (b) Snapshot cron detects large `total_equity` jumps and auto-writes an
      inferred capital_event
  (c) UI exposes an explicit "I made a capital change — reset baseline"
      button that writes a capital_event without requiring `capital_usd`
      changes

Scope estimate: ~80-120 LOC (schema + writer + 3-4 query updates in
`manager.py`). Gated on: not urgent; user can manually reset using the
2026-04-20 SQL pattern (DELETE `exchange_snapshots` rows with
`snapshot_at < <transfer completion time>` for affected connections)
in the interim.

## Session F+ follow-up: exit-fill reconcile reliability + size-mismatch visibility

Triggered while diagnosing the 2026-04-21 allocation_returns null-telemetry
bug (fixed by reordering `_log_allocation_return()` to run AFTER
`reconcile_exit_prices()`). Two orthogonal issues remain:

**1. Exit-fill reconcile race** — `reconcile_exit_prices()` emits `"exit fill
price not found on blofin -- leaving null"` for a fraction of symbols each
session (3 of 7 on 2026-04-21: BAS, TRADOOR, AAVE). The new WARN in
`_log_allocation_return()` will surface the frequency; if it fires daily,
the root cause is worth chasing. Likely: a narrow window between the close
order returning and BloFin's fill-history API indexing the fill. Potential
fixes: retry with backoff, or fallback to `close-position`'s response body
(which carries fill price in its ack). Partial data is structurally honest
(null-safe _mean_slip ignores nulls) so this isn't blocking.

**2. Size mismatch between state and BloFin** — session logs show
consistent state/exchange disagreement at close (`state=760 BloFin=842.0`
etc., 2026-04-21 log lines 413-419). Trader already recovers by closing the
BloFin size rather than the state size, so accounting stays correct. But
the drift implies state-write misses somewhere mid-session — could mean
partial fills that never reconcile, or orders placed outside the trader's
visibility. Not urgent; flag fires as WARN already; revisit if it grows
beyond cosmetic.

## Future (larger scope, not scheduled)

- Generic strategy executor dispatch (today's `trader-blofin.py` hardcodes Overlap logic)
- Manager module product work
- Publish more strategy variants (operational via Simulator UI, not code)
- Portfolios master NDJSON overlay — parallel to Execution tab's "Include master history" toggle. Reads `/root/benji/blofin_execution_reports/portfolios/*.ndjson`, aggregates per-session summaries from bar rows, merges into `/api/manager/portfolios` response. Deferred because `portfolio_sessions` table has 0 master rows today (host cron only writes DB on conviction-pass days, which are rare); 3+ years of master portfolio history lives exclusively in NDJSON files not surfaced by this endpoint. Scope: ~80-100 LOC backend (new file-reader + aggregator), frontend toggle component reuse from Execution tab. Session F+.

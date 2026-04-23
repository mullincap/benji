# Open Work List

## 🔴 Follow-up from Stream D-medium (2026-04-23)

### D-perf — precomputed abs_dollar leaderboards

`pipeline/overlap_analysis.py --ranking-metric abs_dollar` (shipped `fcb88be`
on 2026-04-23) reads raw values from `market.futures_1m` and ranks on-the-fly
via a CTE + `ROW_NUMBER()` partition. Measured wall-clock per metric: **~12
minutes** (vs sub-second for `--ranking-metric pct_change` via the
pre-ranked `market.leaderboards` fast path). Full audit under abs_dollar:
**~27 minutes** vs `~3.5` min canonical → **~8× slower end-to-end, ~700× on
the ranking step itself**.

That's a UX problem for the governance framework (§ 5 of
`docs/strategy_specification.md`): the Simulator's Compare-to-Canonical flow
asks users to explore candidate configs, but the 20+ minute per-abs_dollar-run
cost discourages exploration on the single axis where discovery is most
likely (abs_dollar vs pct_change). Needs a real optimization.

**Scope:**
- Extend `pipeline/indexer/build_intraday_leaderboard.py` to emit `abs_dollar`
  rankings alongside `pct_change`. Two options for persistence:
  (a) Add a `ranking_metric` column to `market.leaderboards` (currently has
      `metric` ∈ {price, open_interest, volume} but assumes one ranking formula).
      PK becomes `(timestamp_utc, metric, ranking_metric, variant, anchor_hour, rank)`.
  (b) Parallel table `market.leaderboards_abs_dollar` with identical schema.
  Option (a) is cleaner but requires a schema migration and reading-code
  updates; option (b) is additive and avoids touching any canonical read path.
  **Recommend (b)**: lower risk, same query cost, easy to deprecate later if
  abs_dollar turns out to not be a long-lived axis.
- Update `_load_canonical_futures_1m_frequencies` in `overlap_analysis.py`
  (or add a parallel `_load_abs_dollar_frequencies_from_leaderboards`) to
  read from the precomputed source when available. Fall back to on-the-fly
  only if the table is missing / stale.
- Backfill: re-run the indexer for the full 432-day window to populate
  historical abs_dollar rankings. Daily nightly cron maintains it going
  forward.
- Scope estimate: ~80 LOC in the builder, ~40 LOC in the consumer, a schema
  migration, and a ~3-hour backfill. Total ~2-3 hours of code + multi-hour
  compute time for backfill.

**Outcome:** abs_dollar audits at parity with pct_change wall-clock (~3-5 min
end-to-end), which is the precondition for `--ranking-metric` being a
genuinely useful exploration axis in the Simulator UI.

### D-bug — sub-second timestamp handling in `_load_canonical_futures_1m_frequencies`

Discovered during Stream D-medium Test 3 (2026-04-23): the new snapshot
branch uses `n.timestamp_utc = ANY(%s)` where `%s` is a list of exact
`HH:00:00` timestamps. `market.futures_1m` stores sub-second timestamps
(00:00:07, 00:00:09, etc.), so only ~2 rows per day land exactly on
`HH:00:00`. Result: 276 of 432 dates in the walk-forward window have valid
data; 158 dates silently dropped based on ingestion-timing artifacts.

Fix: rewrite the snapshot branch to use a range query + minute aggregation,
mirroring `_load_live_parity_frequencies_from_db`'s pattern
(`WHERE timestamp_utc >= minute_start AND timestamp_utc < minute_end`). Then
in Python, take the last-seen value per `(symbol_id, minute)` as the bar
close.

Test 3's measured Sharpe (−2.089) is on the biased 276-day subset. The
direction of the finding (abs_dollar on canonical universe disqualifies per
§ 5) is robust — −2.089 is catastrophically below the ≥canonical+0.3
threshold, and a 158-day correction is unlikely to flip the sign. But the
exact number shouldn't be reported as governance-grade until the fix ships.

**Scope:** ~20 LOC change in `_load_canonical_futures_1m_frequencies`
snapshot branch + rerun Test 3 to produce a clean 432-day Sharpe. ~30 min.

Bundled with D-perf above since the optimized path (read from precomputed
leaderboards) side-steps this bug entirely — if D-perf ships first, the
sub-second timestamp handling becomes moot.

## 🔴 TOP PRIORITY — Next session (starting 2026-04-22)

### Simulator parity with live symbol selection

**Goal:** Make `pipeline/overlap_analysis.py` (audit/simulator) produce the *same* symbol basket per day as `daily_signal.py` (live), with the **only** permitted difference being the data source (live Binance/BloFin API vs. pre-built parquet / `market.leaderboards` DB). Once the simulator can match live basket-for-basket, audit RET NET% becomes a credible proxy for what live-money actually earns — closing the 46pp-over-14-days measurement gap we found 2026-04-21.

**Motivation:** Live money is running on daily_signal.py. The audit/simulator is how we evaluate strategy edits, parameter sweeps, and new filters before promoting them. Right now the two pipelines share w20c20 constants and a comment claiming they match ("matches backtest exactly", `daily_signal.py:245`) but diverge on six specific axes — no parity test exists, no fixture compares them, and the assumption was never verified. Until parity is restored, every Simulator-driven decision about live strategy is made against a basket that doesn't reflect what live actually trades.

**Six divergences to reconcile** (source: 2026-04-21 investigation, full detail in session memory `project_open_work_strat_roi_vs_audit.md` and the diff we ran on job `63bbe197-9a46-42a9-8fa4-788315065bb2`):

| # | Axis | Live (`daily_signal.py`) | Audit (`overlap_analysis.py`) | Fix direction |
|---|---|---|---|---|
| 1 | Universe size | Top **100** by 24h quoteVolume ([daily_signal.py:49,205](../daily_signal.py#L49)) | Top **333** pre-ranked ([overlap_analysis.py:176](../pipeline/overlap_analysis.py#L176)) | Add `--leaderboard-universe N` CLI flag to `overlap_analysis.py`; default to 100 to match live. Existing `--leaderboard-index` is close but check semantics match exactly. |
| 2 | Symbol normalization | `bare = sym[:-4]` — strips "USDT" only ([daily_signal.py:197](../daily_signal.py#L197)) | `normalize_symbol()` — multi-quote + 1000× prefix strip + stablecoin/non-crypto reject ([overlap_analysis.py:216-244](../pipeline/overlap_analysis.py#L216-L244)) | Extract `normalize_symbol()` into `pipeline/symbol_utils.py`, expose a `--normalization minimal\|full` flag on audit. Default audit to `full` but allow `minimal` to match live. Parallel: fix `daily_signal.py` to import the full normalizer (separate task — live-money change, handle carefully). |
| 3 | Market cap filter | None | Optional `--min-mcap` (audit often runs with $50M) | Already configurable — just ensure audit runs with `--min-mcap 0` when targeting live-parity mode. Document in README. |
| 4 | OI sparse fallback | If OI coverage < 50%, fall back to `quoteVolume` ([daily_signal.py:352-366](../daily_signal.py#L352-L366)) | No fallback — queries OI leaderboard directly | Add a volume-fallback branch in `overlap_analysis.py`'s DB mode. Only triggers in historical periods where OI data was sparse. |
| 5 | Entry filters | Tail Guardrail / Tail + Dispersion applied inside `daily_signal.py` itself (sit_flat written to CSV) | Raw baskets output; audit.py applies filters during backtest | Keep the architectural split (audit produces baskets, audit.py applies filters) — but ensure the filter configs available in audit.py include *exactly* the ones daily_signal.py can run (currently Tail Guardrail and Tail + Dispersion). Verify both filters match bit-for-bit including the filter-switch-midway case we saw 2026-04-15 → 04-16. |
| 6 | Data-source timing | Binance REST pulled at 05:58 UTC daily | Parquet built hours/days ago; `market.leaderboards` DB populated nightly 01:00 UTC | **Permitted difference** — this is the exception the user explicitly allowed. But document that audit runs should prefer `market.leaderboards` DB mode (freshest) over parquet unless reproducing a specific historical run. |

**Approach (ordered):**

1. **Diagnose on one day first.** Pick 2026-04-16 (live traded, audit didn't). Pull both baskets for that exact date, walk the delta symbol-by-symbol to confirm which of the six divergences is actually driving the mismatch. ~1 hour, no code changes.
2. **Extract `normalize_symbol()` + related constants** into `pipeline/symbol_utils.py`. Pure refactor, no behavior change. Both scripts import from the shared module.
3. **Add `--live-parity` preset to `overlap_analysis.py`** that sets: `--leaderboard-universe 100`, `--min-mcap 0`, `--normalization minimal`, `--volume-fallback on`. One flag, not six, so the Simulator UI can flip it cleanly.
4. **Expose the preset in the Simulator UI.** Toggle: "Use live symbol selection" → sends `live_parity: true` through PromoteRequest / audit params. When on, any other conflicting flag is overridden.
5. **Parity harness.** Nightly script that runs both basket generators for yesterday's date, diffs the symbol lists, logs any divergence. Should be silent on parity-mode days and loud on any drift. ~60 LOC.
6. **Re-run the 2026-04-07 → 2026-04-21 diff** with live-parity mode on. If the 46pp spread closes to < 5pp, we've succeeded. If a material gap remains, a seventh divergence exists and we repeat step 1.

**Scope estimate:** ~200 LOC backend (overlap_analysis CLI flags + symbol_utils extraction + volume fallback + parity harness) + ~30 LOC frontend (Simulator toggle). One session, maybe two if step 6 surfaces a seventh divergence.

**Non-goals (explicit):**
- Do NOT change `daily_signal.py` in this work. Live money runs against it daily at 05:58 UTC; any change is a separate, carefully-sequenced task with its own validation. The audit conforms *to* live, not the other way around.
- Do NOT change the fee/leverage math in the audit. The 2026-04-21 investigation confirmed fee/SL/TSL/profit-take constants match; only symbol selection diverges.

**Depends on:** none. Can start immediately 2026-04-22.

**Blocks:** every future Simulator-driven strategy decision.

**Sub-item A — Per-exchange symbol-availability gate in Simulator.** Surfaced by the 2026-04-22 06:05 UTC session launch: Alpha Main on Binance dropped 5 of 7 signal symbols (`BAS, CLO, EDGE, M, RIVER`) as unsupported, traded on only `MET` and `PENGU` — a 29% basket. The audit/simulator currently assumes every signal symbol is tradeable on every exchange, so simulator returns for Binance-backed allocations **systematically overstate** what those allocations can actually earn (they include PnL from symbols Binance can't trade). Fix: before computing returns in audit.py, drop symbols not in the target exchange's instrument list (cache the list per-exchange in `market.exchange_instruments` or similar, refresh nightly). Backtests for BloFin allocations should keep all symbols (BloFin has broader coverage); Binance backtests should filter. Scope: ~40 LOC in audit.py + ~20 LOC nightly refresh job. Prereq: decide whether "drop symbol" means (a) zero-weight it and re-normalize the remaining basket, or (b) treat the allocation as sit-flat that day if coverage < N%. The 2-symbol-out-of-7 case tonight is an existence proof that (a) can produce nonsense returns on some days.

**Sub-item B — Backfill `active_filter` in strategy_version configs.** Same 2026-04-22 session surfaced config drift: Alpha Main's strategy_version.config is missing `active_filter` (WARN: `TraderConfig.from_strategy_version: field 'active_filter' not found (tried aliases ['active_filter', 'filter_mode'])`); Alpha Max's has it correctly. Both allocations ended up with filter='Tail Guardrail' tonight — Alpha Main via master-default fallback, Alpha Max via config — but this means the two strategy_versions carry different config schemas, and any filter change to one won't automatically propagate to the other. Fix: one-shot SQL to populate `active_filter` on every `audit.strategy_versions.config` JSONB where it's currently absent. Values should match the `filter_mode` column on the parent `audit.strategies` row. Confirm via `SELECT sv.strategy_version_id, sv.config ? 'active_filter', s.filter_mode FROM audit.strategy_versions sv JOIN audit.strategies s ON s.strategy_id = sv.strategy_id` before/after. Scope: ~10-line migration. Low risk — the fallback path works, so this is cleanup, not a fire.

---

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

**Supersession note** (2026-04-22): a tactical fix in
`/api/allocator/trader/{id}/pnl` now baselines Session P&L and Total
P&L against immutable sources instead of `allocation.capital_usd`
(Session: `runtime_state.session_start_equity_usdt` during active
sessions only; Total: earliest `exchange_snapshot` at/after
`allocation.created_at`). Stops the most common mid-session allocation
edit from shifting displayed P&L. The full capital_events table
subsumes this — once it ships, both formulas should migrate to use
`max(capital_event_ts, session_start)` as the baseline reference.

## Session F+ follow-up: exchange account-history backfill + net-imports PnL

Surfaced 2026-04-22 while fixing the Max DD scope bug. Two related gaps:

**1. `user_mgmt.performance_daily` has almost no rows.** Across three
allocations (Alpha Max active, Alpha Main paused, Alpha Low closed), the
table currently holds **one row** (Alpha Max 2026-04-21). The Manager
Overview's Portfolio Equity 30D curve, per-allocation drawdown rollup,
and WTD/MTD aggregates all read from this table, so they're almost
entirely running on the `exchange_snapshots`-based fallback path in
`manager.py:_fetch_portfolio_context` (lines 146-184) rather than their
primary source. That fallback works but loses precision on days where
snapshots had gaps. Whatever upstream job is meant to populate
`performance_daily` per (allocation, day) is either not running,
scoped to a subset of allocations, or silently failing.

**2. No authoritative record of capital in/out.** Session E+ capital_events
(above) tracks when the *allocator UI* changes `capital_usd`, but exchange
wallets also receive deposits, withdrawals, sub-account transfers, and
cross-exchange moves that bypass our system entirely. Today's P&L math
treats any equity change as strategy return — a $500 deposit and a
$500 win are indistinguishable in the displayed numbers. On 2026-04-20
a manual $980 transfer between BloFin and Binance caused +5083% nonsense
returns until the pre-transfer snapshots were manually deleted.

**What to build:**

- **Account-history pullers** per exchange adapter:
  - BloFin: `/api/v1/asset/deposit-withdrawal-history` (already partly
    exposed in `BlofinREST`; needs wiring)
  - Binance: `/sapi/v1/capital/deposit/hisrec` + `/sapi/v1/capital/withdraw/history`
    + `/sapi/v1/margin/transfer` (cross-margin in/out)
- **New table** `user_mgmt.capital_imports`
  `(connection_id, event_ts, amount_usd, direction in ('deposit', 'withdrawal', 'transfer_in', 'transfer_out'), exchange_ref_id, source)`
  Pulled nightly via a new cron entry, deduplicated by `exchange_ref_id`.
- **Backfill job** — one-time script that walks each connection's account
  history as far back as the exchange API allows (BloFin: 90 days;
  Binance: 365 days on some endpoints) and populates `capital_imports`
  + rebuilds `performance_daily` rows using
  `return_usd[d] = equity[d] - equity[d-1] - sum(capital_imports where event_ts in d)`.
- **Net-imports PnL formula** — update `manager.py` and
  `allocator.py:/trader/{id}/pnl` so every P&L number (Session,
  Today, WTD, MTD, Total, Max DD USD) is computed as:
  ```
  real_pnl = end_equity - start_equity - net_imports_over_window
  ```
  Same baseline-reset behavior as capital_events but driven by
  exchange-authoritative history rather than UI state.

**Why this matters:**
- Backfilled `performance_daily` gives the Portfolio Equity 30D curve
  real data (currently it's effectively a 1-point chart for most users).
- Net-imports-aware PnL eliminates an entire class of silent
  misattribution — the allocator UI stops lying about returns whenever
  the user moves capital in/out of an exchange.
- Capital_events (Session E+) and capital_imports are complementary:
  capital_events is the UI-driven intent record; capital_imports is the
  exchange-authoritative fact record. When both ship, queries should
  use `max(capital_event_ts, capital_import_ts, period_start)` as the
  baseline anchor.

**Scope estimate:** ~250-400 LOC across adapter methods (~80), new
table + cron (~60), backfill CLI (~100), PnL formula updates in 3-4
routes (~80). Gated on Session E+ capital_events landing first OR
shipping as the full replacement (either order works; the formula
update in route handlers is shared).

**Not urgent today** — current single-user deployment works because the
user is not actively moving capital around. Becomes urgent before
multi-user or before we publish "verified track record" numbers to
anyone external.

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

### Session F+ — Simulator promote: strategy matching by string

**Current** (`backend/app/api/routes/simulator.py:218-222`):

```python
cur.execute(
    "SELECT strategy_id, name FROM audit.strategies WHERE display_name = %s",
    (body.strategy_name,),
)
```

**Bug:** if a strategy is renamed, re-promoting an old audit that still carries the old `display_name` doesn't match the renamed row — it creates a brand-new duplicate strategy row with the old name. The rename flow itself is fine (joins across allocator/trader/signals all use `strategy_id`); only the Simulator promote path is string-coupled.

**Proposed fix:** extend `PromoteRequest` with an optional `strategy_id` field. When present, promote into that strategy directly. When absent, fall back to the existing `display_name` match (preserves the new-strategy-from-fresh-audit path).

**Frontend:** Simulator Promote modal gains a `Pick existing strategy ▾` dropdown above the free-text name field. Dropdown queries `audit.strategies` live. Selecting a strategy sends `strategy_id`; typing a new name sends `null strategy_id + new name`.

**Scope:** ~30 LOC backend (PromoteRequest + SELECT branch) + ~50 LOC frontend (dropdown + modal update).

**Not urgent.** Edge case — admin-level re-promote flow only. Workaround if triggered: delete the duplicate `audit.strategies` row.

**Bundled sub-item — rename confirmation M-count:** the rename modal toast currently shows only `N active allocations now reference '<new name>'`. The original plan also called for `M daily_signals` but was dropped from the initial ship because extending the rename endpoint would have required a backend redeploy inside the 06:00 UTC trader window. When this Session F+ item ships (which requires a backend redeploy anyway), extend `POST /api/allocator/strategies/{id}/rename` to also return:

```sql
SELECT COUNT(*) FROM user_mgmt.daily_signals
 WHERE strategy_version_id IN (
   SELECT strategy_version_id FROM audit.strategy_versions
    WHERE strategy_id = %s
 )
```

and plumb the count into the toast. ~5 LOC backend + ~3 LOC frontend.

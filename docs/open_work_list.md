# Open Work List

_Last cleanup: 2026-04-24 (late). Anything that shipped is intentionally
pruned — git log + commit messages are the historical record._

## ✅ Recently shipped (2026-04-23 → 2026-04-24)

- **ALTS MAIN live promotion** (former Gaps 1-8) — strategy live on allocation `f87fe130`, per-strategy `tail_drop_pct` config-read in `daily_signal_v2.py`, Dispersion filter ported, allocation pointed at ALTS MAIN's `strategy_version_id`, capital-events reconciliation (compute-on-read + auto-poll exchange income APIs)
- **Manager Execution table fixes** — Gap 6 (stale 04-23 "filtered" row when session was actually recovered) → commit `0721e3b`. Gap 7 (per-symbol expand on each daily row, with NOTIONAL + LEV columns) → commits `bc6339b` + `28333f5` + `29d5284`. Gap 8 (reconcile vs Session E spec) → substantively addressed by Gap 6 + Gap 7 fixes; ALLOCATION column already restored.
- **Strategy detail page real data** — equity curve + calendar heatmap rendered from `audit.equity_curves`, populated nightly via the existing `refresh_strategy_metrics` cron + new audit.py CSV write hook → commits `72a3363`, `2623d00`, `aaf8be9`, `57e405b`, `5d4758e`
- **Simulator live-parity backend + CLI** — `--live-parity` knob in `overlap_analysis.py` + JobRequest `live_parity` field. Previously marked as shipping the UI toggle too; that was incorrect (those commits shipped `ranking_metric` dropdown, not the live_parity toggle) → commits `c8768fc`, `89d8e5c`, `fcb88be`, `392094f`
- **Capital Events UI** on `/trader/settings` + auto-poll exchange income APIs → commit `99ddf61`
- **Trader fill_rate / entry_slip / retries** — verified working in current code via dry-call (2026-04-24). The historical NULLs on the 2026-04-21 row are pre-writer-extension stale data, not a current bug. All 6 KPIs at the top of `/manager/execution` will populate naturally starting with the next session close.
- **Trader resilience hardening** (2026-04-24) — six-commit bundle deployed via manual rebuild after kill+respawn of PID 623 with ~3 min unmanaged window. Commits: `6c3b506` SIGTERM/SIGINT/atexit → lock release on graceful shutdown. `957524a` revert shrinking-size ladder (hypothesis disproven by operator $1,800 UI fill succeeding). `6911252` per-bar reconcile in monitoring loop (every 5 min) + raw BloFin reject payload logging in `BlofinAdapter._submit_order`. `a3917db` classify "risk control"/"low-liquidity pair" BloFin errors as non-retriable (avoids extending BloFin's 5-min cooldown via further retries). `21cfca2` entry order spacing (5s base + 0-3s jitter between symbols; 5-symbol basket now 20-32s total span vs prior 4.5s). `e0c6129` retry cadence 5s → 330s (5.5 min) + `MAX_ENTRY_RETRIES` 3 → 1.
- **Manager Overview KPI A+B** (2026-04-24) — commit `adc5e66`. Today P&L fallback gate changed from `not perf_by_alloc` to `no today row` (so snapshot-based fallback fills the gap when today's performance_daily row is absent). Max DD scope intersected with connections that have a snapshot in the last 48h (drops phantom-cliff contribution from retired Binance connection_id `f428458f`).
- **Portfolio_session auto-close on SIGTERM + supervisor sweep** (2026-04-24 late) — commit `7496f47`. SIGTERM/SIGINT/atexit handler now closes the held portfolio_sessions row as `exit_reason='subprocess_died'` before exit (next to the existing lock release). Belt-and-suspenders sweep in `trader_supervisor.py` covers past-date active rows (signal_date < today) and today rows where runtime_state is stale for >15m AND phase ≠ 'active' — catches the SIGKILL case the signal handler can't.
- **Manager Overview KPI C — Total AUM baseline unified** (2026-04-24 late) — commit `fd78a61`. Manager's Total P&L subvalue now derives principal the same way Allocator's `/trader/{id}/pnl` does: `SUM(connection.principal_baseline_usd + capital_events since principal_anchor_at)` across all connections with an active allocation, then `total_live_equity - sum_principal`. Verified live on 04-24: $4,874.96 − $2,673.32 = **$2,201.65** matches the Allocator card number. Bootstrap fallback preserved for fresh installs without capital-events history.
- **Simulator live_parity=True default + audit-log label + symbol registry refresh** (2026-04-24 late) — commit `11da28a`. Flipped `JobRequest.live_parity` default from False → True so Simulator audits match the universe the live trader sees. Prominent `LIVE-PARITY: ON/OFF` header added to every `overlap_analysis.py` run. New `backend/app/cli/refresh_symbol_registry.py` pulls BloFin `/api/v1/market/instruments` + Binance `/fapi/v1/exchangeInfo` and UPSERTs `market.symbols.binance_id`/`blofin_id` — 15 inserts + 144 updates on first prod run, GENIUS now populated (`GENIUS-USDT`/`GENIUSUSDT`).
- **Simulator live_parity UI toggle** (2026-04-24 late) — commit `59f52c5`. New UNIVERSE PARITY subsection at the top of EXECUTION CONFIG with an inline `live_parity` toggle (default On, matches backend default). Closes the "no opt-out path" gap noted after the default flip.
- **Manager Overview Max DD — daily-close + capital-events adjusted** (2026-04-24 late) — commit `1a913f0`. Previous 5-min bucketed calculation was picking up transient unrealized MTM mid-session (04-23 dipped to $3,775 intraday but closed at $4,022 — labelled a -15.9% drawdown for what was actually a +17% gain day) AND unnetted operator capital movements between exchanges. New formula: daily-close equity per connection, summed, with cumulative capital events netted out via correlated subquery. Verified live: -15.9% / -$715 → -6.23% / -$239.

---

## 🟠 Follow-ups (same-week)

### Cron-schedule the symbol registry refresh

`backend/app/cli/refresh_symbol_registry.py` runs fine on prod (exercised
manually on 2026-04-24), but it's not yet on cron. Schedule alongside
the other nightly pulls (metl 00:15, coingecko 00:30, indexer 01:00).
Suggested slot: **00:45 UTC daily** — after the previous day's
leaderboards land but before `indexer 01:00` so downstream references
see fresh ids. ~3 LOC crontab add.

---

## 🟡 Active — when ready

### Account-history backfill + net-imports PnL (Session F+)

Surfaced 2026-04-22. Two related gaps:

**1. `user_mgmt.performance_daily` is sparse.** Manager Overview's Portfolio
Equity 30D curve, per-allocation drawdown rollup, and WTD/MTD aggregates
all read from this table; today they fall through to the
`exchange_snapshots`-based fallback in `manager.py:_fetch_portfolio_context`.
That fallback works but loses precision on snapshot-gap days.

**2. No authoritative record of capital in/out from exchange wallet level.**
Today's P&L math treats any equity change as strategy return — a $500
deposit and a $500 win are indistinguishable. Capital_events (already
shipped) tracks UI-driven intent; we still need exchange-authoritative
deposits/withdrawals/transfers to back-stop it.

**What to build:**
- Account-history pullers per exchange adapter (BloFin: existing
  `/api/v1/asset/deposit-withdrawal-history`; Binance: `/sapi/v1/capital/
  deposit/hisrec` + `/withdraw/history` + `/sapi/v1/margin/transfer`)
- New `user_mgmt.capital_imports` table keyed on `exchange_ref_id`
- Backfill CLI + nightly cron
- Update P&L math in `manager.py` + `allocator.py:/trader/{id}/pnl` to
  net out `capital_imports` over the displayed window
- Use `max(capital_event_ts, capital_import_ts, period_start)` as the
  baseline anchor everywhere

**Scope:** ~250-400 LOC. ~5-6h.

**Not urgent today** — single-user deployment; the user is not actively
moving capital around. Becomes urgent before publishing "verified track
record" numbers externally.

---

## 🟢 Low priority — opportunistic

### D-perf — precomputed abs_dollar leaderboards

`overlap_analysis.py --ranking-metric abs_dollar` ranks on-the-fly via a
CTE + `ROW_NUMBER()` partition. ~12 minutes per metric vs sub-second for
`pct_change` via the pre-ranked `market.leaderboards` fast path. Full
audit under `abs_dollar` is ~27 minutes vs ~3.5 min canonical (~8× slower
end-to-end).

UX problem for the governance Compare-to-Canonical flow on the single
axis where exploration is most likely (abs_dollar vs pct_change).

**Recommend (b):** parallel `market.leaderboards_abs_dollar` table with
identical schema. Lower risk than schema migration; easy to deprecate
later. Update `_load_canonical_futures_1m_frequencies` (or add a sibling)
to read from the precomputed source.

**Scope:** ~80 LOC builder, ~40 LOC consumer, schema migration, ~3h
backfill compute. Total ~2-3h code + multi-hour compute.

### D-bug — sub-second timestamp handling in `_load_canonical_futures_1m_frequencies`

The snapshot branch uses `n.timestamp_utc = ANY(%s)` against exact
`HH:00:00` timestamps. `market.futures_1m` stores sub-second timestamps,
so only ~2 rows per day land exactly on the hour. Result: 276 of 432
dates land valid; 158 dates silently dropped.

Fix: rewrite snapshot branch to use range query + minute aggregation,
mirroring `_load_live_parity_frequencies_from_db`. Then take last-seen
value per `(symbol_id, minute)` as the bar close.

**Scope:** ~20 LOC + retest. ~30 min.

Bundled with D-perf — if D-perf ships first using precomputed leaderboards,
the sub-second timestamp issue becomes moot.

### Sub-item B — backfill `active_filter` in strategy_version configs

Alpha Main's `audit.strategy_versions.config` is missing `active_filter`
(WARN: `TraderConfig.from_strategy_version: field 'active_filter' not
found`). Falls through to master default — works but means rename of one
strategy doesn't propagate config. One-shot SQL.

**Scope:** ~10 LOC migration. Low risk.

---

## ⚪ Deferred (priority dropped)

### Track 3 Item 9 — Binance margin executor

**Deferred 2026-04-24:** Binance allocation emptied; coverage too narrow
for the published strategies (5 of 7 ALTS MAIN signal symbols dropped on
the 2026-04-22 Binance run). No active capital on Binance. Revisit only
if a future strategy specifically targets Binance-only symbols.

Investigation notes preserved in `session_handoff_2026-04-20.md`. Six
architectural decisions still unratified. Implementation scope was
12-20h.

### Master-cron removal — Phase 2 (operationally gated)

> Phase 1 master-cron removal **completed 2026-04-20** (early, collision-
> driven). Master `trader-blofin.py` archived. Container path is sole
> trader entry-point.

Phase 2 wraps up the legacy artifact set after the multi-tenant executor
runs ≥7 days stable. Mostly bookkeeping:

1. Decide Fork A (consolidate $3,951 into user's BloFin connection +
   retire `/root/benji/trader-blofin.py` + `blofin_logger.py`) vs Fork B
   (preserve master account separation for compliance/operations)
2. Verify allocation-path telemetry covers what master's
   `execution_report` did (spot-check one master vs one allocation run)
3. Remove the two cron lines via `crontab -e`
4. Decide fate of `/root/benji/live_deploys_signal.csv` write path
5. Verify `_LIVE_BASELINE` in `trader_config.py` matches current host
   constants before retirement

**Gate:** ≥7 days of stable multi-tenant operation. Counter starts on
first `spawn_traders` cron tick.

---

## 🔧 Polish — any time

- Avg lev column on Allocator strategy cards
- $25K allocation slider UI clamp (display-only; server enforcement landed in Track 3 item 10)
- "Last refreshed N ago" label on Allocator cards using `metrics_updated_at`
- Exit-fill reconcile reliability — `reconcile_exit_prices()` emits "exit fill price not found" for ~1 of 7 symbols per session. Null-safe today; revisit if frequency grows
- Size mismatch between state and BloFin at close (state=760 vs exchange=842) — trader recovers correctly, but the drift implies a state-write miss mid-session. Cosmetic until it grows

---

## ⏳ Future / unscheduled (larger scope)

- **Generic strategy executor dispatch** — today's `trader_blofin.py` hardcodes Overlap logic
- **Manager module product work** — broader feature buildout
- **Portfolios master NDJSON overlay** — parallel to Execution tab's "Include master history" toggle. Reads `/root/benji/blofin_execution_reports/portfolios/*.ndjson`, aggregates per-session summaries, merges into `/api/manager/portfolios`. ~80-100 LOC backend + frontend toggle reuse
- **Simulator promote: strategy matching by `strategy_id`** — current free-text `display_name` match creates duplicate `audit.strategies` rows when an old audit is re-promoted after a rename. Extend `PromoteRequest` with optional `strategy_id`. ~30 LOC backend + ~50 LOC frontend dropdown. Edge case — admin re-promote only.

---

## 🔌 Environment / infrastructure

- BASE_DATA_DIR drift between backend and celery services (see `docs/deferred_work.md`)

---

## Multi-tenant assumption audit — 2026-04-20 ✅ closed

Swept `backend/app/api/routes/`, `backend/app/cli/`, `backend/app/services/`
for patterns that silently assume a single allocation. Result: zero
remaining bugs. Pattern for new code: default to allocation-id projection
in SELECT + optional filter param, following `execution-summary`
(`fd9fad3`) and `portfolios` (`d6acfae`).

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
- **Manager Execution table — full audit pass** (2026-04-24 late) — commit chain `7947949` → `635b35b`. Trader writer now persists `ctval` on every position dict (fresh + retry + reconcile-add) so pnl_usd computes for every symbol; added migration 013 column. `notional_usd` recalculated as `contracts × price × ctval` (fixes INX/KAT/SKR/ZEREBRO 100×/10× under-reports). `fill_report` mirrored into runtime_state at session start + every per-bar write so fill_rate + retries_used survive a subprocess respawn. `est_entry_price` writer fallback to `entry_price`. Reconcile-add path rounds fill_entry to 6 decimals, sets entry_slip to NULL (not fake 0.0), and updates `portfolio_sessions.entered[]` via new `_append_portfolio_session_entered` helper so Portfolios + Execution counts stay consistent. Manager backend: stale-filtered branch derives gross/net + est from `ps.fpr × lev_int` (gap = 0 honest); `est_return_pct` uses `effective_leverage` (was `lev_int`, made est-vs-actual look like a 9-pt miss when it was 1-pt); `pnl_gap = actual − est` (positive=beat, negative=underperform); per-symbol `est_pnl_pct` + `pnl_gap_pct` returned; per-symbol leverage uses `effective_leverage` not `lev_int`; fill_rate fallback from `len(entered)/len(symbols)`. Frontend: Est PnL % + PnL Gap columns added to per-symbol expand; Total Slip KPI card + daily-row column + per-symbol column (entry+exit, sign-preserving); pnlGapColor flipped to signed thresholds (>+0.25% green, <-0.25% red, near-zero amber); fmtPriceBare defensive rounding for any raw float that ever slips through. One-shot backfills: today's row pnl_usd + ctval + est_entry_price + ZEREBRO fill_entry rounded; ZEREBRO appended to portfolio_sessions.entered.
- **Symbol registry nightly cron + tracked crontab file** (2026-04-24 late) — commit `ca4f11a`. `refresh_symbol_registry` scheduled at 00:45 UTC (between 00:15 metl pull and 01:00 indexer build) on prod. `ops/crontab.txt` is now a verbatim mirror of mcap's live crontab — pull fresh from prod after any cron edit so it stays in sync.

---

## 🟠 Follow-ups (same-week)

### Audit-vs-live structural divergences

Surfaced 2026-04-25 reproducing canonical ALTS MAIN through the simulator.
After PRs #6 (audit `_load_mcap_from_db` coin_id mapping) and #7 (live
trader 1000X-prefix fix), the audit + live trader's *dispersion universes*
agree on the 1000X-prefix subset (PEPE/SHIB/FLOKI/BONK no longer dropped).
Two structural mismatches still produce divergent baskets between
nightly audits (e.g. `41fec4df`, ALTS MAIN) and what the live trader
actually trades the same morning.

**1. Dispersion-universe selection — point-in-time vs today's snapshot**

| | Audit | Live trader |
|---|---|---|
| For day X dispersion calc, uses... | top-N mcap **as of day X** | top-N mcap **as of TODAY**, then their historical returns |
| Look-ahead bias | none — point-in-time | yes — today's mcap rankings applied to history |
| Code | `pipeline/audit.py:build_dispersion_filter` | `daily_signal_v2.py:compute_dispersion_filter` (line 535–548) |

The live trader self-documents this as an explicit approximation at
`daily_signal_v2.py:516` ("Approximation vs audit: ... Live uses
TODAY's top-N (i.e. fixed snapshot of mcap, not per-day)"). Magnitude
depends on universe churn — modest for established mcaps, larger after
big mcap rotations. **Fix scope**: rewrite live's dispersion calc to do
N daily DB queries, mapping each day's symbol set to its kline returns.
Manageable but ~100-200 LOC and adds latency to the 05:58 cron.

**2. Basket-selection — snapshot bar + data path**

Both audit and live use essentially the same universe (~all USDT perps
with snapshot data). The audit's
`overlap_analysis.py` query at lines 812-829 reads
`market.leaderboards` filtered by `metric IN ('close','open_interest')`
+ `rank<=20` — no volume threshold or top-100 cutoff (corrected
2026-04-25, prior version of this entry incorrectly attributed a
volume filter). The indexer ranks all symbols it has data for (~565 on
04-24); the audit takes top-20 by each metric and intersects.

Real differences that produce divergent baskets:

| | Audit | Live trader |
|---|---|---|
| Snapshot bar | 06:00 UTC bar (open at 06:00, close at 06:04:59) — always closed by audit-time | At 05:58 cron: in-progress 05:55-05:59 bar via `klines[-1]`. Effectively 5 min EARLIER than audit, AND in-progress (uses partial close at moment of fetch) |
| Anchor bar | 00:00 UTC bar from `market.futures_1m` (`anchor_hour=0`) | 00:00 UTC 5m bar via Binance FAPI klines |
| Data path | `market.futures_1m` populated by metl batch nightly | Binance FAPI direct, real-time |
| OI source | Same `market.futures_1m` row | `/futures/data/openInterestHist` separate endpoint |

Concrete impact (observed 2026-04-24): audit's basket = `ENJ, INX, KAT,
RED, SKR, SPORTFUN, ZEREBRO`; live's basket = `ENJ, INX, KAT, SKR,
ZEREBRO`. RED was top-3 OI in BOTH universes (same scope), but it
ranked outside live's price top-20 because the 5-min-earlier in-progress
bar showed different price action than the audit's closed 06:00 bar.
Same root cause as divergence #3 below (wall-clock non-determinism)
applied to live's snapshot bar choice.

Three fix paths (decision needed before coding):

- **A. Live waits for 06:00 bar to close** before fetching — moves the
  cron from 05:58 to 06:00:30, uses the closed 06:00 bar. Matches
  audit on snapshot timing. Adds 2-3 min latency before trader entry
  but eliminates this divergence + divergence #3
- **B. Audit moves to live's snapshot semantics** — uses the 05:55-05:59
  bar instead of the 06:00 bar. Smaller change, but locks the audit
  into the wall-clock-dependent semantics that bit us today
- **C. Both run from the same data source** with the same explicit
  snapshot timestamp — biggest refactor, but eliminates path/source
  mismatches too

**Side-channel work**: market.symbols.binance_id has NULL for PEPE/SHIB
and is missing FLOKI/BONK entirely (PR #7 worked around it with an
inline override map). Symbol-registry refresh has its own 1000X-prefix
detection bug worth a separate small fix so future 1000X-style listings
are handled automatically.

### Live trader basket — wall-clock non-determinism

Surfaced 2026-04-25. `daily_signal_v2.py:compute_canonical_basket`
computes its snapshot via:

```python
target_snap = ref_date 06:00 UTC
end_ms = (target_snap + 5min).timestamp()  # 06:05 UTC
# fetch klines startTime=00:00, endTime=06:05
snap_close = float(klines[-1][4])  # last bar's close
```

Binance's klines endpoint returns BOTH closed and in-progress bars.
`klines[-1]` therefore depends on wall-clock at fetch time:

- **Run at 05:58 UTC** (cron): `klines[-1]` = 05:55–05:59 bar **still open**,
  close = price at ~05:58
- **Run at 06:14 UTC** (manual rerun): `klines[-1]` = 06:00–06:04 bar
  closed at 06:04:59

Different snapshot bars → different price/OI pct_change → different
top-20 ranks → different baskets. Concrete impact today: 05:58 cron
basket = `[API3, AXS, BSB, MAGIC, SAFE, YGG]`; 06:14 manual rerun
basket = `[API3, AXS, BSB, SAFE, SAND, YGG]`. MAGIC ↔ SAND swap at
the rank-20 boundary because of 16 min of price drift in the snapshot
window. **Caused today's accidental double-spawn** — ALTS MAIN's 06:05
trader had already entered MAGIC before the manual signal regen wrote
SAND for ALTS DAILY.

This is a third divergence dimension distinct from #1 and #2 above —
they're audit-vs-live; this one is live-vs-itself. Note: Path A from
divergence #2 (live moves to leaderboards) would fix this as a side
effect since leaderboards are precomputed once per day. Otherwise this
needs its own fix.

**Two fix options (cleaner is #2):**

1. **Filter out the in-progress bar** in `compute_canonical_basket`:
   after fetching klines, discard any bar where
   `open_time + 5min > now()` → use the latest *closed* bar. ~10 LOC
   in one place.
2. **Wait until `06:00 UTC + 30s` before fetching**: ensures the
   05:55–05:59 bar (and the 06:00 bar if needed) is fully closed →
   all runs of that day produce the same basket. Requires moving the
   cron from 05:58 to 06:00 plus a small post-fetch close-bar guard.

Either makes basket selection deterministic-per-day independent of
when the script runs.

### Canonical-compare card false-negative on identical methodology

Surfaced 2026-04-25. `CanonicalCompareCard.paramsMatchCanonical()` has a
short-circuit that suppresses the side-by-side comparison + governance
verdict when a candidate matches canonical on all `METHODOLOGY_KEYS`
("Nothing to compare — this candidate IS canonical"). The check is
asymmetric on undefined-vs-defined: if EITHER side is undefined and
the other is set, returns false even when the defined value equals
the system default.

The published `ALTS MAIN v1` (`5cb04dc8`) was promoted before 4 of the
16 methodology keys existed, so its stored config is missing
`price_ranking_metric`, `oi_ranking_metric`, `apply_blofin_filter`,
`ranking_metric`. Today's candidates (frontend `DEFAULT_PARAMS` merge
from PR #5) always carry those fields with default values. Result:
short-circuit fails → full comparison renders → shows "DOES NOT
QUALIFY: Sharpe below canonical + 0.3" against literally the same audit.

**Fix scope** (~10 LOC, frontend-only):

```ts
const METHODOLOGY_KEY_DEFAULTS: Record<string, unknown> = {
  price_ranking_metric: 'pct_change',
  oi_ranking_metric: 'pct_change',
  apply_blofin_filter: false,
  ranking_metric: null,
};

function paramsMatchCanonical(candidate, canonical) {
  for (const k of METHODOLOGY_KEYS) {
    const a = candidate[k] ?? METHODOLOGY_KEY_DEFAULTS[k];
    const b = canonical[k] ?? METHODOLOGY_KEY_DEFAULTS[k];
    if (a === undefined && b === undefined) continue;
    if (String(a) !== String(b)) return false;
  }
  return true;
}
```

Backend alternative (more invasive but cleaner long-term): backfill
the canonical strategy version's config to include current defaults
at promotion time. Frontend fix is the safe ship-now path.

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

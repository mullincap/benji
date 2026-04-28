# Open Work List

_Last cleanup: 2026-04-25. Anything that shipped is intentionally pruned —
git log + commit messages are the historical record._

## ✅ Recently shipped (2026-04-25)

- **Audit-vs-live snapshot bar — exact match** (2026-04-25) — PR #8 (commit `ea06213`). Cron 05:58 → 06:00 UTC, 65s sleep at script start, price interval 5m → 1m + closed-bar tail filter. Live's snapshot now reads the 1m bar at 06:00 UTC (close 06:00:59) which is the exact reference the audit's `market.leaderboards` row at `timestamp=06:00` was built from. Eliminates the wall-clock-non-determinism bug that produced MAGIC vs SAND swap at the rank-20 boundary on 2026-04-25. Trade deployment at 06:35 unchanged.
- **Audit + live trader 1000X-prefix mapping** (2026-04-25) — PRs #6 + #7. Audit's `_load_mcap_from_db` was concatenating `base + 'USDT'` and missing 1000PEPE/1000SHIB/1000FLOKI/1000BONK; live trader's dispersion kline fetch had the same bug. Both fixed: audit maps via `coin_id` through `COINGECKO_TO_BINANCE`; live uses an inline override map. Closed the 0.45 Sharpe gap between db-sourced and parquet-sourced audits (3.502 → 3.957 — exact match to nightly).
- **Simulator atomic-write fix + DEFAULT_PARAMS merge + remove live_parity toggle + db-default mcap_source + add coingecko mcap option** (2026-04-25) — PRs #1-#5. Cluster of simulator usability + reproducibility fixes surfaced while reproducing canonical CANNON. Job race that left submissions stuck in `queued`, "Edit & Re-run" silently dropping fields not in the saved params, removal of the live_parity toggle that was producing Sharpe 0.85 vs canonical 3.9, and the env-default migrations to keep frontend + nightly cron paths converged.

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

**2. Basket-selection residuals (snapshot bar SHIPPED via PR #8)**

PR #8 (commit `ea06213`) closed the largest part of this divergence:
live now reads the 1m bar at 06:00 UTC for price, exactly matching the
audit's `market.leaderboards` row at `timestamp=06:00`. Wall-clock
non-determinism (originally listed as divergence #3) is also resolved
by the same change. Three residual seams remain:

- **OI 1-second granularity gap**: live uses 5m OI from Binance's
  `openInterestHist` (period=5m, no 1m supported); audit uses 1m OI
  from `market.futures_1m` (built via amberdata batch). Live reads the
  5m OI bar at 05:55 (close 05:59:59), 1 second off audit's 06:00:59
  reference. OI doesn't move fast enough on most coins for this to
  swap symbols, but it can on volatile small caps at the rank-20
  boundary. **Fix scope**: alternate OI source with 1m granularity
  (amberdata API, requires key + new fetch logic) OR push Binance for
  1m OI support (won't happen). Probably not worth chasing unless
  audit-vs-live disagreement gets traced back to OI.

- **Anchor bar source asymmetry**: audit reads 00:00 1m bar from
  `market.futures_1m`; live reads 00:00 1m bar from Binance FAPI. Same
  underlying data (both originate at Binance), but two paths can
  diverge on rare missed/delayed rows. Negligible on most days.

- **Universe scope micro-difference**: audit reads symbols ranked by
  the indexer from `market.futures_1m` (~565 on 04-24); live reads
  Binance `exchangeInfo` filtered to `status=TRADING` (~535). The
  audit can include recently-delisted symbols that still have
  historical futures_1m data; live only sees currently-trading. Tiny
  impact on active days.

**Side-channel work**: market.symbols.binance_id has NULL for PEPE/SHIB
and is missing FLOKI/BONK entirely (PR #7 worked around it with an
inline override map). Symbol-registry refresh has its own 1000X-prefix
detection bug worth a separate small fix so future 1000X-style listings
are handled automatically.

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

### [v2] Include today's partial day in fill-missing auto-detect

Surfaced 2026-04-28. The `fill missing` button on `/compiler/coverage` and
`/indexer/coverage` currently auto-detects gaps in the last 30 days but
**excludes today (UTC) from auto-detect** — today's data is in-progress
and would always look "partial" regardless of any data fix.

For v2: add an explicit "include today" path so users can top up today's
in-progress data on demand. Two options:
- **Smart threshold**: re-define "complete for today" as `≥ floor(elapsed_minutes_since_00:00_UTC × 0.9)` per symbol, then today is auto-detectable when its actual coverage falls below that threshold.
- **Separate "fill today" button** that always runs a fresh metl for today + cagg refresh, regardless of completeness.

Workaround in current version: pass `?dates=YYYY-MM-DD` explicitly to the
POST endpoint to fill any specific date including today.

> **Top 3 priorities for 2026-04-27** (set late 2026-04-26):
> 1. **BloFin futures instrument daily snapshot** — survivorship bias foundation
> 2. **Audit-derived basket as canonical signal source** — daily_signal as fallback
> 3. **Allocation distribution shim** — multi-strategy on shared exchange account
>
> *Nothing else in this section is more important than these three.*

### [#1] BloFin futures instrument daily snapshot — survivorship bias foundation

Surfaced 2026-04-26 late. Today the audit and the live trader each
discover the universe of "currently tradable" symbols at runtime
(audit reads `market.futures_1m`; live reads Binance `exchangeInfo`
+ BloFin `/instruments` endpoints at session start). Neither persists
a daily snapshot of "these were the actually-tradable instruments on
day X", which means:

1. **Survivorship bias is invisible.** The audit can include symbols
   that were tradable on day X but have since been delisted from
   BloFin (or Binance). The live trader can never trade them today,
   but they show up in historical baskets via `market.futures_1m`.
   We have no way to detect this gap because we don't snapshot the
   per-day instrument list.

2. **Backtest results may be inflated.** Symbols that delist
   typically do so after sustained price decline; including their
   pre-delisting returns while excluding their unrecoverable end
   inflates Sharpe + CAGR. Magnitude unknown today.

3. **Audit-vs-live universe mismatch can't be quantified
   historically.** Today's "audit ~565 / live ~535" gap is a single
   snapshot — the historical drift is unknown.

**What to build:**

1. **New cron at 00:05 UTC** (right after metl 00:15 — actually
   slightly before, or a separate slot like 23:55 prior-day): pulls
   BloFin's `/api/v1/market/instruments?instType=SWAP` and persists
   every entry as a row keyed by `(date, symbol)`. Snapshot
   includes: `instrument_id`, `contract_value (ctval)`, `tick_size`,
   `status`, `listing_date` if available.

2. **Same for Binance** (`/fapi/v1/exchangeInfo` filtered to
   `contractType=PERPETUAL`). Together the two snapshots let us
   answer "what was tradable on each exchange on day X?"
   historically.

3. **New table** `market.exchange_instruments_daily(date, exchange,
   binance_id|blofin_id, status, ctval, listing_date, ...)` with PK
   `(date, exchange, symbol)`.

4. **Backfill from BloFin/Binance API** if they return historical
   listing metadata. If not, the snapshot starts from day-1 of the
   cron and we accept the historical gap (mark it explicitly in the
   table).

5. **Audit hook**: when the audit's basket-selection step picks a
   symbol for day X, cross-reference against
   `market.exchange_instruments_daily WHERE date=X` and emit a
   counter for "selected but not tradable on BloFin that day."
   Initially logging-only; later optionally exclude from baskets.

6. **Survivorship-bias estimator**: nightly job that recomputes the
   canonical audit *with* the day-X tradability filter applied and
   reports the Sharpe/CAGR/maxDD delta vs unfiltered. This becomes
   the "survivorship bias coefficient" for the strategy.

**Why this is #1 priority.** Every audit-vs-live alignment question
is more reliable once we can answer "was this symbol tradable on
that exchange that day?" historically. Today we're guessing — the
live=8 vs audit=10 basket gap on 2026-04-26 may already be partially
explained by symbols audit included that BloFin had delisted, but we
have no way to check.

**Scope:** ~4-6h. New cron + table + writer (~150 LOC), backfill
shim (~50 LOC if APIs allow), audit cross-ref hook (~30 LOC), CLI to
query the snapshot (~30 LOC). Survivorship-bias estimator is a
follow-up that builds on this foundation.

---

### [#2] Audit-derived basket as canonical signal source (daily_signal as fallback)

Surfaced 2026-04-26. Today's flow runs the canonical audit *after* the
06:00 UTC trading session has already happened, so the trader can never
read the audit's exact basket for today — it has to recompute its own
basket via `daily_signal_v2.py` at 05:58 UTC. The two paths can pick
different symbols even after PR #6/#7/#8 closed the structural gaps
(live=8 vs audit=10 for 2026-04-26 is the most recent example) because
each runs its own dispersion universe selection, mcap cutoff, and
ranking under slightly different data assumptions.

The fix is to invert the dependency: **audit becomes the primary basket
source, daily_signal becomes the fallback**.

**What this requires:**

1. **Update audit so it runs and persists today's basket *before* the
   06:00 UTC trading session, not after.** Today the canonical audit
   runs end-of-day after the session closes. We need a "morning-only"
   variant (or the same script invoked earlier) that completes by
   ~05:30 UTC using yesterday's closed data + this morning's
   leaderboards row (already produced by the 01:00 UTC indexer cron).
   The audit already computes per-day baskets for every day in its
   window — extending it through "today" before 06:00 is a question of
   cron placement + ensuring the `market.leaderboards` row at
   `timestamp=today 06:00 UTC` is available pre-session (it currently
   is, generated at 01:00).

2. **Persist per-portfolio baskets to a queryable table.** The
   `audit.daily_baskets` table exists from migration 018; needs a
   schema check (date, strategy_version_id, filter_label, symbol,
   rank, weight, source_audit_id, computed_at) and an audit.py write
   hook that fires once per audit run.

3. **Trader read-path: audit table first, fallback to daily_signal.**
   At 05:58 UTC `daily_signal_v2.py` reads
   `audit.daily_baskets WHERE date=today AND strategy_version_id=...`.
   If a row exists with non-empty symbols → use it. If missing/empty/
   stale → fall back to its own in-process computation (current path).

4. **Always dual-write daily_signal's output to a comparison table.**
   Even when audit-derived basket is used, daily_signal still computes
   its own basket and writes it to `trader.fallback_basket` (or
   similar). Builds an automatic side-by-side log so we can spot when
   the two diverge and investigate without running ad-hoc queries.

**Why this is high priority.** Without it, every divergence between
audit and live (filter decisions, basket symbols, sizing) becomes a
separate investigation against shifting data. With it, the audit's
basket is what trades, end of story; daily_signal is just the
safety net.

**Scope:** ~6-8h. Audit cron repositioning + write hook (~150 LOC),
trader read-path with fallback (~50 LOC in `daily_signal_v2.py`),
fallback dual-write table + writer (~50 LOC), schema verification on
`audit.daily_baskets` (~30 min). Does NOT require changing audit.py's
basket-selection logic — only when/where it runs and what it persists.

**Priority: #2 in top-3 (2026-04-26).** Gates real basket alignment
between audit and live; everything else becomes simpler with this
in place.

---

### [#3] Allocation distribution shim — multi-strategy on shared exchange account

Surfaced 2026-04-26. Two strategies (e.g. ALTS MAIN + ALTS MAX)
sharing the same BloFin connection / API key cannot run
simultaneously today: the exchange holds a single net position per
symbol, but each trader process independently tracks "its" position
size and reads the full account balance for capital sizing. Two
consequences if a user splits capital:

1. **Capital double-counting.** Each trader reads exchange equity
   and sizes against the full balance. Two traders against $5k each
   try to deploy ~$5k notional → 2× intended exposure.
2. **Position-close interference.** When trader A closes "its" SOL
   leg, it sends a close order sized to its recorded position. If
   trader B also has a SOL position open, A's close partially
   flattens B's position. B's own close later fails / over-reduces.

This blocked the user from splitting $5k across MAIN+MAX on
2026-04-26 — had to pick one strategy and commit fully.

**Option A — sub-account isolation (preferred long-term)**
Each strategy gets its own BloFin sub-account + API key. Connections
table already supports multiple rows per user. Requires the user to
create a sub-account on BloFin manually + transfer capital + key the
allocation to the new connection. Zero code changes if connections
schema is already per-strategy.

**Option B — software attribution layer**
Ledger table `trader.strategy_positions(connection_id, strategy_id,
symbol, contracts, entry_price, ...)` tracks per-strategy ownership.
Capital sizing reads `account_equity − sum(other_strategies'
notional)` not raw balance. Close orders sized from
`strategy_positions[me]` not runtime_state's local view.
Reconciliation cron reconciles `SUM(strategy_positions[symbol]) ≈
exchange_position[symbol]` per bar.

**Scope:** Option A is ~0 LOC + operational (user creates
sub-account). Option B is ~400-600 LOC across `ExchangeAdapter`
(strategy_id-aware size/close helpers), `trader_blofin.py`
(read/write strategy_positions on entry+exit), new migration +
reconcile cron. Option B is the right call if we want UI-driven
splits without operator action.

**Priority: #3 in top-3 (2026-04-26).** Gates a real product
capability: running multiple strategies on a single exchange account
without operator-level workarounds.

---

### Stale `fa_oos_sharpe` / `Sortino` / `Calmar` across audit runs

Surfaced 2026-04-26 late. Three-way comparison of dispersion settings
(A=curated+dyn `384c45fd`, B=all+dyn `d7edf759`, C=all+static
`ba83fb24`) shows:
- `fa_oos_sharpe` = 3.858 across all three runs (3 decimals)
- `Sortino` = 8.234 across all three
- `Calmar` = 46.061 across all three

Other metrics (Sharpe, CAGR, max_drawdown, CV) all moved as expected.
Three independent risk-adjusted metrics matching to 3 decimals across
three runs with materially different daily-return series is not
coincidence — these metrics are either:
1. Computed on a fixed/cached upstream that doesn't refresh per run
2. Reading from a stale parquet/db row that gets copied through
3. Computed on a window that's identical across runs (less likely
   given the size of the changes)

**Why this matters.** `fa_oos_sharpe` is the only out-of-sample
metric in the audit's headline summary — if it's actually being
copied from somewhere stale, we've been mis-reporting it for a while
in audit history cards + canonical-compare cards + nightly metrics
emails. Sortino and Calmar being identical alongside reinforces
hypothesis (1)/(2).

**What to investigate:**
1. Grep audit.py for `fa_oos_sharpe`, `sortino`, `calmar`
   computation. If they're computed per run, check what input they
   read (df_4x, daily_with_zeros, etc.) and whether that input is
   cached.
2. Check `audit.equity_curves` and `refresh_strategy_metrics` paths
   — are these metrics persisted from a single canonical run and
   then copied to subsequent rows by tag?
3. Run a 4th audit with materially different params (e.g. swap
   strategy version, change leverage) and check whether the three
   "stuck" metrics finally move. If they don't, the cache is in
   audit.py itself; if they do, the cache is somewhere upstream.

**Scope:** ~1-2h. Mostly grep + trace; the fix depends on what's
found but is likely small once located.

---

### Lock dispersion universe to "all + dynamic on" + surface mode in audit history

Surfaced 2026-04-26. The simulator already exposes a
`dispersion_universe_mode` toggle (curated 90 / all full-mcap) and
a `DISPERSION_DYNAMIC_UNIVERSE` flag. Decision made 2026-04-26 to
**default to `mode='all'` + dynamic-on regardless of Sharpe outcome**
— see `feedback_dispersion_universe_default.md`. Methodological
correctness wins over Sharpe optimization; reverting to curated for
better Sharpe would be curve-fitting on universe choice and would
silently drift from live's universe over time.

**Open work:**

1. **Flip the simulator UI default toggle from "curated (90 coins)"
   to "all (full mcap table)".** Path of least resistance should be
   the locked default. Currently runs silently default to curated,
   which led to today's footgun (audits labelled "dynamic" were
   actually curated for several re-runs).

2. **Surface `dispersion_universe_mode` on each audit history card.**
   The current card shows sharpe + date + name — needs a "universe:
   all" / "universe: curated" line so the operator can tell at a
   glance which pool was used. Eliminates the "wait, was this curated
   or dynamic?" failure mode that ate ~30 min today.

3. **Echo `dispersion_universe_mode` in `FINAL_METRICS` from
   audit.py.** So the saved results reliably carry the mode through
   to the audit job's persisted metrics, not just the run-config side.

4. **Verify the "curated" mode mechanics** — quick read of audit.py
   to confirm `mode='curated'` actually resolves to
   `COINGECKO_TO_BINANCE` (89 entries, but UI says 90 — minor
   discrepancy worth understanding).

5. **Re-attribute today's Sharpe drop.** Pre-everything baseline was
   ~3.815, post-Change-1 re-runs showed 3.614 with curated mode.
   Both runs were on curated, so the 0.2 delta is NOT the dynamic-pool
   effect — it's some downstream interaction from the date-fix or
   BTC-source change. Once the new "all + dynamic on" and
   "all + dynamic off" audits land (kicked off 2026-04-26 late), do a
   clean 3-cell side-by-side and document real attribution.

6. **If "all + dynamic on" Sharpe is materially below curated** (>0.3
   drop), open a follow-up item to **re-tune the dispersion threshold
   (currently 0.66) for the wider universe's noise profile** — NOT to
   revert to curated.

**Scope:** ~2-3h. UI default flip (~15 min), audit history card
addition (~30 min), audit.py FINAL_METRICS echo (~30 min),
verification + re-attribution writeup (~60 min).

---

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

### Reinvest knobs — capital redeployment from stopped positions

Surfaced 2026-04-25. Today the audit implicitly models "freed capital
from stopped-out positions sits idle" as the only behavior — when a
symbol stops at -6%, its slot stays clamped at the loss for the rest of
the session, and there's no way to ask "what if I redeployed that
capital into the survivors?". Hides whether a more aggressive
reinvestment policy would meaningfully improve risk-adjusted returns.

**Two new knobs in the simulator:**
- `reinvest_amount_pct`: `0.0` (default, current behavior) | `0.5` | `1.0`
- `reinvest_trigger`: `"instant"` (default) | `"profitable"` (NAV >= 1.0)

When per-symbol stop fires, the freed capital × amount_pct gets
redeployed equal-weight across remaining active symbols. Non-reinvested
portion sits as idle cash. Trigger controls whether redeployment
happens at the stop bar or waits for the next bar where portfolio NAV
is non-negative.

**Verified math:** at `amount_pct=0`, the new share-weighted NAV path
collapses exactly to today's `mean(clamped_paths)` — byte-identical
canonical preservation. Confirmed across a 2-symbol example and a
3-symbol example with mixed stop timings.

**Implementation surface:**
- `pipeline/rebuild_portfolio_matrix.py` — replace `mean()` with
  `build_reinvest_path()` taking per-sym clamped paths + 2 new env
  vars + returning NAV-based path. Pass to `_apply_hybrid_day_param`
  unchanged (triggers fire on the new path; at `amount_pct=0` they
  fire identically to today).
- `backend/app/api/routes/jobs.py` — add 2 fields to JobRequest
- `backend/app/services/audit/pipeline_runner.py` — env passthrough
  (`REINVEST_AMOUNT_PCT`, `REINVEST_TRIGGER`)
- `frontend/app/components/LeftPanel/ParamForm.tsx` — new "Capital
  reinvestment" section with two dropdowns
- `frontend/app/simulator/page.tsx` — DEFAULT_PARAMS adds the fields

**Acceptance:**
- canonical with `reinvest_amount_pct=0` → identical Sharpe/MaxDD/
  CAGR to today's nightly
- same with `amount_pct=1.0`, `trigger=instant` → Sharpe shifts in
  expected direction; spot-check single-session basket math
- same with `amount_pct=0.5`, `trigger=profitable` → results land
  between 0% and 100% cases

**Scope:** ~300-400 LOC. ~4-6h focused. Doable in one pass.

**Not implemented to live trader yet** — simulator-only until the
backtest results validate the policy. Live integration would be a
follow-up PR (~150 LOC for order placement on each stop event +
profitable trigger watch loop).

**Concentration risk note:** with `amount_pct=1.0`, every stop
concentrates capital into fewer symbols. By end of day if 8 of 10
stopped, surviving 2 hold ~5x their original weight. Magnifies wins
AND losses on survivors — the audit will reveal this directly, but
worth understanding before promoting any reinvest config to live.

---

### Portfolios tab — actuals vs estimates toggle

Surfaced 2026-04-25. The Portfolios tab's per-symbol cumulative ROI
chart currently shows a single anchor — open_prices (06:00 UTC bar),
audit-canonical, after Option 3 fix shipped 2026-04-25. User wants to
toggle between two views to compare what manual cash redeployment
after a stop costs vs. what the strategy-mechanical baseline would
have delivered:

- **Estimates** (current default): open-anchored returns. Strategy-
  mechanical baseline. Ignores fill slippage and any manual
  intervention. Matches per-symbol stop check.
- **Actuals**: entry-anchored returns (current_price / entry_price).
  Captures actual fill slippage and any manual buys that shifted
  weighted-avg entry. Reflects realized account performance per
  symbol.

**What to build:**
- Trader: write BOTH series to portfolio_bars per bar. Either restructure
  `symbol_returns` JSONB to `{"BTC-USDT": {"open": -0.05, "entry": -0.08}}`
  or add a sibling `symbol_returns_entry` column.
- API: return both arrays in the bars[] response.
- UI: toggle button in the Portfolios chart header (`[ESTIMATES] [ACTUALS]`).
  Switches which array `b.sym_returns` reads. Default = estimates.
- Portfolio aggregate line: also expose both `incr_open` (already
  computed internally as the open-anchored equivalent) and `incr_entry`
  (current `incr`). Toggle switches both per-symbol and aggregate
  together so the chart stays internally consistent.

**Use case:** user manually adds buys after a stop fires. Estimates view
shows what would have happened mechanically (no manual reinvest); actuals
shows what actually happened to the account. Difference = the impact of
the manual policy.

**Scope:** ~2-3h. Backend change requires backend container rebuild —
deploy risk applies (dup-spawn race + lock-stuck recovery, see runbook
playbooks B/C). Bundle with the early-fill progress bar feature for
single deploy.

---

### Portfolios tab — early-fill progress bar (real-time)

Surfaced 2026-04-25. The Portfolios tab shows portfolio ROI as a number
+ chart but doesn't visualize how close the session is to its early-fill
trigger. User wants a real-time progress bar showing `current_incr /
early_fill_y`, with a time-remaining indicator for the fill window.

**What to build:**
- API: extend `/api/manager/portfolios/{date}` meta to include
  `early_fill_y` (target return %) and `early_fill_x` (max minutes in
  fill window) by JOINing through to `audit.strategy_versions.config`.
- UI: progress bar component below KPIs. Width = `min(1.0, current_incr
  / early_fill_y)`. Fill color: amber while < 1.0, green when fired.
  Show `incr%` over `early_fill_y%` numerically. Time-remaining badge:
  `(early_fill_x - elapsed_minutes)` until the fill window closes;
  flips to "fill window closed" past that.
- Live polling already in place on the Portfolios page (`status=active`
  triggers 30s polls), so no new fetch logic.

**Scope:** ~2h. Bundle with the actuals/estimates toggle PR — both are
Portfolios-tab visualization improvements that share a backend deploy.

---

### Simulator — per-symbol take-profit toggle

Surfaced 2026-04-25. Mirror of the existing `stop_raw_pct=-6%` per-
symbol stop, but on the upside: when a symbol's return crosses a
configurable take-profit threshold, the position closes and the
symbol's contribution clamps at the TP value for the rest of the
session. Captures individual-symbol winners that may give back gains
later, complementing the portfolio-level early_fill trigger which
operates on portfolio-wide return.

**Two new params in the simulator:**
- `enable_sym_tp`: bool, default `false` (preserves canonical)
- `sym_tp_pct`: float, default `0.10` (e.g., +10% per symbol). Only
  used when `enable_sym_tp=true`.

**Implementation:**
- `pipeline/rebuild_portfolio_matrix.py`: extend `apply_raw_stop` (or
  add `apply_raw_stop_with_tp`) to also clamp from above when
  `raw[i] >= sym_tp_pct`. Same clamp-and-freeze pattern: once fired,
  symbol contributes `sym_tp_pct` for all subsequent bars.
- `backend/app/api/routes/jobs.py`: 2 new JobRequest fields.
- `backend/app/services/audit/pipeline_runner.py`: env passthrough
  (`ENABLE_SYM_TP`, `SYM_TP_PCT`).
- `frontend/app/components/LeftPanel/ParamForm.tsx`: toggle + numeric
  input under existing per-symbol risk controls section.
- `frontend/app/simulator/page.tsx`: DEFAULT_PARAMS adds the fields.

**Default preserves canonical:** `enable_sym_tp=false` is identical to
today's behavior (no upper clamp, only the -6% stop).

**Scope:** ~2-3h. Pairs cleanly with the reinvest knobs feature — both
modify per-symbol path construction. Could ship together as a single
"per-symbol risk controls" PR, or independently.

**Not implemented to live trader yet** — simulator-only until
backtests validate the TP threshold. Live integration would mirror
the per-symbol stop close path in trader_blofin.py (~80 LOC).

---

### Simulator — capital distribution mode

Surfaced 2026-04-25. The audit currently assumes uniform 1/N
equal-weight allocation across the basket at session start. Want a
toggle to test alternative distribution methods so we can backtest
whether weighting by some signal improves risk-adjusted returns.

**Initial set of distribution modes:**
- `equal` (default, current behavior): weight = 1/N for all symbols
- `mcap_weighted`: weight ∝ market cap (large-cap gets more)
- `inverse_mcap_weighted`: weight ∝ 1/mcap (small-cap gets more —
  the basket's small-caps tend to be where the dispersion-driven
  signal is strongest)
- `rank_weighted`: weight ∝ (N - rank + 1) (top-of-basket linearly
  preferred — rewards the strongest price+OI signal)
- `vol_weighted_inverse`: weight ∝ 1/historical_vol (risk parity —
  smooths per-symbol contribution to portfolio variance)

Extensible: each mode is a function of (basket symbols, available
metadata at session-start time). Easy to add more (volume-weighted,
score-weighted, etc.) without touching the framework.

**Implementation:**
- `pipeline/rebuild_portfolio_matrix.py`: replace implicit
  `mean(clamped_paths)` with `np.average(clamped_paths, weights=w)`
  where `w` is computed once at session start by a new
  `compute_basket_weights(symbols, mode, snapshot_data)` function.
  Weights normalized to sum to 1.0.
- Data fetch: at session-start anchor, pull mcap (from
  `market.market_cap_daily`) and historical vol (from
  `market.futures_1m` rolling stdev). Already loaded for
  dispersion/ranking purposes — reuse the existing fetch.
- `backend/app/api/routes/jobs.py`: 1 new JobRequest field
  (`distribution_mode: str = "equal"`).
- `backend/app/services/audit/pipeline_runner.py`: env passthrough
  (`DISTRIBUTION_MODE`).
- `frontend/app/components/LeftPanel/ParamForm.tsx`: dropdown under
  Universe + Risk section.
- `frontend/app/simulator/page.tsx`: DEFAULT_PARAMS adds
  `distribution_mode: 'equal'`.

**Default preserves canonical:** `distribution_mode='equal'` is
identical to today's behavior.

**Pairs naturally with reinvest knobs:** both features modify
weights — distribution_mode sets the INITIAL weights; reinvest
modifies them DYNAMICALLY as stops fire. Implementation can share
the same share-weighted NAV path machinery.

**Scope:** ~3-4h. Bundle with reinvest knobs + per-symbol TP into a
single "advanced position-sizing" PR — all three touch the same
per-symbol path construction code, share infrastructure, and benefit
from a single acceptance test pass.

**Not implemented to live trader yet** — simulator-only until
backtests validate. Live integration would touch the order-sizing
logic in trader_blofin.py at session start.

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

### Filter-math exposure on Audit Breakdown tab — surfaced 2026-04-27

Backstory: spent ~4h on 2026-04-27 diagnosing why audit's
`A - Tail + Dispersion` returned `no_entry=False` for 2026-04-26 while
live ALTS MAIN sat flat with `dispersion_low: ratio=0.343 < 0.66`. Root
cause turned out to be 35+ hardcoded `"2026-03-01"`/`"2026-03-02"` date
literals in `audit.py` (`build_*_filter` reindex + fetcher defaults +
caller missing `end`) that silently truncated filter Series past those
dates. Fixed in `1997b32` via module-level `_AUDIT_IDX_END` constant.

The bug was invisible because the Breakdown tab only shows
PASS/FLAT booleans per filter — no per-day ratio numbers. If we'd seen
`disp_ratio = (no value)` instead of `disp_ratio = 0.557` or
`disp_ratio = 1.13` per day, the truncation would have been obvious
within minutes.

**Proposal:** add per-day filter-math columns to the Breakdown table.

For each row (day):
- **Tail Guardrail**: `prev_day_ret`, `vol_ratio`, `tg_fires`, short
  reason (`crash gate` / `vol gate` / `both` / `clear`)
- **Dispersion**: `yesterday_disp`, `baseline_median`, `disp_ratio`,
  `disp_fires`
- Color-code ratio cells (red below threshold, green above)

**Scope:** ~2h backend (modify `build_tail_guardrail` and
`build_dispersion_filter` to also return per-day diagnostic DataFrames,
inject into `metrics.filter_diagnostics_per_day`, expose via job.json),
~2h frontend (read from job.json, render new columns or expandable
diagnostic row in Breakdown table). Total ~4h.

**Why this matters:** future filter bugs of this class become visible
on inspection rather than requiring a 4h reproduction-and-diagnosis
session. Same observability principle as the audit verifier
(intraday_audit) — make divergence loud, not silent.

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

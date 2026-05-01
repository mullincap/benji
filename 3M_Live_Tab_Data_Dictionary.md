# 3M · Manager · Live — Data Dictionary

> **Override notice:** This dictionary was authored with a Binance-account assumption.
> For v1, account data (§3, §4, §12, §13a) comes from BloFin; Binance is market-data only.
> See CLAUDE.md "Manager Live tab — venue + storage conventions" for canonical conventions.

> **Encoding note:** the document arrived through a channel that introduced
> UTF-8 mojibake (e.g. `·` shown as `Â·`, em-dashes as `â`). The version
> below has been hand-cleaned for readability. The original-source bytes
> in the attachment are reproducible from conversation context if exact
> verbatim is needed for any future audit.

**Status:** v1 draft · companion to `3M_Manager_Live_Tab_Mockup.html`
**Scope:** every number displayed on the Live tab, with its source, computation, refresh cadence, and fallback behavior.
**Venue:** Binance USDM Futures (single venue v1) — see override notice above.
**Reference:** Binance Futures API docs at `https://binance-docs.github.io/apidocs/futures/en/`.

---

## 0 · Document Conventions

- All endpoint paths are relative to `https://fapi.binance.com`.
- All times in **UTC** unless otherwise stated.
- All dollar amounts in **USDT** (USDM Futures collateral).
- "Today" = current UTC day (00:00–23:59).
- "Session" = strategy execution window, defined by `daily_signal.py` schedule (currently 06:35 entry → 23:55 exit, configurable).
- Sign convention: positive distance from MA means mark > MA. Long position is "aligned" when distance is positive; short position is "aligned" when distance is negative. UI color encodes alignment, displayed value preserves raw sign.
- `signed user data` = endpoint requires API key + signature; rate-limited per UID.
- `public market data` = no auth, IP-rate-limited (2400 weight/min).
- `user data stream` = WebSocket, account events pushed in real time.

---

## 1 · Refresh Cadence Tiers

The page does not poll a single refresh interval. Each data class has its own cadence and visual staleness indicator.

| Tier | Cadence | Source | Visual indicator |
|------|---------|--------|------------------|
| **T0 · live tick** | 2 s | `markPrice` WebSocket stream + `/fapi/v2/account` | green pulse dot in page header |
| **T1 · order events** | event-driven | user data stream `ORDER_TRADE_UPDATE` | row updates in place |
| **T2 · minute** | 60 s | funding, OI, 24h ticker | small `↻ Ns ago` stamp per viz |
| **T3 · five-minute** | 300 s | L/S skew (Binance updates every 5m anyway) | as above |
| **T4 · bar-close** | per timeframe | klines for box plots, MAs, regression | recompute on each closed bar |
| **T5 · hourly** | 3600 s | correlations, effective-N, factor decomp | recompute on 1H bar close |

**Stale state:** if a tier's last refresh exceeds 2× its cadence, that viz greys out to ~60% opacity and shows a `STALE · Ns` badge in red.

---

## 2 · Page Header

| Field | Format | Source | Computation | Tier |
|-------|--------|--------|-------------|------|
| Account equity (top-right pill) | `$3,348.22` | `/fapi/v2/account` → `totalWalletBalance + totalUnrealizedProfit` | sum | T0 |
| Live tick badge | `LIVE · TICK 2s` | derived | constant if WS connected; `RECONNECTING` if disconnected | T0 |
| Venue badge | `VENUE · BINANCE USDM` | static | constant v1 | — |
| Open position count | `6 OPEN POSITIONS` | `/fapi/v2/positionRisk` filter `positionAmt != 0` | count | T0 |
| Last-update timestamp | `UPDATED 23:55:14 UTC` | derived | timestamp of last successful T0 fetch | T0 |
| **Action: REFRESH ↻** | button | — | force-refresh all tiers; bypasses cache | — |
| **Action: EXPORT** | button | — | dumps current page snapshot to JSON; out of v1 scope | — |
| **Action: ⚠ FLATTEN ALL · HOLD** | button | — | hold-to-confirm 2s; on confirm → market-close every position via `POST /fapi/v1/order` with `closePosition=true` per symbol; user-data stream confirms each | T1 |

**Empty state:** if zero open positions, page shows a centered "No open positions" panel with a link to Manager > Portfolios.
**Error state:** if `/fapi/v2/account` returns an error, top-right pill shows `—` in amber and the live tag shows `STALE`. All downstream vizes inherit stale styling.

---

## 3 · Account Snapshot (KPI strip)

Six cards, all driven by `/fapi/v2/account` plus `/fapi/v2/positionRisk` aggregation.

| Card | Field | Format | Computation | Tier |
|------|-------|--------|-------------|------|
| **Account Equity** | headline | `$3,348.22` | `totalWalletBalance + totalUnrealizedProfit` | T0 |
| | "cash $1,663.86" | `$N` | `availableBalance` from `/fapi/v2/account` | T0 |
| | "−$31.78 today" | signed `$` | `equity_now − equity_at_00:00_UTC`; requires daily snapshot job (see §13) | T0 + daily snapshot |
| **Deployed Margin** | headline | `$1,684.36` | `totalInitialMargin` from `/fapi/v2/account` | T0 |
| | "50.3% of equity" | `%` | `totalInitialMargin / (totalWalletBalance + totalUnrealizedProfit) × 100` | T0 |
| | "6 positions" | count | from `positionRisk` count | T0 |
| **Total Notional** | headline | `$4,339.41` | `Σ |positionAmt × markPrice|` across open positions | T0 |
| | "1.30x equity" | `Nx` | `total_notional / equity` | T0 |
| | "long $X / short $Y" | `$ / $` | `Σ longs` and `Σ shorts` separately by sign of `positionAmt` | T0 |
| **Net Unrealized PnL** | headline | signed `$` | `Σ unrealizedProfit` from `positionRisk` | T0 |
| | "+0.93% on equity" | signed `%` | `unrealized_pnl / equity × 100` | T0 |
| | "4 of 6 green" | `N of M green` | count positions with `unrealizedProfit > 0` | T0 |
| **Avg PnL / Position** | headline | signed `%` | `mean(unrealizedProfitPercent_per_position)` where percent is per notional | T0 |
| | "median +0.85%" | signed `%` | `median(unrealizedProfitPercent_per_position)` | T0 |
| | "σ 5.8%" | `%` | `stdev(unrealizedProfitPercent_per_position)` | T0 |
| **Avg Leverage** | headline | `Nx` | `Σ(notional_i × leverage_i) / Σ notional_i` (notional-weighted) | T0 |
| | "weighted by notional" | label | static | — |
| | "range 1.5x – 4.0x" | `Nx – Nx` | `min/max` per-position effective leverage; effective lev = `notional / initialMargin` per position | T0 |

**Collapsed-state summary line:** `EQUITY $3,348.22 · NET +$31.12 · DEPLOYED $1,684 · LEV 2.1x` — same data as expanded, abbreviated.

**Caveats:**
- Today's PnL requires snapshotting equity at 00:00 UTC daily. Need a small backend job (`equity_snapshot.py`) that writes to a `account_snapshots` table.
- "Effective leverage" per position is computed locally as `notional/initialMargin`; Binance's `leverage` field is the user-set max, not the actual ratio in use.

---

## 4 · Risk Signals (4 cells)

| Cell | Field | Format | Computation | Tier |
|------|-------|--------|-------------|------|
| **Margin Level** | headline | `12.4x` | `totalMarginBalance / totalMaintMargin` from `/fapi/v2/account` | T0 |
| | "liquidation buffer 91.9%" | `%` | `1 − (totalMaintMargin / totalMarginBalance) × 100` | T0 |
| **Largest Position** | headline | `BTC · 40.3% notional` | symbol of `argmax(|positionAmt × markPrice|)` + that position's share of total notional | T0 |
| | "$1,748 long · 4.0x · manual" | `$ · Nx · src` | notional, side, effective lev, source attribution (see §14 source attribution) | T0 |
| **Nearest Stop** | headline | `MEGA · −0.7% to SL` | `argmin(|markPrice − stopLossPrice| / markPrice)` across positions with active SL | T0 + T1 |
| | "SL $X · mark $Y" | `$ · $` | from `/fapi/v1/openOrders` filter `type='STOP_MARKET'` and `markPrice` | T0 + T1 |
| **Unhedged Concentration** | headline | `73.5% on BTC + SOL longs` | identify largest single-direction concentration: `max(long_notional, short_notional) / total_notional`; label with constituent symbols if ≥ 2 | T0 |
| | "manual book — no protective stops on BTC" | flag | true if any position in the concentrated cluster has no SL order | T0 + T1 |

**Collapsed-state summary line:** `MARGIN 12.4x · NEAREST STOP MEGA −0.7% · CONCENTRATION 73.5% LONGS`

**Caveats:**
- "Nearest stop" only counts user-placed SL orders. If no SL exists on any position, show `—` rather than computing distance to liquidation (which is a different signal — see Improvement Note A).
- "Source attribution" requires the strategy session ledger to know which positions belong to ALTS MAIN vs MANUAL (see §14).

---

## 5 · Position Map · Notional × PnL · SINCE OPEN (Treemap)

Each tile = one open position. Tile area ∝ notional. Tile color intensity ∝ unrealized PnL %.

Per-tile fields: ticker, side badge, source line, PnL %, notional + share, PnL $.
Layout: top 1–2 positions (>20% of book) get full-height column tiles; remaining bundle into a 2×N sub-grid sized proportionally. Floor on tile size: 60×40 px so tiny positions stay legible.

Color tiers (binned `unrealizedProfitPercent`): >+5% strong-green, 0–5% soft-green, 0 to −3% soft-red, −3 to −10% mid-red, <−10% strong-red. Apply 2-tick hysteresis to avoid flicker on price oscillation around tier boundaries.

Tier: T0.

---

## 6 · PnL Attribution · TODAY (Waterfall)

Horizontal divergent bars, one per position, sorted by absolute today's PnL contribution descending.

Per-position contribution = `unrealizedProfit_now − unrealizedProfit_at_00:00_UTC` for positions open at midnight; `unrealizedProfit_now − 0` for positions opened today; `realizedProfit_today` for positions closed today (added separately if any). Bar width = `|contribution_i| / max(|contribution|) × 50%` of total bar zone.

Period toggle (post-v1): TODAY · 7D · 30D · SINCE OPEN. Wider windows use `unrealizedProfit_now − unrealizedProfit_at_window_start` for still-open positions plus realized PnL events from `/fapi/v1/income?incomeType=REALIZED_PNL`.

Tier: T0 + daily snapshot.

---

## 7 · Exposure · Long vs Short · LIVE

Single horizontal divergent bar centered on zero. Right side = longs, left side = shorts. Sub-segments per position; ordered by notional desc within each side.

Long total = `Σ |positionAmt × markPrice|` for `positionAmt > 0`; short total = same for `< 0`. Net = `long_total − short_total`, labelled `NET +$X LONG` or `NET −$X SHORT`. Net as multiple of equity = `|net| / equity`.

Bar scale: per-side max-anchored so the larger side fills 50% of total bar; smaller side scales proportionally. Stat strip below shows LONG / SHORT / NET with totals + position counts. Source breakdown table groups by source (MANUAL vs strategy name) within long/short.

Tier: T0.

---

## 8 · Coverage Matrix · PnL Correlation · 30D ROLLING

N×N matrix of pairwise PnL correlations, sign-adjusted for direction.

Cell value: rolling Pearson correlation of position-level daily PnL series. Color tiers: >+0.7 strong-con (deep red), +0.4–0.7 mid-con, +0.2–0.4 soft-con, −0.2 to +0.2 neutral, −0.4 to −0.2 soft-hedge (light blue), −0.7 to −0.4 mid-hedge, <−0.7 strong-hedge (deep blue). Diagonal: em-dash.

**Computation Note A — Position PnL series:**
For each currently-open position, build a 30-day daily PnL series from historical kline data scaled by current position size and direction. Use `/fapi/v1/klines?interval=1d&limit=30` per symbol. Series: `pnl_t = positionAmt × (close_t − close_{t-1})`. Sign-adjustment for direction is implicit in `positionAmt`'s sign — long+long correlated → positive (concentration); long+short correlated → negative (hedge).

Empty/insufficient: <14 days of underlying kline history → display em-dash with tooltip.

Refresh: 1H bar close (T5). Caches keyed by `(symbol_set, position_sizes_hash)`.

---

## 9 · Factor Decomposition · 30D ROLLING + Effective-N Gauge

### 9a · Effective-N Gauge

> **Spec correction (2026-04-30):** the original draft of this section
> defined `effective_N = 1 / Σ(w_i^2)` — an inverse-Herfindahl on the
> σ-weighted notional weights. That form is **correlation-blind**:
> N perfectly-correlated equal positions yield `effective_N = N` under
> it, which is the opposite of what the gauge needs to communicate.
> The corrected formula below is the **diversification ratio squared**
> (also called the "effective number of independent bets"), which
> reduces to `N` only when positions are uncorrelated and equal-σ, and
> collapses to `1` when positions are perfectly collinear — matching
> the gauge's stated reference cases. The implementation in
> `backend/app/services/correlation_cache.py` has used this form since
> step 10; this note brings the dictionary in line.

Headline number = correlation-aware diversification ratio squared: how many independent silos the book behaves like. Denominator on the gauge is the **nominal position count** (the count of open positions).

```
σ_i         = stdev of position-level daily $-PnL series (30d, dir-signed)
σ_silo      = Σ σ_i                          # standalone risk if all independent
σ_portfolio = sqrt(σ' × Σ_corr × σ)          # actual portfolio risk
            = sqrt( Σ_ij σ_i × σ_j × ρ_ij )  # equivalent expansion
effective_N = (σ_silo / σ_portfolio)^2
```

For N perfectly-uncorrelated, equal-σ positions, `effective_N = N`. For perfectly-correlated positions, `effective_N = 1`. Sign of ρ matters — short positions get `dir_sign = −1` baked into the $-PnL series so a long-vs-short pair against the same underlying surfaces as ρ near `−1` (a hedge), pushing `effective_N` above the long-only ceiling.

Detail prose (rule-based, absolute thresholds): `<2`: "concentrated, not diversified"; `2–4`: "moderately concentrated"; `>4`: "well diversified". Zone bands red 0→2, amber 2→4, green 4→nominal_count. With ≤4 positions the green band collapses (fully-diversified is unreachable for a small book — that's honest).

Diversification benefit (surfaced inline next to the headline): `(1 − σ_portfolio / σ_silo) × 100%`. 0% means the book carries the same risk as N silos; 100% means the cross-correlations have completely cancelled out the silo risk (only achievable with deliberate short hedges).

### 9b · Factor Decomposition

> **Spec correction · Note C (2026-05-01):** the original draft used the
> raw equal-weighted alt index return as `factor_ALT`. On 30d daily
> windows, BTC and the raw alt index are typically ~0.85+ correlated
> (deployment 2026-05-01 measured 0.97 on the live 29d window) — too
> collinear for a 2-factor regression to cleanly separate on n≈29 obs
> even with ridge. Symptoms: portfolio variance attribution stays
> stable (the formula is rotation-invariant), but per-position β's
> split arbitrarily across the collinear pair, producing
> economically nonsensical reads like β_BTC < 0 on a meme-coin long.
>
> Fix: `factor_ALT` is now the **BTC-orthogonal residual** of the
> raw alt-index returns. Build the raw equal-weighted return series,
> OLS-regress it on BTC over the same window, use the residuals.
> BTC and ALT factors are then uncorrelated by construction. β_BTC
> absorbs all BTC-correlated variance (including the BTC-correlated
> component of alt moves); β_ALT picks up only genuinely alt-specific
> moves above and beyond BTC. Portfolio-level numbers (variance
> attribution percentages, σ_portfolio, diversification benefit) are
> invariant to this rotation, but BTC% jumps from "near zero by
> cancellation" to "honest BTC factor share," and per-position β
> values become economically interpretable.
>
> Implementation: `backend/app/services/factor_decomp_cache.py`,
> `_orthogonalize()` helper plumbed in `compute_factor_decomposition`
> right after the BTC and raw-alt return series are built.

Bar segments = factor variance contributions, normalized to 100%. v1 factors: BTC, BTC-orthogonal alt residual, idiosyncratic (residual). Diversification benefit = `1 − (σ_portfolio / Σ σ_i) × 100`.

```
Build factors (this order matters — orthogonalization is the load-bearing step):
  raw_alt_t  = mean over alt_index.yml members of daily ret on day t
  factor_BTC = daily return of BTCUSDT
  factor_ALT = residuals of OLS(raw_alt ~ factor_BTC)   ← BTC-orthogonal

Per position (ridge λ=0.01, applied to standardized features):
  pnl_i = α + β_BTC × factor_BTC + β_ALT × factor_ALT + ε_i

Variance attribution (orthogonal factors → no cross-term to allocate):
  var_BTC_i  = β_BTC^2  × Var(factor_BTC)
  var_ALT_i  = β_ALT^2  × Var(factor_ALT)        # smaller than Var(raw_alt)
  var_IDIO_i = Var(ε_i)
  pct_x = var_x / (var_BTC + var_ALT + var_IDIO) × 100

Portfolio-level: regress portfolio_pnl_t = Σ pnl_i_t on the same factors;
the coefficients become aggregated dollar exposures (β_BTC = Σ β_BTC_i etc).

Diversification benefit:
  σ_silo      = Σ σ_i              # sum of standalone $-PnL stdevs
  σ_portfolio = stdev(portfolio_pnl_t)  # actual portfolio risk
  benefit     = (1 − σ_portfolio / σ_silo) × 100
```

Caveats: alt-index pool needs quarterly rebalance review (config/alt_index.yml — drop delisted members, add liquid newcomers); positions with <14 days of history show em-dash + INSUFFICIENT HISTORY warning. Per-position β's are the noisier output (n=29 daily obs is thin); portfolio-level percentages are the trustworthy headline.

---

## 10 · Trailing Distribution · 24H WINDOW (Box Plot Strip)

Per-position SVG box plot from last 24h of 5m bars (288 data points): p5/p25/p50/p75/p95 percentiles plus live mark dot and entry triangle.

Mark dot color: `good` (long+mark above p50 OR short+mark below p50), `bad` (long+mark below p25 OR short+mark above p75), otherwise `neu`.

Trend arrow + σ: linear regression slope on the 288-point series, normalized as σ-units of typical bar move.

```
Fit linear regression: price_t = α + β × t over the 288 bars
σ_typical = stdev of bar-to-bar absolute moves over the window
slope_σ = β × √N / σ_typical
       (= "trend's total move in units of expected √N random-walk
           drift over N bars" — drift-to-noise, not raw drift)

Display:
  > +1.5σ : ↗ strong up
  +0.5 to +1.5σ : ↗ up
  −0.5 to +0.5σ : → flat
  −0.5 to −1.5σ : ↘ down
  < −1.5σ : ↘ strong down

Color: aligned with position = green; against = red; flat = amber.
```

The intent is drift-vs-noise, not raw drift. An earlier draft used `× N`
in the numerator; that scales linearly with the window — even a trivial
daily drift saturates the "strong up" bin because the trend term grows N
while the per-bar σ stays constant. Dividing by `σ_typical × √N` (i.e.
the expected dispersion of an N-step random walk) produces the canonical
finance trend-strength signal where ±1.5σ is a meaningful "direction
emerging from noise" threshold. Cross-check: the mockup's displayed σ
values (BTC +2.1σ, MEGA +1.8σ, AIXBT −2.6σ) are only consistent with the
√N normalization. Implemented in `backend/app/services/boxplot_cache.py`.

Empty/insufficient: <50 bars of history → INSUFFICIENT DATA placeholder. Tier: T4 (5m).

---

## 11 · MA Alignment · DISTANCE FROM EMA · BY TIMEFRAME

Heatmap, rows = positions, columns = `[5m, 15m, 30m, 1h, 4h, 8h, 1D]`. Plus a CONFLUENCE column: `N / 7` count of timeframes where mark is aligned with position direction.

Cell value = `(markPrice − EMA_at_timeframe) / markPrice × 100`. Cell color binned by alignment with position direction (long+mark>MA = aligned; short+mark<MA = aligned). Magnitude determines intensity (strong/mid/soft).

v1 default EMA: **EMA20** at every timeframe. Compute via `/fapi/v1/klines?interval={tf}&limit=200` per symbol per timeframe (200 bars ensures convergence). Per-cell cache keyed by `(symbol, tf, last_close_time)`. Recompute only on bar close for that timeframe (T4).

Refresh strategy: never fully recompute the heatmap at once; each cell has its own bar-close trigger. Cell that just updated gets a brief flash animation.

Empty/insufficient: <20 bars at a given timeframe → em-dash with neutral background.

---

## 12 · Open Positions Table

| Column | Format | Source / Computation | Tier |
|--------|--------|----------------------|------|
| SYMBOL | `BTC` + `USDT-PERP` | parse `positionRisk.symbol` | T0 |
| SIDE | LONG / SHORT | sign of `positionAmt` | T0 |
| SOURCE | strategy name / MANUAL | match position to active strategy session by entry timestamp + symbol; default MANUAL if no match | T0 + ledger |
| ENTRY | `$` | `entryPrice` from `positionRisk` | T0 |
| MARK | `$` | `markPrice` from `positionRisk` (or premium index) | T0 |
| SIZE | base units | `|positionAmt|` | T0 |
| NOTIONAL | `$` | `|positionAmt × markPrice|` | T0 |
| LEV | `Nx` | effective lev = `notional / initialMargin` | T0 |
| UNREALIZED PNL | signed `$` + `%` | `unrealizedProfit` and `unrealizedProfitPercent` | T0 |
| AGE | `Nh Nm` | `now − position_open_timestamp`; needs position-history table | T0 + ledger |
| SL · DIST | `$` + signed `%` | from `/fapi/v1/openOrders` filter `type='STOP_MARKET'`; distance = `(SL − mark) / mark × 100` | T0 + T1 |
| TP · DIST | `$` + signed `%` | from `/fapi/v1/openOrders` filter `type='TAKE_PROFIT_MARKET'` | T0 + T1 |
| R:R | `N.NN` | `|TP − entry| / |SL − entry|` if both exist; em-dash otherwise | T0 + T1 |
| Row click | event | toggles drill-down expansion (only one row expanded at a time) | — |

**Filter chips:** ALL · STRATEGY · MANUAL · LONG · SHORT (counts dynamic, updated each T0 tick).

**Sort:** default by absolute notional descending; click-to-sort post-v1.

---

## 13 · Row Drill-Down (per-position deep view)

Three columns inside the expanded row.

### 13a · Execution History

| Field | Computation | Tier |
|-------|-------------|------|
| Funding paid / cumulative | sum of `INCOME` events with `incomeType=FUNDING_FEE` for this symbol since position open | T2 |
| Entry slippage | `(entryPrice − signal_price) / signal_price × 10000` bps; only if signal price recorded by `daily_signal.py` | T0 + ledger |
| Trades vs avg | count of trades for this symbol since open from `/fapi/v1/userTrades` | T1 |
| MFE since open | `max((mark − entry) × dir / entry × 100)` over position lifetime, sampled at T0 | T0 + position-history |
| MAE since open | `min(...)` of the same series | T0 + position-history |
| Time at peak | timestamp at which MFE was reached | T0 + position-history |
| Bars in profit | T0 ticks where unrealizedProfit > 0 / total ticks since open | T0 + position-history |

### 13b · Market Context

Open Interest, OI Δ 24h, 24h Volume, 24h Change, Funding (8h + annualized), L/S Skew, Mark vs Index, L/S positioning bar — all from Binance public market-data endpoints (T2/T3).

24h Liq · L/S: **GAP** — see §15. v1 recommendation: drop entirely.

### 13c · Session Context

Strategy name, session ID, bar at entry, signal grade, conviction price, session exit time, time-to-exit countdown, force-close enabled flag. All from strategy session ledger. Session-controlled callout shown only when source ≠ MANUAL.

### Position-history table (§14 dependency)

Backend table populated from user-data stream `ORDER_TRADE_UPDATE` events:

```sql
position_history (
  symbol, position_id, opened_at, closed_at,
  entry_price, side, signal_session_id,
  signal_grade, signal_conviction_px,
  mark_at_open, mark_max_favorable, mark_max_adverse,
  ...
)
```

Required for: AGE column, MFE/MAE, time-at-peak, source attribution.

---

## 14 · Source Attribution Logic

Strategy vs manual classification.

```
source(position) =
  if position.symbol in active_strategy_session.symbols
     AND position.opened_at within active_strategy_session.window
     AND position.side matches active_strategy_session.signal[symbol]:
     return active_strategy_session.strategy_name  # e.g., "ALTS MAIN"
  else:
     return "MANUAL"
```

Active strategy session info comes from the `audit.jobs` / strategy execution ledger written by `trader-binance.py` (renamed from `trader-blofin.py`) at the start of each session.

**Edge cases:**
- Position opened as strategy, manually adjusted (size changed): keep strategy attribution, flag with `· MODIFIED`.
- Position closed by strategy then manually re-opened: classified by re-open event (manual).
- Multiple concurrent strategies: extend `source` to disambiguate (out of v1 scope).

---

## 15 · Gaps · Data Not Available from Binance

### 15a · Market Cap
Drop entirely for v1. Not a primary trading signal and adds dependency.

### 15b · 24h Liquidations (long/short)
Public liquidation orders WebSocket stream (`!forceOrder@arr`) was rate-limited starting June 2021 to push only one liquidation per second per symbol — making aggregated 24h totals unreliable from Binance alone.

Options: drop the fields (cleanest for v1), use Coinglass API (paid, reliable, sidecar service), or stream + aggregate yourself (lower-bound only).

**Recommendation v1:** drop. Revisit when Coinglass integration is justified by usage.

---

## 16 · Symbol Universe Verification

The mockup uses positions in BTC, SOL, AIXBT, BIO, CGPT, MEGA. Before transitioning to Claude Code, verify each is listed on Binance USDM Futures and store the canonical perpetual symbols. Run `GET /fapi/v1/exchangeInfo` and filter the returned symbols list at start of any session work.

---

## 17 · Chat Panel · Context Definition

The chat advertises the following context "chips" — these need to actually be loaded into the prompt context for the PM responses to be honest:

| Chip | Backing data |
|------|--------------|
| `OPEN POSITIONS` | live snapshot from §3–§4 above |
| `90D ACCOUNT HISTORY` | daily equity curve, daily PnL, position turnover from `account_snapshots` table |
| `LIVE FUNDING` | current funding rates for all open-position symbols |
| `OI · L/S SKEW` | current OI and L/S ratios for all open-position symbols |
| `24H LIQUIDATIONS` | only if §15b is implemented; otherwise drop chip |
| `TA · 1m–1d` | EMA distances at all timeframes (§11), regression slopes (§10) |
| `STRATEGY METADATA` | active strategy session info, recent session results, full strategy ledger access |

**Each chat turn should:**
1. Fetch fresh T0 data (positions, equity, marks).
2. Use cached T2/T3/T4/T5 data unless stale.
3. Be served the structured snapshot as a context block, not pre-summarized.
4. Be allowed to call out to the same endpoints if the user asks something the snapshot doesn't cover.

**Out of v1 scope but flagged:** the chat should be able to *act* — suggest "want me to set a stop on BTC?" and execute on confirmation. Not part of v1 build but data shape should support it.

---

## 18 · Empty / Loading / Error State Matrix

Per-section: empty has a domain-specific message ("No open positions" / "Need ≥ 2 positions" / etc); loading is skeleton bars at section dimensions; error shows last-known data with stale tint and red `STALE` tag.

Skeleton style: dim grey rectangles at section dimensions, no animation in v1 (defer pulse animations to polish phase).

---

## 19 · Open Decisions · Resolved at Build Time

1. **Alt-coin index proxy** — fixed list of 20 symbols defined in config, rebalance quarterly.
2. **Correlation window** — 30d × 1d for v1.
3. **EMA period** — fixed EMA20 for v1.
4. **Box plot window** — 24h × 5m bars (288 points).
5. **Today's PnL** — KPI strip uses UTC day; waterfall uses session window with explicit label.
6. **Refresh on screen unfocused** — throttle to 60s when `document.hidden`.
7. **Position-history persistence** — new table in user_mgmt schema (see CLAUDE.md venue split).
8. **Daily snapshot job** — runs at 00:00 UTC on the production host alongside `daily_signal.py`.

---

## 20 · Improvement Notes (Post-v1)

- **Note A · Liquidation distance** alongside SL distance in the Risk Signals card.
- **Note B · Period toggle** on waterfall and possibly KPI strip — TODAY · 7D · 30D · SINCE OPEN.
- **Note C · Hedge graph** — alternative to coverage matrix; deferred to v2.
- **Note D · Action-capable chat** — allow PM to propose and execute orders with confirmation.
- **Note E · Multi-venue support** — schema designed to absorb additional venues without migration.
- **Note F · Trade journey bar** per row — proportional spatial encoding of liq | SL | entry | mark | TP. Deferred from prior recommendation.
- **Note G · Intraday equity curve** — small chart in or near the page header showing equity path over the session.

---

*End of v1 data dictionary.*

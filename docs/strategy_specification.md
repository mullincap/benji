# Strategy Specification — Alpha Tail Guardrail

Canonical specification for the strategies published under
`mullincap.com`: **Alpha Low**, **Alpha Main**, and **Alpha Max** (collectively the
"Alpha Tail Guardrail family"). This document is the authoritative description of
what the strategy IS, what data it reads, what decisions it makes, what metrics
describe its performance, and under what conditions the specification changes.

If any implementation (live signal generator, audit pipeline, simulator candidate)
disagrees with this document, the implementation is wrong — not the document.
Commits that change the spec must update this file in the same PR.

---

## 1. Strategy definition

Three published strategies share one **underlying methodology** and differ only
in leverage configuration (`l_high`):

| Strategy | `l_high` | Canonical Sharpe | Canonical CAGR | Canonical Max DD |
|---|---|---|---|---|
| Alpha Low  | 1.0 | 3.4981 | +731.36% | −20.22% |
| Alpha Main | 1.5 | 3.4981 | +1957.42% | −29.23% |
| Alpha Max  | 2.0 | 3.4981 | +4534.46% | −37.55% |

Source: `audit.strategy_versions.current_metrics`, computed on the 432-day
walk-forward window 2025-02-13 → 2026-04-21.

Sharpe is leverage-invariant (same number for all three); CAGR and Max DD scale
non-linearly with `l_high` via compounding. The "Alpha Tail Guardrail"
methodology is the single object described below.

---

## 2. Canonical methodology

Every step below is a **binding element** of the spec. Changes require a
governance event (§5).

### 2.1 Basket selection — snapshot at 06:00 UTC

**Electoral window**: 00:00 UTC → 06:00 UTC of the deployment day.
**Anchor**: 00:00 UTC `close` (price) and `open_interest` (OI), per-symbol.
**Snapshot**: 06:00 UTC `close` and `open_interest`, per-symbol.

For each of the two metrics (`price`, `open_interest`), compute:

```
pct_change(symbol) = snapshot(symbol) / anchor(symbol) − 1
```

Hygiene: symbols with `anchor == 0` or `anchor is NULL` are dropped before
ranking (mirrors `pipeline/indexer/build_intraday_leaderboard.py:1043`
`anchor_vals.replace(0, pd.NA)` + `dropna`).

Rank all surviving symbols globally by `pct_change` (descending). Keep the top 20
per metric.

**Basket** = intersection of the two top-20 sets after normalization (§2.2).
Sorted by binance_id ascending for determinism.

### 2.2 Symbol normalization

Verbatim per `pipeline/overlap_analysis.py:normalize_symbol`:

1. Uppercase the raw binance_id; reject if non-ASCII.
2. Strip a trailing `_YYYYMM` date suffix (quarterly contract marker).
3. Strip a trailing `_PERP` suffix.
4. Strip the first recognized quote currency from the end of the string (first match wins): `USDT`, `USDC`, `USD`, `BUSD`, `BTC`, `ETH`, `BNB`. If no quote matches, the symbol is rejected.
5. Strip a numeric prefix (e.g., `1000PEPE` → `PEPE`).
6. Reject if the remaining string is empty.
7. Reject if it matches `NON_CRYPTO` ∈ {AMZN, TSLA, INTC, XAU, XAG, XPD, XPT, AAPL, GOOGL, MSFT, NVDA, META}.
8. Reject if it matches `STABLECOINS` ∈ {USDT, USDC, BUSD, TUSD, USDP, FDUSD, USDS, USDE, FRAX, DAI, PYUSD, USD1}.

### 2.3 Exchange-availability filter

After basket selection, drop symbols not in BloFin's USDT perpetual instrument
list (queried at generation time from
`GET https://openapi.blofin.com/api/v1/market/instruments?instType=SWAP`).

**Asymmetry note**: the audit pipeline does NOT apply this filter — the
published Sharpe 3.5 is computed on pre-filter baskets. In practice the BloFin
filter drops 0–2 symbols per basket (see 2026-04-22 example: MOG dropped, basket
went 7 → 6). This asymmetry is accepted and documented; §6 discusses.

### 2.4 Tail Guardrail filter

After the exchange filter, apply the Tail Guardrail sit-flat gate. Both sub-gates
are computed from BTC daily closes over the trailing 61 calendar days. If
either fires, sit flat today:

1. **Crash gate**: BTC prev-day simple return < −0.04 (−4%).
2. **Vol gate**: std(log returns, trailing 5 days) / std(log returns, trailing 60 days) > 1.4.

Sit-flat means the final basket is empty; no trades are entered. The guardrail
is evaluated from DB-sourced BTC data (`market.futures_1m`, daily rollup), not
from Binance REST.

### 2.5 Conviction gate at 06:35 UTC

Separate from basket selection. After the 06:00 basket is locked, the trader
monitors each symbol's 5-minute bar returns. At 06:35 UTC (end of the 7th
5-minute bar from 06:00), the aggregate `conviction_roi_x` is compared to the
`conviction_kill_y` threshold:

- `KILL_Y = 0.003` (0.3%)
- If `roi_x >= KILL_Y` → enter positions at `L_HIGH × vol_boost` leverage.
- If `roi_x < KILL_Y` → sit flat for the day.

`roi_x` is the portfolio-aggregate 06:00→06:35 return at the chosen leverage.
`vol_boost` is the current adaptive scalar published in
`audit.strategy_versions.current_metrics.vol_boost` — varies by volatility
regime, recomputed nightly.

### 2.6 Execution constants — position management

After entry, each open position is tracked against three exit gates. Source of
truth: `trader-blofin.py` header constants.

| Gate | Variable | Value | Semantics |
|---|---|---|---|
| Hard portfolio stop | `PORT_SL_PCT` | −0.06 (−6%) | Portfolio aggregate incremental return ≤ −6% → close all (unleveraged, measured from entry) |
| Trailing portfolio stop | `PORT_TSL_PCT` | −0.075 (−7.5%) | Portfolio incremental return from peak ≤ −7.5% → close all |
| Profit-take / early-fill | `EARLY_FILL_Y` | 0.09 (9%) | Portfolio aggregate return from session open ≥ 9% → close all |

Per-symbol sub-stop: individual symbol whose return ≤ `PORT_SL_PCT` is closed
immediately and clamped at −6% in the portfolio aggregate.

### 2.7 Fee model

Applied in the audit simulation and in the live trader's accounting:

- **Taker fee**: 0.0008 per side × 2 sides = 0.0016 round-trip (actual: 0.04% × 2 = 0.08%).
- **Funding**: 0.0002 daily (0.01% × 2 standard funding windows per 24h).
- Both are scaled by `lev_used` (effective leverage at the time of execution).

### 2.8 Leverage

- **Base**: `LEVERAGE = 4.0` (pivot_leverage, applied to unleveraged stops).
- **Strategy `l_high`**: Alpha Low = 1.0, Alpha Main = 1.5, Alpha Max = 2.0. Multiplier on top of `LEVERAGE`.
- **`vol_boost`**: adaptive scalar (0.8–2.0 range), recomputed nightly from the trailing 10-day portfolio volatility. Applied at entry as `eff_lev = LEVERAGE × l_high × vol_boost`.

---

## 3. Canonical data sources

| Data | Source | Frequency |
|---|---|---|
| 1-minute bars (close, volume, open_interest) | `market.futures_1m` (from Amberdata via `metl.py`) | 1 min, populated continuously |
| Leaderboards (pre-ranked pct_change per metric) | `market.leaderboards` (built by `build_intraday_leaderboard.py` at 01:00 UTC) | Daily, covers day D−1 |
| Market caps | `market.market_cap_daily` (CoinGecko via `coingecko_marketcap.py`) | Daily |
| Exchange instrument list | BloFin REST (`/api/v1/market/instruments`) | Fetched at each signal generation |
| BTC daily closes | Derived from `market.futures_1m` | — |

**Canonical reference implementation** for basket selection: the audit pipeline
`pipeline/overlap_analysis.py --mode snapshot --leaderboard-index 100 --freq-width 20`. Any
live signal generator MUST produce the same basket (modulo the documented
exchange-availability filter in §2.3) as this reference for any given date.

### 3.1 Methodology-exploration CLI flags

Two CLI flags on `pipeline/overlap_analysis.py` support methodology exploration and
are NOT interchangeable:

**`--live-parity`** reproduces `daily_signal.py` v1 exactly, including its
*asymmetric* ranking (log-return on price via `log(close/anchor)`, absolute-$
on OI via `oi_usd - anchor_oi_usd`). This is a forensic reproduction tool;
the −0.55 Sharpe recorded in § 11.2 was measured against this configuration
and is reproducible via `overlap_analysis.py --live-parity --source db`.

**`--ranking-metric {pct_change, abs_dollar}`** applies a *symmetric* ranking
across both price and OI (both use the same formula), operating on the
canonical universe (no v1-specific volume or BloFin filters). This is an
exploratory tool for testing candidate methodology variations under § 5
governance rules. `pct_change` (default) reads `market.leaderboards`;
`abs_dollar` reads raw values from `market.futures_1m` and ranks on-the-fly.

**The two flags are mutually exclusive.** argparse errors with a clear
message if both are set. Combining them would conflate "reproduce v1"
(asymmetric) with "explore symmetric variants" (symmetric) and produce
nonsense.

---

## 4. Evidence supporting canonical choice

### 4.1 Provenance

1. **22 audit jobs behind published Sharpe 3.5**: every single `audit.jobs` row linked to the three active published strategy versions (Alpha Low, Alpha Main, Alpha Max) across 2026-04-17 → 2026-04-22 shows identical `config_overrides`: `mode=snapshot, leaderboard_index=100, sort_by=price, freq_width=20, sort_lookback=6, min_mcap=0.0`. No exceptions.
2. **`overlap_analysis.py` canonical run command**: the header docstring of `pipeline/overlap_analysis.py` has contained the same `CANONICAL RUN COMMAND` since the initial commit `5c72e62` (2026-03-31), explicitly using `--mode snapshot`.
3. **Nightly refresh cron**: `backend/app/cli/refresh_strategy_metrics.py` invokes `run_audit(params)` where `params` comes from each `audit.strategy_versions.config` JSONB; all three published versions carry `mode=snapshot`.

### 4.2 Performance evidence (Part 1b comparison audit, 2026-04-22)

Using `pipeline/overlap_analysis.py --live-parity` (shipped in commit `e0b1dd4`) to
simulate an alternative methodology matching `daily_signal.py` v1's implementation
(frequency mode + absolute-$ OI delta + top-100-by-volume universe):

| | Canonical (snapshot) | Live-parity (v1-style) |
|---|---|---|
| Sharpe (Alpha Low, 432-day) | **3.4981** | **−0.5513** |
| CAGR | +731% | −36.9% |
| Max DD | −20% | −66% |
| Total Return | — | −36.5% |
| Active trade days | — | 102 / 433 |

Delta: −4.05 Sharpe points. Thirteen-point-five times the pre-committed 0.3 measurement-noise
threshold (Part 1b caveat). Live-parity method is net-negative EV over the full window;
canonical method is the one that produces the published metric.

### 4.3 Verification of canonical reproducibility

`daily_signal_v2.py` (shipped `ba2bd41`) computes canonical baskets on-the-fly
from `market.futures_1m` and was cross-validated against `market.leaderboards`
for 2026-04-22:

- Price top-20 via on-the-fly computation: 20/20 match to `market.leaderboards`.
- OI top-20 via on-the-fly computation: 20/20 match to `market.leaderboards`.
- Final intersection basket (pre-BloFin): 7/7 identical.

`shadow_diff.py` cross-check for the same day (2026-04-22): `J(v2, leaderboard_DB) = 1.000`
post-BloFin-filter. Two independent implementations, bit-identical output.

---

## 5. Change-control policy

A candidate methodology may replace canonical ONLY if it satisfies ALL:

### 5.1 Performance criteria (all three must hold)

Over the full available walk-forward window (currently 432 days; always at
least 365 days at evaluation time):

1. **Candidate Sharpe > canonical Sharpe** by ≥ 0.3 points.
2. **Candidate CAGR > canonical CAGR** (in percentage points).
3. **Candidate Max DD not materially worse than canonical Max DD** — defined as within 10% relative (e.g., canonical −20.22% → candidate may not exceed −22.24%).

All three metrics computed with identical fee model, execution constants, and
conviction gate (§2.5-2.7). Only the basket-selection methodology varies
between candidate and canonical.

### 5.2 Governance event

Promotion of a candidate to canonical requires:

1. **Audit comparison table** posted in the promotion commit message or linked
   doc, showing candidate vs canonical across Sharpe, CAGR, Max DD, Total Return,
   Active Days, Worst Day / Week / Month, per-filter metrics for all supported filters.
2. **This spec doc (`docs/strategy_specification.md`) updated** in the same commit as the
   methodology change. § 2 updated to describe the new canonical;
   § 11 (incident log) appended with the promotion reason and evidence.
3. **`daily_signal_v2.py` updated** in the same commit to produce the new canonical
   basket. If the methodology change is non-trivial, a new daily_signal_vN.py
   ships alongside v2 and goes through the shadow process below (§7).
4. **Allocator notification**: every user with active capital in the affected
   strategy receives a notification (in-app toast + email) before promotion
   takes effect in production cron. 24h notice minimum.

### 5.3 Evidence tiers for candidate evaluation

| Tier | Evidence | Implication |
|---|---|---|
| Single 432-day backtest | Required baseline. | Not sufficient alone — see §6.1. |
| Out-of-sample (held-out tail segment) | Strongly recommended. | A candidate that beats canonical in-sample but regresses out-of-sample is rejected. |
| Regime-decomposed (monthly breakdown) | Required. | Candidate must not catastrophically underperform in any single month. |
| Ensemble robustness | Optional. | Noise-stability, signal-shuffle, ranking-noise tests: bonus evidence. |

---

## 6. Known unknowns

### 6.1 Canonical may reflect a lucky configuration

No written research record compares `(snapshot, pct_change, leaderboard-index-100)` to
alternative configurations and concludes it was the winning choice. The
methodology pre-existed the git history (initial commit, 2026-03-31, already
contained `--mode snapshot` as the `CANONICAL RUN COMMAND`). Its 3.5 Sharpe on
432 days of prior data is NOT a priori evidence that the same configuration
will perform identically going forward.

**Implication**: the Simulator (UI + `pipeline/overlap_analysis.py --live-parity` +
candidate modes) should be used ongoing to stress-test canonical against
variants. The ≥ 0.3 Sharpe margin in §5.1 is calibrated against measurement
noise, not model uncertainty.

### 6.2 Exchange-availability asymmetry

The audit computes Sharpe 3.5 on PRE-BloFin-filter baskets. Live execution
applies the filter post-basket, dropping 0–2 symbols per basket typical. True
live Sharpe (with the filter applied) may be 0.1–0.3 Sharpe points below
published. Current treatment: accept the asymmetry, document it in this spec.
Escalation path: if a credible estimate of the filter's Sharpe impact emerges,
update published metrics to "BloFin-filtered Sharpe" and note the pre-filter
Sharpe as an upper bound.

### 6.3 BloFin instrument-list membership drift

The BloFin USDT perp list changes over time (listings, delistings). `shadow_diff.py`
and live use today's snapshot. Historical backtests use today's snapshot applied
retroactively (per Part 1b Gap B, <2-3 symbol/day lookback error). Acceptable for
directional decisions; for landing-page-grade precision, would need a
historically-correct BloFin membership table.

### 6.4 `market.futures_1m` open_interest quality

Per 2026-04-22 investigation: a subset of symbols (4–30 per day, average ~18)
have `open_interest = 0` stored at exactly 00:00 UTC when adjacent minutes
contain normal values. The leaderboard builder drops these via
`replace(0, pd.NA) + dropna` so canonical output is unaffected. Any new consumer
of raw `futures_1m` must apply the same hygiene. See incident log §11 entry
dated 2026-04-22 for full diagnosis.

---

## 7. Migration: v1 (legacy) → v2 (canonical) — COMPLETED 2026-04-23

**Status**: v2 is the LIVE signal generator as of 05:58 UTC 2026-04-23.
Cutover executed in the 2026-04-23 session; the 7-day shadow-gate specified
in §7.2 below was **deliberately bypassed** after a 1-day smoke test
(n=1 evidence) on the grounds that continuing v1 (Sharpe −0.55, net-negative
EV per §11.2) carried more risk than accepting a reduced-evidence cutover.

### 7.1 Post-cutover state (as of 2026-04-23)

| Component | Cron | Status |
|---|---|---|
| v2 daily_signal_v2.py | **05:58 UTC daily** (replaces v1's slot) → live_deploys_signal.csv + user_mgmt.daily_signals DB (3 rows/day, one per Alpha Low/Main/Max) | **LIVE** |
| v1 daily_signal.py | commented in crontab, archived to `/root/benji/archive/daily_signal_v1_archived_2026-04-23.py` + repo `archive/daily_signal_v1_host_snapshot_20260423.py` | DISABLED |
| shadow_diff.py | 06:15 UTC daily → daily_signal_shadow_diff.log + history CSV | ACTIVE (continues as regression detector — v2 vs canonical leaderboards cross-check) |

v2's `write_to_db` function was ported byte-for-byte from host-v1's
multi-version DB-write logic (commit `e1522db` applied to host 2026-04-20 but
never propagated to repo; host snapshot committed as
`archive/daily_signal_v1_host_snapshot_20260423.py` and ported into v2 via
the cutover commit). v2 reads raw values from `market.futures_1m` at 06:00 UTC
and computes pct_change snapshot rankings on-the-fly (same math as the
leaderboard builder), so canonical basket is available ~2 minutes before
the 06:05 UTC trader-spawn cron reads from `user_mgmt.daily_signals`.

### 7.2 Cutover gate

**Pass**: `J(v2, canonical) ≥ 0.95` on **≥ 7 consecutive days**, AND
daily cross-check `J(v2_yest, leaderboard_DB_yest) ≈ 1.000` throughout.

**Reset**: any day violating the 0.95 threshold restarts the counter. Below-0.95
means v2 drifted from canonical — investigate before resuming shadow.

### 7.3 Cutover execution (when gate passes)

1. Confirm ≥ 7 consecutive PASS days in `daily_signal_shadow_diff_history.csv`.
2. Disable v1 cron (`crontab -e`, comment out the 05:58 UTC line with `# DISABLED YYYY-MM-DD` prefix).
3. Rename v2 cron target: install a new entry invoking `daily_signal_v2.py` at 05:58 UTC (same time v1 ran).
4. Remove v2 shadow cron (06:02 UTC).
5. Keep shadow_diff running at 06:15 UTC — useful as a regression detector indefinitely (still compares v2 live baskets vs canonical DB).
6. Archive `daily_signal.py` → `archive/daily_signal_v1_archived_YYYYMMDD.py`.
7. Update this spec § 7: change "Migration status" → "Migration complete YYYY-MM-DD".
8. Notify allocators of the methodology correction (per §5.2.4).

### 7.4 Rollback plan

If cutover produces unexpected live behavior:

1. Restore v1 cron (uncomment the disabled line).
2. Disable v2 cron (comment out the 05:58 UTC line for v2).
3. Keep collecting shadow_diff for diagnosis.
4. Investigate and resolve before retrying cutover.

---

## 8. Simulator and governance

The Simulator (`/simulator` UI + `POST /api/simulator/audits/{job_id}/promote`)
is the governance gate for canonical changes:

- Candidates are submitted via the Simulator UI with config parameters (mode, metric, universe size, filters, leverage, etc.).
- `/api/simulator/audits/{job_id}/promote` writes a new `audit.strategy_versions` row tagged as inactive by default.
- Promotion of an audited candidate to "production canonical" is GATED by §5.2 governance requirements and is NOT currently automated — see Stream D (`docs/open_work_list.md`) for the planned "Compare to canonical" UI.

---

## 9. Related documents

- `docs/open_work_list.md` — active work items including the Simulator compare-to-canonical UI (Stream D).
- `pipeline/overlap_analysis.py` — canonical audit pipeline.
- `daily_signal_v2.py` — canonical live signal generator.
- `shadow_diff.py` — canonical/v2/v1 daily diff.
- `backend/app/cli/refresh_strategy_metrics.py` — nightly audit refresh cron.

---

## 10. Implementation references

Current implementations producing canonical output (all must agree):

1. `pipeline/overlap_analysis.py --mode snapshot` (audit pipeline; source of published metrics).
2. `daily_signal_v2.py` (live signal generator after cutover §7.3).
3. `shadow_diff.py::compute_canonical_from_futures_1m` (daily shadow-diff tool).

If any two of the above produce different baskets for the same day (modulo the
documented BloFin-filter asymmetry §2.3), at least one has a bug. Standing
cross-check:`shadow_diff.py` logs `J(v2, leaderboard_DB)` daily; any drift from
1.000 fires a cutover-gate counter reset.

---

## 11. Incident log

### 11.1 Methodology drift discovered (2026-04-22)

`daily_signal.py` (v1, live) and `pipeline/overlap_analysis.py` (audit, producing
published Sharpe 3.5) were operating on **different methodologies** despite
sharing the strategy name "Alpha Tail Guardrail". Root cause: v1 was written
as a standalone Binance REST-based signal generator; author believed it
reproduced the backtest ("matches backtest exactly" comment at
`daily_signal.py:245`), but no parity test existed.

Divergence axes:
1. **Selection mode**: v1 used frequency count over 72 bars; canonical uses 06:00 UTC snapshot.
2. **Ranking metric**: v1 used absolute USD-$ OI delta; canonical uses pct_change from the 00:00 UTC anchor.
3. **Universe**: v1 used top-100 by 24h quoteVolume filtered to BloFin; canonical uses the 333-symbol pre-ranked leaderboard filtered by `rank ≤ 20` post-pct_change ranking.

### 11.2 Part 1b comparison audit (2026-04-23)

Ran `pipeline/overlap_analysis.py --live-parity` (new CLI flag, commit `e0b1dd4`) against the
full 432-day walk-forward window for the "A - Tail Guardrail" filter. Result:

- Canonical Sharpe: 3.4981
- V1-methodology Sharpe: **−0.5513**
- Delta: −4.05 Sharpe points

V1 methodology backtests to net-negative EV over 432 days. Live recent-history
positive returns (~2 weeks of real trading) are not statistically distinguishable
from noise given the measured edge. See commit messages on `e0b1dd4` and `0202cb7`
for pipeline changes enabling the comparison audit.

### 11.3 Migration plan execution

- **2026-04-23** commit `e0b1dd4`: add `--live-parity` preset to `overlap_analysis.py` (comparison-audit capability).
- **2026-04-23** commit `0202cb7`: forward `live_parity` param through `backend/app/services/audit/pipeline_runner.py` → `build_cli_args`.
- **2026-04-23** commit `ba2bd41`: add `daily_signal_v2.py` implementing canonical methodology for shadow-run.
- **2026-04-23** commit `8c32706`: align v2 CSV format with v1 for drop-in cutover.
- **2026-04-23** commit `d1fa68a`: add `shadow_diff.py` for daily basket-diff + cutover-gate tracking.
- **2026-04-23** (ops): install host cron entries for v2 (06:02 UTC) and shadow_diff (06:15 UTC). Backup at `/root/crontab_backup_20260423_015430.bak`.
- **2026-04-23** commit `9a89749`: this spec doc (§1-11).
- **2026-04-23** commit `bd356c3`: snapshot host-v1 → `archive/daily_signal_v1_host_snapshot_20260423.py` (pre-cutover artifact).
- **2026-04-23** (ops ~05:20 UTC): cutover executed. Shadow-gate bypass deliberate (1-day smoke test evidence; continuing v1 net-negative EV judged higher risk). Crontab changes applied, v1 archived on host to `/root/benji/archive/daily_signal_v1_archived_2026-04-23.py`.

### 11.4 Shadow-gate bypass record (2026-04-23 cutover)

The §7.2 cutover-gate requirement (J(v2, canonical) ≥ 0.95 for ≥7 consecutive
shadow days) was **not satisfied** at cutover time. Bypass justification:

- v2 shadow cron installed at 02:00 UTC, first run would have been 06:02 UTC
  (so only n=0 shadow days at cutover decision time).
- v1 known to run a methodology that backtests at Sharpe −0.55 over 432 days
  (per § 11.2) — continuing v1 one more trading day carries known negative EV.
- Evidence substituting for the 7-day gate:
  1. 2026-04-22 one-day smoke test: v2 basket (7 symbols pre-BloFin, 6 post)
     exactly matches `market.leaderboards` direct query (20/20 price top-20 +
     20/20 OI top-20 + 7/7 intersection).
  2. v2 on-the-fly `futures_1m` computation is mathematically equivalent to
     the builder's output (verified via shadow_diff lagged cross-check path).
  3. Port-source integrity: `write_to_db` was ported byte-for-byte from
     host-v1 (which has been writing the same DB rows daily for ≥3 days
     without issue).
- Rollback path: `crontab /root/crontab_backup_cutover_20260423_043317.bak`
  restores the exact pre-cutover crontab state; v1 is preserved in
  `/root/benji/archive/` for re-activation in <5 minutes if needed.

If cutover produces a regression, rollback fires + this spec section gets
updated with the failure mode + a revised gate-satisfaction plan for retry.

### 11.4 Related data-quality finding

Per-symbol `open_interest = 0` at 00:00 UTC in `market.futures_1m` for 4–30 symbols/day.
Root cause: unresolved (likely an Amberdata boundary artifact). Mitigated at the
leaderboard-builder layer (`replace(0, pd.NA) + dropna` hygiene, line 1043). Queued
as low-priority cleanup — audit and v2 both defensively handle it. See
`docs/open_work_list.md` §"metl.py ingestion fix".

---

*Last updated: 2026-04-23. Next review: after shadow-period cutover executes, or on the first §5 governance event, whichever comes first.*

# Session Handoff — 2026-04-19 (end of Track 3 Session A + Item 10)

## Session update

Session extended past original Session A scope. Items 5, 4, AND 10 shipped. Item 10's investigation and implementation were completed in the same session after Session B decisions were pre-committed. Remaining Track 3 work: Item 6 (VOL boost), Item 9 (Binance margin executor, with Item 10 end-to-end gate as hard prerequisite).

## Tomorrow's first action

1. **Verify the 01:30 UTC nightly cron tick cleared clean.**
   ```bash
   ssh mcap 'tail -50 /mnt/quant-data/logs/trader/refresh_metrics.log'
   ssh mcap 'docker exec -i timescaledb psql -U quant -d marketdata -c "
     SELECT version_label, metrics_updated_at, metrics_data_through
     FROM audit.strategy_versions WHERE is_active = TRUE;"'
   ```
   Expected (per Item 4 post-deploy pattern): log shows `ok=1 fail=0` ~01:37 UTC, `metrics_updated_at` bumped to ~01:37 UTC 2026-04-20, `metrics_data_through` advanced to 2026-04-19 if today's data is in, else stays 2026-04-18. Preview values may differ as window extends; shape must stay 30 keys.

2. **If green:** proceed to Track 3 Session B per scope below. If red: pause Session B, debug the Item 4 refactor's production behavior first.

---

## What shipped today (Session A)

### Item 5 — IDENTITY_FIELDS convention sweep

Status: **COMPLETE, zero mismatches found.**

Commit: `9b14233` (docs only — `docs/deferred_work.md` entry for `active_filter` string-namespace issue).

No code changes. Classification:
- 6 fields consumed by per-allocation executor — all match sign/magnitude convention. port_tsl mismatch already normalized at `trader_config.py:126` via `-abs()`.
- 6 signal-generation fields (tail_*, dispersion_*, freq_*) — N/A (produced by host master trader, not per-allocation).
- 6 VOL boost fields — N/A today (Item 6 will consume; conventions would match when wired).

Adjacent finding filed to `docs/deferred_work.md`: `active_filter` string-namespace inconsistency between `audit.strategies.filter_mode` label form ("A - Tail Guardrail"), host master `ACTIVE_FILTER = "Tail Guardrail"`, and factory default. Alpha v1 config JSONB lacks the key → factory fallback happens to align with `daily_signals.filter`. Next Simulator-UI promotion would persist the label form and silently break signal matching. **Tracked for its own session; Item 4 confirmed it does not touch the promote-path write site.**

### Item 4 — `audit.py` refactor to `run_audit(params) → dict`

Status: **COMPLETE, verified across parse identity + both orchestration paths.**

Commit: `1199708` (refactor).

Components:
- **New file**: `backend/app/services/audit/metrics_parser.py` — 580 LOC. Holds 29 regex constants + 6 helpers + `parse_metrics(path) → dict`. Logic identical to pre-refactor `_parse_metrics`.
- **Modified**: `backend/app/services/audit/pipeline_runner.py` — added `run_audit(params, *, output_path, progress_cb=None, cancellation_cb=None, on_rebuild_start=None) → dict`. Composes `build_pipeline_env` + `prestage_parquet` + `run_audit_subprocess` + `parse_metrics`.
- **Modified**: `backend/app/workers/pipeline_worker.py` — 795 → ~200 LOC. `run_pipeline` delegates to `run_audit()`.
- **Modified**: `backend/app/cli/refresh_strategy_metrics.py` — `_run_audit_for_version` shrinks ~40 LOC → ~10.
- **Modified**: `backend/app/api/routes/jobs.py` — import `parse_metrics` from new module.

Net: **−536 LOC**.

Verification:
- **Parse identity** ✅ — `parse_metrics(path)` byte-identical to pre-refactor `_parse_metrics(path)` for both saved baselines (Path A: existing Simulator job `044c17d9`, Path B: fresh refresh CLI run at 23:04 UTC). 30 keys each.
- **Orchestration identity — nightly CLI** ✅ — re-run at 23:54 UTC produced preview (`total_return_pct=415.7392, sharpe=3.5106, cagr_pct=2001.2918, max_dd_pct=-29.2286`) and 30-key metrics dict byte-identical to Path B baseline.
- **Orchestration identity — Simulator** ✅ — J triggered two concurrent UI audits:
  - `31c1f050-d20c-4cdf-b766-453e433a4b9b` (l_high=0.88 test): complete, 30 keys, `best_filter="A - Tail Guardrail"`, grade C+ (92.0).
  - `46b1e583-4a53-4d43-9cd1-a3716ba28591` (l_high=2.3 alpha v1 prod): complete, 30 keys, `best_filter="A - Tail Guardrail"`, grade C+ (93.0).
  - Bonus: concurrent execution exercised job isolation on new `run_audit()` path — no cross-contamination.

Accepted behavior differences (called out at draft time, approved):
- Prestage errors in Simulator path now caught as `stage="overlap"` failed rather than bubbling raw (closes an orphan-job scenario).
- `stage="overlap" progress=90` + `stage="parsing" progress=95` bumps removed; parse is sub-second so UX flashes were invisible anyway.
- `_is_cancelled` check between parse and `results={}` dropped — cancellation during sub-second parse window now completes as normal.

### Item 10 — Per-allocation capital sizing

Status: **COMPLETE at the code level, partially validated (see gaps below). Hard integration gate to clear before Item 9 activates live BloFin deploys.**

Commit: `5a7bdc7` (single file: `backend/app/cli/trader_blofin.py`, +123 / -10 LOC).

Components added:
- `_account_lock_key(connection_id) -> int`: SHA-256 of UUID → signed int64 for `pg_advisory_lock` keys.
- `_account_advisory_lock(connection_id)`: session-level lock context manager. Acquired via `pg_advisory_lock`, released via `pg_advisory_unlock`, auto-released on subprocess death via DB session drop.
- `_compute_allocation_capital(allocation_id, requested, balance) -> tuple[float, str | None]`: pure helper returning `(usdt_for_allocation, warning_msg_or_None)`. Caller owns `log.warning()` — helper returns the message string for unit testability.
- `_run_fresh_session_for_allocation`: new `connection_id: str` parameter; Phase 5 rewritten to wrap balance-read + sizing + enter_positions inside the advisory lock; sizing routed through `_compute_allocation_capital`.
- `TRADER_LOCK_TEST_SLEEP_S` env-var scaffolding: dry-run-only, holds the lock for a specified duration after balance read. For operator concurrency tests. **Do NOT `export` in shell** — prefix only to the leading process; subprocesses must not inherit.
- `run_session_for_allocation` call site passes `connection_id=str(alloc["connection_id"])` through.

Decisions (Session B pre-commits, ratified this session):
- **10.1 (α)**: `allocations.capital_usd` stays NOT NULL at schema; no NULL fallback branch in code.
- **10.2**: if `capital_usd > available_balance`, size down to available balance + emit WARN, continue (not abort).
- **10.3 (b)**: PostgreSQL advisory lock on `connection_id` serializes critical section. Earlier allocation (spawn_traders' `created_at ASC`) acquires first; later allocation blocks until release, then reads post-deploy balance and sizes down as needed.
- **10.4**: `capital_usd` mutable — fresh read each session via existing `_fetch_allocation` call in `run_session_for_allocation`.

Legacy untouched:
- Master `run_session()` legacy path in the containerized copy.
- Module constants `CAPITAL_MODE="pct_balance"` / `CAPITAL_VALUE=1.0` (now dead in per-allocation path; cleanup filed in `deferred_work.md`).
- Host master `/root/benji/trader-blofin.py` (guardrail honored).

#### Verification — Path 1 harness (pre-commit, on deployed image equivalent)

Harness at `/tmp/item10_verify.py` (not committed; ephemeral). SHA-256 bit-identity verified between local working copy and container-staged copy before harness run (local = `d0aaf495640bf9f7fc3031b8e8199857d688017b034808dedf3a2975e3a45e3e` = container).

Verbatim harness output:

```
[2026-04-20 03:04:34,562] INFO [1] B denied while A holds lock (expected).
[2026-04-20 03:04:34,564] INFO [1] B acquired after A released (expected). PASS.
[2026-04-20 03:04:36,584] INFO [2] A held lock 2.002s; B waited 1.817s; B acquired 0.3ms after A released.
[2026-04-20 03:04:36,584] INFO [2] Blocking acquire works; first-come-wins verified. PASS.
[2026-04-20 03:04:36,624] INFO [3] Inside context: external try_advisory_lock denied (expected).
[2026-04-20 03:04:36,646] INFO [3] After context exit: lock released (expected). PASS.
[2026-04-20 03:04:36,646] INFO [4.1] $1 < $3951 -> usdt=$1, no warn. PASS.
[2026-04-20 03:04:36,646] INFO [4.3] $5000 > $3951 -> usdt=$3951, warn emitted. PASS.
[2026-04-20 03:04:36,646] INFO [4.edge] $3951 == $3951 -> usdt=$3951, no warn. PASS.
[2026-04-20 03:04:36,646] INFO ====== ALL PASSED ======
```

Test coverage map:
- `test_1_lock_mutex` — `pg_try_advisory_lock` denies while A holds lock; succeeds after A releases. ✅
- `test_2_lock_blocking` — A holds 2.0s; B's blocking acquire waited 1.817s, acquired 0.3ms after release. First-come-wins + prompt wake verified. ✅
- `test_3_context_manager` — `_account_advisory_lock` round-trip: inside-context external try denied; post-exit external try succeeds. ✅
- `test_4_sizing` — real `_compute_allocation_capital` against three cases: under (no warn), over (warn emitted with correct text), equal (no warn, takes `requested` branch). ✅

#### Post-deploy smoke (on rebuilt image)

```
imports: OK
_run_fresh_session_for_allocation params: ['allocation_id', 'config', 'api', 'connection_id', 'dry_run']
sizing smoke: usdt=3.0 warn-starts='Allocation post-deploy-smoke: requested '
lock key stable: -4881515115874462459
```

All Item 10 symbols import cleanly from the redeployed image; signature includes the new `connection_id` parameter in the expected position; sizing helper produces expected `(usdt, warning)` tuple; lock-key helper deterministic on deployed code.

#### NOT validated by this session's harness — Item 9 pre-deploy gate

> **Expanded 2026-04-20 (Session C, Item 6):** gate now covers FOUR integration paths, not three. Path 4 added below.

Four integration paths untouched by commit-time verification:
1. **`connection_id` threading** through `run_session_for_allocation` → `_run_fresh_session_for_allocation` → `_account_advisory_lock` (real-CLI call sequence, not harness-synthesized). [Item 10]
2. **`TRADER_LOCK_TEST_SLEEP_S` activation** inside the live CLI flow (the env-var scaffolding lives inside Phase 5 after the lock is acquired; harness tests the lock directly, never reaching Phase 5 via CLI). [Item 10]
3. **Phase 5 integration** with surrounding CLI phases — signal load, conviction check, credential load, monitoring loop handoff. [Item 10]
4. **Phase 4 `vol_boost` read + log format** — caller-side read of `strategy_version.current_metrics["vol_boost"]` threading through `_run_fresh_session_for_allocation`'s new `vol_boost: float` param, producing the `l_high × vol_boost = eff_lev` log line. Item 6 validated the refresh-side (vol_boost written to current_metrics for all three alpha strategies); the read-side was smoke-tested via signature introspection only, not exercised end-to-end because today's preflight had no active BloFin allocations and signals aren't written until 05:58 UTC. [Item 6]

These four gaps close only when a real BloFin allocation activates with today's signals present AND conviction passing. Today's preflight showed `conviction_passed=false` on the last 3 days of `daily_signals` (and no active BloFin allocations), so no real-signal exercise was available during Sessions B or C.

**Gate to enforce when Item 9 ships:** before any live BloFin allocation deploys under Item 9, exercise Items 10 + 6 end-to-end with real signals, specifically covering the four gaps above. Document results in that session's handoff. This is a HARD prerequisite, not a suggestion.

---

## Track 3 Session B — scope + pre-committed decisions

### Session B items (remaining)

- ~~**Item 10**~~ — SHIPPED this session (`5a7bdc7`). See "Item 10 — Per-allocation capital sizing" section above for full validation status + Item 9 pre-deploy gate.
- **Item 6** — VOL boost publication (original Session B item, unchanged).

Item 10's historical investigation notes + decisions are preserved below for reference; they're no longer action items.

### Item 10 — decisions pre-committed (historical; shipped this session)

> Preserved for reference. Decisions were ratified and implemented in commit `5a7bdc7`.

**Decision 10.1 revision**: schema **Option α** locked.
- `user_mgmt.allocations.capital_usd numeric(18,2) NOT NULL` stays as-is.
- Drop the "NULL → 90%-of-balance fallback" branch from the original decision. `capital_usd` is always present (enforced at schema); no NULL code path.

**Decision 10.3 reinforcement**: ordering **Option (b)** locked.
- PostgreSQL advisory lock around `get_account_balance_usdt` + `enter_positions`, keyed on user_id or connection_id. Preserves parallel-spawn architecture, eliminates the balance-read race, minimal LOC.
- Reject (a) "accept the race" on live-money grounds.
- Reject (c) "serial spawn" — throws away parallelism unnecessarily.

**Scenario teardown protocol** (originally planned; superseded by Path 1 harness): dedicated scratch user with a known `user_id`. Allocations inserted for scenario testing, soft-closed via `status='closed'` afterward. No DELETE — preserves no-data-deletion guardrail, re-runnable. Not executed because preflight surfaced two blockers: (a) today's `daily_signals` not yet written (master signal runs at 05:58 UTC), (b) last 3 days' `conviction_passed=false` on Tail Guardrail filter. Path 1 harness (DB-level lock primitives + real sizing helper) substituted; integration gaps deferred to Item 9's pre-deploy gate.

### Item 10 — known code sites (from Session A investigation)

**Capital-sizing read sites in `backend/app/cli/trader_blofin.py`:**

| Line | Site | Action for Item 10 |
|---|---|---|
| 126–127 | Module constants `CAPITAL_MODE="pct_balance"`, `CAPITAL_VALUE=1.0` | Keep as legacy defaults (host-master-parity — unused in per-allocation path after fix). |
| 1075 | `enter_positions(..., balance, dry_run)` signature | Caller will pass per-allocation capital here (not full balance). |
| 1093–1094 | `usdt_total = balance * CAPITAL_VALUE if ...` | Reinterpret: `balance` param becomes "capital allocated" (already scoped). |
| 2977 | `balance = get_account_balance_usdt(api)` in `_run_fresh_session_for_allocation` | Wrap in advisory lock + size-down-with-warning logic. |
| 2987 | `enter_positions(api, ..., balance, dry_run)` | Pass `usdt_for_allocation` (computed from `config.capital_value` vs account_balance) instead of full balance. |

**Sketch of the per-allocation sizing block** (Session B starting point, not final):
```python
# Phase 5: balance + enter positions (new: advisory-locked + per-allocation)
with _account_advisory_lock(connection_id):  # new helper
    account_balance = get_account_balance_usdt(api)
    requested = float(config.capital_value)   # = allocations.capital_usd
    if requested > account_balance:
        log.warning(
            f"Allocation {allocation_id}: requested ${requested:,.2f} exceeds "
            f"available ${account_balance:,.2f} — sizing down to ${account_balance:,.2f}"
        )
        usdt_for_allocation = account_balance
    else:
        usdt_for_allocation = requested
    positions, fill_report = enter_positions(
        api, inst_ids, entry_prices, eff_lev, usdt_for_allocation, dry_run,
    )
```

### ⚠ Pre-diff callout for Session B

**The containerized `enter_positions` currently deploys 100% of account balance, not 90%.**

The kickoff prompt for Item 10 assumed a "90% of full account balance" default. The code uses `CAPITAL_MODE="pct_balance"` with `CAPITAL_VALUE=1.0`, which is 100% of balance. The 90% that J mentioned is after the internal `MARGIN_BUFFER=0.10` applied to `usdt_deployable` (line 1120), which yields ~90% of the chosen-capital figure — not 90% of account balance.

Before Session B writes the diff: confirm what any currently-live allocation is actually receiving. If a live allocation is running (see `allocation_returns` history) and its deployed capital is 100% of balance (pre-buffer) × 90% (buffer) = 90% of balance, then the kickoff's "90% default" is already accurate — it's a description of the pre-buffer × buffer product, not a CAPITAL_VALUE of 0.9. In that case Item 10 is purely a sizing-source change (full_balance → per-allocation), no 100%-vs-90% drift exists.

If live allocations exist and deployed capital is unexpected, resolve that first before Session B's diff.

As of handoff writeup: **zero active allocations** (per yesterday's handoff) — so this is a documentation/understanding question, not a live-impact question. But confirm before diffing.

### Dry-run scenarios (to re-run in Session B)

Four scenarios; #2 explicitly dropped under Option α.

1. `capital_usd=$1000`, balance=$4274 → expect `usdt_for_allocation=$1000`.
2. ~~`capital_usd=NULL`~~ — dropped (schema NOT NULL).
3. `capital_usd=$5000`, balance=$4274 → expect size-down-with-warning to $4274.
4. Two allocations $3000 each, balance=$4274 → expect first $3000, second $1274 with warning. Exercises ordering + balance-decrement behavior.

Setup per scenario: insert allocation for scratch user w/ specific `capital_usd`, run `python -m app.cli.trader_blofin --allocation-id <UUID> --dry-run`, observe log, soft-close allocation (`status='closed'`).

Prerequisite: today's `user_mgmt.daily_signals` must have rows with `filter="Tail Guardrail"` matching alpha v1, else session exits at Phase 2. The master trader at 06:00 UTC writes these; scenarios work after that tick.

### Item 6 — SHIPPED 2026-04-20 (Session C)

Status: **COMPLETE, verified across all three active alpha strategies.**

Commit: `f26d460` (4 files: new `backend/app/services/audit/vol_boost.py`; modified `backend/app/cli/trader_blofin.py`, `backend/app/cli/refresh_strategy_metrics.py`, `backend/app/services/trading/trader_config.py`).

**Decisions (ratified at Session C investigation):**
- 6.1 revised: (b) **new `compute_strategy_vol_boost(rets)` sibling** in a new service module `app/services/audit/vol_boost.py`, not same-file as legacy `compute_vol_boost`. Math identical; constants duplicated for independence; legacy CSV-reader untouched (dies with CAPITAL_MODE cleanup).
- 6.3 revised: **JSONB**, not dedicated column. `current_metrics["vol_boost"]` is a first-class key; `metrics_updated_at` doubles as companion timestamp. No migration required.
- 6.new: **Accept NULL-after-promote lag.** Phase 4 defaults to `boost=1.0` on NULL (matches host "returns log missing → 1.0" fallback); clears on next 01:30 UTC tick. Explicit code comment documents this.
- 6.new-2: **Picked-filter returns.** Use `fees_tables_by_filter[strategy.filter_mode]` (not best_filter's table) to compute vol_boost, so the boost matches the simulated returns the metrics record publishes.

**Components:**
- `compute_strategy_vol_boost(rets: list[float]) -> float`: service fn with host-parity constants; 4-stage clip; trailing-window vol/sharpe, full-history DD.
- `refresh_strategy_metrics.refresh_one`: extracts picked-filter's `fees_table`, converts `ret_net` (pct) to decimals, computes boost, merges into `current_metrics["vol_boost"]` before UPDATE. Logs `vol_boost={x} (from N daily returns)`.
- `_fetch_strategy_version`: SELECT extended to include `current_metrics`.
- `run_session_for_allocation`: reads `current_metrics.get("vol_boost") or 1.0`, threads as new `vol_boost: float` param to `_run_fresh_session_for_allocation`.
- `_run_fresh_session_for_allocation` Phase 4: `eff_lev = round(l_high * vol_boost, 4)`; logs `l_high × vol_boost = eff_lev`.
- `TraderConfig.vol_boost_enabled` field deleted (was dead — factory hardcoded False).

**Verification (pre-push, via docker cp + docker exec against running celery container):**

| Strategy | l_high | vol_boost | eff_lev |
|---|---|---|---|
| Med lev (strategy_id=2, 6b6168b0) | 1.5 | 1.4768 | 2.2152 |
| Low lev (strategy_id=3, 3100d339) | 1.0 | 1.4768 | 1.4768 |
| High lev (strategy_id=4, 987312fd) | 2.0 | 1.4767 | 2.9534 |

All three refreshed clean: 431 daily returns sampled, sharpe=3.5064 unchanged, data_through=2026-04-19 unchanged, metrics_updated_at bumped. Post-deploy smoke (fresh image) imports cleanly from both celery and backend containers.

#### Saturation observation — worth preserving

Today's three-strategy vol_boost (1.4767–1.4768) is **leverage-invariant** — effectively identical across Low/Med/High lev despite their different l_high values. This is NOT a bug:

- **vc stage** (`target_vol / realized_vol`) saturates at `MAX_BOOST=2.0` because trailing 30d realized_vol is small (many flat/no-entry days → ret_net=0 dominates the window). vc saturation is leverage-invariant: even at 2× higher leverage, realized_vol doesn't recover enough to un-saturate vc.
- **sc stage** (`sharpe_ref / rolling_sharpe`) is mathematically leverage-invariant — mean_ret and realized_vol both scale with leverage, so rolling_sharpe cancels out.
- **dg stage** is structurally 1.0 (VOL_LEV_DD_SCALE inactive).
- Net: boost ≈ 2.0 × (3.3 / rolling_sharpe) ≈ 1.477, same math for all three.

**Under today's low-vol conditions, eff_lev differentiation lives entirely in `l_high`; vol_boost is a common multiplier.** If future market conditions push realized_vol above ~0.01 (target_vol/2), vc will un-saturate and the three strategies will show different vol_boost values. Preserve this finding so future readers don't flag near-identical boost values as a bug.

**Not validated — folds into Item 9's pre-deploy gate as Path 4 (see section above).**

### Item 6 — original decisions (historical; shipped this session)

> Preserved for reference. Decisions 6.1 and 6.3 were revised at Session C investigation; see the SHIPPED section above.

Original Session B plan:
- 6.1 VOL formula: port `compute_vol_boost` verbatim from trader-blofin.py, change data source to strategy-level simulated returns.
- 6.2 VOL cadence: nightly at 01:30, same job as metrics refresh.
- 6.3 VOL storage: column on `strategy_versions` + companion timestamp.
- 6.4 VOL read timing: session entry, fixed for session.

---

## Production state at end of session

- Active published strategy versions: 3 alpha variants (all renamed to the "lev" axis mid-session):
  - `alpha_tail_guardrail_low_risk` slug / display "Alpha Tail Guardrail - Med lev" (strategy_id=2, strategy_version_id `6b6168b0-b6df-4cd4-8621-c29a9beb1dc4`)
  - `alpha_tail_guardrail_low_lev` slug / display "Alpha Tail Guardrail - Low lev" (strategy_id=3)
  - `alpha_tail_guardrail_high_lev` slug / display "Alpha Tail Guardrail - High lev" (strategy_id=4)
- Active allocations: 1. allocation_id `686077fc-82a4-45e4-872f-47f7ad328780`, capital_usd=$20, exchange=**binance**, user=j@mullincap.com. Dormant under current code (no Binance executor; spawn_traders `SUPPORTED_EXCHANGES={"blofin"}`). Item 9's eventual activation triggers the Item 10 pre-deploy gate above.
- Connection balances (as of ~02:25 UTC): BloFin $3,951.03 available; Binance $19.29 available.
- Deploy state: commit `5a7bdc7` on `main`, deployed via `./redeploy.sh` at ~03:10 UTC 2026-04-20.
- Services: backend/celery/redis/frontend/nginx all healthy post-redeploy.
- 01:30 UTC nightly cron on 2026-04-20 cleared GREEN (3/3 ok, 0 fail) — independent production validation of Item 4 on the deployed code.

## Cron state (unchanged from 2026-04-18)

```
0 6 * * *    [host]               master BloFin trader
5 6 * * *    [container:backend]  spawn_traders
30 1 * * *   [container:celery]   refresh_strategy_metrics
*/5 * * * *  [host]               blofin_logger.py (legacy — retire after multi-tenant stable ≥ 7d)
*/5 * * * *  [container]          sync_exchange_snapshots
+ metl 00:15, coingecko 00:30, indexer 01:00, caggs 01:15–18, overlap 01:20
+ manager briefing 00:30, sync-to-storage 02:00, signal 05:58, certbot 03:00
```

## Guardrails active across all future sessions

- `/root/benji/trader-blofin.py` on host — NEVER modified.
- Master cron line at `0 6 * * *` — NEVER modified.
- `audit.jobs` + `audit.results` — append-only; nightly runs add rows, never update/delete.
- User allocations + exchange_connections — no DELETE; soft-close via status column.
- Any live-money execution path change — smoke test against real allocation in dry-run before cron integration.
- Redeploy — check `celery inspect active` before `./redeploy.sh` to avoid `--force-recreate` killing in-flight jobs.

## Files/paths touched this session

Commits on `main`:
- `9b14233` — Item 5 docs (`docs/deferred_work.md`)
- `1199708` — Item 4 refactor (5 files: metrics_parser, pipeline_runner, pipeline_worker, refresh_strategy_metrics, jobs)
- `fe53619` — Session A handoff (this file, initial)
- `a2fc343` — deferred_work.md: CAPITAL_MODE/CAPITAL_VALUE cleanup entry + strategy taxonomy entry
- `95f4a2c` — Allocator strategy display_name rename feature (backend endpoint + frontend modal)
- `5a7bdc7` — **Item 10: per-allocation capital sizing** (`backend/app/cli/trader_blofin.py`)
- `4b80b7f` — deferred_work.md: strategy_id=1/v1.0 refresh exclusion note (Session C housekeeping)
- `f26d460` — **Item 6: strategy-level vol_boost publication + per-allocation read** (new `vol_boost.py` service module + 3 modified files)

Baselines preserved locally at `/tmp/benji_baselines/`:
- `path_a_audit_output.txt`, `path_a_reparsed.json` (Path A, 30 keys)
- `path_b_audit_output.txt`, `path_b_baseline_metrics.json` (Path B, 30 keys, 23:04 UTC)
- `path_b_post_metrics.json` (post-refactor re-run, byte-identical to baseline)

These can be cleared any time — they're session scratch, not authoritative.

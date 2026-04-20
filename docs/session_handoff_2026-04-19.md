# Session Handoff — 2026-04-19 (end of Track 3 Session A)

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

---

## Track 3 Session B — scope + pre-committed decisions

### Session B items

- **Item 10** — Per-allocation capital sizing (deferred from Session A after investigation surfaced decision-drift flags).
- **Item 6** — VOL boost publication (original Session B item, unchanged).

Investigation on Item 10 is complete. Start Session B with the decisions below already committed; no re-investigation needed.

### Item 10 — decisions pre-committed for Session B

**Decision 10.1 revision**: schema **Option α** locked.
- `user_mgmt.allocations.capital_usd numeric(18,2) NOT NULL` stays as-is.
- Drop the "NULL → 90%-of-balance fallback" branch from the original decision. `capital_usd` is always present (enforced at schema); no NULL code path.

**Decision 10.3 reinforcement**: ordering **Option (b)** locked.
- PostgreSQL advisory lock around `get_account_balance_usdt` + `enter_positions`, keyed on user_id or connection_id. Preserves parallel-spawn architecture, eliminates the balance-read race, minimal LOC.
- Reject (a) "accept the race" on live-money grounds.
- Reject (c) "serial spawn" — throws away parallelism unnecessarily.

**Scenario teardown protocol**: dedicated scratch user with a known `user_id`. Allocations inserted for scenario testing, soft-closed via `status='closed'` afterward. No DELETE — preserves no-data-deletion guardrail, re-runnable.

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

### Item 6 — unchanged

VOL boost publication, per original Session B plan. Decisions 6.1–6.4 remain locked.
- 6.1 VOL formula: port `compute_vol_boost` verbatim from trader-blofin.py, change data source to strategy-level simulated returns.
- 6.2 VOL cadence: nightly at 01:30, same job as metrics refresh.
- 6.3 VOL storage: column on `strategy_versions` + companion timestamp.
- 6.4 VOL read timing: session entry, fixed for session.

---

## Production state at end of Session A

- Active published strategy versions: `alpha_tail_guardrail_low_risk v1` (strategy_version_id `6b6168b0-b6df-4cd4-8621-c29a9beb1dc4`).
- Active allocations: 0.
- Connection state (unchanged): j@mullincap.com BloFin CONNECTED (~$4,274); Binance CONNECTED (~$19).
- Deploy state: commit `1199708` on `main`, deployed via `./redeploy.sh` at ~23:51 UTC 2026-04-19.
- Services: backend/celery/redis/frontend/nginx all healthy post-redeploy.

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

Baselines preserved locally at `/tmp/benji_baselines/`:
- `path_a_audit_output.txt`, `path_a_reparsed.json` (Path A, 30 keys)
- `path_b_audit_output.txt`, `path_b_baseline_metrics.json` (Path B, 30 keys, 23:04 UTC)
- `path_b_post_metrics.json` (post-refactor re-run, byte-identical to baseline)

These can be cleared any time — they're session scratch, not authoritative.

# Session Handoff — 2026-04-18 (end of session)

## Tomorrow's first action — BEFORE any Track 3 implementation

1. **Verify tonight's 01:30 UTC cron tick succeeded:**
   ```bash
   # Did the cron fire and complete cleanly?
   ssh mcap 'tail -50 /mnt/quant-data/logs/trader/refresh_metrics.log'

   # Did metrics_updated_at advance by ~24h on alpha v1?
   ssh mcap 'docker exec -i timescaledb psql -U quant -d marketdata -c "
     SELECT version_label, metrics_updated_at, metrics_data_through
     FROM audit.strategy_versions WHERE is_active = TRUE;"'
   ```
   Expected: log shows `ok=1 fail=0` around 01:37–01:38 UTC. `metrics_updated_at` bumped to ~01:37 UTC (from 2026-04-18 23:30:24 UTC). `metrics_data_through` advanced to 2026-04-18 (from 2026-04-17).

   Note: the user's originally-proposed psql command form was `docker compose -f /root/benji/docker-compose.yml exec -T db psql …` — that won't work on this prod because the DB runs as a standalone container named `timescaledb`, not as a `db` service in the compose stack. Use `docker exec -i timescaledb psql …` as shown.

2. **If cron didn't fire or produced errors, pause Track 3 and debug the failure first.** Do not start Track 3 implementation work until the nightly refresh is confirmed working.

3. **Ask J to do visual confirmation of the Allocator card** if not already done. 30-second check: J loads `/trader/strategies` in a browser with an authenticated session, confirms `alpha_tail_guardrail_low_risk v1` card renders with populated metrics (Sharpe 3.51, CAGR 2016%, Max DD -29.23%, Total Return 415.74%, etc.) plus the new `metrics_updated_at` / `metrics_data_through` fields. Catches any frontend regression from the API response shape change in Part 11.

Only after (1) confirms green and (3) is checked off, proceed with Track 3 Session A (items 5 + 4 + 10).

## What shipped today

### Track 1 — Nightly strategy metrics refresh + Group A

Status: **COMPLETE, awaiting first cron tick at 01:30 UTC tonight (2026-04-19)**.

Commits on `main`:
- `c0b5e87` — Track 1 core implementation (schema, config_hash, pipeline_runner, nightly CLI, promote fixes, allocator read-path swap)
- `8cbfb42` — `docs/deferred_work.md` env drift entry

Shipped components:
- **Schema:** `audit.strategy_versions` gained `config_hash TEXT`, `current_metrics JSONB`, `metrics_updated_at TIMESTAMPTZ`, `metrics_data_through DATE`. Partial unique index on `(strategy_id, config_hash)`.
- **New files:**
  - `backend/app/services/audit/config_hash.py` (92 LOC) — canonicalize + hash
  - `backend/app/services/audit/pipeline_runner.py` (409 LOC) — shared pre-stage + subprocess helpers
  - `backend/app/services/audit/current_metrics.py` (54 LOC) — shared JSONB builder
  - `backend/app/cli/refresh_strategy_metrics.py` (220 LOC) — nightly CLI
- **Refactor:** `pipeline_worker.py` trimmed 1045 → 739 LOC; pre-stage + subprocess extracted into `pipeline_runner.py`, byte-identical behavior. Celery task unchanged functionally.
- **Regex extensions:** `_parse_metrics` now extracts 5 previously-NULL fields (`profit_factor`, `win_rate_daily`, `avg_daily_ret_pct`, `best_month_pct`, `equity_r2`). All best-filter top-level.
- **Promote path (`simulator.py`):** persists `active_filter` into `strategy_version.config`; hash-match branch reuses existing version; writes `current_metrics` on every call; `_TOP_LEVEL_ONLY_COLUMNS` extended with 5 new entries.
- **Allocator API (`allocator.py`):** card metric read swapped from `audit.results` DISTINCT ON join to `sv.current_metrics ->>`; response includes `metrics_updated_at` + `metrics_data_through`.

Cron (on mcap host, as root):
- `30 1 * * * cd /root/benji && docker compose -f /root/benji/docker-compose.yml exec -T celery python -m app.cli.refresh_strategy_metrics >> /mnt/quant-data/logs/trader/refresh_metrics.log 2>&1`
- **Target is `celery`, not `backend`** — see `deferred_work.md` env drift note. Using `backend` would trigger a 3-hour parquet rebuild every night.

Smoke test result (2026-04-18 23:23–23:30 UTC):
- Real refresh against `alpha_tail_guardrail_low_risk v1` (strategy_version_id `6b6168b0-b6df-4cd4-8621-c29a9beb1dc4`)
- Wall time 7m 18s
- Populated `current_metrics`: `total_return_pct=415.7392`, `sharpe=3.5149`, `max_dd_pct=-29.2286`, `cagr_pct=2016.2603`, `profit_factor=2.025`, `win_rate_daily=53.91`, `avg_daily_ret_pct=3.25`, `best_month_pct=124.49`, `equity_r2=0.9786`, `metrics_data_through=2026-04-17`, `is_best=True`
- `audit.jobs` + `audit.results` rows written (`job_id f57599eb-5468-4df0-a07e-3eeaf27e55e8`, `result_id 53127153-10ca-4b05-a22d-1395a99c5925`)
- Allocator card now renders populated (unverified visually — pending J's confirmation per Action 3 above)

## Deferred work items logged

From `docs/deferred_work.md`:

1. **BASE_DATA_DIR env drift between backend and celery services.** `.env.production` has legacy `/data`; celery has explicit `/mnt/quant-data` override; backend does not. Mitigation in place (nightly cron targets celery). Durable fix deferred — update `.env.production` or add backend-service override in `docker-compose.yml`.

## What's next — Track 3 (Group B)

Status: **NOT STARTED. Decisions locked in, awaiting implementation.**

Track 3 covers five items from the open work list. Full spec + decisions captured here so a new session can pick up without re-asking.

### Decisions locked (no re-litigation)

| ID   | Question                         | Decision |
|------|----------------------------------|----------|
| 4.1  | run_audit return type            | dict |
| 4.2  | audit_output.txt side effect     | preserve |
| 4.3  | Progress callback                | keep as optional param |
| 6.1  | VOL formula                      | port `compute_vol_boost` verbatim, change data source from account CSV to strategy-level simulated returns |
| 6.2  | VOL cadence                      | nightly at 01:30, same job as metrics refresh |
| 6.3  | VOL storage                      | column on `strategy_versions` + companion timestamp |
| 6.4  | VOL read timing                  | session entry, fixed for session |
| 9.1  | Binance symbols                  | filter existing BloFin signal CSV to Binance-supported pairs (translate `BTC-USDT` → `BTCUSDT`) |
| 9.2  | Binance repayment                | `sideEffectType=AUTO_REPAY` |
| 9.3  | Binance leverage math            | new `_place_order_chunked_binance_margin` (borrow-then-buy); dispatch in `enter_positions` by exchange |
| 9.4  | Binance leverage source          | strategy's `l_high` (single source of truth) |
| 10.1 | Capital sizing                   | always `capital_usd`, NULL → 90%-of-balance fallback |
| 10.2 | Balance below alloc              | size down with warning log, continue |
| 10.3 | Over-allocation                  | first-come wins (`created_at ASC`) |
| 10.4 | `capital_usd` mutability         | mutable, fresh read each session entry |
| 5    | Audit convention sweep           | scope to `IDENTITY_FIELDS` only |

### Recommended session split

Track 3 is estimated 10–15h wall time. **Don't do it as one continuous session.** Recommended split:

**Session A** (3–5h): Items 5 + 4 + 10.
- Item 5: audit convention sweep — investigation + 0–3 small fixes
- Item 4: `audit.py` `run_audit(params) → dict` refactor — replaces subprocess+regex path; both Simulator Celery task and nightly CLI migrate to it
- Item 10: per-allocation capital sizing — `capital_usd` flows from allocations table through `enter_positions`

**Session B** (2–3h): Item 6 — VOL boost publication.
- Nightly CLI computes `current_leverage_multiplier` per strategy_version during the same 01:30 UTC job
- Executor reads it at session entry, fixed for session

**Session C** (4–6h): Item 9 — Binance margin executor.
- New execution path alongside BloFin futures
- ~250 LOC of Binance-specific order logic + dispatch wiring + signal filtering
- Smoke test is real Binance margin orders

Each session ends with the system in a functional state. Sessions B and C are independent of each other but both depend on Session A landing first.

### Items NOT in Track 3 scope

From the open work list, these are intentionally deferred:
- Retire `blofin_logger.py` cron (conditional on multi-tenant executor stable ≥ 7 days)
- Resolve plaintext BloFin row under `admin@mullincap.com` (small, Option A delete)
- Frontend "Last refreshed N ago" label on Allocator cards (exposes Track 1's `metrics_updated_at`)
- $25K allocation slider UI clamp (display-only, server-side enforcement lands in Track 3 via item 10)
- Publish 2 more strategy variants via Simulator UI (operational, not code — do via Simulator modal with different `l_high` values)

## Key file paths (preserve verbatim)

Prod server: mcap (Hetzner, `/root/benji/`)
- Host trader: `/root/benji/trader-blofin.py` (**NEVER TOUCH**)
- Secrets: `/mnt/quant-data/credentials/secrets.env`
- Logs: `/mnt/quant-data/logs/trader/`
- Trader state: `/mnt/quant-data/trader/`
- Compose: `/root/benji/docker-compose.yml`
- Deploy: `/root/benji/redeploy.sh`

Backend package:
- `backend/app/cli/trader_blofin.py` — per-allocation BloFin executor
- `backend/app/cli/spawn_traders.py` — parent spawner (cron at 06:05 UTC)
- `backend/app/cli/refresh_strategy_metrics.py` — nightly metrics refresh (cron at 01:30 UTC)
- `backend/app/cli/sync_exchange_snapshots.py` — every 5 min
- `backend/app/services/trading/{trader_config,credential_loader,blofin_auth}.py`
- `backend/app/services/exchanges/{binance,permissions}.py`
- `backend/app/services/audit/{config_hash,pipeline_runner,current_metrics}.py`
- `backend/app/workers/pipeline_worker.py` — Simulator Celery task
- `backend/app/api/routes/{simulator,allocator}.py`

Frontend:
- `frontend/app/trader/(protected)/settings/page.tsx` — exchange linking wizard
- `frontend/app/trader/(protected)/strategies/page.tsx` — Allocator strategy grid
- `frontend/app/trader/(protected)/strategies/[id]/page.tsx`
- `frontend/app/trader/(protected)/traders/[id]/page.tsx` — trader detail
- `frontend/app/trader/{api.ts,context.tsx}`
- `frontend/app/simulator/{page.tsx,PromoteStrategyModal.tsx}`
- `frontend/app/trader/components/{SetupWizard,AllocationPicker,TraderCard}.tsx`

DB schema references:
- `user_mgmt.{users,allocations,exchange_connections,exchange_snapshots,portfolio_sessions,portfolio_bars,allocation_returns,daily_signals}`
- `audit.{strategies,strategy_versions,jobs,results}`

## Current cron state on mcap (verified at handoff)

```
0 6 * * *    [host]               master BloFin trader (. secrets.env && python /root/benji/trader-blofin.py)
5 6 * * *    [container:backend]  spawn_traders — per-allocation BloFin executors
30 1 * * *   [container:celery]   refresh_strategy_metrics — nightly metrics refresh (NEW TODAY)
*/5 * * * *  [host]               blofin_logger.py — legacy per-user balance logger
*/5 * * * *  [container]          sync_exchange_snapshots — validates pending, snapshots active
+ metl 00:15, coingecko 00:30, indexer 01:00, caggs 01:15–18, overlap 01:20
+ manager briefing 00:30, sync-to-storage 02:00, signal 05:58, certbot 03:00
```

## Guardrails active across all future sessions

- `/root/benji/trader-blofin.py` on host — NEVER modified
- Master cron line at `0 6 * * *` — NEVER modified
- `audit.jobs` + `audit.results` — append-only; nightly runs add rows, never update/delete
- Simulator user-driven audit flow — preserved byte-identically by `pipeline_runner.py` extraction
- For any live-money execution path change, require smoke test against real allocation before cron integration

## Prod state at end of session

Active published strategy versions (nightly refresh targets):
- `alpha_tail_guardrail_low_risk` v1 — active=TRUE, published=TRUE, `current_metrics` populated as of 2026-04-18 23:30:24 UTC

Active but unpublished versions (skipped by nightly):
- `overlap_tail_disp` v1.0 — active=TRUE, published=FALSE (hand-seeded, not promoted)

Active allocations: 0 (per Part 2b cleanup after spawn_traders smoke test)

Connection state:
- `j@mullincap.com`: BloFin CONNECTED ($4,274.06 as of last snapshot), Binance CONNECTED ($19.32)
- `admin@mullincap.com`: one plaintext BloFin row pending cleanup (deferred)

Services:
- Backend: healthy, commit `8cbfb42`
- Celery: healthy
- DB: `marketdata`, `timescaledb` container
- Redis: healthy
- `/health` → 200

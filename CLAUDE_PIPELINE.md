# Benji3m — Pipeline (CLAUDE_PIPELINE.md)

Context for work in `pipeline/` and `trader-blofin.py`. Read CLAUDE.md first.

## Stack

- Pure Python 3.13 CLI scripts (no framework)
- Separate venv at `pipeline/.venv/` with heavy data deps (pandas, pyarrow, numpy, psycopg2, chart.js not here — reports are generated via Node in `generate_audit_report.js`)
- Scripts invoked two ways:
  1. As subprocess from `backend/app/workers/pipeline_worker.py` (UI-driven simulator runs)
  2. Directly via host cron (nightly ETL, indexer, daily_signal, trader)

## Core scripts

```
pipeline/
  compiler/
    metl.py                         Amberdata futures ETL → market.futures_1m
    coingecko_marketcap.py          Daily marketcap → market.market_cap_daily
  indexer/
    build_intraday_leaderboard.py   Daily top-N leaderboard → market.leaderboards
  allocator/
    blofin_auth.py                  Signed-request helper used by trader + allocator backend
    blofin_logger.py                5-min exchange snapshot → user_mgmt.exchange_snapshots
  overlap_analysis.py               Orchestrator — computes deploy dates, chains audit.py
  audit.py                          Strategy backtester (simulate() per filter/config)
  rebuild_portfolio_matrix.py       Per-session price series builder (parquet or db)
  db/connection.py                  psycopg2.connect() helper

trader-blofin.py                    Live daily session orchestrator (runs at 06:00 UTC)
daily_signal.py                     Generates live_deploys_signal.csv (runs at 05:58 UTC)
```

## Data source selection

Most scripts accept `--source {parquet|db}` or read `PRICE_SOURCE` / `MCAP_SOURCE` env vars. DB mode is the current default; parquet mode is legacy but still supported.

**Why DB mode matters for query performance:** hypertable chunks can't be index-pruned when you filter via `EXTRACT(HOUR/MINUTE FROM timestamp_utc)` — Postgres can't push computed-expression filters to the PK index, so every chunk gets seq-scanned. On `market.leaderboards` (1.18B rows, 60+ chunks, 216GB) that was ~4m38s per call and killed audits.

The pattern is: build the list of target UTC timestamps in Python, pass as `ANY(array)` plus an explicit `timestamp_utc BETWEEN $lo AND $hi` range for chunk exclusion. Example in `overlap_analysis.py::_load_frequency_from_db` (snapshot branch). Verified 2,800× speedup on the live DB. Apply the same pattern to any new DB-mode query that filters by hour/minute of day.

## Env vars (pipeline subprocess)

Set by `pipeline_worker.py` for UI-driven runs; set in `secrets.env` for cron runs. Key ones:

| Var | Purpose |
|---|---|
| `PRICE_SOURCE` / `MCAP_SOURCE` | `parquet` or `db` |
| `DEPLOYMENT_START_HOUR` | 6 (UTC) — session anchor |
| `INDEX_LOOKBACK` / `SORT_LOOKBACK` | hours before session start |
| `LEADERBOARD_INDEX` | top-N rank cap |
| `FREQ_WIDTH` / `FREQ_CUTOFF` | overlap window config |
| `STARTING_CAPITAL` / `CAPITAL_MODE` | audit sizing |
| `L_HIGH` / `L_BASE` | leverage floors |
| `PORT_SL` / `PORT_TSL` / `STOP_RAW_PCT` | stop thresholds |
| `PYTHONUNBUFFERED=1` | forces stdout line-flush so UI pane scrolls smoothly |

Full list in `pipeline_worker.py::pipeline_env`.

## Nightly cron (on prod host, not Docker)

All times UTC:

| Time | What |
|---|---|
| 00:15 | `metl.py` — Amberdata ETL for yesterday's 1m data |
| 00:30 | `coingecko_marketcap.py` — daily mcap snapshot |
| 00:30 | `POST /api/manager/briefing` — auto portfolio briefing |
| 01:00 | `build_intraday_leaderboard.py` for price + OI + volume |
| 01:15 | `CALL refresh_continuous_aggregate(leaderboards_daily_count, 2d)` |
| 01:17 | `CALL refresh_continuous_aggregate(symbol_day_counts, 30d)` |
| 01:18 | `CALL refresh_continuous_aggregate(futures_1m_daily_symbol_count, 30d)` |
| 01:20 | `overlap_analysis.py` — regenerate deploys CSV |
| 02:00 | `sync-to-storage.sh` — cloud backup |
| 05:58 | `daily_signal.py` — today's signal |
| 06:00 | `trader-blofin.py` — live session |
| every 5 min | `blofin_logger.py` — balance + position snapshot |

The 01:17 and 01:18 entries were added after an 18-day gap was found in `symbol_day_counts`. The built-in hourly cagg-refresh policy only covers the last 7 days; the nightly 30-day refresh is belt-and-suspenders against silent materialization holes.

## Trader observability (trader-blofin.py)

Live trader writes to four places on every run, so operators can reconstruct a session even if any one store is unavailable:

1. **`blofin_executor.log`** (append-only) — full log stream, tailed by the Manager Execution tab's Session Logs viewer via `/api/manager/execution-logs`
2. **`blofin_executor_state.json`** (rewritten per bar) — crash-recovery state for `--resume`
3. **`blofin_execution_reports/YYYY-MM-DD.json`** — per-session execution report (signal, conviction, leverage, capital, fills incl. BloFin-reconciled fill prices + bps slippage, monitoring stats, exit reason, actual vs estimated P&L). Read by the Execution tab.
4. **`user_mgmt.portfolio_sessions` + `user_mgmt.portfolio_bars`** (SQL, primary) **and** `blofin_execution_reports/portfolios/YYYY-MM-DD.ndjson` (backup) — bar-by-bar timeline. Dual-write is independent + fault-tolerant: DB failure doesn't block NDJSON, NDJSON I/O error doesn't block DB. Read by the Portfolios tab (SQL only).

All four are written with best-effort error handling — a persistence hiccup logs a warning but never stops the monitoring loop. The trader is the "source of truth"; everything downstream is observational.

Trader is **not containerized** — runs directly on the host at `/root/benji/trader-blofin.py` via cron. The backend container mounts `/root/benji` as `/host_trader:ro` to read the log + reports files (see CLAUDE_BACKEND.md).

## Pipeline runs under UI

When the user clicks "Run Audit" in the simulator:
1. FastAPI `/api/jobs` creates a job JSON in `JOBS_DIR` and enqueues to Celery
2. Celery worker (`pipeline_worker.run_pipeline`) subprocesses `overlap_analysis.py --audit ...` with all params as env vars
3. `overlap_analysis.py` generates deploys CSV, then chains to `audit.py --deploys ... --source db ...`
4. `audit.py` calls `rebuild_portfolio_matrix.py` for price series + runs `simulate()` per filter
5. Node post-processor (`generate_audit_report.js`) generates summary + breakdown CSVs
6. Backend parser extracts metrics from `audit_output.txt` for the UI

## Redeploy and pipeline code

Pipeline scripts are **baked into the celery Docker image** (both `backend/Dockerfile.backend` — the celery service reuses it). So pipeline code changes require `./redeploy.sh` on prod to take effect for UI-driven audits.

Host-cron runs (`metl.py`, `trader-blofin.py`, etc.) use `/root/benji/pipeline/.venv/bin/python` against live disk files, so they pick up `git pull` immediately — no rebuild.

## Running locally

```bash
source .venv/bin/activate   # project-root venv has a separate `pipeline/.venv` too
cd pipeline
python overlap_analysis.py --leaderboard-index 100 --freq-width 20 --mode snapshot \
    --deployment-start-hour 6 --sort-lookback 6 --min-mcap 0 --source db
```

Requires reachable TimescaleDB (SSH tunnel), `BASE_DATA_DIR` set, and secrets.env loaded.

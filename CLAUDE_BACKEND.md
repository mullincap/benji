# Benji3m — Backend (CLAUDE_BACKEND.md)

Context for work in `backend/`. Read CLAUDE.md first for high-level project rules.

## Stack

- FastAPI + uvicorn
- Celery (broker: Redis at `redis:6379/0` inside docker network)
- PostgreSQL 15 + TimescaleDB extension
- psycopg2 via `app.db.get_cursor()` (RealDictCursor — rows come back as dicts)
- JSON job persistence at `JOBS_DIR` (flat dir, one folder per job)

## Router layout

Every module has its own `APIRouter` registered in `backend/app/main.py`:

```
backend/app/api/routes/
  admin.py       /api/admin/*       shared-secret login, session cookies
  auth.py        /api/auth/*        per-user allocator auth
  jobs.py        /api/jobs/*        audit job CRUD, output streaming
  compiler.py    /api/compiler/*    coverage, days, marketcap, symbols
  indexer.py     /api/indexer/*     leaderboard stats
  allocator.py   /api/allocator/*   exchange connections, snapshots
  manager.py     /api/manager/*     overview, conversations, briefings,
                                    execution-reports, portfolios, execution-logs
```

## Auth

- `Depends(require_admin)` on routers that manage pipeline/state (compiler, indexer, manager)
- `Depends(require_user)` on allocator for per-user sessions
- Cookie-based sessions; admin tokens in `backend/data/admin_sessions.json`

## DB access

```python
from ...db import get_cursor
@router.get("/foo")
def foo(cur=Depends(get_cursor)):
    cur.execute("SELECT ...", (params,))
    rows = cur.fetchall()  # list of RealDictRow (dict-like)
```

Never pass user input into format strings — always use `%s` placeholders. Connection pooling is handled upstream; don't open your own psycopg2 connections unless you specifically need isolation (e.g., the trader-side writes that live outside FastAPI).

## Schemas

Three Postgres schemas:

- **`market`** — raw + indexed market data (hypertables: `futures_1m` ~1.18B rows, `leaderboards`, `derivatives_analytics`, `spot_1m`, `options_quotes`)
- **`audit`** — backtest/simulator jobs, strategy_versions, results, equity_curves
- **`user_mgmt`** — live operations: users, allocations, deployments, daily_signals, exchange_connections, exchange_snapshots, **portfolio_sessions + portfolio_bars** (live-trader timeline)

Continuous aggregates under `market`: `futures_1m_daily_symbol_count`, `symbol_day_counts`, `leaderboards_daily_count`. All three have a hourly built-in refresh policy covering only the last 7 days — a wider nightly refresh runs via cron (see CLAUDE_PIPELINE.md for cron schedule).

## Celery worker

`backend/app/workers/pipeline_worker.py` — single long-running worker. Main task is `run_pipeline(job_id, params)`:

1. Build `pipeline_env` dict (all params as SNAKE_UPPER_CASE env vars)
2. Subprocess `pipeline/overlap_analysis.py` with `_build_cli_args(params)`
3. Stream stdout line-by-line to `JOBS_DIR/{job_id}/audit_output.txt`
4. Pulse `progress` field on the job's JSON state as lines arrive
5. Post-process: generate report, parse metrics, finalize job state

Env bits worth knowing:
- `PYTHONUNBUFFERED=1` is set on the subprocess so `log.info()` flushes immediately (otherwise block-buffering makes the frontend's "LIVE OUTPUT" pane look stuck during long quiet phases).
- `PATH` is augmented so `python3` in pipeline scripts resolves to the pipeline-specific Python (separate venv with full scientific stack).
- Cancellation is cooperative: worker checks `_is_cancelled(job_id)` each line, then `proc.terminate()`.

## Docker filesystem mounts

Backend runs in Docker but the trader (`trader-blofin.py`) runs on the **host** — not containerized. So the backend container can't see the trader's working dir by default. The fix (added this session) is a read-only bind mount in `docker-compose.yml`:

```yaml
backend:
  volumes:
    - /root/benji:/host_trader:ro
  environment:
    - BLOFIN_LOG_FILE=/host_trader/blofin_executor.log
    - BLOFIN_REPORTS_DIR=/host_trader/blofin_execution_reports
```

Endpoints that read trader artifacts (`/execution-reports`, `/execution-logs`) resolve paths via these env vars with a project-root fallback for local dev (`Path(__file__).resolve().parents[4]`).

Local dev runs uvicorn directly without Docker, so the fallback path works; no mount needed.

## Manager endpoints (this session's surface)

- `GET /api/manager/execution-reports` — reads JSON files from `BLOFIN_REPORTS_DIR`, sorts by date desc
- `GET /api/manager/execution-logs?date=&since_line=&limit=` — parses `blofin_executor.log`, session-windowed by the trader's `SESSION {date}` headers, tolerates partial last lines (mid-write safe), supports live polling via `since_line` cursor
- `GET /api/manager/portfolios` — summary list from `user_mgmt.portfolio_sessions` (cached columns on the session row, no per-bar aggregation at list time)
- `GET /api/manager/portfolios/{date}` — meta + ordered bars for one session; safe to poll while `status='active'`

Response shape note: the portfolio endpoints deliberately match the legacy NDJSON shape (`{meta, bars[]}`) so the frontend was unchanged when storage switched from files to SQL.

## Running locally

```bash
cd backend && source ../.venv/bin/activate
uvicorn app.main:app --reload --port 8000      # API
celery -A app.workers.pipeline_worker.celery_app worker --loglevel=info   # worker (separate terminal)
```

Swagger at `http://localhost:8000/docs`. Requires Redis running locally (or `REDIS_URL=redis://remote-host:6379/0`) and a reachable TimescaleDB (SSH tunnel to prod DB is the usual dev path).

## Redeploy on prod

The backend is Dockerized, so code changes need a rebuild:

```bash
ssh mcap 'cd ~/benji && ./redeploy.sh'
```

`redeploy.sh` does `docker compose up -d --build --force-recreate`. **This kills any in-flight celery tasks** — always check `celery inspect active` before redeploying.

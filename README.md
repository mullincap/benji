# Benji3m

Quantitative trading risk audit platform for crypto futures. Backtests portfolio strategies and produces institutional-grade analytics for fund managers and allocators.

## Architecture

```
frontend/     Next.js 16 + TypeScript + Tailwind CSS
backend/      FastAPI + Celery + Redis (job queue)
pipeline/     Pure Python CLI scripts (data compilation, indexing, audit engine)
```

**Database:** TimescaleDB (PostgreSQL) for time-series market data
**Job persistence:** Flat JSON files per job + Celery/Redis for async execution
**Deployment:** Docker Compose on Hetzner (nginx reverse proxy)

## Modules

| Module | Purpose |
|--------|---------|
| **Compiler** | Downloads and compiles market data (Binance, BloFin, CoinGecko, Amberdata) |
| **Indexer** | Builds intraday leaderboards and ranking signals |
| **Simulator** | Backtesting engine with risk audit pipeline |
| **Manager** | Portfolio intelligence — Overview / Execution / Portfolios / Chat tabs (see below) |
| **Allocator** | Live exchange connections and allocation management |
| **Trader** | Live trading interface (BloFin) |

## Manager surface

Four tabs under `/manager`:

- **Overview** — total AUM, allocations, pipeline status, 30-day equity curve, today's intraday equity
- **Execution** — per-session trader scorecards (signal / conviction / fills with bps slippage / retries / P&L vs estimate) backed by `blofin_execution_reports/YYYY-MM-DD.json`. Collapsible Session Logs viewer tails the live trader log with 15s polling during active sessions
- **Portfolios** — bar-by-bar live-trader timeline per traded day. List view + drill-down with Chart.js cumulative ROI chart (per-symbol + portfolio overlay) and a 5-min ROI matrix. 30s polling when a session is active. Backed by `user_mgmt.portfolio_sessions` + `user_mgmt.portfolio_bars`
- **Chat** — conversational Claude interface over the portfolio context

The trader (`trader-blofin.py`) is the source of truth for Execution + Portfolios data — it writes to disk + Postgres on every 5-minute bar. The dashboards are read-only views over those stores.

See `CLAUDE.md`, `CLAUDE_FRONTEND.md`, `CLAUDE_BACKEND.md`, `CLAUDE_PIPELINE.md` for domain-specific context.

## Local Development

```bash
# 1. Set up Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r backend/requirements.txt
pip install -r pipeline/requirements.txt

# 2. Configure environment
cp .env.example .env
# Fill in DB_PASSWORD, API keys, PIPELINE_PYTHON path

# 3. Start backend API
cd backend && uvicorn app.main:app --reload --port 8000

# 4. Start Celery worker (separate terminal)
cd backend && celery -A app.workers.pipeline_worker.celery_app worker --loglevel=info

# 5. Start frontend (separate terminal)
cd frontend && npm install && npm run dev
```

**Prerequisites:** Python 3.13+, Node.js 22+, Redis, TimescaleDB (or SSH tunnel to remote)

## Production Deployment

```bash
# On the server:
docker compose --build up -d
```

See `setup.sh` for full server provisioning (Hetzner).

## Environment Variables

See `.env.example` for all required and optional variables.

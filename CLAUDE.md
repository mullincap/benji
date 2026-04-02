# Benji3m — Project Context for Claude Code

## What this project is
A web application wrapping a Python quantitative trading risk audit pipeline.
The pipeline backtests a crypto trading strategy and produces institutional-grade analytics.
Target users: crypto fund managers and allocators.

## Tech stack
- Backend: FastAPI + Celery + Redis + flat JSON job store
- Frontend: Next.js + TypeScript + Tailwind CSS
- Pipeline: Pure Python CLI scripts (no framework)
- Report generation: Node.js (generate_audit_report.js)
- Python virtual env: .venv (venv) at ~/Projects/benji3m/.venv

## Rules
- Read only the files needed for the current task.
- Do not load full project context unless required.
- For UI work, read CLAUDE_FRONTEND.md
- For API/backend work, read CLAUDE_BACKEND.md.
- For pipeline/audit work, read CLAUDE_PIPELINE.md.
- Start a fresh session when switching domains.
- Avoid re-reading CLAUDE files if already loaded in this session.
- Prefer working from direct file inspection over loading project context files.

## Important decisions already made
- Google Sheets: SKIPPED — using local parquet files only (--source parquet)
- Virtual env: .venv (venv)
- Job queue: Celery + Redis
- Job persistence: flat JSON files per job in JOBS_DIR
- API style: REST (not GraphQL)
- Frontend framework: Next.js with TypeScript + Tailwind
- Font: Space Mono throughout (no other fonts anywhere)
- Visual direction: Premium dark — dark mode first, financial data aesthetic
- UX model: Single page, three states (idle / running / results)

## Design system (LOCKED — do not deviate)

### Font
Space Mono 400 + 700 only — import via Google Fonts in frontend/app/layout.tsx
`@import url('https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;0,700;1,400&display=swap');`
Zero use of any non-monospace font anywhere in the application.

### Color palette (CSS variables — define in globals.css)
```css
--bg0: #080809;   /* page background */
--bg1: #0e0e10;   /* panels, topbar */
--bg2: #141416;   /* cards */
--bg3: #1a1a1d;   /* inputs */
--bg4: #222226;   /* hover states */
--line:  #242428; /* default borders */
--line2: #2e2e33; /* hover borders */
--t0: #f0ede6;    /* primary text */
--t1: #a09d96;    /* secondary text */
--t2: #5a5754;    /* muted text */
--t3: #35332f;    /* hint text */
--green: #00c896;        /* accent — positive, active, complete */
--green-dim: #00c89618;
--green-mid: #00c89640;
--amber: #f0a500;        /* warning — running, borderline metrics */
--amber-dim: #f0a50018;
--red: #ff4d4d;          /* danger — failed, breached */
--red-dim: #ff4d4d18;
```

### Typography rules
- All section labels: 9px, uppercase, letter-spacing 0.12em, color var(--t3), font-weight 700
- All field labels: 10px, color var(--t2), font-weight 400
- Field values (read-only): 10px, color var(--t1), font-family monospace
- Hero numbers (Sharpe, MaxDD, CAGR): 15–18px, font-weight 700
- Metric card values: 18px, font-weight 700
- Status bar text: 10px, color var(--t2)
- Topbar logo: 14px, font-weight 700

## Environment variables (.env)
BASE_DATA_DIR, PARQUET_PATH, MARKETCAP_DIR, JOBS_DIR, PIPELINE_DIR,
REDIS_URL, NODE_BIN

## FastAPI server test
cd ~/Projects/benji3m/backend && uvicorn app.main:app --reload --port 8000
Swagger UI: http://localhost:8000/docs

## Run locally
# Terminal 1 — API
cd ~/Projects/benji3m/backend && source ../.venv/bin/activate && uvicorn app.main:app --reload --port 8000
# Terminal 2 — Celery worker
cd ~/Projects/benji3m/backend && source ../.venv/bin/activate && celery -A app.workers.pipeline_worker.celery_app worker --loglevel=info
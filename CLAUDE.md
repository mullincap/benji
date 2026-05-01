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

## Manager Live tab — venue + storage conventions

The Manager Live tab (`/manager/live`) follows a deliberate **account-data
vs. market-data venue split** that future sessions must preserve:

- **Account data → BloFin.** Equity, positions, PnL, orders, fills,
  funding history. Reuse the existing trader-blofin code paths
  (`backend/app/cli/trader_blofin.py`, `pipeline/allocator/blofin_*`).
  Do NOT route account reads through Binance — the Binance API key in
  this project is permission-restricted to public market-data endpoints
  (verified via `python -m app.cli.binance_perm_audit`).

- **Market data → Binance USDM Futures (public endpoints only).**
  Klines, premium index, OI, OI history, top-trader L/S ratio, ticker
  24h, exchangeInfo. No signed Binance calls in v1.

- **Multi-venue from day one.** Every Live-tab table is keyed on
  `(venue, connection_id, …)` with `venue IN ('blofin','binance')`. In
  v1 every row has `venue='blofin'`. When Binance trading lands later
  it adds rows under `venue='binance'` — no schema migration needed.

### Three new tables (migrations 019/020/021)
- `user_mgmt.account_daily_anchors` — 00:00 UTC equity anchor per
  (venue, connection_id, anchor_date). Drives "today's PnL" math
  on the KPI strip. Distinct from `exchange_snapshots` (5-min cadence,
  intraday) — guarantees one row per UTC day.
- `user_mgmt.position_history` — lifecycle row per position
  (open → close), with entry context, source attribution
  (manual vs strategy via active session lookup), MFE/MAE peaks,
  and a free-form `metadata` JSONB for tags / manual notes. Augments,
  does not replace, `exchange_snapshots.positions`.
- `user_mgmt.position_snapshots` — TimescaleDB hypertable, 1-minute
  cadence per open position, 7-day chunks, compressed after 30 days.
  Drives waterfall window-anchored PnL deltas (today / 7D / 30D /
  since-open) and feeds MFE/MAE maintenance for `position_history`.

### Live-data infrastructure
- **Sidecar WebSocket process per venue** maintains the BloFin user-
  data + market-data streams and writes to Redis with TTLs aligned to
  the data dictionary's refresh tiers (T0 2s, T1 event-driven, T2 60s,
  T3 5m, T4 bar-close, T5 hourly). FastAPI reads from Redis only —
  no WS state inside the request/response path. No standalone WS
  gateway service in v1.
- **Implementation:** `backend/app/services/sidecars/blofin_account_sidecar.py`,
  gated by `LIVE_SIDECAR_ENABLED` (default OFF). Endpoints prefer
  Redis when sidecar heartbeat is fresh; fall back to the 5-min
  `sync_exchange_snapshots` cron-cached row otherwise. Response
  carries `stale_source` + `sidecar_stale` flags for the UI badge.

### BloFin WebSocket gotchas (verified 2026-05-01 against the prod
endpoint `wss://openapi.blofin.com/ws/private`; pinned by tests in
`tests/test_blofin_sidecar_state.py`)

The four BloFin-specific protocol details that diverge from OKX-family
expectations and would cost a day of debugging if guessed:

1. **Auth signature is `base64(hex_digest_string)`, NOT
   `base64(raw_bytes)`.** HMAC-SHA256 the message, hex-stringify the
   digest, then base64-encode those hex chars. OKX uses raw bytes →
   base64 directly; using OKX style here silently rejects every login.

2. **Login message includes a `nonce` field** (in both the args object
   and the signed string), where `nonce == timestamp`. Signed string
   is `path + method + timestamp + nonce + body` with `path=
   "/users/self/verify"`, `method="GET"`, `body=""`. OKX has no nonce.

3. **Heartbeat is application-level `"ping"` / `"pong"` text frames**
   at 15s, NOT WebSocket protocol-level Ping/Pong opcodes. Send the
   literal four-character string `"ping"`; server replies with the
   literal `"pong"` string. Use `ping_interval=None` on the
   `websockets` client so the protocol-level keepalive doesn't fight
   the application-level one.

4. **`orders` and `orders-algo` channels do NOT push initial state**
   on subscribe — only on subsequent order events. REST-seed open
   orders + TPSL orders BEFORE subscribing or the dashboard shows
   empty SL/TP cells until the first user-triggered order event.
   The `account` and `positions` channels DO push initial state.

5. **`positions` channel pushes initial snapshot BEFORE the subscribe
   ack arrives.** This was the soak-step finding (not in the docs).
   Don't read the subscribe ack inline per channel — fire all
   subscribes, then drain incoming frames in a loop dispatching
   pushes while collecting acks. Treating an early snapshot frame as
   a subscribe failure causes a backoff loop on every connect.

If a Binance sidecar is built later (currently market-data only via
public REST), Binance's gotcha list will be different — having
BloFin's documented makes the next venue's research a comparison
exercise rather than another from-scratch investigation.

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
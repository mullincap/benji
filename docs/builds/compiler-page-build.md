# Compiler Page Build — Live Spec & Progress

> **If you are reading this in a new session:** This is the durable record of an in-progress multi-phase build of the Compiler admin page. Read the **Current State** section first, then the most recently checked-off phase. To resume, start at the first unchecked phase below. Every decision and constraint is captured here so you do not need to ask the user to repeat anything.

---

## Current State

**Last updated:** Phase 0 complete (read conventions, doc created).
**Next action:** Wait for user ack, then begin Phase 1 (backend FastAPI compiler router).
**Resume command for next session:** "Resume the compiler build from `docs/builds/compiler-page-build.md`. Start at the first unchecked phase."

---

## Original spec (verbatim from user)

> Build a Compiler admin page (read-only this round) for monitoring the data ingestion pipeline. The Compiler page is a view for the admin to monitor the ingestion of raw data and equip the admin with the ability to run jobs to fill gaps/inconsistencies (trigger-jobs functionality is a follow-up phase, NOT this round).

### Scoping answers (locked decisions)

| # | Question | Answer |
|---|---|---|
| 1 | Design files | None exist. Build from scratch. |
| 2 | Scope | **B**: Frontend + read-only FastAPI GET endpoints returning real DB data. No POST. No mock data. |
| 3 | Pipeline relationship | Read-only monitor. Compiler reads `market.compiler_jobs`, `market.futures_1m` coverage, `market.symbols`. Triggering is a follow-up phase. |
| 4 | State model | FastAPI backend only. No in-memory mock state. |
| 5 | Existing frontend | None. Build `frontend/app/compiler/*` from scratch. Only navbar theme keys exist. |
| 6 | Priority | Compiler first, then Indexer. |

### Additional decisions (from clarifying questions)

| # | Decision |
|---|---|
| **Accent color** | Hard-pin to amber `#f0a500`. Ignore the user's selected navbar theme. |
| **Route location** | `frontend/app/compiler/*` — top-level peer to `trader/`. Do NOT nest under `admin/`. |
| **Auth** | Random server-side token in flat file at `backend/data/admin_sessions.json`. Do NOT use SHA256-of-passphrase. |
| **Completeness math** | A symbol-day is "complete" if `COUNT(*) >= 1440` rows in `market.futures_1m` (any row, not per-column null check). A day is "green" if ≥95% of `market.symbols WHERE active = TRUE` reach 1440 rows. |
| **Per-column completeness** | Only used on Symbols page (Symbol Inspector), not on Coverage map. |
| **Resilience** | Build doc + commits at every phase boundary. Each commit must leave repo in runnable state. |

---

## Phase 0 — Existing conventions observed

### Frontend (`frontend/app/`)

- **Framework:** Next.js 16 with App Router. Top-level routes are `app/<module>/page.tsx`.
- **Existing modules:** `trader/` (Allocator), `simulator/`. No `compiler/` or `indexer/` routes exist yet.
- **Font:** `@fontsource/space-mono` 400 + 700 imported in `app/layout.tsx`. Body font is `var(--font-space-mono), Space Mono, monospace`. Body font-size is `10px`.
- **Color system:** Defined in `app/globals.css` as CSS variables. Confirmed values:
  - Backgrounds: `--bg0` `#0C0C0E` (page), `--bg1` `#131316` (panels), `--bg2` `#1C1C21` (cards), `--bg3` `#26262D` (raised), `--bg4` same as bg3
  - Borders: `--line` and `--line2` both `#32323B`
  - Text: `--t0` `#f0ede6` (primary), `--t1` `#a09d96` (secondary), `--t2` `#5a5754` (muted), `--t3` `#bbbbbb` (hint)
  - Accents: `--green` `#00c896`, `--amber` `#f0a500`, `--red` `#ff4d4d`, plus `-dim` (12% alpha) and `-mid` (25% alpha) variants
- **Animations:** `@keyframes pulse-dot`, `spin`, `blink-cursor` already defined in globals.css. Use `pulse-dot` for the running-job indicator.
- **Layout pattern:** Each module has its own `layout.tsx` that wraps children in `<Topbar />` + `<Sidebar />` + content area. The trader layout uses `display: flex; flex-direction: column; height: 100%` with the sidebar at fixed `width: 288` (collapses to 38).
- **Sidebar conventions:**
  - Section labels: `9px 700 uppercase letter-spacing 0.12em var(--t3)`
  - Nav items: `10px var(--t2)` inactive, `var(--t0)` + `font-weight: 700` active
  - Active item has NO left border in trader (the spec says compiler should). I will add a 2px left border in `--amber` for compiler active items.
  - Hover transitions `color 0.15s ease`
  - Sidebar uses inline-styled `<button>` elements with `usePathname()` for active state
- **Cards:** `--bg2` background, `--line` border, `border-radius: 5px` (NOT 6px — the spec says 6 but the trader code uses 5; I'll match trader at 5 for consistency)
- **Tables:** Trader doesn't have a generic table component; pages build tables inline with `<table>` + inline styles. Headers are `9px 700 uppercase var(--t3)`, cells are `10px var(--t1)`.
- **Data fetching:** `process.env.NEXT_PUBLIC_API_BASE || ''` is the existing convention, used in `simulator/page.tsx` and a couple right-panel components. I will follow this.
- **Page padding:** `padding: 28px` with `maxWidth: 960` centered.
- **Charts:** `chart.js` + `react-chartjs-2` are already installed. `recharts` is also available. I'll use chart.js for the sparkline (matches existing simulator/trader use).

### Backend (`backend/app/`)

- **Framework:** FastAPI with `app.include_router()` pattern. Single existing router at `backend/app/api/routes/jobs.py` with prefix `/api/jobs`.
- **CORS:** Already configured for `http://localhost:3000` via `CORS_ORIGINS` env var. Will work as-is for dev.
- **DB connection:** `pipeline/db/connection.py` exposes `get_conn()` (raw psycopg2). The backend currently does not import this. I'll add a thin wrapper at `backend/app/db.py` that shares the same connection logic but lives inside the backend module so the import path is clean (`from app.db import get_conn`).
- **Config:** `backend/app/core/config.py` uses `pydantic_settings.BaseSettings` reading from `.env` at project root. `JOBS_DIR` etc. live there. I'll add `ADMIN_PASSPHRASE` and `ADMIN_SESSIONS_FILE` here.
- **Existing routes pattern:** Routes use `APIRouter(prefix="/api/...", tags=[...])` with pydantic `BaseModel` request bodies. Returns are usually `dict[str, Any]` or wrapped models. No formal response models in jobs.py — I'll add them for the compiler endpoints since this is a fresh router.

### Database (TimescaleDB)

- **Tables already exist** (from prior schema migrations):
  - `market.futures_1m` — hypertable, 124M rows currently, growing via the running backfill. PK `(timestamp_utc, symbol_id, source_id)`. Has `idx_futures_1m_symbol_ts (symbol_id, timestamp_utc DESC)`.
  - `market.symbols` — 756 rows, has `binance_id`, `active` columns
  - `market.sources` — 6 rows, source_id 1 = `amberdata_binance`
  - `market.compiler_jobs` — created in prior migration, has `last_heartbeat` column from latest migration
- **TimescaleDB note:** Coverage queries that scan large date ranges should use `time_bucket('1 day', timestamp_utc)` for chunk-aware grouping. Per-day `COUNT(*)` per symbol on a 90-day window over 124M rows could be expensive — must verify with `EXPLAIN` and limit default range.
- **Always filter `source_id = 1`** unless a `source` query param is provided (per spec).
- **Always join through `symbol_id`** (integer FK), never string symbol matching (per spec).

### Things to flag / known followups

1. **Trader layout uses icon SVGs in the sidebar.** The compiler spec says "no icons anywhere" — I'll build the compiler sidebar text-only as instructed, but it will visually differ from the trader sidebar. Worth noting for design review.
2. **The active item border:** Trader sidebar has no left border on active items. Compiler spec says active items have a `--green` left border, but the compiler accent is `--amber`. **I'll use `--amber` for the active border to match the module accent**, not green. Will note this in the code as a deliberate deviation from the spec wording.
3. **The page title.** Spec says "24px 700 --t0" but trader pages use `9px 700 uppercase var(--t3)` section labels and don't have a 24px page title. I'll add the 24px title as the spec asks but it will be a new convention not used elsewhere.
4. **`backend/data/` directory does not exist yet.** I'll create it during Phase 2 and add `backend/data/.gitignore` to ignore the sessions file (since it'll contain live session tokens).
5. **Coverage queries on 124M rows are the biggest performance risk.** Plan: implement with conservative `LIMIT 90` day default, add `EXPLAIN ANALYZE` to my own debugging during Phase 1. If the default 90-day query takes >2s I'll either pre-aggregate into a materialized view or shrink the default window.

---

## Phase checklist

- [x] **Phase 0** — Read conventions, create this build doc
- [ ] **Phase 1** — Backend: FastAPI compiler router (`backend/app/api/routes/compiler.py`)
  - [ ] `backend/app/db.py` — psycopg2 connection helper
  - [ ] `GET /api/compiler/coverage` — last N days symbol completeness
  - [ ] `GET /api/compiler/gaps` — incomplete days
  - [ ] `GET /api/compiler/jobs` — recent jobs with `is_stale`
  - [ ] `GET /api/compiler/jobs/{job_id}` — single job
  - [ ] `GET /api/compiler/symbols/{symbol}` — symbol inspector
  - [ ] Register router in `backend/app/main.py`
  - [ ] Smoke test each endpoint via curl
- [ ] **Phase 2** — Auth: random token sessions
  - [ ] `backend/app/services/admin_sessions.py` — flat-file token store
  - [ ] `backend/data/.gitignore`
  - [ ] `ADMIN_PASSPHRASE` env var loading via `core/config.py`
  - [ ] `POST /api/admin/login` — accepts passphrase, sets cookie
  - [ ] `POST /api/admin/logout` — clears cookie + deletes token
  - [ ] `Depends(require_admin)` dependency that compiler routes can use
  - [ ] Wire compiler routes to require admin
- [ ] **Phase 3** — Frontend shell
  - [ ] `frontend/app/compiler/layout.tsx` — sidebar + topbar + auth check
  - [ ] `frontend/app/compiler/login/page.tsx` — passphrase form
  - [ ] `frontend/app/compiler/page.tsx` — redirect to `/compiler/coverage`
  - [ ] Sidebar with text-only nav: COVERAGE / JOBS / SYMBOLS
- [ ] **Phase 4** — Coverage page (`/compiler/coverage`)
  - [ ] 4 KPI cards
  - [ ] Calendar heatmap component
  - [ ] Gap table
- [ ] **Phase 5** — Jobs page (`/compiler/jobs`)
  - [ ] Job table with status badges
  - [ ] 10s polling when any job running
  - [ ] Progress bar for in-flight jobs
- [ ] **Phase 6** — Symbols page (`/compiler/symbols`)
  - [ ] Search input
  - [ ] Endpoint completeness bars (15 columns)
  - [ ] 30-day sparkline

---

## Files touched (live log — updated as we go)

| Phase | File | Type |
|---|---|---|
| 0 | `docs/builds/compiler-page-build.md` | Created |

---

## Query Reference

### Coverage query — use in `GET /api/compiler/coverage`

Optimized to scan `market.futures_1m` only once. The inner subquery groups by `(day, symbol_id)` to get per-symbol-per-day row counts, then the outer query collapses to per-day stats using `FILTER` clauses (single pass instead of three).

```sql
SELECT day, symbols_with_data, symbols_complete, symbols_partial
FROM (
  SELECT
    time_bucket('1 day', timestamp_utc) AS day,
    COUNT(DISTINCT symbol_id) AS symbols_with_data,
    COUNT(DISTINCT symbol_id) FILTER (WHERE cnt >= 1440) AS symbols_complete,
    COUNT(DISTINCT symbol_id) FILTER (WHERE cnt BETWEEN 1 AND 1439) AS symbols_partial
  FROM (
    SELECT time_bucket('1 day', timestamp_utc) AS day, symbol_id, COUNT(*) AS cnt
    FROM market.futures_1m
    WHERE source_id = 1 AND timestamp_utc >= NOW() - INTERVAL '90 days'
    GROUP BY 1, 2
  ) sub
  GROUP BY day
  ORDER BY day DESC
) coverage;
```

Get the total active symbol count separately (cheap — 756 rows max):

```sql
SELECT COUNT(*) FROM market.symbols WHERE active = TRUE;
```

Then in Python: `symbols_missing = total_active - symbols_with_data` per day.

This means the coverage endpoint does **two** DB round-trips: one for the per-day stats above, one for the active total. Both are cached-friendly — the active count rarely changes and could be cached for 60s.

## Open questions / mid-build TODOs

(none yet)

---

## How to resume in a new session

If a session ends mid-build:
1. Read this entire file.
2. Check the **Phase checklist** for the first unchecked box.
3. Check **Files touched** to see what already exists.
4. Run `git log --oneline -10` to see what's already committed.
5. Continue from the next sub-task. Every phase is a single commit (or a small group of commits), so the working tree should always be clean at a phase boundary.
6. If you find yourself mid-phase (working tree dirty), check `git diff` to see what's in flight, then either finish the current phase or `git stash` and start a new phase.

The user has explicitly said: **"prepare proactively in advance for any abrupt stoppages so that if that happens we have minimal damage."** Honor that — commit early, commit often, update this doc at the start and end of every phase.

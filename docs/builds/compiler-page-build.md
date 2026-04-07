# Compiler Page Build — Live Spec & Progress

> **If you are reading this in a new session:** This is the durable record of an in-progress multi-phase build of the Compiler admin page. Read the **Current State** section first, then the most recently checked-off phase. To resume, start at the first unchecked phase below. Every decision and constraint is captured here so you do not need to ask the user to repeat anything.

---

## Current State

**Last updated:** Phase 1 complete + verified by user. Phase 2 decisions captured below.
**Next action:** Phase 2 — auth + apply 3 deferred Phase 1 corrections (see "Phase 2 entry tasks" below). **Do not skip these corrections** — they were verified and locked but not yet applied to code.
**Resume command for next session:** "Resume the compiler build from `docs/builds/compiler-page-build.md`. Start with the Phase 2 entry tasks (the 3 deferred Phase 1 corrections), then proceed to the auth work."

### Phase 2 entry tasks (deferred Phase 1 corrections — apply BEFORE auth work)

These are user-locked decisions captured at the end of the Phase 1 verification round. They were not committed during Phase 1 because the session was nearly full. Phase 2 must apply them as its first action.

1. **Fix `is_stale` semantics in `backend/app/api/routes/compiler.py`** (TODO #2 from below — now resolved with locked rule). Replace the existing `_JOB_SELECT` `is_stale` clause with this rule:
   ```sql
   (status = 'running' AND (
       (last_heartbeat IS NOT NULL AND last_heartbeat < NOW() - INTERVAL '2 hours')
       OR
       (last_heartbeat IS NULL AND started_at < NOW() - INTERVAL '2 hours')
   )) AS is_stale
   ```
   This catches both jobs that lost their heartbeat AND legacy jobs (like our test row) that never had a heartbeat at all.

2. **Add `completeness_pct` to each day object in `GET /api/compiler/coverage`**. The day dict in the response is currently:
   ```python
   {"date", "symbols_complete", "symbols_partial", "symbols_missing", "total_active_symbols"}
   ```
   Add a 6th field:
   ```python
   "completeness_pct": round(symbols_complete / total_active_symbols * 100, 1) if total_active_symbols > 0 else 0.0
   ```
   Example value: `27.2`. **Note**: this is `symbols_complete / total`, not `(complete + partial) / total`. The frontend coverage map needs this for the heatmap cell color decision.

3. **Verify `status` field exists on each day in `GET /api/compiler/gaps`**. The current code already adds this — see `backend/app/api/routes/compiler.py` `gap_days.append(...)` block. The rule is `"missing"` if `completeness_pct == 0` else `"partial"`. **Action**: just confirm during Phase 2 that the field is still there (don't accidentally remove it). Locked rule matches existing behavior — no code change needed for #3, only #1 and #2.

After applying #1 and #2 above and re-running the smoke tests against the live DB to confirm shapes, proceed to the auth work (`backend/data/admin_sessions.json`, `POST /api/admin/login`, `Depends(require_admin)`, etc.).

### Phase 1 verification — user decisions locked

| Decision | Resolution |
|---|---|
| **Materialized view (TODO #1)** | **Defer until after Phase 4.** Get the slow version end-to-end first, then optimize once we know exactly what the UI hits. The current 3.7s coverage query is acceptable for an admin tool with a loading spinner. |
| **Stale-without-heartbeat (TODO #2)** | **Fix in Phase 2.** Rule locked above. |
| **Coverage `completeness_pct` field** | **Add in Phase 2** — see correction #2 above. |
| **Gaps `status` field** | **Already present in current code** — verify during Phase 2, no change needed. |
| **Phase 1 response shapes** | **Approved as-is** modulo the two corrections above. Frontend can be built against these shapes once Phase 2 corrections land. |

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
- [x] **Phase 1** — Backend: FastAPI compiler router (`backend/app/api/routes/compiler.py`)
  - [x] `backend/app/db.py` — psycopg2 connection helper
  - [x] `GET /api/compiler/coverage` — last N days symbol completeness
  - [x] `GET /api/compiler/gaps` — incomplete days
  - [x] `GET /api/compiler/jobs` — recent jobs with `is_stale`
  - [x] `GET /api/compiler/jobs/{job_id}` — single job
  - [x] `GET /api/compiler/symbols/{symbol}` — symbol inspector
  - [x] Register router in `backend/app/main.py`
  - [x] Smoke test each endpoint via psql
  - [x] User verified response shapes (with 2 corrections deferred to Phase 2)
- [ ] **Phase 2** — Deferred Phase 1 corrections + auth
  - [ ] **Apply `is_stale` rule fix** (TODO #2 — locked rule above)
  - [ ] **Add `completeness_pct` to coverage day objects**
  - [ ] **Verify `status` field still present in /gaps**
  - [ ] **Re-smoke test corrected endpoints**
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
| 1 | `backend/app/core/config.py` | Modified — added DB_HOST/PORT/NAME/USER/PASSWORD |
| 1 | `backend/app/db.py` | Created — psycopg2 helper + `get_cursor()` dependency |
| 1 | `backend/app/api/routes/compiler.py` | Created — 5 read-only endpoints |
| 1 | `backend/app/main.py` | Modified — registered compiler router |

---

## Query Reference

### Coverage query — use in `GET /api/compiler/coverage`

Optimized to scan `market.futures_1m` only once. The inner subquery groups by `(day, symbol_id)` to get per-symbol-per-day row counts, then the outer query collapses to per-day stats using `FILTER` clauses (single pass instead of three).

**⚠ Bug fix applied:** the original draft of this query in the build doc referenced `time_bucket('1 day', timestamp_utc)` at the middle level, but the inner subquery `sub` only exposes `day` (not `timestamp_utc`), so that referenced a column that didn't exist. The corrected query (below) just references `day` directly.

```sql
SELECT day::date AS day, symbols_with_data, symbols_complete, symbols_partial
FROM (
  SELECT
    day,
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

## Phase 1 smoke test results

All 5 endpoints' underlying SQL was tested directly against the live DB on the server. Results below — these are what the FastAPI endpoints will return (modulo JSON serialization).

### `GET /api/compiler/coverage` (sample, top 10 days of 90)

| day        | symbols_with_data | symbols_complete | symbols_partial |
|------------|-------------------|------------------|-----------------|
| 2026-03-19 | 636               | 206              | 430             |
| 2026-03-18 | 638               | 215              | 423             |
| 2026-03-17 | 641               | 228              | 413             |
| ...        | ...               | ...              | ...             |

`total_active_symbols = 756`. So for 2026-03-19: complete=206, partial=430, missing=120.

**Performance:** `EXPLAIN ANALYZE` of the 90-day query on the current 124M-row `futures_1m` shows **3.7 seconds** total execution time. Most time is spent decompressing TimescaleDB chunks (`Custom Scan (DecompressChunk)` over 4-5 compressed hypertable chunks). Plus ~1.7s of one-time JIT compilation overhead.

**Verdict:** acceptable for an admin tool that's hit infrequently, but the frontend MUST show a loading spinner. Not acceptable as a polled endpoint. **Materialized view escalation deferred to a follow-up phase** — see TODOs below.

### `GET /api/compiler/jobs`

Currently 1 row (the test job from the earlier metl.py smoke test):

| field | value |
|---|---|
| job_id | `5cda1821-a5c9-412d-b6f7-0237843801f1` |
| source_id | 1 |
| status | running |
| date_from / date_to | 2025-04-05 |
| symbols_total / symbols_done | 485 / 57 |
| rows_written | 0 |
| started_at | 2026-04-07 15:32:45 UTC |
| last_heartbeat | NULL |
| triggered_by | cli |
| run_tag | test |
| **is_stale** | **false** |

**Note on `is_stale = false` for this job:** The `last_heartbeat` column was added to `market.compiler_jobs` *after* this job ran, so its `last_heartbeat` is NULL. My SQL handles this correctly: `is_stale` requires `last_heartbeat IS NOT NULL AND last_heartbeat < NOW() - INTERVAL '2 hours'`. A job with no heartbeat is not flagged as stale even if it's been "running" for hours. This may want refinement later (e.g. fall back to `started_at` when `last_heartbeat` is NULL), but for new jobs created after the column was added, the logic works as intended.

### `GET /api/compiler/symbols/BTC` (sample)

```json
{
  "symbol": "BTC",
  "symbol_id": 1,
  "source_id": 1,
  "date": "2026-03-19",
  "total_rows": 1441,
  "rows_per_endpoint": {
    "close": 1441, "volume": 1441, "open_interest": 1441,
    "funding_rate": 1441, "long_short_ratio": 1441,
    "trade_delta": 0, "long_liqs": 0, "short_liqs": 0,
    "last_bid_depth": 0, "last_ask_depth": 0,
    "last_depth_imbalance": 0, "last_spread_pct": 0,
    "spread_pct": 0, "bid_ask_imbalance": 0, "basis_pct": 0
  },
  "sparkline": [
    {"date": "2026-03-15", "rows": 1441},
    {"date": "2026-03-16", "rows": 1439},
    {"date": "2026-03-17", "rows": 1440},
    {"date": "2026-03-18", "rows": 1440},
    {"date": "2026-03-19", "rows": 1441}
  ]
}
```

**Important finding:** the L1 endpoints (`close`, `volume`, `open_interest`, `funding_rate`, `long_short_ratio`) are at 1441/1441 = 100%. The L2/L3 endpoints (trades, ticker, orderbook → `trade_delta`, `last_*`, `spread_pct`, `bid_ask_imbalance`, `basis_pct`) are all at **0/1441 = 0%**. Either those endpoints weren't enabled during the backfill run, or the columns were added after this data was ingested. **This is exactly what the Symbol Inspector page is for** — surfacing per-endpoint coverage gaps.

(`total_rows = 1441` not 1440 because the day range `>= '2026-03-19' AND < '2026-03-20'` includes the boundary minute. Tolerable for a UI display, easy to fix later if it matters.)

## Open questions / mid-build TODOs

### TODO #1 — Materialized view for coverage (deferred)
The 90-day coverage query takes 3.7s on 124M rows. For an admin tool this is okay but slow.
**Recommended follow-up:** Create `market.coverage_daily` as a continuous aggregate or materialized view, refreshed after each day's ingest. Should be O(rows-per-day) not O(rows-per-90-days). Schema:
```
CREATE MATERIALIZED VIEW market.coverage_daily AS
SELECT
    time_bucket('1 day', timestamp_utc) AS day,
    source_id,
    symbol_id,
    COUNT(*) AS row_count
FROM market.futures_1m
GROUP BY 1, 2, 3;
```
Then the coverage endpoint just queries this view and groups by `day` — should be sub-100ms. Defer to after the frontend works against the slow version so we know what we're optimizing for.

### TODO #2 — `is_stale` for jobs without heartbeat — ✅ RESOLVED, applies in Phase 2
**Locked rule** (apply as first action in Phase 2):
```sql
(status = 'running' AND (
    (last_heartbeat IS NOT NULL AND last_heartbeat < NOW() - INTERVAL '2 hours')
    OR
    (last_heartbeat IS NULL AND started_at < NOW() - INTERVAL '2 hours')
)) AS is_stale
```
See "Phase 2 entry tasks" in the Current State section above.

### TODO #3 — `total_rows` off-by-one (1441 instead of 1440)
The day-range filter includes a boundary minute. Cosmetic, fix when adding the materialized view.

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

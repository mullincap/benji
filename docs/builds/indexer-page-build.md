# Indexer Page Build — Live Spec & Progress

> **If you are reading this in a new session:** This is the durable record of the in-progress Indexer admin page build. Read the **Current State** section first, then the most recently checked-off phase. To resume, start at the first unchecked phase below. Every decision and constraint is captured here so you do not need to ask the user to repeat anything.

> **Sister build**: `docs/builds/compiler-page-build.md` is the proven template this build copies. The Compiler module is now in production with 6 phases shipped (commits `97ada61` through `135665f`). The Indexer follows the same shape (FastAPI router + auth-protected route group + sub-pages) and reuses the existing admin auth wholesale.

---

## Current State

**Last updated:** Phases 5 and 6 complete. **All phases of the indexer page build are shipped.**
**Next action:** None — the indexer admin page is feature-complete for this round (read-only). Follow-up work (out of scope here) is tracked separately:
  1. Wire `build_intraday_leaderboard.py` to record runs in `market.indexer_jobs` (mirror what was done for `metl.py → compiler_jobs`)
  2. Fix Bug B (OOM in row-group iteration) so the cron can pass `--parquet-path` and produce fresh leaderboards nightly
  3. Add POST/trigger endpoints for ad-hoc job runs from the UI (this entire round was read-only by design)
**Resume command for next session:** N/A — build is complete. If further indexer work is needed, start a new build doc or extend this one with a Phase 7+ section.

### Phase 5 — what shipped

- `frontend/app/indexer/(protected)/signals/page.tsx` — full Signals page replacing the placeholder. Source filter chips (All · Live · Backtest · Research) trigger a re-fetch with the `source=` query param so the server filters at the SQL layer.
- Table columns: Date · Source · Status (sit_flat badge) · Filter · Version · Symbols (count). Click-row toggles inline expand showing the full symbol list with rank/base/weight chips, plus the `filter_reason` if present.
- The list endpoint already pre-aggregates symbols via LATERAL/jsonb_agg, so the inline expand uses the row's existing `symbols` array — no second fetch to `/api/indexer/signals/{id}` needed.
- No polling — daily_signals advances at most a few times a day; manual refresh is fine.

### Phase 6 — what shipped

- `frontend/app/indexer/(protected)/strategies/page.tsx` — full Strategies page replacing the placeholder. One card per strategy, each showing the display_name (or name fallback), Published/Draft badge, strategy_id, description, and a 4-field metadata grid (Filter Mode · Capital Cap · Created · Updated).
- Versions section under each strategy lists every version with its label (or first 8 chars of UUID), Active badge if `is_active`, published_at + created_at dates, and a `config_excerpt` `<pre>` block (first 200 chars of the JSONB config, server-side truncation).
- Single fetch on mount, no polling. Read-only — no edit affordances this round.
- Build verification: `next build` clean, all 6 indexer routes present.

### Phase 4 — what shipped

- `frontend/app/indexer/(protected)/jobs/page.tsx` — full Jobs page replacing the placeholder. Polling loop, 1s ticker, status badges, duration formatter all lifted verbatim from `compiler/(protected)/jobs/page.tsx`.
- Filter chips: All · Leaderboard · Overlap · Full. Counts shown per chip, computed from the unfiltered job list. Active chip uses `var(--module-accent)` border.
- Table columns: Type · Metric · Date Range · Status · Symbols Done/Total · Rows Written · Duration · Triggered By. Running jobs render a `ProgressBar` instead of a flat fraction.
- Empty state: dedicated card explaining that `market.indexer_jobs` is currently empty because no script writes to it yet, with the wiring follow-up referenced. Renders day one without implying user error.
- Build verification: `next build` clean, all 6 indexer routes still present.

### Phase 3 — what shipped

- `frontend/app/indexer/(protected)/coverage/page.tsx` — full Coverage page replacing the placeholder. Renders one block per metric (Price / Open Interest / Volume), each with 4 KPI cards (Days Complete / Days Partial / Days Missing / Most Recent Day) plus a calendar heatmap. Below all three: a 4-state legend and a gap table grouped by metric (most recent 50 rows).
- The frontend synthesizes the full date axis from `today - lookback_days` so days the API omits (because the indexer never wrote any rows for that metric on that date) still render as "no-data" cells. This is the honest "broken state" visualization specified in the build doc.
- Thresholds: ≥ 95% = complete (green), 5–94% = partial (amber), < 5% = missing (red), no row at all = no-data (bg2/line).
- Single fetch to `GET /api/indexer/coverage?days=90`. No polling — coverage moves at most once a day.
- Build verification: `next build` clean. All 6 indexer routes still present.

### Phase 2 — what shipped

- `frontend/app/components/Topbar.tsx` — added `href: '/indexer'` to the indexer entry in `MODULES`. The active-route accent now picks up `theme.colors.indexer` automatically when the user navigates to `/indexer/*`.
- `frontend/app/indexer/(protected)/layout.tsx` — auth-checked layout with `IndexerSidebar` (200px, 9px "INDEXER" label, 4 nav items, 2px left border in `var(--module-accent)` for active). Verifies session via `GET /api/admin/whoami`, redirects unauth users to `/indexer/login`.
- `frontend/app/indexer/(protected)/page.tsx` — server-side redirect to `/indexer/coverage`.
- `frontend/app/indexer/(protected)/{coverage,jobs,signals,strategies}/page.tsx` — four placeholder pages, each with the locked convention (9px section label, 24px page title, one `--bg2` card describing what the corresponding phase will render).
- `frontend/app/indexer/(public)/layout.tsx` — minimal centered layout that renders Topbar so `--module-accent` is set on the login redirect path.
- `frontend/app/indexer/(public)/login/page.tsx` — stub that `router.replace('/compiler/login')`. Auth is shared (same `admin_session` cookie), so duplicating the form would be churn.
- Verification: `next build` clean. All 6 indexer routes appear in the static route table: `/indexer`, `/indexer/coverage`, `/indexer/jobs`, `/indexer/signals`, `/indexer/strategies`, `/indexer/login`.

### Phase 1 — what shipped (commits `f73083c` and `b95db30`)

- All 3 entry tasks applied (docstring fix, cookie rename to `admin_session`, `last_heartbeat` verified)
- 6 read-only FastAPI endpoints built in `backend/app/api/routes/indexer.py` and registered in `main.py`:
  - `GET /api/indexer/coverage?days=N` — per-day, per-metric leaderboard completeness, strict math `1440 × 333 = 479520`
  - `GET /api/indexer/jobs?limit=50` — recent `market.indexer_jobs` rows with computed `is_stale`
  - `GET /api/indexer/jobs/{job_id}` — single job by UUID
  - `GET /api/indexer/signals?days=N&source=live|backtest|research` — daily signals with symbol list pre-aggregated via LATERAL/jsonb_agg
  - `GET /api/indexer/signals/{signal_batch_id}` — single batch with full symbol list
  - `GET /api/indexer/strategies` — all strategies with versions nested via LATERAL/jsonb_agg
- All 6 endpoints wired with `dependencies=[Depends(require_admin)]` at the router level — same auth as compiler, sharing the renamed `admin_session` cookie
- Smoke tests: 6/6 TestClient (route registration + auth + cookie rename verification) + 4/4 SQL against live DB (all 4 query patterns return real data with the expected shapes)
- Build doc updated with the cron diagnostic findings (cron has been a no-op since creation, master parquets stale at 2026-04-06, OOM bug B blocks the cron fix)

### Phase 2 plan (locked)

**Goal**: build the frontend shell so all 6 indexer routes are reachable in `next build` with the proper auth gating, sidebar navigation, and theme accent. No data fetching yet — Phases 3-6 will fill in the data pages against the real API endpoints from Phase 1.

**File structure** — same as the compiler shell (Option A: Next.js route groups):

```
frontend/app/indexer/
├── (protected)/
│   ├── layout.tsx                     ← whoami check, redirect to login if unauth, Topbar + sidebar
│   ├── page.tsx                       ← server-side redirect to /indexer/coverage
│   ├── coverage/page.tsx              ← Phase 3 placeholder
│   ├── jobs/page.tsx                  ← Phase 4 placeholder
│   ├── signals/page.tsx               ← Phase 5 placeholder
│   └── strategies/page.tsx            ← Phase 6 placeholder
└── (public)/
    ├── layout.tsx                     ← minimal centered, renders Topbar so --module-accent is set
    └── login/page.tsx                 ← passphrase form (or just redirect to /compiler/login — see decision below)
```

**Login page reuse decision**: the auth backend is shared (same `require_admin` dependency, same `admin_session` cookie, same `POST /api/admin/login`). The user instruction is **"reuse the existing /compiler/login page"** — so the indexer's `(public)/login/page.tsx` should be a tiny stub that **redirects to `/compiler/login`** rather than duplicating the form. After successful login at `/compiler/login`, the user is redirected back to `/compiler` per the existing code; they would then have to manually navigate to `/indexer`. This is acceptable for now (admin-only tool, single user). If this UX is annoying, a small enhancement can pass a `?return_to=/indexer` query param to the compiler login page in a follow-up — but not in this round.

**Sidebar nav** (4 items, mirroring compiler's text-only style):
- **COVERAGE** → `/indexer/coverage`
- **JOBS** → `/indexer/jobs`
- **SIGNALS** → `/indexer/signals`
- **STRATEGIES** → `/indexer/strategies`

200px wide, 9px section label "INDEXER" at top, 10px nav items, 2px left border in `var(--module-accent)` for the active item. Identical structure to `frontend/app/compiler/(protected)/layout.tsx` `CompilerSidebar` component — just renamed and with 4 items instead of 3.

**Topbar integration**: add `href: '/indexer'` to the indexer entry in the `MODULES` array in `frontend/app/components/Topbar.tsx`. The indexer module already has theme colors in every theme entry — the navbar will pick up the active theme's `theme.colors.indexer` automatically when the user navigates to `/indexer/*`. **No hardcoded accent color** — the locked decision in this build doc is "follow the active theme like every other module, no override."

**Module accent inheritance**: the protected layout's sidebar active border uses `var(--module-accent)`, which the Topbar's `applyAccent()` sets on `:root` whenever the active route is under `/indexer/*`. Same mechanism the compiler uses — no extra wiring needed beyond the `MODULES` href update.

**Placeholder page convention** (lifted from compiler Phase 3):
- 9px section label (e.g. "Indexer · Coverage")
- 24px page title (e.g. "Coverage Map")
- One `--bg2` card with a "Phase X Placeholder" label and a one-paragraph description of what will render there

**Verification**:
- `tsc --noEmit` clean
- `next build` succeeds
- All 6 indexer routes appear in the static route table: `/indexer`, `/indexer/coverage`, `/indexer/jobs`, `/indexer/signals`, `/indexer/strategies`, `/indexer/login`

### Things to flag before Phase 2 starts (none currently — checkpoint clean)

The Phase 1 entry tasks were all resolved during Phase 1. No new issues surfaced that need user input. Phase 2 should be heads-down execution following the compiler shell template.

### Phase 1 entry tasks (small fixes flagged during Phase 0)

These are not blockers — they're cleanups I noticed while reading the existing code. Apply them as the first action of Phase 1 so the codebase stays consistent.

1. **Stale docstring in `frontend/app/compiler/(protected)/layout.tsx` line 13.** The docstring says *"Topbar (shared component, hardcoded amber accent for compiler module)"* but the hardcoded amber accent was removed in commit `244c633`. The actual code at line 47 correctly uses `var(--module-accent)`. Fix the comment to match.

2. **The auth cookie is named `compiler_session`** (`backend/app/api/routes/admin.py` line 44). This was fine when only the compiler used it, but the Indexer will share the same auth. The cookie name should be `admin_session` (or similar generic name) before the Indexer ships. **Decision needed**: rename the cookie now (one-line backend change + one-line frontend constant rename) and force a re-login, OR leave it and accept the misleading name. My recommendation: **rename** — it's a 5-minute change before any Indexer code lands, and the user is the only one with an active session anyway.

3. **`market.indexer_jobs` has no `last_heartbeat` column.** The Compiler `compiler_jobs` table got `last_heartbeat` added in a follow-up migration after I built the page. The Indexer table was created without it. **Decision needed**: add it now via a tiny migration so the Indexer Jobs page can use the same `is_stale` rule the Compiler uses, OR start the Indexer page with a simpler `is_stale` rule that just looks at `started_at`. My recommendation: **add the column now** — it's a 2-line `ALTER TABLE` and keeps the two job tables structurally identical.

---

## Original spec (verbatim from user)

> Build an Indexer admin page (read-only this round) for monitoring the construction of market indices/leaderboards. The Indexer page is a view for the admin to monitor the construction of market indices and equip the admin with the ability to run jobs to fill gaps/inconsistencies (trigger-jobs functionality is a follow-up phase, NOT this round).

### Scoping answers (locked decisions, inherited from compiler build)

| # | Question | Answer |
|---|---|---|
| 1 | Design files | None exist. Build from scratch using the same patterns as the Compiler page. |
| 2 | Scope | **B**: Frontend + read-only FastAPI GET endpoints returning real DB data. No POST. No mock data. |
| 3 | Pipeline relationship | Read-only monitor. Indexer reads `market.leaderboards`, `market.indexer_jobs`, `user_mgmt.daily_signals` + `daily_signal_items`. Triggering is a follow-up phase. |
| 4 | State model | FastAPI backend only. No in-memory mock state. |
| 5 | Existing frontend | None at `/indexer/*`. Build `frontend/app/indexer/*` from scratch as a top-level peer to `/compiler/*` and `/trader/*`. The navbar already has `indexer` as a module key. |
| 6 | Auth | **Reuse the existing admin auth wholesale.** Same `require_admin` dependency, same login route, same session cookie. No second login. |
| 7 | Theme accent | Follow the active theme via `var(--module-accent)`. Same approach as the Compiler page (no hardcoded hex). |
| 8 | Route location | `frontend/app/indexer/*` — top-level peer, NOT nested under `admin/`. |
| 9 | Resilience | Build doc + commits at every phase boundary. Each commit must leave repo in runnable state. |

---

## Phase 0 — Existing conventions observed

### What's already shipped that the Indexer can reuse as-is

- **Admin auth backend**: `backend/app/api/routes/admin.py` exposes `POST /api/admin/login`, `POST /api/admin/logout`, `GET /api/admin/whoami`, and `require_admin(request)` as a `Depends`-able function. The Indexer router will mount this dependency at the router level the same way the Compiler does:
  ```python
  router = APIRouter(
      prefix="/api/indexer",
      tags=["indexer"],
      dependencies=[Depends(require_admin)],
  )
  ```
- **Session store**: `backend/app/services/admin_sessions.py` flat-file token store in `backend/data/admin_sessions.json`. Already proven, no changes needed.
- **DB connection**: `backend/app/db.py` `get_cursor()` FastAPI dependency. Returns a `RealDictCursor`, handles connection failures as 503, query errors as 500. Reused as-is.
- **Frontend route group pattern**: `frontend/app/compiler/(protected)/` + `frontend/app/compiler/(public)/` is the proven structure for "some pages need auth, login does not." Mirror exactly for `frontend/app/indexer/`.
- **Sidebar pattern**: 200px text-only nav, 9px section label, 10px items, 2px left border in `var(--module-accent)` for active. Lift the `NavItem` + `Sidebar` shape from the Compiler layout.
- **Topbar integration**: `Topbar.tsx` already has `indexer` as a `ModuleKey` and a theme color in every theme entry. **One small change needed** in Phase 3: add `href: '/indexer'` to the indexer entry in the `MODULES` array (currently has no href, mirroring how compiler was missing one before Phase 3 of that build).
- **API contract conventions**:
  - All fetches use `process.env.NEXT_PUBLIC_API_BASE || ''`
  - All fetches include `credentials: 'include'`
  - TypeScript types mirror FastAPI response shapes exactly — they are the contract
- **Status badge pattern**: 6 states (`COMPLETE` `RUNNING` `FAILED` `STALE` `CANCELLED` `QUEUED`), pulsing dot for live running jobs, STALE overrides RUNNING. Lift verbatim from `compiler/(protected)/jobs/page.tsx`.
- **Polling pattern**: 10s polling when any job is running, paused on `document.visibilityState === "hidden"`, 1s ticker for live duration column. Lift verbatim from `compiler/(protected)/jobs/page.tsx`.
- **Duration formatter**: `12s` / `2m 14s` / `1h 22m` / `1d 4h`. Lift verbatim.
- **24px page title** + **6px card border-radius** + **9px section labels** + **no icons in sidebar** — all locked from the Compiler build.

### Indexer-specific conventions to establish

- **Module accent**: Whatever the active theme assigns to `indexer`. From the THEMES table in `Topbar.tsx`: spectrum=`#38B4FF` (blue), terminal=`#39F084` (lime), institutional=`#5BA3D9` (muted blue), electric=`#00E5C8` (teal). User picks via the navbar theme dropdown — we don't override.
- **Pipeline scripts the page monitors**:
  - `pipeline/indexer/build_intraday_leaderboard.py` — runs nightly at 01:00, 01:05, 01:10 UTC for `--metric price`, `--metric open_interest`, `--metric volume`. Writes parquet files to `/mnt/quant-data/leaderboards/{metric}/`. **Currently does NOT write to `market.indexer_jobs` or `market.leaderboards`** — the leaderboards table is populated only via the one-shot `pipeline/db/backfill_leaderboards.py`.
  - `pipeline/overlap_analysis.py` — produces overlap pools (the Tail+Dispersion strategy's daily symbol picks). Not on a cron yet but referenced in `daily_signal.py`. Writes deploys CSV.
- **Three job types** (per the `indexer_jobs.job_type` CHECK constraint):
  - `leaderboard` — a single leaderboard build run
  - `overlap` — an overlap analysis run
  - `full` — a combined leaderboard + overlap run
- **Three metrics** (per the `RANK_METRIC` validation in `build_intraday_leaderboard.py`):
  - `price`
  - `open_interest`
  - `volume`
  Each metric produces its own leaderboard artifacts.

### Database schema observed (Phase 0 inspection)

#### `market.leaderboards`
- TimescaleDB hypertable, **31,938,971 rows** as of 2026-04-08
- PK: `(timestamp_utc, metric, variant, anchor_hour, rank)`
- Indexes: `(metric, variant, timestamp_utc DESC)`, `(symbol_id, metric, timestamp_utc DESC)`, `(timestamp_utc DESC)`
- FK: `symbol_id → market.symbols(symbol_id)`
- **Current state**: only `(metric='price', variant='close', anchor_hour=0)` rows exist. The OI and volume metrics have **never been backfilled to the DB**, only to disk parquet files. Most recent row: `2026-03-19 23:59` UTC.
- **`pct_change` is NULL for all rows** — known limitation from the Compiler build's TODO #1 (the wide-format parquet backfill couldn't reverse-derive percentage values; the value gets populated only by the live indexer-to-DB write path that doesn't exist yet).

#### `market.indexer_jobs`
- Plain table (NOT a hypertable)
- 17 columns, all the standard job-tracking fields plus `params JSONB` and the `job_type` enum
- **Currently 0 rows** — no script has ever written to this table. The Indexer page will be inspecting an empty table at first; the empty state must be designed in from day one.
- **Missing column**: `last_heartbeat TIMESTAMPTZ`. The Compiler `compiler_jobs` table got this in a follow-up migration. The Indexer table was created earlier without it. See Phase 1 entry task #3.

#### `user_mgmt.daily_signals` + `daily_signal_items`
- `daily_signals`: 1 row currently (the test signal from the daily_signal.py task — `signal_date=2026-04-07`, `filter_name='Tail + Dispersion'`, `sit_flat=false`)
- `daily_signal_items`: 9 rows (the symbols selected in that test signal)
- Has `signal_source` enum (`live`/`backtest`/`research`) — the Indexer page should let an admin filter by source, since it makes a big difference whether they're inspecting live signals or backtest signals
- `audit.strategy_versions` foreign key — joining gets us the version label (e.g. `v1.0`) and the strategy name from `audit.strategies`

### Indexer pipeline scripts surveyed

#### `pipeline/indexer/build_intraday_leaderboard.py` (538 lines)
- CLI: `--metric {price|open_interest|volume}` (required), `--parquet-path`, `--output-dir`, `--index-lookback`, `--deployment-start-hour`
- Reads master parquet from `PARQUET_DIR`
- Computes per-minute leaderboard (top `TOP_N=333` symbols) ranked by % change vs anchor_hour
- Writes wide-format parquet files to `/mnt/quant-data/leaderboards/{metric}/`
- **Does NOT write to the DB.** `market.leaderboards` is populated only by `pipeline/db/backfill_leaderboards.py` which reads the parquet files.
- Three nightly cron entries (one per metric).

#### `pipeline/overlap_analysis.py` (1327 lines)
- The Tail+Dispersion strategy's overlap pool builder
- CLI: `--metric`, plus many strategy params
- Reads filtered leaderboard parquets
- Computes daily overlap (intersection of price + OI top symbols by frequency)
- Writes deploys CSV consumed by `daily_signal.py` and `audit.py`
- **Not on a cron yet.** The live signal generator (`/root/benji/daily_signal.py`) does its own overlap computation against live Binance data, separate from this batch script.

### Things to flag / known followups (will be reflected in Phase 1+)

1. **`market.indexer_jobs` is empty.** The Indexer page will show an empty table on day one for the Jobs view. Build a clear empty state with a one-line description of the underlying issue: *"No indexer jobs have been recorded. The current indexer cron writes parquet files but does not yet record runs to `market.indexer_jobs`. To enable run tracking, wire `build_intraday_leaderboard.py` to write to `market.indexer_jobs` (TODO follow-up phase, mirrors what was done for `metl.py` → `compiler_jobs`)."*

2. **The Indexer "Coverage" view is conceptually different from the Compiler one.** The Compiler page measures completeness as "% of active symbols that hit 1440 rows in `futures_1m` per day." The Indexer needs to measure something different — probably "% of expected leaderboard timestamps that exist in `market.leaderboards` per day per metric." For a daily build at midnight, expected = 1440 minutes per day per metric. **Decision needed in Phase 1**: confirm the completeness math before writing the SQL.

3. **The Indexer page has more entities to display than the Compiler.** Compiler had 3 sub-pages (Coverage, Jobs, Symbols). Indexer needs at least:
   - **Coverage** — leaderboard completeness per metric per day
   - **Jobs** — `market.indexer_jobs` table with job_type filter
   - **Signals** — `user_mgmt.daily_signals` history (live + backtest, filterable by source)
   - **Strategies** — list `audit.strategies` + `strategy_versions` so the admin can see what's published
   - (Optional) **Leaderboard inspector** — pick a date + metric, see the top N symbols for that day
   
   **Decision needed in Phase 1**: confirm the sub-page list. My recommendation: start with **Coverage / Jobs / Signals / Strategies** as 4 sub-pages, defer the leaderboard inspector to a follow-up. That gives the Indexer the same "monitor + inspect" shape as the Compiler without ballooning the scope.

4. **`market.leaderboards.pct_change` is NULL for all 31.9M rows.** This is the same column noted in the Compiler build doc (TODO #1 deferred). The Indexer page will need to handle NULL pct_change gracefully — either by displaying "—" in the leaderboard inspector (if we build it) or by not showing pct_change at all. The locked decision from the Compiler build was *"defer the materialized view + populate-on-write fix until after the page ships"* — same applies here.

5. **No cookie rename** = mild UX wart. The session cookie named `compiler_session` will work for the Indexer too, just with the wrong name on the wire. Phase 1 entry task #2 above addresses this. The user should make the call.

---

## Phase checklist

- [x] **Phase 0** — Read conventions, create this build doc
- [x] **Phase 1** — Backend: FastAPI indexer router (`backend/app/api/routes/indexer.py`)
  - [x] Apply Phase 1 entry task #1 (fix stale Compiler layout docstring)
  - [x] Apply Phase 1 entry task #2 (rename cookie to `admin_session`)
  - [x] Apply Phase 1 entry task #3 (`last_heartbeat` verified — already migrated in checkpointing work)
  - [x] `GET /api/indexer/coverage` — leaderboard completeness per metric per day
  - [x] `GET /api/indexer/jobs` — recent indexer_jobs with `is_stale`
  - [x] `GET /api/indexer/jobs/{job_id}` — single job
  - [x] `GET /api/indexer/signals` — recent daily_signals with optional `?source=` filter
  - [x] `GET /api/indexer/signals/{signal_batch_id}` — signal items for one batch
  - [x] `GET /api/indexer/strategies` — strategies + versions
  - [x] Register router in `backend/app/main.py`
  - [x] TestClient smoke test 6/6 PASS (route registration, auth 401, login, cookie rename, post-login 503-on-DB)
  - [x] SQL smoke test 4/4 PASS against live DB (real response shapes captured)
- [x] **Phase 2** — Frontend shell
  - [x] `frontend/app/indexer/(protected)/layout.tsx` — auth check + Topbar + sidebar
  - [x] `frontend/app/indexer/(protected)/page.tsx` — server redirect to `/indexer/coverage`
  - [x] `frontend/app/indexer/(protected)/coverage/page.tsx` — Phase 3 placeholder
  - [x] `frontend/app/indexer/(protected)/jobs/page.tsx` — Phase 4 placeholder
  - [x] `frontend/app/indexer/(protected)/signals/page.tsx` — Phase 5 placeholder
  - [x] `frontend/app/indexer/(protected)/strategies/page.tsx` — Phase 6 placeholder
  - [x] `frontend/app/indexer/(public)/layout.tsx` — minimal centered (renders Topbar)
  - [x] `frontend/app/indexer/(public)/login/page.tsx` — redirect stub to `/compiler/login`
  - [x] `Topbar.tsx` — add `href: '/indexer'` to the indexer MODULES entry
  - [x] Verified via `next build` that all 6 indexer routes appear
- [x] **Phase 3** — Coverage page (`/indexer/coverage`)
  - [x] KPI cards (Days Complete / Days Partial / Days Missing / Most Recent Day) per metric
  - [x] Per-metric heatmap (3 heatmaps stacked: price, open_interest, volume)
  - [x] Gap table grouped by metric
- [x] **Phase 4** — Jobs page (`/indexer/jobs`)
  - [x] Empty state for the current 0-row reality
  - [x] Job table with `job_type` filter chips (leaderboard / overlap / full / all)
  - [x] Status badges + polling + live duration (lifted from compiler/jobs)
- [x] **Phase 5** — Signals page (`/indexer/signals`)
  - [x] Filter chips: source (live / backtest / research / all)
  - [x] Date column + strategy version label + sit_flat badge + filter_name
  - [x] Click row → inline expand showing symbol list (uses pre-aggregated data, no detail fetch)
- [x] **Phase 6** — Strategies page (`/indexer/strategies`)
  - [x] Strategy list with versions nested under each
  - [x] Show `is_active`, `published_at`, `config` JSONB excerpt
  - [x] Read-only — no edit affordances this round

---

## Files touched (live log — updated as we go)

| Phase | File | Type |
|---|---|---|
| 0 | `docs/builds/indexer-page-build.md` | Created |
| 1 | `frontend/app/compiler/(protected)/layout.tsx` | Modified — fixed stale docstring (entry task #1) |
| 1 | `backend/app/api/routes/admin.py` | Modified — renamed COOKIE_NAME compiler_session → admin_session (entry task #2) |
| 1 | `backend/app/api/routes/compiler.py` | Modified — updated stale comment to match new cookie name |
| 1 | `frontend/app/compiler/(public)/login/page.tsx` | Modified — updated stale comment to match new cookie name |
| 1 | `backend/app/api/routes/indexer.py` | Created — 6 read-only endpoints (coverage, jobs, jobs/{id}, signals, signals/{id}, strategies) |
| 1 | `backend/app/main.py` | Modified — registered indexer_router |
| 2 | `frontend/app/components/Topbar.tsx` | Modified — added `href: '/indexer'` to MODULES |
| 2 | `frontend/app/indexer/(protected)/layout.tsx` | Created — auth check + IndexerSidebar (4 nav items) |
| 2 | `frontend/app/indexer/(protected)/page.tsx` | Created — server redirect to /indexer/coverage |
| 2 | `frontend/app/indexer/(protected)/coverage/page.tsx` | Created — Phase 3 placeholder |
| 2 | `frontend/app/indexer/(protected)/jobs/page.tsx` | Created — Phase 4 placeholder |
| 2 | `frontend/app/indexer/(protected)/signals/page.tsx` | Created — Phase 5 placeholder |
| 2 | `frontend/app/indexer/(protected)/strategies/page.tsx` | Created — Phase 6 placeholder |
| 2 | `frontend/app/indexer/(public)/layout.tsx` | Created — minimal centered + Topbar |
| 2 | `frontend/app/indexer/(public)/login/page.tsx` | Created — redirect stub to /compiler/login |
| 3 | `frontend/app/indexer/(protected)/coverage/page.tsx` | Replaced placeholder — full Coverage page (per-metric KPIs + heatmaps + gap table) |
| 4 | `frontend/app/indexer/(protected)/jobs/page.tsx` | Replaced placeholder — full Jobs page (filter chips + polling table + empty state) |
| 5 | `frontend/app/indexer/(protected)/signals/page.tsx` | Replaced placeholder — full Signals page (source chips + click-to-expand symbol list) |
| 6 | `frontend/app/indexer/(protected)/strategies/page.tsx` | Replaced placeholder — full Strategies page (cards with nested versions + config excerpts) |

---

## Cron situation (as of 2026-04-08)

Diagnosed before Phase 1 started. Important context for the Coverage page's "honest empty state" later in this build.

### Current state

- **The indexer cron has been a no-op since creation.** The crontab entries (3 lines, one per metric) invoke `build_intraday_leaderboard.py --metric <m>` with **no `--parquet-path` arg**. Without that arg the script falls into partition mode, scans `/mnt/quant-data/compiled/` for `date=*` directories, finds zero, and exits cleanly with no work done. No log file is ever produced because there's no stdout/stderr in the empty-input path.
- **The master leaderboard parquets that DO exist** at `/mnt/quant-data/leaderboards/{price,open_interest}/intraday_pct_leaderboard_*_top333_anchor0000_ALL.parquet` (229 MB and 227 MB) were written by a **single manual run on 2026-04-06** (mtimes 23:08 / 23:37 UTC). Both files contain `573,120` rows spanning **2025-02-13 → 2026-03-19** (1440 min/day × 398 days). They have not been modified by any cron run.
- **The `volume` master parquet has never been built.** `/mnt/quant-data/leaderboards/volume/` is empty. The volume cron line has never produced output. Cause is the same as the other two metrics (no `--parquet-path`) plus possibly a separate volume-specific bug in the cumsum logic.
- **`market.leaderboards`** therefore has data only from the one-shot historical backfill via `pipeline/db/backfill_leaderboards.py` (run manually on 2026-04-06): `31,938,971` rows of `(price, anchor_hour=0)` covering the same 2025-02-13 → 2026-03-19 range as the parquets. **The DB is frozen at 2026-03-19** — 19+ days stale and growing every day until the cron is fixed.
- **`market.indexer_jobs`** is empty (`0` rows after the smoke-test cleanup).

### Bug B (OOM) blocks the cron fix

`build_intraday_leaderboard.py` cannot safely use `--parquet-path /mnt/quant-data/raw/amberdata/master_data_table.parquet` because `pd.read_parquet(path, columns=[...])` loads all 312M rows into RAM before the column filter takes effect, OOM-killing the kernel (verified `exit=137` on the server). The script needs row-group iteration like `pipeline/db/backfill_futures_1m.py` uses before this is viable.

### Workaround until Bug B is fixed

To populate `market.leaderboards` with new data, manually:

1. Run `build_intraday_leaderboard.py` against a dated subset of the master parquet (or against a smaller compiled-by-day file if one exists)
2. Run `pipeline/db/backfill_leaderboards.py` to load the resulting parquet into TimescaleDB

This is what was done on 2026-04-06 to produce the current data. There is no automation for this step yet.

### Cron fix is deferred

Updating the crontab to pass `--parquet-path` would just cause OOM kills every night. The fix sequence is:

1. **First**: rewrite `build_intraday_leaderboard.py` to read the master parquet via row-group iteration (separate task, not in scope for the indexer page build)
2. **Then**: update the crontab to pass `--parquet-path` and chain the three metrics sequentially (proposed in the cron diagnostic, not yet applied)
3. **Then**: the nightly indexer cron starts producing fresh `market.leaderboards` data automatically, with the new checkpointing skipping any dates already processed

**Until that sequence is complete, the Indexer Coverage page will show this honest "stale at 2026-03-19" reality.** That's the intended behavior — the page is supposed to surface broken pipeline state, not hide it.

## Open questions / mid-build TODOs

(none yet beyond the Phase 1 entry tasks above and the decisions inside the "Things to flag" section)

---

## How to resume in a new session

If a session ends mid-build:
1. Read this entire file.
2. Check the **Phase checklist** for the first unchecked box.
3. Check **Files touched** to see what already exists.
4. Run `git log --oneline -10` to see what's already committed.
5. Continue from the next sub-task. Every phase is a single commit (or a small group of commits), so the working tree should always be clean at a phase boundary.
6. If you find yourself mid-phase (working tree dirty), check `git diff` to see what's in flight, then either finish the current phase or `git stash` and start a new phase.

The user has explicitly said: **"prepare proactively in advance for any abrupt stoppages so that if that happens we have minimal damage."** Honor that — commit early, commit often, update this doc at the start and end of every phase. Same resilience strategy used for the Compiler build.

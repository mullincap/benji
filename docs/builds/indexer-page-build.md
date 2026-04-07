# Indexer Page Build — Live Spec & Progress

> **If you are reading this in a new session:** This is the durable record of the in-progress Indexer admin page build. Read the **Current State** section first, then the most recently checked-off phase. To resume, start at the first unchecked phase below. Every decision and constraint is captured here so you do not need to ask the user to repeat anything.

> **Sister build**: `docs/builds/compiler-page-build.md` is the proven template this build copies. The Compiler module is now in production with 6 phases shipped (commits `97ada61` through `135665f`). The Indexer follows the same shape (FastAPI router + auth-protected route group + sub-pages) and reuses the existing admin auth wholesale.

---

## Current State

**Last updated:** Phase 0 — read existing conventions (Compiler frontend now part of those conventions), DB schema inspected, indexer pipeline scripts surveyed. Build doc created.
**Next action:** Wait for user ack on Phase 0 report, then begin Phase 1 (FastAPI indexer router). The Phase 1 entry tasks include three small **carry-over fixes** flagged during Phase 0 (see "Phase 1 entry tasks" below).
**Resume command for next session:** "Resume the indexer build from `docs/builds/indexer-page-build.md`. Start at the first unchecked phase."

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
- [ ] **Phase 1** — Backend: FastAPI indexer router (`backend/app/api/routes/indexer.py`)
  - [ ] Apply Phase 1 entry task #1 (fix stale Compiler layout docstring)
  - [ ] Apply Phase 1 entry task #2 (rename cookie to `admin_session`) — IF user approves
  - [ ] Apply Phase 1 entry task #3 (add `last_heartbeat` to `indexer_jobs`) — IF user approves
  - [ ] `GET /api/indexer/coverage` — leaderboard completeness per metric per day
  - [ ] `GET /api/indexer/gaps` — leaderboard days with missing/partial coverage
  - [ ] `GET /api/indexer/jobs` — recent indexer_jobs with `is_stale`
  - [ ] `GET /api/indexer/jobs/{job_id}` — single job
  - [ ] `GET /api/indexer/signals` — recent daily_signals with optional `?source=` filter
  - [ ] `GET /api/indexer/signals/{signal_batch_id}` — signal items for one batch
  - [ ] `GET /api/indexer/strategies` — strategies + versions
  - [ ] Register router in `backend/app/main.py`
  - [ ] Smoke test each endpoint via psql + capture response shapes
- [ ] **Phase 2** — Frontend shell
  - [ ] `frontend/app/indexer/(protected)/layout.tsx` — auth check + Topbar + sidebar
  - [ ] `frontend/app/indexer/(protected)/page.tsx` — server redirect to `/indexer/coverage`
  - [ ] `frontend/app/indexer/(protected)/coverage/page.tsx` — Phase 3 placeholder
  - [ ] `frontend/app/indexer/(protected)/jobs/page.tsx` — Phase 4 placeholder
  - [ ] `frontend/app/indexer/(protected)/signals/page.tsx` — Phase 5 placeholder
  - [ ] `frontend/app/indexer/(protected)/strategies/page.tsx` — Phase 6 placeholder
  - [ ] `frontend/app/indexer/(public)/layout.tsx` — minimal centered (renders Topbar)
  - [ ] `frontend/app/indexer/(public)/login/page.tsx` — passphrase form (or just redirect to /compiler/login if cookie is shared)
  - [ ] `Topbar.tsx` — add `href: '/indexer'` to the indexer MODULES entry
  - [ ] Verify via `next build` that all 6 indexer routes appear
- [ ] **Phase 3** — Coverage page (`/indexer/coverage`)
  - [ ] KPI cards (Total Days / Days Complete / Days With Gaps / Days Missing) per metric
  - [ ] Per-metric heatmap (3 heatmaps stacked: price, open_interest, volume)
  - [ ] Gap table grouped by metric
- [ ] **Phase 4** — Jobs page (`/indexer/jobs`)
  - [ ] Empty state for the current 0-row reality
  - [ ] Job table with `job_type` filter chips (leaderboard / overlap / full / all)
  - [ ] Status badges + polling + live duration (lifted from compiler/jobs)
- [ ] **Phase 5** — Signals page (`/indexer/signals`)
  - [ ] Filter chips: source (live / backtest / research / all)
  - [ ] Date column + strategy version label + sit_flat badge + filter_name
  - [ ] Click row → detail view showing signal items (symbol + rank)
- [ ] **Phase 6** — Strategies page (`/indexer/strategies`)
  - [ ] Strategy list with versions nested under each
  - [ ] Show `is_active`, `published_at`, `config` JSONB excerpt
  - [ ] Read-only — no edit affordances this round

---

## Files touched (live log — updated as we go)

| Phase | File | Type |
|---|---|---|
| 0 | `docs/builds/indexer-page-build.md` | Created |

---

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

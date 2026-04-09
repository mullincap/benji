# Manager Page Build — Live Spec & Progress

> **If you are reading this in a new session:** This is the durable record of an in-progress multi-phase build of the Manager module. Read the **Current State** section first, then the most recently checked-off phase. To resume, start at the first unchecked phase below. Every decision and constraint is captured here so you do not need to ask the user to repeat anything.

---

## Current State

**All 5 phases complete.** Manager module is end-to-end functional: overview page with KPI cards + charts + pipeline status, Claude-powered chat with conversation history and action card parsing, daily briefing cron at 00:30 UTC.
**Next action:** None — build complete. Follow-up work includes wiring action execution (pause/adjust/add), read-only toggle implementation, and performance_daily population.
**Resume command for next session:** N/A — build is complete.

---

## Original spec summary

The Manager module is a Claude-powered portfolio intelligence layer at `/manager`. It provides:
1. **Overview page** — KPI cards, equity curve, allocation breakdown, pipeline status, live exchange data
2. **Chat page** — conversational interface powered by Claude (Sonnet) with full portfolio context
3. **Daily briefing** — automated cron-triggered portfolio summary via internal API token

The module follows the same architecture as Compiler and Indexer: FastAPI router + auth-protected Next.js route group + sub-pages. Same `admin_session` cookie and `require_admin` dependency. Public layout redirects to `/compiler/login`.

### Multi-allocation architecture

The user runs multiple simultaneous allocations across exchanges and strategies. `user_mgmt.allocations` has multiple active rows. All metrics aggregate across allocations:
- Total AUM = `SUM(capital_usd)` across active allocations
- Daily P&L % = total dollar P&L / total AUM * 100
- WTD/MTD = same weighted approach
- Max drawdown = worst single-allocation drawdown
- Handle zero `performance_daily` rows gracefully — return zeros, never divide by zero

### Live exchange data

Real-time account balance and open positions stored in `user_mgmt.exchange_snapshots` (populated every 5 min by BloFin logger). Exposed via `GET /api/allocator/snapshots`. If no snapshot exists, return nulls gracefully.

---

## Scoping answers (locked decisions)

| # | Question | Answer |
|---|---|---|
| 1 | Design files | None. Build from scratch using Compiler/Indexer patterns. |
| 2 | Scope | Full-stack: FastAPI endpoints (read + Anthropic API calls) + Next.js frontend. |
| 3 | Auth | Reuse existing `admin_session` cookie + `require_admin`. Add `X-Internal-Token` exception for briefing cron. |
| 4 | Theme accent | Follow active theme via `var(--module-accent)`. No hardcoded hex. |
| 5 | Route location | `frontend/app/manager/*` — top-level peer to `/compiler/`, `/indexer/`. |
| 6 | Login | Redirect to `/compiler/login` — no separate login form. |
| 7 | Claude model | `claude-sonnet-4-20250514`, max_tokens=1000 |
| 8 | Resilience | Build doc + commits at every phase boundary. |

---

## Phase 0 — Existing conventions observed

### What's already shipped that the Manager can reuse as-is

- **Admin auth backend**: `backend/app/api/routes/admin.py` exposes `require_admin(request)` dependency. Manager router will mount this at the router level for all endpoints except `/briefing` (which uses `X-Internal-Token`).
- **Session store**: `backend/app/services/admin_sessions.py` flat-file token store. No changes needed.
- **DB connection**: `backend/app/db.py` `get_cursor()` FastAPI dependency. Returns `RealDictCursor`, handles errors as 503/500.
- **Frontend route group pattern**: `(protected)` + `(public)` route groups, proven in Compiler and Indexer.
- **Sidebar pattern**: 200px text-only nav, 9px section label, 10px items, 2px left border in `var(--module-accent)` for active.
- **Topbar integration**: `Topbar.tsx` already has `manager` as a `ModuleKey` with theme colors in every theme entry. Needs `href: '/manager'` added to the `MODULES` array (line 110, currently has no href).
- **API contract conventions**: `NEXT_PUBLIC_API_BASE || ''`, `credentials: 'include'`, TypeScript types mirror FastAPI shapes.
- **Card/table/badge patterns**: `--bg2` background, `--line` border, `border-radius: 5px`, 9px section labels, 10px table cells.

### Existing route files

| File | Prefix | Purpose |
|---|---|---|
| `backend/app/api/routes/admin.py` | `/api/admin` | Login/logout/whoami + `require_admin` dependency |
| `backend/app/api/routes/compiler.py` | `/api/compiler` | Compiler read-only endpoints |
| `backend/app/api/routes/indexer.py` | `/api/indexer` | Indexer read-only endpoints |
| `backend/app/api/routes/jobs.py` | `/api/jobs` | Legacy job runner |

**No allocator router exists yet.** The `GET /api/allocator/snapshots` endpoint will be created as a new router file `backend/app/api/routes/allocator.py`.

### Database schema for Manager queries

#### Join path for Sharpe per allocation
`user_mgmt.allocations.strategy_version_id` → `audit.strategy_versions.strategy_version_id` → `audit.jobs.strategy_version_id` → `audit.results.job_id`

Most recent Sharpe for a strategy version:
```sql
SELECT r.sharpe
FROM audit.results r
JOIN audit.jobs j ON r.job_id = j.job_id
WHERE j.strategy_version_id = %s AND j.status = 'complete'
ORDER BY j.completed_at DESC NULLS LAST
LIMIT 1
```

#### Key tables

| Table | Key columns for Manager |
|---|---|
| `user_mgmt.allocations` | `allocation_id, strategy_version_id, connection_id, capital_usd, status` |
| `audit.strategy_versions` | `strategy_version_id, strategy_id, version_label, is_active` |
| `audit.strategies` | `strategy_id, name, display_name, filter_mode` |
| `user_mgmt.performance_daily` | `allocation_id, date, equity_usd, daily_return, drawdown` — PK `(allocation_id, date)` |
| `user_mgmt.exchange_connections` | `connection_id, exchange, label, status` |
| `user_mgmt.exchange_snapshots` | `connection_id, snapshot_at, total_equity_usd, available_usd, used_margin_usd, unrealized_pnl, positions, fetch_ok` |
| `user_mgmt.daily_signals` | `signal_batch_id, signal_date, strategy_version_id, computed_at, sit_flat, filter_name` |
| `user_mgmt.daily_signal_items` | `signal_batch_id, symbol_id, rank, weight` |
| `market.compiler_jobs` | `job_id, status, started_at, completed_at, last_heartbeat` |
| `market.indexer_jobs` | `job_id, status, started_at, completed_at, last_heartbeat` |
| `audit.results` | `result_id, job_id, sharpe, max_dd_pct, ...` |
| `audit.jobs` | `job_id, strategy_version_id, status, completed_at` |

### Prerequisites checked

| Check | Result |
|---|---|
| `ANTHROPIC_API_KEY` in secrets.env | **MISSING** — must be added before Phase 2. Reported to user. |
| `INTERNAL_API_TOKEN` in secrets.env | Was missing. **Generated and added**: `8f2a643e3a0d9881076f0f9530931d24fd12e367a1a482138f814f0476ab481d` |
| Allocator routes exist? | **No.** No `allocator.py` in routes. Will create `backend/app/api/routes/allocator.py` in Phase 2. |

### Manager-specific conventions to establish

- **Sidebar structure**: Different from Compiler/Indexer. The Manager sidebar has:
  - OVERVIEW nav item at top
  - Divider
  - CHAT nav item
  - "CONVERSATIONS" label with + NEW button (green, right-aligned)
  - Conversation list (title + relative time, active has green left border)
  - Bottom section: "ALLOCATIONS" label, compact rows per active allocation
  - Total AUM summary below allocations
  - Read-only toggle in **topbar only**, not sidebar
- **Chat action cards**: When Claude proposes an action, the trailing JSON block `{"action": true, ...}` is stripped from displayed text and rendered as an inline confirmation card with Confirm/Cancel buttons.
- **Module accent**: Whatever the active theme assigns to `manager`. From THEMES: spectrum=`#FF5E5E` (red), terminal=`#E060FF` (purple), institutional=`#C96060` (muted red), electric=`#7B5FFF` (violet).

### Things to flag / known followups

1. **`performance_daily` is likely empty.** The overview must handle zero rows gracefully everywhere — show "No data yet" placeholders, never error.
2. **`audit.results` may have zero rows for a strategy version.** Sharpe shows "n/a" in that case.
3. **`exchange_snapshots` may have zero rows.** Omit live exchange data section from system prompt, show "No snapshots yet" in UI.
4. **`allocations` table may have zero active rows.** All metrics return zeros. Overview shows "No active allocations" in the table.
5. **The `audit.results` → `strategy_version_id` join goes through `audit.jobs`.** This is a 2-hop join. If `audit.jobs` has no `complete` rows for a version, Sharpe is null.
6. **Briefing cron calls the public HTTPS URL.** This means the backend must handle `X-Internal-Token` auth at the FastAPI level, bypassing the cookie requirement.

---

## Phase checklist

- [x] **Phase 0** — Read conventions, check prerequisites, create this build doc
- [x] **Phase 1** — Database migrations (`manager_conversations`, `manager_messages`)
  - [x] Add tables to `schema.sql`
  - [x] Run migration on live DB
  - [x] Smoke test
- [x] **Phase 2** — FastAPI routes
  - [x] Part A: `backend/app/api/routes/allocator.py` — `GET /api/allocator/snapshots`
  - [x] Part B: `backend/app/api/routes/manager.py` — all manager endpoints
  - [x] Add `X-Internal-Token` exception to `require_admin` in `admin.py`
  - [x] Add `INTERNAL_API_TOKEN` + `ANTHROPIC_API_KEY` to `core/config.py`
  - [x] Register new routers in `main.py`
  - [x] `GET /api/manager/overview` — full portfolio context
  - [x] `GET /api/manager/conversations` — list
  - [x] `POST /api/manager/conversations` — create
  - [x] `GET /api/manager/conversations/{id}` — full history
  - [x] `DELETE /api/manager/conversations/{id}` — cascade delete
  - [x] `POST /api/manager/conversations/{id}/messages` — send + Claude response
  - [x] `POST /api/manager/briefing` — internal token auth, auto-conversation
  - [x] Smoke test all endpoints
- [x] **Phase 3** — Frontend shell
  - [x] `frontend/app/manager/(protected)/layout.tsx` — auth + sidebar with conversations
  - [x] `frontend/app/manager/(protected)/page.tsx` — redirect to `/manager/overview`
  - [x] `frontend/app/manager/(protected)/overview/page.tsx` — placeholder
  - [x] `frontend/app/manager/(protected)/chat/page.tsx` — empty state with suggested prompts
  - [x] `frontend/app/manager/(public)/layout.tsx` — redirect to `/compiler/login`
  - [x] Add `href: '/manager'` to Topbar MODULES
  - [x] Verify `next build` clean
- [x] **Phase 4** — Overview page
  - [x] Row 1: 5 KPI cards (TODAY % / WTD % / MTD % / MAX DRAWDOWN / TOTAL AUM)
  - [x] Row 2: equity curve (30d Chart.js line) + daily return bars (30d Chart.js bar)
  - [x] Row 3 left: per-allocation breakdown table
  - [x] Row 3 right: pipeline status
  - [x] Handle all empty-data states
- [x] **Phase 5** — Chat page
  - [x] Conversation list with load/create/delete
  - [x] Message history rendering
  - [x] Send message → Claude response → render
  - [x] Action card parsing (trailing JSON block)
  - [x] Loading/error/empty states
  - [x] Briefing cron entry

---

## Files to create/modify (planned)

| Phase | File | Type |
|---|---|---|
| 0 | `docs/builds/manager-page-build.md` | Created |
| 1 | `schema.sql` | Modified — add `manager_conversations`, `manager_messages` |
| 2 | `backend/app/api/routes/allocator.py` | Created — snapshots endpoint |
| 2 | `backend/app/api/routes/manager.py` | Created — all manager endpoints |
| 2 | `backend/app/api/routes/admin.py` | Modified — add `X-Internal-Token` exception |
| 2 | `backend/app/core/config.py` | Modified — add `INTERNAL_API_TOKEN`, `ANTHROPIC_API_KEY` |
| 2 | `backend/app/main.py` | Modified — register allocator + manager routers |
| 3 | `frontend/app/manager/(protected)/layout.tsx` | Created |
| 3 | `frontend/app/manager/(protected)/page.tsx` | Created |
| 3 | `frontend/app/manager/(protected)/overview/page.tsx` | Created (placeholder) |
| 3 | `frontend/app/manager/(protected)/chat/page.tsx` | Created (empty state) |
| 3 | `frontend/app/manager/(public)/layout.tsx` | Created |
| 3 | `frontend/app/components/Topbar.tsx` | Modified — add manager href |
| 4 | `frontend/app/manager/(protected)/overview/page.tsx` | Replaced — real overview |
| 5 | `frontend/app/manager/(protected)/chat/page.tsx` | Replaced — real chat |

---

## System prompt template (locked)

```
You are the portfolio intelligence layer for a quantitative crypto trading platform called 3M. You are speaking with a professional institutional allocator managing multiple strategies across multiple exchanges.

Be concise, precise, and analytical. Use exact numbers. Flag risks proactively. Lead responses with the total portfolio view, then break down per allocation when relevant. For any action that modifies the portfolio (pause allocation, adjust capital, add symbols to market.symbols), you MUST present a structured summary before executing — never act without explicit user confirmation.

When proposing an action, include a JSON block at the end of your response in this exact format:
{"action": true, "type": "pause_allocation|adjust_capital|add_symbol", "params": {...}, "summary": "one line description"}

If no action is being proposed, do not include any JSON block.

Current portfolio context:
{portfolio_context}

Today: {today} UTC
Read-only mode: {read_only}
```

### Portfolio context format

```
Total AUM: ${total_aum:,.0f} across {n} active allocations
Total live equity: ${total_live_equity:,.0f} (unrealized P&L: ${total_unrealized_pnl:+,.0f})

Allocation 1: ${capital:,.0f} · {exchange} · {strategy_display_name} · today {daily_return:+.2f}% · drawdown {drawdown:.1f}% · sharpe {sharpe:.2f}
Allocation 2: ...

Live exchange data (as of {snapshot_at}):
  {exchange}: equity=${total_equity:,.0f} available=${available:,.0f} unrealized=${unrealized_pnl:+,.0f}
  Open positions: {symbol: side size @ entry_price, ... or 'flat'}

Today's signals:
  {strategy_display_name}: {symbol_list} ({n} symbols, filter: {filter_name})

Pipeline:
  compiler: {compiler_status} · last run {compiler_time}
  indexer: {indexer_status} · last run {indexer_time}
  signals: last generated {signals_time}
```

If `performance_daily` has no rows: `today +0.00% · drawdown 0.0% · sharpe n/a`
If no snapshot exists: omit the live exchange data section entirely.

---

## How to resume in a new session

1. Read this entire file.
2. Check the **Phase checklist** for the first unchecked box.
3. Check **Files to create/modify** to see what already exists.
4. Run `git log --oneline -10` to see what's already committed.
5. Continue from the next sub-task.
6. If working tree is dirty, check `git diff` and finish the current phase or stash.

# Benji3m — Frontend (CLAUDE_FRONTEND.md)

Context for UI work in `frontend/`. Read CLAUDE.md first for design-system rules.

## Stack

- Next.js 16 with **app router** (not pages router)
- TypeScript strict, no component library
- Styling via inline `style={{}}` + CSS variables from `frontend/app/globals.css`
- Space Mono only (locked — see CLAUDE.md)
- Charts via `chart.js` + `react-chartjs-2` (not Recharts)

## Module layout

Every module uses Next.js route groups for auth-gating:

```
frontend/app/
  login/                  neutral admin login (shared by compiler, indexer, manager)
  compiler/(protected)/   coverage | days | marketcap | symbols | jobs
  indexer/(protected)/
  simulator/              page.tsx — single-page audit runner (no subroutes)
  allocator/(protected)/  trader-user login under /trader/(public)/login (separate auth)
  manager/(protected)/    overview | execution | portfolios | chat
  components/             shared UI (Topbar, StatusBar, Skeleton, RightPanel, LeftPanel, ...)
```

`(protected)` routes require a valid session cookie. Compiler / indexer / manager share the admin `admin_session` cookie and all redirect unauthenticated users to `/login?next=<current-path>`, which returns them to the originating route after a successful login. The trader module uses a separate per-user session and still has its own `/trader/login`.

## API_BASE pattern

```ts
const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
fetch(`${API_BASE}/api/...`, { credentials: "include" })
```

- Local dev: `NEXT_PUBLIC_API_BASE=http://localhost:8000` (FastAPI on :8000)
- Prod: `API_BASE=""` — nginx reverse-proxies same-origin to backend

**Gotcha:** do NOT use `new URL("/api/...")` — throws when API_BASE is empty because URL requires an absolute base. Use string concat + `URLSearchParams` instead (see `SessionLogs.tsx:buildLogsUrl` for the canonical helper). This bit Part 2 of the Session Logs feature.

## Auth conventions

- `credentials: "include"` on every fetch
- `res.status === 401` → show "Session expired" banner (don't auto-redirect)
- Admin pages (compiler, indexer, manager) gate on `/api/admin/whoami` and redirect to `/login?next=<path>` on 401; allocator pages use a per-user session via `/api/auth/me` and redirect to `/trader/login`

## Design system tokens

From `frontend/app/globals.css`:

| Token | Use |
|---|---|
| `--bg0` / `--bg1` / `--bg2` / `--bg3` / `--bg4` | surfaces, ascending lightness |
| `--line` / `--line2` | default / hover borders |
| `--t0` / `--t1` / `--t2` / `--t3` | primary → muted text |
| `--green` / `--green-dim` / `--green-mid` | active / positive |
| `--amber` / `--amber-dim` | warning / borderline |
| `--red` / `--red-dim` | danger / breached |
| `--module-accent` | per-module highlight color (set by layout) |

Typography rules are locked — see CLAUDE.md "Design system".

## Common patterns

### Collapsible section with mutually-exclusive expansion
Pattern used on the Execution tab (Daily Summary + Session Logs can only have one open at a time). Parent owns `activeSection: "a" | "b" | null` state; each section receives `expanded` + `onToggle` props rather than managing its own collapse state. See `execution/page.tsx` + `SessionLogs.tsx`.

### Live-polling panel with auto-scroll
`SessionLogs.tsx` and `portfolios/[date]/page.tsx` both poll while a session is active. Pattern: `useEffect` with `setInterval`, clear in cleanup, `scrollRef` + `bottomedOutRef` to preserve user's manual scroll position.

### Table styling
Match `manager/(protected)/overview/page.tsx` ALLOCATIONS / PIPELINE STATUS tables:
- `thStyle`: 9px, weight 700, letter-spacing 0.12em, uppercase, color `var(--t3)`, 1px bottom border
- `tdStyle`: 10–11px, color `var(--t1)`, 1px bottom border, monospace
- No explicit borders between cells — rely on the row border + whitespace

### KPI cards
Standard card is 14px top padding, 9px uppercase label, 18px weight-700 value. Color the value via a `color` prop that the caller sets based on threshold logic.

### Threshold color pattern
Most numeric displays (fill rate, slippage, PnL gap) use a helper `fooColor(value)` that maps to green/amber/red. Keep thresholds in `frontend/app/manager/(protected)/execution/page.tsx` and `portfolios/[date]/page.tsx` in sync if you add more.

### Chart defaults
When using `react-chartjs-2`:
- `maintainAspectRatio: false` + parent div with explicit `height`
- `animation: false` if polling live data (avoids re-render flicker)
- `tension: 0.15–0.3` for smooth lines
- Grid: `color: "#1a1a1d"` (very muted dark)
- Ticks: `color: "#5a5754", font: { family: "Space Mono" }`

## Manager surface

Three tabs under `manager/(protected)/`:

- **Overview** — allocations table, pipeline status, equity curves, intraday equity. Data source: `/api/manager/overview` + `/api/allocator/snapshots`.
- **Execution** — per-session trader reports with expandable per-symbol detail + collapsible Session Logs viewer. Data source: `/api/manager/execution-reports` (file-backed) + `/api/manager/execution-logs` (tails `blofin_executor.log`).
- **Portfolios** — list of live-trader sessions with click-through to a bar-by-bar detail view (Chart.js cumulative ROI + 5-min ROI matrix). Data source: `/api/manager/portfolios[/{date}]` (SQL-backed, see CLAUDE_BACKEND.md).

Chat sidebar is shared across all tabs and persists conversations via `/api/manager/conversations`.

## Running locally

```bash
cd frontend && npm install && npm run dev
# → http://localhost:3000
```

Requires the FastAPI backend on :8000 and `NEXT_PUBLIC_API_BASE=http://localhost:8000` in `frontend/.env.local`.

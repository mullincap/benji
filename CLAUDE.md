# Benji3m — Project Context for Claude Code

## What this project is
A web application wrapping a Python quantitative trading risk audit pipeline.
The pipeline backtests a crypto trading strategy and produces institutional-grade analytics.
Target users: crypto fund managers and allocators.

## Current status
- Phase 1 (environment setup): COMPLETE
- Phase 2 (pipeline audit + API design): COMPLETE
- Phase 3 (UX research + wireframing): COMPLETE
- Phase 4 (hi-fi design + component library): COMPLETE
- Phase 5 (backend development): COMPLETE
- Phase 6 (frontend development): READY TO BUILD
- Phase 7 (deployment): NOT STARTED

## Tech stack
- Backend: FastAPI + Celery + Redis + flat JSON job store
- Frontend: Next.js + TypeScript + Tailwind CSS
- Pipeline: Pure Python CLI scripts (no framework)
- Report generation: Node.js (generate_audit_report.js)
- Python virtual env: .venv (venv) at ~/Projects/benji3m/.venv

## Project structure
benji3m/
  backend/
    app/
      main.py                  # FastAPI app — DONE
      core/config.py           # Pydantic settings — DONE
      api/routes/health.py     # Health check route — DONE
      api/routes/jobs.py       # Job CRUD routes — DONE (needs param additions below)
      services/job_store.py    # Flat JSON job store — DONE
      workers/
        pipeline_worker.py     # Celery worker — DONE
  frontend/                    # Next.js app — scaffolded, ready to build
  pipeline/                    # All 6 pipeline scripts — DONE (paths use env vars)
    overlap_analysis.py
    audit.py
    rebuild_portfolio_matrix.py
    build_intraday_leaderboard.py
    institutional_audit.py
    generate_audit_report.js

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

## Recommended workflow
- Design decisions, architecture, wireframes → Claude.ai chat interface
- File creation, coding, terminal commands → Claude Code inside Antigravity
- Always say "read CLAUDE.md first" when starting a new Claude Code session

---

## Phase 4 — Design system (LOCKED — do not deviate)

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

### Layout
- Border radius: 3–4px throughout (sharp, not rounded)
- Borders: 1px solid, never thicker
- Two-column layout: 288px fixed left panel + flexible right panel
- Topbar height: 46px
- Status bar height: 32px
- Left panel scrollable, right panel scrollable independently
- Custom scrollbar: 3px width, var(--line2) thumb, transparent track

### Input styling
- Background: var(--bg3)
- Border: 1px solid var(--line2)
- Border-radius: 2px
- Padding: 3px 7px
- Font: Space Mono 10px
- Text-align: right
- Focus border: var(--green)
- Width: 82px for numeric inputs, 100px for selects

### Toggle component
- Width: 26px, height: 14px, border-radius: 7px
- Off state: background var(--bg4), border var(--line2), dot color var(--t2)
- On state: background var(--green-mid), border var(--green), dot color var(--green)
- Disabled state: opacity 0.28, cursor not-allowed
- Dot: 10px × 10px, absolute positioned, transition transform 0.2s

### Three app states (React useState controls which renders)
1. IDLE — left panel shows full editable parameter form, right panel shows centered empty state
2. RUNNING — left panel dims to opacity 0.4 + pointer-events none, right panel shows progress + live log
3. RESULTS — left panel shows grade hero + KPI trio + param summary + scorecard, right panel shows dashboard

### Animations
- Pulsing dot: amber, 1.2s ease-in-out infinite, for running status indicators
- Spinning loader: 0.8s linear infinite, for active stage icon in progress card
- Blinking cursor: 1s step-end infinite, appended to last log line
- Progress bar fill: CSS width transition 0.5s ease
- Tier expand/collapse: max-height transition 0.25s ease, chevron rotate 90deg

---

## API endpoints

### POST /api/jobs
Submit a new pipeline run. Body: JobRequest JSON. Returns: { job_id, status: "queued", estimated_seconds: 180 }

### GET /api/jobs/{job_id}
Poll job status every 2 seconds while running.
Returns: { job_id, status, stage, progress, params, created_at, updated_at, error, results }
- status: "queued" | "running" | "complete" | "failed"
- stage: "overlap" | "rebuild" | "audit" | "report" | "parsing" | "done"
- progress: 0–100

### GET /api/jobs/{job_id}/results
Returns full results when status = "complete".
Includes: metrics{}, filter_comparison[], equity_curve[], drawdown_curve[], scorecard[], grade

### GET /api/jobs/{job_id}/download/report
Streams the .docx audit report file.

### GET /api/jobs
Returns last 20 jobs for run history panel.

### GET /api/health
Returns API + Redis + parquet file status.

---

## Component structure

```
frontend/
  app/
    layout.tsx            -- imports Space Mono, sets CSS variables in :root
    globals.css           -- all CSS variables, scrollbar styles, base resets
    page.tsx              -- root component, holds appState + jobId + results state
    components/
      Topbar.tsx           -- logo, tagline, run history btn, share btn, download btn
      StatusBar.tsx        -- status dot, stage, elapsed time, warnings
      LeftPanel/
        ParamForm.tsx      -- IDLE: full editable form with all tiers
        RunningParams.tsx  -- RUNNING: locked read-only param summary
        ResultsSummary.tsx -- RESULTS: grade hero + KPI trio + scorecard + re-run btn
      RightPanel/
        EmptyState.tsx     -- IDLE: centered bracket corners + icon + prompt text
        RunningView.tsx    -- RUNNING: progress card (4 stages) + live log card
        ResultsView.tsx    -- RESULTS: 8 metric cards + 2 charts + filter table
      ui/
        Toggle.tsx         -- reusable toggle with on/off/disabled states
        MetricCard.tsx     -- single metric card with bottom accent bar
        FilterTable.tsx    -- filter comparison table with inline bar charts
        TierSection.tsx    -- collapsible Advanced/Expert section with chevron
        ConditionalParams.tsx -- params revealed when parent toggle is on
```

---

## Left panel — complete parameter spec

### BASIC tier (always visible, no collapse)

#### Section: Strategy
- leaderboard_index: int = 100
- sort_by: enum = "price" | "open_interest" | "combined" | "price-only" | "oi-only"
- mode: enum = "snapshot" | "frequency"
- freq_width: int = 20
- freq_cutoff: int = 20

#### Section: Deployment window
- deployment_start_hour: int = 6  (0–23)
- index_lookback: int = 6
- sort_lookback: str = "6"  (int or "daily")
- deployment_runtime_hours: str = "daily"  (int or "daily")
- end_cross_midnight: bool = True  (toggle)

#### Section: Universe + risk
- starting_capital: float = 100000
- capital_mode: enum = "fixed" | "compounding"
- fixed_notional_cap: enum = "internal" | "external"  — ONLY visible when capital_mode = "fixed"
- pivot_leverage: float = 4.0
- min_mcap: float = 0
- max_mcap: float = 0
- min_listing_age: int = 0
- max_port: int | null = null  (empty = no cap)
- drop_unverified: bool = False  (toggle)
- leverage: float = 4.0
- stop_raw_pct: float = -6.0
- price_source: enum = "parquet" | "binance"
- save_charts: bool = True  (toggle)
- trial_purchases: bool = False  (toggle)
- quick: bool = False  (toggle)
Sub-section "Trading costs":
- taker_fee_pct: float = 0.0008
- funding_rate_daily_pct: float = 0.0002
Note text: "Round-trip cost per symbol applied to all filter modes"

#### Section: Execution config  (darker bg: var(--bg0), badge: "CANDIDATE_CONFIGS" in green)
Sub-section "Early exit":
- early_kill_x: int = 5
- early_kill_y: float = -999
- early_instill_y: float = -999
Sub-section "Leverage":
- l_base: float = 0.0
- l_high: float = 1.0
Sub-section "Stops":
- port_tsl: float = 0.99
- port_sl: float = -0.99
Sub-section "Early fill":
- early_fill_y: float = 0.99
- early_fill_x: int = 5

#### Section: Filters  (two-column layout: left = ENABLE_*, right = RUN_FILTER_*)
Column headers: "Enable filter" | "Include in run"
JS dependency rule: RUN_FILTER_* toggle disables + greys when its ENABLE_* is off.

Filter pairs (enable → run):
- enable_tail_guardrail (default True)     → run_filter_tail (default False)
- enable_dispersion_filter (default True)  → run_filter_dispersion (default False)
- enable_tail_plus_disp (default True)     → run_filter_tail_disp (default False)
- enable_vol_filter (default True)         → run_filter_vol (default False)
- enable_tail_disp_vol (default False)     → run_filter_tail_disp_vol (default False)
- enable_tail_or_vol (default False)       → run_filter_tail_or_vol (default False)
- enable_tail_and_vol (default False)      → run_filter_tail_and_vol (default False)
- enable_blofin_filter (default False)     → run_filter_tail_blofin (default False)

Standalone toggles (no enable dependency):
- run_filter_none: bool = True   (No filter baseline)
- enable_btc_ma_filter: bool = False
- enable_ic_diagnostic: bool = False
- enable_ic_filter: bool = False
- run_filter_calendar: bool = False

---

### ADVANCED tier (collapsed by default)
Header: "ADVANCED" in blue (#378ADD) + chevron + "strategy tuning + audit modules"
Background when expanded: var(--bg0)

#### Sub-section: Strategy tuning
- dispersion_threshold: float = 0.66
- dispersion_baseline_win: int = 33
- dispersion_dynamic_universe: bool = True  (toggle)
- dispersion_n: int = 40
- vol_lookback: int = 10
- vol_percentile: float = 0.25
- vol_baseline_win: int = 90
- tail_drop_pct: float = 0.04
- tail_vol_mult: float = 1.4
- ic_signal: enum = "mom1d" | "mom5d" | "skew20d" | "vol20d_inv"
- ic_window: int = 30
- ic_threshold: float = 0.02
- btc_ma_days: int = 20
- blofin_min_symbols: int = 1
- leaderboard_top_n: int = 333
- train_test_split: float = 0.60
- n_trials: int = 3

#### Sub-section: Leverage scaling models
Each toggle reveals conditional params below it when turned on.

- enable_perf_lev_scaling: bool = False
  → When on, reveal:
    perf_lev_window: int = 10
    perf_lev_sortino_target: float = 3.0
    perf_lev_max_boost: float = 1.5

- enable_vol_lev_scaling: bool = False
  → When on, reveal:
    vol_lev_window: int = 30
    vol_lev_target_vol: float = 0.02
    vol_lev_max_boost: float = 2.0
    vol_lev_dd_threshold: float = -0.06

- enable_contra_lev_scaling: bool = False
  → When on, reveal:
    contra_lev_window: int = 30
    contra_lev_max_boost: float = 2.0
    contra_lev_dd_threshold: float = -0.15

#### Sub-section: Risk overlays

- enable_pph: bool = False
  → When on, reveal:
    pph_frequency: enum = "daily" | "weekly" | "monthly" (default "weekly")
    pph_threshold: float = 0.20
    pph_harvest_frac: float = 0.50
- pph_sweep_enabled: bool = False

- enable_ratchet: bool = False
  → When on, reveal:
    ratchet_frequency: enum = "daily" | "weekly" | "monthly" (default "weekly")
    ratchet_trigger: float = 0.20
    ratchet_lock_pct: float = 0.15
    ratchet_risk_off_lev_scale: float = 0.0
- ratchet_sweep_enabled: bool = False

- enable_adaptive_ratchet: bool = False
  → When on, reveal:
    adaptive_ratchet_frequency: enum = "daily" | "weekly" | "monthly" (default "weekly")
    adaptive_ratchet_vol_window: int = 20
    adaptive_ratchet_vol_low: float = 0.03
    adaptive_ratchet_vol_high: float = 0.07
    adaptive_ratchet_risk_off_scale: float = 0.0
    adaptive_ratchet_floor_decay: float = 0.995
- adaptive_ratchet_sweep_enabled: bool = False

#### Sub-section: Parameter sweeps
- enable_sweep_l_high: bool = False
- enable_sweep_tail_guardrail: bool = False
- enable_sweep_trail_wide: bool = False
- enable_sweep_trail_narrow: bool = False
- enable_param_surfaces: bool = False

#### Sub-section: Stability cubes
- enable_stability_cube: bool = False
- enable_risk_throttle_cube: bool = False
- enable_exit_cube: bool = False

#### Sub-section: Robustness + stress tests
- enable_noise_stability: bool = False
- enable_slippage_sweep: bool = False
- enable_equity_ensemble: bool = False
- enable_param_jitter: bool = False
- enable_return_concentration: bool = False
- enable_sharpe_ridge_map: bool = False
- enable_sharpe_plateau: bool = False
- enable_top_n_removal: bool = False
- enable_lucky_streak: bool = False
- enable_periodic_breakdown: bool = False
- enable_weekly_milestones: bool = False
- enable_monthly_milestones: bool = False
- enable_dsr_mtl: bool = False
- enable_shock_injection: bool = False
- enable_ruin_probability: bool = False

#### Sub-section: Diagnostics
- enable_mcap_diagnostic: bool = False
- enable_capacity_curve: bool = False
- enable_regime_robustness: bool = False
- enable_min_cum_return: bool = False

---

### EXPERT tier (collapsed by default)
Header: "EXPERT" in amber (var(--amber)) + chevron + "simulation mechanics" + ⚠ icon
Background when expanded: var(--bg0)
Every field shows inline amber "⚠ caution" label (8px, opacity 0.7) to the right of the field label.

- annualization_factor: int = 365
- bar_minutes: int = 5
- end_cross_midnight: bool = True  (toggle + caution)
- save_daily_files: bool = False  (toggle + caution)
- build_master_file: bool = True  (toggle + caution)

---

## Backend — JobRequest additions needed

All parameters listed above must be added to JobRequest in backend/app/api/routes/jobs.py.
All must be passed as environment variables in pipeline_worker.py to the audit subprocess.
In pipeline/audit.py, replace every hardcoded constant with os.environ.get() reads with current values as fallbacks.

Environment variable naming convention: SNAKE_UPPER_CASE matching the param name.
Example: dispersion_threshold → os.environ.get("DISPERSION_THRESHOLD", "0.66")

---

## Results panel — right panel when state = RESULTS

### Metric cards (8 cards, 4×2 grid)
- Sortino ratio
- Calmar ratio
- Omega ratio
- Ulcer index
- FA-OOS Sharpe
- DSR %
- WF-CV
- Flat days

### Charts (side by side)
- Equity curve (4× levered) — green polyline with subtle gradient fill
- Drawdown curve — amber polyline with subtle gradient fill

### Filter comparison table
Columns: Filter | Sharpe (with inline mini bar) | Max DD | CAGR | WF-CV | DSR% | Grade
Best filter row: highlighted with var(--green-dim) background, green text, ★ suffix
Sorted by Sharpe descending.
Mini bar: 40px wide, 2px height, green fill proportional to Sharpe vs max Sharpe in set, amber for lower performers.

### Left panel results state
Grade hero block (var(--bg0) background, green border):
- Grade circle (B−, A, C+ etc)
- Filter name
- "CANONICAL FILTER" badge
- KPI trio: Sharpe | Max DD | CAGR

Below grade hero:
- Run parameters summary (read-only, muted values)
- Scorecard (Pass/Warn/Fail rows)
- "Edit parameters & re-run" button

---

## Running state details

### Progress card (4 stages)
1. Overlap analysis
2. Portfolio matrix rebuild
3. Institutional audit (shows current filter N/6)
4. Report generation

Stage icons:
- Done: green square with ✓
- Active: amber square with spinning dot, pulsing border animation
- Pending: dark square with — dash

### Live log card
Header: "LIVE OUTPUT" label + pulsing amber "streaming" indicator
Log lines: Space Mono 9px, timestamp in var(--t3), level tag in green/amber
Last line has blinking amber cursor block appended

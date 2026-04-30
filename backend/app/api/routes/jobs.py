import json
import logging
import subprocess
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, model_validator

from app.core.config import settings
from app.db import get_worker_conn
from app.services.job_store import (
    create_job, delete_job, get_job, list_jobs, update_job,
    list_folders, create_folder, rename_folder, delete_folder,
)
from app.services.audit.metrics_parser import parse_metrics
from app.workers.pipeline_worker import run_pipeline
from .admin import require_admin

# Admin-gated. All 15 routes (job CRUD, cancel, results, folders, report
# generation, download) are administrative operations — no public routes.
# Pre-2026-04-23 the router was unauthenticated, which allowed any anonymous
# POST to queue audit jobs and burn Celery capacity. Discovered + fixed
# during the Stream D-medium acceptance run — see docs/strategy_specification.md
# § 11 follow-up.
router = APIRouter(
    prefix="/api/jobs",
    tags=["jobs"],
    dependencies=[Depends(require_admin)],
)


def _refresh_results_if_needed(job: dict[str, Any]) -> dict[str, Any]:
    status = str(job.get("status", "")).lower()
    if status not in {"complete", "completed", "done"}:
        return job

    results = (job.get("results") or {}) if isinstance(job.get("results"), dict) else {}
    metrics = results.get("metrics") if isinstance(results.get("metrics"), dict) else {}
    filter_rows = metrics.get("filter_comparison") if isinstance(metrics.get("filter_comparison"), list) else []

    needs_refresh = (
        not metrics.get("filters")
        or (len(filter_rows) > 0 and not any(isinstance(r, dict) and r.get("is_run_summary_best") for r in filter_rows))
    )
    if not needs_refresh:
        return job

    job_id = str(job.get("id", "")).strip()
    if not job_id:
        return job

    output_path = Path(settings.JOBS_DIR) / job_id / "audit_output.txt"
    if not output_path.exists():
        return job

    parsed_metrics = parse_metrics(output_path)
    if not parsed_metrics:
        return job

    merged_metrics = {**metrics, **parsed_metrics}
    merged_results = {**results, "metrics": merged_metrics}
    updated = update_job(job_id, results=merged_results)
    return updated


# ---------------------------------------------------------------------------
# Request model — every param from the left panel spec in CLAUDE.md
# ---------------------------------------------------------------------------
class JobRequest(BaseModel):
    # ── Strategy ─────────────────────────────────────────────────────────────
    leaderboard_index:          int   = 100
    sort_by:                    str   = "price"
    mode:                       str   = "snapshot"
    # D-medium-split (2026-04-23): individual ranking-metric knobs per metric,
    # plus a BloFin-universe filter toggle. Replaces the earlier symmetric
    # `ranking_metric` field. Backward-compat: if an old client still sends
    # `ranking_metric`, overlap_analysis.py maps it onto both new flags + emits
    # a deprecation warning.
    price_ranking_metric:       str   = "pct_change"   # log_return | pct_change | abs_dollar
    oi_ranking_metric:          str   = "pct_change"   # pct_change | abs_dollar (log_return invalid for OI)
    apply_blofin_filter:        bool  = False          # narrow universe to BloFin listings
    ranking_metric:             str | None = None      # DEPRECATED — kept for backward-compat
    # Stream D-explore (2026-04-23): three-way overlap with volume as a third axis.
    # 'price_oi' is canonical (preserves current behavior). Other values are
    # candidate-exploration variants evaluated via the Simulator governance
    # framework — not canonical unless promoted per §5.
    overlap_dimensions:         str   = "price_oi"      # price_oi | price_volume | oi_volume | price_oi_volume
    freq_width:                 int   = 20
    freq_cutoff:                int   = 20
    sample_interval:            int   = 5

    # ── Deployment window ─────────────────────────────────────────────────────
    deployment_start_hour:      int   = 6
    index_lookback:             int   = 6
    sort_lookback:              int | str = 6
    deployment_runtime_hours:   str   = "daily"
    end_cross_midnight:         bool  = True

    # ── Universe + risk ───────────────────────────────────────────────────────
    starting_capital:           float = 100000.0
    capital_mode:               str   = "fixed"
    fixed_notional_cap:         str   = "internal"
    pivot_leverage:             float = 4.0
    min_mcap:                   float = 0.0
    max_mcap:                   float = 0.0
    min_listing_age:            int   = 0
    max_port:                   int | None = None
    drop_unverified:            bool  = False
    leverage:                   float = 4.0
    stop_raw_pct:               float = -6.0
    price_source:               str   = "db"
    mcap_source:                str   = "db"
    # Dispersion-universe selection mode (audit's _load_mcap_from_db):
    # 'curated' = filter to COINGECKO_TO_BINANCE.keys() (90-coin whitelist,
    #             current default — dispersion threshold tuned for this)
    # 'all'     = no whitelist; join market.symbols.binance_id (matches
    #             live trader's compute_dispersion_filter universe scope)
    # Default 'all' since 2026-04-29: matches audit.py's own default and
    # the live trader's universe scope (memory note `feedback_dispersion_
    # universe_default`: locked to mode='all'+dynamic-on regardless of
    # Sharpe; if it degrades, re-tune threshold not pool).
    dispersion_universe_mode:   str   = "all"
    # Mid-session splice — when True, audit.py appends today's
    # partial intraday column to the matrix using Binance 5m kline
    # closes (read live for the basket from live_deploys_signal.csv).
    # Aggregate metrics (Sharpe, CAGR, MaxDD) include today's partial
    # day; the audit emits FINAL_LIVE_TODAY_PARTIAL with metadata so
    # the UI can render a "* includes today's partial" footnote.
    live_today:                 bool  = False
    save_charts:                bool  = True
    trial_purchases:            bool  = False
    quick:                      bool  = False

    # Trading costs
    taker_fee_pct:              float = 0.0008
    funding_rate_daily_pct:     float = 0.0002

    # ── CANDIDATE_CONFIGS execution params ────────────────────────────────────
    early_kill_x:               int   = 5
    early_kill_y:               float = -999.0
    early_instill_y:            float = -999.0
    l_base:                     float = 0.0
    l_high:                     float = 1.0
    port_tsl:                   float = 0.99
    port_sl:                    float = -0.99
    early_fill_y:               float = 0.99
    early_fill_x:               int   = 5
    dd_stop_x:                  int   = 9999    # minutes from session open past which dd_stop activates; 9999 = disabled
    dd_stop_y:                  float = -0.99   # incr threshold (unleveraged, negative); -0.99 = disabled

    # ── Filters — enable ──────────────────────────────────────────────────────
    enable_tail_guardrail:      bool  = True
    enable_dispersion_filter:   bool  = True
    enable_tail_plus_disp:      bool  = True
    enable_vol_filter:          bool  = True
    enable_tail_disp_vol:       bool  = False
    enable_tail_or_vol:         bool  = False
    enable_tail_and_vol:        bool  = False
    enable_blofin_filter:       bool  = False
    enable_btc_ma_filter:       bool  = False
    enable_ic_diagnostic:       bool  = False
    enable_ic_filter:           bool  = False

    # ── Filters — run ─────────────────────────────────────────────────────────
    run_filter_none:            bool  = True
    run_filter_tail:            bool  = False
    run_filter_dispersion:      bool  = False
    run_filter_tail_disp:       bool  = False
    run_filter_vol:             bool  = False
    run_filter_tail_disp_vol:   bool  = False
    run_filter_tail_or_vol:     bool  = False
    run_filter_tail_and_vol:    bool  = False
    run_filter_tail_blofin:     bool  = False
    run_filter_calendar:        bool  = False

    # ── Exchanges — universe modifier ─────────────────────────────────────────
    # New BloFin model (replaces the legacy run_filter_tail_blofin sit-flat).
    # blofin_variants:
    #   "off"          → vanilla audit only
    #   "blofin_only"  → single audit run with BloFin universe restriction
    #   "both"         → two runs (vanilla + BloFin), merged into 10-row pairs
    # The backend's run_audit_with_blofin_variants derives apply_blofin_filter
    # and blofin_universe_enabled from this enum — frontend should pass
    # blofin_variants only and leave the underlying flags alone.
    blofin_variants:            str   = "off"
    blofin_universe_enabled:    bool  = False

    # ── ADVANCED — Strategy tuning ────────────────────────────────────────────
    dispersion_threshold:       float = 0.66
    dispersion_baseline_win:    int   = 33
    dispersion_dynamic_universe: bool = True
    dispersion_n:               int   = 40
    # Days to lag the mcap snapshot when picking the per-day dispersion
    # universe. 1 (default) = mask[T] uses mcap[T-1] — canonical
    # no-lookahead. 0 = same-day mcap. 2 = extra cautious.
    dispersion_universe_lag_days: int = 1
    # When True, the dispersion returns universe is the union of per-day
    # top-N (lagged) symbols from market.market_cap_daily — a genuinely
    # dynamic pool that tracks the market. When False (default for
    # backward compat), uses the hardcoded audit.DISPERSION_SYMBOLS_<N>
    # list. Live ops should flip True via stored strategy config once
    # the dispersion_n sweep settles on a winner.
    dispersion_universe_strict_dynamic: bool = False
    vol_lookback:               int   = 10
    vol_percentile:             float = 0.25
    vol_baseline_win:           int   = 90
    tail_drop_pct:              float = 0.04
    tail_vol_mult:              float = 1.4
    ic_signal:                  str   = "mom1d"
    ic_window:                  int   = 30
    ic_threshold:               float = 0.02
    btc_ma_days:                int   = 20
    blofin_min_symbols:         int   = 1
    leaderboard_top_n:          int   = 333
    train_test_split:           float = 0.60
    n_trials:                   int   = 3

    # ── ADVANCED — Leverage scaling ───────────────────────────────────────────
    enable_perf_lev_scaling:    bool  = False
    perf_lev_window:            int   = 10
    perf_lev_sortino_target:    float = 3.0
    perf_lev_max_boost:         float = 1.5

    enable_vol_lev_scaling:     bool  = False
    vol_lev_window:             int   = 30
    vol_lev_target_vol:         float = 0.02
    vol_lev_max_boost:          float = 2.0
    vol_lev_dd_threshold:       float = -0.06
    lev_quantization_mode:      str   = "off"   # off | binary | stepped
    lev_quantization_step:      float = 0.1

    enable_contra_lev_scaling:  bool  = False
    contra_lev_window:          int   = 30
    contra_lev_max_boost:       float = 2.0
    contra_lev_dd_threshold:    float = -0.15

    # ── ADVANCED — Risk overlays ──────────────────────────────────────────────
    enable_pph:                 bool  = False
    pph_frequency:              str   = "weekly"
    pph_threshold:              float = 0.20
    pph_harvest_frac:           float = 0.50
    pph_sweep_enabled:          bool  = False

    enable_ratchet:             bool  = False
    ratchet_frequency:          str   = "weekly"
    ratchet_trigger:            float = 0.20
    ratchet_lock_pct:           float = 0.15
    ratchet_risk_off_lev_scale: float = 0.0
    ratchet_sweep_enabled:      bool  = False

    enable_adaptive_ratchet:              bool  = False
    adaptive_ratchet_frequency:           str   = "weekly"
    adaptive_ratchet_vol_window:          int   = 20
    adaptive_ratchet_vol_low:             float = 0.03
    adaptive_ratchet_vol_high:            float = 0.07
    adaptive_ratchet_risk_off_scale:      float = 0.0
    adaptive_ratchet_floor_decay:         float = 0.995
    adaptive_ratchet_sweep_enabled:       bool  = False

    # ── ADVANCED — Parameter sweeps ───────────────────────────────────────────
    enable_sweep_l_high:        bool  = False
    enable_sweep_tail_guardrail: bool = False
    enable_sweep_trail_wide:    bool  = False
    enable_sweep_trail_narrow:  bool  = False
    enable_param_surfaces:      bool  = False

    # ── ADVANCED — Stability cubes ────────────────────────────────────────────
    enable_stability_cube:      bool  = False
    enable_risk_throttle_cube:  bool  = False
    enable_exit_cube:           bool  = False

    # ── ADVANCED — Robustness + stress tests ─────────────────────────────────
    enable_noise_stability:     bool  = False
    enable_slippage_sweep:      bool  = False
    enable_equity_ensemble:     bool  = False
    enable_param_jitter:        bool  = False
    enable_return_concentration: bool = False
    enable_sharpe_ridge_map:    bool  = False
    enable_sharpe_plateau:      bool  = False
    enable_top_n_removal:       bool  = False
    enable_lucky_streak:        bool  = False
    enable_periodic_breakdown:  bool  = False
    enable_weekly_milestones:   bool  = False
    enable_monthly_milestones:  bool  = False
    enable_dsr_mtl:             bool  = False
    enable_shock_injection:     bool  = False
    enable_ruin_probability:    bool  = False

    # ── ADVANCED — Diagnostics ────────────────────────────────────────────────
    enable_mcap_diagnostic:     bool  = False
    enable_capacity_curve:      bool  = False
    enable_regime_robustness:   bool  = False
    enable_min_cum_return:      bool  = False

    # ── EXPERT ────────────────────────────────────────────────────────────────
    annualization_factor:       int   = 365
    bar_minutes:                int   = 5
    save_daily_files:           bool  = False
    build_master_file:          bool  = True

    @model_validator(mode="after")
    def _check_overlap_dimensions_price_source(self) -> "JobRequest":
        if "volume" in self.overlap_dimensions and self.price_source != "db":
            raise ValueError(
                f"overlap_dimensions={self.overlap_dimensions!r} requires "
                f"price_source='db'; got price_source={self.price_source!r}. "
                f"Volume is not in the pre-built market.leaderboards tables — "
                f"the three-way overlap must be computed on-the-fly from "
                f"market.futures_1m. Either set price_source='db' or use "
                f"overlap_dimensions='price_oi'."
            )
        return self


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------
class JobCreateResponse(BaseModel):
    job_id:            str
    status:            str
    estimated_seconds: int = 180


class JobResponse(BaseModel):
    id:         str
    display_name: str | None = None
    folder_id:  str | None = None
    status:     str
    stage:      str | None
    progress:   int
    params:     dict[str, Any]
    results:    dict[str, Any] | None
    error:      str | None
    created_at: float
    updated_at: float
    # Populated from audit.jobs (DB) when the job has been promoted as a
    # strategy version. NULL for audits that were never promoted. Frontend
    # uses strategy_version_id to render an "Already promoted" badge on the
    # Results view without a second round-trip.
    strategy_version_id: str | None = None
    promoted_at:         str | None = None


class JobPatchRequest(BaseModel):
    display_name: str | None = None
    folder_id: str | None = None


class JobRenameRequest(BaseModel):
    display_name: str | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@router.post("", response_model=JobCreateResponse, status_code=202)
def create_job_endpoint(req: JobRequest) -> JobCreateResponse:
    job_id = str(uuid.uuid4())
    create_job(job_id, req.model_dump())
    task = run_pipeline.delay(job_id, req.model_dump())
    update_job(job_id, task_id=task.id)
    return JobCreateResponse(job_id=job_id, status="queued")


_jobs_log = logging.getLogger("jobs_route")


def _fetch_promote_state(job_ids: list[str]) -> dict[str, dict[str, Any]]:
    """
    Best-effort lookup of strategy_version_id + promoted_at for a set of
    job_ids. Returns a dict keyed by job_id; absent entries = unknown. A DB
    failure logs a warning and returns {} so the endpoint still serves the
    primary JSON-file data.
    """
    if not job_ids:
        return {}
    try:
        conn = get_worker_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_id::text AS job_id,
                           strategy_version_id::text AS strategy_version_id,
                           promoted_at
                    FROM audit.jobs
                    WHERE job_id = ANY(%s::uuid[])
                    """,
                    (job_ids,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()
    except Exception as e:
        _jobs_log.warning("promote-state lookup failed: %s", e)
        return {}

    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        out[row[0]] = {
            "strategy_version_id": row[1],
            "promoted_at": row[2].isoformat() if row[2] is not None else None,
        }
    return out


def _enrich_with_promote_state(job: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    merged = dict(job)
    merged["strategy_version_id"] = state.get("strategy_version_id")
    merged["promoted_at"]         = state.get("promoted_at")
    return merged


@router.get("", response_model=list[JobResponse])
def list_jobs_endpoint() -> list[JobResponse]:
    jobs = list_jobs()[:20]
    refreshed = [_refresh_results_if_needed(job) for job in jobs]
    promote_state = _fetch_promote_state([j["id"] for j in refreshed])
    return [
        _enrich_with_promote_state(j, promote_state.get(j["id"], {}))
        for j in refreshed
    ]


@router.get("/{job_id}", response_model=JobResponse)
def get_job_endpoint(job_id: str) -> JobResponse:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    promote_state = _fetch_promote_state([job_id]).get(job_id, {})
    return _enrich_with_promote_state(job, promote_state)


@router.patch("/{job_id}", response_model=JobResponse)
def patch_job_endpoint(job_id: str, req: JobPatchRequest) -> JobResponse:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    fields: dict[str, Any] = {}
    if "display_name" in req.model_fields_set:
        raw = req.display_name
        if isinstance(raw, str):
            name = raw.strip()
            fields["display_name"] = name or None
        else:
            fields["display_name"] = None
    if "folder_id" in req.model_fields_set:
        fields["folder_id"] = req.folder_id

    if not fields:
        return job

    return update_job(job_id, **fields)


@router.post("/{job_id}/rename", response_model=JobResponse)
def rename_job_endpoint(job_id: str, req: JobRenameRequest) -> JobResponse:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    raw = req.display_name
    if isinstance(raw, str):
        name = raw.strip()
        return update_job(job_id, display_name=name or None)
    return update_job(job_id, display_name=None)


@router.delete("/{job_id}")
def delete_job_endpoint(job_id: str) -> dict[str, str]:
    if not delete_job(job_id):
        raise HTTPException(status_code=404, detail="Job not found")
    return {"status": "deleted", "job_id": job_id}


@router.post("/{job_id}/cancel")
def cancel_job_endpoint(job_id: str) -> dict[str, str]:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    status = str(job.get("status", "")).lower()
    if status in {"complete", "completed", "done", "failed", "error", "cancelled", "canceled"}:
        return {"status": status, "job_id": job_id}

    task_id = job.get("task_id")
    if task_id:
        run_pipeline.AsyncResult(task_id).revoke(terminate=True, signal="SIGTERM")

    update_job(job_id, status="cancelled", stage="done", error="Cancelled by user.")
    return {"status": "cancelled", "job_id": job_id}


@router.get("/{job_id}/results")
def get_job_results(job_id: str) -> dict[str, Any]:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] != "complete":
        raise HTTPException(status_code=409, detail=f"Job status is '{job['status']}', not 'complete'")
    refreshed = _refresh_results_if_needed(job)
    return (refreshed.get("results") or {}) if isinstance(refreshed.get("results"), dict) else {}


@router.get("/{job_id}/output")
def get_job_output(job_id: str) -> dict[str, str]:
    """Live audit log for the simulator's STREAMING panel.

    For single-variant runs (blofin_variants in {off, blofin_only}), audit
    stdout streams into audit_output.txt directly.

    For blofin_variants=both, the wrapper writes vanilla output into
    audit_output_vanilla.txt during the vanilla pass, then BloFin output
    into audit_output_blofin.txt during the BloFin pass, and only
    concatenates into audit_output.txt at the very end. To keep the
    panel populated during both passes, fall back to synthesizing from
    the per-variant files in order, with separators.
    """
    job_dir = Path(settings.JOBS_DIR) / job_id
    final = job_dir / "audit_output.txt"
    if final.exists():
        return {"text": final.read_text(errors="replace")}
    parts: list[str] = []
    vanilla = job_dir / "audit_output_vanilla.txt"
    if vanilla.exists():
        parts.append(
            "══════════════════════════════════════════════════════════════════\n"
            "  VANILLA PASS (no BloFin universe restriction)\n"
            "══════════════════════════════════════════════════════════════════\n"
        )
        parts.append(vanilla.read_text(errors="replace"))
    blofin = job_dir / "audit_output_blofin.txt"
    if blofin.exists():
        if parts:
            parts.append(
                "\n\n══════════════════════════════════════════════════════════════════\n"
                "  BLOFIN PASS (universe restricted to BloFin SWAP listings)\n"
                "══════════════════════════════════════════════════════════════════\n"
            )
        parts.append(blofin.read_text(errors="replace"))
    return {"text": "".join(parts)}


@router.post("/{job_id}/report")
def generate_report(job_id: str) -> dict[str, str]:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") != "complete":
        raise HTTPException(status_code=409, detail="Job is not complete")

    results = job.get("results") or {}
    audit_output_path = results.get("audit_output_path")
    if not audit_output_path or not Path(audit_output_path).exists():
        raise HTTPException(status_code=404, detail="audit_output.txt not found")

    job_dir = Path(settings.JOBS_DIR) / job_id
    report_out = job_dir / "overlap_audit_report.docx"
    report_script = Path(settings.PIPELINE_DIR) / "generate_audit_report.js"

    result = subprocess.run(
        [settings.NODE_BIN, str(report_script), audit_output_path, str(report_out)],
        capture_output=True,
        text=True,
        cwd=str(job_dir),
    )
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"Report generation failed: {result.stderr[:1000]}",
        )

    results["report_path"] = str(report_out)
    update_job(job_id, results=results)
    return {"report_path": str(report_out)}


@router.get("/{job_id}/download/report")
def download_report(job_id: str) -> FileResponse:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    results = job.get("results") or {}
    report_path = results.get("report_path")
    if not report_path or not Path(report_path).exists():
        raise HTTPException(status_code=404, detail="Report file not found")
    return FileResponse(
        path=report_path,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=f"audit_report_{job_id[:8]}.docx",
    )


# ---------------------------------------------------------------------------
# Charts — audit-generated image gallery
# ---------------------------------------------------------------------------
#
# At audit time, the pipeline drops dozens of PNG/JPG/SVG files (equity
# curves, drawdown analysis, sensitivity heatmaps, regime comparisons,
# etc.) into a timestamped run-dir. The worker copies them into
# job_dir/charts/ so they can be served per-job.
#
# Charts are categorized for UI grouping:
#   - "comparison"  Files that span multiple filters (regime_filter_*,
#                   pbo_cscv_*, monthly_cumulative_*, equity_curve_ensemble_*)
#   - "<filter>"    Per-filter files, identified by leading "A_-_<Filter>_"
#                   or other strategy-specific prefix
#   - "blofin/<…>"  Same shape, but from the BloFin variant pass — kept in a
#                   parallel category so 10-row pair audits don't crowd

_CHART_EXTS = {".png", ".jpg", ".jpeg", ".svg"}


def _classify_chart(filename: str) -> str:
    """Return a UI category string for a chart filename."""
    stem = filename.lower()
    is_blofin = stem.startswith("blofin_")
    if is_blofin:
        stem = stem[len("blofin_"):]
    prefix = "blofin/" if is_blofin else ""

    # Comparison-style filenames don't have a per-filter prefix
    comparison_markers = (
        "regime_filter_comparison",
        "monthly_cumulative_returns",
        "equity_curve_ensemble",
        "pbo_cscv",
    )
    if any(m in stem for m in comparison_markers):
        return f"{prefix}comparison"

    # Per-filter prefix: "a_-_<filter>_..." (e.g. a_-_tail_guardrail_*)
    # The audit normalizes spaces / pluses to underscores in filenames.
    if stem.startswith("a_-_") or stem.startswith("a__"):
        rest = stem[4:] if stem.startswith("a_-_") else stem[3:]
        # Stop at the first known plot-type token to leave the filter name
        for sep in (
            "_drawdown_analysis", "_equity_curve", "_performance_dashboard",
            "_disp_decile", "_disp_surface", "_dispersion_scatter",
            "_btc_vol_scatter", "_skew_vs_equity", "_sharpe_vs_corr",
            "_regime_heatmap", "_strategy_vs_btc",
        ):
            if sep in rest:
                rest = rest.split(sep, 1)[0]
                break
        # Compress trailing underscores
        rest = rest.rstrip("_") or "filter"
        return f"{prefix}{rest}"

    return f"{prefix}other"


@router.get("/{job_id}/charts")
def list_charts(job_id: str) -> dict:
    """List audit-generated chart files for a job, grouped by category."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    charts_dir = Path(settings.JOBS_DIR) / job_id / "charts"
    if not charts_dir.is_dir():
        return {"job_id": job_id, "charts_dir_present": False, "categories": []}

    by_cat: dict[str, list[dict]] = {}
    for p in sorted(charts_dir.iterdir()):
        if not p.is_file() or p.suffix.lower() not in _CHART_EXTS:
            continue
        cat = _classify_chart(p.name)
        by_cat.setdefault(cat, []).append({
            "filename":  p.name,
            "url":       f"/api/jobs/{job_id}/charts/{p.name}",
            "size_bytes": p.stat().st_size,
        })

    # Stable category ordering: comparison first, then per-filter alpha,
    # then blofin/ variants at the end.
    def _cat_sort(c: str) -> tuple[int, str]:
        if c == "comparison":           return (0, c)
        if c.startswith("blofin/comparison"):  return (10, c)
        if c.startswith("blofin/"):     return (20, c)
        return (5, c)

    categories = []
    for cat in sorted(by_cat.keys(), key=_cat_sort):
        categories.append({"name": cat, "files": by_cat[cat]})

    total_count = sum(len(c["files"]) for c in categories)
    return {
        "job_id": job_id,
        "charts_dir_present": True,
        "total_count": total_count,
        "categories": categories,
    }


@router.get("/{job_id}/charts/{filename}")
def serve_chart(job_id: str, filename: str) -> FileResponse:
    """Serve a single chart file from job_dir/charts/."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    # Path-traversal guard: filename must be a single component, no slashes
    if "/" in filename or "\\" in filename or filename.startswith(".."):
        raise HTTPException(status_code=400, detail="Invalid filename")
    charts_dir = Path(settings.JOBS_DIR) / job_id / "charts"
    target = (charts_dir / filename).resolve()
    # Resolve loop just in case + final containment check
    if not str(target).startswith(str(charts_dir.resolve())):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not target.is_file() or target.suffix.lower() not in _CHART_EXTS:
        raise HTTPException(status_code=404, detail="Chart not found")
    media_type = {
        ".png":  "image/png",
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".svg":  "image/svg+xml",
    }.get(target.suffix.lower(), "application/octet-stream")
    return FileResponse(
        path=str(target),
        media_type=media_type,
        # Hint browsers to cache aggressively; charts are immutable per-job
        headers={"Cache-Control": "private, max-age=86400"},
    )


# ---------------------------------------------------------------------------
# Per-day basket detail — diagnostic modal for the Breakdown tab
# ---------------------------------------------------------------------------
#
# Returns per-symbol intraday cumulative ROI for a specific basket date,
# plus Binance and BloFin basket lists derived from the job's eligibility
# gate report. Used by the Breakdown tab's per-day click-to-expand modal.
#
# The "Binance basket" is reconstructed by union-ing the gate-report's
# `eligible` symbols (= what the audit actually traded) with `dropped`
# symbols whose drop_reason was "not_on_blofin_at_date" (i.e. dropped by
# the BloFin universe filter — present in the original strategy basket
# but unavailable on BloFin at the date). The "BloFin basket" is just
# `eligible`.
#
# Per-symbol cumulative ROI: for each base in the Binance basket, query
# market.futures_1m for 5-min closes from session-start to session-end,
# resample / forward-fill to a fixed bar grid, then apply per-symbol
# stop (-6% default) — same formula as
# pipeline/rebuild_portfolio_matrix.py:apply_raw_stop. Returns
# UNLEVERAGED per-symbol % paths so the frontend can compute portfolio
# averages on the fly when the user toggles Binance vs BloFin.

import csv as _csv

_BAR_MINUTES = 5
_N_BARS_DEFAULT = 216  # 18 hours × 12 bars/hour (06:00 → 23:55)


def _apply_raw_stop_pct(prices: list[float], stop_raw_pct: float) -> list[float | None]:
    """Per-symbol cumulative %% return from entry, clamped at stop_raw_pct.
    Mirrors pipeline.rebuild_portfolio_matrix.apply_raw_stop. Returns a
    list (None for NaN or pre-entry bars)."""
    if not prices:
        return []
    p0 = prices[0]
    if p0 is None or p0 == 0:
        return [None] * len(prices)
    out: list[float | None] = []
    stopped = False
    for p in prices:
        if p is None:
            out.append(None)
            continue
        if stopped:
            out.append(stop_raw_pct)
            continue
        raw = (p / p0 - 1.0) * 100.0
        if raw <= stop_raw_pct:
            out.append(stop_raw_pct)
            stopped = True
        else:
            out.append(raw)
    return out


def _to_ticker(base: str, first_seen_raw: dict[str, str]) -> str:
    """Reconstruct the futures market symbol from a base, e.g. BTC → BTCUSDT.
    1000RATS → 1000RATSUSDT (via first_seen_raw lookup). Mirrors
    pipeline.rebuild_portfolio_matrix.to_ticker."""
    raw = first_seen_raw.get(base)
    if raw:
        return raw
    if base.endswith("USDT"):
        return base
    if base.endswith("USD"):
        return base + "T"
    return base + "USDT"


def _load_first_seen_raw() -> dict[str, str]:
    """Load BASE_DATA_DIR/symbol_first_seen.csv → {base: full_ticker}.
    Empty dict if the file is absent."""
    path = Path(settings.BASE_DATA_DIR) / "symbol_first_seen.csv"
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    with path.open(newline="") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            sym_full = (row.get("symbol") or "").strip()
            if not sym_full:
                continue
            base = sym_full.replace("USDT", "").replace("USDC", "")
            out[base] = sym_full
    return out


def _query_futures_1m_pivot(
    symbols: list[str],
    t_start,
    t_end,
):
    """Query market.futures_1m for prices, return {symbol: [bar_prices]}
    after resampling to 5-min bars on a fixed grid t_start → t_end."""
    import pandas as pd
    if not symbols:
        return {}, []
    conn = get_worker_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT f.timestamp_utc, s.binance_id AS symbol, f.close AS price
                FROM market.futures_1m f
                JOIN market.symbols s ON s.symbol_id = f.symbol_id
                WHERE f.source_id = 1
                  AND f.timestamp_utc >= %s
                  AND f.timestamp_utc <= %s
                  AND s.binance_id = ANY(%s)
                  AND f.close IS NOT NULL
                ORDER BY f.timestamp_utc
                """,
                (
                    pd.Timestamp(t_start).to_pydatetime(),
                    pd.Timestamp(t_end).to_pydatetime(),
                    list(symbols),
                ),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return {}, []

    df = pd.DataFrame(rows, columns=["timestamp_utc", "symbol", "price"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    if df["timestamp_utc"].dt.tz is not None:
        df["timestamp_utc"] = df["timestamp_utc"].dt.tz_localize(None)
    pivot = df.pivot_table(
        index="timestamp_utc", columns="symbol",
        values="price", aggfunc="last",
    )
    bar_grid = pd.date_range(
        start=pd.Timestamp(t_start),
        end=pd.Timestamp(t_end) - pd.Timedelta(minutes=_BAR_MINUTES),
        freq=f"{_BAR_MINUTES}min",
    )
    resampled = (
        pivot.resample(f"{_BAR_MINUTES}min").last()
              .reindex(bar_grid)
              .ffill().bfill()
    )
    out: dict[str, list[float | None]] = {}
    for sym in resampled.columns:
        col_vals = resampled[sym].tolist()
        out[sym] = [None if (v is None or (isinstance(v, float) and v != v)) else float(v) for v in col_vals]
    return out, [ts.isoformat() for ts in bar_grid]


@router.get("/{job_id}/baskets/{date}")
def get_basket_detail(job_id: str, date: str, filter: str | None = None):
    """Per-symbol intraday data for a single basket date.

    The optional `filter` query param (e.g. "A - Tail Guardrail") makes the
    response carry the audit's canonical daily return for that filter+date,
    sourced from `metrics.fees_tables_by_filter` — the same source the fees
    panel reads. The frontend uses this for the Portfolio ROI KPI so the
    modal stays consistent with the equity curve / monthly heatmap / fees
    panel (all four read the same simulation output).

    Without the param the canonical fields come back null and the modal
    falls back to the naive intraday basket aggregate.

    Response shape:
      {
        "job_id":             str,
        "date":               "YYYY-MM-DD",
        "session_start":      "YYYY-MM-DDTHH:MM:SS",
        "leverage":           4.0,
        "stop_raw_pct":       -6.0,
        "binance_basket":     ["BTC", "ETH", ...],   # what the strategy intended
        "blofin_basket":      ["BTC", "ETH", ...],   # subset, BloFin-tradeable
        "non_blofin_dropped": ["FARTCOIN", ...],     # in Binance but not BloFin
        "bar_timestamps":     ["2025-02-13T06:00:00", ...],
        "bars":               [
          {
            "ts": "2025-02-13T06:00:00",
            "sym_returns": {"BTC": 0.0, "ETH": 0.0, ...},  # unleveraged %
            "portfolio_binance": 0.0,                      # leveraged %
            "portfolio_blofin": 0.0,                       # leveraged %
          }, ...
        ],
        "symbols": [
          {"base": "BTC", "in_binance_basket": true, "in_blofin_basket": true,
           "blofin_listed_at_date": true, "list_ms": 1673517600000, "list_date": "2023-01-12"},
          ...
        ]
      }
    """
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    # Validate date
    try:
        import pandas as pd
        target_ts = pd.Timestamp(date).normalize()
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid date: {date!r}")
    target_str = target_ts.strftime("%Y-%m-%d")

    # Read this job's gate report and find the row for date.
    # Falls back to audit.daily_baskets DB table for older jobs that
    # predate the gate-report-snapshot worker logic — those audits
    # have basket data in postgres but no on-disk CSV. The DB row
    # only carries the post-rebuild eligible basket (no drop_reasons),
    # so we reconstruct BO/BF classification via listTime in that path.
    gate_path = Path(settings.JOBS_DIR) / job_id / "eligibility_gate_report.csv"
    eligible_bases: list[str] = []
    dropped_bases: list[str] = []
    drop_reasons:  list[str] = []
    found = False
    csv_was_present = gate_path.exists()
    if csv_was_present:
        with gate_path.open(newline="") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                if (row.get("date") or "").strip() != target_str:
                    continue
                found = True
                eligible_bases = [s for s in (row.get("eligible") or "").split("|") if s]
                dropped_bases  = [s for s in (row.get("dropped") or "").split("|") if s]
                drop_reasons   = [r for r in (row.get("drop_reasons") or "").split("|") if r]
                break

    fallback_used = False
    if not found:
        # Fallback 1 — audit.daily_baskets (DB-backed, populated by the
        # simulator celery worker; nightly cron audits don't write here).
        try:
            conn = get_worker_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT basket
                        FROM audit.daily_baskets
                        WHERE job_id = %s::uuid AND date = %s
                        LIMIT 1
                        """,
                        (job_id, target_str),
                    )
                    db_row = cur.fetchone()
            finally:
                conn.close()
        except Exception as e:
            _jobs_log.warning("basket_detail: daily_baskets DB lookup failed: %s", e)
            db_row = None

        if db_row and db_row[0]:
            eligible_bases = list(db_row[0])
            fallback_used = True
            found = True

    if not found:
        # Fallback 2 — metrics.daily_portfolio in job.json. Always present
        # for completed audits (written by audit.py at end-of-run regardless
        # of which celery path triggered the audit). Each entry is a dict:
        #   {symbols, filter, filter_name, conviction, raw_roi,
        #    strat_roi, exit_reason}
        # The 'symbols' list IS the day's basket as evaluated by the audit.
        m = (job.get("results") or {}).get("metrics") or {}
        daily_portfolio = m.get("daily_portfolio") or {}
        if isinstance(daily_portfolio, dict):
            entry = daily_portfolio.get(target_str)
            if isinstance(entry, dict):
                syms = entry.get("symbols") or []
                if syms:
                    eligible_bases = list(syms)
                    fallback_used = True
                    found = True

    if not found:
        if csv_was_present:
            raise HTTPException(
                status_code=404,
                detail=f"Date {target_str} not in this job's gate report",
            )
        raise HTTPException(
            status_code=404,
            detail=(
                f"No basket data for this job on {target_str}. Tried: "
                "gate-report CSV (job_dir), audit.daily_baskets (DB), "
                "metrics.daily_portfolio (job.json)."
            ),
        )

    if fallback_used:
        # The DB-backed basket has no drop_reasons. Classify each symbol
        # via listTime: bases NOT on BloFin at the target date are the
        # implied non_blofin_dropped set. This works correctly for vanilla
        # audits (the DB basket IS the Binance basket). For old BloFin-
        # variant audits the DB basket is already filtered, so this path
        # under-reports non_blofin_dropped — acceptable trade-off.
        try:
            import sys as _sys
            _pdir = str(Path(settings.PIPELINE_DIR))
            if _pdir not in _sys.path:
                _sys.path.insert(0, _pdir)
            from blofin_universe import (  # type: ignore[import-not-found]
                load_blofin_universe,
                is_listed_at,
            )
            _early_universe = load_blofin_universe()
        except Exception as e:
            _jobs_log.warning("basket_detail: BloFin universe load failed: %s", e)
            _early_universe = {}
        non_blofin_dropped = [
            s for s in eligible_bases
            if _early_universe and not is_listed_at(s, target_str, _early_universe)
        ]
        blofin_basket = sorted({s for s in eligible_bases if s not in non_blofin_dropped})
        binance_basket = sorted(set(eligible_bases))
    else:
        non_blofin_dropped = [
            sym for sym, reason in zip(dropped_bases, drop_reasons)
            if reason == "not_on_blofin_at_date"
        ]
        blofin_basket = sorted(set(eligible_bases))
        binance_basket = sorted(set(eligible_bases) | set(non_blofin_dropped))

    # Job params for session config
    params = job.get("params") or {}
    start_hour = int(params.get("deployment_start_hour", 6))
    leverage = float(params.get("leverage", 4.0))
    stop_raw_pct = float(params.get("stop_raw_pct", -6.0))

    # Bar grid
    session_start = target_ts + pd.Timedelta(hours=start_hour)
    t_start = session_start
    t_end = session_start + pd.Timedelta(minutes=_BAR_MINUTES * _N_BARS_DEFAULT)

    # Resolve symbols → market tickers (BTC → BTCUSDT, 1000RATS → 1000RATSUSDT)
    first_seen_raw = _load_first_seen_raw()
    sym_to_ticker = {b: _to_ticker(b, first_seen_raw) for b in binance_basket}
    ticker_to_sym = {v: k for k, v in sym_to_ticker.items()}

    # Query prices
    sym_prices, bar_timestamps = _query_futures_1m_pivot(
        symbols=list(sym_to_ticker.values()),
        t_start=t_start,
        t_end=t_end,
    )

    # Compute per-symbol cumulative ROI with stop applied
    sym_returns_per_bar: dict[str, list[float | None]] = {}
    for ticker, prices in sym_prices.items():
        base = ticker_to_sym.get(ticker)
        if not base:
            continue
        sym_returns_per_bar[base] = _apply_raw_stop_pct(prices, stop_raw_pct)

    # Build bars[] array with per-bar sym_returns + portfolio aggregates
    n_bars = len(bar_timestamps)
    bars_out = []
    for i in range(n_bars):
        sym_at_bar = {
            base: vals[i] for base, vals in sym_returns_per_bar.items()
            if i < len(vals)
        }
        valid_b = [v for sym, v in sym_at_bar.items()
                   if sym in binance_basket and v is not None]
        valid_bf = [v for sym, v in sym_at_bar.items()
                    if sym in blofin_basket and v is not None]
        port_b = (sum(valid_b) / len(valid_b) * leverage) if valid_b else 0.0
        port_bf = (sum(valid_bf) / len(valid_bf) * leverage) if valid_bf else 0.0
        bars_out.append({
            "ts": bar_timestamps[i],
            "sym_returns": sym_at_bar,
            "portfolio_binance": port_b,
            "portfolio_blofin": port_bf,
        })

    # BloFin universe metadata (listTime per base)
    universe = {}
    try:
        # Same-process import to avoid subprocess overhead
        import sys as _sys
        _pipeline_dir = str(Path(settings.PIPELINE_DIR))
        if _pipeline_dir not in _sys.path:
            _sys.path.insert(0, _pipeline_dir)
        from blofin_universe import (  # type: ignore[import-not-found]
            load_blofin_universe,
            is_listed_at,
        )
        universe = load_blofin_universe()
    except Exception as e:
        _jobs_log.warning("basket_detail: BloFin universe load failed: %s", e)
        universe = {}

    sym_meta = []
    for base in binance_basket:
        list_ms = universe.get(base) if universe else None
        listed_at_date = bool(universe) and (
            is_listed_at(base, target_str, universe) if universe else False
        )
        list_date_iso = None
        if list_ms:
            list_date_iso = pd.Timestamp(list_ms, unit="ms", tz="UTC").strftime("%Y-%m-%d")
        sym_meta.append({
            "base": base,
            "in_binance_basket": base in binance_basket,
            "in_blofin_basket":  base in blofin_basket,
            "blofin_listed_at_date": listed_at_date,
            "list_ms": list_ms,
            "list_date": list_date_iso,
        })

    # Audit-canonical daily return for this filter+date (from the same
    # simulation pass that drives the equity curve, monthly heatmap, and
    # fees panel). When filter is None or no matching row is found, these
    # come back None and the frontend falls back to the naive intraday
    # basket aggregate. ret_net is "%", e.g. -9.01 for a -9.01% day.
    audit_daily_return_pct: float | None = None
    audit_no_entry: bool | None = None
    audit_no_entry_reason: str | None = None
    audit_filter_label: str | None = None
    if filter:
        m = (job.get("results") or {}).get("metrics") or {}
        fees_by_filter = m.get("fees_tables_by_filter") or {}
        # Tolerant lookup: exact match first, then case-insensitive whitespace-normalized.
        rows = fees_by_filter.get(filter)
        if rows is None and isinstance(fees_by_filter, dict):
            wanted = " ".join(filter.split()).lower()
            for k, v in fees_by_filter.items():
                if " ".join(str(k).split()).lower() == wanted:
                    rows = v
                    audit_filter_label = k
                    break
        else:
            audit_filter_label = filter
        if isinstance(rows, list):
            for r in rows:
                if isinstance(r, dict) and (r.get("date") or "") == target_str:
                    rn = r.get("ret_net")
                    if isinstance(rn, (int, float)):
                        audit_daily_return_pct = float(rn)
                    ne = r.get("no_entry")
                    if isinstance(ne, bool):
                        audit_no_entry = ne
                    ner = r.get("no_entry_reason")
                    if isinstance(ner, str):
                        audit_no_entry_reason = ner
                    break

    return {
        "job_id":                   job_id,
        "date":                     target_str,
        "session_start":            session_start.isoformat(),
        "bar_minutes":              _BAR_MINUTES,
        "leverage":                 leverage,
        "stop_raw_pct":             stop_raw_pct,
        "binance_basket":           binance_basket,
        "blofin_basket":            blofin_basket,
        "non_blofin_dropped":       sorted(non_blofin_dropped),
        "bar_timestamps":           bar_timestamps,
        "bars":                     bars_out,
        "symbols":                  sym_meta,
        "audit_filter_label":       audit_filter_label,
        "audit_daily_return_pct":   audit_daily_return_pct,
        "audit_no_entry":           audit_no_entry,
        "audit_no_entry_reason":    audit_no_entry_reason,
    }


# ---------------------------------------------------------------------------
# Per-symbol contribution attribution — BO vs BF
# ---------------------------------------------------------------------------
#
# Decomposes the audit's daily portfolio return into per-symbol
# contributions, then aggregates by category:
#   binance_only  — symbols dropped on BloFin (drop_reason='not_on_blofin_at_date')
#   blofin       — symbols on both Binance and BloFin (the BloFin-tradeable subset)
#
# Each symbol's daily contribution to the leveraged portfolio return:
#   contribution = (symbol_cumulative_pct / N) * leverage
# where N is the day's basket size. These contributions sum to the
# day's leveraged return — the same formula the audit uses to build
# the portfolio matrix.
#
# Computation is ~30s for a 14-month audit (440 daily SQL queries on
# market.futures_1m). Result is cached as JSON in
# job_dir/attribution.json — subsequent calls return instantly.

import time as _time

ATTRIBUTION_CACHE_NAME = "attribution.json"


def _compute_attribution(job_id: str, job: dict) -> dict:
    """The hot loop. Returns a serializable dict ready for the API."""
    import pandas as pd

    params = job.get("params") or {}
    start_hour = int(params.get("deployment_start_hour", 6))
    leverage = float(params.get("leverage", 4.0))
    stop_raw_pct = float(params.get("stop_raw_pct", -6.0))

    gate_path = Path(settings.JOBS_DIR) / job_id / "eligibility_gate_report.csv"
    csv_present = gate_path.exists()

    # Build the per-day basket source. CSV path gives us drop_reasons
    # (so we know which symbols were filtered out by BloFin filter at
    # rebuild). DB fallback gives us only the eligible basket — for BO
    # classification we re-derive via listTime when CSV is absent.
    rows: list[dict] = []
    if csv_present:
        with gate_path.open(newline="") as f:
            rows = list(_csv.DictReader(f))
    else:
        # Fallback 1 — audit.daily_baskets (DB)
        db_rows: list = []
        try:
            conn = get_worker_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT date::text AS date, basket
                        FROM audit.daily_baskets
                        WHERE job_id = %s::uuid
                        ORDER BY date
                        """,
                        (job_id,),
                    )
                    db_rows = cur.fetchall()
            finally:
                conn.close()
        except Exception as e:
            _jobs_log.warning("attribution: daily_baskets DB lookup failed: %s", e)
            db_rows = []
        if db_rows:
            rows = [
                {"date": r[0], "eligible": "|".join(r[1] or []),
                 "dropped": "", "drop_reasons": ""}
                for r in db_rows
            ]
        else:
            # Fallback 2 — metrics.daily_portfolio in job.json (always
            # written by audit.py at end-of-run; the only universally
            # available source for nightly-cron audits).
            m = (job.get("results") or {}).get("metrics") or {}
            daily_portfolio = m.get("daily_portfolio") or {}
            if isinstance(daily_portfolio, dict):
                rows = [
                    {"date": date_str,
                     "eligible": "|".join((entry or {}).get("symbols") or []),
                     "dropped": "",
                     "drop_reasons": ""}
                    for date_str, entry in daily_portfolio.items()
                    if isinstance(entry, dict) and (entry.get("symbols") or [])
                ]
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=(
                    "No basket data for this job. Tried: gate-report CSV, "
                    "audit.daily_baskets DB, metrics.daily_portfolio."
                ),
            )

    # Lazy-load BloFin universe (only needed in DB-fallback mode)
    blofin_universe_cache: dict[str, int] | None = None

    def _is_bo_via_listtime(sym: str, date_str: str) -> bool:
        nonlocal blofin_universe_cache
        if blofin_universe_cache is None:
            try:
                import sys as _sys
                _pdir = str(Path(settings.PIPELINE_DIR))
                if _pdir not in _sys.path:
                    _sys.path.insert(0, _pdir)
                from blofin_universe import (  # type: ignore[import-not-found]
                    load_blofin_universe,
                )
                blofin_universe_cache = load_blofin_universe()
            except Exception:
                blofin_universe_cache = {}
        if not blofin_universe_cache:
            return False
        from blofin_universe import is_listed_at  # type: ignore[import-not-found]
        return not is_listed_at(sym, date_str, blofin_universe_cache)

    first_seen_raw = _load_first_seen_raw()
    sym_contrib_total: dict[str, float] = {}
    sym_count: dict[str, int] = {}
    sym_is_bo: dict[str, bool] = {}
    per_day: list[dict] = []
    started = _time.time()

    for row in rows:
        date = (row.get("date") or "").strip()
        if not date:
            continue
        eligible = [s for s in (row.get("eligible") or "").split("|") if s]
        dropped = [s for s in (row.get("dropped") or "").split("|") if s]
        drop_reasons = [r for r in (row.get("drop_reasons") or "").split("|") if r]
        if csv_present:
            binance_only = {sym for sym, reason in zip(dropped, drop_reasons)
                            if reason == "not_on_blofin_at_date"}
            blofin_basket = set(eligible)
            binance_basket = sorted(blofin_basket | binance_only)
        else:
            # DB fallback: classify via listTime. Note `eligible` may
            # already be BloFin-filtered for old BloFin-variant audits;
            # we treat the basket as Binance-equivalent and back-derive
            # the BO subset from listTime — best we can do without
            # drop_reasons.
            binance_basket = sorted(set(eligible))
            binance_only = {s for s in binance_basket if _is_bo_via_listtime(s, date)}
            blofin_basket = set(binance_basket) - binance_only
        if not binance_basket:
            continue

        target_ts = pd.Timestamp(date).normalize()
        session_start = target_ts + pd.Timedelta(hours=start_hour)
        t_end = session_start + pd.Timedelta(minutes=_BAR_MINUTES * _N_BARS_DEFAULT)
        sym_to_ticker = {b: _to_ticker(b, first_seen_raw) for b in binance_basket}
        sym_prices, _ts_grid = _query_futures_1m_pivot(
            symbols=list(sym_to_ticker.values()),
            t_start=session_start,
            t_end=t_end,
        )
        ticker_to_sym = {v: k for k, v in sym_to_ticker.items()}
        sym_final_pct: dict[str, float] = {}
        for ticker, prices in sym_prices.items():
            base = ticker_to_sym.get(ticker)
            if not base:
                continue
            rets = _apply_raw_stop_pct(prices, stop_raw_pct)
            if rets and rets[-1] is not None:
                sym_final_pct[base] = float(rets[-1])

        valid = [(s, v) for s, v in sym_final_pct.items() if v is not None]
        if not valid:
            continue
        n = len(valid)
        bo_contrib = sum(v for s, v in valid if s in binance_only) / n * leverage / 100
        bf_contrib = sum(v for s, v in valid if s in blofin_basket) / n * leverage / 100
        daily_ret = bo_contrib + bf_contrib

        for s, v in valid:
            sym_contrib_total[s] = sym_contrib_total.get(s, 0.0) + v / n * leverage / 100
            sym_count[s] = sym_count.get(s, 0) + 1
            sym_is_bo[s] = s in binance_only

        per_day.append({
            "date": date,
            "n_binance": n,
            "n_bo": sum(1 for s, _ in valid if s in binance_only),
            "n_bf": sum(1 for s, _ in valid if s in blofin_basket),
            "daily_ret": daily_ret,
            "bo_contrib": bo_contrib,
            "bf_contrib": bf_contrib,
        })

    n_days = len(per_day)
    total_pp = sum(r["daily_ret"] for r in per_day)
    bo_pp = sum(r["bo_contrib"] for r in per_day)
    bf_pp = sum(r["bf_contrib"] for r in per_day)

    # Compounded counterfactual
    eq_actual = 1.0
    eq_no_bo = 1.0
    for r in per_day:
        eq_actual *= (1 + r["daily_ret"])
        eq_no_bo *= (1 + r["bf_contrib"])

    profit_actual = eq_actual - 1
    profit_no_bo = eq_no_bo - 1
    bo_share_compounded = (
        (profit_actual - profit_no_bo) / profit_actual
        if profit_actual > 0 else None
    )

    # Build symbol leaderboard
    symbols_ordered = sorted(sym_contrib_total.items(), key=lambda kv: -kv[1])
    bo_only_total = sum(c for s, c in symbols_ordered if sym_is_bo.get(s, False))

    cum = 0.0
    bo_leaderboard = []
    for s, c in symbols_ordered:
        if not sym_is_bo.get(s, False):
            continue
        cum += c
        bo_leaderboard.append({
            "base": s,
            "days": sym_count[s],
            "contrib_pp": c * 100,
            "cum_share": (cum / bo_only_total) if bo_only_total else None,
        })

    bf_leaderboard = []
    for s, c in symbols_ordered:
        if sym_is_bo.get(s, False):
            continue
        bf_leaderboard.append({
            "base": s,
            "days": sym_count[s],
            "contrib_pp": c * 100,
        })

    elapsed = _time.time() - started

    return {
        "job_id": job_id,
        "computed_at": int(_time.time()),
        "elapsed_sec": round(elapsed, 2),
        "n_days": n_days,
        "leverage": leverage,
        "stop_raw_pct": stop_raw_pct,
        "totals": {
            "total_pp": total_pp * 100,
            "from_blofin_tradeable_pp": bf_pp * 100,
            "from_binance_only_pp": bo_pp * 100,
            "bo_pct_of_total": (bo_pp / total_pp) if total_pp else None,
        },
        "compounded": {
            "actual_equity": eq_actual,
            "no_bo_equity": eq_no_bo,
            "actual_profit_pct": profit_actual * 100,
            "no_bo_profit_pct": profit_no_bo * 100,
            "bo_share_of_compounded_profit": bo_share_compounded,
        },
        "bo_total_pp": bo_only_total * 100,
        "bo_count": len(bo_leaderboard),
        "bo_leaderboard": bo_leaderboard,
        "bf_leaderboard": bf_leaderboard,
        "per_day": per_day,  # let frontend draw a cumulative-contribution chart
    }


@router.get("/{job_id}/attribution")
def get_attribution(job_id: str, refresh: bool = False) -> dict:
    """Per-symbol contribution analysis: BO vs BF.

    First call computes (~30s for a 14-month audit). Result cached at
    job_dir/attribution.json — subsequent calls return instantly.
    Pass ?refresh=true to force a recompute.
    """
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    cache_path = Path(settings.JOBS_DIR) / job_id / ATTRIBUTION_CACHE_NAME
    if cache_path.exists() and not refresh:
        try:
            return {**json.loads(cache_path.read_text()), "from_cache": True}
        except Exception:
            # Fall through to recompute on parse error
            pass

    result = _compute_attribution(job_id, job)
    try:
        cache_path.write_text(json.dumps(result))
    except Exception:
        # Best-effort cache write
        pass
    return {**result, "from_cache": False}


# ---------------------------------------------------------------------------
# Folder endpoints
# ---------------------------------------------------------------------------

class FolderCreateRequest(BaseModel):
    name: str


class FolderRenameRequest(BaseModel):
    name: str


@router.get("/folders/list")
def list_folders_endpoint() -> list[dict[str, Any]]:
    return list_folders()


@router.post("/folders")
def create_folder_endpoint(req: FolderCreateRequest) -> dict[str, Any]:
    return create_folder(req.name)


@router.patch("/folders/{folder_id}")
def rename_folder_endpoint(folder_id: str, req: FolderRenameRequest) -> dict[str, Any]:
    result = rename_folder(folder_id, req.name)
    if result is None:
        raise HTTPException(status_code=404, detail="Folder not found")
    return result


@router.delete("/folders/{folder_id}")
def delete_folder_endpoint(folder_id: str) -> dict[str, str]:
    if not delete_folder(folder_id):
        raise HTTPException(status_code=404, detail="Folder not found")
    return {"status": "deleted", "folder_id": folder_id}

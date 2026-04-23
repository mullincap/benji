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
    live_parity:                bool  = False
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

    # ── ADVANCED — Strategy tuning ────────────────────────────────────────────
    dispersion_threshold:       float = 0.66
    dispersion_baseline_win:    int   = 33
    dispersion_dynamic_universe: bool = True
    dispersion_n:               int   = 40
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
    def _check_live_parity_knob_mutex(self) -> "JobRequest":
        """Mirror overlap_analysis.py's argparse mutual-exclusion check at the
        HTTP boundary. Without this, combining live_parity with any individual
        ranking/filter knob queues a Celery job that fails deep inside the
        subprocess (argparse error), burning worker time and muddying the
        job status timeline. 422 at submit time is the correct UX.

        See docs/strategy_specification.md § 3.1. Updated 2026-04-23 for
        D-medium-split: live_parity conflicts with price_ranking_metric,
        oi_ranking_metric, apply_blofin_filter, and the deprecated
        `ranking_metric` field — live_parity's asymmetric v1-matching setup
        is already encoded internally and can't be re-parameterized per-knob
        without contradicting what v1 actually did."""
        if self.live_parity and (
            self.price_ranking_metric != "pct_change"
            or self.oi_ranking_metric != "pct_change"
            or self.apply_blofin_filter
            or (self.ranking_metric is not None and self.ranking_metric != "pct_change")
            or self.overlap_dimensions != "price_oi"
        ):
            raise ValueError(
                "live_parity is mutually exclusive with price_ranking_metric, "
                "oi_ranking_metric, apply_blofin_filter, the deprecated "
                "ranking_metric field, and overlap_dimensions (must stay "
                "'price_oi'). live_parity reproduces daily_signal.py v1 "
                "exactly (asymmetric: log-return on price, abs-$ on OI; "
                "top-100 by 24h volume; BloFin-filtered; price ∩ OI only) — "
                "its internal setup can't be layered with individual knobs. "
                "Use one OR the other per docs/strategy_specification.md § 3.1."
            )
        if "volume" in self.overlap_dimensions and self.source != "db":
            raise ValueError(
                f"overlap_dimensions={self.overlap_dimensions!r} requires "
                f"source='db'; got source={self.source!r}. Volume is not in "
                f"the pre-built market.leaderboards tables — the three-way "
                f"overlap must be computed on-the-fly from market.futures_1m. "
                f"Either set source='db' or use overlap_dimensions='price_oi'."
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
    job_dir = Path(settings.JOBS_DIR) / job_id
    output_path = job_dir / "audit_output.txt"
    if not output_path.exists():
        return {"text": ""}
    return {"text": output_path.read_text(errors="replace")}


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

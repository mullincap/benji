"""Shared helpers for invoking the audit pipeline subprocess.

Called by both:
  - app.workers.pipeline_worker.run_pipeline  (Simulator Celery task)
  - app.cli.refresh_strategy_metrics           (nightly CLI)

Wraps the existing "shell out to overlap_analysis.py, stream stdout, scrape
audit_output.txt" pattern. Not a replacement for audit.py — that refactor
lives in Track 3 (run_audit(params) -> AuditResult).

Extraction is behavior-preserving. Any drift from pipeline_worker.py's prior
inline implementation is a bug.
"""
import glob as _glob
import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, Optional

from app.core.config import settings
from app.services.audit.metrics_parser import parse_metrics

_PIPELINE_PYTHON = settings.PIPELINE_PYTHON


class JobCancelled(Exception):
    """Raised when the caller's cancellation_cb returns True mid-stream."""


def _boolenv(v: bool) -> str:
    return "1" if v else "0"


# ---------------------------------------------------------------------------
# CLI arg construction
# ---------------------------------------------------------------------------

def build_cli_args(params: dict) -> list[str]:
    """Map job params → overlap_analysis.py CLI flags.

    Only flags that overlap_analysis.py actually accepts go here.
    audit.py-only params (leverage, stop_raw_pct, min_listing_age, etc.)
    are passed via env vars in pipeline_env and do not appear on the CLI.
    """
    flag_map = {
        "leaderboard_index":        "--leaderboard-index",
        "min_mcap":                 "--min-mcap",
        "max_mcap":                 "--max-mcap",
        "sort_by":                  "--sort-by",
        "mode":                     "--mode",
        "sample_interval":          "--sample-interval",
        "freq_width":               "--freq-width",
        "freq_cutoff":              "--freq-cutoff",
        "deployment_start_hour":    "--deployment-start-hour",
        "index_lookback":           "--index-lookback",
        "sort_lookback":            "--sort-lookback",
        "deployment_runtime_hours": "--deployment-runtime-hours",
        "capital_mode":             "--capital-mode",
        "fixed_notional_cap":       "--fixed-notional-cap",
        "overlap_source":           "--source",
    }
    bool_flags = {
        "end_cross_midnight": "--end-cross-midnight",
        "drop_unverified":    "--drop-unverified",
        "quick":              "--quick",
    }

    audit_source = params.get("price_source", "parquet")
    args: list[str] = ["--audit", "--audit-source", audit_source]

    for param, flag in flag_map.items():
        value = params.get(param)
        if value is not None:
            args += [flag, str(value)]

    for param, flag in bool_flags.items():
        if params.get(param):
            args.append(flag)

    return args


# ---------------------------------------------------------------------------
# Env dict construction (formerly inline in pipeline_worker.run_pipeline)
# ---------------------------------------------------------------------------

def build_pipeline_env(params: dict) -> dict[str, str]:
    """Build the env dict for pipeline subprocess invocations.

    Every JobRequest param is forwarded as SNAKE_UPPER_CASE so audit.py
    can read it via os.environ.get("PARAM_NAME", default).
    """
    return {
        **os.environ,
        # Ensure pipeline python's directory is on PATH so subprocess "python3" resolves correctly
        "PATH": str(Path(_PIPELINE_PYTHON).parent) + ":" + os.environ.get("PATH", ""),
        # Force Python to flush stdout/stderr line-by-line instead of 4KB
        # block buffering. Without this, log.info() calls emitted by the
        # pipeline script accumulate in a buffer and only reach our stdout
        # pipe once the buffer fills or the process exits — which makes the
        # simulator's "LIVE OUTPUT" pane appear stuck during long quiet
        # phases (e.g. big DB queries).
        "PYTHONUNBUFFERED": "1",
        # Infrastructure paths
        "BASE_DATA_DIR":       str(settings.BASE_DATA_DIR),
        "PARQUET_PATH":        settings.PARQUET_PATH,
        "MARKETCAP_DIR":       settings.MARKETCAP_DIR,
        # Blank LOCAL_MATRIX_CSV tells audit.py to rebuild the matrix fresh
        "LOCAL_MATRIX_CSV":    "",

        # ── Basic ─────────────────────────────────────────────────────────────
        "LEADERBOARD_INDEX":          str(params.get("leaderboard_index", 100)),
        "SORT_BY":                    str(params.get("sort_by", "price")),
        "MODE":                       str(params.get("mode", "snapshot")),
        "FREQ_WIDTH":                 str(params.get("freq_width", 20)),
        "FREQ_CUTOFF":                str(params.get("freq_cutoff", 20)),
        "SAMPLE_INTERVAL":            str(params.get("sample_interval", 5)),
        "DEPLOYMENT_START_HOUR":      str(params.get("deployment_start_hour", 6)),
        "INDEX_LOOKBACK":             str(params.get("index_lookback", 6)),
        "SORT_LOOKBACK":              str(params.get("sort_lookback", "6")),
        "DEPLOYMENT_RUNTIME_HOURS":   str(params.get("deployment_runtime_hours", "daily")),
        "END_CROSS_MIDNIGHT":         _boolenv(params.get("end_cross_midnight", True)),
        "STARTING_CAPITAL":           str(params.get("starting_capital", 100000.0)),
        "CAPITAL_MODE":               str(params.get("capital_mode", "fixed")),
        "FIXED_NOTIONAL_CAP":         str(params.get("fixed_notional_cap", "internal")),
        "PIVOT_LEVERAGE":             str(params.get("pivot_leverage", 4.0)),
        "MIN_MCAP":                   str(params.get("min_mcap", 0.0)),
        "MAX_MCAP":                   str(params.get("max_mcap", 0.0)),
        "MIN_LISTING_AGE":            str(params.get("min_listing_age", 0)),
        "MAX_PORT":                   "" if params.get("max_port") is None else str(params.get("max_port")),
        "DROP_UNVERIFIED":            _boolenv(params.get("drop_unverified", False)),
        "LEVERAGE":                   str(params.get("leverage", 4.0)),
        "STOP_RAW_PCT":               str(params.get("stop_raw_pct", -6.0)),
        "PRICE_SOURCE":               str(params.get("price_source", "parquet")),
        "MCAP_SOURCE":                str(params.get("mcap_source", "parquet")),
        "SAVE_CHARTS":                _boolenv(params.get("save_charts", True)),
        "TRIAL_PURCHASES":            _boolenv(params.get("trial_purchases", False)),
        "QUICK":                      _boolenv(params.get("quick", False)),
        "TAKER_FEE_PCT":              str(params.get("taker_fee_pct", 0.0008)),
        "FUNDING_RATE_DAILY_PCT":     str(params.get("funding_rate_daily_pct", 0.0002)),

        # CANDIDATE_CONFIGS execution params
        "EARLY_KILL_X":               str(params.get("early_kill_x", 5)),
        "EARLY_KILL_Y":               str(params.get("early_kill_y", -999.0)),
        "EARLY_INSTILL_Y":            str(params.get("early_instill_y", -999.0)),
        "L_BASE":                     str(params.get("l_base", 0.0)),
        "L_HIGH":                     str(params.get("l_high", 1.0)),
        "PORT_TSL":                   str(params.get("port_tsl", 0.99)),
        "PORT_SL":                    str(params.get("port_sl", -0.99)),
        "EARLY_FILL_Y":               str(params.get("early_fill_y", 0.99)),
        "EARLY_FILL_X":               str(params.get("early_fill_x", 5)),

        # ── Filters ───────────────────────────────────────────────────────────
        "ENABLE_TAIL_GUARDRAIL":      _boolenv(params.get("enable_tail_guardrail", True)),
        "ENABLE_DISPERSION_FILTER":   _boolenv(params.get("enable_dispersion_filter", True)),
        "ENABLE_TAIL_PLUS_DISP":      _boolenv(params.get("enable_tail_plus_disp", True)),
        "ENABLE_VOL_FILTER":          _boolenv(params.get("enable_vol_filter", True)),
        "ENABLE_TAIL_DISP_VOL":       _boolenv(params.get("enable_tail_disp_vol", False)),
        "ENABLE_TAIL_OR_VOL":         _boolenv(params.get("enable_tail_or_vol", False)),
        "ENABLE_TAIL_AND_VOL":        _boolenv(params.get("enable_tail_and_vol", False)),
        "ENABLE_BLOFIN_FILTER":       _boolenv(params.get("enable_blofin_filter", False)),
        "ENABLE_BTC_MA_FILTER":       _boolenv(params.get("enable_btc_ma_filter", False)),
        "ENABLE_IC_DIAGNOSTIC":       _boolenv(params.get("enable_ic_diagnostic", False)),
        "ENABLE_IC_FILTER":           _boolenv(params.get("enable_ic_filter", False)),
        "RUN_FILTER_NONE":            _boolenv(params.get("run_filter_none", True)),
        "RUN_FILTER_TAIL":            _boolenv(params.get("run_filter_tail", False)),
        "RUN_FILTER_DISPERSION":      _boolenv(params.get("run_filter_dispersion", False)),
        "RUN_FILTER_TAIL_DISP":       _boolenv(params.get("run_filter_tail_disp", False)),
        "RUN_FILTER_VOL":             _boolenv(params.get("run_filter_vol", False)),
        "RUN_FILTER_TAIL_DISP_VOL":   _boolenv(params.get("run_filter_tail_disp_vol", False)),
        "RUN_FILTER_TAIL_OR_VOL":     _boolenv(params.get("run_filter_tail_or_vol", False)),
        "RUN_FILTER_TAIL_AND_VOL":    _boolenv(params.get("run_filter_tail_and_vol", False)),
        "RUN_FILTER_TAIL_BLOFIN":     _boolenv(params.get("run_filter_tail_blofin", False)),
        "RUN_FILTER_CALENDAR":        _boolenv(params.get("run_filter_calendar", False)),

        # ── Advanced — Strategy tuning ────────────────────────────────────────
        "DISPERSION_THRESHOLD":         str(params.get("dispersion_threshold", 0.66)),
        "DISPERSION_BASELINE_WIN":      str(params.get("dispersion_baseline_win", 33)),
        "DISPERSION_DYNAMIC_UNIVERSE":  _boolenv(params.get("dispersion_dynamic_universe", True)),
        "DISPERSION_N":                 str(params.get("dispersion_n", 40)),
        "VOL_LOOKBACK":                 str(params.get("vol_lookback", 10)),
        "VOL_PERCENTILE":               str(params.get("vol_percentile", 0.25)),
        "VOL_BASELINE_WIN":             str(params.get("vol_baseline_win", 90)),
        "TAIL_DROP_PCT":                str(params.get("tail_drop_pct", 0.04)),
        "TAIL_VOL_MULT":                str(params.get("tail_vol_mult", 1.4)),
        "IC_SIGNAL":                    str(params.get("ic_signal", "mom1d")),
        "IC_WINDOW":                    str(params.get("ic_window", 30)),
        "IC_THRESHOLD":                 str(params.get("ic_threshold", 0.02)),
        "BTC_MA_DAYS":                  str(params.get("btc_ma_days", 20)),
        "BLOFIN_MIN_SYMBOLS":           str(params.get("blofin_min_symbols", 1)),
        "LEADERBOARD_TOP_N":            str(params.get("leaderboard_top_n", 333)),
        "TRAIN_TEST_SPLIT":             str(params.get("train_test_split", 0.60)),
        "N_TRIALS":                     str(params.get("n_trials", 3)),

        # ── Advanced — Leverage scaling ───────────────────────────────────────
        "ENABLE_PERF_LEV_SCALING":    _boolenv(params.get("enable_perf_lev_scaling", False)),
        "PERF_LEV_WINDOW":            str(params.get("perf_lev_window", 10)),
        "PERF_LEV_SORTINO_TARGET":    str(params.get("perf_lev_sortino_target", 3.0)),
        "PERF_LEV_MAX_BOOST":         str(params.get("perf_lev_max_boost", 1.5)),
        "ENABLE_VOL_LEV_SCALING":     _boolenv(params.get("enable_vol_lev_scaling", False)),
        "VOL_LEV_WINDOW":             str(params.get("vol_lev_window", 30)),
        "VOL_LEV_TARGET_VOL":         str(params.get("vol_lev_target_vol", 0.02)),
        "VOL_LEV_MAX_BOOST":          str(params.get("vol_lev_max_boost", 2.0)),
        "VOL_LEV_DD_THRESHOLD":       str(params.get("vol_lev_dd_threshold", -0.06)),
        "LEV_QUANTIZATION_MODE":      str(params.get("lev_quantization_mode", "off")),
        "LEV_QUANTIZATION_STEP":      str(params.get("lev_quantization_step", 0.1)),
        "ENABLE_CONTRA_LEV_SCALING":  _boolenv(params.get("enable_contra_lev_scaling", False)),
        "CONTRA_LEV_WINDOW":          str(params.get("contra_lev_window", 30)),
        "CONTRA_LEV_MAX_BOOST":       str(params.get("contra_lev_max_boost", 2.0)),
        "CONTRA_LEV_DD_THRESHOLD":    str(params.get("contra_lev_dd_threshold", -0.15)),

        # ── Advanced — Risk overlays ──────────────────────────────────────────
        "ENABLE_PPH":                       _boolenv(params.get("enable_pph", False)),
        "PPH_FREQUENCY":                    str(params.get("pph_frequency", "weekly")),
        "PPH_THRESHOLD":                    str(params.get("pph_threshold", 0.20)),
        "PPH_HARVEST_FRAC":                 str(params.get("pph_harvest_frac", 0.50)),
        "PPH_SWEEP_ENABLED":                _boolenv(params.get("pph_sweep_enabled", False)),
        "ENABLE_RATCHET":                   _boolenv(params.get("enable_ratchet", False)),
        "RATCHET_FREQUENCY":                str(params.get("ratchet_frequency", "weekly")),
        "RATCHET_TRIGGER":                  str(params.get("ratchet_trigger", 0.20)),
        "RATCHET_LOCK_PCT":                 str(params.get("ratchet_lock_pct", 0.15)),
        "RATCHET_RISK_OFF_LEV_SCALE":       str(params.get("ratchet_risk_off_lev_scale", 0.0)),
        "RATCHET_SWEEP_ENABLED":            _boolenv(params.get("ratchet_sweep_enabled", False)),
        "ENABLE_ADAPTIVE_RATCHET":          _boolenv(params.get("enable_adaptive_ratchet", False)),
        "ADAPTIVE_RATCHET_FREQUENCY":       str(params.get("adaptive_ratchet_frequency", "weekly")),
        "ADAPTIVE_RATCHET_VOL_WINDOW":      str(params.get("adaptive_ratchet_vol_window", 20)),
        "ADAPTIVE_RATCHET_VOL_LOW":         str(params.get("adaptive_ratchet_vol_low", 0.03)),
        "ADAPTIVE_RATCHET_VOL_HIGH":        str(params.get("adaptive_ratchet_vol_high", 0.07)),
        "ADAPTIVE_RATCHET_RISK_OFF_SCALE":  str(params.get("adaptive_ratchet_risk_off_scale", 0.0)),
        "ADAPTIVE_RATCHET_FLOOR_DECAY":     str(params.get("adaptive_ratchet_floor_decay", 0.995)),
        "ADAPTIVE_RATCHET_SWEEP_ENABLED":   _boolenv(params.get("adaptive_ratchet_sweep_enabled", False)),

        # ── Advanced — Sweeps, cubes, robustness ─────────────────────────────
        "ENABLE_SWEEP_L_HIGH":          _boolenv(params.get("enable_sweep_l_high", False)),
        "ENABLE_SWEEP_TAIL_GUARDRAIL":  _boolenv(params.get("enable_sweep_tail_guardrail", False)),
        "ENABLE_SWEEP_TRAIL_WIDE":      _boolenv(params.get("enable_sweep_trail_wide", False)),
        "ENABLE_SWEEP_TRAIL_NARROW":    _boolenv(params.get("enable_sweep_trail_narrow", False)),
        "ENABLE_PARAM_SURFACES":        _boolenv(params.get("enable_param_surfaces", False)),
        "ENABLE_STABILITY_CUBE":        _boolenv(params.get("enable_stability_cube", False)),
        "ENABLE_RISK_THROTTLE_CUBE":    _boolenv(params.get("enable_risk_throttle_cube", False)),
        "ENABLE_EXIT_CUBE":             _boolenv(params.get("enable_exit_cube", False)),
        "ENABLE_NOISE_STABILITY":       _boolenv(params.get("enable_noise_stability", False)),
        "ENABLE_SLIPPAGE_SWEEP":        _boolenv(params.get("enable_slippage_sweep", False)),
        "ENABLE_EQUITY_ENSEMBLE":       _boolenv(params.get("enable_equity_ensemble", False)),
        "ENABLE_PARAM_JITTER":          _boolenv(params.get("enable_param_jitter", False)),
        "ENABLE_RETURN_CONCENTRATION":  _boolenv(params.get("enable_return_concentration", False)),
        "ENABLE_SHARPE_RIDGE_MAP":      _boolenv(params.get("enable_sharpe_ridge_map", False)),
        "ENABLE_SHARPE_PLATEAU":        _boolenv(params.get("enable_sharpe_plateau", False)),
        "ENABLE_TOP_N_REMOVAL":         _boolenv(params.get("enable_top_n_removal", False)),
        "ENABLE_LUCKY_STREAK":          _boolenv(params.get("enable_lucky_streak", False)),
        "ENABLE_PERIODIC_BREAKDOWN":    _boolenv(params.get("enable_periodic_breakdown", False)),
        "ENABLE_WEEKLY_MILESTONES":     _boolenv(params.get("enable_weekly_milestones", False)),
        "ENABLE_MONTHLY_MILESTONES":    _boolenv(params.get("enable_monthly_milestones", False)),
        "ENABLE_DSR_MTL":               _boolenv(params.get("enable_dsr_mtl", False)),
        "ENABLE_SHOCK_INJECTION":       _boolenv(params.get("enable_shock_injection", False)),
        "ENABLE_RUIN_PROBABILITY":      _boolenv(params.get("enable_ruin_probability", False)),
        "ENABLE_MCAP_DIAGNOSTIC":       _boolenv(params.get("enable_mcap_diagnostic", False)),
        "ENABLE_CAPACITY_CURVE":        _boolenv(params.get("enable_capacity_curve", False)),
        "ENABLE_REGIME_ROBUSTNESS":     _boolenv(params.get("enable_regime_robustness", False)),
        "ENABLE_MIN_CUM_RETURN":        _boolenv(params.get("enable_min_cum_return", False)),

        # ── Expert ────────────────────────────────────────────────────────────
        "ANNUALIZATION_FACTOR":  str(params.get("annualization_factor", 365)),
        "BAR_MINUTES":           str(params.get("bar_minutes", 5)),
        "SAVE_DAILY_FILES":      _boolenv(params.get("save_daily_files", False)),
        "BUILD_MASTER_FILE":     _boolenv(params.get("build_master_file", True)),
    }


# ---------------------------------------------------------------------------
# Pre-stage: DB-mode parquet freshness check + conditional rebuild
# ---------------------------------------------------------------------------

def prestage_parquet(
    params: dict,
    *,
    pipeline_env: dict,
    pipeline_dir: Path,
    on_rebuild_start: Optional[Callable[[], None]] = None,
) -> None:
    """Check leaderboard parquets against DB market.leaderboards; rebuild if stale.

    Only runs when params["price_source"] == "db". No-op otherwise.

    on_rebuild_start (optional) is called exactly once if a rebuild is required —
    before any parquet deletion, so the caller can surface status to the user
    (e.g. the Celery worker bumps the job status to 'running/leaderboard_refresh').
    Nightly CLI passes None.

    Raises if an indexer subprocess fails (via subprocess.run capture_output).
    """
    if params.get("price_source") != "db":
        return

    import pyarrow.parquet as _pq
    # Climb from backend/app/services/audit/pipeline_runner.py up to /app
    # so `from pipeline.db.connection import get_conn` resolves.
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
    from pipeline.db.connection import get_conn as _get_conn

    base_dir = pipeline_env.get("BASE_DATA_DIR", "/mnt/quant-data")
    indexer_script = pipeline_dir / "indexer" / "build_intraday_leaderboard.py"

    # Check if parquets are already up to date with the DB.
    # Compare both start AND end dates — a parquet that only covers
    # the last 14 days is stale even if its end date matches.
    _conn = _get_conn()
    _cur = _conn.cursor()
    _cur.execute("SELECT MIN(timestamp_utc)::date, MAX(timestamp_utc)::date FROM market.leaderboards")
    db_first, db_last = _cur.fetchone()
    _cur.close()
    _conn.close()

    parquet_stale = False
    for metric in ("price", "open_interest", "volume"):
        pq_path = Path(base_dir) / "leaderboards" / metric / f"intraday_pct_leaderboard_{metric}_top333_anchor0000_ALL.parquet"
        if not pq_path.exists():
            parquet_stale = True
            break
        try:
            pf = _pq.ParquetFile(str(pq_path))
            first_rg = pf.read_row_group(0, columns=["timestamp_utc"])
            last_rg = pf.read_row_group(pf.metadata.num_row_groups - 1, columns=["timestamp_utc"])
            pq_first = first_rg.to_pandas()["timestamp_utc"].min().date()
            pq_last = last_rg.to_pandas()["timestamp_utc"].max().date()
            if pq_last < db_last or pq_first > db_first:
                parquet_stale = True
                break
        except Exception:
            parquet_stale = True
            break

    if parquet_stale:
        if on_rebuild_start:
            on_rebuild_start()
        # Delete ALL parquets + filtered caches so the rebuild starts
        # from scratch across the full DB date range, not just the
        # delta since the last parquet end date.
        for stale in _glob.glob(f"{base_dir}/leaderboard_*_filtered_*"):
            os.remove(stale)
        db_first_str = db_first.strftime("%Y-%m-%d")
        for metric in ("price", "open_interest", "volume"):
            lb_dir = Path(base_dir) / "leaderboards" / metric
            for old_pq in _glob.glob(str(lb_dir / "intraday_pct_leaderboard_*_ALL.parquet")):
                os.remove(old_pq)
            lb_cmd = [
                _PIPELINE_PYTHON, str(indexer_script),
                "--source", "db", "--metric", metric, "--force",
                "--start", db_first_str,
            ]
            subprocess.run(lb_cmd, cwd=str(pipeline_dir), env=pipeline_env,
                           capture_output=True, timeout=14400)  # 4h — full rebuild is ~3h
    else:
        # Parquets are current — still need to clear filtered caches
        # in case the filter params changed since last run.
        for stale in _glob.glob(f"{base_dir}/leaderboard_*_filtered_*"):
            os.remove(stale)


# ---------------------------------------------------------------------------
# Audit subprocess: run overlap_analysis.py --audit, stream stdout to file
# ---------------------------------------------------------------------------

def run_audit_subprocess(
    *,
    cmd: list[str],
    output_path: Path,
    cwd: Path,
    env: dict,
    on_line: Optional[Callable[[bytes], None]] = None,
    cancelled: Optional[Callable[[], bool]] = None,
) -> None:
    """Run `cmd` as a subprocess, streaming merged stdout+stderr to output_path.

    on_line — invoked with each raw bytes line after it's written to disk.
               Use it to update progress, parse incremental state, etc.
    cancelled — polled before each line; when True, subprocess is terminated
                and JobCancelled is raised.

    Raises RuntimeError if the subprocess exits non-zero. Raises JobCancelled
    if cancellation was requested.
    """
    with output_path.open("wb") as out_fh:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(cwd),
            env=env,
        )
        for line in proc.stdout:  # type: ignore[union-attr]
            if cancelled and cancelled():
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()
                raise JobCancelled("Cancelled by user.")
            out_fh.write(line)
            out_fh.flush()
            if on_line:
                on_line(line)
        proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(
            f"overlap_analysis.py exited with code {proc.returncode}. "
            f"See audit_output.txt for details."
        )


# ---------------------------------------------------------------------------
# Full audit: prestage + subprocess + scrape → metrics dict
# ---------------------------------------------------------------------------

def run_audit(
    params: dict,
    *,
    output_path: Path,
    progress_cb: Optional[Callable[[bytes], None]] = None,
    cancellation_cb: Optional[Callable[[], bool]] = None,
    on_rebuild_start: Optional[Callable[[], None]] = None,
) -> dict:
    """Run the full audit path: prestage parquets + overlap_analysis.py subprocess + scrape.

    One-stop entry point for both the Simulator Celery task and the nightly
    refresh CLI. Preserves audit_output.txt on disk at the caller-owned
    output_path (audit trail / debugging).

    Returns the same metrics dict shape as the pre-refactor _parse_metrics.

    Callbacks (all optional):
      progress_cb       — called with each raw stdout bytes line from subprocess
      cancellation_cb   — polled before each line; True → terminate + JobCancelled
      on_rebuild_start  — invoked once if prestage determines parquets are stale
                          and begins the ~3h rebuild

    Caller owns output_path's parent directory.
    """
    pipeline_env = build_pipeline_env(params)
    pipeline_dir = Path(settings.PIPELINE_DIR)
    overlap_script = pipeline_dir / "overlap_analysis.py"
    cmd = [_PIPELINE_PYTHON, str(overlap_script)] + build_cli_args(params)

    prestage_parquet(
        params,
        pipeline_env=pipeline_env,
        pipeline_dir=pipeline_dir,
        on_rebuild_start=on_rebuild_start,
    )

    run_audit_subprocess(
        cmd=cmd,
        output_path=output_path,
        cwd=pipeline_dir,
        env=pipeline_env,
        on_line=progress_cb,
        cancelled=cancellation_cb,
    )

    return parse_metrics(output_path)

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
        # D-medium-split (2026-04-23): individual ranking-metric knobs + BloFin
        # universe filter. Replaces the deprecated single `ranking_metric`
        # flag which is kept below for backward-compat plumbing.
        "price_ranking_metric":     "--price-ranking-metric",
        "oi_ranking_metric":        "--oi-ranking-metric",
        # Keep `ranking_metric` in the map for backward-compat: if a caller
        # passes it, overlap_analysis.py maps it onto both new flags and emits
        # a deprecation warning. New callers should use the two split keys.
        "ranking_metric":           "--ranking-metric",
        # Stream D-explore (2026-04-23): overlap-dimensions knob for
        # candidate-exploration variants (price_oi canonical / price_volume /
        # oi_volume / price_oi_volume).
        "overlap_dimensions":       "--overlap-dimensions",
    }
    bool_flags = {
        "end_cross_midnight":    "--end-cross-midnight",
        "drop_unverified":       "--drop-unverified",
        "quick":                 "--quick",
        "apply_blofin_filter":   "--apply-blofin-filter",
        # Trigger the canonical pct_change SQL ranking path WITHOUT
        # restricting the universe to BloFin. Set by the BloFin twin-run
        # wrapper so the vanilla baseline uses the same ranking method
        # as the BloFin-restricted pass.
        "force_canonical":       "--force-canonical",
    }

    audit_source = params.get("price_source", "db")
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
        "PRICE_SOURCE":               str(params.get("price_source", "db")),
        "MCAP_SOURCE":                str(params.get("mcap_source", "db")),
        "DISPERSION_UNIVERSE_MODE":   str(params.get("dispersion_universe_mode", "all")),
        # Mid-session splice — when True, audit.py appends today's
        # partial intraday column fetched live from Binance klines
        # (read from live_deploys_signal.csv for today's basket). See
        # _splice_today_partial_into_matrix in audit.py:2575. Used by
        # the simulator's "Live Today" toggle.
        "LIVE_TODAY":                 _boolenv(params.get("live_today", False)),
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
        "DD_STOP_X":                  str(params.get("dd_stop_x", 9999)),
        "DD_STOP_Y":                  str(params.get("dd_stop_y", -0.99)),

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
        "DISPERSION_UNIVERSE_LAG_DAYS": str(params.get("dispersion_universe_lag_days", 1)),
        "DISPERSION_UNIVERSE_STRICT_DYNAMIC": _boolenv(params.get("dispersion_universe_strict_dynamic", False)),
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
        # Pre-mode universe restriction at the rebuild step (listTime-aware,
        # time-correct). Set by the worker for BloFin-variant audit runs.
        # Composes with overlap_analysis.py's apply_blofin_filter (live
        # universe at SQL ranking time): apply_blofin filters at ranking,
        # this env triggers an additional listTime check at eligibility.
        "BLOFIN_UNIVERSE_ENABLED":      _boolenv(params.get("blofin_universe_enabled", False)),
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

    # Two failure modes worth distinguishing:
    #   - parquet_missing: file genuinely absent (e.g. fresh deploy / wiped
    #     state / corrupt). Triggers a full rebuild — without the file,
    #     downstream code paths that try to read it will crash hard.
    #   - parquet_stale: file exists but doesn't span [db_first, db_last]
    #     (e.g. an emergency 1-day topup left a thin parquet). For
    #     price_source="db" audits, overlap_analysis reads from
    #     market.leaderboards directly and doesn't open the parquet, so
    #     a stale parquet is acceptable. Log a warning instead of doing
    #     a 3-hour rebuild that would race against any concurrent manual
    #     rebuild and block the audit user for hours.
    parquet_missing = False
    stale_metrics: list[str] = []
    for metric in ("price", "open_interest", "volume"):
        pq_path = Path(base_dir) / "leaderboards" / metric / f"intraday_pct_leaderboard_{metric}_top333_anchor0000_ALL.parquet"
        if not pq_path.exists():
            parquet_missing = True
            break
        try:
            pf = _pq.ParquetFile(str(pq_path))
            first_rg = pf.read_row_group(0, columns=["timestamp_utc"])
            last_rg = pf.read_row_group(pf.metadata.num_row_groups - 1, columns=["timestamp_utc"])
            pq_first = first_rg.to_pandas()["timestamp_utc"].min().date()
            pq_last = last_rg.to_pandas()["timestamp_utc"].max().date()
            if pq_last < db_last or pq_first > db_first:
                stale_metrics.append(
                    f"{metric}: pq=[{pq_first}, {pq_last}] vs db=[{db_first}, {db_last}]"
                )
        except Exception as _e:
            # Corrupt parquet — treat as missing so a rebuild repairs it.
            print(f"  ⚠ {metric} parquet read failed ({_e}); treating as missing")
            parquet_missing = True
            break

    if parquet_missing:
        if on_rebuild_start:
            on_rebuild_start()
        # Delete ALL parquets + filtered caches so the rebuild starts
        # from scratch across the full DB date range.
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
        if stale_metrics:
            print(
                f"  ⚠ leaderboard parquets are stale (db source path doesn't "
                f"read them, so audit will proceed): " + "; ".join(stale_metrics)
            )
        # Clear filtered caches in case the filter params changed since
        # last run — applies whether or not the master parquets are stale.
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

def _ensure_today_in_db(
    *,
    pipeline_env: dict,
    pipeline_dir: Path,
    overlap_dimensions: str = "price_oi",
    progress_cb: Optional[Callable[[bytes], None]] = None,
) -> dict:
    """Mid-session ingest pre-flight for live_today=True.

    Walks the data dependency chain and triggers any missing pieces so
    today's data is available for overlap_analysis + audit:

       metl ingest  (today's 1m bars → market.futures_1m)
            ↓
       indexer      (today's 06:00 anchor → market.leaderboards × needed metrics)
            ↓
       overlap_analysis (today's basket → deploys_overlap CSV)

    Skips any step whose output already exists in the DB — re-running
    after the nightly cron has caught up is a no-op.

    Speedups vs naive sequential x3:
      • Only runs the indexer metrics that overlap_dimensions actually
        consumes (canonical "price_oi" needs price + open_interest;
        volume skipped — saves 5-15 min)
      • Runs the remaining indexer metrics CONCURRENTLY via thread pool.
        Each subprocess opens its own DB connection; they read from
        market.futures_1m (concurrent reads OK) and write to disjoint
        (metric, …) keys in market.leaderboards (no row contention).

    Total wall-clock now bounded by max(metl, max-indexer-metric)
    rather than metl + sum(indexers). Typical mid-session run on a
    cold DB: ~10-20 min vs the naive ~30-50 min.

    Returns a dict with status of each step for the caller to log /
    surface in the job UI:
        {"metl": "ran"|"skipped"|"failed", "indexer": {metric: ...}, "errors": [...]}
    """
    import datetime as _dt
    sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
    from pipeline.db.connection import get_conn as _get_conn

    today = _dt.datetime.now(_dt.timezone.utc).date()
    today_str = today.isoformat()
    status: dict = {"metl": None, "indexer": None, "errors": []}

    def _emit(msg: str) -> None:
        line = f"[live-today preflight] {msg}\n"
        print(line, end="", flush=True)
        if progress_cb:
            try:
                progress_cb(line.encode())
            except Exception:
                pass

    _emit(f"checking data availability for {today_str}…")

    # ── 1. metl ingest ────────────────────────────────────────────
    # Check if today's 1m bars are in market.futures_1m. If not,
    # invoke metl.py to pull from Amberdata. Idempotent on re-run
    # (existing rows skipped via ON CONFLICT in metl's writer).
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM market.futures_1m "
            "WHERE timestamp_utc::date = %s::date",
            (today_str,),
        )
        rows_today = int(cur.fetchone()[0])
        cur.close()
        conn.close()
    except Exception as e:
        _emit(f"DB check for futures_1m failed: {e}; assuming missing")
        rows_today = 0

    if rows_today == 0:
        _emit(f"futures_1m has 0 rows for {today_str}; running metl…")
        metl_script = pipeline_dir / "compiler" / "metl.py"
        metl_cmd = [
            _PIPELINE_PYTHON, str(metl_script),
            "--start", today_str,
            "--end", today_str,
            "--triggered-by", "cli",
            "--run-tag", "live_today_preflight",
        ]
        try:
            subprocess.run(
                metl_cmd, cwd=str(pipeline_dir), env=pipeline_env,
                capture_output=True, timeout=1200,  # 20 min
            )
            status["metl"] = "ran"
            _emit("metl complete")
        except subprocess.TimeoutExpired:
            status["metl"] = "timeout"
            status["errors"].append("metl exceeded 20-min timeout")
            _emit("⚠ metl timed out; proceeding (overlap may still pick up partial data)")
        except Exception as e:
            status["metl"] = "failed"
            status["errors"].append(f"metl: {e}")
            _emit(f"⚠ metl failed: {e}")
    else:
        status["metl"] = "skipped"
        _emit(f"futures_1m already has {rows_today} rows for {today_str}; skipping metl")

    # ── 2. indexer ────────────────────────────────────────────────
    # Check if today's leaderboards rows exist. If not, run the
    # indexer for each of the three metrics. The indexer's
    # date_already_in_db() guard makes re-runs idempotent.
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM market.leaderboards "
            "WHERE timestamp_utc::date = %s::date",
            (today_str,),
        )
        lb_rows = int(cur.fetchone()[0])
        cur.close()
        conn.close()
    except Exception as e:
        _emit(f"DB check for leaderboards failed: {e}; assuming missing")
        lb_rows = 0

    if lb_rows == 0:
        # Only run the indexer metrics that overlap_dimensions actually
        # consumes. Canonical "price_oi" → {price, open_interest}; volume
        # skipped (saves 5-15 min). overlap_analysis enforces this in
        # its OVERLAP_DIMENSIONS_CHOICES tuple.
        dim_str = (overlap_dimensions or "price_oi").lower()
        needed_metrics: list[str] = []
        if "price" in dim_str:
            needed_metrics.append("price")
        if "oi" in dim_str:
            needed_metrics.append("open_interest")
        if "volume" in dim_str:
            needed_metrics.append("volume")
        if not needed_metrics:
            needed_metrics = ["price", "open_interest"]  # safe canonical
        _emit(
            f"leaderboards has 0 rows for {today_str}; running indexer × "
            f"{len(needed_metrics)} metric(s) ({', '.join(needed_metrics)}) "
            f"in parallel…"
        )
        indexer_script = pipeline_dir / "indexer" / "build_intraday_leaderboard.py"

        def _run_one_metric(metric: str) -> tuple[str, str]:
            """Run indexer for one metric. Returns (metric, status)."""
            ix_cmd = [
                _PIPELINE_PYTHON, str(indexer_script),
                "--metric", metric,
                "--source", "db",
                "--start", today_str,
                "--end", today_str,
                "--triggered-by", "cli",
                "--run-tag", "live_today_preflight",
            ]
            try:
                subprocess.run(
                    ix_cmd, cwd=str(pipeline_dir), env=pipeline_env,
                    capture_output=True, timeout=1800,
                )
                return (metric, "ok")
            except subprocess.TimeoutExpired:
                return (metric, "timeout")
            except Exception as e:
                return (metric, f"failed:{e}")

        # Concurrent execution — independent metric writes to
        # market.leaderboards on disjoint (metric, …) keys; reads from
        # market.futures_1m are pure SELECTs. No row contention; DB
        # handles concurrent connections without issue. Total wall-
        # clock = max(per-metric duration) instead of sum.
        import concurrent.futures as _cf
        indexer_results: dict[str, str] = {}
        with _cf.ThreadPoolExecutor(max_workers=len(needed_metrics)) as ex:
            futures = {ex.submit(_run_one_metric, m): m for m in needed_metrics}
            for fut in _cf.as_completed(futures):
                metric, result = fut.result()
                indexer_results[metric] = result
                _emit(f"  indexer metric={metric}: {result}")
                if result not in ("ok",):
                    status["errors"].append(f"indexer metric={metric}: {result}")
        status["indexer"] = indexer_results
        _emit(f"indexer complete (parallel): {indexer_results}")
    else:
        status["indexer"] = "skipped"
        _emit(f"leaderboards already has {lb_rows} rows for {today_str}; skipping indexer")

    return status


def run_audit_with_blofin_variants(
    params: dict,
    *,
    output_dir: Path,
    progress_cb: Optional[Callable[[bytes], None]] = None,
    cancellation_cb: Optional[Callable[[], bool]] = None,
    on_rebuild_start: Optional[Callable[[], None]] = None,
) -> dict:
    """Wrapper around `run_audit` that orchestrates BloFin variant runs.

    Branches on `params["blofin_variants"]`:

      "off" (default)
        Single vanilla audit run. Returns the same metrics dict shape as
        `run_audit` directly.

      "blofin_only"
        Single audit run with `apply_blofin_filter=True` (overlap_analysis
        ranks within BloFin universe) AND `blofin_universe_enabled=True`
        (rebuild adds time-correct listTime check). Filter labels are
        emitted unchanged ("A - Tail Guardrail", etc.) — there is no
        vanilla baseline to disambiguate against.

      "both"
        Two sequential audit runs. Vanilla first, then BloFin. Output
        artifacts go to separate audit_output_*.txt files in `output_dir`.
        Metrics are merged: vanilla rows tagged `blofin_variant=False`,
        BloFin rows tagged `blofin_variant=True` AND label suffixed with
        " (BloFin)" so the frontend can render 10-row pairs.

    `output_dir` is the per-job directory (settings.JOBS_DIR / job_id);
    we own the audit_output*.txt files inside it.
    """
    mode = (params.get("blofin_variants") or "off").lower()

    if mode in ("", "off"):
        return run_audit(
            params,
            output_path=output_dir / "audit_output.txt",
            progress_cb=progress_cb,
            cancellation_cb=cancellation_cb,
            on_rebuild_start=on_rebuild_start,
        )

    if mode == "blofin_only":
        blofin_params = {
            **params,
            "apply_blofin_filter":      True,
            "blofin_universe_enabled":  True,
        }
        return run_audit(
            blofin_params,
            output_path=output_dir / "audit_output.txt",
            progress_cb=progress_cb,
            cancellation_cb=cancellation_cb,
            on_rebuild_start=on_rebuild_start,
        )

    if mode == "both":
        # Both passes use the canonical pct_change SQL ranking path so
        # the only difference between vanilla and BloFin is the universe
        # — apples-to-apples comparison. Without this, the vanilla pass
        # would use market.leaderboards (fast path) while the BloFin
        # pass would use the canonical SQL (forced by apply_blofin_filter),
        # confounding the comparison with a ranking-method difference.
        # See spec § 3.1 / overlap_analysis.py:_non_canonical_ranking.
        vanilla_params = {
            **params,
            "apply_blofin_filter":      False,
            "blofin_universe_enabled":  False,
            "force_canonical":          True,
        }
        vanilla_metrics = run_audit(
            vanilla_params,
            output_path=output_dir / "audit_output_vanilla.txt",
            progress_cb=progress_cb,
            cancellation_cb=cancellation_cb,
            on_rebuild_start=on_rebuild_start,
        )

        # Bail early on cancellation between runs — don't waste a second run
        if cancellation_cb is not None and cancellation_cb():
            raise JobCancelled("Cancelled between vanilla and BloFin variants")

        # Second: BloFin pass
        blofin_params = {
            **params,
            "apply_blofin_filter":      True,
            "blofin_universe_enabled":  True,
        }
        blofin_metrics = run_audit(
            blofin_params,
            output_path=output_dir / "audit_output_blofin.txt",
            progress_cb=progress_cb,
            cancellation_cb=cancellation_cb,
            on_rebuild_start=on_rebuild_start,
        )

        # Concatenate the two stdout files into a single audit_output.txt
        # so the frontend's "Raw Output" tab shows both passes in one view,
        # with a clear separator between them.
        combined = output_dir / "audit_output.txt"
        try:
            with combined.open("w") as out_f:
                out_f.write("══════════════════════════════════════════════════════════════════\n")
                out_f.write("  VANILLA PASS (no BloFin universe restriction)\n")
                out_f.write("══════════════════════════════════════════════════════════════════\n")
                v_path = output_dir / "audit_output_vanilla.txt"
                if v_path.exists():
                    out_f.write(v_path.read_text())
                out_f.write("\n\n══════════════════════════════════════════════════════════════════\n")
                out_f.write("  BLOFIN PASS (universe restricted to BloFin SWAP listings, time-correct)\n")
                out_f.write("══════════════════════════════════════════════════════════════════\n")
                b_path = output_dir / "audit_output_blofin.txt"
                if b_path.exists():
                    out_f.write(b_path.read_text())
        except OSError:
            # Best-effort — Raw Output tab will still find the per-pass files
            pass

        return merge_audit_metrics(vanilla_metrics, blofin_metrics)

    raise ValueError(
        f"Unknown blofin_variants mode: {mode!r}. "
        f"Expected one of: off, blofin_only, both."
    )


def merge_audit_metrics(vanilla: dict, blofin: dict) -> dict:
    """Merge two audit metrics dicts (vanilla + BloFin) into one.

    Vanilla rows are taken as the base (top-level fields like best_filter,
    sharpe, scorecard come from there). BloFin rows are appended to the
    `filters` and `filter_comparison` arrays with:
      * label suffixed " (BloFin)"
      * `blofin_variant: True`

    Vanilla rows get `blofin_variant: False` for symmetry.

    `fees_tables_by_filter` is also merged with BloFin keys suffixed.
    """
    import copy
    merged = copy.deepcopy(vanilla)

    for arr_key in ("filters", "filter_comparison"):
        for row in merged.get(arr_key, []) or []:
            if isinstance(row, dict):
                row["blofin_variant"] = False
        for row in (blofin.get(arr_key) or []):
            if not isinstance(row, dict):
                continue
            new_row = dict(row)
            label = str(new_row.get("filter") or "")
            new_row["filter"] = f"{label} (BloFin)" if label else "(BloFin)"
            new_row["blofin_variant"] = True
            merged.setdefault(arr_key, []).append(new_row)

    blofin_fees = blofin.get("fees_tables_by_filter") or {}
    if blofin_fees:
        merged.setdefault("fees_tables_by_filter", {})
        for label, fees_table in blofin_fees.items():
            merged["fees_tables_by_filter"][f"{label} (BloFin)"] = fees_table

    return merged


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

    # Mid-session pre-flight: if live_today is set, ensure today's
    # data is in the DB before overlap_analysis runs against it.
    # Triggers metl + indexer subprocesses as needed; idempotent on
    # re-run (skips steps whose output is already in DB).
    if params.get("live_today"):
        _ensure_today_in_db(
            pipeline_env=pipeline_env,
            pipeline_dir=pipeline_dir,
            overlap_dimensions=params.get("overlap_dimensions") or "price_oi",
            progress_cb=progress_cb,
        )

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

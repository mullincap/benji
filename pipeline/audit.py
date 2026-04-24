# ── BEFORE-COMPARISON BRANCH ─────────────────────────────────────────────────
# This file has been reverted to pre-optimisation settings for the purpose of
# reproducing the baseline audit results. Do NOT merge to main.
# Original 25 settings recorded in: before_comparison_settings.md
# ─────────────────────────────────────────────────────────────────────────────

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_test.py
=============
CANONICAL BASELINE — locked 2026-03-06
=======================================
Config:  Balanced-Opt — Tail + Disp + Vol
Filter:  tail | (disp & vol)   [hybrid AND/OR — tail always exits,
         normal days sit out only when BOTH dispersion weak AND vol compressed]

Key parameters:
  DISPERSION_THRESHOLD = 0.70   (70th percentile, sit out moderate-disp days)
  VOL_LOOKBACK         = 10d
  VOL_PERCENTILE       = 0.25   (25th percentile rolling vol baseline)
  VOL_BASELINE_WIN     = 90d

Canonical results (run 2026-03-06):
  Sharpe=3.083  MaxDD=-28.12%  CV=0.828  FA-OOS Sharpe=2.915
  Unstable folds=1  Active=337d  Flat=27d  Grade=B−

Runs full institutional audit across seven filter modes for comparison:
  1. No filter
  2. Tail Guardrail
  3. Dispersion only
  4. Tail + Dispersion
  5. Tail + Disp + Vol  ← CANONICAL
  6. Tail + Blofin      ← NEW: Tail Guardrail + Blofin exchange availability gate

Next step: IC filter (information coefficient — directly measures signal
           predictive power decay rather than proxying via regime conditions).

Usage:
    python audit_test.py

Upstream Usage:
    python3 overlap_analysis.py --audit 2>&1 | tee audit_output.txt

Requirements: credentials.json + token.json in working directory,
              plus VPN active for Binance funding rate fetch.
              pip install hmmlearn scikit-learn
"""

import os, time, datetime, math, random, requests, argparse, subprocess, logging
import warnings
from pathlib import Path
from datetime import datetime as dt
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
import csv
import itertools
import copy
import shutil
import glob
import io
import sys
from copy import deepcopy
from contextlib import contextmanager
import gspread
from gspread.exceptions import APIError
from sklearn.linear_model import LinearRegression
from scipy.stats import spearmanr
from scipy import stats as scipy_stats

# ── Optional heavy dependencies ───────────────────────────────────────
try:

    from sklearn.cluster import KMeans
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import f1_score, classification_report
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from hmmlearn.hmm import GaussianHMM
    HMM_AVAILABLE = True
except ImportError:
    GaussianHMM = None  # type: ignore[assignment,misc]
    HMM_AVAILABLE = False

try:
    import seaborn as sns
    HAS_SNS = True
except ImportError:
    sns = None  # type: ignore[assignment]
    HAS_SNS = False

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    yf = None  # type: ignore[assignment]
    YFINANCE_AVAILABLE = False

from institutional_audit import (
    run_institutional_audit,
    institutional_scorecard,
    _apply_hybrid_day_param,
    _in_earnings_window,
    # Internal helpers used by run_daily_series_audit
    _equity   as _ia_equity,
    _sharpe   as _ia_sharpe,
    _max_dd   as _ia_max_dd,
    _dd_curve as _ia_dd_curve,
)

from enum import Enum

# Suppress known non-fatal matplotlib warnings that pollute audit_output alerts.
# These do not affect computed metrics/results; they only concern figure layout/legend rendering.
warnings.filterwarnings(
    "ignore",
    message="This figure includes Axes that are not compatible with tight_layout, so results might be incorrect.",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    message="No artists with labels found to put in legend.*",
    category=UserWarning,
)


# ══════════════════════════════════════════════════════════════════════
# MATHEMATICAL & ALGORITHMIC CONSTANTS
# ══════════════════════════════════════════════════════════════════════

ANNUALIZATION_FACTOR   = int(os.environ.get("ANNUALIZATION_FACTOR", "365"))    # trading days per year (vol/Sharpe annualization)
RSI_PERIOD             = 14     # standard RSI window (matching feats["rsi_14"])
TRAIN_TEST_SPLIT_RATIO = float(os.environ.get("TRAIN_TEST_SPLIT", "0.60"))   # fraction of valid days used for ML filter training

# ── Filter mode identifiers ───────────────────────────────────────────
# Using str mixin so existing  filter_mode == "tail"  comparisons work
# unchanged whether filter_mode is a plain string or a FilterMode member.

class FilterMode(str, Enum):
    NONE         = "none"
    CALENDAR     = "calendar"
    TAIL         = "tail"
    DISPERSION   = "dispersion"
    TAIL_DISP    = "tail_disp"
    TAIL_DISP_VOL= "tail_disp_vol"
    TAIL_OR_VOL  = "tail_or_vol"
    TAIL_AND_VOL = "tail_and_vol"
    BTC_MA       = "btc_ma"
    BTC_MA_TAIL  = "btc_ma_tail"
    TAIL_BLOFIN  = "tail_blofin"
    V3           = "v3"
    V4           = "v4"
    V5           = "v5"
    V5A          = "v5a"
    V5B          = "v5b"
    V5B1         = "v5b1"
    V5B2         = "v5b2"
    V5B3         = "v5b3"
    V5B4         = "v5b4"
    V5C          = "v5c"
    V5D          = "v5d"
    V5D1         = "v5d1"
    V5D2         = "v5d2"
    V5D3         = "v5d3"
    V5D4         = "v5d4"

# ══════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS CONFIG
# ══════════════════════════════════════════════════════════════════════

SPREADSHEET_ID   = "1gciwiGDlCJoUxRKgPtDXlInV05c3wmH49MJmee8IX0k"
MATRIX_TAB       = "PORTFOLIO_ROI_LEV_MATRIX"
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE       = "token.json"
# ── Local matrix override ──────────────────────────────────────────────────────
# Set to a CSV path to skip Google Sheets and load a local matrix instead.
# Format: first col = timestamp_utc, remaining cols = Bt_YYYYMMDD_060000,
# values as "X.XX%" strings — matches portfolio_matrix_gated.csv exactly.
# Set to "" (empty string) to use Google Sheets as normal.
LOCAL_MATRIX_CSV = ""   # e.g. "portfolio_matrix_gated.csv"

# ── Deployment window config ──────────────────────────────────────────────────
# These three variables define the 24-hour cycle structure.
# deployment_start_time : hour (UTC) at which the deployment period begins.
#   Default 6 = 06:00 UTC. Grid search varies this across 0–23.
# deployment_lookback   : electoral period length in hours (sit out, measure).
# deployment_runtime    : deployment period length in hours (invested).
# N_ROWS is derived so the matrix loader always reads exactly one runtime window.
DEPLOYMENT_START_HOUR    = 6    # 06:00 UTC — independent variable for grid search
SORT_LOOKBACK = 6  # electoral window length: int hours, or "daily"
                             # "daily" scans from 00:05 to deployment_start_hour
                             # (resolves to deployment_start_hour hours)
DEPLOYMENT_RUNTIME_HOURS  = "daily"  # deployment period: int hours, or "daily"
                                      # "daily" => runtime = 24 - sort_lookback

def _resolve_runtime_hours() -> int:
    """Return the actual integer runtime given current DEPLOYMENT_RUNTIME_HOURS,
    SORT_LOOKBACK, and DEPLOYMENT_START_HOUR.

    DEPLOYMENT_RUNTIME_HOURS can be:
      - An integer: used directly
      - "daily": runtime = 24 - effective_sort_lookback_hours
          where effective_sort_lookback_hours is:
            SORT_LOOKBACK (int)            if sort_lookback is a number
            DEPLOYMENT_START_HOUR          if sort_lookback is "daily"
              (daily mode scans 00:05 -> start_hour, so start_hour IS the lookback length)

    Example: start_hour=6, sort_lookback="daily" -> lookback=6h -> runtime=18h
    Example: start_hour=0, sort_lookback="daily" -> lookback=0h -> runtime=24h (full day)
    """
    if DEPLOYMENT_RUNTIME_HOURS != "daily":
        return int(DEPLOYMENT_RUNTIME_HOURS)
    effective_lookback = DEPLOYMENT_START_HOUR \
        if SORT_LOOKBACK == "daily" else int(SORT_LOOKBACK)
    return 24 - effective_lookback

BAR_MINUTES    = int(os.environ.get("BAR_MINUTES", "5"))
N_ROWS         = int(_resolve_runtime_hours() * 60 // BAR_MINUTES)  # derived from runtime
START_ROW      = 2
START_COL      = 2
PIVOT_LEVERAGE = float(os.environ.get("PIVOT_LEVERAGE", "4.0"))

# ── Transaction cost & funding rate adjustments ───────────────────────────────
# Applied inside simulate() to each active (non-flat) day.
#
# TAKER_FEE_PCT: Binance futures taker fee = 0.04% per side × 2 sides = 0.08%
#   Applied as: fee_cost = TAKER_FEE_PCT × n_symbols (each symbol costs 2 fills)
#   At PIVOT_LEVERAGE=4x the leveraged cost is fee_cost × PIVOT_LEVERAGE but
#   since df_4x returns are already 4x and we deduct in 1x space after dividing,
#   we deduct fee_cost directly from the 1x return.
#
# FUNDING_RATE_DAILY_PCT: Average daily funding cost across 3 Binance funding
#   windows (00:00, 08:00, 16:00 UTC). BTC long avg ≈ 0.01% per 8h = 0.03%/day.
#   We hold 06:00–23:55 UTC so we cross 2 funding windows (08:00 and 16:00).
#   Use 0.02% as the base per-symbol daily funding drag.
#   The actual rate is fetched live from Binance for the V3 filter; here we use
#   a conservative flat estimate. Set to 0.0 to disable.
TAKER_FEE_PCT         = float(os.environ.get("TAKER_FEE_PCT", "0.0008"))   # 0.04% per side × 2 = 0.08% round-trip per symbol
FUNDING_RATE_DAILY_PCT = float(os.environ.get("FUNDING_RATE_DAILY_PCT", "0.0002"))   # ~0.02% per symbol per day (2 windows at ~0.01% each)

# ██████████████████████████████████████████████████████████████████████
# ██                                                                  ██
# ██                  MASTER FEATURE TOGGLE PANEL                     ██
# ██                                                                  ██
# ██  All on/off switches are collected here in one place.            ██
# ██  Parameter values (thresholds, windows, grid values) remain      ██
# ██  in their respective config sections below.                      ██
# ██                                                                  ██
# ██████████████████████████████████████████████████████████████████████

# ┌─────────────────────────────────────────────────────────────────────┐
# │  GENERAL RUN BEHAVIOUR                                              │
# └─────────────────────────────────────────────────────────────────────┘
def _env_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, "1" if default else "0") == "1"


QUICK_MODE       = _env_bool("QUICK", False)   # skip all heavy audits, charts, sweeps — simulate only
SAVE_CHARTS      = _env_bool("SAVE_CHARTS", True)    # save all chart PNGs to the run output directory
PRINT_FEES_PANEL = True    # print per-day fee breakdown table to terminal
TRIAL_PURCHASES  = _env_bool("TRIAL_PURCHASES", False)   # legacy testing flag

# ┌─────────────────────────────────────────────────────────────────────┐
# │  FILTER MODES — what gets BUILT                                     │
# │  (building a filter does not add it to the comparison run;          │
# │   use the RUN_FILTER_* switches below for that)                     │
# └─────────────────────────────────────────────────────────────────────┘
ENABLE_TAIL_GUARDRAIL    = _env_bool("ENABLE_TAIL_GUARDRAIL", True)    # tail guardrail (BTC crash + vol spike)
ENABLE_DISPERSION_FILTER = _env_bool("ENABLE_DISPERSION_FILTER", True)   # cross-sectional dispersion gate
ENABLE_TAIL_PLUS_DISP    = _env_bool("ENABLE_TAIL_PLUS_DISP", True)   # Tail OR Dispersion combo
ENABLE_VOL_FILTER        = _env_bool("ENABLE_VOL_FILTER", True)   # realised volatility compression gate
ENABLE_TAIL_OR_VOL       = _env_bool("ENABLE_TAIL_OR_VOL", False)   # Tail OR Vol: sit flat when either fires
ENABLE_TAIL_AND_VOL      = _env_bool("ENABLE_TAIL_AND_VOL", False)   # Tail AND Vol: sit flat only when both fire simultaneously
ENABLE_TAIL_DISP_VOL     = _env_bool("ENABLE_TAIL_DISP_VOL", False)   # Tail + Disp + Vol triple combo
ENABLE_IC_DIAGNOSTIC     = _env_bool("ENABLE_IC_DIAGNOSTIC", False)   # IC decay diagnostics per fold (always prints)
ENABLE_IC_FILTER         = _env_bool("ENABLE_IC_FILTER", False)   # IC-gated filter mode
ENABLE_BTC_MA_FILTER     = _env_bool("ENABLE_BTC_MA_FILTER", False)   # BTC moving-average trend filter
ENABLE_BLOFIN_FILTER     = _env_bool("ENABLE_BLOFIN_FILTER", False)    # Tail + Blofin exchange availability filter

# ┌─────────────────────────────────────────────────────────────────────┐
# │  FILTER MODES — what gets added to the COMPARISON RUN              │
# └─────────────────────────────────────────────────────────────────────┘
RUN_FILTER_NONE          = _env_bool("RUN_FILTER_NONE", True)    # 3 - baseline: no regime filter applied
RUN_FILTER_CALENDAR      = _env_bool("RUN_FILTER_CALENDAR", False)   #     calendar windows (manually defined bad periods)
RUN_FILTER_TAIL          = _env_bool("RUN_FILTER_TAIL", False)    # 2 - tail guardrail
RUN_FILTER_DISPERSION    = _env_bool("RUN_FILTER_DISPERSION", False)   #      dispersion gate (requires ENABLE_DISPERSION_FILTER)
RUN_FILTER_TAIL_DISP     = _env_bool("RUN_FILTER_TAIL_DISP", False)    # 1 -  Tail + Dispersion (requires ENABLE_TAIL_PLUS_DISP)
RUN_FILTER_VOL           = _env_bool("RUN_FILTER_VOL", False)   #      standalone vol gate (requires ENABLE_VOL_FILTER)
RUN_FILTER_TAIL_OR_VOL   = _env_bool("RUN_FILTER_TAIL_OR_VOL", False)   #      Tail OR Vol  (requires ENABLE_TAIL_OR_VOL)
RUN_FILTER_TAIL_AND_VOL  = _env_bool("RUN_FILTER_TAIL_AND_VOL", False)   #      Tail AND Vol (requires ENABLE_TAIL_AND_VOL)
RUN_FILTER_TAIL_DISP_VOL = _env_bool("RUN_FILTER_TAIL_DISP_VOL", False)   #      Tail + Disp + Vol (requires ENABLE_TAIL_DISP_VOL)
RUN_FILTER_TAIL_BLOFIN   = _env_bool("RUN_FILTER_TAIL_BLOFIN", False)   #      Tail + Blofin (requires ENABLE_BLOFIN_FILTER)

# ┌─────────────────────────────────────────────────────────────────────┐
# │  LEVERAGE SCALING MODELS                                            │
# └─────────────────────────────────────────────────────────────────────┘
ENABLE_PERF_LEV_SCALING   = os.environ.get("ENABLE_PERF_LEV_SCALING", "0") == "1"  # performance-based (contrarian Sortino) boost
ENABLE_VOL_LEV_SCALING    = os.environ.get("ENABLE_VOL_LEV_SCALING", "0") == "1"   # volatility-targeting leverage model
ENABLE_CONTRA_LEV_SCALING = os.environ.get("ENABLE_CONTRA_LEV_SCALING", "0") == "1"  # percentile-rank contrarian leverage

# ┌─────────────────────────────────────────────────────────────────────┐
# │  RISK MANAGEMENT OVERLAYS                                           │
# └─────────────────────────────────────────────────────────────────────┘
ENABLE_PPH               = os.environ.get("ENABLE_PPH", "0") == "1"   # Periodic Profit Harvest
PPH_SWEEP_ENABLED        = os.environ.get("PPH_SWEEP_ENABLED", "0") == "1"   # 48-config PPH grid search after main audit
ENABLE_RATCHET           = os.environ.get("ENABLE_RATCHET", "0") == "1"   # Equity Ratchet (rising floor, risk-off on breach)
RATCHET_SWEEP_ENABLED    = os.environ.get("RATCHET_SWEEP_ENABLED", "0") == "1"   # 27-config ratchet grid search after main audit
ENABLE_ADAPTIVE_RATCHET  = os.environ.get("ENABLE_ADAPTIVE_RATCHET", "0") == "1"   # vol-regime-aware ratchet
ADAPTIVE_RATCHET_SWEEP_ENABLED = os.environ.get("ADAPTIVE_RATCHET_SWEEP_ENABLED", "0") == "1"  # 162-config adaptive ratchet grid search

# ┌─────────────────────────────────────────────────────────────────────┐
# │  PARAMETER SWEEPS                                                   │
# └─────────────────────────────────────────────────────────────────────┘
ENABLE_SWEEP_L_HIGH         = os.environ.get("ENABLE_SWEEP_L_HIGH", "0") == "1"   # L_HIGH surface (0.8 → 3.0, step 0.1)
ENABLE_SWEEP_TAIL_GUARDRAIL = os.environ.get("ENABLE_SWEEP_TAIL_GUARDRAIL", "0") == "1"   # TAIL_DROP_PCT × TAIL_VOL_MULT grid
ENABLE_SWEEP_TRAIL_WIDE     = os.environ.get("ENABLE_SWEEP_TRAIL_WIDE", "0") == "1"   # TRAIL_DD × EARLY_X wide surface
ENABLE_SWEEP_TRAIL_NARROW   = os.environ.get("ENABLE_SWEEP_TRAIL_NARROW", "0") == "1"   # TRAIL_DD × EARLY_X narrow surface
ENABLE_PARAM_SURFACES       = os.environ.get("ENABLE_PARAM_SURFACES", "0") == "1"   # 2-D Sharpe/MaxDD/Calmar/WF_CV heatmaps

# ┌─────────────────────────────────────────────────────────────────────┐
# │  PARAMETRIC STABILITY CUBES                                         │
# └─────────────────────────────────────────────────────────────────────┘
ENABLE_STABILITY_CUBE        = os.environ.get("ENABLE_STABILITY_CUBE", "0") == "1"  # leverage cube: L_BASE × L_HIGH × BOOST
ENABLE_RISK_THROTTLE_CUBE    = os.environ.get("ENABLE_RISK_THROTTLE_CUBE", "0") == "1"  # risk throttle: FILL_Y × KILL_Y × BOOST
ENABLE_EXIT_CUBE             = os.environ.get("ENABLE_EXIT_CUBE", "0") == "1"  # exit architecture: PORT_SL × PORT_TSL × KILL_Y

# ┌─────────────────────────────────────────────────────────────────────┐
# │  ROBUSTNESS & STRESS TESTS                                          │
# └─────────────────────────────────────────────────────────────────────┘
ENABLE_NOISE_STABILITY       = os.environ.get("ENABLE_NOISE_STABILITY", "0") == "1"  # noise perturbation stability test
ENABLE_SLIPPAGE_SWEEP        = os.environ.get("ENABLE_SLIPPAGE_SWEEP", "0") == "1"  # slippage impact sweep
ENABLE_EQUITY_ENSEMBLE       = os.environ.get("ENABLE_EQUITY_ENSEMBLE", "0") == "1"  # block-bootstrap equity curve ensemble
ENABLE_PARAM_JITTER          = os.environ.get("ENABLE_PARAM_JITTER", "0") == "1"  # parameter jitter / Sharpe stability test
ENABLE_RETURN_CONCENTRATION  = os.environ.get("ENABLE_RETURN_CONCENTRATION", "0") == "1"  # return concentration / Lorenz curve
ENABLE_SHARPE_RIDGE_MAP      = os.environ.get("ENABLE_SHARPE_RIDGE_MAP", "0") == "1"  # Sharpe ridge map (post-processes surface CSVs)
ENABLE_SHARPE_PLATEAU        = os.environ.get("ENABLE_SHARPE_PLATEAU", "0") == "1"  # Sharpe plateau detector (post-processes surface CSVs)
ENABLE_TOP_N_REMOVAL         = os.environ.get("ENABLE_TOP_N_REMOVAL", "0") == "1"  # top-N day removal test
ENABLE_LUCKY_STREAK          = os.environ.get("ENABLE_LUCKY_STREAK", "0") == "1"  # lucky streak test (best 30-day blocks removed)
ENABLE_PERIODIC_BREAKDOWN    = os.environ.get("ENABLE_PERIODIC_BREAKDOWN", "0") == "1"  # win rate / avg win-loss weekly + monthly
ENABLE_WEEKLY_MILESTONES     = os.environ.get("ENABLE_WEEKLY_MILESTONES", "0") == "1"  # account balance, net PnL, net ROI, cum ROI — weekly
ENABLE_MONTHLY_MILESTONES    = os.environ.get("ENABLE_MONTHLY_MILESTONES", "0") == "1"  # account balance, net PnL, net ROI, cum ROI — monthly
ENABLE_DSR_MTL               = os.environ.get("ENABLE_DSR_MTL", "0") == "1"  # deflated Sharpe ratio + minimum track record length
ENABLE_SHOCK_INJECTION       = os.environ.get("ENABLE_SHOCK_INJECTION", "0") == "1"  # shock injection stress test
ENABLE_RUIN_PROBABILITY      = os.environ.get("ENABLE_RUIN_PROBABILITY", "0") == "1"  # Monte Carlo ruin probability

# ┌─────────────────────────────────────────────────────────────────────┐
# │  DIAGNOSTICS                                                        │
# └─────────────────────────────────────────────────────────────────────┘
ENABLE_MCAP_DIAGNOSTIC       = os.environ.get("ENABLE_MCAP_DIAGNOSTIC", "0") == "1"  # market cap coverage diagnostic (~4 min)
ENABLE_CAPACITY_CURVE        = os.environ.get("ENABLE_CAPACITY_CURVE", "0") == "1"  # Almgren-Chriss liquidity capacity curve
ENABLE_REGIME_ROBUSTNESS     = os.environ.get("ENABLE_REGIME_ROBUSTNESS", "0") == "1"  # regime robustness (calendar slice analysis)
ENABLE_MIN_CUM_RETURN        = os.environ.get("ENABLE_MIN_CUM_RETURN", "0") == "1"  # minimum fixed cumulative return over rolling windows

# ── Math verification diagnostic export ───────────────────────────────
# When enabled, exports a full step-by-step math verification to Google
# Sheets after each run so every output metric can be independently verified.
#
# Tabs written (overwriting each run):
#   DiagSummary  — config params, capital mode, key metrics
#   DiagDaily    — one row per active day with all interim steps
#   DiagPath_YYYYMMDD — full 216-bar intraday path for each date in
#                       DIAGNOSTIC_PATH_DATES (empty = skip path tabs)
#
# Uses the same credentials.json / token.json as the matrix loader.
ENABLE_DIAGNOSTIC_EXPORT  = False
DIAGNOSTIC_SHEET_ID       = "1_gD9-nIrVROYuXTP2INO52jGYGwzS0qh1_IPYMtOGP4"
DIAGNOSTIC_PATH_DATES     = ["2025-03-06", "2025-03-07"]   # e.g. ["2025-03-06", "2025-05-09"]
DIAGNOSTIC_PATH_ALL_DATES = False  # True = export a DiagPath tab for every active day

# ██████████████████████████████████████████████████████████████████████
# ██                  END OF MASTER TOGGLE PANEL                      ██
# ██████████████████████████████████████████████████████████████████████


# ══════════════════════════════════════════════════════════════════════
# AUDIT SETTINGS
# ══════════════════════════════════════════════════════════════════════

STARTING_CAPITAL = float(os.environ.get("STARTING_CAPITAL", "100000"))

# ── Capital allocation mode ───────────────────────────────────────────
# "compounding" : position size = current equity × leverage  (default)
#                 P&L compounds — a winning run increases future positions.
# "fixed"       : position size fixed at STARTING_CAPITAL × leverage every day.
#                 Behaviour when account dips below STARTING_CAPITAL is
#                 controlled by FIXED_NOTIONAL_CAP (see below).
CAPITAL_MODE = os.environ.get("CAPITAL_MODE", "fixed")   # "compounding" | "fixed"

# ── Fixed-notional cap (only used when CAPITAL_MODE = "fixed") ───────────
# "external" : always trade STARTING_CAPITAL notional regardless of balance.
#              Account can go negative if losses exceed current equity.
#              Represents deploying from an external capital pool.
# "internal"   : trade min(STARTING_CAPITAL, current_equity) — position scales
#              down if account drops below STARTING_CAPITAL, preventing deficit.
#              More conservative; represents a self-funded account.
FIXED_NOTIONAL_CAP = os.environ.get("FIXED_NOTIONAL_CAP", "internal")   # "external" | "internal"

TRADING_DAYS     = 365
N_TRIALS         = int(os.environ.get("N_TRIALS", "3"))
#OUTPUT_DIR_BASE  = "audit_outputs_regime_comparison_v2"
#OUTPUT_DIR_BASE  = "audit_outputs_BEFORE"
OUTPUT_DIR_BASE  = "audit_outputs_BEFORE"


# ══════════════════════════════════════════════════════════════════════
# FILTER PARAMETERS
# ══════════════════════════════════════════════════════════════════════

# ── Tail-event guardrail settings ────────────────────────────────────
# Drop guard: sit out if previous day BTC return < -TAIL_DROP_PCT
# Vol spike guard: sit out if 5d rvol > TAIL_VOL_MULT × 60d baseline rvol
TAIL_DROP_PCT         = float(os.environ.get("TAIL_DROP_PCT", "0.04"))   # 5% single-day drop threshold (try 0.04, 0.05, 0.06)
TAIL_VOL_MULT         = float(os.environ.get("TAIL_VOL_MULT", "1.4"))    # vol spike multiplier (try 2.0, 2.5)

# ── Cross-sectional dispersion filter settings ───────────────────────
# Sits out when alt-coin dispersion falls below a fraction of its rolling median.
# Low dispersion = everything moves together = momentum edge disappears.
DISPERSION_THRESHOLD     = float(os.environ.get("DISPERSION_THRESHOLD", "0.66"))   # sit out if disp_ratio < this (try 0.50, 0.70, 0.80)
DISPERSION_BASELINE_WIN  = int(os.environ.get("DISPERSION_BASELINE_WIN", "33"))     # rolling median window (days)
# When DISPERSION_DYNAMIC_UNIVERSE=False (default), the filter uses the
# hardcoded DISPERSION_SYMBOLS list selected by DISPERSION_UNIVERSE_SIZE.
# When True, the filter selects the top-DISPERSION_N symbols by market
# cap each day using daily mcap history fetched from CoinGecko, lagged
# by 1 day to prevent lookahead.  See fetch_mcap_history() and
# build_dynamic_symbol_mask() for implementation details.
DISPERSION_DYNAMIC_UNIVERSE = _env_bool("DISPERSION_DYNAMIC_UNIVERSE", True)   # True  = top-N by market cap per day (lagged)
                                      # False = use DISPERSION_SYMBOLS list (default)
DISPERSION_N                = int(os.environ.get("DISPERSION_N", "40"))      # top-N symbols to select per day (dynamic mode)
                                      # must be <= len(COINGECKO_TO_BINANCE)

DISPERSION_UNIVERSE_SIZE = DISPERSION_N

if DISPERSION_DYNAMIC_UNIVERSE:
    print("DISPERSION_UNIVERSE_SIZE:", DISPERSION_N)
else:
    print("DISPERSION_UNIVERSE_SIZE:", DISPERSION_UNIVERSE_SIZE)

DISPERSION_MCAP_CACHE_FILE  = "dispersion_mcap_cache.csv"
DISPERSION_DYNAMIC_RETURNS_CACHE_FILE = "dispersion_dynamic_returns_cache.csv"
# ^ separate returns cache for dynamic mode — avoids overwriting the
#   static DISPERSION_CACHE_FILE when toggling between modes

# ── coingecko_marketcap.py parquet path ───────────────────────────────
# If you have run coingecko_marketcap.py (--mode historical) its output
# parquet is used INSTEAD of the live CoinGecko API in fetch_mcap_history().
# Advantages over the live API:
#   * Historically accurate daily ranks — 2000-coin universe, rank
#     recomputed from actual per-date market caps (no survivorship bias)
#   * Zero runtime API calls once downloaded — reads local file instantly
#   * Resumable download with checkpoint file
# Set to "" to always use the live API (original behaviour).
# Default path matches coingecko_marketcap.py --output-dir data/marketcap
_default_mcap_dir = os.environ.get("MARKETCAP_DIR", "/Users/johnmullin/Desktop/desk/benji3m/binetl/data/marketcap")
DISPERSION_MCAP_PARQUET = os.environ.get("MARKETCAP_PARQUET", os.path.join(_default_mcap_dir, "marketcap_daily.parquet"))
# When set to "db", market cap is read from market.market_cap_daily instead
# of the parquet file. Set via env var or the frontend data-source toggle.
MCAP_SOURCE = os.environ.get("MCAP_SOURCE", "parquet")

# ── CoinGecko API key ─────────────────────────────────────────────────
# CoinGecko now requires an API key even on the free Demo tier (changed
# late 2024).  Without it all requests return HTTP 401.
# Get a free key at: https://www.coingecko.com/en/api
# Then paste it below.  The key is sent as the x-cg-demo-api-key header.
# Leave as "" to attempt keyless requests (will fail with 401).
COINGECKO_API_KEY = "CG-qTu1Re8jaHojeMQuwUdtasoT"

# ── Realised Volatility Gate ──────────────────────────────────────────
# Sit out when BTC intraday vol is compressed below its rolling floor.
# Lagged 1 day: yesterday's vol gates today's trade.
VOL_LOOKBACK             = int(os.environ.get("VOL_LOOKBACK", "10"))     # rolling window for realised vol (days)
VOL_PERCENTILE           = float(os.environ.get("VOL_PERCENTILE", "0.25"))   # sit out when vol < this rolling quantile
VOL_BASELINE_WIN         = int(os.environ.get("VOL_BASELINE_WIN", "90"))     # window for rolling quantile baseline

# ── IC (Information Coefficient) Filter ──────────────────────────────
# IC_SIGNAL   : signal to use: 'mom1d' | 'mom5d' | 'skew20d' | 'vol20d_inv'
# IC_WINDOW   : rolling window (days) to compute mean IC
# IC_THRESHOLD: sit out when rolling mean IC < this value
IC_SIGNAL             = os.environ.get("IC_SIGNAL", "mom1d")
IC_WINDOW             = int(os.environ.get("IC_WINDOW", "30"))
IC_THRESHOLD          = float(os.environ.get("IC_THRESHOLD", "0.02"))

# ── BTC Trend (Moving Average) Filter ────────────────────────────────
# Sits out when BTC closed BELOW its N-day SMA on the previous day.
BTC_MA_DAYS           = int(os.environ.get("BTC_MA_DAYS", "20"))    # SMA window in calendar days (try 10, 20, 50)

# ── Blofin Availability Filter ────────────────────────────────────────
# The Blofin public API may be geo-blocked on some servers. If the live
# API fetch returns 403, generate a static snapshot CSV once from a machine
# that can reach Blofin by running:
#
#   python3 fetch_blofin_snapshot.py          # creates blofin_instruments.csv
#
# Then set BLOFIN_CSV_PATH to that file's path. The CSV has two columns:
#   date     — today's date (YYYY-MM-DD), applied to all rows
#   symbol   — base symbol e.g. BTC, ETH, SOL
#
# BLOFIN_MIN_SYMBOLS: sit flat on days where fewer than this many of your
#   portfolio symbols are listed on Blofin.
# BLOFIN_PORTFOLIO_SYMBOLS: set of symbols your strategy trades. Required
#   when no deploys CSV is provided — without it the gate cannot evaluate
#   symbol availability and will silently add zero filter days.
BLOFIN_MIN_SYMBOLS       = int(os.environ.get("BLOFIN_MIN_SYMBOLS", "1"))     # sit flat if fewer than N portfolio symbols are on Blofin
BLOFIN_CSV_PATH          = ""    # path to blofin_instruments.csv (run fetch_blofin_snapshot.py)
                                  # leave empty to attempt live API fetch
BLOFIN_PORTFOLIO_SYMBOLS = set() # e.g. {"BTC","ETH","SOL","XRP","DOGE","ADA","AVAX","LINK","DOT","MATIC"}
                                  # leave empty only if you supply a deploys CSV via --deploys

# ── Performance-Based Leverage Scaling (BOOST mode) ──────────────────
# PERF_LEV_WINDOW         rolling window in days for both signals (lagged 1d)
# PERF_LEV_SORTINO_TARGET Sortino at which boost reaches zero
# PERF_LEV_DR_FLOOR       avg daily return ceiling — above this, suppress boost
# PERF_LEV_MAX_BOOST      max multiplier when Sortino near zero (1.5 = +50%)
PERF_LEV_WINDOW           = int(os.environ.get("PERF_LEV_WINDOW", "10"))      # rolling window (days)
PERF_LEV_SORTINO_TARGET   = float(os.environ.get("PERF_LEV_SORTINO_TARGET", "3.0"))     # max boost when rolling Sortino >= this
PERF_LEV_DR_FLOOR         = 0.0     # boost suppressed when avg daily ret < this
PERF_LEV_MAX_BOOST        = float(os.environ.get("PERF_LEV_MAX_BOOST", "1.5"))     # max leverage multiplier (1.5 = up to 1.5x static)

# ── Volatility-Targeted Leverage ─────────────────────────────────────
# VOL_LEV_TARGET_VOL    vol level at which boost starts
# VOL_LEV_WINDOW        rolling window for vol and Sharpe (days)
# VOL_LEV_SHARPE_REF    Sharpe at which Sharpe scalar = 1.0 (neutral)
# VOL_LEV_DD_THRESHOLD  drawdown level that suppresses all boost
# VOL_LEV_DD_SCALE      multiplier during DD guard (1.0 = no boost)
# VOL_LEV_MAX_BOOST     ceiling on the boost scalar
# LEV_QUANTIZATION_MODE off|binary|stepped quantization on vol-lev scalar
# LEV_QUANTIZATION_STEP step size for stepped mode
VOL_LEV_WINDOW            = int(os.environ.get("VOL_LEV_WINDOW", "30"))      # rolling window (days)
VOL_LEV_TARGET_VOL        = float(os.environ.get("VOL_LEV_TARGET_VOL", "0.02"))    # target daily vol (2% — boost when vol < this)
VOL_LEV_SHARPE_REF        = 3.0     # Sharpe at which Sharpe scalar = 1.0

VOL_LEV_MAX_BOOST         = float(os.environ.get("VOL_LEV_MAX_BOOST", "2.0"))     # max boost multiplier on static leverage
LEV_QUANTIZATION_MODE     = os.environ.get("LEV_QUANTIZATION_MODE", "off").strip().lower()  # off | binary | stepped
LEV_QUANTIZATION_STEP     = float(os.environ.get("LEV_QUANTIZATION_STEP", "0.1"))            # scalar step for stepped mode



VOL_LEV_DD_THRESHOLD      = float(os.environ.get("VOL_LEV_DD_THRESHOLD", "-0.06"))   # running DD below this → suppress boost
VOL_LEV_DD_SCALE          = 1.0     # during DD guard: 1.0 = flat, <1.0 = de-lever

# ── Vol-Lev DD Grid Search ─────────────────────────────────────────────────
# Activated by --grid-search-vol-lev-dd.
# Sweeps VOL_LEV_DD_THRESHOLD (0% → -20%, step -1%) ×
#        VOL_LEV_DD_SCALE     (0.0 → 1.0, step 0.1)
# Records: Sharpe, CAGR%, MaxDD%, Active, WF-CV, TotRet%, Eq,
#          Wst1D%, Wst1W%, Wst1M%, DSR%
# Output: grid_search_vol_lev_dd_results.csv + ranked console table.
GRID_SEARCH_VOL_LEV_DD    = False

# ── Contrarian Leverage Scaling ──────────────────────────────────────
# CONTRA_LEV_SIGNALS      which metrics to combine (list of signal names)
# CONTRA_LEV_WINDOW       trailing window for percentile rank (days)
# CONTRA_LEV_MAX_BOOST    max leverage multiplier at rank=0
# CONTRA_LEV_DD_THRESHOLD running DD below this suppresses all boost
CONTRA_LEV_SIGNALS         = ["Sharpe", "AvgDailyRet%"]  # signals to combine
CONTRA_LEV_WINDOW          = int(os.environ.get("CONTRA_LEV_WINDOW", "30"))      # trailing window for percentile rank (days)
CONTRA_LEV_MAX_BOOST       = float(os.environ.get("CONTRA_LEV_MAX_BOOST", "2.0"))     # max boost at rank=0 (fully recovered/cheap)
CONTRA_LEV_DD_THRESHOLD    = float(os.environ.get("CONTRA_LEV_DD_THRESHOLD", "-0.15"))   # suppress boost when drawdown below this

# ── Periodic Profit Harvest (PPH) ────────────────────────────────────
# PPH_FREQUENCY     "daily" | "weekly" | "monthly"
# PPH_THRESHOLD     harvest when profit >= watermark × this fraction
# PPH_HARVEST_FRAC  fraction of excess profit to harvest
PPH_FREQUENCY           = os.environ.get("PPH_FREQUENCY", "weekly")      # "daily" | "weekly" | "monthly"
PPH_THRESHOLD           = float(os.environ.get("PPH_THRESHOLD", "0.20"))          # harvest when profit >= watermark × this
PPH_HARVEST_FRAC        = float(os.environ.get("PPH_HARVEST_FRAC", "0.50"))          # fraction of excess profit to harvest

PPH_SWEEP_FREQUENCIES   = ["daily", "weekly", "monthly"]
PPH_SWEEP_THRESHOLDS    = [0.10, 0.20, 0.30, 0.40]
PPH_SWEEP_FRACTIONS     = [0.25, 0.50, 0.75, 1.00]

# ── Equity Ratchet ────────────────────────────────────────────────────
# RATCHET_FREQUENCY        "daily" | "weekly" | "monthly"
# RATCHET_TRIGGER          min equity growth before floor ratchets up
# RATCHET_LOCK_PCT         floor set this far below new high
# RATCHET_RISK_OFF_LEV_SCALE  leverage when in risk-off (0.0 = flat)
RATCHET_FREQUENCY           = os.environ.get("RATCHET_FREQUENCY", "weekly")      # "daily" | "weekly" | "monthly"
RATCHET_TRIGGER             = float(os.environ.get("RATCHET_TRIGGER", "0.20"))          # ratchet when growth >= this fraction
RATCHET_LOCK_PCT            = float(os.environ.get("RATCHET_LOCK_PCT", "0.15"))          # floor = new_high * (1 - lock_pct)
RATCHET_RISK_OFF_LEV_SCALE  = float(os.environ.get("RATCHET_RISK_OFF_LEV_SCALE", "0.0"))          # 0.0 = sit flat, 0.5 = half leverage

RATCHET_SWEEP_FREQUENCIES   = ["daily", "weekly", "monthly"]
RATCHET_SWEEP_TRIGGERS      = [0.10, 0.20, 0.30]
RATCHET_SWEEP_LOCK_PCTS     = [0.10, 0.15, 0.20]

# ── Regime-Adaptive Equity Ratchet ───────────────────────────────────
# ADAPTIVE_RATCHET_FREQUENCY      "daily" | "weekly" | "monthly"
# ADAPTIVE_RATCHET_VOL_WINDOW     rolling window (days) for vol regime
# ADAPTIVE_RATCHET_VOL_LOW        daily vol below this → "low" regime
# ADAPTIVE_RATCHET_VOL_HIGH       daily vol above this → "high" regime
# ADAPTIVE_RATCHET_RISK_OFF_SCALE leverage when below floor (0.0 = flat)
# ADAPTIVE_RATCHET_FLOOR_DECAY    floor multiplier per flat/risk-off day
ADAPTIVE_RATCHET_FREQUENCY         = os.environ.get("ADAPTIVE_RATCHET_FREQUENCY", "weekly")
ADAPTIVE_RATCHET_VOL_WINDOW        = int(os.environ.get("ADAPTIVE_RATCHET_VOL_WINDOW", "20"))           # rolling window for vol regime (days)
ADAPTIVE_RATCHET_VOL_LOW           = float(os.environ.get("ADAPTIVE_RATCHET_VOL_LOW", "0.03"))         # daily vol < 3% → low regime
ADAPTIVE_RATCHET_VOL_HIGH          = float(os.environ.get("ADAPTIVE_RATCHET_VOL_HIGH", "0.07"))         # daily vol > 7% → high regime
ADAPTIVE_RATCHET_RISK_OFF_SCALE    = float(os.environ.get("ADAPTIVE_RATCHET_RISK_OFF_SCALE", "0.0"))          # 0.0 = flat, 0.5 = half leverage
ADAPTIVE_RATCHET_FLOOR_DECAY       = float(os.environ.get("ADAPTIVE_RATCHET_FLOOR_DECAY", "0.995"))        # floor multiplier per flat day
ADAPTIVE_RATCHET_TABLE             = {
    "low":    {"trigger": 0.10, "lock_pct": 0.10},  # quiet → protect quickly
    "normal": {"trigger": 0.20, "lock_pct": 0.15},  # balanced
    "high":   {"trigger": 0.35, "lock_pct": 0.20},  # explosive → let winners run
}

ADAPTIVE_RATCHET_SWEEP_FREQUENCIES = ["daily", "weekly", "monthly"]
ADAPTIVE_RATCHET_SWEEP_VOL_LOWS    = [0.02, 0.03, 0.04]   # low/normal boundary
ADAPTIVE_RATCHET_SWEEP_VOL_HIGHS   = [0.06, 0.07, 0.08]   # normal/high boundary
ADAPTIVE_RATCHET_SWEEP_DECAY_RATES = [0.990, 0.995, 1.000] # 1.000 = no decay (original)
ADAPTIVE_RATCHET_SWEEP_TABLES      = [
    # Config A — from the proposal
    {
        "low":    {"trigger": 0.10, "lock_pct": 0.10},
        "normal": {"trigger": 0.20, "lock_pct": 0.15},
        "high":   {"trigger": 0.35, "lock_pct": 0.20},
    },
    # Config B — wider trigger bands, slightly tighter locks
    {
        "low":    {"trigger": 0.15, "lock_pct": 0.10},
        "normal": {"trigger": 0.25, "lock_pct": 0.15},
        "high":   {"trigger": 0.40, "lock_pct": 0.20},
    },
]

# ── Parameter Surface Sweeps ──────────────────────────────────────────
TAIL_SWEEP_DROP_VALUES      = [0.02, 0.023, 0.024, 0.025, 0.026, 0.027, 0.028, 0.029, 0.03, 0.031, 0.032, 0.033, 0.034, 0.035, 0.04]
# TAIL_SWEEP_VOL_VALUES       = [1.0, 1.1, 1.2, 1.3, 1.33, 1.4, 1.5, 1.6, 1.7, 1.8]
TAIL_SWEEP_VOL_VALUES       = [1.4]

# ── 2-Parameter Surface Maps ──────────────────────────────────────────
PARAM_SURFACE_PAIRS = [
    # ────────────────────────────────────────────────────────────────
    # Tier 1 — Core Strategy Geometry
    # ────────────────────────────────────────────────────────────────
    {   # Gate timing: trial window length vs entry threshold
        "param_x":      "EARLY_KILL_X",
        "param_y":      "EARLY_KILL_Y",
        "values_x":     [15, 20, 25, 30, 35, 40, 45, 50],
        "values_y":     [0.0, 0.0005, 0.0008, 0.0010, 0.0015, 0.0020, 0.0025, 0.0030],
        "surface_label":"killx_killy",
    },
    {   # Risk controls: hard stop vs trailing stop
        "param_x":      "PORT_SL",
        "param_y":      "PORT_TSL",
        "values_x":     [ -0.04, -0.05, -0.055, -0.06, -0.065, -0.07, -0.075, -0.08, -0.085, -0.10],
        "values_y":     [0.040, 0.045, 0.050, 0.055, 0.060, 0.065, 0.075, 0.08, 0.085, 0.090, 0.095, 0.10, 0.105, 0.110, 0.115, 0.12, 0.125, 0.130],
        "surface_label":"sl_tsl",
    },
    {   # Leverage architecture (base → ceiling)
        "param_x":      "L_BASE",
        "param_y":      "L_HIGH",
        "values_x":     [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0],
        "values_y":     [0.6, 0.8, 1.0, 1.2, 1.4, 1.5, 1.6, 1.8, 2.0],
        "surface_label":"lbase_lhigh",
    },
    {   # Risk / reward interaction: leverage vs trailing stop
        "param_x":      "L_HIGH",
        "param_y":      "PORT_TSL",
        "values_x":     [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.75, 2.0],
        "values_y":     [0.040, 0.045, 0.050, 0.055, 0.060, 0.065, 0.075, 0.08, 0.085, 0.090, 0.095, 0.10, 0.105, 0.110, 0.115, 0.12, 0.125, 0.130],
        "surface_label":"lhigh_tsl",
    },
    # ────────────────────────────────────────────────────────────────
    # Tier 2 — Cross-Parameter Interaction Diagnostics
    # ────────────────────────────────────────────────────────────────
    {   # Leverage sensitivity to hard stop level
        "param_x":      "PORT_SL",
        "param_y":      "L_HIGH",
        "values_x":     [ -0.04, -0.05, -0.055, -0.06, -0.065, -0.07, -0.075, -0.08, -0.085, -0.10],
        "values_y":     [0.8, 1.0, 1.2, 1.4, 1.5, 1.6, 1.8, 2.0],
        "surface_label":"sl_lhigh",
    },
    {   # Entry selectivity vs downside protection
        "param_x":      "EARLY_KILL_Y",
        "param_y":      "PORT_SL",
        "values_x":     [0.0, 0.0005, 0.0008, 0.0010, 0.0015, 0.0020, 0.0025, 0.0030],
        "values_y":     [ -0.04, -0.05, -0.055, -0.06, -0.065, -0.07, -0.075, -0.08, -0.085, -0.10],
        "surface_label":"killy_sl",
    },
    {   # Profit target vs entry threshold
        "param_x":      "EARLY_FILL_Y",
        "param_y":      "EARLY_KILL_Y",
        "values_x":     [0.07, 0.08, 0.09, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50],
        "values_y":     [0.0, 0.0005, 0.0008, 0.0010, 0.0015, 0.0020, 0.0025, 0.0030],
        "surface_label":"filly_killy",
    },
    {   # Fine-resolution EARLY_FILL_Y sweep — zoom into 0.15–0.35 zone
        # Motivated by coarse surface showing 0.20 outperforming baseline 0.30
        # across most KILL_Y values. 9x7 = 63 cells at 0.025 steps.
        "param_x":      "EARLY_FILL_Y",
        "param_y":      "EARLY_KILL_Y",
        "values_x":     [0.07, 0.08, 0.09, 0.10, 0.15, 0.175, 0.20, 0.225, 0.25, 0.275, 0.30, 0.325],
        "values_y":     [0.0, 0.0005, 0.0008, 0.0010, 0.0015, 0.0020, 0.0025, 0.0030],
        "surface_label":"filly_fine",
    },
    {   # Fill window duration vs profit threshold — how long the fill window
        # is open (minutes) vs how much gain triggers the early exit.
        # EARLY_FILL_X=1000 is effectively uncapped intraday; sweep tests whether
        # tighter windows (ending fill opportunity earlier) hurt or help.
        # X axis: window length in minutes (360=6h, 540=9h, 720=12h, 900=15h, 1080=18h)
        # Y axis: profit threshold (same range as filly_killy)
        "param_x":      "EARLY_FILL_X",
        "param_y":      "EARLY_FILL_Y",
        "values_x":     [360, 540, 600, 660, 720, 900, 1080],   # 6h, 9h, 12h, 15h, 18h
        "values_y":     [0.07, 0.08, 0.09, 0.10, 0.15, 0.175, 0.20, 0.225, 0.25, 0.30],
        "surface_label":"fillx_filly",
    },
    {   # Nested entry gates: skip threshold vs conviction threshold
        "param_x":      "EARLY_KILL_Y",
        "param_y":      "EARLY_INSTILL_Y",
        "values_x":     [0.0010, 0.0015, 0.0020, 0.0025, 0.0030],
        "values_y":     [0.0010, 0.0015, 0.0020, 0.0025, 0.0030],
        "surface_label":"killy_instilly",
    },
    # ────────────────────────────────────────────────────────────────
    # Tier 3 — Vol-Target Leverage Architecture
    # Use "VOL_LEV:" prefix to route values into vol_lev_params.
    # ────────────────────────────────────────────────────────────────
    # {   # Primary aggressiveness: target vol × max boost ceiling
    #     "param_x":      "VOL_LEV:target_vol",
    #     "param_y":      "VOL_LEV:max_boost",
    #     "values_x":     [0.015, 0.018, 0.020, 0.023, 0.025, 0.030],
    #     "values_y":     [1.5, 1.75, 2.0, 2.25, 2.5],
    #     "surface_label":"vl_targetvol_maxboost",
    # },
    # {   # Drawdown guard depth × de-lever scale
    #     "param_x":      "VOL_LEV:dd_threshold",
    #     "param_y":      "VOL_LEV:dd_scale",
    #     "values_x":     [-0.10, -0.12, -0.15, -0.18, -0.20],
    #     "values_y":     [0.5, 0.75, 1.0],
    #     "surface_label":"vl_ddthresh_ddscale",
    # },
    # {   # Sharpe reference × rolling window — responsiveness tuning
    #     "param_x":      "VOL_LEV:sharpe_ref",
    #     "param_y":      "VOL_LEV:window",
    #     "values_x":     [1.5, 2.0, 2.5, 2.6, 3.0, 3.3, 3.5],
    #     "values_y":     [15, 20, 30, 45, 60],
    #     "surface_label":"vl_sharperef_window",
    # },
]

# ── Noise Perturbation Stability Test ────────────────────────────────────────
# Deployment-readiness test: a robust strategy survives small random
# perturbations to its inputs. An overfit one collapses.
#
# Two independent perturbation modes — run independently or together:
#
# MODE A — Return noise (microstructure / execution noise):
#   Adds IID Gaussian noise to every bar of each day's intraday price path
#   before the day is simulated. Noise is parameterised as annualised daily vol
#   equivalent (e.g. 0.005 = 0.5% daily std). Tests whether the strategy's
#   edge survives realistic fill-price and microstructure uncertainty.
#
# MODE B — Signal shuffle (filter robustness):
#   Randomly flips K% of active days → flat and K% of flat days → active.
#   Tests whether the filter's specific day selection drives the edge, or
#   whether any similar set of days would do equally well (the dangerous case).
#
# For each mode, N_TRIALS independent seeds are run. Output: mean ± std of
# Sharpe, CAGR, MaxDD, Calmar across trials, vs clean baseline. A stability
# score (mean_sharpe / baseline_sharpe) is printed — target ≥ 0.85.
#
# Uses the best available filter (tail_disp combo → tail-only → none).
#
#   NOISE_N_TRIALS              trials per perturbation level  (50–200 typical)
#   NOISE_RETURN_LEVELS         list of daily-vol noise magnitudes to test
#   NOISE_SHUFFLE_LEVELS        list of shuffle fractions to test (0.05 = 5%)
NOISE_N_TRIALS              = 100
NOISE_RETURN_LEVELS         = [0.001, 0.003, 0.005, 0.010]
NOISE_SHUFFLE_LEVELS        = [0.02, 0.05, 0.10, 0.20]

# ── Slippage Impact Sweep ─────────────────────────────────────────────
# SLIPPAGE_LEVELS: one-way slippage per trade in decimal (0.0025 = 0.25%)
SLIPPAGE_LEVELS             = [0.0005, 0.0010, 0.0025, 0.0050, 0.0100]

# ── Equity Curve Ensemble ─────────────────────────────────────────────
EQUITY_ENSEMBLE_N_TRIALS    = 500    # bootstrap paths (200–1000 typical)

# ── Param Jitter / Sharpe Stability Test ─────────────────────────────
# "rel" → new_val = base * Uniform(1-mag, 1+mag)
# "abs" → new_val = base + Uniform(-mag, +mag)
PARAM_JITTER_N_TRIALS    = 300
PARAM_JITTER_SPEC        = {
    "L_HIGH":          ("rel", 0.10),   # ±10%
    "PORT_SL":         ("rel", 0.15),   # ±15%
    "PORT_TSL":        ("rel", 0.15),   # ±15%
    "EARLY_KILL_X":    ("abs", 5),      # ±5 bars
    "EARLY_KILL_Y":    ("rel", 0.40),   # ±40%
    "EARLY_FILL_Y":    ("rel", 0.40),   # ±40%
    "EARLY_FILL_X":    ("rel", 0.40),   # ±40%
}

# ── Return Concentration ──────────────────────────────────────────────
CONCENTRATION_TOP_NS         = [1, 5, 10, 20, 25]

# ── Sharpe Ridge Map + Plateau Detector ──────────────────────────────
PLATEAU_PCT_OF_MAX_LIST      = [0.95, 0.975, 0.99]
PLATEAU_MIN_CLUSTER_CELLS    = 3

# ── Top-N Day Removal Test ────────────────────────────────────────────
TOP_N_REMOVAL_NS         = [1, 3, 5, 10]

# ── Lucky Streak Test ─────────────────────────────────────────────────
LUCKY_STREAK_WINDOW      = 30   # days per block

# ── Deflated Sharpe Ratio + Minimum Track Record Length ──────────────
# DSR_N_TRIALS holds the number of trials for Deflated Sharpe Ratio.
# DEPRECATED: N_TRIALS is now the single source of truth for all report-level calculations.
# DSR_N_TRIALS             = 1010   # total configurations tested across all sweeps
DSR_TARGET_SHARPE        = 1.0    # H0 threshold for MTL calculation

# ── Shock Injection Test ──────────────────────────────────────────────
SHOCK_SIZES              = [-10.0, -20.0, -30.0, -40.0]   # percent loss per day
SHOCK_N_DAYS             = [1, 3, 5]                       # number of consecutive days

# ── Ruin Probability ──────────────────────────────────────────────────
RUIN_THRESHOLDS          = [-0.33, -0.50, -0.75]   # drawdown levels to test
RUIN_N_SIMS              = 10_000                   # bootstrap simulations

# ── Parametric Stability Cube (L_BASE × L_HIGH × BOOST) ──────────────
CUBE_VALUES_LBASE        = [0.0, 0.5, 1.0]
CUBE_VALUES_LHIGH        = [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 2.0]
CUBE_VALUES_BOOST        = [1.0, 2.0, 2.5, 3.0]

# ── Risk Throttle Stability Cube (FILL_Y × KILL_Y × BOOST) ───────────
CUBE_VALUES_FILL_Y           = [0.07, 0.08, 0.09, 0.10, 0.11]
CUBE_RT_VALUES_KILL_Y        = [0.001, 0.0015, 0.003]
CUBE_RT_VALUES_BOOST         = [1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0, 2.5, 3.0]

# ── Exit Architecture Stability Cube (PORT_SL × PORT_TSL × KILL_Y) ───
CUBE_VALUES_PORT_SL          = [-0.07, -0.08, -0.09, -0.10, -0.11, -0.12, -0.13]
CUBE_VALUES_PORT_TSL         = [0.040, 0.045, 0.050, 0.055, 0.060, 0.065, 0.075, 0.08, 0.085, 0.090, 0.095, 0.10, 0.105, 0.110, 0.115, 0.12, 0.125, 0.130]
CUBE_EX_VALUES_KILL_Y        = [0.001, 0.0015, 0.003]

# ── Liquidity Capacity Curve ──────────────────────────────────────────
# ADV calibrated from master_oi_training_table.parquet (745 symbols, 369 days):
#   Median of daily mean ADV : $14.0M  ← base case (CAPACITY_ADV_PER_SYMBOL)
#   Median of daily min ADV  :  $2.7M  ← conservative floor
#   Mean of daily mean ADV   : $32.2M  ← optimistic case
# Switch CAPACITY_ADV_PER_SYMBOL to 2_700_000 for worst-case or 32_000_000 for best-case.
CAPACITY_ADV_PER_SYMBOL  = 14_000_000.0  # $14M ADV — median of daily mean ADV (empirical)
CAPACITY_IMPACT_ALPHA    = 0.10           # Almgren-Chriss alpha for crypto markets

# ── Regime Robustness Test ────────────────────────────────────────────
REGIME_MIN_DAYS          = 30    # minimum days per slice to run

# ── Minimum Cumulative Return Table ──────────────────────────────────
# Non-compounding (arithmetic sum) worst-case over all contiguous N-day
# windows in the return series. Reports the floor a capital allocator
# would have experienced at the worst entry point for each horizon.
MIN_CUM_RETURN_WINDOWS   = [1, 3, 7, 14, 21, 30, 40, 50, 60, 70, 80, 90, 100]

# ── Three candidate configs from V5-D majority-vote grid search ──────
# Each entry: (short_name, params_dict)
# The audit runs every config × every filter mode and prints a combined table.

AFTER_CONFIGS = [
    (
        "A",
        {
            "EARLY_KILL_X":    35,
            "EARLY_KILL_Y":    0.003,
            "EARLY_INSTILL_Y": 0.003,
            "L_BASE":          0,
            "L_HIGH":          1.33,

            "PORT_TSL":        0.075,
            "PORT_SL":         -0.06,

            "EARLY_FILL_Y":    0.09,
            "EARLY_FILL_X":    720,
        },
    ),
]

CANDIDATE_CONFIGS = [
    (
        "A",
        {
            "EARLY_KILL_X":    int(os.environ.get("EARLY_KILL_X", "5")),
            "EARLY_KILL_Y":    float(os.environ.get("EARLY_KILL_Y", "-999")),
            "EARLY_INSTILL_Y": float(os.environ.get("EARLY_INSTILL_Y", "-999")),
            "L_BASE":          float(os.environ.get("L_BASE", "0")),
            "L_HIGH":          float(os.environ.get("L_HIGH", "1.0")),

            "PORT_TSL":        float(os.environ.get("PORT_TSL", "0.99")),
            "PORT_SL":         float(os.environ.get("PORT_SL", "-0.99")),

            "EARLY_FILL_Y":    float(os.environ.get("EARLY_FILL_Y", "0.99")),
            "EARLY_FILL_X":    int(os.environ.get("EARLY_FILL_X", "5")),
        },
    ),
]



# Legacy alias so any remaining references to BEST_SHARPE_PARAMS still work.
BEST_SHARPE_PARAMS = CANDIDATE_CONFIGS[0][1]

# ══════════════════════════════════════════════════════════════════════
# BEST_SHARPE CONFIG  (from prior audit sessions)
# ══════════════════════════════════════════════════════════════════════

# BEST_SHARPE_PARAMS = {
#     "EARLY_X_MINUTES":         35,
#     "EARLY_Y_4X":              0.0085,
#     "TRAIL_DD_1X":             0.085,
#     "L_HIGH":                  0.875,
#     "L_BASE":                  0.75,
#     "STRONG_THR_1X":           0.005,
#     "PORT_STOP_1X":            -0.050,
#     "EARLY_FILL_THRESHOLD_1X": 0.30,
#     "EARLY_FILL_MAX_MINUTES":  720,
# }

    # ("Best_Sharpe", {
    #     "EARLY_X_MINUTES":         35,           # locked
    #     "EARLY_Y_4X":              0.0085,                    # tbd
    #     "TRAIL_DD_1X":             0.085,        # locked
    #     "L_HIGH":                  0.875,        # locked
    #     "L_BASE":                  0.75,         # locked
    #     "STRONG_THR_1X":           0.005,        # locked
    #     "PORT_STOP_1X":            -0.050,       # locked
    #     "EARLY_FILL_THRESHOLD_1X": 0.30,                      # tbd
    #     "EARLY_FILL_MAX_MINUTES":  720,                       # tbd
    # }),

# ══════════════════════════════════════════════════════════════════════
# V3 FILTER THRESHOLDS  (from validate_regime_filter_v3.py results)
# Best AND filter: fr_raw < 0.0100% AND fg_7d_ma < 45  (F1=0.635)
# ══════════════════════════════════════════════════════════════════════

V3_FR_COL       = "fr_raw"       # daily mean funding rate
V3_FR_THRESHOLD = 0.0100         # % - sit flat when FR < this
V3_FG_COL       = "fg_7d_ma"    # 7-day MA of F&G
V3_FG_THRESHOLD = 45             # sit flat when F&G 7d MA < this

# ══════════════════════════════════════════════════════════════════════
# V4 FILTER THRESHOLDS  (from validate_regime_filter_v4.py results)
# Best triple: (FR<0.01% AND FG<45) OR (ADX<29.0 AND rvol20d<32.6)  F1=0.688
# Branch A - same as V3 AND - catches fear/liquidation (R1, R3, R4)
# Branch B - ADX+rvol - catches low-vol bull-grind (R2)
# ══════════════════════════════════════════════════════════════════════

V4_FR_THRESHOLD   = 0.0100   # % - Branch A: FR daily mean below this
V4_FG_THRESHOLD   = 45       # Branch A: F&G 7d MA below this
V4_ADX_THRESHOLD  = 29.0     # Branch B: ADX 14d below this
V4_RVOL_THRESHOLD = 32.6     # Branch B: rvol 20d below this (%)

# ══════════════════════════════════════════════════════════════════════
# CALENDAR WINDOWS  (exact observed failure regions)
# ══════════════════════════════════════════════════════════════════════

CALENDAR_WINDOWS = [
    ("2025-03-01", "2025-04-08"),
    ("2025-05-14", "2025-06-21"),
    ("2025-10-13", "2025-11-20"),
    ("2026-01-05", "2026-02-12"),
]

# ══════════════════════════════════════════════════════════════════════
# LIVE DATA FETCH - FUNDING RATE + FEAR & GREED
# ══════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════
# MARKET CAP DIAGNOSTIC  (consolidated from check_mcap1–4)
# ══════════════════════════════════════════════════════════════════════

def _mcap_build_freq_csv_path(
    base_dir: Path,
    leaderboard_index: int,
    freq_width: int,
    mode: str,
    min_mcap: float,) -> Path:
    """
    Derive the freq_price CSV path from overlap_analysis.py run parameters.
    Mirrors the exact naming formula in overlap_analysis.py::run():

        freq_label      = f"{idx_label}_w{freq_width}_{mode_label}_{mcap_label}"
        price_freq_path = BASE_DIR / f"freq_price_{freq_label}.csv"

    This is the single source of truth for that formula inside audit.py.
    """
    idx_label  = f"top{leaderboard_index}"
    mcap_label = f"{int(min_mcap / 1_000_000)}M"
    mode_label = "snapshot_0600" if mode == "snapshot" else "freq_0000_0600"
    freq_label = f"{idx_label}_w{freq_width}_{mode_label}_{mcap_label}"
    return base_dir / f"freq_price_{freq_label}.csv"


def _mcap_load_freq(freq_path: Path):
    """Load freq CSV → tidy long DataFrame with (date, symbol) rows."""
    import pandas as _pd
    freq = _pd.read_csv(freq_path)
    sym_cols = [c for c in freq.columns if c.startswith("R") and not c.endswith("_count")]
    if not sym_cols:
        raise ValueError(f"No R-columns in {freq_path.name}. Expected R1, R2, … RN.")
    date_col = "date" if "date" in freq.columns else freq.columns[0]
    long = (
        freq[[date_col] + sym_cols]
        .melt(id_vars=date_col, value_name="symbol")
        .dropna(subset=["symbol"])
    )
    long["symbol"] = long["symbol"].astype(str).str.upper().str.strip()
    long = long[long["symbol"].notna() & (long["symbol"] != "NAN") & (long["symbol"] != "")]
    long["date"] = _pd.to_datetime(long[date_col]).dt.tz_localize(None)
    return long[["date", "symbol"]]


def _mcap_load_deploys(deploys_path: Path):
    """Load deploys CSV → tidy long DataFrame with (date, symbol) rows.
    Only includes symbols actually deployed each day (not the full candidate pool).
    This is the correct input for the mcap diagnostic when using a portfolio matrix.
    """
    import pandas as _pd
    dep = _pd.read_csv(deploys_path)
    dep.columns = [c.strip() for c in dep.columns]
    ts_col  = "timestamp_utc" if "timestamp_utc" in dep.columns else dep.columns[0]
    sym_cols = [c for c in dep.columns if c.startswith("R") and c[1:].isdigit()]
    sym_cols = sorted(sym_cols, key=lambda c: int(c[1:]))
    long = (
        dep[[ts_col] + sym_cols]
        .melt(id_vars=ts_col, value_name="symbol")
        .dropna(subset=["symbol"])
    )
    long["symbol"] = long["symbol"].astype(str).str.upper().str.strip()
    long = long[long["symbol"].notna() & (long["symbol"] != "NAN") & (long["symbol"] != "")]
    long["date"] = _pd.to_datetime(long[ts_col]).dt.normalize().dt.tz_localize(None)
    return long[["date", "symbol"]]


def run_mcap_diagnostic(
    freq_path,
    mcap_path,
    min_mcap: float = 0.0,
    quiet: bool = False,
) -> dict:
    """
    Full market-cap diagnostic combining check_mcap1–5.

    Prints a formatted report and MCAP_STATS_* machine-readable lines
    (parsed downstream by generate_audit_report.js).

    Returns a dict of summary stats.
    """
    import pandas as _pd

    freq_path = Path(freq_path)
    mcap_path = Path(mcap_path)

    print(f"\n{'─'*70}")
    print(f"  MARKET CAP DIAGNOSTIC  (check_mcap1–5)")
    print(f"  Symbol src: {freq_path.name}")
    print(f"  Mcap file : {mcap_path.name}")
    if min_mcap > 0:
        print(f"  Min mcap  : ${min_mcap/1e6:.0f}M threshold")
    print(f"{'─'*70}\n")

    # ── Load ──────────────────────────────────────────────────────────
    # Use deploys loader if the path looks like a deploys file, else freq loader.
    # Deploys files have R-columns with integer suffixes and a timestamp_utc column.
    _dep_cols = None
    try:
        import pandas as _pd_peek
        _peek = _pd_peek.read_csv(freq_path, nrows=1)
        _ts_present = "timestamp_utc" in _peek.columns or (
            len(_peek.columns) > 0 and
            _pd_peek.to_datetime(_peek.iloc[:, 0], errors="coerce").notna().any()
        )
        _r_int_cols = [c for c in _peek.columns if c.startswith("R") and c[1:].isdigit()]
        _is_deploys = bool(_r_int_cols) and _ts_present
    except Exception:
        _is_deploys = False

    if _is_deploys:
        long = _mcap_load_deploys(freq_path)
        print(f"  [mcap] Loaded as deploys file: {len(long)} symbol-day rows "
              f"({long['date'].nunique()} days, {long['symbol'].nunique()} unique symbols)")
    else:
        long = _mcap_load_freq(freq_path)
        print(f"  [mcap] Loaded as freq CSV: {len(long)} symbol-day rows "
              f"({long['date'].nunique()} days, {long['symbol'].nunique()} unique symbols)")
    mc   = _pd.read_parquet(mcap_path)
    mc["date"]   = _pd.to_datetime(mc["date"]).dt.tz_localize(None)
    mc["symbol"] = mc["symbol"].astype(str).str.upper().str.strip()
    mc_lookup    = mc[["date", "symbol", "market_cap_usd"]].drop_duplicates(
                       subset=["date", "symbol"])

    # ── 1. Symbol-level coverage ─────────────────────────────────────
    all_syms       = set(long["symbol"].unique())
    mc_syms        = set(mc["symbol"].unique())
    matched_syms   = all_syms & mc_syms
    unmatched_syms = all_syms - mc_syms
    pct_sym        = 100 * len(matched_syms) / len(all_syms) if all_syms else 0.0

    print(f"── 1. Symbol-level coverage ──────────────────────────────────────────")
    print(f"  Unique symbols in freq CSV  : {len(all_syms)}")
    print(f"  Matched in mcap parquet     : {len(matched_syms)}  ({pct_sym:.1f}%)")
    print(f"  Unmatched (no mcap data)    : {len(unmatched_syms)}  ({100-pct_sym:.1f}%)")
    if unmatched_syms:
        print(f"  Unmatched symbols: {sorted(unmatched_syms)}")

    # ── 2. Row-level match rate ───────────────────────────────────────
    merged  = long.merge(mc_lookup, on=["date", "symbol"], how="left")
    total   = len(merged)
    matched = int(merged["market_cap_usd"].notna().sum())
    missing = total - matched
    pct_row = 100 * matched / total if total else 0.0

    print(f"\n── 2. Row-level match rate (date × symbol) ───────────────────────────")
    print(f"  Total rows   : {total:,}")
    print(f"  Matched rows : {matched:,}  ({pct_row:.1f}%)")
    print(f"  Missing mcap : {missing:,}  ({100-pct_row:.1f}%)")

    if missing > 0:
        missing_rows = merged[merged["market_cap_usd"].isna()]
        # Per-symbol: how many days missing + which dates
        miss_by_sym = (
            missing_rows.groupby("symbol")["date"]
            .agg(n_missing="count", dates=lambda d: sorted(d.dt.date.tolist()))
            .reset_index()
            .sort_values("n_missing", ascending=False)
        )
        print(f"\n── 2b. Missing mcap breakdown by symbol ──────────────────────────────")
        print(f"  {'Symbol':<20}  {'Missing days':>12}  Dates")
        print(f"  {'─'*20}  {'─'*12}  {'─'*40}")
        for _, row in miss_by_sym.iterrows():
            dates_str = ", ".join(str(d) for d in row["dates"][:5])
            if len(row["dates"]) > 5:
                dates_str += f" … +{len(row['dates'])-5} more"
            print(f"  {row['symbol']:<20}  {row['n_missing']:>12}  {dates_str}")

    # ── 3. Per-day average mcap ───────────────────────────────────────
    daily = (
        merged.groupby("date")["market_cap_usd"]
        .agg(mean="mean", median="median", count="count",
             matched_n=lambda s: s.notna().sum())
        .reset_index()
    )
    daily["mean_M"]    = daily["mean"]   / 1e6
    daily["median_M"]  = daily["median"] / 1e6
    daily["missing_n"] = daily["count"]  - daily["matched_n"]

    if not quiet:
        print(f"\n── 3. Per-day average market cap ─────────────────────────────────────")
        print(f"  {'Date':<12}  {'Mean ($M)':>10}  {'Median ($M)':>12}  "
              f"{'Matched':>8}  {'Missing':>8}")
        print(f"  {'─'*12}  {'─'*10}  {'─'*12}  {'─'*8}  {'─'*8}")
        for _, row in daily.iterrows():
            ms = f"${row['mean_M']:.1f}M"   if _pd.notna(row["mean"])   else "N/A"
            md = f"${row['median_M']:.1f}M" if _pd.notna(row["median"]) else "N/A"
            print(f"  {str(row['date'].date()):<12}  {ms:>10}  {md:>12}  "
                  f"{int(row['matched_n']):>8}  {int(row['missing_n']):>8}")

    # ── 4. Overall summary ────────────────────────────────────────────
    matched_df     = merged.dropna(subset=["market_cap_usd"])
    overall_mean_M = matched_df["market_cap_usd"].mean()   / 1e6 if len(matched_df) else float("nan")
    overall_med_M  = matched_df["market_cap_usd"].median() / 1e6 if len(matched_df) else float("nan")
    daily_avg_M    = daily["mean_M"].mean()

    print(f"\n── 4. Summary ────────────────────────────────────────────────────────")
    print(f"  Symbol coverage      : {pct_sym:.1f}%  ({len(matched_syms)}/{len(all_syms)} symbols)")
    print(f"  Row match rate       : {pct_row:.1f}%  ({matched:,}/{total:,} rows)")
    print(f"  Mean mcap (matched)  : ${overall_mean_M:.1f}M")
    print(f"  Median mcap (matched): ${overall_med_M:.1f}M")
    print(f"  Daily-mean avg       : ${daily_avg_M:.1f}M")

    # ── 5. Outlier day analysis (check_mcap5) ────────────────────────
    daily_m = matched_df.groupby("date")["market_cap_usd"].agg(
        mean_mcap="mean", median_mcap="median", n_matched="count"
    ).reset_index()
    daily_m["mean_M"]   = daily_m["mean_mcap"]   / 1e6
    daily_m["median_M"] = daily_m["median_mcap"] / 1e6

    print(f"\n── 5. Outlier day analysis ───────────────────────────────────────────")
    print(f"  Mean of daily medians  : ${daily_m['median_M'].mean():.1f}M")
    print(f"  Median of daily medians: ${daily_m['median_M'].median():.1f}M")
    print(f"  Mean of daily means    : ${daily_m['mean_M'].mean():.1f}M")

    top10 = daily_m.nlargest(10, "mean_M")[["date", "mean_M", "median_M", "n_matched"]]
    if not top10.empty:
        print(f"\n  Top {len(top10)} outlier days (by mean mcap):")
        print(f"  {'Date':<12}  {'Mean ($M)':>10}  {'Median ($M)':>12}  {'Symbols':>8}")
        print(f"  {'─'*12}  {'─'*10}  {'─'*12}  {'─'*8}")
        for _, row in top10.iterrows():
            print(f"  {str(row['date'].date()):<12}  "
                  f"${row['mean_M']:>8.1f}M  "
                  f"${row['median_M']:>10.1f}M  "
                  f"{int(row['n_matched']):>8}")
        if not quiet:
            print(f"\n  Symbols on outlier days (sorted by mcap desc):")
            for _, row in top10.iterrows():
                day_syms = (
                    matched_df[matched_df["date"] == row["date"]]
                    [["symbol", "market_cap_usd"]].copy()
                )
                day_syms["mcap_M"] = day_syms["market_cap_usd"] / 1e6
                day_syms = day_syms.sort_values("mcap_M", ascending=False)
                syms_str = ", ".join(
                    f"{r['symbol']}(${r['mcap_M']:.0f}M)" for _, r in day_syms.iterrows()
                )
                print(f"    {str(row['date'].date())}: {syms_str}")

    # ── 6. Eligibility at threshold ───────────────────────────────────
    above_pct = None
    if min_mcap > 0 and len(matched_df):
        n_above   = int((matched_df["market_cap_usd"] >= min_mcap).sum())
        above_pct = 100 * n_above / len(matched_df)
        print(f"\n── 6. Eligibility at ${min_mcap/1e6:.0f}M threshold ─────────────────────────")
        print(f"  Rows above threshold : {n_above:,} / {len(matched_df):,}  ({above_pct:.1f}%)")

    # ── 7. Machine-readable lines (parsed by generate_audit_report.js) ─
    print(f"\n── Market cap stats (machine-readable) ───────────────────────────────")
    print(f"MCAP_STATS_SYMBOL_COVERAGE: {pct_sym:.1f}%")
    print(f"MCAP_STATS_ROW_MATCH_RATE:  {pct_row:.1f}%")
    print(f"MCAP_STATS_MEAN_MCAP:       ${overall_mean_M:.1f}M")
    print(f"MCAP_STATS_MEDIAN_MCAP:     ${overall_med_M:.1f}M")
    print(f"MCAP_STATS_MIN_FILTER:      ${min_mcap/1e6:.0f}M")
    print(f"MCAP_STATS_TOTAL_ROWS:      {total}")
    print(f"MCAP_STATS_MISSING_ROWS:    {missing}")
    if above_pct is not None:
        print(f"MCAP_STATS_ABOVE_THRESHOLD: {above_pct:.1f}%")
    print(f"{'─'*70}\n")

    return {
        "symbol_coverage_pct":  pct_sym,
        "row_match_rate_pct":   pct_row,
        "mean_mcap_M":          overall_mean_M,
        "median_mcap_M":        overall_med_M,
        "daily_avg_M":          daily_avg_M,
        "total_rows":           total,
        "missing_rows":         missing,
        "matched_symbols":      len(matched_syms),
        "unmatched_symbols":    sorted(unmatched_syms),
        "above_threshold_pct":  above_pct,
    }

def _to_ms(date_str: str) -> int:
    return int(datetime.datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)


def fetch_funding_rate(start: str = "2025-01-01",
                       end:   str = "2026-03-01") -> pd.Series:
    print("  Fetching BTC funding rate (Binance) ...")
    BIN = "https://fapi.binance.com"
    rows = []
    start_ms = _to_ms(start)
    end_ms   = _to_ms(end)
    cur = start_ms
    while cur < end_ms:
        try:
            r = requests.get(
                f"{BIN}/fapi/v1/fundingRate",
                params={"symbol": "BTCUSDT", "startTime": cur,
                        "endTime": end_ms, "limit": 1000},
                timeout=15)
            if r.status_code != 200:
                print(f"    ! HTTP {r.status_code}")
                break
            batch = r.json()
            if not batch:
                break
            rows.extend(batch)
            cur = batch[-1]["fundingTime"] + 1
            if len(batch) < 1000:
                break
            time.sleep(0.2)
        except Exception as e:
            print(f"    ! {e}")
            break

    if not rows:
        print("    -> EMPTY - Binance unavailable (VPN?)")
        return pd.Series(dtype=float)

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["fundingTime"], unit="ms").dt.normalize()
    df["rate"] = pd.to_numeric(df["fundingRate"]) * 100
    out = df.groupby("date")["rate"].mean()
    out = out[(out.index >= start) & (out.index <= end)]
    print(f"    -> {len(out)} days")
    return out


def fetch_fear_greed(start: str = "2025-01-01",end:   str = "2026-03-01") -> pd.Series:
    print("  Fetching Fear & Greed (alternative.me) ...")
    try:
        n = (datetime.datetime.strptime(end, "%Y-%m-%d") -
             datetime.datetime.strptime(start, "%Y-%m-%d")).days + 30
        r = requests.get(
            f"https://api.alternative.me/fng/?limit={n}&format=json&date_format=us",
            timeout=15)
        df = pd.DataFrame(r.json()["data"])
        df["date"]  = pd.to_datetime(df["timestamp"], format="%m-%d-%Y")
        df["value"] = pd.to_numeric(df["value"])
        out = df.set_index("date")["value"].sort_index()
        out = out[(out.index >= start) & (out.index <= end)]
        print(f"    -> {len(out)} days")
        return out
    except Exception as e:
        print(f"    ! {e}")
        return pd.Series(dtype=float)


def build_v3_filter(fr: pd.Series, fg: pd.Series) -> pd.Series:
    """
    Returns a pd.Series indexed by date with True = SIT FLAT (bad regime).
    Condition: fr_raw < V3_FR_THRESHOLD AND fg_7d_ma < V3_FG_THRESHOLD
    """
    idx = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    fr_r = fr.reindex(idx)
    fg_r = fg.reindex(idx)
    fg_7d = fg_r.rolling(7, min_periods=4).mean()
    bad = (fr_r < V3_FR_THRESHOLD) & (fg_7d < V3_FG_THRESHOLD)
    bad = bad.fillna(False)

    n_bad  = bad.sum()
    n_good = (~bad).sum()
    pct    = n_bad / (n_bad + n_good) * 100
    print(f"    V3 filter: {n_bad} days flagged ({pct:.1f}% of period)")

    # Per-window coverage
    for s, e in CALENDAR_WINDOWS:
        mask = (idx >= s) & (idx <= e)
        total   = mask.sum()
        flagged = bad[mask].sum()
        pct_w   = flagged / total * 100 if total > 0 else 0
        ok = "✅" if pct_w >= 80 else ("⚠️ " if pct_w >= 50 else "❌")
        print(f"      {s[:10]} -> {e[:10]}: {flagged}/{total} ({pct_w:.0f}%)  {ok}")

    return bad


def build_v4_filter(fr: pd.Series, fg: pd.Series, btc_ohlcv: pd.DataFrame) -> pd.Series:
    """
    Two-branch causal filter (F1=0.688, beats calendar 0.683).

    Branch A (fear/liquidation - R1, R3, R4):
        fr_raw < V4_FR_THRESHOLD  AND  fg_7d_ma < V4_FG_THRESHOLD

    Branch B (low-vol bull grind - R2):
        adx_14d < V4_ADX_THRESHOLD  AND  rvol_20d < V4_RVOL_THRESHOLD

    Sit flat if Branch A OR Branch B fires.
    """
    idx = pd.date_range("2025-01-01", "2026-03-01", freq="D")

    # Branch A
    fr_r  = fr.reindex(idx)
    fg_r  = fg.reindex(idx)
    fg_7d = fg_r.rolling(7, min_periods=4).mean()
    branch_a = (fr_r < V4_FR_THRESHOLD) & (fg_7d < V4_FG_THRESHOLD)

    # Branch B - compute ADX and rvol from OHLCV
    ohlcv = btc_ohlcv.copy()

    # ADX 14d (proper DM-based)
    up   = ohlcv["high"].diff()
    dn   = -ohlcv["low"].diff()
    pdm  = up.where((up > dn) & (up > 0), 0.0)
    ndm  = dn.where((dn > up) & (dn > 0), 0.0)
    tr   = pd.concat([
        ohlcv["high"] - ohlcv["low"],
        (ohlcv["high"] - ohlcv["close"].shift(1)).abs(),
        (ohlcv["low"]  - ohlcv["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr14 = tr.rolling(14, min_periods=5).mean()
    pdi   = 100 * pdm.rolling(14, min_periods=5).mean() / atr14.replace(0, np.nan)
    ndi   = 100 * ndm.rolling(14, min_periods=5).mean() / atr14.replace(0, np.nan)
    dx    = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, np.nan)
    adx   = dx.rolling(14, min_periods=5).mean()

    # rvol 20d (annualised daily log-return std)
    lr    = np.log(ohlcv["close"] / ohlcv["close"].shift(1))
    rvol  = lr.rolling(20, min_periods=10).std() * np.sqrt(ANNUALIZATION_FACTOR) * 100

    adx_d  = adx.reindex(idx)
    rvol_d = rvol.reindex(idx)
    branch_b = (adx_d < V4_ADX_THRESHOLD) & (rvol_d < V4_RVOL_THRESHOLD)

    bad = (branch_a | branch_b).fillna(False)

    n_bad = bad.sum()
    pct   = n_bad / len(bad) * 100
    print(f"    V4 filter: {n_bad} days flagged ({pct:.1f}% of period)")
    a_days = branch_a.fillna(False).sum()
    b_days = branch_b.fillna(False).sum()
    print(f"      Branch A (FR+F&G):  {a_days} days")
    print(f"      Branch B (ADX+rvol): {b_days} days")

    for s, e in CALENDAR_WINDOWS:
        mask    = (idx >= s) & (idx <= e)
        total   = mask.sum()
        flagged = bad[mask].sum()
        pct_w   = flagged / total * 100 if total > 0 else 0
        ok = "✅" if pct_w >= 80 else ("⚠️ " if pct_w >= 50 else "❌")
        print(f"      {s[:10]} -> {e[:10]}: {flagged}/{total} ({pct_w:.0f}%)  {ok}")

    return bad


# ══════════════════════════════════════════════════════════════════════
# V5 ML ENSEMBLE FILTER
# ══════════════════════════════════════════════════════════════════════

def build_v5_ml_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """
    Walk-forward ML ensemble regime filter (2-state: good / bad).

    Three models trained on first 60% of available days, predicting the
    remaining 40%:
      - HMM  (hmmlearn GaussianHMM, 2 states, unsupervised)
      - K-Means (sklearn, 2 clusters, unsupervised)
      - Random Forest (sklearn, supervised - calendar windows = bad label)

    Sit flat when >= 2 of 3 models agree the day is a bad regime.

    Bias guards:
      - All features are lagged 1 day (yesterday's signal gates today)
      - Walk-forward split: models never see future returns during training
      - Random Forest labels are calendar windows only (no strategy returns)

    Features (all lagged 1 day):
      ret_1d, ret_5d, ret_10d, ret_20d   - log returns over N days
      ma20_dist, ma50_dist               - close vs 20d/50d MA (%)
      mom_20d                            - 20d price momentum
      rvol_10d, rvol_20d                 - rolling realised volatility
      rsi_14                             - 14-day RSI
    """
    if not SKLEARN_AVAILABLE:
        raise ImportError("scikit-learn not installed — pip install scikit-learn")
    # HMM_AVAILABLE is set at module level

    idx     = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    # ── Build features (shared helper, lagged 1 day) ──────────────────
    feats_r = _price_features(btc_ohlcv, idx)
    # ── Calendar labels for Random Forest ────────────────────────────
    cal_bad = _cal_bad_series(idx)

    # ── Walk-forward split ────────────────────────────────────────────
    valid_mask = feats_r.notna().all(axis=1)
    valid_idx  = idx[valid_mask]
    n          = len(valid_idx)
    split      = int(n * TRAIN_TEST_SPLIT_RATIO)
    train_idx  = valid_idx[:split]
    pred_idx   = valid_idx[split:]

    print(f"    V5 walk-forward: {len(train_idx)} train days / "
          f"{len(pred_idx)} predict days  "
          f"(split at {train_idx[-1].date()} -> {pred_idx[0].date()})")

    X_train = feats_r.loc[train_idx].values
    X_pred  = feats_r.loc[pred_idx].values
    y_train_cal = cal_bad.loc[train_idx].astype(int).values

    scaler  = StandardScaler().fit(X_train)
    Xs_train = scaler.transform(X_train)
    Xs_pred  = scaler.transform(X_pred)

    predictions = {}   # model_name -> pd.Series(bool, index=pred_idx)

    # ── Model 1: HMM ──────────────────────────────────────────────────
    if HMM_AVAILABLE:
        try:
            hmm = GaussianHMM(n_components=2, covariance_type="full",
                              n_iter=100, random_state=42)
            hmm.fit(Xs_train)
            states_pred = hmm.predict(Xs_pred)
            # Map state with lower mean ret_1d feature to "bad"
            state_means = {s: Xs_pred[states_pred == s, 0].mean()
                           for s in [0, 1]}
            bad_state = min(state_means, key=state_means.get)
            hmm_bad = pd.Series(states_pred == bad_state, index=pred_idx)
            predictions["HMM"] = hmm_bad
            print(f"      HMM: bad_state={bad_state}  "
                  f"flagged={hmm_bad.sum()} days "
                  f"({hmm_bad.mean()*100:.1f}%)")
        except Exception as e:
            print(f"      HMM failed: {e}")

    # ── Model 2: K-Means ──────────────────────────────────────────────
    try:
        km = KMeans(n_clusters=2, random_state=42, n_init=20)
        km.fit(Xs_train)
        clusters_pred = km.predict(Xs_pred)
        # Map cluster with lower mean ret_1d to "bad"
        cluster_means = {c: Xs_pred[clusters_pred == c, 0].mean()
                         for c in [0, 1]}
        bad_cluster = min(cluster_means, key=cluster_means.get)
        km_bad = pd.Series(clusters_pred == bad_cluster, index=pred_idx)
        predictions["KMeans"] = km_bad
        print(f"      K-Means: bad_cluster={bad_cluster}  "
              f"flagged={km_bad.sum()} days "
              f"({km_bad.mean()*100:.1f}%)")
    except Exception as e:
        print(f"      K-Means failed: {e}")

    # ── Model 3: Random Forest ────────────────────────────────────────
    try:
        rf = RandomForestClassifier(n_estimators=200, max_depth=4,
                                    random_state=42, class_weight="balanced")
        rf.fit(Xs_train, y_train_cal)
        rf_pred = rf.predict(Xs_pred)
        rf_bad  = pd.Series(rf_pred.astype(bool), index=pred_idx)
        predictions["RF"] = rf_bad
        # Feature importances
        feat_names = list(feats.columns)
        imps = sorted(zip(feat_names, rf.feature_importances_),
                      key=lambda x: -x[1])
        top3 = ", ".join(f"{n}={v:.3f}" for n, v in imps[:3])
        print(f"      RF: flagged={rf_bad.sum()} days "
              f"({rf_bad.mean()*100:.1f}%)  top features: {top3}")
    except Exception as e:
        print(f"      RF failed: {e}")

    if not predictions:
        print("    ! All V5 models failed - falling back to calendar filter")
        return cal_bad

    # ── Ensemble: majority vote ───────────────────────────────────────
    votes = pd.DataFrame(predictions).astype(int)
    majority = votes.sum(axis=1) >= 2   # >=2 of N models agree
    ensemble_bad_pred = majority         # bool Series over pred_idx only

    # Print per-model F1 vs calendar labels on the prediction window
    cal_pred_labels = cal_bad.loc[pred_idx].astype(int)
    print(f"\n      Ensemble performance on prediction window "
          f"({pred_idx[0].date()} -> {pred_idx[-1].date()}):")
    for name, preds in predictions.items():
        f1 = f1_score(cal_pred_labels, preds.astype(int), zero_division=0)
        print(f"        {name:<8} F1={f1:.3f}  "
              f"flagged={preds.sum()} / {len(preds)} days")
    ens_f1 = f1_score(cal_pred_labels, majority.astype(int), zero_division=0)
    print(f"        Ensemble F1={ens_f1:.3f}  "
          f"flagged={majority.sum()} / {len(majority)} days")

    # ── Assemble full-index filter ────────────────────────────────────
    # Training window: use calendar labels directly (we have no OOS predictions)
    # Prediction window: use ensemble majority vote
    # Outside both windows: not flagged
    bad_full = pd.Series(False, index=idx)
    bad_full.loc[train_idx] = cal_bad.loc[train_idx].values
    bad_full.loc[pred_idx]  = ensemble_bad_pred.values

    n_bad = bad_full.sum()
    print(f"\n    V5 filter: {n_bad} days flagged total ({n_bad/len(idx)*100:.1f}%)")
    print(f"      Training window (calendar): {cal_bad.loc[train_idx].sum()} days")
    print(f"      Prediction window (ensemble): {majority.sum()} days")

    # Per-calendar-window coverage check
    for s, e in CALENDAR_WINDOWS:
        mask    = (idx >= s) & (idx <= e)
        total   = mask.sum()
        flagged = bad_full[mask].sum()
        pct_w   = flagged / total * 100 if total > 0 else 0
        ok = "✅" if pct_w >= 80 else ("⚠️ " if pct_w >= 50 else "❌")
        print(f"      {s[:10]} -> {e[:10]}: {flagged}/{total} ({pct_w:.0f}%)  {ok}")

    return bad_full


# ══════════════════════════════════════════════════════════════════════
# HMM DIAGNOSTICS STORE
# Populated during build_v5*_filter_majority; consumed by
# print_comparison_table.  Returns are injected after trading sim via
# _hmm_diag_inject_returns().
# ══════════════════════════════════════════════════════════════════════

_HMM_DIAG: Dict[str, dict] = {}   # key = "V5-B Majority" / "V5-D Majority"


def _hmm_diag_build(
    hmm,                           # already-fitted GaussianHMM (seed 0)
    X_train:     np.ndarray,       # scaled train features
    X_pred:      np.ndarray,       # scaled pred features (never seen during fit)
    dates_train: pd.DatetimeIndex,
    dates_pred:  pd.DatetimeIndex,
    n_states:    int,
    bad_states:  set,              # state indices labelled risk-off
    label: str,                    # "V5-B Majority" / "V5-D Majority"
) -> None:
    """
    Compute state occupancy, transition stability and per-state feature
    stats immediately after the filter is built (no daily returns yet).
    Stores results in _HMM_DIAG[label].  Returns are patched in later
    by _hmm_diag_inject_returns() once the trading sim has run.
    """
    train_seq = hmm.predict(X_train)
    pred_seq  = hmm.predict(X_pred)
    all_seq   = np.concatenate([train_seq, pred_seq])
    dates_all = pd.DatetimeIndex(list(dates_train) + list(dates_pred))
    total     = len(all_seq)

    states: Dict[int, dict] = {}
    for s in range(n_states):
        tr_occ = int((train_seq == s).sum())
        pr_occ = int((pred_seq  == s).sum())
        occ_pct = (tr_occ + pr_occ) / total * 100

        # Avg run-length (transition stability)
        runs, cur = [], 1
        for i in range(1, len(all_seq)):
            if all_seq[i] == all_seq[i - 1]:
                cur += 1
            else:
                if all_seq[i - 1] == s:
                    runs.append(cur)
                cur = 1
        if all_seq[-1] == s:
            runs.append(cur)
        avg_dur = float(np.mean(runs)) if runs else float("nan")

        states[s] = dict(
            occ_pct  = occ_pct,
            tr_occ   = tr_occ,
            pr_occ   = pr_occ,
            avg_dur  = avg_dur,
            role     = "RISK-OFF" if s in bad_states else "risk-on",
            # return stats filled later
            mean_ret = float("nan"),
            vol      = float("nan"),
            winrate  = float("nan"),
            mean_dd  = float("nan"),
        )

    _HMM_DIAG[label] = dict(
        label       = label,
        n_states    = n_states,
        bad_states  = bad_states,
        states      = states,
        dates_train = dates_train,
        dates_pred  = dates_pred,
        all_seq     = all_seq,
        dates_all   = dates_all,
    )


def _hmm_diag_inject_returns(label: str, daily_returns: pd.Series) -> None:
    """
    Called after the trading simulation for `label` has run.
    Patches per-state conditional return stats into _HMM_DIAG[label].
    daily_returns must be a pd.Series indexed by date (NaN on flat days OK).
    """
    if label not in _HMM_DIAG:
        return
    d         = _HMM_DIAG[label]
    all_seq   = d["all_seq"]
    dates_all = d["dates_all"]

    for s, info in d["states"].items():
        state_dates = dates_all[all_seq == s]
        common      = daily_returns.index.intersection(state_dates)
        rets        = daily_returns.loc[common].dropna()
        if len(rets) > 1:
            info["mean_ret"] = float(rets.mean() * 100)
            info["vol"]      = float(rets.std()  * 100)
            info["winrate"]  = float((rets > 0).mean() * 100)
            # mean single-period drawdown (max loss per day, only down days)
            neg = rets[rets < 0]
            info["mean_dd"]  = float(neg.mean() * 100) if len(neg) else 0.0


def _hmm_diag_print(label: str) -> None:
    """
    Print the full state diagnostic block for one HMM filter.
    Called inside print_comparison_table after all results are assembled.
    """
    if label not in _HMM_DIAG:
        print(f"  [no HMM diagnostics for {label}]")
        return

    d          = _HMM_DIAG[label]
    n_states   = d["n_states"]
    bad_states = d["bad_states"]
    states     = d["states"]

    W = 110
    SEP2 = "─" * W

    print(f"\n  \u250c{'\u2500' * (W - 2)}\u2510")
    print(f"  │  HMM STATE DIAGNOSTICS - {label:<{W - 32}}│")
    print(f"  ├{'─' * (W - 2)}┤")

    # ── Block A: Occupancy + transition stability ────────────────────
    print(f"  │  {'Block A - Occupancy & Transition Stability':<{W - 4}}│")
    hdr = (f"  │    {'State':<8}  {'Role':<10}  {'Train d':>8}  {'Test d':>7}"
           f"  {'Occ %':>7}  {'AvgDur (d)':>11}  {'Stability':>11}  {'Flag':<14}│")
    print(hdr)
    print(f"  │    {'─'*8}  {'─'*10}  {'─'*8}  {'─'*7}  {'─'*7}  {'─'*11}  {'─'*11}  {'─'*14}│")

    for s in range(n_states):
        info     = states[s]
        role     = info["role"]
        occ      = info["occ_pct"]
        dur      = info["avg_dur"]
        dur_str  = f"{dur:.1f}d" if not math.isnan(dur) else " n/a"

        if dur < 2.0:
            stab = "⚠️  NOISY"
        elif dur < 5.0:
            stab = "↕  SHORT"
        elif dur < 15.0:
            stab = "✓  OK"
        else:
            stab = "✅ STABLE"

        flag = ""
        if s in bad_states:
            if occ < 5:
                flag = "⚠️  THIN (<5%)"
            elif occ > 60:
                flag = "⚠️  DOMINANT"

        print(f"  │    S{s:<7}  {role:<10}  {info['tr_occ']:>8d}  {info['pr_occ']:>7d}"
              f"  {occ:>6.1f}%  {dur_str:>11}  {stab:<11}  {flag:<14}│")

    # ── Block B: Conditional return stats ───────────────────────────
    print(f"  ├{'─' * (W - 2)}┤")
    print(f"  │  {'Block B - Conditional Return Stats (active trading days only)':<{W - 4}}│")
    hdr2 = (f"  │    {'State':<8}  {'Role':<10}  {'MeanRet%':>9}  {'Vol%':>8}"
            f"  {'WinRate%':>9}  {'MeanDD%':>8}  {'Signal?':>10}  {'':>14}│")
    print(hdr2)
    print(f"  │    {'─'*8}  {'─'*10}  {'─'*9}  {'─'*8}  {'─'*9}  {'─'*8}  {'─'*10}  {'─'*14}│")

    for s in range(n_states):
        info     = states[s]
        mr       = info["mean_ret"]
        vol      = info["vol"]
        wr       = info["winrate"]
        mdd      = info["mean_dd"]
        no_ret   = math.isnan(mr)

        mr_s  = f"{mr:+.2f}%" if not math.isnan(mr)  else "  n/a"
        v_s   = f"{vol:.2f}%" if not math.isnan(vol)  else "  n/a"
        wr_s  = f"{wr:.1f}%"  if not math.isnan(wr)   else "  n/a"
        mdd_s = f"{mdd:.2f}%" if not math.isnan(mdd)  else "  n/a"

        # Signal quality: risk-off should have negative mean ret & low winrate
        if s in bad_states and not no_ret:
            if mr < 0 and wr < 45:
                sig = "✅ VALID"
            elif mr < 0 or wr < 45:
                sig = "↕  WEAK"
            else:
                sig = "❌ INVALID"
        elif s not in bad_states and not no_ret:
            sig = "✅ OK" if mr > 0 else "↕  CHECK"
        else:
            sig = " n/a"

        print(f"  │    S{s:<7}  {info['role']:<10}  {mr_s:>9}  {v_s:>8}"
              f"  {wr_s:>9}  {mdd_s:>8}  {sig:>10}  {'':>14}│")

    # ── Block C: Summary verdict ────────────────────────────────────
    print(f"  ├{'─' * (W - 2)}┤")
    print(f"  │  {'Block C - Filter Quality Verdict':<{W - 4}}│")

    issues = []
    for s in bad_states:
        info = states.get(s, {})
        if info.get("occ_pct", 100) < 5:
            issues.append(f"S{s} occupies only {info['occ_pct']:.1f}% of days (thin -> overfit risk)")
        if not math.isnan(info.get("avg_dur", float("nan"))) and info["avg_dur"] < 2:
            issues.append(f"S{s} avg duration {info['avg_dur']:.1f}d (noise-level gating)")
        mr = info.get("mean_ret", float("nan"))
        wr = info.get("winrate", float("nan"))
        if not math.isnan(mr) and mr > 0:
            issues.append(f"S{s} RISK-OFF has positive mean return {mr:+.2f}% (filter may be inverted)")
        if not math.isnan(wr) and wr > 50:
            issues.append(f"S{s} RISK-OFF has {wr:.0f}% win-rate (not structurally bad)")

    if issues:
        for issue in issues:
            print(f"  │    ⚠️   {issue:<{W - 10}}│")
    else:
        print(f"  │    ✅  All checks passed - states are structurally separated and stable│")
        pad = W - 74
        print(f"  │    {'':>{pad}}│")

    print(f"  └{'─' * (W - 2)}┘")




# ══════════════════════════════════════════════════════════════════════
# HMM HELPER - shared logic used by all V5 variants
# ══════════════════════════════════════════════════════════════════════

def _hmm_bad_series(
    X_train: np.ndarray,
    dates_train: pd.DatetimeIndex,
    n_states: int,
    bad_state_count: int,        # how many states to label "bad" (lowest conditional return)
    emission_col: int = 0,       # fallback column index if no strategy returns supplied
    random_state: int = 42,      # HMM random seed
    X_pred: np.ndarray   = None, # test-window features (never seen during fit)
    dates_pred: pd.DatetimeIndex = None,
    strategy_returns_train: pd.Series = None,  # actual strategy returns on train days
                                               # index=dates_train; used to rank states
                                               # by conditional mean return instead of
                                               # feature mean.  Prevents "inverted state"
                                               # problem when all states are profitable.
) -> pd.Series:
    """
    Fit a GaussianHMM on X_train ONLY.  State-to-bad mapping is derived
    exclusively from train-window statistics.

    State ranking (controls which states are labelled risk-off):
      - If strategy_returns_train is supplied: rank states by mean STRATEGY return
        conditioned on that state in the train window (lower = worse = risk-off).
        This anchors the label to actual P&L, not to an arbitrary feature column.
      - Fallback (no strategy returns): rank by mean of emission_col feature.

    This ensures zero lookahead: the HMM never sees test-window data during fit
    or during bad-state labelling.
    """
    try:
        hmm = GaussianHMM(n_components=n_states, covariance_type="full",
                          n_iter=200, random_state=random_state)
        # --- fit on TRAIN only ---
        hmm.fit(X_train)
        train_states = hmm.predict(X_train)

        # --- bad-state mapping from TRAIN statistics only ---
        if strategy_returns_train is not None:
            # Align strategy returns to dates_train index
            rets = strategy_returns_train.reindex(dates_train).fillna(0.0).values
            means = {s: rets[train_states == s].mean()
                     for s in range(n_states)
                     if (train_states == s).any()}
            criterion = "strategy conditional return"
        else:
            means = {s: X_train[train_states == s, emission_col].mean()
                     for s in range(n_states)
                     if (train_states == s).any()}
            criterion = f"feature col-{emission_col} mean"
        sorted_states = sorted(means, key=means.get)
        bad_states    = set(sorted_states[:bad_state_count])
        print(f"        State ranking by {criterion}: "
              + "  ".join(f"S{s}={means[s]:+.4f}" for s in sorted_states)
              + f"  ->  risk-off={bad_states}")

        if X_pred is not None and dates_pred is not None:
            # --- predict on TEST only, using train-derived bad_states ---
            pred_states = hmm.predict(X_pred)
            return pd.Series([s in bad_states for s in pred_states],
                             index=dates_pred)
        else:
            # Legacy: return train-window labels
            return pd.Series([s in bad_states for s in train_states],
                             index=dates_train)
    except Exception as e:
        print(f"        HMM failed: {e}")
        out_dates = dates_pred if (dates_pred is not None) else dates_train
        return pd.Series(False, index=out_dates)


def _expanding_hmm(
    feats_r: pd.DataFrame,
    idx: pd.DatetimeIndex,
    n_states: int,
    bad_state_count: int,
    min_train_days: int = 60,
    retrain_every: int  = 30,
    emission_col: int   = 0,
) -> pd.Series:
    """
    Expanding-window HMM: start with `min_train_days`, retrain every
    `retrain_every` days, always using ALL past data.  Only predicts on
    days after the first training window.
    """


    valid_mask = feats_r.notna().all(axis=1)
    valid_idx  = idx[valid_mask]
    n          = len(valid_idx)
    bad_full   = pd.Series(False, index=idx)

    pred_start = min_train_days
    t = pred_start
    while t < n:
        # Retrain on all data up to t
        train_idx_local = valid_idx[:t]
        X_train = feats_r.loc[train_idx_local].values
        scaler  = StandardScaler().fit(X_train)
        Xs      = scaler.transform(X_train)

        # Predict next `retrain_every` days (or to end)
        pred_end  = min(t + retrain_every, n)
        pred_dates = valid_idx[t:pred_end]
        X_pred    = feats_r.loc[pred_dates].values
        Xs_pred   = scaler.transform(X_pred)

        preds = _hmm_bad_series(
            X_train     = Xs,
            dates_train = train_idx_local,
            n_states    = n_states,
            bad_state_count = bad_state_count,
            emission_col    = emission_col,
            X_pred      = Xs_pred,
            dates_pred  = pred_dates,
        )
        bad_full.loc[pred_dates] = preds.values
        t = pred_end

    return bad_full


def _assemble_filter(bad_pred: pd.Series, train_idx: pd.DatetimeIndex,
                     cal_bad: pd.Series, idx: pd.DatetimeIndex,
                     label: str) -> pd.Series:
    """Combine calendar-hardcoded train labels + model predictions."""
    bad_full = pd.Series(False, index=idx)
    bad_full.loc[train_idx] = cal_bad.loc[train_idx].values
    bad_full.loc[bad_pred.index] = bad_pred.values
    n_bad = bad_full.sum()
    print(f"    {label}: {n_bad} days flagged ({n_bad/len(idx)*100:.1f}%)")
    for s, e in CALENDAR_WINDOWS:
        mask    = (idx >= s) & (idx <= e)
        total   = mask.sum()
        flagged = bad_full[mask].sum()
        pct_w   = flagged / total * 100 if total > 0 else 0
        ok = "✅" if pct_w >= 80 else ("⚠️ " if pct_w >= 50 else "❌")
        print(f"      {s[:10]} -> {e[:10]}: {flagged}/{total} ({pct_w:.0f}%)  {ok}")
    return bad_full


# ──────────────────────────────────────────────────────────────────────
# TAIL-EVENT GUARDRAIL - simple daily-OHLCV rules, no ML
# Sits out when either condition fires (OR logic):
#   1. Previous day BTC return < -DROP_PCT  (crash guard)
#   2. 5-day realized vol > VOL_MULT × 60-day baseline vol  (spike guard)
# Both conditions use lagged data - yesterday's signal gates today's trade.
# ──────────────────────────────────────────────────────────────────────

def build_tail_guardrail(
    btc_ohlcv: pd.DataFrame,
    drop_pct: float = 0.05,   # flag if prev-day return < -5%
    vol_mult: float = 2.0,    # flag if 5d rvol > 2.0× 60d baseline rvol
    short_window: int = 5,    # short rvol window (days)
    long_window:  int = 60,   # baseline rvol window (days)
) -> pd.Series:
    """
    Returns pd.Series[bool] index=daily DatetimeIndex, True = sit flat.
    All signals are lagged by 1 day to prevent lookahead.
    Calendar windows are applied independently in _assemble_filter;
    this filter adds ADDITIONAL protection against tail events.
    """
    idx   = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    close = btc_ohlcv["close"].sort_index()
    lr    = np.log(close / close.shift(1))

    # ── Signal 1: sharp single-day drop ───────────────────────────────
    # Lag by 1: yesterday's return gates today
    prev_ret   = lr.shift(1)
    crash_flag = (prev_ret < -drop_pct)

    # ── Signal 2: vol spike relative to rolling baseline ──────────────
    # Short-window realized vol (5d), lagged by 1
    rvol_short = lr.rolling(short_window, min_periods=3).std() * np.sqrt(ANNUALIZATION_FACTOR)
    # Long-window baseline vol (60d), lagged by 1
    rvol_long  = lr.rolling(long_window, min_periods=30).std() * np.sqrt(ANNUALIZATION_FACTOR)
    vol_ratio  = (rvol_short / rvol_long.replace(0, np.nan)).shift(1)
    spike_flag = (vol_ratio > vol_mult)

    # ── Combine (OR) and reindex to idx ───────────────────────────────
    bad_raw = (crash_flag | spike_flag).reindex(idx, fill_value=False).fillna(False)

    n_crash = crash_flag.reindex(idx, fill_value=False).sum()
    n_spike = spike_flag.reindex(idx, fill_value=False).sum()
    n_both  = (crash_flag & spike_flag).reindex(idx, fill_value=False).sum()
    n_total = bad_raw.sum()
    print(f"    Tail guardrail: drop<-{drop_pct*100:.0f}%={n_crash}d  "
          f"vol>{vol_mult:.1f}×baseline={n_spike}d  "
          f"both={n_both}d  total={n_total}d flagged")
    for s, e in CALENDAR_WINDOWS:
        mask    = (idx >= s) & (idx <= e)
        flagged = bad_raw[mask].sum()
        total   = mask.sum()
        print(f"      {s[:10]}->{e[:10]}: {flagged}/{total} guardrail-covered")

    return bad_raw


# ──────────────────────────────────────────────────────────────────────
# BTC TREND (MOVING AVERAGE) FILTER
# Sit out on day T when BTC close_{T-1} < SMA_N_{T-1}.
# One parameter, no ML, no cross-sectional data required.
# ──────────────────────────────────────────────────────────────────────

def build_btc_ma_filter(
    btc_ohlcv: pd.DataFrame,
    ma_days: int = 20,
) -> pd.Series:
    """
    Returns pd.Series[bool] index=daily DatetimeIndex, True = sit flat.

    Logic: sit out on day T if BTC's prior-day close was below its
    `ma_days`-day simple moving average on that prior day.
    Signal is lagged 1 day to prevent lookahead bias.
    """
    idx   = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    close = btc_ohlcv["close"].sort_index()
    sma   = close.rolling(ma_days, min_periods=max(3, ma_days // 2)).mean()

    # True on days where prior-day close < prior-day SMA (lagged 1)
    below_ma  = (close < sma).shift(1)
    bad_raw   = below_ma.reindex(idx, fill_value=False).fillna(False)

    n_flagged  = int(bad_raw.sum())
    n_total    = len(idx)
    pct        = 100 * n_flagged / n_total
    print(f"    BTC MA filter ({ma_days}d SMA): {n_flagged}/{n_total} days flagged ({pct:.1f}%)")

    # Coverage during known unstable fold (Aug 29 – Sep 27 2025)
    fold5_mask = (idx >= "2025-08-29") & (idx <= "2025-09-27")
    f5_flagged = int(bad_raw[fold5_mask].sum())
    f5_total   = int(fold5_mask.sum())
    print(f"      Fold 5 (Aug29-Sep27): {f5_flagged}/{f5_total} days flagged")

    return bad_raw


# ──────────────────────────────────────────────────────────────────────
# CROSS-SECTIONAL DISPERSION FILTER
# Hypothesis: momentum strategies require high cross-section dispersion.
# When all alts move together (low dispersion), the edge disappears.
# Signal: std(daily_return_i for i in top-30 perps) on each day.
# Sit out when this falls below a fraction of its rolling 20d median.
# ──────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────
# DISPERSION UNIVERSE — ranked by market cap (March 2026)
# Source: CoinMarketCap / Slickcharts, filtered for Binance USDT-M perps
# Stablecoins, exchange tokens, gold tokens, locked tokens excluded.
# ──────────────────────────────────────────────────────────────────────

# ── TOP-30  (market cap ranks #1–#53, perp-filtered) ─────────────────
DISPERSION_SYMBOLS_5 = [
    "BTCUSDT",    "ETHUSDT",    "BNBUSDT",     "XRPUSDT",    "SOLUSDT",
]


DISPERSION_SYMBOLS_10 = [
    "BTCUSDT",    "ETHUSDT",    "BNBUSDT",     "XRPUSDT",    "SOLUSDT",
    "TRXUSDT",    "DOGEUSDT",   "HYPEUSDT",    "BCHUSDT",    "ADAUSDT",
]

DISPERSION_SYMBOLS_20 = [
    "BTCUSDT",    "ETHUSDT",    "BNBUSDT",     "XRPUSDT",    "SOLUSDT",
    "TRXUSDT",    "DOGEUSDT",   "HYPEUSDT",    "BCHUSDT",    "ADAUSDT",
    "LINKUSDT",   "XLMUSDT",    "LTCUSDT",     "AVAXUSDT",   "HBARUSDT",
    "ZECUSDT",    "SUIUSDT",    "1000SHIBUSDT","TONUSDT",    "TAOUSDT",
]


DISPERSION_SYMBOLS_30 = [
    "BTCUSDT",    "ETHUSDT",    "BNBUSDT",     "XRPUSDT",    "SOLUSDT",
    "TRXUSDT",    "DOGEUSDT",   "HYPEUSDT",    "BCHUSDT",    "ADAUSDT",
    "LINKUSDT",   "XLMUSDT",    "LTCUSDT",     "AVAXUSDT",   "HBARUSDT",
    "ZECUSDT",    "SUIUSDT",    "1000SHIBUSDT","TONUSDT",    "TAOUSDT",
    "DOTUSDT",    "MNTUSDT",    "UNIUSDT",     "NEARUSDT",   "AAVEUSDT",
    "1000PEPEUSDT","ICPUSDT",   "ETCUSDT",     "ONDOUSDT",   "KASUSDT",
]

# ── ADDED to reach TOP-60  (market cap ranks #55–#84, perp-filtered) ──
_ADDED_60 = [
    "POLUSDT",    "WLDUSDT",    "QNTUSDT",     "ATOMUSDT",   "RENDERUSDT",
    "ENAUSDT",    "APTUSDT",    "FILUSDT",     "STXUSDT",    "ARBUSDT",
    "OPUSDT",     "INJUSDT",    "SEIUSDT",     "TIAUSDT",    "RUNEUSDT",
    "FETUSDT",    "LDOUSDT",    "JUPUSDT",     "WIFUSDT",    "GALAUSDT",
    "EGLDUSDT",   "VETUSDT",    "SONICUSDT",   "GRTUSDT",    "IMXUSDT",
    "ALGOUSDT",   "PYTHUSDT",   "SANDUSDT",    "THETAUSDT",  "MANAUSDT",
]

# ── ADDED to reach TOP-90  (market cap ranks #85–#119, perp-filtered) ─
_ADDED_90 = [
    "CHZUSDT",    "FLOWUSDT",   "1INCHUSDT",   "CRVUSDT",    "MKRUSDT",
    "SNXUSDT",    "1000FLOKIUSDT","GMXUSDT",   "COMPUSDT",   "ORDIUSDT",
    "LRCUSDT",    "1000BONKUSDT","APEUSDT",    "AXSUSDT",    "KAVAUSDT",
    "XTZUSDT",    "SUSHIUSDT",  "YFIUSDT",     "NOTUSDT",    "COTIUSDT",
    "OCEANUSDT",  "ANKRUSDT",   "BANDUSDT",    "MAGICUSDT",  "STRKUSDT",
    "DYDXUSDT",   "ENSUSDT",    "STGUSDT",     "CFXUSDT",    "BLURUSDT",
]

DISPERSION_SYMBOLS_60 = DISPERSION_SYMBOLS_30 + _ADDED_60
DISPERSION_SYMBOLS_90 = DISPERSION_SYMBOLS_60 + _ADDED_90
# ──────────────────────────────────────────────────────────────────────
# Convenience map — pass universe size as a parameter
# ──────────────────────────────────────────────────────────────────────
DISPERSION_UNIVERSE = {
    5: DISPERSION_SYMBOLS_5,
    10: DISPERSION_SYMBOLS_10,
    20: DISPERSION_SYMBOLS_20,
    30: DISPERSION_SYMBOLS_30,
    60: DISPERSION_SYMBOLS_60,
    90: DISPERSION_SYMBOLS_90,
}

if DISPERSION_UNIVERSE_SIZE not in DISPERSION_UNIVERSE:
    _fallback = max(DISPERSION_UNIVERSE.keys())
    print(f"[INFO] DISPERSION_UNIVERSE_SIZE={DISPERSION_UNIVERSE_SIZE} not in "
          f"{sorted(DISPERSION_UNIVERSE.keys())} — using {_fallback}")
    DISPERSION_UNIVERSE_SIZE = _fallback
DISPERSION_SYMBOLS = DISPERSION_UNIVERSE[DISPERSION_UNIVERSE_SIZE]
DISPERSION_CACHE_FILE = f"dispersion_cache_{DISPERSION_UNIVERSE_SIZE}.csv"

# DISPERSION_CACHE_FILE = "dispersion_cache.csv"

# ── CoinGecko IDs for the full dynamic candidate universe ─────────────
# Must be a superset of whatever top-N you intend to use.
# Only IDs that appear in COINGECKO_TO_BINANCE are fetched/used.
# Unused when DISPERSION_DYNAMIC_UNIVERSE=False.
DISPERSION_MCAP_SYMBOLS: List[str] = [
    "bitcoin",        "ethereum",       "binancecoin",     "ripple",
    "solana",         "tron",           "dogecoin",        "hyperliquid",
    "bitcoin-cash",   "cardano",        "chainlink",       "stellar",
    "litecoin",       "avalanche-2",    "hedera-hashgraph","zcash",
    "sui",            "shiba-inu",      "toncoin",         "bittensor",
    "polkadot",       "mantle-2",       "uniswap",         "near",
    "aave",           "pepe",           "internet-computer","ethereum-classic",
    "ondo-finance",   "kaspa",          "matic-network",   "worldcoin",
    "quant-network",  "cosmos",         "render-token",    "ethena",
    "aptos",          "filecoin",       "stacks",          "arbitrum",
    "optimism",       "injective-protocol","sei-network",  "celestia",
    "thorchain",      "fetch-ai",       "lido-dao",        "jupiter-exchange-solana",
    "dogwifcoin",     "gala",           "elrond-erd-2",    "vechain",
    "fantom",         "the-graph",      "immutable-x",     "algorand",
    "pyth-network",   "the-sandbox",    "theta-token",     "decentraland",
    "chiliz",         "flow",           "1inch",           "curve-dao-token",
    "maker",          "synthetix-network-token","floki",   "gmx",
    "compound-governance-token","ordi", "loopring",        "bonk",
    "apecoin",        "axie-infinity",  "kava",            "tezos",
    "sushi",          "yearn-finance",  "notcoin",         "coti",
    "ocean-protocol", "ankr",           "band-protocol",   "magic",
    "starknet",       "dydx",           "ethereum-name-service","stargate-finance",
    "conflux-token",  "blur",
]

# ── Authoritative CoinGecko ID → Binance USDT-M perp ticker mapping ───
# Only symbols in this dict will appear in the dynamic universe.
# Update if you add new candidates or Binance renames a ticker.
COINGECKO_TO_BINANCE: dict = {
    "bitcoin":                    "BTCUSDT",
    "ethereum":                   "ETHUSDT",
    "binancecoin":                "BNBUSDT",
    "ripple":                     "XRPUSDT",
    "solana":                     "SOLUSDT",
    "tron":                       "TRXUSDT",
    "dogecoin":                   "DOGEUSDT",
    "hyperliquid":                "HYPEUSDT",
    "bitcoin-cash":               "BCHUSDT",
    "cardano":                    "ADAUSDT",
    "chainlink":                  "LINKUSDT",
    "stellar":                    "XLMUSDT",
    "litecoin":                   "LTCUSDT",
    "avalanche-2":                "AVAXUSDT",
    "hedera-hashgraph":           "HBARUSDT",
    "zcash":                      "ZECUSDT",
    "sui":                        "SUIUSDT",
    "shiba-inu":                  "1000SHIBUSDT",
    "toncoin":                    "TONUSDT",
    "bittensor":                  "TAOUSDT",
    "polkadot":                   "DOTUSDT",
    "mantle-2":                   "MNTUSDT",        # "mantle" is ambiguous on CoinGecko
    "uniswap":                    "UNIUSDT",
    "near":                       "NEARUSDT",
    "aave":                       "AAVEUSDT",
    "pepe":                       "1000PEPEUSDT",
    "internet-computer":          "ICPUSDT",
    "ethereum-classic":           "ETCUSDT",
    "ondo-finance":               "ONDOUSDT",
    "kaspa":                      "KASUSDT",
    "matic-network":              "POLUSDT",        # POL (formerly MATIC)
    "worldcoin":                  "WLDUSDT",        # canonical CoinGecko ID
    "quant-network":              "QNTUSDT",
    "cosmos":                     "ATOMUSDT",
    "render-token":               "RENDERUSDT",
    "ethena":                     "ENAUSDT",
    "aptos":                      "APTUSDT",
    "filecoin":                   "FILUSDT",
    "stacks":                     "STXUSDT",        # Blockstack rebranded to Stacks 2021
    "arbitrum":                   "ARBUSDT",
    "optimism":                   "OPUSDT",
    "injective-protocol":         "INJUSDT",
    "sei-network":                "SEIUSDT",
    "celestia":                   "TIAUSDT",
    "thorchain":                  "RUNEUSDT",
    "fetch-ai":                   "FETUSDT",
    "lido-dao":                   "LDOUSDT",
    "jupiter-exchange-solana":    "JUPUSDT",
    "dogwifcoin":                 "WIFUSDT",
    "gala":                       "GALAUSDT",
    "elrond-erd-2":               "EGLDUSDT",
    "vechain":                    "VETUSDT",
    "fantom":                     "SONICUSDT",      # CoinGecko still uses "fantom" for Sonic
    "the-graph":                  "GRTUSDT",
    "immutable-x":                "IMXUSDT",
    "algorand":                   "ALGOUSDT",
    "pyth-network":               "PYTHUSDT",
    "the-sandbox":                "SANDUSDT",
    "theta-token":                "THETAUSDT",
    "decentraland":               "MANAUSDT",
    "chiliz":                     "CHZUSDT",
    "flow":                       "FLOWUSDT",
    "1inch":                      "1INCHUSDT",
    "curve-dao-token":            "CRVUSDT",
    "maker":                      "MKRUSDT",
    "synthetix-network-token":    "SNXUSDT",
    "floki":                      "1000FLOKIUSDT",
    "gmx":                        "GMXUSDT",
    "compound-governance-token":  "COMPUSDT",
    "ordi":                       "ORDIUSDT",       # BRC-20 token; "ordinals" is wrong
    "loopring":                   "LRCUSDT",
    "bonk":                       "1000BONKUSDT",
    "apecoin":                    "APEUSDT",
    "axie-infinity":              "AXSUSDT",
    "kava":                       "KAVAUSDT",
    "tezos":                      "XTZUSDT",
    "sushi":                      "SUSHIUSDT",
    "yearn-finance":              "YFIUSDT",
    "notcoin":                    "NOTUSDT",
    "coti":                       "COTIUSDT",
    "ocean-protocol":             "OCEANUSDT",
    "ankr":                       "ANKRUSDT",
    "band-protocol":              "BANDUSDT",
    "magic":                      "MAGICUSDT",
    "starknet":                   "STRKUSDT",
    "dydx":                       "DYDXUSDT",       # unified CoinGecko ID
    "ethereum-name-service":      "ENSUSDT",
    "stargate-finance":           "STGUSDT",
    "conflux-token":              "CFXUSDT",
    "blur":                       "BLURUSDT",
}

# Reverse map (derived — do not edit manually)
BINANCE_TO_COINGECKO: dict = {v: k for k, v in COINGECKO_TO_BINANCE.items()}


def fetch_altcoin_daily_returns(
    symbols: List[str] = DISPERSION_SYMBOLS,
    start:   str = "2024-11-01",   # extra lookback for rolling baseline warm-up
    end:     str = "2026-03-02",
    cache_file: str = DISPERSION_CACHE_FILE,
) -> pd.DataFrame:
    """
    Fetch daily close prices for each symbol from Binance futures klines.
    Returns a DataFrame of daily log-returns: index=date, columns=symbols.
    Results are cached to CSV; delete the cache to force a fresh fetch.
    """
    # ── Cache hit ────────────────────────────────────────────────────
    if os.path.exists(cache_file):
        try:
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            cached.index = pd.to_datetime(cached.index).tz_localize(None)
            end_dt  = pd.Timestamp(end).tz_localize(None)
            # Accept cache if it covers through yesterday (live data may not have today)
            if cached.index[-1] >= end_dt - pd.Timedelta(days=2):
                print(f"    Dispersion cache hit: {cache_file}  "
                      f"({len(cached)} rows × {len(cached.columns)} symbols)")
                return cached
            print(f"    Cache stale (ends {cached.index[-1].date()}), re-fetching ...")
        except Exception as e:
            print(f"    Cache read failed ({e}), re-fetching ...")

    BIN      = "https://fapi.binance.com"
    start_ms = _to_ms(start)
    end_ms   = _to_ms(end)
    closes   = {}

    print(f"    Fetching daily klines for {len(symbols)} symbols ...")
    for sym in symbols:
        prices = []
        cur    = start_ms
        while cur < end_ms:
            try:
                r = requests.get(
                    f"{BIN}/fapi/v1/klines",
                    params={"symbol": sym, "interval": "1d",
                            "startTime": cur, "endTime": end_ms, "limit": 500},
                    timeout=15)
                if r.status_code != 200:
                    break
                batch = r.json()
                if not batch:
                    break
                prices.extend(batch)
                cur = int(batch[-1][6]) + 1   # close_time + 1ms -> next candle
                if len(batch) < 500:
                    break
                time.sleep(0.05)
            except Exception as e:
                print(f"      ! {sym}: {e}")
                break
        if prices:
            dates_i = pd.to_datetime([c[0] for c in prices], unit="ms").normalize()
            close_i = pd.Series([float(c[4]) for c in prices], index=dates_i)
            closes[sym] = close_i.groupby(level=0).last()   # deduplicate

    if not closes:
        print("    ! All fetches failed - dispersion filter unavailable")
        return pd.DataFrame()

    # Align all series to a common daily index
    price_df  = pd.DataFrame(closes)
    price_df  = price_df.sort_index()
    # Log-returns (NaN on first row per symbol)
    ret_df    = np.log(price_df / price_df.shift(1))
    ret_df.index = pd.to_datetime(ret_df.index).tz_localize(None)

    n_ok = (ret_df.notna().sum(axis=1) >= 20).sum()
    print(f"    Fetched {len(ret_df)} days, {len(ret_df.columns)} symbols, "
          f"{n_ok} days with >=20 valid symbols")

    # Cache to CSV
    try:
        ret_df.to_csv(cache_file)
        print(f"    Cached to {cache_file}")
    except Exception as e:
        print(f"    Cache write failed: {e}")

    return ret_df


def _load_mcap_from_parquet(
    parquet_path: str,
    start:        str,
    end:          str,
) -> pd.DataFrame:
    """
    Load market cap history from a coingecko_marketcap.py parquet file and
    convert to the wide format expected by build_dynamic_symbol_mask().

    The parquet schema (from coingecko_marketcap.py) is:
        coin_id (str), date (datetime64[UTC]), price_usd, market_cap_usd,
        volume_usd, rank_num (int)

    Returns
    -------
    pd.DataFrame
        index   : date (tz-naive, daily)
        columns : Binance USDT-M perp ticker  (e.g. "BTCUSDT")
        values  : market cap in USD (NaN where coin had no data that day)

    Only rows whose coin_id maps to a Binance perp in COINGECKO_TO_BINANCE
    are included.  Coins in the parquet with no Binance perp are ignored.
    """
    p = Path(parquet_path)
    if not p.exists():
        raise FileNotFoundError(f"Parquet not found: {parquet_path}")

    df = pd.read_parquet(p, engine="pyarrow")

    # Normalise date column to tz-naive daily timestamps
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize().dt.tz_localize(None)

    # Trim to requested date range
    start_dt = pd.Timestamp(start)
    end_dt   = pd.Timestamp(end)
    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]

    if df.empty:
        raise ValueError(f"No data in parquet for range {start} → {end}")

    # Map coin_id → Binance ticker; drop coins with no mapping
    df["binance_ticker"] = df["coin_id"].map(COINGECKO_TO_BINANCE)
    df = df.dropna(subset=["binance_ticker"])

    if df.empty:
        raise ValueError("No coin_ids in parquet match COINGECKO_TO_BINANCE mapping")

    # Deduplicate (coin_id, date) — keep highest market cap row if dupes exist
    df = (df.sort_values("market_cap_usd", ascending=False)
            .drop_duplicates(subset=["binance_ticker", "date"])
            .sort_values("date"))

    # Pivot to wide: date × binance_ticker → market_cap_usd
    wide = df.pivot(index="date", columns="binance_ticker", values="market_cap_usd")
    wide.index = pd.to_datetime(wide.index).tz_localize(None)
    wide.columns.name = None

    n_coins  = len(wide.columns)
    n_days   = len(wide)
    coverage = wide.notna().mean().mean() * 100
    print(f"    Parquet loaded: {n_days} days × {n_coins} Binance tickers  "
          f"(avg coverage {coverage:.0f}%)")
    print(f"    Date range: {wide.index[0].date()} → {wide.index[-1].date()}")
    return wide


def _load_mcap_from_db(start: str, end: str) -> pd.DataFrame:
    """
    Load market cap from market.market_cap_daily and return the same
    wide-format DataFrame as _load_mcap_from_parquet().

    The DB table uses `base` (e.g. "BTC") whereas the audit expects
    Binance USDT-M perp tickers (e.g. "BTCUSDT"). We append "USDT"
    to each base to match the COINGECKO_TO_BINANCE convention.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from pipeline.db.connection import get_conn
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, base, market_cap_usd
        FROM market.market_cap_daily
        WHERE date >= %s::date AND date <= %s::date
          AND market_cap_usd IS NOT NULL
        ORDER BY date
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        raise ValueError(f"No mcap data in DB for {start} → {end}")
    df = pd.DataFrame(rows, columns=["date", "base", "market_cap_usd"])
    df["date"] = pd.to_datetime(df["date"])
    df["binance_ticker"] = df["base"] + "USDT"
    # Only keep tickers that appear in the COINGECKO_TO_BINANCE mapping
    valid_tickers = set(COINGECKO_TO_BINANCE.values())
    df = df[df["binance_ticker"].isin(valid_tickers)]
    if df.empty:
        raise ValueError("No DB mcap rows match COINGECKO_TO_BINANCE mapping")
    df = (df.sort_values("market_cap_usd", ascending=False)
            .drop_duplicates(subset=["binance_ticker", "date"])
            .sort_values("date"))
    wide = df.pivot(index="date", columns="binance_ticker", values="market_cap_usd")
    wide.index = pd.to_datetime(wide.index).tz_localize(None)
    wide.columns.name = None
    n_coins  = len(wide.columns)
    n_days   = len(wide)
    coverage = wide.notna().mean().mean() * 100
    print(f"    DB loaded: {n_days} days × {n_coins} Binance tickers  "
          f"(avg coverage {coverage:.0f}%)")
    print(f"    Date range: {wide.index[0].date()} → {wide.index[-1].date()}")
    return wide


def fetch_mcap_history(
    coingecko_ids: List[str] = None,
    start:         str = "2024-11-01",
    end:           str = "2026-03-02",
    cache_file:    str = None,
    rate_limit_s:  float = 3.0,
) -> pd.DataFrame:
    """
    Return daily market cap history as a wide DataFrame:
        index   : date (tz-naive, daily)
        columns : Binance USDT-M perp ticker
        values  : market cap in USD

    Source priority
    ---------------
    1. coingecko_marketcap.py parquet  (DISPERSION_MCAP_PARQUET)
       Preferred — historically accurate 2000-coin universe with daily ranks
       recomputed from actual per-date market caps.  No survivorship bias.
       Produces this file:
           python coingecko_marketcap.py --api-key KEY --mode historical --start 2025-01-01

    2. Live CoinGecko API  (fallback when parquet is absent or set to "")
       Fetches /market_chart per coin, ~4.5 min for 90 symbols on first run.
       Results cached to DISPERSION_MCAP_CACHE_FILE (CSV).

    Only CoinGecko IDs present in COINGECKO_TO_BINANCE are included.
    """
    # ── Source 0: live database (market.market_cap_daily) ─────────────
    if MCAP_SOURCE == "db":
        try:
            wide = _load_mcap_from_db(start, end)
            if not wide.empty:
                print(f"    Mcap source: database (market.market_cap_daily)  "
                      f"[{len(wide.columns)} tickers]")
                return wide
        except Exception as e:
            print(f"    DB mcap load failed ({e}), falling back to parquet ...")

    # ── Source 1: parquet from coingecko_marketcap.py ─────────────────
    if DISPERSION_MCAP_PARQUET:
        pq_path = Path(DISPERSION_MCAP_PARQUET)
        if pq_path.exists():
            try:
                wide = _load_mcap_from_parquet(DISPERSION_MCAP_PARQUET, start, end)
                end_dt = pd.Timestamp(end).tz_localize(None)
                if not wide.empty and wide.index[-1] >= end_dt - pd.Timedelta(days=2):
                    print(f"    Mcap source: parquet ({DISPERSION_MCAP_PARQUET})  "
                          f"[historically accurate, {len(wide.columns)} tickers]")
                    return wide
                print(f"    Parquet stale (ends {wide.index[-1].date()}), "
                      f"falling back to live API ...")
            except Exception as e:
                print(f"    Parquet load failed ({e}), falling back to live API ...")
        else:
            print(f"    Parquet not found ({DISPERSION_MCAP_PARQUET}).")
            print(f"    Run:  python coingecko_marketcap.py --api-key KEY "
                  f"--mode historical --start 2024-11-01")
            print(f"    Falling back to live CoinGecko API ...")

    # ── Source 2: live CoinGecko API (original behaviour) ────────────
    if coingecko_ids is None:
        coingecko_ids = DISPERSION_MCAP_SYMBOLS
    if cache_file is None:
        cache_file = DISPERSION_MCAP_CACHE_FILE

    # ── CSV cache check ───────────────────────────────────────────────
    cache_path = Path(cache_file)
    if cache_path.exists():
        try:
            cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            cached.index = pd.to_datetime(cached.index).tz_localize(None)
            end_dt = pd.Timestamp(end).tz_localize(None)
            if not cached.empty and cached.index[-1] >= end_dt - pd.Timedelta(days=2):
                print(f"    Mcap CSV cache hit: {cache_file}  "
                      f"({len(cached)} days × {len(cached.columns)} symbols)")
                return cached
            print(f"    Mcap CSV cache stale (ends {cached.index[-1].date()}), re-fetching ...")
        except Exception as e:
            print(f"    Mcap CSV cache read failed ({e}), re-fetching ...")

    start_dt = pd.Timestamp(start)
    end_dt   = pd.Timestamp(end)
    GECKO    = "https://api.coingecko.com/api/v3"
    mcap_cols: dict = {}
    failed:    list = []

    # Build request headers — CoinGecko requires x-cg-demo-api-key since late 2024.
    # Free Demo keys: https://www.coingecko.com/en/api
    _cg_headers = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        _cg_headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
    else:
        print("    WARNING: COINGECKO_API_KEY is empty — requests will return HTTP 401.")
        print("             Get a free key at https://www.coingecko.com/en/api and set")
        print("             COINGECKO_API_KEY in audit.py before using dynamic mode.")

    # Only fetch IDs that have a mapped Binance perp ticker
    ids_to_fetch = [cg for cg in coingecko_ids if cg in COINGECKO_TO_BINANCE]
    print(f"    Fetching market cap history for {len(ids_to_fetch)} coins "
          f"from CoinGecko ({start} → {end}) ...")
    print(f"    Est. time: ~{len(ids_to_fetch) * rate_limit_s / 60:.1f} min "
          f"(rate_limit={rate_limit_s}s/request)")

    for i, cg_id in enumerate(ids_to_fetch, 1):
        binance_ticker = COINGECKO_TO_BINANCE[cg_id]
        try:
            resp = requests.get(
                f"{GECKO}/coins/{cg_id}/market_chart",
                params={"vs_currency": "usd", "days": "max", "interval": "daily"},
                timeout=30,
                headers=_cg_headers,
            )
            if resp.status_code == 401:
                print(f"\n    ✗ HTTP 401 Unauthorized — CoinGecko API key missing or invalid.")
                print(f"      Demo keys must target api.coingecko.com, NOT pro-api.coingecko.com.")
                print(f"      Test: curl -H 'x-cg-demo-api-key: YOUR_KEY' https://api.coingecko.com/api/v3/ping")
                print(f"      Expected: {{\"gecko_says\":\"(V3) To the Moon!\"}}")
                print(f"      Set COINGECKO_API_KEY in audit.py. Free keys: https://www.coingecko.com/en/api")
                print(f"      Aborting mcap fetch.\n")
                return pd.DataFrame()
            if resp.status_code == 429:
                # Rate limited — back off 4× and retry once
                wait = rate_limit_s * 4
                print(f"      [{i}/{len(ids_to_fetch)}] {cg_id}: rate limited, "
                      f"waiting {wait:.0f}s ...")
                time.sleep(wait)
                resp = requests.get(
                    f"{GECKO}/coins/{cg_id}/market_chart",
                    params={"vs_currency": "usd", "days": "max", "interval": "daily"},
                    timeout=30,
                    headers=_cg_headers,
                )

            if resp.status_code != 200:
                print(f"      [{i}/{len(ids_to_fetch)}] {cg_id} → {binance_ticker}: "
                      f"HTTP {resp.status_code} — skip")
                failed.append(cg_id)
                time.sleep(rate_limit_s)
                continue

            data  = resp.json()
            mcaps = data.get("market_caps", [])
            if not mcaps:
                print(f"      [{i}/{len(ids_to_fetch)}] {cg_id}: empty response — skip")
                failed.append(cg_id)
                time.sleep(rate_limit_s)
                continue

            # CoinGecko returns [timestamp_ms, value] pairs.
            # For recently-listed coins, early data may be at hourly granularity.
            # The groupby().last() dedup handles this correctly.
            dates  = pd.to_datetime([m[0] for m in mcaps], unit="ms").normalize()
            values = [float(m[1]) for m in mcaps]
            s      = pd.Series(values, index=dates, name=binance_ticker)
            s      = s.groupby(level=0).last()                          # deduplicate
            s      = s[(s.index >= start_dt) & (s.index <= end_dt)]    # trim to range
            s      = s[s > 0]                                           # drop zero mcaps

            mcap_cols[binance_ticker] = s
            print(f"      [{i}/{len(ids_to_fetch)}] {cg_id} → {binance_ticker}: "
                  f"{len(s)} days  (last: {s.index[-1].date() if len(s) else 'n/a'})")

        except Exception as e:
            print(f"      [{i}/{len(ids_to_fetch)}] {cg_id}: {e} — skip")
            failed.append(cg_id)

        time.sleep(rate_limit_s)

    if not mcap_cols:
        print("    ! fetch_mcap_history: all fetches failed — dynamic universe unavailable")
        return pd.DataFrame()

    mcap_df = pd.DataFrame(mcap_cols).sort_index()
    mcap_df.index = pd.to_datetime(mcap_df.index).tz_localize(None)

    if failed:
        print(f"    Warning: {len(failed)} coin(s) failed: "
              f"{', '.join(failed[:10])}{'...' if len(failed) > 10 else ''}")
    print(f"    Mcap history: {len(mcap_df)} days × {len(mcap_df.columns)} symbols")

    try:
        mcap_df.to_csv(cache_file)
        print(f"    Cached → {cache_file}")
    except Exception as e:
        print(f"    Cache write failed: {e}")

    return mcap_df


def build_dynamic_symbol_mask(
    mcap_df:       pd.DataFrame,
    alt_returns:   pd.DataFrame,
    n:             int = None,
) -> pd.DataFrame:
    """
    Build a daily boolean mask selecting the top-N symbols by market cap.

    Returns
    -------
    pd.DataFrame
        index   : same as alt_returns.index
        columns : same as alt_returns.columns
        values  : True  = symbol is in top-N on this day
                  False = not in top-N (or no mcap data available)

    Lag discipline
    --------------
    mask[T] is derived from mcap[T-1] via a .shift(1) on the
    date-aligned mcap_df. Combined with build_dispersion_filter's own
    .shift(1), trades on day T+1 use mcap data from day T-1 — clean.

    Edge cases handled
    ------------------
    - Warmup (Day 1 / pre-data days): all-NaN after shift → all False.
      pandas rank() on an all-NaN row with na_option='bottom' assigns
      rank=1 to every element (all tied at minimum), incorrectly
      selecting all symbols. We explicitly zero out these rows.
    - Symbols in mcap_df but not in alt_returns: ignored.
    - Symbols in alt_returns but not in mcap_df: always False.
    - NaN mcap on a specific day: symbol treated as rank=last.
    """
    if n is None:
        n = DISPERSION_N

    if mcap_df.empty:
        print("    [dynamic mask] mcap_df empty — returning all-True mask "
              "(filter will use all symbols in alt_returns)")
        return pd.DataFrame(True, index=alt_returns.index, columns=alt_returns.columns)

    common_tickers = [c for c in mcap_df.columns if c in alt_returns.columns]
    if not common_tickers:
        print("    [dynamic mask] no common tickers between mcap_df and alt_returns "
              "— returning all-True mask")
        return pd.DataFrame(True, index=alt_returns.index, columns=alt_returns.columns)

    # Align mcap to alt_returns date index; forward-fill gaps (weekends etc.)
    mcap_aligned = (
        mcap_df[common_tickers]
        .reindex(alt_returns.index, method="ffill")
        .shift(1)              # ← 1-day lag: mask[T] uses mcap[T-1]
    )

    # Identify warmup rows where all mcap values are NaN (before data starts).
    # Must be handled before ranking — see docstring for why.
    all_nan_rows = mcap_aligned.isna().all(axis=1)

    ranked = mcap_aligned.rank(
        axis=1, ascending=False, method="min", na_option="bottom"
    )
    top_n_mask = ranked <= n

    # Zero out all-NaN warmup rows — no mcap data = no valid top-N selection
    top_n_mask.loc[all_nan_rows] = False

    # Expand to full alt_returns column space; symbols with no mcap → False
    full_mask = pd.DataFrame(False, index=alt_returns.index, columns=alt_returns.columns)
    full_mask[common_tickers] = top_n_mask

    # Diagnostics
    daily_counts = full_mask.sum(axis=1)
    warmup_days  = int(all_nan_rows.sum())
    print(f"    [dynamic mask] top-{n}: mean {daily_counts.mean():.1f} symbols/day, "
          f"min {int(daily_counts.min())}, warmup days excluded: {warmup_days}")
    if daily_counts[~all_nan_rows].min() < max(3, n // 5):
        print(f"    [dynamic mask] WARNING: some non-warmup days have <{max(3, n//5)} "
              f"active symbols — check mcap data coverage")

    return full_mask


def build_dispersion_filter(
    alt_returns:    pd.DataFrame,
    threshold:      float = 0.75,
    baseline_win:   int   = 20,
    min_symbols:    int   = 20,
    symbol_mask_df: "Optional[pd.DataFrame]" = None,
) -> pd.Series:
    """
    Returns pd.Series[bool] index=daily DatetimeIndex, True = sit flat.
    Lagged by 1 day: yesterday's dispersion gates today's trade.

    dispersion_t  = cross-sectional std of log-returns across active symbols
    baseline_t    = rolling(baseline_win).median(dispersion)
    disp_ratio_t  = dispersion_t / baseline_t
    sit_out_t+1   = disp_ratio_t < threshold   (lagged by 1 day)

    Parameters
    ----------
    symbol_mask_df : Optional[pd.DataFrame]
        Boolean DataFrame (date × ticker). When supplied (dynamic mode),
        each day's std is restricted to True-valued symbols on that day.
        When None (default), all symbols are used — original behaviour.
    """
    idx = pd.date_range("2025-01-01", "2026-03-01", freq="D")

    if alt_returns.empty:
        print("    Dispersion filter: no data — returning all-False (no filter)")
        return pd.Series(False, index=idx)

    use_dynamic = symbol_mask_df is not None and not symbol_mask_df.empty

    if use_dynamic:
        # Align mask to alt_returns (handles any index/column mismatches)
        mask = symbol_mask_df.reindex(
            index=alt_returns.index, columns=alt_returns.columns, fill_value=False
        )

        # NaN out returns for non-active (non-top-N) symbols each day
        masked_returns = alt_returns.where(mask, other=np.nan)

        # Require min_symbols actual non-NaN returns per day.
        # Use notna count on masked_returns, NOT mask.sum() — a top-N symbol
        # can still have NaN returns (data gap, new listing), and counting
        # mask booleans would pass days with too few real return values.
        valid_mask = masked_returns.notna().sum(axis=1) >= min_symbols
        masked_returns = masked_returns[valid_mask]

        # Derive label from mask's actual active count, not global DISPERSION_N
        # (which would be wrong if called from a sweep with a different n).
        actual_n   = int(mask.sum(axis=1).median())
        mode_label = f"dynamic top-{actual_n}"

    else:
        # Static path — original behaviour, unchanged
        valid_mask     = alt_returns.notna().sum(axis=1) >= min_symbols
        masked_returns = alt_returns[valid_mask]
        mode_label     = "static"

    # Cross-sectional std per day (across symbols, not across time)
    dispersion = masked_returns.std(axis=1)

    # Rolling median baseline — past data only, no lookahead
    baseline   = dispersion.rolling(
        baseline_win, min_periods=max(5, baseline_win // 2)
    ).median()
    disp_ratio = dispersion / baseline.replace(0, np.nan)

    # Lag by 1: yesterday's signal gates today's trade
    low_disp_flag = (
        (disp_ratio < threshold)
        .shift(1)
        .reindex(idx, fill_value=False)
        .fillna(False)
    )

    n_flagged = int(low_disp_flag.sum())
    pct       = n_flagged / len(idx) * 100

    print(f"    Dispersion filter [{mode_label}] (threshold={threshold:.2f}): "
          f"{n_flagged} days flagged ({pct:.1f}%)")
    print(f"      dispersion: mean={dispersion.mean():.4f}  "
          f"median={dispersion.median():.4f}  "
          f"10th pct={dispersion.quantile(0.10):.4f}")
    print(f"      disp_ratio: mean={disp_ratio.mean():.3f}  "
          f"days below threshold: {(disp_ratio < threshold).sum()} raw "
          f"(before reindex/lag)")

    return low_disp_flag

def build_vol_filter(
    btc_ohlcv:      "pd.DataFrame",
    lookback:       int   = 10,
    percentile:     float = 0.25,
    baseline_win:   int   = 90,
) -> "pd.Series":
    """
    Returns pd.Series[bool] index=daily DatetimeIndex, True = sit flat.
    Lagged by 1 day: yesterday's vol gates today's trade.

    Realised vol = rolling std of daily BTC returns over `lookback` days.
    Baseline     = rolling quantile(percentile) over `baseline_win` days.
    sit_out_t+1  = rvol_t < baseline_t   (vol compression regime - Case C fix)
    """

    idx = pd.date_range("2025-01-01", "2026-03-01", freq="D")

    if btc_ohlcv.empty or "close" not in btc_ohlcv.columns:
        print("    Vol filter: no BTC data - returning all-False (no filter)")
        return pd.Series(False, index=idx)

    close   = btc_ohlcv["close"].sort_index()
    btc_ret = close.pct_change()

    # Realised vol: rolling std of returns
    rvol = btc_ret.rolling(lookback, min_periods=max(3, lookback // 2)).std()

    # Rolling quantile baseline (past data only - no lookahead)
    baseline = rvol.rolling(baseline_win,
                            min_periods=max(10, baseline_win // 3)).quantile(percentile)

    # Compression flag: vol is below its own rolling floor
    vol_compressed = rvol < baseline

    # Lag by 1: yesterday's signal gates today
    vol_flag = vol_compressed.shift(1).reindex(idx, fill_value=False).fillna(False)

    n_flagged = int(vol_flag.sum())
    pct       = n_flagged / len(idx) * 100

    print(f"    Vol filter (lookback={lookback}d, "
          f"p{percentile*100:.0f} baseline={baseline_win}d): "
          f"{n_flagged} days flagged ({pct:.1f}%)")
    print(f"      rvol: mean={rvol.mean():.5f}  "
          f"median={rvol.median():.5f}  "
          f"25th pct={rvol.quantile(0.25):.5f}")

    # Max compression cluster - tells us if vol suppression is sustained or scattered
    try:
        cluster = vol_flag.astype(int).groupby(
            (~vol_flag).cumsum()
        ).sum().max()
        print(f"      max compression cluster: {cluster} consecutive days")
    except Exception:
        pass

    return vol_flag


# ══════════════════════════════════════════════════════════════════════
# BLOFIN AVAILABILITY FILTER
# ══════════════════════════════════════════════════════════════════════

def fetch_blofin_instruments() -> set:
    """
    Fetch the current list of tradeable SWAP instruments from the Blofin
    public API (no API key required).

    Tries multiple approaches in order:
      1. GET /api/v1/market/instruments?instType=SWAP  (correct param)
      2. GET /api/v1/market/instruments                (no param, broader)
    Both requests include a browser-style User-Agent to avoid WAF 403s.

    Returns a set of uppercase symbol strings in both base ("BTC") and
    concatenated ("BTCUSDT") form to maximise matching against portfolio
    symbol strings from the deploys CSV.

    Falls back to an empty set on any network or parse error — the caller
    degrades gracefully (Blofin layer adds no additional filter days).
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
    attempts = [
        ("https://openapi.blofin.com/api/v1/market/instruments", {"instType": "SWAP"}),
        ("https://openapi.blofin.com/api/v1/market/instruments", {}),
    ]

    for url, params in attempts:
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            if not resp.ok:
                print(f"    [Blofin] HTTP {resp.status_code} from {resp.url}")
                print(f"    [Blofin] Response body: {resp.text[:300]}")
                continue
            data = resp.json()
            instruments = data.get("data", [])
            if not instruments:
                continue
            # instId format: "BTC-USDT" or "BTC-USDT-SWAP"
            # Store both base ("BTC") and concat ("BTCUSDT") to match portfolio symbols.
            bases: set = set()
            for inst in instruments:
                inst_id = inst.get("instId", "")
                if inst_id:
                    base = inst_id.split("-")[0].upper().strip()
                    if base:
                        bases.add(base)
                        bases.add(base + "USDT")
            param_str = f"?instType={params['instType']}" if params else ""
            print(f"    [Blofin] Fetched {len(instruments)} instruments "
                  f"(via {param_str or 'no params'}) "
                  f"-> {len(bases)} unique base/symbol entries")
            return bases
        except Exception as e:
            print(f"    [Blofin] Attempt failed (params={params}): {type(e).__name__}: {e}")

    print("    [Blofin] Info: all fetch attempts failed — "
          "Blofin layer adds no filter beyond tail guardrail")
    return set()


def build_blofin_filter(
    df_4x,
    deploys_path: str = "",
    blofin_csv:   str = "",
    min_symbols:  int = 1,
    tail_filter=None,
    portfolio_symbols: set = None,
) -> "pd.Series":
    """
    Build a pd.Series[bool] (index=DatetimeIndex, True=sit flat) for the
    Tail + Blofin combined filter.

    Logic:
      - Tail guardrail days are always sat flat (if tail_filter supplied).
      - On non-tail days, count how many of that day's portfolio symbols are
        listed on Blofin. If fewer than `min_symbols`, sit flat.

    Symbol source (priority order):
      1. blofin_csv      — historical CSV (date, symbol) built by a daily cron.
      2. deploys_path    — deploys CSV (R1..RN cols); intersected with live API.
      3. portfolio_symbols — static set of expected symbols; intersected with live API.
      4. Fallback         — no per-symbol data; Blofin layer is skipped (warning printed).

    portfolio_symbols: optional static set of uppercase symbol strings (e.g. {"BTC","ETH"})
        used as a fallback when neither deploys_path nor blofin_csv is available.
        Without this, the Blofin gate cannot be applied and silently adds no filter.
    """
    # ── Resolve trading dates from df_4x columns ──────────────────────
    col_dates = [_col_to_timestamp(str(col)) for col in df_4x.columns]
    sit_flat_vals = [False] * len(col_dates)

    # ── Layer 1: tail guardrail ───────────────────────────────────────
    n_tail_flat = 0
    if tail_filter is not None:
        for i, ts in enumerate(col_dates):
            if ts is None:
                continue
            ts_norm = pd.Timestamp(ts).normalize()
            try:
                if tail_filter.reindex([ts_norm], method="nearest").iloc[0]:
                    sit_flat_vals[i] = True
                    n_tail_flat += 1
            except Exception:
                pass

    # ── Load Blofin availability ──────────────────────────────────────
    use_historical  = False
    blofin_by_date: dict = {}
    blofin_live:    set  = set()

    if blofin_csv and Path(blofin_csv).exists():
        print(f"    [Blofin] Loading historical availability from {blofin_csv}")
        try:
            bdf = pd.read_csv(blofin_csv)
            bdf.columns = [c.strip().lower() for c in bdf.columns]
            bdf["date"]   = pd.to_datetime(bdf["date"]).dt.normalize()
            bdf["symbol"] = bdf["symbol"].str.upper().str.strip()
            blofin_by_date = bdf.groupby("date")["symbol"].apply(set).to_dict()
            use_historical = True
            print(f"    [Blofin] Historical CSV: {len(blofin_by_date)} days of availability data")
        except Exception as e:
            print(f"    [Blofin] Could not load historical CSV ({e}), "
                  f"falling back to live API")

    if not use_historical:
        blofin_live = fetch_blofin_instruments()
        if not blofin_live:
            print("    [Blofin] Falling back to tail-only filter (no instrument data available)")
            out_idx = pd.DatetimeIndex([
                pd.Timestamp(ts).normalize() if ts is not None else pd.NaT
                for ts in col_dates
            ])
            return pd.Series(sit_flat_vals, index=out_idx, dtype=bool)

    # ── Load per-day portfolio symbols from deploys CSV ───────────────
    portfolio_by_date: dict = {}
    dep_path = deploys_path or ""
    if dep_path and Path(dep_path).exists():
        try:
            dep = pd.read_csv(dep_path)
            dep.columns = [c.strip() for c in dep.columns]
            ts_col   = "timestamp_utc" if "timestamp_utc" in dep.columns else dep.columns[0]
            sym_cols = sorted(
                [c for c in dep.columns if c.startswith("R") and c[1:].isdigit()],
                key=lambda c: int(c[1:])
            )
            for _, row in dep.iterrows():
                d    = pd.Timestamp(row[ts_col]).normalize()
                syms = set()
                for sc in sym_cols:
                    v = str(row[sc]).upper().strip()
                    if v and v not in ("NAN", "NONE", ""):
                        syms.add(v)
                        syms.add(v.replace("USDT", ""))  # store base form too
                if syms:
                    portfolio_by_date[d] = syms
            print(f"    [Blofin] Loaded per-day portfolio symbols from "
                  f"{Path(dep_path).name} ({len(portfolio_by_date)} days)")
        except Exception as e:
            print(f"    [Blofin] Could not load deploys CSV ({e})")

    # ── Determine static symbol fallback ─────────────────────────────
    # If no per-day deploy data, use portfolio_symbols (static set) as proxy.
    # Without any symbol reference we cannot evaluate the Blofin gate.
    static_syms = None
    if not portfolio_by_date:
        if portfolio_symbols:
            # Normalise: store both base and base+USDT forms
            static_syms = set()
            for s in portfolio_symbols:
                s = s.upper().strip()
                static_syms.add(s)
                static_syms.add(s.replace("USDT", ""))
                static_syms.add(s.rstrip("USDT") + "USDT")
            print(f"    [Blofin] No deploys CSV — using static portfolio_symbols "
                  f"({len(portfolio_symbols)} symbols) as proxy")
        else:
            print("    [Blofin] ⚠ WARNING: no deploys CSV and no portfolio_symbols "
                  "provided. Cannot evaluate per-symbol Blofin availability. "
                  "Blofin layer will add NO additional filter days beyond tail guardrail. "
                  "Set BLOFIN_PORTFOLIO_SYMBOLS in config or provide a deploys CSV.")

    # ── Diagnostic: what fraction of static/live symbols match Blofin? ─
    if static_syms is not None and blofin_live:
        matched = static_syms & blofin_live
        print(f"    [Blofin] Symbol match check: {len(matched)}/{len(static_syms)} "
              f"static symbols found on Blofin live list")
        if matched:
            print(f"    [Blofin] Matched: {sorted(matched)[:20]}"
                  f"{'...' if len(matched) > 20 else ''}")
        else:
            print(f"    [Blofin] ⚠ ZERO matches — check symbol format "
                  f"(Blofin sample: {sorted(blofin_live)[:10]})")

    # ── Layer 2: Blofin availability gate ─────────────────────────────
    n_blofin_flat    = 0
    n_checked        = 0
    removed_syms_all: list  = []   # (date_str, sym) for every symbol missed
    removed_days:     list  = []   # date_str for every newly-flatted day
    for i, ts in enumerate(col_dates):
        if sit_flat_vals[i]:
            continue   # already flat from tail filter
        if ts is None:
            continue
        ts_norm = pd.Timestamp(ts).normalize()

        blofin_avail   = blofin_by_date.get(ts_norm, set()) if use_historical else blofin_live
        portfolio_syms = portfolio_by_date.get(ts_norm, static_syms)

        if portfolio_syms is not None:
            n_on_blofin = len(portfolio_syms & blofin_avail)
            n_checked  += 1
        else:
            # No symbol reference at all — skip Blofin gate for this day
            continue

        if n_on_blofin < min_symbols:
            sit_flat_vals[i] = True
            n_blofin_flat    += 1
            day_str = ts_norm.strftime("%Y-%m-%d")
            removed_days.append(day_str)
            # Record which symbols in the portfolio were NOT on Blofin
            missing = portfolio_syms - blofin_avail
            for sym in missing:
                removed_syms_all.append((day_str, sym))

    n_total = sum(sit_flat_vals)
    n_days  = len(col_dates)

    # Deduplicated symbol count and first-20 examples
    unique_removed_syms = sorted({sym for _, sym in removed_syms_all})
    n_unique_removed    = len(unique_removed_syms)
    examples            = unique_removed_syms[:20]

    # ── Save removed-symbols log to CSV ───────────────────────────────
    if removed_syms_all:
        try:
            import datetime as _dt
            _out_dir = Path(dep_path).parent if dep_path else Path(".")
            _ts      = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            _csv_out = _out_dir / f"blofin_removed_symbols_{_ts}.csv"
            _rows    = sorted(removed_syms_all, key=lambda x: (x[0], x[1]))
            with open(_csv_out, "w") as _f:
                _f.write("date,symbol\n")
                for _date, _sym in _rows:
                    _f.write(f"{_date},{_sym}\n")
            print(f"    [Blofin]   Removed-symbols log   : {_csv_out}")
        except Exception as _e:
            print(f"    [Blofin]   \u26a0 Could not save removed-symbols CSV: {_e}")

    print(f"    [Blofin] ── Filter summary ──────────────────────────────")
    print(f"    [Blofin]   Total calendar days    : {n_days}")
    print(f"    [Blofin]   Tail guardrail flat    : {n_tail_flat}d  "
          f"({n_tail_flat/n_days*100:.1f}%)")
    print(f"    [Blofin]   Blofin gate adds flat  : {n_blofin_flat}d  "
          f"({n_blofin_flat/n_days*100:.1f}%)")
    print(f"    [Blofin]   Total flat days        : {n_total}d  "
          f"({n_total/n_days*100:.1f}%)")
    print(f"    [Blofin]   Days with symbol check : {n_checked}  "
          f"(min_symbols={min_symbols})")
    print(f"    [Blofin]   Unique symbols removed : {n_unique_removed}")
    print(f"    [Blofin]   Days changed by gate   : {n_blofin_flat}")
    if examples:
        print(f"    [Blofin]   First {len(examples)} removed symbols  : "
              f"{', '.join(examples)}"
              f"{'  ...' if n_unique_removed > 20 else ''}")
    if removed_days:
        print(f"    [Blofin]   Days flatted by gate   : "
              f"{', '.join(removed_days[:10])}"
              f"{'  ...' if len(removed_days) > 10 else ''}")
    if n_blofin_flat == 0 and n_checked > 0:
        print(f"    [Blofin]   ℹ  0 additional days filtered — all checked days "
              f"had ≥{min_symbols} symbols on Blofin. Filter is working but "
              f"adds no extra flat days with current settings.")
    elif n_checked == 0:
        print(f"    [Blofin]   ⚠  0 days were symbol-checked — Blofin gate "
              f"was never evaluated. Provide deploys CSV or portfolio_symbols.")

    out_idx = pd.DatetimeIndex([
        pd.Timestamp(ts).normalize() if ts is not None else pd.NaT
        for ts in col_dates
    ])
    return pd.Series(sit_flat_vals, index=out_idx, dtype=bool)
def regime_attribution(daily_returns, btc_returns, alt_returns=None):
    df = pd.concat([
        pd.Series(daily_returns, name="strategy"),
        pd.Series(btc_returns, name="btc")
    ], axis=1).dropna()

    if len(df) < 10:
        print("⚠ Regime attribution skipped - insufficient samples")
        return

    # ── BTC volatility regime ─────────────────────
    vol = df["btc"].rolling(20).std()
    vol_med = vol.median()

    df["high_vol"] = vol > vol_med

    # ── BTC trend regime ──────────────────────────
    trend = df["btc"].rolling(20).mean()
    df["btc_up"] = trend > 0

    # ── Real cross-section dispersion (if available)
    if alt_returns is not None:

        disp = alt_returns.std(axis=1, skipna=True)
        disp = disp.reindex(df.index)  # align to strategy/BTC

        disp_med = disp.median()

        df["high_disp"] = disp > disp_med

    else:
        # fallback proxy
        disp = df["btc"].abs()
        disp_med = disp.median()
        df["high_disp"] = disp > disp_med


    regimes = {

        "High Dispersion": df[df["high_disp"]],
        "Low Dispersion": df[~df["high_disp"]],

        "BTC Uptrend": df[df["btc_up"]],
        "BTC Downtrend": df[~df["btc_up"]],

        "High Volatility": df[df["high_vol"]],
        "Low Volatility": df[~df["high_vol"]],

        # Combined regimes (very important)
        "LowDisp + LowVol": df[(~df["high_disp"]) & (~df["high_vol"])],
        "HighDisp + HighVol": df[(df["high_disp"]) & (df["high_vol"])]

    }

    print("\n════════════════════════════════════")
    print(" REGIME ATTRIBUTION")
    print("════════════════════════════════════")

    _regime_sharpes = {}
    for name, sub in regimes.items():

        r = sub["strategy"]

        if len(r) < 5:
            continue

        sharpe = np.sqrt(365) * r.mean() / r.std() if r.std() > 0 else np.nan
        _regime_sharpes[name] = float(sharpe) if not np.isnan(sharpe) else float("nan")

        print(
            f"{name:18} "
            f"Days={len(r):4d} "
            f"Mean={r.mean()*100:6.2f}% "
            f"Sharpe={sharpe:5.2f}"
        )

    return _regime_sharpes

def alpha_beta_decomposition(strategy_returns, btc_returns):


    df = pd.concat([
        pd.Series(strategy_returns, name="strategy"),
        pd.Series(btc_returns, name="btc")
    ], axis=1).dropna()

    if len(df) < 10:
        print("⚠ Alpha/Beta skipped - insufficient overlapping samples")
        return

    X = df["btc"].values.reshape(-1,1)
    y = df["strategy"].values

    model = LinearRegression().fit(X, y)

    beta = model.coef_[0]
    alpha_daily = model.intercept_
    alpha_annual = alpha_daily * 365
    r2 = model.score(X, y)

    print("\n════════════════════════════════════")
    print(" ALPHA vs BETA DECOMPOSITION")
    print("════════════════════════════════════")

    print(f"Samples: {len(df)}")
    print(f"Beta to BTC:        {beta:.3f}")
    print(f"Daily alpha:        {alpha_daily:.5f}")
    print(f"Annual alpha:       {alpha_annual:.2%}")
    print(f"Explained variance: {r2:.2%}")

    return beta, alpha_annual, r2

def plot_strategy_vs_dispersion(strategy_returns, alt_returns, label, outdir):


    # Cross-section dispersion
    dispersion = alt_returns.std(axis=1).reindex(strategy_returns.index)

    df = pd.DataFrame({
        "strategy": strategy_returns,
        "dispersion": dispersion
    }).dropna()

    if len(df) < 5:
        print("⚠ Dispersion scatter skipped - insufficient samples")
        return

    x = df["dispersion"].values
    y = df["strategy"].values

    # Regression
    model = LinearRegression().fit(x.reshape(-1,1), y)

    beta = model.coef_[0]
    alpha = model.intercept_

    x_vals = np.linspace(x.min(), x.max(), 100)
    y_vals = alpha + beta * x_vals

    plt.figure(figsize=(6,6))

    plt.scatter(x, y, alpha=0.4)

    plt.plot(x_vals, y_vals, linewidth=2)

    plt.axhline(0)
    plt.axvline(np.median(x))

    plt.xlabel("Market Dispersion (cross-section std)")
    plt.ylabel("Strategy Return")

    plt.title(f"{label}\nReturn vs Dispersion")

    fname = f"{label.replace(' ','_')}_dispersion_scatter.png"

    path = Path(outdir) / fname

    plt.tight_layout()
    plt.savefig(path)
    plt.close()

    print("  Dispersion scatter saved:", fname)


def plot_sharpe_vs_correlation(strategy_returns: pd.Series,
                                alt_returns: pd.DataFrame,
                                label: str,
                                outdir,
                                btc_returns: pd.Series = None,
                                corr_window: int = 20,
                                n_bins: int = 10):
    """
    Diagnostic: Sharpe vs cross-sectional correlation regime + dispersion/vol signal.

    Produces two views side-by-side:
      VIEW A - Rolling Spearman mean pairwise correlation (lag-adjusted)
      VIEW B - Dispersion-adjusted signal: cross-sectional std / BTC rolling vol

    Each view has 3 panels:
      1. Mean daily return per decile
      2. Sharpe per decile
      3. Scatter (signal vs return) with regression line, slope, R2, corr coef

    Plus a shared 4th panel: correlation regime time series + equity overlay.

    Interpretation barometer (corr_coef, return vs signal):
      <= -0.5  -> strong dispersion fingerprint
      -0.2    -> mild dispersion
       0      -> no relationship
      > 0     -> anti-dispersion strategy
    """



    # ── 1. Prepare alt returns ────────────────────────────────────────
    alt = alt_returns.copy()
    alt.index = pd.to_datetime(alt.index).tz_localize(None)
    alt = alt.loc[:, alt.notna().mean() > 0.5]
    if alt.shape[1] < 3:
        print("  ⚠ Correlation chart skipped - insufficient symbols")
        return

    # Diagnostic: confirm enough assets for stable correlation
    avg_syms = alt.notna().sum(axis=1).mean()
    print(f"  Avg symbols per day: {avg_syms:.2f}  "
          f"({'✅ stable' if avg_syms >= 5 else '⚠ low - correlation may be unstable (<5)'})")

    strat = strategy_returns.copy()
    strat.index = pd.to_datetime(strat.index).tz_localize(None)

    # ── 2. Rolling Spearman mean pairwise correlation (lag-adjusted) ──
    # Spearman is robust to heavy-tailed crypto returns / large single-coin spikes.
    # Shift by 1 to use only information available before the trade (no look-ahead).
    def _rolling_spearman_corr(df: pd.DataFrame, window: int) -> pd.Series:
        n   = len(df)
        arr = df.to_numpy(dtype=np.float32)   # float32 reduces memory & speeds matrix ops
        result = pd.Series(np.nan, index=df.index)
        for i in range(window - 1, n):
            block = arr[i - window + 1: i + 1]
            stds  = np.nanstd(block, axis=0)
            valid = stds > 1e-10
            if valid.sum() < 3:
                continue
            b = block[:, valid]
            col_means = np.nanmean(b, axis=0)
            inds = np.where(np.isnan(b))
            b[inds] = np.take(col_means, inds[1])
            # Spearman correlation matrix - axis=0: columns=variables, rows=observations
            # Ensures consistent matrix shape when column count varies after filtering
            corr_mat, _ = spearmanr(b, axis=0)
            if corr_mat.ndim < 2:   # guard: spearmanr can return scalar/1d when cols vary
                continue
            nv = corr_mat.shape[0]
            tri_idx = np.triu_indices(nv, k=1)
            vals = corr_mat[tri_idx]
            result.iloc[i] = float(np.nanmean(vals))
        return result

    print(f"  Computing rolling {corr_window}d Spearman pairwise correlation "
          f"({alt.shape[1]} symbols) with 1-day lag…")
    rolling_corr = _rolling_spearman_corr(alt, corr_window).shift(1)  # lag-adjust

    # ── 3. Dispersion-adjusted signal: cross-sec std / BTC rolling vol ─
    dispersion_raw = alt.std(axis=1, ddof=0)  # population std: cross-sectional, not sample estimate

    if btc_returns is not None:
        btc = btc_returns.copy()
        btc.index = pd.to_datetime(btc.index).tz_localize(None)
        btc_vol = btc.rolling(corr_window).std().reindex(strat.index)
        disp_signal = (dispersion_raw.reindex(strat.index) / btc_vol).shift(1)
        disp_signal_label = f"Dispersion / BTC Vol  (rolling {corr_window}d, 1d lag)"
    else:
        disp_signal = dispersion_raw.reindex(strat.index).shift(1)
        disp_signal_label = f"Cross-sectional Dispersion  (1d lag)"

    # ── 4. Shared helpers ─────────────────────────────────────────────
    def _sharpe(r):
        r = np.asarray(r, dtype=float)
        if len(r) < 3 or r.std() == 0:
            return np.nan
        return float(r.mean() / r.std() * np.sqrt(365))

    def _build_df(signal: pd.Series, sig_name: str) -> pd.DataFrame:
        df = pd.DataFrame({"ret": strat, "signal": signal,
                           "dispersion": dispersion_raw.reindex(strat.index)}).dropna()
        df = df[df["ret"] != 0.0]   # exclude filtered (flat) days
        return df

    def _bin_stats(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["decile"] = pd.qcut(df["signal"], q=n_bins,
                                labels=[f"D{i+1}" for i in range(n_bins)],
                                duplicates="drop")
        stats = df.groupby("decile", observed=True)["ret"].agg(
            mean_ret = "mean",
            n        = "count",
            win_rate = lambda x: (x > 0).mean(),
        )
        stats["sharpe"]   = df.groupby("decile", observed=True)["ret"].apply(_sharpe)
        stats["sig_mid"]  = df.groupby("decile", observed=True)["signal"].median()
        stats["disp_mid"] = df.groupby("decile", observed=True)["dispersion"].mean()
        return stats

    def _regression(df: pd.DataFrame):
        x = df["signal"].values
        y = df["ret"].values
        if np.std(x) < 1e-10:   # near-constant signal -> regression undefined
            return np.nan, np.nan, np.nan, np.nan
        slope, intercept = np.polyfit(x, y, 1)
        ss_res = np.sum((y - (slope * x + intercept)) ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
        # Spearman correlation coefficient - consistent with rank-based signal
        corr_coef, _ = spearmanr(x, y)
        return slope, intercept, r2, corr_coef

    def _verdict(stats: pd.DataFrame, corr_coef: float) -> str:
        is_disp = stats["sharpe"].iloc[0] > stats["sharpe"].iloc[-1]
        if corr_coef <= -0.5:
            strength = "STRONG"
        elif corr_coef <= -0.2:
            strength = "MILD"
        elif corr_coef < 0.0:
            strength = "WEAK"
        else:
            strength = "NONE / ANTI-DISPERSION"
        if is_disp and corr_coef < 0:
            return f"✅ {strength} DISPERSION FINGERPRINT  (r={corr_coef:.3f}, Sharpe ↓ as signal ↑)"
        else:
            return f"⚠  {strength} - NO CLEAR FINGERPRINT  (r={corr_coef:.3f})"

    def _draw_view(axes_row, df: pd.DataFrame, stats: pd.DataFrame,
                   slope, intercept, r2, corr_coef,
                   sig_xlabel: str, view_title: str):
        """Draw the 3 panels for one view (correlation or disp/vol signal)."""
        ax_ret, ax_sh, ax_sc = axes_row
        bins = stats.index.tolist()
        x_pos = np.arange(len(bins))

        # Panel A: mean return per decile
        bar_colors = ["#2ecc71" if v >= 0 else "#e74c3c"
                      for v in stats["mean_ret"]]
        ax_ret.bar(x_pos, stats["mean_ret"] * 100,
                   color=bar_colors, edgecolor="white", linewidth=0.6, alpha=0.85)
        ax_ret.axhline(0, color="black", linewidth=0.8)
        ax_ret.set_xticks(x_pos)
        ax_ret.set_xticklabels(
            [f"{b}\n({stats.loc[b,'sig_mid']:.3f})" for b in bins],
            fontsize=6, rotation=45, ha="right"
        )
        ax_ret.set_ylabel("Mean Daily Return (%)")
        ax_ret.set_title(f"{view_title}\nMean Return per Decile", fontsize=9)
        ax_ret.grid(True, alpha=0.3, axis="y")
        # Annotate Sharpe + avg dispersion
        for xi, b in zip(x_pos, bins):
            sh = stats.loc[b, "sharpe"]
            disp = stats.loc[b, "disp_mid"]
            ax_ret.text(xi, ax_ret.get_ylim()[0],
                        f"Sh={sh:.1f}\nd={disp:.4f}",
                        ha="center", fontsize=5.5, color="navy", va="bottom")

        # Panel B: Sharpe per decile
        sh_colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in stats["sharpe"]]
        ax_sh.bar(x_pos, stats["sharpe"],
                  color=sh_colors, edgecolor="white", linewidth=0.6, alpha=0.85)
        ax_sh.axhline(0, color="black", linewidth=0.8)
        ax_sh.axhline(2.0, color="green", linestyle=":", alpha=0.5, linewidth=1)
        ax_sh.set_xticks(x_pos)
        ax_sh.set_xticklabels(
            [f"{b}\n({stats.loc[b,'sig_mid']:.3f})" for b in bins],
            fontsize=6, rotation=45, ha="right"
        )
        ax_sh.set_ylabel("Sharpe")
        ax_sh.set_title(f"{view_title}\nSharpe per Decile", fontsize=9)
        ax_sh.grid(True, alpha=0.3, axis="y")

        # Background colour based on verdict
        is_disp = stats["sharpe"].iloc[0] > stats["sharpe"].iloc[-1] and corr_coef < 0
        bg = "#f0fff4" if is_disp else "#fff5f5"
        ax_ret.set_facecolor(bg); ax_sh.set_facecolor(bg)

        # Panel C: scatter + regression
        x_all = df["signal"].values
        y_all = df["ret"].values
        sc = ax_sc.scatter(x_all, y_all * 100,
                           alpha=0.3, s=15, c=y_all, cmap="RdYlGn",
                           vmin=-0.10, vmax=0.10)
        x_line = np.linspace(x_all.min(), x_all.max(), 200)
        ax_sc.plot(x_line, (slope * x_line + intercept) * 100,
                   color="navy", linewidth=1.8,
                   label=f"slope={slope*100:.4f}  R²={r2:.4f}  r={corr_coef:.3f}")
        ax_sc.axhline(0, color="black", linewidth=0.8)
        ax_sc.axvline(np.median(x_all), color="grey", linestyle="--",
                      linewidth=0.8, alpha=0.6, label="Median signal")
        ax_sc.set_xlabel(sig_xlabel, fontsize=8)
        ax_sc.set_ylabel("Strategy Daily Return (%)")
        ax_sc.set_title(f"{view_title}\nReturn vs Signal (scatter)", fontsize=9)
        ax_sc.legend(fontsize=7)
        ax_sc.grid(True, alpha=0.3)

    # ── 5. Compute stats for all four views ───────────────────────────
    # View C: normalized dispersion = dispersion / 90-day rolling median
    # Reveals whether strategy profits from absolute or relative dispersion spikes.
    dispersion_norm = (dispersion_raw /
                       dispersion_raw.rolling(90).median()
                       ).reindex(strat.index).shift(1)
    dispersion_norm_label = "Normalized Dispersion  (disp / 90d median, 1d lag)"

    # View D: cross-sectional skewness
    # Some dispersion strategies trigger on skew expansion, not std.
    # Positive skew = a few assets running hard while most are flat.
    disp_skew = alt.skew(axis=1).reindex(strat.index).shift(1)
    disp_skew_label = "Cross-sectional Skewness  (daily, 1d lag)"

    df_corr = _build_df(rolling_corr,    "corr")
    df_disp = _build_df(disp_signal,     "disp_signal")
    df_norm = _build_df(dispersion_norm, "disp_norm")
    df_skew = _build_df(disp_skew,       "disp_skew")

    if len(df_corr) < n_bins or len(df_disp) < n_bins:
        print("  ⚠ Correlation chart skipped - insufficient observations for decile binning")
        return

    stats_corr = _bin_stats(df_corr)
    stats_disp = _bin_stats(df_disp)
    stats_norm = _bin_stats(df_norm) if len(df_norm) >= n_bins else None
    stats_skew = _bin_stats(df_skew) if len(df_skew) >= n_bins else None

    sl_c, ic_c, r2_c, cc_c = _regression(df_corr)
    sl_d, ic_d, r2_d, cc_d = _regression(df_disp)
    sl_n, ic_n, r2_n, cc_n = (_regression(df_norm) if stats_norm is not None
                               else (np.nan, np.nan, np.nan, np.nan))
    sl_s, ic_s, r2_s, cc_s = (_regression(df_skew) if stats_skew is not None
                               else (np.nan, np.nan, np.nan, np.nan))

    verdict_corr = _verdict(stats_corr, cc_c)
    verdict_disp = _verdict(stats_disp, cc_d)
    verdict_norm = (_verdict(stats_norm, cc_n) if stats_norm is not None
                    else "⚠  View C skipped - insufficient data")
    verdict_skew = (_verdict(stats_skew, cc_s) if stats_skew is not None
                    else "⚠  View D skipped - insufficient data")

    # ── 6. Print diagnostics ──────────────────────────────────────────
    print(f"\n  ── Sharpe vs Correlation Diagnostics - {label} ──")
    print(f"  VIEW A (Spearman corr):  r={cc_c:.3f}  slope={sl_c*100:.4f}  R²={r2_c:.4f}  n={len(df_corr)}")
    print(f"  VIEW B (Disp/Vol):       r={cc_d:.3f}  slope={sl_d*100:.4f}  R²={r2_d:.4f}  n={len(df_disp)}")
    if stats_norm is not None:
        print(f"  VIEW C (Norm Disp):      r={cc_n:.3f}  slope={sl_n*100:.4f}  R²={r2_n:.4f}  n={len(df_norm)}")

    def _barometer(r):
        if r <= -0.5:   return "strong dispersion fingerprint"
        elif r <= -0.2: return "mild dispersion"
        elif r < 0:     return "weak dispersion"
        elif r == 0:    return "no relationship"
        else:           return "anti-dispersion"
    print(f"  Corr barometer:  VIEW A -> {_barometer(cc_c)}  |  VIEW B -> {_barometer(cc_d)}", end="")
    if stats_norm is not None:
        print(f"  |  VIEW C -> {_barometer(cc_n)}", end="")
    if stats_skew is not None:
        print(f"  |  VIEW D -> {_barometer(cc_s)}", end="")
    print()

    # Signal ranges for each view - aids decile interpretation
    x_c = df_corr["signal"].values
    x_d = df_disp["signal"].values
    print(f"  Signal range VIEW A: [{x_c.min():.4f}, {x_c.max():.4f}]  median={np.median(x_c):.4f}")
    print(f"  Signal range VIEW B: [{x_d.min():.4f}, {x_d.max():.4f}]  median={np.median(x_d):.4f}")
    if stats_norm is not None:
        x_n = df_norm["signal"].values
        print(f"  Signal range VIEW C: [{x_n.min():.4f}, {x_n.max():.4f}]  median={np.median(x_n):.4f}")
    if stats_skew is not None:
        x_s = df_skew["signal"].values
        print(f"  Signal range VIEW D: [{x_s.min():.4f}, {x_s.max():.4f}]  median={np.median(x_s):.4f}")

    # Decile stats tables
    hdr = (f"\n  {'Decile':>6}  {'n':>4}  {'MeanRet%':>9}  {'Sharpe':>7}  "
           f"{'WinRate':>8}  {'SigMid':>8}  {'AvgDisp':>8}")
    sep = "  " + "─" * 72

    def _print_stats(stats, title):
        print(hdr)
        print(f"  {title}")
        print(sep)
        for b in stats.index:
            r = stats.loc[b]
            print(f"  {b:>6}  {int(r['n']):>4}  {r['mean_ret']*100:>9.3f}  "
                  f"{r['sharpe']:>7.3f}  {r['win_rate']:>8.1%}  "
                  f"{r['sig_mid']:>8.4f}  {r['disp_mid']:>8.4f}")

    _print_stats(stats_corr, "VIEW A - Spearman Correlation")
    _print_stats(stats_disp, "VIEW B - Dispersion / BTC Vol")
    if stats_norm is not None:
        _print_stats(stats_norm, "VIEW C - Normalized Dispersion (disp / 90d median)")
    if stats_skew is not None:
        _print_stats(stats_skew, "VIEW D - Cross-sectional Skewness")

    # ── 7. Build chart: up to 5 rows × 3 cols ────────────────────────
    # Row 0 - View A: Spearman correlation
    # Row 1 - View B: Dispersion / BTC vol
    # Row 2 - View C: Normalized dispersion  (disp / 90d median)
    # Row 3 - View D: Cross-sectional skewness
    # Row 4 - Time series: corr + norm-disp + equity overlay (wide)
    n_view_rows = (2
                   + (1 if stats_norm is not None else 0)
                   + (1 if stats_skew is not None else 0))
    height_ratios = [1] * n_view_rows + [0.9]
    fig = plt.figure(figsize=(20, 6 * n_view_rows + 5))
    fig.suptitle(
        f"{label}\n"
        f"VIEW A: Spearman Corr  |  VIEW B: Disp/BTC Vol  |  "
        f"VIEW C: Norm Disp  |  VIEW D: Skewness\n"
        f"(rolling {corr_window}d, 1-day lag, active days only, {n_bins} deciles)",
        fontsize=12, fontweight="bold"
    )
    gs = gridspec.GridSpec(n_view_rows + 1, 3, figure=fig,
                           hspace=0.58, wspace=0.32,
                           height_ratios=height_ratios)

    # Row 0 - View A (Spearman correlation)
    ax_a_ret = fig.add_subplot(gs[0, 0])
    ax_a_sh  = fig.add_subplot(gs[0, 1])
    ax_a_sc  = fig.add_subplot(gs[0, 2])
    _draw_view([ax_a_ret, ax_a_sh, ax_a_sc], df_corr, stats_corr,
               sl_c, ic_c, r2_c, cc_c,
               f"Rolling {corr_window}d Spearman Corr (1d lag)",
               "VIEW A - Spearman Correlation")

    # Row 1 - View B (dispersion / BTC vol)
    ax_b_ret = fig.add_subplot(gs[1, 0])
    ax_b_sh  = fig.add_subplot(gs[1, 1])
    ax_b_sc  = fig.add_subplot(gs[1, 2])
    _draw_view([ax_b_ret, ax_b_sh, ax_b_sc], df_disp, stats_disp,
               sl_d, ic_d, r2_d, cc_d,
               disp_signal_label,
               "VIEW B - Dispersion / BTC Vol")

    # Row 2 - View C (normalized dispersion) - only if enough data
    row_c = 2
    if stats_norm is not None:
        ax_c_ret = fig.add_subplot(gs[row_c, 0])
        ax_c_sh  = fig.add_subplot(gs[row_c, 1])
        ax_c_sc  = fig.add_subplot(gs[row_c, 2])
        _draw_view([ax_c_ret, ax_c_sh, ax_c_sc], df_norm, stats_norm,
                   sl_n, ic_n, r2_n, cc_n,
                   dispersion_norm_label,
                   "VIEW C - Normalized Dispersion")

    # Row 3 - View D (cross-sectional skewness) - only if enough data
    row_d = row_c + (1 if stats_norm is not None else 0)
    ax_d_ret = None
    if stats_skew is not None:
        ax_d_ret = fig.add_subplot(gs[row_d, 0])
        ax_d_sh  = fig.add_subplot(gs[row_d, 1])
        ax_d_sc  = fig.add_subplot(gs[row_d, 2])
        _draw_view([ax_d_ret, ax_d_sh, ax_d_sc], df_skew, stats_skew,
                   sl_s, ic_s, r2_s, cc_s,
                   disp_skew_label,
                   "VIEW D - Cross-sectional Skewness")

    # Bottom row - regime time series: corr + norm-disp + equity overlay
    ax_ts = fig.add_subplot(gs[n_view_rows, :])
    full_corr      = rolling_corr.reindex(strat.index)
    full_norm_disp = dispersion_norm.reindex(strat.index)
    equity         = (1 + strat).cumprod()

    # Primary axis: rolling Spearman correlation
    ax_ts.fill_between(full_corr.index, full_corr.values,
                       alpha=0.18, color="steelblue")
    ax_ts.plot(full_corr.index, full_corr.values,
               color="steelblue", linewidth=0.9,
               label=f"Spearman corr ({corr_window}d, lag-1)")
    ax_ts.axhline(float(full_corr.median()), color="steelblue", linestyle="--",
                  linewidth=0.7, alpha=0.5)
    ax_ts.set_ylabel("Rolling Spearman Corr", color="steelblue", fontsize=8)
    ax_ts.tick_params(axis="y", labelcolor="steelblue")
    ax_ts.set_title("Regime Time Series: Correlation (blue)  +  Norm Dispersion (orange)  +  Equity (black)",
                    fontsize=9)

    # Secondary axis: normalized dispersion
    ax_ts2 = ax_ts.twinx()
    ax_ts2.plot(full_norm_disp.index, full_norm_disp.values,
                color="darkorange", linewidth=0.9, alpha=0.75,
                label="Norm disp (disp/90d median, lag-1)")
    ax_ts2.axhline(1.0, color="darkorange", linestyle=":", linewidth=0.7, alpha=0.5,
                   label="Norm disp = 1.0 (baseline)")
    ax_ts2.set_ylabel("Norm Dispersion", color="darkorange", fontsize=8)
    ax_ts2.tick_params(axis="y", labelcolor="darkorange")

    # Tertiary axis: equity (use a parasitic axis sharing x with ax_ts)
    ax_ts3 = ax_ts.twinx()
    ax_ts3.spines["right"].set_position(("axes", 1.06))  # offset third y-axis
    ax_ts3.plot(equity.index, equity.values,
                color="black", linewidth=1.4, label="Strategy equity")
    ax_ts3.set_ylabel("Equity Multiple", color="black", fontsize=8)
    ax_ts3.tick_params(axis="y", labelcolor="black")

    ax_ts.set_xlabel("Date", fontsize=8)
    lines1, labs1 = ax_ts.get_legend_handles_labels()
    lines2, labs2 = ax_ts2.get_legend_handles_labels()
    lines3, labs3 = ax_ts3.get_legend_handles_labels()
    ax_ts.legend(lines1 + lines2 + lines3, labs1 + labs2 + labs3,
                 fontsize=7, loc="upper left")

    # Verdict annotations (left margin, one per view row)
    verdict_y_positions = [0.96, 0.96, 0.96]  # will be computed from gs
    def _verdict_color(v):
        return ("#1a7a1a" if "✅" in v else "#b00000",
                "#f0fff4" if "✅" in v else "#fff0f0")

    # Place verdict text inside each view's first panel title
    view_axes     = [ax_a_ret, ax_b_ret]
    view_verdicts = [verdict_corr, verdict_disp]
    if stats_norm is not None:
        view_axes.append(ax_c_ret)
        view_verdicts.append(verdict_norm)
    if stats_skew is not None and ax_d_ret is not None:
        view_axes.append(ax_d_ret)
        view_verdicts.append(verdict_skew)

    for ax, vtext in zip(view_axes, view_verdicts):
        fc, bg = _verdict_color(vtext)
        ax.set_title(ax.get_title() + f"\n{vtext}", fontsize=7,
                     color=fc)

    fname   = f"{label.replace(' ', '_').replace('-','_')}_sharpe_vs_corr.png"
    outpath = Path(outdir) / fname
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Sharpe vs correlation chart saved: {fname}")

def plot_strategy_vs_btc_scatter(strat_returns, btc_returns, run_name, run_dir):
    df = pd.concat(
        [pd.Series(strat_returns, name="strategy"),pd.Series(btc_returns, name="btc")],
        axis=1
    ).dropna()

    if len(df) < 5:
        print("⚠ BTC scatter skipped - insufficient overlapping samples")
        return

    X = df["btc"].values.reshape(-1,1)
    y = df["strategy"].values

    model = LinearRegression().fit(X,y)

    beta = model.coef_[0]
    alpha = model.intercept_

    print(f"Alpha: {alpha:.6f}")
    print(f"Beta:  {beta:.3f}")

    # Plot
    plt.figure(figsize=(6,6))
    plt.scatter(df["btc"], df["strategy"], alpha=0.4)

    x_line = np.linspace(df["btc"].min(), df["btc"].max(), 100)
    y_line = alpha + beta * x_line

    plt.plot(x_line, y_line)

    plt.xlabel("BTC Return")
    plt.ylabel("Strategy Return")
    plt.title(f"{run_name} - Strategy vs BTC")

    plt.savefig(f"{run_dir}/{run_name}_strategy_vs_btc.png")
    plt.close()

def plot_strategy_vs_btc_vol(strategy_returns, btc_returns, label, outdir):
    df = pd.concat([
        pd.Series(strategy_returns, name="strategy"),
        pd.Series(btc_returns, name="btc")
    ], axis=1).dropna()

    # compute BTC volatility
    btc_vol = df["btc"].rolling(30).std()

    df = df.assign(vol=btc_vol).dropna()

    x = df["vol"].values
    y = df["strategy"].values

    # ---- GUARD ----
    if len(x) < 5:
        print("⚠ Strategy vs BTC vol skipped - insufficient overlapping samples")
        return

    # regression
    model = LinearRegression().fit(x.reshape(-1,1), y)

    beta = model.coef_[0]
    alpha = model.intercept_

    x_vals = np.linspace(x.min(), x.max(), 100)
    y_vals = alpha + beta * x_vals

    plt.figure(figsize=(6,6))

    plt.scatter(x, y, alpha=0.4)

    plt.plot(x_vals, y_vals, linewidth=2)

    plt.axhline(0)
    plt.axvline(np.median(x))

    plt.xlabel("BTC 20d Volatility")
    plt.ylabel("Strategy Return")

    plt.title(f"{label}\nReturn vs BTC Volatility")

    fname = f"{label.replace(' ','_')}_btc_vol_scatter.png"

    path = Path(outdir) / fname

    plt.tight_layout()
    plt.savefig(path)
    plt.close()

    print("  BTC volatility scatter saved:", fname)

def plot_regime_heatmap(strategy_returns, btc_returns, alt_returns, label, outdir):
    df = pd.DataFrame({
        "strategy": strategy_returns,
        "btc": btc_returns
    }).dropna()

    # BTC volatility
    btc_vol = df["btc"].rolling(20).std()

    # Cross-section dispersion
    dispersion = alt_returns.std(axis=1).reindex(df.index)

    df = pd.DataFrame({
        "strategy": df["strategy"],
        "btc_vol": btc_vol,
        "disp": dispersion
    }).dropna()

    if len(df) < 20:
        print("⚠ Regime heatmap skipped - insufficient samples")
        return

    vol_med = df["btc_vol"].median()
    disp_med = df["disp"].median()

    regimes = {
        "LowDisp_LowVol": df[(df["disp"] <= disp_med) & (df["btc_vol"] <= vol_med)],
        "LowDisp_HighVol": df[(df["disp"] <= disp_med) & (df["btc_vol"] > vol_med)],
        "HighDisp_LowVol": df[(df["disp"] > disp_med) & (df["btc_vol"] <= vol_med)],
        "HighDisp_HighVol": df[(df["disp"] > disp_med) & (df["btc_vol"] > vol_med)],
    }

    matrix = np.zeros((2,2))

    def sharpe(x):
        if len(x) < 5:
            return np.nan
        return np.sqrt(365) * x.mean() / x.std()

    matrix[0,0] = sharpe(regimes["LowDisp_LowVol"]["strategy"])
    matrix[0,1] = sharpe(regimes["LowDisp_HighVol"]["strategy"])
    matrix[1,0] = sharpe(regimes["HighDisp_LowVol"]["strategy"])
    matrix[1,1] = sharpe(regimes["HighDisp_HighVol"]["strategy"])

    fig, ax = plt.subplots(figsize=(6,6))

    im = ax.imshow(matrix)

    ax.set_xticks([0,1])
    ax.set_yticks([0,1])

    ax.set_xticklabels(["Low BTC Vol", "High BTC Vol"])
    ax.set_yticklabels(["Low Disp", "High Disp"])

    for i in range(2):
        for j in range(2):
            val = matrix[i,j]
            txt = "nan" if np.isnan(val) else f"{val:.2f}"
            ax.text(j, i, txt, ha="center", va="center")

    ax.set_title(f"{label}\nSharpe by Regime")

    fname = f"{label.replace(' ','_')}_regime_heatmap.png"

    path = Path(outdir) / fname

    plt.tight_layout()
    plt.savefig(path)
    plt.close()

    print("  Regime heatmap saved:", fname)

def regime_duration_analysis(strategy_returns, btc_returns, alt_returns, label):

    df = pd.DataFrame({"strategy": strategy_returns,"btc": btc_returns}).dropna()

    # BTC volatility
    btc_vol = df["btc"].rolling(20).std()

    # Cross-section dispersion
    dispersion = alt_returns.std(axis=1).reindex(df.index)

    df = pd.DataFrame({
        "strategy": df["strategy"],
        "btc_vol": btc_vol,
        "disp": dispersion
    }).dropna()

    if len(df) < 20:
        print("⚠ Regime duration analysis skipped - insufficient samples")
        return

    vol_med = df["btc_vol"].median()
    disp_med = df["disp"].median()

    # Define the regime your strategy likely thrives in
    df["good_regime"] = (df["disp"] > disp_med) & (df["btc_vol"] > vol_med)

    runs = []
    run_len = 0

    for flag in df["good_regime"]:

        if flag:
            run_len += 1
        else:
            if run_len > 0:
                runs.append(run_len)
                run_len = 0

    if run_len > 0:
        runs.append(run_len)

    if len(runs) == 0:
        print("\nREGIME DURATION - none detected")
        return

    runs = np.array(runs)

    print("\n════════════════════════════════════")
    print(f" REGIME DURATION ANALYSIS - {label}")
    print("════════════════════════════════════")

    print(f"Regime definition: High Dispersion + High BTC Vol")

    print(f"\nNumber of regimes: {len(runs)}")

    print(f"Mean duration: {runs.mean():.2f} days")
    print(f"Median duration: {np.median(runs):.2f} days")
    print(f"Max duration: {runs.max()} days")
    print(f"Min duration: {runs.min()} days")

    print(f"\nDistribution:")
    print(f"  <=3 days : {(runs <= 3).sum()}")
    print(f"  4-7 days: {((runs > 3) & (runs <= 7)).sum()}")
    print(f"  8-14 days: {((runs > 7) & (runs <= 14)).sum()}")
    print(f"  >14 days: {(runs > 14).sum()}")

def plot_skew_vs_equity(strategy_returns, alt_returns, label, outdir,
                        wf_fold_dates=None, unstable_folds=None):
    """
    Signal Quality vs Equity - cross-sectional skewness & dispersion over time.

    Panel 1 (top) : Equity curve + signal-collapse markers.
    Panel 2 (mid) : Rolling skewness (5d fast, 20d slow) + skew acceleration.
    Panel 3 (bot) : Normalised dispersion (disp / mean-abs-return).

    Fold bands: alternating blue/white. Unstable folds highlighted light red.
    Signal collapse: red dots on equity where skew_fast < 20th-pct rolling floor.

    Parameters
    ----------
    unstable_folds : list[int] | None
        1-based fold numbers that produced negative OOS Sharpe (e.g. [5, 8]).
    """

    # ── align ──────────────────────────────────────────────────────────
    strat = pd.Series(strategy_returns).sort_index()
    alt   = (pd.DataFrame(alt_returns) if not isinstance(alt_returns, pd.DataFrame)
             else alt_returns).sort_index()
    common = strat.index.intersection(alt.index)
    if len(common) < 20:
        print("  ⚠ plot_skew_vs_equity skipped - insufficient overlapping data")
        return
    strat = strat.loc[common]
    alt   = alt.loc[common]

    # ── signals (1-day lag - no lookahead) ────────────────────────────
    skew_raw = alt.skew(axis=1).shift(1)

    # Improvement 5: normalise dispersion so it is comparable across vol regimes
    mean_abs  = alt.abs().mean(axis=1).replace(0, np.nan)
    disp_raw  = (alt.std(axis=1, ddof=0) / (mean_abs + 1e-8)).shift(1)

    skew_fast   = skew_raw.rolling(5,  min_periods=3).mean()
    skew_slow   = skew_raw.rolling(20, min_periods=10).mean()
    disp_smooth = disp_raw.rolling(10, min_periods=5).mean()

    # Improvement 4: skew acceleration (3-day diff of fast signal)
    skew_accel = skew_fast.diff(3)

    # Improvement 2: signal collapse = skew_fast below its rolling 20th pct
    skew_thresh    = skew_fast.rolling(90, min_periods=30).quantile(0.20)
    signal_collapse = ((skew_fast < skew_thresh) & (skew_accel < 0))

    equity = (1 + strat).cumprod()

    # ── fold boundaries ────────────────────────────────────────────────
    n_days = len(strat)
    idx    = strat.index
    if wf_fold_dates is not None:
        boundaries = list(pd.to_datetime(wf_fold_dates))
    else:
        step = max(1, n_days // 8)
        boundaries = [idx[min(i * step, n_days - 1)] for i in range(1, 8)]

    all_bounds   = [idx[0]] + boundaries + [idx[-1]]
    fold_windows = [(all_bounds[i], all_bounds[i + 1])
                    for i in range(len(all_bounds) - 1)]

    # colour palette
    band_colors  = ["#f0f4ff", "#ffffff"]
    unstable_col = "#fff0f0"   # light red - Improvement 1

    # ── figure ─────────────────────────────────────────────────────────
    fig, (ax1, ax2, ax3) = plt.subplots(
        3, 1, figsize=(16, 11), sharex=True,
        gridspec_kw={"height_ratios": [2, 1.4, 1.1]}
    )
    unstable_note = (f"  ⚠ Unstable folds: {unstable_folds}" if unstable_folds else "")
    fig.suptitle(
        f"{label}\n"
        f"Skewness & Dispersion Signal Quality vs Equity  "
        f"(1-day lag, WFA fold boundaries shown){unstable_note}",
        fontsize=12, fontweight="bold"
    )
    fig.subplots_adjust(hspace=0.08)

    # ── fold shading - Improvement 1: unstable folds in red ───────────
    for fi, (t0, t1) in enumerate(fold_windows):
        if unstable_folds and (fi + 1) in unstable_folds:
            fc    = unstable_col
            alpha = 0.75
        else:
            fc    = band_colors[fi % 2]
            alpha = 0.60
        for ax in (ax1, ax2, ax3):
            ax.axvspan(t0, t1, facecolor=fc, alpha=alpha, zorder=0)

    # ── fold boundary lines ────────────────────────────────────────────
    for b in boundaries:
        for ax in (ax1, ax2, ax3):
            ax.axvline(b, color="#444444", linewidth=0.9,
                       linestyle="--", alpha=0.55, zorder=4)

    # ── panel 1: equity + signal-collapse scatter ─────────────────────
    ax1.plot(equity.index, equity.values,
             color="#1a3a6b", linewidth=1.6, label="Strategy equity", zorder=2)
    ax1.fill_between(equity.index, 1, equity.values,
                     where=equity.values >= 1, alpha=0.12, color="#1a3a6b")
    ax1.fill_between(equity.index, 1, equity.values,
                     where=equity.values < 1,  alpha=0.18, color="#b00000")
    ax1.axhline(1.0, color="grey", linewidth=0.7, linestyle="--", alpha=0.5)

    # Improvement 2: signal collapse dots on equity
    collapse_mask = signal_collapse.reindex(equity.index).fillna(False)
    collapse_idx  = equity.index[collapse_mask]
    ax1.scatter(collapse_idx, equity.loc[collapse_idx],
                color="red", s=14, alpha=0.55, zorder=5,
                label=f"Signal collapse ({len(collapse_idx)}d)")

    ax1.set_ylabel("Equity Multiple", fontsize=9)
    ax1.legend(loc="upper left", fontsize=8, ncol=2)
    ax1.set_title(
        "Panel 1 - Equity Curve  (red dots = signal collapse below 20th-pct skew)",
        fontsize=9, loc="left", pad=3)

    # fold labels after y-limits stabilise
    ylims = ax1.get_ylim()
    for fi, (t0, t1) in enumerate(fold_windows):
        mid   = t0 + (t1 - t0) / 2
        color = "#b00000" if (unstable_folds and (fi + 1) in unstable_folds) else "#444444"
        ax1.text(mid, ylims[0] + (ylims[1] - ylims[0]) * 0.02,
                 f"F{fi+1}", ha="center", va="bottom",
                 fontsize=7.5, color=color, fontweight="bold", zorder=5)

    # ── panel 2: skewness + acceleration ─────────────────────────────
    ax2.axhline(0, color="grey", linewidth=0.7, linestyle="--", alpha=0.5)
    ax2.fill_between(skew_fast.index, 0, skew_fast.values,
                     where=skew_fast.fillna(0) > 0, alpha=0.15, color="darkorange")
    ax2.fill_between(skew_fast.index, 0, skew_fast.values,
                     where=skew_fast.fillna(0) < 0, alpha=0.15, color="#b00000")
    ax2.plot(skew_fast.index, skew_fast.values,
             color="darkorange", linewidth=1.0, alpha=0.85, label="Skew 5d")
    ax2.plot(skew_slow.index, skew_slow.values,
             color="#7a3800",   linewidth=1.6,              label="Skew 20d")

    # Improvement 4: skew acceleration
    ax2.plot(skew_accel.index, skew_accel.values,
             color="purple", linewidth=0.8, alpha=0.50, label="Skew accel (3d diff)")

    # signal collapse threshold
    ax2.plot(skew_thresh.index, skew_thresh.values,
             color="red", linewidth=0.8, linestyle=":", alpha=0.70,
             label="Collapse floor (20th pct, 90d)")

    sk_med = float(skew_fast.median())
    ax2.axhline(sk_med, color="darkorange", linewidth=0.6, linestyle=":", alpha=0.6,
                label=f"Median={sk_med:.2f}")

    ax2.set_ylabel("Cross-sectional\nSkewness", fontsize=9)
    ax2.legend(loc="upper left", fontsize=7, ncol=5)
    ax2.set_title(
        "Panel 2 - Skewness (primary driver)  +  Skew Acceleration (purple)",
        fontsize=9, loc="left", pad=3)

    # ── panel 3: normalised dispersion ────────────────────────────────
    ax3.plot(disp_smooth.index, disp_smooth.values,
             color="#2a7a2a", linewidth=1.3, label="Norm. disp 10d")
    ax3.fill_between(disp_smooth.index,
                     float(disp_smooth.min(skipna=True)), disp_smooth.values,
                     alpha=0.15, color="#2a7a2a")

    d_med = float(disp_smooth.median())
    ax3.axhline(d_med,        color="#2a7a2a", linewidth=0.7, linestyle=":",
                alpha=0.7, label=f"Median={d_med:.4f}")
    ax3.axhline(d_med * 0.50, color="#b00000", linewidth=0.8, linestyle="--",
                alpha=0.6, label="50% threshold (filter)")

    ax3.set_ylabel("Norm. Dispersion\n(sigma / |mean|)", fontsize=9)
    ax3.set_xlabel("Date", fontsize=9)
    ax3.legend(loc="upper left", fontsize=7, ncol=3)
    ax3.set_title(
        "Panel 3 - Normalised Dispersion (secondary driver)  sigma / mean|return|",
        fontsize=9, loc="left", pad=3)

    # ── Improvement 3: per-fold regime summary ─────────────────────────
    print(f"\n  ── Skew vs Equity Diagnostics - {label} ──")
    print(f"  Skew fast (5d) :  mean={skew_fast.mean():.3f}  "
          f"std={skew_fast.std():.3f}  "
          f"% positive={100*(skew_fast>0).mean():.1f}%")
    print(f"  Skew slow (20d):  mean={skew_slow.mean():.3f}  "
          f"std={skew_slow.std():.3f}")
    print(f"  Norm disp (10d):  mean={disp_smooth.mean():.4f}  "
          f"std={disp_smooth.std():.4f}")
    print(f"  Signal collapse:  {int(signal_collapse.sum())} days  "
          f"({100*signal_collapse.mean():.1f}% of sample)")

    print(f"\n  {'Fold':>5}  {'Days':>5}  {'SkewMean':>9}  "
          f"{'Skew%>0':>8}  {'DispMean':>9}  {'Collapse%':>10}  {'Status':>10}")
    print("  " + "-" * 65)
    for fi, (t0, t1) in enumerate(fold_windows, 1):
        sub_sk = skew_fast.loc[t0:t1].dropna()
        sub_di = disp_smooth.loc[t0:t1].dropna()
        sub_co = signal_collapse.reindex(skew_fast.loc[t0:t1].index).fillna(False)
        if len(sub_sk) < 5:
            continue
        status = "UNSTABLE" if (unstable_folds and fi in unstable_folds) else "stable"
        print(f"  {fi:>5}  {len(sub_sk):>5}  {sub_sk.mean():>9.3f}  "
              f"{100*(sub_sk>0).mean():>7.1f}%  {sub_di.mean():>9.4f}  "
              f"{100*float(sub_co.mean()):>9.1f}%  {status:>10}")

    print()
    pairs = pd.DataFrame({"skew": skew_fast, "ret": strat}).dropna()
    if len(pairs) >= 20:
        r, p = spearmanr(pairs["skew"], pairs["ret"])
        print(f"  Spearman(skew_fast, ret): r={r:.3f}  p={p:.4f}  "
              f"({'significant' if p < 0.05 else 'not significant'})")
    pairs_d = pd.DataFrame({"disp": disp_smooth, "ret": strat}).dropna()
    if len(pairs_d) >= 20:
        r2, p2 = spearmanr(pairs_d["disp"], pairs_d["ret"])
        print(f"  Spearman(norm_disp, ret): r={r2:.3f}  p={p2:.4f}  "
              f"({'significant' if p2 < 0.05 else 'not significant'})")

    fname   = f"{label.replace(' ', '_').replace('-','_').replace('+','_')}_skew_vs_equity.png"
    outpath = Path(outdir) / fname
    fig.savefig(outpath, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Skew vs equity chart saved: {fname}")


def dispersion_decile_expectancy(strategy_returns, alt_returns, label="", outdir=None):
    """
    Splits trading days into 10 equal-frequency dispersion buckets and reports
    mean return, Sharpe, and win-rate per bucket.

    Uses explicit index alignment before joining - avoids silent NaN-drop when
    strategy_returns has a trimmed DatetimeIndex vs alt_returns full history.
    Saves a bar chart to outdir if provided.
    """

    dispersion = alt_returns.std(axis=1)

    # Explicit alignment - critical to avoid positional join on mismatched indices
    strat = pd.Series(strategy_returns, name="strategy")
    disp  = dispersion.reindex(strat.index)

    df = pd.concat([strat, disp.rename("disp")], axis=1).dropna()

    if len(df) < 50:
        print("⚠ Dispersion decile analysis skipped - insufficient aligned samples "
              f"({len(df)} rows after join)")
        return

    df["decile"] = pd.qcut(df["disp"], 10, labels=False, duplicates="drop")

    print("\n════════════════════════════════════════════════════════")
    print(" DISPERSION DECILE EXPECTANCY")
    print(f"  (strategy days sorted into 10 equal-freq dispersion buckets)")
    print("════════════════════════════════════════════════════════")
    print(f"  {'Decile':>7}  {'Days':>5}  {'DispLow':>8}  {'DispHigh':>9}  "
          f"{'MeanRet':>8}  {'WinRate':>8}  {'Sharpe':>7}")
    print("  " + "─" * 62)

    results = []
    sharpes = []

    for d in sorted(df["decile"].unique()):
        sub  = df[df["decile"] == d]
        rets = sub["strategy"]

        if len(rets) < 5:
            continue

        mean    = rets.mean()
        std     = rets.std()
        sharpe  = np.sqrt(365) * mean / std if std > 0 else float("nan")
        winrate = (rets > 0).mean() * 100
        dlo     = sub["disp"].min()
        dhi     = sub["disp"].max()

        print(f"  Decile {int(d)+1:2d}  "
              f"{len(rets):5d}  "
              f"{dlo:8.4f}  "
              f"{dhi:9.4f}  "
              f"{mean*100:7.2f}%  "
              f"{winrate:7.1f}%  "
              f"{sharpe:7.2f}")

        results.append((int(d) + 1, mean, sharpe, winrate))
        sharpes.append(sharpe)

    # ── Chart ──────────────────────────────────────────────────────────
    if outdir and results:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        fig.suptitle(f"{label} - Dispersion Decile Expectancy" if label
                     else "Dispersion Decile Expectancy", fontsize=11)

        xs = [r[0] for r in results]

        ax = axes[0]
        colors = ["#d62728" if r[1] < 0 else "#2ca02c" for r in results]
        ax.bar(xs, [r[1] * 100 for r in results], color=colors, alpha=0.8)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_xlabel("Dispersion Decile")
        ax.set_ylabel("Mean Daily Return (%)")
        ax.set_title("Return by Dispersion Decile")
        ax.set_xticks(xs)

        ax = axes[1]
        colors2 = ["#d62728" if s < 0 else "#1f77b4" for s in sharpes]
        ax.bar(xs, sharpes, color=colors2, alpha=0.8)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_xlabel("Dispersion Decile")
        ax.set_ylabel("Sharpe Ratio")
        ax.set_title("Sharpe by Dispersion Decile")
        ax.set_xticks(xs)

        plt.tight_layout()
        fname = f"{label.replace(' ', '_').replace('-','').strip()}_disp_decile.png" \
                if label else "disp_decile.png"
        fpath = f"{outdir}/{fname}"
        plt.savefig(fpath, dpi=130, bbox_inches="tight")
        plt.close()
        print(f"  Decile chart saved: {fpath}")

    return results

def dispersion_threshold_surface(strategy_returns, alt_returns, label="", outdir=None):
    """
    Sweeps dispersion band filter parameters and plots Sharpe at each setting.

    Uses returns-masking instead of re-running simulate() - filtered days are
    set to 0.0 return (same as live), active days keep their actual return.
    This is fast, correct, and requires no access to df_4x or config params.

    Two sweeps:
      A) Fix low_pct=0.20, sweep high_pct 0.50->0.95
      B) Fix high_pct=0.70, sweep low_pct 0.05->0.40
    """

    strat      = pd.Series(strategy_returns, name="strategy")
    dispersion = alt_returns.std(axis=1).reindex(strat.index)
    df         = pd.concat([strat, dispersion.rename("disp")], axis=1).dropna()

    if len(df) < 50:
        print("⚠ Dispersion threshold surface skipped - insufficient aligned samples "
              f"({len(df)} rows)")
        return

    print("\n════════════════════════════════════════════════════════")
    print(" DISPERSION THRESHOLD SURFACE")
    print("  (flat days = 0% return, same as live trading)")
    print("════════════════════════════════════════════════════════")

    n_total = len(df)
    sharpe_no_filter = np.sqrt(365) * df["strategy"].mean() / df["strategy"].std()

    def _masked_sharpe(low_q, high_q):
        lo    = df["disp"].quantile(low_q)
        hi    = df["disp"].quantile(high_q)
        trade = (df["disp"] <= lo) | (df["disp"] >= hi)
        masked = df["strategy"].where(trade, other=0.0)
        if masked.std() == 0:
            return float("nan"), int((~trade).sum())
        return np.sqrt(365) * masked.mean() / masked.std(), int((~trade).sum())

    # ── Sweep A ────────────────────────────────────────────────────────
    print(f"\n  Sweep A - low_pct=0.20 fixed, varying high_pct:")
    print(f"  {'high_pct':>9}  {'Flat':>6}  {'Active%':>8}  {'Sharpe':>7}")
    print("  " + "─" * 36)
    sweep_a_x, sweep_a_y = [], []
    for hp in np.arange(0.50, 0.96, 0.05):
        sh, flat = _masked_sharpe(0.20, hp)
        marker = " ◄" if abs(hp - DISPERSION_THRESHOLD) < 0.01 else ""
        print(f"  {hp:9.2f}  {flat:6d}  {(n_total-flat)/n_total*100:7.1f}%  {sh:7.3f}{marker}")
        sweep_a_x.append(hp); sweep_a_y.append(sh)

    # ── Sweep B ────────────────────────────────────────────────────────
    print(f"\n  Sweep B - high_pct=0.70 fixed, varying low_pct:")
    print(f"  {'low_pct':>8}  {'Flat':>6}  {'Active%':>8}  {'Sharpe':>7}")
    print("  " + "─" * 36)
    sweep_b_x, sweep_b_y = [], []
    for lp in np.arange(0.05, 0.41, 0.05):
        sh, flat = _masked_sharpe(lp, 0.70)
        print(f"  {lp:8.2f}  {flat:6d}  {(n_total-flat)/n_total*100:7.1f}%  {sh:7.3f}")
        sweep_b_x.append(lp); sweep_b_y.append(sh)

    print(f"\n  No-filter baseline Sharpe: {sharpe_no_filter:.3f}")

    # ── Chart ──────────────────────────────────────────────────────────
    if outdir:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5))
        fig.suptitle(f"{label} - Dispersion Threshold Surface" if label
                     else "Dispersion Threshold Surface", fontsize=11)

        for ax, xs, ys, xlabel, cur in [
            (axes[0], sweep_a_x, sweep_a_y, "high_pct (upper cutoff)", DISPERSION_THRESHOLD),
            (axes[1], sweep_b_x, sweep_b_y, "low_pct (lower cutoff)",  0.20),
        ]:
            ax.plot(xs, ys, marker="o", linewidth=1.8)
            ax.axhline(sharpe_no_filter, color="gray", linestyle="--",
                       linewidth=1.2, label=f"No filter ({sharpe_no_filter:.2f})")
            ax.axvline(cur, color="red", linestyle="--",
                       linewidth=1.2, label=f"Current ({cur:.2f})")
            ax.set_xlabel(xlabel)
            ax.set_ylabel("Sharpe (flat days = 0%)")
            ax.legend(fontsize=8)
            ax.grid(alpha=0.3)

        axes[0].set_title("Sweep A: fix low=0.20, vary high")
        axes[1].set_title("Sweep B: fix high=0.70, vary low")

        plt.tight_layout()
        fname = f"{label.replace(' ', '_').replace('-','').strip()}_disp_surface.png" \
                if label else "disp_surface.png"
        fpath = f"{outdir}/{fname}"
        plt.savefig(fpath, dpi=130, bbox_inches="tight")
        plt.close()
        print(f"  Threshold surface chart saved: {fpath}")

    return {"sweep_a": list(zip(sweep_a_x, sweep_a_y)),
            "sweep_b": list(zip(sweep_b_x, sweep_b_y))}

# ──────────────────────────────────────────────────────────────────────
# V5-A: HMM with vol + FR + F&G emissions  (60/40 split, 2 states)
# Hypothesis: cleaner regime signal than price - less noise overfitting
# ──────────────────────────────────────────────────────────────────────

def build_v5a_filter(btc_ohlcv: pd.DataFrame,fr: pd.Series, fg: pd.Series) -> pd.Series:
    """HMM, 2 states, emissions = rvol_10d + rvol_20d + FR + FG_7dMA."""

    print("    Building V5-A: HMM | vol+FR+F&G emissions | 2 states | 60/40")

    idx    = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    close  = btc_ohlcv["close"].sort_index()
    lr     = np.log(close / close.shift(1))

    feats  = pd.DataFrame(index=btc_ohlcv.index)
    feats["rvol_10d"] = lr.rolling(10, min_periods=5).std() * np.sqrt(ANNUALIZATION_FACTOR)
    feats["rvol_20d"] = lr.rolling(20, min_periods=10).std() * np.sqrt(ANNUALIZATION_FACTOR)

    # FR and F&G - reindex to OHLCV dates, forward-fill
    fr_r = fr.reindex(btc_ohlcv.index).ffill()
    fg_r = fg.reindex(btc_ohlcv.index).ffill()
    feats["fr"]      = fr_r
    feats["fg_7dma"] = fg_r.rolling(7, min_periods=4).mean()

    feats  = feats.shift(1).dropna()
    feats_r = feats.reindex(idx).ffill()

    cal_bad    = pd.Series(False, index=idx)
    for s, e in CALENDAR_WINDOWS:
        cal_bad |= (idx >= s) & (idx <= e)

    valid_mask = feats_r.notna().all(axis=1)
    valid_idx  = idx[valid_mask]
    split      = int(len(valid_idx) * TRAIN_TEST_SPLIT_RATIO)
    train_idx  = valid_idx[:split]
    pred_idx   = valid_idx[split:]

    # Calendar isolation: exclude calendar-bad days from HMM training.
    cal_train_mask = ~cal_bad.reindex(train_idx, fill_value=False)
    hmm_train_idx  = train_idx[cal_train_mask]
    print(f"    Calendar isolation: {len(train_idx)-len(hmm_train_idx)}d excluded "
          f"from HMM training ({len(hmm_train_idx)} clean days remain)")
    X_train = feats_r.loc[hmm_train_idx].values
    X_pred  = feats_r.loc[pred_idx].values
    scaler  = StandardScaler().fit(X_train)
    # emission_col=0 = rvol_10d (higher vol -> bad)
    # so we want states with HIGHER mean rvol -> bad -> invert: use -rvol
    Xs_train = scaler.transform(X_train)
    Xs_pred  = scaler.transform(X_pred)
    # For rvol emissions the BAD state has HIGHER rvol - flip emission_col sign
    Xs_train_flipped = Xs_train.copy(); Xs_train_flipped[:, 0] *= -1
    Xs_pred_flipped  = Xs_pred.copy();  Xs_pred_flipped[:, 0]  *= -1

    bad_pred = _hmm_bad_series(
        X_train=Xs_train_flipped, dates_train=hmm_train_idx,
        n_states=2, bad_state_count=1, emission_col=0,
        X_pred=Xs_pred_flipped, dates_pred=pred_idx,
    )

    return _assemble_filter(bad_pred, train_idx, cal_bad, idx, "V5-A")


# ──────────────────────────────────────────────────────────────────────
# V5-B: HMM, 3 states, price features  (60/40 split)
# Hypothesis: separating "slow grind" from "crash" avoids over-filtering
# ──────────────────────────────────────────────────────────────────────

def build_v5b_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """HMM, 3 states (bad+neutral both flagged), price features, 60/40."""

    print("    Building V5-B: HMM | price features | 3 states | 60/40")

    idx     = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r = _price_features(btc_ohlcv, idx)
    cal_bad = _cal_bad_series(idx)

    valid_mask = feats_r.notna().all(axis=1)
    valid_idx  = idx[valid_mask]
    split      = int(len(valid_idx) * TRAIN_TEST_SPLIT_RATIO)
    train_idx  = valid_idx[:split]
    pred_idx   = valid_idx[split:]

    # Calendar isolation: exclude calendar-bad days from HMM training.
    cal_train_mask = ~cal_bad.reindex(train_idx, fill_value=False)
    hmm_train_idx  = train_idx[cal_train_mask]
    print(f"    Calendar isolation: {len(train_idx)-len(hmm_train_idx)}d excluded "
          f"from HMM training ({len(hmm_train_idx)} clean days remain)")
    X_train = feats_r.loc[hmm_train_idx].values
    X_pred  = feats_r.loc[pred_idx].values
    scaler  = StandardScaler().fit(X_train)
    Xs_tr = scaler.transform(X_train)
    Xs_pr = scaler.transform(X_pred)

    # 3 states - flag bottom 2 (lowest mean ret_1d), train-only fit
    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=hmm_train_idx,
        n_states=3, bad_state_count=2, emission_col=0,
        X_pred=Xs_pr, dates_pred=pred_idx,
    )

    n_flagged = bad_pred.sum()
    print(f"      3-state HMM flagged {n_flagged} days in pred window")
    return _assemble_filter(bad_pred, train_idx, cal_bad, idx, "V5-B")


# ──────────────────────────────────────────────────────────────────────
# V5-B MAJORITY: same as V5-B but averaged over N seeds + configurable threshold
# ──────────────────────────────────────────────────────────────────────

def build_v5b_filter_majority(btc_ohlcv: pd.DataFrame,n_seeds: int = 30,vote_threshold: float = 0.50,) -> pd.Series:
    """
    Run V5-B (3-state HMM, price features, 60/40 split) with `n_seeds`
    different random seeds.  A day is flagged bad only if at least
    `vote_threshold` fraction of seeds agree it is bad.

    This removes sensitivity to a single lucky/unlucky HMM initialisation
    and produces a filter that reflects the true stochastic variance of
    the model - the same one that would be used in live deployment.
    """


    threshold_label = f"{vote_threshold*100:.0f}%"
    print(f"    Building V5-B Majority: 3-state HMM | {n_seeds} seeds | "
          f"vote threshold >={threshold_label}")

    idx     = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r = _price_features(btc_ohlcv, idx)
    cal_bad = _cal_bad_series(idx)

    valid_mask = feats_r.notna().all(axis=1)
    valid_idx  = idx[valid_mask]
    split      = int(len(valid_idx) * TRAIN_TEST_SPLIT_RATIO)
    train_idx  = valid_idx[:split]
    pred_idx   = valid_idx[split:]

    # ── Calendar isolation: exclude calendar-bad days from HMM training ──
    # The HMM must discover regime structure from unsupervised clustering.
    # Including calendar-bad days would let their return/vol profile shape
    # the bad-state centroid, effectively smuggling in a supervised signal.
    # Calendar is applied as a hard gate in _assemble_filter afterwards.
    cal_train_mask = ~cal_bad.reindex(train_idx, fill_value=False)
    hmm_train_idx  = train_idx[cal_train_mask]
    cal_excluded   = len(train_idx) - len(hmm_train_idx)
    print(f"    Calendar isolation: {cal_excluded} calendar-bad days excluded from HMM "
          f"training ({len(hmm_train_idx)} clean days remain)")

    X_train  = feats_r.loc[hmm_train_idx].values
    X_pred   = feats_r.loc[pred_idx].values
    scaler   = StandardScaler().fit(X_train)
    Xs_train = scaler.transform(X_train)
    Xs_pred  = scaler.transform(X_pred)   # scaled with TRAIN params only

    print(f"    Walk-forward split: train {len(train_idx)}d "
          f"({train_idx[0].date()} -> {train_idx[-1].date()})  |  "
          f"test {len(pred_idx)}d "
          f"({pred_idx[0].date()} -> {pred_idx[-1].date()})")
    print(f"    For each seed:")
    print(f"      1. Fit HMM on {len(hmm_train_idx)} clean train days (calendar-bad excluded)")
    print(f"      2. Map risk-off states by lowest mean ret_1d in clean train window")
    print(f"      3. Predict states for test window using fitted model")
    print(f"      4. Calendar applied as hard gate in _assemble_filter (independent layer)")

    # Strategy-conditional return series for train window (ret_1d, lagged)
    # Used to anchor bad-state mapping to actual P&L, not feature value.
    strat_ret_train = feats_r["ret_1d"].reindex(hmm_train_idx)

    # Accumulate per-day vote counts over the OOS prediction window only
    vote_counts = pd.Series(0, index=pred_idx, dtype=float)

    for seed in range(n_seeds):
        bad_pred = _hmm_bad_series(
            X_train     = Xs_train,
            dates_train = hmm_train_idx,
            n_states    = 3,
            bad_state_count = 2,
            emission_col    = 0,
            random_state    = seed,
            X_pred      = Xs_pred,
            dates_pred  = pred_idx,
            strategy_returns_train = strat_ret_train,
        )
        vote_counts += bad_pred.astype(float)

    # Flag day if >= vote_threshold of seeds agree it is bad
    min_votes   = vote_threshold * n_seeds
    bad_pred_mv = vote_counts >= min_votes

    # Seed stability diagnostics
    always_bad  = (vote_counts == n_seeds).sum()
    always_good = (vote_counts == 0).sum()
    contested   = ((vote_counts > 0) & (vote_counts < n_seeds)).sum()
    print(f"      Seed stability across {n_seeds} seeds:")
    print(f"        Always bad  (all {n_seeds} seeds agree): {always_bad:3d} days")
    print(f"        Always good (no seeds flag):             {always_good:3d} days")
    print(f"        Contested   (split vote):                {contested:3d} days")
    print(f"        Flagged after >={threshold_label} vote (>={min_votes:.0f} seeds): "
          f"{bad_pred_mv.sum()} OOS days")

    # ── Diagnostic HMM: seed-0 only, train-only fit, for state reporting ──
    try:
        _b_hmm = GaussianHMM(n_components=3, covariance_type="full",
                        n_iter=200, random_state=0)
        _b_hmm.fit(Xs_train)
        _b_tr    = _b_hmm.predict(Xs_train)
        _b_means = {s: Xs_train[_b_tr == s, 0].mean()
                    for s in range(3) if (_b_tr == s).any()}
        _sorted  = sorted(_b_means, key=_b_means.get)
        _b_bad   = set(_sorted[:2])   # bottom 2 states = risk-off
        _hmm_diag_build(
            hmm         = _b_hmm,
            X_train     = Xs_train,
            X_pred      = Xs_pred,
            dates_train = hmm_train_idx,
            dates_pred  = pred_idx,
            n_states    = 3,
            bad_states  = _b_bad,
            label       = "V5-B Majority",
        )
    except Exception as _e:
        print(f"      [V5-B diagnostics skipped: {_e}]")

    return _assemble_filter(bad_pred_mv, train_idx, cal_bad, idx,
                            f"V5-B Majority(n={n_seeds},thr={vote_threshold})")


# ──────────────────────────────────────────────────────────────────────
# V5-C: HMM, price features, 2 states, expanding window (monthly retrain)
# Hypothesis: more training data per prediction reduces over-flagging
# ──────────────────────────────────────────────────────────────────────

def build_v5c_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """HMM, 2 states, price features, expanding window (retrain every 30d)."""
    print("    Building V5-C: HMM | price features | 2 states | expanding window")

    idx     = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r = _price_features(btc_ohlcv, idx)
    cal_bad = _cal_bad_series(idx)

    bad_expanding = _expanding_hmm(
        feats_r, idx,
        n_states=2, bad_state_count=1,
        min_train_days=90, retrain_every=30,
        emission_col=0,
    )

    # Expanding window: HMM predictions fill non-calendar days only.
    # Calendar labels are sacrosanct - never overwrite them with HMM output.
    bad_full = cal_bad.copy()
    non_cal_predicted = bad_expanding.index[
        bad_expanding.index.isin(idx) & ~cal_bad.reindex(bad_expanding.index, fill_value=False)
    ]
    bad_full.loc[non_cal_predicted] = bad_expanding.loc[non_cal_predicted].values

    n_bad = bad_full.sum()
    print(f"    V5-C: {n_bad} days flagged ({n_bad/len(idx)*100:.1f}%)")
    for s, e in CALENDAR_WINDOWS:
        mask    = (idx >= s) & (idx <= e)
        total   = mask.sum()
        flagged = bad_full[mask].sum()
        pct_w   = flagged / total * 100 if total > 0 else 0
        ok = "✅" if pct_w >= 80 else ("⚠️ " if pct_w >= 50 else "❌")
        print(f"      {s[:10]} -> {e[:10]}: {flagged}/{total} ({pct_w:.0f}%)  {ok}")
    return bad_full


# ──────────────────────────────────────────────────────────────────────
# V5-D: HMM only (no KMeans/RF), 2 states, price features, 60/40
# Hypothesis: RF trained on calendar labels is causing false positives
# ──────────────────────────────────────────────────────────────────────

def build_v5d_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """HMM only, 2 states, price features, 60/40 - no ensemble voting."""

    print("    Building V5-D: HMM ONLY | price features | 2 states | 60/40")

    idx     = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r = _price_features(btc_ohlcv, idx)
    cal_bad = _cal_bad_series(idx)

    valid_mask = feats_r.notna().all(axis=1)
    valid_idx  = idx[valid_mask]
    split      = int(len(valid_idx) * TRAIN_TEST_SPLIT_RATIO)
    train_idx  = valid_idx[:split]
    pred_idx   = valid_idx[split:]

    # Calendar isolation: exclude calendar-bad days from HMM training.
    cal_train_mask = ~cal_bad.reindex(train_idx, fill_value=False)
    hmm_train_idx  = train_idx[cal_train_mask]
    print(f"    Calendar isolation: {len(train_idx)-len(hmm_train_idx)}d excluded "
          f"from HMM training ({len(hmm_train_idx)} clean days remain)")
    X_train_s = feats_r.loc[hmm_train_idx].values
    X_pred_s  = feats_r.loc[pred_idx].values
    scaler    = StandardScaler().fit(X_train_s)
    Xs_tr     = scaler.transform(X_train_s)
    Xs_pr     = scaler.transform(X_pred_s)

    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=hmm_train_idx,
        n_states=2, bad_state_count=1, emission_col=0,
        X_pred=Xs_pr, dates_pred=pred_idx,
    )
    print(f"      HMM-only flagged {bad_pred.sum()} days in pred window")
    return _assemble_filter(bad_pred, train_idx, cal_bad, idx, "V5-D")


def build_v5d_filter_majority(btc_ohlcv: pd.DataFrame,n_seeds: int = 30,vote_threshold: float = 0.70) -> pd.Series:
    """
    V5-D majority-vote filter - 2-state HMM, more stable across seeds than V5-B.
    A day is flagged bad only if >= vote_threshold fraction of seeds agree.
    """

    import math as _math
    print(f"    Building V5-D Majority: 2-state HMM | {n_seeds} seeds | "
          f"vote threshold \u2265{vote_threshold*100:.0f}%")

    idx     = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r = _price_features(btc_ohlcv, idx)
    cal_bad = _cal_bad_series(idx)

    valid_mask = feats_r.notna().all(axis=1)
    valid_idx  = idx[valid_mask]
    split      = int(len(valid_idx) * TRAIN_TEST_SPLIT_RATIO)
    train_idx  = valid_idx[:split]
    pred_idx   = valid_idx[split:]

    # ── Calendar isolation: exclude calendar-bad days from HMM training ──
    # The HMM must discover regime structure from unsupervised clustering.
    # Including calendar-bad days would let their return/vol profile shape
    # the bad-state centroid, effectively smuggling in a supervised signal.
    # Calendar is applied as a hard gate in _assemble_filter afterwards.
    cal_train_mask = ~cal_bad.reindex(train_idx, fill_value=False)
    hmm_train_idx  = train_idx[cal_train_mask]
    cal_excluded   = len(train_idx) - len(hmm_train_idx)
    print(f"    Calendar isolation: {cal_excluded} calendar-bad days excluded from HMM "
          f"training ({len(hmm_train_idx)} clean days remain)")

    X_train  = feats_r.loc[hmm_train_idx].values
    X_pred   = feats_r.loc[pred_idx].values
    scaler   = StandardScaler().fit(X_train)
    Xs_train = scaler.transform(X_train)
    Xs_pred  = scaler.transform(X_pred)   # scaled with TRAIN params only

    print(f"    Walk-forward split: train {len(train_idx)}d "
          f"({train_idx[0].date()} -> {train_idx[-1].date()})  |  "
          f"test {len(pred_idx)}d "
          f"({pred_idx[0].date()} -> {pred_idx[-1].date()})")
    print(f"    For each seed:")
    print(f"      1. Fit HMM on {len(hmm_train_idx)} clean train days (calendar-bad excluded)")
    print(f"      2. Map risk-off state by lowest mean ret_1d in clean train window")
    print(f"      3. Predict states for test window using fitted model")
    print(f"      4. Calendar applied as hard gate in _assemble_filter (independent layer)")

    vote_counts = pd.Series(0, index=pred_idx, dtype=float)
    # Strategy-conditional return series for train window (ret_1d, lagged)
    # Used to anchor bad-state mapping to actual P&L, not feature value.
    strat_ret_train = feats_r["ret_1d"].reindex(hmm_train_idx)

    for seed in range(n_seeds):
        # Each call: fits on Xs_train (clean days only), maps bad states from
        # clean-train stats, predicts on Xs_pred - calendar excluded from fit.
        bad_pred_seed = _hmm_bad_series(
            X_train     = Xs_train,
            dates_train = hmm_train_idx,
            n_states    = 2,
            bad_state_count = 1,
            emission_col    = 0,
            random_state    = seed,
            X_pred      = Xs_pred,
            dates_pred  = pred_idx,
            strategy_returns_train = strat_ret_train,
        )
        vote_counts += bad_pred_seed.astype(float)

    min_votes   = vote_threshold * n_seeds
    bad_pred_mv = vote_counts >= min_votes

    always_bad  = (vote_counts == n_seeds).sum()
    always_good = (vote_counts == 0).sum()
    contested   = ((vote_counts > 0) & (vote_counts < n_seeds)).sum()
    print(f"    Seed stability across {n_seeds} seeds (test window only):")
    print(f"      Always bad  (all {n_seeds} seeds agree): {int(always_bad):3d} days")
    print(f"      Always good (no seeds flag):             {int(always_good):3d} days")
    print(f"      Contested   (split vote):                {int(contested):3d} days")
    print(f"      Flagged after >={vote_threshold*100:.0f}% vote "
          f"(>={min_votes:.0f} seeds): {int(bad_pred_mv.sum())} OOS days")

    # ── Diagnostic HMM: seed-0 only, train-only fit, for state reporting ──
    # Does NOT affect filter output. Returns are injected later via
    # _hmm_diag_inject_returns() once the trading sim result is available.
    try:
        _d_hmm = GaussianHMM(n_components=2, covariance_type="full",
                        n_iter=200, random_state=0)
        _d_hmm.fit(Xs_train)
        _d_tr    = _d_hmm.predict(Xs_train)
        _d_means = {s: Xs_train[_d_tr == s, 0].mean()
                    for s in range(2) if (_d_tr == s).any()}
        _d_bad   = {min(_d_means, key=_d_means.get)}
        _hmm_diag_build(
            hmm         = _d_hmm,
            X_train     = Xs_train,
            X_pred      = Xs_pred,
            dates_train = hmm_train_idx,
            dates_pred  = pred_idx,
            n_states    = 2,
            bad_states  = _d_bad,
            label       = "V5-D Majority",
        )
    except Exception as _e:
        print(f"      [V5-D diagnostics skipped: {_e}]")

    return _assemble_filter(bad_pred_mv, train_idx, cal_bad, idx,
                            f"V5-D Majority(n={n_seeds},thr={vote_threshold})")


# ──────────────────────────────────────────────────────────────────────
# V5-E: HMM, vol+FR+F&G emissions, 3 states, expanding window
# Combined best hypothesis from A + B + C
# ──────────────────────────────────────────────────────────────────────

def build_v5e_filter(btc_ohlcv: pd.DataFrame,fr: pd.Series, fg: pd.Series) -> pd.Series:
    """HMM, 3 states (bottom 2 = bad), vol+FR+F&G, expanding window."""
    print("    Building V5-E: HMM | vol+FR+F&G | 3 states | expanding window")

    idx   = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    close = btc_ohlcv["close"].sort_index()
    lr    = np.log(close / close.shift(1))

    feats = pd.DataFrame(index=btc_ohlcv.index)
    feats["rvol_10d"] = lr.rolling(10, min_periods=5).std() * np.sqrt(ANNUALIZATION_FACTOR)
    feats["rvol_20d"] = lr.rolling(20, min_periods=10).std() * np.sqrt(ANNUALIZATION_FACTOR)
    fr_r = fr.reindex(btc_ohlcv.index).ffill()
    fg_r = fg.reindex(btc_ohlcv.index).ffill()
    feats["fr"]      = fr_r
    feats["fg_7dma"] = fg_r.rolling(7, min_periods=4).mean()

    feats   = feats.shift(1).dropna()
    feats_r = feats.reindex(idx).ffill()

    cal_bad = pd.Series(False, index=idx)
    for s, e in CALENDAR_WINDOWS:
        cal_bad |= (idx >= s) & (idx <= e)

    # For vol emissions: higher rvol = bad, so flip col 0 sign before ranking
    # We handle this by negating rvol columns in a copy
    feats_flipped = feats_r.copy()
    feats_flipped["rvol_10d"] *= -1
    feats_flipped["rvol_20d"] *= -1

    bad_expanding = _expanding_hmm(
        feats_flipped, idx,
        n_states=3, bad_state_count=2,
        min_train_days=90, retrain_every=30,
        emission_col=0,
    )

    bad_full = cal_bad.copy()
    non_cal_predicted = bad_expanding.index[
        bad_expanding.index.isin(idx) & ~cal_bad.reindex(bad_expanding.index, fill_value=False)
    ]
    bad_full.loc[non_cal_predicted] = bad_expanding.loc[non_cal_predicted].values

    n_bad = bad_full.sum()
    print(f"    V5-E: {n_bad} days flagged ({n_bad/len(idx)*100:.1f}%)")
    for s, e in CALENDAR_WINDOWS:
        mask    = (idx >= s) & (idx <= e)
        total   = mask.sum()
        flagged = bad_full[mask].sum()
        pct_w   = flagged / total * 100 if total > 0 else 0
        ok = "✅" if pct_w >= 80 else ("⚠️ " if pct_w >= 50 else "❌")
        print(f"      {s[:10]} -> {e[:10]}: {flagged}/{total} ({pct_w:.0f}%)  {ok}")
    return bad_full


# ══════════════════════════════════════════════════════════════════════
# SHARED FEATURE BUILDERS
# ══════════════════════════════════════════════════════════════════════

def _price_features(btc_ohlcv: pd.DataFrame,idx: pd.DatetimeIndex) -> pd.DataFrame:
    """10 price-based features, lagged 1 day, reindexed to idx."""
    close = btc_ohlcv["close"].sort_index()
    lr    = np.log(close / close.shift(1))
    f = pd.DataFrame(index=btc_ohlcv.index)
    f["ret_1d"]    = lr
    f["ret_5d"]    = np.log(close / close.shift(5))
    f["ret_10d"]   = np.log(close / close.shift(10))
    f["ret_20d"]   = np.log(close / close.shift(20))
    f["ma20_dist"] = close / close.rolling(20).mean() - 1
    f["ma50_dist"] = close / close.rolling(50).mean() - 1
    f["mom_20d"]   = close / close.shift(20) - 1
    f["rvol_10d"]  = lr.rolling(10, min_periods=5).std() * np.sqrt(ANNUALIZATION_FACTOR)
    f["rvol_20d"]  = lr.rolling(20, min_periods=10).std() * np.sqrt(ANNUALIZATION_FACTOR)
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(RSI_PERIOD, min_periods=RSI_PERIOD // 2).mean()
    loss  = (-delta.clip(upper=0)).rolling(RSI_PERIOD, min_periods=RSI_PERIOD // 2).mean()
    f["rsi_14"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    return f.shift(1).dropna().reindex(idx).ffill()


def _hybrid_features(btc_ohlcv: pd.DataFrame, fr: pd.Series,fg: pd.Series, idx: pd.DatetimeIndex) -> pd.DataFrame:
    """Price + vol + FR + F&G features, lagged 1 day, reindexed to idx."""
    close = btc_ohlcv["close"].sort_index()
    lr    = np.log(close / close.shift(1))
    f = pd.DataFrame(index=btc_ohlcv.index)
    f["ret_1d"]    = lr
    f["ret_5d"]    = np.log(close / close.shift(5))
    f["ret_10d"]   = np.log(close / close.shift(10))
    f["ret_20d"]   = np.log(close / close.shift(20))
    f["ma20_dist"] = close / close.rolling(20).mean() - 1
    f["ma50_dist"] = close / close.rolling(50).mean() - 1
    f["rvol_10d"]  = lr.rolling(10, min_periods=5).std() * np.sqrt(ANNUALIZATION_FACTOR)
    f["rvol_20d"]  = lr.rolling(20, min_periods=10).std() * np.sqrt(ANNUALIZATION_FACTOR)
    f["fr"]        = fr.reindex(btc_ohlcv.index).ffill()
    f["fg_7dma"]   = fg.reindex(btc_ohlcv.index).ffill().rolling(7, min_periods=4).mean()
    return f.shift(1).dropna().reindex(idx).ffill()


def _cal_bad_series(idx: pd.DatetimeIndex) -> pd.Series:
    bad = pd.Series(False, index=idx)
    for s, e in CALENDAR_WINDOWS:
        bad |= (idx >= s) & (idx <= e)
    return bad


def _walkforward_split(feats_r: pd.DataFrame, idx: pd.DatetimeIndex,train_frac: float = 0.60):
    """Return (train_idx, pred_idx, X_train, X_pred, scaler)."""

    valid  = idx[feats_r.notna().all(axis=1)]
    split  = int(len(valid) * train_frac)
    tr_idx = valid[:split]
    pr_idx = valid[split:]
    scaler = StandardScaler().fit(feats_r.loc[tr_idx].values)
    Xs_tr  = scaler.transform(feats_r.loc[tr_idx].values)
    Xs_pr  = scaler.transform(feats_r.loc[pr_idx].values)
    return tr_idx, pr_idx, Xs_tr, Xs_pr, scaler


def _hmm_gated(X_train: np.ndarray, dates_train: pd.DatetimeIndex,
               X_pred: np.ndarray, dates_pred: pd.DatetimeIndex,
               n_states: int, bad_state_count: int,
               prob_threshold: float, emission_col: int = 0) -> pd.Series:
    """
    Like _hmm_bad_series but gates on posterior probability.
    Fit on X_train only; bad-state mapping from train statistics;
    posterior threshold applied to test predictions only.
    """
    try:
        hmm = GaussianHMM(n_components=n_states, covariance_type="full",
                          n_iter=200, random_state=42)
        # Fit on train only
        hmm.fit(X_train)
        train_states = hmm.predict(X_train)
        # Bad-state mapping from train statistics only
        means = {s: X_train[train_states == s, emission_col].mean()
                 for s in range(n_states) if (train_states == s).any()}
        sorted_states = sorted(means, key=means.get)
        bad_states    = set(sorted_states[:bad_state_count])
        # Posterior probability on test window only
        posteriors = hmm.predict_proba(X_pred)
        bad_prob   = posteriors[:, list(bad_states)].sum(axis=1)
        flagged    = bad_prob >= prob_threshold
        return pd.Series(flagged, index=dates_pred)
    except Exception as e:
        print(f"        Gated HMM failed: {e}")
        return pd.Series(False, index=dates_pred)


def _print_coverage(bad_full: pd.Series, idx: pd.DatetimeIndex, tag: str):
    n_bad = bad_full.sum()
    print(f"    {tag}: {n_bad} days flagged ({n_bad/len(idx)*100:.1f}%)")
    for s, e in CALENDAR_WINDOWS:
        mask    = (idx >= s) & (idx <= e)
        total   = mask.sum()
        flagged = bad_full[mask].sum()
        pct_w   = flagged / total * 100 if total > 0 else 0
        ok      = "✅" if pct_w >= 80 else ("⚠️ " if pct_w >= 50 else "❌")
        print(f"      {s[:10]} -> {e[:10]}: {flagged}/{total} ({pct_w:.0f}%)  {ok}")


# ══════════════════════════════════════════════════════════════════════
# V5-B TUNED VARIANTS
# ══════════════════════════════════════════════════════════════════════

def build_v5b1_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """V5-B1: 3 states, flag bottom 1 only (more selective than V5-B's bottom 2)."""
    print("    Building V5-B1: HMM | price | 3 states flag-1 | 60/40")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _price_features(btc_ohlcv, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.60)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=tr_idx,
        n_states=3, bad_state_count=1, emission_col=0,
        X_pred=Xs_pr, dates_pred=pr_idx,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-B1")
    _print_coverage(bad_full, idx, "V5-B1")
    return bad_full


def build_v5b2_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """V5-B2: 4 states, flag bottom 1 (finest regime resolution)."""
    print("    Building V5-B2: HMM | price | 4 states flag-1 | 60/40")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _price_features(btc_ohlcv, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.60)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=tr_idx,
        n_states=4, bad_state_count=1, emission_col=0,
        X_pred=Xs_pr, dates_pred=pr_idx,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-B2")
    _print_coverage(bad_full, idx, "V5-B2")
    return bad_full


def build_v5b3_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """V5-B3: 3 states, confidence-gated at P(bad) >= 0.70, flag bottom 2."""
    print("    Building V5-B3: HMM | price | 3 states | gated p>=0.70 | 60/40")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _price_features(btc_ohlcv, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.60)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    # TODO: fix _hmm_gated lookahead (legacy unused filter)
    bad_pred = _hmm_gated(
        X_train=Xs_tr, dates_train=tr_idx,
        X_pred=Xs_pr, dates_pred=pr_idx,
        n_states=3, bad_state_count=2,
                          prob_threshold=0.70, emission_col=0,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-B3")
    _print_coverage(bad_full, idx, "V5-B3")
    return bad_full


def build_v5b4_filter(btc_ohlcv: pd.DataFrame,fr: pd.Series, fg: pd.Series) -> pd.Series:
    """V5-B4: 3 states, hybrid price+vol+FR emissions, flag bottom 2."""
    print("    Building V5-B4: HMM | hybrid price+vol+FR | 3 states flag-2 | 60/40")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _hybrid_features(btc_ohlcv, fr, fg, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.60)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=tr_idx,
        n_states=3, bad_state_count=2, emission_col=0,
        X_pred=Xs_pr, dates_pred=pr_idx,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-B4")
    _print_coverage(bad_full, idx, "V5-B4")
    return bad_full


# ══════════════════════════════════════════════════════════════════════
# V5-D TUNED VARIANTS
# ══════════════════════════════════════════════════════════════════════

def build_v5d1_filter(btc_ohlcv: pd.DataFrame,fr: pd.Series, fg: pd.Series) -> pd.Series:
    """V5-D1: HMM-only, 2 states, hybrid price+vol+FR features, 60/40."""
    print("    Building V5-D1: HMM-only | hybrid price+vol+FR | 2 states | 60/40")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _hybrid_features(btc_ohlcv, fr, fg, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.60)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=tr_idx,
        n_states=2, bad_state_count=1, emission_col=0,
        X_pred=Xs_pr, dates_pred=pr_idx,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-D1")
    _print_coverage(bad_full, idx, "V5-D1")
    return bad_full


def build_v5d2_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """V5-D2: HMM-only, 3 states, flag bottom 1 (separate crash from grind)."""
    print("    Building V5-D2: HMM-only | price | 3 states flag-1 | 60/40")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _price_features(btc_ohlcv, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.60)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=tr_idx,
        n_states=3, bad_state_count=1, emission_col=0,
        X_pred=Xs_pr, dates_pred=pr_idx,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-D2")
    _print_coverage(bad_full, idx, "V5-D2")
    return bad_full


def build_v5d3_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """V5-D3: HMM-only, 2 states, confidence-gated at P(bad) >= 0.65."""
    print("    Building V5-D3: HMM-only | price | 2 states | gated p>=0.65 | 60/40")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _price_features(btc_ohlcv, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.60)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    # TODO: fix _hmm_gated lookahead (legacy unused filter)
    bad_pred = _hmm_gated(
        X_train=Xs_tr, dates_train=tr_idx,
        X_pred=Xs_pr, dates_pred=pr_idx,
        n_states=2, bad_state_count=1,
                          prob_threshold=0.65, emission_col=0,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-D3")
    _print_coverage(bad_full, idx, "V5-D3")
    return bad_full


def build_v5d4_filter(btc_ohlcv: pd.DataFrame) -> pd.Series:
    """V5-D4: HMM-only, 2 states, price features, expanded 70% training window."""
    print("    Building V5-D4: HMM-only | price | 2 states | 70/30 split")
    idx      = pd.date_range("2025-01-01", "2026-03-01", freq="D")
    feats_r  = _price_features(btc_ohlcv, idx)
    cal_bad  = _cal_bad_series(idx)
    tr_idx, pr_idx, Xs_tr, Xs_pr, _ = _walkforward_split(feats_r, idx, 0.70)
    all_idx  = pd.DatetimeIndex(list(tr_idx) + list(pr_idx))
    print(f"      70/30 split: {len(tr_idx)} train / {len(pr_idx)} predict "
          f"(split at {tr_idx[-1].date()} -> {pr_idx[0].date()})")
    bad_pred = _hmm_bad_series(
        X_train=Xs_tr, dates_train=tr_idx,
        n_states=2, bad_state_count=1, emission_col=0,
        X_pred=Xs_pr, dates_pred=pr_idx,
    )
    bad_full = _assemble_filter(bad_pred, tr_idx, cal_bad, idx, "V5-D4")
    _print_coverage(bad_full, idx, "V5-D4")
    return bad_full


def _col_to_timestamp(col_str: str) -> Optional[pd.Timestamp]:
    """
    Convert a df_4x column name to a pd.Timestamp.
    Handles formats:
      - "Bt_20250301"      -> 2025-03-01  (Google Sheets pivot format)
      - "Bt_2025301"       -> 2025-03-01  (compact: YYYY + 3-digit day-of-year)
      - "2025-03-01"       -> 2025-03-01  (ISO)
      - "2025-03-01 00:00" -> 2025-03-01
      - "3/1/2025"         -> 2025-03-01  (US date format from Sheets)
      - "45716"            -> Excel serial date (days since 1899-12-30)
    Returns None if unparseable.
    """
    s = col_str.strip()
    # Strip "Bt_" prefix (Google Sheets column header format)
    if s.startswith("Bt_"):
        s = s[3:]

    # Handle "YYYYMMDD_HHMMSS" (after Bt_ prefix stripped)
    if len(s) == 15 and s[8] == "_" and s[:8].isdigit():
        try:
            return pd.Timestamp(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
        except Exception:
            pass

    # --- Guard: bare integers are Excel serial dates, NOT nanosecond epochs ---
    # pd.Timestamp(integer) treats it as nanoseconds -> gives ~1970 dates. Wrong.
    if s.isdigit():
        n = int(s)
        # Excel serial: 1 = 1900-01-01, typical 2025 dates are ~45xxx
        if 40000 <= n <= 60000:
            try:
                return (pd.Timestamp("1899-12-30") + pd.Timedelta(days=n)).normalize()
            except Exception:
                pass
        # YYYYMMDD (8 digits)
        if len(s) == 8:
            try:
                return pd.Timestamp(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
            except Exception:
                pass
        # YYYYDDD (7 digits - year + 3-digit day-of-year)
        if len(s) == 7:
            try:
                return pd.Timestamp(f"{s[:4]}-01-01") + pd.Timedelta(days=int(s[4:]) - 1)
            except Exception:
                pass
        return None  # unknown integer format - do not pass to pd.Timestamp()

    # Non-integer: try direct parse (covers ISO, "YYYY-MM-DD HH:MM", etc.)
    try:
        return pd.Timestamp(s).normalize()
    except Exception:
        pass
    return None


# Module-level cache: id(filter_series) -> {date: bool}
_V3_DATE_CACHE: Dict[int, dict] = {}


def is_v3_bad(date_str: str, v3_filter: pd.Series) -> bool:
    """Check if a column date string falls on a v3 or v4-flagged day.

    The filter index is built from pd.date_range (freq='D', tz-naive midnight).
    Column timestamps may differ in sub-day components, so we normalise to
    midnight and try both Timestamp and plain date() key lookups.

    A module-level cache (_V3_DATE_CACHE) maps python date -> bool so the
    O(n) fallback scan only runs once per unique filter series (keyed by id).
    """
    ts = _col_to_timestamp(date_str)
    if ts is None:
        return False
    ts = pd.Timestamp(ts).tz_localize(None).normalize()
    # Fast path: direct Timestamp lookup (works when index is exact tz-naive midnight)
    try:
        return bool(v3_filter.loc[ts])
    except KeyError:
        pass
    # Slow-but-correct path: build a date->bool dict once per filter series.
    # IMPORTANT: use a content hash, not id(v3_filter).  Python may reuse the
    # same memory address for two different Series objects if one is released
    # before the next is created (e.g. tail_or_vol_filter followed by
    # tail_and_vol_filter in the same loop iteration).  id() collision causes
    # the AND filter to silently use the OR filter's cached lookup table,
    # making both filters produce identical results.
    try:
        cache_key = hash(v3_filter.values.tobytes())
    except Exception:
        cache_key = id(v3_filter)
    if cache_key not in _V3_DATE_CACHE:
        _V3_DATE_CACHE[cache_key] = {
            pd.Timestamp(k).date(): bool(v)
            for k, v in v3_filter.items()
        }
    return _V3_DATE_CACHE[cache_key].get(ts.date(), False)


def is_calendar_bad(date_str: str) -> bool:
    """Check if a column date string falls in a calendar failure window."""
    ts = _col_to_timestamp(date_str)
    if ts is None:
        return False
    ts = pd.Timestamp(ts).tz_localize(None).normalize()
    return any(pd.Timestamp(s) <= ts <= pd.Timestamp(e)
               for s, e in CALENDAR_WINDOWS)

# ══════════════════════════════════════════════════════════════════════
# PROBABILITY OF BACKTEST OVERFITTING  (PBO)
# ══════════════════════════════════════════════════════════════════════
#
# Implements the Combinatorially Symmetric Cross-Validation (CSCV) method
# from Bailey, Borwein, Lopez de Prado & Zhu (2015).
#
# Reference
# ---------
# Bailey, D., Borwein, J., Lopez de Prado, M., & Zhu, Q. (2015).
# "The probability of backtest overfitting."
# Journal of Computational Finance, 20(4), 39–70.
#
# Algorithm
# ---------
# Inputs:
#   M   – (T × N) matrix of daily returns for N strategies over T days
#   S   – even number of sub-periods to partition T (default 16)
#
# 1. Partition T days into S equal sub-periods of length T//S.
# 2. Enumerate all C(S, S/2) combinations of sub-periods.
# 3. For each combination c:
#      - TRAIN : the S/2 chosen sub-periods  (concatenated)
#      - TEST  : the remaining S/2 sub-periods
#      - Compute annualised Sharpe for each of the N strategies in
#        both halves.
#      - Identify the strategy n* with highest TRAIN Sharpe.
#      - Rank n* by its TEST Sharpe among all N strategies
#        (rank 1 = worst, N = best).
#      - Normalised rank:  ω_c = (rank − 0.5) / N   ∈ (0, 1)
#      - Logit:            λ_c = log(ω_c / (1 − ω_c))
# 4. PBO = fraction of combinations where λ_c < 0
#          (training winner ranked below median in test)
#
# Additional metrics
# ------------------
# Performance Degradation (PD):
#   Mean drop in Sharpe between TRAIN and TEST for the training winner.
#   PD > 0 means the winner always degrades; PD < 0 means it improves.
#
# Probability of Loss (POL):
#   Fraction of combinations where the training winner has TEST Sharpe < 0.
#
# Overfitting Interpretation
# --------------------------
#   PBO < 0.10  →  low overfitting risk
#   PBO 0.10–0.25  →  moderate risk, monitor
#   PBO 0.25–0.50  →  elevated risk
#   PBO > 0.50  →  high risk — strategy likely overfit to training window
# ══════════════════════════════════════════════════════════════════════

def _sharpe_from_returns(r: np.ndarray, trading_days: int) -> float:
    """Annualised Sharpe of a 1-D return array.  Returns nan if < 2 obs."""
    r = r[np.isfinite(r)]
    if len(r) < 2:
        return float("nan")
    mu = float(np.mean(r))
    sd = float(np.std(r, ddof=1))
    return (mu / sd) * math.sqrt(trading_days) if sd > 1e-14 else float("nan")


def compute_pbo(
    returns_matrix:  np.ndarray,       # shape (T, N)
    strategy_labels: List[str],        # length N
    S:               int  = 16,        # sub-periods (must be even, ≥ 4)
    trading_days:    int  = 365,
) -> Dict:
    """
    Compute Probability of Backtest Overfitting via CSCV.

    Parameters
    ----------
    returns_matrix  : (T × N) array — each column is one strategy's
                      daily return series (zeros on filtered/flat days).
    strategy_labels : names corresponding to columns of returns_matrix.
    S               : number of sub-periods. Must be even and ≥ 4.
                      Larger S → more combinations but shorter sub-periods.
                      S=16 is the canonical choice from the paper.
    trading_days    : annualisation factor for Sharpe.

    Returns
    -------
    dict with keys:
      pbo             – scalar in [0, 1]
      performance_deg – mean Sharpe degradation (train_winner_sharpe − test_winner_sharpe)
      prob_loss       – fraction of combos where test Sharpe of winner < 0
      n_combos        – number of train/test combinations evaluated
      n_strategies    – N
      n_days          – T
      S               – sub-periods used
      sub_period_len  – days per sub-period
      logit_values    – np.ndarray of all λ_c values (for histogram)
      train_sharpes   – np.ndarray of training Sharpe of winner per combo
      test_sharpes    – np.ndarray of test Sharpe of winner per combo
      dominant_idx    – index (0-based) of strategy most often selected as winner
      dominant_label  – label of that strategy
      win_counts      – dict {label: times selected as training winner}
      warnings        – list of warning strings (empty if all checks pass)
    """
    from math import comb as _comb

    warnings_list: List[str] = []
    T, N = returns_matrix.shape

    # ── Input validation ──────────────────────────────────────────────
    if N < 2:
        warnings_list.append(f"PBO requires N ≥ 2 strategies; got {N}. Returning PBO=nan.")
        return dict(pbo=float("nan"), performance_deg=float("nan"),
                    prob_loss=float("nan"), n_combos=0, n_strategies=N,
                    n_days=T, S=S, sub_period_len=0,
                    logit_values=np.array([]), train_sharpes=np.array([]),
                    test_sharpes=np.array([]), dominant_idx=0,
                    dominant_label=strategy_labels[0] if strategy_labels else "",
                    win_counts={}, warnings=warnings_list)

    if S % 2 != 0 or S < 4:
        warnings_list.append(f"S must be even and ≥ 4; got S={S}. Clamping to S=4.")
        S = max(4, S + (S % 2))   # round up to next even number

    sub_len = T // S
    if sub_len < 5:
        # Not enough days per sub-period for a meaningful Sharpe
        new_S = max(4, (T // 5) & ~1)   # largest even S giving sub_len ≥ 5
        warnings_list.append(
            f"T={T} days / S={S} → {sub_len} days/sub-period (< 5). "
            f"Auto-reducing to S={new_S} ({T//new_S} days/sub-period)."
        )
        S = new_S
        sub_len = T // S

    n_combos_expected = _comb(S, S // 2)

    # ── Build sub-period index ranges (use only T - (T % S) days) ────
    T_used = sub_len * S
    M = returns_matrix[:T_used, :]        # shape (T_used, N)

    # Sub-period Sharpe matrix: (S, N) — Sharpe of each strategy in each sub-period
    sub_sharpes = np.full((S, N), float("nan"))
    for s in range(S):
        block = M[s * sub_len : (s + 1) * sub_len, :]   # (sub_len, N)
        for n in range(N):
            sub_sharpes[s, n] = _sharpe_from_returns(block[:, n], trading_days)

    # Replace NaN Sharpes with 0 so they never win (NaN from zero-variance)
    sub_sharpes_clean = np.where(np.isfinite(sub_sharpes), sub_sharpes, 0.0)

    # ── Enumerate all C(S, S/2) train/test splits ─────────────────────
    all_sub_indices = list(range(S))
    half = S // 2

    logit_vals:   List[float] = []
    train_shs:    List[float] = []
    test_shs:     List[float] = []
    win_counts_arr = np.zeros(N, dtype=int)

    for train_subs in itertools.combinations(all_sub_indices, half):
        test_subs = tuple(i for i in all_sub_indices if i not in set(train_subs))

        # Aggregate Sharpe over the S/2 sub-periods by averaging sub-period returns
        # (rather than computing Sharpe on concatenated block) — consistent with paper.
        # train_sh = sub_sharpes_clean[list(train_subs), :].mean(axis=0)  # (N,)
        # test_sh  = sub_sharpes_clean[list(test_subs),  :].mean(axis=0)  # (N,)
        train_block = np.vstack([M[i*sub_len:(i+1)*sub_len] for i in train_subs])
        test_block  = np.vstack([M[i*sub_len:(i+1)*sub_len] for i in test_subs])

        train_sh = np.array([
            _sharpe_from_returns(train_block[:,n], trading_days)
            for n in range(N)
        ])

        test_sh = np.array([
            _sharpe_from_returns(test_block[:,n], trading_days)
            for n in range(N)
        ])


        # Best strategy in training (deterministic: first maximum on ties)
        n_star = int(np.argmax(train_sh))
        win_counts_arr[n_star] += 1

        # Rank n_star in test (1 = worst, N = best)
        test_sh_winner = test_sh[n_star]
        rank = int(np.sum(test_sh <= test_sh_winner))   # number of strategies ≤ winner
        rank = max(1, min(rank, N))                      # clamp to [1, N]

        # Normalised rank → logit
        omega = np.clip((rank - 0.5) / N, 1e-6, 1 - 1e-6) # ∈ (0, 1)
        lambda_c = math.log(omega / (1.0 - omega))       # logit

        logit_vals.append(lambda_c)
        train_shs.append(float(train_sh[n_star]))
        test_shs.append(float(test_sh_winner))

    logit_arr  = np.array(logit_vals)
    train_arr  = np.array(train_shs)
    test_arr   = np.array(test_shs)

    pbo         = float(np.mean(logit_arr < 0))
    perf_deg    = float(np.mean(train_arr - test_arr))   # average Sharpe drop
    prob_loss   = float(np.mean(test_arr < 0))

    dominant_idx   = int(np.argmax(win_counts_arr))
    dominant_label = strategy_labels[dominant_idx] if strategy_labels else str(dominant_idx)
    win_counts_dict = {strategy_labels[i]: int(win_counts_arr[i])
                       for i in range(N)} if strategy_labels else {}

    # ── Low-resolution check: N < 10 gives discrete logit spikes ────────
    # With N strategies the normalised rank ω = (k-0.5)/N can only take
    # exactly N values, so logit(ω) is a discrete distribution with N
    # spikes.  PBO is then just counting which spike is above/below zero,
    # not a continuous probability — the grade bands lose their meaning.
    low_resolution = N < 10
    if low_resolution:
        # Compute the exact set of possible logit values for the report
        possible_logits = sorted(
            math.log(((k - 0.5) / N) / (1 - (k - 0.5) / N))
            for k in range(1, N + 1)
        )
        possible_str = ", ".join(f"{v:+.4f}" for v in possible_logits)
        warnings_list.append(
            f"LOW RESOLUTION: N={N} strategies means logit(ω) can only take "
            f"{N} discrete values ({possible_str}). PBO={pbo:.3f} reflects "
            f"which of these {N} buckets the training winner falls into — "
            f"not a continuous probability. Grade bands are not reliable at N < 10."
        )
        warnings_list.append(
            f"STRUCTURAL BIAS: when the N strategies are filter variants of the "
            f"same base strategy, unfiltered variants have more active trading days "
            f"per sub-period, which mechanically inflates their sub-period Sharpe. "
            f"This causes unfiltered variants to win more training splits even when "
            f"filtered variants are stronger over the full period. "
            f"Interpret win counts with this in mind."
        )
    else:
        possible_logits = []
        if pbo > 0.50:
            warnings_list.append(
                f"PBO={pbo:.3f} > 0.50 — high overfitting risk. "
                "Results may not generalise out-of-sample."
            )

    dominant_frac = win_counts_arr[dominant_idx] / max(len(logit_arr), 1)
    if dominant_frac > 0.90:
        warnings_list.append(
            f"Strategy '{dominant_label}' won {dominant_frac*100:.0f}% of training "
            "splits — near-deterministic winner suggests a dominant strategy, which "
            "may indicate insufficient strategy diversity for a meaningful PBO."
        )

    return dict(
        pbo              = pbo,
        performance_deg  = perf_deg,
        prob_loss        = prob_loss,
        n_combos         = len(logit_arr),
        n_strategies     = N,
        n_days           = T,
        S                = S,
        sub_period_len   = sub_len,
        logit_values     = logit_arr,
        train_sharpes    = train_arr,
        test_sharpes     = test_arr,
        dominant_idx     = dominant_idx,
        dominant_label   = dominant_label,
        win_counts       = win_counts_dict,
        warnings         = warnings_list,
        low_resolution   = low_resolution,
        possible_logits  = possible_logits,
    )


def _wrap_print(text: str, width: int = 65, indent: str = "  ") -> None:
    """Print text wrapped at `width` chars, each line prefixed by `indent`."""
    words = text.split()
    line: List[str] = []
    for w in words:
        if sum(len(x) + 1 for x in line) + len(w) > width:
            print(indent + " ".join(line))
            line = [w]
        else:
            line.append(w)
    if line:
        print(indent + " ".join(line))


def print_pbo_report(result: Dict, label: str = "") -> None:
    """
    Print a formatted PBO report to stdout.

    Parameters
    ----------
    result : dict returned by compute_pbo()
    label  : optional title string
    """
    W   = 70
    SEP = "═" * W

    pbo        = result.get("pbo",             float("nan"))
    pd_val     = result.get("performance_deg", float("nan"))
    pol        = result.get("prob_loss",       float("nan"))
    n_c        = result.get("n_combos",        0)
    N          = result.get("n_strategies",    0)
    T          = result.get("n_days",          0)
    S          = result.get("S",               0)
    sl         = result.get("sub_period_len",  0)
    wins       = result.get("win_counts",      {})
    warns      = result.get("warnings",        [])
    low_res    = result.get("low_resolution",  False)
    poss_logit = result.get("possible_logits", [])

    # Grade — suppressed with a resolution caveat when N < 10
    if not math.isfinite(pbo):
        grade = "⬜ N/A"
    elif low_res:
        # PBO number is technically correct but its bands are meaningless at N<10
        grade = f"⚠  LOW RESOLUTION (N={N} < 10 — see warnings)"
    elif pbo < 0.10:
        grade = "✅ LOW OVERFIT RISK"
    elif pbo < 0.25:
        grade = "✓  MODERATE RISK"
    elif pbo < 0.50:
        grade = "⚠  ELEVATED RISK"
    else:
        grade = "❌ HIGH OVERFIT RISK"

    print()
    print(SEP)
    title = f"  PBO — PROBABILITY OF BACKTEST OVERFITTING"
    if label:
        title += f"  [{label}]"
    print(title)
    print(f"  CSCV method · Bailey et al. (2015) · J. Computational Finance")
    print(SEP)
    print(f"  Strategies (N)        : {N}")
    print(f"  Trading days (T)      : {T}  (sub-period len={sl}d, S={S})")
    print(f"  Combinations C(S,S/2) : {n_c:,}")
    print()
    pbo_s   = f"{pbo:.3f}  ({pbo*100:.1f}%)" if math.isfinite(pbo) else "n/a"
    pd_s    = f"{pd_val:+.4f}" if math.isfinite(pd_val) else "n/a"
    pol_s   = f"{pol:.3f}  ({pol*100:.1f}%)" if math.isfinite(pol) else "n/a"
    print(f"  PBO                   : {pbo_s}")
    print(f"  Grade                 : {grade}")
    print(f"  Performance Degradation (PD) : {pd_s}  Sharpe drop train→test")
    print(f"  Probability of Loss (POL)    : {pol_s}  (test Sharpe of winner < 0)")
    if low_res and poss_logit:
        poss_str = "  ".join(f"{v:+.4f}" for v in poss_logit)
        print(f"  Discrete logit values ({N}) : {poss_str}")
    print()

    # Per-strategy win counts
    print(f"  ── Training-window win counts ──")
    if wins:
        sorted_wins = sorted(wins.items(), key=lambda kv: -kv[1])
        for strat, cnt in sorted_wins:
            pct = cnt / n_c * 100 if n_c > 0 else 0.0
            bar = "█" * int(pct / 2)
            print(f"  {strat:<40}  {cnt:>5}×  ({pct:5.1f}%)  {bar}")
    print()

    # Logit distribution summary
    lv = result.get("logit_values", np.array([]))
    if len(lv) > 0:
        print(f"  ── Logit(ω) distribution (λ_c = log(rank/(N−rank))) ──")
        print(f"  Mean  : {lv.mean():+.4f}    Median: {np.median(lv):+.4f}")
        print(f"  Stdev : {lv.std():.4f}     Range : [{lv.min():+.4f}, {lv.max():+.4f}]")
        print(f"  λ < 0 : {(lv<0).sum():>4} / {len(lv)}  = PBO")
        print(f"  λ = 0 : {(lv==0).sum():>4} / {len(lv)}  (winner ranked at exact median)")
        print(f"  λ > 0 : {(lv>0).sum():>4} / {len(lv)}")
        print()

    # Warnings (always shown — they carry the substance when low_res)
    if warns:
        print(f"  ── Warnings ──")
        for w in warns:
            words = w.split()
            wrapped, cur = [], []
            for word in words:
                if sum(len(x) + 1 for x in cur) + len(word) > 64:
                    wrapped.append(" ".join(cur))
                    cur = [word]
                else:
                    cur.append(word)
            if cur:
                wrapped.append(" ".join(cur))
            for i, ln in enumerate(wrapped):
                prefix = "  ⚠  " if i == 0 else "       "
                print(f"{prefix}{ln}")
            print()

    # Plain-language interpretation — distinguishes low-res from normal case
    print(f"  ── Interpretation ──")
    if math.isfinite(pbo):
        if low_res:
            # What the result actually means and doesn't mean
            interp_lines = [
                f"With N={N} strategies the logit(ω) distribution has only {N}",
                "discrete values, so PBO grade bands (Low / Moderate / High) are",
                "not statistically meaningful here.",
                "",
                "What this result DOES tell you:",
                f"  · In {pbo*100:.0f}% of train/test splits the filter with the best",
                "    in-sample Sharpe did NOT rank in the top half out-of-sample.",
                "    → Filter selection by in-sample Sharpe alone is unstable.",
                "  · The unfiltered variant dominates training splits because it",
                "    has more active trading days per sub-period, mechanically",
                "    inflating its sub-period Sharpe (active-day structural bias).",
                "  · This is expected behaviour, not evidence of overfitting.",
                "",
                "What this result does NOT tell you:",
                "  · It does NOT mean the underlying strategy is overfit.",
                f"  · POL = {pol*100:.0f}% means the training winner ALWAYS had positive",
                "    test Sharpe — the strategy was profitable in every OOS window.",
                "",
                "Recommended action:",
                "  · To get a meaningful PBO, run CSCV across a large grid of",
                "    independently-tuned parameter configurations (N ≥ 20 ideally),",
                "    not across filter variants of the same base strategy.",
                "  · Use POL and PD as the primary overfitting diagnostics here.",
            ]
            for ln in interp_lines:
                print(f"  {ln}")
        elif pbo < 0.10:
            interp = ("The training winner ranks above the median in the test window "
                      "the vast majority of the time. Strategy selection appears "
                      "robust to the choice of in-sample period.")
            _wrap_print(interp)
        elif pbo < 0.25:
            interp = ("The training winner degrades somewhat out-of-sample but still "
                      "beats the median test strategy more often than not. Moderate "
                      "caution warranted; watch for regime dependence.")
            _wrap_print(interp)
        elif pbo < 0.50:
            interp = ("The selected strategy ranks below the test median in a large "
                      "fraction of splits. In-sample optimisation is providing "
                      "limited predictive lift. Consider diversifying the strategy "
                      "set or reducing the number of free parameters.")
            _wrap_print(interp)
        else:
            interp = ("The training winner ranks below the test median in MORE THAN "
                      "HALF of all train/test splits. This is the hallmark of an "
                      "overfit backtest. Performance is unlikely to replicate "
                      "out-of-sample. Strongly recommended: reduce strategy count, "
                      "widen test windows, or apply stricter regularisation.")
            _wrap_print(interp)
    else:
        print("  Insufficient data for interpretation.")

    print(SEP)


def plot_pbo_logit_dist(result: Dict, outdir, label: str = "") -> Optional[str]:
    """
    Save a histogram of the logit(ω) distribution with interpretive
    annotations.  Returns the saved file path, or None on failure.
    """
    try:
        import matplotlib.pyplot as plt   # noqa: F401 — availability check
        _mpl_ok = True
    except Exception:
        _mpl_ok = False
    if not _mpl_ok:
        return None

    lv = result.get("logit_values", np.array([]))
    if len(lv) == 0:
        return None

    pbo        = result.get("pbo",             float("nan"))
    pd_val     = result.get("performance_deg", float("nan"))
    pol        = result.get("prob_loss",       float("nan"))
    N          = result.get("n_strategies",    0)
    n_c        = result.get("n_combos",        0)
    ts         = result.get("test_sharpes",    np.array([]))
    low_res    = result.get("low_resolution",  False)
    poss_logit = result.get("possible_logits", [])

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    low_res_tag = f"  ⚠ N={N} — discrete logit, low resolution" if low_res else ""
    fig.suptitle(
        f"Probability of Backtest Overfitting (PBO)  "
        f"{'— ' + label if label else ''}\n"
        f"CSCV  ·  N={N} strategies  ·  {n_c:,} train/test combinations"
        f"{low_res_tag}",
        fontsize=11, fontweight="bold",
    )

    # ── Left panel: logit(ω) histogram ───────────────────────────────
    ax = axes[0]

    if low_res and len(poss_logit) > 0:
        # For small N the distribution is discrete — use a bar chart keyed
        # to the exact possible logit values rather than a histogram with
        # arbitrary bins that makes spikes look like continuous data.
        unique_vals, counts = np.unique(np.round(lv, 6), return_counts=True)
        bar_colors = ["#cc2020" if v < 0 else "#2060a0" for v in unique_vals]
        ax.bar(unique_vals, counts, width=0.25, color=bar_colors, alpha=0.80,
               edgecolor="white", linewidth=0.6)
        # Label each bar with its exact logit value and count
        for v, c in zip(unique_vals, counts):
            ax.text(v, c + n_c * 0.005, f"{v:+.3f}\n({c:,})",
                    ha="center", va="bottom", fontsize=7.5, color="#333333")
        ax.set_xticks(unique_vals)
        ax.set_xticklabels([f"{v:+.3f}" for v in unique_vals], fontsize=7.5)
        discrete_note = f"Discrete: {N} possible values  (λ < 0 = overfit bucket)"
    else:
        n_bins = max(20, min(50, n_c // 100))
        ax.hist(lv, bins=n_bins, color="#2060a0", alpha=0.75, edgecolor="white",
                linewidth=0.4, label=f"λ_c  ({n_c:,} combos)")
        neg_vals = lv[lv < 0]
        if len(neg_vals) > 0:
            ax.hist(neg_vals, bins=n_bins, color="#cc2020", alpha=0.35,
                    edgecolor="none", label=f"Overfit region  (PBO={pbo:.3f})")
        ax.axvline(lv.mean(), color="#208020", linewidth=1.2, linestyle=":",
                   label=f"Mean λ = {lv.mean():+.3f}")
        ax.legend(fontsize=8)
        discrete_note = ""

    ax.axvline(0, color="#cc2020", linewidth=2.0, linestyle="--",
               label="λ=0 (median boundary)")

    pbo_s = f"{pbo:.3f}" if math.isfinite(pbo) else "n/a"
    pd_s  = f"{pd_val:+.4f}" if math.isfinite(pd_val) else "n/a"
    pol_s = f"{pol:.3f}" if math.isfinite(pol) else "n/a"

    title_line2 = discrete_note if discrete_note else f"PBO = {pbo_s}   PD = {pd_s}   POL = {pol_s}"
    ax.set_xlabel("Logit of normalised rank  λ_c", fontsize=9)
    ax.set_ylabel("Frequency", fontsize=9)
    ax.set_title(
        f"Logit(ω) Distribution\n"
        f"PBO = {pbo_s}   PD = {pd_s}   POL = {pol_s}\n"
        + (f"⚠ {discrete_note}" if discrete_note else ""),
        fontsize=9, loc="left",
    )
    ax.tick_params(labelsize=8)

    # ── Right panel: test Sharpe of winner vs train Sharpe ───────────
    ax2 = axes[1]
    if len(ts) > 0:
        train_arr = result.get("train_sharpes", np.array([]))
        if len(train_arr) == len(ts):
            # Colour points by rank bucket when low-res so discrete structure is visible
            if low_res:
                scatter_colors = ["#cc2020" if t_sh < tr_sh else "#2060a0"
                                  for t_sh, tr_sh in zip(ts, train_arr)]
                ax2.scatter(train_arr, ts, c=scatter_colors, alpha=0.25, s=6,
                            rasterized=True)
            else:
                ax2.scatter(train_arr, ts, alpha=0.20, s=6, color="#2060a0",
                            rasterized=True)
            all_vals = np.concatenate([train_arr, ts])
            mn, mx = float(np.nanmin(all_vals)), float(np.nanmax(all_vals))
            ax2.plot([mn, mx], [mn, mx], color="grey", linewidth=0.8,
                     linestyle="--", alpha=0.6, label="parity (no degradation)")
            ax2.axhline(0, color="#cc2020", linewidth=0.8, linestyle=":",
                        alpha=0.7, label="Test Sharpe = 0")
            if low_res:
                ax2.text(0.03, 0.97,
                         f"Red = test below train (degraded)\nBlue = test above train",
                         transform=ax2.transAxes, fontsize=7.5, va="top",
                         color="#555555")
            ax2.set_xlabel("Train Sharpe  (of winner)", fontsize=9)
            ax2.set_ylabel("Test Sharpe  (of winner)", fontsize=9)
            ax2.set_title(
                f"Train vs Test Sharpe of Training Winner\n"
                f"Points below parity line = degradation   "
                f"PD = {pd_s}  Sharpe units",
                fontsize=9, loc="left",
            )
            ax2.legend(fontsize=8)
            ax2.tick_params(labelsize=8)

    fig.tight_layout()

    lbl_safe = label.replace(" ", "_").replace("+", "_").replace("-", "_")
    fname    = f"pbo_cscv{'_' + lbl_safe if lbl_safe else ''}.png"
    fpath    = str(Path(outdir) / fname)
    try:
        fig.savefig(fpath, dpi=130, bbox_inches="tight")
    finally:
        plt.close(fig)

    return fpath



# ══════════════════════════════════════════════════════════════════════
# BTC OPEN INTEREST FETCH
# ══════════════════════════════════════════════════════════════════════
# Binance Futures /futures/data/openInterestHist endpoint.
# Returns daily sumOpenInterestValue (USD notional) — no API key required.
# period=1d, limit=500 per call (~16 months). We page forward until
# the full requested date range is covered.
#
# Returns a DataFrame with columns:
#   oi_usd      — absolute OI in USD (raw from API)
#   oi_change   — day-over-day change in OI (USD, can be negative)
#   oi_pct_chg  — day-over-day % change
#
# date index is tz-naive, normalised to midnight.
# ══════════════════════════════════════════════════════════════════════

OI_CACHE_FILE = "oi_cache.csv"

def fetch_btc_open_interest(start: str = "2025-01-01",end:   str = "2026-03-01") -> pd.DataFrame:
    """
    Fetch daily BTC perpetual open interest from Binance Futures.

    Endpoint: GET https://fapi.binance.com/futures/data/openInterestHist
    No authentication required.

    Returns
    -------
    pd.DataFrame  indexed by date, columns: oi_usd, oi_change, oi_pct_chg
    Empty DataFrame on failure.
    """
    print("  Fetching BTC open interest (Binance Futures) ...")

    # ── Try cache first ──────────────────────────────────────────────
    cache_path = Path(OI_CACHE_FILE)
    if cache_path.exists():
        try:
            cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            cached.index = pd.to_datetime(cached.index).tz_localize(None).normalize()
            need_end = pd.Timestamp(end)
            if not cached.empty and cached.index[-1] >= need_end - pd.Timedelta(days=2):
                cached = cached[(cached.index >= start) & (cached.index <= end)]
                print(f"    -> {len(cached)} days (from cache)")
                return cached
            print(f"    Cache stale (ends {cached.index[-1].date()}), re-fetching ...")
        except Exception as e:
            print(f"    Cache read failed ({e}), re-fetching ...")

    BIN = "https://fapi.binance.com"
    rows = []
    # Binance openInterestHist only retains ~30 days of history.
    # Passing startTime older than that returns HTTP 400.
    # Single call with limit=500 returns whatever is available (≤30 days).
    try:
        r = requests.get(
            f"{BIN}/futures/data/openInterestHist",
            params={"symbol": "BTCUSDT", "period": "1d", "limit": 500},
            timeout=15,
        )
        if r.status_code != 200:
            print(f"    ! HTTP {r.status_code}: {r.text[:120]}")
        else:
            rows = r.json()
    except Exception as e:
        print(f"    ! {e}")

    if not rows:
        print("    -> EMPTY — Binance OI endpoint unavailable (VPN?)")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms").dt.normalize()
    df = df.drop_duplicates("date").sort_values("date").set_index("date")
    df.index = df.index.tz_localize(None)

    out = pd.DataFrame(index=df.index)
    out["oi_usd"]     = pd.to_numeric(df["sumOpenInterestValue"], errors="coerce")
    out["oi_change"]  = out["oi_usd"].diff()
    out["oi_pct_chg"] = out["oi_usd"].pct_change() * 100

    # ── Cache to disk ────────────────────────────────────────────────
    try:
        out.to_csv(cache_path)
    except Exception:
        pass

    print(f"    -> {len(out)} days  "
          f"(OI range: ${out['oi_usd'].min()/1e9:.1f}B – ${out['oi_usd'].max()/1e9:.1f}B)")
    return out


# ══════════════════════════════════════════════════════════════════════
# MULTI-ASSET OI FETCH  (for OI dispersion + OI-price alignment)
# ══════════════════════════════════════════════════════════════════════
# Same endpoint as fetch_btc_open_interest but loops over all symbols
# in DISPERSION_SYMBOLS.  Returns a DataFrame of day-over-day OI %
# changes:
#   index   : date (tz-naive, normalised to midnight)
#   columns : symbol strings  (e.g. "BTCUSDT", "ETHUSDT", ...)
#   values  : daily OI % change  (float, NaN where unavailable)
#
# Cached to OI_MULTI_CACHE_FILE.  ~30 HTTP calls, adds ~20-30s to run
# time on first execution; subsequent runs are instant from cache.
# ══════════════════════════════════════════════════════════════════════

OI_MULTI_CACHE_FILE = "oi_multi_cache.csv"


def fetch_multi_asset_oi(
    symbols:    List[str] = None,
    start:      str = "2024-11-01",
    end:        str = "2026-03-01",
    cache_file: str = OI_MULTI_CACHE_FILE,
) -> pd.DataFrame:
    """
    Fetch daily open-interest for each symbol in `symbols` from Binance
    Futures (/futures/data/openInterestHist) and return day-over-day
    % changes.

    Returns
    -------
    pd.DataFrame  index=date, columns=symbols, values=daily OI % change.
    Empty DataFrame on complete failure.
    """
    if symbols is None:
        symbols = DISPERSION_SYMBOLS

    print(f"  Fetching multi-asset OI ({len(symbols)} symbols, Binance Futures) ...")

    # ── Cache ────────────────────────────────────────────────────────
    cache_path = Path(cache_file)
    if cache_path.exists():
        try:
            cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            cached.index = pd.to_datetime(cached.index).tz_localize(None).normalize()
            need_end = pd.Timestamp(end)
            if not cached.empty and cached.index[-1] >= need_end - pd.Timedelta(days=2):
                cached = cached[(cached.index >= start) & (cached.index <= end)]
                print(f"    -> {len(cached)} days × {len(cached.columns)} symbols (from cache)")
                return cached
            print(f"    Cache stale (ends {cached.index[-1].date()}), re-fetching ...")
        except Exception as e:
            print(f"    Cache read failed ({e}), re-fetching ...")

    BIN = "https://fapi.binance.com"
    oi_cols: Dict[str, pd.Series] = {}

    for sym in symbols:
        try:
            r = requests.get(
                f"{BIN}/futures/data/openInterestHist",
                params={"symbol": sym, "period": "1d", "limit": 500},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            rows = r.json()
            if not rows:
                continue
            df_s = pd.DataFrame(rows)
            df_s["date"] = pd.to_datetime(
                df_s["timestamp"].astype(int), unit="ms"
            ).dt.normalize()
            df_s = (df_s.drop_duplicates("date")
                       .sort_values("date")
                       .set_index("date"))
            df_s.index = df_s.index.tz_localize(None)
            oi_usd = pd.to_numeric(df_s["sumOpenInterestValue"], errors="coerce")
            oi_cols[sym] = oi_usd.pct_change() * 100   # daily % change
            time.sleep(0.05)
        except Exception as e:
            print(f"      ! {sym}: {e}")

    if not oi_cols:
        print("    -> EMPTY — all symbols failed")
        return pd.DataFrame()

    out = pd.DataFrame(oi_cols).sort_index()
    out = out[(out.index >= pd.Timestamp(start)) & (out.index <= pd.Timestamp(end))]

    try:
        out.to_csv(cache_path)
    except Exception:
        pass

    n_ok = int((out.notna().sum(axis=1) >= 10).sum())
    print(f"    -> {len(out)} days × {len(out.columns)} symbols  "
          f"({n_ok} days with ≥10 valid symbols)")
    return out


# Validates the *portfolio-level* daily return series produced by
# simulate().  Complements the per-day simulation bias checks (T1-T10)
# inside institutional_audit.simulation_bias_audit().
#
# Requires a daily return series, which is constructed here by calling
# simulate() and uses internal helpers from institutional_audit.py
# (_equity, _sharpe, _max_dd, _dd_curve) for independent verification.
#
# Tests
# -----
# T11 FILTER ZERO INJECTION   - every day the filter fires must be
#                               exactly 0.0 in the return series.
# T12 ACTIVE DAY COUNT BOUND  - non-zero return days ≤ total – filtered.
# T13 EQUITY CURVE CONSISTENCY- equity moves up/down/flat in lockstep
#                               with positive/negative/zero returns.
# T14 SHARPE REPRODUCIBILITY  - Sharpe from the series matches
#                               institutional_audit._sharpe().
# T15 MAX DRAWDOWN SIGN       - max drawdown ≤ 0 (it is a loss metric,
#                               never positive).
# T16 EQUITY POSITIVITY       - equity never reaches zero or goes
#                               negative (total-ruin check).
# ══════════════════════════════════════════════════════════════════════

def run_daily_series_audit(
    df_4x:             pd.DataFrame,
    params:            Dict,
    filter_mode:       str,
    v3_filter:         Optional[pd.Series],
    label:             str = "",
    daily_with_zeros:  Optional[np.ndarray] = None,
) -> Dict:
    """
    Run T11-T16 checks on a daily return series.

    Parameters
    ----------
    df_4x             : intraday price matrix (rows=bars, cols=trading days)
    params            : strategy parameter dict (same as passed to simulate)
    filter_mode       : 'none' | 'calendar' | 'tail' | 'dispersion' | …
    v3_filter         : pd.Series[bool] index=DatetimeIndex, True=sit flat
                        (pass None when filter_mode == 'none')
    label             : human-readable run identifier for reporting
    daily_with_zeros  : pre-computed return series from simulate(). If supplied,
                        simulate() is NOT called again (avoids double fees panel
                        print and duplicate cost summary output).

    Returns
    -------
    dict with keys: passed, failed, total, tests, label,
                    daily_with_zeros (the constructed series as np.ndarray)
    """
    results: Dict[str, dict] = {}
    passed  = 0
    failed  = 0

    # ── Use pre-computed series or simulate fresh ──────────────────────
    if daily_with_zeros is None:
        daily_with_zeros = simulate(df_4x, params, filter_mode, v3_filter, verbose=False)["daily"]
    r = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)

    # Reconstruct which days should have been filtered to validate T11/T12
    filtered_mask = np.zeros(len(df_4x.columns), dtype=bool)
    for i, col in enumerate(df_4x.columns):
        col_str = str(col)
        if filter_mode == "calendar":
            filtered_mask[i] = is_calendar_bad(col_str)
        elif filter_mode != "none" and v3_filter is not None:
            filtered_mask[i] = is_v3_bad(col_str, v3_filter)

    n_total    = len(r)
    n_filtered = int(filtered_mask.sum())

    # ── T11: Filter zero injection ─────────────────────────────────────
    # Every day flagged by the filter must produce exactly 0.0 in the
    # return series.  A non-zero value would mean live P&L on a day the
    # strategy was supposed to be flat — a direct simulation error.
    if n_filtered > 0:
        filtered_vals = r[filtered_mask]
        n_nonzero     = int((filtered_vals != 0.0).sum())
        t11_pass      = n_nonzero == 0
        results["T11_filter_zero_injection"] = {
            "pass":        t11_pass,
            "expected":    f"All {n_filtered} filtered days = 0.0",
            "got":         f"{n_nonzero} non-zero value(s) on filtered days",
            "description": "Filtered days must produce exactly 0.0 — no phantom P&L while flat",
        }
    else:
        # No filter active: trivially passes (nothing to check)
        t11_pass = True
        results["T11_filter_zero_injection"] = {
            "pass":        True,
            "expected":    "N/A (filter_mode='none' — 0 filtered days)",
            "got":         "N/A",
            "description": "No filter active — test not applicable",
        }
    passed += t11_pass; failed += not t11_pass

    # ── T12: Active day count upper bound ─────────────────────────────
    # The number of days with a non-zero return must not exceed
    # (total_days – filtered_days).  Equality holds when every active
    # day happened to trade; strict inequality is allowed (e.g. a day
    # passes the filter but signal fails and returns exactly 0.0).
    n_active_max = n_total - n_filtered
    n_active_got = int((r != 0.0).sum())
    t12_pass     = n_active_got <= n_active_max
    results["T12_active_day_count_bound"] = {
        "pass":        t12_pass,
        "expected":    f"active days <= {n_active_max}  (total={n_total} filtered={n_filtered})",
        "got":         f"{n_active_got} active (non-zero) days",
        "description": "Non-zero days must not exceed total minus filtered (no trading on flat days)",
    }
    passed += t12_pass; failed += not t12_pass

    # ── T13: Equity curve consistency ─────────────────────────────────
    # Build the equity curve independently with institutional_audit's
    # _equity() and verify it moves in lockstep with the return signs:
    #   r > 0  → equity strictly higher than previous day
    #   r < 0  → equity strictly lower
    #   r == 0 → equity unchanged
    eq       = _ia_equity(r)         # length n+1 (starts at 1.0)
    eq_diffs = np.diff(eq)           # length n

    pos_mask  = r > 0
    neg_mask  = r < 0
    zero_mask = r == 0.0

    up_ok   = bool(np.all(eq_diffs[pos_mask]   > -1e-12))
    down_ok = bool(np.all(eq_diffs[neg_mask]   <  1e-12))
    flat_ok = bool(np.all(np.abs(eq_diffs[zero_mask]) < 1e-12))

    t13_pass   = up_ok and down_ok and flat_ok
    violations = (int(not up_ok) + int(not down_ok) + int(not flat_ok))
    results["T13_equity_curve_consistency"] = {
        "pass":        t13_pass,
        "expected":    "Equity rises on +ret, falls on -ret, unchanged on 0",
        "got":         f"up_ok={up_ok}  down_ok={down_ok}  flat_ok={flat_ok}  (violations={violations})",
        "description": "Equity must move in strict lockstep with return signs — no phantom drifts",
    }
    passed += t13_pass; failed += not t13_pass

    # ── T14: Sharpe reproducibility ───────────────────────────────────
    # Compute Sharpe two ways and verify they agree to floating-point
    # precision.  Discrepancy would indicate the series being passed to
    # run_institutional_audit differs from what the audit actually uses.
    active_r = r[r != 0.0]
    if len(active_r) >= 2:
        sharpe_manual = float(
            np.mean(active_r) / np.std(active_r, ddof=1) * TRADING_DAYS ** 0.5
        )
        sharpe_ia = _ia_sharpe(active_r, TRADING_DAYS)
        t14_pass  = abs(sharpe_manual - sharpe_ia) < 1e-6
        results["T14_sharpe_reproducibility"] = {
            "pass":        t14_pass,
            "expected":    f"{sharpe_manual:.8f}  (manual)",
            "got":         f"{sharpe_ia:.8f}  (institutional_audit._sharpe)",
            "description": "Manual Sharpe must match institutional_audit helper to 1e-6 tolerance",
        }
    else:
        t14_pass = True
        results["T14_sharpe_reproducibility"] = {
            "pass":        True,
            "expected":    "N/A (fewer than 2 active days)",
            "got":         "N/A",
            "description": "Skipped — insufficient active trading days for Sharpe calculation",
        }
    passed += t14_pass; failed += not t14_pass

    # ── T15: Max drawdown is non-positive ─────────────────────────────
    # Max drawdown is defined as (equity – peak) / peak ≤ 0 everywhere.
    # A positive value would mean equity exceeded its own historical peak
    # AFTER the peak was recorded — a numerical impossibility indicating
    # a rolling-peak bug in _dd_curve() or _equity().
    if len(r) > 0:
        mdd      = _ia_max_dd(eq)   # already computed eq above
        t15_pass = mdd <= 1e-12     # allow floating-point rounding
        results["T15_maxdd_non_positive"] = {
            "pass":        t15_pass,
            "expected":    "MaxDD <= 0.0",
            "got":         f"{mdd:.8f}",
            "description": "Max drawdown must be non-positive — equity cannot exceed its own peak",
        }
    else:
        t15_pass = True
        results["T15_maxdd_non_positive"] = {
            "pass":        True,
            "expected":    "N/A (empty series)",
            "got":         "N/A",
            "description": "Skipped — empty return series",
        }
    passed += t15_pass; failed += not t15_pass

    # ── T16: Equity positivity ─────────────────────────────────────────
    # Every point on the equity curve must be strictly positive.
    # eq[0] == 1.0 by construction; reaching 0 or below means a single
    # day returned –100% or worse — either a data error or a leverage
    # calculation that allows total ruin.
    n_nonpositive = int(np.sum(eq <= 0))
    t16_pass      = n_nonpositive == 0
    results["T16_equity_positivity"] = {
        "pass":        t16_pass,
        "expected":    "All equity values > 0",
        "got":         f"{n_nonpositive} value(s) <= 0 out of {len(eq)} equity points",
        "description": "Equity must remain positive throughout — a zero/negative value implies total ruin",
    }
    passed += t16_pass; failed += not t16_pass

    return {
        "passed":           passed,
        "failed":           failed,
        "total":            passed + failed,
        "tests":            results,
        "label":            label,
        "daily_with_zeros": daily_with_zeros,   # expose series for downstream use
    }


def print_daily_series_audit_report(audit: Dict, label: str = "") -> None:
    """Print T11-T16 daily return series audit results in the same style
    as institutional_audit.print_simulation_bias_report()."""
    if not audit:
        return

    total  = audit["total"]
    passed = audit["passed"]
    failed = audit["failed"]
    lbl    = label or audit.get("label", "")

    grade = "✅ CLEAN" if failed == 0 else (
            "⚠ SUSPECT" if failed <= 1 else "❌ BIASED")

    print()
    print("┌─ DAILY SERIES AUDIT ────────────────────────────────")
    print(f"│  {lbl}{'  |  ' if lbl else ''}Passed: {passed}/{total}   {grade}")
    print(f"│")

    for key, t in audit["tests"].items():
        icon = "✅" if t["pass"] else "❌"
        name = key.replace("_", " ").title()
        print(f"│  {icon} {name}")
        if not t["pass"]:
            print(f"│     Expected : {t['expected']}")
            print(f"│     Got      : {t['got']}")
            print(f"│     → {t['description']}")
        else:
            print(f"│     {t['description']}")

    print(f"│")
    if failed == 0:
        print(f"│  All {total} daily-series checks passed.")
        print(f"│  Portfolio-level return construction is causally sound.")
    else:
        print(f"│  ⚠  {failed} check(s) FAILED — investigate before trusting results.")
    print(f"└{'─'*53}")


# ══════════════════════════════════════════════════════════════════════
# PERFORMANCE-BASED LEVERAGE SCALAR
# ══════════════════════════════════════════════════════════════════════

def build_perf_leverage_scalar(
    col_dates:          list,
    window:             int   = 30,
    sortino_target:     float = 3.0,
    dr_floor:           float = 0.0,
    max_boost:          float = 1.5,
    prior_returns:      Optional[pd.Series] = None,
) -> pd.Series:
    """
    Build a per-day leverage scalar in [1.0, max_boost] from lagged rolling
    Sortino and avg-daily-return signals (BOOST mode).

    Static params are the floor — the scalar multiplies them upward during
    strong periods:
        boosted_lev = static_lev * scalar
        scalar = 1.0 + (max_boost - 1.0) * sortino_scalar

    Parameters
    ----------
    col_dates       : list of pd.Timestamp (one per matrix column / trading day)
    window          : rolling look-back in days
    sortino_target  : annualised Sortino at which scalar reaches max_boost
    dr_floor        : if rolling avg daily return (lagged) < dr_floor, scalar
                      is held at 1.0 (no boost, stay at static leverage)
    max_boost       : maximum multiplier on static leverage (1.5 = up to +50%)
    prior_returns   : optional warm-start returns (see simulate() for details)

    Returns
    -------
    pd.Series indexed by pd.Timestamp, values in [1.0, max_boost].
    Days before the warm-up window receive scalar = 1.0 (static leverage).
    """
    TRADING_DAYS_ANN = 365

    # Build a return buffer: if prior_returns supplied, prepend for warm-up
    dates_valid = [d for d in col_dates if d is not None]
    if not dates_valid:
        return pd.Series(1.0, index=pd.DatetimeIndex(col_dates))

    # We accumulate returns as they are simulated day-by-day, so this function
    # pre-computes the scalar from prior_returns only (bootstrap phase).
    # During live simulation the scalar is recomputed incrementally — see
    # simulate() which calls _update_perf_scalar() each day.
    # Here we just build the initial warm-up Series if prior_returns exist.

    scalar_map: Dict[pd.Timestamp, float] = {}

    ret_buf: list = []
    if prior_returns is not None:
        prior_aligned = prior_returns.reindex(
            pd.DatetimeIndex([d for d in dates_valid])
        ).dropna()
        ret_buf = list(prior_aligned.values)

    for date in dates_valid:
        # Scalar for TODAY uses yesterday's rolling window (already in ret_buf)
        if len(ret_buf) < window:
            scalar_map[date] = 1.0   # warm-up: static leverage, no boost
        else:
            r_win = np.array(ret_buf[-window:])
            mu    = float(r_win.mean())
            neg   = r_win[r_win < 0]
            dsd   = float(neg.std(ddof=1)) if len(neg) > 1 else 1e-9
            sortino_val = mu / dsd * np.sqrt(TRADING_DAYS_ANN) if dsd > 1e-9 else 0.0
            avg_dr = mu * 100

            if avg_dr < dr_floor:
                scalar_map[date] = 1.0   # below floor: no boost
            else:
                s = float(np.clip(sortino_val / sortino_target, 0.0, 1.0))
                scalar_map[date] = 1.0 + (max_boost - 1.0) * s

        # NOTE: we do NOT push the actual return for 'date' here because we
        # don't have it yet — simulate() pushes returns incrementally via its
        # internal ret_buf.  The values in scalar_map for dates not covered
        # by prior_returns will be overwritten by simulate()'s inline logic.

    return pd.Series(scalar_map)


# ══════════════════════════════════════════════════════════════════════
# SIMULATION
# ══════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════
# PERIODIC PROFIT HARVEST
# ══════════════════════════════════════════════════════════════════════

class PeriodicProfitHarvest:
    """
    Skims profits from the compounding equity base at a fixed frequency
    whenever cumulative profit since the last harvest exceeds a threshold.

    Preserves convexity: does NOT cap intraday returns or truncate winners.
    Instead, removes a fraction of accumulated profit from the equity base
    at each checkpoint, reducing the capital at risk going forward.

    Parameters
    ----------
    frequency     : "daily" | "weekly" | "monthly"
    threshold     : harvest when profit >= watermark * threshold  (e.g. 0.20)
    harvest_frac  : fraction of that profit to remove             (e.g. 0.50)
    """

    def __init__(self, frequency: str, threshold: float, harvest_frac: float):
        if frequency not in ("daily", "weekly", "monthly"):
            raise ValueError(f"PPH frequency must be daily/weekly/monthly, got {frequency!r}")
        self.frequency     = frequency
        self.threshold     = threshold
        self.harvest_frac  = harvest_frac
        self._watermark: Optional[float] = None   # equity at last harvest
        self.total_harvested: float = 0.0          # cumulative $ harvested
        self.harvest_log: list = []                # [(date_str, equity_before, harvest_$)]

    # ── public ────────────────────────────────────────────────────────

    def check(self, date_str: str, equity: float) -> float:
        """
        Call once per day, after the day's return is applied.
        Returns the $ amount harvested (0.0 if no harvest today).
        The caller subtracts the returned amount from running equity.
        """
        if not self._is_rebalance_date(date_str):
            return 0.0

        if self._watermark is None:
            self._watermark = equity
            return 0.0

        profit = equity - self._watermark
        if profit <= self._watermark * self.threshold:
            return 0.0

        harvest              = profit * self.harvest_frac
        self._watermark      = equity - harvest   # new watermark on reduced equity
        self.total_harvested += harvest
        self.harvest_log.append((date_str, equity, harvest))
        return harvest

    def reset(self):
        """Reset for a fresh simulation run (e.g. walk-forward fold)."""
        self._watermark      = None
        self.total_harvested = 0.0
        self.harvest_log     = []

    # ── private ───────────────────────────────────────────────────────

    def _is_rebalance_date(self, date_str: str) -> bool:
        if self.frequency == "daily":
            return True
        try:
            dt = pd.Timestamp(date_str)
        except Exception:
            return False
        if self.frequency == "weekly":
            return dt.weekday() == 4          # Friday
        if self.frequency == "monthly":
            next_day = dt + pd.Timedelta(days=1)
            return next_day.month != dt.month
        return False

    def summary(self) -> dict:
        return dict(
            frequency        = self.frequency,
            threshold        = self.threshold,
            harvest_frac     = self.harvest_frac,
            n_harvests       = len(self.harvest_log),
            total_harvested  = self.total_harvested,
            harvest_log      = self.harvest_log,
        )

    def print_summary(self, starting_capital: float, label: str = ""):
        tag = f" [{label}]" if label else ""
        pct = self.total_harvested / starting_capital * 100 if starting_capital else 0.0
        print(f"    ── PPH Summary{tag} ─────────────────────────────────────────────")
        print(f"       Frequency     : {self.frequency}")
        print(f"       Threshold     : {self.threshold*100:.0f}%  "
              f"Harvest fraction: {self.harvest_frac*100:.0f}%")
        print(f"       Harvests fired: {len(self.harvest_log)}")
        print(f"       Total banked  : ${self.total_harvested:,.2f}  "
              f"({pct:.1f}% of starting capital)")
        if self.harvest_log:
            print(f"       Harvest log   :")
            for (dt, eq_b, h) in self.harvest_log:
                print(f"         {dt}  equity_before=${eq_b:,.2f}  "
                      f"harvested=${h:,.2f}")
        print(f"    ────────────────────────────────────────────────────────────")


# ══════════════════════════════════════════════════════════════════════
# EQUITY RATCHET
# ══════════════════════════════════════════════════════════════════════

class EquityRatchet:
    """
    Portfolio-level floor mechanism that locks in gains by ratcheting an
    equity floor upward whenever the portfolio grows past a trigger threshold.

    When equity falls below the floor the ratchet enters risk-off mode.
    Risk-off persists until equity recovers above the floor; the floor
    itself does NOT move down (it only ever ratchets upward).

    Parameters
    ----------
    frequency   : "daily" | "weekly" | "monthly"  — how often to check / update
    trigger     : min fractional growth before the floor ratchets up  (e.g. 0.20)
    lock_pct    : floor sits this far below the ratchet equity         (e.g. 0.15)
    risk_off_lev_scale : leverage multiplier when in risk-off mode
                    0.0 = sit completely flat
                    0.5 = trade at half leverage
                    1.0 = no change (ratchet only tracks, never acts)
    """

    def __init__(self, frequency: str, trigger: float, lock_pct: float,
                 risk_off_lev_scale: float = 0.0):
        if frequency not in ("daily", "weekly", "monthly"):
            raise ValueError(f"Ratchet frequency must be daily/weekly/monthly, got {frequency!r}")
        self.frequency          = frequency
        self.trigger            = trigger
        self.lock_pct           = lock_pct
        self.risk_off_lev_scale = risk_off_lev_scale

        self._ratchet_equity: Optional[float] = None  # last high that moved the floor
        self._floor:          Optional[float] = None  # current protection floor ($)
        self._risk_off:       bool = False             # are we below the floor right now?

        self.ratchet_log:  list = []   # [(date_str, old_floor, new_floor, equity)]
        self.risk_off_log: list = []   # [(date_str, equity, floor, "enter"|"exit")]
        self.n_flat_days:  int  = 0    # trading days spent in risk-off

    # ── public ────────────────────────────────────────────────────────

    def update(self, date_str: str, equity: float) -> bool:
        """
        Call once per day AFTER the day's return is applied.

        1. On rebalance days: checks whether equity has grown enough to
           ratchet the floor upward.
        2. Every day: checks whether equity is below the floor and updates
           the risk-off flag accordingly.

        Returns True if risk-off is ACTIVE going into the NEXT day
        (i.e. the caller should reduce leverage tomorrow).
        """
        # ── Initialise on first call ───────────────────────────────────
        if self._ratchet_equity is None:
            self._ratchet_equity = equity
            self._floor          = equity * (1.0 - self.lock_pct)
            return False

        # ── Rebalance day: try to ratchet floor upward ─────────────────
        if self._is_rebalance_date(date_str):
            growth = (equity - self._ratchet_equity) / self._ratchet_equity
            if growth >= self.trigger:
                old_floor            = self._floor
                self._ratchet_equity = equity
                self._floor          = equity * (1.0 - self.lock_pct)
                self.ratchet_log.append((date_str, old_floor, self._floor, equity))

        # ── Every day: evaluate floor breach ──────────────────────────
        was_risk_off = self._risk_off
        self._risk_off = (equity < self._floor)

        if self._risk_off and not was_risk_off:
            self.risk_off_log.append((date_str, equity, self._floor, "enter"))
        elif not self._risk_off and was_risk_off:
            self.risk_off_log.append((date_str, equity, self._floor, "exit"))
            # Do NOT lower the floor on recovery — it only moves up

        return self._risk_off

    @property
    def in_risk_off(self) -> bool:
        """True if the NEXT day should be risk-off (set by update())."""
        return self._risk_off

    @property
    def floor(self) -> Optional[float]:
        return self._floor

    def reset(self):
        """Reset for a fresh simulation run (e.g. walk-forward fold)."""
        self._ratchet_equity = None
        self._floor          = None
        self._risk_off       = False
        self.ratchet_log     = []
        self.risk_off_log    = []
        self.n_flat_days     = 0

    # ── private ───────────────────────────────────────────────────────

    def _is_rebalance_date(self, date_str: str) -> bool:
        if self.frequency == "daily":
            return True
        try:
            dt = pd.Timestamp(date_str)
        except Exception:
            return False
        if self.frequency == "weekly":
            return dt.weekday() == 4          # Friday
        if self.frequency == "monthly":
            next_day = dt + pd.Timedelta(days=1)
            return next_day.month != dt.month
        return False

    def summary(self) -> dict:
        return dict(
            frequency          = self.frequency,
            trigger            = self.trigger,
            lock_pct           = self.lock_pct,
            risk_off_lev_scale = self.risk_off_lev_scale,
            n_ratchets         = len(self.ratchet_log),
            n_risk_off_events  = sum(1 for e in self.risk_off_log if e[3] == "enter"),
            n_flat_days        = self.n_flat_days,
            floor              = self._floor,
            ratchet_log        = self.ratchet_log,
            risk_off_log       = self.risk_off_log,
        )

    def print_summary(self, label: str = ""):
        tag = f" [{label}]" if label else ""
        n_events = sum(1 for e in self.risk_off_log if e[3] == "enter")
        print(f"    ── Equity Ratchet Summary{tag} ──────────────────────────────────")
        print(f"       Frequency      : {self.frequency}")
        print(f"       Trigger        : {self.trigger*100:.0f}%  "
              f"Lock: {self.lock_pct*100:.0f}%  "
              f"Risk-off scale: {self.risk_off_lev_scale:.2f}")
        print(f"       Floor ratchets : {len(self.ratchet_log)}")
        print(f"       Risk-off events: {n_events}  "
              f"({self.n_flat_days} flat/reduced days)")
        if self._floor is not None:
            print(f"       Final floor    : ${self._floor:,.2f}")
        if self.ratchet_log:
            print(f"       Ratchet log    :")
            for (dt, old_f, new_f, eq) in self.ratchet_log:
                print(f"         {dt}  equity=${eq:,.2f}  "
                      f"floor ${old_f:,.2f} → ${new_f:,.2f}")
        if self.risk_off_log:
            print(f"       Risk-off log   :")
            for (dt, eq, fl, evt) in self.risk_off_log:
                print(f"         {dt}  [{evt:>5}]  equity=${eq:,.2f}  floor=${fl:,.2f}")
        print(f"    ────────────────────────────────────────────────────────────")


# ══════════════════════════════════════════════════════════════════════
# REGIME-ADAPTIVE EQUITY RATCHET
# ══════════════════════════════════════════════════════════════════════

def build_vol_regime_series(daily_returns:  "pd.Series",vol_window: int = 20,vol_low:float = 0.03,vol_high: float = 0.07) -> "pd.Series":
    """
    Pre-compute a date-indexed regime label series from a returns Series.

    Labels: "low" | "normal" | "high"

    Uses a 1-day lag: today's regime label is built from yesterday's
    rolling vol, so there is zero lookahead inside the simulation loop.

    Parameters
    ----------
    daily_returns : pd.Series, index=DatetimeIndex, values=fractional daily returns
    vol_window    : rolling std window (days)
    vol_low       : daily vol (std) below this → "low" regime
    vol_high      : daily vol (std) above this → "high" regime

    Returns
    -------
    pd.Series[str], same index as daily_returns, values "low"/"normal"/"high"
    """
    r = daily_returns.copy().fillna(0.0)
    rvol = r.rolling(vol_window, min_periods=max(3, vol_window // 2)).std()
    # Lag 1 day: yesterday's vol gates today's ratchet parameters
    rvol_lag = rvol.shift(1)

    regime = pd.Series("normal", index=r.index, dtype=object)
    regime[rvol_lag < vol_low]  = "low"
    regime[rvol_lag > vol_high] = "high"
    # Warm-up days (NaN vol) → default "normal"
    regime[rvol_lag.isna()]     = "normal"
    return regime


def _get_regime(vol_regime_series: Optional["pd.Series"],date_str: str) -> str:
    """
    Look up the pre-computed regime label for a given date string.
    Returns "normal" if the series is None or the date is not found.
    Fast O(1) dict-based lookup (series converted to dict on first call
    via the _col_to_timestamp / .loc path).
    """
    if vol_regime_series is None:
        return "normal"
    try:
        ts = _col_to_timestamp(date_str)
        if ts is None:
            return "normal"
        if ts in vol_regime_series.index:
            return str(vol_regime_series.loc[ts])
        # Try floor-matching by date
        ts_date = ts.normalize()
        if ts_date in vol_regime_series.index:
            return str(vol_regime_series.loc[ts_date])
    except Exception:
        pass
    return "normal"


class AdaptiveEquityRatchet:
    """
    Equity Ratchet with regime-varying trigger and lock_pct parameters.

    Instead of fixed thresholds, the ratchet tightens in quiet markets
    (protect small gains quickly) and loosens in explosive markets
    (let convex winners run before locking in gains).

    The volatility regime is pre-computed outside this class as a
    date-indexed pd.Series of "low"/"normal"/"high" labels, then passed
    into update() each day. This keeps the class simple and the vol
    computation identical across the main run and the sweep.

    Parameters
    ----------
    frequency          : "daily" | "weekly" | "monthly"
    regime_table       : dict mapping regime label → {"trigger": float, "lock_pct": float}
                         e.g. {"low": {"trigger":0.10,"lock_pct":0.10}, ...}
    risk_off_lev_scale : leverage multiplier when in risk-off (0.0 = flat)
    """

    def __init__(self, frequency: str, regime_table: dict,
                 risk_off_lev_scale: float = 0.0,
                 floor_decay_rate: float = 1.0):
        if frequency not in ("daily", "weekly", "monthly"):
            raise ValueError(f"AdaptiveRatchet frequency must be daily/weekly/monthly, "
                             f"got {frequency!r}")
        self.frequency          = frequency
        self.regime_table       = regime_table
        self.risk_off_lev_scale = risk_off_lev_scale
        self.floor_decay_rate   = float(floor_decay_rate)   # applied each flat day

        self._ratchet_equity: Optional[float] = None
        self._floor:          Optional[float] = None
        self._risk_off:       bool = False
        self._consecutive_flat: int = 0   # days spent continuously in risk-off

        self.ratchet_log:    list = []  # (date, regime, old_floor, new_floor, equity)
        self.risk_off_log:   list = []  # (date, equity, floor, "enter"|"exit")
        self.regime_log:     list = []  # (date, regime)  — one per rebalance day
        self.n_flat_days:    int  = 0

    # ── public ────────────────────────────────────────────────────────

    def update(self, date_str: str, equity: float, regime: str = "normal") -> bool:
        """
        Call once per day AFTER the day's return is applied.

        regime : "low" | "normal" | "high" — pre-computed for this date_str.
                 The ratchet uses the regime's trigger/lock_pct on rebalance days.

        Returns True if risk-off is ACTIVE going into the NEXT day.
        """
        params   = self.regime_table.get(regime, self.regime_table.get("normal", {}))
        trigger  = float(params.get("trigger",  0.20))
        lock_pct = float(params.get("lock_pct", 0.15))

        # ── Initialise on first call ───────────────────────────────────
        if self._ratchet_equity is None:
            self._ratchet_equity = equity
            self._floor          = equity * (1.0 - lock_pct)
            return False

        # ── Floor decay: soften the floor each day we're in risk-off ──
        # Applied BEFORE the rebalance check so that a recovering equity
        # can naturally break back through the decayed floor and trigger
        # an upward ratchet on the same rebalance day.
        if self._risk_off and self.floor_decay_rate < 1.0 and self._floor is not None:
            self._floor *= self.floor_decay_rate

        # ── Rebalance day: try to ratchet floor upward ─────────────────
        if self._is_rebalance_date(date_str):
            self.regime_log.append((date_str, regime))
            growth = (equity - self._ratchet_equity) / self._ratchet_equity
            if growth >= trigger:
                old_floor            = self._floor
                self._ratchet_equity = equity
                self._floor          = equity * (1.0 - lock_pct)
                self.ratchet_log.append(
                    (date_str, regime, old_floor, self._floor, equity))

        # ── Every day: evaluate floor breach ──────────────────────────
        was_risk_off   = self._risk_off
        self._risk_off = (equity < self._floor)

        if self._risk_off and not was_risk_off:
            self.risk_off_log.append((date_str, equity, self._floor, "enter"))
            self._consecutive_flat = 0
        elif not self._risk_off and was_risk_off:
            self.risk_off_log.append((date_str, equity, self._floor, "exit"))
            self._consecutive_flat = 0
        elif self._risk_off:
            self._consecutive_flat += 1

        return self._risk_off

    @property
    def in_risk_off(self) -> bool:
        return self._risk_off

    @property
    def floor(self) -> Optional[float]:
        return self._floor

    def reset(self):
        self._ratchet_equity   = None
        self._floor            = None
        self._risk_off         = False
        self._consecutive_flat = 0
        self.ratchet_log       = []
        self.risk_off_log      = []
        self.regime_log        = []
        self.n_flat_days       = 0

    # ── private ───────────────────────────────────────────────────────

    def _is_rebalance_date(self, date_str: str) -> bool:
        if self.frequency == "daily":
            return True
        try:
            dt = pd.Timestamp(date_str)
        except Exception:
            return False
        if self.frequency == "weekly":
            return dt.weekday() == 4
        if self.frequency == "monthly":
            next_day = dt + pd.Timedelta(days=1)
            return next_day.month != dt.month
        return False

    def summary(self) -> dict:
        regime_counts = {}
        for _, rg in self.regime_log:
            regime_counts[rg] = regime_counts.get(rg, 0) + 1
        return dict(
            frequency          = self.frequency,
            regime_table       = self.regime_table,
            risk_off_lev_scale = self.risk_off_lev_scale,
            floor_decay_rate   = self.floor_decay_rate,
            n_ratchets         = len(self.ratchet_log),
            n_risk_off_events  = sum(1 for e in self.risk_off_log if e[3] == "enter"),
            n_flat_days        = self.n_flat_days,
            floor              = self._floor,
            regime_counts      = regime_counts,
            ratchet_log        = self.ratchet_log,
            risk_off_log       = self.risk_off_log,
        )

    def print_summary(self, label: str = ""):
        tag = f" [{label}]" if label else ""
        n_events = sum(1 for e in self.risk_off_log if e[3] == "enter")
        regime_counts = {}
        for _, rg in self.regime_log:
            regime_counts[rg] = regime_counts.get(rg, 0) + 1

        print(f"    ── Adaptive Ratchet Summary{tag} ────────────────────────────────")
        print(f"       Frequency      : {self.frequency}")
        print(f"       Risk-off scale : {self.risk_off_lev_scale:.2f}")
        print(f"       Floor decay    : {self.floor_decay_rate:.4f}/day"
              f"  (~{(1 - self.floor_decay_rate**30)*100:.1f}% over 30 flat days)"
              if self.floor_decay_rate < 1.0 else
              f"       Floor decay    : none (1.0000)")
        print(f"       Regime table   :")
        for rg_name in ("low", "normal", "high"):
            p   = self.regime_table.get(rg_name, {})
            cnt = regime_counts.get(rg_name, 0)
            print(f"         {rg_name:<7} trigger={p.get('trigger','-')*100:.0f}%  "
                  f"lock={p.get('lock_pct','-')*100:.0f}%  "
                  f"({cnt} rebalance days)")
        print(f"       Floor ratchets : {len(self.ratchet_log)}")
        print(f"       Risk-off events: {n_events}  ({self.n_flat_days} flat/reduced days)")
        if self._floor is not None:
            print(f"       Final floor    : ${self._floor:,.2f}")
        if self.ratchet_log:
            print(f"       Ratchet log    :")
            for (dt, rg, old_f, new_f, eq) in self.ratchet_log:
                print(f"         {dt}  [{rg:<6}]  equity=${eq:,.2f}  "
                      f"floor ${old_f:,.2f} → ${new_f:,.2f}")
        if self.risk_off_log:
            print(f"       Risk-off log   :")
            for (dt, eq, fl, evt) in self.risk_off_log:
                print(f"         {dt}  [{evt:>5}]  equity=${eq:,.2f}  floor=${fl:,.2f}")
        print(f"    ────────────────────────────────────────────────────────────")


def simulate(df_4x: pd.DataFrame,
             params: Dict,
             filter_mode: str,
             v3_filter: Optional[pd.Series] = None,
             symbol_counts: Optional[pd.Series] = None,
             perf_lev_params: Optional[Dict] = None,
             vol_lev_params:   Optional[Dict] = None,
             contra_lev_params: Optional[Dict] = None,
             fees_csv_path: Optional[str] = None,
             pph: Optional["PeriodicProfitHarvest"] = None,
             ratchet: Optional["EquityRatchet"] = None,
             adaptive_ratchet: Optional["AdaptiveEquityRatchet"] = None,
             vol_regime_series: Optional["pd.Series"] = None,
             verbose: bool = True) -> np.ndarray:
    """
    filter_mode: 'none' | 'calendar' | 'v3'
    v3_filter: pd.Series[bool], index=DatetimeIndex, True=sit flat
    symbol_counts: pd.Series[int], index=DatetimeIndex, symbols deployed per day.
        Used to compute realistic fee drag. If None, uses a flat estimate of 7.
    perf_lev_params: dict with keys window, sortino_target, dr_floor, min_scalar.
        When supplied, L_BASE and L_HIGH are scaled each day by a rolling
        performance scalar built from lagged Sortino and avg daily return.
        Pass None (default) to use static leverage from params as before.
    vol_lev_params: dict with keys target_vol, window, sharpe_ref,
        dd_threshold, dd_scale, lev_min, lev_max.
        When supplied, leverage is set each day by a volatility-targeting
        model (risk-budget style). Overrides perf_lev_params if both are
        supplied — use one or the other.
        Pass None (default) to use static or perf-lev leverage.
    pph: PeriodicProfitHarvest instance (optional).
        When supplied, checks each day for a profit harvest event and
        reduces the compounding equity base accordingly. Pass None (default)
        to run without the harvest system (legacy behaviour).
    ratchet: EquityRatchet instance (optional).
        When supplied, maintains a rising equity floor. If equity breaches
        the floor the next day trades at ratchet.risk_off_lev_scale × leverage
        (0.0 = sit flat). The ratchet only moves upward; breaching the floor
        does NOT reset it. Pass None (default) for legacy behaviour.
        Can be combined with pph — both operate independently post-trade.
    adaptive_ratchet: AdaptiveEquityRatchet instance (optional).
        Like EquityRatchet but trigger and lock_pct vary by vol regime.
        Requires vol_regime_series to be supplied alongside it.
        Cannot be combined with ratchet (use one or the other).
    vol_regime_series: pd.Series[str], index=DatetimeIndex, values "low"/"normal"/"high".
        Pre-computed regime labels (1-day lag already applied). Built by
        build_vol_regime_series() before simulate() is called. Required
        when adaptive_ratchet is not None; ignored otherwise.
    """
    early_y_1x  = float(params["EARLY_KILL_Y"])   # stored as 1x — no scaling needed
    _l_base_val = float(params.get("L_BASE", 1.0))
    _l_high_val = float(params["L_HIGH"])
    strong_thr  = float(params.get("EARLY_INSTILL_Y", 0.002))

    # ── Performance leverage scaling state ────────────────────────────
    _plev_on      = perf_lev_params is not None
    _plev_window  = int(perf_lev_params["window"])          if _plev_on else 30
    _plev_st_tgt  = float(perf_lev_params["sortino_target"]) if _plev_on else 3.0
    _plev_dr_flr  = float(perf_lev_params["dr_floor"])       if _plev_on else 0.0
    _plev_max_b   = float(perf_lev_params["max_boost"])       if _plev_on else 1.0
    _plev_ret_buf: list = []   # rolling buffer of net daily returns (filled as we go)
    _plev_scalar_history: list = []  # (date_str, scalar, sortino, avg_dr) for reporting
    TRADING_DAYS_ANN = 365

    # ── Volatility-targeted leverage state ───────────────────────────
    _vlev_on       = vol_lev_params is not None
    _vlev_tgt      = float(vol_lev_params['target_vol'])   if _vlev_on else 0.02
    _vlev_window   = int(vol_lev_params['window'])         if _vlev_on else 20
    _vlev_sh_ref   = float(vol_lev_params['sharpe_ref'])   if _vlev_on else 2.0
    _vlev_dd_thr   = float(vol_lev_params['dd_threshold']) if _vlev_on else -0.15
    _vlev_dd_scale = float(vol_lev_params['dd_scale'])     if _vlev_on else 1.0
    _vlev_max_b    = float(vol_lev_params['max_boost'])    if _vlev_on else 2.0
    _lev_q_mode    = LEV_QUANTIZATION_MODE if _vlev_on else "off"
    _lev_q_step    = max(float(LEV_QUANTIZATION_STEP), 1e-9) if _vlev_on else 0.1
    _vlev_ret_buf: list  = []   # rolling return buffer (same lag discipline as plev)
    _vlev_eq_peak: float = 1.0  # running equity peak (for drawdown tracking)
    _vlev_scalar_history: list = []  # (date_str, final_lev) for dashboard panel

    # ── Contrarian leverage state ─────────────────────────────────
    _clev_on      = contra_lev_params is not None
    _clev_sigs    = list(contra_lev_params['signals'])    if _clev_on else ['Sharpe', 'AvgDailyRet%']
    _clev_window  = int(contra_lev_params['window'])      if _clev_on else 30
    _clev_max_b   = float(contra_lev_params['max_boost']) if _clev_on else 2.0
    _clev_dd_thr  = float(contra_lev_params['dd_threshold']) if _clev_on else -0.15
    _clev_ret_buf: list  = []   # rolling return buffer
    _clev_eq_peak: float = 1.0  # equity peak for DD tracking
    _clev_eq_lvl:  float = 1.0  # current equity level
    _clev_scalar_history: list = []  # (date_str, boost) for dashboard

    raw_ps  = params.get("PORT_SL", -0.049)
    en_ps   = raw_ps is not None and raw_ps != 0
    ps_val  = float(raw_ps) if en_ps else -999.0

    raw_ef  = params.get("EARLY_FILL_Y", None)
    en_ef   = raw_ef is not None and raw_ef != 0
    ef_val  = float(raw_ef) if en_ef else 999.0

    raw_efx = params.get("EARLY_FILL_X", None)
    efx_val = int(raw_efx) if (raw_efx is not None and raw_efx != 0) else 0

    daily = []
    flat_days = 0
    first_flat_logged = False
    parse_fail = 0
    parse_ok   = 0
    trade_days     = 0
    _exit_bars: dict = {}  # date_str -> bar index where position was closed
    total_gross    = 0.0
    total_fees     = 0.0
    equity_running = 1.0   # compound equity multiplier (1.0 = starting capital)
                           # updated as: equity_running *= (1 + r_net)
                           # _equity_start = STARTING_CAPITAL * equity_running
    _eq_before_sim = 1.0   # equity_running before daily update (for fixed mode acct return)
    _notional_sim  = float(STARTING_CAPITAL)  # deployment notional (for fixed mode acct return)
    _equity_level  = 1.0   # for vol-lev drawdown guard (1 = starting capital)
    _fees_rows     = []    # [(date, start, invested, vol, fee, fund, end, gross%, net%, pnl)]
    _lev_used_list    = []    # leverage scalar on each active (non-flat) trading day
    _lev_high_returns = []    # net returns on days where lev_used >= _l_high_val
    _lev_base_returns = []    # net returns on days where lev_used <  _l_high_val
    _ratchet_risk_off = False  # carries over from previous day's ratchet.update()
    _adaptive_risk_off = False  # same, for adaptive ratchet
    for col in df_4x.columns:
        col_str = str(col)

        # Diagnostic: count parse hits/misses
        _ts_check = _col_to_timestamp(col_str)
        if _ts_check is None:
            parse_fail += 1
        else:
            parse_ok += 1

        # Apply filter
        sit_flat = False
        if filter_mode == "calendar":
            sit_flat = is_calendar_bad(col_str)
        elif filter_mode in ("v3", "v4", "v5", "v5a", "v5b", "v5c", "v5d",
                             "v5b1", "v5b2", "v5b3", "v5b4",
                             "v5d1", "v5d2", "v5d3", "v5d4",
                             "tail", "dispersion", "tail_disp",
                             "tail_disp_vol", "tail_or_vol", "tail_and_vol",
                             "btc_ma", "btc_ma_tail",
                             "tail_blofin") \
                and v3_filter is not None:
            sit_flat = is_v3_bad(col_str, v3_filter)

        if sit_flat:
            if verbose and not first_flat_logged:
                ts = _col_to_timestamp(col_str)
                print(f"    [filter check] first flat day: col='{col_str}' -> {ts}")
                first_flat_logged = True
            if verbose and PRINT_FEES_PANEL:
                _col_ts_str = str(_col_to_timestamp(col_str) or col_str)[:10]
                _equity_now = STARTING_CAPITAL * equity_running
                _fees_rows.append((
                    _col_ts_str,
                    _equity_now,   # Start ($)
                    0.0,           # Margin — not posted (flat)
                    0.0,           # Lev
                    0.0,           # Invested
                    0.0,           # Trade Vol
                    0.0,           # Taker Fee
                    0.0,           # Funding
                    _equity_now,   # End ($) — unchanged
                    0.0,           # Gross%
                    0.0,           # Net%
                    0.0,           # Net P&L
                ))
            daily.append(0.0)
            flat_days += 1
            # Ratchet must track equity even on filter-flat days
            if ratchet is not None:
                _col_ts_flat = str(_col_to_timestamp(col_str) or col_str)[:10]
                _ratchet_risk_off = ratchet.update(
                    _col_ts_flat, STARTING_CAPITAL * equity_running)
            if adaptive_ratchet is not None:
                _col_ts_flat = str(_col_to_timestamp(col_str) or col_str)[:10]
                _ar_rg_flat  = _get_regime(vol_regime_series, _col_ts_flat)
                _adaptive_risk_off = adaptive_ratchet.update(
                    _col_ts_flat, STARTING_CAPITAL * equity_running, _ar_rg_flat)
            continue

        path_4x = df_4x[col].to_numpy(dtype=float)
        path_1x = path_4x / PIVOT_LEVERAGE

        # ── Performance-based leverage scaling ────────────────────────
        # Compute today's scalar from yesterday's rolling window BEFORE
        # calling _apply_hybrid_day_param.  The buffer is filled with the
        # net return AFTER the day completes (strict lag, no lookahead).
        if _plev_on:
            if len(_plev_ret_buf) < _plev_window:
                _day_scalar = 1.0   # warm-up: no boost yet, run at static leverage
            else:
                r_win   = np.array(_plev_ret_buf[-_plev_window:])
                mu      = float(r_win.mean())
                neg     = r_win[r_win < 0]
                dsd     = float(neg.std(ddof=1)) if len(neg) > 1 else 1e-9
                _sortino = mu / dsd * np.sqrt(TRADING_DAYS_ANN) if dsd > 1e-9 else 0.0
                _avg_dr  = mu * 100
                # Contrarian: boost when Sortino is LOW (IC evidence shows
                # high Sortino predicts weaker forward returns).
                # _plev_dr_flr acts as a CEILING — above this avg return,
                # suppress boost (returns already elevated, mean-reversion risk).
                if _avg_dr > _plev_dr_flr and _plev_dr_flr != 0.0:
                    _day_scalar = 1.0   # above ceiling: returns extended, no boost
                else:
                    # Invert: high Sortino → low scalar, low Sortino → high scalar
                    _s = float(np.clip(1.0 - _sortino / _plev_st_tgt, 0.0, 1.0))
                    _day_scalar = 1.0 + (_plev_max_b - 1.0) * _s
            _col_ts_diag = str(_col_to_timestamp(col_str) or col_str)[:10]
            _plev_scalar_history.append((_col_ts_diag, _day_scalar))
            l_base_today = _l_base_val * _day_scalar
            l_high_today = _l_high_val * _day_scalar
        else:
            l_base_today = _l_base_val
            l_high_today = _l_high_val

        # ── Volatility-targeted leverage ──────────────────────────
        # Overrides whatever l_base/l_high were set to above.
        # Uses strict lag: yesterday's buffer gates today's leverage.
        if _vlev_on:
            if len(_vlev_ret_buf) < _vlev_window:
                # Warm-up: no boost yet — run at static leverage
                _vlev_boost = 1.0
            else:
                r_win  = np.array(_vlev_ret_buf[-_vlev_window:])
                _rvol  = float(r_win.std(ddof=1))
                _rvol  = max(_rvol, 1e-9)
                # 1. Vol scalar: boost when vol < target, clamp to [1, max_boost]
                #    target_vol / realized_vol > 1 when vol is compressed (calm regime)
                _vc    = float(np.clip(_vlev_tgt / _rvol, 1.0, _vlev_max_b))
                # 2. Sharpe scalar: contrarian — dampen boost when Sharpe is high
                # (IC evidence: high Sharpe predicts weaker fwd returns)
                # clip(ref/sh): high Sharpe → scalar < 1 → reduce vol boost
                #               low Sharpe  → scalar > 1 → amplify vol boost
                _mu    = float(r_win.mean())
                _sh    = _mu / _rvol * np.sqrt(TRADING_DAYS_ANN)
                _sh    = max(_sh, 0.1)   # avoid div/0 or negative
                _sc    = float(np.clip(_vlev_sh_ref / _sh, 0.5, 2.0))
                # 3. Drawdown guard: suppress boost during active drawdown
                _dd    = (_equity_level / max(_vlev_eq_peak, 1e-9)) - 1.0
                _dg    = _vlev_dd_scale if _dd < _vlev_dd_thr else 1.0
                # Final boost scalar: floor at 1.0 (never reduce below static)
                _vlev_boost = float(np.clip(_vc * _sc * _dg, 1.0, _vlev_max_b))
            if _lev_q_mode == "binary":
                _mid = 1.0 + ((_vlev_max_b - 1.0) * 0.5)
                _vlev_boost = _vlev_max_b if _vlev_boost >= _mid else 1.0
            elif _lev_q_mode == "stepped":
                _steps = round((_vlev_boost - 1.0) / _lev_q_step)
                _vlev_boost = 1.0 + (_steps * _lev_q_step)
                _vlev_boost = float(np.clip(_vlev_boost, 1.0, _vlev_max_b))
            _col_ts_v = str(_col_to_timestamp(col_str) or col_str)[:10]
            _vlev_scalar_history.append((_col_ts_v, _vlev_boost))
            # Multiply static params — ratio between L_BASE and L_HIGH preserved
            l_high_today = _l_high_val * _vlev_boost
            l_base_today = _l_base_val * _vlev_boost

        # ── Contrarian leverage ──────────────────────────────────
        # Boost above static when rolling metrics are at recent lows.
        # Uses percentile rank of each signal in its own trailing window.
        if _clev_on:
            if len(_clev_ret_buf) < _clev_window:
                _clev_boost = 1.0   # warm-up: static leverage
            else:
                r_win = np.array(_clev_ret_buf[-_clev_window:])
                _rvol = float(r_win.std(ddof=1))
                _rvol = max(_rvol, 1e-9)
                _mu   = float(r_win.mean())
                # Build named signal values for this window
                _sig_vals = {
                    'Sharpe':       _mu / _rvol * np.sqrt(TRADING_DAYS_ANN),
                    'AvgDailyRet%': _mu * 100,
                    'Sortino':      (_mu / float(r_win[r_win < 0].std(ddof=1))
                                     * np.sqrt(TRADING_DAYS_ANN))
                                    if len(r_win[r_win < 0]) > 1 else 0.0,
                    'CAGR%':        (float(np.prod(1 + r_win) ** (TRADING_DAYS_ANN / len(r_win))) - 1) * 100,
                    'Calmar':       (lambda _cagr, _cum:
                                        _cagr / abs(float((((_cum - np.maximum.accumulate(_cum))
                                                            / np.maximum.accumulate(_cum)).min()) * 100))
                                        if abs(((_cum - np.maximum.accumulate(_cum))
                                                / np.maximum.accumulate(_cum)).min()) > 1e-9
                                        else 0.0
                                   )(
                                       (float(np.prod(1 + r_win) ** (TRADING_DAYS_ANN / len(r_win))) - 1) * 100,
                                       np.cumprod(1 + r_win)
                                   ),
                }
                # Percentile rank of current value vs trailing history
                # Rank is computed across the full ret_buf (not just window)
                # so the comparison is against the whole known distribution.
                _full = np.array(_clev_ret_buf)
                _full_rvol = max(float(_full.std(ddof=1)), 1e-9)
                _full_mu   = float(_full.mean())
                _full_sigs = {
                    'Sharpe':       _full_mu / _full_rvol * np.sqrt(TRADING_DAYS_ANN),
                    'AvgDailyRet%': _full_mu * 100,
                    'Sortino':      (_full_mu / float(_full[_full < 0].std(ddof=1))
                                     * np.sqrt(TRADING_DAYS_ANN))
                                    if len(_full[_full < 0]) > 1 else 0.0,
                }
                # Rolling percentile: what fraction of the window is BELOW current val
                ranks = []
                for _sn in _clev_sigs:
                    if _sn not in _sig_vals:
                        continue
                    # Build the signal's distribution over the trailing window
                    # by computing it for each sub-window ending at each past day.
                    # For speed: approximate with a rolling buffer of signal values.
                    # We store signal snapshots in _clev_scalar_history — but that
                    # would require per-signal histories. Instead use a simpler
                    # approach: percentile of current single-point signal value
                    # vs a distribution built from the ret_buf directly.
                    cur_val = _sig_vals[_sn]
                    # Compute signal on each rolling sub-window of _clev_window
                    hist_vals = []
                    buf = _clev_ret_buf
                    w   = _clev_window
                    for _k in range(max(0, len(buf) - w * 2), len(buf) - w + 1):
                        _sw = np.array(buf[_k: _k + w])
                        _sv = max(float(_sw.std(ddof=1)), 1e-9)
                        _sm = float(_sw.mean())
                        if _sn == 'Sharpe':
                            hist_vals.append(_sm / _sv * np.sqrt(TRADING_DAYS_ANN))
                        elif _sn == 'AvgDailyRet%':
                            hist_vals.append(_sm * 100)
                        elif _sn == 'Sortino':
                            _neg = _sw[_sw < 0]
                            _dsd = float(_neg.std(ddof=1)) if len(_neg) > 1 else 1e-9
                            hist_vals.append(_sm / _dsd * np.sqrt(TRADING_DAYS_ANN))
                        elif _sn == 'CAGR%':
                            hist_vals.append(
                                (float(np.prod(1 + _sw) ** (TRADING_DAYS_ANN / len(_sw))) - 1) * 100
                            )
                        elif _sn == 'Calmar':
                            _ccum = np.cumprod(1 + _sw)
                            _cpk  = np.maximum.accumulate(_ccum)
                            _cdd  = float(((_ccum - _cpk) / _cpk).min())
                            _ccagr = (float(np.prod(1 + _sw) ** (TRADING_DAYS_ANN / len(_sw))) - 1) * 100
                            hist_vals.append(_ccagr / abs(_cdd * 100) if abs(_cdd) > 1e-9 else 0.0)
                    if not hist_vals:
                        ranks.append(0.5)
                        continue
                    hist_arr = np.array(hist_vals)
                    rank = float(np.mean(hist_arr <= cur_val))  # 0=low, 1=high
                    ranks.append(rank)
                # Average rank across selected signals
                avg_rank = float(np.mean(ranks)) if ranks else 0.5
                # DD guard: suppress boost during active drawdown
                _clev_dd = (_clev_eq_lvl / max(_clev_eq_peak, 1e-9)) - 1.0
                if _clev_dd < _clev_dd_thr:
                    _clev_boost = 1.0   # in DD: no boost, stay at static
                else:
                    # Contrarian: high rank -> low boost, low rank -> high boost
                    _clev_boost = 1.0 + (_clev_max_b - 1.0) * (1.0 - avg_rank)
            _col_ts_c = str(_col_to_timestamp(col_str) or col_str)[:10]
            _clev_scalar_history.append((_col_ts_c, _clev_boost))
            l_high_today = _l_high_val * _clev_boost
            l_base_today = _l_base_val * _clev_boost

        # ── Equity Ratchet: risk-off leverage override ─────────────────
        # _ratchet_risk_off / _adaptive_risk_off set at end of previous day (strict lag).
        # When active, scale leverage down — 0.0 means sit flat entirely.
        # Fixed ratchet and adaptive ratchet are mutually exclusive; use one or the other.
        _ratchet_forced_flat = False
        if ratchet is not None and _ratchet_risk_off:
            scale = ratchet.risk_off_lev_scale
            if scale <= 0.0:
                _ratchet_forced_flat = True
                ratchet.n_flat_days += 1
            else:
                l_high_today = l_high_today * scale
                l_base_today = l_base_today * scale

        if adaptive_ratchet is not None and _adaptive_risk_off:
            scale = adaptive_ratchet.risk_off_lev_scale
            if scale <= 0.0:
                _ratchet_forced_flat = True
                adaptive_ratchet.n_flat_days += 1
            else:
                l_high_today = l_high_today * scale
                l_base_today = l_base_today * scale

        if _ratchet_forced_flat:
            # Skip this day entirely — same path as a filtered flat day
            _col_ts_str = str(_col_to_timestamp(col_str) or col_str)[:10]
            _equity_now = STARTING_CAPITAL * equity_running
            if verbose and PRINT_FEES_PANEL:
                _fees_rows.append((
                    _col_ts_str,
                    _equity_now, -2.0,  # sentinel -2.0 = ratchet risk-off flat
                    0.0, 0.0, 0.0, 0.0, 0.0,
                    _equity_now, 0.0, 0.0, 0.0,
                ))
            daily.append(0.0)
            flat_days += 1
            # Update both ratchets with unchanged equity so floors stay current
            _col_ts_str_ro = _col_ts_str
            if ratchet is not None:
                _ratchet_risk_off  = ratchet.update(_col_ts_str_ro, _equity_now)
            if adaptive_ratchet is not None:
                _ar_regime         = _get_regime(vol_regime_series, _col_ts_str_ro)
                _adaptive_risk_off = adaptive_ratchet.update(
                    _col_ts_str_ro, _equity_now, _ar_regime)
            # Push 0.0 into lever buffers (no return this day)
            if _plev_on:
                _plev_ret_buf.append(0.0)
            if _vlev_on:
                _vlev_ret_buf.append(0.0)
                # _equity_level unchanged
            if _clev_on:
                _clev_ret_buf.append(0.0)
                # _clev_eq_lvl unchanged
            continue

        r, lev_used, _day_exit_bar = _apply_hybrid_day_param(
            path_1x,
            early_x_minutes          = int(round(params["EARLY_KILL_X"])),
            early_y_1x               = early_y_1x,
            trail_dd_1x              = float(params["PORT_TSL"]),
            l_base                   = l_base_today,
            l_high                   = l_high_today,
            port_stop_1x             = ps_val,
            early_fill_threshold_1x  = ef_val,
            strong_thr_1x            = strong_thr,
            enable_portfolio_stop    = en_ps,
            enable_early_fill        = en_ef,
            early_fill_max_minutes   = efx_val,
            trial_purchases          = TRIAL_PURCHASES,
            return_exit_bar          = True,
        )
        _eb_ts = _col_to_timestamp(col_str)
        if _eb_ts is not None:
            _exit_bars[_eb_ts.strftime("%Y-%m-%d")] = int(_day_exit_bar)
        # ── Deduct transaction costs (fees + funding) ──────────────────────
        # Capital is split evenly across N symbols, so total notional = 1x of capital
        # regardless of symbol count. Fees are therefore a fixed % of capital per
        # active trading day — NOT multiplied by n_sym.
        #
        # fee_drag    = TAKER_FEE_PCT * lev_used   (round-trip on total position)
        # funding_drag= FUNDING_RATE_DAILY_PCT * lev_used  (2 windows × total position)
        #
        # lev_used == 0.0 → no trade (early_kill fired, trial_purchases=False) → no fees.
        if math.isfinite(r) and lev_used > 0.0:
            fee_drag     = TAKER_FEE_PCT           * lev_used
            funding_drag = FUNDING_RATE_DAILY_PCT  * lev_used
            r_gross      = r
            r_net        = r - fee_drag - funding_drag
            total_gross  += r_gross
            total_fees   += fee_drag + funding_drag
            trade_days   += 1
            _lev_used_list.append(lev_used)
            if lev_used >= _l_high_val - 1e-9:
                _lev_high_returns.append(r_net)
            else:
                _lev_base_returns.append(r_net)

            # ── Per-day fees panel row (accumulated for printing) ──────────
            if verbose and PRINT_FEES_PANEL:
                _equity_start = STARTING_CAPITAL * equity_running
                if CAPITAL_MODE == "fixed":
                    _notional = (min(STARTING_CAPITAL, _equity_start)
                                 if FIXED_NOTIONAL_CAP == "internal"
                                 else STARTING_CAPITAL)
                else:
                    _notional = _equity_start
                _margin       = _equity_start                      # full account = margin posted
                _trade_lev    = lev_used                           # adaptive leverage scalar
                _invested     = _notional * lev_used               # notional = deployment base × leverage
                _trade_vol    = _invested * 2.0                    # round-trip notional
                _fee_amt      = _notional * fee_drag               # $ taker fees
                _fund_amt     = _notional * funding_drag           # $ funding fees
                _equity_end   = _equity_start + _notional * r_net if CAPITAL_MODE == "fixed" else _equity_start * (1.0 + r_net)
                _net_pnl      = _equity_end - _equity_start
                _col_ts_str   = str(_col_to_timestamp(col_str) or col_str)[:10]
                _fees_rows.append((
                    _col_ts_str,
                    _equity_start,
                    _margin,
                    _trade_lev,
                    _invested,
                    _trade_vol,
                    _fee_amt,
                    _fund_amt,
                    _equity_end,
                    r_gross * 100,
                    r_net   * 100,
                    _net_pnl,
                ))
            _eq_before_sim = equity_running
            if CAPITAL_MODE == "fixed":
                _eq_now_sim = STARTING_CAPITAL * equity_running
                _notional_sim = (min(STARTING_CAPITAL, _eq_now_sim)
                                 if FIXED_NOTIONAL_CAP == "internal"
                                 else STARTING_CAPITAL)
                equity_running += (_notional_sim / STARTING_CAPITAL) * r_net
            else:
                _notional_sim = STARTING_CAPITAL * equity_running
                equity_running *= (1.0 + r_net)

            # ── Periodic Profit Harvest ────────────────────────────────
            # The harvest removes capital from the account. To keep daily[]
            # accurate for Sharpe/CAGR/MaxDD stats, we express the removal
            # as a fractional reduction of the post-trade equity, then fold
            # it into r_net so the single appended value reflects both the
            # trade return AND the cash taken out.
            if pph is not None:
                _pph_equity   = STARTING_CAPITAL * equity_running
                _pph_date_str = str(_col_to_timestamp(col_str) or col_str)[:10]
                _pph_harvest  = pph.check(_pph_date_str, _pph_equity)
                if _pph_harvest > 0.0:
                    # Express harvest as a fractional hit on current equity
                    _harvest_frac  = _pph_harvest / _pph_equity   # e.g. 0.05 = 5% removed
                    equity_running -= _pph_harvest / STARTING_CAPITAL
                    # Fold into r_net: (1 + r_net) * (1 - harvest_frac) - 1
                    r_net = (1.0 + r_net) * (1.0 - _harvest_frac) - 1.0

            # ── Equity Ratchet: post-trade update ─────────────────────
            # Called AFTER PPH so the floor reflects harvested-adjusted equity.
            # Stores risk-off flag for use at START of the next iteration.
            if ratchet is not None:
                _ratchet_eq_now   = STARTING_CAPITAL * equity_running
                _ratchet_date_str = str(_col_to_timestamp(col_str) or col_str)[:10]
                _ratchet_risk_off = ratchet.update(_ratchet_date_str, _ratchet_eq_now)

            if adaptive_ratchet is not None:
                _ar_eq_now    = STARTING_CAPITAL * equity_running
                _ar_date_str  = str(_col_to_timestamp(col_str) or col_str)[:10]
                _ar_regime    = _get_regime(vol_regime_series, _ar_date_str)
                _adaptive_risk_off = adaptive_ratchet.update(
                    _ar_date_str, _ar_eq_now, _ar_regime)

        elif not math.isfinite(r):
            r_net = float("nan")
        else:
            r_net = 0.0  # no trade — no costs
            # ── Log no-entry days so fees panel has no silent gaps ──────
            if verbose and PRINT_FEES_PANEL:
                _col_ts_str  = str(_col_to_timestamp(col_str) or col_str)[:10]
                _equity_now  = STARTING_CAPITAL * equity_running
                _fees_rows.append((
                    _col_ts_str,
                    _equity_now,  # Start ($)
                    -1.0,         # sentinel: distinguishes no-entry from filtered flat
                    0.0,          # lev
                    0.0, 0.0, 0.0, 0.0,
                    _equity_now,  # End ($) unchanged
                    0.0, 0.0, 0.0,
                ))
            # Ratchet still needs to track equity even on no-entry days
            if ratchet is not None:
                _col_ts_str_r = str(_col_to_timestamp(col_str) or col_str)[:10]
                _ratchet_risk_off = ratchet.update(
                    _col_ts_str_r, STARTING_CAPITAL * equity_running)
            if adaptive_ratchet is not None:
                _col_ts_str_r  = str(_col_to_timestamp(col_str) or col_str)[:10]
                _ar_rg_noentry = _get_regime(vol_regime_series, _col_ts_str_r)
                _adaptive_risk_off = adaptive_ratchet.update(
                    _col_ts_str_r, STARTING_CAPITAL * equity_running, _ar_rg_noentry)

        # Push completed return into the performance-lever rolling buffer.
        # Flat days push 0.0 so the Sortino/avg-dr window correctly reflects
        # the full deployed-capital experience (matches the dashboard panels).
        if _plev_on:
            _plev_ret_buf.append(r_net if math.isfinite(r_net) else 0.0)
        if _vlev_on:
            _rn_safe = r_net if math.isfinite(r_net) else 0.0
            _vlev_ret_buf.append(_rn_safe)
            _equity_level *= (1.0 + _rn_safe)
            if _equity_level > _vlev_eq_peak:
                _vlev_eq_peak = _equity_level
        if _clev_on:
            _rn_c = r_net if math.isfinite(r_net) else 0.0
            _clev_ret_buf.append(_rn_c)
            _clev_eq_lvl  *= (1.0 + _rn_c)
            if _clev_eq_lvl > _clev_eq_peak:
                _clev_eq_peak = _clev_eq_lvl

        # Store nan for non-finite returns so daily_with_zeros stays length-aligned
        # with df_4x.columns (required for date-aligned equity curve plotting).
        # In fixed mode, record the account-relative return (smaller than r_net
        # when equity exceeds starting capital).
        if CAPITAL_MODE == "fixed" and math.isfinite(r_net):
            _acct_ret = ((_notional_sim / STARTING_CAPITAL) * r_net / _eq_before_sim) if abs(_eq_before_sim) > 1e-9 else r_net
            _acct_ret = float(np.clip(_acct_ret, -1.0, 10.0))
            daily.append(_acct_ret)
        else:
            daily.append(r_net if math.isfinite(r_net) else float("nan"))

    _total_net = total_gross - total_fees
    if verbose:
        print(f"    Simulated {len(daily)} days total, {flat_days} flat (filtered)")
        print(f"    Column date parse: {parse_ok} OK / {parse_fail} FAILED")
        if _vlev_on and _vlev_scalar_history:
            vl = [s for _, s in _vlev_scalar_history]
            print(f"    ── Vol-target leverage summary (BOOST mode) ─────────────────")
            print(f"       Mean boost : {np.mean(vl):.3f}  "
                  f"Min: {np.min(vl):.3f}  Max: {np.max(vl):.3f}")
            print(f"       Days at floor (1.00): "
                  f"{sum(1 for s in vl if abs(s - 1.0) < 1e-6)}")
            print(f"       Days at max boost ({_vlev_max_b:.2f}): "
                  f"{sum(1 for s in vl if abs(s - _vlev_max_b) < 1e-6)}")
            print(f"    ────────────────────────────────────────────────────────────")
        if _clev_on and _clev_scalar_history:
            cl = [s for _, s in _clev_scalar_history]
            print(f"    ── Contrarian-lev scalar summary ────────────────────────────")
            print(f"       Signals : {_clev_sigs}")
            print(f"       Mean boost : {np.mean(cl):.3f}  "
                  f"Min: {np.min(cl):.3f}  Max: {np.max(cl):.3f}")
            print(f"       Days at floor (1.00): "
                  f"{sum(1 for s in cl if abs(s - 1.0) < 1e-6)}")
            print(f"       Days at max ({_clev_max_b:.2f}): "
                  f"{sum(1 for s in cl if abs(s - _clev_max_b) < 1e-6)}")
            print(f"    ────────────────────────────────────────────────────────────")
        if _plev_on and _plev_scalar_history:
            scalars = [s for _, s in _plev_scalar_history]
            print(f"    ── Perf-lev scalar summary (BOOST mode) ────────────────────")
            print(f"       Mean scalar : {np.mean(scalars):.3f}  "
                  f"Min: {np.min(scalars):.3f}  Max: {np.max(scalars):.3f}")
            print(f"       Days at floor (1.00): "
                  f"{sum(1 for s in scalars if abs(s - 1.0) < 1e-6)}")
            print(f"       Days at max boost ({_plev_max_b:.2f}): "
                  f"{sum(1 for s in scalars if abs(s - _plev_max_b) < 1e-6)}")
            print(f"    ────────────────────────────────────────────────────────────")
        print(f"    ── Cost summary ────────────────────────────────────────────")
        print(f"       Active trading days : {trade_days}")
        print(f"       Total gross return  : {total_gross*100:+.2f}%")
        print(f"       Total fees charged  : {total_fees*100:+.2f}%")
        print(f"       Total net return    : {_total_net*100:+.2f}%")
        print(f"       Avg fee per day     : {(total_fees/trade_days*100) if trade_days else 0:.4f}%")
        print(f"    ────────────────────────────────────────────────────────────")
        if pph is not None:
            pph.print_summary(STARTING_CAPITAL)
        if ratchet is not None:
            ratchet.print_summary()
        if adaptive_ratchet is not None:
            adaptive_ratchet.print_summary()

    if verbose and PRINT_FEES_PANEL and _fees_rows:
        _W = 162
        _HDR = (f"{'Date':<12} {'Start ($)':>12} {'Margin ($)':>12} {'Lev':>6} "
                f"{'Invested ($)':>13} {'Trade Vol ($)':>14} "
                f"{'Taker Fee ($)':>13} {'Funding ($)':>12} "
                f"{'End ($)':>13} {'Ret Gross%':>10} {'Ret Net%':>9} {'Net P&L ($)':>12}")
        print()
        print("  " + "─" * _W)
        print("  FEES PANEL  —  per active trading day  "
              f"(capital=${STARTING_CAPITAL:,.0f}  "
              f"taker={TAKER_FEE_PCT*100:.3f}%  "
              f"funding={FUNDING_RATE_DAILY_PCT*100:.3f}%)")
        print("  " + "─" * _W)
        print("  " + _HDR)
        print("  " + "─" * _W)
        _prev_dt_fp = None
        _n_nodata   = 0
        for (_dt, _es, _mg, _lv, _inv, _tvol, _fee, _fund, _ee, _gr, _nr, _pnl) in _fees_rows:
            # ── Gap annotation: crypto is 24/7 so any gap = missing matrix data ──
            try:
                _cur_dt_fp = _pd.Timestamp(_dt)
                if _prev_dt_fp is not None:
                    _gap = (_cur_dt_fp - _prev_dt_fp).days - 1
                    if _gap > 0:
                        _n_nodata += _gap
                        print(f"  ··· {_gap}d missing from matrix")
                _prev_dt_fp = _cur_dt_fp
            except Exception:
                pass
            if _mg < 0.0:  # sentinel: -1.0 = no-entry, -2.0 = ratchet risk-off
                _row_label = "— RATCHET OFF —" if _mg < -1.5 else "— NO ENTRY —"
                print(f"  {_dt:<12} {_es:>12,.2f} {_row_label:>15} {'':>3} "
                      f"{'':>13} {'':>14} "
                      f"{'':>13} {'':>12} "
                      f"{_ee:>13,.2f} {'  0.000%':>10} {'  0.000%':>9} {'      0.00':>12}")
            elif _lv == 0.0 and _inv == 0.0:  # flat / filtered day
                print(f"  {_dt:<12} {_es:>12,.2f} {'— FILTERED —':>14} {'':>6} "
                      f"{'':>13} {'':>14} "
                      f"{'':>13} {'':>12} "
                      f"{_ee:>13,.2f} {'  0.000%':>10} {'  0.000%':>9} {'      0.00':>12}")
            else:
                print(f"  {_dt:<12} {_es:>12,.2f} {_mg:>12,.2f} {_lv:>6.3f} "
                      f"{_inv:>13,.2f} {_tvol:>14,.2f} "
                      f"{_fee:>13,.4f} {_fund:>12,.4f} "
                      f"{_ee:>13,.2f} {_gr:>+10.3f}% {_nr:>+9.3f}% {_pnl:>+12,.2f}")
        print("  " + "─" * _W)
        _tot_fees_d  = sum(r[6] for r in _fees_rows)
        _tot_fund_d  = sum(r[7] for r in _fees_rows)
        _tot_pnl_d   = sum(r[11] for r in _fees_rows)
        _final_eq    = _fees_rows[-1][8]
        _n_active    = sum(1 for r in _fees_rows if r[3] > 0.0)
        _n_flat      = sum(1 for r in _fees_rows if r[3] == 0.0 and r[1] >= 0.0)
        _n_no_entry  = sum(1 for r in _fees_rows if r[1] >= 0.0 and r[2] < 0.0)
        print(f"  {'TOTAL':<12} {'':>12} {'':>12} {'':>6} {'':>13} {'':>14} "
              f"{_tot_fees_d:>13,.4f} {_tot_fund_d:>12,.4f} "
              f"{_final_eq:>13,.2f} {'':>10} {'':>9} {_tot_pnl_d:>+12,.2f}")
        print(f"  {'':12}  Active: {_n_active}   Filtered flat: {_n_flat}   "
              f"No-entry: {_n_no_entry}   Missing matrix: {_n_nodata}   "
              f"Total rows: {len(_fees_rows)}")
        print("  " + "─" * _W)
        print()

        # ── CSV export ────────────────────────────────────────────────
        if fees_csv_path:
            import csv as _csv
            _csv_cols = ["date", "type", "start_usd", "margin_usd", "lev",
                         "invested_usd", "trade_vol_usd", "taker_fee_usd",
                         "funding_usd", "end_usd", "ret_gross_pct", "ret_net_pct",
                         "net_pnl_usd"]
            with open(fees_csv_path, "w", newline="") as _fh:
                _w = _csv.writer(_fh)
                _w.writerow(_csv_cols)
                for (_dt, _es, _mg, _lv, _inv, _tvol, _fee, _fund,
                     _ee, _gr, _nr, _pnl) in _fees_rows:
                    if _mg < -1.5:
                        _type = "ratchet_off"
                    elif _mg < 0.0:
                        _type = "no_entry"
                    elif _lv == 0.0 and _inv == 0.0:
                        _type = "flat"
                    else:
                        _type = "active"
                    _w.writerow([
                        _dt, _type,
                        round(_es,   2), round(_mg if _mg >= 0 else 0.0, 2),
                        round(_lv,   4),
                        round(_inv,  2), round(_tvol, 2),
                        round(_fee,  4), round(_fund, 4),
                        round(_ee,   2),
                        round(_gr,   4), round(_nr,   4),
                        round(_pnl,  2),
                    ])
            print(f"  [fees] CSV saved → {fees_csv_path}")
            print()

    _total_net     = total_gross - total_fees
    _calendar_days = len(daily)
    return dict(
        daily         = np.array(daily, dtype=float),
        total_gross   = total_gross,
        total_fees    = total_fees,
        total_net     = _total_net,
        trade_days    = trade_days,
        avg_fee_per_active_day  = (total_fees / trade_days * 100.0)    if trade_days     > 0 else float("nan"),
        avg_fee_per_cal_day     = (total_fees / _calendar_days * 100.0) if _calendar_days > 0 else float("nan"),
        avg_lev          = float(np.mean(_lev_used_list)) if _lev_used_list else float("nan"),
        lev_high_days    = len(_lev_high_returns),
        lev_base_days    = len(_lev_base_returns),
        lev_high_winrate = (float(sum(1 for v in _lev_high_returns if v > 0)
                             / len(_lev_high_returns) * 100)
                            if _lev_high_returns else float("nan")),
        lev_base_winrate = (float(sum(1 for v in _lev_base_returns if v > 0)
                             / len(_lev_base_returns) * 100)
                            if _lev_base_returns else float("nan")),
        lev_high_avg_ret = (float(sum(_lev_high_returns) / len(_lev_high_returns) * 100)
                            if _lev_high_returns else float("nan")),
        lev_base_avg_ret = (float(sum(_lev_base_returns) / len(_lev_base_returns) * 100)
                            if _lev_base_returns else float("nan")),
        perf_lev_scalar_history = _plev_scalar_history if _plev_on else [],
        vol_lev_scalar_history    = _vlev_scalar_history   if _vlev_on  else [],
        contra_lev_scalar_history = _clev_scalar_history   if _clev_on  else [],
        pph_summary               = pph.summary() if pph is not None else None,
        ratchet_summary           = ratchet.summary() if ratchet is not None else None,
        adaptive_ratchet_summary  = adaptive_ratchet.summary() if adaptive_ratchet is not None else None,
        fees_rows                 = list(_fees_rows),   # for weekly/monthly milestone tables
        exit_bars                 = _exit_bars,          # {YYYY-MM-DD: bar_idx} for intraday chart
    )

# ══════════════════════════════════════════════════════════════════════
# FILTER-AWARE WALK-FORWARD VALIDATION
# ══════════════════════════════════════════════════════════════════════

def _wf_fold_stats(returns: np.ndarray) -> dict:
    """Compute Sharpe, CAGR, MaxDD, Sortino, R², DSR for a fold."""
    r = returns
    n = len(r)
    if n == 0:
        return dict(sharpe=float("nan"), cagr=float("nan"), maxdd=float("nan"),
                    sortino=float("nan"), r2=float("nan"), dsr=float("nan"))

    mean_r = float(np.mean(r))
    std_r  = float(np.std(r, ddof=1)) if n > 1 else 0.0
    sharpe = float(mean_r / std_r * TRADING_DAYS**0.5) if std_r > 0 else 0.0

    # CAGR over fold days (including zeros)
    eq   = np.cumprod(1 + r)
    cagr = float((eq[-1] ** (TRADING_DAYS / n) - 1) * 100) if n > 0 else float("nan")

    # MaxDD
    peak = np.maximum.accumulate(eq)
    dd   = (eq - peak) / peak
    maxdd = float(np.min(dd) * 100)

    # Sortino (downside std)
    neg = r[r < 0]
    sortino = float(mean_r / (np.std(neg, ddof=1) if len(neg) > 1 else 1e-9) * TRADING_DAYS**0.5)

    # R² (equity curve linearity in log space)
    try:
        log_eq = np.log(eq)
        x = np.arange(n)
        slope, intercept = np.polyfit(x, log_eq, 1)
        fitted = slope * x + intercept
        ss_res = np.sum((log_eq - fitted)**2)
        ss_tot = np.sum((log_eq - np.mean(log_eq))**2)
        r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else float("nan")
    except Exception:
        r2 = float("nan")

    # Simplified DSR (using 1 trial - genuine OOS)
    try:
        from scipy.stats import norm as _norm
        sr_obs = sharpe / TRADING_DAYS**0.5  # daily Sharpe
        sr_benchmark = 0.0
        skew = float(pd.Series(r).skew())
        kurt = float(pd.Series(r).kurtosis())
        dsr_z = (sr_obs - sr_benchmark) * (n**0.5) / (
            (1 - skew * sr_obs + (kurt / 4) * sr_obs**2)**0.5 + 1e-9
        )
        dsr = float(_norm.cdf(dsr_z) * 100)
    except Exception:
        dsr = float("nan")

    return dict(sharpe=sharpe, cagr=cagr, maxdd=maxdd,
                sortino=sortino, r2=r2, dsr=dsr)


def run_filter_aware_wf(daily_with_zeros: np.ndarray,
                        label: str,
                        train_days: int = 120,
                        test_days:  int = 30,
                        step_days:  int = 30,
                        min_active_frac: float = 0.33,
                        oi_df:         "Optional[pd.DataFrame]" = None,
                        multi_oi_df:   "Optional[pd.DataFrame]" = None,
                        alt_rets_df:   "Optional[pd.DataFrame]" = None,
                        col_dates:     "Optional[list]"          = None) -> dict:
    """
    Filter-aware rolling walk-forward validation.

    Uses daily_with_zeros (0.0 on filtered days, actual return on active days)
    so fold results reflect the filter's contribution: a filter that blanks out
    a bad test window will show Sharpe ≈ 0 instead of the unfiltered -0.334.

    Calendar-saturation fix
    -----------------------
    When a test window falls mostly inside a calendar-hardcoded flat period
    (e.g. a full 39-day bad window inside a 30-day fold), the Sharpe is
    structurally near zero not because the *strategy* is unstable but because
    there are no trades to measure.  Including those folds in CV inflates it
    from ~0.3 to 3+, making every filtered run look wildly inconsistent.

    Folds where active_test / test_days < min_active_frac are tagged
    "calendar-saturated" and excluded from the primary CV / mean-Sharpe
    / pct_positive / n_unstable aggregates.  They are still stored and
    printed so nothing is hidden.

    Both the raw (all-fold) and filtered (active-fold) aggregates are
    returned; the comparison table uses the active-fold values as primary.

    OI enrichment (optional)
    ------------------------
    When oi_df (a DataFrame with columns oi_usd, oi_change, oi_pct_chg,
    date-indexed) and col_dates (list of timestamps aligned to the
    daily_with_zeros array) are both supplied, each fold dict is enriched
    with OI statistics for its TEST window:

      oi_mean_chg    – mean daily OI change (USD) over the test window
      oi_net_chg     – net OI change over the test window (last − first)
      oi_pct_net     – net OI % change
      oi_regime      – "accumulating" | "unwinding" | "flat" | "n/a"
      oi_n_days      – number of OI observations matched to this fold

    A Spearman correlation between OI mean daily change and OOS Sharpe
    is computed across all active folds and included in the returned dict
    as oi_sharpe_corr.

    min_active_frac : float
        Fraction of test-window days that must be active (non-zero) for a
        fold to count toward CV.  Default 0.33 - at least 1/3 of the test
        window must be tradeable.  Folds below this threshold are labelled
        SATURATED in the printed output.
    """
    N = len(daily_with_zeros)
    r = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
    min_active_days = int(math.ceil(test_days * min_active_frac))

    # Pre-process OI for fast per-fold lookup
    _oi_ready = (
        oi_df is not None
        and not oi_df.empty
        and col_dates is not None
        and "oi_change" in oi_df.columns
        and "oi_usd"    in oi_df.columns
    )
    if _oi_ready:
        oi_idx = oi_df.index.normalize().tz_localize(None)

    # Multi-asset OI readiness (dispersion + price alignment)
    _multi_oi_ready = (
        multi_oi_df is not None
        and not multi_oi_df.empty
        and col_dates is not None
    )
    if _multi_oi_ready:
        multi_oi_idx = multi_oi_df.index.normalize().tz_localize(None)

    # Alt-returns readiness (price alignment)
    _alt_rets_ready = (
        alt_rets_df is not None
        and not alt_rets_df.empty
    )
    if _alt_rets_ready:
        alt_idx = alt_rets_df.index.normalize().tz_localize(None)

    folds = []
    fold_num = 0
    start = 0
    while start + train_days + test_days <= N:
        fold_num += 1
        train_end = start + train_days
        test_end  = min(train_end + test_days, N)

        train_r = r[start:train_end]
        test_r  = r[train_end:test_end]

        is_stats  = _wf_fold_stats(train_r)
        oos_stats = _wf_fold_stats(test_r)

        active_test = int(np.sum(test_r != 0.0))
        flat_test   = int(np.sum(test_r == 0.0))
        saturated = active_test < min_active_days

        fold = dict(
            fold=fold_num,
            train_start=start, train_end=train_end,
            test_start=train_end, test_end=test_end,
            is_sharpe=is_stats["sharpe"], is_cagr=is_stats["cagr"],
            is_maxdd=is_stats["maxdd"],
            oos_sharpe=oos_stats["sharpe"], oos_cagr=oos_stats["cagr"],
            oos_maxdd=oos_stats["maxdd"], oos_sortino=oos_stats["sortino"],
            oos_r2=oos_stats["r2"], oos_dsr=oos_stats["dsr"],
            active_test=active_test, flat_test=flat_test,
            saturated=saturated,
            # OI fields — populated below if data available
            oi_mean_chg=float("nan"), oi_net_chg=float("nan"),
            oi_pct_net=float("nan"),  oi_regime="n/a", oi_n_days=0,
            # New OI diagnostic fields
            oi_persistence=float("nan"),   # lag-1 Spearman(ΔOI_t, ΔOI_{t+1}) BTC
            oi_dispersion=float("nan"),    # mean(std(ΔOI across assets)) per day
            oi_price_align=float("nan"),   # mean cross-sectional corr(ΔOI, Δprice)
        )

        # ── Resolve test-window dates once (used by all OI blocks) ──────
        ts_idx  = train_end
        te_idx  = test_end - 1
        d_start = col_dates[ts_idx] if (col_dates and ts_idx < len(col_dates)) else None
        d_end   = col_dates[te_idx] if (col_dates and te_idx < len(col_dates)) else None

        # ── Per-fold OI enrichment (single-asset BTC) ─────────────────
        if _oi_ready and d_start is not None and d_end is not None:
            d_s = pd.Timestamp(d_start).normalize().tz_localize(None)
            d_e = pd.Timestamp(d_end).normalize().tz_localize(None)
            mask = (oi_idx >= d_s) & (oi_idx <= d_e)
            oi_slice = oi_df.loc[mask]

            if len(oi_slice) >= 2:
                chg_vals  = oi_slice["oi_change"].dropna()
                mean_chg  = float(chg_vals.mean()) if len(chg_vals) else float("nan")
                net_chg   = float(
                    oi_slice["oi_usd"].iloc[-1] - oi_slice["oi_usd"].iloc[0]
                )
                first_oi  = float(oi_slice["oi_usd"].iloc[0])
                pct_net   = (net_chg / first_oi * 100) if first_oi != 0 else float("nan")

                if not math.isfinite(pct_net):
                    regime = "n/a"
                elif pct_net >  1.0:
                    regime = "accumulating"
                elif pct_net < -1.0:
                    regime = "unwinding"
                else:
                    regime = "flat"

                fold.update(
                    oi_mean_chg = mean_chg,
                    oi_net_chg  = net_chg,
                    oi_pct_net  = pct_net,
                    oi_regime   = regime,
                    oi_n_days   = len(oi_slice),
                )

                # ── OI Persistence (lag-1 Spearman on BTC ΔOI) ───────
                # rank_corr(ΔOI_t, ΔOI_{t+1}) — do leaders stay leaders?
                # Positive = OI momentum (accumulation begets accumulation)
                # Negative = OI mean-reversion
                pct_chg = oi_slice["oi_pct_chg"].dropna()
                if len(pct_chg) >= 4:
                    try:
                        import scipy.stats as _sc
                        rho, _ = _sc.spearmanr(
                            pct_chg.values[:-1],
                            pct_chg.values[1:],
                        )
                        fold["oi_persistence"] = float(rho)
                    except Exception:
                        pass

        # ── Multi-asset OI metrics ─────────────────────────────────────
        if _multi_oi_ready and d_start is not None and d_end is not None:
            d_s_m = pd.Timestamp(d_start).normalize().tz_localize(None)
            d_e_m = pd.Timestamp(d_end).normalize().tz_localize(None)
            mask_m = (multi_oi_idx >= d_s_m) & (multi_oi_idx <= d_e_m)
            mo_slice = multi_oi_df.loc[mask_m]   # (days × symbols)

            if len(mo_slice) >= 2:
                # ── OI Dispersion: mean daily std(ΔOI across assets) ──
                # High dispersion = capital rotating between assets
                # Low dispersion = uniform OI change (risk-on/off macro move)
                daily_std = mo_slice.std(axis=1, ddof=1).dropna()
                if len(daily_std) > 0:
                    fold["oi_dispersion"] = float(daily_std.mean())

                # ── OI-Price Alignment: cross-sectional corr(ΔOI, Δprice) ──
                # Positive = OI building where prices rising (trend follow)
                # Negative = OI building against price move (mean-revert)
                # Computed per day, then averaged over the fold window.
                if _alt_rets_ready:
                    common_syms = [
                        c for c in mo_slice.columns
                        if c in alt_rets_df.columns
                    ]
                    if len(common_syms) >= 5:
                        alt_slice = alt_rets_df.reindex(
                            index=mo_slice.index,
                            columns=common_syms,
                        )
                        mo_common = mo_slice[common_syms]
                        daily_corrs: List[float] = []
                        for day in mo_slice.index:
                            oi_row  = mo_common.loc[day].dropna()
                            pr_row  = alt_slice.loc[day].dropna() if day in alt_slice.index else pd.Series(dtype=float)
                            shared  = oi_row.index.intersection(pr_row.index)
                            if len(shared) >= 5:
                                try:
                                    import scipy.stats as _sc
                                    rho, _ = _sc.spearmanr(
                                        oi_row[shared].values,
                                        pr_row[shared].values,
                                    )
                                    if math.isfinite(rho):
                                        daily_corrs.append(float(rho))
                                except Exception:
                                    pass
                        if daily_corrs:
                            fold["oi_price_align"] = float(np.mean(daily_corrs))

        folds.append(fold)
        start += step_days

    if not folds:
        empty = dict(folds=[], mean_sharpe=float("nan"), std_sharpe=float("nan"),
                     cv=float("nan"), mean_dsr=float("nan"), pct_positive=float("nan"),
                     n_unstable=0, train_days=train_days, test_days=test_days,
                     n_saturated=0, min_active_frac=min_active_frac,
                     oi_sharpe_corr=float("nan"), oi_available=False,
                     oi_persistence_corr=float("nan"),
                     oi_dispersion_corr=float("nan"),
                     oi_price_align_corr=float("nan"))
        return empty

    def _agg(fold_subset):
        sharpes = [f["oos_sharpe"] for f in fold_subset]
        valid_s = [s for s in sharpes if math.isfinite(s)]
        mean_s  = float(np.mean(valid_s))      if valid_s           else float("nan")
        std_s   = float(np.std(valid_s, ddof=1)) if len(valid_s) > 1 else float("nan")
        cv      = float(std_s / abs(mean_s))   if (math.isfinite(mean_s) and mean_s != 0
                                                    and math.isfinite(std_s)) \
                                               else float("nan")
        dsrs    = [f["oos_dsr"] for f in fold_subset if math.isfinite(f["oos_dsr"])]
        mean_dsr = float(np.mean(dsrs)) if dsrs else float("nan")
        pct_pos  = float(sum(s > 0 for s in valid_s) / len(valid_s) * 100) if valid_s else float("nan")
        n_unstable = int(sum(s < 0 for s in valid_s))
        return dict(mean_sharpe=mean_s, std_sharpe=std_s, cv=cv,
                    mean_dsr=mean_dsr, pct_positive=pct_pos,
                    n_unstable=n_unstable, n_folds=len(fold_subset))

    active_folds    = [f for f in folds if not f["saturated"]]
    saturated_folds = [f for f in folds if     f["saturated"]]

    agg_active = _agg(active_folds) if active_folds else _agg(folds)

    # ── OI × Sharpe Spearman correlations (active folds only) ────────
    oi_sharpe_corr       = float("nan")
    oi_persistence_corr  = float("nan")
    oi_dispersion_corr   = float("nan")
    oi_price_align_corr  = float("nan")
    oi_available         = _oi_ready or _multi_oi_ready

    if active_folds:
        try:
            import scipy.stats as _sc

            def _spearman_corr(xkey: str, ykey: str = "oos_sharpe") -> float:
                pairs = [
                    (f[xkey], f[ykey]) for f in active_folds
                    if math.isfinite(f.get(xkey, float("nan")))
                    and math.isfinite(f.get(ykey, float("nan")))
                ]
                if len(pairs) < 3:
                    return float("nan")
                rho, _ = _sc.spearmanr([p[0] for p in pairs], [p[1] for p in pairs])
                return float(rho)

            oi_sharpe_corr      = _spearman_corr("oi_mean_chg")
            oi_persistence_corr = _spearman_corr("oi_persistence")
            oi_dispersion_corr  = _spearman_corr("oi_dispersion")
            oi_price_align_corr = _spearman_corr("oi_price_align")
        except Exception:
            pass

    return dict(
        folds           = folds,
        # ── primary (active folds) ──────────────────────────────
        mean_sharpe     = agg_active["mean_sharpe"],
        std_sharpe      = agg_active["std_sharpe"],
        cv              = agg_active["cv"],
        mean_dsr        = agg_active["mean_dsr"],
        pct_positive    = agg_active["pct_positive"],
        n_unstable      = agg_active["n_unstable"],
        n_active_folds  = agg_active["n_folds"],
        # ── metadata ────────────────────────────────────────────
        n_saturated     = len(saturated_folds),
        min_active_frac = min_active_frac,
        min_active_days = min_active_days,
        train_days      = train_days,
        test_days       = test_days,
        # ── OI diagnostics ──────────────────────────────────────
        oi_sharpe_corr       = oi_sharpe_corr,
        oi_persistence_corr  = oi_persistence_corr,
        oi_dispersion_corr   = oi_dispersion_corr,
        oi_price_align_corr  = oi_price_align_corr,
        oi_available         = oi_available,
    )


def print_filter_aware_wf(wf: dict, label: str, col_dates: Optional[list] = None):
    """Print filter-aware WF results in the same style as institutional_audit."""
    folds           = wf.get("folds", [])
    train_days      = wf.get("train_days", 120)
    test_days       = wf.get("test_days",  30)
    n_saturated     = wf.get("n_saturated", 0)
    min_active_frac = wf.get("min_active_frac", 0.33)
    min_active_days = wf.get("min_active_days", int(math.ceil(test_days * min_active_frac)))
    n_active_folds  = wf.get("n_active_folds", len(folds) - n_saturated)
    oi_available    = wf.get("oi_available", False)

    print(f"\n┌─ FILTER-AWARE WALK-FORWARD  (train={train_days}d  test={test_days}d  step={test_days}d) ──")
    print(f"│  Filter applied to BOTH train and test windows")
    print(f"│  0% return on filtered days -> fold reflects true filter behaviour")
    print(f"│  Calendar-saturation threshold: <{min_active_frac:.0%} active ({min_active_days}d/{test_days}d)")
    print(f"│  Saturated folds ({n_saturated} of {len(folds)}) excluded from primary CV - "
          f"Sharpe near 0 reflects calendar gap, not strategy instability")
    if oi_available:
        print(f"│  OI regime: accumulating = net OI >+1%  |  unwinding = net OI <-1%  |  flat = within ±1%")
    print(f"│")
    print(f"│  {'─'*118}")

    for f in folds:
        fn      = f["fold"]
        ts      = f["test_start"]
        te      = f["test_end"]
        os_s    = f["oos_sharpe"]
        os_c    = f["oos_cagr"]
        os_d    = f["oos_maxdd"]
        os_r    = f["oos_r2"]
        os_dsr  = f["oos_dsr"]
        os_sort = f["oos_sortino"]
        act     = f["active_test"]
        flat    = f["flat_test"]
        sat     = f.get("saturated", False)

        # OI fields
        oi_regime   = f.get("oi_regime",   "n/a")
        oi_mean_chg = f.get("oi_mean_chg", float("nan"))
        oi_pct_net  = f.get("oi_pct_net",  float("nan"))
        oi_n        = f.get("oi_n_days",   0)

        # Resolve calendar dates
        d_start = d_end = ""
        if col_dates:
            ds = col_dates[ts]   if ts        < len(col_dates) and col_dates[ts]   else None
            de = col_dates[te-1] if (te - 1)  < len(col_dates) and col_dates[te-1] else None
            if ds: d_start = ds.strftime("%Y-%m-%d")
            if de: d_end   = de.strftime("%Y-%m-%d")

        date_str = f"  ({d_start} -> {d_end})" if d_start else ""
        sat_tag  = "  ⊘ SATURATED - excluded from CV" if sat else ""
        neg_tag  = "  ⚠ OOS Sharpe negative - unstable fold" if (not sat and os_s < 0) else ""
        flag     = sat_tag or neg_tag

        print(f"│  FOLD {fn}   Train: d{f['train_start']+1}-{f['train_end']} ({train_days}d)   "
              f"Test: d{ts+1}-{te} ({test_days}d){date_str}{flag}")
        print(f"│    In-sample  (train): Sharpe={f['is_sharpe']:6.3f}  CAGR={f['is_cagr']:8.0f}%  "
              f"MaxDD={f['is_maxdd']:7.2f}%")

        sharpe_note = "  [EXCLUDED from CV - calendar-saturated]" if sat else ""
        print(f"│    OOS (test):         Sharpe={os_s:6.3f}  CAGR={os_c:8.0f}%  "
              f"MaxDD={os_d:7.2f}%  Sortino={os_sort:6.3f}  R²={os_r:.3f}  DSR={os_dsr:.1f}%  "
              f"[active={act}d  flat={flat}d]{sharpe_note}")

        # OI line — only when data was matched
        if oi_available and oi_n > 0:
            regime_icon = {"accumulating": "↑", "unwinding": "↓", "flat": "→"}.get(oi_regime, "?")
            mean_chg_bn = oi_mean_chg / 1e9 if math.isfinite(oi_mean_chg) else float("nan")
            pct_str     = f"{oi_pct_net:+.1f}%" if math.isfinite(oi_pct_net) else "n/a"
            chg_str     = f"{mean_chg_bn:+.2f}B/d" if math.isfinite(mean_chg_bn) else "n/a"
            print(f"│    OI (test window):   {regime_icon} {oi_regime:<14}  "
                  f"net={pct_str:>7}  mean_daily={chg_str}  ({oi_n}d matched)")

            # Three new OI diagnostic metrics on a second OI line
            oi_pers  = f.get("oi_persistence",  float("nan"))
            oi_disp  = f.get("oi_dispersion",   float("nan"))
            oi_align = f.get("oi_price_align",  float("nan"))
            parts = []
            if math.isfinite(oi_pers):
                icon = "↑" if oi_pers > 0.2 else ("↓" if oi_pers < -0.2 else "→")
                parts.append(f"persistence={oi_pers:+.3f}{icon}")
            if math.isfinite(oi_disp):
                parts.append(f"dispersion={oi_disp:.2f}%σ")
            if math.isfinite(oi_align):
                icon = "+" if oi_align > 0.2 else ("-" if oi_align < -0.2 else "~")
                parts.append(f"price_align={oi_align:+.3f}{icon}")
            if parts:
                print(f"│                        " + "  ".join(parts))

        if not sat and os_s < 0:
            print(f"│")
            print(f"│    ⚠ UNSTABLE FOLD DETAIL - active_days={act}  flat_days={flat}")
            if act == 0:
                print(f"│    No active trading days - filter sat out entirely.")
            else:
                pct_flat = flat / test_days * 100
                print(f"│    Filter blocked {flat}/{test_days} days ({pct_flat:.0f}%) - "
                      f"remaining {act} active days still produced negative Sharpe.")

        print(f"│  {'─'*118}")

    # ── Aggregates ──────────────────────────────────────────────────
    n_unstable      = wf.get("n_unstable",      0)
    mean_s          = wf.get("mean_sharpe",      float("nan"))
    std_s           = wf.get("std_sharpe",       float("nan"))
    cv              = wf.get("cv",               float("nan"))
    mean_d          = wf.get("mean_dsr",         float("nan"))
    pct_p           = wf.get("pct_positive",     float("nan"))
    oi_corr         = wf.get("oi_sharpe_corr",   float("nan"))

    cv_flag  = "⚠ UNSTABLE" if (math.isfinite(cv)     and cv     > 0.25) else "✅ STABLE"
    dsr_flag = "⚠ FAIL"     if (math.isfinite(mean_d) and mean_d < 80)   else "✅ PASS"

    print(f"│  Saturated folds excluded from primary: {n_saturated}/{len(folds)}"
          f"  (active folds used: {n_active_folds})")
    print(f"│  Unstable folds (neg OOS Sharpe, active only): {n_unstable}/{n_active_folds}")
    print(f"│")
    print(f"│  PRIMARY AGGREGATES  ({n_active_folds} active folds - calendar-saturated excluded):")
    print(f"│  Mean Sharpe:    {mean_s:.3f}  (±{std_s:.3f})")
    print(f"│  Mean OOS DSR:   {mean_d:.1f}%  {dsr_flag}")
    print(f"│  % folds positive Sharpe: {pct_p:.0f}%")
    print(f"│  Stability (CV={cv:.3f}): {cv_flag}")

    # OI summary block
    if oi_available:
        print(f"│")
        print(f"│  ── OI Regime Diagnostics ──────────────────────────────────────────────────")
        active_folds_list = [f for f in folds if not f.get("saturated", False)]
        regime_counts: Dict[str, List[float]] = {}
        for f in active_folds_list:
            reg = f.get("oi_regime", "n/a")
            sh  = f.get("oos_sharpe", float("nan"))
            if reg not in regime_counts:
                regime_counts[reg] = []
            if math.isfinite(sh):
                regime_counts[reg].append(sh)

        for reg in ("accumulating", "flat", "unwinding", "n/a"):
            if reg not in regime_counts:
                continue
            sharpes = regime_counts[reg]
            mean_sh = float(np.mean(sharpes)) if sharpes else float("nan")
            icon    = {"accumulating": "↑", "flat": "→", "unwinding": "↓"}.get(reg, "?")
            print(f"│    {icon} {reg:<14}  {len(sharpes):>2} fold(s)  "
                  f"mean OOS Sharpe = {mean_sh:.3f}" if sharpes else
                  f"│    {icon} {reg:<14}  0 fold(s)")

        if math.isfinite(oi_corr):
            strength = ("|corr| > 0.5 — meaningful signal"
                        if abs(oi_corr) > 0.5 else
                        "|corr| ≤ 0.5 — weak signal in this window")
            print(f"│    Spearman corr(OI daily change  → OOS Sharpe) = {oi_corr:+.3f}  [{strength}]")
        else:
            print(f"│    Spearman corr(OI daily change  → OOS Sharpe) = n/a")

        oi_p_corr  = wf.get("oi_persistence_corr",  float("nan"))
        oi_d_corr  = wf.get("oi_dispersion_corr",   float("nan"))
        oi_a_corr  = wf.get("oi_price_align_corr",  float("nan"))

        def _corr_line(label: str, val: float) -> str:
            if not math.isfinite(val):
                return f"│    Spearman corr({label:<28}) = n/a  (insufficient data)"
            sig = "★ meaningful" if abs(val) > 0.5 else "  weak"
            return f"│    Spearman corr({label:<28}) = {val:+.3f}  [{sig}]"

        print(_corr_line("OI persistence    → OOS Sharpe", oi_p_corr))
        print(_corr_line("OI dispersion     → OOS Sharpe", oi_d_corr))
        print(_corr_line("OI-price align    → OOS Sharpe", oi_a_corr))
        print(f"│    (★ = |corr| > 0.5 — consider as regime filter input)")

    print(f"└─{'─'*119}")


# ══════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS LOADER
# ══════════════════════════════════════════════════════════════════════

def _with_retry(fn, max_retries=25, base=1.6, cap=75.0):
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except APIError as e:
            msg = str(e).lower()
            retriable = any(k in msg for k in [
                "429","quota","rate","resource_exhausted","503","backend","timeout"])
            if not retriable or attempt >= max_retries:
                raise
            s = min(cap, base ** attempt) + random.uniform(0, 2.0)
            print(f"  ⚠ retry {attempt}/{max_retries} -> sleep {s:.1f}s")
            time.sleep(s)



# ══════════════════════════════════════════════════════════════════════════════
# MATH VERIFICATION DIAGNOSTIC EXPORT
# ══════════════════════════════════════════════════════════════════════════════
# Exports a full step-by-step breakdown of every simulation calculation to
# Google Sheets for independent verification.
#
# Tabs written:
#   DiagSummary         — run config + key metrics
#   DiagDaily           — every active day, all interim calculation steps
#   DiagPath_YYYYMMDD   — full 216-bar intraday path for configurable dates
#
# All tabs are overwritten on each run. Activated by ENABLE_DIAGNOSTIC_EXPORT.
# ──────────────────────────────────────────────────────────────────────────────

# Sheets API quota: 60 read + 60 write requests per minute per project.
# With 400 path tabs × 3 calls each = 1200 calls — we pace writes to stay
# safely under the limit. DIAG_INTER_CALL_DELAY is the minimum sleep between
# consecutive API calls. DIAG_CHUNK_SIZE controls rows per append_rows call.
_DIAG_INTER_CALL_DELAY = 1.1   # seconds between API calls (≈54 calls/min, safely under 60)
_DIAG_CHUNK_SIZE       = 200   # rows per append_rows call


def _diag_get_or_create_tab(sh, title: str):
    """Return worksheet named title, creating it if it doesn't exist.
    Paces calls with a sleep to stay under Sheets API quota.
    """
    try:
        ws = _with_retry(lambda: sh.worksheet(title))
        time.sleep(_DIAG_INTER_CALL_DELAY)
        _with_retry(lambda: ws.clear())
        time.sleep(_DIAG_INTER_CALL_DELAY)
        return ws
    except Exception:
        time.sleep(_DIAG_INTER_CALL_DELAY)
        ws = _with_retry(lambda: sh.add_worksheet(title=title, rows=5000, cols=50))
        time.sleep(_DIAG_INTER_CALL_DELAY)
        return ws


def _diag_batch_write(ws, rows: list, chunk: int = _DIAG_CHUNK_SIZE):
    """Write rows to worksheet in chunks with inter-call pacing.
    Each chunk is a separate append_rows call wrapped in _with_retry.
    A sleep of _DIAG_INTER_CALL_DELAY seconds is inserted between chunks
    to prevent quota exhaustion on large exports.
    """
    total = len(rows)
    for i in range(0, total, chunk):
        sl = rows[i:i + chunk]
        _with_retry(lambda s=sl: ws.append_rows(s, value_input_option="USER_ENTERED"))
        if i + chunk < total:   # no sleep after the last chunk
            time.sleep(_DIAG_INTER_CALL_DELAY)


def export_diagnostic_to_sheets(
    df_4x:          "pd.DataFrame",
    params:         dict,
    filter_mode:    str,
    v3_filter,
    col_dates:      list,
    run_label:      str,
    key_metrics:    dict,
):
    """
    Full math verification export to Google Sheets.

    Parameters
    ----------
    df_4x        : intraday matrix (rows=bars, cols=trading day columns)
    params       : CANDIDATE_CONFIGS param dict for the best filter
    filter_mode  : filter mode string used for the best result
    v3_filter    : v3 filter object (may be None)
    col_dates    : list of column date strings from df_4x.columns
    run_label    : human-readable label e.g. "A - No Filter"
    key_metrics  : dict of final metrics {sharpe, cagr, maxdd, total_return, ...}
    """
    if not ENABLE_DIAGNOSTIC_EXPORT:
        return

    print(f"\n  [diagnostic] Exporting math verification to Google Sheets ...")

    try:
        gc = gspread.oauth(credentials_filename=CREDENTIALS_FILE,
                           authorized_user_filename=TOKEN_FILE)
        sh = _with_retry(lambda: gc.open_by_key(DIAGNOSTIC_SHEET_ID))
    except Exception as e:
        print(f"  [diagnostic] ❌ Could not connect to Google Sheets: {e}")
        return

    import datetime as _dt_mod

    # ── Pre-compute simulation state day-by-day ───────────────────────────────
    # Re-run simulate logic step-by-step to capture every interim value.
    # We do NOT call simulate() here — we replay the logic so we can intercept
    # every intermediate calculation.

    BAR_MINS   = 5
    PIVOT_LEV  = PIVOT_LEVERAGE   # typically 4.0

    # Resolve param values (mirrors simulate() logic)
    early_x    = int(round(params["EARLY_KILL_X"]))
    early_y_1x = float(params.get("EARLY_KILL_Y",    -9999)) / PIVOT_LEV \
                 if params.get("EARLY_KILL_Y") is not None else -9999.0
    # Correct: early_y and instill_y are stored as 4x values in params
    raw_ey     = params.get("EARLY_KILL_Y", -9999)
    raw_iy     = params.get("EARLY_INSTILL_Y", -9999)
    early_y_1x    = float(raw_ey) / PIVOT_LEV if raw_ey is not None else -9999.0
    instill_y_1x  = float(raw_iy) / PIVOT_LEV if raw_iy is not None else -9999.0
    l_base     = float(params.get("L_BASE",     0))
    l_high     = float(params.get("L_HIGH",     1))
    port_tsl   = float(params.get("PORT_TSL",   0.99))
    raw_ps     = params.get("PORT_SL", -0.049)
    en_ps      = raw_ps is not None and raw_ps != 0
    ps_val     = float(raw_ps) if en_ps else -999.0
    raw_ef     = params.get("EARLY_FILL_Y", 999)
    en_ef      = raw_ef is not None and float(raw_ef) < 99.0
    ef_val     = float(raw_ef) if en_ef else 999.0
    strong_thr = float(params.get("STRONG_THR_1X", 0.01))

    def _idx(mins, n):
        return max(0, min((mins // BAR_MINS) - 1, n - 1))

    equity_running  = 1.0
    _eq_before_sim  = 1.0
    _notional_sim   = STARTING_CAPITAL
    diag_rows       = []
    path_tabs       = {}   # date_str -> list of bar rows

    # If DIAGNOSTIC_PATH_ALL_DATES is set, every active day gets a path tab
    target_dates = None if DIAGNOSTIC_PATH_ALL_DATES else set(DIAGNOSTIC_PATH_DATES)

    for col in df_4x.columns:
        col_str  = str(col)
        col_date = str(_col_to_timestamp(col_str) or col_str)[:10]

        # Filter check
        sit_flat = False
        if filter_mode == "calendar":
            sit_flat = is_calendar_bad(col_str)
        elif v3_filter is not None and filter_mode not in ("none", ""):
            sit_flat = is_v3_bad(col_str, v3_filter)

        if sit_flat:
            continue

        path_4x = df_4x[col].to_numpy(dtype=float)
        n       = len(path_4x)
        path_1x = path_4x / PIVOT_LEV

        x_idx      = _idx(early_x, n)
        roi_x      = float(path_1x[x_idx]) if np.isfinite(path_1x[x_idx]) else 0.0
        kill_fires = (roi_x < early_y_1x)
        instill_ok = (roi_x >= instill_y_1x)
        lev        = l_high if roi_x >= strong_thr else l_base

        entry_1x  = roi_x
        exit_bar  = n - 1
        exit_incr = float(path_1x[-1]) - entry_1x
        stop_type = "none"

        if not kill_fires:
            peak = 0.0
            for i in range(x_idx + 1, n):
                v    = float(path_1x[i])
                if not np.isfinite(v): continue
                incr = v - entry_1x
                if en_ps and incr <= ps_val:
                    exit_incr = incr
                    exit_bar  = i
                    stop_type = "PORT_SL"
                    break
                if incr > peak: peak = incr
                if (incr - peak) <= -abs(port_tsl):
                    exit_incr = incr
                    exit_bar  = i
                    stop_type = "PORT_TSL"
                    break

        r_gross = 0.0 if kill_fires else exit_incr * lev
        fee_drag     = TAKER_FEE_PCT * lev if not kill_fires else 0.0
        funding_drag = FUNDING_RATE_DAILY_PCT * lev if not kill_fires else 0.0
        r_net    = r_gross - fee_drag - funding_drag

        equity_before = equity_running
        _eq_now       = STARTING_CAPITAL * equity_running
        if CAPITAL_MODE == "fixed":
            _fixed_notional = (min(STARTING_CAPITAL, _eq_now)
                               if FIXED_NOTIONAL_CAP == "internal"
                               else STARTING_CAPITAL)
            equity_running += (_fixed_notional / STARTING_CAPITAL) * r_net
        else:
            _fixed_notional = _eq_now
            equity_running *= (1.0 + r_net)

        eq_start_usd = STARTING_CAPITAL * equity_before
        eq_end_usd   = STARTING_CAPITAL * equity_running
        invested_usd = _fixed_notional * lev
        fee_usd      = _fixed_notional * fee_drag
        fund_usd     = _fixed_notional * funding_drag
        pnl_usd      = eq_end_usd - eq_start_usd

        # account-relative daily return (matches daily[] in fixed mode)
        if CAPITAL_MODE == "fixed" and abs(equity_before) > 1e-9:
            acct_ret = (_fixed_notional / STARTING_CAPITAL) * r_net / equity_before
            acct_ret = float(np.clip(acct_ret, -1.0, 10.0))
        else:
            acct_ret = r_net

        diag_rows.append([
            col_date,
            # Gate evaluations
            f"{roi_x*100:.4f}%",                      # path_1x at x_idx
            f"{early_y_1x*100:.4f}%",                 # kill threshold
            "KILL" if kill_fires else "PASS",          # kill result
            f"{instill_y_1x*100:.4f}%",               # instill threshold
            "PASS" if instill_ok else "BELOW",         # instill result
            # Leverage selection
            f"{lev:.4f}",                              # leverage applied
            f"{strong_thr*100:.4f}%",                  # strong threshold
            "L_HIGH" if lev >= l_high - 1e-9 else "L_BASE",
            # Entry / exit
            x_idx,                                     # entry bar index
            f"{entry_1x*100:.4f}%",                    # entry path value
            exit_bar,                                  # exit bar index
            f"{exit_incr*100:.4f}%",                   # raw incremental return
            stop_type,                                 # which stop fired
            # Return computation
            f"{r_gross*100:.4f}%",                     # r_gross = exit_incr * lev
            f"{fee_drag*100:.4f}%",                    # fee drag
            f"{funding_drag*100:.4f}%",                # funding drag
            f"{r_net*100:.4f}%",                       # r_net = r_gross - fees
            # Equity update
            f"${eq_start_usd:,.2f}",                   # equity before (USD)
            f"${_fixed_notional:,.2f}",                # notional deployed
            f"${invested_usd:,.2f}",                   # invested = notional * lev
            f"${fee_usd:,.4f}",                        # fee USD
            f"${fund_usd:,.4f}",                       # funding USD
            f"${eq_end_usd:,.2f}",                     # equity after (USD)
            f"${pnl_usd:+,.2f}",                       # net P&L USD
            f"{acct_ret*100:.4f}%",                    # daily[] value stored
            # Capital mode context
            CAPITAL_MODE,
            FIXED_NOTIONAL_CAP if CAPITAL_MODE == "fixed" else "n/a",
        ])

        # Full intraday path tab for target dates
        if target_dates is None or col_date in target_dates:
            path_rows = []
            for bar_i, (raw_v, v1x) in enumerate(zip(path_4x, path_1x)):
                bar_time_mins = 6 * 60 + bar_i * BAR_MINS
                bar_hhmm = f"{bar_time_mins // 60:02d}:{bar_time_mins % 60:02d}"
                incr_from_entry = (v1x - entry_1x) if not kill_fires else 0.0
                path_rows.append([
                    bar_i,
                    bar_hhmm,
                    f"{raw_v:.4f}",
                    f"{v1x*100:.4f}%",
                    f"{incr_from_entry*100:.4f}%",
                    "← x_idx (entry)" if bar_i == x_idx else
                    ("← exit" if bar_i == exit_bar else ""),
                ])
            path_tabs[col_date] = path_rows

    # ── Tab 1: DiagSummary ────────────────────────────────────────────────────
    print(f"  [diagnostic] Writing DiagSummary ...")
    ws_sum = _diag_get_or_create_tab(sh, "DiagSummary")
    import datetime as _dt2
    sum_rows = [
        ["MATH VERIFICATION DIAGNOSTIC", "", ""],
        ["Run date",         _dt2.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ""],
        ["Filter",           run_label,          ""],
        ["Capital mode",     CAPITAL_MODE,        ""],
        ["Fixed notional cap", FIXED_NOTIONAL_CAP if CAPITAL_MODE == "fixed" else "n/a", ""],
        ["Starting capital", f"${STARTING_CAPITAL:,}", ""],
        ["", "", ""],
        ["── PARAMS ──", "", ""],
        ["EARLY_KILL_X",    params.get("EARLY_KILL_X"), "minutes"],
        ["EARLY_KILL_Y",    params.get("EARLY_KILL_Y"), f"(1x: {early_y_1x*100:.4f}%)"],
        ["EARLY_INSTILL_Y", params.get("EARLY_INSTILL_Y"), f"(1x: {instill_y_1x*100:.4f}%)"],
        ["L_BASE",          params.get("L_BASE"), ""],
        ["L_HIGH",          params.get("L_HIGH"), ""],
        ["PORT_TSL",        params.get("PORT_TSL"), "1x fraction"],
        ["PORT_SL",         params.get("PORT_SL"), "1x fraction"],
        ["EARLY_FILL_Y",    params.get("EARLY_FILL_Y"), ""],
        ["EARLY_FILL_X",    params.get("EARLY_FILL_X"), "minutes"],
        ["TAKER_FEE_PCT",   TAKER_FEE_PCT, "per lev unit"],
        ["FUNDING_RATE_DAILY_PCT", FUNDING_RATE_DAILY_PCT, "per lev unit"],
        ["PIVOT_LEVERAGE",  PIVOT_LEVERAGE, ""],
        ["", "", ""],
        ["── KEY METRICS ──", "", ""],
    ] + [[k, v, ""] for k, v in key_metrics.items()]
    _with_retry(lambda: ws_sum.append_rows(sum_rows, value_input_option="USER_ENTERED"))
    time.sleep(_DIAG_INTER_CALL_DELAY)

    # ── Tab 2: DiagDaily ─────────────────────────────────────────────────────
    print(f"  [diagnostic] Writing DiagDaily ({len(diag_rows)} active days) ...")
    ws_daily = _diag_get_or_create_tab(sh, "DiagDaily")  # includes pacing sleeps
    header = [
        "date",
        "path_1x@x_idx", "kill_threshold", "kill_result",
        "instill_threshold", "instill_result",
        "lev_applied", "strong_thr", "lev_tier",
        "entry_bar", "entry_path_1x", "exit_bar", "exit_incr_1x", "stop_type",
        "r_gross", "fee_drag", "funding_drag", "r_net",
        "equity_start_usd", "notional_usd", "invested_usd",
        "fee_usd", "funding_usd", "equity_end_usd", "pnl_usd",
        "daily_ret_stored",
        "capital_mode", "notional_cap",
    ]
    _with_retry(lambda: ws_daily.append_rows([header], value_input_option="USER_ENTERED"))
    time.sleep(_DIAG_INTER_CALL_DELAY)
    _diag_batch_write(ws_daily, diag_rows)  # includes inter-chunk pacing

    # ── Tab 3+: DiagPath_YYYYMMDD ─────────────────────────────────────────────
    _n_path_tabs = len(path_tabs)
    if _n_path_tabs > 0:
        print(f"  [diagnostic] Writing {_n_path_tabs} DiagPath tabs "
              f"(~{_n_path_tabs * _DIAG_INTER_CALL_DELAY * 4 / 60:.1f} min estimated) ...")
    for _pt_idx, (date_str, p_rows) in enumerate(path_tabs.items(), 1):
        tab_name = f"DiagPath_{date_str}"
        if _pt_idx % 20 == 0 or _pt_idx == 1:
            print(f"  [diagnostic]   {_pt_idx}/{_n_path_tabs} — {tab_name}")
        ws_path = _diag_get_or_create_tab(sh, tab_name)  # includes pacing sleeps
        path_header = ["bar_idx", "bar_time_utc", "raw_4x", "path_1x_pct",
                       "incr_from_entry_pct", "annotation"]
        _with_retry(lambda h=path_header: ws_path.append_rows(
            [h], value_input_option="USER_ENTERED"))
        time.sleep(_DIAG_INTER_CALL_DELAY)
        _diag_batch_write(ws_path, p_rows)  # includes inter-chunk pacing

    print(f"  [diagnostic] ✅ Export complete → "
          f"https://docs.google.com/spreadsheets/d/{DIAGNOSTIC_SHEET_ID}")



def load_matrix() -> pd.DataFrame:
    if LOCAL_MATRIX_CSV:
        # ── Load from local CSV (eligibility-gated or other override) ──────────
        # Read all rows — N_ROWS is not used here. The caller syncs N_ROWS to
        # the actual row count after loading so the simulation stays consistent.
        df = pd.read_csv(LOCAL_MATRIX_CSV)
        # Drop the timestamp_utc index column
        df = df.drop(columns=["timestamp_utc"], errors="ignore")
        df = df.replace('%', '', regex=True)
        df = df.apply(pd.to_numeric, errors="coerce") / 100.0
        df = df.dropna(axis=1, how="all")
        print(f"  [load_matrix] Loaded local CSV: {LOCAL_MATRIX_CSV}  "
              f"({len(df)} rows × {len(df.columns)} cols)")
        return df

    # ── Load from Google Sheets (default) ─────────────────────────────────────
    # N_ROWS is used as the row ceiling when reading from Sheets. For local CSV
    # runs the actual row count drives N_ROWS (see caller sync below).
    gc     = gspread.oauth(credentials_filename=CREDENTIALS_FILE,
                           authorized_user_filename=TOKEN_FILE)
    sh     = _with_retry(lambda: gc.open_by_key(SPREADSHEET_ID))
    ws     = _with_retry(lambda: sh.worksheet(MATRIX_TAB))
    values = _with_retry(lambda: ws.get_all_values())

    header   = values[0]
    day_names = [str(x).strip() for x in header[START_COL - 1:]]
    body     = values[START_ROW - 1: START_ROW - 1 + N_ROWS]
    body     = [row[START_COL - 1: START_COL - 1 + len(day_names)] for row in body]

    df = pd.DataFrame(body, columns=day_names)
    df = df.replace('%', '', regex=True)
    df = df.apply(pd.to_numeric, errors="coerce") / 100.0
    df = df.dropna(axis=1, how="all")
    return df

# ══════════════════════════════════════════════════════════════════════
# COMPARISON CHART
# ══════════════════════════════════════════════════════════════════════

def plot_monthly_cumulative_returns(
    results_map: Dict,
    run_dir:     Path,
    col_dates:   list = None,
) -> Optional[Path]:
    """
    Bar chart showing cumulative return for each calendar month in the sample,
    one group of bars per filter mode in results_map.

    Layout
    ------
    Each month on the x-axis.  Bars grouped by filter mode, coloured using
    the same palette as plot_comparison.  Green = positive, red = negative
    (individual bar colour overrides the filter colour for clarity).
    A line overlay traces the cumulative compounded equity for each filter.

    Saved to: run_dir / "monthly_cumulative_returns.png"
    """
    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"
    GRID_COL = "#21262d"

    # ── Filter colour map (mirrors plot_comparison) ───────────────────
    _BASE_COLORS = {
        "No Filter":          "#f0b429",
        "Tail Guardrail":     "#58a6ff",
        "Tail + Dispersion":  "#56d364",
        "Tail + Disp + Vol":  "#f778ba",
        "Tail + Blofin":      "#e06c75",
        "Dispersion":         "#c9a0dc",
        "Calendar":           "#aaaaaa",
    }
    _FALLBACK = ["#ff7b72","#ffa657","#d2a679","#79c0ff","#c9a0dc","#56d364"]
    _fb_idx   = [0]

    def _color(run_key):
        for filt, col in _BASE_COLORS.items():
            if filt in run_key:
                return col
        col = _FALLBACK[_fb_idx[0] % len(_FALLBACK)]
        _fb_idx[0] += 1
        return col

    if col_dates is None:
        return None

    # ── Build monthly return series per run_key ───────────────────────
    monthly_data: Dict[str, pd.Series] = {}

    for run_key, res in results_map.items():
        dwz = res.get("daily_with_zeros")
        if dwz is None:
            continue
        dwz_arr = np.where(np.isfinite(dwz), dwz, 0.0)

        # Align to calendar dates from the matrix columns
        valid_pairs = [
            (d, v) for d, v in zip(col_dates, dwz_arr)
            if d is not None and math.isfinite(v)
        ]
        if not valid_pairs:
            continue

        idx, vals = zip(*valid_pairs)
        s = pd.Series(list(vals), index=pd.DatetimeIndex(list(idx)))

        # Compound daily returns within each calendar month
        monthly = (1 + s).resample("ME").prod() - 1
        monthly_data[run_key] = monthly * 100   # convert to %

    if not monthly_data:
        return None

    # ── Common month index (union of all months present) ─────────────
    all_months = sorted(
        set().union(*[set(m.index) for m in monthly_data.values()])
    )
    if not all_months:
        return None

    n_runs   = len(monthly_data)
    n_months = len(all_months)
    labels   = [m.strftime("%b\n'%y") for m in all_months]

    # Bar positioning
    bar_w     = 0.8 / max(n_runs, 1)
    offsets   = np.linspace(-(n_runs - 1) / 2, (n_runs - 1) / 2, n_runs) * bar_w
    x_pos     = np.arange(n_months)

    # ── Figure layout: bar chart top, cumulative equity line bottom ───
    fig, (ax_bar, ax_eq) = plt.subplots(
        2, 1, figsize=(max(14, n_months * 1.1), 12),
        gridspec_kw={"height_ratios": [3, 1.2]},
    )
    fig.patch.set_facecolor(FIG_BG)

    for ax in (ax_bar, ax_eq):
        ax.set_facecolor(PANEL_BG)
        ax.tick_params(colors=TEXT_COL, labelsize=9)
        for sp in ax.spines.values():
            sp.set_color(GRID_COL)
        ax.grid(alpha=0.20, color=GRID_COL, linewidth=0.6, axis="y")
        ax.set_axisbelow(True)

    handles = []
    for i, (run_key, monthly) in enumerate(monthly_data.items()):
        col   = _color(run_key)
        off   = offsets[i]
        vals  = [monthly.get(m, float("nan")) for m in all_months]

        # Individual bars: green if positive, red if negative
        bar_colors = [
            "#3fb950" if (v > 0 and math.isfinite(v)) else
            "#f85149" if (v < 0 and math.isfinite(v)) else
            "#555555"
            for v in vals
        ]
        bars = ax_bar.bar(
            x_pos + off, vals,
            width=bar_w * 0.88,
            color=bar_colors,
            edgecolor=col,
            linewidth=0.8,
            zorder=3,
            alpha=0.85,
        )
        # Invisible proxy for the legend (shows the filter's colour)
        proxy = mpatches.Patch(facecolor=col, label=run_key, alpha=0.9)
        handles.append(proxy)

        # ── Cumulative equity overlay on bottom panel ─────────────────
        cum_vals = []
        cum      = 1.0
        for v in vals:
            if math.isfinite(v):
                cum *= (1 + v / 100)
            cum_vals.append((cum - 1) * 100)

        ax_eq.plot(
            x_pos, cum_vals,
            color=col, linewidth=1.8, marker="o", markersize=4,
            label=run_key, zorder=4,
        )

    # ── Formatting — bar panel ────────────────────────────────────────
    ax_bar.axhline(0, color=GRID_COL, linewidth=1.0, zorder=2)
    ax_bar.set_xticks(x_pos)
    ax_bar.set_xticklabels(labels, color=TEXT_COL, fontsize=8)
    ax_bar.set_ylabel("Monthly Return (%)", color=TEXT_COL, fontsize=10)
    ax_bar.tick_params(axis="y", colors=TEXT_COL)
    ax_bar.set_xlim(-0.6, n_months - 0.4)

    # Zero line and value labels on bars if few runs and months
    if n_runs <= 3 and n_months <= 18:
        for i, (run_key, monthly) in enumerate(monthly_data.items()):
            off  = offsets[i]
            vals = [monthly.get(m, float("nan")) for m in all_months]
            for j, v in enumerate(vals):
                if math.isfinite(v) and abs(v) >= 1.0:
                    ax_bar.text(
                        x_pos[j] + off, v + (0.3 if v >= 0 else -0.8),
                        f"{v:+.0f}%", ha="center", va="bottom" if v >= 0 else "top",
                        fontsize=6.5, color=TEXT_COL, zorder=5,
                    )

    ax_bar.legend(
        handles=handles, loc="upper left",
        facecolor=PANEL_BG, edgecolor=GRID_COL,
        labelcolor=TEXT_COL, fontsize=8, framealpha=0.9,
    )
    ax_bar.set_title(
        "Monthly Returns by Filter Mode",
        color=TEXT_COL, fontsize=13, pad=10, fontweight="bold",
    )

    # ── Formatting — cumulative equity panel ──────────────────────────
    ax_eq.axhline(0, color=GRID_COL, linewidth=0.8, zorder=2)
    ax_eq.set_xticks(x_pos)
    ax_eq.set_xticklabels(labels, color=TEXT_COL, fontsize=8)
    ax_eq.set_ylabel("Cumulative Return (%)", color=TEXT_COL, fontsize=9)
    ax_eq.tick_params(axis="y", colors=TEXT_COL)
    ax_eq.set_xlim(-0.6, n_months - 0.4)
    ax_eq.set_title(
        "Compounded Cumulative Return (month-by-month)",
        color=TEXT_COL, fontsize=10, pad=6,
    )

    fig.tight_layout(pad=2.0)

    out = run_dir / "monthly_cumulative_returns.png"
    fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor=FIG_BG)
    plt.close(fig)
    print(f"  Saved: {out.name}")
    return out


def plot_comparison(results_map: Dict, run_dir: Path, col_dates=None):
    """Side-by-side equity curves (date-aligned) + rolling Sharpe + subplots per metric."""


    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"
    GRID_COL = "#21262d"

    # Colour = filter type,  linestyle = config
    _BASE_COLORS = {
        "No Filter":          "#f0b429",   # amber
        "Tail Guardrail":     "#58a6ff",   # blue
        "Tail + Dispersion":  "#56d364",   # green
        "Tail + Disp + Vol":  "#f778ba",   # pink
        "Tail + Blofin":      "#e06c75",   # coral-red
        "Dispersion":         "#c9a0dc",   # lavender
        "Calendar":           "#aaaaaa",   # gray (disabled but present for safety)
        "V5-B Majority":      "#79c0ff",   # light blue
        "V5-D Majority":      "#d2a679",   # tan
    }
    # Fallback palette for any run_key not matched above — cycles through
    # visually distinct hues so nothing ever defaults to gray.
    _FALLBACK_PALETTE = [
        "#ff7b72",  # salmon
        "#ffa657",  # orange
        "#d2a679",  # tan
        "#79c0ff",  # sky blue
        "#c9a0dc",  # lavender
        "#56d364",  # green
        "#f778ba",  # pink
        "#58a6ff",  # blue
    ]
    _fallback_index = [0]   # mutable counter for closure

    def _color(run_key):
        for filt, col in _BASE_COLORS.items():
            if filt in run_key:
                return col
        # Assign a unique fallback colour and advance the counter
        col = _FALLBACK_PALETTE[_fallback_index[0] % len(_FALLBACK_PALETTE)]
        _fallback_index[0] += 1
        return col

    _CFG_LS = {"Balanced-Opt": "-"}

    def _ls(run_key):
        for cfg in _CFG_LS:
            if cfg in run_key:
                return _CFG_LS[cfg]
        return "-"

    # Keep legacy dicts for any other code that still uses them
    FILTER_COLORS = {k: _color(k) for k in results_map}
    FILTER_LS     = {k: _ls(k)    for k in results_map}

    # 6 panels: equity, rolling Sharpe, then 4 individual metric subplots
    fig = plt.figure(figsize=(20, 24))
    fig.patch.set_facecolor(FIG_BG)
    from matplotlib.gridspec import GridSpec
    gs_top = GridSpec(2, 1, figure=fig, hspace=0.08,
                      top=0.93, bottom=0.50, left=0.09, right=0.96)
    gs_bot = GridSpec(1, 4, figure=fig, hspace=0.05, wspace=0.35,
                      top=0.44, bottom=0.07, left=0.09, right=0.96)

    ax_eq  = fig.add_subplot(gs_top[0])
    ax_rs  = fig.add_subplot(gs_top[1])
    metric_axes = [fig.add_subplot(gs_bot[0, j]) for j in range(4)]

    def style(ax, ylabel, date_axis=False):
        ax.set_facecolor(PANEL_BG)
        ax.tick_params(colors=TEXT_COL, labelsize=9)
        for sp in ax.spines.values(): sp.set_color(GRID_COL)
        ax.grid(alpha=0.22, color=GRID_COL, linewidth=0.7)
        ax.set_ylabel(ylabel, color=TEXT_COL, fontsize=10, labelpad=5)
        ax.tick_params(axis="y", colors=TEXT_COL)
        if date_axis and col_dates is not None:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b \'%y"))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            plt.setp(ax.xaxis.get_majorticklabels(),
                     rotation=30, ha="right", color=TEXT_COL, fontsize=8)

    # ── Panel 0: Equity curves (date-aligned, includes flat 0-return days) ──
    ax = ax_eq
    for label, res in results_map.items():
        # daily_with_zeros: 0.0 = filtered flat day, nan = non-finite sim day,
        # finite non-zero = normal return.  Replace nan with 0.0 for cumulative
        # product so the curve neither jumps nor loses date alignment.
        r_raw = res["daily_with_zeros"].copy()
        r_plot = np.where(np.isfinite(r_raw), r_raw, 0.0)
        eq = np.cumprod(1 + r_plot) * STARTING_CAPITAL
        x  = col_dates if (col_dates is not None and len(col_dates) == len(eq)) \
             else np.arange(len(eq))
        final = eq[-1]
        ax.plot(x, eq, color=FILTER_COLORS[label], lw=2.2,
                ls=FILTER_LS.get(label, "-"),
                label=f"{label}  ->  ${final:,.0f}")
    style(ax, "Portfolio Value ($)", date_axis=True)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda v, _: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v:,.0f}"))
    ax.set_title("Best_Sharpe - Regime Filter Comparison  "
                 "(solid=No Filter  dashed=Calendar  dotted=V3  dash-dot=V4)",
                 color=TEXT_COL, fontsize=12, pad=8, fontweight="bold")
    ax.legend(fontsize=10, facecolor=PANEL_BG, labelcolor=TEXT_COL, loc="upper left")

    # ── Panel 1: Rolling 60d Sharpe ──────────────────────────────────
    ax = ax_rs
    for label, res in results_map.items():
        r_raw  = res["daily_with_zeros"].copy()
        r_plot = np.where(np.isfinite(r_raw), r_raw, 0.0)
        rs = pd.Series(r_plot).rolling(60, min_periods=20).apply(
            lambda x: float(np.mean(x) / np.std(x, ddof=1) * np.sqrt(TRADING_DAYS))
            if float(np.std(x, ddof=1)) > 0 else 0.0, raw=True)
        x  = col_dates if (col_dates is not None and len(col_dates) == len(rs)) \
             else np.arange(len(rs))
        ax.plot(x, rs.values, color=FILTER_COLORS[label], lw=1.8,
                ls=FILTER_LS.get(label, "-"), alpha=0.9, label=label)
    ax.axhline(0, color=TEXT_COL, lw=0.8, ls="--", alpha=0.4)
    ax.axhline(3, color="#ffa657", lw=1.0, ls=":", alpha=0.6, label="Sharpe=3")
    style(ax, "Rolling 60d Sharpe (ann.)", date_axis=True)
    ax.legend(fontsize=9, facecolor=PANEL_BG, labelcolor=TEXT_COL, loc="upper left")

    # ── Panels 2-5: One subplot per metric ───────────────────────────
    filter_labels = list(results_map.keys())
    x_pos = np.arange(len(filter_labels))

    METRIC_DEFS = [
        ("Sharpe",       lambda r: r.get("sharpe",  float("nan")),  "{:.3f}", None),
        ("MaxDD %",      lambda r: abs(r.get("maxdd", float("nan"))), "{:.1f}%", None),
        ("WF CV",        lambda r: r.get("cv",       float("nan")),  "{:.3f}", 0.25),
        ("Score / 100",  lambda r: float(r.get("scorecard",{}).get("total_score",0)), "{:.0f}", None),
    ]

    for ax, (title, getter, fmt, target) in zip(metric_axes, METRIC_DEFS):
        ax.set_facecolor(PANEL_BG)
        for sp in ax.spines.values(): sp.set_color(GRID_COL)
        ax.grid(alpha=0.22, color=GRID_COL, linewidth=0.7, axis="y")
        ax.tick_params(colors=TEXT_COL, labelsize=9)

        vals = []
        for lbl in filter_labels:
            try:
                v = getter(results_map[lbl])
                vals.append(v if math.isfinite(v) else 0.0)
            except Exception:
                vals.append(0.0)

        bar_colors = [FILTER_COLORS[l] for l in filter_labels]
        bars = ax.bar(x_pos, vals, 0.55, color=bar_colors, alpha=0.85,
                      edgecolor=GRID_COL, linewidth=0.5)

        for bar, val, lbl in zip(bars, vals, filter_labels):
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(vals) * 0.01,
                    fmt.format(val),
                    ha="center", va="bottom",
                    color=FILTER_COLORS[lbl], fontsize=10, fontweight="bold")

        if target is not None:
            ax.axhline(target, color="#ff7b72", lw=1.2, ls="--",
                       alpha=0.7, label=f"target={target}")
            ax.legend(fontsize=8, facecolor=PANEL_BG, labelcolor=TEXT_COL)

        ax.set_xticks(x_pos)
        short = [l.replace(" Filter","\nFilter").replace(" FR+F&G","\nFR+F&G")
                 for l in filter_labels]
        ax.set_xticklabels(short, color=TEXT_COL, fontsize=8)
        ax.set_title(title, color=TEXT_COL, fontsize=10, pad=5, fontweight="bold")
        # y-axis: start from 0 or slightly below min
        lo = min(v for v in vals if v > 0) * 0.85 if any(v > 0 for v in vals) else 0
        ax.set_ylim(bottom=max(0, lo * 0.9))

    out = run_dir / "regime_filter_comparison.png"
    plt.savefig(str(out), dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()
    return out

# ══════════════════════════════════════════════════════════════════════
# SCORECARD PRINTER
# ══════════════════════════════════════════════════════════════════════

def print_comparison_table(results_map: Dict):
    W   = 90
    SEP = "═" * W

    print("\n" + SEP)
    print("  REGIME FILTER COMPARISON - BEST_SHARPE CONFIG")
    print("  Note: CAGR and Sharpe annualised over TOTAL elapsed days (incl. flat).")
    print("  Flat days count as 0% return. This prevents filtered runs from")
    print("  appearing better merely because they traded fewer days.")
    print(SEP)

    filters = list(results_map.keys())
    col_w   = 22                        # per-column width
    lbl_w   = 34                        # metric label width

    def _hdr():
        print(f"\n  {'Metric':<{lbl_w}}", end="")
        for f in filters:
            short = f[:col_w - 2]
            print(f"  {short:>{col_w - 2}}", end="")
        print()
        print("  " + "─" * (lbl_w + col_w * len(filters)))

    def _row(label, getter, fmt=lambda v: f"{v:>{col_w - 2}.3f}", star=False):
        prefix = "★ " if star else "  "
        print(f"{prefix}{label:<{lbl_w}}", end="")
        for f in filters:
            try:
                v = getter(results_map[f])
                print(f"  {fmt(v)}", end="")
            except Exception:
                print(f"  {'n/a':>{col_w - 2}}", end="")
        print()

    def _sect(title):
        print(f"\n  ── {title} {'─' * (W - len(title) - 6)}")

    # ════════════════════════════════════════════════════════════════
    # SECTION 1 - PRIORITY METRICS  (starred rows = decision-critical)
    # ════════════════════════════════════════════════════════════════
    _sect("PRIORITY METRICS")
    _hdr()

    _row("Net Return % (total)",
         lambda r: r.get("net_return_pct", float("nan")),
         lambda v: f"{v:>{col_w - 2}.1f}%", star=True)
    _row("Sharpe (full period)",
         lambda r: r["sharpe"], star=True)
    _row("Max Drawdown %",
         lambda r: r["maxdd"],
         lambda v: f"{v:>{col_w - 2}.2f}%", star=True)
    _row("Walk-Forward CV (↓better)",
         lambda r: r["cv"], star=True)
    _row("WF IS/OOS CAGR ratio (>=0.50)",
         lambda r: r["cagr_ratio"], star=True)
    _row("Sharpe Decay IS->OOS % (<40%)",
         lambda r: r["decay_pct"],
         lambda v: f"{v:>{col_w - 2}.1f}%", star=True)
    _row("FA-WF CV (active folds only)",
         lambda r: r.get("fa_wf_cv", float("nan")), star=True)
    _row("  FA-WF saturated folds excluded",
         lambda r: r.get("fa_wf_n_saturated", float("nan")),
         lambda v: f"{int(v):>{col_w - 2}d}")
    _row("FA-WF Mean OOS Sharpe",
         lambda r: r.get("fa_wf_mean_sharpe", float("nan")), star=True)
    _row("FA-WF % Folds Positive",
         lambda r: r.get("fa_wf_pct_positive", float("nan")),
         lambda v: f"{v:>{col_w - 2}.1f}%", star=True)
    _row("FA-WF Unstable Folds",
         lambda r: r.get("fa_wf_n_unstable", float("nan")),
         lambda v: f"{int(v):>{col_w - 2}d}", star=True)
    _row("Flat days (filtered out)",
         lambda r: r["flat_days"],
         lambda v: f"{int(v):>{col_w - 2}d}", star=True)
    _row("Active trading days",
         lambda r: r["active_days"],
         lambda v: f"{int(v):>{col_w - 2}d}", star=True)

    # ════════════════════════════════════════════════════════════════
    # SECTION 2 - SECONDARY METRICS
    # ════════════════════════════════════════════════════════════════
    _sect("SECONDARY METRICS")
    _hdr()

    _row("CAGR % (annualised)",
         lambda r: r["cagr"],
         lambda v: f"{v:>{col_w - 2}.1f}%")
    _row("CAGR / Total Return (×)",
         lambda r: (r["cagr"] / r["net_return_pct"]
                    if math.isfinite(r.get("net_return_pct", float("nan")))
                    and r.get("net_return_pct", 0) != 0
                    else float("nan")),
         lambda v: f"{v:>{col_w - 2}.2f}×")
    _row("Equity Multiplier (×)",
         lambda r: (1.0 + r["net_return_pct"] / 100.0
                    if math.isfinite(r.get("net_return_pct", float("nan")))
                    else float("nan")),
         lambda v: f"{v:>{col_w - 2}.2f}×")
    _row("Doubling Periods",
         lambda r: (math.log2(1.0 + r["net_return_pct"] / 100.0)
                    if math.isfinite(r.get("net_return_pct", float("nan")))
                    and (1.0 + r["net_return_pct"] / 100.0) > 0
                    else float("nan")),
         lambda v: f"{v:>{col_w - 2}.2f}")
    _row("Volatility Drag Ann %",
         lambda r: (float(np.std(np.where(np.isfinite(r.get("daily_with_zeros", np.array([]))),
                                          r.get("daily_with_zeros", np.array([])), 0.0), ddof=1) ** 2
                          / 2 * TRADING_DAYS * 100)
                    if r.get("daily_with_zeros") is not None
                    and len(r.get("daily_with_zeros", [])) > 1
                    else float("nan")),
         lambda v: f"{v:>{col_w - 2}.1f}%")
    _row("FA-WF CV (filter-aware, <0.25)",
         lambda r: r.get("fa_wf_cv", float("nan")))
    _row("Sortino Ratio",
         lambda r: r["sortino"])
    _row("Calmar Ratio",
         lambda r: r["calmar"])
    _row("DSR (prob genuine, >=95%)",
         lambda r: r["dsr"],
         lambda v: f"{v:>{col_w - 2}.1f}%")
    _row("Beta to BTC",
         lambda r: r.get("beta", float("nan")),
         lambda v: f"{v:>{col_w - 2}.3f}")
    _row("Annual Alpha (BTC-neutral)",
         lambda r: r.get("alpha_annual", float("nan")),
         lambda v: f"{v:>{col_w - 2}.0f}%")
    _row("R² (BTC explained var, ↓better)",
         lambda r: r.get("r2", float("nan")),
         lambda v: f"{v:>{col_w - 2}.2f}%", star=True)

    # ════════════════════════════════════════════════════════════════
    # SECTION 3 - SCORECARD
    # ════════════════════════════════════════════════════════════════
    _sect("SCORECARD")
    print()
    print(f"\n  {'OVERALL GRADE':<{lbl_w}}", end="")
    for f in filters:
        sc    = results_map[f].get("scorecard", {})
        grade = sc.get("overall_grade", "?")
        score = sc.get("total_score", "?")
        cell  = f"{grade} ({score}/100)"
        print(f"  {cell:>{col_w - 2}}", end="")
    print()

    all_metrics: set = set()
    for res in results_map.values():
        for k in res.get("scorecard", {}).get("metrics", {}).keys():
            all_metrics.add(k)

    # Walk-forward CV metric names used by institutional_audit (varies by version)
    _wf_metric_names = {"walk_forward", "walk_forward_cv", "wf_cv", "wf_stability",
                        "regime_robustness", "rolling_sharpe_cv"}

    for metric in sorted(all_metrics):
        is_wf_metric = any(w in metric.lower() for w in _wf_metric_names)
        prefix = "  ⚠ " if is_wf_metric else "    "
        print(f"{prefix}{metric:<{lbl_w}}", end="")
        for f in filters:
            sc  = results_map[f].get("scorecard", {})
            m   = sc.get("metrics", {}).get(metric, {})
            pts = m.get("score", 0)
            mx  = m.get("max_score", 0)
            ok  = "✅" if m.get("passed", False) else "❌"
            # For WF metrics on filtered runs: flag that CV may be inflated
            n_sat = results_map[f].get("fa_wf_n_saturated", 0)
            if is_wf_metric and n_sat and n_sat > 0 and not m.get("passed", True):
                ok = "⚠❌"
            cell = f"{ok} {pts}/{mx}"
            print(f"  {cell:>{col_w - 2}}", end="")
        print()
    print(f"  Note: ⚠❌ on WF metrics = failed due to calendar-saturated folds in internal CV.")
    print(f"        Check FA-WF CV (active folds) in PRIORITY METRICS for the honest value.")

    # ════════════════════════════════════════════════════════════════
    # SECTION 4 - HMM STATE DIAGNOSTICS (one block per HMM filter)
    # ════════════════════════════════════════════════════════════════
    hmm_labels = [lbl for lbl in ("V5-B Majority", "V5-D Majority")
                  if lbl in _HMM_DIAG]
    if hmm_labels:
        print(f"\n\n{'═' * W}")
        print("  HMM STATE DIAGNOSTICS")
        print(f"{'═' * W}")
        print("  Columns: Occ% = days in this state (train+test).")
        print("  AvgDur  = mean consecutive-day run length.")
        print("           < 2d -> noise gating   5-15d -> genuine regime")
        print("  Signal? = does risk-off state actually have negative mean return")
        print("            and < 50% win-rate?  ✅ = valid   ❌ = inverted/wrong")
        for lbl in hmm_labels:
            _hmm_diag_print(lbl)

    # ════════════════════════════════════════════════════════════════
    # SECTION 5 - BEST FILTER VERDICT
    # ════════════════════════════════════════════════════════════════
    print(f"\n{'═' * W}")
    print("  VERDICT")
    print(f"{'═' * W}")

    # ── Minimum-activity disqualification ────────────────────────────
    # A filter that sits flat >45% of the year is not a filter - it's
    # a strategy replacement. Such runs are DQ'd from the ranking and
    # shown separately. Threshold: must trade at least 200 days/year.
    MIN_ACTIVE_DAYS = 80
    total_days = max((results_map[f].get("active_days", 0) +
                      results_map[f].get("flat_days", 0))
                     for f in filters) or 365
    dq_filters    = {f for f in filters
                     if results_map[f].get("active_days", 9999) < MIN_ACTIVE_DAYS}
    active_filters = [f for f in filters if f not in dq_filters]

    if dq_filters:
        print(f"  ⛔  DISQUALIFIED (< {MIN_ACTIVE_DAYS} active trading days):")
        for f in sorted(dq_filters):
            act = results_map[f].get("active_days", "?")
            print(f"       {f}  ->  {act} active days  "
                  f"(rule: must trade >={MIN_ACTIVE_DAYS}d/yr to be ranked)")
        print()

    # Score each filter on priority metrics (lower rank = better)
    # Only active (non-DQ'd) filters participate in the ranking.
    rank_filters = active_filters if active_filters else filters   # fallback if all DQ
    scores: Dict[str, float] = {f: 0.0 for f in rank_filters}
    criteria = [
        # (metric_getter, higher_is_better)
        (lambda r: r["sharpe"],                              True),
        (lambda r: -abs(r["maxdd"]),                         True),   # less negative = better
        (lambda r: -r["cv"],                                 True),   # lower CV = better
        (lambda r: r.get("fa_wf_mean_sharpe", float("-inf")), True),
        (lambda r: r.get("fa_wf_pct_positive", 0),          True),
        (lambda r: -r.get("fa_wf_n_unstable", 99),          True),
        (lambda r: r["active_days"],                         True),
    ]
    for getter, higher_is_better in criteria:
        vals = []
        for f in rank_filters:
            try:
                v = getter(results_map[f])
                vals.append((f, float(v)))
            except Exception:
                vals.append((f, float("-inf")))
        vals.sort(key=lambda x: x[1], reverse=higher_is_better)
        for rank, (f, _) in enumerate(vals):
            scores[f] += rank   # lower total = better

    ranked = sorted(scores.items(), key=lambda x: x[1])
    print()
    # Show DQ'd runs at the bottom with a clear marker
    ranked_all = ranked + [(f, None) for f in sorted(dq_filters)]
    for rank, (f, score) in enumerate(ranked, 1):
        medal = ["🥇", "🥈", "🥉"][min(rank - 1, 2)]
        r     = results_map[f]
        sh    = r["sharpe"]
        dd    = r["maxdd"]
        cv    = r["cv"]
        fa    = r.get("fa_wf_mean_sharpe", float("nan"))
        flat  = int(r["flat_days"])
        act   = int(r["active_days"])
        fa_s  = f"{fa:.3f}" if not math.isnan(fa) else "n/a"
        eq_r2 = r.get("equity_r2", float("nan"))
        eq_r2_s = f"{eq_r2:.4f}" if not math.isnan(eq_r2) else "n/a"
        nr    = r.get("net_return_pct", float("nan"))
        nr_s  = f"{nr:+.1f}%" if not math.isnan(nr) else "n/a"
        print(f"  {medal}  #{rank}  {f}")
        print(f"       NetRet={nr_s}  Sharpe={sh:.3f}  MaxDD={dd:.2f}%  WF-CV={cv:.3f}"
              f"  FA-OOS Sharpe={fa_s}  Flat={flat}d  Active={act}d  Equity-R²={eq_r2_s}")

        # HMM-specific sanity note
        for diag_lbl in ("V5-B Majority", "V5-D Majority"):
            if diag_lbl in f and diag_lbl in _HMM_DIAG:
                d = _HMM_DIAG[diag_lbl]
                issues = []
                for s in d["bad_states"]:
                    info = d["states"].get(s, {})
                    if info.get("occ_pct", 100) < 5:
                        issues.append(f"S{s} thin ({info['occ_pct']:.0f}%)")
                    dur = info.get("avg_dur", 99)
                    if not math.isnan(dur) and dur < 2:
                        issues.append(f"S{s} noisy ({dur:.1f}d avg dur)")
                    mr = info.get("mean_ret", float("nan"))
                    if not math.isnan(mr) and mr > 0:
                        issues.append(f"S{s} risk-off has +ret ({mr:+.2f}%)")
                if issues:
                    print(f"       ⚠️  HMM issues: {', '.join(issues)}")
                else:
                    print(f"       ✅  HMM states structurally separated + stable")
        print()

    # ── Show DQ'd runs below ranked ones ─────────────────────────────
    if dq_filters:
        print(f"  {'─'*70}")
        print(f"  ⛔  INELIGIBLE RUNS (active days < {MIN_ACTIVE_DAYS} - cannot be ranked):")
        for f in sorted(dq_filters):
            r   = results_map[f]
            sh  = r["sharpe"]
            dd  = r["maxdd"]
            act = int(r["active_days"])
            flat = int(r["flat_days"])
            nr   = r.get("net_return_pct", float("nan"))
            nr_s = f"{nr:+.1f}%" if not math.isnan(nr) else "n/a"
            print(f"       ⛔  {f}")
            print(f"            NetRet={nr_s}  Sharpe={sh:.3f}  MaxDD={dd:.2f}%  "
                  f"Active={act}d ({act/total_days*100:.0f}%)  Flat={flat}d")
        print()

    print(SEP + "\n")


# ══════════════════════════════════════════════════════════════════════
# CORRECTED INDIVIDUAL EQUITY CHART
# ══════════════════════════════════════════════════════════════════════

def _overwrite_equity_chart(outdir, label, daily_with_zeros, col_dates,
                            starting_capital, flat_days, cagr, sharpe, maxdd,
                            perf_lev_scalar_history=None,
                            vol_lev_scalar_history=None,
                            contra_lev_scalar_history=None):
    """
    Replace the equity chart written by run_institutional_audit with a
    correctly date-aligned version that shows flat horizontal lines during
    filtered (sit-out) periods. Finds any *equity*.png in outdir and
    overwrites it; also saves a guaranteed copy as equity_corrected.png.

    Chart layout (3 panels):
      Top    — Equity curve + ATH line + rolling avg-DD band
      Middle — Running drawdown from ATH (waterfall style)
      Bottom — Rolling 30d Sharpe heatmap strip
    """

    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"
    GRID_COL = "#21262d"
    LINE_COL = "#58a6ff"
    ATH_COL  = "#3fb950"   # green — all-time high line
    DD_COL   = "#f85149"   # red  — drawdown fill
    BAND_COL = "#e3b341"   # gold — rolling avg DD band
    FLAT_COL = "#30363d"

    # Build equity and drawdown series
    r_plot = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
    eq     = np.cumprod(1 + r_plot) * starting_capital
    ath    = np.maximum.accumulate(eq)          # all-time high
    dd_pct = (eq - ath) / ath * 100             # running drawdown %

    # Persist the daily equity series to CSV when AUDIT_DAILY_EQUITY_DIR is set.
    # Consumed by backend/app/cli/backfill_equity_curves.py to populate
    # audit.equity_curves for the strategy view. One file per filter label
    # (e.g. equity_tail.csv, equity_dispersion.csv) so the caller can read
    # whichever label matches the strategy's picked filter_mode.
    # Columns: date (YYYY-MM-DD), equity (USD), daily_return (fractional),
    # drawdown_pct. One row per trading day in col_dates order.
    _equity_csv_dir = os.environ.get("AUDIT_DAILY_EQUITY_DIR")
    if _equity_csv_dir:
        try:
            import csv as _csv
            _safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(label))
            _path = os.path.join(_equity_csv_dir, f"equity_{_safe}.csv")
            with open(_path, "w", newline="") as _fh:
                _w = _csv.writer(_fh)
                _w.writerow(["date", "equity", "daily_return", "drawdown_pct"])
                for i, _d in enumerate(col_dates or []):
                    if _d is None or i >= len(eq):
                        continue
                    _w.writerow([
                        str(_d)[:10],
                        float(eq[i]),
                        float(r_plot[i]),
                        float(dd_pct[i]),
                    ])
            print(f"[INFO] wrote daily equity CSV → {_path} ({len(eq)} rows)", flush=True)
        except Exception as _e:
            print(f"[WARN] AUDIT_DAILY_EQUITY_DIR write failed: {_e}", flush=True)

    # Rolling avg max-DD bands: 30-day rolling window of min DD
    win = 30
    rolling_min_dd = np.full_like(dd_pct, np.nan)
    for i in range(len(dd_pct)):
        start = max(0, i - win + 1)
        rolling_min_dd[i] = float(np.min(dd_pct[start:i+1]))

    # Distinct drawdown episodes: depth, duration, recovery
    episodes = []
    in_dd = False
    ep_start = ep_peak = ep_depth = 0
    for i in range(len(dd_pct)):
        if dd_pct[i] < -0.001 and not in_dd:
            in_dd = True
            ep_start = i
            ep_depth = dd_pct[i]
        elif dd_pct[i] < -0.001 and in_dd:
            if dd_pct[i] < ep_depth:
                ep_depth = dd_pct[i]
                ep_peak = i
        elif dd_pct[i] >= -0.001 and in_dd:
            episodes.append((ep_depth, ep_peak - ep_start, i - ep_peak))
            in_dd = False
    if in_dd:
        episodes.append((ep_depth, ep_peak - ep_start, len(dd_pct) - ep_peak))

    avg_dd_depth = float(np.mean([e[0] for e in episodes])) if episodes else 0.0
    n_episodes   = len(episodes)

    # Rolling 30d Sharpe
    roll_sharpe = np.full(len(r_plot), np.nan)
    for i in range(win, len(r_plot)):
        seg = r_plot[i-win:i]
        std = float(np.std(seg, ddof=1))
        if std > 1e-9:
            roll_sharpe[i] = float(np.mean(seg) / std * np.sqrt(365))

    # X-axis
    dates_ok = (col_dates is not None
                and len(col_dates) == len(eq)
                and all(d is not None for d in col_dates))
    x = col_dates if dates_ok else np.arange(len(eq))

    # ── Layout: 3 rows ──────────────────────────────────────────────
    fig, axes = plt.subplots(3, 1, figsize=(14, 12),
                             gridspec_kw={"height_ratios": [5, 3, 2],
                                          "hspace": 0.10})
    fig.patch.set_facecolor(FIG_BG)

    def _style(ax):
        ax.set_facecolor(PANEL_BG)
        for sp in ax.spines.values():
            sp.set_color(GRID_COL)
        ax.tick_params(colors=TEXT_COL, labelsize=8)
        ax.grid(alpha=0.18, color=GRID_COL, linewidth=0.6)

    ax_eq, ax_dd, ax_rs = axes
    for ax in axes:
        _style(ax)

    # ── Shade flat periods on all panels ────────────────────────────
    def _shade_flat(ax):
        if not dates_ok or flat_days == 0:
            return
        in_flat = False
        flat_start = None
        for i, (xi, ri) in enumerate(zip(x, r_plot)):
            if daily_with_zeros[i] == 0.0 and not in_flat:
                flat_start = xi; in_flat = True
            elif daily_with_zeros[i] != 0.0 and in_flat:
                ax.axvspan(flat_start, xi, color=FLAT_COL, alpha=0.35, lw=0)
                in_flat = False
        if in_flat:
            ax.axvspan(flat_start, x[-1], color=FLAT_COL, alpha=0.35, lw=0)

    for ax in axes:
        _shade_flat(ax)

    # ── Panel 1: Equity + ATH + rolling DD band ──────────────────────
    # Band: fill between equity and a shifted equity representing avg DD envelope
    band_lower = ath * (1 + rolling_min_dd / 100)   # ATH shifted by rolling min DD
    ax_eq.fill_between(x, band_lower, ath,
                       alpha=0.10, color=BAND_COL, label=f"Avg DD envelope (30d rolling)")
    ax_eq.plot(x, ath, color=ATH_COL, lw=1.0, alpha=0.7, linestyle="--", label="All-Time High")
    ax_eq.plot(x, eq,  color=LINE_COL, lw=1.8, label="Equity curve")

    # Annotate final value
    ax_eq.annotate(f"${eq[-1]:,.0f}",
                   xy=(x[-1], eq[-1]),
                   xytext=(-70, 12), textcoords="offset points",
                   color=LINE_COL, fontsize=10, fontweight="bold",
                   arrowprops=dict(arrowstyle="->", color=LINE_COL, lw=1.2))

    ax_eq.set_ylabel("Portfolio Value ($)", color=TEXT_COL, fontsize=9)
    ax_eq.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda v, _: f"${v/1e6:.2f}M" if v >= 1e6 else f"${v:,.0f}"))
    ax_eq.tick_params(axis="y", colors=TEXT_COL)
    ax_eq.tick_params(axis="x", labelbottom=False)

    safe_label = label.replace(" ", "_").replace("+", "_")
    ax_eq.set_title(
        f"BestSharpe_{safe_label}  |  CAGR={cagr:.0f}%  Sharpe={sharpe:.3f}"
        f"  MaxDD={maxdd:.1f}%  Flat={flat_days}d  "
        f"(shaded = sit-out periods)",
        color=TEXT_COL, fontsize=11, pad=8, fontweight="bold")

    legend_patches = [
        mpatches.Patch(color=LINE_COL, label="Equity curve"),
        mpatches.Patch(color=ATH_COL, alpha=0.7, label="All-Time High"),
        mpatches.Patch(color=BAND_COL, alpha=0.4, label="Rolling 30d DD envelope"),
        mpatches.Patch(color=FLAT_COL, alpha=0.6, label=f"Filtered flat ({flat_days}d)"),
    ]
    ax_eq.legend(handles=legend_patches, fontsize=8,
                 facecolor=PANEL_BG, labelcolor=TEXT_COL, loc="upper left")

    # ── Panel 2: Running drawdown from ATH ──────────────────────────
    ax_dd.fill_between(x, dd_pct, 0, alpha=0.45, color=DD_COL, label="Drawdown from ATH")
    ax_dd.plot(x, dd_pct, color=DD_COL, lw=0.9, alpha=0.85)
    ax_dd.plot(x, rolling_min_dd, color=BAND_COL, lw=1.2, alpha=0.8,
               linestyle="--", label="30d rolling min DD")
    # Horizontal reference lines
    for lvl, alpha in [(-10, 0.5), (-20, 0.4), (-33, 0.35)]:
        ax_dd.axhline(lvl, color=DD_COL, lw=0.7, alpha=alpha, linestyle=":")
        ax_dd.text(x[0] if not dates_ok else x[1], lvl + 0.5,
                   f"{lvl}%", color=DD_COL, fontsize=7, alpha=alpha+0.1)
    # Annotate worst DD
    worst_idx = int(np.argmin(dd_pct))
    ax_dd.annotate(f"{dd_pct[worst_idx]:.1f}%",
                   xy=(x[worst_idx], dd_pct[worst_idx]),
                   xytext=(0, -16), textcoords="offset points",
                   color=DD_COL, fontsize=9, fontweight="bold", ha="center",
                   arrowprops=dict(arrowstyle="->", color=DD_COL, lw=1.0))
    # Avg DD line
    ax_dd.axhline(avg_dd_depth, color=BAND_COL, lw=1.0, alpha=0.7, linestyle="-.")
    ax_dd.text(x[-1] if not dates_ok else x[int(len(x)*0.02)],
               avg_dd_depth - 1.0,
               f"Avg DD {avg_dd_depth:.1f}%  ({n_episodes} episodes)",
               color=BAND_COL, fontsize=7.5, ha="left", alpha=0.85)

    ax_dd.set_ylabel("Drawdown %", color=TEXT_COL, fontsize=9)
    ax_dd.tick_params(axis="y", colors=TEXT_COL)
    ax_dd.tick_params(axis="x", labelbottom=False)
    ax_dd.set_ylim(min(dd_pct.min() * 1.15, -5), 2)
    ax_dd.legend(fontsize=8, facecolor=PANEL_BG, labelcolor=TEXT_COL, loc="lower left")

    # ── Panel 3: Rolling 30d Sharpe ─────────────────────────────────
    # Colour-code: green > 2, amber 0–2, red < 0
    pos_mask  = roll_sharpe >= 2.0
    mid_mask  = (roll_sharpe >= 0) & (roll_sharpe < 2.0)
    neg_mask  = roll_sharpe < 0

    def _bar_strip(ax, mask, color):
        xs = np.array(x, dtype=object)
        ys = np.where(mask, roll_sharpe, np.nan)
        if not dates_ok:
            ax.bar(xs[mask], ys[mask], color=color, alpha=0.75, width=1.0)
        else:
            ax.fill_between(xs, 0, np.where(mask, ys, 0),
                            alpha=0.70, color=color, step="pre")

    _bar_strip(ax_rs, pos_mask, ATH_COL)
    _bar_strip(ax_rs, mid_mask, BAND_COL)
    _bar_strip(ax_rs, neg_mask, DD_COL)
    ax_rs.axhline(0, color=GRID_COL, lw=0.8)
    ax_rs.axhline(2, color=ATH_COL,  lw=0.6, alpha=0.5, linestyle=":")
    ax_rs.set_ylabel("30d Sharpe", color=TEXT_COL, fontsize=9)
    ax_rs.tick_params(axis="y", colors=TEXT_COL)

    if dates_ok:
        for ax in [ax_rs]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            plt.setp(ax.xaxis.get_majorticklabels(),
                     rotation=30, ha="right", color=TEXT_COL, fontsize=8)
        ax_rs.set_xlabel("Date", color=TEXT_COL, fontsize=10)
    else:
        ax_rs.set_xlabel("Calendar Day", color=TEXT_COL, fontsize=10)

    plt.tight_layout(rect=[0, 0, 1, 1])

    # ── Save ────────────────────────────────────────────────────────
    outdir_path = Path(outdir)
    all_pngs = list(outdir_path.rglob("*.png"))
    print(f"    [equity overwrite] found {len(all_pngs)} PNG(s) in {outdir_path.name}: "
          f"{[p.name for p in all_pngs]}")

    # Overwrite ALL existing equity PNGs (including any old corrected copies)
    equity_pngs = [p for p in all_pngs if "equity" in p.name.lower()]
    overwritten = []
    for existing in equity_pngs:
        plt.savefig(str(existing), dpi=150, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        overwritten.append(existing.name)

    # Save canonical equity_curve.png regardless of what existed before
    canonical_path = outdir_path / f"{safe_label}_equity_curve.png"
    plt.savefig(str(canonical_path), dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())

    # Save dedicated drawdown analysis copy
    dd_path = outdir_path / f"{safe_label}_drawdown_analysis.png"
    plt.savefig(str(dd_path), dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())

    plt.close()

    if overwritten:
        print(f"    ✅ Equity chart overwritten: {overwritten}")
    print(f"    ✅ Canonical equity chart saved: {canonical_path.name}")
    print(f"    ✅ Drawdown analysis chart saved: {dd_path.name}")

    # Save the full performance dashboard alongside the equity chart
    save_performance_dashboard(
        outdir                    = outdir_path,
        label                     = label,
        daily_with_zeros          = daily_with_zeros,
        col_dates                 = col_dates,
        starting_capital          = starting_capital,
        flat_days                 = flat_days,
        cagr                      = cagr,
        sharpe                    = sharpe,
        maxdd                     = maxdd,
        perf_lev_scalar_history   = perf_lev_scalar_history,
        vol_lev_scalar_history    = vol_lev_scalar_history,
        contra_lev_scalar_history = contra_lev_scalar_history,
    )


def save_performance_dashboard(
    outdir,
    label,
    daily_with_zeros,
    col_dates,
    starting_capital,
    flat_days,
    cagr,
    sharpe,
    maxdd,
    window: int = 30,
    perf_lev_scalar_history: Optional[list] = None,
    vol_lev_scalar_history:   Optional[list] = None,
    contra_lev_scalar_history: Optional[list] = None,
):
    """
    Save a multi-panel performance dashboard PNG:

      Panel 0 (top, tall) — Equity curve, flat periods shaded
      Panel 1             — Rolling Sharpe      (annualised, window-day)
      Panel 2             — Rolling CAGR %      (annualised, window-day)
      Panel 3             — Rolling Calmar      (CAGR / |MaxDD|, window-day)
      Panel 4             — Rolling Sortino     (annualised, window-day)
      Panel 5             — Rolling Avg Daily Return %  (window-day)
      Panel 6 (optional)  — Perf-lev boost scalar  [1, max_boost]
                            Only included when perf_lev_scalar_history is supplied.
      Panel 7 (optional)  — Vol-target leverage  [lev_min, lev_max]
                            Only included when vol_lev_scalar_history is supplied.

    Saved as ``<label>_performance_dashboard.png`` inside ``outdir``.
    """



    # ── Colour palette ────────────────────────────────────────────────
    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"
    GRID_COL = "#21262d"
    FLAT_COL = "#30363d"
    EQ_COL   = "#58a6ff"
    SH_COL   = "#3fb950"
    CA_COL   = "#d2a8ff"
    CM_COL   = "#ffa657"
    SO_COL   = "#79c0ff"
    DR_COL   = "#ff7b72"
    SC_COL   = "#f0e68c"   # khaki — leverage scalar
    ZERO_COL = "#6e7681"

    # ── Base arrays ───────────────────────────────────────────────────
    r_plot = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
    eq     = np.cumprod(1 + r_plot) * starting_capital
    n      = len(r_plot)

    dates_ok = (
        col_dates is not None
        and len(col_dates) == n
        and all(d is not None for d in col_dates)
    )
    x = np.array(col_dates) if dates_ok else np.arange(n)

    # ── Rolling metric helpers ────────────────────────────────────────
    ANN = 365

    def _roll(r, w, fn):
        out = np.full(n, np.nan)
        for i in range(w - 1, n):
            out[i] = fn(r[i - w + 1: i + 1])
        return out

    def _sharpe_fn(s):
        mu, sd = s.mean(), s.std(ddof=1)
        return mu / sd * np.sqrt(ANN) if sd > 1e-14 else np.nan

    def _cagr_fn(s):
        return (np.prod(1 + s) ** (ANN / len(s)) - 1) * 100

    def _calmar_fn(s):
        cum = np.cumprod(1 + s)
        md  = ((cum - np.maximum.accumulate(cum)) / np.maximum.accumulate(cum)).min()
        return (_cagr_fn(s) / abs(md * 100)) if abs(md) > 1e-9 else np.nan

    def _sortino_fn(s):
        mu, neg = s.mean(), s[s < 0]
        dsd = neg.std(ddof=1) if len(neg) > 1 else 1e-9
        return mu / dsd * np.sqrt(ANN) if dsd > 1e-14 else np.nan

    def _dr_fn(s):
        return s.mean() * 100

    roll_sh = _roll(r_plot, window, _sharpe_fn)
    roll_ca = _roll(r_plot, window, _cagr_fn)
    roll_cm = _roll(r_plot, window, _calmar_fn)
    roll_so = _roll(r_plot, window, _sortino_fn)
    roll_dr = _roll(r_plot, window, _dr_fn)

    # ── Rolling Sharpe percentile bands (p25 / median / p75) ──────────────────
    # For each day t, collect all finite rolling-Sharpe values in a
    # trailing band_window period and compute three percentiles.
    # The band width reveals volatility of edge over time.
    band_window = min(max(window * 3, 180), n)   # ~3x rolling window
    sh_p25 = np.full(n, np.nan)
    sh_med = np.full(n, np.nan)
    sh_p75 = np.full(n, np.nan)
    for _i in range(band_window - 1, n):
        _slice  = roll_sh[_i - band_window + 1: _i + 1]
        _finite = _slice[np.isfinite(_slice)]
        if len(_finite) >= 5:
            sh_p25[_i] = float(np.percentile(_finite, 25))
            sh_med[_i] = float(np.percentile(_finite, 50))
            sh_p75[_i] = float(np.percentile(_finite, 75))


    has_scalar = (
        perf_lev_scalar_history is not None
        and len(perf_lev_scalar_history) > 0
    )
    if has_scalar:
        scalar_dates  = [pd.Timestamp(d) for d, _ in perf_lev_scalar_history]
        scalar_vals   = [s for _, s in perf_lev_scalar_history]
        scalar_series = pd.Series(scalar_vals, index=pd.DatetimeIndex(scalar_dates))
    else:
        scalar_series = None

    has_vol_scalar = (
        vol_lev_scalar_history is not None
        and len(vol_lev_scalar_history) > 0
    )
    if has_vol_scalar:
        vsc_dates  = [pd.Timestamp(d) for d, _ in vol_lev_scalar_history]
        vsc_vals   = [s for _, s in vol_lev_scalar_history]
        vol_series = pd.Series(vsc_vals, index=pd.DatetimeIndex(vsc_dates))
    else:
        vol_series = None

    has_contra_scalar = (
        contra_lev_scalar_history is not None
        and len(contra_lev_scalar_history) > 0
    )
    if has_contra_scalar:
        csc_dates     = [pd.Timestamp(d) for d, _ in contra_lev_scalar_history]
        csc_vals      = [s for _, s in contra_lev_scalar_history]
        contra_series = pd.Series(csc_vals, index=pd.DatetimeIndex(csc_dates))
    else:
        contra_series = None

    # ── Layout ────────────────────────────────────────────────────────
    n_panels      = 6 + int(has_scalar) + int(has_vol_scalar) + int(has_contra_scalar)
    height_ratios = [3] + [1] * (n_panels - 1)

    fig = plt.figure(figsize=(16, 3 * n_panels))
    fig.patch.set_facecolor(FIG_BG)
    gs  = gridspec.GridSpec(n_panels, 1, height_ratios=height_ratios,
                            hspace=0.08, figure=fig)
    axes = [fig.add_subplot(gs[i]) for i in range(n_panels)]

    def _style(ax, ylabel, color, last=False):
        ax.set_facecolor(PANEL_BG)
        for sp in ax.spines.values():
            sp.set_color(GRID_COL)
        ax.tick_params(colors=TEXT_COL, labelsize=8)
        ax.grid(alpha=0.20, color=GRID_COL, linewidth=0.6)
        ax.set_ylabel(ylabel, color=TEXT_COL, fontsize=8, labelpad=4)
        ax.yaxis.set_label_position("right")
        ax.yaxis.tick_right()
        ax.tick_params(axis="y", colors=TEXT_COL, labelsize=8)
        if not last:
            ax.set_xticklabels([])
            ax.tick_params(axis="x", length=0)
        if dates_ok:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        if last and dates_ok:
            plt.setp(ax.xaxis.get_majorticklabels(),
                     rotation=30, ha="right", color=TEXT_COL, fontsize=8)

    # ── Panel 0: Equity curve ─────────────────────────────────────────
    ax0 = axes[0]
    _style(ax0, "Portfolio Value ($)", EQ_COL)

    if dates_ok and flat_days > 0:
        in_flat = False
        flat_start = None
        for i in range(n):
            if daily_with_zeros[i] == 0.0 and not in_flat:
                flat_start, in_flat = x[i], True
            elif daily_with_zeros[i] != 0.0 and in_flat:
                ax0.axvspan(flat_start, x[i], color=FLAT_COL, alpha=0.45, lw=0)
                in_flat = False
        if in_flat:
            ax0.axvspan(flat_start, x[-1], color=FLAT_COL, alpha=0.45, lw=0)

    ax0.plot(x, eq, color=EQ_COL, lw=1.8)
    ax0.yaxis.set_major_formatter(plt.FuncFormatter(
        lambda v, _: f"${v/1e6:.2f}M" if v >= 1e6 else f"${v:,.0f}"))

    safe_label = label.replace(" ", "_").replace("+", "_")
    scalar_tag  = "  +PerfLev" if has_scalar else ""
    ax0.set_title(
        f"{safe_label}{scalar_tag}  |  CAGR={cagr:.0f}%  Sharpe={sharpe:.3f}"
        f"  MaxDD={maxdd:.1f}%  Flat={flat_days}d  —  "
        f"Performance Dashboard  ({window}d rolling)",
        color=TEXT_COL, fontsize=11, pad=8, fontweight="bold",
    )
    ax0.annotate(
        f"${eq[-1]:,.0f}",
        xy=(x[-1], eq[-1]), xytext=(-70, 12), textcoords="offset points",
        color=EQ_COL, fontsize=9, fontweight="bold",
        arrowprops=dict(arrowstyle="->", color=EQ_COL, lw=1.1),
    )
    legend_patches = [
        mpatches.Patch(color=EQ_COL,  label="Equity curve"),
        mpatches.Patch(color=FLAT_COL, alpha=0.6,
                       label=f"Filtered flat ({flat_days}d)"),
    ]
    ax0.legend(handles=legend_patches, fontsize=8,
               facecolor=PANEL_BG, labelcolor=TEXT_COL, loc="upper left")

    # ── Panels 1-5: rolling metrics ───────────────────────────────────
    # clip_outliers=True → set ylim to [mean-2σ, mean+2σ] of finite values,
    # so extreme spikes don't flatten the readable signal to a thin line.
    metric_panels = [
        (roll_sh, f"Sharpe ({window}d)",        SH_COL, [(0, ZERO_COL), (2, SH_COL)], False),
        (roll_ca, f"CAGR % ({window}d)",         CA_COL, [(0, ZERO_COL)],              True),
        (roll_cm, f"Calmar ({window}d)",          CM_COL, [(0, ZERO_COL), (3, CM_COL)], True),
        (roll_so, f"Sortino ({window}d)",         SO_COL, [(0, ZERO_COL), (3, SO_COL)], True),
        (roll_dr, f"Avg Daily Ret % ({window}d)", DR_COL, [(0, ZERO_COL)],              False),
    ]
    for idx, (data, ylabel, color, hlines, clip_outliers) in enumerate(metric_panels):
        ax = axes[idx + 1]
        is_last = (idx + 1 == n_panels - 1) and not has_scalar and not has_vol_scalar and not has_contra_scalar
        _style(ax, ylabel, color, last=is_last)

        # ── Rolling Sharpe panel: draw percentile bands first ─────────
        if idx == 0:
            BAND_MED = "#3fb950"   # same green as the Sharpe line
            BAND_IQR = "#238636"   # darker green for IQR fill
            has_bands = np.any(np.isfinite(sh_p25)) and np.any(np.isfinite(sh_p75))
            if has_bands:
                # p25–p75 shaded band
                ax.fill_between(x, sh_p25, sh_p75,
                                where=(np.isfinite(sh_p25) & np.isfinite(sh_p75)),
                                color=BAND_IQR, alpha=0.30, lw=0, zorder=1,
                                label="p25–p75")
                # Median line (slightly different weight/style to distinguish from point estimate)
                ax.plot(x, sh_med, color=BAND_MED, lw=1.0, ls="--",
                        alpha=0.75, zorder=2, label="Median")
                ax.plot(x, sh_p25, color=BAND_IQR, lw=0.6, ls=":",
                        alpha=0.60, zorder=2)
                ax.plot(x, sh_p75, color=BAND_IQR, lw=0.6, ls=":",
                        alpha=0.60, zorder=2)

        ax.plot(x, data, color=color, lw=1.3, zorder=3)

        # Rolling Sharpe legend
        if idx == 0 and has_bands:
            import matplotlib.patches as _mp
            _leg_handles = [
                plt.Line2D([0], [0], color=color,    lw=1.3,            label=f"Rolling {window}d"),
                plt.Line2D([0], [0], color=BAND_MED, lw=1.0, ls="--",  label="Median"),
                _mp.Patch(             color=BAND_IQR, alpha=0.40,       label="p25–p75"),
            ]
            ax.legend(handles=_leg_handles, fontsize=7.5,
                      facecolor=PANEL_BG, edgecolor=GRID_COL, labelcolor=TEXT_COL,
                      loc="upper left", framealpha=0.85)

        for hval, hcol in hlines:
            ls = "--" if hval == 0 else ":"
            ax.axhline(hval, color=hcol, lw=0.8 if hval == 0 else 0.6,
                       ls=ls, alpha=1.0 if hval == 0 else 0.5)
        if "%" in ylabel:
            ax.yaxis.set_major_formatter(
                plt.FuncFormatter(lambda v, _: f"{v:.1f}%"))
        if clip_outliers:
            finite = data[np.isfinite(data)]
            if len(finite) > 5:
                lo = float(np.percentile(finite, 5))
                hi = float(np.percentile(finite, 90))
                pad = (hi - lo) * 0.08
                ax.set_ylim(lo - pad, hi + pad)

    # ── Panel 6 (optional): leverage scalar ───────────────────────────
    if has_scalar:
        ax_sc = axes[6]
        _sc_is_last = not has_vol_scalar and not has_contra_scalar
        _style(ax_sc, "Lev Boost", SC_COL, last=_sc_is_last)
        if dates_ok:
            ax_sc.plot(scalar_series.index, scalar_series.values,
                       color=SC_COL, lw=1.3)
        else:
            ax_sc.plot(scalar_series.values, color=SC_COL, lw=1.3)
        ax_sc.axhline(1.0, color=SC_COL,   lw=0.6, ls=":",  alpha=0.5)
        ax_sc.axhline(1.0, color=ZERO_COL, lw=0.8, ls="--")  # floor = static leverage
        # Shade the boost region between 1.0 and the scalar
        if dates_ok and len(scalar_series) > 0:
            sc_x = np.array(scalar_series.index)
            sc_y = np.array(scalar_series.values)
            ax_sc.fill_between(sc_x, 1.0, sc_y,
                               where=(sc_y > 1.0),
                               color=SC_COL, alpha=0.25, lw=0)
        sc_max = float(scalar_series.max()) if len(scalar_series) else 2.0
        ax_sc.set_ylim(0.9, sc_max * 1.05)
        ax_sc.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: f"{v:.2f}"))
        if dates_ok:
            ax_sc.set_xlabel("Date", color=TEXT_COL, fontsize=9)
        ax_sc.tick_params(axis="x", colors=TEXT_COL, labelsize=8)

        # Annotate mean scalar
        mean_sc = float(np.mean(list(scalar_series.values)))
        ax_sc.axhline(mean_sc, color=SC_COL, lw=0.9, ls="-", alpha=0.6)
        ax_sc.annotate(
            f"mean={mean_sc:.2f}",
            xy=(scalar_series.index[-1], mean_sc),
            xytext=(-80, 6), textcoords="offset points",
            color=SC_COL, fontsize=8,
        )
    else:
        if not has_vol_scalar and not has_contra_scalar:
            axes[-1].set_xlabel(
                "Date" if dates_ok else "Calendar Day",
                color=TEXT_COL, fontsize=9)
            axes[-1].tick_params(axis="x", colors=TEXT_COL, labelsize=8)

    # ── Panel 7 (optional): vol-target leverage ───────────────────────
    VL_COL = "#56d364"   # bright green — distinct from SC_COL khaki
    if has_vol_scalar:
        ax_vl = axes[6 + int(has_scalar)]
        _style(ax_vl, "Vol-Tgt Lev", VL_COL, last=True)
        if dates_ok:
            ax_vl.plot(vol_series.index, vol_series.values,
                       color=VL_COL, lw=1.3)
        else:
            ax_vl.plot(vol_series.values, color=VL_COL, lw=1.3)
        # Reference lines: min/max clip bounds and mean
        mean_vl = float(np.mean(list(vol_series.values)))
        vl_min  = float(vol_series.min())
        vl_max  = float(vol_series.max())
        ax_vl.axhline(mean_vl, color=VL_COL, lw=0.9, ls="-",  alpha=0.6)
        ax_vl.axhline(vl_min,  color=ZERO_COL, lw=0.6, ls=":", alpha=0.5)
        ax_vl.axhline(vl_max,  color=ZERO_COL, lw=0.6, ls=":", alpha=0.5)
        # Shade: fill between mean and current level
        if dates_ok:
            sc_x = np.array(vol_series.index)
            sc_y = np.array(vol_series.values)
            ax_vl.fill_between(sc_x, mean_vl, sc_y,
                               where=(sc_y > mean_vl),
                               color=VL_COL, alpha=0.18, lw=0)
            ax_vl.fill_between(sc_x, mean_vl, sc_y,
                               where=(sc_y < mean_vl),
                               color="#f85149", alpha=0.15, lw=0)
        pad_vl = (vl_max - 1.0) * 0.08
        ax_vl.set_ylim(0.9, vl_max + pad_vl)
        ax_vl.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: f"{v:.2f}x"))
        ax_vl.annotate(
            f"mean={mean_vl:.2f}x",
            xy=(vol_series.index[-1], mean_vl),
            xytext=(-90, 6), textcoords="offset points",
            color=VL_COL, fontsize=8,
        )
        if not has_contra_scalar:
            ax_vl.set_xlabel(
                "Date" if dates_ok else "Calendar Day",
                color=TEXT_COL, fontsize=9)
            ax_vl.tick_params(axis="x", colors=TEXT_COL, labelsize=8)

    # ── Panel (optional): contrarian leverage scalar ───────────────
    CT_COL = "#e8b4fb"   # soft lavender — distinct from green and khaki
    if has_contra_scalar:
        ax_ct = axes[6 + int(has_scalar) + int(has_vol_scalar)]
        _style(ax_ct, "Contra Lev", CT_COL, last=True)
        if dates_ok:
            ax_ct.plot(contra_series.index, contra_series.values,
                       color=CT_COL, lw=1.3)
        else:
            ax_ct.plot(contra_series.values, color=CT_COL, lw=1.3)
        mean_ct = float(np.mean(list(contra_series.values)))
        ct_min  = float(contra_series.min())
        ct_max  = float(contra_series.max())
        ax_ct.axhline(1.0,     color=ZERO_COL, lw=0.8, ls="--")  # static floor
        ax_ct.axhline(mean_ct, color=CT_COL,   lw=0.9, ls="-",  alpha=0.6)
        if dates_ok:
            ct_x = np.array(contra_series.index)
            ct_y = np.array(contra_series.values)
            ax_ct.fill_between(ct_x, 1.0, ct_y,
                               where=(ct_y > 1.0),
                               color=CT_COL, alpha=0.20, lw=0)
        pad_ct = (ct_max - 1.0) * 0.08
        ax_ct.set_ylim(0.9, ct_max + pad_ct)
        ax_ct.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: f"{v:.2f}x"))
        ax_ct.annotate(
            f"mean={mean_ct:.2f}x",
            xy=(contra_series.index[-1], mean_ct),
            xytext=(-90, 6), textcoords="offset points",
            color=CT_COL, fontsize=8,
        )
        ax_ct.set_xlabel(
            "Date" if dates_ok else "Calendar Day",
            color=TEXT_COL, fontsize=9)
        ax_ct.tick_params(axis="x", colors=TEXT_COL, labelsize=8)

    # ── Align x-axes ──────────────────────────────────────────────────
    if dates_ok:
        xlim = (x[0], x[-1])
        for ax in axes:
            ax.set_xlim(xlim)

    plt.tight_layout()

    dashboard_path = Path(outdir) / f"{safe_label}_performance_dashboard.png"
    plt.savefig(str(dashboard_path), dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close()
    print(f"    ✅ Performance dashboard saved: {dashboard_path.name}")
    # ── Signal predictiveness — 30d, 20d, 10d rolling windows ──────────
    _pred_windows = sorted(set([window, 20, 10]), reverse=True)  # dedup, largest first
    for _pred_win in _pred_windows:
        compute_signal_predictiveness(
            outdir           = outdir,
            label            = label,
            daily_with_zeros = daily_with_zeros,
            col_dates        = col_dates,
            window           = _pred_win,
            forward_days     = (1, 3, 5),
        )


def compute_signal_predictiveness(
    outdir,
    label,
    daily_with_zeros,
    col_dates,
    window: int = 30,
    forward_days: tuple = (1, 3, 5),
):
    """
    Compute Pearson and Spearman IC between each rolling metric (level and
    first-difference) and forward N-day returns. Prints a ranked table and
    saves a CSV.

    IC > 0.10 = useful,  IC > 0.20 = strong,  IC > 0.30 = very strong.
    p < 0.05  = statistically significant.
    """
    try:
        from scipy import stats as _stats
    except ImportError:
        print("    [predictiveness] scipy not available — skipped")
        return

    import csv as _csv

    ANN = 365
    r   = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
    n   = len(r)
    if n < 40:
        print("    [predictiveness] too few days — skipped")
        return

    # ── Rolling metric helpers ────────────────────────────────────────
    def _roll(arr, w, fn):
        out = np.full(n, np.nan)
        for i in range(w - 1, n):
            out[i] = fn(arr[i - w + 1: i + 1])
        return out

    def _sharpe_fn(s):
        mu, sd = s.mean(), s.std(ddof=1)
        return mu / sd * np.sqrt(ANN) if sd > 1e-14 else np.nan

    def _cagr_fn(s):
        return (np.prod(1 + s) ** (ANN / len(s)) - 1) * 100

    def _calmar_fn(s):
        cum = np.cumprod(1 + s)
        pk  = np.maximum.accumulate(cum)
        md  = ((cum - pk) / pk).min()
        cagr = _cagr_fn(s)
        return cagr / abs(md * 100) if abs(md) > 1e-9 else np.nan

    def _sortino_fn(s):
        mu, neg = s.mean(), s[s < 0]
        dsd = neg.std(ddof=1) if len(neg) > 1 else 1e-9
        return mu / dsd * np.sqrt(ANN) if dsd > 1e-14 else np.nan

    def _dr_fn(s):
        return s.mean() * 100

    signals = {
        "Sharpe":       _roll(r, window, _sharpe_fn),
        "CAGR%":        _roll(r, window, _cagr_fn),
        "Calmar":       _roll(r, window, _calmar_fn),
        "Sortino":      _roll(r, window, _sortino_fn),
        "AvgDailyRet%": _roll(r, window, _dr_fn),
    }

    # ── Forward return targets (sum of next h days, in %) ─────────────
    fwd = {}
    for h in forward_days:
        arr = np.full(n, np.nan)
        for i in range(n - h):
            arr[i] = np.sum(r[i + 1: i + 1 + h]) * 100
        fwd[h] = arr

    # ── IC computation ────────────────────────────────────────────────
    rows = []
    for sig_name, sig_arr in signals.items():
        # First difference of the signal
        diff_arr = np.full(n, np.nan)
        vi = np.where(np.isfinite(sig_arr))[0]
        if len(vi) > 1:
            diff_arr[vi[1:]] = np.diff(sig_arr[vi])

        for h in forward_days:
            for arr, kind in [(sig_arr, "level"), (diff_arr, "delta")]:
                mask = np.isfinite(arr) & np.isfinite(fwd[h])
                if mask.sum() < 20:
                    continue
                x, y = arr[mask], fwd[h][mask]
                pr, pp = _stats.pearsonr(x, y)
                sr, sp = _stats.spearmanr(x, y)
                rows.append({
                    "Signal":      sig_name,
                    "Kind":        kind,
                    "Fwd Days":    h,
                    "Pearson r":   round(float(pr), 4),
                    "Pearson p":   round(float(pp), 4),
                    "Spearman IC": round(float(sr), 4),
                    "Spearman p":  round(float(sp), 4),
                    "N":           int(mask.sum()),
                    "Sig*":        "yes" if float(sp) < 0.05 else "",
                })

    if not rows:
        print("    [predictiveness] insufficient data — skipped")
        return

    rows_sorted = sorted(rows, key=lambda x: (x["Fwd Days"], -abs(x["Spearman IC"])))

    # ── Print ranked table ────────────────────────────────────────────
    W   = 84
    SEP = "  " + "-" * W
    print()
    print("  " + "=" * W)
    print(f"  SIGNAL PREDICTIVENESS  |  {label}  |  {window}d rolling window")
    print(f"  Forward horizons: {list(forward_days)} days  |  Spearman rank-IC vs fwd returns")
    print(f"  IC guide:  >0.10 useful   >0.20 strong   >0.30 very strong   * = p<0.05")
    print("  " + "=" * W)
    print(f"  {'Signal':<16} {'Kind':<7} {'Fwd':>4}  {'Pearson r':>10}  "
          f"{'Spearman IC':>12}  {'p-val':>8}  {'N':>5}  {'*':>3}  Bar")
    print(SEP)
    cur_h = None
    for row in rows_sorted:
        if row["Fwd Days"] != cur_h:
            if cur_h is not None:
                print(SEP)
            cur_h = row["Fwd Days"]
            print(f"  -- Forward {cur_h}d " + "-" * (W - 14))
        ic   = row["Spearman IC"]
        bar_n = min(int(abs(ic) * 25), 12)
        bar  = ("+" * bar_n) if ic > 0 else ("-" * bar_n)
        star = "*" if row["Sig*"] == "yes" else ""
        print(f"  {row['Signal']:<16} {row['Kind']:<7} {row['Fwd Days']:>4}  "
              f"{row['Pearson r']:>+10.4f}  {ic:>+12.4f}  "
              f"{row['Spearman p']:>8.4f}  {row['N']:>5}  {star:>3}  {bar}")
    print(SEP)

    # ── Top signals summary ───────────────────────────────────────────
    sig_rows = sorted(
        [r for r in rows if r["Sig*"] == "yes"],
        key=lambda x: -abs(x["Spearman IC"])
    )
    if sig_rows:
        print()
        print("  Top predictive signals (significant only, ranked by |IC|):")
        for i, row in enumerate(sig_rows[:6]):
            direction = "bullish" if row["Spearman IC"] > 0 else "contrarian"
            print(f"    #{i+1}  {row['Signal']} ({row['Kind']}) -> fwd{row['Fwd Days']}d  "
                  f"IC={row['Spearman IC']:+.4f}  [{direction}]")
    print()

    # ── Save CSV ──────────────────────────────────────────────────────
    safe_label = label.replace(" ", "_").replace("+", "_")
    csv_path   = Path(outdir) / f"{safe_label}_signal_predictiveness_{window}d.csv"
    fieldnames = ["Signal", "Kind", "Fwd Days", "Pearson r", "Pearson p",
                  "Spearman IC", "Spearman p", "N", "Sig*"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = _csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_sorted)
    print(f"    [predictiveness] CSV saved: {csv_path.name}")

# ══════════════════════════════════════════════════════════════════════
# PARAMETER SURFACE SWEEP HELPERS
# ══════════════════════════════════════════════════════════════════════

def _sweep_sharpe(returns: np.ndarray) -> float:
    r = np.asarray(returns, dtype=float)
    if len(r) < 5 or r.std() == 0:
        return np.nan
    return float(r.mean() / r.std() * np.sqrt(365))

def _sweep_maxdd(returns: np.ndarray) -> float:
    cum  = np.cumprod(1 + np.asarray(returns, dtype=float))
    peak = np.maximum.accumulate(cum)
    return float(((cum - peak) / peak).min()) * 100

def _sweep_cagr(returns: np.ndarray, n_days: int) -> float:
    cum = np.prod(1 + np.asarray(returns, dtype=float))
    return float(cum ** (365 / max(n_days, 1)) - 1) * 100

def _sweep_wf_cv(fold_sharpes) -> float:
    s = [x for x in fold_sharpes if not np.isnan(x)]
    if len(s) < 2:
        return np.nan
    return float(np.std(s) / abs(np.mean(s))) if abs(np.mean(s)) > 1e-9 else np.nan

def _sweep_folds(daily: np.ndarray,
                 train_win: int = 120,
                 test_win:  int = 30,
                 step:      int = 30) -> list:
    n = len(daily)
    sharpes = []
    pos = 0
    while pos + train_win + test_win <= n:
        test_ret = daily[pos + train_win: pos + train_win + test_win]
        sharpes.append(_sweep_sharpe(test_ret))
        pos += step
    return sharpes


def run_tail_guardrail_sweep(df_4x, base_params, btc_ohlcv, out_dir: Path):
    """
    Grid search over TAIL_DROP_PCT × TAIL_VOL_MULT.
    Runs simulate() once per cell using only the Tail Guardrail filter.
    Prints a compact 2-D table (rows=drop_pct, cols=vol_mult) for each
    metric, then saves a CSV. Uses already-fetched btc_ohlcv — no
    extra network calls required.
    """


    drop_values = TAIL_SWEEP_DROP_VALUES
    vol_values  = TAIL_SWEEP_VOL_VALUES
    n_total     = len(df_4x.columns)

    baseline_drop = TAIL_DROP_PCT
    baseline_vol  = TAIL_VOL_MULT

    print("\n" + "═" * 80)
    print("  TAIL GUARDRAIL GRID SWEEP  —  TAIL_DROP_PCT × TAIL_VOL_MULT")
    print(f"  Baseline: drop={baseline_drop*100:.0f}%  vol={baseline_vol:.1f}×")
    print(f"  Grid: {len(drop_values)} drop values × {len(vol_values)} vol values "
          f"= {len(drop_values)*len(vol_values)} cells")
    print("═" * 80)

    rows = []
    for drop in drop_values:
        for vol in vol_values:
            try:
                tail_f = build_tail_guardrail(btc_ohlcv, drop_pct=drop, vol_mult=vol)
            except Exception as e:
                print(f"  ! build_tail_guardrail failed (drop={drop}, vol={vol}): {e}")
                continue

            daily = simulate(df_4x, base_params, "tail", tail_f, verbose=False)["daily"]

            sh    = _sweep_sharpe(daily)
            md    = _sweep_maxdd(daily)
            ca    = _sweep_cagr(daily, n_total)
            flat  = int(np.sum(daily == 0.0))
            act   = n_total - flat

            fold_sh   = _sweep_folds(daily)
            cv        = _sweep_wf_cv(fold_sh)
            valid     = [x for x in fold_sh if not np.isnan(x)]
            mean_oos  = float(np.mean(valid)) if valid else np.nan
            pct_pos   = sum(1 for x in valid if x > 0) / len(valid) * 100 if valid else np.nan
            unstable  = sum(1 for x in valid if x < 0)

            is_baseline = (abs(drop - baseline_drop) < 1e-9 and
                           abs(vol  - baseline_vol)  < 1e-9)
            rows.append({
                "drop_pct":   drop,
                "vol_mult":   vol,
                "Sharpe":     round(sh,  3),
                "CAGR%":      round(ca,  1),
                "MaxDD%":     round(md,  2),
                "Active":     act,
                "Flat":       flat,
                "WF_CV":      round(cv,  3) if not np.isnan(cv)       else np.nan,
                "WF_MeanOOS": round(mean_oos, 3) if not np.isnan(mean_oos) else np.nan,
                "WF_PctPos%": round(pct_pos, 1) if not np.isnan(pct_pos)  else np.nan,
                "WF_Unstable":unstable,
                "baseline":   is_baseline,
            })
            marker = " ◄ BASELINE" if is_baseline else ""
            print(f"  drop={drop*100:.0f}%  vol={vol:.1f}x  │  "
                  f"Sharpe={sh:>6.3f}  CAGR={ca:>7.1f}%  MaxDD={md:>7.2f}%  "
                  f"Active={act:>3d}  WF_CV={cv:>6.3f}{marker}")

    # ── 2-D summary tables ─────────────────────────────────────────────
    for metric in ("Sharpe", "CAGR%", "MaxDD%", "WF_CV", "Active"):
        print(f"\n  ── {metric} ──")
        hdr = f"  {'drop/vol':>12}" + "".join(f"  {v:.1f}x" .rjust(9) for v in vol_values)
        print(hdr)
        print("  " + "─" * (len(hdr) - 2))
        for drop in drop_values:
            row_cells = []
            for vol in vol_values:
                match = [r for r in rows if abs(r["drop_pct"]-drop)<1e-9
                                         and abs(r["vol_mult"]-vol)<1e-9]
                val = match[0][metric] if match else float("nan")
                is_bl = match[0]["baseline"] if match else False
                cell = f"{val:>8.3f}" if isinstance(val, float) else f"{val:>8}"
                cell += ("*" if is_bl else " ")
                row_cells.append(cell)
            print(f"  {drop*100:.0f}%{' ':>10}" + "  ".join(row_cells))
        print(f"  (* = baseline: drop={baseline_drop*100:.0f}%  vol={baseline_vol:.1f}x)")

    # ── CSV ────────────────────────────────────────────────────────────
    if rows:
        df_out = pd.DataFrame(rows).drop(columns=["baseline"])
        csv_path = out_dir / "tail_guardrail_sweep.csv"
        df_out.to_csv(csv_path, index=False)
        print(f"\n  Saved: {csv_path}")
    print("═" * 80 + "\n")


def run_l_high_sweep(df_4x, base_params, combo_filter, out_dir: Path):
    """L_HIGH surface sweep - Tail + Dispersion filter."""



    L_HIGH_VALUES = [round(v, 2) for v in np.arange(0.8, 3.01, 0.1)]
    n_total = len(df_4x.columns)
    BASELINE_L = float(base_params["L_HIGH"])

    # Build vol_lev_params template once — max_boost will be overridden per cell
    _sweep_vlev_base = None
    if ENABLE_VOL_LEV_SCALING:
        _sweep_vlev_base = dict(
            target_vol   = VOL_LEV_TARGET_VOL,
            window       = VOL_LEV_WINDOW,
            sharpe_ref   = VOL_LEV_SHARPE_REF,
            dd_threshold = VOL_LEV_DD_THRESHOLD,
            dd_scale     = VOL_LEV_DD_SCALE,
            max_boost    = VOL_LEV_MAX_BOOST,  # overridden per l_high below
        )

    print("\n" + "═"*70)
    print("  PARAMETER SWEEP - L_HIGH SURFACE  (Tail + Dispersion filter)")
    print("═"*70)

    rows = []
    for l_high in L_HIGH_VALUES:
        params = deepcopy(base_params)
        params["L_HIGH"] = l_high

        # Scale vol_lev max_boost proportionally with l_high so the vol model
        # ceiling tracks the leverage being swept — otherwise vol_lev overrides
        # L_HIGH and Sharpe is invariant across the sweep.
        _vlev = None
        if _sweep_vlev_base is not None:
            _vlev = dict(_sweep_vlev_base)
            _vlev["max_boost"] = VOL_LEV_MAX_BOOST * (l_high / max(BASELINE_L, 1e-9))

        daily = simulate(df_4x, params, "tail_disp", combo_filter,
                         vol_lev_params=_vlev, verbose=False)["daily"]

        sh   = _sweep_sharpe(daily)
        md   = _sweep_maxdd(daily)
        ca   = _sweep_cagr(daily, n_total)
        flat = int(np.sum(daily == 0.0))

        # IS/OOS split on active days
        active_idx = np.where(daily != 0.0)[0]
        if len(active_idx) >= 10:
            sp   = active_idx[min(181, len(active_idx)-1)]
            sh_is  = _sweep_sharpe(daily[:sp+1])
            sh_oos = _sweep_sharpe(daily[sp+1:])
            decay  = (sh_is - sh_oos) / abs(sh_is) * 100 if sh_is > 0 else np.nan
        else:
            sh_is = sh_oos = decay = np.nan

        fold_sh = _sweep_folds(daily)
        f5  = fold_sh[4] if len(fold_sh) > 4 else np.nan
        f8  = fold_sh[7] if len(fold_sh) > 7 else np.nan
        cv  = _sweep_wf_cv(fold_sh)
        valid = [x for x in fold_sh if not np.isnan(x)]
        mean_oos  = float(np.mean(valid)) if valid else np.nan
        pct_pos   = sum(1 for x in valid if x > 0) / len(valid) * 100 if valid else np.nan
        unstable  = sum(1 for x in valid if x < 0)

        rows.append({"L_HIGH": l_high, "Sharpe": round(sh,3), "CAGR%": round(ca,1),
                     "MaxDD%": round(md,2), "Flat_days": flat,
                     "IS_Sharpe": round(sh_is,3) if not np.isnan(sh_is) else np.nan,
                     "OOS_Sharpe": round(sh_oos,3) if not np.isnan(sh_oos) else np.nan,
                     "Decay%": round(decay,1) if not np.isnan(decay) else np.nan,
                     "WF_CV": round(cv,3) if not np.isnan(cv) else np.nan,
                     "WF_MeanOOS": round(mean_oos,3) if not np.isnan(mean_oos) else np.nan,
                     "WF_PctPos%": round(pct_pos,1) if not np.isnan(pct_pos) else np.nan,
                     "WF_Unstable": unstable,
                     "Fold5_Sharpe": round(f5,3) if not np.isnan(f5) else np.nan,
                     "Fold8_Sharpe": round(f8,3) if not np.isnan(f8) else np.nan})
        marker = " ◄" if abs(l_high - BASELINE_L) < 0.01 else ""
        print(f"  L_HIGH={l_high:.1f}  Sharpe={sh:.3f}  MaxDD={md:.1f}%  "
              f"WF_CV={cv:.3f}  F5={f5:.3f}  F8={f8:.3f}{marker}")

    df = pd.DataFrame(rows)
    csv_path = out_dir / "l_high_surface.csv"
    df.to_csv(csv_path, index=False)

    # ── Chart ────────────────────────────────────────────────────────
    fig, axes = plt.subplots(3, 2, figsize=(14, 12))
    fig.suptitle("L_HIGH Surface - Tail + Dispersion Filter", fontsize=14, fontweight="bold")
    xs = df["L_HIGH"]

    def _vline(ax):
        ax.axvline(BASELINE_L, color="red", linestyle="--", alpha=0.5,
                   label=f"Current ({BASELINE_L})")

    panels = [
        (axes[0,0], "Sharpe",      "Full-Period Sharpe",      "steelblue",  None),
        (axes[0,1], "MaxDD%",      "Max Drawdown %",          "tomato",     None),
        (axes[1,0], "WF_CV",       "Walk-Forward CV (↓)",     "purple",     None),
        (axes[2,0], "IS_Sharpe",   "IS vs OOS Sharpe",        "steelblue",  None),
        (axes[2,1], "Decay%",      "Sharpe Decay IS->OOS %",   "teal",       None),
    ]
    for ax, col, title, color, _ in panels:
        ax.plot(xs, df[col], marker="o", color=color)
        _vline(ax); ax.set_title(title); ax.set_xlabel("L_HIGH")
        ax.grid(True, alpha=0.3); ax.legend(fontsize=7)

    # OOS overlay on IS panel
    axes[2,0].plot(xs, df["OOS_Sharpe"], marker="s", color="coral", label="OOS Sharpe")
    axes[2,0].legend(fontsize=7)

    # Problem folds panel
    ax = axes[1,1]
    ax.plot(xs, df["Fold5_Sharpe"], marker="o", color="darkorange", label="Fold 5 (Oct-Nov)")
    ax.plot(xs, df["Fold8_Sharpe"], marker="s", color="darkred",    label="Fold 8 (Jan-Feb)")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.axhline(1.0, color="green", linestyle=":", alpha=0.5)
    _vline(ax); ax.set_title("Problem Fold Sharpe"); ax.set_xlabel("L_HIGH")
    ax.legend(fontsize=7); ax.grid(True, alpha=0.3)

    # Reference lines
    axes[0,0].axhline(3.0, color="green", linestyle=":", alpha=0.5, label="3.0 target")
    axes[0,1].axhline(-30, color="orange", linestyle=":", alpha=0.5, label="-30% threshold")
    axes[1,0].axhline(0.25, color="green", linestyle=":", alpha=0.5, label="0.25 target")
    axes[2,1].axhline(40, color="orange", linestyle=":", alpha=0.5, label="40% fail")
    axes[2,1].axhline(30, color="green",  linestyle=":", alpha=0.5, label="30% pass")
    for ax in [axes[0,0], axes[0,1], axes[1,0], axes[2,1]]:
        ax.legend(fontsize=7)

    plt.tight_layout()
    png_path = out_dir / "l_high_surface.png"
    fig.savefig(png_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # ── Ranked table ─────────────────────────────────────────────────
    print("\n" + "═"*95)
    print(f"  L_HIGH SURFACE - RANKED BY SHARPE")
    print("═"*95)
    print(f"  {'L_HIGH':>6}  {'Sharpe':>6}  {'CAGR%':>7}  {'MaxDD%':>7}  "
          f"{'IS_Sh':>6}  {'OOS_Sh':>6}  {'Decay%':>7}  "
          f"{'WF_CV':>6}  {'MeanOOS':>7}  {'F5_Sh':>6}  {'F8_Sh':>6}  {'Unstbl':>6}")
    print("─"*95)
    for _, row in df.sort_values("Sharpe", ascending=False).iterrows():
        mk = " ◄" if abs(row["L_HIGH"] - BASELINE_L) < 0.01 else ""
        print(f"  {row['L_HIGH']:>6.1f}  {row['Sharpe']:>6.3f}  {row['CAGR%']:>7.0f}  "
              f"{row['MaxDD%']:>7.2f}  {row['IS_Sharpe']:>6.3f}  {row['OOS_Sharpe']:>6.3f}  "
              f"{row['Decay%']:>7.1f}  {row['WF_CV']:>6.3f}  {row['WF_MeanOOS']:>7.3f}  "
              f"{row['Fold5_Sharpe']:>6.3f}  {row['Fold8_Sharpe']:>6.3f}  "
              f"{row['WF_Unstable']:>6}{mk}")
    print(f"\n  CSV: {csv_path}\n  Chart: {png_path}")


def run_pph_sweep(
    df_4x:          pd.DataFrame,
    params:         dict,
    filter_mode:    str,
    v3_filter,
    symbol_counts,
    vol_lev_params: Optional[dict],
    filter_label:   str,
    out_dir:        Path,
):
    """
    Grid-search all 48 PPH configurations (3 freq × 4 threshold × 4 fraction).

    For each config, runs a full simulate() call with the PPH active,
    computes Sharpe / MaxDD / Calmar / CAGR / total_harvested, and prints
    a ranked summary table.  Also saves a CSV of all results to out_dir.
    """

    freqs      = PPH_SWEEP_FREQUENCIES    # e.g. ["daily","weekly","monthly"]
    thresholds = PPH_SWEEP_THRESHOLDS     # e.g. [0.10, 0.20, 0.30, 0.40]
    fractions  = PPH_SWEEP_FRACTIONS      # e.g. [0.25, 0.50, 0.75, 1.00]

    configs = list(itertools.product(freqs, thresholds, fractions))
    total   = len(configs)
    print(f"\n  Running {total} PPH configs on filter: {filter_label}")
    print(f"  (Each config runs a full simulate — please wait)\n")

    # ── Baseline: no PPH ──────────────────────────────────────────────
    _base_out  = simulate(df_4x, params, filter_mode, v3_filter,
                          symbol_counts=symbol_counts,
                          vol_lev_params=vol_lev_params,
                          pph=None,
                          verbose=False)
    _base_rets = _base_out["daily"]
    _base_stats = _pph_stats(_base_rets, None, 0.0)

    results = []

    for idx, (freq, thr, frac) in enumerate(configs, 1):
        pph = PeriodicProfitHarvest(frequency=freq, threshold=thr, harvest_frac=frac)
        sim = simulate(df_4x, params, filter_mode, v3_filter,
                       symbol_counts=symbol_counts,
                       vol_lev_params=vol_lev_params,
                       pph=pph,
                       verbose=False)
        rets   = sim["daily"]
        stats  = _pph_stats(rets, pph, STARTING_CAPITAL)
        results.append(dict(
            freq=freq, threshold=thr, frac=frac,
            **stats,
        ))
        if idx % 12 == 0 or idx == total:
            print(f"  ... {idx}/{total} done")

    # Sort by Sharpe descending
    results.sort(key=lambda r: r["sharpe"] if np.isfinite(r["sharpe"]) else -999,
                 reverse=True)

    # ── Print table ───────────────────────────────────────────────────
    _W = 108
    _HDR = (f"  {'#':>3}  {'Freq':<8}  {'Thr':>5}  {'Frac':>5}  "
            f"{'Sharpe':>7}  {'CAGR%':>8}  {'MaxDD%':>8}  "
            f"{'Calmar':>7}  {'Harvests':>9}  {'Banked%':>8}  {'Net Eq%':>8}")
    print()
    print("  " + "─" * _W)
    print("  PPH GRID SEARCH RESULTS  (ranked by Sharpe)  "
          f"Filter: {filter_label}")
    print("  Baseline (no PPH):  "
          f"Sharpe={_base_stats['sharpe']:.3f}  "
          f"CAGR={_base_stats['cagr']:.1f}%  "
          f"MaxDD={_base_stats['maxdd']:.2f}%  "
          f"Calmar={_base_stats['calmar']:.2f}")
    print("  " + "─" * _W)
    print(_HDR)
    print("  " + "─" * _W)

    for rank, r in enumerate(results, 1):
        sharpe_delta = r["sharpe"] - _base_stats["sharpe"]
        flag = " ▲" if sharpe_delta > 0.05 else (" ▼" if sharpe_delta < -0.05 else "  ")
        print(f"  {rank:>3}  {r['freq']:<8}  {r['threshold']*100:>4.0f}%  "
              f"{r['frac']*100:>4.0f}%  "
              f"{r['sharpe']:>7.3f}{flag}  {r['cagr']:>7.1f}%  "
              f"{r['maxdd']:>7.2f}%  {r['calmar']:>7.2f}  "
              f"{r['n_harvests']:>9}  {r['banked_pct']:>7.1f}%  "
              f"{r['net_eq_pct']:>7.1f}%")

    print("  " + "─" * _W)

    # Top-5 highlight
    print(f"\n  Top-5 configs by Sharpe:")
    for rank, r in enumerate(results[:5], 1):
        print(f"    #{rank}  freq={r['freq']:<8}  threshold={r['threshold']*100:.0f}%  "
              f"harvest_frac={r['frac']*100:.0f}%  "
              f"→ Sharpe={r['sharpe']:.3f}  MaxDD={r['maxdd']:.2f}%  "
              f"Calmar={r['calmar']:.2f}  banked={r['banked_pct']:.1f}%")

    # Best MaxDD improvement
    dd_sorted = sorted(results, key=lambda r: r["maxdd"], reverse=True)
    print(f"\n  Best MaxDD improvement:")
    for rank, r in enumerate(dd_sorted[:3], 1):
        print(f"    #{rank}  freq={r['freq']:<8}  threshold={r['threshold']*100:.0f}%  "
              f"harvest_frac={r['frac']*100:.0f}%  "
              f"→ MaxDD={r['maxdd']:.2f}%  (baseline {_base_stats['maxdd']:.2f}%)")

    print()

    # ── Save CSV ──────────────────────────────────────────────────────
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "pph_sweep_results.csv"
        import csv as _csv
        fieldnames = ["rank","freq","threshold","frac","sharpe","cagr","maxdd",
                      "calmar","n_harvests","banked_pct","net_eq_pct"]
        with open(csv_path, "w", newline="") as fh:
            w = _csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            for rank, r in enumerate(results, 1):
                w.writerow(dict(rank=rank, freq=r["freq"],
                                threshold=r["threshold"], frac=r["frac"],
                                sharpe=round(r["sharpe"], 4),
                                cagr=round(r["cagr"], 2),
                                maxdd=round(r["maxdd"], 2),
                                calmar=round(r["calmar"], 2),
                                n_harvests=r["n_harvests"],
                                banked_pct=round(r["banked_pct"], 2),
                                net_eq_pct=round(r["net_eq_pct"], 2)))
        print(f"  PPH sweep CSV saved: {csv_path}")
    except Exception as e:
        print(f"  ⚠  Could not save PPH CSV: {e}")


def _pph_stats(daily: np.ndarray, pph: Optional[PeriodicProfitHarvest],
               starting_capital: float) -> dict:
    """Compute summary stats for a PPH simulate() run."""
    r = daily[np.isfinite(daily)]
    if len(r) == 0:
        return dict(sharpe=float("nan"), cagr=float("nan"), maxdd=float("nan"),
                    calmar=float("nan"), n_harvests=0, banked_pct=0.0, net_eq_pct=0.0)

    TDAYS = 365
    n     = len(r)
    mu    = float(np.mean(r))
    sd    = float(np.std(r, ddof=1)) if n > 1 else 1e-9
    sharpe = mu / sd * TDAYS**0.5 if sd > 0 else 0.0

    eq    = np.cumprod(1 + r)
    cagr  = float((eq[-1] ** (TDAYS / n) - 1) * 100)

    peak  = np.maximum.accumulate(eq)
    dd    = (eq - peak) / peak
    maxdd = float(np.min(dd) * 100)

    calmar = cagr / abs(maxdd) if abs(maxdd) > 1e-9 else 0.0

    n_harvests  = len(pph.harvest_log) if pph else 0
    banked_pct  = (pph.total_harvested / starting_capital * 100
                   if pph and starting_capital > 0 else 0.0)
    # "net equity %" = compounded equity of what's left in the account
    net_eq_pct  = float(eq[-1] - 1.0) * 100

    return dict(sharpe=sharpe, cagr=cagr, maxdd=maxdd, calmar=calmar,
                n_harvests=n_harvests, banked_pct=banked_pct, net_eq_pct=net_eq_pct)


def run_ratchet_sweep(
    df_4x:          pd.DataFrame,
    params:         dict,
    filter_mode:    str,
    v3_filter,
    symbol_counts,
    vol_lev_params: Optional[dict],
    filter_label:   str,
    out_dir:        Path,
):
    """
    Grid-search all 27 Equity Ratchet configurations (3 freq × 3 trigger × 3 lock_pct).

    For each config, runs a full simulate() call with the ratchet active,
    computes Sharpe / MaxDD / Calmar / CAGR / risk-off stats, and prints
    a ranked summary table. Also saves a CSV of all results to out_dir.
    """

    freqs     = RATCHET_SWEEP_FREQUENCIES   # ["daily", "weekly", "monthly"]
    triggers  = RATCHET_SWEEP_TRIGGERS      # [0.10, 0.20, 0.30]
    lock_pcts = RATCHET_SWEEP_LOCK_PCTS     # [0.10, 0.15, 0.20]

    configs = list(itertools.product(freqs, triggers, lock_pcts))
    total   = len(configs)
    print(f"\n  Running {total} Ratchet configs on filter: {filter_label}")
    print(f"  (risk-off mode: lev_scale={RATCHET_RISK_OFF_LEV_SCALE:.2f}  "
          f"— 0.0 = sit flat, 1.0 = no change)\n")

    # ── Baseline: no ratchet ──────────────────────────────────────────
    _base_out   = simulate(df_4x, params, filter_mode, v3_filter,
                           symbol_counts=symbol_counts,
                           vol_lev_params=vol_lev_params,
                           ratchet=None,
                           verbose=False)
    _base_stats = _ratchet_stats(_base_out["daily"], None)

    results = []

    for idx, (freq, trig, lock) in enumerate(configs, 1):
        rat = EquityRatchet(
            frequency          = freq,
            trigger            = trig,
            lock_pct           = lock,
            risk_off_lev_scale = RATCHET_RISK_OFF_LEV_SCALE,
        )
        sim   = simulate(df_4x, params, filter_mode, v3_filter,
                         symbol_counts=symbol_counts,
                         vol_lev_params=vol_lev_params,
                         ratchet=rat,
                         verbose=False)
        stats = _ratchet_stats(sim["daily"], rat)
        results.append(dict(freq=freq, trigger=trig, lock=lock, **stats))
        if idx % 9 == 0 or idx == total:
            print(f"  ... {idx}/{total} done")

    # Sort by Sharpe descending
    results.sort(key=lambda r: r["sharpe"] if np.isfinite(r["sharpe"]) else -999,
                 reverse=True)

    # ── Print table ───────────────────────────────────────────────────
    _W = 110
    _HDR = (f"  {'#':>3}  {'Freq':<8}  {'Trig':>5}  {'Lock':>5}  "
            f"{'Sharpe':>7}  {'CAGR%':>8}  {'MaxDD%':>8}  "
            f"{'Calmar':>7}  {'Ratchets':>9}  {'RiskOff':>8}  {'FlatDays':>9}")
    print()
    print("  " + "─" * _W)
    print("  EQUITY RATCHET GRID SEARCH RESULTS  (ranked by Sharpe)  "
          f"Filter: {filter_label}")
    print("  Baseline (no ratchet):  "
          f"Sharpe={_base_stats['sharpe']:.3f}  "
          f"CAGR={_base_stats['cagr']:.1f}%  "
          f"MaxDD={_base_stats['maxdd']:.2f}%  "
          f"Calmar={_base_stats['calmar']:.2f}")
    print("  " + "─" * _W)
    print(_HDR)
    print("  " + "─" * _W)

    for rank, r in enumerate(results, 1):
        sharpe_delta = r["sharpe"] - _base_stats["sharpe"]
        flag = " ▲" if sharpe_delta > 0.05 else (" ▼" if sharpe_delta < -0.05 else "  ")
        dd_delta = r["maxdd"] - _base_stats["maxdd"]
        dd_flag  = " ▲" if dd_delta > 1.0 else (" ▼" if dd_delta < -1.0 else "  ")
        print(f"  {rank:>3}  {r['freq']:<8}  {r['trigger']*100:>4.0f}%  "
              f"{r['lock']*100:>4.0f}%  "
              f"{r['sharpe']:>7.3f}{flag}  {r['cagr']:>7.1f}%  "
              f"{r['maxdd']:>7.2f}%{dd_flag}  {r['calmar']:>7.2f}  "
              f"{r['n_ratchets']:>9}  {r['n_risk_off_events']:>8}  "
              f"{r['n_flat_days']:>9}")

    print("  " + "─" * _W)

    # Top-5 highlight by Sharpe
    print(f"\n  Top-5 configs by Sharpe:")
    for rank, r in enumerate(results[:5], 1):
        print(f"    #{rank}  freq={r['freq']:<8}  trigger={r['trigger']*100:.0f}%  "
              f"lock={r['lock']*100:.0f}%  "
              f"→ Sharpe={r['sharpe']:.3f}  MaxDD={r['maxdd']:.2f}%  "
              f"Calmar={r['calmar']:.2f}  "
              f"ratchets={r['n_ratchets']}  risk-off-events={r['n_risk_off_events']}")

    # Best MaxDD improvement
    dd_sorted = sorted(results, key=lambda r: r["maxdd"], reverse=True)
    print(f"\n  Best MaxDD improvement:")
    for rank, r in enumerate(dd_sorted[:3], 1):
        print(f"    #{rank}  freq={r['freq']:<8}  trigger={r['trigger']*100:.0f}%  "
              f"lock={r['lock']*100:.0f}%  "
              f"→ MaxDD={r['maxdd']:.2f}%  (baseline {_base_stats['maxdd']:.2f}%)  "
              f"flat_days={r['n_flat_days']}")

    print()

    # ── Save CSV ──────────────────────────────────────────────────────
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "ratchet_sweep_results.csv"
        import csv as _csv
        fieldnames = ["rank", "freq", "trigger", "lock", "sharpe", "cagr", "maxdd",
                      "calmar", "n_ratchets", "n_risk_off_events", "n_flat_days"]
        with open(csv_path, "w", newline="") as fh:
            w = _csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            for rank, r in enumerate(results, 1):
                w.writerow(dict(
                    rank=rank, freq=r["freq"],
                    trigger=r["trigger"], lock=r["lock"],
                    sharpe=round(r["sharpe"], 4),
                    cagr=round(r["cagr"], 2),
                    maxdd=round(r["maxdd"], 2),
                    calmar=round(r["calmar"], 2),
                    n_ratchets=r["n_ratchets"],
                    n_risk_off_events=r["n_risk_off_events"],
                    n_flat_days=r["n_flat_days"],
                ))
        print(f"  Ratchet sweep CSV saved: {csv_path}")
    except Exception as e:
        print(f"  ⚠  Could not save ratchet CSV: {e}")


def _ratchet_stats(daily: np.ndarray,
                   rat: Optional[EquityRatchet]) -> dict:
    """Compute summary stats for a ratchet simulate() run."""
    r = daily[np.isfinite(daily)]
    if len(r) == 0:
        return dict(sharpe=float("nan"), cagr=float("nan"), maxdd=float("nan"),
                    calmar=float("nan"), n_ratchets=0,
                    n_risk_off_events=0, n_flat_days=0)

    TDAYS = 365
    n     = len(r)
    mu    = float(np.mean(r))
    sd    = float(np.std(r, ddof=1)) if n > 1 else 1e-9
    sharpe = mu / sd * TDAYS**0.5 if sd > 0 else 0.0

    eq     = np.cumprod(1 + r)
    cagr   = float((eq[-1] ** (TDAYS / n) - 1) * 100)
    peak   = np.maximum.accumulate(eq)
    dd     = (eq - peak) / peak
    maxdd  = float(np.min(dd) * 100)
    calmar = cagr / abs(maxdd) if abs(maxdd) > 1e-9 else 0.0

    n_ratchets        = len(rat.ratchet_log)       if rat else 0
    n_risk_off_events = sum(1 for e in rat.risk_off_log if e[3] == "enter") if rat else 0
    n_flat_days       = rat.n_flat_days             if rat else 0

    return dict(sharpe=sharpe, cagr=cagr, maxdd=maxdd, calmar=calmar,
                n_ratchets=n_ratchets, n_risk_off_events=n_risk_off_events,
                n_flat_days=n_flat_days)


def run_adaptive_ratchet_sweep(
    df_4x:          pd.DataFrame,
    params:         dict,
    filter_mode:    str,
    v3_filter,
    symbol_counts,
    vol_lev_params: Optional[dict],
    filter_label:   str,
    out_dir:        Path,
):
    """
    Grid-search Regime-Adaptive Ratchet configurations.

    Grid: 3 freq × 3 vol_low × 3 vol_high × 2 regime_tables = 54 configs.
    For each config, builds the vol regime series from a baseline simulate()
    run (no ratchet), then runs simulate() with the adaptive ratchet active.
    Prints a ranked table and saves a CSV.
    """

    freqs       = ADAPTIVE_RATCHET_SWEEP_FREQUENCIES
    vol_lows    = ADAPTIVE_RATCHET_SWEEP_VOL_LOWS
    vol_highs   = ADAPTIVE_RATCHET_SWEEP_VOL_HIGHS
    tables      = ADAPTIVE_RATCHET_SWEEP_TABLES
    decay_rates = ADAPTIVE_RATCHET_SWEEP_DECAY_RATES

    # Only valid combos: vol_low < vol_high
    freq_vl_vh = [(f, vl, vh) for f, vl, vh
                  in itertools.product(freqs, vol_lows, vol_highs)
                  if vl < vh]
    configs = list(itertools.product(freq_vl_vh, range(len(tables)), decay_rates))
    total   = len(configs)

    print(f"\n  Running {total} Adaptive Ratchet configs on filter: {filter_label}")
    print(f"  (regime tables: {len(tables)}  |  decay rates: {decay_rates}  |  "
          f"risk-off scale: {ADAPTIVE_RATCHET_RISK_OFF_SCALE:.2f})\n")

    # ── Baseline: no adaptive ratchet ─────────────────────────────────
    _base_out  = simulate(df_4x, params, filter_mode, v3_filter,
                          symbol_counts=symbol_counts,
                          vol_lev_params=vol_lev_params,
                          adaptive_ratchet=None,
                          verbose=False)
    _base_rets = _base_out["daily"]
    _base_stats = _ratchet_stats(_base_rets, None)

    # Build a date-aligned index for vol regime computation
    _col_dates = [_col_to_timestamp(str(c)) for c in df_4x.columns]
    _col_dates = [d for d in _col_dates if d is not None]
    _base_ret_series = pd.Series(
        _base_rets[:len(_col_dates)],
        index=pd.DatetimeIndex(_col_dates[:len(_base_rets)]),
        dtype=float,
    ).fillna(0.0)

    results = []


    for idx, ((freq, vl, vh), tbl_idx, decay) in enumerate(configs, 1):
        tbl = tables[tbl_idx]
        vrs = build_vol_regime_series(
            daily_returns = _base_ret_series,
            vol_window    = ADAPTIVE_RATCHET_VOL_WINDOW,
            vol_low       = vl,
            vol_high      = vh,
        )
        ar = AdaptiveEquityRatchet(
            frequency          = freq,
            regime_table       = tbl,
            risk_off_lev_scale = ADAPTIVE_RATCHET_RISK_OFF_SCALE,
            floor_decay_rate   = decay,
        )
        sim   = simulate(df_4x, params, filter_mode, v3_filter,
                         symbol_counts=symbol_counts,
                         vol_lev_params=vol_lev_params,
                         adaptive_ratchet=ar,
                         vol_regime_series=vrs,
                         verbose=False)
        stats = _adaptive_ratchet_stats(sim["daily"], ar)
        results.append(dict(
            freq=freq, vol_low=vl, vol_high=vh, tbl_idx=tbl_idx, decay=decay,
            **stats,
        ))
        if idx % 18 == 0 or idx == total:
            print(f"  ... {idx}/{total} done")
    # Sort by Sharpe descending
    results.sort(key=lambda r: r["sharpe"] if np.isfinite(r["sharpe"]) else -999,
                 reverse=True)

    # ── Print table ───────────────────────────────────────────────────
    _W = 130
    _HDR = (f"  {'#':>3}  {'Freq':<8}  {'vLow':>5}  {'vHigh':>6}  {'Tbl':>4}  {'Decay':>7}  "
            f"{'Sharpe':>7}  {'CAGR%':>8}  {'MaxDD%':>8}  "
            f"{'Calmar':>7}  {'Ratchets':>9}  {'RiskOff':>8}  {'FlatDays':>9}  {'RegCounts':>12}")
    print()
    print("  " + "─" * _W)
    print("  ADAPTIVE RATCHET GRID SEARCH RESULTS  (ranked by Sharpe)  "
          f"Filter: {filter_label}")
    print("  Baseline (no adaptive ratchet):  "
          f"Sharpe={_base_stats['sharpe']:.3f}  "
          f"CAGR={_base_stats['cagr']:.1f}%  "
          f"MaxDD={_base_stats['maxdd']:.2f}%  "
          f"Calmar={_base_stats['calmar']:.2f}")
    for i, tbl in enumerate(tables):
        low_p = tbl.get("low", {})
        nrm_p = tbl.get("normal", {})
        hgh_p = tbl.get("high", {})
        print(f"  Table {i}:  "
              f"low(t={low_p.get('trigger','-')*100:.0f}%,l={low_p.get('lock_pct','-')*100:.0f}%)  "
              f"normal(t={nrm_p.get('trigger','-')*100:.0f}%,l={nrm_p.get('lock_pct','-')*100:.0f}%)  "
              f"high(t={hgh_p.get('trigger','-')*100:.0f}%,l={hgh_p.get('lock_pct','-')*100:.0f}%)")
    print("  " + "─" * _W)
    print(_HDR)
    print("  " + "─" * _W)

    for rank, r in enumerate(results, 1):
        sharpe_delta = r["sharpe"] - _base_stats["sharpe"]
        flag = " ▲" if sharpe_delta > 0.05 else (" ▼" if sharpe_delta < -0.05 else "  ")
        dd_delta = r["maxdd"] - _base_stats["maxdd"]
        dd_flag  = " ▲" if dd_delta > 1.0 else (" ▼" if dd_delta < -1.0 else "  ")
        rc = r.get("regime_counts", {})
        rc_str = f"L:{rc.get('low',0)} N:{rc.get('normal',0)} H:{rc.get('high',0)}"
        print(f"  {rank:>3}  {r['freq']:<8}  {r['vol_low']*100:>4.0f}%  "
              f"{r['vol_high']*100:>5.0f}%  {r['tbl_idx']:>4}  {r['decay']:>6.3f}  "
              f"{r['sharpe']:>7.3f}{flag}  {r['cagr']:>7.1f}%  "
              f"{r['maxdd']:>7.2f}%{dd_flag}  {r['calmar']:>7.2f}  "
              f"{r['n_ratchets']:>9}  {r['n_risk_off_events']:>8}  "
              f"{r['n_flat_days']:>9}  {rc_str:>12}")

    print("  " + "─" * _W)

    print(f"\n  Top-5 configs by Sharpe:")
    for rank, r in enumerate(results[:5], 1):
        rc = r.get("regime_counts", {})
        print(f"    #{rank}  freq={r['freq']:<8}  vol_low={r['vol_low']*100:.0f}%  "
              f"vol_high={r['vol_high']*100:.0f}%  table={r['tbl_idx']}  decay={r['decay']:.3f}  "
              f"→ Sharpe={r['sharpe']:.3f}  MaxDD={r['maxdd']:.2f}%  "
              f"Calmar={r['calmar']:.2f}  "
              f"L:{rc.get('low',0)}/N:{rc.get('normal',0)}/H:{rc.get('high',0)} rebalance days")

    dd_sorted = sorted(results, key=lambda r: r["maxdd"], reverse=True)
    print(f"\n  Best MaxDD improvement:")
    for rank, r in enumerate(dd_sorted[:3], 1):
        print(f"    #{rank}  freq={r['freq']:<8}  vol_low={r['vol_low']*100:.0f}%  "
              f"vol_high={r['vol_high']*100:.0f}%  table={r['tbl_idx']}  decay={r['decay']:.3f}  "
              f"→ MaxDD={r['maxdd']:.2f}%  (baseline {_base_stats['maxdd']:.2f}%)  "
              f"flat_days={r['n_flat_days']}")
    print()

    # ── Save CSV ──────────────────────────────────────────────────────
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "adaptive_ratchet_sweep_results.csv"
        import csv as _csv
        fieldnames = ["rank", "freq", "vol_low", "vol_high", "tbl_idx", "decay",
                      "sharpe", "cagr", "maxdd", "calmar",
                      "n_ratchets", "n_risk_off_events", "n_flat_days",
                      "regime_low", "regime_normal", "regime_high"]
        with open(csv_path, "w", newline="") as fh:
            w = _csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            for rank, r in enumerate(results, 1):
                rc = r.get("regime_counts", {})
                w.writerow(dict(
                    rank=rank, freq=r["freq"],
                    vol_low=r["vol_low"], vol_high=r["vol_high"],
                    tbl_idx=r["tbl_idx"], decay=r["decay"],
                    sharpe=round(r["sharpe"], 4),
                    cagr=round(r["cagr"], 2),
                    maxdd=round(r["maxdd"], 2),
                    calmar=round(r["calmar"], 2),
                    n_ratchets=r["n_ratchets"],
                    n_risk_off_events=r["n_risk_off_events"],
                    n_flat_days=r["n_flat_days"],
                    regime_low=rc.get("low", 0),
                    regime_normal=rc.get("normal", 0),
                    regime_high=rc.get("high", 0),
                ))
        print(f"  Adaptive ratchet sweep CSV saved: {csv_path}")
    except Exception as e:
        print(f"  ⚠  Could not save adaptive ratchet CSV: {e}")


def _adaptive_ratchet_stats(daily: np.ndarray,
                             ar: Optional[AdaptiveEquityRatchet]) -> dict:
    """Compute summary stats for an adaptive ratchet simulate() run."""
    r = daily[np.isfinite(daily)]
    if len(r) == 0:
        return dict(sharpe=float("nan"), cagr=float("nan"), maxdd=float("nan"),
                    calmar=float("nan"), n_ratchets=0,
                    n_risk_off_events=0, n_flat_days=0, regime_counts={})

    TDAYS = 365
    n     = len(r)
    mu    = float(np.mean(r))
    sd    = float(np.std(r, ddof=1)) if n > 1 else 1e-9
    sharpe = mu / sd * TDAYS**0.5 if sd > 0 else 0.0

    eq     = np.cumprod(1 + r)
    cagr   = float((eq[-1] ** (TDAYS / n) - 1) * 100)
    peak   = np.maximum.accumulate(eq)
    dd     = (eq - peak) / peak
    maxdd  = float(np.min(dd) * 100)
    calmar = cagr / abs(maxdd) if abs(maxdd) > 1e-9 else 0.0

    if ar is not None:
        n_ratchets        = len(ar.ratchet_log)
        n_risk_off_events = sum(1 for e in ar.risk_off_log if e[3] == "enter")
        n_flat_days       = ar.n_flat_days
        regime_counts     = {}
        for _, rg in ar.regime_log:
            regime_counts[rg] = regime_counts.get(rg, 0) + 1
    else:
        n_ratchets = n_risk_off_events = n_flat_days = 0
        regime_counts = {}

    return dict(sharpe=sharpe, cagr=cagr, maxdd=maxdd, calmar=calmar,
                n_ratchets=n_ratchets, n_risk_off_events=n_risk_off_events,
                n_flat_days=n_flat_days, regime_counts=regime_counts)


# ══════════════════════════════════════════════════════════════════════
# NOISE PERTURBATION STABILITY TEST
# ══════════════════════════════════════════════════════════════════════

def _noise_stats(daily: np.ndarray) -> dict:
    """Quick stats from a daily returns array."""
    r = daily[np.isfinite(daily)]
    if len(r) < 10:
        return dict(sharpe=np.nan, cagr=np.nan, maxdd=np.nan, calmar=np.nan)
    TDAYS = 365
    n   = len(r)
    mu  = float(np.mean(r))
    sd  = float(np.std(r, ddof=1)) if n > 1 else 1e-9
    sharpe = mu / sd * TDAYS**0.5 if sd > 1e-9 else 0.0
    eq   = np.cumprod(1 + r)
    cagr = float((eq[-1] ** (TDAYS / n) - 1) * 100)
    peak = np.maximum.accumulate(eq)
    maxdd = float(np.min((eq - peak) / peak) * 100)
    calmar = cagr / abs(maxdd) if abs(maxdd) > 1e-9 else 0.0
    return dict(sharpe=sharpe, cagr=cagr, maxdd=maxdd, calmar=calmar)


# ══════════════════════════════════════════════════════════════════════
# EQUITY CURVE ENSEMBLE PLOT
# ══════════════════════════════════════════════════════════════════════

def plot_equity_ensemble(
    daily_with_zeros: np.ndarray,
    col_dates,
    run_dir:          Path,
    filter_label:     str,
    starting_capital: float = 10_000.0,
    n_trials:         int   = 500,
    label:            str   = "ensemble",
):
    """
    Bootstrap equity curve ensemble (fan chart).

    Draws N_TRIALS paths by resampling daily returns with replacement,
    then plots percentile bands alongside the actual equity curve.

    Answers: How much of the CAGR is path-order luck vs structural edge?
    If the actual curve sits near the median of the fan, the edge is
    structural. If it rides the top 5%, the backtest may be path-lucky.
    """



    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"
    GRID_COL = "#21262d"
    ACCENT   = "#58a6ff"
    MED_COL  = "#3fb950"

    rng = np.random.default_rng(42)

    # ── Build actual equity curve ────────────────────────────────────
    r_act  = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
    eq_act = np.cumprod(1 + r_act) * starting_capital
    n_days = len(r_act)

    # Active returns only (non-zero, finite) for resampling pool
    pool = r_act[r_act != 0.0]
    if len(pool) < 10:
        print("  ⚠  Ensemble plot: not enough active return days — skipping.")
        return

    # ── Block bootstrap: resample 5-day blocks of active returns ─────
    # IID day-by-day resampling breaks autocorrelation structure —
    # volatility clustering, momentum bursts, and drawdown streaks all
    # disappear.  Sampling contiguous blocks preserves these regime
    # properties and produces a more realistic return distribution.
    #
    # Block size 5 ≈ one trading week.  We oversample then trim to
    # exactly n_active to handle non-divisible lengths cleanly.
    BLOCK = 5
    flat_mask = (r_act == 0.0)
    n_active  = int((~flat_mask).sum())
    n_pool    = len(pool)
    all_eq    = np.zeros((n_trials, n_days))

    # Maximum valid block start index
    max_start = max(n_pool - BLOCK, 0)

    for i in range(n_trials):
        r_trial = r_act.copy()

        # Draw enough block start indices to cover n_active returns
        n_blocks_needed = (n_active // BLOCK) + 2      # +2 guarantees overshoot
        starts  = rng.integers(0, max_start + 1, size=n_blocks_needed)
        # Concatenate blocks, trim to exactly n_active
        sampled = np.concatenate([pool[s : s + BLOCK] for s in starts])[:n_active]
        r_trial[~flat_mask] = sampled
        all_eq[i] = np.cumprod(1 + r_trial) * starting_capital

    # ── Percentile bands ─────────────────────────────────────────────
    p05 = np.percentile(all_eq, 5,  axis=0)
    p25 = np.percentile(all_eq, 25, axis=0)
    p50 = np.percentile(all_eq, 50, axis=0)
    p75 = np.percentile(all_eq, 75, axis=0)
    p95 = np.percentile(all_eq, 95, axis=0)

    # ── X-axis ───────────────────────────────────────────────────────
    dates_ok = (col_dates is not None
                and len(col_dates) == n_days
                and all(d is not None for d in col_dates))
    x = col_dates if dates_ok else np.arange(n_days)

    # Percentile rank of actual curve at final day
    final_vals = all_eq[:, -1]
    actual_pct = float(np.mean(final_vals <= eq_act[-1]) * 100)

    # ── Plot ─────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(14, 7), facecolor=FIG_BG)
    ax.set_facecolor(PANEL_BG)

    # Outer band: 5–95
    ax.fill_between(x, p05, p95, color=ACCENT, alpha=0.10, label="5–95th pct")
    # Inner band: 25–75
    ax.fill_between(x, p25, p75, color=ACCENT, alpha=0.22, label="25–75th pct")
    # Median path
    ax.plot(x, p50, color=MED_COL, lw=1.4, linestyle="--", label="Median path", zorder=3)
    # Actual curve
    ax.plot(x, eq_act, color="#ffffff", lw=2.0, label=f"Actual (p{actual_pct:.0f})", zorder=5)

    ax.set_yscale("log")
    ax.set_title(
        f"Equity Curve Ensemble  ({n_trials} block-bootstrap paths, block=5)  —  {filter_label}\n"
        f"Actual curve at {actual_pct:.0f}th percentile of final equity",
        color=TEXT_COL, fontsize=12, pad=10,
    )
    ax.set_xlabel("Date" if dates_ok else "Trading Day", color=TEXT_COL)
    ax.set_ylabel(f"Portfolio Value (log scale, start={starting_capital:,.0f})", color=TEXT_COL)
    ax.tick_params(colors=TEXT_COL)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"${v:,.0f}" if v >= 1000 else f"${v:.0f}"
    ))
    for spine in ax.spines.values():
        spine.set_edgecolor(GRID_COL)
    ax.grid(True, color=GRID_COL, linewidth=0.5, alpha=0.6)
    if dates_ok:

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        fig.autofmt_xdate(rotation=30)

    leg = ax.legend(facecolor=PANEL_BG, edgecolor=GRID_COL, labelcolor=TEXT_COL, fontsize=10)

    # ── Annotation: where actual sits ────────────────────────────────
    verdict = (
        "near median — edge appears structural"      if 35 <= actual_pct <= 65 else
        "above median — modest path luck"            if 65 < actual_pct <= 80 else
        "well above median — significant path luck"  if actual_pct > 80 else
        "below median — conservative path"
    )
    ax.annotate(
        f"Actual at p{actual_pct:.0f}: {verdict}",
        xy=(0.02, 0.04), xycoords="axes fraction",
        color=TEXT_COL, fontsize=9, alpha=0.85,
        bbox=dict(boxstyle="round,pad=0.3", facecolor=FIG_BG, edgecolor=GRID_COL, alpha=0.7),
    )

    plt.tight_layout()
    out_path = run_dir / f"equity_curve_ensemble_{label}.png"
    fig.savefig(out_path, dpi=150, facecolor=FIG_BG, bbox_inches="tight")
    plt.close(fig)
    print(f"  Equity ensemble chart saved: {out_path}")
    print(f"  Actual curve percentile rank: p{actual_pct:.0f}  ({verdict})")
    return str(out_path)


# ══════════════════════════════════════════════════════════════════════
# SLIPPAGE IMPACT SWEEP
# ══════════════════════════════════════════════════════════════════════

def run_slippage_sweep(
    daily_active:   np.ndarray,
    daily_all:      np.ndarray,
    active_mask:    np.ndarray,
    filter_label:   str,
    out_dir:        Path,
    slippage_levels: list = None,
    taker_fee_pct:  float = 0.0,
):
    """
    Slippage Impact Sweep — measures execution-cost sensitivity.

    Injects slippage directly into per-trade daily returns by subtracting
    `slippage_pct` on every active day, then recomputes all key metrics.
    This is the correct formulation: it shifts both mean AND distribution,
    unlike a mean-drag approximation.

    Parameters
    ----------
    daily_active : array of raw daily returns on active trading days only
    daily_all    : array of daily returns with zeros on flat days
                   (same length as trading calendar, zeros on flat days)
    active_mask  : boolean array aligned with daily_all (True = active day)
    filter_label : name of the active filter, for display
    out_dir      : directory to save CSV
    slippage_levels : one-way slippage fractions (entry + exit = 2× round-trip)
    taker_fee_pct   : already-baked fee per side (informational, for context line)
    """
    if slippage_levels is None:
        slippage_levels = [0.0005, 0.0010, 0.0025, 0.0050, 0.0100]

    TDAYS = 365

    def _stats(d: np.ndarray) -> dict:
        r = d[np.isfinite(d)]
        if len(r) < 10:
            return dict(sharpe=np.nan, cagr=np.nan, maxdd=np.nan, calmar=np.nan)
        n    = len(r)
        mu   = float(np.mean(r))
        sd   = float(np.std(r, ddof=1)) if n > 1 else 1e-9
        sh   = mu / sd * TDAYS**0.5 if sd > 1e-9 else 0.0
        eq   = np.cumprod(1 + r)
        cagr = float((eq[-1] ** (TDAYS / n) - 1) * 100)
        peak = np.maximum.accumulate(eq)
        mdd  = float(np.min((eq - peak) / peak) * 100)
        cal  = cagr / abs(mdd) if abs(mdd) > 1e-9 else 0.0
        return dict(sharpe=sh, cagr=cagr, maxdd=mdd, calmar=cal)

    # Baseline (no extra slippage — taker fee already in returns)
    base = _stats(daily_all)

    n_active    = int(np.sum(active_mask))
    n_total     = len(daily_all)
    active_pct  = n_active / n_total * 100 if n_total > 0 else 0.0

    print(f"\n{'═'*76}")
    print(f"  SLIPPAGE IMPACT SWEEP   Filter: {filter_label}")
    print(f"{'═'*76}")
    print(f"  Active days : {n_active} / {n_total}  ({active_pct:.1f}%)")
    print(f"  Taker fee already baked in: {taker_fee_pct*100:.3f}% per side "
          f"({taker_fee_pct*2*100:.3f}% round-trip)")
    print(f"  Slippage below is ADDITIONAL one-way cost (entry + exit = 2× shown)")
    print(f"  Scalability bar: Sharpe ≥ 2.0 @ 0.25% → institutional-grade")
    print()
    print(f"  {'Slippage':>10}  {'RT Cost':>9}  {'Sharpe':>8}  {'vs Base':>8}  "
          f"{'CAGR%':>9}  {'MaxDD%':>8}  {'Calmar':>8}  {'Grade':>8}")
    print(f"  {'─'*76}")

    results = []
    for slip in slippage_levels:
        # Apply slippage to every active day: r_adj = r - 2*slip (round-trip)
        d_adj = daily_all.copy().astype(float)
        d_adj[active_mask] -= 2.0 * slip

        s       = _stats(d_adj)
        sh_delta = s["sharpe"] - base["sharpe"]
        rt_cost  = slip * 2 * 100   # round-trip %

        # Grade
        if   s["sharpe"] >= 2.5:  grade = "Excellent"
        elif s["sharpe"] >= 2.0:  grade = "Strong   "
        elif s["sharpe"] >= 1.5:  grade = "Marginal "
        elif s["sharpe"] >= 1.0:  grade = "Weak     "
        else:                     grade = "Unusable "

        # Scalability flag
        if   slip == 0.0025 and s["sharpe"] >= 2.0: scale_flag = " ✓ scalable"
        elif slip == 0.0025 and s["sharpe"] >= 1.5: scale_flag = " ? marginal"
        elif slip == 0.0025:                          scale_flag = " ✗ fragile "
        else:                                         scale_flag = ""

        print(f"  {slip*100:>9.3f}%  {rt_cost:>8.3f}%  {s['sharpe']:>8.3f}  "
              f"{sh_delta:>+8.3f}  {s['cagr']:>8.1f}%  {s['maxdd']:>8.2f}%  "
              f"{s['calmar']:>8.2f}  {grade}{scale_flag}")

        results.append(dict(
            slippage_one_way_pct = round(slip * 100, 4),
            round_trip_pct       = round(rt_cost, 4),
            sharpe               = round(s["sharpe"], 4),
            sharpe_delta         = round(sh_delta, 4),
            cagr_pct             = round(s["cagr"], 2),
            maxdd_pct            = round(s["maxdd"], 2),
            calmar               = round(s["calmar"], 2),
            grade                = grade.strip(),
        ))

    # Baseline row for reference
    print(f"  {'─'*76}")
    print(f"  {'Baseline':>10}  {'0.000%':>9}  {base['sharpe']:>8.3f}  "
          f"{'+0.000':>8}  {base['cagr']:>8.1f}%  {base['maxdd']:>8.2f}%  "
          f"{base['calmar']:>8.2f}  (taker fee already included)")

    # Break-even slippage: where Sharpe crosses 2.0 and 1.5
    print(f"\n  Break-even analysis:")
    sharpes = [r["sharpe"] for r in results]
    slips   = [r["slippage_one_way_pct"] for r in results]
    for threshold in [2.5, 2.0, 1.5, 1.0]:
        surviving = [s for s, sh in zip(slips, sharpes) if sh >= threshold]
        if surviving:
            print(f"    Sharpe ≥ {threshold:.1f} : survives up to {max(surviving):.3f}% one-way slippage")
        else:
            print(f"    Sharpe ≥ {threshold:.1f} : does not survive even at lowest tested level")

    print(f"  {'═'*76}")

    # Save CSV
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "slippage_sweep_results.csv"
        import csv as _csv
        fieldnames = list(results[0].keys()) if results else []
        with open(csv_path, "w", newline="") as fh:
            w = _csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(results)
        print(f"  Slippage sweep CSV saved: {csv_path}")
    except Exception as e:
        print(f"  ⚠  Could not save slippage CSV: {e}")


def run_noise_stability_test(
    df_4x:         pd.DataFrame,
    params:        dict,
    filter_mode:   str,
    v3_filter,
    symbol_counts,
    vol_lev_params: Optional[dict],
    filter_label:  str,
    out_dir:       Path,
    n_trials:      int              = 100,
    return_levels: list             = None,
    shuffle_levels: list            = None,
):
    """
    Noise-Perturbation Stability Test.

    Runs two independent perturbation sweeps across N_TRIALS random seeds:

    MODE A — Return noise
        Adds IID Gaussian noise (scale = level / sqrt(n_bars_per_day)) to
        every bar of every day's intraday price path. The noise is applied
        in log-return space so price paths remain positive. Tests execution
        noise, microstructure, and fill-price uncertainty.

    MODE B — Signal shuffle
        For each trial, flips `level` fraction of active days → flat and
        the same fraction of flat days → active (by randomly selecting from
        each pool). Tests whether the filter's specific day choices drive
        the edge, or whether similar-looking day sets produce similar results.

    Output: degradation table (mean ± std vs baseline), stability scores,
    CSV saved to out_dir.
    """
    if return_levels  is None: return_levels  = [0.001, 0.003, 0.005, 0.010]
    if shuffle_levels is None: shuffle_levels = [0.02,  0.05,  0.10,  0.20 ]

    rng = np.random.default_rng(42)   # reproducible across runs

    # ── Suppress per-trial output ─────────────────────────────────────

    # ── Baseline (clean, no perturbation) ────────────────────────────
    _base_out  = simulate(df_4x, params, filter_mode, v3_filter,
                          symbol_counts=symbol_counts,
                          vol_lev_params=vol_lev_params,
                          verbose=False)
    _base      = _noise_stats(np.array(_base_out["daily"]))

    print(f"\n{'═'*72}")
    print(f"  NOISE PERTURBATION STABILITY TEST   Filter: {filter_label}")
    print(f"{'═'*72}")
    print(f"  Baseline → Sharpe={_base['sharpe']:.3f}  CAGR={_base['cagr']:.1f}%  "
          f"MaxDD={_base['maxdd']:.2f}%  Calmar={_base['calmar']:.2f}")
    print(f"  Trials per level: {n_trials}")

    # Pre-compute column order and bar count per day
    _cols       = list(df_4x.columns)
    _n_cols     = len(_cols)
    _n_bars     = len(df_4x.index)   # rows = intraday bars

    # ── Helper: build a perturbed df_4x ──────────────────────────────
    def _perturb_returns(seed: int, daily_vol: float) -> pd.DataFrame:
        """Add Gaussian noise in log-price space (prevents multiplicative distortion).

        Correct approach per review: work in log(price) space, add noise, exponentiate.
        This avoids the compounding distortion of arr *= exp(noise) applied cumulatively
        across bars. bar_std = daily_vol / sqrt(n_bars) keeps the total daily noise
        correctly calibrated regardless of bar count.
        """
        rng_t      = np.random.default_rng(seed)
        arr        = df_4x.values.copy().astype(float)   # shape: (n_bars, n_days)
        bar_std    = daily_vol / np.sqrt(max(_n_bars, 1))
        noise      = rng_t.normal(0.0, bar_std, size=arr.shape)
        log_prices = np.log(np.clip(arr, 1e-12, None))   # guard against zeros
        log_prices += noise
        arr        = np.exp(log_prices)
        return pd.DataFrame(arr, index=df_4x.index, columns=df_4x.columns)

    def _perturb_filter(seed: int, frac: float) -> Optional[pd.Series]:
        """Flip `frac` of active↔flat days in v3_filter.

        Uses integer positional indexing (O(n) not O(n²)) — fix per review.
        Preserves total active day count by flipping equal numbers in each direction.
        """
        if v3_filter is None:
            return None
        rng_t      = np.random.default_rng(seed)
        vals       = v3_filter.values.copy().astype(bool)
        # True = sit flat; active days are where vals == False
        active_idx = np.where(~vals)[0]
        flat_idx   = np.where( vals)[0]
        n_flip     = int(min(len(active_idx), len(flat_idx)) * frac)
        if n_flip == 0:
            return pd.Series(vals, index=v3_filter.index)
        flip_active = rng_t.choice(len(active_idx), size=n_flip, replace=False)
        flip_flat   = rng_t.choice(len(flat_idx),   size=n_flip, replace=False)
        vals[active_idx[flip_active]] = True    # active → flat
        vals[flat_idx[flip_flat]]     = False   # flat   → active
        return pd.Series(vals, index=v3_filter.index)

    all_results = []

    # ── Shared per-level stats helper ────────────────────────────────
    def _level_stats(sharpes, cagrs, maxdds):
        mean_sh   = float(np.nanmean(sharpes))
        std_sh    = float(np.nanstd(sharpes))
        min_sh    = float(np.nanmin(sharpes))
        mean_cagr = float(np.nanmean(cagrs))
        mean_dd   = float(np.nanmean(maxdds))
        score     = mean_sh / _base["sharpe"] if abs(_base["sharpe"]) > 1e-9 else 0.0
        worst_sc  = min_sh  / _base["sharpe"] if abs(_base["sharpe"]) > 1e-9 else 0.0
        flag      = " ✓" if score >= 0.85 else (" ?" if score >= 0.70 else " ✗")
        return mean_sh, std_sh, min_sh, mean_cagr, mean_dd, score, worst_sc, flag

    # ── MODE A: Return noise ──────────────────────────────────────────
    print(f"\n  ── Mode A: Return Noise ({'|'.join(f'{l*100:.1f}%' for l in return_levels)} daily vol) ──")
    _W = 90
    print(f"  {'Level':>8}  {'Mean Sharpe':>12}  {'Std':>7}  {'Min':>7}  "
          f"{'Mean CAGR':>10}  {'MeanMaxDD':>10}  {'Score':>7}  {'Worst':>7}")
    print(f"  {'─'*_W}")

    for level in return_levels:
        sharpes, cagrs, maxdds = [], [], []
        for trial in range(n_trials):
            seed = int(rng.integers(0, 2**31))
            df_p = _perturb_returns(seed, level)
            out = simulate(df_p, params, filter_mode, v3_filter,
                           symbol_counts=symbol_counts,
                           vol_lev_params=vol_lev_params,
                           verbose=False)
            s    = _noise_stats(np.array(out["daily"]))
            sharpes.append(s["sharpe"]); cagrs.append(s["cagr"]); maxdds.append(s["maxdd"])

        mean_sh, std_sh, min_sh, mean_cagr, mean_dd, score, worst_sc, flag = \
            _level_stats(sharpes, cagrs, maxdds)
        print(f"  {level*100:>7.1f}%  {mean_sh:>12.3f}  {std_sh:>7.3f}  {min_sh:>7.3f}  "
              f"{mean_cagr:>9.1f}%  {mean_dd:>9.2f}%  {score:>6.3f}{flag}  {worst_sc:>6.3f}")
        all_results.append(dict(
            mode="return_noise", level=level,
            mean_sharpe=round(mean_sh, 4), std_sharpe=round(std_sh, 4),
            min_sharpe=round(min_sh, 4), mean_cagr=round(mean_cagr, 2),
            mean_maxdd=round(mean_dd, 2), score=round(score, 4),
            worst_score=round(worst_sc, 4), n_trials=n_trials,
        ))

    # ── MODE B: Signal shuffle ────────────────────────────────────────
    print(f"\n  ── Mode B: Signal Shuffle ({'|'.join(f'{l*100:.0f}%' for l in shuffle_levels)} of days flipped) ──")
    print(f"  {'Level':>8}  {'Mean Sharpe':>12}  {'Std':>7}  {'Min':>7}  "
          f"{'Mean CAGR':>10}  {'MeanMaxDD':>10}  {'Score':>7}  {'Worst':>7}")
    print(f"  {'─'*_W}")

    for level in shuffle_levels:
        sharpes, cagrs, maxdds = [], [], []
        for trial in range(n_trials):
            seed = int(rng.integers(0, 2**31))
            v3_p = _perturb_filter(seed, level)
            out = simulate(df_4x, params, filter_mode, v3_p,
                           symbol_counts=symbol_counts,
                           vol_lev_params=vol_lev_params,
                           verbose=False)
            s    = _noise_stats(np.array(out["daily"]))
            sharpes.append(s["sharpe"]); cagrs.append(s["cagr"]); maxdds.append(s["maxdd"])

        mean_sh, std_sh, min_sh, mean_cagr, mean_dd, score, worst_sc, flag = \
            _level_stats(sharpes, cagrs, maxdds)
        print(f"  {level*100:>7.1f}%  {mean_sh:>12.3f}  {std_sh:>7.3f}  {min_sh:>7.3f}  "
              f"{mean_cagr:>9.1f}%  {mean_dd:>9.2f}%  {score:>6.3f}{flag}  {worst_sc:>6.3f}")
        all_results.append(dict(
            mode="signal_shuffle", level=level,
            mean_sharpe=round(mean_sh, 4), std_sharpe=round(std_sh, 4),
            min_sharpe=round(min_sh, 4), mean_cagr=round(mean_cagr, 2),
            mean_maxdd=round(mean_dd, 2), score=round(score, 4),
            worst_score=round(worst_sc, 4), n_trials=n_trials,
        ))

    # ── MODE C: Ranking noise ─────────────────────────────────────────
    # Perturbs the relative ordering of assets within each day's selection
    # by adding noise proportional to score std.  Simulates feed latency,
    # exchange differences, and slightly different OHLC bars that would
    # shift symbol rankings in a real deployment.
    #
    # Because this pipeline selects assets based on intraday bar scores
    # rather than an explicit ranking array, we proxy the ranking noise
    # by jittering a small fraction of *which columns are eligible* on
    # each day — concretely, for every day we swap K randomly-chosen
    # active columns with K randomly-chosen inactive columns drawn from
    # the full column pool.  This preserves the number of active assets
    # while simulating ranking-boundary sensitivity.
    _ranking_shuffle_levels = [0.02, 0.05, 0.10]
    print(f"\n  ── Mode C: Ranking Noise ({'|'.join(f'{l*100:.0f}%' for l in _ranking_shuffle_levels)} of assets swapped per day) ──")
    print(f"  {'Level':>8}  {'Mean Sharpe':>12}  {'Std':>7}  {'Min':>7}  "
          f"{'Mean CAGR':>10}  {'MeanMaxDD':>10}  {'Score':>7}  {'Worst':>7}")
    print(f"  {'─'*_W}")

    _all_cols   = np.array(df_4x.columns.tolist())
    _n_all_cols = len(_all_cols)

    def _perturb_columns(seed: int, frac: float) -> pd.DataFrame:
        """Swap `frac` of columns per day with random replacements.

        Each day's price path is a column.  Swapping columns simulates
        assets near the ranking boundary being bumped in or out — the
        most realistic proxy for ranking noise when there is no explicit
        per-day score array exposed at this level.
        """
        rng_t   = np.random.default_rng(seed)
        n_swap  = max(1, int(_n_all_cols * frac))
        # Build a jittered column order for the entire matrix
        col_idx = np.arange(_n_all_cols)
        swap_a  = rng_t.choice(_n_all_cols, size=n_swap, replace=False)
        swap_b  = rng_t.choice(_n_all_cols, size=n_swap, replace=False)
        col_idx[swap_a], col_idx[swap_b] = col_idx[swap_b].copy(), col_idx[swap_a].copy()
        jittered_cols = _all_cols[col_idx]
        # Return df with same index/column names but shuffled data
        new_df = pd.DataFrame(
            df_4x.values[:, col_idx],
            index=df_4x.index,
            columns=_all_cols,          # keep original col names so simulate() can match filter dates
        )
        return new_df

    for level in _ranking_shuffle_levels:
        sharpes, cagrs, maxdds = [], [], []
        for trial in range(n_trials):
            seed = int(rng.integers(0, 2**31))
            df_p = _perturb_columns(seed, level)
            out = simulate(df_p, params, filter_mode, v3_filter,
                           symbol_counts=symbol_counts,
                           vol_lev_params=vol_lev_params,
                           verbose=False)
            s    = _noise_stats(np.array(out["daily"]))
            sharpes.append(s["sharpe"]); cagrs.append(s["cagr"]); maxdds.append(s["maxdd"])

        mean_sh, std_sh, min_sh, mean_cagr, mean_dd, score, worst_sc, flag = \
            _level_stats(sharpes, cagrs, maxdds)
        print(f"  {level*100:>7.1f}%  {mean_sh:>12.3f}  {std_sh:>7.3f}  {min_sh:>7.3f}  "
              f"{mean_cagr:>9.1f}%  {mean_dd:>9.2f}%  {score:>6.3f}{flag}  {worst_sc:>6.3f}")
        all_results.append(dict(
            mode="ranking_noise", level=level,
            mean_sharpe=round(mean_sh, 4), std_sharpe=round(std_sh, 4),
            min_sharpe=round(min_sh, 4), mean_cagr=round(mean_cagr, 2),
            mean_maxdd=round(mean_dd, 2), score=round(score, 4),
            worst_score=round(worst_sc, 4), n_trials=n_trials,
        ))

    # ── Summary verdict ───────────────────────────────────────────────
    print(f"\n  {'─'*_W}")
    return_scores  = [r["score"] for r in all_results if r["mode"] == "return_noise"]
    shuffle_scores = [r["score"] for r in all_results if r["mode"] == "signal_shuffle"]
    ranking_scores = [r["score"] for r in all_results if r["mode"] == "ranking_noise"]

    def _verdict(scores):
        worst = min(scores) if scores else 0.0
        if   all(s >= 0.85 for s in scores): label = "ROBUST       ✓  (all levels ≥ 0.85)"
        elif all(s >= 0.70 for s in scores): label = "MARGINAL     ?  (all levels ≥ 0.70)"
        elif scores[0] >= 0.85:              label = "PARTIAL      ~  (low noise ok, degrades at high)"
        else:                                label = "FRAGILE      ✗  (collapses at low perturbation)"
        tail_flag = "  ⚠ TAIL FRAGILE (worst < 0.50)" if worst < 0.50 else ""
        return f"{label}{tail_flag}  [worst={worst:.3f}]"

    print(f"  Return noise verdict  : {_verdict(return_scores)}")
    print(f"  Signal shuffle verdict: {_verdict(shuffle_scores)}")
    if ranking_scores:
        print(f"  Ranking noise verdict : {_verdict(ranking_scores)}")
    print(f"  Baseline Sharpe       : {_base['sharpe']:.3f}")
    print(f"  Score threshold       : ≥0.85 = robust  |  0.70–0.84 = marginal  |  <0.70 = fragile")
    print(f"  {'═'*_W}")

    # ── Save CSV ──────────────────────────────────────────────────────
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "noise_stability_results.csv"
        import csv as _csv
        fieldnames = ["mode", "level", "mean_sharpe", "std_sharpe", "min_sharpe",
                      "mean_cagr", "mean_maxdd", "score", "worst_score", "n_trials"]
        with open(csv_path, "w", newline="") as fh:
            w = _csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(all_results)
        print(f"  Noise stability CSV saved: {csv_path}")
    except Exception as e:
        print(f"  ⚠  Could not save noise stability CSV: {e}")






# ══════════════════════════════════════════════════════════════════════════════
# RETURN CONCENTRATION / TOP-N CONTRIBUTION CURVE
# ══════════════════════════════════════════════════════════════════════════════
# Answers: "Is the strategy's edge broad-based or driven by a few outlier days?"
#
# Computes Top-N contribution (allocator convention: denominator = total net PnL),
# worst-day concentration, PnL half-life, and Lorenz curves for winners/losers.
#
# Outputs
#   parameter_sweeps/return_concentration.png  — Lorenz + bar + winners/losers
#   parameter_sweeps/return_concentration.csv  — per-cutoff concentration table

def run_return_concentration(
    daily_returns: np.ndarray,       # full daily series incl. flat days (zeros)
    out_dir: Path,
    filter_label: str = "",
    top_ns: list = None,             # percentile cutoffs e.g. [1, 5, 10, 20]
):
    """
    Return Concentration / Top-N Contribution Curve.

    Computes what fraction of total net PnL is contributed by the best N%
    of trading days (allocator convention: denominator = total net return).
    Plots a Lorenz-style curve plus separate winners / losers concentration
    curves and a Gini coefficient.
    """
    top_ns = top_ns or [1, 5, 10, 20, 25]

    # ── Active days only ────────────────────────────────────────────────
    arr        = np.array(daily_returns, dtype=float)
    active     = arr[np.isfinite(arr) & (arr != 0.0)]
    n_active   = len(active)
    n_total    = len(arr)
    n_flat     = n_total - n_active

    if n_active < 10:
        print("  ⚠  Return concentration: insufficient active days — skipping.")
        return

    # ── Core metrics ────────────────────────────────────────────────────
    positive   = active[active > 0]
    negative   = active[active < 0]
    total_pos  = float(positive.sum())    # gross positive return
    total_neg  = float(negative.sum())    # gross negative return (< 0)
    total_pnl  = float(active.sum())      # net return — allocator denominator

    # Sort all active days descending (best first) for Lorenz curve
    r_sorted     = np.sort(active)[::-1]
    cumulative   = np.cumsum(r_sorted)
    pct_days     = np.arange(1, n_active + 1) / n_active * 100

    # ── Top-N concentration table (allocator convention) ────────────────
    # ── Top-N concentration table ────────────────────────────────────────
    # Denominator: total_pos (gross positive return).
    # Using net PnL as denominator breaks above 100% when the top days contribute
    # more than the net (losses from remaining days pull net below gross+).
    # Gross+ denominator is stable, interpretable, and what most allocators
    # expect when they ask "what % of your winners are concentrated?".
    #
    # Allocator thresholds — calibrated for crypto systematic (higher concentration
    # is normal vs equity due to regime clustering and vol compression):
    #   Top 1%  < 20%   healthy        (equity benchmark: < 15%)
    #   Top 5%  < 45%   healthy        (equity benchmark: < 30%)
    #   Top 10% < 65%   healthy        (equity benchmark: < 50%)
    ALLOC_THRESHOLDS = {1: 20.0, 5: 45.0, 10: 65.0}

    rows_conc = []
    for pct in top_ns:
        k        = max(1, int(len(r_sorted) * pct / 100))
        top_ret  = float(np.sum(r_sorted[:k]))
        contrib  = top_ret / total_pos * 100 if total_pos > 0 else np.nan
        thresh   = ALLOC_THRESHOLDS.get(pct)
        if thresh is not None and np.isfinite(contrib):
            flag = "\u2713" if contrib < thresh else "\u2717"
        else:
            flag = ""
        rows_conc.append({
            "Top N%":          pct,
            "N days":          k,
            "Return sum":      round(top_ret, 4),
            "% of gross +":    round(contrib, 1),
            "Threshold":       thresh,
            "Flag":            flag,
        })

    # Gini coefficient — computed on positive days only.
    # Mixed-sign series break the standard formula (Gini assumes non-negative
    # values); applying it to a net-return series with losses produces values
    # outside [0, 1].  Winner-only Gini measures inequality among winning days.
    sorted_pos_asc = np.sort(positive) if len(positive) else np.array([0.0])
    n_pos  = len(sorted_pos_asc)
    idx_p  = np.arange(1, n_pos + 1)
    gini   = float((2 * np.sum(idx_p * sorted_pos_asc) - (n_pos + 1) * sorted_pos_asc.sum())
                   / (n_pos * sorted_pos_asc.sum())) if sorted_pos_asc.sum() > 0 else np.nan

    # ── Worst-day concentration ─────────────────────────────────────────
    # How much of total losses is explained by the worst 5% of days?
    # Healthy: worst 5% < 40% of total losses
    worst_sorted = np.sort(active)               # ascending (worst first)
    k_worst5     = max(1, int(np.ceil(n_active * 0.05)))
    worst5_sum   = float(worst_sorted[:k_worst5].sum())   # sum of worst 5% (negative)
    worst5_pct   = worst5_sum / total_neg * 100 if total_neg < 0 else np.nan
    # worst5_pct: 0–100, higher = losses more concentrated in a few bad days

    if   np.isfinite(worst5_pct) and worst5_pct < 40: worst_verdict = "healthy \u2014 losses distributed"
    elif np.isfinite(worst5_pct) and worst5_pct < 60: worst_verdict = "moderate \u2014 monitor large loss days"
    else:                                               worst_verdict = "CONCENTRATED \u2014 few days drive most losses"

    # ── PnL Half-Life ───────────────────────────────────────────────────
    # How many days (as % of active) needed to generate 50% of total PnL?
    # cumulative is sorted desc (best days first)
    half_target = total_pnl * 0.50
    half_idx    = int(np.searchsorted(cumulative, half_target))
    half_idx    = min(half_idx, n_active - 1)
    half_pct    = (half_idx + 1) / n_active * 100

    if   half_pct <= 15: halflife_verdict = "excellent \u2014 highly concentrated winners"
    elif half_pct <= 25: halflife_verdict = "normal for systematic strategies"
    elif half_pct <= 40: halflife_verdict = "moderate \u2014 returns moderately spread"
    else:                halflife_verdict = "DIFFUSE \u2014 no dominant return days"

    # ── Winners / losers Lorenz data ────────────────────────────────────
    # Winners curve: positive days sorted desc, cumulative as % of total_pos
    pos_sorted   = np.sort(positive)[::-1]
    pos_cum      = np.cumsum(pos_sorted)
    pos_pct_days = np.arange(1, len(pos_sorted) + 1) / len(pos_sorted) * 100 if len(pos_sorted) else np.array([])
    pos_cum_pct  = pos_cum / total_pos * 100 if total_pos > 0 else np.zeros_like(pos_cum)

    # Losers curve: negative days sorted asc (worst first), cumulative as % of |total_neg|
    neg_sorted   = np.sort(negative)           # ascending = worst first
    neg_cum      = np.cumsum(neg_sorted)
    neg_pct_days = np.arange(1, len(neg_sorted) + 1) / len(neg_sorted) * 100 if len(neg_sorted) else np.array([])
    neg_cum_pct  = neg_cum / total_neg * 100 if total_neg < 0 else np.zeros_like(neg_cum)

    # ── Concentration verdicts ──────────────────────────────────────────
    top5_row = next((r for r in rows_conc if r["Top N%"] == 5), None)
    top1_row = next((r for r in rows_conc if r["Top N%"] == 1), None)
    top5_pct = top5_row["% of gross +"] if top5_row else np.nan
    top1_pct = top1_row["% of gross +"] if top1_row else np.nan

    # Crypto-calibrated thresholds (regime clustering makes concentration higher
    # than equity; use gross+ denominator which is bounded and interpretable)
    if   np.isfinite(top5_pct) and top5_pct < 40:
        base_verdict = "DIVERSIFIED \u2014 return broadly distributed across winning days"
    elif np.isfinite(top5_pct) and top5_pct < 60:
        base_verdict = "Acceptable \u2014 moderate concentration, normal for crypto systematic"
    elif np.isfinite(top5_pct) and top5_pct < 75:
        base_verdict = "CONCENTRATED \u2014 top days dominate, monitor outlier dependency"
    else:
        base_verdict = "HIGH CONCENTRATION \u2014 strategy may be event-driven not systematic"

    # Half-life override: if top N% of days generate 50% of PnL within a very
    # short window, the Top-N table may understate true concentration because
    # gross+ spreads the denominator across all winning days.
    # Rule: if half_pct ≤ 5% of active days, bump verdict up one tier.
    if half_pct <= 5.0 and base_verdict.startswith("DIVERSIFIED"):
        conc_verdict = ("Acceptable \u2014 Top-N distributed but half-life very short "
                        f"(50%% of PnL in top {half_pct:.1f}% of days)")
    elif half_pct <= 5.0 and base_verdict.startswith("Acceptable"):
        conc_verdict = ("CONCENTRATED \u2014 Top-N acceptable but half-life very short "
                        f"(50%% of PnL in top {half_pct:.1f}% of days)")
    else:
        conc_verdict = base_verdict

    if   np.isfinite(gini) and gini < 0.30: gini_verdict = "low inequality"
    elif np.isfinite(gini) and gini < 0.50: gini_verdict = "moderate inequality"
    elif np.isfinite(gini) and gini < 0.70: gini_verdict = "high inequality"
    else:                                    gini_verdict = "extreme inequality"

    # ── Console output ──────────────────────────────────────────────────
    SEP = "-" * 66
    print(f"\n{'='*66}")
    print(f"  RETURN CONCENTRATION ANALYSIS")
    lbl = f"  Filter: {filter_label}  |  " if filter_label else "  "
    print(f"{lbl}Active days: {n_active}  |  Flat days: {n_flat}")
    print(f"  Total PnL: {total_pnl*100:+.2f}%   Gross+: {total_pos*100:.2f}%   Gross-: {total_neg*100:.2f}%")
    print(f"  {'='*66}")
    print(f"  Gini coefficient : {gini:.3f} (winners only)  \u2192 {gini_verdict}")
    print(f"  {SEP}")
    print(f"  {'Top N%':>7}  {'N days':>7}  {'% of gross +':>14}  {'Threshold':>10}  {'':>2}")
    print(f"  {SEP}")
    for r in rows_conc:
        thresh_str = f"< {r['Threshold']:.0f}%" if r["Threshold"] else "\u2014"
        print(f"  {r['Top N%']:>6}%  {r['N days']:>7}  "
              f"{r['% of gross +']:>13.1f}%  {thresh_str:>10}  {r['Flag']:>2}")
    print(f"  {SEP}")
    print(f"  Verdict  : {conc_verdict}")
    print(f"  {SEP}")
    print(f"  Worst-day concentration (worst 5% of days = {k_worst5} days)")
    print(f"    Worst 5% contribute : {worst5_pct:.1f}% of total losses  \u2192 {worst_verdict}")
    print(f"  {SEP}")
    print(f"  PnL Half-Life : top {half_pct:.1f}% of days generate 50% of total PnL")
    print(f"    ({half_idx+1} of {n_active} active days)  \u2192 {halflife_verdict}")
    print(f"{'='*66}\n")

    # ── Save CSV ────────────────────────────────────────────────────────
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_conc).to_csv(out_dir / "return_concentration.csv", index=False)
    pd.DataFrame({
        "rank":         np.arange(1, n_active + 1),
        "daily_return": r_sorted,
        "cum_return":   cumulative,
        "pct_days":     pct_days,
        "cum_pct_gross_pos": cumulative / total_pos * 100 if total_pos > 0 else np.full(n_active, np.nan),
    }).to_csv(out_dir / "return_concentration_days.csv", index=False)

    # ── Figure ──────────────────────────────────────────────────────────
    BG     = "#0d1117"
    PANEL  = "#161b22"
    ACCENT = "#58a6ff"
    GREEN  = "#3fb950"
    ORANGE = "#d29922"
    RED    = "#f85149"
    GOLD   = "#e3b341"
    MUTED  = "#8b949e"

    fig = plt.figure(figsize=(16, 9), facecolor=BG)
    fig.suptitle(
        f"Return Concentration  |  {filter_label + '  |  ' if filter_label else ''}"
        f"Gini={gini:.3f}  |  Active days={n_active}",
        color="white", fontsize=13, fontweight="bold", y=0.97,
    )

    gs = fig.add_gridspec(2, 2, hspace=0.42, wspace=0.32,
                          left=0.07, right=0.97, top=0.91, bottom=0.09)
    ax_lorenz  = fig.add_subplot(gs[:, 0])   # full left — combined Lorenz
    ax_bar     = fig.add_subplot(gs[0, 1])   # top-right — Top-N bar chart
    ax_winloss = fig.add_subplot(gs[1, 1])   # bottom-right — winners/losers curves

    for ax in (ax_lorenz, ax_bar, ax_winloss):
        ax.set_facecolor(PANEL)
        ax.tick_params(colors=MUTED, labelsize=8)
        for spine in ax.spines.values():
            spine.set_edgecolor("#30363d")

    # ── Lorenz curve (all days vs gross positive return) ─────────────────
    cum_pct_pos = cumulative / total_pos * 100 if total_pos > 0 else np.zeros(n_active)

    ax_lorenz.plot([0, 100], [0, 100], color=MUTED, lw=1, ls="--", label="Perfect equality")
    ax_lorenz.plot(pct_days, cum_pct_pos, color=ACCENT, lw=2.2, label="All active days")
    ax_lorenz.fill_between(pct_days, pct_days, cum_pct_pos, alpha=0.12, color=ACCENT)

    colors_ref = [GREEN, GOLD, ORANGE, RED, RED]
    for i, r in enumerate(rows_conc):
        xv = r["Top N%"]
        yv = r["% of gross +"]
        c  = colors_ref[i] if i < len(colors_ref) else MUTED
        ax_lorenz.axvline(xv, color=c, lw=0.8, ls=":", alpha=0.55)
        ax_lorenz.axhline(yv, color=c, lw=0.8, ls=":", alpha=0.55)
        ax_lorenz.scatter([xv], [yv], color=c, s=38, zorder=5)
        ax_lorenz.annotate(f"Top {xv}%\u2192{yv:.0f}%",
                           xy=(xv, yv), xytext=(xv + 1.5, yv - 9),
                           color=c, fontsize=7.2,
                           arrowprops=dict(arrowstyle="-", color=c, lw=0.6))

    ax_lorenz.set_xlim(0, 100)
    ax_lorenz.set_ylim(0, 115)
    ax_lorenz.set_xlabel("% of active trading days (best \u2192 worst)", color=MUTED, fontsize=9)
    ax_lorenz.set_ylabel("Cumulative % of gross positive return", color=MUTED, fontsize=9)
    ax_lorenz.set_title("Return Lorenz Curve  (denominator = gross positive return)", color="white", fontsize=10, pad=6)
    ax_lorenz.legend(fontsize=8, facecolor=PANEL, labelcolor="white", edgecolor="#30363d")

    # PnL half-life reference line
    ax_lorenz.axvline(half_pct, color=GOLD, lw=1.2, ls="-.", alpha=0.8)
    ax_lorenz.axhline(50,       color=GOLD, lw=1.2, ls="-.", alpha=0.8)
    ax_lorenz.annotate(f"50% PnL\n@ top {half_pct:.0f}%",
                       xy=(half_pct, 50), xytext=(min(half_pct + 3, 80), 42),
                       color=GOLD, fontsize=7.5,
                       arrowprops=dict(arrowstyle="-", color=GOLD, lw=0.7))

    ax_lorenz.text(0.97, 0.05, f"Gini = {gini:.3f}\n({gini_verdict})",
                   transform=ax_lorenz.transAxes, ha="right", va="bottom",
                   color=GOLD, fontsize=9, fontweight="bold")

    # ── Top-N bar chart with allocator thresholds ────────────────────────
    bar_labels = [f"Top {r['Top N%']}%" for r in rows_conc]
    bar_vals   = [r["% of gross +"] for r in rows_conc]
    bar_colors = []
    for r in rows_conc:
        v = r["% of gross +"]
        t = r["Threshold"]
        if t is None:
            bar_colors.append(ACCENT)
        elif v < t:
            bar_colors.append(GREEN)
        elif v < t * 1.3:
            bar_colors.append(ORANGE)
        else:
            bar_colors.append(RED)

    bars = ax_bar.bar(bar_labels, bar_vals, color=bar_colors, width=0.6,
                      edgecolor="#30363d", linewidth=0.5)
    for bar, r in zip(bars, rows_conc):
        val  = r["% of gross +"]
        flag = r["Flag"]
        ax_bar.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.8, f"{val:.1f}% {flag}",
                    ha="center", va="bottom", color="white", fontsize=8)

    # Allocator threshold reference lines
    thresh_drawn = set()
    for r in rows_conc:
        t = r["Threshold"]
        if t is not None and t not in thresh_drawn:
            ax_bar.axhline(t, color=MUTED, lw=0.7, ls="--", alpha=0.5)
            thresh_drawn.add(t)

    ax_bar.set_ylim(0, max(bar_vals) * 1.28 if bar_vals else 100)
    ax_bar.set_ylabel("% of gross positive return", color=MUTED, fontsize=8)
    ax_bar.set_title("Top-N Day Contribution  (% of gross+, crypto thresholds)", color="white", fontsize=10, pad=6)

    # ── Winners / Losers concentration curves ────────────────────────────
    ax_winloss.plot([0, 100], [0, 100], color=MUTED, lw=0.8, ls="--", label="Perfect equality")

    if len(pos_pct_days):
        ax_winloss.plot(pos_pct_days, pos_cum_pct, color=GREEN, lw=2.0,
                        label=f"Winners ({len(positive)} days, +{total_pos*100:.1f}%)")
        ax_winloss.fill_between(pos_pct_days, pos_pct_days, pos_cum_pct,
                                alpha=0.10, color=GREEN)
    if len(neg_pct_days):
        ax_winloss.plot(neg_pct_days, neg_cum_pct, color=RED, lw=2.0,
                        label=f"Losers  ({len(negative)} days, {total_neg*100:.1f}%)")
        ax_winloss.fill_between(neg_pct_days, neg_pct_days, neg_cum_pct,
                                alpha=0.10, color=RED)

    def _gini(vals):
        s = np.sort(vals)
        n = len(s)
        if n == 0 or s.sum() == 0: return np.nan
        idx = np.arange(1, n + 1)
        return float((2 * np.sum(idx * s) - (n + 1) * s.sum()) / (n * s.sum()))

    win_gini = _gini(positive)        if len(positive) else np.nan
    los_gini = _gini(np.abs(negative)) if len(negative) else np.nan

    ax_winloss.set_xlim(0, 100)
    ax_winloss.set_ylim(0, 115)
    ax_winloss.set_xlabel("% of days within group (best \u2192 worst)", color=MUTED, fontsize=8)
    ax_winloss.set_ylabel("Cumulative % of group total", color=MUTED, fontsize=8)
    ax_winloss.set_title("Winners vs Losers Concentration", color="white", fontsize=10, pad=6)
    ax_winloss.legend(fontsize=7.5, facecolor=PANEL, labelcolor="white", edgecolor="#30363d")
    ax_winloss.text(0.97, 0.05,
                    f"Win Gini:  {win_gini:.3f}\nLoss Gini: {los_gini:.3f}\n"
                    f"Worst 5%:  {worst5_pct:.1f}% of losses",
                    transform=ax_winloss.transAxes, ha="right", va="bottom",
                    color=MUTED, fontsize=8,
                    bbox=dict(facecolor=BG, edgecolor="#30363d", boxstyle="round,pad=0.3"))

    fig.text(0.5, 0.01, conc_verdict, ha="center", va="bottom",
             color=(GREEN  if "DIVERSIFIED"  in conc_verdict else
                    ORANGE if "CONCENTRATED" in conc_verdict else
                    RED    if "HIGH"         in conc_verdict else ACCENT),
             fontsize=10, fontweight="bold")

    plt.savefig(out_dir / "return_concentration.png", dpi=150,
                bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  [return concentration] saved \u2192 {out_dir / 'return_concentration.png'}")
# ── end run_return_concentration ─────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# SHARPE RIDGE MAP + SHARPE PLATEAU DETECTOR
# ══════════════════════════════════════════════════════════════════════════════
# Both functions operate on the in-memory `rows` list produced by
# run_param_surface(), so they run inline — no CSV reads, no extra simulate()
# calls.  They are called at the end of each surface's computation loop.
#
# Ridge Map:    for each row (fixed param_x) find best param_y; for each col
#               (fixed param_y) find best param_x.  Shows whether the optimum
#               forms a stable ridge or a needle spike.
#
# Plateau Detector: masks cells >= 95%/97.5%/99% of max Sharpe; finds the
#               largest contiguous (4-neighbor) cluster; reports whether the
#               baseline sits inside it.

def _surface_rows_to_grids(rows, param_x, param_y, values_x, values_y):
    """Convert run_param_surface() rows into metric grids (nx × ny)."""
    nx, ny = len(values_x), len(values_y)
    grids  = {m: np.full((nx, ny), np.nan)
               for m in ("Sharpe", "CAGR%", "MaxDD%", "Calmar", "WF_CV")}
    for r in rows:
        try:
            xi = next(i for i, v in enumerate(values_x) if abs(float(v) - float(r[param_x])) < 1e-9)
            yi = next(i for i, v in enumerate(values_y) if abs(float(v) - float(r[param_y])) < 1e-9)
        except StopIteration:
            continue
        for m in grids:
            val = r.get(m, np.nan)
            if val is not None and np.isfinite(float(val)):
                grids[m][xi, yi] = float(val)
    return grids


def _find_connected_components(mask):
    """4-neighbor connected components on boolean mask. Returns list of cell lists."""
    mask = np.array(mask, dtype=bool)
    h, w = mask.shape
    seen = np.zeros_like(mask, dtype=bool)
    comps = []
    for i in range(h):
        for j in range(w):
            if not mask[i, j] or seen[i, j]:
                continue
            stack = [(i, j)]
            seen[i, j] = True
            comp = []
            while stack:
                x, y = stack.pop()
                comp.append((x, y))
                for dx, dy in [(-1,0),(1,0),(0,-1),(0,1)]:
                    nx_, ny_ = x+dx, y+dy
                    if 0 <= nx_ < h and 0 <= ny_ < w and mask[nx_, ny_] and not seen[nx_, ny_]:
                        seen[nx_, ny_] = True
                        stack.append((nx_, ny_))
            comps.append(comp)
    return comps


def plot_sharpe_ridge_map(
    rows, param_x, param_y, values_x, values_y,
    out_dir, filter_label="", surface_label="",
    baseline_x=None, baseline_y=None,
):
    """
    Sharpe Ridge Map.

    Left panel  : Sharpe heatmap with row-ridge (green) and col-ridge (blue) overlaid.
    Top-right   : row-wise best param_y path as param_x varies.
    Bottom-right: col-wise best param_x path as param_y varies.

    Ridge stability verdict is based on how much the ridge path wanders
    (span of best-index positions across slices).
    """


    BG    = "#0d1117";  PANEL = "#161b22";  TEXT  = "#e6edf3"
    GRID  = "#21262d";  BLUE  = "#58a6ff";  GREEN = "#3fb950"
    GOLD  = "#f0e68c";  RED   = "#f85149"

    grids  = _surface_rows_to_grids(rows, param_x, param_y, values_x, values_y)
    sharpe = grids["Sharpe"]
    nx, ny = sharpe.shape

    # Row ridge: for each fixed x-row, which y-col has best Sharpe?
    row_best_y = []
    for i in range(nx):
        row = sharpe[i, :]
        row_best_y.append(int(np.nanargmax(row)) if not np.all(np.isnan(row)) else np.nan)

    # Col ridge: for each fixed y-col, which x-row has best Sharpe?
    col_best_x = []
    for j in range(ny):
        col = sharpe[:, j]
        col_best_x.append(int(np.nanargmax(col)) if not np.all(np.isnan(col)) else np.nan)

    valid_ry = [v for v in row_best_y if np.isfinite(v)]
    valid_cx = [v for v in col_best_x if np.isfinite(v)]
    row_span = (max(valid_ry) - min(valid_ry)) if valid_ry else np.nan
    col_span = (max(valid_cx) - min(valid_cx)) if valid_cx else np.nan
    row_std  = float(np.std(valid_ry)) if len(valid_ry) > 1 else np.nan
    col_std  = float(np.std(valid_cx)) if len(valid_cx) > 1 else np.nan

    if   np.isfinite(row_span) and row_span <= 1 and col_span <= 1: ridge_verdict = "Very stable ridge"
    elif np.isfinite(row_span) and row_span <= 2 and col_span <= 2: ridge_verdict = "Stable ridge"
    elif np.isfinite(row_span) and (row_span <= 3 or col_span <= 3): ridge_verdict = "Moderate ridge drift"
    else:                                                             ridge_verdict = "Sharp peak / unstable ridge"

    global_rng = float(np.nanmax(sharpe) - np.nanmin(sharpe))

    fig = plt.figure(figsize=(15, 10), facecolor=BG)
    gs  = fig.add_gridspec(2, 2, hspace=0.30, wspace=0.25)
    ax_heat = fig.add_subplot(gs[:, 0])
    ax_row  = fig.add_subplot(gs[0, 1])
    ax_col  = fig.add_subplot(gs[1, 1])

    for ax in (ax_heat, ax_row, ax_col):
        ax.set_facecolor(PANEL)
        for sp in ax.spines.values(): sp.set_color(GRID)
        ax.tick_params(colors=TEXT)

    # Heatmap
    cmap = plt.get_cmap("viridis").copy()
    cmap.set_bad(color="#2d333b")
    im = ax_heat.imshow(sharpe, cmap=cmap, aspect="auto", origin="upper", interpolation="nearest")
    for i in range(nx):
        for j in range(ny):
            v = sharpe[i, j]
            if np.isfinite(v):
                ax_heat.text(j, i, f"{v:.3f}", ha="center", va="center", color="white", fontsize=7)

    # Row ridge overlay
    ry_valid_x = [i for i, v in enumerate(row_best_y) if np.isfinite(v)]
    ry_valid_y = [v for v in row_best_y if np.isfinite(v)]
    if ry_valid_x:
        ax_heat.plot(ry_valid_y, ry_valid_x, color=GREEN, lw=2.0, marker="o", ms=4, label="Row ridge")

    # Col ridge overlay
    cx_valid_x = [j for j, v in enumerate(col_best_x) if np.isfinite(v)]
    cx_valid_y = [v for v in col_best_x if np.isfinite(v)]
    if cx_valid_x:
        ax_heat.plot(cx_valid_x, cx_valid_y, color=BLUE, lw=1.6, marker="s", ms=3, alpha=0.85, label="Col ridge")

    # Baseline marker
    bi = bj = None
    if baseline_x is not None and baseline_y is not None:
        try:
            bi = next(i for i, v in enumerate(values_x) if abs(float(v)-float(baseline_x)) < 1e-9)
            bj = next(j for j, v in enumerate(values_y) if abs(float(v)-float(baseline_y)) < 1e-9)
            ax_heat.scatter([bj], [bi], marker="*", s=180, color=GOLD,
                            edgecolor="black", lw=0.8, zorder=6, label="Baseline")
        except StopIteration:
            pass

    ax_heat.set_xticks(range(ny))
    ax_heat.set_xticklabels([f"{v:.4g}" for v in values_y], rotation=45, ha="right", color=TEXT, fontsize=8)
    ax_heat.set_yticks(range(nx))
    ax_heat.set_yticklabels([f"{v:.4g}" for v in values_x], color=TEXT, fontsize=8)
    ax_heat.set_xlabel(param_y, color=TEXT)
    ax_heat.set_ylabel(param_x, color=TEXT)
    ax_heat.set_title("Sharpe Surface with Ridge Overlays", color=TEXT, fontsize=11)
    cb = fig.colorbar(im, ax=ax_heat, fraction=0.046, pad=0.04)
    cb.ax.tick_params(colors=TEXT)
    cb.outline.set_edgecolor(GRID)
    ax_heat.legend(facecolor=PANEL, edgecolor=GRID, labelcolor=TEXT, fontsize=8, loc="lower right")

    # Row ridge panel
    ax_row.plot(range(nx), row_best_y, color=GREEN, marker="o", lw=1.8)
    ax_row.set_xticks(range(nx))
    ax_row.set_xticklabels([f"{v:.4g}" for v in values_x], color=TEXT, fontsize=8, rotation=45, ha="right")
    ax_row.set_yticks(range(ny))
    ax_row.set_yticklabels([f"{v:.4g}" for v in values_y], color=TEXT, fontsize=8)
    ax_row.set_xlabel(param_x, color=TEXT)
    ax_row.set_ylabel(f"Best {param_y}", color=TEXT)
    ax_row.set_title(f"Row-wise Best {param_y} Path  (span={row_span:.0f})", color=TEXT, fontsize=10)
    ax_row.grid(True, color=GRID, lw=0.5, alpha=0.6)

    # Col ridge panel
    ax_col.plot(range(ny), col_best_x, color=BLUE, marker="s", lw=1.8)
    ax_col.set_xticks(range(ny))
    ax_col.set_xticklabels([f"{v:.4g}" for v in values_y], color=TEXT, fontsize=8, rotation=45, ha="right")
    ax_col.set_yticks(range(nx))
    ax_col.set_yticklabels([f"{v:.4g}" for v in values_x], color=TEXT, fontsize=8)
    ax_col.set_xlabel(param_y, color=TEXT)
    ax_col.set_ylabel(f"Best {param_x}", color=TEXT)
    ax_col.set_title(f"Col-wise Best {param_x} Path  (span={col_span:.0f})", color=TEXT, fontsize=10)
    ax_col.grid(True, color=GRID, lw=0.5, alpha=0.6)

    fig.suptitle(
        f"Sharpe Ridge Map  \u2014  {param_x} \u00d7 {param_y}\n"
        f"Filter: {filter_label}   |   Sharpe range={global_rng:.3f}   |   {ridge_verdict}",
        color=TEXT, fontsize=13, fontweight="bold",
    )
    plt.tight_layout(rect=[0, 0, 1, 0.94])

    out_path = Path(out_dir) / f"sharpe_ridge_map_{surface_label or (param_x+'_'+param_y)}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"  Sharpe ridge map saved : {out_path}")
    print(f"  Ridge verdict          : {ridge_verdict}  "
          f"(row_span={row_span}  col_span={col_span}  "
          f"row_std={row_std:.2f}  col_std={col_std:.2f})")

    return {"row_span": row_span, "col_span": col_span,
            "row_std": row_std, "col_std": col_std,
            "ridge_verdict": ridge_verdict, "output_path": str(out_path)}


def plot_sharpe_plateau_detector(
    rows, param_x, param_y, values_x, values_y,
    out_dir, filter_label="", surface_label="",
    baseline_x=None, baseline_y=None,
    plateau_pct_list=None, min_cluster_cells=3,
):
    """
    Sharpe Plateau Detector.

    For each threshold level (95%, 97.5%, 99% of max Sharpe), shades cells
    above the threshold, finds the largest contiguous (4-neighbour) cluster,
    and reports whether the baseline sits inside it.

    A robust parameter region should show the baseline inside the 95% plateau
    with a connected cluster spanning multiple rows and columns.
    """

    from matplotlib.patches import Rectangle

    plateau_pct_list = plateau_pct_list or [0.95, 0.975, 0.99]

    BG    = "#0d1117";  PANEL  = "#161b22";  TEXT   = "#e6edf3"
    GRID  = "#21262d";  GOLD   = "#f0e68c";  GREEN  = "#3fb950"
    BLUE  = "#58a6ff";  RED    = "#f85149";  ORANGE = "#d29922"

    grids  = _surface_rows_to_grids(rows, param_x, param_y, values_x, values_y)
    sharpe = grids["Sharpe"]
    nx, ny = sharpe.shape
    sh_max = float(np.nanmax(sharpe))
    sh_min = float(np.nanmin(sharpe))

    bi = bj = None
    if baseline_x is not None and baseline_y is not None:
        try:
            # Use nearest-neighbour matching: baseline may not be exactly in the grid
            # (e.g. L_HIGH=1.33 between grid points 1.3 and 1.4)
            bx_f = float(baseline_x)
            by_f = float(baseline_y)
            fx   = [float(v) for v in values_x]
            fy   = [float(v) for v in values_y]
            bi_exact = next((i for i, v in enumerate(fx) if abs(v - bx_f) < 1e-9), None)
            bj_exact = next((j for j, v in enumerate(fy) if abs(v - by_f) < 1e-9), None)
            if bi_exact is not None:
                bi = bi_exact
            else:
                # Nearest neighbour — only accept if within 5% of grid spacing
                diffs = [abs(v - bx_f) for v in fx]
                nearest_i = int(np.argmin(diffs))
                spacing = abs(fx[1] - fx[0]) if len(fx) > 1 else 1.0
                if diffs[nearest_i] <= spacing * 0.6:
                    bi = nearest_i
            if bj_exact is not None:
                bj = bj_exact
            else:
                diffs = [abs(v - by_f) for v in fy]
                nearest_j = int(np.argmin(diffs))
                spacing = abs(fy[1] - fy[0]) if len(fy) > 1 else 1.0
                if diffs[nearest_j] <= spacing * 0.6:
                    bj = nearest_j
        except Exception:
            pass

    n_panels = len(plateau_pct_list)
    fig, axes = plt.subplots(1, n_panels, figsize=(5.5 * n_panels, 6.5), facecolor=BG)
    if n_panels == 1:
        axes = [axes]

    summary_rows = []

    for ax, pct in zip(axes, plateau_pct_list):
        ax.set_facecolor(PANEL)
        for sp in ax.spines.values(): sp.set_color(GRID)
        ax.tick_params(colors=TEXT)

        thr  = sh_max * pct
        mask = np.isfinite(sharpe) & (sharpe >= thr)

        comps   = _find_connected_components(mask)
        largest = max(comps, key=len) if comps else []
        largest_set = set(largest)

        im = ax.imshow(sharpe, cmap="viridis", aspect="auto", origin="upper", interpolation="nearest")
        for i in range(nx):
            for j in range(ny):
                v = sharpe[i, j]
                if np.isfinite(v):
                    ax.text(j, i, f"{v:.3f}", ha="center", va="center", color="white", fontsize=7)

        # All above-threshold cells — blue border
        for i in range(nx):
            for j in range(ny):
                if mask[i, j]:
                    ax.add_patch(Rectangle((j-.5, i-.5), 1, 1,
                                           fill=False, edgecolor=BLUE, lw=1.0, alpha=0.7))

        # Largest contiguous cluster — green border (thicker)
        for i, j in largest:
            ax.add_patch(Rectangle((j-.5, i-.5), 1, 1,
                                   fill=False, edgecolor=GREEN, lw=2.2))

        # Baseline marker
        baseline_in = False
        if bi is not None and bj is not None:
            baseline_in = (bi, bj) in largest_set
            ax.scatter([bj], [bi], marker="*", s=180, color=GOLD,
                       edgecolor="black", lw=0.8, zorder=6)

        n_cells   = int(mask.sum())
        n_finite  = int(np.isfinite(sharpe).sum())
        pct_cells = n_cells / n_finite * 100 if n_finite else np.nan
        n_largest = len(largest)

        if   n_largest >= min_cluster_cells: pv = "Robust plateau"
        elif n_cells >= 2:                   pv = "Thin plateau"
        else:                                pv = "Single-cell peak"
        pv += " | baseline inside" if baseline_in else " | baseline outside"

        ax.set_xticks(range(ny))
        ax.set_xticklabels([f"{v:.4g}" for v in values_y],
                           rotation=45, ha="right", color=TEXT, fontsize=8)
        ax.set_yticks(range(nx))
        ax.set_yticklabels([f"{v:.4g}" for v in values_x], color=TEXT, fontsize=8)
        ax.set_xlabel(param_y, color=TEXT)
        ax.set_ylabel(param_x, color=TEXT)
        ax.set_title(
            f"\u2265 {pct*100:.1f}% of max  (thr={thr:.3f})\n"
            f"cells={n_cells}  cluster={n_largest}  ({pct_cells:.0f}% of surface)",
            color=TEXT, fontsize=10,
        )
        vc = GREEN if "Robust" in pv else (ORANGE if "Thin" in pv else RED)
        ax.text(0.5, -0.22, pv, transform=ax.transAxes, ha="center", va="top",
                color=vc, fontsize=8, fontweight="bold")

        summary_rows.append({
            "threshold_pct":          pct,
            "threshold_sharpe":       round(thr, 4),
            "cells_above_threshold":  n_cells,
            "pct_of_surface":         round(pct_cells, 2),
            "largest_cluster_cells":  n_largest,
            "baseline_in_cluster":    baseline_in,
            "verdict":                pv,
        })

    fig.suptitle(
        f"Sharpe Plateau Detector  \u2014  {param_x} \u00d7 {param_y}\n"
        f"Filter: {filter_label}   |   Max={sh_max:.3f}   Min={sh_min:.3f}",
        color=TEXT, fontsize=13, fontweight="bold",
    )
    plt.tight_layout(rect=[0, 0, 1, 0.92])

    lbl = surface_label or f"{param_x}_{param_y}"
    out_path = Path(out_dir) / f"sharpe_plateau_detector_{lbl}.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)

    pd.DataFrame(summary_rows).to_csv(
        Path(out_dir) / f"sharpe_plateau_detector_{lbl}.csv", index=False)

    print(f"  Sharpe plateau detector: {out_path}")
    for r in summary_rows:
        print(f"    \u2265{r['threshold_pct']*100:.1f}% max: "
              f"cells={r['cells_above_threshold']}  "
              f"cluster={r['largest_cluster_cells']}  "
              f"baseline_in={r['baseline_in_cluster']}  "
              f"\u2192 {r['verdict']}")

    return {"output_path": str(out_path), "summary": summary_rows}

# ── end run_sharpe_ridge_map / run_sharpe_plateau_detector ───────────────────

# ══════════════════════════════════════════════════════════════════════════════
# PARAM JITTER / SHARPE STABILITY TEST
# ══════════════════════════════════════════════════════════════════════════════
#
# Simultaneously perturbs all listed parameters around the baseline using
# independent uniform draws within each parameter's jitter band.  Runs
# N_TRIALS simulate() calls (stdout suppressed) and plots the resulting
# Sharpe distribution as a histogram with key percentile statistics.
#
# Outputs
#   parameter_sweeps/param_jitter_sharpe.png   — histogram + stats panel
#   parameter_sweeps/param_jitter_results.csv  — all trials (params + metrics)



# ══════════════════════════════════════════════════════════════════════
# TOP-N DAY REMOVAL TEST
# ══════════════════════════════════════════════════════════════════════
# Sequentially removes the top-N best return days and recomputes CAGR,
# Sharpe, MaxDD. Answers: how dependent is the edge on a handful of
# outlier days? Complements the Lorenz/concentration curve by showing
# the metric impact directly rather than just the PnL share.
#
# Outputs:
#   top_n_removal_results.csv
#   top_n_removal.png   (CAGR, Sharpe, MaxDD vs N removed)

def run_top_n_removal(
    daily_with_zeros: "np.ndarray",
    filter_label:     str  = "",
    out_dir:          "Path" = None,
    ns:               list = None,
    tdy:              int  = 365,
):

    import csv as _csv
    import math as _math

    if ns is None:
        ns = [1, 3, 5, 10]

    r = np.asarray(daily_with_zeros, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) < max(ns) + 5:
        print("  ⚠  Top-N removal: insufficient data — skipping.")
        return

    def _eq(arr):  return np.cumprod(1.0 + arr)
    def _mdd(arr):
        eq = _eq(arr); pk = np.maximum.accumulate(eq)
        return float(np.min((eq - pk) / pk) * 100)
    def _sh(arr):
        if len(arr) < 2 or arr.std(ddof=1) == 0: return float("nan")
        return float(arr.mean() / arr.std(ddof=1) * _math.sqrt(tdy))
    def _ca(arr):
        eq = _eq(arr)
        if eq[-1] <= 0: return float("nan")
        return float((eq[-1] ** (tdy / len(arr)) - 1) * 100)

    sorted_idx  = np.argsort(r)[::-1]   # best days first
    base_sh     = _sh(r)
    base_mdd    = _mdd(r)
    base_ca     = _ca(r)

    print(f"\n{'='*74}")
    print(f"  TOP-N DAY REMOVAL TEST  |  Filter: {filter_label}")
    print(f"{'='*74}")
    print(f"  Baseline  Sharpe={base_sh:.3f}  CAGR={base_ca:.1f}%  MaxDD={base_mdd:.2f}%")
    print()
    print(f"  {'N removed':>10}  {'Days removed':>40}  {'Sharpe':>8}  "
          f"{'ΔSharpe':>8}  {'CAGR%':>8}  {'MaxDD%':>8}")
    print(f"  {'─'*90}")

    rows = [dict(n_removed=0, days_removed="",
                 sharpe=round(base_sh,4), cagr_pct=round(base_ca,2),
                 maxdd_pct=round(base_mdd,3),
                 delta_sharpe=0.0, delta_cagr_pct=0.0)]

    for n in ns:
        if n > len(r): continue
        remove_idx = set(sorted_idx[:n])
        mask  = np.array([i not in remove_idx for i in range(len(r))])
        r_adj = r[mask]

        removed_vals = r[sorted_idx[:n]]
        days_str = ", ".join(f"{v*100:.1f}%" for v in removed_vals[:5])
        if n > 5: days_str += f" … +{n-5} more"

        sh  = _sh(r_adj)
        mdd = _mdd(r_adj)
        ca  = _ca(r_adj)
        d_sh = sh - base_sh if _math.isfinite(sh) else float("nan")
        d_ca = ca - base_ca if _math.isfinite(ca) else float("nan")

        print(f"  {n:>10}  {days_str:>40}  {sh:>8.3f}  {d_sh:>+8.3f}  "
              f"{ca:>8.1f}%  {mdd:>8.2f}%")

        rows.append(dict(n_removed=n, days_removed=days_str,
                         sharpe=round(sh,4) if _math.isfinite(sh) else float("nan"),
                         cagr_pct=round(ca,2) if _math.isfinite(ca) else float("nan"),
                         maxdd_pct=round(mdd,3) if _math.isfinite(mdd) else float("nan"),
                         delta_sharpe=round(d_sh,4) if _math.isfinite(d_sh) else float("nan"),
                         delta_cagr_pct=round(d_ca,2) if _math.isfinite(d_ca) else float("nan"))
              )

    print(f"{'='*74}")

    if out_dir is None:
        return
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "top_n_removal_results.csv"
    with open(csv_path, "w", newline="") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  CSV saved: {csv_path}")

    try:
        FIG_BG   = "#0d1117"
        PANEL_BG = "#161b22"
        TEXT_COL = "#e6edf3"

        plot_rows = [r2 for r2 in rows if r2["n_removed"] > 0]
        xs  = [r2["n_removed"]  for r2 in plot_rows]
        shs = [r2["sharpe"]     for r2 in plot_rows]
        cas = [r2["cagr_pct"]   for r2 in plot_rows]
        mds = [r2["maxdd_pct"]  for r2 in plot_rows]

        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(14, 5), facecolor=FIG_BG)
        for ax, ys, base, col, ylabel in [
            (ax1, shs, base_sh,  "#58a6ff", "Sharpe Ratio"),
            (ax2, cas, base_ca,  "#3fb950", "CAGR %"),
            (ax3, mds, base_mdd, "#ff7b72", "Max Drawdown %"),
        ]:
            ax.set_facecolor(PANEL_BG)
            ax.tick_params(colors=TEXT_COL)
            for sp in ax.spines.values(): sp.set_edgecolor(TEXT_COL)
            ax.plot(xs, ys, marker="o", color=col, lw=2.0)
            ax.axhline(base, color="#e6edf3", lw=1.0, ls="--", alpha=0.6,
                       label="Baseline")
            ax.set_xlabel("Best days removed", color=TEXT_COL)
            ax.set_ylabel(ylabel,              color=TEXT_COL)
            ax.set_title(ylabel,               color=TEXT_COL)
            ax.legend(facecolor=PANEL_BG, labelcolor=TEXT_COL, fontsize=8)
            ax.xaxis.label.set_color(TEXT_COL)

        fig.suptitle(f"Top-N Day Removal  |  {filter_label}",
                     color=TEXT_COL, fontsize=12)
        plt.tight_layout()
        png_path = out_dir / "top_n_removal.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Chart saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Chart failed: {e}")


# ══════════════════════════════════════════════════════════════════════
# LUCKY STREAK TEST
# ══════════════════════════════════════════════════════════════════════
# Splits the return series into non-overlapping 30-day blocks, ranks
# them by compounded return, then zeros out the best 1/2/3 blocks and
# recomputes full-sample CAGR and Sharpe.
#
# Answers: how much of the total return depends on the single luckiest
# month? Distinct from top-N day removal (day-level) — this is
# month-level streak sensitivity.
#
# Outputs:
#   lucky_streak_results.csv
#   lucky_streak.png

def run_lucky_streak(
    daily_with_zeros: "np.ndarray",
    filter_label:     str  = "",
    out_dir:          "Path" = None,
    window:           int  = 30,
    tdy:              int  = 365,
):

    import csv as _csv
    import math as _math

    r = np.asarray(daily_with_zeros, dtype=float)
    r = r[np.isfinite(r)]
    n_blocks = len(r) // window

    if n_blocks < 2:
        print("  ⚠  Lucky streak: fewer than 2 blocks — skipping.")
        return

    def _eq(arr):  return np.cumprod(1.0 + arr)
    def _mdd(arr):
        eq = _eq(arr); pk = np.maximum.accumulate(eq)
        return float(np.min((eq - pk) / pk) * 100)
    def _sh(arr):
        if len(arr) < 2 or arr.std(ddof=1) == 0: return float("nan")
        return float(arr.mean() / arr.std(ddof=1) * _math.sqrt(tdy))
    def _ca(arr):
        eq = _eq(arr)
        if eq[-1] <= 0: return float("nan")
        return float((eq[-1] ** (tdy / len(arr)) - 1) * 100)

    # Rank blocks by compounded return
    block_rets = []
    for i in range(n_blocks):
        blk = r[i*window:(i+1)*window]
        eq  = _eq(blk)
        block_rets.append((i, float(eq[-1]/eq[0] - 1.0)))
    block_rets.sort(key=lambda x: x[1], reverse=True)

    base_sh  = _sh(r)
    base_ca  = _ca(r)
    base_mdd = _mdd(r)

    print(f"\n{'='*74}")
    print(f"  LUCKY STREAK TEST  |  Filter: {filter_label}  |  window={window}d")
    print(f"{'='*74}")
    print(f"  {n_blocks} non-overlapping {window}-day blocks")
    print(f"  Baseline  Sharpe={base_sh:.3f}  CAGR={base_ca:.1f}%  MaxDD={base_mdd:.2f}%")
    print()
    print(f"  {'Blocks removed':>15}  {'Block returns zeroed':>30}  "
          f"{'Sharpe':>8}  {'ΔSharpe':>8}  {'CAGR%':>8}  {'ΔCalmar':>8}")
    print(f"  {'─'*80}")

    rows = [dict(blocks_removed=0, removed_returns="",
                 sharpe=round(base_sh,4), cagr_pct=round(base_ca,2),
                 maxdd_pct=round(base_mdd,3),
                 delta_sharpe=0.0, delta_cagr_pct=0.0)]

    for k in range(1, min(4, n_blocks)):
        removed_indices = set(block_rets[i][0] for i in range(k))
        r_adj = r.copy()
        removed_rets = []
        for bi in sorted(removed_indices):
            ret = block_rets[[b[0] for b in block_rets].index(bi)][1]
            removed_rets.append(ret)
            r_adj[bi*window:(bi+1)*window] = 0.0

        sh  = _sh(r_adj)
        ca  = _ca(r_adj)
        mdd = _mdd(r_adj)
        d_sh = sh - base_sh if _math.isfinite(sh) else float("nan")
        d_ca = ca - base_ca if _math.isfinite(ca) else float("nan")

        ret_str = ", ".join(f"+{v*100:.1f}%" for v in
                            sorted(removed_rets, reverse=True))

        print(f"  {k:>15}  {ret_str:>30}  {sh:>8.3f}  {d_sh:>+8.3f}  "
              f"{ca:>8.1f}%  {d_ca:>+8.1f}%")

        rows.append(dict(blocks_removed=k, removed_returns=ret_str,
                         sharpe=round(sh,4) if _math.isfinite(sh) else float("nan"),
                         cagr_pct=round(ca,2) if _math.isfinite(ca) else float("nan"),
                         maxdd_pct=round(mdd,3) if _math.isfinite(mdd) else float("nan"),
                         delta_sharpe=round(d_sh,4) if _math.isfinite(d_sh) else float("nan"),
                         delta_cagr_pct=round(d_ca,2) if _math.isfinite(d_ca) else float("nan"))
              )

    print(f"{'='*74}")

    if out_dir is None:
        return
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "lucky_streak_results.csv"
    with open(csv_path, "w", newline="") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  CSV saved: {csv_path}")

    try:
        FIG_BG   = "#0d1117"
        PANEL_BG = "#161b22"
        TEXT_COL = "#e6edf3"

        plot_rows = [r2 for r2 in rows if r2["blocks_removed"] > 0]
        xs  = [r2["blocks_removed"] for r2 in plot_rows]
        shs = [r2["sharpe"]         for r2 in plot_rows]
        cas = [r2["cagr_pct"]       for r2 in plot_rows]

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5), facecolor=FIG_BG)
        for ax, ys, base, col, ylabel in [
            (ax1, shs, base_sh, "#58a6ff", "Sharpe Ratio"),
            (ax2, cas, base_ca, "#3fb950", "CAGR %"),
        ]:
            ax.set_facecolor(PANEL_BG)
            ax.tick_params(colors=TEXT_COL)
            for sp in ax.spines.values(): sp.set_edgecolor(TEXT_COL)
            ax.bar(xs, ys, color=col, alpha=0.80, width=0.5)
            ax.axhline(base, color="#e6edf3", lw=1.2, ls="--", alpha=0.7,
                       label="Baseline")
            ax.set_xticks(xs)
            ax.set_xticklabels([f"Remove\nbest {i}" for i in xs],
                               color=TEXT_COL, fontsize=9)
            ax.set_ylabel(ylabel, color=TEXT_COL)
            ax.set_title(ylabel,  color=TEXT_COL)
            ax.legend(facecolor=PANEL_BG, labelcolor=TEXT_COL, fontsize=8)
            ax.xaxis.label.set_color(TEXT_COL)

        fig.suptitle(f"Lucky Streak Test  (window={window}d)  |  {filter_label}",
                     color=TEXT_COL, fontsize=12)
        plt.tight_layout()
        png_path = out_dir / "lucky_streak.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Chart saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Chart failed: {e}")



# ══════════════════════════════════════════════════════════════════════
# WEEKLY / MONTHLY MILESTONE TABLES
# ══════════════════════════════════════════════════════════════════════
# Rolls up per-day fees_rows from simulate() into weekly and monthly
# buckets and prints: period end date, closing account balance,
# period net PnL ($), period net ROI (%), and cumulative ROI (%).
# One toggle each: ENABLE_WEEKLY_MILESTONES / ENABLE_MONTHLY_MILESTONES

def _milestone_table(
    fees_rows:       list,
    freq:            str,          # "W" (weekly) or "M" (monthly)
    starting_capital: float,
    label:           str = "",
):
    """
    fees_rows: list of (date_str, eq_start, margin, lev, invested, tvol,
                        taker_fee, funding, eq_end, ret_gross, ret_net, pnl)
    Rows where eq_start < 0 are sentinels (no-entry / ratchet-off) — skipped
    for PnL but equity_end is still used as the running balance.
    """
    import pandas as _pd2

    if not fees_rows:
        print(f"  ⚠  {freq} milestones: no fees_rows data available.")
        return

    # Build a dated equity series from fees_rows
    rows = []
    for r in fees_rows:
        try:
            dt    = _pd2.Timestamp(r[0]).normalize()
            eq_s  = float(r[1])
            eq_e  = float(r[8])
            pnl   = float(r[11])
            rows.append((dt, eq_s, eq_e, pnl))
        except Exception:
            continue
    if not rows:
        return

    df = _pd2.DataFrame(rows, columns=["date", "eq_start", "eq_end", "pnl"])
    df = df.set_index("date").sort_index()

    # Resample: last equity_end of the period, sum of pnl
    period_key = "W-SUN" if freq == "W" else "ME"
    eq_close   = df["eq_end"].resample(period_key).last().dropna()
    pnl_sum    = df["pnl"].resample(period_key).sum()

    if eq_close.empty:
        print(f"  ⚠  {freq} milestones: insufficient data to bucket.")
        return

    freq_label = "WEEKLY" if freq == "W" else "MONTHLY"
    col_w      = 74
    hdr_period = "Week ending" if freq == "W" else "Month ending"

    print(f"\n{'='*col_w}")
    print(f"  {freq_label} MILESTONES  |  {label}  |  capital=${starting_capital:,.0f}")
    print(f"{'='*col_w}")
    print(f"  {'Period':<14}  {'Balance ($)':>14}  {'Net PnL ($)':>13}"
          f"  {'Period ROI':>11}  {'Cum ROI':>10}")
    print(f"  {'-'*14}  {'-'*14}  {'-'*13}  {'-'*11}  {'-'*10}")

    prev_eq = starting_capital
    for dt, eq in eq_close.items():
        period_pnl = float(pnl_sum.get(dt, 0.0))
        period_roi = period_pnl / prev_eq * 100.0 if prev_eq != 0 else 0.0
        cum_roi    = (eq - starting_capital) / starting_capital * 100.0
        date_str   = dt.strftime("%Y-%m-%d")
        sign_p     = "+" if period_pnl >= 0 else ""
        sign_r     = "+" if period_roi >= 0 else ""
        sign_c     = "+" if cum_roi    >= 0 else ""
        print(f"  {date_str:<14}  {eq:>14,.2f}  {sign_p}{period_pnl:>12,.2f}"
              f"  {sign_r}{period_roi:>10.2f}%  {sign_c}{cum_roi:>9.2f}%")
        prev_eq = eq

    # Summary footer
    final_eq   = float(eq_close.iloc[-1])
    total_pnl  = final_eq - starting_capital
    total_roi  = total_pnl / starting_capital * 100.0
    n_periods  = len(eq_close)
    n_pos      = int((pnl_sum > 0).sum())
    n_neg      = int((pnl_sum < 0).sum())
    print(f"  {'='*14}  {'='*14}  {'='*13}  {'='*11}  {'='*10}")
    print(f"  {'TOTAL':<14}  {final_eq:>14,.2f}  {total_pnl:>+13,.2f}"
          f"  {total_roi:>+11.2f}%  {'':>10}")
    print(f"  {n_periods} periods  |  {n_pos} positive  |  {n_neg} negative  |  "
          f"win rate {n_pos/n_periods*100:.1f}%")
    print(f"{'='*col_w}\n")

# ══════════════════════════════════════════════════════════════════════
# PERIODIC RETURN BREAKDOWN
# ══════════════════════════════════════════════════════════════════════
# Aggregates daily returns into weekly and monthly compounded buckets
# and reports win rate, avg win/loss, best/worst for each period.
# Gives a human-readable rhythm of the strategy's profitability.
#
# Output: console table only (no chart — the numbers are self-contained)

def run_periodic_return_breakdown(
    daily_with_zeros: "np.ndarray",
    filter_label:     str = "",
    out_dir:          "Path" = None,
    tdy:              int = 365,
    days_per_week:    int = 7,
    days_per_month:   int = 30,
):
    import math as _math
    import csv as _csv

    r = np.asarray(daily_with_zeros, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) < days_per_week * 2:
        print("  ⚠  Periodic breakdown: insufficient data — skipping.")
        return

    def _bucket(arr, bucket):
        out = []
        for i in range(0, len(arr) - bucket + 1, bucket):
            out.append(float(np.prod(1.0 + arr[i:i+bucket]) - 1.0))
        return np.array(out, dtype=float)

    def _stats(arr):
        if len(arr) == 0:
            nan = float("nan")
            return dict(n=0, avg=nan, best=nan, worst=nan,
                        win_rate=nan, avg_win=nan, avg_loss=nan,
                        n_wins=0, n_losses=0)
        wins = arr[arr > 0]; loss = arr[arr < 0]
        n_wins = int(np.sum(arr > 0))
        n_losses = int(np.sum(arr < 0))
        n_decided = n_wins + n_losses
        return dict(
            n        = len(arr),
            avg      = float(np.mean(arr)),
            best     = float(np.max(arr)),
            worst    = float(np.min(arr)),
            win_rate = float(n_wins / n_decided) if n_decided > 0 else float("nan"),
            avg_win  = float(np.mean(wins)) if len(wins) else float("nan"),
            avg_loss = float(np.mean(loss)) if len(loss) else float("nan"),
            n_wins   = n_wins,
            n_losses = n_losses,
        )

    daily   = _stats(r)
    weekly  = _stats(_bucket(r, days_per_week))
    monthly = _stats(_bucket(r, days_per_month))

    def _pct(v, plus=False):
        if not _math.isfinite(v): return "   n/a"
        return f"{v*100:+.2f}%" if plus else f"{v*100:.2f}%"

    print(f"\n{'='*74}")
    print(f"  PERIODIC RETURN BREAKDOWN  |  Filter: {filter_label}")
    print(f"{'='*74}")

    for label, s, period in [
        ("MONTHLY",  monthly, f"{monthly['n']} months"),
        ("WEEKLY",   weekly,  f"{weekly['n']} weeks"),
        ("DAILY",    daily,   f"{daily['n']} days"),
    ]:
        print(f"\n  {label}  ({period})")
        print(f"    Win rate : {s['win_rate']*100:.1f}%  "
              f"({s['n_wins']}W / {s['n_losses']}L)")
        print(f"    Avg      : {_pct(s['avg'], plus=True)}")
        if _math.isfinite(s['avg_win']):
            print(f"    Avg win  : {_pct(s['avg_win'])}")
        if _math.isfinite(s['avg_loss']):
            print(f"    Avg loss : {_pct(s['avg_loss'])}")
        print(f"    Best     : {_pct(s['best'])}   "
              f"Worst: {_pct(s['worst'])}")

    print(f"\n{'='*74}")

    if out_dir is not None:
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / "periodic_return_breakdown.csv"
            with open(csv_path, "w", newline="") as fh:
                fields = ["period", "n", "win_rate", "avg", "avg_win",
                          "avg_loss", "best", "worst", "n_wins", "n_losses"]
                w = _csv.DictWriter(fh, fieldnames=fields)
                w.writeheader()
                for lbl, s in [("daily", daily), ("weekly", weekly),
                                ("monthly", monthly)]:
                    w.writerow({"period": lbl, **{k: round(v, 6)
                                if _math.isfinite(v) else float("nan")
                                for k, v in s.items()
                                if k not in ("n_wins","n_losses")},
                                "n_wins": s["n_wins"],
                                "n_losses": s["n_losses"]})
            print(f"  CSV saved: {csv_path}")
        except Exception as e:
            print(f"  ⚠  Could not save periodic breakdown: {e}")

# ══════════════════════════════════════════════════════════════════════
# MINIMUM CUMULATIVE RETURN TABLE
# ══════════════════════════════════════════════════════════════════════
# For each window length N, slides a contiguous N-day window across the
# full daily_with_zeros series and records the MINIMUM arithmetic
# (non-compounding) cumulative return — i.e. the worst-case outcome for
# a capital allocator who enters at the worst possible day.
#
# Non-compounding = sum(r[t:t+N]), which is what a fixed-notional
# allocator experiences and what most risk budgets are measured against.
#
# Output: printed table + CSV saved to out_dir.

def run_min_cumulative_return(
    daily_with_zeros: "np.ndarray",
    windows:          list,
    filter_label:     str  = "",
    out_dir:          "Path | None" = None,
) -> dict:
    """
    Compute minimum fixed (non-compounding) cumulative return over every
    contiguous window of length N for each N in `windows`.

    Returns a dict mapping window_days -> (min_cum_ret, window_start_idx).
    Prints a formatted table and optionally saves a CSV.
    """
    import csv as _csv

    r = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
    n = len(r)

    label_str = f" [{filter_label}]" if filter_label else ""
    print(f"\n{'═'*62}")
    print(f"  MINIMUM CUMULATIVE RETURN (non-compounding){label_str}")
    print(f"{'═'*62}")
    print(f"  {'Window':>8}  {'Min Cum Ret':>12}  {'Worst Start':>11}  {'Worst End':>11}")
    print(f"  {'-'*8}  {'-'*12}  {'-'*11}  {'-'*11}")

    results = {}
    for w in sorted(windows):
        if w > n:
            print(f"  {w:>7}d  {'—':>12}  (insufficient data: {n} days)")
            results[w] = None
            continue
        # Sliding sum via cumsum for efficiency
        cs   = np.concatenate(([0.0], np.cumsum(r)))
        sums = cs[w:] - cs[:n - w + 1]
        idx  = int(np.argmin(sums))
        min_val = float(sums[idx])
        results[w] = {"min_cum_ret": min_val, "start_idx": idx, "end_idx": idx + w - 1}
        print(f"  {w:>7}d  {min_val:>+11.2%}  {idx:>11d}  {idx + w - 1:>11d}")

    print(f"{'═'*62}\n")

    if out_dir is not None:
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / "min_cumulative_return.csv"
            rows = []
            for w in sorted(windows):
                if results[w] is None:
                    rows.append({"window_days": w, "min_cum_ret": "", "start_idx": "", "end_idx": ""})
                else:
                    v = results[w]
                    rows.append({"window_days": w,
                                 "min_cum_ret": f"{v['min_cum_ret']:.6f}",
                                 "start_idx":   v["start_idx"],
                                 "end_idx":     v["end_idx"]})
            with open(csv_path, "w", newline="") as fh:
                w_ = _csv.DictWriter(fh, fieldnames=["window_days", "min_cum_ret", "start_idx", "end_idx"])
                w_.writeheader()
                w_.writerows(rows)
            print(f"  CSV saved: {csv_path}")
        except Exception as e:
            print(f"  ⚠  Could not save min cumulative return CSV: {e}")

    return results


# ══════════════════════════════════════════════════════════════════════
# DEFLATED SHARPE RATIO + MINIMUM TRACK RECORD LENGTH
# ══════════════════════════════════════════════════════════════════════
# Bailey & Lopez de Prado (2014).
#
# DSR answers: given how many parameter configurations were tested,
# what is the probability that the observed Sharpe is a genuine edge
# rather than a selection artifact (false discovery)?
#
# MTL answers: how many live trading days are needed before the
# observed Sharpe is statistically credible at 95% confidence?
#
# Both are printed as part of the existing scorecard output and
# require no additional simulate() calls — they operate on the
# already-computed daily return series and trial count.
#
# Inputs:
#   observed_sharpe   annualised Sharpe from the main simulation
#   n_days            number of trading days in the sample
#   n_trials          total number of configurations tested across all
#                     sweeps (param surfaces + jitter trials + cube)
#   skewness          return skewness (from distribution stats)
#   excess_kurtosis   excess kurtosis (from distribution stats)
#
# ENABLE flag: ENABLE_DSR_MTL  (default True, fast — no sim calls)

def run_dsr_mtl(
    daily_returns:    "np.ndarray",
    observed_sharpe:  float,
    n_trials:         int,
    filter_label:     str  = "",
    out_dir:          "Path" = None,
    target_sharpe:    float = 1.0,
    confidence:       float = 0.95,
    tdy:              int   = 365,
):
    """
    Deflated Sharpe Ratio and Minimum Track Record Length.
    Operates on existing daily returns — no simulate() calls.
    """
    import math as _math
    from scipy import stats as _stats

    r   = np.asarray(daily_returns, dtype=float)
    r   = r[np.isfinite(r)]
    n   = len(r)

    if n < 10 or not _math.isfinite(observed_sharpe):
        print("  ⚠  DSR/MTL: insufficient data — skipping.")
        return

    # Distribution moments
    skew = float(_stats.skew(r))
    kurt = float(_stats.kurtosis(r, fisher=True))   # excess kurtosis

    # ── Deflated Sharpe Ratio ─────────────────────────────────────────
    euler_gamma  = 0.5772156649
    if n_trials > 1:
        sr_benchmark = (
            (1.0 - euler_gamma) * _stats.norm.ppf(1.0 - 1.0 / n_trials)
            + euler_gamma       * _stats.norm.ppf(1.0 - 1.0 / (n_trials * _math.e))
        )
    else:
        sr_benchmark = 0.0

    sr_hat  = observed_sharpe / _math.sqrt(tdy)   # daily-scale Sharpe
    var_sr  = (1.0 / n) * (
        1.0
        + 0.5  * sr_hat ** 2
        - skew * sr_hat
        + (kurt / 4.0) * sr_hat ** 2
    )
    var_sr  = max(var_sr, 1.0 / n)
    sr_std  = _math.sqrt(var_sr) * _math.sqrt(tdy)

    z_dsr   = (observed_sharpe - sr_benchmark) / sr_std if sr_std > 0 else float("nan")
    dsr     = float(_stats.norm.cdf(z_dsr))   if _math.isfinite(z_dsr) else float("nan")
    p_false = 1.0 - dsr                        if _math.isfinite(dsr)   else float("nan")

    # ── Minimum Track Record Length ───────────────────────────────────
    sr_daily      = observed_sharpe / _math.sqrt(tdy)
    sr_star_daily = target_sharpe   / _math.sqrt(tdy)
    z_alpha       = float(_stats.norm.ppf(confidence))
    bracket       = 1.0 + 0.5 * sr_daily**2 - skew * sr_daily + (kurt / 4.0) * sr_daily**2
    if observed_sharpe > target_sharpe and (sr_daily - sr_star_daily) > 1e-9:
        mtl = 1.0 + bracket * (z_alpha / (sr_daily - sr_star_daily)) ** 2
    else:
        mtl = float("nan")

    # ── Print ─────────────────────────────────────────────────────────
    print(f"\n{'='*74}")
    print(f"  DEFLATED SHARPE RATIO + MINIMUM TRACK RECORD LENGTH")
    print(f"  Filter: {filter_label}  |  n_days={n}  n_trials={n_trials}")
    print(f"{'='*74}")
    print(f"  Return distribution:")
    print(f"    Skewness        : {skew:>+.4f}")
    print(f"    Excess kurtosis : {kurt:>+.4f}")
    print()
    print(f"  Deflated Sharpe Ratio (DSR):")
    print(f"    Observed Sharpe : {observed_sharpe:.4f}")
    print(f"    SR benchmark    : {sr_benchmark:.4f}  "
          f"(expected max from {n_trials} trials)")
    print(f"    SR std error    : {sr_std:.4f}")
    print(f"    Z-score         : {z_dsr:>+.3f}")
    print(f"    DSR             : {dsr:.4f}  "
          f"(prob. of genuine edge)")
    print(f"    P(false pos.)   : {p_false:.4f}")

    if   dsr >= 0.99: dsr_verdict = "PASS — near-certain genuine edge"
    elif dsr >= 0.95: dsr_verdict = "PASS — strong evidence of genuine edge"
    elif dsr >= 0.90: dsr_verdict = "MARGINAL — reduce trial count or increase sample"
    else:              dsr_verdict = "FAIL — high false-discovery risk"
    print(f"    Verdict         : {dsr_verdict}")

    print()
    print(f"  Minimum Track Record Length (MTL @ {confidence*100:.0f}% conf, "
          f"target SR={target_sharpe:.1f}):")
    if _math.isfinite(mtl):
        print(f"    MTL             : {mtl:.1f} days  ({mtl/tdy*12:.1f} months)")
        if n >= mtl:
            mtl_verdict = f"PASS — {n} days observed ≥ {mtl:.0f} days required"
        else:
            mtl_verdict = f"INSUFFICIENT — need {mtl:.0f} days, have {n}"
        print(f"    Verdict         : {mtl_verdict}")
    else:
        print(f"    MTL             : n/a (observed Sharpe ≤ target)")

    print(f"{'='*74}")

    # ── Save results ──────────────────────────────────────────────────
    if out_dir is not None:
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            txt_path = out_dir / "dsr_mtl_results.txt"
            with open(txt_path, "w") as fh:
                fh.write(f"DSR/MTL RESULTS  |  Filter: {filter_label}\n")
                fh.write(f"n_days          : {n}\n")
                fh.write(f"n_trials        : {n_trials}\n")
                fh.write(f"observed_sharpe : {observed_sharpe:.4f}\n")
                fh.write(f"skewness        : {skew:.4f}\n")
                fh.write(f"excess_kurtosis : {kurt:.4f}\n")
                fh.write(f"sr_benchmark    : {sr_benchmark:.4f}\n")
                fh.write(f"dsr             : {dsr:.4f}\n")
                fh.write(f"prob_false_pos  : {p_false:.4f}\n")
                fh.write(f"dsr_verdict     : {dsr_verdict}\n")
                fh.write(f"mtl_days        : {mtl:.1f}\n" if _math.isfinite(mtl) else "mtl_days        : n/a\n")
                if _math.isfinite(mtl):
                    fh.write(f"mtl_verdict     : {mtl_verdict}\n")
            print(f"  Results saved: {txt_path}")
        except Exception as e:
            print(f"  ⚠  Could not save DSR/MTL results: {e}")


# ══════════════════════════════════════════════════════════════════════
# SHOCK INJECTION TEST
# ══════════════════════════════════════════════════════════════════════
# Injects N consecutive artificial loss days of size X% into the
# existing return series at the worst-drawdown cluster and measures
# whether the strategy survives (MaxDD stays above -50%).
#
# This is explicit stress testing — distinct from noise perturbation
# (which adds random microstructure noise) and slippage sweep (which
# adjusts costs). This asks: what happens if we get a correlated crash
# sequence of a given severity?
#
# Outputs:
#   shock_injection_results.csv
#   shock_injection.png   (Sharpe and MaxDD vs n_shocks, by shock size)

def run_shock_injection(
    daily_with_zeros: "np.ndarray",
    filter_label:     str  = "",
    out_dir:          "Path" = None,
    shock_sizes:      list = None,
    n_shocks_list:    list = None,
    ruin_threshold:   float = -50.0,   # MaxDD% below this = ruin
    tdy:              int   = 365,
):

    import csv as _csv
    import math as _math

    if shock_sizes   is None: shock_sizes    = [-10.0, -20.0, -30.0, -50.0]
    if n_shocks_list is None: n_shocks_list  = [1, 3, 5]

    r = np.asarray(daily_with_zeros, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) < 20:
        print("  ⚠  Shock injection: insufficient data — skipping.")
        return

    def _eq(arr):   return np.cumprod(1.0 + arr)
    def _mdd(arr):
        eq = _eq(arr); pk = np.maximum.accumulate(eq)
        return float(np.min((eq - pk) / pk) * 100)
    def _sh(arr):
        if len(arr) < 2 or arr.std(ddof=1) == 0: return float("nan")
        return float(arr.mean() / arr.std(ddof=1) * _math.sqrt(tdy))
    def _ca(arr):
        eq = _eq(arr)
        if eq[-1] <= 0: return float("nan")
        return float((eq[-1] ** (tdy / len(arr)) - 1) * 100)

    # Baseline
    base_sh  = _sh(r)
    base_mdd = _mdd(r)
    base_ca  = _ca(r)

    # Worst drawdown trough index
    eq_b = _eq(r); pk_b = np.maximum.accumulate(eq_b)
    dd_b = (eq_b - pk_b) / pk_b
    worst_idx = int(np.argmin(dd_b))

    print(f"\n{'='*74}")
    print(f"  SHOCK INJECTION TEST  |  Filter: {filter_label}")
    print(f"{'='*74}")
    print(f"  Baseline  Sharpe={base_sh:.3f}  CAGR={base_ca:.1f}%  MaxDD={base_mdd:.2f}%")
    print(f"  Inject at: worst-drawdown cluster (idx={worst_idx})")
    print(f"  Ruin threshold: MaxDD ≤ {ruin_threshold:.0f}%")
    print()
    print(f"  {'Shock%':>8}  {'N days':>7}  {'Sharpe':>8}  {'ΔSharpe':>8}  "
          f"{'CAGR%':>8}  {'MaxDD%':>8}  {'ΔMaxDD%':>8}  {'Status':>10}")
    print(f"  {'─'*74}")

    rows = [dict(shock_pct=0, n_shocks=0, sharpe=round(base_sh,4),
                 cagr_pct=round(base_ca,2), maxdd_pct=round(base_mdd,3),
                 delta_sharpe=0.0, delta_maxdd_pct=0.0, survived=True)]

    for shock in shock_sizes:
        for ns in n_shocks_list:
            r_adj = r.copy()
            insert_at = max(0, worst_idx - ns)
            for pos in range(insert_at, min(insert_at + ns, len(r_adj))):
                r_adj[pos] = shock / 100.0

            sh  = _sh(r_adj)
            mdd = _mdd(r_adj)
            ca  = _ca(r_adj)
            d_sh  = sh  - base_sh  if _math.isfinite(sh)  else float("nan")
            d_mdd = mdd - base_mdd if _math.isfinite(mdd) else float("nan")
            surv  = mdd > ruin_threshold if _math.isfinite(mdd) else False
            flag  = "✓ survive" if surv else "✗ RUIN"

            print(f"  {shock:>7.0f}%  {ns:>7d}  {sh:>8.3f}  {d_sh:>+8.3f}  "
                  f"{ca:>8.1f}%  {mdd:>8.2f}%  {d_mdd:>+8.2f}%  {flag:>10}")

            rows.append(dict(shock_pct=shock, n_shocks=ns,
                             sharpe=round(sh,4) if _math.isfinite(sh) else float("nan"),
                             cagr_pct=round(ca,2) if _math.isfinite(ca) else float("nan"),
                             maxdd_pct=round(mdd,3) if _math.isfinite(mdd) else float("nan"),
                             delta_sharpe=round(d_sh,4) if _math.isfinite(d_sh) else float("nan"),
                             delta_maxdd_pct=round(d_mdd,3) if _math.isfinite(d_mdd) else float("nan"),
                             survived=surv))

    n_survive = sum(1 for r2 in rows[1:] if r2["survived"])
    n_total   = len(rows) - 1
    print(f"\n  Survival rate: {n_survive}/{n_total} scenarios")
    print(f"{'='*74}")

    if out_dir is None:
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    # CSV
    csv_path = out_dir / "shock_injection_results.csv"
    with open(csv_path, "w", newline="") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  CSV saved: {csv_path}")

    # Chart
    try:
        FIG_BG   = "#0d1117"
        PANEL_BG = "#161b22"
        TEXT_COL = "#e6edf3"

        plot_rows = [r2 for r2 in rows if r2["n_shocks"] > 0]
        unique_shocks = sorted(set(r2["shock_pct"] for r2 in plot_rows))
        colors = plt.cm.Reds(np.linspace(0.4, 0.95, len(unique_shocks)))

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5), facecolor=FIG_BG)
        for ax in (ax1, ax2):
            ax.set_facecolor(PANEL_BG)
            ax.tick_params(colors=TEXT_COL)
            for sp in ax.spines.values(): sp.set_edgecolor(TEXT_COL)
            ax.xaxis.label.set_color(TEXT_COL)
            ax.yaxis.label.set_color(TEXT_COL)
            ax.title.set_color(TEXT_COL)

        for shock, col in zip(unique_shocks, colors):
            sub = sorted([r2 for r2 in plot_rows if r2["shock_pct"] == shock],
                         key=lambda x: x["n_shocks"])
            xs  = [r2["n_shocks"]  for r2 in sub]
            shs = [r2["sharpe"]    for r2 in sub]
            mds = [r2["maxdd_pct"] for r2 in sub]
            ax1.plot(xs, shs, marker="o", color=col, lw=1.8,
                     label=f"{shock:.0f}% shock")
            ax2.plot(xs, mds, marker="o", color=col, lw=1.8,
                     label=f"{shock:.0f}% shock")

        ax1.axhline(base_sh,        color="#3fb950", lw=1.0, ls="-.",
                    label=f"Baseline ({base_sh:.3f})")
        ax1.axhline(2.0,            color="#f0883e", lw=1.0, ls="--",
                    label="Floor 2.0")
        ax2.axhline(base_mdd,       color="#3fb950", lw=1.0, ls="-.",
                    label=f"Baseline ({base_mdd:.1f}%)")
        ax2.axhline(ruin_threshold, color="#ff7b72", lw=1.0, ls="--",
                    label=f"Ruin ({ruin_threshold:.0f}%)")

        ax1.set_xlabel("Shock days injected", color=TEXT_COL)
        ax1.set_ylabel("Sharpe Ratio",        color=TEXT_COL)
        ax1.set_title("Sharpe vs Shock",      color=TEXT_COL)
        ax2.set_xlabel("Shock days injected", color=TEXT_COL)
        ax2.set_ylabel("MaxDD %",             color=TEXT_COL)
        ax2.set_title("MaxDD vs Shock",       color=TEXT_COL)
        for ax in (ax1, ax2):
            ax.legend(facecolor=PANEL_BG, labelcolor=TEXT_COL, fontsize=8)

        fig.suptitle(f"Shock Injection — worst-cluster mode  |  {filter_label}",
                     color=TEXT_COL, fontsize=12)
        plt.tight_layout()
        png_path = out_dir / "shock_injection.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Chart saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Chart failed: {e}")


# ══════════════════════════════════════════════════════════════════════
# RUIN PROBABILITY
# ══════════════════════════════════════════════════════════════════════
# Monte Carlo bootstrap of the probability that a cumulative drawdown
# hits ruin_threshold within horizon_days, given the empirical return
# distribution. Draws with replacement from actual daily returns.
#
# Two thresholds:
#   -50%  — catastrophic loss (typical institutional hard stop)
#   -75%  — near-total loss
#
# Output: single printed table + ruin_probability_results.txt

def run_ruin_probability(
    daily_with_zeros: "np.ndarray",
    filter_label:     str   = "",
    out_dir:          "Path" = None,
    ruin_thresholds:  list  = None,
    horizon_days:     int   = 365,
    n_sims:           int   = 10_000,
    seed:             int   = 42,
):
    import math as _math

    if ruin_thresholds is None:
        ruin_thresholds = [-0.25, -0.50, -0.75]

    r   = np.asarray(daily_with_zeros, dtype=float)
    r   = r[np.isfinite(r)]
    n_r = len(r)

    if n_r < 20:
        print("  ⚠  Ruin probability: insufficient data — skipping.")
        return

    rng = np.random.default_rng(seed)

    print(f"\n{'='*74}")
    print(f"  RUIN PROBABILITY  |  Filter: {filter_label}")
    print(f"{'='*74}")
    print(f"  Bootstrap draws  : {n_sims:,} simulations × {horizon_days} days")
    print(f"  Sample size      : {n_r} daily returns")
    print()
    print(f"  {'Threshold':>12}  {'P(ruin)':>10}  {'1-in-N':>12}  {'Verdict':>20}")
    print(f"  {'─'*60}")

    results = []
    for thresh in ruin_thresholds:
        ruin_count = 0
        for _ in range(n_sims):
            idx  = rng.integers(0, n_r, size=horizon_days)
            samp = r[idx]
            eq   = np.cumprod(1.0 + samp)
            pk   = np.maximum.accumulate(eq)
            dd   = eq / pk - 1.0
            if np.any(dd <= thresh):
                ruin_count += 1

        prob = ruin_count / n_sims
        one_in_n = f"1-in-{1/prob:.0f}" if prob > 0 else "never"

        if   prob == 0.0:      verdict = "NEGLIGIBLE"
        elif prob < 0.01:      verdict = "VERY LOW"
        elif prob < 0.05:      verdict = "LOW"
        elif prob < 0.10:      verdict = "MODERATE"
        elif prob < 0.25:      verdict = "ELEVATED"
        else:                   verdict = "HIGH — review risk controls"

        print(f"  {thresh*100:>11.0f}%  {prob:>10.4f}  {one_in_n:>12}  {verdict:>20}")
        results.append(dict(threshold_pct=thresh*100, prob=round(prob,5),
                            verdict=verdict))

    print(f"{'='*74}")

    if out_dir is not None:
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            txt_path = out_dir / "ruin_probability_results.txt"
            with open(txt_path, "w") as fh:
                fh.write(f"RUIN PROBABILITY  |  Filter: {filter_label}\n")
                fh.write(f"n_sims={n_sims}  horizon_days={horizon_days}  "
                         f"sample_size={n_r}\n\n")
                for res in results:
                    fh.write(f"Threshold {res['threshold_pct']:.0f}%: "
                             f"P(ruin)={res['prob']:.5f}  {res['verdict']}\n")
            print(f"  Results saved: {txt_path}")
        except Exception as e:
            print(f"  ⚠  Could not save ruin results: {e}")

    return results


# ══════════════════════════════════════════════════════════════════════
# PARAMETRIC STABILITY CUBE  —  L_BASE × L_HIGH × VOL_LEV_MAX_BOOST
# ══════════════════════════════════════════════════════════════════════
# Exhaustive 3-D grid over the three leverage-architecture parameters.
# Motivation: the 2-D surfaces can miss interactions that only appear
# when a third parameter shifts; the cube makes those visible.
#
# For every (L_BASE, L_HIGH, VOL_LEV_MAX_BOOST) triplet:
#   • Runs simulate() with the full tail-guardrail filter
#   • Records Sharpe, CAGR, MaxDD, Calmar, WF_CV
#
# Outputs (in out_dir):
#   stability_cube_results.csv       — all cells
#   stability_cube_lbase_slices.png  — Sharpe heatmap for each L_BASE slice
#   stability_cube_summary.txt       — plateau stats + verdict
#
# Stability verdict:
#   ≥ 80% of cells within 95% of peak Sharpe → ROBUST PLATEAU
#   50–79%                                   → MODERATE SENSITIVITY
#   < 50%                                    → HIGH SENSITIVITY

def run_parametric_stability_cube(
    df_4x,
    base_params:    dict,
    filter_mode:    str,
    v3_filter,
    symbol_counts,
    vol_lev_params: dict,          # base vol-lev dict (max_boost will be overridden)
    out_dir:        Path,
    filter_label:   str  = "",
    values_lbase:   list = None,
    values_lhigh:   list = None,
    values_boost:   list = None,
):



    import csv as _csv

    # ── Grid defaults ──────────────────────────────────────────────────
    if values_lbase is None:
        values_lbase = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    if values_lhigh is None:
        values_lhigh = [1.0, 1.2, 1.3, 1.4, 1.5, 1.6, 1.8, 2.0]
    if values_boost is None:
        values_boost = [1.0, 1.2, 1.4, 1.5, 1.6, 1.7, 2.0, 2.5]

    bl_lbase  = base_params.get("L_BASE",         0.8)
    bl_lhigh  = base_params.get("L_HIGH",         1.4)
    bl_boost  = (vol_lev_params or {}).get("max_boost", 1.7)
    n_total   = len(df_4x.columns)
    n_cells   = len(values_lbase) * len(values_lhigh) * len(values_boost)

    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"

    print(f"\n{'='*74}")
    print(f"  PARAMETRIC STABILITY CUBE  —  L_BASE × L_HIGH × VOL_LEV_MAX_BOOST")
    print(f"  Filter : {filter_label}  |  Grid: {len(values_lbase)}×{len(values_lhigh)}×{len(values_boost)} = {n_cells} cells")
    print(f"  Baseline: L_BASE={bl_lbase}  L_HIGH={bl_lhigh}  BOOST={bl_boost}")
    print(f"{'='*74}")

    rows = []
    done = 0
    for lb in values_lbase:
        for lh in values_lhigh:
            # Skip physically invalid combinations (base > high)
            if lb >= lh:
                done += len(values_boost)
                continue
            for bst in values_boost:
                params = deepcopy(base_params)
                params["L_BASE"] = lb
                params["L_HIGH"] = lh
                vp = deepcopy(vol_lev_params) if vol_lev_params else {}
                vp["max_boost"] = bst

                out = simulate(df_4x, params, filter_mode, v3_filter,
                               symbol_counts=symbol_counts,
                               vol_lev_params=vp,
                               verbose=False)
                daily = np.array(out["daily"], dtype=float)

                sh  = _sweep_sharpe(daily)
                md  = _sweep_maxdd(daily)
                ca  = _sweep_cagr(daily, n_total)
                cal = ca / abs(md) if (np.isfinite(md) and abs(md) > 1e-9) else np.nan
                cv  = _sweep_wf_cv(_sweep_folds(daily))
                is_bl = (abs(lb - bl_lbase) < 1e-9 and
                         abs(lh - bl_lhigh) < 1e-9 and
                         abs(bst - bl_boost) < 1e-9)

                rows.append(dict(
                    L_BASE=lb, L_HIGH=lh, VOL_LEV_MAX_BOOST=bst,
                    Sharpe=round(sh,  4) if np.isfinite(sh)  else np.nan,
                    CAGR_pct=round(ca, 2) if np.isfinite(ca) else np.nan,
                    MaxDD_pct=round(md, 3) if np.isfinite(md) else np.nan,
                    Calmar=round(cal, 3)   if np.isfinite(cal) else np.nan,
                    WF_CV=round(cv,  4)    if np.isfinite(cv)  else np.nan,
                    baseline=is_bl,
                ))
                done += 1
                bl_tag = " ◄ BASELINE" if is_bl else ""
                print(f"  [{done:>4}/{n_cells}]  L_BASE={lb:.2f}  L_HIGH={lh:.2f}"
                      f"  BOOST={bst:.2f}  |  Sharpe={sh:>6.3f}"
                      f"  CAGR={ca:>7.1f}%  MaxDD={md:>7.2f}%{bl_tag}")

    if not rows:
        print("  ⚠  No valid cells — check grid values.")
        return

    # ── Plateau analysis ───────────────────────────────────────────────
    sharpes   = np.array([r["Sharpe"] for r in rows], dtype=float)
    peak_sh   = float(np.nanmax(sharpes))
    peak_row  = rows[int(np.nanargmax(sharpes))]
    n_valid   = int(np.sum(np.isfinite(sharpes)))
    n_p95     = int(np.sum(sharpes >= 0.95 * peak_sh))
    n_p90     = int(np.sum(sharpes >= 0.90 * peak_sh))
    bl_row    = next((r for r in rows if r["baseline"]), None)
    bl_sh     = bl_row["Sharpe"] if bl_row else float("nan")

    if   n_p95 / n_valid >= 0.80: verdict = "ROBUST PLATEAU"
    elif n_p95 / n_valid >= 0.50: verdict = "MODERATE SENSITIVITY"
    else:                          verdict = "HIGH SENSITIVITY"

    print(f"\n{'='*74}")
    print(f"  STABILITY CUBE SUMMARY")
    print(f"{'='*74}")
    print(f"  Cells evaluated  : {n_valid} / {n_cells}")
    print(f"  Peak Sharpe      : {peak_sh:.3f}  at  L_BASE={peak_row['L_BASE']}"
          f"  L_HIGH={peak_row['L_HIGH']}  BOOST={peak_row['VOL_LEV_MAX_BOOST']}")
    print(f"  Baseline Sharpe  : {bl_sh:.3f}  ({bl_sh/peak_sh*100:.1f}% of peak)")
    print(f"  Plateau ≥95% pk  : {n_p95}/{n_valid}  ({n_p95/n_valid*100:.1f}%)")
    print(f"  Plateau ≥90% pk  : {n_p90}/{n_valid}  ({n_p90/n_valid*100:.1f}%)")
    print(f"  Sharpe std       : {float(np.nanstd(sharpes)):.4f}")
    print(f"  Verdict          : {verdict}")

    # ── Save CSV ───────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "stability_cube_results.csv"
    with open(csv_path, "w", newline="") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  CSV saved: {csv_path}")

    # ── Save summary text ──────────────────────────────────────────────
    txt_path = out_dir / "stability_cube_summary.txt"
    with open(txt_path, "w") as fh:
        fh.write(f"PARAMETRIC STABILITY CUBE — L_BASE × L_HIGH × VOL_LEV_MAX_BOOST\n")
        fh.write(f"Filter        : {filter_label}\n")
        fh.write(f"Grid          : {len(values_lbase)}×{len(values_lhigh)}×{len(values_boost)} = {n_cells} cells\n")
        fh.write(f"Cells valid   : {n_valid}\n")
        fh.write(f"Peak Sharpe   : {peak_sh:.4f}  at  L_BASE={peak_row['L_BASE']}"
                 f"  L_HIGH={peak_row['L_HIGH']}  BOOST={peak_row['VOL_LEV_MAX_BOOST']}\n")
        fh.write(f"Baseline Sharpe: {bl_sh:.4f}\n")
        fh.write(f"Plateau ≥95%  : {n_p95}/{n_valid}  ({n_p95/n_valid*100:.1f}%)\n")
        fh.write(f"Verdict       : {verdict}\n")
    print(f"  Summary saved: {txt_path}")

    # ── Slice heatmaps: one per L_BASE value ──────────────────────────
    try:
        n_slices = len(values_lbase)
        ncols    = min(3, n_slices)
        nrows_fig = math.ceil(n_slices / ncols)
        fig, axes = plt.subplots(nrows_fig, ncols,
                                 figsize=(6 * ncols, 5 * nrows_fig),
                                 facecolor=FIG_BG)
        axes_flat = np.array(axes).flatten() if n_slices > 1 else [axes]
        fig.suptitle(f"Stability Cube — Sharpe  |  Filter: {filter_label}",
                     color=TEXT_COL, fontsize=13, y=1.01)

        cmap = plt.cm.RdYlGn
        all_sh = [r["Sharpe"] for r in rows if np.isfinite(r["Sharpe"])]
        vmin, vmax = (min(all_sh), max(all_sh)) if all_sh else (0, 1)

        for si, lb in enumerate(values_lbase):
            ax = axes_flat[si]
            ax.set_facecolor(PANEL_BG)
            slice_rows = [r for r in rows if abs(r["L_BASE"] - lb) < 1e-9]

            grid = np.full((len(values_lhigh), len(values_boost)), np.nan)
            for r in slice_rows:
                yi = next((i for i, v in enumerate(values_lhigh)
                           if abs(v - r["L_HIGH"]) < 1e-9), None)
                xi = next((i for i, v in enumerate(values_boost)
                           if abs(v - r["VOL_LEV_MAX_BOOST"]) < 1e-9), None)
                if yi is not None and xi is not None and np.isfinite(r["Sharpe"]):
                    grid[yi, xi] = r["Sharpe"]

            im = ax.imshow(grid, cmap=cmap, vmin=vmin, vmax=vmax,
                           aspect="auto", origin="lower")

            # Annotate cells
            for yi in range(len(values_lhigh)):
                for xi in range(len(values_boost)):
                    val = grid[yi, xi]
                    if np.isfinite(val):
                        r_match = next((r for r in slice_rows
                                        if abs(r["L_HIGH"] - values_lhigh[yi]) < 1e-9
                                        and abs(r["VOL_LEV_MAX_BOOST"] - values_boost[xi]) < 1e-9), None)
                        is_bl_cell = r_match["baseline"] if r_match else False
                        is_pk_cell = (abs(val - peak_sh) < 1e-4)
                        txt = f"{val:.3f}"
                        if is_bl_cell: txt += "\n★"
                        if is_pk_cell: txt += "\n▲"
                        ax.text(xi, yi, txt, ha="center", va="center",
                                color="white", fontsize=7,
                                fontweight="bold" if (is_bl_cell or is_pk_cell) else "normal")

            ax.set_xticks(range(len(values_boost)))
            ax.set_xticklabels([f"{v:.2f}" for v in values_boost],
                               color=TEXT_COL, fontsize=8)
            ax.set_yticks(range(len(values_lhigh)))
            ax.set_yticklabels([f"{v:.2f}" for v in values_lhigh],
                               color=TEXT_COL, fontsize=8)
            ax.set_xlabel("VOL_LEV_MAX_BOOST", color=TEXT_COL, fontsize=9)
            ax.set_ylabel("L_HIGH",            color=TEXT_COL, fontsize=9)
            ax.set_title(f"L_BASE = {lb:.2f}", color=TEXT_COL, fontsize=10)
            ax.tick_params(colors=TEXT_COL)
            for spine in ax.spines.values():
                spine.set_edgecolor(TEXT_COL)
            plt.colorbar(im, ax=ax).ax.yaxis.set_tick_params(color=TEXT_COL)

        for si in range(n_slices, len(axes_flat)):
            axes_flat[si].set_visible(False)

        plt.tight_layout()
        png_path = out_dir / "stability_cube_lbase_slices.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Heatmap saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Heatmap failed: {e}")

    print(f"{'='*74}")


# ══════════════════════════════════════════════════════════════════════
# RISK THROTTLE STABILITY CUBE
# ══════════════════════════════════════════════════════════════════════
# 3-D exhaustive grid: EARLY_FILL_Y × EARLY_KILL_Y × VOL_LEV_MAX_BOOST
#
# Outputs (written to stability_cube_risk_throttle/ inside sweep_dir):
#   stability_cube_results.csv   — all cells
#   stability_cube_summary.txt   — plateau stats + verdict
#   stability_cube_heatmap.png   — Sharpe heatmaps sliced by EARLY_FILL_Y
#
# Stability verdict (same thresholds as leverage cube):
#   ≥ 80% of cells within 95% of peak Sharpe → ROBUST PLATEAU
#   50–79%                                   → MODERATE SENSITIVITY
#   < 50%                                    → HIGH SENSITIVITY

def run_risk_throttle_stability_cube(
    df_4x,
    base_params:    dict,
    filter_mode:    str,
    v3_filter,
    symbol_counts,
    vol_lev_params: dict,
    out_dir:        Path,
    filter_label:   str  = "",
    values_fill_y:  list = None,
    values_kill_y:  list = None,
    values_boost:   list = None,
):
    import csv as _csv

    if values_fill_y is None:
        values_fill_y = [0.10, 0.15, 0.20, 0.25, 0.30]
    if values_kill_y is None:
        values_kill_y = [0.001, 0.0015, 0.002, 0.0025]
    if values_boost is None:
        values_boost  = [1.2, 1.4, 1.7, 2.0]

    bl_fill  = float(base_params.get("EARLY_FILL_Y", 0.20))
    bl_kill  = float(base_params.get("EARLY_KILL_Y", 0.002))
    bl_boost = float((vol_lev_params or {}).get("max_boost", 1.7))
    n_total  = len(df_4x.columns)
    n_cells  = len(values_fill_y) * len(values_kill_y) * len(values_boost)

    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"

    print(f"\n{'='*74}")
    print(f"  RISK THROTTLE STABILITY CUBE  —  EARLY_FILL_Y × EARLY_KILL_Y × BOOST")
    print(f"  Filter : {filter_label}  |  Grid: "
          f"{len(values_fill_y)}×{len(values_kill_y)}×{len(values_boost)} = {n_cells} cells")
    print(f"  Baseline: FILL_Y={bl_fill}  KILL_Y={bl_kill}  BOOST={bl_boost}")
    print(f"{'='*74}")

    rows = []
    done = 0
    for fy in values_fill_y:
        for ky in values_kill_y:
            for bst in values_boost:
                params = deepcopy(base_params)
                params["EARLY_FILL_Y"] = fy
                params["EARLY_KILL_Y"] = ky
                vp = deepcopy(vol_lev_params) if vol_lev_params else {}
                vp["max_boost"] = bst

                out = simulate(df_4x, params, filter_mode, v3_filter,
                               symbol_counts=symbol_counts,
                               vol_lev_params=vp,
                               verbose=False)
                daily = np.array(out["daily"], dtype=float)

                sh  = _sweep_sharpe(daily)
                md  = _sweep_maxdd(daily)
                ca  = _sweep_cagr(daily, n_total)
                cal = ca / abs(md) if (np.isfinite(md) and abs(md) > 1e-9) else np.nan
                cv  = _sweep_wf_cv(_sweep_folds(daily))
                is_bl = (abs(fy  - bl_fill)  < 1e-9 and
                         abs(ky  - bl_kill)  < 1e-9 and
                         abs(bst - bl_boost) < 1e-9)

                rows.append(dict(
                    EARLY_FILL_Y=fy, EARLY_KILL_Y=ky, VOL_LEV_MAX_BOOST=bst,
                    Sharpe=round(sh,  4) if np.isfinite(sh)  else np.nan,
                    CAGR_pct=round(ca, 2) if np.isfinite(ca) else np.nan,
                    MaxDD_pct=round(md, 3) if np.isfinite(md) else np.nan,
                    Calmar=round(cal, 3)   if np.isfinite(cal) else np.nan,
                    WF_CV=round(cv,  4)    if np.isfinite(cv)  else np.nan,
                    baseline=is_bl,
                ))
                done += 1
                bl_tag = " ◄ BASELINE" if is_bl else ""
                print(f"  [{done:>4}/{n_cells}]  FILL_Y={fy:.4f}  KILL_Y={ky:.4f}"
                      f"  BOOST={bst:.2f}  |  Sharpe={sh:>6.3f}"
                      f"  CAGR={ca:>7.1f}%  MaxDD={md:>7.2f}%{bl_tag}")

    if not rows:
        print("  ⚠  No valid cells — check grid values.")
        return

    # ── Plateau analysis ───────────────────────────────────────────────
    sharpes  = np.array([r["Sharpe"] for r in rows], dtype=float)
    peak_sh  = float(np.nanmax(sharpes))
    peak_row = rows[int(np.nanargmax(sharpes))]
    n_valid  = int(np.sum(np.isfinite(sharpes)))
    n_p95    = int(np.sum(sharpes >= 0.95 * peak_sh))
    n_p90    = int(np.sum(sharpes >= 0.90 * peak_sh))
    bl_row   = next((r for r in rows if r["baseline"]), None)
    bl_sh    = bl_row["Sharpe"] if bl_row else float("nan")

    if   n_p95 / n_valid >= 0.80: verdict = "ROBUST PLATEAU"
    elif n_p95 / n_valid >= 0.50: verdict = "MODERATE SENSITIVITY"
    else:                          verdict = "HIGH SENSITIVITY"

    print(f"\n{'='*74}")
    print(f"  RISK THROTTLE STABILITY CUBE SUMMARY")
    print(f"{'='*74}")
    print(f"  Cells evaluated  : {n_valid} / {n_cells}")
    print(f"  Peak Sharpe      : {peak_sh:.3f}  at  "
          f"FILL_Y={peak_row['EARLY_FILL_Y']}  "
          f"KILL_Y={peak_row['EARLY_KILL_Y']}  "
          f"BOOST={peak_row['VOL_LEV_MAX_BOOST']}")
    print(f"  Baseline Sharpe  : {bl_sh:.3f}  ({bl_sh/peak_sh*100:.1f}% of peak)")
    print(f"  Plateau ≥95% pk  : {n_p95}/{n_valid}  ({n_p95/n_valid*100:.1f}%)")
    print(f"  Plateau ≥90% pk  : {n_p90}/{n_valid}  ({n_p90/n_valid*100:.1f}%)")
    print(f"  Sharpe std       : {float(np.nanstd(sharpes)):.4f}")
    print(f"  Verdict          : {verdict}")

    # ── Save CSV ───────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "stability_cube_results.csv"
    with open(csv_path, "w", newline="") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  CSV saved: {csv_path}")

    # ── Save summary text ──────────────────────────────────────────────
    txt_path = out_dir / "stability_cube_summary.txt"
    with open(txt_path, "w") as fh:
        fh.write("RISK THROTTLE STABILITY CUBE — EARLY_FILL_Y × EARLY_KILL_Y × BOOST\n")
        fh.write(f"Filter        : {filter_label}\n")
        fh.write(f"Grid          : {len(values_fill_y)}×{len(values_kill_y)}"
                 f"×{len(values_boost)} = {n_cells} cells\n")
        fh.write(f"Cells valid   : {n_valid}\n")
        fh.write(f"Peak Sharpe   : {peak_sh:.4f}  at  "
                 f"FILL_Y={peak_row['EARLY_FILL_Y']}  "
                 f"KILL_Y={peak_row['EARLY_KILL_Y']}  "
                 f"BOOST={peak_row['VOL_LEV_MAX_BOOST']}\n")
        fh.write(f"Baseline Sharpe: {bl_sh:.4f}\n")
        fh.write(f"Plateau ≥95%  : {n_p95}/{n_valid}  ({n_p95/n_valid*100:.1f}%)\n")
        fh.write(f"Verdict       : {verdict}\n")
    print(f"  Summary saved: {txt_path}")

    # ── Heatmaps: one slice per EARLY_FILL_Y value ────────────────────
    try:
        n_slices  = len(values_fill_y)
        ncols     = min(3, n_slices)
        nrows_fig = math.ceil(n_slices / ncols)
        fig, axes = plt.subplots(nrows_fig, ncols,
                                 figsize=(6 * ncols, 5 * nrows_fig),
                                 facecolor=FIG_BG)
        axes_flat = np.array(axes).flatten() if n_slices > 1 else [axes]
        fig.suptitle(
            f"Risk Throttle Stability Cube — Sharpe  |  Filter: {filter_label}",
            color=TEXT_COL, fontsize=13, y=1.01)

        cmap = plt.cm.RdYlGn
        all_sh = [r["Sharpe"] for r in rows if np.isfinite(r["Sharpe"])]
        vmin, vmax = (min(all_sh), max(all_sh)) if all_sh else (0, 1)

        for si, fy in enumerate(values_fill_y):
            ax = axes_flat[si]
            ax.set_facecolor(PANEL_BG)
            slice_rows = [r for r in rows if abs(r["EARLY_FILL_Y"] - fy) < 1e-9]

            # Grid: rows=KILL_Y, cols=BOOST
            grid = np.full((len(values_kill_y), len(values_boost)), np.nan)
            for r in slice_rows:
                yi = next((i for i, v in enumerate(values_kill_y)
                           if abs(v - r["EARLY_KILL_Y"]) < 1e-9), None)
                xi = next((i for i, v in enumerate(values_boost)
                           if abs(v - r["VOL_LEV_MAX_BOOST"]) < 1e-9), None)
                if yi is not None and xi is not None and np.isfinite(r["Sharpe"]):
                    grid[yi, xi] = r["Sharpe"]

            im = ax.imshow(grid, cmap=cmap, vmin=vmin, vmax=vmax,
                           aspect="auto", origin="lower")

            for yi in range(len(values_kill_y)):
                for xi in range(len(values_boost)):
                    val = grid[yi, xi]
                    if np.isfinite(val):
                        r_match = next(
                            (r for r in slice_rows
                             if abs(r["EARLY_KILL_Y"]       - values_kill_y[yi]) < 1e-9
                             and abs(r["VOL_LEV_MAX_BOOST"] - values_boost[xi])  < 1e-9),
                            None)
                        is_bl_cell = r_match["baseline"] if r_match else False
                        is_pk_cell = (abs(val - peak_sh) < 1e-4)
                        txt = f"{val:.3f}"
                        if is_bl_cell: txt += "\n★"
                        if is_pk_cell: txt += "\n▲"
                        ax.text(xi, yi, txt, ha="center", va="center",
                                color="white", fontsize=7,
                                fontweight="bold" if (is_bl_cell or is_pk_cell) else "normal")

            ax.set_xticks(range(len(values_boost)))
            ax.set_xticklabels([f"{v:.2f}" for v in values_boost],
                               color=TEXT_COL, fontsize=8)
            ax.set_yticks(range(len(values_kill_y)))
            ax.set_yticklabels([f"{v:.4f}" for v in values_kill_y],
                               color=TEXT_COL, fontsize=8)
            ax.set_xlabel("VOL_LEV_MAX_BOOST", color=TEXT_COL, fontsize=9)
            ax.set_ylabel("EARLY_KILL_Y",      color=TEXT_COL, fontsize=9)
            ax.set_title(f"EARLY_FILL_Y = {fy:.2f}", color=TEXT_COL, fontsize=10)
            ax.tick_params(colors=TEXT_COL)
            for spine in ax.spines.values():
                spine.set_edgecolor(TEXT_COL)
            plt.colorbar(im, ax=ax).ax.yaxis.set_tick_params(color=TEXT_COL)

        for si in range(n_slices, len(axes_flat)):
            axes_flat[si].set_visible(False)

        plt.tight_layout()
        png_path = out_dir / "stability_cube_heatmap.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Heatmap saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Heatmap failed: {e}")

    print(f"{'='*74}")


# ══════════════════════════════════════════════════════════════════════
# EXIT ARCHITECTURE STABILITY CUBE
# ══════════════════════════════════════════════════════════════════════
# 3-D exhaustive grid: PORT_SL × PORT_TSL × EARLY_KILL_Y
#
# Outputs (written to stability_cube_exit_architecture/ inside sweep_dir):
#   stability_cube_results.csv   — all cells
#   stability_cube_summary.txt   — plateau stats + verdict
#   stability_cube_heatmap.png   — Sharpe heatmaps sliced by PORT_SL
#
# Stability verdict:
#   ≥ 80% of cells within 95% of peak Sharpe → ROBUST PLATEAU
#   50–79%                                   → MODERATE SENSITIVITY
#   < 50%                                    → HIGH SENSITIVITY

def run_exit_stability_cube(
    df_4x,
    base_params:    dict,
    filter_mode:    str,
    v3_filter,
    symbol_counts,
    vol_lev_params: dict,
    out_dir:        Path,
    filter_label:   str  = "",
    values_port_sl:  list = None,
    values_port_tsl: list = None,
    values_kill_y:   list = None,
):
    import csv as _csv

    if values_port_sl  is None:
        values_port_sl  = [-0.05, -0.06, -0.07, -0.08, -0.09]
    if values_port_tsl is None:
        values_port_tsl = [0.06, 0.075, 0.09, 0.11]
    if values_kill_y   is None:
        values_kill_y   = [0.001, 0.0015, 0.002, 0.0025, 0.003]

    bl_sl   = float(base_params.get("PORT_SL",     -0.07))
    bl_tsl  = float(base_params.get("PORT_TSL",     0.09))
    bl_kill = float(base_params.get("EARLY_KILL_Y", 0.002))
    n_total = len(df_4x.columns)
    n_cells = len(values_port_sl) * len(values_port_tsl) * len(values_kill_y)

    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"

    print(f"\n{'='*74}")
    print(f"  EXIT ARCHITECTURE STABILITY CUBE  —  PORT_SL × PORT_TSL × EARLY_KILL_Y")
    print(f"  Filter : {filter_label}  |  Grid: "
          f"{len(values_port_sl)}×{len(values_port_tsl)}×{len(values_kill_y)} = {n_cells} cells")
    print(f"  Baseline: PORT_SL={bl_sl}  PORT_TSL={bl_tsl}  KILL_Y={bl_kill}")
    print(f"{'='*74}")

    rows = []
    done = 0
    for sl in values_port_sl:
        for tsl in values_port_tsl:
            for ky in values_kill_y:
                params = deepcopy(base_params)
                params["PORT_SL"]     = sl
                params["PORT_TSL"]    = tsl
                params["EARLY_KILL_Y"] = ky
                vp = deepcopy(vol_lev_params) if vol_lev_params else {}

                out = simulate(df_4x, params, filter_mode, v3_filter,
                               symbol_counts=symbol_counts,
                               vol_lev_params=vp,
                               verbose=False)
                daily = np.array(out["daily"], dtype=float)

                sh  = _sweep_sharpe(daily)
                md  = _sweep_maxdd(daily)
                ca  = _sweep_cagr(daily, n_total)
                cal = ca / abs(md) if (np.isfinite(md) and abs(md) > 1e-9) else np.nan
                cv  = _sweep_wf_cv(_sweep_folds(daily))
                is_bl = (abs(sl  - bl_sl)   < 1e-9 and
                         abs(tsl - bl_tsl)  < 1e-9 and
                         abs(ky  - bl_kill) < 1e-9)

                rows.append(dict(
                    PORT_SL=sl, PORT_TSL=tsl, EARLY_KILL_Y=ky,
                    Sharpe=round(sh,  4) if np.isfinite(sh)  else np.nan,
                    CAGR_pct=round(ca, 2) if np.isfinite(ca) else np.nan,
                    MaxDD_pct=round(md, 3) if np.isfinite(md) else np.nan,
                    Calmar=round(cal, 3)   if np.isfinite(cal) else np.nan,
                    WF_CV=round(cv,  4)    if np.isfinite(cv)  else np.nan,
                    baseline=is_bl,
                ))
                done += 1
                bl_tag = " ◄ BASELINE" if is_bl else ""
                print(f"  [{done:>4}/{n_cells}]  PORT_SL={sl:.3f}  PORT_TSL={tsl:.3f}"
                      f"  KILL_Y={ky:.4f}  |  Sharpe={sh:>6.3f}"
                      f"  CAGR={ca:>7.1f}%  MaxDD={md:>7.2f}%{bl_tag}")

    if not rows:
        print("  ⚠  No valid cells — check grid values.")
        return

    # ── Plateau analysis ───────────────────────────────────────────────
    sharpes  = np.array([r["Sharpe"] for r in rows], dtype=float)
    peak_sh  = float(np.nanmax(sharpes))
    peak_row = rows[int(np.nanargmax(sharpes))]
    n_valid  = int(np.sum(np.isfinite(sharpes)))
    n_p95    = int(np.sum(sharpes >= 0.95 * peak_sh))
    n_p90    = int(np.sum(sharpes >= 0.90 * peak_sh))
    bl_row   = next((r for r in rows if r["baseline"]), None)
    bl_sh    = bl_row["Sharpe"] if bl_row else float("nan")

    if   n_p95 / n_valid >= 0.80: verdict = "ROBUST PLATEAU"
    elif n_p95 / n_valid >= 0.50: verdict = "MODERATE SENSITIVITY"
    else:                          verdict = "HIGH SENSITIVITY"

    print(f"\n{'='*74}")
    print(f"  EXIT ARCHITECTURE STABILITY CUBE SUMMARY")
    print(f"{'='*74}")
    print(f"  Cells evaluated  : {n_valid} / {n_cells}")
    print(f"  Peak Sharpe      : {peak_sh:.3f}  at  "
          f"PORT_SL={peak_row['PORT_SL']}  "
          f"PORT_TSL={peak_row['PORT_TSL']}  "
          f"KILL_Y={peak_row['EARLY_KILL_Y']}")
    print(f"  Baseline Sharpe  : {bl_sh:.3f}  ({bl_sh/peak_sh*100:.1f}% of peak)")
    print(f"  Plateau ≥95% pk  : {n_p95}/{n_valid}  ({n_p95/n_valid*100:.1f}%)")
    print(f"  Plateau ≥90% pk  : {n_p90}/{n_valid}  ({n_p90/n_valid*100:.1f}%)")
    print(f"  Sharpe std       : {float(np.nanstd(sharpes)):.4f}")
    print(f"  Verdict          : {verdict}")

    # ── Save CSV ───────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "stability_cube_results.csv"
    with open(csv_path, "w", newline="") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  CSV saved: {csv_path}")

    # ── Save summary text ──────────────────────────────────────────────
    txt_path = out_dir / "stability_cube_summary.txt"
    with open(txt_path, "w") as fh:
        fh.write("EXIT ARCHITECTURE STABILITY CUBE — PORT_SL × PORT_TSL × EARLY_KILL_Y\n")
        fh.write(f"Filter        : {filter_label}\n")
        fh.write(f"Grid          : {len(values_port_sl)}×{len(values_port_tsl)}"
                 f"×{len(values_kill_y)} = {n_cells} cells\n")
        fh.write(f"Cells valid   : {n_valid}\n")
        fh.write(f"Peak Sharpe   : {peak_sh:.4f}  at  "
                 f"PORT_SL={peak_row['PORT_SL']}  "
                 f"PORT_TSL={peak_row['PORT_TSL']}  "
                 f"KILL_Y={peak_row['EARLY_KILL_Y']}\n")
        fh.write(f"Baseline Sharpe: {bl_sh:.4f}\n")
        fh.write(f"Plateau ≥95%  : {n_p95}/{n_valid}  ({n_p95/n_valid*100:.1f}%)\n")
        fh.write(f"Verdict       : {verdict}\n")
    print(f"  Summary saved: {txt_path}")

    # ── Heatmaps: one slice per PORT_SL value ─────────────────────────
    try:
        n_slices  = len(values_port_sl)
        ncols     = min(3, n_slices)
        nrows_fig = math.ceil(n_slices / ncols)
        fig, axes = plt.subplots(nrows_fig, ncols,
                                 figsize=(6 * ncols, 5 * nrows_fig),
                                 facecolor=FIG_BG)
        axes_flat = np.array(axes).flatten() if n_slices > 1 else [axes]
        fig.suptitle(
            f"Exit Architecture Stability Cube — Sharpe  |  Filter: {filter_label}",
            color=TEXT_COL, fontsize=13, y=1.01)

        cmap = plt.cm.RdYlGn
        all_sh = [r["Sharpe"] for r in rows if np.isfinite(r["Sharpe"])]
        vmin, vmax = (min(all_sh), max(all_sh)) if all_sh else (0, 1)

        for si, sl in enumerate(values_port_sl):
            ax = axes_flat[si]
            ax.set_facecolor(PANEL_BG)
            slice_rows = [r for r in rows if abs(r["PORT_SL"] - sl) < 1e-9]

            # Grid: rows=PORT_TSL, cols=EARLY_KILL_Y
            grid = np.full((len(values_port_tsl), len(values_kill_y)), np.nan)
            for r in slice_rows:
                yi = next((i for i, v in enumerate(values_port_tsl)
                           if abs(v - r["PORT_TSL"]) < 1e-9), None)
                xi = next((i for i, v in enumerate(values_kill_y)
                           if abs(v - r["EARLY_KILL_Y"]) < 1e-9), None)
                if yi is not None and xi is not None and np.isfinite(r["Sharpe"]):
                    grid[yi, xi] = r["Sharpe"]

            im = ax.imshow(grid, cmap=cmap, vmin=vmin, vmax=vmax,
                           aspect="auto", origin="lower")

            for yi in range(len(values_port_tsl)):
                for xi in range(len(values_kill_y)):
                    val = grid[yi, xi]
                    if np.isfinite(val):
                        r_match = next(
                            (r for r in slice_rows
                             if abs(r["PORT_TSL"]     - values_port_tsl[yi]) < 1e-9
                             and abs(r["EARLY_KILL_Y"] - values_kill_y[xi])  < 1e-9),
                            None)
                        is_bl_cell = r_match["baseline"] if r_match else False
                        is_pk_cell = (abs(val - peak_sh) < 1e-4)
                        txt = f"{val:.3f}"
                        if is_bl_cell: txt += "\n★"
                        if is_pk_cell: txt += "\n▲"
                        ax.text(xi, yi, txt, ha="center", va="center",
                                color="white", fontsize=7,
                                fontweight="bold" if (is_bl_cell or is_pk_cell) else "normal")

            ax.set_xticks(range(len(values_kill_y)))
            ax.set_xticklabels([f"{v:.4f}" for v in values_kill_y],
                               color=TEXT_COL, fontsize=8)
            ax.set_yticks(range(len(values_port_tsl)))
            ax.set_yticklabels([f"{v:.3f}" for v in values_port_tsl],
                               color=TEXT_COL, fontsize=8)
            ax.set_xlabel("EARLY_KILL_Y", color=TEXT_COL, fontsize=9)
            ax.set_ylabel("PORT_TSL",     color=TEXT_COL, fontsize=9)
            ax.set_title(f"PORT_SL = {sl:.3f}", color=TEXT_COL, fontsize=10)
            ax.tick_params(colors=TEXT_COL)
            for spine in ax.spines.values():
                spine.set_edgecolor(TEXT_COL)
            plt.colorbar(im, ax=ax).ax.yaxis.set_tick_params(color=TEXT_COL)

        for si in range(n_slices, len(axes_flat)):
            axes_flat[si].set_visible(False)

        plt.tight_layout()
        png_path = out_dir / "stability_cube_heatmap.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Heatmap saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Heatmap failed: {e}")

    print(f"{'='*74}")



# Industry-standard methodology for institutional capacity testing.
#
# Model: Almgren-Chriss square-root market impact
#   impact_pct = IMPACT_ALPHA × sqrt(trade_size_usd / ADV_usd)
#
# where:
#   ADV_usd       = estimated average daily volume per symbol
#   trade_size    = AUM × avg_lev / avg_symbols   (position per symbol per day)
#   IMPACT_ALPHA  = 0.1  (empirically calibrated for crypto perpetuals)
#
# This is the correct institutional formulation — it captures that impact
# scales with the square root of participation rate, not linearly.
# The round-trip cost (entry + exit) = 2 × impact_pct per active day.
#
# AUM sweep: $100K → $100M (log-spaced).  At each level:
#   • inject round-trip impact cost into every active day's return
#   • recompute Sharpe, CAGR, MaxDD, Calmar
#   • flag AUM levels where Sharpe drops below 2.0 (institutional floor)
#     and 1.5 (survival floor)
#
# Output:
#   capacity_curve_results.csv    — AUM × metrics table
#   capacity_curve.png            — Sharpe vs AUM with threshold lines

def run_liquidity_capacity_curve(
    daily_with_zeros:  np.ndarray,   # daily returns incl. zeros on flat days
    avg_lev:           float,        # average leverage scalar from simulate()
    avg_symbols:       float,        # average symbols deployed per active day
    filter_label:      str,
    out_dir:           Path,
    aum_levels_usd:    list  = None,
    adv_per_symbol:    float = 5_000_000.0,   # $5M ADV — mid-tier altcoin perp
    impact_alpha:      float = 0.10,           # Almgren-Chriss alpha for crypto
    sharpe_floor_inst: float = 2.0,
    sharpe_floor_surv: float = 1.5,
):
    """
    Almgren-Chriss square-root market impact capacity curve.

    Impact per trade: impact_pct = alpha × sqrt(trade_usd / ADV_usd)
    Round-trip cost per active day: 2 × impact_pct  (entry + exit)
    """

    import csv as _csv

    TDAYS = 365

    if aum_levels_usd is None:
        # Log-spaced from $100K to $100M — 20 points
        aum_levels_usd = [int(x) for x in
                          np.logspace(np.log10(100_000), np.log10(100_000_000), 20)]

    if avg_symbols < 1:
        avg_symbols = 7.0   # fallback to flat estimate

    daily = np.array(daily_with_zeros, dtype=float)
    active_mask = np.isfinite(daily) & (daily != 0.0)
    n_active = int(active_mask.sum())
    n_total  = len(daily)

    def _stats(d: np.ndarray) -> dict:
        r = d[np.isfinite(d)]
        if len(r) < 10:
            return dict(sharpe=np.nan, cagr=np.nan, maxdd=np.nan, calmar=np.nan)
        mu  = float(np.mean(r))
        sd  = float(np.std(r, ddof=1)) if len(r) > 1 else 1e-9
        sh  = mu / sd * TDAYS**0.5 if sd > 1e-9 else 0.0
        eq  = np.cumprod(1 + r)
        ca  = float((eq[-1] ** (TDAYS / len(r)) - 1) * 100)
        pk  = np.maximum.accumulate(eq)
        md  = float(np.min((eq - pk) / pk) * 100)
        cal = ca / abs(md) if abs(md) > 1e-9 else 0.0
        return dict(sharpe=sh, cagr=ca, maxdd=md, calmar=cal)

    base_stats = _stats(daily)

    print(f"\n{'='*76}")
    print(f"  LIQUIDITY CAPACITY CURVE  |  Filter: {filter_label}")
    print(f"{'='*76}")
    print(f"  Model    : Almgren-Chriss √(participation) market impact")
    print(f"  ADV/sym  : ${adv_per_symbol:,.0f}   Alpha: {impact_alpha:.3f}")
    print(f"  Avg lev  : {avg_lev:.3f}x   Avg symbols: {avg_symbols:.1f}")
    print(f"  Active days: {n_active}/{n_total}  "
          f"Baseline Sharpe: {base_stats['sharpe']:.3f}")
    print(f"  Institutional floor: Sharpe ≥ {sharpe_floor_inst:.1f}")
    print(f"  Survival floor     : Sharpe ≥ {sharpe_floor_surv:.1f}")
    print()
    print(f"  {'AUM':>14}  {'Impact%':>8}  {'RT Cost%':>9}  {'Sharpe':>8}  "
          f"{'vs Base':>8}  {'CAGR%':>9}  {'MaxDD%':>8}  {'Grade':>12}")
    print(f"  {'─'*90}")

    results = []
    for aum in aum_levels_usd:
        # Trade size per symbol per day
        trade_usd   = aum * avg_lev / max(avg_symbols, 1.0)
        # Almgren-Chriss square-root impact (one-way)
        impact_pct  = impact_alpha * math.sqrt(trade_usd / adv_per_symbol)
        rt_cost_pct = 2.0 * impact_pct   # round-trip per active day

        d_adj = daily.copy()
        d_adj[active_mask] -= rt_cost_pct

        s        = _stats(d_adj)
        sh_delta = s["sharpe"] - base_stats["sharpe"]

        if   s["sharpe"] >= sharpe_floor_inst + 0.5: grade = "Excellent"
        elif s["sharpe"] >= sharpe_floor_inst:        grade = "Institutional"
        elif s["sharpe"] >= sharpe_floor_surv:        grade = "Survival"
        elif s["sharpe"] >= 1.0:                      grade = "Marginal"
        else:                                          grade = "Unusable"

        flag = ""
        if abs(s["sharpe"] - sharpe_floor_inst) < 0.05: flag = " ← inst. floor"
        if abs(s["sharpe"] - sharpe_floor_surv) < 0.05: flag = " ← surv. floor"

        print(f"  ${aum:>13,.0f}  {impact_pct*100:>7.4f}%  {rt_cost_pct*100:>8.4f}%  "
              f"{s['sharpe']:>8.3f}  {sh_delta:>+8.3f}  {s['cagr']:>8.1f}%  "
              f"{s['maxdd']:>8.2f}%  {grade}{flag}")

        results.append(dict(
            aum_usd            = aum,
            impact_one_way_pct = round(impact_pct * 100, 5),
            rt_cost_pct        = round(rt_cost_pct * 100, 5),
            sharpe             = round(s["sharpe"], 4),
            sharpe_delta       = round(sh_delta, 4),
            cagr_pct           = round(s["cagr"], 2),
            maxdd_pct          = round(s["maxdd"], 2),
            calmar             = round(s["calmar"], 2),
            grade              = grade,
        ))

    # ── Break-even AUM ─────────────────────────────────────────────────
    print(f"\n  Break-even AUM analysis:")
    for floor, label in [(sharpe_floor_inst, "Institutional (Sharpe ≥ 2.0)"),
                         (sharpe_floor_surv, "Survival (Sharpe ≥ 1.5)")]:
        surviving = [r["aum_usd"] for r in results if r["sharpe"] >= floor]
        if surviving:
            max_aum = max(surviving)
            print(f"    {label}: capacity up to ${max_aum:,.0f}")
        else:
            print(f"    {label}: below floor at all tested AUM levels")

    print(f"{'='*76}")

    # ── Save CSV ───────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "capacity_curve_results.csv"
    with open(csv_path, "w", newline="") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(results[0].keys()))
        w.writeheader(); w.writerows(results)
    print(f"  CSV saved: {csv_path}")

    # ── Plot ───────────────────────────────────────────────────────────
    try:
        FIG_BG   = "#0d1117"
        PANEL_BG = "#161b22"
        TEXT_COL = "#e6edf3"

        aum_vals  = [r["aum_usd"]   for r in results]
        sh_vals   = [r["sharpe"]    for r in results]
        ca_vals   = [r["cagr_pct"]  for r in results]
        md_vals   = [r["maxdd_pct"] for r in results]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), facecolor=FIG_BG)
        for ax in (ax1, ax2):
            ax.set_facecolor(PANEL_BG)
            ax.tick_params(colors=TEXT_COL)
            for spine in ax.spines.values():
                spine.set_edgecolor(TEXT_COL)
            ax.xaxis.label.set_color(TEXT_COL)
            ax.yaxis.label.set_color(TEXT_COL)
            ax.title.set_color(TEXT_COL)

        # Sharpe vs AUM
        ax1.semilogx(aum_vals, sh_vals, color="#58a6ff", lw=2.0, marker="o",
                     markersize=4, label="Sharpe")
        ax1.axhline(sharpe_floor_inst, color="#f0883e", lw=1.4, ls="--",
                    label=f"Institutional floor ({sharpe_floor_inst:.1f})")
        ax1.axhline(sharpe_floor_surv, color="#ff7b72", lw=1.4, ls=":",
                    label=f"Survival floor ({sharpe_floor_surv:.1f})")
        ax1.axhline(base_stats["sharpe"], color="#3fb950", lw=1.0, ls="-.",
                    label=f"Baseline ({base_stats['sharpe']:.3f})")
        ax1.set_xlabel("AUM (USD)", color=TEXT_COL)
        ax1.set_ylabel("Sharpe Ratio", color=TEXT_COL)
        ax1.set_title(f"Liquidity Capacity Curve  |  Filter: {filter_label}",
                      color=TEXT_COL)
        ax1.legend(facecolor=PANEL_BG, labelcolor=TEXT_COL, fontsize=8)
        ax1.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.0f}K"))

        # CAGR & MaxDD vs AUM
        ax2.semilogx(aum_vals, ca_vals, color="#58a6ff", lw=2.0,
                     marker="o", markersize=4, label="CAGR%")
        ax2b = ax2.twinx()
        ax2b.set_facecolor(PANEL_BG)
        ax2b.semilogx(aum_vals, md_vals, color="#ff7b72", lw=2.0, ls="--",
                      marker="s", markersize=4, label="MaxDD%")
        ax2b.tick_params(colors=TEXT_COL)
        ax2b.yaxis.label.set_color(TEXT_COL)
        ax2b.set_ylabel("MaxDD %", color=TEXT_COL)
        ax2.set_xlabel("AUM (USD)", color=TEXT_COL)
        ax2.set_ylabel("CAGR %", color=TEXT_COL)
        ax2.set_title("CAGR & MaxDD vs AUM", color=TEXT_COL)
        lines1, labels1 = ax2.get_legend_handles_labels()
        lines2, labels2 = ax2b.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2,
                   facecolor=PANEL_BG, labelcolor=TEXT_COL, fontsize=8)
        ax2.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.0f}K"))

        plt.tight_layout()
        png_path = out_dir / "capacity_curve.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Chart saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Chart failed: {e}")

    # Return max AUM at institutional Sharpe floor (≥2.0)
    surviving_inst = [r["aum_usd"] for r in results if r["sharpe"] >= sharpe_floor_inst]
    institutional_capacity_usd = max(surviving_inst) if surviving_inst else 0.0
    return dict(results=results, institutional_capacity_usd=institutional_capacity_usd)


# ══════════════════════════════════════════════════════════════════════
# REGIME ROBUSTNESS TEST
# ══════════════════════════════════════════════════════════════════════
# Slices the matrix columns into regimes using BTC OHLCV, then runs
# simulate() on each slice independently.  Three regime taxonomies:
#
# Taxonomy A — Volatility buckets (realized vol)
#   Low vol  : 30d rvol in bottom tercile of sample
#   High vol : 30d rvol in top tercile
#   Vol spike: 5d rvol > 1.5× 60d baseline (same signal as tail guardrail)
#
# Taxonomy B — BTC dominance (trend strength proxy)
#   Low DOM  : BTC price momentum (21d) in bottom tercile — altcoin-friendly
#   High DOM : BTC price momentum in top tercile — BTC-dominance regime
#   Neutral  : middle tercile
#
# Taxonomy C — Trend (BTC directional regime)
#   Bull     : 21d BTC return > +5%
#   Bear     : 21d BTC return < -5%
#   Sideways : between -5% and +5%
#
# For each regime slice:
#   • Subset df_4x to columns whose dates fall in that regime
#   • Require ≥ 30 days (skip thinner slices)
#   • Run simulate() → Sharpe, CAGR, MaxDD, Calmar, WF_CV, n_days
#
# Output:
#   regime_robustness_results.csv
#   regime_robustness.png  (bar chart per taxonomy)

def run_regime_robustness(
    df_4x,
    base_params:    dict,
    filter_mode:    str,
    v3_filter,
    symbol_counts,
    vol_lev_params: dict,
    btc_ohlcv:      "pd.DataFrame",   # must have 'close' column, DatetimeIndex
    filter_label:   str  = "",
    out_dir:        Path = None,
    min_days:       int  = 30,
    rvol_window_short: int = 5,
    rvol_window_long:  int = 60,
    momentum_window:   int = 21,
    vol_spike_mult:    float = 1.5,
    bull_threshold:    float = 0.05,
    bear_threshold:    float = -0.05,
):

    import csv as _csv
    out_dir.mkdir(parents=True, exist_ok=True)
    n_total_cols = len(df_4x.columns)

    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"

    # ── Build regime series from BTC OHLCV ─────────────────────────────
    close = btc_ohlcv["close"].sort_index().astype(float)
    close.index = pd.to_datetime(close.index).tz_localize(None).normalize()
    lr    = np.log(close / close.shift(1))

    # Realized vol (30d)
    rvol30 = lr.rolling(30, min_periods=15).std() * np.sqrt(365)
    # Short/long vol for spike detection
    rvol_s = lr.rolling(rvol_window_short, min_periods=3).std() * np.sqrt(365)
    rvol_l = lr.rolling(rvol_window_long,  min_periods=30).std() * np.sqrt(365)
    vol_ratio = rvol_s / rvol_l.replace(0, np.nan)
    # 21d momentum
    mom21 = close.pct_change(momentum_window)

    # Tercile thresholds (computed over full sample)
    rv_p33  = float(np.nanpercentile(rvol30.dropna(), 33))
    rv_p67  = float(np.nanpercentile(rvol30.dropna(), 67))
    mom_p33 = float(np.nanpercentile(mom21.dropna(),  33))
    mom_p67 = float(np.nanpercentile(mom21.dropna(),  67))

    def _date_of_col(col_str: str) -> "Optional[pd.Timestamp]":
        return _col_to_timestamp(str(col_str))

    # ── Map each matrix column to a date ───────────────────────────────
    col_dates = {}
    for c in df_4x.columns:
        ts = _date_of_col(c)
        if ts is not None:
            col_dates[c] = pd.Timestamp(ts).tz_localize(None).normalize()

    def _regime_lookup(date: pd.Timestamp, series: pd.Series,
                       fill: bool = False) -> float:
        try:    return float(series.loc[date])
        except: return float("nan")

    def _cols_in_regime(regime_fn) -> list:
        """Return list of column names where regime_fn(date) is True."""
        return [c for c, d in col_dates.items() if regime_fn(d)]

    def _run_slice(cols: list, label: str):
        if len(cols) < min_days:
            print(f"    {label:<30} : only {len(cols)} days — skip (min={min_days})")
            return None
        sub = df_4x[cols]

        # Subset v3_filter to the slice dates if available
        sub_v3 = None
        if v3_filter is not None:
            slice_dates = pd.DatetimeIndex([col_dates[c] for c in cols
                                            if c in col_dates])
            try:
                sub_v3 = v3_filter.reindex(slice_dates, fill_value=False)
            except Exception:
                sub_v3 = v3_filter

        out = simulate(sub, base_params, filter_mode, sub_v3,
                       symbol_counts=symbol_counts,
                       vol_lev_params=vol_lev_params,
                       verbose=False)
        daily = np.array(out["daily"], dtype=float)
        n     = len(cols)

        r = daily[np.isfinite(daily)]
        if len(r) < 5:
            return None

        mu  = float(np.mean(r))
        sd  = float(np.std(r, ddof=1)) if len(r) > 1 else 1e-9
        sh  = mu / sd * 365**0.5 if sd > 1e-9 else 0.0
        eq  = np.cumprod(1 + r)
        ca  = float((eq[-1] ** (365 / len(r)) - 1) * 100)
        pk  = np.maximum.accumulate(eq)
        md  = float(np.min((eq - pk) / pk) * 100)
        cal = ca / abs(md) if abs(md) > 1e-9 else 0.0
        cv  = _sweep_wf_cv(_sweep_folds(daily))

        print(f"    {label:<30} : n={n:>3}d  Sharpe={sh:>6.3f}  "
              f"CAGR={ca:>8.1f}%  MaxDD={md:>7.2f}%  Calmar={cal:>7.2f}  WF_CV={cv:.3f}")

        return dict(label=label, n_days=n,
                    sharpe=round(sh, 4), cagr_pct=round(ca, 2),
                    maxdd_pct=round(md, 3), calmar=round(cal, 3),
                    wf_cv=round(cv, 4))

    print(f"\n{'='*76}")
    print(f"  REGIME ROBUSTNESS TEST  |  Filter: {filter_label}")
    print(f"{'='*76}")
    print(f"  Taxonomy thresholds:")
    print(f"    Rvol30 low/mid/high splits : p33={rv_p33:.4f}  p67={rv_p67:.4f}")
    print(f"    Momentum low/mid/high      : p33={mom_p33:.4f}  p67={mom_p67:.4f}")
    print(f"    Bull/Bear thresholds       : bull>{bull_threshold*100:.0f}%  "
          f"bear<{bear_threshold*100:.0f}%")
    print()

    all_results = []

    # ── Taxonomy A: Volatility buckets ────────────────────────────────
    print(f"  [A] VOLATILITY REGIMES")
    for label, fn in [
        ("Vol: Low  (rvol30 ≤ p33)",
         lambda d: _regime_lookup(d, rvol30) <= rv_p33),
        ("Vol: Mid  (p33 < rvol30 ≤ p67)",
         lambda d: rv_p33 < _regime_lookup(d, rvol30) <= rv_p67),
        ("Vol: High (rvol30 > p67)",
         lambda d: _regime_lookup(d, rvol30) > rv_p67),
        ("Vol: Spike (5d/60d ratio > {:.1f}×)".format(vol_spike_mult),
         lambda d: _regime_lookup(d, vol_ratio) > vol_spike_mult),
    ]:
        cols = _cols_in_regime(fn)
        res  = _run_slice(cols, label)
        if res: res["taxonomy"] = "Volatility"; all_results.append(res)

    # ── Taxonomy B: BTC dominance (momentum proxy) ────────────────────
    print(f"\n  [B] BTC MOMENTUM / DOMINANCE REGIMES")
    for label, fn in [
        ("DOM: Low  (BTC mom ≤ p33 — alt-season)",
         lambda d: _regime_lookup(d, mom21) <= mom_p33),
        ("DOM: Mid  (neutral momentum)",
         lambda d: mom_p33 < _regime_lookup(d, mom21) <= mom_p67),
        ("DOM: High (BTC mom > p67 — BTC dominant)",
         lambda d: _regime_lookup(d, mom21) > mom_p67),
    ]:
        cols = _cols_in_regime(fn)
        res  = _run_slice(cols, label)
        if res: res["taxonomy"] = "BTC Dominance"; all_results.append(res)

    # ── Taxonomy C: Trend ─────────────────────────────────────────────
    print(f"\n  [C] TREND REGIMES")
    for label, fn in [
        ("Trend: Bull (21d BTC > +5%)",
         lambda d: _regime_lookup(d, mom21) > bull_threshold),
        ("Trend: Sideways (-5% to +5%)",
         lambda d: bear_threshold <= _regime_lookup(d, mom21) <= bull_threshold),
        ("Trend: Bear  (21d BTC < -5%)",
         lambda d: _regime_lookup(d, mom21) < bear_threshold),
    ]:
        cols = _cols_in_regime(fn)
        res  = _run_slice(cols, label)
        if res: res["taxonomy"] = "Trend"; all_results.append(res)

    if not all_results:
        print("  ⚠  No regime slices had sufficient data.")
        return

    # ── Consistency verdict ────────────────────────────────────────────
    sharpes = [r["sharpe"] for r in all_results]
    sh_min, sh_max = min(sharpes), max(sharpes)
    sh_range = sh_max - sh_min

    print(f"\n{'='*76}")
    print(f"  REGIME CONSISTENCY SUMMARY")
    print(f"{'='*76}")
    print(f"  Sharpe range across regimes: {sh_min:.3f} – {sh_max:.3f}  "
          f"(spread={sh_range:.3f})")
    if   sh_range < 0.50: verdict = "CONSISTENT — strategy works across regimes"
    elif sh_range < 1.00: verdict = "MODERATE — some regime sensitivity"
    else:                  verdict = "HIGH SENSITIVITY — strong regime dependence"
    print(f"  Verdict: {verdict}")

    neg_sh = [r for r in all_results if r["sharpe"] < 0]
    if neg_sh:
        print(f"  ⚠  Negative Sharpe in: {[r['label'] for r in neg_sh]}")

    # ── Save CSV ───────────────────────────────────────────────────────
    csv_path = out_dir / "regime_robustness_results.csv"
    with open(csv_path, "w", newline="") as fh:
        fields = ["taxonomy", "label", "n_days", "sharpe", "cagr_pct",
                  "maxdd_pct", "calmar", "wf_cv"]
        w = _csv.DictWriter(fh, fieldnames=fields)
        w.writeheader(); w.writerows(all_results)
    print(f"  CSV saved: {csv_path}")

    # ── Bar chart ──────────────────────────────────────────────────────
    try:
        taxonomies = ["Volatility", "BTC Dominance", "Trend"]
        fig, axes  = plt.subplots(1, 3, figsize=(15, 6), facecolor=FIG_BG)
        fig.suptitle(f"Regime Robustness  |  Filter: {filter_label}",
                     color=TEXT_COL, fontsize=13)

        for ax, tax in zip(axes, taxonomies):
            ax.set_facecolor(PANEL_BG)
            tax_rows = [r for r in all_results if r["taxonomy"] == tax]
            if not tax_rows:
                ax.set_visible(False); continue

            labels  = [r["label"].split("(")[0].strip() for r in tax_rows]
            sharpes_t = [r["sharpe"] for r in tax_rows]
            colors  = ["#3fb950" if s >= 2.0 else
                       "#f0883e" if s >= 1.0 else "#ff7b72"
                       for s in sharpes_t]

            bars = ax.bar(range(len(labels)), sharpes_t, color=colors, alpha=0.85)
            ax.axhline(2.0, color="#f0883e", lw=1.2, ls="--", label="Floor 2.0")
            ax.axhline(1.5, color="#ff7b72", lw=1.0, ls=":",  label="Floor 1.5")
            ax.axhline(0,   color=TEXT_COL,  lw=0.5)

            for bar, val, row in zip(bars, sharpes_t, tax_rows):
                ax.text(bar.get_x() + bar.get_width()/2,
                        bar.get_height() + 0.02,
                        f"{val:.2f}\n({row['n_days']}d)",
                        ha="center", va="bottom", color=TEXT_COL, fontsize=7)

            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, rotation=20, ha="right",
                               color=TEXT_COL, fontsize=8)
            ax.set_ylabel("Sharpe Ratio", color=TEXT_COL)
            ax.set_title(tax, color=TEXT_COL, fontsize=10)
            ax.tick_params(colors=TEXT_COL)
            for spine in ax.spines.values():
                spine.set_edgecolor(TEXT_COL)
            ax.legend(facecolor=PANEL_BG, labelcolor=TEXT_COL, fontsize=7)

        plt.tight_layout()
        png_path = out_dir / "regime_robustness.png"
        plt.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=FIG_BG)
        plt.close(fig)
        print(f"  Chart saved: {png_path}")
    except Exception as e:
        print(f"  ⚠  Chart failed: {e}")

    print(f"{'='*76}")


def run_param_jitter(
    df_4x,
    base_params:   dict,
    filter_mode:   str,
    v3_filter,
    symbol_counts,
    vol_lev_params,
    jitter_spec:   dict,   # {param_name: (mode, magnitude)}
                           #   mode "rel" → multiply by Uniform(1-mag, 1+mag)
                           #   mode "abs" → add    Uniform(-mag, +mag)
    n_trials:      int,
    out_dir:       Path,
    filter_label:  str  = "",
    rng_seed:      int  = 42,
):
    """
    jitter_spec example:
        {
            "L_HIGH":          ("rel", 0.10),   # ±10%
            "PORT_SL":         ("rel", 0.15),   # ±15%
            "PORT_TSL":        ("rel", 0.15),
            "EARLY_KILL_X":    ("abs", 5),      # ±5 bars
            "EARLY_KILL_Y":    ("rel", 0.40),
            "EARLY_FILL_Y":    ("rel", 0.40),
        }
    """



    rng    = np.random.default_rng(rng_seed)
    rows   = []

    # ── Stdout suppression for per-trial simulate() calls ───────────
    print(f"\n  {'='*60}")
    print(f"  PARAM JITTER / SHARPE STABILITY TEST")
    print(f"  Filter: {filter_label}  |  Trials: {n_trials}")
    print(f"  {'='*60}")
    print(f"  Jitter spec:")
    for pname, (mode, mag) in jitter_spec.items():
        bl_val = base_params.get(pname, "?")
        if mode == "rel":
            print(f"    {pname:<22} baseline={bl_val}  ±{mag*100:.0f}%")
        else:
            print(f"    {pname:<22} baseline={bl_val}  ±{mag}")
    print(f"  {'-'*60}")

    # ── Baseline run ────────────────────────────────────────────────
    try:
        bl_out    = simulate(df_4x, base_params, filter_mode=filter_mode,
                             v3_filter=v3_filter, symbol_counts=symbol_counts,
                             vol_lev_params=vol_lev_params)
        _bl_daily = np.array(bl_out["daily"], dtype=float)
        n_total   = len(_bl_daily)
        bl_sharpe = _sweep_sharpe(_bl_daily)
        bl_cagr   = _sweep_cagr(_bl_daily, n_total)
        bl_dd     = _sweep_maxdd(_bl_daily)
    except Exception as _e:
        print(f"  ⚠  Baseline simulate() failed: {_e}")
        return pd.DataFrame()
    print(f"  Baseline simulate() OK — Sharpe={bl_sharpe:.3f}  "
          f"CAGR={bl_cagr:.1f}%  MaxDD={bl_dd:.2f}%")

    # ── Jitter trials ───────────────────────────────────────────────
    for trial_i in range(n_trials):
        trial_params = dict(base_params)
        perturbations = {}
        for pname, (mode, mag) in jitter_spec.items():
            base_val = base_params[pname]
            if mode == "rel":
                factor   = rng.uniform(1.0 - mag, 1.0 + mag)
                new_val  = base_val * factor
            else:  # abs
                delta    = rng.uniform(-mag, mag)
                new_val  = base_val + delta
            # For integer params (e.g. EARLY_KILL_X) round to nearest int
            if isinstance(base_val, int):
                new_val = int(round(new_val))
            trial_params[pname] = new_val
            perturbations[pname] = new_val

        # ── Safety clipping — prevent pathological configs ─────────
        if "PORT_SL"      in trial_params: trial_params["PORT_SL"]      = min(trial_params["PORT_SL"],  -0.01)
        if "PORT_TSL"     in trial_params: trial_params["PORT_TSL"]     = max(trial_params["PORT_TSL"],  0.01)
        if "L_HIGH"       in trial_params: trial_params["L_HIGH"]       = max(trial_params["L_HIGH"],    0.10)
        if "L_BASE"       in trial_params: trial_params["L_BASE"]       = max(trial_params["L_BASE"],    0.10)
        if "EARLY_KILL_X" in trial_params: trial_params["EARLY_KILL_X"] = max(trial_params["EARLY_KILL_X"], 5)
        # Keep TSL > |SL| so trailing stop is looser than hard stop
        if "PORT_SL" in trial_params and "PORT_TSL" in trial_params:
            trial_params["PORT_TSL"] = max(trial_params["PORT_TSL"],
                                           abs(trial_params["PORT_SL"]) * 1.05)
        # Reflect clipped values back into perturbations record
        for pname in jitter_spec:
            if pname in trial_params:
                perturbations[pname] = trial_params[pname]

        try:
            out   = simulate(df_4x, trial_params, filter_mode=filter_mode,
                             v3_filter=v3_filter, symbol_counts=symbol_counts,
                             vol_lev_params=vol_lev_params,
                             verbose=False)
            daily = np.array(out["daily"], dtype=float)
            sh    = _sweep_sharpe(daily)
            ca    = _sweep_cagr(daily, n_total)
            md    = _sweep_maxdd(daily)
            cal   = ca / abs(md) if (np.isfinite(md) and abs(md) > 1e-9) else np.nan
            cv    = _sweep_wf_cv(_sweep_folds(daily))
        except Exception as _exc:
            if trial_i == 0:
                print(f"  ⚠  Trial 0 simulate() exception: {_exc}")
            sh = ca = md = cal = cv = np.nan

        row = {"trial": trial_i, **perturbations,
               "Sharpe": round(sh,  4) if np.isfinite(sh)  else np.nan,
               "CAGR%":  round(ca,  2) if np.isfinite(ca)  else np.nan,
               "MaxDD%": round(md,  3) if np.isfinite(md)  else np.nan,
               "Calmar": round(cal, 3) if np.isfinite(cal) else np.nan,
               "WF_CV":  round(cv,  4) if np.isfinite(cv)  else np.nan}
        rows.append(row)

    df_jitter = pd.DataFrame(rows)
    sharpes   = df_jitter["Sharpe"].dropna().values

    n_valid = len(sharpes)
    n_failed = n_trials - n_valid
    if n_failed > 0:
        print(f"  ⚠  {n_failed}/{n_trials} trials returned NaN Sharpe (exceptions caught silently)")
    if n_valid == 0:
        print(f"  ✗  No valid trials — all simulate() calls failed. Aborting jitter analysis.")
        print(f"     Check that filter_mode='{filter_mode}' and v3_filter are correctly resolved.")
        return df_jitter
    print(f"  ✓  {n_valid}/{n_trials} trials completed successfully")

    # ── Summary stats ───────────────────────────────────────────────
    sh_mean   = float(np.mean(sharpes))
    sh_median = float(np.median(sharpes))
    sh_std    = float(np.std(sharpes))
    sh_p5     = float(np.percentile(sharpes, 5))
    sh_p10    = float(np.percentile(sharpes, 10))
    sh_p25    = float(np.percentile(sharpes, 25))
    sh_p75    = float(np.percentile(sharpes, 75))
    sh_min    = float(np.min(sharpes))
    sh_max    = float(np.max(sharpes))
    pct_below_2  = float(np.mean(sharpes < 2.0) * 100)
    pct_below_15 = float(np.mean(sharpes < 1.5) * 100)
    bias         = sh_mean - bl_sharpe
    bias_pct     = bias / bl_sharpe * 100 if bl_sharpe > 0 else np.nan
    elasticity   = sh_std / bl_sharpe if bl_sharpe > 0 else np.nan
    # Elasticity interpretation: std(Sharpe) / baseline_Sharpe
    #   < 0.05 → parameters have negligible collective effect
    #   < 0.10 → low sensitivity (robust)
    #   < 0.20 → moderate sensitivity
    #   ≥ 0.20 → high sensitivity — strategy is parameter-dependent
    if   np.isfinite(elasticity) and elasticity < 0.05: elast_verdict = "negligible collective sensitivity"
    elif np.isfinite(elasticity) and elasticity < 0.10: elast_verdict = "low sensitivity (robust)"
    elif np.isfinite(elasticity) and elasticity < 0.20: elast_verdict = "moderate sensitivity"
    else:                                                elast_verdict = "high sensitivity \u2014 parameter-dependent"

    # Bias verdict: how far the mean drifts from baseline under jitter
    if   np.isfinite(bias_pct) and abs(bias_pct) <  5: bias_verdict = "mean stable \u2014 baseline is typical"
    elif np.isfinite(bias_pct) and abs(bias_pct) < 10: bias_verdict = "mild drift \u2014 baseline near-optimal"
    elif np.isfinite(bias_pct) and abs(bias_pct) < 20: bias_verdict = "NOTABLE \u2014 baseline may be a local peak"
    else:                                               bias_verdict = "LARGE DRIFT \u2014 baseline is likely a spike"

    # Overall stability verdict — simultaneous multi-parameter jitter context
    # Thresholds reflect that 6 params are perturbed together (some ±40%),
    # so the distribution naturally spreads more than single-param sweeps.
    # Industry benchmarks for simultaneous jitter:
    #   p10 ≥ 80% baseline  →  institutional-grade robustness
    #   p10 ≥ 70% baseline  →  acceptable for live deployment
    #   std < 0.25          →  tight (top-tier institutional < 0.15, good < 0.25)
    high_bias = np.isfinite(bias_pct) and abs(bias_pct) >= 10
    if sh_p10 >= bl_sharpe * 0.85 and sh_std < 0.20 and not high_bias:
        stability_verdict = "ROBUST \u2014 institutional-grade stability under simultaneous jitter"
    elif sh_p10 >= bl_sharpe * 0.80 and sh_std < 0.25:
        stability_verdict = "ROBUST \u2014 solid robustness, p10 holds \u226580% of baseline"
    elif sh_p10 >= bl_sharpe * 0.70 and sh_std < 0.35 and not high_bias:
        stability_verdict = "Acceptable \u2014 moderate spread, deployable"
    elif high_bias and sh_p10 >= bl_sharpe * 0.75:
        stability_verdict = "CAUTION \u2014 baseline is a local peak (confirm with surface maps)"
    elif sh_p5 < 2.0:
        stability_verdict = "CAUTION \u2014 meaningful left-tail degradation"
    elif sh_p5 < 1.5:
        stability_verdict = "WARNING \u2014 left tail collapses below investable"
    else:
        stability_verdict = "Moderate sensitivity \u2014 review left tail and bias"

    SEP  = "-" * 56
    SEP2 = "=" * 56
    print(f"\n  Baseline Sharpe : {bl_sharpe:.3f}")
    print(f"  {SEP}")
    print(f"  Mean            : {sh_mean:.3f}   Bias: {bias:+.3f}  ({bias_pct:+.1f}%)  \u2192 {bias_verdict}")
    print(f"  Median          : {sh_median:.3f}")
    print(f"  Std             : {sh_std:.3f}")
    print(f"  {SEP}")
    print(f"  p5              : {sh_p5:.3f}")
    print(f"  p10             : {sh_p10:.3f}")
    print(f"  p25             : {sh_p25:.3f}")
    print(f"  p75             : {sh_p75:.3f}")
    print(f"  Min / Max       : {sh_min:.3f}  /  {sh_max:.3f}")
    print(f"  {SEP}")
    print(f"  % trials < 2.0  : {pct_below_2:.1f}%")
    print(f"  % trials < 1.5  : {pct_below_15:.1f}%")
    print(f"  {SEP}")
    print(f"  Elasticity      : {elasticity:.4f}  \u2192 {elast_verdict}"
          if np.isfinite(elasticity) else f"  Elasticity      : n/a")
    print(f"  {SEP}")
    print(f"  Verdict         : {stability_verdict}")
    print(f"  {SEP2}\n")

    # ── Plot ────────────────────────────────────────────────────────
    try:
        BG      = "#0d1117"
        PANEL   = "#161b22"
        TEXT    = "#e6edf3"
        GRID    = "#21262d"
        ACCENT  = "#58a6ff"
        GOLD    = "#f0e68c"
        RED     = "#f85149"
        GREEN   = "#3fb950"
        ORANGE  = "#d29922"

        fig = plt.figure(figsize=(14, 8), facecolor=BG)
        gs  = gridspec.GridSpec(1, 2, width_ratios=[3, 1], figure=fig)
        ax_hist  = fig.add_subplot(gs[0])
        ax_stats = fig.add_subplot(gs[1])
        for ax in (ax_hist, ax_stats):
            ax.set_facecolor(PANEL)
            for sp in ax.spines.values():
                sp.set_color(GRID)

        # ── Histogram ──────────────────────────────────────────────
        n_bins = max(20, min(50, n_trials // 10))
        counts, bin_edges, patches = ax_hist.hist(
            sharpes, bins=n_bins, color=ACCENT, alpha=0.80, edgecolor=BG, linewidth=0.5)

        # Colour patches below threshold red
        for patch, left_edge in zip(patches, bin_edges[:-1]):
            if left_edge < 2.0:
                patch.set_facecolor(RED)
                patch.set_alpha(0.85)
            elif left_edge < bl_sharpe * 0.90:
                patch.set_facecolor(ORANGE)
                patch.set_alpha(0.85)

        # Reference lines
        ax_hist.axvline(bl_sharpe, color=GOLD,   lw=2.0, ls="-",  label=f"Baseline {bl_sharpe:.3f}", zorder=5)
        ax_hist.axvline(sh_mean,   color=GREEN,  lw=1.5, ls="--", label=f"Mean {sh_mean:.3f}",       zorder=5)
        ax_hist.axvline(sh_median, color=ACCENT, lw=1.5, ls=":",  label=f"Median {sh_median:.3f}",   zorder=5)
        ax_hist.axvline(sh_p10,    color=ORANGE, lw=1.2, ls="--", label=f"p10 {sh_p10:.3f}",         zorder=5)
        ax_hist.axvline(sh_p5,     color=RED,    lw=1.2, ls="--", label=f"p5 {sh_p5:.3f}",           zorder=5)

        # Threshold bands
        x_min_plot = min(sh_min - 0.1, 1.2)
        ax_hist.axvspan(x_min_plot, 1.5, color=RED,    alpha=0.08, zorder=0)
        ax_hist.axvspan(1.5,        2.0, color=ORANGE, alpha=0.08, zorder=0)

        ax_hist.set_xlabel("Sharpe Ratio", color=TEXT, fontsize=11)
        ax_hist.set_ylabel("Count",        color=TEXT, fontsize=11)
        ax_hist.tick_params(colors=TEXT)
        ax_hist.yaxis.grid(True, color=GRID, lw=0.5, alpha=0.6)
        ax_hist.set_axisbelow(True)
        leg = ax_hist.legend(fontsize=9, facecolor=PANEL, edgecolor=GRID, labelcolor=TEXT)
        ax_hist.set_title("Sharpe Distribution under Parameter Jitter",
                          color=TEXT, fontsize=12, pad=8)

        # ── Stats panel ────────────────────────────────────────────
        ax_stats.axis("off")
        stat_lines = [
            ("JITTER STATS",         "",      TEXT,  12, "bold"),
            ("",                     "",      TEXT,   9, "normal"),
            ("Baseline",  f"{bl_sharpe:.3f}", GOLD,  10, "bold"),
            ("Mean",      f"{sh_mean:.3f}",   GREEN, 10, "normal"),
            ("Median",    f"{sh_median:.3f}", ACCENT,10, "normal"),
            ("Std",       f"{sh_std:.3f}",    TEXT,  10, "normal"),
            ("",          "",                 TEXT,   6, "normal"),
            ("Min",       f"{sh_min:.3f}",    TEXT,  10, "normal"),
            ("p5",        f"{sh_p5:.3f}",     RED,   10, "normal"),
            ("p10",       f"{sh_p10:.3f}",    ORANGE,10, "normal"),
            ("p25",       f"{sh_p25:.3f}",    TEXT,  10, "normal"),
            ("p75",       f"{sh_p75:.3f}",    TEXT,  10, "normal"),
            ("Max",       f"{sh_max:.3f}",    TEXT,  10, "normal"),
            ("",          "",                 TEXT,   6, "normal"),
            ("< 2.0",     f"{pct_below_2:.1f}%",  RED if pct_below_2 > 10 else TEXT,  10, "normal"),
            ("< 1.5",     f"{pct_below_15:.1f}%", RED if pct_below_15 > 2  else TEXT, 10, "normal"),
            ("",          "",                 TEXT,   6, "normal"),
            ("Bias",      f"{bias:+.3f} ({bias_pct:+.1f}%)",
                          RED if (np.isfinite(bias_pct) and abs(bias_pct) >= 10)
                          else (ORANGE if (np.isfinite(bias_pct) and abs(bias_pct) >= 5)
                          else GREEN), 10, "normal"),
            ("Elasticity",f"{elasticity:.4f}" if np.isfinite(elasticity) else "n/a",
                          RED if (np.isfinite(elasticity) and elasticity >= 0.20)
                          else (ORANGE if (np.isfinite(elasticity) and elasticity >= 0.10)
                          else GREEN), 10, "normal"),
            ("N trials",  f"{len(sharpes)}",  TEXT,  10, "normal"),
        ]
        y_pos = 0.97
        for label, value, color, fsize, fweight in stat_lines:
            if not label and not value:
                y_pos -= 0.025
                continue
            ax_stats.text(0.05, y_pos, label, transform=ax_stats.transAxes,
                          color=color, fontsize=fsize, fontweight=fweight,
                          va="top", ha="left")
            if value:
                ax_stats.text(0.95, y_pos, value, transform=ax_stats.transAxes,
                              color=color, fontsize=fsize, fontweight=fweight,
                              va="top", ha="right")
            y_pos -= 0.055

        # Verdict box at bottom of stats panel
        v_color = GREEN if "ROBUST" in stability_verdict else (
                  ORANGE if "Acceptable" in stability_verdict else RED)
        ax_stats.text(0.50, 0.03, stability_verdict,
                      transform=ax_stats.transAxes,
                      color=v_color, fontsize=8, fontweight="bold",
                      va="bottom", ha="center", wrap=True,
                      bbox=dict(boxstyle="round,pad=0.4", facecolor=BG,
                                edgecolor=v_color, alpha=0.9))

        # ── Suptitle ───────────────────────────────────────────────
        jitter_desc = "  ".join(
            f"{p} ±{int(m*100)}%" if mode == "rel" else f"{p} ±{m}"
            for p, (mode, m) in jitter_spec.items()
        )
        fig.suptitle(
            f"Param Jitter / Sharpe Stability Test     Filter: {filter_label}     N={n_trials}\n"
            f"{jitter_desc}",
            color=TEXT, fontsize=10, fontweight="bold",
        )

        plt.tight_layout(rect=[0, 0, 1, 0.92])
        out_dir.mkdir(parents=True, exist_ok=True)
        png_path = out_dir / "param_jitter_sharpe.png"
        fig.savefig(png_path, dpi=130, bbox_inches="tight", facecolor=BG)
        plt.close(fig)
        print(f"  Jitter histogram saved: {png_path}")

    except Exception as _exc:
        print(f"  ⚠  Jitter plot failed: {_exc}")

    # ── CSV ────────────────────────────────────────────────────────
    try:
        csv_path = out_dir / "param_jitter_results.csv"
        df_jitter.to_csv(csv_path, index=False, float_format="%.6f")
        print(f"  Jitter CSV saved:      {csv_path}")
    except Exception as _exc:
        print(f"  ⚠  Jitter CSV failed: {_exc}")

    return df_jitter


# ══════════════════════════════════════════════════════════════════════
# GENERAL 2-PARAMETER SURFACE MAP
# ══════════════════════════════════════════════════════════════════════

def run_param_surface(
    df_4x:         pd.DataFrame,
    base_params:   dict,
    filter_mode:   str,
    v3_filter,
    symbol_counts,
    vol_lev_params,
    param_x:       str,
    param_y:       str,
    values_x:      list,
    values_y:      list,
    out_dir:       Path,
    filter_label:         str  = "",
    surface_label:        str  = "",
    _plateau_summary_out: list = None,   # if provided, plateau rows are appended here
):
    """
    General 2-parameter Sharpe surface sweep.

    Runs simulate() for every (param_x, param_y) combination, computing
    Sharpe, CAGR, MaxDD, Calmar, and WF_CV at each cell.  Produces:
      - A console 2-D table per metric
      - A 2×2 heatmap PNG  (Sharpe / MaxDD / Calmar / WF_CV)
      - A CSV of all results

    The current config baseline is highlighted with a gold star on the heatmap.
    Output filenames are derived from surface_label.
    """


    FIG_BG   = "#0d1117"
    PANEL_BG = "#161b22"
    TEXT_COL = "#e6edf3"
    GRID_COL = "#21262d"

    # Baseline values: strategy params OR vol_lev_params depending on prefix
    _bx_src = vol_lev_params if (param_x.startswith("VOL_LEV:") and vol_lev_params) else base_params
    _by_src = vol_lev_params if (param_y.startswith("VOL_LEV:") and vol_lev_params) else base_params
    _px_key = param_x[8:] if param_x.startswith("VOL_LEV:") else param_x
    _py_key = param_y[8:] if param_y.startswith("VOL_LEV:") else param_y
    baseline_x = _bx_src.get(_px_key)
    baseline_y = _by_src.get(_py_key)
    n_total    = len(df_4x.columns)
    n_cells    = len(values_x) * len(values_y)
    lbl        = surface_label or f"{param_x}_x_{param_y}"

    print(f"\n{'='*74}")
    print(f"  PARAMETER SURFACE MAP  --  {param_x} x {param_y}")
    print(f"  Filter: {filter_label}  |  Grid: {len(values_x)}x{len(values_y)} = {n_cells} cells")
    print(f"  Baseline: {param_x}={baseline_x}  {param_y}={baseline_y}")
    print(f"{'='*74}")

    rows = []
    for vx in values_x:
        for vy in values_y:
            params = deepcopy(base_params)
            vlev   = deepcopy(vol_lev_params) if vol_lev_params else None
            # VOL_LEV: prefix routes the value into vol_lev_params instead of base_params
            _px_key = param_x[8:] if param_x.startswith("VOL_LEV:") else param_x
            _py_key = param_y[8:] if param_y.startswith("VOL_LEV:") else param_y
            if param_x.startswith("VOL_LEV:"):
                if vlev is None: vlev = {}
                vlev[_px_key] = vx
            else:
                params[param_x] = vx
            if param_y.startswith("VOL_LEV:"):
                if vlev is None: vlev = {}
                vlev[_py_key] = vy
            else:
                params[param_y] = vy
            out = simulate(df_4x, params, filter_mode, v3_filter,
                           symbol_counts=symbol_counts,
                           vol_lev_params=vlev,
                           verbose=False)
            daily = np.array(out["daily"], dtype=float)

            sh   = _sweep_sharpe(daily)
            md   = _sweep_maxdd(daily)
            ca   = _sweep_cagr(daily, n_total)
            cal  = ca / abs(md) if (np.isfinite(md) and abs(md) > 1e-9) else np.nan
            cv   = _sweep_wf_cv(_sweep_folds(daily))
            active = daily[np.isfinite(daily) & (daily != 0.0)]
            w1d  = float(np.min(active) * 100) if len(active) > 0 else np.nan
            is_bl = (baseline_x is not None and
                     abs(float(vx) - float(baseline_x)) < 1e-9 and
                     baseline_y is not None and
                     abs(float(vy) - float(baseline_y)) < 1e-9)

            rows.append({
                param_x:      vx,
                param_y:      vy,
                "Sharpe":     round(sh,  4) if np.isfinite(sh)  else np.nan,
                "CAGR%":      round(ca,  2) if np.isfinite(ca)  else np.nan,
                "MaxDD%":     round(md,  3) if np.isfinite(md)  else np.nan,
                "Calmar":     round(cal, 3) if np.isfinite(cal) else np.nan,
                "WF_CV":      round(cv,  4) if np.isfinite(cv)  else np.nan,
                "Worst1D%":   round(w1d, 3) if np.isfinite(w1d) else np.nan,
                "baseline":   is_bl,
            })
            bl_mark = " <-- BASELINE" if is_bl else ""
            print(f"  {param_x}={vx}  {param_y}={vy}  |  "
                  f"Sharpe={sh:>6.3f}  CAGR={ca:>7.1f}%  "
                  f"MaxDD={md:>7.2f}%  Worst1D={w1d:>7.2f}%  WF_CV={cv:>6.3f}{bl_mark}")

    # -- Console 2-D tables -------------------------------------------
    for metric in ("Sharpe", "CAGR%", "MaxDD%", "Worst1D%", "Calmar", "WF_CV"):
        print(f"\n  -- {metric} surface ({param_x} rows x {param_y} cols) --")
        col_hdr = f"  {param_x:>14}" + "".join(f"  {str(vy):>9}" for vy in values_y)
        print(col_hdr)
        print("  " + "-" * (len(col_hdr) - 2))
        for vx in values_x:
            cells = []
            for vy in values_y:
                match = [r for r in rows
                         if abs(float(r[param_x]) - float(vx)) < 1e-9
                         and abs(float(r[param_y]) - float(vy)) < 1e-9]
                val = match[0][metric] if match else np.nan
                bl  = match[0]["baseline"] if match else False
                if isinstance(val, float) and np.isfinite(val):
                    cell = f"{val:>8.3f}"
                else:
                    cell = "     nan"
                cell += ("*" if bl else " ")
                cells.append(cell)
            print(f"  {str(vx):>14}" + "  ".join(cells))
        print(f"  (* = baseline: {param_x}={baseline_x}  {param_y}={baseline_y})")

    # ── Upgrade 1: Build Sharpe grid once, reuse across all panels ───
    nx, ny = len(values_x), len(values_y)

    def _build_grid(metric):
        g = np.full((nx, ny), np.nan)
        for r in rows:
            xi = next((i for i, v in enumerate(values_x)
                       if abs(float(v) - float(r[param_x])) < 1e-9), None)
            yi = next((i for i, v in enumerate(values_y)
                       if abs(float(v) - float(r[param_y])) < 1e-9), None)
            if xi is not None and yi is not None:
                val = r[metric]
                if val is not None and np.isfinite(float(val)):
                    g[xi, yi] = float(val)
        return g

    sharpe_grid = _build_grid("Sharpe")

    # Sharpe-max cell indices (computed once, reused in every panel)
    if not np.all(np.isnan(sharpe_grid)):
        flat_idx  = int(np.nanargmax(sharpe_grid))
        max_sh_xi = flat_idx // ny
        max_sh_yi = flat_idx %  ny
        max_sh_val = float(sharpe_grid[max_sh_xi, max_sh_yi])
    else:
        max_sh_xi = max_sh_yi = None
        max_sh_val = np.nan

    # Baseline grid indices
    bl_xi = next((i for i, v in enumerate(values_x)
                  if baseline_x is not None and
                  abs(float(v) - float(baseline_x)) < 1e-9), None)
    bl_yi = next((i for i, v in enumerate(values_y)
                  if baseline_y is not None and
                  abs(float(v) - float(baseline_y)) < 1e-9), None)

    # ── Parameter Sensitivity Summary ────────────────────────────────
    sharpe_vals  = np.array(
        [r["Sharpe"] for r in rows if r["Sharpe"] is not None and np.isfinite(r["Sharpe"])],
        dtype=float,
    )
    best         = max(rows, key=lambda r: r["Sharpe"] if np.isfinite(r["Sharpe"]) else -999)
    baseline_row = next((r for r in rows
                         if r[param_x] == baseline_x and r[param_y] == baseline_y), None)
    bl_sharpe    = baseline_row["Sharpe"] if baseline_row and np.isfinite(baseline_row["Sharpe"]) else np.nan
    gap          = best["Sharpe"] - bl_sharpe if np.isfinite(bl_sharpe) else np.nan
    bl_is_max    = np.isfinite(gap) and abs(gap) < 1e-9

    n_cells      = len(rows)
    n_valid      = len(sharpe_vals)
    sh_max       = float(np.nanmax(sharpe_vals)) if n_valid else np.nan
    sh_min       = float(np.nanmin(sharpe_vals)) if n_valid else np.nan
    surface_range = sh_max - sh_min if (np.isfinite(sh_max) and np.isfinite(sh_min)) else np.nan

    # Plateau at 95% (for sensitivity summary) — also keep 90% for ridge/title
    if np.isfinite(sh_max) and sh_max > 0:
        p95_thresh     = sh_max * 0.95
        p95_count      = int(np.sum(sharpe_vals >= p95_thresh))
        plateau_pct_95 = p95_count / n_cells * 100

        p90_thresh     = sh_max * 0.90
        p90_count      = int(np.sum(sharpe_vals >= p90_thresh))
        plateau_pct    = p90_count / n_cells * 100        # used in ridge title
    else:
        p95_count = p95_thresh = plateau_pct_95 = np.nan
        plateau_pct = np.nan

    baseline_ratio = bl_sharpe / sh_max if (np.isfinite(bl_sharpe) and np.isfinite(sh_max) and sh_max > 0) else np.nan

    # Verdict logic
    if not np.isfinite(surface_range):
        verdict = "insufficient data"
    elif np.isfinite(baseline_ratio) and baseline_ratio >= 0.99 and np.isfinite(plateau_pct_95) and plateau_pct_95 >= 30:
        verdict = "Robust plateau — baseline at optimum, wide stable region"
    elif np.isfinite(baseline_ratio) and baseline_ratio >= 0.95 and np.isfinite(plateau_pct_95) and plateau_pct_95 >= 20:
        verdict = "Acceptable — baseline near optimum, reasonable plateau"
    elif np.isfinite(surface_range) and surface_range < 0.15:
        verdict = "Flat surface — parameter has minimal impact on Sharpe"
    elif np.isfinite(baseline_ratio) and baseline_ratio < 0.90:
        verdict = "CAUTION — baseline significantly below optimum"
    elif np.isfinite(plateau_pct_95) and plateau_pct_95 < 10:
        verdict = "CAUTION — narrow peak, possible overfit"
    else:
        verdict = "Moderate sensitivity — review ridge curves"

    SEP  = "-" * 50
    SEP2 = "=" * 50
    print(f"\n  {SEP2}")
    print(f"  PARAMETER SENSITIVITY SUMMARY")
    print(f"  {SEP}")
    print(f"  Surface         : {param_x} \u00d7 {param_y}")
    print(f"  Filter          : {filter_label}")
    print(f"  Grid            : {len(values_x)}\u00d7{len(values_y)} = {n_cells} cells  ({n_valid} valid)")
    print(f"  {SEP}")
    sharpe_std = float(np.std(sharpe_vals)) if n_valid >= 2 else np.nan
    if   np.isfinite(sharpe_std) and sharpe_std < 0.05: std_verdict = "parameter has negligible effect"
    elif np.isfinite(sharpe_std) and sharpe_std < 0.15: std_verdict = "low sensitivity"
    elif np.isfinite(sharpe_std) and sharpe_std < 0.35: std_verdict = "moderate sensitivity"
    else:                                                std_verdict = "high sensitivity \u2014 parameter matters"

    print(f"  Sharpe max      : {sh_max:.3f}  at  {param_x}={best[param_x]}  {param_y}={best[param_y]}")
    print(f"  Sharpe min      : {sh_min:.3f}")
    print(f"  Surface range   : {surface_range:.3f}  (max \u2212 min)")
    print(f"  Sharpe std      : {sharpe_std:.3f}  \u2192 {std_verdict}"
          if np.isfinite(sharpe_std) else f"  Sharpe std      : n/a")
    print(f"  {SEP}")
    if np.isfinite(plateau_pct_95):
        print(f"  Plateau \u226595%     : {p95_count} / {n_cells} cells  ({plateau_pct_95:.1f}%)")
    else:
        print(f"  Plateau \u226595%     : n/a")
    print(f"  {SEP}")
    if np.isfinite(bl_sharpe):
        print(f"  Baseline Sharpe : {bl_sharpe:.3f}  at  {param_x}={baseline_x}  {param_y}={baseline_y}")
    else:
        print(f"  Baseline Sharpe : not in grid")
    if np.isfinite(baseline_ratio):
        print(f"  Baseline ratio  : {baseline_ratio*100:.1f}% of max")
    if np.isfinite(gap):
        print(f"  Gap to optimum  : {gap:+.3f}")
    print(f"  {SEP}")
    print(f"  Verdict         : {verdict}")
    print(f"  {SEP2}\n")

    # ── Ridge curves: max Sharpe along each axis ──────────────────────
    # ridge_x[i] = max Sharpe across all Y for values_x[i]  (row max)
    # ridge_y[j] = max Sharpe across all X for values_y[j]  (col max)
    ridge_x = np.nanmax(sharpe_grid, axis=1)   # shape (nx,)
    ridge_y = np.nanmax(sharpe_grid, axis=0)   # shape (ny,)

    # ── Heatmap PNG --------------------------------------------------
    try:
        metrics_to_plot = [
            ("Sharpe",   "viridis",  "Sharpe Ratio"),
            ("MaxDD%",   "RdYlGn_r", "Max Drawdown %"),
            ("Worst1D%", "RdYlGn_r", "Worst Single Day %"),
            ("Calmar",   "plasma",   "Calmar Ratio"),
            ("WF_CV",    "RdYlGn_r", "WF CV (lower = more stable)"),
        ]

        # Layout: 3 rows — top two rows = heatmaps (3 cols), bottom row = ridge curves
        fig = plt.figure(figsize=(24, 16), facecolor=FIG_BG)
        gs  = fig.add_gridspec(3, 3, height_ratios=[5, 5, 3],
                               hspace=0.45, wspace=0.30)
        hm_axes   = [fig.add_subplot(gs[r, c]) for r in range(2) for c in range(3)]
        ridge_ax_x = fig.add_subplot(gs[2, 0])   # ridge across X
        ridge_ax_y = fig.add_subplot(gs[2, 1])   # ridge across Y
        # 6th heatmap slot is unused — hide so it doesn't render as a white box
        if len(hm_axes) > len(metrics_to_plot):
            for _ax_extra in hm_axes[len(metrics_to_plot):]:
                _ax_extra.set_visible(False)

        gap_str  = f"{gap:+.3f}"           if np.isfinite(gap)           else "n/a"
        plat_str = f"{plateau_pct_95:.1f}%" if np.isfinite(plateau_pct_95) else "n/a"
        bl_str   = f"{bl_sharpe:.3f}"       if np.isfinite(bl_sharpe)     else "n/a"
        std_str  = f"{sharpe_std:.3f}"      if np.isfinite(sharpe_std)    else "n/a"
        fig.suptitle(
            f"Parameter Surface:  {param_x}  \u00d7  {param_y}     Filter: {filter_label}\n"
            f"\u2605 Baseline ({param_x}={baseline_x}, {param_y}={baseline_y})  "
            f"Sharpe={bl_str}     "
            f"\u25b2 Max Sharpe={best['Sharpe']:.3f}     "
            f"Gap={gap_str}     Sharpe std={std_str}     Plateau\u226595%={plat_str}     {verdict}",
            color=TEXT_COL, fontsize=10, fontweight="bold",
        )

        xlabels = [str(round(float(v), 6)).rstrip("0").rstrip(".") for v in values_y]
        ylabels = [str(round(float(v), 6)).rstrip("0").rstrip(".") for v in values_x]

        from matplotlib.patches import Rectangle, Circle

        for ax, (metric, cmap_name, title) in zip(hm_axes, metrics_to_plot):
            grid = _build_grid(metric)      # reuse helper; sharpe_grid already built

            cmap = plt.get_cmap(cmap_name).copy()
            cmap.set_bad(color="#2d333b")
            vmin = np.nanmin(grid) if not np.all(np.isnan(grid)) else 0
            vmax = np.nanmax(grid) if not np.all(np.isnan(grid)) else 1

            im = ax.imshow(grid, cmap=cmap, aspect="auto",
                           vmin=vmin, vmax=vmax, origin="upper",
                           interpolation="nearest")

            # Cell value labels
            for xi in range(nx):
                for yi in range(ny):
                    val = grid[xi, yi]
                    if np.isfinite(val):
                        txt = f"{val:.3f}" if abs(val) < 100 else f"{val:.1f}"
                        is_bl  = (xi == bl_xi  and yi == bl_yi)
                        is_max = (xi == max_sh_xi and yi == max_sh_yi)
                        ax.text(yi, xi, txt, ha="center", va="center",
                                fontsize=7, color="white",
                                fontweight="bold" if (is_bl or is_max) else "normal")

            # Gold star border = baseline
            if bl_xi is not None and bl_yi is not None:
                rect = Rectangle((bl_yi - 0.5, bl_xi - 0.5), 1, 1,
                                  linewidth=2.5, edgecolor="#f0e68c",
                                  facecolor="none", zorder=5)
                ax.add_patch(rect)
                ax.text(bl_yi - 0.38, bl_xi - 0.38, "\u2605",
                        ha="center", va="center",
                        fontsize=8, color="#f0e68c", zorder=6)

            # Green circle = Sharpe-max cell (only when different from baseline)
            if max_sh_xi is not None and not (max_sh_xi == bl_xi and max_sh_yi == bl_yi):
                circ = Circle((max_sh_yi, max_sh_xi), 0.42,
                               linewidth=2.0, edgecolor="#3fb950",
                               facecolor="none", zorder=5)
                ax.add_patch(circ)
                ax.text(max_sh_yi + 0.38, max_sh_xi - 0.38, "\u25b2",
                        ha="center", va="center",
                        fontsize=7, color="#3fb950", zorder=6)

            ax.set_xticks(range(ny))
            ax.set_xticklabels(xlabels, rotation=45, ha="right", fontsize=8, color=TEXT_COL)
            ax.set_yticks(range(nx))
            ax.set_yticklabels(ylabels, fontsize=8, color=TEXT_COL)
            ax.set_xlabel(param_y, color=TEXT_COL, fontsize=9)
            ax.set_ylabel(param_x, color=TEXT_COL, fontsize=9)
            ax.set_title(title, color=TEXT_COL, fontsize=10, pad=6)
            ax.set_facecolor(PANEL_BG)
            for spine in ax.spines.values():
                spine.set_edgecolor(GRID_COL)
            ax.tick_params(colors=TEXT_COL)
            cb = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
            cb.ax.tick_params(colors=TEXT_COL, labelsize=7)
            cb.outline.set_edgecolor(GRID_COL)

        # ── Upgrade 4: Ridge curves ───────────────────────────────────
        RIDGE_COL  = "#58a6ff"
        THRESH_COL = "#f0e68c"

        # Left panel: max Sharpe for each value of param_x (row ridge)
        x_vals_f = [float(v) for v in values_x]
        ridge_ax_x.plot(x_vals_f, ridge_x, color=RIDGE_COL, lw=2, marker="o",
                        markersize=5, zorder=3)
        ridge_ax_x.fill_between(x_vals_f, ridge_x.min() * 0.95, ridge_x,
                                 alpha=0.15, color=RIDGE_COL)
        if np.isfinite(max_sh_val):
            ridge_ax_x.axhline(max_sh_val * 0.90, color=THRESH_COL,
                                linestyle="--", lw=1.2, alpha=0.7,
                                label="90% max Sharpe")
        if baseline_x is not None:
            ridge_ax_x.axvline(float(baseline_x), color="#f0e68c",
                                linestyle=":", lw=1.5, alpha=0.8, label=f"Baseline {param_x}")
        ridge_ax_x.set_xlabel(param_x, color=TEXT_COL, fontsize=9)
        ridge_ax_x.set_ylabel("Max Sharpe (across all " + param_y + ")", color=TEXT_COL, fontsize=8)
        ridge_ax_x.set_title(f"Ridge: best Sharpe per {param_x}", color=TEXT_COL, fontsize=10)
        ridge_ax_x.set_facecolor(PANEL_BG)
        ridge_ax_x.tick_params(colors=TEXT_COL, labelsize=8)
        for spine in ridge_ax_x.spines.values():
            spine.set_edgecolor(GRID_COL)
        ridge_ax_x.grid(True, color=GRID_COL, lw=0.5, alpha=0.5)
        leg = ridge_ax_x.legend(fontsize=7, facecolor=PANEL_BG,
                                 edgecolor=GRID_COL, labelcolor=TEXT_COL)

        # Right panel: max Sharpe for each value of param_y (col ridge)
        y_vals_f = [float(v) for v in values_y]
        ridge_ax_y.plot(y_vals_f, ridge_y, color="#d2a8ff", lw=2, marker="o",
                        markersize=5, zorder=3)
        ridge_ax_y.fill_between(y_vals_f, ridge_y.min() * 0.95, ridge_y,
                                 alpha=0.15, color="#d2a8ff")
        if np.isfinite(max_sh_val):
            ridge_ax_y.axhline(max_sh_val * 0.90, color=THRESH_COL,
                                linestyle="--", lw=1.2, alpha=0.7,
                                label="90% max Sharpe")
        if baseline_y is not None:
            ridge_ax_y.axvline(float(baseline_y), color="#f0e68c",
                                linestyle=":", lw=1.5, alpha=0.8, label=f"Baseline {param_y}")
        ridge_ax_y.set_xlabel(param_y, color=TEXT_COL, fontsize=9)
        ridge_ax_y.set_ylabel("Max Sharpe (across all " + param_x + ")", color=TEXT_COL, fontsize=8)
        ridge_ax_y.set_title(f"Ridge: best Sharpe per {param_y}", color=TEXT_COL, fontsize=10)
        ridge_ax_y.set_facecolor(PANEL_BG)
        ridge_ax_y.tick_params(colors=TEXT_COL, labelsize=8)
        for spine in ridge_ax_y.spines.values():
            spine.set_edgecolor(GRID_COL)
        ridge_ax_y.grid(True, color=GRID_COL, lw=0.5, alpha=0.5)
        leg2 = ridge_ax_y.legend(fontsize=7, facecolor=PANEL_BG,
                                  edgecolor=GRID_COL, labelcolor=TEXT_COL)

        png_path = out_dir / f"param_surface_{lbl}.png"
        fig.savefig(png_path, dpi=150, facecolor=FIG_BG, bbox_inches="tight")
        plt.close(fig)
        print(f"\n  Surface heatmap saved: {png_path}")
    except Exception as _e:
        print(f"\n  Warning: heatmap failed: {_e}")

    # -- CSV ----------------------------------------------------------
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / f"param_surface_{lbl}.csv"
        _cols = [param_x, param_y, "Sharpe", "CAGR%", "MaxDD%", "Worst1D%", "Calmar", "WF_CV", "baseline"]
        import csv as _csv
        with open(csv_path, "w", newline="") as fh:
            w = _csv.DictWriter(fh, fieldnames=_cols)
            w.writeheader()
            w.writerows(rows)
        print(f"  Surface CSV saved:     {csv_path}")
    except Exception as _e:
        print(f"  Warning: CSV save failed: {_e}")

    # -- Ridge Map --------------------------------------------------------
    try:
        if ENABLE_SHARPE_RIDGE_MAP:
            plot_sharpe_ridge_map(
                rows=rows, param_x=param_x, param_y=param_y,
                values_x=values_x, values_y=values_y,
                out_dir=out_dir, filter_label=filter_label,
                surface_label=lbl, baseline_x=baseline_x, baseline_y=baseline_y,
            )
    except Exception as _e:
        print(f"  Warning: ridge map failed: {_e}")

    # -- Plateau Detector -------------------------------------------------
    try:
        if ENABLE_SHARPE_PLATEAU:
            _plateau_result = plot_sharpe_plateau_detector(
                rows=rows, param_x=param_x, param_y=param_y,
                values_x=values_x, values_y=values_y,
                out_dir=out_dir, filter_label=filter_label,
                surface_label=lbl, baseline_x=baseline_x, baseline_y=baseline_y,
                plateau_pct_list=PLATEAU_PCT_OF_MAX_LIST,
                min_cluster_cells=PLATEAU_MIN_CLUSTER_CELLS,
            )
            if _plateau_summary_out is not None and _plateau_result:
                for _row in _plateau_result.get("summary", []):
                    _plateau_summary_out.append({"surface": lbl, "param_x": param_x,
                                                 "param_y": param_y, **_row})
    except Exception as _e:
        print(f"  Warning: plateau detector failed: {_e}")

    print(f"{'='*74}\n")


def run_trail_early_sweep(df_4x, base_params, combo_filter, out_dir: Path,
                          early_x_values: list, label: str):
    """PORT_TSL × EARLY_KILL_X surface sweep - Tail + Dispersion filter."""



    TRAIL_DD_VALUES   = [round(v, 3) for v in np.arange(0.040, 0.161, 0.010)]
    BASELINE_TRAIL    = float(base_params["PORT_TSL"])
    BASELINE_EARLY    = int(base_params["EARLY_KILL_X"])
    n_total           = len(df_4x.columns)
    total             = len(TRAIL_DD_VALUES) * len(early_x_values)

    print("\n" + "═"*70)
    print(f"  PARAMETER SWEEP - TRAIL_DD × EARLY_X ({label.upper()})  "
          f"(Tail + Dispersion filter)")
    print(f"  Grid: {len(TRAIL_DD_VALUES)} TRAIL_DD × {len(early_x_values)} EARLY_X "
          f"= {total} combinations")
    print("═"*70)

    rows = []
    done = 0
    for trail in TRAIL_DD_VALUES:
        for early_x in early_x_values:
            params = deepcopy(base_params)
            params["PORT_TSL"]      = trail
            params["EARLY_KILL_X"]  = early_x
            daily = simulate(df_4x, params, "tail_disp", combo_filter, verbose=False)["daily"]

            sh   = _sweep_sharpe(daily)
            md   = _sweep_maxdd(daily)
            flat = int(np.sum(daily == 0.0))
            fold_sh = _sweep_folds(daily)
            f5   = fold_sh[4] if len(fold_sh) > 4 else np.nan
            f8   = fold_sh[7] if len(fold_sh) > 7 else np.nan
            cv   = _sweep_wf_cv(fold_sh)

            rows.append({"TRAIL_DD": trail, "EARLY_X": early_x,
                         "Sharpe": round(sh,3), "MaxDD%": round(md,2),
                         "WF_CV": round(cv,3) if not np.isnan(cv) else np.nan,
                         "Fold5_Sharpe": round(f5,3) if not np.isnan(f5) else np.nan,
                         "Fold8_Sharpe": round(f8,3) if not np.isnan(f8) else np.nan,
                         "Flat_days": flat})
            done += 1
            bl = " ◄ BASELINE" if (abs(trail - BASELINE_TRAIL) < 1e-9 and
                                    early_x == BASELINE_EARLY) else ""
            print(f"  [{done:3d}/{total}]  trail={trail:.3f}  early_x={early_x:2d}  "
                  f"Sharpe={sh:.3f}  CV={cv:.3f}  F5={f5:.3f}{bl}")

    df_res = pd.DataFrame(rows)
    csv_path = out_dir / f"trail_early_surface_{label}.csv"
    df_res.to_csv(csv_path, index=False)

    # ── Heatmaps ─────────────────────────────────────────────────────
    fig, axes = plt.subplots(2, 3, figsize=(18, 11))
    fig.suptitle(f"PORT_TSL × EARLY_KILL_X ({label}) - Tail + Dispersion Filter",
                 fontsize=13, fontweight="bold")

    metrics = [
        ("Sharpe",       "Full-Period Sharpe",          "RdYlGn"),
        ("MaxDD%",       "Max Drawdown %",               "RdYlGn_r"),
        ("WF_CV",        "Walk-Forward CV (↓ better)",  "RdYlGn_r"),
        ("Fold5_Sharpe", "Fold 5 OOS Sharpe (Oct-Nov)", "RdYlGn"),
        ("Fold8_Sharpe", "Fold 8 OOS Sharpe (Jan-Feb)", "RdYlGn"),
    ]
    for i, (col, title, cmap) in enumerate(metrics):
        ax = axes.flatten()[i]
        pivot = df_res.pivot(index="TRAIL_DD", columns="EARLY_X", values=col)
        pivot = pivot.sort_index(ascending=False)
        if HAS_SNS:
            sns.heatmap(pivot, ax=ax, cmap=cmap, annot=True, fmt=".2f",
                        linewidths=0.5, linecolor="white", annot_kws={"fontsize": 7})
        else:
            im = ax.imshow(pivot.values, cmap=cmap, aspect="auto")
            plt.colorbar(im, ax=ax)
            ax.set_xticks(range(len(pivot.columns)))
            ax.set_xticklabels(pivot.columns, fontsize=7)
            ax.set_yticks(range(len(pivot.index)))
            ax.set_yticklabels([f"{v:.3f}" for v in pivot.index], fontsize=7)
            for ri in range(pivot.shape[0]):
                for ci in range(pivot.shape[1]):
                    val = pivot.values[ri, ci]
                    if not np.isnan(val):
                        ax.text(ci, ri, f"{val:.2f}", ha="center", va="center", fontsize=6)
        ax.set_title(title, fontsize=10)
        ax.set_xlabel("EARLY_KILL_X"); ax.set_ylabel("PORT_TSL")
        # Mark baseline cell
        try:
            row_idx = list(pivot.index).index(BASELINE_TRAIL)
            col_idx = list(pivot.columns).index(BASELINE_EARLY)
            ax.add_patch(plt.Rectangle((col_idx, row_idx), 1, 1,
                                        fill=False, edgecolor="red", lw=2.5))
        except (ValueError, KeyError):
            pass

    # 6th panel: Sharpe vs CV scatter coloured by Fold5
    ax = axes.flatten()[5]
    sc = ax.scatter(df_res["WF_CV"], df_res["Sharpe"],
                    c=df_res["Fold5_Sharpe"], cmap="RdYlGn",
                    s=60, edgecolors="k", linewidths=0.4, vmin=-2, vmax=4)
    plt.colorbar(sc, ax=ax, label="Fold 5 Sharpe")
    bl = df_res[(df_res["TRAIL_DD"].round(3) == BASELINE_TRAIL) &
                (df_res["EARLY_X"] == BASELINE_EARLY)]
    if not bl.empty:
        ax.scatter(bl["WF_CV"], bl["Sharpe"], marker="*", s=200,
                   color="red", zorder=5,
                   label=f"Baseline ({BASELINE_TRAIL},{BASELINE_EARLY})")
    ax.set_xlabel("WF CV"); ax.set_ylabel("Full-Period Sharpe")
    ax.set_title("Sharpe vs CV (Fold5 colour)")
    ax.legend(fontsize=7); ax.grid(True, alpha=0.3)

    plt.tight_layout()
    png_path = out_dir / f"trail_early_surface_{label}.png"
    fig.savefig(png_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    # ── Ranked tables ─────────────────────────────────────────────────
    hdr = f"  {'Trail':>7}  {'EarlyX':>6}  {'Sharpe':>6}  {'MaxDD%':>7}  " \
          f"{'WF_CV':>6}  {'F5_Sh':>6}  {'F8_Sh':>6}"
    sep = "─"*70

    print(f"\n{'═'*70}")
    print(f"  TOP 20 - {label.upper()} - RANKED BY SHARPE")
    print(f"{'═'*70}")
    print(hdr); print(sep)
    for _, row in df_res.sort_values("Sharpe", ascending=False).head(20).iterrows():
        mk = " ◄" if (abs(row["TRAIL_DD"] - BASELINE_TRAIL) < 1e-9 and
                      row["EARLY_X"] == BASELINE_EARLY) else ""
        print(f"  {row['TRAIL_DD']:>7.3f}  {row['EARLY_X']:>6.0f}  "
              f"{row['Sharpe']:>6.3f}  {row['MaxDD%']:>7.2f}  "
              f"{row['WF_CV']:>6.3f}  {row['Fold5_Sharpe']:>6.3f}  "
              f"{row['Fold8_Sharpe']:>6.3f}{mk}")

    print(f"\n{'═'*70}")
    print(f"  TOP 10 - {label.upper()} - RANKED BY WF_CV (most stable)")
    print(f"{'═'*70}")
    print(hdr); print(sep)
    for _, row in df_res.sort_values("WF_CV").head(10).iterrows():
        mk = " ◄" if (abs(row["TRAIL_DD"] - BASELINE_TRAIL) < 1e-9 and
                      row["EARLY_X"] == BASELINE_EARLY) else ""
        print(f"  {row['TRAIL_DD']:>7.3f}  {row['EARLY_X']:>6.0f}  "
              f"{row['Sharpe']:>6.3f}  {row['MaxDD%']:>7.2f}  "
              f"{row['WF_CV']:>6.3f}  {row['Fold5_Sharpe']:>6.3f}  "
              f"{row['Fold8_Sharpe']:>6.3f}{mk}")

    print(f"\n  CSV: {csv_path}\n  Chart: {png_path}")


# ══════════════════════════════════════════════════════════════════════
# IC DIAGNOSTIC  &  IC FILTER
# ══════════════════════════════════════════════════════════════════════

def _spearman_ic(signal_vec: np.ndarray, ret_vec: np.ndarray) -> float:
    """Spearman rank IC between a signal and realized cross-sectional returns."""

    valid = np.isfinite(signal_vec) & np.isfinite(ret_vec)
    if valid.sum() < 10:
        return float("nan")
    corr, _ = spearmanr(signal_vec[valid], ret_vec[valid])
    return float(corr)


def _build_signal_df(
    alt_returns: pd.DataFrame,
    signal: str,
) -> pd.DataFrame:
    """
    Build a (date × asset) signal DataFrame from alt_returns.
    All signals are lagged 1 day so they are observable before the trade.

      mom1d       - previous day's return per asset
      mom5d       - 5-day rolling return lagged 1 day
      skew20d     - 20-day rolling skew lagged 1 day (+ skew = right tail bias)
      vol20d_inv  - negative 20-day vol lagged 1 day (higher rank = lower vol)
    """
    if signal == "mom1d":
        return alt_returns.shift(1)
    elif signal == "mom5d":
        return alt_returns.rolling(5, min_periods=3).sum().shift(1)
    elif signal == "skew20d":
        return alt_returns.rolling(20, min_periods=10).skew().shift(1)
    elif signal == "vol20d_inv":
        return -alt_returns.rolling(20, min_periods=10).std().shift(1)
    else:
        raise ValueError(f"Unknown IC signal: {signal!r}")


def _daily_ic_series(
    alt_returns: pd.DataFrame,
    signal: str,
    dates: list,
) -> pd.Series:
    """
    Return a pd.Series of daily IC values keyed by date for every date in `dates`
    that can be matched to alt_returns.
    """
    sig_df  = _build_signal_df(alt_returns, signal)
    ic_vals = {}
    for date in dates:
        if date is None:
            continue
        if date not in sig_df.index or date not in alt_returns.index:
            continue
        ic = _spearman_ic(
            sig_df.loc[date].to_numpy(),
            alt_returns.loc[date].to_numpy(),
        )
        ic_vals[date] = ic
    return pd.Series(ic_vals).sort_index()


def compute_cross_sectional_ic(
    alt_returns: pd.DataFrame,
    col_dates:   list,
    train_days:  int = 120,
    test_days:   int = 30,
    step_days:   int = 30,
) -> dict:
    """
    Compute per-fold cross-sectional IC for four ranking signals.

    For each day t the IC is Spearman rank corr between the lagged signal
    (observable before trading day t, i.e. signal_{t-1}) and realized
    cross-sectional return_t across the altcoin universe.

    Fold windows mirror run_filter_aware_wf (train=120, test=30, step=30).

    Returns
    -------
    dict with:
      folds      - list of fold dicts, each carrying per-signal IC stats
      daily_ic   - dict[signal_name -> pd.Series] of daily IC values
      signals    - list of signal names tested
    """
    SIGNALS = ["mom1d", "mom5d", "skew20d", "vol20d_inv"]

    all_dates = [d for d in col_dates if d is not None]
    if not all_dates or alt_returns.empty:
        print("  IC Diagnostic: missing alt_returns or col_dates — skipping")
        return {}

    # Trim alt_returns to a window that covers our strategy dates with warm-up
    date_min = min(all_dates) - pd.Timedelta(days=30)
    date_max = max(all_dates) + pd.Timedelta(days=2)
    alt = alt_returns.loc[
        (alt_returns.index >= date_min) & (alt_returns.index <= date_max)
    ].copy()

    if alt.empty:
        print("  IC Diagnostic: alt_returns does not overlap col_dates — skipping")
        return {}

    # Compute daily IC series for every signal
    print("  Computing cross-sectional IC series ...")
    daily_ic: dict = {}
    for sig in SIGNALS:
        daily_ic[sig] = _daily_ic_series(alt, sig, all_dates)
        finite_count  = daily_ic[sig].notna().sum()
        mean_ic       = daily_ic[sig].mean()
        print(f"    {sig:<14}: {finite_count} days with valid IC  "
              f"mean_IC={mean_ic:+.4f}")

    # Build fold windows (identical logic to run_filter_aware_wf)
    N      = len(col_dates)
    folds  = []
    start  = 0
    fn     = 0
    while start + train_days + test_days <= N:
        fn       += 1
        train_end = start + train_days
        test_end  = min(train_end + test_days, N)

        test_dates  = [col_dates[i] for i in range(train_end, test_end)
                       if col_dates[i] is not None]
        train_dates = [col_dates[i] for i in range(start, train_end)
                       if col_dates[i] is not None]

        fold_info = {
            "fold":        fn,
            "test_start":  test_dates[0]  if test_dates  else None,
            "test_end":    test_dates[-1] if test_dates  else None,
            "n_test_days": len(test_dates),
            "signals":     {},
        }

        for sig in SIGNALS:
            ic_s = daily_ic[sig]

            test_ic  = ic_s[ic_s.index.isin(test_dates)].dropna()
            train_ic = ic_s[ic_s.index.isin(train_dates)].dropna()

            if len(test_ic) == 0:
                fold_info["signals"][sig] = dict(
                    mean_ic=float("nan"), ic_ir=float("nan"),
                    pct_pos=float("nan"), n_days=0,
                    train_mean_ic=float("nan"),
                )
                continue

            mean_ic   = float(test_ic.mean())
            std_ic    = float(test_ic.std(ddof=1)) if len(test_ic) > 1 else float("nan")
            ic_ir     = (mean_ic / std_ic
                         if std_ic > 0 and math.isfinite(std_ic)
                         else float("nan"))
            pct_pos   = float((test_ic > 0).sum() / len(test_ic) * 100)
            train_mic = float(train_ic.mean()) if len(train_ic) > 0 else float("nan")

            fold_info["signals"][sig] = dict(
                mean_ic      = mean_ic,
                ic_ir        = ic_ir,
                pct_pos      = pct_pos,
                n_days       = len(test_ic),
                train_mean_ic= train_mic,
            )

        folds.append(fold_info)
        start += step_days

    return dict(folds=folds, daily_ic=daily_ic, signals=SIGNALS)


def print_ic_diagnostic(
    ic_result: dict,
    canonical_fa_wf: Optional[dict] = None,
) -> None:
    """
    Print the per-fold IC table to stdout.

    Marks unstable folds (neg OOS Sharpe) from the canonical walk-forward
    result so Fold 5 and any other weak folds stand out visually.
    """
    if not ic_result or not ic_result.get("folds"):
        print("  IC Diagnostic: no results to display")
        return

    folds   = ic_result["folds"]
    signals = ic_result["signals"]

    # Which folds had negative OOS Sharpe in the canonical run?
    unstable_folds: set = set()
    if canonical_fa_wf:
        for f in canonical_fa_wf.get("folds", []):
            if f.get("oos_sharpe", 0) < 0 and not f.get("saturated", False):
                unstable_folds.add(f["fold"])

    SEP = "═" * 104

    print(f"\n{SEP}")
    print("  IC DIAGNOSTIC BY FOLD  —  cross-sectional rank correlation")
    print("  IC   = Spearman( rank(lagged signal) , rank(return_t) ) across altcoin universe")
    print("  IC-IR = mean_IC / std(IC)   |   %Pos = % days IC > 0")
    print("  Columns show:  train_mean_IC → test_mean_IC  |  IC-IR  |  %Pos")
    print(SEP)

    # Build column header
    hdr = f"  {'FOLD':<6}  {'Test Window':<24}"
    for s in signals:
        hdr += f"   {s:^28}"
    print(hdr)
    print(f"  {'-'*100}")

    for f in folds:
        fn    = f["fold"]
        ts    = f["test_start"]
        te    = f["test_end"]
        d_str = (f"{ts.strftime('%Y-%m-%d')} → {te.strftime('%Y-%m-%d')}"
                 if ts and te else "?? → ??")

        unstable_tag = "  ⚠ UNSTABLE" if fn in unstable_folds else ""
        fold5_tag    = "  ◀ FOCUS" if fn == 5 else ""
        tag = unstable_tag or fold5_tag

        print(f"  FOLD {fn:<2}  {d_str:<24}{tag}")

        row = f"  {'':6}  {'train→test IC  | IC-IR | %Pos':24}"
        for s in signals:
            st  = f["signals"].get(s, {})
            mic = st.get("mean_ic",       float("nan"))
            ir  = st.get("ic_ir",         float("nan"))
            pp  = st.get("pct_pos",       float("nan"))
            tmc = st.get("train_mean_ic", float("nan"))

            t_s = f"{tmc:+.3f}" if math.isfinite(tmc) else "  ??? "
            m_s = f"{mic:+.3f}" if math.isfinite(mic)  else "  ??? "
            i_s = f"{ir:+.2f}"  if math.isfinite(ir)   else "  ?"
            p_s = f"{pp:.0f}%"  if math.isfinite(pp)   else "?"

            col_str = f"{t_s}→{m_s}  {i_s}  {p_s}"
            row += f"   {col_str:<28}"
        print(row)
        print(f"  {'-'*100}")

    # ── Fold 5 verdict ────────────────────────────────────────────────
    print(f"\n  FOLD 5 VERDICT — signal breakdown analysis:")
    fold5 = next((f for f in folds if f["fold"] == 5), None)
    if fold5:
        for s in signals:
            st  = fold5["signals"].get(s, {})
            mic = st.get("mean_ic", float("nan"))
            ir  = st.get("ic_ir",   float("nan"))
            pp  = st.get("pct_pos", float("nan"))
            if not math.isfinite(mic):
                verdict = "⬜ insufficient data"
            elif mic < 0.0:
                verdict = "❌ NEGATIVE IC — signal actively mispredicting ranks"
            elif mic < 0.02:
                verdict = "❌ NEAR-ZERO IC — ranking signal broke down"
            elif mic < 0.05:
                verdict = "⚠  WEAK IC — marginal predictability, marginal edge"
            else:
                verdict = "✅ IC PRESENT — ranking signal active in this fold"
            pp_s = f"{pp:.0f}%" if math.isfinite(pp) else "?"
            print(f"    {s:<14}  mean_IC={mic:+.4f}  IC-IR={ir:+.2f}  "
                  f"%Pos={pp_s}   →  {verdict}")
    else:
        print("    Fold 5 not found in IC results.")

    # ── Cross-fold IC summary ─────────────────────────────────────────
    print(f"\n  CROSS-FOLD IC SUMMARY  (mean IC per signal, all folds):")
    hdr2 = f"  {'Signal':<14}"
    for f in folds:
        hdr2 += f"  Fold{f['fold']:>2}"
    hdr2 += "    Mean    Min"
    print(hdr2)
    for s in signals:
        row2 = f"  {s:<14}"
        vals = []
        for f in folds:
            mic = f["signals"].get(s, {}).get("mean_ic", float("nan"))
            vals.append(mic)
            cell = f"{mic:+.3f}" if math.isfinite(mic) else "  ??? "
            row2 += f"  {cell:>6}"
        fin = [v for v in vals if math.isfinite(v)]
        mean_all = float(np.mean(fin))  if fin else float("nan")
        min_all  = float(np.min(fin))   if fin else float("nan")
        row2 += (f"   {mean_all:+.3f}  {min_all:+.3f}"
                 if math.isfinite(mean_all) else "   ???")
        print(row2)

    print(f"{SEP}\n")


def build_ic_filter(
    alt_returns:  pd.DataFrame,
    col_dates:    list,
    signal:       str   = "mom1d",
    window:       int   = 30,
    threshold:    float = 0.02,
) -> pd.Series:
    """
    Build a daily filter (True = sit flat) that fires when rolling mean IC
    collapses below `threshold`.

    Rolling mean IC is computed over the most recent `window` days and
    lagged by 1 day so yesterday's IC gates today's trade.

    Parameters
    ----------
    alt_returns : daily cross-sectional return matrix (date × assets)
    col_dates   : list of pd.Timestamp for each strategy trading day
    signal      : one of 'mom1d' | 'mom5d' | 'skew20d' | 'vol20d_inv'
    window      : rolling window in days for mean IC calculation
    threshold   : sit out when rolling mean IC < this value
    """
    idx = pd.date_range("2025-01-01", "2026-03-01", freq="D")

    all_dates = [d for d in col_dates if d is not None]
    if not all_dates or alt_returns.empty:
        print("    IC filter: missing data — returning all-False (no filter)")
        return pd.Series(False, index=idx)

    # Extend lookback for rolling warm-up
    date_min = min(all_dates) - pd.Timedelta(days=window + 10)
    alt      = alt_returns[alt_returns.index >= date_min].copy()

    # Build daily IC across all available alt dates (not just strategy days)
    sig_df   = _build_signal_df(alt, signal)
    ic_vals  = {}
    for date in alt.index:
        if date not in sig_df.index:
            continue
        ic = _spearman_ic(
            sig_df.loc[date].to_numpy(),
            alt.loc[date].to_numpy(),
        )
        ic_vals[date] = ic

    ic_series   = pd.Series(ic_vals).sort_index()

    # Rolling mean IC — lag 1 day before applying as a gate
    rolling_ic  = ic_series.rolling(window, min_periods=window).mean()
    low_ic_flag = rolling_ic < threshold

    # Reindex to standard daily range and lag
    ic_filter = low_ic_flag.shift(1).reindex(idx, fill_value=False).fillna(False)

    n_flagged = int(ic_filter.sum())
    pct       = n_flagged / len(idx) * 100
    print(f"    IC filter ({signal}, win={window}d, thr={threshold:.3f}): "
          f"{n_flagged} days flagged ({pct:.1f}%)")
    print(f"      rolling IC: mean={rolling_ic.mean():+.4f}  "
          f"min={rolling_ic.min():+.4f}  "
          f"days below threshold={int((rolling_ic < threshold).sum())}")

    return ic_filter


# ══════════════════════════════════════════════════════════════════════
# ALLOCATOR SCORECARD
# ══════════════════════════════════════════════════════════════════════
# Two CSVs are produced at the end of every audit run:
#
#   allocator_view_scorecard.csv      — categorised, ~35 key metrics,
#                                       investor-facing summary
#   technical_appendix_scorecard.csv  — exhaustive ~50 metric index,
#                                       ordered by theme
#
# Both are printed to terminal and saved to run_dir.
# ──────────────────────────────────────────────────────────────────────

def _sc_status(actual, goal_type, goal_val, na=False):
    """
    Return (status_str, sort_key) for one metric row.
    goal_type: 'gt'  → actual > goal_val  (higher is better)
               'lt'  → actual < goal_val  (lower is better)
               'abs' → abs(actual) < goal_val (near-zero is better)
               'na'  → not applicable
    """
    if na or actual is None or (isinstance(actual, float) and math.isnan(actual)):
        return "N/A", 3
    try:
        v = float(actual)
    except (TypeError, ValueError):
        return "N/A", 3

    if goal_type == "gt":
        if   v >= goal_val * 1.0:  return "Pass",       0
        elif v >= goal_val * 0.85: return "Borderline", 1
        else:                      return "Fail",        2
    elif goal_type == "lt":
        if   v <= goal_val * 1.0:  return "Pass",       0
        elif v <= goal_val * 1.15: return "Borderline", 1
        else:                      return "Fail",        2
    elif goal_type == "abs":
        if   abs(v) <= goal_val:        return "Pass",       0
        elif abs(v) <= goal_val * 1.15: return "Borderline", 1
        else:                           return "Fail",        2
    return "N/A", 3


def build_scorecards(results_map: dict, run_dir: Path,
                     avg_overlap:    float = float("nan"),
                     total_days:     int   = 0,
                     leaderboard_n:  int   = 100):
    """
    Build and save two scorecard CSVs for the best filter (highest Sharpe):

      allocator_view_scorecard.csv      — categorised, ~35 key metrics
      technical_appendix_scorecard.csv  — exhaustive ~50 metric index

    Also prints both tables to terminal.
    """

    # ── Pick the winner (highest Sharpe) ─────────────────────────────
    if not results_map:
        print("  [scorecard] No results to score.")
        return

    # Select winner by best (least negative) max drawdown — the allocator
    # cares most about downside containment; Sharpe and CAGR are secondary.
    # Calendar filter is excluded: it's manually curated, not systematic,
    # and would give a misleadingly clean MaxDD.
    _eligible = {k: v for k, v in results_map.items()
                 if "calendar" not in k.lower()}
    winner_key = max(_eligible or results_map,
                     key=lambda k: results_map[k].get("maxdd", float("-inf")))
    r = results_map[winner_key]
    print(f"\n  [scorecard] Scoring: {winner_key}")

    # ── Helper to pull metric values cleanly ─────────────────────────
    def _g(key, default=float("nan")):
        v = r.get(key, default)
        try:
            return float(v)
        except (TypeError, ValueError):
            return float("nan")

    # ── Pull all stored metrics ───────────────────────────────────────
    sharpe        = _g("sharpe")
    cagr          = _g("cagr")            # %
    maxdd         = _g("maxdd")           # % (negative)
    sortino       = _g("sortino")
    calmar        = _g("calmar")
    dsr           = _g("dsr")             # % (0–100)
    cv            = _g("cv")
    fa_wf_cv      = _g("fa_wf_cv")
    fa_wf_mean_s  = _g("fa_wf_mean_sharpe")
    fa_wf_med_s   = _g("fa_wf_mean_sharpe")   # use mean as proxy if median not stored
    fa_wf_pct_p   = _g("fa_wf_pct_positive")  # %
    fa_wf_unstab  = _g("fa_wf_n_unstable")
    cagr_ratio    = _g("cagr_ratio")
    decay_pct     = _g("decay_pct")       # %
    beta          = _g("beta")
    alpha_ann     = _g("alpha_annual")    # %
    btc_r2        = _g("r2")              # %
    equity_r2     = _g("equity_r2")       # 0–1
    pbo           = _g("pbo")             # 0–1 fraction
    perf_deg      = _g("performance_deg") # Sharpe drop train→test
    prob_loss     = _g("prob_loss")       # 0–1 fraction
    ruin_prob_50  = _g("ruin_prob_50")    # 0–1 fraction (50% DD bootstrap)
    active_days   = _g("active_days")
    flat_days     = _g("flat_days")
    inst_cap_usd  = _g("institutional_capacity_usd")   # max AUM at Sharpe ≥ 2.0
    neighbor_plateau_pct = _g("neighbor_plateau_pct")  # % neighbors within ±0.5 Sharpe

    # ── New fa_wf extras ─────────────────────────────────────────────
    fa_wf_std_s       = _g("fa_wf_std_sharpe")
    fa_wf_n_sat       = _g("fa_wf_n_saturated")
    fa_wf_n_active    = _g("fa_wf_n_active_folds")
    fa_wf_mean_dsr    = _g("fa_wf_mean_dsr")
    fa_wf_min_oos     = _g("fa_wf_min_oos_sharpe")
    fa_wf_max_oos     = _g("fa_wf_max_oos_sharpe")

    # ── Cost / fee metrics ────────────────────────────────────────────
    gross_return_pct   = _g("gross_return_pct")
    total_fee_drag_pct = _g("total_fee_drag_pct")
    net_return_pct     = _g("net_return_pct")
    avg_fee_per_active_day = _g("avg_fee_per_active_day_pct")
    avg_fee_per_cal_day    = _g("avg_fee_per_cal_day_pct")
    avg_lev            = _g("avg_lev")
    lev_high_winrate   = _g("lev_high_winrate")   # %
    lev_base_winrate   = _g("lev_base_winrate")   # %
    lev_high_avg_ret   = _g("lev_high_avg_ret")   # %
    lev_base_avg_ret   = _g("lev_base_avg_ret")   # %

    # ── Now-live metrics (previously dead nan) ────────────────────────
    win_rate           = _g("win_rate")            # % of active days positive
    avg_win_loss       = _g("avg_win_loss")        # ratio
    profit_factor      = _g("profit_factor")
    omega              = _g("omega")
    gain_pain          = _g("gain_pain")
    tail_ratio         = _g("tail_ratio")
    skewness           = _g("skewness")
    kurtosis           = _g("kurtosis")
    autocorr           = _g("autocorr")
    ulcer_index        = _g("ulcer_index")
    avg_drawdown       = _g("avg_drawdown")        # % (negative)
    avg_episode_dd     = _g("avg_episode_dd")      # % (negative)
    max_dd_dur         = _g("max_dd_dur")          # days
    dd_recovery        = _g("dd_recovery")         # days
    pct_time_underwater = _g("pct_time_underwater") # %
    cvar_5pct          = _g("cvar_5pct")           # % (negative)
    cvar_1pct          = _g("cvar_1pct")           # % (negative)
    weekly_cvar        = _g("weekly_cvar")         # % (negative)
    mc_pct_losing      = _g("mc_pct_losing")       # %
    turnover_ann       = _g("turnover_ann")        # round-trips / year
    cagr_compounding_multiple = _g("cagr_compounding_multiple")
    equity_multiplier  = _g("equity_multiplier")
    doubling_periods   = _g("doubling_periods")
    vol_drag_ann       = _g("vol_drag_ann")        # % annual vol drag
    weekly_win_rate    = _g("weekly_win_rate")     # %
    lev_high_days      = int(_g("lev_high_days") or 0)
    lev_base_days      = int(_g("lev_base_days") or 0)

    # ── Per-return stats ─────────────────────────────────────────────
    best_day_pct       = _g("best_day_pct")
    worst_day_pct      = _g("worst_day_pct")
    avg_daily_pct      = _g("avg_daily_pct")
    avg_weekly_pct     = _g("avg_weekly_pct")
    avg_monthly_pct    = _g("avg_monthly_pct")
    worst_monthly_pct  = _g("worst_monthly_pct")
    std_daily_pct      = _g("std_daily_pct")

    # ── Streak metrics ────────────────────────────────────────────────
    def _gi(key):
        v = r.get(key, float("nan"))
        try:    return int(v)
        except: return 0
    consec_win_streak  = _gi("consec_win_streak")
    consec_loss_streak = _gi("consec_loss_streak")

    # ── Regime sharpes ────────────────────────────────────────────────
    reg_high_disp      = _g("regime_sharpe_high_disp")
    reg_low_disp       = _g("regime_sharpe_low_disp")
    reg_btc_up         = _g("regime_sharpe_btc_up")
    reg_btc_down       = _g("regime_sharpe_btc_down")
    reg_high_vol       = _g("regime_sharpe_high_vol")
    reg_low_vol        = _g("regime_sharpe_low_vol")
    reg_hd_hv          = _g("regime_sharpe_high_disp_high_vol")
    reg_ld_lv          = _g("regime_sharpe_low_disp_low_vol")

    # Derived values
    total_days_r  = (active_days + flat_days
                     if math.isfinite(active_days) and math.isfinite(flat_days)
                     else float(total_days))
    active_pct    = 100.0 * active_days / total_days_r if total_days_r > 0 else float("nan")
    # Annualised turnover: this strategy fully replaces the portfolio each active day,
    # so turnover/year = active_days scaled to a 365-day year.
    turnover_ann  = active_days / total_days_r * 365.0  if total_days_r > 0 else float("nan")
    return_maxdd  = (abs(cagr / maxdd)
                     if math.isfinite(maxdd) and maxdd != 0 else float("nan"))
    # Compounding multiple: CAGR ÷ Net Return.  Over a ~1-year window this
    # stays near 1×; over multi-year periods with a high reinvestment rate it
    # grows rapidly, quantifying how much compounding amplifies the total gain
    # relative to what simple annualisation would imply.
    cagr_compounding_multiple = (cagr / net_return_pct
                                 if math.isfinite(net_return_pct) and net_return_pct != 0
                                 else float("nan"))
    # Equity multiplier: terminal value of $1 invested.  More visceral than a
    # percentage for capital committee presentations — "4× your money" vs "+308%".
    equity_multiplier = (1.0 + net_return_pct / 100.0
                         if math.isfinite(net_return_pct) else float("nan"))
    # Doubling periods: log2 of the equity multiplier.  Each unit = one doubling
    # of capital.  Makes the compounding trajectory comparable across timeframes
    # and benchmarks regardless of the raw return magnitude.
    doubling_periods  = (math.log2(equity_multiplier)
                         if math.isfinite(equity_multiplier) and equity_multiplier > 0
                         else float("nan"))
    # Volatility drag (annualised %): the gap between arithmetic and geometric
    # mean return, approximated as σ²/2 × 365.  Quantifies the compounding tax
    # paid for daily volatility — directly informs leverage and position-sizing
    # decisions.  Uses daily_with_zeros so flat days are included consistently.
    _d_wz = r.get("daily_with_zeros", np.array([]))
    _d_wz = np.where(np.isfinite(_d_wz), _d_wz, 0.0) if _d_wz is not None and len(_d_wz) > 1 else np.array([])
    vol_drag_ann = (float(np.std(_d_wz, ddof=1) ** 2 / 2 * TRADING_DAYS * 100)
                    if len(_d_wz) > 1 else float("nan"))
    pbo_pct          = pbo * 100        if math.isfinite(pbo)        else float("nan")
    prob_loss_pct    = prob_loss * 100  if math.isfinite(prob_loss)  else float("nan")
    ruin_prob_50_pct = ruin_prob_50 * 100 if math.isfinite(ruin_prob_50) else float("nan")

    # Slippage-adjusted Sharpe: approximate 2× slippage as a constant daily drag.
    # Assume avg holding 1 day, 2× round-trip cost ≈ 0.10% per trade per side.
    # Drag = 2 × 0.001 × active_pct/100  applied to daily mean return.
    daily_arr = r.get("daily_with_zeros", np.array([]))
    d = np.where(np.isfinite(daily_arr), daily_arr, 0.0) if daily_arr is not None and len(daily_arr) > 2 else np.array([])
    if len(d) > 2:
        _slip_drag       = 2 * 0.001 * (active_pct / 100.0 if math.isfinite(active_pct) else 0.5)
        _d_slipped       = d - _slip_drag / len(d[d != 0]) if np.any(d != 0) else d
        _std_s           = float(np.std(_d_slipped, ddof=1))
        sharpe_2x_slip   = (float(np.mean(_d_slipped)) / _std_s * TRADING_DAYS**0.5
                            if _std_s > 0 else float("nan"))
    else:
        sharpe_2x_slip   = float("nan")

    # ── Metrics computed from daily returns ───────────────────────────
    if len(d) > 2:
        neg = d[d < 0]
        pos = d[d > 0]

        # Exclude flat (filtered) days — stored as exactly 0.0 — so they are
        # not counted as losses in the denominator.  Win rate is computed over
        # active trading days only, consistent with _hmm_diag_inject_returns.
        d_active = d[d != 0.0]
        win_rate = float(np.mean(d_active > 0) * 100) if len(d_active) > 0 else float("nan")
        profit_factor = (float(np.sum(pos) / abs(np.sum(neg)))
                         if len(neg) > 0 and np.sum(neg) != 0 else float("nan"))
        avg_win_loss  = (float(np.mean(pos) / abs(np.mean(neg)))
                         if len(neg) > 0 and len(pos) > 0 else float("nan"))
        # skewness, kurtosis, tail_ratio, and autocorr all use d_active:
        # zeros on flat days create a spike at 0 that compresses tails,
        # inflates kurtosis, skews percentiles, and injects artificial
        # reversal structure into the autocorrelation signal.
        skewness      = float(pd.Series(d_active).skew())     if len(d_active) > 2 else float("nan")
        kurtosis      = float(pd.Series(d_active).kurtosis()) if len(d_active) > 2 else float("nan")
        omega         = profit_factor   # omega at threshold=0 equals profit factor

        # Tail ratio: 95th pct / |5th pct|
        p95, p05    = (np.percentile(d_active, 95), np.percentile(d_active, 5)) if len(d_active) > 0 else (float("nan"), float("nan"))
        tail_ratio  = float(abs(p95) / abs(p05)) if len(d_active) > 0 and p05 != 0 else float("nan")

        # Gain-to-Pain
        gain_pain   = (float(np.sum(d) / abs(np.sum(neg)))
                       if len(neg) > 0 and np.sum(neg) != 0 else float("nan"))

        # Drawdown series (used for multiple metrics)
        eq      = np.cumprod(1 + d)
        peak    = np.maximum.accumulate(eq)
        dd_pct  = (eq - peak) / peak * 100     # negative values

        # Ulcer Index = RMS of drawdown depth
        ulcer    = float(np.sqrt(np.mean(dd_pct ** 2)))

        # Avg drawdown (mean of negative drawdown values only)
        neg_dd   = dd_pct[dd_pct < 0]
        avg_dd   = float(np.mean(neg_dd)) if len(neg_dd) > 0 else 0.0

        # Max DD duration (longest consecutive run below peak)
        below    = dd_pct < 0
        max_dur, cur_dur = 0, 0
        for bp in below:
            cur_dur = cur_dur + 1 if bp else 0
            max_dur = max(max_dur, cur_dur)
        max_dd_dur = float(max_dur)

        # Average max-DD per episode: mean trough depth across all drawdown episodes
        _ep_troughs, _in_ep, _ep_min = [], False, 0.0
        for _bp, _dv in zip(below, dd_pct):
            if _bp and not _in_ep:
                _in_ep, _ep_min = True, _dv
            elif _bp:
                _ep_min = min(_ep_min, _dv)
            elif _in_ep:
                _ep_troughs.append(_ep_min)
                _in_ep = False
        if _in_ep:
            _ep_troughs.append(_ep_min)
        avg_episode_dd = float(np.mean(_ep_troughs)) if _ep_troughs else float("nan")

        # Drawdown recovery: avg days from trough to new peak
        in_dd, trough_day, recoveries = False, 0, []
        for i, (bp, dd_val) in enumerate(zip(below, dd_pct)):
            if bp and not in_dd:
                in_dd, trough_day = True, i
            elif not bp and in_dd:
                recoveries.append(i - trough_day)
                in_dd = False
        dd_recovery = float(np.mean(recoveries)) if recoveries else float("nan")

        # Return autocorrelation (lag-1): use active days only — zeros between
        # active days inject artificial reversals into the lag-1 signal.
        autocorr = float(pd.Series(d_active).autocorr(lag=1)) if len(d_active) > 2 else float("nan")

    else:
        win_rate = profit_factor = avg_win_loss = skewness = kurtosis = omega = float("nan")
        tail_ratio = gain_pain = ulcer = avg_dd = max_dd_dur = float("nan")
        dd_recovery = autocorr = avg_episode_dd = float("nan")

    # ── TECHNICAL APPENDIX ───────────────────────────────────────────
    # Format: (display_name, goal_string, value, goal_type, threshold)
    # goal_type: 'gt' higher-is-better  'lt' lower-is-better  'abs' near-zero
    tech_rows = [
        # ── Return quality ────────────────────────────────────────────
        ("Sharpe",                     ">2.0",          sharpe,         "gt",   2.0),
        ("CAGR",                       ">30%",          cagr,           "gt",  30.0),
        ("Sortino",                    ">3.0",          sortino,        "gt",   3.0),
        ("Calmar",                     ">3.0",          calmar,         "gt",   3.0),
        ("Return/MaxDD",               ">3x",           return_maxdd,   "gt",   3.0),
        ("Mean OOS Sharpe",            ">1.5",          fa_wf_mean_s,   "gt",   1.5),
        ("Median OOS Sharpe",          ">1.0",          fa_wf_med_s,    "gt",   1.0),
        ("OOS Sharpe",                 ">1.5",          fa_wf_mean_s,   "gt",   1.5),
        ("Win Rate",                   ">50%",          win_rate,          "gt",  50.0),
        ("Avg Win / Avg Loss",         ">1.2",          avg_win_loss,       "gt",   1.2),
        ("Profit Factor",              ">1.3",          profit_factor,      "gt",   1.3),
        ("Gain-to-Pain",               ">1.5",          gain_pain,          "gt",   1.5),
        ("Omega Ratio",                ">1.3",          omega,              "gt",   1.3),
        ("Weekly Win Rate %",          ">55%",          weekly_win_rate,    "gt",  55.0),
        ("Gross Return %",             "informational", gross_return_pct,           "gt",   0.0),
        ("Net Return %",               "informational", net_return_pct,             "gt",   0.0),
        ("CAGR / Total Return (×)",    "informational", cagr_compounding_multiple,  "gt",   0.0),
        ("Equity Multiplier (×)",      "informational", equity_multiplier,          "gt",   0.0),
        ("Doubling Periods",           "informational", doubling_periods,           "gt",   0.0),
        ("Volatility Drag Ann %",      "informational", vol_drag_ann,               "gt",   0.0),
        ("Total Fee Drag %",           "<5%",           total_fee_drag_pct,"lt",   5.0),
        ("Avg Fee Per Active Day %",   "<0.15%",        avg_fee_per_active_day, "lt",   0.15),
        ("Avg Fee Per Calendar Day %", "<0.05%",        avg_fee_per_cal_day,    "lt",   0.05),
        ("Avg Leverage Used",          "informational", avg_lev,             "gt",   0.0),
        ("Win Rate (L_HIGH tier)",     ">50%",          lev_high_winrate,    "gt",  50.0),
        ("Win Rate (L_BASE tier)",     ">50%",          lev_base_winrate,    "gt",  50.0),
        ("Avg Daily Return (L_HIGH tier)", ">0%",       lev_high_avg_ret,    "gt",   0.0),
        ("Avg Daily Return (L_BASE tier)", ">0%",       lev_base_avg_ret,    "gt",   0.0),
        ("Days at L_HIGH",             "informational", float(lev_high_days),"gt",   0.0),
        ("Days at L_BASE",             "informational", float(lev_base_days),"gt",   0.0),
        # ── Return distribution ───────────────────────────────────────
        ("Best Single Day %",          "informational", best_day_pct,      "gt",   0.0),
        ("Worst Single Day %",         ">-10%",         worst_day_pct,     "gt", -10.0),
        ("Avg Daily Return %",         ">0%",           avg_daily_pct,     "gt",   0.0),
        ("Avg Weekly Return %",        ">0%",           avg_weekly_pct,    "gt",   0.0),
        ("Avg Monthly Return %",       ">0%",           avg_monthly_pct,   "gt",   0.0),
        ("Worst Monthly Return %",     ">-20%",         worst_monthly_pct, "gt", -20.0),
        ("Std Dev Daily Return %",     "informational", std_daily_pct,     "gt",   0.0),
        ("Consecutive Win Streak",     "informational", float(consec_win_streak),  "gt", 0.0),
        ("Consecutive Loss Streak",    "<7d",           float(consec_loss_streak), "lt", 7.0),
        # ── Risk ──────────────────────────────────────────────────────
        ("Max Drawdown",               ">-30%",         maxdd,             "gt", -30.0),
        ("Avg Drawdown",               "<-15%",         avg_drawdown,       "gt", -15.0),
        ("Avg Max DD per Episode",     "<-10%",         avg_episode_dd,     "gt", -10.0),
        ("Longest Drawdown (days)",    "<120d",         max_dd_dur,         "lt", 120.0),
        ("Avg DD Duration",            "<90d",          dd_recovery,        "lt",  90.0),
        ("Max DD Duration",            "<180d",         max_dd_dur,         "lt", 180.0),
        ("% Time Underwater",          "<70%",          pct_time_underwater,"lt",  70.0),
        ("Ulcer Index",                "<10",           ulcer_index,        "lt",  10.0),
        ("CVaR 5% (weekly) %",         ">-20%",         weekly_cvar,        "gt", -20.0),
        ("CVaR 1% (daily) %",          ">-15%",         cvar_1pct,          "gt", -15.0),
        ("Tail Ratio",                 ">1.2",          tail_ratio,         "gt",   1.2),
        ("Skewness",                   ">0",            skewness,           "gt",   0.0),
        ("Kurtosis",                   "<10",           kurtosis,           "lt",  10.0),
        ("Return Autocorrelation",     "≈0  (|r|<0.1)", autocorr,           "abs",  0.1),
        # ── Robustness & validation ───────────────────────────────────
        ("Walk-Forward CV",            "<0.40",         fa_wf_cv,       "lt",   0.40),
        ("FA-WF Mean DSR %",           ">80%",          fa_wf_mean_dsr, "gt",  80.0),
        ("IS/OOS CAGR Ratio",          ">0.5",          cagr_ratio,     "gt",   0.5),
        ("Sharpe Decay",               "<40%",          decay_pct,      "lt",  40.0),
        ("Positive WF Folds",          ">80%",          fa_wf_pct_p,    "gt",  80.0),
        ("Positive OOS Folds",         "100%",          fa_wf_pct_p,    "gt", 100.0),
        ("Unstable WF Folds",          "0",             fa_wf_unstab,   "lt",   0.5),
        ("Saturated WF Folds",         "informational", fa_wf_n_sat,    "lt",   2.0),
        ("Active WF Folds",            "informational", fa_wf_n_active, "gt",   5.0),
        ("Sharpe Std Dev (WF folds)",  "<1.0",          fa_wf_std_s,    "lt",   1.0),
        ("Min OOS Fold Sharpe",        ">0.5",          fa_wf_min_oos,  "gt",   0.5),
        ("Max OOS Fold Sharpe",        "informational", fa_wf_max_oos,  "gt",   0.0),
        ("Deflated Sharpe",            ">95%",          dsr,            "gt",  95.0),
        ("Probability of Loss",        "<5%",           prob_loss_pct,        "lt",   5.0),
        ("Ruin Prob (50% DD, 365d) %", "<10%",          ruin_prob_50_pct,     "lt",  10.0),
        ("Performance Degradation",    "<0.3",          perf_deg,             "lt",   0.3),
        ("PBO",                        "<30%",          pbo_pct,        "lt",  30.0),
        ("Neighbor Plateau",           ">70%",          neighbor_plateau_pct, "gt",  70.0),
        # ── Regime attribution ────────────────────────────────────────
        ("Sharpe in High Dispersion",  ">1.5",          reg_high_disp,  "gt",   1.5),
        ("Sharpe in Low Dispersion",   ">0.5",          reg_low_disp,   "gt",   0.5),
        ("Sharpe in BTC Uptrend",      ">1.5",          reg_btc_up,     "gt",   1.5),
        ("Sharpe in BTC Downtrend",    ">0.5",          reg_btc_down,   "gt",   0.5),
        ("Sharpe in High BTC Vol",     ">1.0",          reg_high_vol,   "gt",   1.0),
        ("Sharpe in Low BTC Vol",      ">1.0",          reg_low_vol,    "gt",   1.0),
        ("Sharpe in HighDisp+HighVol", ">2.0",          reg_hd_hv,      "gt",   2.0),
        ("Sharpe in LowDisp+LowVol",   ">1.0",          reg_ld_lv,      "gt",   1.0),
        # ── Market independence ───────────────────────────────────────
        ("Equity Curve R²",            ">0.9",          equity_r2,      "gt",   0.9),
        ("Beta to BTC",                "<0.5",          beta,           "lt",   0.5),
        ("Annual Alpha",               ">20%",          alpha_ann,      "gt",  20.0),
        ("BTC Variance Explained",     "<10%",          btc_r2,         "lt",  10.0),
        ("Strategy Alpha",             "positive",      alpha_ann,      "gt",   0.0),
        # ── Execution & capacity ──────────────────────────────────────
        ("Sharpe @2x Slippage",        ">1.5",          sharpe_2x_slip, "gt",   1.5),
        ("Active Days %",              "20–60%",        active_pct,     "gt",  20.0),
        ("Universe Size",              ">20",           float(leaderboard_n), "gt", 20.0),
        ("Overlap Stability",          "moderate",      avg_overlap,    "gt",   3.0),
        ("Data Coverage",              ">1yr",          total_days_r,   "gt", 365.0),
        ("Sample Size",                ">500d",         total_days_r,   "gt", 500.0),
        ("Turnover",                   "moderate",      turnover_ann,   "gt",   0.0),
        ("Estimated Capacity",         ">$25,000K",     inst_cap_usd / 1e3
                                                          if math.isfinite(inst_cap_usd) else float("nan"),
                                                                          "gt",  25000.0),
    ]

    # ── ALLOCATOR VIEW ────────────────────────────────────────────────
    # Format: (category, display_name, goal_string, value, goal_type, threshold)
    alloc_rows = [
        # ── Return Quality ────────────────────────────────────────────
        ("Return Quality", "Sharpe Ratio",              ">2.0",   sharpe,         "gt",  2.0),
        ("Return Quality", "Sortino Ratio",             ">3.0",   sortino,        "gt",  3.0),
        ("Return Quality", "Calmar Ratio",              ">3.0",   calmar,         "gt",  3.0),
        ("Return Quality", "CAGR %",                   ">30%",    cagr,           "gt", 30.0),
        ("Return Quality", "Return / MaxDD",            ">3x",    return_maxdd,   "gt",  3.0),
        ("Return Quality", "Gross Return %",            "info",   gross_return_pct,          "gt",  0.0),
        ("Return Quality", "Net Return %",              "info",   net_return_pct,            "gt",  0.0),
        ("Return Quality", "CAGR / Total Return (×)",  "info",   cagr_compounding_multiple, "gt",  0.0),
        ("Return Quality", "Equity Multiplier (×)",    "info",   equity_multiplier,         "gt",  0.0),
        ("Return Quality", "Doubling Periods",         "info",   doubling_periods,          "gt",  0.0),
        ("Return Quality", "Volatility Drag Ann %",    "info",   vol_drag_ann,              "gt",  0.0),
        ("Return Quality", "FA-WF Mean OOS Sharpe",    ">1.5",    fa_wf_mean_s,   "gt",  1.5),
        ("Return Quality", "FA-WF Median OOS Sharpe",  ">1.0",    fa_wf_med_s,    "gt",  1.0),
        ("Return Quality", "Min OOS Fold Sharpe",      ">0.5",    fa_wf_min_oos,  "gt",  0.5),
        ("Return Quality", "Avg Monthly Return %",     ">0%",     avg_monthly_pct, "gt",  0.0),
        ("Return Quality", "Worst Monthly Return %",   ">-20%",   worst_monthly_pct,"gt",-20.0),
        # ── Robustness & Validation ───────────────────────────────────
        ("Robustness & Validation", "FA Walk-Forward CV",         "<0.25",  fa_wf_cv,      "lt",  0.40),
        ("Robustness & Validation", "Sharpe Std Dev (WF folds)", "<1.0",   fa_wf_std_s,   "lt",  1.0),
        ("Robustness & Validation", "FA-WF % Folds Positive",    ">80%",   fa_wf_pct_p,   "gt", 80.0),
        ("Robustness & Validation", "FA-WF Unstable Folds",      "0",      fa_wf_unstab,  "lt",  0.5),
        ("Robustness & Validation", "FA-WF Mean DSR %",          ">80%",   fa_wf_mean_dsr,"gt", 80.0),
        ("Robustness & Validation", "Deflated Sharpe (DSR) %",   ">95%",   dsr,           "gt", 95.0),
        ("Robustness & Validation", "PBO %",                     "<30%",   pbo_pct,       "lt", 30.0),
        ("Robustness & Validation", "Performance Degradation",   "<0.3",   perf_deg,      "lt",  0.3),
        ("Robustness & Validation", "IS/OOS CAGR Ratio",         ">0.5",   cagr_ratio,    "gt",  0.5),
        ("Robustness & Validation", "Sharpe Decay IS→OOS %",     "<40%",   decay_pct,     "lt", 40.0),
        # ── Risk Profile ─────────────────────────────────────────────
        ("Risk Profile", "Max Drawdown %",              ">-30%",  maxdd,               "gt", -30.0),
        ("Risk Profile", "Avg Drawdown %",              ">-15%",  avg_drawdown,         "gt", -15.0),
        ("Risk Profile", "Avg DD Recovery (days)",      "<90d",   dd_recovery,          "lt",  90.0),
        ("Risk Profile", "Ulcer Index",                 "<10",    ulcer_index,          "lt",  10.0),
        ("Risk Profile", "Max DD Duration (days)",      "<180d",  max_dd_dur,           "lt", 180.0),
        ("Risk Profile", "% Time Underwater",           "<70%",   pct_time_underwater,  "lt",  70.0),
        ("Risk Profile", "Worst Single Day %",          ">-10%",  worst_day_pct,        "gt", -10.0),
        ("Risk Profile", "CVaR 5% (weekly) %",          ">-20%",  weekly_cvar,          "gt", -20.0),
        ("Risk Profile", "Consecutive Loss Streak",     "<7d",    float(consec_loss_streak), "lt",  7.0),
        ("Risk Profile", "Probability of Loss %",       "<5%",    prob_loss_pct,        "lt",   5.0),
        ("Risk Profile", "Ruin Prob (50% DD, 365d) %",  "<10%",   ruin_prob_50_pct,     "lt",  10.0),
        ("Risk Profile", "Tail Ratio",                  ">1.2",   tail_ratio,           "gt",   1.2),
        ("Risk Profile", "Gain-to-Pain Ratio",          ">1.5",   gain_pain,            "gt",   1.5),
        # ── Regime Attribution ────────────────────────────────────────
        ("Regime Attribution", "Sharpe in High Dispersion",  ">1.5", reg_high_disp, "gt",  1.5),
        ("Regime Attribution", "Sharpe in Low Dispersion",   ">0.5", reg_low_disp,  "gt",  0.5),
        ("Regime Attribution", "Sharpe in BTC Uptrend",      ">1.5", reg_btc_up,    "gt",  1.5),
        ("Regime Attribution", "Sharpe in BTC Downtrend",    ">0.5", reg_btc_down,  "gt",  0.5),
        ("Regime Attribution", "Sharpe in High BTC Vol",     ">1.0", reg_high_vol,  "gt",  1.0),
        ("Regime Attribution", "Sharpe in Low BTC Vol",      ">1.0", reg_low_vol,   "gt",  1.0),
        ("Regime Attribution", "Sharpe HighDisp+HighVol",    ">2.0", reg_hd_hv,     "gt",  2.0),
        ("Regime Attribution", "Sharpe LowDisp+LowVol",      ">1.0", reg_ld_lv,     "gt",  1.0),
        # ── Market Independence ───────────────────────────────────────
        ("Market Independence", "Beta to BTC",          "<0.5",   beta,           "lt",  0.5),
        ("Market Independence", "BTC Variance Explained %", "<10%", btc_r2,       "lt", 10.0),
        ("Market Independence", "Annual Alpha %",       ">0%",    alpha_ann,      "gt",  0.0),
        ("Market Independence", "Equity Curve R²",      ">0.9",   equity_r2,      "gt",  0.9),
        ("Market Independence", "Return Autocorrelation", "≈0",   autocorr,       "abs", 0.1),
        # ── Execution & Capacity ──────────────────────────────────────
        ("Execution & Capacity", "Sharpe @2× Slippage", ">1.5",  sharpe_2x_slip, "gt",  1.5),
        ("Execution & Capacity", "Total Fee Drag %",    "<5%",    total_fee_drag_pct, "lt",  5.0),
        ("Execution & Capacity", "Avg Fee Per Active Day %",   "<0.15%", avg_fee_per_active_day, "lt",  0.15),
        ("Execution & Capacity", "Avg Fee Per Calendar Day %", "<0.05%", avg_fee_per_cal_day,    "lt",  0.05),
        ("Execution & Capacity", "Avg Leverage Used",   "info",   avg_lev,        "gt",  0.0),
        ("Execution & Capacity", "Universe Size",        ">20",   float(leaderboard_n), "gt", 20.0),
        ("Execution & Capacity", "Active Days %",        "20–60%", active_pct,   "gt", 20.0),
        ("Execution & Capacity", "Avg Overlap Symbols/Day", ">3", avg_overlap,   "gt",  3.0),
        ("Execution & Capacity", "Data Coverage (days)", ">365d", total_days_r,  "gt", 365.0),
        ("Execution & Capacity", "Sample Size",          ">500d", total_days_r,  "gt", 500.0),
    ]

    # ── Value formatter ───────────────────────────────────────────────
    _PCT_KEYS = {
        "CAGR", "Max Drawdown", "Avg Drawdown", "Active Days %",
        "Deflated Sharpe", "Positive WF Folds", "Positive OOS Folds",
        "BTC Variance Explained", "Annual Alpha", "Strategy Alpha",
        "Sharpe Decay", "Win Rate", "PBO", "Probability of Loss",
        "FA-WF % Folds Positive", "FA-WF Mean DSR %",
        "Gross Return %", "Net Return %", "Total Fee Drag %",
        "Avg Fee Per Active Day %", "Avg Fee Per Calendar Day %", "CAGR %",
        "Best Single Day %", "Worst Single Day %",
        "Avg Daily Return %", "Avg Weekly Return %",
        "Avg Monthly Return %", "Worst Monthly Return %",
        "Std Dev Daily Return %",
        "BTC Variance Explained %", "Annual Alpha %",
        "Deflated Sharpe (DSR) %", "Probability of Loss %",
        "Avg Drawdown %", "Max Drawdown %",
        "Gross Return %", "Net Return %",
        "Avg Max DD per Episode",
        "Win Rate (L_HIGH tier)", "Win Rate (L_BASE tier)",
    }
    _INT_DAY_KEYS = {
        "Drawdown Recovery", "Max DD Duration", "Avg DD Recovery (days)",
        "Data Coverage", "Sample Size", "Active Trading Days", "Total Calendar Days",
        "Data Coverage (days)", "Longest Drawdown (days)",
    }
    _INT_COUNT_KEYS = {
        "Consecutive Win Streak", "Consecutive Loss Streak",
        "Saturated WF Folds", "Active WF Folds",
        "FA-WF Unstable Folds", "Unstable WF Folds",
        "Days at L_HIGH", "Days at L_BASE",
    }
    # Qualitative fields — just echo the value string directly
    _QUALITATIVE = {"Overlap Stability"}
    # "informational" goal fields — shown as ── N/A in status but value is real
    _INFORMATIONAL_GOALS = {
        "Gross Return %", "Net Return %", "Best Single Day %",
        "Std Dev Daily Return %", "Avg Leverage Used",
        "Days at L_HIGH", "Days at L_BASE",
        "Avg Leverage Used", "Avg Lev", "info",
        "Consecutive Win Streak", "Max OOS Fold Sharpe",
        "Saturated WF Folds", "Active WF Folds",
    }

    def _fmt(key, val):
        if val is None or (isinstance(val, float) and not math.isfinite(val)):
            return "N/A"
        if key in _QUALITATIVE:
            return "N/A"   # no computed value — filled manually if needed
        if key in _PCT_KEYS:
            return f"{val:.2f}%"
        if key in _INT_DAY_KEYS:
            return f"{int(round(val))}d"
        if key in _INT_COUNT_KEYS:
            return str(int(round(val)))
        if key == "Universe Size":
            return f"Top{int(round(val))}"
        if key in {"Avg Overlap Symbols/Day", "Overlap Stability"}:
            return f"{val:.1f}/day"
        if key in {"Equity Curve R²", "Beta to BTC", "Return Autocorrelation",
                   "IS/OOS CAGR Ratio", "Performance Degradation",
                   "WF CV (all folds)", "Sharpe Std Dev (WF folds)"}:
            return f"{val:.3f}"
        if key == "Return/MaxDD":
            return f"~{val:.0f}x"
        if key in {"Avg Leverage Used", "Avg Lev"}:
            return f"{val:.2f}×"
        if key == "Turnover":
            return f"{int(round(val))}/yr"
        return f"{val:.2f}"

    # ── Assemble rows ─────────────────────────────────────────────────
    def _build_rows(row_spec, include_category=False):
        out = []
        for row in row_spec:
            if include_category:
                cat, metric, goal_str, actual, gtype, gthresh = row
            else:
                metric, goal_str, actual, gtype, gthresh = row
                cat = None
            # Informational rows show a real value but no pass/fail judgement
            if goal_str in ("informational", "info"):
                status = "N/A"
            else:
                status, _ = _sc_status(actual, gtype, gthresh)
            entry = {}
            if include_category:
                entry["Category"] = cat
            entry["Metric"]             = metric
            entry["Institutional Goal"] = goal_str
            entry["Actual"]             = _fmt(metric, actual)
            entry["Status"]             = status
            out.append(entry)
        return out

    tech_out_rows  = _build_rows(tech_rows,  include_category=False)
    alloc_out_rows = _build_rows(alloc_rows, include_category=True)

    # ── Save CSVs ─────────────────────────────────────────────────────
    tech_path  = run_dir / "technical_appendix_scorecard.csv"
    alloc_path = run_dir / "allocator_view_scorecard.csv"
    pd.DataFrame(tech_out_rows).to_csv(tech_path,  index=False)
    pd.DataFrame(alloc_out_rows).to_csv(alloc_path, index=False)

    # ── Terminal output ───────────────────────────────────────────────
    W    = 92
    SEP  = "═" * W
    ICON = {"Pass": "✅", "Fail": "❌", "Borderline": "⚠ ", "N/A": "──"}

    def _header(title):
        print(f"\n{SEP}")
        print(f"  {title}")
        print(f"  Best filter: {winner_key}")
        print(SEP)

    def _summary(rows):
        p = sum(1 for x in rows if x["Status"] == "Pass")
        f = sum(1 for x in rows if x["Status"] == "Fail")
        b = sum(1 for x in rows if x["Status"] == "Borderline")
        n = sum(1 for x in rows if x["Status"] == "N/A")
        print(f"\n  ✅ {p} Pass   ❌ {f} Fail   ⚠  {b} Borderline   ── {n} N/A"
              f"   (of {len(rows)} metrics)")

    # Allocator View
    _header("ALLOCATOR VIEW SCORECARD")
    cur_cat = None
    for row in alloc_out_rows:
        if row["Category"] != cur_cat:
            cur_cat = row["Category"]
            print(f"\n  ── {cur_cat} {'─' * max(0, W - len(cur_cat) - 6)}")
            print(f"  {'Metric':<40}  {'Goal':<12}  {'Actual':>12}  Status")
            print(f"  {'─'*40}  {'─'*12}  {'─'*12}  {'─'*10}")
        icon = ICON.get(row["Status"], "  ")
        print(f"  {row['Metric']:<40}  {row['Institutional Goal']:<12}  "
              f"{row['Actual']:>12}  {icon} {row['Status']}")
    _summary(alloc_out_rows)
    print(f"  Saved → {alloc_path}")

    # Technical Appendix
    _header("TECHNICAL APPENDIX SCORECARD")
    print(f"  {'Metric':<40}  {'Goal':<14}  {'Actual':>12}  Status")
    print(f"  {'─'*40}  {'─'*14}  {'─'*12}  {'─'*10}")
    for row in tech_out_rows:
        icon = ICON.get(row["Status"], "  ")
        print(f"  {row['Metric']:<40}  {row['Institutional Goal']:<14}  "
              f"{row['Actual']:>12}  {icon} {row['Status']}")
    _summary(tech_out_rows)
    print(f"  Saved → {tech_path}")
    print(SEP)


# ══════════════════════════════════════════════════════════════════════
# VOL-LEV DD PARAMETRIC GRID SEARCH
# ══════════════════════════════════════════════════════════════════════

def run_vol_lev_dd_grid_search(
    df_4x:         pd.DataFrame,
    base_params:   dict,
    filter_mode:   str,
    v3_filter,
    symbol_counts,
    vlev_base:     dict,
    out_dir:       Path,
) -> pd.DataFrame:
    """
    Parametric grid search over VOL_LEV_DD_THRESHOLD × VOL_LEV_DD_SCALE.

    Grid:
      dd_threshold : 0.00, -0.01, -0.02, ..., -0.20  (21 values)
      dd_scale     : 0.0,  0.1,  0.2,  ...,  1.0     (11 values)
      Total        : 231 cells

    Metrics recorded per cell:
      Sharpe, CAGR%, MaxDD%, Active, WF-CV, TotRet%, Eq,
      Wst1D%, Wst1W%, Wst1M%, DSR%

    Returns a DataFrame of all results, sorted by Sharpe descending.
    Also saves grid_search_vol_lev_dd_results.csv to out_dir.
    """
    from copy import deepcopy
    from scipy.stats import norm as _scipy_norm

    # ── Grid definition ────────────────────────────────────────────────
    # 21 threshold values: 0.00, -0.01, ..., -0.20
    dd_thresholds = [round(-i * 0.01, 2) for i in range(21)]
    # 11 scale values: 0.0, 0.1, ..., 1.0
    dd_scales     = [round(i * 0.1, 1)  for i in range(11)]

    n_cells   = len(dd_thresholds) * len(dd_scales)
    n_total   = len(df_4x.columns)     # calendar day span for CAGR denominator

    print("\n" + "═" * 78)
    print("  VOL-LEV DD GRID SEARCH")
    print(f"  VOL_LEV_DD_THRESHOLD : {dd_thresholds[0]:+.2f} → {dd_thresholds[-1]:+.2f}  "
          f"(step -0.01,  {len(dd_thresholds)} values)")
    print(f"  VOL_LEV_DD_SCALE     :  {dd_scales[0]:.1f}  →  {dd_scales[-1]:.1f}  "
          f"(step  0.1,   {len(dd_scales)} values)")
    print(f"  Total cells          : {n_cells}")
    print(f"  Filter               : {filter_mode}")
    print(f"  Baseline             : threshold={vlev_base.get('dd_threshold'):+.2f}  "
          f"scale={vlev_base.get('dd_scale'):.1f}")
    print("═" * 78)

    baseline_thr   = float(vlev_base.get("dd_threshold", VOL_LEV_DD_THRESHOLD))
    baseline_scale = float(vlev_base.get("dd_scale",     VOL_LEV_DD_SCALE))

    rows = []
    for cell_idx, (thr, sc) in enumerate(
            itertools.product(dd_thresholds, dd_scales), start=1):

        vlev = deepcopy(vlev_base)
        vlev["dd_threshold"] = thr
        vlev["dd_scale"]     = sc

        out      = simulate(df_4x, base_params, filter_mode, v3_filter,
                            symbol_counts=symbol_counts,
                            vol_lev_params=vlev,
                            verbose=False)
        daily_wz = np.asarray(out["daily"], dtype=float)

        # Active returns: non-zero AND finite
        active = daily_wz[np.isfinite(daily_wz) & (daily_wz != 0.0)]
        n_act  = len(active)

        # ── Sharpe (full series — zeros count for flat days) ───────────
        r_full = np.where(np.isfinite(daily_wz), daily_wz, 0.0)
        _std   = float(np.std(r_full, ddof=1))
        sharpe = float(np.mean(r_full) / _std * 365 ** 0.5) \
                 if (_std > 0 and len(r_full) > 1) else np.nan

        # ── Equity curve & total return ────────────────────────────────
        eq_val  = float(np.prod(1.0 + active)) if n_act > 0 else 1.0
        tot_ret = (eq_val - 1.0) * 100.0       # %

        # ── CAGR (annualised over full calendar span) ──────────────────
        cagr = float((eq_val ** (365.0 / max(n_total, 1)) - 1.0) * 100.0) \
               if (n_total > 0 and eq_val > 0.0) else np.nan

        # ── MaxDD (from active equity curve) ──────────────────────────
        if n_act > 0:
            eq_curve = np.cumprod(1.0 + active)
            peak_c   = np.maximum.accumulate(eq_curve)
            maxdd    = float(((eq_curve - peak_c) / peak_c).min() * 100.0)
        else:
            maxdd = np.nan

        # ── Walk-Forward CV (lightweight rolling folds) ────────────────
        wf_cv = _sweep_wf_cv(_sweep_folds(daily_wz))

        # ── Worst single active day ────────────────────────────────────
        wst1d = float(np.min(active) * 100.0) if n_act > 0 else np.nan

        # ── Worst 1-week window (rolling 5-bar, log-sum trick) ─────────
        if n_act >= 5:
            _log  = np.log1p(active)
            _cs   = np.concatenate([[0.0], np.cumsum(_log)])
            _wlog = _cs[5:] - _cs[:-5]
            wst1w = float((np.exp(np.min(_wlog)) - 1.0) * 100.0)
        else:
            wst1w = np.nan

        # ── Worst 1-month window (rolling 21-bar, log-sum trick) ───────
        if n_act >= 21:
            _log  = np.log1p(active)
            _cs   = np.concatenate([[0.0], np.cumsum(_log)])
            _wlog = _cs[21:] - _cs[:-21]
            wst1m = float((np.exp(np.min(_wlog)) - 1.0) * 100.0)
        else:
            wst1m = np.nan

        # ── DSR% (simplified Bailey-López de Prado) ────────────────────
        try:
            _sr_obs = sharpe / 365.0 ** 0.5
            _skew   = float(pd.Series(r_full).skew())
            _kurt   = float(pd.Series(r_full).kurtosis())
            _denom  = (max(1 - _skew * _sr_obs
                           + (_kurt / 4.0) * _sr_obs ** 2, 1e-12)) ** 0.5
            _dsr_z  = (_sr_obs - 0.0) * (len(r_full) ** 0.5) / _denom
            dsr     = float(_scipy_norm.cdf(_dsr_z) * 100.0)
        except Exception:
            dsr = np.nan

        is_bl = (abs(thr - baseline_thr) < 1e-9 and
                 abs(sc  - baseline_scale) < 1e-9)

        rows.append({
            "dd_threshold": thr,
            "dd_scale":     sc,
            "Sharpe":       round(sharpe, 4) if np.isfinite(sharpe) else np.nan,
            "CAGR%":        round(cagr,   2) if np.isfinite(cagr)   else np.nan,
            "MaxDD%":       round(maxdd,  3) if np.isfinite(maxdd)  else np.nan,
            "Active":       n_act,
            "WF-CV":        round(wf_cv,  4) if np.isfinite(wf_cv)  else np.nan,
            "TotRet%":      round(tot_ret, 2),
            "Eq":           round(eq_val,  4),
            "Wst1D%":       round(wst1d,  3) if np.isfinite(wst1d)  else np.nan,
            "Wst1W%":       round(wst1w,  3) if np.isfinite(wst1w)  else np.nan,
            "Wst1M%":       round(wst1m,  3) if np.isfinite(wst1m)  else np.nan,
            "DSR%":         round(dsr,    2) if np.isfinite(dsr)    else np.nan,
            "baseline":     is_bl,
        })

        bl_tag = "  ← BASELINE" if is_bl else ""
        print(f"  [{cell_idx:>3}/{n_cells}]  "
              f"thr={thr:+.2f}  sc={sc:.1f}  |  "
              f"Sharpe={sharpe:>6.3f}  CAGR={cagr:>7.1f}%  "
              f"MaxDD={maxdd:>7.2f}%  Active={n_act:>3d}  "
              f"WF-CV={wf_cv:>6.3f}  DSR={dsr:>5.1f}%{bl_tag}")

    # ── Build DataFrame, rank by Sharpe ───────────────────────────────
    df_res = pd.DataFrame(rows)
    df_sorted = df_res.sort_values("Sharpe", ascending=False).reset_index(drop=True)

    # ── Save CSV ───────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "grid_search_vol_lev_dd_results.csv"
    df_res.sort_values(["dd_threshold", "dd_scale"]).to_csv(csv_path, index=False)
    print(f"\n  Full results saved → {csv_path}")

    # ── Console ranked table (top-30 by Sharpe) ───────────────────────
    _top = df_sorted.head(30)
    print("\n" + "═" * 78)
    print("  VOL-LEV DD GRID SEARCH — TOP 30 RANKED BY SHARPE")
    print("═" * 78)
    _hdr = (f"  {'#':>3}  {'Threshold':>10}  {'Scale':>6}  "
            f"{'Sharpe':>7}  {'CAGR%':>7}  {'MaxDD%':>8}  "
            f"{'Active':>7}  {'WF-CV':>7}  {'TotRet%':>9}  "
            f"{'Eq':>7}  {'Wst1D%':>7}  {'Wst1W%':>7}  "
            f"{'Wst1M%':>7}  {'DSR%':>6}")
    print(_hdr)
    print("  " + "-" * (len(_hdr) - 2))
    for rank_i, row in _top.iterrows():
        _bl = " *" if row["baseline"] else "  "
        print(f"  {rank_i+1:>3}{_bl} "
              f"{row['dd_threshold']:>+10.2f}  {row['dd_scale']:>6.1f}  "
              f"{row['Sharpe']:>7.3f}  {row['CAGR%']:>7.1f}  "
              f"{row['MaxDD%']:>8.2f}  "
              f"{int(row['Active']):>7d}  {row['WF-CV']:>7.3f}  "
              f"{row['TotRet%']:>9.1f}  {row['Eq']:>7.4f}  "
              f"{row['Wst1D%']:>7.2f}  {row['Wst1W%']:>7.2f}  "
              f"{row['Wst1M%']:>7.2f}  {row['DSR%']:>6.1f}")
    print("  (* = current baseline configuration)")

    # ── Best cell summary ──────────────────────────────────────────────
    best = df_sorted.iloc[0]
    bl_row = df_res[df_res["baseline"]].iloc[0] if df_res["baseline"].any() else None
    print("\n" + "═" * 78)
    print("  BEST CELL (highest Sharpe)")
    print("═" * 78)
    print(f"  dd_threshold : {best['dd_threshold']:+.2f}")
    print(f"  dd_scale     :  {best['dd_scale']:.1f}")
    print(f"  Sharpe       :  {best['Sharpe']:.4f}")
    print(f"  CAGR%        :  {best['CAGR%']:.2f}%")
    print(f"  MaxDD%       :  {best['MaxDD%']:.2f}%")
    print(f"  Active       :  {int(best['Active'])}d")
    print(f"  WF-CV        :  {best['WF-CV']:.4f}")
    print(f"  TotRet%      :  {best['TotRet%']:.2f}%")
    print(f"  Eq           :  {best['Eq']:.4f}×")
    print(f"  Wst1D%       :  {best['Wst1D%']:.2f}%")
    print(f"  Wst1W%       :  {best['Wst1W%']:.2f}%")
    print(f"  Wst1M%       :  {best['Wst1M%']:.2f}%")
    print(f"  DSR%         :  {best['DSR%']:.1f}%")
    if bl_row is not None:
        sharpe_uplift = best["Sharpe"] - bl_row["Sharpe"]
        print(f"\n  Sharpe vs baseline ({bl_row['dd_threshold']:+.2f}/{bl_row['dd_scale']:.1f}): "
              f"{'+'  if sharpe_uplift >= 0 else ''}{sharpe_uplift:.4f}")
    print("═" * 78)

    return df_sorted


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    global CAPITAL_MODE, FIXED_NOTIONAL_CAP
    run_dir = Path(OUTPUT_DIR_BASE) / dt.now().strftime("run_%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    # ── Mirror stdout to audit_output.txt inside this run folder ─────
    class _Tee:
        def __init__(self, *streams): self._streams = streams
        def write(self, data):
            for s in self._streams: s.write(data)
        def flush(self):
            for s in self._streams: s.flush()
        def fileno(self): return self._streams[0].fileno()

    _log_path    = run_dir / "audit_output.txt"
    _log_fh      = open(_log_path, "w", buffering=1, encoding="utf-8")
    _orig_stdout = sys.stdout
    sys.stdout   = _Tee(_orig_stdout, _log_fh)
    print(f"  [log] Writing audit output to: {_log_path}")

    # ── Snapshot the source file that generated this run ──────────────
    try:
        _src_path  = Path(__file__).resolve()
        _snap_path = run_dir / "audit_snapshot.txt"
        shutil.copy2(_src_path, _snap_path)
        print(f"  [log] Source snapshot saved: {_snap_path.name}")
    except Exception as _se:
        print(f"  [log] Could not snapshot source: {_se}")

    print("\n" + "═" * 70)
    print("  REGIME FILTER COMPARISON - COMPREHENSIVE AUDIT")
    print("  Candidate Configs × {No Filter | Tail Guardrail | Dispersion | Tail+Dispersion}")
    print(f"  TRIAL_PURCHASES  = {TRIAL_PURCHASES}")
    print(f"  N_TRIALS         = {N_TRIALS}")
    print(f"  STARTING_CAPITAL = ${STARTING_CAPITAL:,.0f}")
    print(f"  TAKER_FEE_PCT    = {TAKER_FEE_PCT*100:.4f}%  ({TAKER_FEE_PCT*100/2:.4f}% per side)")
    print(f"  FUNDING_RATE_PCT = {FUNDING_RATE_DAILY_PCT*100:.4f}%/day")
    print(f"  PRINT_FEES_PANEL = {PRINT_FEES_PANEL}")
    _sweep_flags = []
    if ENABLE_SWEEP_TAIL_GUARDRAIL: _sweep_flags.append(
        f"TailGuardrail({len(TAIL_SWEEP_DROP_VALUES)}drop×{len(TAIL_SWEEP_VOL_VALUES)}vol"        f"={len(TAIL_SWEEP_DROP_VALUES)*len(TAIL_SWEEP_VOL_VALUES)}cells)")
    if ENABLE_SWEEP_L_HIGH:        _sweep_flags.append("L_HIGH")
    if ENABLE_SWEEP_TRAIL_WIDE:    _sweep_flags.append("TrailWide")
    if ENABLE_SWEEP_TRAIL_NARROW:  _sweep_flags.append("TrailNarrow")
    print(f"  Sweeps enabled   = {_sweep_flags if _sweep_flags else "none"}")
    print("  Filter toggles:")
    _toggle_map = [
        ("No Filter",        RUN_FILTER_NONE),
        ("Calendar",         RUN_FILTER_CALENDAR),
        ("Tail",             RUN_FILTER_TAIL),
        ("Dispersion",       RUN_FILTER_DISPERSION),
        ("Tail+Disp",        RUN_FILTER_TAIL_DISP),
        ("Vol",              RUN_FILTER_VOL),
        ("Tail+Vol (OR)",     RUN_FILTER_TAIL_OR_VOL),
        ("Tail+Vol (AND)",    RUN_FILTER_TAIL_AND_VOL),
        ("Tail+Disp+Vol",    RUN_FILTER_TAIL_DISP_VOL),
        ("Tail+Blofin",      RUN_FILTER_TAIL_BLOFIN),
    ]
    for _fname, _fon in _toggle_map:
        _state = "ON " if _fon else "OFF"
        print(f"    [{_state}]  {_fname}")
    print("  Candidate configs:")
    for cfg_name, cfg_params in CANDIDATE_CONFIGS:
        print(f"    [{cfg_name}]  L_HIGH={cfg_params['L_HIGH']}  L_BASE={cfg_params['L_BASE']}  "
              f"PORT_TSL={cfg_params['PORT_TSL']}  PORT_SL={cfg_params.get('PORT_SL')}")
    print("  Leverage scaling models:")
    if ENABLE_PERF_LEV_SCALING:
        print(f"    [ON]  PERF_LEV   window={PERF_LEV_WINDOW}d  "
              f"sortino_target={PERF_LEV_SORTINO_TARGET}  dr_floor={PERF_LEV_DR_FLOOR}  "
              f"max_boost={PERF_LEV_MAX_BOOST}x  [contrarian: boosts when Sortino LOW]")
    else:
        print("    [OFF] PERF_LEV")
    if ENABLE_VOL_LEV_SCALING:
        print(f"    [ON]  VOL_LEV    window={VOL_LEV_WINDOW}d  "
              f"target_vol={VOL_LEV_TARGET_VOL*100:.1f}%  sharpe_ref={VOL_LEV_SHARPE_REF}  "
              f"dd_thr={VOL_LEV_DD_THRESHOLD*100:.0f}%  max_boost={VOL_LEV_MAX_BOOST}x  "
              f"quant={LEV_QUANTIZATION_MODE}"
              f"{(f' step={LEV_QUANTIZATION_STEP:g}' if LEV_QUANTIZATION_MODE == 'stepped' else '')}  "
              f"[contrarian: boosts when vol LOW + Sharpe LOW]")
    else:
        print("    [OFF] VOL_LEV")
    if ENABLE_CONTRA_LEV_SCALING:
        print(f"    [ON]  CONTRA_LEV window={CONTRA_LEV_WINDOW}d  "
              f"signals={CONTRA_LEV_SIGNALS}  max_boost={CONTRA_LEV_MAX_BOOST}x  "
              f"dd_thr={CONTRA_LEV_DD_THRESHOLD*100:.0f}%  [percentile-rank contrarian]")
    else:
        print("    [OFF] CONTRA_LEV")
    print()
    print("═" * 70 + "\n")

    # ── 1. Fetch live signals ─────────────────────────────────────────
    print("Fetching live regime signals ...")
    fr = fetch_funding_rate()
    fg = fetch_fear_greed()
    oi_df       = fetch_btc_open_interest()
    multi_oi_df = fetch_multi_asset_oi()

    print("\n  Fetching BTC OHLCV for V4 ADX/rvol (yfinance) ...")
    if yf is None:
        raise ImportError("yfinance not installed — pip install yfinance")
    try:
        btc_raw = yf.Ticker("BTC-USD").history(start="2014-11-01", end=None, interval="1d", auto_adjust=True)
        btc_raw.index = pd.to_datetime(btc_raw.index).tz_localize(None)
        btc_ohlcv = btc_raw[["Open","High","Low","Close","Volume"]].copy()
        btc_ohlcv.columns = ["open","high","low","close","volume"]
        print(f"    -> {len(btc_ohlcv)} rows")
    except Exception as e:
        print(f"    ! yfinance failed: {e} - V4 Branch B will be empty")
        btc_ohlcv = pd.DataFrame()

    # ── 2. Load matrix ────────────────────────────────────────────────
    print("\nLoading pivot matrix from Google Sheets ...")
    df_4x = load_matrix()

    # ── Sync N_ROWS to the actual matrix row count ────────────────────
    # When END_CROSS_MIDNIGHT=False the matrix may have fewer rows than
    # DEPLOYMENT_RUNTIME_HOURS implies (session capped at 23:55 same day).
    # Driving N_ROWS from the matrix itself ensures simulate() never pads
    # with phantom zeros or reads past the end of real data.
    global N_ROWS
    _actual_rows = len(df_4x)
    _configured_rows = N_ROWS
    if _actual_rows != _configured_rows:
        print(f"  ⚠  N_ROWS mismatch: configured={_configured_rows} "
              f"actual={_actual_rows} — using actual.")
        print(f"     (This is expected when --no-end-cross-midnight truncates "
              f"Expected when --no-end-cross-midnight truncates the session.)")
        N_ROWS = _actual_rows
    else:
        print(f"  ✓  N_ROWS={N_ROWS} matches matrix row count.")
    sample_cols = list(df_4x.columns[:8])
    parsed = [str(_col_to_timestamp(str(c))) for c in sample_cols]
    print(f"  ✅ {df_4x.shape[0]} rows × {df_4x.shape[1]} days")
    print(f"  Column format sample (raw):   {sample_cols}")
    print(f"  Parsed as dates:              {parsed}")
    # Extra: show ALL unique column prefixes/lengths to catch unexpected formats
    col_types = {}
    for c in df_4x.columns:
        s = str(c).strip()
        key = f"len={len(s)} prefix='{s[:6]}'"
        col_types[key] = col_types.get(key, 0) + 1
    print(f"  Column format breakdown:      {col_types}\n")

    # ── 2b. Load symbol counts from deploys CSV (for fee calculation) ───
    symbol_counts = None
    try:
        _dep_path = LOCAL_MATRIX_CSV.replace("portfolio_matrix_gated.csv", "").rstrip("/\\")
        # Try to find a deploys CSV adjacent to the matrix
        _candidates = (
            glob.glob(str(Path(LOCAL_MATRIX_CSV).parent / "deploys_*.csv"))
            if LOCAL_MATRIX_CSV else []
        )
        # Also check the args-provided deploys path (stored globally after CLI parse)
        if not _candidates and "_DEPLOYS_PATH" in globals() and _DEPLOYS_PATH:
            _candidates = [_DEPLOYS_PATH]
        if _candidates:
            _dep_df = pd.read_csv(_candidates[0])
            if "deploy_count" in _dep_df.columns and "timestamp_utc" in _dep_df.columns:
                _dep_df["_date"] = pd.to_datetime(_dep_df["timestamp_utc"]).dt.normalize()
                symbol_counts = _dep_df.set_index("_date")["deploy_count"].astype(int)
                print(f"  [costs] Loaded symbol counts from {Path(_candidates[0]).name} "
                      f"({len(symbol_counts)} days, avg={symbol_counts.mean():.1f} syms/day)")
            else:
                # Try 'assets' column — count pipe-separated symbols
                if "assets" in _dep_df.columns:
                    _dep_df["_date"] = pd.to_datetime(_dep_df.iloc[:,0]).dt.normalize()
                    _dep_df["_n"] = _dep_df["assets"].apply(
                        lambda x: len(str(x).split("|")) if pd.notna(x) and str(x).strip() else 0
                    )
                    symbol_counts = _dep_df.set_index("_date")["_n"].astype(int)
                    print(f"  [costs] Inferred symbol counts from assets col "
                          f"({len(symbol_counts)} days, avg={symbol_counts.mean():.1f} syms/day)")
    except Exception as _e:
        print(f"  [costs] Could not load symbol counts ({_e}) — using flat estimate of 7")

    # ── 3. Run simulations ────────────────────────────────────────────
    FILTER_MODES = []
    if RUN_FILTER_NONE:
        FILTER_MODES.append(("No Filter",       "none",     None))
    if RUN_FILTER_CALENDAR:
        FILTER_MODES.append(("Calendar Filter", "calendar", None))
    # HMMs dropped - using dispersion filter instead

    # Tail-event guardrail (Change C) - simple rules, no ML
    tail_filter = None
    if ENABLE_TAIL_GUARDRAIL and not btc_ohlcv.empty:
        print(f"\nBuilding Tail Guardrail "
              f"(drop<-{TAIL_DROP_PCT*100:.0f}%, vol>{TAIL_VOL_MULT:.1f}×baseline) ...")
        try:
            tail_filter = build_tail_guardrail(
                btc_ohlcv,
                drop_pct = TAIL_DROP_PCT,
                vol_mult = TAIL_VOL_MULT,
            )
            if RUN_FILTER_TAIL:
                FILTER_MODES.append(("Tail Guardrail", "tail", tail_filter))
        except Exception as e:
            print(f"    ! Tail guardrail build failed: {e}")


    # ── Determine return fetch universe ──────────────────────────────
    # Dynamic mode: fetch ALL mapped Binance tickers so the top-N
    #   selection has all candidates available.
    # Static mode:  fetch only the hardcoded DISPERSION_SYMBOLS list —
    #   same as before, using the same cache file.
    if DISPERSION_DYNAMIC_UNIVERSE:
        _fetch_symbols = list(dict.fromkeys(COINGECKO_TO_BINANCE.values()))
        _ret_cache     = DISPERSION_DYNAMIC_RETURNS_CACHE_FILE
    else:
        _fetch_symbols = DISPERSION_SYMBOLS
        _ret_cache     = DISPERSION_CACHE_FILE

    # Cross-sectional price returns — fetched unconditionally so OI-price
    # alignment metrics are available even when the dispersion filter is off.
    print("\nFetching altcoin daily returns for OI-price alignment ...")
    try:
        alt_rets = fetch_altcoin_daily_returns(
            symbols    = _fetch_symbols,
            cache_file = _ret_cache,
        )
    except Exception as e:
        print(f"    ! fetch_altcoin_daily_returns failed: {e}")
        alt_rets = pd.DataFrame()

    # ── Market cap history + dynamic symbol mask (dynamic mode only) ──
    mcap_df     = pd.DataFrame()
    symbol_mask = None

    if DISPERSION_DYNAMIC_UNIVERSE and ENABLE_DISPERSION_FILTER:
        print(f"\nFetching market cap history for dynamic dispersion universe "
              f"(top-{DISPERSION_N} per day, 1-day lag) ...")
        try:
            mcap_df = fetch_mcap_history()
            if not mcap_df.empty and not alt_rets.empty:
                symbol_mask = build_dynamic_symbol_mask(
                    mcap_df     = mcap_df,
                    alt_returns = alt_rets,
                    n           = DISPERSION_N,
                )
            else:
                print("    ! mcap_df or alt_rets empty — dynamic mode unavailable")
        except Exception as e:
            print(f"    ! Market cap fetch/mask failed: {e}")

        if symbol_mask is None:
            # Fallback: narrow alt_rets to DISPERSION_SYMBOLS so the effective
            # universe matches static mode exactly, not an unintended 90-symbol run.
            print("    [dynamic fallback] Reverting to DISPERSION_SYMBOLS "
                  "(static list) — mcap data unavailable")
            if not alt_rets.empty:
                static_cols = [c for c in DISPERSION_SYMBOLS if c in alt_rets.columns]
                if static_cols:
                    alt_rets = alt_rets[static_cols]

    # ── Cross-sectional dispersion filter ─────────────────────────────
    disp_filter = None
    combo_filter = None
    if ENABLE_DISPERSION_FILTER:
        _mode_str = (f"dynamic top-{DISPERSION_N}" if DISPERSION_DYNAMIC_UNIVERSE
                     else f"static {len(DISPERSION_SYMBOLS)} symbols")
        print(f"\nBuilding Dispersion Filter [{_mode_str}] "
              f"(threshold={DISPERSION_THRESHOLD}, baseline={DISPERSION_BASELINE_WIN}d) ...")
        try:
            if not alt_rets.empty:
                disp_filter = build_dispersion_filter(
                    alt_rets,
                    threshold      = DISPERSION_THRESHOLD,
                    baseline_win   = DISPERSION_BASELINE_WIN,
                    symbol_mask_df = symbol_mask,  # None → static path (original behaviour)
                )
                if RUN_FILTER_DISPERSION:
                    FILTER_MODES.append(("Dispersion", "dispersion", disp_filter))

                # Combined: sit out if EITHER tail OR dispersion fires
                if ENABLE_TAIL_PLUS_DISP and tail_filter is not None:
                    combo_filter = (tail_filter | disp_filter)
                    n_tail  = int(tail_filter.sum())
                    n_disp  = int(disp_filter.sum())
                    n_combo = int(combo_filter.sum())
                    n_both  = int((tail_filter & disp_filter).sum())
                    print(f"    Tail+Disp combo: tail={n_tail}d  disp={n_disp}d  "
                          f"overlap={n_both}d  total={n_combo}d flagged")
                    if RUN_FILTER_TAIL_DISP:
                        FILTER_MODES.append(("Tail + Dispersion", "tail_disp", combo_filter))
        except Exception as e:
            print(f"    ! Dispersion filter build failed: {e}")

    # ── Realised Volatility Gate (Case C fix) ─────────────────────────
    vol_filter = None
    if ENABLE_VOL_FILTER and not btc_ohlcv.empty:
        print(f"\nBuilding Vol Filter "
              f"(lookback={VOL_LOOKBACK}d, p{VOL_PERCENTILE*100:.0f}, "
              f"baseline={VOL_BASELINE_WIN}d) ...")
        try:
            vol_filter = build_vol_filter(
                btc_ohlcv,
                lookback     = VOL_LOOKBACK,
                percentile   = VOL_PERCENTILE,
                baseline_win = VOL_BASELINE_WIN,
            )
            if RUN_FILTER_VOL:
                FILTER_MODES.append(("Volatility", "vol", vol_filter))

            # Triple combo: tail always enforced; normal days sit out only if
            # BOTH dispersion weak AND vol compressed (prevents over-filtering)
            if ENABLE_TAIL_DISP_VOL and combo_filter is not None:
                triple_filter = (tail_filter | (disp_filter & vol_filter))
                n_vol    = int(vol_filter.sum())
                n_combo  = int(combo_filter.sum())
                n_triple = int(triple_filter.sum())
                n_new    = n_triple - n_combo
                print(f"    Tail+Disp+Vol triple: existing={n_combo}d  "
                      f"vol_adds={n_new}d  total={n_triple}d flagged")
                if RUN_FILTER_TAIL_DISP_VOL:
                    FILTER_MODES.append(("Tail + Disp + Vol", "tail_disp_vol", triple_filter))

            # Tail OR Vol: sit flat when tail fires OR vol is compressed.
            # More conservative — flags ~150d. Tests vol gate value without dispersion.
            if ENABLE_TAIL_OR_VOL and tail_filter is not None:
                tail_or_vol_filter = (tail_filter | vol_filter)
                n_tail    = int(tail_filter.sum())
                n_vol_f   = int(vol_filter.sum())
                n_tv      = int(tail_or_vol_filter.sum())
                n_overlap = int((tail_filter & vol_filter).sum())
                print(f"    Tail OR Vol: tail={n_tail}d  vol={n_vol_f}d  "
                      f"overlap={n_overlap}d  total={n_tv}d flagged")
                if RUN_FILTER_TAIL_OR_VOL:
                    FILTER_MODES.append(("Tail + Vol (OR)", "tail_or_vol", tail_or_vol_filter))

            # Tail AND Vol: sit flat only when BOTH tail AND vol fire on the same day.
            # More selective — flags only the intersection. Tests for days where crash
            # risk and vol compression coincide (unusual but high-conviction sit-out).
            if ENABLE_TAIL_AND_VOL and tail_filter is not None:
                tail_and_vol_filter = (tail_filter & vol_filter)
                n_and = int(tail_and_vol_filter.sum())
                print(f"    Tail AND Vol: intersection={n_and}d flagged "
                      f"(tail={int(tail_filter.sum())}d ∩ vol={int(vol_filter.sum())}d)")
                if RUN_FILTER_TAIL_AND_VOL:
                    FILTER_MODES.append(("Tail + Vol (AND)", "tail_and_vol", tail_and_vol_filter))
        except Exception as e:
            print(f"    ! Vol filter build failed: {e}")

    # ── Blofin Availability Filter ─────────────────────────────────────
    # Tail guardrail + Blofin exchange availability gate.
    # Uses current Blofin instrument list as a proxy for historical availability
    # (accurate going forward once daily snapshots are collected via BLOFIN_CSV_PATH).
    blofin_filter = None
    if ENABLE_BLOFIN_FILTER and tail_filter is not None:
        print(f"\nBuilding Tail + Blofin Filter "
              f"(min_symbols={BLOFIN_MIN_SYMBOLS}, "
              f"csv={'yes' if BLOFIN_CSV_PATH else 'no — using live API'}) ...")
        try:
            blofin_filter = build_blofin_filter(
                df_4x,
                deploys_path      = _DEPLOYS_PATH if "_DEPLOYS_PATH" in globals() and _DEPLOYS_PATH else "",
                blofin_csv        = BLOFIN_CSV_PATH,
                min_symbols       = BLOFIN_MIN_SYMBOLS,
                tail_filter       = tail_filter,
                portfolio_symbols = BLOFIN_PORTFOLIO_SYMBOLS or None,
            )
            if RUN_FILTER_TAIL_BLOFIN:
                FILTER_MODES.append(("Tail + Blofin", "tail_blofin", blofin_filter))
        except Exception as e:
            print(f"    ! Blofin filter build failed: {e}")
    elif ENABLE_BLOFIN_FILTER and tail_filter is None:
        print("\n  [Blofin] Skipping Blofin filter — tail guardrail unavailable "
              "(required as base layer)")

    # ── BTC Trend (Moving Average) Filter ─────────────────────────────
    results_map = {}
    _exit_bars_by_filter: dict = {}  # {label: {YYYY-MM-DD: bar_idx}}
    _fees_rows_by_filter: dict = {}  # {label: [(date, start, margin, lev, ...)]}

    for cfg_name, cfg_params in CANDIDATE_CONFIGS:
      for label, mode, v3 in FILTER_MODES:
        run_key = f"{cfg_name} - {label}"
        print("\n" + "═" * 140)
        print(f"  SIMULATING: {run_key}")
        print("═" * 140)

        outdir = run_dir / run_key.replace(" ", "_").replace("+", "_").replace("-", "-")
        outdir.mkdir(parents=True, exist_ok=True)
        generated_files: List[str] = []

        def track_file(fn, _d=outdir):
            p = str((_d / fn).resolve())
            generated_files.append(p)
            return p

        # ── Choose perf-lev params (None = static leverage as before) ─
        _plev_params = None
        if ENABLE_PERF_LEV_SCALING:
            _plev_params = dict(
                window         = PERF_LEV_WINDOW,
                sortino_target = PERF_LEV_SORTINO_TARGET,
                dr_floor       = PERF_LEV_DR_FLOOR,
                max_boost      = PERF_LEV_MAX_BOOST,
            )

        _vlev_params = None
        if ENABLE_VOL_LEV_SCALING:
            _vlev_params = dict(
                target_vol   = VOL_LEV_TARGET_VOL,
                window       = VOL_LEV_WINDOW,
                sharpe_ref   = VOL_LEV_SHARPE_REF,
                dd_threshold = VOL_LEV_DD_THRESHOLD,
                dd_scale     = VOL_LEV_DD_SCALE,
                max_boost    = VOL_LEV_MAX_BOOST,
            )

        _clev_params = None
        if ENABLE_CONTRA_LEV_SCALING:
            _clev_params = dict(
                signals      = CONTRA_LEV_SIGNALS,
                window       = CONTRA_LEV_WINDOW,
                max_boost    = CONTRA_LEV_MAX_BOOST,
                dd_threshold = CONTRA_LEV_DD_THRESHOLD,
            )

        _fees_csv = str(outdir / f"fees_panel_{cfg_name}_{mode}.csv") \
                    if PRINT_FEES_PANEL else None

        _pph_instance = None
        if ENABLE_PPH:
            _pph_instance = PeriodicProfitHarvest(
                frequency    = PPH_FREQUENCY,
                threshold    = PPH_THRESHOLD,
                harvest_frac = PPH_HARVEST_FRAC,
            )

        _ratchet_instance = None
        if ENABLE_RATCHET:
            _ratchet_instance = EquityRatchet(
                frequency          = RATCHET_FREQUENCY,
                trigger            = RATCHET_TRIGGER,
                lock_pct           = RATCHET_LOCK_PCT,
                risk_off_lev_scale = RATCHET_RISK_OFF_LEV_SCALE,
            )

        _adaptive_ratchet_instance = None
        _vol_regime_series         = None
        if ENABLE_ADAPTIVE_RATCHET:
            # Build vol regime series from the returns already computed.
            # On first call daily_with_zeros may not exist yet — use the
            # entire df_4x column sum as a proxy vol source.  In practice,
            # the adaptive ratchet is most useful when the main audit has
            # already run at least one filter mode, so we use a best-effort
            # approach: try to use the most recent daily_with_zeros if
            # available; otherwise fall back to a zero series (warm-up).
            try:
                _ret_src = pd.Series(
                    daily_with_zeros if "daily_with_zeros" in dir() else [],
                    dtype=float)
            except Exception:
                _ret_src = pd.Series([], dtype=float)
            # Index the series to the df_4x column dates
            _col_dates = [_col_to_timestamp(str(c)) for c in df_4x.columns]
            _col_dates = [d for d in _col_dates if d is not None]
            if len(_ret_src) == len(_col_dates):
                _ret_src = pd.Series(_ret_src.values, index=pd.DatetimeIndex(_col_dates))
            else:
                _ret_src = pd.Series(0.0, index=pd.DatetimeIndex(_col_dates))
            _vol_regime_series = build_vol_regime_series(
                daily_returns = _ret_src,
                vol_window    = ADAPTIVE_RATCHET_VOL_WINDOW,
                vol_low       = ADAPTIVE_RATCHET_VOL_LOW,
                vol_high      = ADAPTIVE_RATCHET_VOL_HIGH,
            )
            _adaptive_ratchet_instance = AdaptiveEquityRatchet(
                frequency          = ADAPTIVE_RATCHET_FREQUENCY,
                regime_table       = ADAPTIVE_RATCHET_TABLE,
                risk_off_lev_scale = ADAPTIVE_RATCHET_RISK_OFF_SCALE,
                floor_decay_rate   = ADAPTIVE_RATCHET_FLOOR_DECAY,
            )

        _sim_out         = simulate(df_4x, cfg_params, mode, v3,
                                    symbol_counts=symbol_counts,
                                    perf_lev_params=_plev_params,
                                    vol_lev_params=_vlev_params,
                                    contra_lev_params=_clev_params,
                                    fees_csv_path=_fees_csv,
                                    pph=_pph_instance,
                                    ratchet=_ratchet_instance,
                                    adaptive_ratchet=_adaptive_ratchet_instance,
                                    vol_regime_series=_vol_regime_series)
        daily_with_zeros         = _sim_out["daily"]
        _sim_total_gross         = _sim_out["total_gross"]   # stored for scorecard
        _sim_total_fees          = _sim_out["total_fees"]
        _sim_total_net           = _sim_out["total_net"]
        _sim_trade_days          = _sim_out["trade_days"]
        _sim_avg_fee_active_day  = _sim_out["avg_fee_per_active_day"]   # % per active day
        _sim_avg_fee_cal_day     = _sim_out["avg_fee_per_cal_day"]      # % per calendar day
        _sim_avg_lev             = _sim_out["avg_lev"]
        _sim_lev_high_days       = _sim_out["lev_high_days"]
        _sim_lev_base_days       = _sim_out["lev_base_days"]
        _sim_lev_high_winrate    = _sim_out["lev_high_winrate"]
        _sim_lev_base_winrate    = _sim_out["lev_base_winrate"]
        _sim_lev_high_avg_ret    = _sim_out["lev_high_avg_ret"]
        _sim_lev_base_avg_ret    = _sim_out["lev_base_avg_ret"]
        _sim_perf_lev_history    = _sim_out.get("perf_lev_scalar_history", [])
        _sim_vol_lev_history     = _sim_out.get("vol_lev_scalar_history",   [])
        _sim_contra_lev_history  = _sim_out.get("contra_lev_scalar_history", [])
        _sim_fees_rows           = _sim_out.get("fees_rows", [])
        _exit_bars_by_filter[run_key] = _sim_out.get("exit_bars", {})
        _fees_rows_by_filter[run_key] = list(_sim_fees_rows)

        # ── Weekly / Monthly milestone tables ─────────────────────────
        if ENABLE_WEEKLY_MILESTONES and _sim_fees_rows:
            _milestone_table(_sim_fees_rows, "W", STARTING_CAPITAL, label)
        if ENABLE_MONTHLY_MILESTONES and _sim_fees_rows:
            _milestone_table(_sim_fees_rows, "M", STARTING_CAPITAL, label)

        # ── Math verification diagnostic export ───────────────────────
        if ENABLE_DIAGNOSTIC_EXPORT:
            _diag_key_metrics = {
                "Sharpe":       round(float(np.mean(np.where(np.isfinite(daily_with_zeros),
                                    daily_with_zeros, 0.0))
                                    / max(float(np.std(np.where(np.isfinite(daily_with_zeros),
                                    daily_with_zeros, 0.0), ddof=1)), 1e-9)
                                    * TRADING_DAYS**0.5), 4),
                "CAGR%":        round(_sim_total_net * 100 / max(len(daily_with_zeros), 1)
                                    * TRADING_DAYS, 4),
                "TotalNet%":    round(_sim_total_net * 100, 4),
                "TotalGross%":  round(_sim_total_gross * 100, 4),
                "TotalFees%":   round(_sim_total_fees * 100, 4),
                "TradeDays":    _sim_trade_days,
                "AvgFeeActive%": round(_sim_avg_fee_active_day, 4),
                "CapitalMode":  CAPITAL_MODE,
                "FixedCap":     FIXED_NOTIONAL_CAP if CAPITAL_MODE == "fixed" else "n/a",
            }
            export_diagnostic_to_sheets(
                df_4x       = df_4x,
                params      = cfg_params,
                filter_mode = mode,
                v3_filter   = v3,
                col_dates   = list(df_4x.columns),
                run_label   = label,
                key_metrics = _diag_key_metrics,
            )

        # ── Daily series audit (T11-T16) ──────────────────────────────
        if not QUICK_MODE:
            print(f"\nRunning daily series audit ({label}) ...")
            ds_audit = run_daily_series_audit(
                df_4x            = df_4x,
                params           = cfg_params,
                filter_mode      = mode,
                v3_filter        = v3,
                label            = run_key,
                daily_with_zeros = daily_with_zeros,
            )
            print_daily_series_audit_report(ds_audit, label=run_key)

        # --- Regime attribution diagnostic ---
        if not QUICK_MODE and label == "No Filter":
            try:
                btc_daily_returns = btc_ohlcv["close"].pct_change()
                regime_attribution(
                    daily_returns = pd.Series(daily_with_zeros),
                    btc_returns   = btc_daily_returns
                )
            except Exception as e:
                print(e)
        # flat_days = columns where filter fired (stored as exactly 0.0)
        flat_days = int((daily_with_zeros == 0.0).sum()) if mode != "none" else 0
        # daily used for stats: drop flat (0.0) AND non-finite (nan) entries
        if mode != "none":
            daily = daily_with_zeros[
                np.isfinite(daily_with_zeros) & (daily_with_zeros != 0.0)
            ]
        else:
            daily = daily_with_zeros[np.isfinite(daily_with_zeros)]

        if not QUICK_MODE:
            print(f"\nRunning institutional audit ({label}) ...\n")
            daily_for_audit = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
            audit_res = run_institutional_audit(
                daily_returns         = daily_for_audit,
                label                 = f"{cfg_name}_{label.replace(' ','_')}",
                outdir                = outdir,
                track_file            = track_file,
                n_trials              = N_TRIALS,
                trading_days_per_year = TRADING_DAYS,
                starting_capital      = STARTING_CAPITAL,
                save_charts           = SAVE_CHARTS,
                df_4x                 = df_4x,
                param_config          = cfg_params,
                trial_purchases       = TRIAL_PURCHASES,
                filter_mode           = mode,
                filter_series         = v3,
            )

            # ── Filter-aware walk-forward ──────────────────────────────────
            col_dates_for_wf = [_col_to_timestamp(str(c)) for c in df_4x.columns]
            print(f"\n  Running filter-aware walk-forward validation ({label}) ...")
            fa_wf = run_filter_aware_wf(daily_with_zeros, label,
                                        train_days=120, test_days=30, step_days=30,
                                        oi_df=oi_df,
                                        multi_oi_df=multi_oi_df,
                                        alt_rets_df=alt_rets if not alt_rets.empty else None,
                                        col_dates=col_dates_for_wf)
            print_filter_aware_wf(fa_wf, label, col_dates=col_dates_for_wf)
            audit_res["walk_forward_rolling"] = {
                "cv":           fa_wf["cv"],
                "n_saturated":  fa_wf.get("n_saturated", 0),
                "mean_dsr":     fa_wf["mean_dsr"],
                "pct_positive": fa_wf["pct_positive"],
                "n_unstable":   fa_wf["n_unstable"],
                "folds":        fa_wf["folds"],
                # ── aggregate sub-dict for institutional_scorecard() ──
                "aggregate": {
                    "mean_sharpe":               fa_wf.get("mean_sharpe",   float("nan")),
                    "std_sharpe":                fa_wf.get("std_sharpe",    float("nan")),
                    "cv":                        fa_wf["cv"],
                    "pct_folds_positive_sharpe": fa_wf["pct_positive"],
                    "unstable_folds":            fa_wf["n_unstable"],
                    "mean_dsr":                  fa_wf["mean_dsr"],
                },
            }
            # Recompute the institutional scorecard now that walk_forward_rolling
            # has been replaced with the superior FA-WF result. The scorecard
            # baked into audit_res was computed inside run_institutional_audit()
            # using the internal rolling WF — which is now stale.
            try:
                audit_res["scorecard"] = institutional_scorecard(audit_res)
            except Exception as _sc_err:
                print(f"  ⚠  Scorecard recompute failed: {_sc_err}")
        else:
            # Quick mode — stub out audit_res and fa_wf with nan placeholders
            audit_res = {"sortino": float("nan"), "calmar": float("nan"),
                         "dsr": {}, "scorecard": {}, "regime_robustness": {}}
            fa_wf     = {"cv": float("nan"),
                         "n_saturated": 0, "mean_sharpe": float("nan"),
                         "pct_positive": float("nan"), "n_unstable": 0,
                         "mean_dsr": float("nan"), "folds": []}

        # Extract KPIs
        r          = daily[np.isfinite(daily)]
        total_days = len(daily_with_zeros)   # full calendar span incl. flat days

        # Equity curve over active days only (correct for drawdown measurement)
        eq   = np.cumprod(1 + r)
        peak = np.maximum.accumulate(eq)
        dd   = (eq - peak) / peak

        # CAGR: annualise over TOTAL elapsed days, not just active days.
        # Using len(r) here would inflate CAGR for filtered runs because fewer
        # active days makes the exponent (365/len(r)) artificially large.
        cagr      = float((eq[-1] ** (TRADING_DAYS / total_days) - 1) * 100) \
                    if total_days > 0 else float("nan")

        # Sharpe: use daily_with_zeros (zeros for flat days) so the mean and std
        # reflect the full period including cash days - otherwise filtered runs
        # are annualised over fewer days and Sharpe is overstated.
        r_full    = daily_with_zeros.copy()
        r_full    = np.where(np.isfinite(r_full), r_full, 0.0)
        sharpe    = float(np.mean(r_full) / np.std(r_full, ddof=1) * TRADING_DAYS**0.5) \
                    if len(r_full) > 1 and np.std(r_full, ddof=1) > 0 else float("nan")

        maxdd     = float(np.min(dd) * 100) if len(dd) > 0 else float("nan")

        wfr       = audit_res.get("walk_forward_rolling", {})
        cv        = float(wfr.get("cv", float("nan")))

        rr        = audit_res.get("regime_robustness", {})
        cr        = float(rr.get("cagr_ratio",  float("nan")))
        decay     = float(rr.get("decay_pct",   float("nan")))

        dsr_res   = audit_res.get("dsr", {})
        dsr_pct   = float(dsr_res.get("dsr", float("nan"))) * 100 \
                    if isinstance(dsr_res, dict) else float("nan")

        results_map[run_key] = {
            "daily":             r,
            "daily_with_zeros":  daily_with_zeros,
            "sharpe":      sharpe,
            "cagr":        cagr,
            "maxdd":       maxdd,
            "cv":          cv,
            "cagr_ratio":  cr,
            "decay_pct":   decay,
            "sortino":     float(audit_res.get("sortino", float("nan"))),
            "calmar":      float(audit_res.get("calmar",  float("nan"))),
            "dsr":         dsr_pct,
            "flat_days":   flat_days,
            "active_days": int(len(r)),   # r already has flat/nan days removed
            "scorecard":   audit_res.get("scorecard", {}),
            "audit_res":   audit_res,
            # ── Filter-aware WF ────────────────────────────────────────
            "fa_wf_cv":           fa_wf.get("cv",            float("nan")),
            "fa_wf_n_saturated":  fa_wf.get("n_saturated",   float("nan")),
            "fa_wf_n_active_folds": fa_wf.get("n_active_folds", float("nan")),
            "fa_wf_mean_sharpe":  fa_wf.get("mean_sharpe",   float("nan")),
            "fa_wf_std_sharpe":   fa_wf.get("std_sharpe",    float("nan")),
            "fa_wf_pct_positive": fa_wf.get("pct_positive",  float("nan")),
            "fa_wf_n_unstable":   fa_wf.get("n_unstable",    float("nan")),
            "fa_wf_mean_dsr":     fa_wf.get("mean_dsr",      float("nan")),
            "fa_wf":              fa_wf,   # full fold object for IC diagnostic
            # ── Min / max OOS fold Sharpe ──────────────────────────────
            "fa_wf_min_oos_sharpe": float(min(
                (f["oos_sharpe"] for f in fa_wf.get("folds", [])
                 if not f.get("saturated", False) and math.isfinite(f.get("oos_sharpe", float("nan")))),
                default=float("nan"))),
            "fa_wf_max_oos_sharpe": float(max(
                (f["oos_sharpe"] for f in fa_wf.get("folds", [])
                 if not f.get("saturated", False) and math.isfinite(f.get("oos_sharpe", float("nan")))),
                default=float("nan"))),
            # ── Simulate() cost metrics ────────────────────────────────
            "gross_return_pct":         _sim_total_gross * 100.0,
            "total_fee_drag_pct":       (_sim_total_fees / (1.0 + _sim_total_net) * 100.0)
                                        if (1.0 + _sim_total_net) > 0 else float("nan"),
            "net_return_pct":           _sim_total_net   * 100.0,
            "avg_fee_per_active_day_pct": _sim_avg_fee_active_day,   # % per active day
            "avg_fee_per_cal_day_pct":    _sim_avg_fee_cal_day,      # % per calendar day
            "avg_lev":                  _sim_avg_lev,
            # ── Leverage-tier breakdowns ───────────────────────────────
            "lev_high_days":       _sim_lev_high_days,
            "lev_base_days":       _sim_lev_base_days,
            "lev_high_winrate":    _sim_lev_high_winrate,   # %
            "lev_base_winrate":    _sim_lev_base_winrate,   # %
            "lev_high_avg_ret":    _sim_lev_high_avg_ret,   # %
            "lev_base_avg_ret":    _sim_lev_base_avg_ret,   # %
            # Populated in post-processing blocks below
            "ruin_prob_50":              float("nan"),
            "neighbor_plateau_pct":      float("nan"),
            "institutional_capacity_usd": float("nan"),
        }

        # ── Extract audit_res metrics not yet in results_map ──────────
        # These all live inside audit_res but were never routed through.
        _ar = audit_res  # shorthand
        _pb = _ar.get("period_breakdown", {})
        _pb_daily  = _pb.get("daily",  {}) if isinstance(_pb, dict) else {}
        _pb_weekly = _pb.get("weekly", {}) if isinstance(_pb, dict) else {}
        _ep_df = _ar.get("episodes_df", None)
        _mc_df = _ar.get("mc_df", None)
        _plateau = _ar.get("plateau", {})

        # Return quality
        results_map[run_key]["win_rate"]         = float(_pb_daily.get("win_rate",  float("nan"))) * 100
        results_map[run_key]["avg_win_pct"]      = float(_pb_daily.get("avg_win",   float("nan"))) * 100
        results_map[run_key]["avg_loss_pct"]     = float(_pb_daily.get("avg_loss",  float("nan"))) * 100
        try:
            _aw = float(_pb_daily.get("avg_win",  float("nan")))
            _al = float(_pb_daily.get("avg_loss", float("nan")))
            results_map[run_key]["avg_win_loss"] = abs(_aw / _al) if _al and _al != 0 else float("nan")
        except Exception:
            results_map[run_key]["avg_win_loss"] = float("nan")
        results_map[run_key]["profit_factor"]    = float(_ar.get("profit_factor", float("nan")))
        results_map[run_key]["omega"]            = float(_ar.get("omega",         float("nan")))
        results_map[run_key]["weekly_win_rate"]  = float(_pb_weekly.get("win_rate", float("nan"))) * 100

        # Risk
        results_map[run_key]["ulcer_index"]      = float(_ar.get("ulcer_index",   float("nan")))
        results_map[run_key]["avg_drawdown"]     = float(_ar.get("avg_drawdown",  float("nan"))) * 100
        results_map[run_key]["pct_time_underwater"] = float(_ar.get("pct_time_underwater", float("nan"))) * 100
        results_map[run_key]["cvar_5pct"]        = float(_ar.get("cvar5",         float("nan"))) * 100
        results_map[run_key]["cvar_1pct"]        = float(_ar.get("cvar1",         float("nan"))) * 100
        results_map[run_key]["weekly_cvar"]      = float(_ar.get("weekly_cvar",   float("nan"))) * 100
        # Episode-level drawdown stats
        try:
            if _ep_df is not None and not _ep_df.empty and "depth" in _ep_df.columns:
                _depths = _ep_df["depth"].dropna() * 100
                results_map[run_key]["avg_episode_dd"]  = float(_depths.mean()) if len(_depths) else float("nan")
                results_map[run_key]["max_dd_dur"]      = float(_ep_df["duration_days"].max()) \
                                                          if "duration_days" in _ep_df.columns else float("nan")
                _rec = _ep_df["recovery_days"].dropna()
                results_map[run_key]["dd_recovery"]     = float(_rec.max()) if len(_rec) else float("nan")
            else:
                for _k in ("avg_episode_dd", "max_dd_dur", "dd_recovery"):
                    results_map[run_key].setdefault(_k, float("nan"))
        except Exception:
            for _k in ("avg_episode_dd", "max_dd_dur", "dd_recovery"):
                results_map[run_key].setdefault(_k, float("nan"))

        # Statistical
        results_map[run_key]["skewness"]         = float(_ar.get("skewness",       float("nan")))
        results_map[run_key]["kurtosis"]         = float(_ar.get("excess_kurtosis", float("nan")))
        try:
            _lb_p = float(_ar.get("lb_pvalue", float("nan")))
            # Ljung-Box p-value → autocorr flag; also pull directly if available
            results_map[run_key]["lb_pvalue"]    = _lb_p
        except Exception:
            results_map[run_key]["lb_pvalue"]    = float("nan")

        # Compounding / vol
        try:
            _r_active = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
            _r_active = _r_active[_r_active != 0.0]
            if len(_r_active) > 1:
                _total_ret  = float(np.prod(1.0 + _r_active) - 1.0)
                #_ann_factor = TRADING_DAYS / len(_r_active)
                _ann_factor = TRADING_DAYS / total_days
                _cagr_dec   = (1.0 + _total_ret) ** _ann_factor - 1.0
                _vol_ann    = float(np.std(_r_active, ddof=1)) * TRADING_DAYS ** 0.5
                _vol_drag   = _vol_ann ** 2 / 2.0 * 100
                _eq_mult    = float(np.prod(1.0 + _r_active))
                _dbl_periods = math.log(2) / math.log(1.0 + _cagr_dec) if _cagr_dec > 0 else float("nan")
                _comp_mult  = (1.0 + _cagr_dec) / (1.0 + float(np.mean(_r_active)) * TRADING_DAYS) \
                              if (1.0 + float(np.mean(_r_active)) * TRADING_DAYS) > 0 else float("nan")
                results_map[run_key]["cagr_compounding_multiple"] = _comp_mult
                results_map[run_key]["equity_multiplier"]         = _eq_mult
                results_map[run_key]["doubling_periods"]          = _dbl_periods
                results_map[run_key]["vol_drag_ann"]              = _vol_drag
            else:
                for _k in ("cagr_compounding_multiple", "equity_multiplier",
                           "doubling_periods", "vol_drag_ann"):
                    results_map[run_key].setdefault(_k, float("nan"))
        except Exception:
            for _k in ("cagr_compounding_multiple", "equity_multiplier",
                       "doubling_periods", "vol_drag_ann"):
                results_map[run_key].setdefault(_k, float("nan"))

        # MC % losing
        try:
            if _mc_df is not None and not _mc_df.empty and "TotalMultiple" in _mc_df.columns:
                results_map[run_key]["mc_pct_losing"] = float((_mc_df["TotalMultiple"] < 1.0).mean() * 100)
            else:
                results_map[run_key]["mc_pct_losing"] = float("nan")
        except Exception:
            results_map[run_key]["mc_pct_losing"] = float("nan")

        # Turnover (from simulate output — already computed in _sim_out)
        try:
            results_map[run_key]["turnover_ann"] = float(_sim_out.get("turnover_ann", float("nan")))
        except Exception:
            results_map[run_key]["turnover_ann"] = float("nan")

        # Plateau ratio (from institutional audit — overrides the lightweight version
        # computed in audit.py if the full plateau test ran)
        try:
            if _plateau and isinstance(_plateau, dict):
                _pr = _plateau.get("plateau_ratio", float("nan"))
                if math.isfinite(float(_pr)):
                    results_map[run_key]["neighbor_plateau_pct"] = float(_pr) * 100
        except Exception:
            pass  # keep the lightweight version already stored

        # Return autocorrelation (from Ljung-Box or direct computation)
        try:
            _d_for_ac = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
            _d_for_ac = _d_for_ac[_d_for_ac != 0.0]
            if len(_d_for_ac) > 10:
                _ac = float(np.corrcoef(_d_for_ac[:-1], _d_for_ac[1:])[0, 1])
                results_map[run_key]["autocorr"] = _ac
            else:
                results_map[run_key]["autocorr"] = float("nan")
        except Exception:
            results_map[run_key]["autocorr"] = float("nan")

        # Tail ratio (95th pct gain / abs(5th pct loss))
        try:
            _d_tr = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
            _d_tr = _d_tr[_d_tr != 0.0]
            if len(_d_tr) > 10:
                _p95 = float(np.percentile(_d_tr, 95))
                _p05 = float(np.percentile(_d_tr,  5))
                results_map[run_key]["tail_ratio"] = abs(_p95 / _p05) if _p05 != 0 else float("nan")
            else:
                results_map[run_key]["tail_ratio"] = float("nan")
        except Exception:
            results_map[run_key]["tail_ratio"] = float("nan")

        # Gain-to-Pain = sum of gains / abs(sum of losses)
        try:
            _d_gp = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
            _d_gp = _d_gp[_d_gp != 0.0]
            if len(_d_gp) > 0:
                _gains  = float(np.sum(_d_gp[_d_gp > 0]))
                _losses = float(abs(np.sum(_d_gp[_d_gp < 0])))
                results_map[run_key]["gain_pain"] = _gains / _losses if _losses > 0 else float("nan")
            else:
                results_map[run_key]["gain_pain"] = float("nan")
        except Exception:
            results_map[run_key]["gain_pain"] = float("nan")
        # Fit OLS trend to log-equity curve; R² measures how straight
        # the compounding is. 1.0 = perfectly linear, 0.0 = random walk.
        try:
            _eq = np.cumprod(1.0 + np.where(np.isfinite(daily_with_zeros),
                                             daily_with_zeros, 0.0))
            _log_eq = np.log(_eq)
            _x = np.arange(len(_log_eq), dtype=float)
            _xm, _ym = _x.mean(), _log_eq.mean()
            _ss_tot = float(np.sum((_log_eq - _ym) ** 2))
            _slope  = float(np.sum((_x - _xm) * (_log_eq - _ym)) /
                            np.sum((_x - _xm) ** 2))
            _resid  = _log_eq - (_ym + _slope * (_x - _xm))
            _ss_res = float(np.sum(_resid ** 2))
            _eq_r2  = 1.0 - _ss_res / _ss_tot if _ss_tot > 0 else float("nan")
        except Exception:
            _eq_r2 = float("nan")
        results_map[run_key]["equity_r2"] = _eq_r2

        # ── Ruin probability (50% DD, bootstrap) ──────────────────────
        try:
            _ruin_r = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
            if len(_ruin_r) >= 20:
                _rng50      = np.random.default_rng(42)
                _ruin_count = 0
                for _ in range(2000):
                    _idx  = _rng50.integers(0, len(_ruin_r), size=365)
                    _eq50 = np.cumprod(1.0 + _ruin_r[_idx])
                    _pk50 = np.maximum.accumulate(_eq50)
                    if np.any(_eq50 / _pk50 - 1.0 <= -0.50):
                        _ruin_count += 1
                results_map[run_key]["ruin_prob_50"] = _ruin_count / 2000
            else:
                results_map[run_key]["ruin_prob_50"] = float("nan")
        except Exception:
            results_map[run_key]["ruin_prob_50"] = float("nan")

        # ── Neighbor plateau ratio (±0.5 Sharpe, 200 neighbors) ───────
        # Perturb all params ±15% jointly; measure fraction of neighbors
        # landing within ±0.5 Sharpe of the baseline.
        try:
            _np_rng     = np.random.default_rng(99)
            _np_params  = dict(cfg_params)
            _np_keys    = [k for k, v in _np_params.items()
                           if isinstance(v, (int, float)) and v != 0 and abs(v) < 100]
            _np_base_sh = results_map[run_key].get("sharpe", float("nan"))
            if math.isfinite(_np_base_sh) and len(_np_keys) >= 2:
                _np_within = 0
                _np_n      = 200
                for _ in range(_np_n):
                    _p = dict(_np_params)
                    for _k in _np_keys:
                        _p[_k] = _np_params[_k] * float(_np_rng.uniform(0.85, 1.15))
                    try:
                        _nd  = simulate(df_4x, _p, mode, v3, verbose=False)["daily"]
                        _nd  = np.where(np.isfinite(_nd), _nd, 0.0)
                        _std = float(np.std(_nd, ddof=1))
                        _sh  = (float(np.mean(_nd)) / _std * TRADING_DAYS**0.5
                                if _std > 1e-9 else 0.0)
                        if abs(_sh - _np_base_sh) <= 0.5:
                            _np_within += 1
                    except Exception:
                        pass
                results_map[run_key]["neighbor_plateau_pct"] = _np_within / _np_n * 100
            else:
                results_map[run_key]["neighbor_plateau_pct"] = float("nan")
        except Exception:
            results_map[run_key]["neighbor_plateau_pct"] = float("nan")

        # ── Per-return derived metrics (best/worst day, streaks, weekly/monthly) ──
        try:
            _dr = np.where(np.isfinite(daily_with_zeros), daily_with_zeros, 0.0)
            _active = _dr[_dr != 0.0]                      # exclude flat/filter days

            # Basic daily stats (active days only)
            results_map[run_key]["best_day_pct"]    = float(np.max(_active)  * 100) if len(_active) else float("nan")
            results_map[run_key]["worst_day_pct"]   = float(np.min(_active)  * 100) if len(_active) else float("nan")
            results_map[run_key]["avg_daily_pct"]   = float(np.mean(_active) * 100) if len(_active) else float("nan")
            results_map[run_key]["std_daily_pct"]   = float(np.std(_active, ddof=1) * 100) if len(_active) > 1 else float("nan")

            # Consecutive win / loss streaks (active days only)
            _win_streak = _loss_streak = _cur_w = _cur_l = 0
            for _ret in _active:
                if _ret > 0:
                    _cur_w += 1; _cur_l = 0
                elif _ret < 0:
                    _cur_l += 1; _cur_w = 0
                else:
                    _cur_w = _cur_l = 0
                _win_streak  = max(_win_streak,  _cur_w)
                _loss_streak = max(_loss_streak, _cur_l)
            results_map[run_key]["consec_win_streak"]  = _win_streak
            results_map[run_key]["consec_loss_streak"] = _loss_streak

            # Weekly / monthly aggregates — requires aligned DatetimeIndex
            _col_dates_ts = [_col_to_timestamp(str(c)) for c in df_4x.columns]
            _valid_pairs  = [(d, v) for d, v in zip(_col_dates_ts, _dr)
                             if d is not None and math.isfinite(v)]
            if _valid_pairs:
                _idx, _vals = zip(*_valid_pairs)
                _s = pd.Series(list(_vals), index=pd.DatetimeIndex(list(_idx)))

                # Weekly: sum of daily log-approx returns per ISO week
                _weekly = (1 + _s).resample("W").prod() - 1
                _w_active = _weekly[_weekly != 0.0]
                results_map[run_key]["avg_weekly_pct"]   = float(_w_active.mean() * 100) if len(_w_active) else float("nan")
                results_map[run_key]["worst_weekly_pct"] = float(_w_active.min()  * 100) if len(_w_active) else float("nan")

                # Monthly
                _monthly = (1 + _s).resample("ME").prod() - 1
                _m_active = _monthly[_monthly != 0.0]
                results_map[run_key]["avg_monthly_pct"]   = float(_m_active.mean() * 100) if len(_m_active) else float("nan")
                results_map[run_key]["worst_monthly_pct"] = float(_m_active.min()  * 100) if len(_m_active) else float("nan")
            else:
                results_map[run_key]["avg_weekly_pct"]    = float("nan")
                results_map[run_key]["worst_weekly_pct"]  = float("nan")
                results_map[run_key]["avg_monthly_pct"]   = float("nan")
                results_map[run_key]["worst_monthly_pct"] = float("nan")
        except Exception as _e:
            for _k in ("best_day_pct", "worst_day_pct", "avg_daily_pct", "std_daily_pct",
                       "consec_win_streak", "consec_loss_streak",
                       "avg_weekly_pct", "worst_weekly_pct", "avg_monthly_pct", "worst_monthly_pct"):
                results_map[run_key].setdefault(_k, float("nan"))


        # Map "V5-B Majority" / "V5-D Majority" from the run_key label.
        for _diag_label in ("V5-B Majority", "V5-D Majority"):
            if _diag_label.replace(" ", "_") in label.replace(" ", "_") or \
               _diag_label in label:
                # Build a date-indexed return series from daily_with_zeros
                # using the column dates from the matrix.
                _col_dates = [_col_to_timestamp(str(c)) for c in df_4x.columns]
                if _col_dates and len(_col_dates) == len(daily_with_zeros):
                    _dr_indexed = pd.Series(
                        daily_with_zeros,
                        index=pd.DatetimeIndex(_col_dates),
                    )
                    _hmm_diag_inject_returns(_diag_label, _dr_indexed)

        # Overwrite the audit's equity chart with a correct date-aligned version.
        # The audit generates its chart from daily_for_audit sequentially by index,
        # so filtered days appear as near-zero-return dips rather than flat lines,
        # and the x-axis is a day count not a calendar date. We replace it here.
        _overwrite_equity_chart(
            outdir        = outdir,
            label         = label,
            daily_with_zeros = daily_with_zeros,
            col_dates     = [_col_to_timestamp(str(c)) for c in df_4x.columns],
            starting_capital = STARTING_CAPITAL,
            flat_days     = flat_days,
            cagr          = cagr,
            sharpe        = sharpe,
            maxdd         = maxdd,
            perf_lev_scalar_history = _sim_perf_lev_history,
            vol_lev_scalar_history    = _sim_vol_lev_history,
            contra_lev_scalar_history = _sim_contra_lev_history,
        )

        print(f"\n  ✅ {run_key}: Sharpe={sharpe:.3f}  CAGR={cagr:.0f}%  "
              f"Eq={1 + _sim_total_net:.2f}×  "
              f"MaxDD={maxdd:.2f}%  CV={cv:.3f}  "
              f"Grade={audit_res.get('scorecard',{}).get('overall_grade','?')}")

    # ── 4. Alpha vs Beta decomposition ───────────────────────────────
    # (must run before print_comparison_table so beta/alpha/r2 populate)
    print("\n════════════════════════════════════════════")
    print(" ALPHA vs BETA DECOMPOSITION")
    print("════════════════════════════════════════════")

    try:
        btc_returns = btc_ohlcv["close"].pct_change()

        for run_key, res in results_map.items():

            strat_returns = pd.Series(res["daily_with_zeros"])

            print(f"\n--- {run_key} ---")

            # ALIGN SERIES
            strat_returns.index = [_col_to_timestamp(str(c)) for c in df_4x.columns]

            # btc_returns already computed above the loop — do not recompute here
            common_idx = strat_returns.index.intersection(btc_returns.index)

            strat_returns = strat_returns.loc[common_idx]
            btc_returns = btc_returns.loc[common_idx]

            if 'alt_rets' in locals(): alt_returns = alt_rets.loc[common_idx]
            else: alt_returns = None

            alpha_beta_res = alpha_beta_decomposition(strat_returns, btc_returns)
            if alpha_beta_res is not None:
                _beta, _alpha_ann, _r2 = alpha_beta_res
                results_map[run_key]["beta"]        = _beta
                results_map[run_key]["alpha_annual"] = _alpha_ann * 100   # store as %
                results_map[run_key]["r2"]          = _r2 * 100            # store as %

            plot_strategy_vs_btc_scatter(strat_returns,btc_returns,run_key,run_dir)

            plot_strategy_vs_btc_vol(strat_returns,btc_returns,run_key,run_dir)

            if alt_returns is not None:
                plot_strategy_vs_dispersion(strat_returns,alt_returns,run_key,run_dir)
                dispersion_decile_expectancy(strat_returns, alt_returns,
                                             label=run_key, outdir=run_dir)
                dispersion_threshold_surface(strat_returns, alt_returns,
                                             label=run_key, outdir=run_dir)
                plot_sharpe_vs_correlation(strat_returns, alt_returns,
                                           label=run_key, outdir=run_dir,
                                           btc_returns=btc_returns)
                _regime_sh = regime_attribution(strat_returns, btc_returns, alt_returns)
                if _regime_sh and run_key in results_map:
                    results_map[run_key]["regime_sharpe_high_disp"]      = _regime_sh.get("High Dispersion",   float("nan"))
                    results_map[run_key]["regime_sharpe_low_disp"]       = _regime_sh.get("Low Dispersion",    float("nan"))
                    results_map[run_key]["regime_sharpe_btc_up"]         = _regime_sh.get("BTC Uptrend",       float("nan"))
                    results_map[run_key]["regime_sharpe_btc_down"]       = _regime_sh.get("BTC Downtrend",     float("nan"))
                    results_map[run_key]["regime_sharpe_high_vol"]       = _regime_sh.get("High Volatility",   float("nan"))
                    results_map[run_key]["regime_sharpe_low_vol"]        = _regime_sh.get("Low Volatility",    float("nan"))
                    results_map[run_key]["regime_sharpe_high_disp_high_vol"] = _regime_sh.get("HighDisp + HighVol", float("nan"))
                    results_map[run_key]["regime_sharpe_low_disp_low_vol"]   = _regime_sh.get("LowDisp + LowVol",  float("nan"))
                plot_regime_heatmap(strat_returns,btc_returns,alt_returns,run_key,run_dir)
                regime_duration_analysis(strat_returns,btc_returns,alt_returns,run_key)
                plot_skew_vs_equity(strat_returns, alt_returns,
                                    label=run_key, outdir=run_dir,
                                    unstable_folds=[5, 8])
    except Exception as e:print(e)

    # ── 5. IC Diagnostic & IC Filter ─────────────────────────────────
    if not QUICK_MODE:
        col_dates_ic = [_col_to_timestamp(str(c)) for c in df_4x.columns]

        # Identify the canonical filter run (Tail+Disp+Vol preferred, else best available)
        canonical_run_key = None
        for _pref in ("Tail + Disp + Vol", "Tail + Dispersion", "Dispersion", "Tail Guardrail"):
            _candidate = f"{CANDIDATE_CONFIGS[0][0]} - {_pref}"
            if _candidate in results_map:
                canonical_run_key = _candidate
                break
        canonical_fa_wf = (results_map[canonical_run_key].get("fa_wf")
                           if canonical_run_key else None)

        if ENABLE_IC_DIAGNOSTIC and alt_rets is not None and not alt_rets.empty:
            print("\n" + "═" * 70)
            print("  STEP 6 — IC DIAGNOSTIC")
            print("  Testing whether ranking signal IC explains walk-forward instability")
            print("  (Null hypothesis: Fold 5 IC ≈ 0 → signal broke down, not regime)")
            print("═" * 70 + "\n")

            ic_result = compute_cross_sectional_ic(
                alt_rets,
                col_dates_ic,
                train_days = 120,
                test_days  = 30,
                step_days  = 30,
            )
            print_ic_diagnostic(ic_result, canonical_fa_wf)

            # ── IC Filter mode ────────────────────────────────────────────
            if ENABLE_IC_FILTER:
                print(f"  Building IC filter  "
                      f"(signal={IC_SIGNAL}, window={IC_WINDOW}d, "
                      f"threshold={IC_THRESHOLD}) ...")
                ic_filter = build_ic_filter(
                    alt_rets,
                    col_dates_ic,
                    signal    = IC_SIGNAL,
                    window    = IC_WINDOW,
                    threshold = IC_THRESHOLD,
                )

                # Test IC filter on its own and combined with canonical filter
                ic_filter_modes = [
                    ("IC Filter",            "tail_disp_vol", ic_filter),
                ]

                # If canonical triple filter exists, add IC as a 4th gate
                if canonical_run_key and canonical_run_key in results_map:
                    _canon_v3 = None
                    for _lbl, _mode, _v3 in FILTER_MODES:
                        if "Vol" in _lbl or "triple" in _lbl.lower() or "Disp + Vol" in _lbl:
                            _canon_v3 = _v3
                            break
                    if _canon_v3 is not None:
                        combined_ic = (_canon_v3 | ic_filter)
                        n_ic_only   = int(ic_filter.sum())
                        n_combined  = int(combined_ic.sum())
                        n_new       = n_combined - int(_canon_v3.sum())
                        print(f"    Canon+IC combo: IC-only={n_ic_only}d  "
                              f"IC adds {n_new} new days  total={n_combined}d flagged")
                        ic_filter_modes.append(
                            ("Canon + IC Filter", "tail_disp_vol", combined_ic)
                        )

                cfg_name, cfg_params = CANDIDATE_CONFIGS[0]
                for ic_label, ic_mode, ic_v3 in ic_filter_modes:
                    ic_run_key = f"{cfg_name} - {ic_label}"
                    print(f"\n{'═'*140}")
                    print(f"  SIMULATING (IC Filter run): {ic_run_key}")
                    print(f"{'═'*140}")

                    ic_outdir = run_dir / ic_run_key.replace(" ", "_").replace("+", "_")
                    ic_outdir.mkdir(parents=True, exist_ok=True)
                    ic_generated: List[str] = []

                    def ic_track_file(fn, _d=ic_outdir):
                        p = str((_d / fn).resolve())
                        ic_generated.append(p)
                        return p

                    ic_daily_wz = simulate(df_4x, cfg_params, ic_mode, ic_v3, symbol_counts=symbol_counts, verbose=False)["daily"]

                    ic_flat_days = int((ic_daily_wz == 0.0).sum())
                    ic_daily = ic_daily_wz[
                        np.isfinite(ic_daily_wz) & (ic_daily_wz != 0.0)
                    ]

                    ic_daily_for_audit = np.where(
                        np.isfinite(ic_daily_wz), ic_daily_wz, 0.0
                    )
                    ic_audit_res = run_institutional_audit(
                        daily_returns         = ic_daily_for_audit,
                        label                 = f"{cfg_name}_{ic_label.replace(' ','_')}",
                        outdir                = ic_outdir,
                        track_file            = ic_track_file,
                        n_trials              = N_TRIALS,
                        trading_days_per_year = TRADING_DAYS,
                        starting_capital      = STARTING_CAPITAL,
                        save_charts           = SAVE_CHARTS,
                        df_4x                 = df_4x,
                        param_config          = cfg_params,
                        trial_purchases       = TRIAL_PURCHASES,
                        filter_mode           = ic_mode,
                        filter_series         = ic_v3,
                    )

                    ic_fa_wf = run_filter_aware_wf(
                        ic_daily_wz, ic_label,
                        train_days=120, test_days=30, step_days=30,
                        oi_df=oi_df, col_dates=col_dates_ic,
                        multi_oi_df=multi_oi_df,
                        alt_rets_df=alt_rets if not alt_rets.empty else None,
                    )
                    print_filter_aware_wf(ic_fa_wf, ic_label, col_dates=col_dates_ic)

                    ic_audit_res["walk_forward_rolling"] = {
                        "cv":           ic_fa_wf["cv"],
                        "n_saturated":  ic_fa_wf.get("n_saturated", 0),
                        "mean_dsr":     ic_fa_wf["mean_dsr"],
                        "pct_positive": ic_fa_wf["pct_positive"],
                        "n_unstable":   ic_fa_wf["n_unstable"],
                        "folds":        ic_fa_wf["folds"],
                        # ── aggregate sub-dict for institutional_scorecard() ──
                        "aggregate": {
                            "mean_sharpe":               ic_fa_wf.get("mean_sharpe",  float("nan")),
                            "std_sharpe":                ic_fa_wf.get("std_sharpe",   float("nan")),
                            "cv":                        ic_fa_wf["cv"],
                            "pct_folds_positive_sharpe": ic_fa_wf["pct_positive"],
                            "unstable_folds":            ic_fa_wf["n_unstable"],
                            "mean_dsr":                  ic_fa_wf["mean_dsr"],
                        },
                    }
                    # Recompute scorecard with the FA-WF data now in place
                    try:
                        ic_audit_res["scorecard"] = institutional_scorecard(ic_audit_res)
                    except Exception as _ic_sc_err:
                        print(f"  ⚠  IC scorecard recompute failed: {_ic_sc_err}")

                    ic_r        = ic_daily[np.isfinite(ic_daily)]
                    ic_total    = len(ic_daily_wz)
                    ic_eq       = np.cumprod(1 + ic_r) if len(ic_r) > 0 else np.array([1.0])
                    ic_peak     = np.maximum.accumulate(ic_eq)
                    ic_dd       = (ic_eq - ic_peak) / ic_peak
                    ic_cagr     = float((ic_eq[-1] ** (TRADING_DAYS / ic_total) - 1) * 100) \
                                  if ic_total > 0 else float("nan")
                    ic_r_full   = np.where(np.isfinite(ic_daily_wz), ic_daily_wz, 0.0)
                    ic_sharpe   = float(
                        np.mean(ic_r_full) / np.std(ic_r_full, ddof=1) * TRADING_DAYS**0.5
                    ) if len(ic_r_full) > 1 and np.std(ic_r_full, ddof=1) > 0 else float("nan")
                    ic_maxdd    = float(np.min(ic_dd) * 100) if len(ic_dd) > 0 else float("nan")

                    ic_wfr      = ic_audit_res.get("walk_forward_rolling", {})
                    ic_cv       = float(ic_wfr.get("cv", float("nan")))
                    ic_dsr_res  = ic_audit_res.get("dsr", {})
                    ic_dsr_pct  = float(ic_dsr_res.get("dsr", float("nan"))) * 100 \
                                  if isinstance(ic_dsr_res, dict) else float("nan")

                    results_map[ic_run_key] = {
                        "daily":             ic_r,
                        "daily_with_zeros":  ic_daily_wz,
                        "sharpe":      ic_sharpe,
                        "cagr":        ic_cagr,
                        "maxdd":       ic_maxdd,
                        "cv":          ic_cv,
                        "cagr_ratio":  float(ic_audit_res.get("regime_robustness", {})
                                             .get("cagr_ratio", float("nan"))),
                        "decay_pct":   float(ic_audit_res.get("regime_robustness", {})
                                             .get("decay_pct", float("nan"))),
                        "sortino":     float(ic_audit_res.get("sortino", float("nan"))),
                        "calmar":      float(ic_audit_res.get("calmar",  float("nan"))),
                        "dsr":         ic_dsr_pct,
                        "flat_days":   ic_flat_days,
                        "active_days": int(len(ic_r)),
                        "scorecard":   ic_audit_res.get("scorecard", {}),
                        "audit_res":   ic_audit_res,
                        "fa_wf_cv":          ic_fa_wf.get("cv",          float("nan")),
                        "fa_wf_n_saturated": ic_fa_wf.get("n_saturated", float("nan")),
                        "fa_wf_mean_sharpe": ic_fa_wf.get("mean_sharpe", float("nan")),
                        "fa_wf_pct_positive":ic_fa_wf.get("pct_positive",float("nan")),
                        "fa_wf_n_unstable":  ic_fa_wf.get("n_unstable",  float("nan")),
                        "fa_wf_mean_dsr":    ic_fa_wf.get("mean_dsr",    float("nan")),
                        "fa_wf":             ic_fa_wf,
                    }

        else:
            ic_result = {}

    if not QUICK_MODE:
        # ── 6. PBO — Probability of Backtest Overfitting (CSCV) ─────────
        print("\n" + "═" * 70)
        print("  STEP 6 — PBO  (Probability of Backtest Overfitting)")
        print("  CSCV  ·  Bailey, Borwein, Lopez de Prado & Zhu (2015)")
        print("═" * 70 + "\n")

        pbo_labels  = list(results_map.keys())
        pbo_series  = [
            np.where(
                np.isfinite(results_map[k]["daily_with_zeros"]),
                results_map[k]["daily_with_zeros"],
                0.0,
            )
            for k in pbo_labels
        ]
        pbo_min_len = min(len(s) for s in pbo_series)
        if any(len(s) != pbo_min_len for s in pbo_series):
            print(f"  ⚠ PBO: series length mismatch — truncating all to {pbo_min_len} days.")
            pbo_series = [s[:pbo_min_len] for s in pbo_series]

        if len(pbo_labels) >= 2 and pbo_min_len >= 20:
            pbo_matrix = np.column_stack(pbo_series)
            pbo_result = compute_pbo(
                returns_matrix  = pbo_matrix,
                strategy_labels = pbo_labels,
                S               = 16,
                trading_days    = TRADING_DAYS,
            )
            print_pbo_report(pbo_result, label="All filter modes")
            pbo_chart_path = plot_pbo_logit_dist(pbo_result, outdir=run_dir,
                                                  label="All_filter_modes")
            if pbo_chart_path:
                print(f"  PBO chart saved: {pbo_chart_path}")
            for k in pbo_labels:
                results_map[k]["pbo"]             = pbo_result["pbo"]
                results_map[k]["performance_deg"] = pbo_result.get("performance_deg", float("nan"))
                results_map[k]["prob_loss"]       = pbo_result.get("prob_loss",       float("nan"))
        else:
            reason = (f"N={len(pbo_labels)} strategies" if len(pbo_labels) < 2
                      else f"T={pbo_min_len} days (< 20)")
            print(f"  ⚠ PBO skipped — insufficient data ({reason}).")
            for k in pbo_labels:
                results_map[k]["pbo"]             = float("nan")
                results_map[k]["performance_deg"] = float("nan")
                results_map[k]["prob_loss"]       = float("nan")
    else:
        for k in results_map:
            results_map[k]["pbo"]             = float("nan")
            results_map[k]["performance_deg"] = float("nan")
            results_map[k]["prob_loss"]       = float("nan")

    # ── 7. Print comparison table (always — even in quick mode) ──────
    print_comparison_table(results_map)

    if not QUICK_MODE:
        # ── 8. Comparison chart ───────────────────────────────────────
        col_dates = [_col_to_timestamp(str(col)) for col in df_4x.columns]
        col_dates = col_dates if all(d is not None for d in col_dates) else None
        print("Rendering comparison chart ...")
        _comparison_chart_path = plot_comparison(results_map, run_dir, col_dates=col_dates)
        print("Rendering monthly cumulative returns chart ...")
        plot_monthly_cumulative_returns(results_map, run_dir, col_dates=col_dates)
    else:
        _comparison_chart_path = None

    # ── 8. Parameter surface sweeps ───────────────────────────────────
    sweep_dir     = run_dir / "parameter_sweeps"   # path defined unconditionally;
                                                   # mkdir() is called only when needed
    _combo_filter = None   # resolved inside if not QUICK_MODE; initialised here
                           # so the elif ridge/plateau branch can reference it safely
    base_params   = CANDIDATE_CONFIGS[0][1]   # default; overridden inside if not QUICK_MODE
    if not QUICK_MODE:
        _combo_filter = None
        for label, mode, v3 in FILTER_MODES:
            if mode == "tail_disp":
                _combo_filter = v3
                break
        if _combo_filter is None:
            for label, mode, v3 in FILTER_MODES:
                if mode == "dispersion":
                    _combo_filter = v3
                    break

        sweep_dir   = run_dir / "parameter_sweeps"
        base_params = CANDIDATE_CONFIGS[0][1]

        if ENABLE_SWEEP_TAIL_GUARDRAIL and not btc_ohlcv.empty:
            sweep_dir.mkdir(parents=True, exist_ok=True)
            run_tail_guardrail_sweep(df_4x, base_params, btc_ohlcv, sweep_dir)

        # Resolve best available filter for L_HIGH / Trail sweeps:
        # prefer tail_disp combo, fall back to tail-only, then dispersion.
        _sweep_filter = _combo_filter
        if _sweep_filter is None:
            for label, mode, v3 in FILTER_MODES:
                if mode == "tail" and v3 is not None:
                    _sweep_filter = v3
                    print(f"  ℹ  Using Tail filter for parameter sweeps (no combo available)")
                    break
        if _sweep_filter is None:
            print("  ⚠  No filter available for L_HIGH/Trail sweeps — skipping.")

        if _sweep_filter is not None:
            sweep_dir.mkdir(parents=True, exist_ok=True)

            if ENABLE_SWEEP_L_HIGH:
                run_l_high_sweep(df_4x, base_params, _sweep_filter, sweep_dir)

            if ENABLE_SWEEP_TRAIL_WIDE:
                _early_wide = [35]
                run_trail_early_sweep(df_4x, base_params, _sweep_filter, sweep_dir,
                                      early_x_values=_early_wide, label="wide")

            if ENABLE_SWEEP_TRAIL_NARROW:
                _early_narrow = list(range(30, 45))
                run_trail_early_sweep(df_4x, base_params, _sweep_filter, sweep_dir,
                                      early_x_values=_early_narrow, label="narrow")

        # ── PPH GRID SEARCH ───────────────────────────────────────────
        if PPH_SWEEP_ENABLED:
            print("\n" + "═"*70)
            print("  PPH GRID SEARCH  —  Periodic Profit Harvest  (48 configs)")
            print("═"*70)
            _pph_filter_label, _pph_filter_mode, _pph_filter_v3 = None, None, None
            # Use the best filter from the main audit (prefer tail guardrail)
            for _lbl, _mode, _v3 in FILTER_MODES:
                if "tail" in _mode.lower() or "guardrail" in _lbl.lower():
                    _pph_filter_label = _lbl
                    _pph_filter_mode  = _mode
                    _pph_filter_v3    = _v3
                    break
            if _pph_filter_label is None and FILTER_MODES:
                _pph_filter_label, _pph_filter_mode, _pph_filter_v3 = FILTER_MODES[0]
            if _pph_filter_label is None:
                print("  ⚠  No filter available for PPH sweep — skipping.")
            else:
                run_pph_sweep(
                    df_4x          = df_4x,
                    params         = base_params,
                    filter_mode    = _pph_filter_mode,
                    v3_filter      = _pph_filter_v3,
                    symbol_counts  = symbol_counts,
                    vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
                    filter_label   = _pph_filter_label,
                    out_dir        = sweep_dir,
                )

        if RATCHET_SWEEP_ENABLED:
            print("\n" + "═"*70)
            print("  EQUITY RATCHET GRID SEARCH  (27 configs)")
            print("═"*70)
            _rat_filter_label, _rat_filter_mode, _rat_filter_v3 = None, None, None
            for _lbl, _mode, _v3 in FILTER_MODES:
                if "tail" in _mode.lower() or "guardrail" in _lbl.lower():
                    _rat_filter_label = _lbl
                    _rat_filter_mode  = _mode
                    _rat_filter_v3    = _v3
                    break
            if _rat_filter_label is None and FILTER_MODES:
                _rat_filter_label, _rat_filter_mode, _rat_filter_v3 = FILTER_MODES[0]
            if _rat_filter_label is None:
                print("  ⚠  No filter available for ratchet sweep — skipping.")
            else:
                run_ratchet_sweep(
                    df_4x          = df_4x,
                    params         = base_params,
                    filter_mode    = _rat_filter_mode,
                    v3_filter      = _rat_filter_v3,
                    symbol_counts  = symbol_counts,
                    vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
                    filter_label   = _rat_filter_label,
                    out_dir        = sweep_dir,
                )

        if ADAPTIVE_RATCHET_SWEEP_ENABLED:
            print("\n" + "═"*70)
            print("  REGIME-ADAPTIVE RATCHET GRID SEARCH  (54 configs)")
            print("═"*70)
            _arat_filter_label, _arat_filter_mode, _arat_filter_v3 = None, None, None
            for _lbl, _mode, _v3 in FILTER_MODES:
                if "tail" in _mode.lower() or "guardrail" in _lbl.lower():
                    _arat_filter_label = _lbl
                    _arat_filter_mode  = _mode
                    _arat_filter_v3    = _v3
                    break
            if _arat_filter_label is None and FILTER_MODES:
                _arat_filter_label, _arat_filter_mode, _arat_filter_v3 = FILTER_MODES[0]
            if _arat_filter_label is None:
                print("  ⚠  No filter available for adaptive ratchet sweep — skipping.")
            else:
                run_adaptive_ratchet_sweep(
                    df_4x          = df_4x,
                    params         = base_params,
                    filter_mode    = _arat_filter_mode,
                    v3_filter      = _arat_filter_v3,
                    symbol_counts  = symbol_counts,
                    vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
                    filter_label   = _arat_filter_label,
                    out_dir        = sweep_dir,
                )

    # ── 2-PARAMETER SURFACE MAPS ────────────────────────────────────
    if ENABLE_PARAM_SURFACES:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        # Resolve filter: prefer tail_disp combo -> tail-only -> first available
        _surf_filter      = _combo_filter
        _surf_filter_mode = "tail_disp"
        _surf_filter_lbl  = "Tail + Dispersion"
        if _surf_filter is None:
            for _lbl, _mode, _v3 in FILTER_MODES:
                if "tail" in _mode.lower():
                    _surf_filter      = _v3
                    _surf_filter_mode = _mode
                    _surf_filter_lbl  = _lbl
                    break
        if _surf_filter is None and FILTER_MODES:
            _surf_filter_lbl, _surf_filter_mode, _surf_filter = FILTER_MODES[0]
        _surf_filter_mode = _surf_filter_mode or "tail"
        _surf_filter_lbl  = _surf_filter_lbl  or "Best Available"

        _plateau_summary_all = []   # accumulates across all surfaces

        for _pair in PARAM_SURFACE_PAIRS:
            run_param_surface(
                df_4x          = df_4x,
                base_params    = base_params,
                filter_mode    = _surf_filter_mode,
                v3_filter      = _surf_filter,
                symbol_counts  = symbol_counts,
                vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
                param_x        = _pair["param_x"],
                param_y        = _pair["param_y"],
                values_x       = _pair["values_x"],
                values_y       = _pair["values_y"],
                out_dir        = sweep_dir,
                filter_label   = _surf_filter_lbl,
                surface_label  = _pair["surface_label"],
                _plateau_summary_out = _plateau_summary_all,
            )

        # Write aggregate plateau summary across all surfaces
        if ENABLE_SHARPE_PLATEAU and _plateau_summary_all:
            _psumm_path = sweep_dir / "sharpe_plateau_summary.csv"
            pd.DataFrame(_plateau_summary_all).to_csv(_psumm_path, index=False)
            print(f"  [plateau summary] saved → {_psumm_path}")

    # ── RIDGE MAP + PLATEAU FROM EXISTING CSVs (when surfaces skipped) ─
    elif ENABLE_SHARPE_RIDGE_MAP or ENABLE_SHARPE_PLATEAU:
        _csv_pairs = []
        for _pair in PARAM_SURFACE_PAIRS:
            _csv = sweep_dir / f"param_surface_{_pair['surface_label']}.csv"
            if _csv.exists():
                _csv_pairs.append((_pair, _csv))

        # Auto-generate any missing CSVs rather than aborting
        _missing_pairs = [p for p in PARAM_SURFACE_PAIRS
                          if not (sweep_dir / f"param_surface_{p['surface_label']}.csv").exists()]
        if _missing_pairs:
            print(f"  [ridge/plateau] {len(_missing_pairs)} surface CSV(s) missing — "
                  f"generating now (ENABLE_PARAM_SURFACES=False)...")
            sweep_dir.mkdir(parents=True, exist_ok=True)
            # Resolve filter (same logic as ENABLE_PARAM_SURFACES block)
            _rp_filter      = _combo_filter
            _rp_filter_mode = "tail_disp"
            _rp_filter_lbl  = "Tail + Dispersion"
            if _rp_filter is None:
                for _lbl, _mode, _v3 in FILTER_MODES:
                    if "tail" in _mode.lower():
                        _rp_filter      = _v3
                        _rp_filter_mode = _mode
                        _rp_filter_lbl  = _lbl
                        break
            if _rp_filter is None and FILTER_MODES:
                _rp_filter_lbl, _rp_filter_mode, _rp_filter = FILTER_MODES[0]
            _rp_filter_mode = _rp_filter_mode or "tail"
            _rp_filter_lbl  = _rp_filter_lbl  or "Best Available"
            for _pair in _missing_pairs:
                try:
                    run_param_surface(
                        df_4x          = df_4x,
                        base_params    = base_params,
                        filter_mode    = _rp_filter_mode,
                        v3_filter      = _rp_filter,
                        symbol_counts  = symbol_counts,
                        vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
                        param_x        = _pair["param_x"],
                        param_y        = _pair["param_y"],
                        values_x       = _pair["values_x"],
                        values_y       = _pair["values_y"],
                        out_dir        = sweep_dir,
                        filter_label   = _rp_filter_lbl,
                        surface_label  = _pair["surface_label"],
                        # Ridge/plateau already run inline — skip to avoid double output
                        _plateau_summary_out = None,
                    )
                    _csv = sweep_dir / f"param_surface_{_pair['surface_label']}.csv"
                    if _csv.exists():
                        _csv_pairs.append((_pair, _csv))
                except Exception as _e:
                    print(f"  Warning: surface generation failed for "
                          f"{_pair['surface_label']}: {_e}")

        if not _csv_pairs:
            print("  ⚠  Ridge/plateau: no surface data available — skipping.")
        else:
            print(f"  [ridge/plateau] reading {len(_csv_pairs)} existing surface CSVs "
                  f"(ENABLE_PARAM_SURFACES=False)")
            _plateau_summary_all = []
            for _pair, _csv in _csv_pairs:
                _lbl = _pair["surface_label"]
                _px  = _pair["param_x"]
                _py  = _pair["param_y"]
                # Reconstruct rows list from CSV — same schema run_param_surface writes
                _df_csv = pd.read_csv(_csv)
                _rows_csv = _df_csv.to_dict("records")
                # Recover baseline x/y from the row flagged baseline=True
                _bl_rows = _df_csv[_df_csv.get("baseline", pd.Series(dtype=bool)) == True]                            if "baseline" in _df_csv.columns else pd.DataFrame()
                _bl_x = _bl_rows.iloc[0][_px] if not _bl_rows.empty else None
                _bl_y = _bl_rows.iloc[0][_py] if not _bl_rows.empty else None
                if ENABLE_SHARPE_RIDGE_MAP:
                    try:
                        plot_sharpe_ridge_map(
                            rows=_rows_csv, param_x=_px, param_y=_py,
                            values_x=_pair["values_x"], values_y=_pair["values_y"],
                            out_dir=sweep_dir, filter_label="(from CSV)",
                            surface_label=_lbl,
                            baseline_x=_bl_x, baseline_y=_bl_y,
                        )
                    except Exception as _e:
                        print(f"  Warning: ridge map failed for {_lbl}: {_e}")
                if ENABLE_SHARPE_PLATEAU:
                    try:
                        _res = plot_sharpe_plateau_detector(
                            rows=_rows_csv, param_x=_px, param_y=_py,
                            values_x=_pair["values_x"], values_y=_pair["values_y"],
                            out_dir=sweep_dir, filter_label="(from CSV)",
                            surface_label=_lbl,
                            baseline_x=_bl_x, baseline_y=_bl_y,
                            plateau_pct_list=PLATEAU_PCT_OF_MAX_LIST,
                            min_cluster_cells=PLATEAU_MIN_CLUSTER_CELLS,
                        )
                        for _row in _res.get("summary", []):
                            _plateau_summary_all.append(
                                {"surface": _lbl, "param_x": _px, "param_y": _py, **_row})
                    except Exception as _e:
                        print(f"  Warning: plateau detector failed for {_lbl}: {_e}")
            if ENABLE_SHARPE_PLATEAU and _plateau_summary_all:
                _psumm_path = sweep_dir / "sharpe_plateau_summary.csv"
                pd.DataFrame(_plateau_summary_all).to_csv(_psumm_path, index=False)
                print(f"  [plateau summary] saved → {_psumm_path}")

    # ── EQUITY CURVE ENSEMBLE PLOT ────────────────────────────────────
    if ENABLE_EQUITY_ENSEMBLE:
        # daily_with_zeros and col_dates are in scope from the FILTER_MODES loop.
        # We use the last-run filter (prefer tail_disp combo).
        _ens_dwz   = daily_with_zeros if "daily_with_zeros" in dir() else None
        _ens_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
        _ens_dates = [_col_to_timestamp(str(c)) for c in df_4x.columns] if "df_4x" in dir() else None
        if _ens_dwz is not None and len(_ens_dwz) > 2:
            plot_equity_ensemble(
                daily_with_zeros = np.array(_ens_dwz, dtype=float),
                col_dates        = _ens_dates,
                run_dir          = run_dir,
                filter_label     = _ens_label,
                starting_capital = STARTING_CAPITAL,
                n_trials         = EQUITY_ENSEMBLE_N_TRIALS,
                label            = _ens_label.lower().replace(" ", "_"),
            )
        else:
            print("  ⚠  Equity ensemble: no daily_with_zeros in scope — skipping.")

    # ── SLIPPAGE IMPACT SWEEP ──────────────────────────────────────────
    if ENABLE_SLIPPAGE_SWEEP:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        # daily_with_zeros is in scope from the FILTER_MODES loop above.
        # It holds the last-run filter results; prefer tail_disp combo.
        _slip_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
        _slip_dwz   = daily_with_zeros if "daily_with_zeros" in dir() else None

        if _slip_dwz is not None and len(_slip_dwz) > 2:
            _slip_arr = np.array(_slip_dwz, dtype=float)
            _act_mask = np.isfinite(_slip_arr) & (_slip_arr != 0.0)
            run_slippage_sweep(
                daily_active    = _slip_arr[_act_mask],
                daily_all       = _slip_arr,
                active_mask     = _act_mask,
                filter_label    = _slip_label,
                out_dir         = sweep_dir,
                slippage_levels = SLIPPAGE_LEVELS,
                taker_fee_pct   = TAKER_FEE_PCT,
            )
        else:
            print("  ⚠  Slippage sweep: no daily_with_zeros in scope — skipping.")

    # ── NOISE PERTURBATION STABILITY TEST ─────────────────────────────
    if ENABLE_NOISE_STABILITY:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        # Resolve best available filter (prefer tail_disp combo → tail-only → none)
        _noise_filter       = _combo_filter
        _noise_filter_mode  = "tail_disp"
        _noise_filter_label = "Tail + Dispersion"
        if _noise_filter is None:
            for _lbl, _mode, _v3 in FILTER_MODES:
                if "tail" in _mode.lower():
                    _noise_filter       = _v3
                    _noise_filter_mode  = _mode
                    _noise_filter_label = _lbl
                    break
        if _noise_filter is None and FILTER_MODES:
            _noise_filter_label, _noise_filter_mode, _noise_filter = FILTER_MODES[0]
        _noise_filter_mode  = _noise_filter_mode or "tail"
        _noise_filter_label = _noise_filter_label or "Best Available"
        run_noise_stability_test(
            df_4x          = df_4x,
            params         = base_params,
            filter_mode    = _noise_filter_mode,
            v3_filter      = _noise_filter,
            symbol_counts  = symbol_counts,
            vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
            filter_label   = _noise_filter_label,
            out_dir        = sweep_dir,
            n_trials       = NOISE_N_TRIALS,
            return_levels  = NOISE_RETURN_LEVELS,
            shuffle_levels = NOISE_SHUFFLE_LEVELS,
        )

    # ── PARAM JITTER / SHARPE STABILITY TEST ──────────────────────
    if ENABLE_PARAM_JITTER:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _jitter_filter       = _combo_filter
        _jitter_filter_mode  = "tail_disp"
        _jitter_filter_label = "Tail Guardrail"
        if _jitter_filter is None:
            for _lbl, _mode, _v3 in FILTER_MODES:
                if "tail" in _mode.lower():
                    _jitter_filter       = _v3
                    _jitter_filter_mode  = _mode
                    _jitter_filter_label = _lbl
                    break
        if _jitter_filter is None and FILTER_MODES:
            _jitter_filter_label, _jitter_filter_mode, _jitter_filter = FILTER_MODES[0]
        _jitter_filter_mode  = _jitter_filter_mode  or "tail"
        _jitter_filter_label = _jitter_filter_label or "Best Available"
        run_param_jitter(
            df_4x          = df_4x,
            base_params    = base_params,
            filter_mode    = _jitter_filter_mode,
            v3_filter      = _jitter_filter,
            symbol_counts  = symbol_counts,
            vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
            jitter_spec    = PARAM_JITTER_SPEC,
            n_trials       = PARAM_JITTER_N_TRIALS,
            out_dir        = sweep_dir,
            filter_label   = _jitter_filter_label,
        )

    # ── RETURN CONCENTRATION / TOP-N CONTRIBUTION CURVE ───────────────
    if ENABLE_RETURN_CONCENTRATION:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        # Use daily_with_zeros from the best available filter run
        _conc_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _conc_daily is not None and len(_conc_daily) > 10:
            _conc_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_return_concentration(
                daily_returns = np.array(_conc_daily, dtype=float),
                out_dir       = sweep_dir,
                filter_label  = _conc_label,
                top_ns        = CONCENTRATION_TOP_NS,
            )
        else:
            print("  ⚠  Return concentration: no daily_with_zeros in scope — skipping.")


    # ── TOP-N DAY REMOVAL TEST ─────────────────────────────────────────
    if ENABLE_TOP_N_REMOVAL:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _topn_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _topn_daily is not None and len(_topn_daily) > max(TOP_N_REMOVAL_NS) + 5:
            _topn_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_top_n_removal(
                daily_with_zeros = np.array(_topn_daily, dtype=float),
                filter_label     = _topn_label,
                out_dir          = sweep_dir,
                ns               = TOP_N_REMOVAL_NS,
            )
        else:
            print("  ⚠  Top-N removal: no daily_with_zeros in scope — skipping.")

    # ── LUCKY STREAK TEST ──────────────────────────────────────────────
    if ENABLE_LUCKY_STREAK:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _luck_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _luck_daily is not None and len(_luck_daily) > LUCKY_STREAK_WINDOW * 2:
            _luck_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_lucky_streak(
                daily_with_zeros = np.array(_luck_daily, dtype=float),
                filter_label     = _luck_label,
                out_dir          = sweep_dir,
                window           = LUCKY_STREAK_WINDOW,
            )
        else:
            print("  ⚠  Lucky streak: no daily_with_zeros in scope — skipping.")

    # ── PERIODIC RETURN BREAKDOWN ──────────────────────────────────────
    if ENABLE_PERIODIC_BREAKDOWN:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _per_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _per_daily is not None and len(_per_daily) > 14:
            _per_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_periodic_return_breakdown(
                daily_with_zeros = np.array(_per_daily, dtype=float),
                filter_label     = _per_label,
                out_dir          = sweep_dir,
            )
        else:
            print("  ⚠  Periodic breakdown: no daily_with_zeros in scope — skipping.")


    # ── MINIMUM CUMULATIVE RETURN TABLE ───────────────────────────────
    if ENABLE_MIN_CUM_RETURN:
        _mcr_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _mcr_daily is not None and len(_mcr_daily) > 0:
            _mcr_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_min_cumulative_return(
                daily_with_zeros = np.array(_mcr_daily, dtype=float),
                windows          = MIN_CUM_RETURN_WINDOWS,
                filter_label     = _mcr_label,
                out_dir          = sweep_dir,
            )
        else:
            print("  ⚠  Min cumulative return: no daily_with_zeros in scope — skipping.")


    # ── DEFLATED SHARPE RATIO + MINIMUM TRACK RECORD LENGTH ───────────
    if ENABLE_DSR_MTL:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _dsr_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _dsr_daily is not None and len(_dsr_daily) > 10:
            _dsr_sh    = _sweep_sharpe(np.array(_dsr_daily, dtype=float))
            _dsr_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_dsr_mtl(
                daily_returns   = np.array(_dsr_daily, dtype=float),
                observed_sharpe = _dsr_sh,
                n_trials        = N_TRIALS,
                filter_label    = _dsr_label,
                out_dir         = sweep_dir,
                target_sharpe   = DSR_TARGET_SHARPE,
            )
        else:
            print("  ⚠  DSR/MTL: no daily_with_zeros in scope — skipping.")

    # ── SHOCK INJECTION TEST ───────────────────────────────────────────
    if ENABLE_SHOCK_INJECTION:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _shock_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _shock_daily is not None and len(_shock_daily) > 20:
            _shock_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_shock_injection(
                daily_with_zeros = np.array(_shock_daily, dtype=float),
                filter_label     = _shock_label,
                out_dir          = sweep_dir,
                shock_sizes      = SHOCK_SIZES,
                n_shocks_list    = SHOCK_N_DAYS,
            )
        else:
            print("  ⚠  Shock injection: no daily_with_zeros in scope — skipping.")

    # ── RUIN PROBABILITY ───────────────────────────────────────────────
    if ENABLE_RUIN_PROBABILITY:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _ruin_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _ruin_daily is not None and len(_ruin_daily) > 20:
            _ruin_label = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            run_ruin_probability(
                daily_with_zeros = np.array(_ruin_daily, dtype=float),
                filter_label     = _ruin_label,
                out_dir          = sweep_dir,
                ruin_thresholds  = RUIN_THRESHOLDS,
                n_sims           = RUIN_N_SIMS,
            )
        else:
            print("  ⚠  Ruin probability: no daily_with_zeros in scope — skipping.")


    # ── PARAMETRIC STABILITY CUBE ──────────────────────────────────────
    if ENABLE_STABILITY_CUBE:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        # Prefer tail filter; fall back to best available
        _cube_filter      = tail_filter
        _cube_filter_mode = "tail"
        _cube_filter_lbl  = "Tail Guardrail"
        if _cube_filter is None and FILTER_MODES:
            _cube_filter_lbl, _cube_filter_mode, _cube_filter = FILTER_MODES[0]
        _cube_filter_mode = _cube_filter_mode or "none"
        _cube_filter_lbl  = _cube_filter_lbl  or "No Filter"
        _cube_vlev = (_vlev_params if ENABLE_VOL_LEV_SCALING else {
            "target_vol": VOL_LEV_TARGET_VOL,
            "window": VOL_LEV_WINDOW,
            "sharpe_ref": VOL_LEV_SHARPE_REF,
            "dd_threshold": VOL_LEV_DD_THRESHOLD,
            "dd_scale": VOL_LEV_DD_SCALE,
            "max_boost": VOL_LEV_MAX_BOOST,
        })
        if not ENABLE_VOL_LEV_SCALING:
            print("  [stability-cube] Vol-lev scaling is OFF; using baseline vol-lev parameters for cube sweeps.")
        run_parametric_stability_cube(
            df_4x          = df_4x,
            base_params    = base_params,
            filter_mode    = _cube_filter_mode,
            v3_filter      = _cube_filter,
            symbol_counts  = symbol_counts,
            vol_lev_params = _cube_vlev,
            out_dir        = sweep_dir / "stability_cube_leverage",
            filter_label   = _cube_filter_lbl,
            values_lbase   = CUBE_VALUES_LBASE,
            values_lhigh   = CUBE_VALUES_LHIGH,
            values_boost   = CUBE_VALUES_BOOST,
        )

    # ── RISK THROTTLE STABILITY CUBE ──────────────────────────────────
    if ENABLE_RISK_THROTTLE_CUBE:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _cube_filter      = tail_filter
        _cube_filter_mode = "tail"
        _cube_filter_lbl  = "Tail Guardrail"
        if _cube_filter is None and FILTER_MODES:
            _cube_filter_lbl, _cube_filter_mode, _cube_filter = FILTER_MODES[0]
        _cube_filter_mode = _cube_filter_mode or "none"
        _cube_filter_lbl  = _cube_filter_lbl  or "No Filter"
        _cube_vlev = (_vlev_params if ENABLE_VOL_LEV_SCALING else {
            "target_vol": VOL_LEV_TARGET_VOL,
            "window": VOL_LEV_WINDOW,
            "sharpe_ref": VOL_LEV_SHARPE_REF,
            "dd_threshold": VOL_LEV_DD_THRESHOLD,
            "dd_scale": VOL_LEV_DD_SCALE,
            "max_boost": VOL_LEV_MAX_BOOST,
        })
        if not ENABLE_VOL_LEV_SCALING:
            print("  [risk-throttle-cube] Vol-lev scaling is OFF; using baseline vol-lev parameters for cube sweeps.")
        run_risk_throttle_stability_cube(
            df_4x           = df_4x,
            base_params     = base_params,
            filter_mode     = _cube_filter_mode,
            v3_filter       = _cube_filter,
            symbol_counts   = symbol_counts,
            vol_lev_params  = _cube_vlev,
            out_dir         = sweep_dir / "stability_cube_risk_throttle",
            filter_label    = _cube_filter_lbl,
            values_fill_y   = CUBE_VALUES_FILL_Y,
            values_kill_y   = CUBE_RT_VALUES_KILL_Y,
            values_boost    = CUBE_RT_VALUES_BOOST,
        )

    # ── EXIT ARCHITECTURE STABILITY CUBE ──────────────────────────────
    if ENABLE_EXIT_CUBE:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _cube_filter      = tail_filter
        _cube_filter_mode = "tail"
        _cube_filter_lbl  = "Tail Guardrail"
        if _cube_filter is None and FILTER_MODES:
            _cube_filter_lbl, _cube_filter_mode, _cube_filter = FILTER_MODES[0]
        _cube_filter_mode = _cube_filter_mode or "none"
        _cube_filter_lbl  = _cube_filter_lbl  or "No Filter"
        _cube_vlev = (_vlev_params if ENABLE_VOL_LEV_SCALING else {
            "target_vol": VOL_LEV_TARGET_VOL,
            "window": VOL_LEV_WINDOW,
            "sharpe_ref": VOL_LEV_SHARPE_REF,
            "dd_threshold": VOL_LEV_DD_THRESHOLD,
            "dd_scale": VOL_LEV_DD_SCALE,
            "max_boost": VOL_LEV_MAX_BOOST,
        })
        if not ENABLE_VOL_LEV_SCALING:
            print("  [exit-cube] Vol-lev scaling is OFF; using baseline vol-lev parameters for cube sweeps.")
        run_exit_stability_cube(
            df_4x            = df_4x,
            base_params      = base_params,
            filter_mode      = _cube_filter_mode,
            v3_filter        = _cube_filter,
            symbol_counts    = symbol_counts,
            vol_lev_params   = _cube_vlev,
            out_dir          = sweep_dir / "stability_cube_exit_architecture",
            filter_label     = _cube_filter_lbl,
            values_port_sl   = CUBE_VALUES_PORT_SL,
            values_port_tsl  = CUBE_VALUES_PORT_TSL,
            values_kill_y    = CUBE_EX_VALUES_KILL_Y,
        )

    # ── LIQUIDITY CAPACITY CURVE ───────────────────────────────────────
    if ENABLE_CAPACITY_CURVE:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        _cap_daily = daily_with_zeros if "daily_with_zeros" in dir() else None
        if _cap_daily is not None and len(_cap_daily) > 10:
            _cap_avg_lev = _sim_avg_lev if "daily_with_zeros" in dir() else float("nan")
            _cap_avg_sym = (float(np.mean(list(symbol_counts.values())))
                            if symbol_counts is not None and len(symbol_counts) > 0
                            else 7.0)
            _cap_label   = FILTER_MODES[-1][0] if FILTER_MODES else "Best Available"
            _cap_result  = run_liquidity_capacity_curve(
                daily_with_zeros = np.array(_cap_daily, dtype=float),
                avg_lev          = _cap_avg_lev if np.isfinite(_cap_avg_lev) else 1.1,
                avg_symbols      = _cap_avg_sym,
                filter_label     = _cap_label,
                out_dir          = sweep_dir,
                adv_per_symbol   = CAPACITY_ADV_PER_SYMBOL,
                impact_alpha     = CAPACITY_IMPACT_ALPHA,
            )
            # Store institutional capacity in the matching results_map entry
            _cap_aum = (_cap_result.get("institutional_capacity_usd", float("nan"))
                        if isinstance(_cap_result, dict) else float("nan"))
            for _rk in results_map:
                if _cap_label in _rk or _rk.endswith(_cap_label):
                    results_map[_rk]["institutional_capacity_usd"] = _cap_aum
                    break
        else:
            print("  ⚠  Capacity curve: no daily_with_zeros in scope — skipping.")

    # ── REGIME ROBUSTNESS TEST ─────────────────────────────────────────
    if ENABLE_REGIME_ROBUSTNESS:
        sweep_dir.mkdir(parents=True, exist_ok=True)
        if not btc_ohlcv.empty:
            _reg_filter      = tail_filter
            _reg_filter_mode = "tail"
            _reg_filter_lbl  = "Tail Guardrail"
            if _reg_filter is None and FILTER_MODES:
                _reg_filter_lbl, _reg_filter_mode, _reg_filter = FILTER_MODES[0]
            _reg_filter_mode = _reg_filter_mode or "none"
            _reg_filter_lbl  = _reg_filter_lbl  or "No Filter"
            run_regime_robustness(
                df_4x          = df_4x,
                base_params    = base_params,
                filter_mode    = _reg_filter_mode,
                v3_filter      = _reg_filter,
                symbol_counts  = symbol_counts,
                vol_lev_params = (_vlev_params if ENABLE_VOL_LEV_SCALING else None),
                btc_ohlcv      = btc_ohlcv,
                filter_label   = _reg_filter_lbl,
                out_dir        = sweep_dir,
                min_days       = REGIME_MIN_DAYS,
            )
        else:
            print("  ⚠  Regime robustness: btc_ohlcv empty — skipping.")

    # ── MARKET CAP DIAGNOSTIC ─────────────────────────────────────────
    # Controlled by ENABLE_MCAP_DIAGNOSTIC at the top of the file.
    # Default: False  (~4 min runtime cost). Enable to verify universe
    # data quality or populate MCAP_STATS_* lines for the audit report.
    _mcap_stats = {}
    if not ENABLE_MCAP_DIAGNOSTIC:
        print("  [mcap] Diagnostic skipped (ENABLE_MCAP_DIAGNOSTIC = False)")
    else:
        _freq_csv_arg   = globals().get("_FREQ_CSV_PATH",  "")   # explicit --freq-csv override
        _mcap_parq_path = globals().get("_MCAP_PARQ_PATH", "")
        _mcap_quiet     = globals().get("_MCAP_QUIET",     False)
        _min_mcap_diag  = globals().get("_MIN_MCAP_DIAG",  0.0)
        _base_dir_diag  = globals().get("_BASE_DIR_DIAG",  "")

        # ── Resolve deploys path (sole symbol source for mcap diagnostic) ──
        # The mcap diagnostic must reflect exactly what went into each portfolio,
        # so it always reads the deploys file — never the frequency table.
        _resolved_freq = None
        _dep_fallback  = globals().get("_DEPLOYS_PATH", "")
        if _dep_fallback and Path(_dep_fallback).exists():
            _resolved_freq = Path(_dep_fallback)
            print(f"  [mcap] Using deploys CSV: {_resolved_freq.name}")
        else:
            print("  [mcap] No deploys CSV found — skipping diagnostic.")
            print("         Pass --deploys PATH to enable it.")

        if _resolved_freq is not None:
            # ── Resolve mcap parquet ──────────────────────────────────
            if _mcap_parq_path and Path(_mcap_parq_path).exists():
                _resolved_mcap = Path(_mcap_parq_path)
            else:
                _fp = _resolved_freq.parent
                _mcap_candidates = [
                    _fp / "binetl/data/marketcap/marketcap_daily.parquet",
                    _fp.parent / "binetl/data/marketcap/marketcap_daily.parquet",
                ]
                _resolved_mcap = next((p for p in _mcap_candidates if p.exists()), None)
                if _resolved_mcap is None:
                    print("  [mcap] Cannot locate marketcap_daily.parquet — skipping diagnostic.")
                    print("         Supply --mcap-parquet PATH to enable it.")

            if _resolved_mcap:
                try:
                    _mcap_stats = run_mcap_diagnostic(
                        freq_path = _resolved_freq,
                        mcap_path = _resolved_mcap,
                        min_mcap  = float(_min_mcap_diag),
                        quiet     = _mcap_quiet,
                    )
                except Exception as _mcap_err:
                    print(f"  [mcap] Diagnostic failed: {_mcap_err}")

    # ── VOL-LEV DD PARAMETRIC GRID SEARCH ────────────────────────────
    # Activated by --grid-search-vol-lev-dd (sets GRID_SEARCH_VOL_LEV_DD=True).
    # Runs AFTER the main simulation loop so df_4x, symbol_counts, filters,
    # and vol_lev params are all already resolved.
    if GRID_SEARCH_VOL_LEV_DD:
        print("\n[GS-VLDD] Launching VOL_LEV DD parametric grid search ...")
        # Resolve filter: prefer canonical tail_disp_vol, fall back to tail, then none
        _gs_mode   = "none"
        _gs_v3     = None
        _gs_label  = "No Filter"
        for _lbl, _fm, _fv3 in FILTER_MODES:
            if _fm == "tail_disp_vol":
                _gs_mode, _gs_v3, _gs_label = _fm, _fv3, _lbl
                break
        if _gs_mode == "none":
            for _lbl, _fm, _fv3 in FILTER_MODES:
                if _fm == "tail":
                    _gs_mode, _gs_v3, _gs_label = _fm, _fv3, _lbl
                    break
        if _gs_mode == "none":
            for _lbl, _fm, _fv3 in FILTER_MODES:
                _gs_mode, _gs_v3, _gs_label = _fm, _fv3, _lbl
                break

        _gs_cfg_name, _gs_cfg_params = CANDIDATE_CONFIGS[0]

        # Build vol_lev base dict from current globals (must be enabled)
        if ENABLE_VOL_LEV_SCALING:
            _gs_vlev_base = dict(
                target_vol   = VOL_LEV_TARGET_VOL,
                window       = VOL_LEV_WINDOW,
                sharpe_ref   = VOL_LEV_SHARPE_REF,
                dd_threshold = VOL_LEV_DD_THRESHOLD,
                dd_scale     = VOL_LEV_DD_SCALE,
                max_boost    = VOL_LEV_MAX_BOOST,
            )
        else:
            # Even when ENABLE_VOL_LEV_SCALING is False, we can still run the
            # sweep — we just enable it synthetically for this search.
            _gs_vlev_base = dict(
                target_vol   = VOL_LEV_TARGET_VOL,
                window       = VOL_LEV_WINDOW,
                sharpe_ref   = VOL_LEV_SHARPE_REF,
                dd_threshold = VOL_LEV_DD_THRESHOLD,
                dd_scale     = VOL_LEV_DD_SCALE,
                max_boost    = VOL_LEV_MAX_BOOST,
            )

        print(f"[GS-VLDD] Config  : {_gs_cfg_name}")
        print(f"[GS-VLDD] Filter  : {_gs_label} ({_gs_mode})")
        print(f"[GS-VLDD] Vol-lev : target_vol={_gs_vlev_base['target_vol']:.3f}  "
              f"window={_gs_vlev_base['window']}d  "
              f"max_boost={_gs_vlev_base['max_boost']}×")

        _gs_outdir = run_dir / "grid_search_vol_lev_dd"
        _gs_df = run_vol_lev_dd_grid_search(
            df_4x        = df_4x,
            base_params  = _gs_cfg_params,
            filter_mode  = _gs_mode,
            v3_filter    = _gs_v3,
            symbol_counts= symbol_counts,
            vlev_base    = _gs_vlev_base,
            out_dir      = _gs_outdir,
        )

        # Also write a copy to the working directory for easy access
        _gs_wdir_csv = Path("grid_search_vol_lev_dd_results.csv")
        _gs_df.sort_values(["dd_threshold", "dd_scale"]).to_csv(
            _gs_wdir_csv, index=False)
        print(f"[GS-VLDD] Copy saved → {_gs_wdir_csv}")

    # ── FINAL SUMMARY LINES (parsed by overlap_analysis.py) ──────────
    # Printed once per filter mode so the caller can extract them.
    # Format: FINAL_<KEY>(<filter_label>): <value>
    print()
    print("=" * 70)
    print("  FINAL METRICS PER FILTER")
    print("=" * 70)
    for _run_key, _r in results_map.items():
        _sh  = _r.get("sharpe",         float("nan"))
        _ca  = _r.get("cagr",           float("nan"))
        _md  = _r.get("maxdd",          float("nan"))
        _act = _r.get("active_days",    0)
        _cv  = _r.get("cv",             float("nan"))
        _tr  = _r.get("net_return_pct", float("nan"))
        _wd  = _r.get("worst_day_pct",     float("nan"))
        _ww  = _r.get("worst_weekly_pct",  float("nan"))
        _wm  = _r.get("worst_monthly_pct", float("nan"))
        _dsr = _r.get("dsr",            float("nan"))  # statistical DSR %
        _grd = _r.get("scorecard", {}).get("overall_grade", "?")
        _gsc = _r.get("scorecard", {}).get("total_score",   float("nan"))
        # Sanitise run_key for easy parsing: replace spaces/special chars
        _tag = _run_key.replace(" ", "_").replace("+", "p").replace("/", "_")
        print(f"FINAL_SHARPE({_tag}):        {_sh:.4f}")
        print(f"FINAL_CAGR({_tag}):          {_ca:.4f}")
        print(f"FINAL_MAX_DD({_tag}):        {_md:.4f}")
        print(f"FINAL_ACTIVE_DAYS({_tag}):   {_act}")
        print(f"FINAL_WF_CV({_tag}):         {_cv:.4f}")
        print(f"FINAL_TOTAL_RETURN({_tag}):  {_tr:.4f}")
        print(f"FINAL_WORST_DAY({_tag}):     {_wd:.4f}" if math.isfinite(_wd) else f"FINAL_WORST_DAY({_tag}):     nan")
        print(f"FINAL_WORST_WEEK({_tag}):    {_ww:.4f}" if math.isfinite(_ww) else f"FINAL_WORST_WEEK({_tag}):    nan")
        print(f"FINAL_WORST_MONTH({_tag}):   {_wm:.4f}" if math.isfinite(_wm) else f"FINAL_WORST_MONTH({_tag}):   nan")
        print(f"FINAL_DSR({_tag}):           {_dsr:.4f}" if math.isfinite(_dsr) else f"FINAL_DSR({_tag}):           nan")
        print(f"FINAL_GRADE({_tag}):         {_grd}")
        print(f"FINAL_GRADE_SCORE({_tag}):   {_gsc:.1f}" if math.isfinite(float(_gsc)) else f"FINAL_GRADE_SCORE({_tag}):   nan")
        _dwz = _r.get("daily_with_zeros")
        if _dwz is not None:
            import json as _json
            import numpy as _np
            _r_plot  = _np.where(_np.isfinite(_dwz), _dwz, 0.0)
            _eq_norm = _np.cumprod(1 + _r_plot)
            _ath     = _np.maximum.accumulate(_eq_norm)
            _dd      = (_eq_norm - _ath) / _ath * 100
            print(f"FINAL_EQUITY_SERIES({_tag}): {_json.dumps([round(float(x), 4) for x in _eq_norm.tolist()])}")
            print(f"FINAL_DD_SERIES({_tag}): {_json.dumps([round(float(x), 4) for x in _dd.tolist()])}")
    print("=" * 70)

    # ── INTRADAY BAR PATHS (raw 1x% cumulative return from open per bar) ─────
    # Emitted once (filter-agnostic) so the frontend can draw actual intraday
    # paths for each day rather than straight-line approximations.
    # Values are: df_4x[col][i] / PIVOT_LEVERAGE * 100  (1x pct from open).
    # Flat/NaN bars are stored as null.
    try:
        import json as _json_id
        import numpy as _np_id
        _intraday_dict: dict = {}
        for _id_col in df_4x.columns:
            _ts = _col_to_timestamp(str(_id_col))
            _id_key = _ts.strftime("%Y-%m-%d") if _ts is not None else str(_id_col)[:10]
            _id_raw  = df_4x[_id_col].to_numpy(dtype=float)
            # df_4x already stores the CUMULATIVE return from session open as a
            # decimal (e.g. -0.016 = -1.6%).  Just scale to % — no cumsum needed.
            _id_arr  = _id_raw * 100.0
            _intraday_dict[_id_key] = [
                round(float(v), 3) if _np_id.isfinite(v) else None
                for v in _id_arr.tolist()
            ]
        print(f"FINAL_INTRADAY_BARS: {_json_id.dumps(_intraday_dict)}")
        print(f"FINAL_INTRADAY_EXIT_BARS: {_json_id.dumps(_exit_bars_by_filter)}")
    except Exception:
        pass

    # ── DAILY PORTFOLIO BREAKDOWN ─────────────────────────────────────
    # Combine deploys CSV (symbol names) + fees_rows (returns, filter
    # decisions) + exit_bars into a per-day breakdown for the UI.
    try:
        _dp_deploys_path = globals().get("_DEPLOYS_PATH", "") or ""
        _dp_symbols_by_date: dict = {}
        if _dp_deploys_path:
            _dp_df = pd.read_csv(_dp_deploys_path)
            _dp_rank_cols = [c for c in _dp_df.columns if c.startswith("R") and c[1:].isdigit()]
            for _, _dp_row in _dp_df.iterrows():
                _dp_date_raw = _dp_row.iloc[0]  # timestamp_utc column
                _dp_date = pd.Timestamp(_dp_date_raw).strftime("%Y-%m-%d")
                _dp_syms = [str(_dp_row[c]) for c in _dp_rank_cols
                            if pd.notna(_dp_row[c]) and str(_dp_row[c]).strip()]
                if _dp_syms:
                    _dp_symbols_by_date[_dp_date] = _dp_syms

        # Build a daily portfolio for EACH filter so the UI can switch views
        import json as _json_dp
        _dp_first_portfolio: dict | None = None
        for _dp_label, _dp_fees in _fees_rows_by_filter.items():
            _dp_exits = _exit_bars_by_filter.get(_dp_label, {})
            _dp_portfolio: dict = {}
            for _dp_frow in _dp_fees:
                _dp_dt    = _dp_frow[0]   # date string
                _dp_mg    = _dp_frow[2]   # margin (-1.0 = no entry, -2.0 = ratchet)
                _dp_lev   = _dp_frow[3]   # leverage
                _dp_gr    = _dp_frow[9]   # gross return %
                _dp_nr    = _dp_frow[10]  # net return %

                # Determine filter/conviction status
                if _dp_mg < 0:
                    if _dp_mg == -2.0:
                        _dp_filter = "ratchet_off"
                        _dp_conviction = "n/a"
                    else:
                        if _dp_lev == 0.0:
                            _dp_filter = "filtered"
                            _dp_conviction = "n/a"
                        else:
                            _dp_filter = "pass"
                            _dp_conviction = "fail"
                else:
                    _dp_filter = "pass"
                    _dp_conviction = "pass"

                _dp_raw = round(_dp_gr / _dp_lev, 4) if _dp_lev and _dp_lev > 0 else 0.0

                _dp_exit_bar = _dp_exits.get(_dp_dt)
                if _dp_filter == "filtered":
                    _dp_exit_reason = "filtered"
                elif _dp_conviction == "fail":
                    _dp_exit_reason = "no_entry"
                elif _dp_exit_bar == -1 or _dp_exit_bar is None:
                    _dp_exit_reason = "held"
                else:
                    _dp_exit_reason = "early_exit"

                _dp_portfolio[_dp_dt] = {
                    "symbols": _dp_symbols_by_date.get(_dp_dt, []),
                    "filter": _dp_filter,
                    "filter_name": _dp_label,
                    "conviction": _dp_conviction,
                    "raw_roi": round(_dp_raw, 2),
                    "strat_roi": round(_dp_nr, 2),
                    "exit_reason": _dp_exit_reason,
                }

            if _dp_portfolio:
                # Tag uses underscores for spaces, matching FINAL_FILTER_VALUE convention
                _dp_tag = _dp_label.replace(" ", "_")
                print(f"FINAL_DAILY_PORTFOLIO_{_dp_tag}: {_json_dp.dumps(_dp_portfolio)}")
                if _dp_first_portfolio is None:
                    _dp_first_portfolio = _dp_portfolio

        # Legacy single-emission for backward compat with older parsers
        if _dp_first_portfolio:
            print(f"FINAL_DAILY_PORTFOLIO: {_json_dp.dumps(_dp_first_portfolio)}")
    except Exception as _dp_err:
        print(f"  [daily_portfolio] Error building portfolio breakdown: {_dp_err}")

    # ── MARKET CAP SUMMARY PANEL ──────────────────────────────────────
    # Printed at the very end so it's visible alongside the performance
    # results without having to scroll back through the full diagnostic.
    # Only shown when the diagnostic ran successfully.
    if _mcap_stats:
        print()
        print("=" * 70)
        print("  MARKET CAP UNIVERSE SUMMARY")
        print("=" * 70)
        _sym_cov  = _mcap_stats.get("symbol_coverage_pct",  float("nan"))
        _row_rate = _mcap_stats.get("row_match_rate_pct",   float("nan"))
        _mean_m   = _mcap_stats.get("mean_mcap_M",          float("nan"))
        _med_m    = _mcap_stats.get("median_mcap_M",        float("nan"))
        _total    = _mcap_stats.get("total_rows",           0)
        _missing  = _mcap_stats.get("missing_rows",         0)
        _unmatched = _mcap_stats.get("unmatched_symbols",   [])
        _above    = _mcap_stats.get("above_threshold_pct",  None)
        _min_filt = globals().get("_MIN_MCAP_DIAG", 0.0)

        print(f"  Symbol coverage   : {_sym_cov:.1f}%  "
              f"({_mcap_stats.get('matched_symbols', '?')} / "
              f"{_mcap_stats.get('matched_symbols', 0) + len(_unmatched)} unique symbols matched in parquet)")
        print(f"  Row match rate    : {_row_rate:.1f}%  "
              f"({_total - _missing:,} / {_total:,} date×symbol rows have mcap data)")
        print(f"  Mean mcap         : ${_mean_m:.1f}M  (matched rows only)")
        print(f"  Median mcap       : ${_med_m:.1f}M  (matched rows only)")
        if _above is not None:
            print(f"  Above ${_min_filt/1e6:.0f}M filter  : {_above:.1f}%  of matched rows qualify")
        if _unmatched:
            print(f"  ⚠  {len(_unmatched)} symbol(s) missing from mcap parquet: {sorted(_unmatched)}")
        else:
            print(f"  ✅ All symbols matched in mcap parquet")

        # Data quality flag
        if _row_rate < 80:
            print(f"  ⚠  LOW match rate ({_row_rate:.1f}%) — mcap stats may be unreliable")
        elif _row_rate < 95:
            print(f"  ⚠  Partial match rate ({_row_rate:.1f}%) — some dates/symbols lack mcap data")
        else:
            print(f"  ✅ Match rate healthy ({_row_rate:.1f}%)")
        print("=" * 70)

    # ── ALLOCATOR SCORECARD ───────────────────────────────────────────
    _avg_overlap = float("nan")
    try:
        # Read from the overlap diagnostic CSV if it exists alongside deploys
        if "_DEPLOYS_PATH" in globals() and _DEPLOYS_PATH:
            _diag_glob = list(Path(_DEPLOYS_PATH).parent.glob(
                "overlap_diagnostic_*.csv"))
            if _diag_glob:
                _diag_df    = pd.read_csv(_diag_glob[0])
                _avg_overlap = float(_diag_df["overlap_count"].mean())
    except Exception:
        pass

    _total_days_sc = int(len(list(results_map.values())[0].get(
        "daily_with_zeros", []))) if results_map else 0

    build_scorecards(
        results_map    = results_map,
        run_dir        = run_dir,
        avg_overlap    = _avg_overlap,
        total_days     = _total_days_sc,
        leaderboard_n  = int(globals().get("_LEADERBOARD_INDEX_DIAG", 100)),
    )

    # ── Rename run_dir to include best Sharpe for easy identification ──
    try:
        _best_sharpe = max(
            (v.get("sharpe", float("nan")) for v in results_map.values()),
            default=float("nan"),
            key=lambda x: x if not (x != x) else float("-inf"),
        )
        if _best_sharpe == _best_sharpe:  # not nan
            _sh_tag      = f"sh{_best_sharpe:.3f}"
            _new_run_dir = run_dir.parent / f"{run_dir.name}_{_sh_tag}"
            run_dir.rename(_new_run_dir)
            run_dir  = _new_run_dir          # keep reference current
            _log_path = run_dir / "audit_output.txt"
            print(f"  [run] Renamed run folder -> {run_dir.name}")
    except Exception as _re:
        print(f"  [run] Could not rename run folder: {_re}")

    # ── Print final output paths (after rename so paths are correct) ──
    if _comparison_chart_path is not None:
        print(f"\n  Comparison chart saved: {_comparison_chart_path}")
    print(f"\n  All outputs saved to: {run_dir}\n")

    # ── Export VOL boost seed CSV (Tail + Dispersion net returns) ─────
    # Finds the fees panel CSV for the tail_disp filter and writes a
    # clean seed file: date, net_return_pct — ready for:
    #   python3 trader-blofin.py --seed-returns vol_boost_seed.csv
    try:
        import csv as _csv_mod, glob as _glob
        _seed_candidates = list(run_dir.glob("**/fees_panel_*_tail_disp.csv"))
        if not _seed_candidates:
            print("  [seed] No tail_disp fees panel CSV found -- skipping seed export")
        else:
            _fees_src = _seed_candidates[0]
            _seed_rows = []
            with open(_fees_src, newline="") as _fh:
                _reader = _csv_mod.DictReader(_fh)
                for _row in _reader:
                    _seed_rows.append({
                        "date":           _row["date"],
                        "net_return_pct": round(float(_row["ret_net_pct"]), 4),
                        "exit_reason":    _row["type"],
                    })
            _seed_path = run_dir / "vol_boost_seed.csv"
            with open(_seed_path, "w", newline="") as _fh:
                _w = _csv_mod.DictWriter(_fh,
                    fieldnames=["date", "net_return_pct", "exit_reason"])
                _w.writeheader()
                _w.writerows(_seed_rows)
            _n_trade = sum(1 for r in _seed_rows if float(r["net_return_pct"]) != 0.0)
            _n_flat  = len(_seed_rows) - _n_trade
            print(f"  [seed] VOL boost seed CSV written: {_seed_path}")
            print(f"         {len(_seed_rows)} rows  "
                  f"({_seed_rows[0]['date']} to {_seed_rows[-1]['date']})  "
                  f"trade={_n_trade}  flat={_n_flat}")
            print(f"  [seed] Usage: python3 trader-blofin.py --seed-returns {_seed_path}")
    except Exception as _se:
        print(f"  [seed] Seed export failed: {_se}")

    # ── Restore stdout and close run log ──────────────────────────────
    sys.stdout = _orig_stdout
    _log_fh.flush()
    _log_fh.close()
    print(f"  [log] audit_output.txt saved to: {_log_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Regime filter audit. Optionally supply a custom deploys CSV to "
                    "rebuild the portfolio matrix before running."
    )
    parser.add_argument(
        "--deploys",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to a deploys CSV file. If provided, rebuild_portfolio_matrix.py is "
             "run first to generate a fresh portfolio matrix, which is then used "
             "instead of Google Sheets. If omitted, the audit runs against Sheets as normal."
    )
    parser.add_argument(
        "--rebuild-script",
        type=str,
        default="rebuild_portfolio_matrix.py",
        metavar="PATH",
        help="Path to rebuild_portfolio_matrix.py (default: same directory)."
    )
    parser.add_argument(
        "--matrix-out",
        type=str,
        default="portfolio_matrix_gated.csv",
        metavar="PATH",
        help="Output path for the rebuilt matrix CSV (default: portfolio_matrix_gated.csv)."
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        default=False,
        help="Skip institutional audit, walk-forward CV, IC diagnostic, PBO, "
             "charts, and sweeps. Runs simulate() only and prints scorecard/verdict. "
             "Much faster — ideal for batch testing many deploys files."
    )
    parser.add_argument(
        "--grid-search-start-time",
        action="store_true",
        default=False,
        dest="grid_search_start_time",
        help="Grid search over all 24 deployment_start_time values (00:00–23:00 UTC). "
             "Requires --deploys. For each hour, rebuilds the portfolio matrix via "
             "--rebuild-script with --start-hour H, runs audit.py --quick, and "
             "collects results into grid_search_start_time_results.csv. "
             "deployment_lookback and deployment_runtime remain fixed."
    )
    parser.add_argument(
        "--grid-search-vol-lev-dd",
        action="store_true",
        default=False,
        dest="grid_search_vol_lev_dd",
        help="Parametric grid search over VOL_LEV_DD_THRESHOLD × VOL_LEV_DD_SCALE. "
             "Sweeps threshold from 0%% to -20%% in 1%% steps (21 values) and "
             "dd_scale from 0.0 to 1.0 in 0.1 steps (11 values) — 231 cells total. "
             "Runs the main audit first, then executes the sweep using the canonical "
             "filter (tail_disp_vol → tail → none) and the first CANDIDATE_CONFIG. "
             "Records Sharpe, CAGR%%, MaxDD%%, Active, WF-CV, TotRet%%, Eq, "
             "Wst1D%%, Wst1W%%, Wst1M%%, DSR%% for each cell. "
             "Saves grid_search_vol_lev_dd_results.csv to the run directory "
             "and to the working directory."
    )
    parser.add_argument(
        "--min-listing-age",
        type=int, default=None, dest="min_listing_age",
        help="Override MIN_LISTING_AGE_DAYS in rebuild script (e.g. 0 to match Sheets)."
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        choices=["binance", "parquet", "db"],
        help="Price source for rebuild: binance (matches Sheets), parquet (fast), or db (market.futures_1m). Default: parquet."
    )
    parser.add_argument(
        "--max-port",
        type=int,
        default=None,
        metavar="N",
        help="Cap symbols per day to N after eligibility gate (passed to rebuild script)."
    )
    parser.add_argument(
        "--freq-csv",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to the symbol source for the mcap diagnostic. Accepts either: "
             "(1) a deploys CSV (e.g. deploys_overlap_top100_w20c20_snapshot_0600_0M.csv) — "
             "recommended when using a portfolio matrix, gives per-day mcap for exactly the "
             "symbols traded; or (2) a freq_price CSV from overlap_analysis.py "
             "(e.g. freq_price_top100_w20_snapshot_0600_0M.csv). "
             "The file type is auto-detected. "
             "When provided alongside --mcap-parquet, the market-cap diagnostic "
             "runs automatically and its stats appear in the report."
    )
    parser.add_argument(
        "--mcap-parquet",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to marketcap_daily.parquet. Required for the mcap diagnostic. "
             "Defaults to <benji3m>/binetl/data/marketcap/marketcap_daily.parquet "
             "when --freq-csv is supplied without this flag."
    )
    parser.add_argument(
        "--mcap-quiet",
        action="store_true",
        default=False,
        help="Suppress the per-day mcap table in the diagnostic output."
    )
    parser.add_argument(
        "--min-mcap",
        type=float,
        default=0.0,
        metavar="N",
        dest="min_mcap",
        help="Market cap threshold in USD (e.g. 50000000 for $50M). Used to derive the "
             "freq CSV filename AND as the eligibility threshold in the diagnostic. "
             "Does not filter simulation data — that is handled by overlap_analysis.py."
    )
    # ── Config args for dynamic freq CSV path derivation ─────────────────
    # These let overlap_analysis.py pass its run config through to audit.py
    # so the diagnostic can reconstruct the exact freq_price_*.csv filename.
    parser.add_argument(
        "--base-dir",
        type=str, default="", metavar="PATH", dest="base_dir",
        help="Base directory containing freq CSV files (i.e. the benji3m root). "
             "When set, the freq CSV path is derived from the run config args below."
    )
    parser.add_argument(
        "--leaderboard-index",
        type=int, default=1000, dest="leaderboard_index_diag", metavar="N",
        help="Leaderboard index used in the overlap_analysis.py run (e.g. 100, 300, 1000). "
             "Used to derive the freq CSV filename. Default: 100"
    )
    parser.add_argument(
        "--freq-width",
        type=int, default=20, dest="freq_width_diag", metavar="N",
        help="freq-width used in the overlap_analysis.py run. "
             "Used to derive the freq CSV filename. Default: 20"
    )
    parser.add_argument(
        "--mode",
        type=str, default="snapshot", dest="mode_diag",
        choices=["snapshot", "frequency"],
        help="Mode used in the overlap_analysis.py run. "
             "Used to derive the freq CSV filename. Default: snapshot"
    )
    parser.add_argument(
        "--deployment-start-hour",
        type=int, default=None, dest="deployment_start_hour", metavar="H",
        help="Deployment window start hour UTC (0-23). "
             "Overrides DEPLOYMENT_START_HOUR constant (default: 6). "
             "Passed to rebuild_portfolio_matrix.py as --start-hour."
    )
    parser.add_argument(
        "--deployment-runtime-hours",
        type=str, default=None, dest="deployment_runtime_hours", metavar="H",
        help="Deployment window length: integer hours or \"daily\". "
             "\"daily\" sets runtime = 24 - sort_lookback so the full 24h "
             "cycle is filled exactly. Overrides DEPLOYMENT_RUNTIME_HOURS "
             "constant (default: daily). Recomputes N_ROWS and passed to rebuild script."
    )
    parser.add_argument(
        "--sort-lookback",
        type=str, default=None, dest="sort_lookback", metavar="N",
        help="Electoral window length: integer hours back from deployment_start_hour, "
             "or \"daily\" to scan from 00:05 to deployment_start_hour. "
             "Overrides SORT_LOOKBACK constant (default: 6). "
             "Passed to rebuild_portfolio_matrix.py."
    )
    parser.add_argument(
        "--capital-mode",
        type=str, default=None, dest="capital_mode",
        choices=["compounding", "fixed"],
        help="Capital allocation mode: compounding (default) compounds P&L so "
             "position size grows with the account; fixed uses a fixed notional "
             "every day. See --fixed-notional-cap for deficit behaviour. "
             "Overrides CAPITAL_MODE constant."
    )
    parser.add_argument(
        "--fixed-notional-cap",
        type=str, default=None, dest="fixed_notional_cap",
        choices=["external", "internal"],
        help="Only used when --capital-mode fixed is set. "
             "external (default): always trade STARTING_CAPITAL notional even if "
             "account balance falls below it (account can go negative). "
             "internal: trade min(STARTING_CAPITAL, current_equity) — position "
             "scales down if account dips below STARTING_CAPITAL. "
             "Overrides FIXED_NOTIONAL_CAP constant."
    )
    parser.add_argument(
        "--end-cross-midnight",
        dest="end_cross_midnight", action="store_true", default=None,
        help="Allow deployment window to overflow past 23:59 UTC into the next "
             "calendar day (default: True). Passed to rebuild_portfolio_matrix.py."
    )
    parser.add_argument(
        "--no-end-cross-midnight",
        dest="end_cross_midnight", action="store_false",
        help="Cap deployment window at 23:55 UTC same day. "
             "Passed to rebuild_portfolio_matrix.py."
    )
    args = parser.parse_args()

    if getattr(args, "capital_mode", None):
        CAPITAL_MODE = args.capital_mode
        print(f"\n[CLI] --capital-mode={args.capital_mode} — "
              f"position sizing set to {args.capital_mode} mode.")

    if getattr(args, "fixed_notional_cap", None):
        FIXED_NOTIONAL_CAP = args.fixed_notional_cap
        print(f"\n[CLI] --fixed-notional-cap={args.fixed_notional_cap}")

    if args.quick:
        QUICK_MODE = True
        print("\n[CLI] --quick mode enabled — skipping heavy audits, charts, and sweeps.")

    if getattr(args, "grid_search_vol_lev_dd", False):
        GRID_SEARCH_VOL_LEV_DD = True
        print("\n[CLI] --grid-search-vol-lev-dd enabled — "
              "VOL_LEV DD parametric grid search will run after main audit.")

    _DEPLOYS_PATH    = args.deploys      if args.deploys      else ""
    # ── Auto-discover deploys CSV if not explicitly provided ──────────────
    # overlap_analysis.py always writes deploys_overlap_*.csv to the working
    # directory. If --deploys wasn't passed we try to find it automatically
    # so the Blofin filter and mcap diagnostic work without extra flags.
    if not _DEPLOYS_PATH:
        _dep_candidates = sorted(
            Path(".").glob("deploys_overlap_*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if _dep_candidates:
            _DEPLOYS_PATH = str(_dep_candidates[0])
            print(f"\n[CLI] Auto-discovered deploys CSV: {_dep_candidates[0].name}"
                  f"  (use --deploys PATH to override)")
        else:
            _dep_candidates_alt = sorted(
                Path(".").glob("deploys_*.csv"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if _dep_candidates_alt:
                _DEPLOYS_PATH = str(_dep_candidates_alt[0])
                print(f"\n[CLI] Auto-discovered deploys CSV: {_dep_candidates_alt[0].name}"
                      f"  (use --deploys PATH to override)")

    _FREQ_CSV_PATH   = args.freq_csv     if args.freq_csv     else ""   # explicit override only
    _MCAP_PARQ_PATH  = args.mcap_parquet if args.mcap_parquet else ""
    _MCAP_QUIET      = args.mcap_quiet
    # ── Config globals for dynamic freq CSV path derivation ───────────────
    # These mirror the overlap_analysis.py run parameters so audit.py can
    # reconstruct the exact freq_price_*.csv filename without it being hardcoded.
    # overlap_analysis.py passes these as CLI args when it calls run_audit().
    _MIN_MCAP_DIAG        = getattr(args, "min_mcap",          0.0)   or 0.0
    _BASE_DIR_DIAG        = getattr(args, "base_dir",          "")    or ""
    _LEADERBOARD_INDEX_DIAG = getattr(args, "leaderboard_index_diag", 100)  or 100
    _FREQ_WIDTH_DIAG      = getattr(args, "freq_width_diag",   20)    or 20
    _MODE_DIAG            = getattr(args, "mode_diag",         "snapshot") or "snapshot"

    # ── Override deployment window globals from CLI ───────────────────────────
    if getattr(args, "deployment_start_hour", None) is not None:
        DEPLOYMENT_START_HOUR = args.deployment_start_hour
    if getattr(args, "deployment_runtime_hours", None) is not None:
        _rt = args.deployment_runtime_hours
        DEPLOYMENT_RUNTIME_HOURS = "daily" if str(_rt).lower() == "daily" else int(_rt)
        N_ROWS = int(_resolve_runtime_hours() * 60 // BAR_MINUTES)
    if getattr(args, "sort_lookback", None) is not None:
        SORT_LOOKBACK = args.sort_lookback if args.sort_lookback == "daily" \
                        else int(args.sort_lookback)

    # ── Grid search: deployment_start_time (00:00–23:00 UTC) ─────────────────
    # Activated by --grid-search-start-time. Requires --deploys.
    # For each candidate start hour:
    #   1. Rebuild the portfolio matrix with --start-hour H (requires rebuild
    #      script to accept this flag — see note below).
    #   2. Run audit.py --quick on the resulting matrix.
    #   3. Parse the scorecard output and collect into a results table.
    #   4. Write ranked CSV: grid_search_start_time_results.csv
    #
    # NOTE FOR rebuild_portfolio_matrix.py:
    #   This loop passes --start-hour H to the rebuild script. You must add a
    #   matching argparse argument there:
    #
    #     parser.add_argument("--start-hour", type=int, default=6, dest="start_hour",
    #                         help="Deployment window start hour UTC (0–23). Default: 6.")
    #
    #   Then use args.start_hour wherever 6 (or "060000") is currently hardcoded
    #   in the matrix builder (e.g. column name construction, bar-slice offsets).
    if args.grid_search_start_time:
        if not args.deploys:
            print("\n[CLI] ERROR: --grid-search-start-time requires --deploys PATH.")
            sys.exit(1)

        import re as _re

        print("\n" + "═" * 70)
        print("  GRID SEARCH: deployment_start_time  (00:00–23:00 UTC)")
        print(f"  Fixed: sort_lookback={SORT_LOOKBACK}  runtime={DEPLOYMENT_RUNTIME_HOURS}h  bars={N_ROWS}")
        print("═" * 70)

        _gs_results: list = []
        _audit_script = Path(__file__).resolve()

        for _hour in range(24):
            _hh = f"{_hour:02d}"
            _matrix_out_h = f"portfolio_matrix_start_{_hh}00.csv"
            print(f"\n[GS] ── Start hour {_hh}:00 UTC ({'rebuilding matrix...'}) ──")

            # Step 1: rebuild matrix for this start hour
            _rebuild_cmd = [
                "python3", args.rebuild_script,
                "--deploys",    args.deploys,
                "--output",     _matrix_out_h,
                "--start-hour", str(_hour),
            ]
            if args.max_port is not None:
                _rebuild_cmd += ["--max-port", str(args.max_port)]
            if getattr(args, "source", None) is not None:
                _rebuild_cmd += ["--source", args.source]
            if getattr(args, "min_listing_age", None) is not None:
                _rebuild_cmd += ["--min-listing-age", str(args.min_listing_age)]
            if getattr(args, "deployment_runtime_hours", None) is not None:
                _rebuild_cmd += ["--deployment-runtime-hours", str(args.deployment_runtime_hours)]
            if getattr(args, "sort_lookback", None) is not None:
                _rebuild_cmd += ["--sort-lookback", str(args.sort_lookback)]
            if getattr(args, "end_cross_midnight", None) is not None:
                _rebuild_cmd += ["--end-cross-midnight" if args.end_cross_midnight
                                 else "--no-end-cross-midnight"]

            try:
                subprocess.run(_rebuild_cmd, check=True)
            except subprocess.CalledProcessError as _e:
                print(f"[GS] WARNING: rebuild failed for hour {_hh} — skipping. ({_e})")
                _gs_results.append({
                    "start_hour_utc": _hour,
                    "start_time":     f"{_hh}:00",
                    "sharpe":         float("nan"),
                    "max_dd_pct":     float("nan"),
                    "total_return_pct": float("nan"),
                    "active_days":    None,
                    "status":         "rebuild_failed",
                })
                continue

            # Step 2: run audit --quick on the new matrix
            _audit_cmd = [
                "python3", str(_audit_script),
                "--quick",
                "--matrix-out", _matrix_out_h,
                # Pass through other relevant flags
                *(["--min-listing-age", str(args.min_listing_age)]
                  if getattr(args, "min_listing_age", None) is not None else []),
                *(["--source", args.source]
                  if getattr(args, "source", None) is not None else []),
            ]

            try:
                _proc = subprocess.run(
                    _audit_cmd, check=True,
                    capture_output=True, text=True,
                )
                _out = _proc.stdout + _proc.stderr
            except subprocess.CalledProcessError as _e:
                _out = (_e.stdout or "") + (_e.stderr or "")
                print(f"[GS] WARNING: audit failed for hour {_hh}.")
                print(_out[-2000:])  # tail of output for debugging
                _gs_results.append({
                    "start_hour_utc": _hour,
                    "start_time":     f"{_hh}:00",
                    "sharpe":         float("nan"),
                    "max_dd_pct":     float("nan"),
                    "total_return_pct": float("nan"),
                    "active_days":    None,
                    "status":         "audit_failed",
                })
                continue

            # Step 3: parse scorecard metrics from stdout
            # Looks for lines like:  Sharpe=2.91  MaxDD=-18.4%  ...
            def _parse_metric(pattern, text, cast=float):
                m = _re.search(pattern, text)
                return cast(m.group(1)) if m else float("nan")

            _sharpe   = _parse_metric(r"Sharpe\s*=\s*([\-\d\.]+)", _out)
            _maxdd    = _parse_metric(r"MaxDD\s*=\s*([\-\d\.]+)%",  _out)
            _active   = _parse_metric(r"Active\s*=\s*(\d+)d",       _out, int)
            # Total return: look for equity curve endpoint printed in scorecard
            _tot_ret  = _parse_metric(r"TotalReturn\s*=\s*([\-\d\.]+)%", _out)

            print(f"[GS]   Sharpe={_sharpe:.3f}  MaxDD={_maxdd:.2f}%  "
                  f"Active={_active}d  TotalReturn={_tot_ret:.2f}%")

            _gs_results.append({
                "start_hour_utc":   _hour,
                "start_time":       f"{_hh}:00 UTC",
                "sharpe":           round(_sharpe, 4),
                "max_dd_pct":       round(_maxdd,  2),
                "total_return_pct": round(_tot_ret, 2),
                "active_days":      _active,
                "matrix_csv":       _matrix_out_h,
                "status":           "ok",
            })

        # Step 4: rank and save
        _gs_df = pd.DataFrame(_gs_results)
        _gs_df_ok = _gs_df[_gs_df["status"] == "ok"].copy()
        if not _gs_df_ok.empty:
            _gs_df_ok = _gs_df_ok.sort_values("sharpe", ascending=False).reset_index(drop=True)

        _gs_out_path = "grid_search_start_time_results.csv"
        _gs_df.sort_values("start_hour_utc").to_csv(_gs_out_path, index=False)

        print("\n" + "═" * 70)
        print("  GRID SEARCH COMPLETE — ranked by Sharpe")
        print("═" * 70)
        if not _gs_df_ok.empty:
            print(_gs_df_ok[["start_time", "sharpe", "max_dd_pct",
                              "total_return_pct", "active_days"]].to_string(index=False))
            _best = _gs_df_ok.iloc[0]
            print(f"\n  ✅ Best start time : {_best['start_time']}")
            print(f"     Sharpe          : {_best['sharpe']}")
            print(f"     Max DD          : {_best['max_dd_pct']}%")
            print(f"     Total Return    : {_best['total_return_pct']}%")
            print(f"     Active days     : {_best['active_days']}")
        else:
            print("  No successful runs to rank.")
        print(f"\n  Full results saved to: {_gs_out_path}")
        print("═" * 70)
        sys.exit(0)   # grid search complete — do not fall through to main()

    if args.deploys:
        print(f"\n[CLI] --deploys provided: {args.deploys}")
        print(f"[CLI] Running {args.rebuild_script} to build matrix ...")
        rebuild_cmd = ["python3", args.rebuild_script,
                       "--deploys", args.deploys,
                       "--output",  args.matrix_out]
        if args.max_port is not None:
            rebuild_cmd += ["--max-port", str(args.max_port)]
        if getattr(args, "source", None) is not None:
            rebuild_cmd += ["--source", args.source]
        if getattr(args, "min_listing_age", None) is not None:
            rebuild_cmd += ["--min-listing-age", str(args.min_listing_age)]
        if getattr(args, "deployment_start_hour", None) is not None:
            rebuild_cmd += ["--start-hour", str(args.deployment_start_hour)]
        if getattr(args, "deployment_runtime_hours", None) is not None:
            rebuild_cmd += ["--deployment-runtime-hours", str(args.deployment_runtime_hours)]
        if getattr(args, "sort_lookback", None) is not None:
            rebuild_cmd += ["--sort-lookback", str(args.sort_lookback)]
        if getattr(args, "end_cross_midnight", None) is not None:
            rebuild_cmd += ["--end-cross-midnight" if args.end_cross_midnight
                            else "--no-end-cross-midnight"]
        result = subprocess.run(rebuild_cmd, check=True)
        print(f"[CLI] Matrix built → {args.matrix_out}")
        # Override the global so load_matrix() picks up the local CSV
        LOCAL_MATRIX_CSV = args.matrix_out
    else:
        print("\n[CLI] No --deploys provided — loading matrix from Google Sheets.")

    main()

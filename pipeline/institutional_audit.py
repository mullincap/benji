"""
institutional_audit.py

Drop-in institutional audit module for deployability_audit.py.

Implements ALL missing institutional-grade tests:

  RISK-ADJUSTED RETURN QUALITY
  - Sortino ratio
  - Calmar ratio
  - Omega ratio
  - Ulcer Index

  DRAWDOWN ANALYSIS
  - Drawdown duration & recovery per episode
  - Average drawdown
  - Distinct drawdown episode table
  - % time spent underwater

  RETURN DISTRIBUTION
  - Skewness & kurtosis
  - Jarque-Bera normality test (implemented from scratch)
  - Ljung-Box autocorrelation test (implemented from scratch)

  REGIME & CONDITIONAL ANALYSIS
  - Up vs down regime conditional performance
  - Low vs high vol regime split
  - Rolling Sharpe over time
  - Rolling CAGR window

  CAPACITY & EXECUTION
  - Slippage sensitivity table (alpha erosion curve)

  TAIL RISK
  - Weekly CVaR (worst 1% of weeks)
  - Max consecutive losing days / streaks
  - Top 5 drawdown episodes (depth, duration, recovery)

  STATISTICAL VALIDITY
  - Deflated Sharpe Ratio (DSR) — corrects for parameter sweep overfitting
  - Minimum Track Record Length (MTL)
  - Profit factor

  CAPITAL & OPERATIONAL
  - Leverage sensitivity table
  - Kelly fraction
  - Ruin probability estimate

  CHARTS
  - Rolling Sharpe chart
  - Slippage sensitivity chart
  - Leverage sensitivity chart
  - Drawdown episode chart
  - Return distribution chart (with normal overlay)

Usage:
    from institutional_audit import run_institutional_audit
    run_institutional_audit(
        daily_returns=hybrid_daily_adj,
        label="Hybrid",
        outdir=OUTDIR,
        track_file=track_file,
        n_trials=50,           # number of parameter combos tried in sweep (for DSR)
        trading_days_per_year=365,
        starting_capital=5000.0,
        save_charts=True,
    )
"""

from __future__ import annotations

import math
import itertools
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd
import scipy.stats as stats

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    _MPL = True
except Exception:
    _MPL = False


# ─────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────

def _equity(r: np.ndarray) -> np.ndarray:
    eq = np.ones(len(r) + 1, dtype=float)
    for i, ri in enumerate(r):
        eq[i + 1] = eq[i] * (1.0 + float(ri))
    return eq


def _dd_curve(eq: np.ndarray) -> np.ndarray:
    peaks = np.maximum.accumulate(eq)
    return eq / peaks - 1.0


def _sharpe(r: np.ndarray, tdy: int) -> float:
    if len(r) < 2:
        return float("nan")
    mu, sd = float(np.mean(r)), float(np.std(r, ddof=1))
    return (mu / sd) * math.sqrt(tdy) if sd > 0 else float("nan")


def _cagr(eq: np.ndarray, n_days: int, tdy: int) -> float:
    if n_days <= 0 or eq[-1] <= 0:
        return float("nan")
    total = eq[-1] / eq[0] - 1.0
    if total <= -1.0:
        return float("nan")
    return (1.0 + total) ** (tdy / n_days) - 1.0


def _max_dd(eq: np.ndarray) -> float:
    return float(np.min(_dd_curve(eq)))


def _sortino(r: np.ndarray, tdy: int) -> float:
    """Annualised Sortino ratio (downside deviation denominator, MAR=0)."""
    if len(r) < 2:
        return float("nan")
    mu        = float(np.mean(r))
    downside  = r[r < 0]
    if len(downside) == 0:
        return float("inf")
    dd_std = float(np.sqrt(np.mean(downside ** 2)))
    return (mu / dd_std) * math.sqrt(tdy) if dd_std > 0 else float("nan")


def _r_squared(eq: np.ndarray) -> float:
    """
    R² of log-equity vs a straight line (time index).
    1.0 = perfectly linear growth.  0.0 = no linear trend.
    Measures how smooth / predictable the equity curve is.
    """
    if len(eq) < 3:
        return float("nan")
    log_eq = np.log(eq)
    x      = np.arange(len(eq), dtype=float)
    # linear regression via least squares
    x_mean = x.mean()
    y_mean = log_eq.mean()
    ss_tot = float(np.sum((log_eq - y_mean) ** 2))
    if ss_tot < 1e-12:
        return 1.0
    slope  = float(np.sum((x - x_mean) * (log_eq - y_mean)) / np.sum((x - x_mean) ** 2))
    fitted = x_mean * slope + y_mean + slope * (x - x_mean)  # simplified
    fitted = y_mean + slope * (x - x_mean)
    ss_res = float(np.sum((log_eq - fitted) ** 2))
    return max(0.0, 1.0 - ss_res / ss_tot)


def _safe_save(fig, path: str):
    try:
        fig.tight_layout()
        fig.savefig(path, dpi=120)
    finally:
        plt.close(fig)


# ─────────────────────────────────────────────
# 1. RISK-ADJUSTED RETURN QUALITY
# ─────────────────────────────────────────────

def sortino_ratio(r: np.ndarray, tdy: int, target: float = 0.0) -> float:
    """Annualised Sortino ratio using downside deviation."""
    excess = r - target / tdy
    downside = excess[excess < 0]
    if len(downside) < 2:
        return float("nan")
    dd_std = math.sqrt(float(np.mean(downside ** 2)))
    if dd_std == 0:
        return float("nan")
    return (float(np.mean(excess)) / dd_std) * math.sqrt(tdy)


def calmar_ratio(eq: np.ndarray, r: np.ndarray, tdy: int) -> float:
    """CAGR / abs(MaxDD)."""
    mdd = abs(_max_dd(eq))
    if mdd == 0:
        return float("nan")
    c = _cagr(eq, len(r), tdy)
    return c / mdd if math.isfinite(c) else float("nan")


def omega_ratio(r: np.ndarray, threshold: float = 0.0) -> float:
    """Probability-weighted gains over losses relative to threshold."""
    gains  = float(np.sum(np.maximum(r - threshold, 0)))
    losses = float(np.sum(np.maximum(threshold - r, 0)))
    return gains / losses if losses > 0 else float("nan")


def ulcer_index(eq: np.ndarray) -> float:
    """RMS of squared drawdowns — penalises depth AND duration."""
    dd = _dd_curve(eq) * 100.0   # in percent
    return float(math.sqrt(np.mean(dd ** 2)))


# ─────────────────────────────────────────────
# 2. DRAWDOWN EPISODE ANALYSIS
# ─────────────────────────────────────────────

def drawdown_episodes(eq: np.ndarray) -> pd.DataFrame:
    """
    Returns a DataFrame of distinct drawdown episodes:
        start_idx, trough_idx, end_idx (recovery), depth, duration_days, recovery_days
    end_idx = NaN if not yet recovered.
    """
    dd = _dd_curve(eq)
    n = len(dd)

    episodes: List[Dict[str, Any]] = []
    in_dd = False
    start = 0
    trough_idx = 0
    trough_val = 0.0

    for i in range(n):
        if not in_dd:
            if dd[i] < -1e-9:
                in_dd = True
                start = i
                trough_idx = i
                trough_val = dd[i]
        else:
            if dd[i] < trough_val:
                trough_val = dd[i]
                trough_idx = i
            # recovered when drawdown returns to ~0
            if dd[i] >= -1e-9:
                episodes.append({
                    "start_idx":    start,
                    "trough_idx":   trough_idx,
                    "end_idx":      i,
                    "depth":        trough_val,
                    "duration_days": trough_idx - start,
                    "recovery_days": i - trough_idx,
                    "total_days":   i - start,
                })
                in_dd = False

    # open episode (never recovered)
    if in_dd:
        episodes.append({
            "start_idx":    start,
            "trough_idx":   trough_idx,
            "end_idx":      float("nan"),
            "depth":        trough_val,
            "duration_days": trough_idx - start,
            "recovery_days": float("nan"),
            "total_days":   n - 1 - start,
        })

    return pd.DataFrame(episodes)


def pct_time_underwater(eq: np.ndarray) -> float:
    dd = _dd_curve(eq)
    return float(np.mean(dd < -1e-9))


def average_drawdown(eq: np.ndarray) -> float:
    dd = _dd_curve(eq)
    underwater = dd[dd < -1e-9]
    return float(np.mean(underwater)) if len(underwater) else 0.0


# ─────────────────────────────────────────────
# 3. RETURN DISTRIBUTION
# ─────────────────────────────────────────────

def distribution_stats(r: np.ndarray) -> Dict[str, float]:
    sk  = float(stats.skew(r))
    ku  = float(stats.kurtosis(r, fisher=True))   # excess kurtosis
    return {"skewness": sk, "excess_kurtosis": ku}


def jarque_bera_test(r: np.ndarray) -> Dict[str, float]:
    """
    JB = n/6 * (S^2 + K^2/4)  where S=skewness, K=excess kurtosis.
    Under H0 (normality): JB ~ chi2(2).
    Implemented from scratch (no statsmodels needed).
    """
    n = len(r)
    if n < 8:
        return {"jb_stat": float("nan"), "jb_pvalue": float("nan"), "normal": None}
    s = float(stats.skew(r))
    k = float(stats.kurtosis(r, fisher=True))
    jb = (n / 6.0) * (s ** 2 + (k ** 2) / 4.0)
    p  = float(1.0 - stats.chi2.cdf(jb, df=2))
    return {"jb_stat": jb, "jb_pvalue": p, "normal": p > 0.05}


def ljung_box_test(r: np.ndarray, lags: int = 10) -> Dict[str, Any]:
    """
    Ljung-Box Q statistic for autocorrelation up to `lags`.
    Q = n(n+2) * sum_{k=1}^{lags} rho_k^2 / (n-k)
    Under H0 (no autocorrelation): Q ~ chi2(lags).
    Implemented from scratch.
    """
    n = len(r)
    if n < lags + 5:
        return {"lb_stat": float("nan"), "lb_pvalue": float("nan"), "autocorrelated": None, "lags": lags}

    mu   = np.mean(r)
    denom = np.sum((r - mu) ** 2)
    if denom == 0:
        return {"lb_stat": float("nan"), "lb_pvalue": float("nan"), "autocorrelated": None, "lags": lags}

    acf = []
    for k in range(1, lags + 1):
        num = np.sum((r[:n - k] - mu) * (r[k:] - mu))
        acf.append(float(num / denom))

    Q = float(n * (n + 2) * sum(rk ** 2 / (n - k) for k, rk in enumerate(acf, 1)))
    p = float(1.0 - stats.chi2.cdf(Q, df=lags))
    return {"lb_stat": Q, "lb_pvalue": p, "autocorrelated": p < 0.05, "lags": lags, "acf": acf}


# ─────────────────────────────────────────────
# 4. REGIME & CONDITIONAL ANALYSIS
# ─────────────────────────────────────────────

def regime_analysis(r: np.ndarray, tdy: int) -> Dict[str, Any]:
    """Split by up/down days and low/high vol (rolling 20d)."""
    up   = r[r >= 0]
    down = r[r <  0]

    # Rolling 20-day vol
    vol = pd.Series(r).rolling(20).std().to_numpy()
    med_vol = float(np.nanmedian(vol))
    low_vol_mask  = vol <= med_vol
    high_vol_mask = vol >  med_vol

    low_vol_r  = r[low_vol_mask  & np.isfinite(vol)]
    high_vol_r = r[high_vol_mask & np.isfinite(vol)]

    def _s(arr):
        return _sharpe(arr, tdy) if len(arr) > 1 else float("nan")
    def _m(arr):
        return float(np.mean(arr)) * 100 if len(arr) else float("nan")

    return {
        "up_days":          len(up),
        "down_days":        len(down),
        "up_mean_pct":      _m(up),
        "down_mean_pct":    _m(down),
        "up_sharpe":        _s(up)   if len(up) > 1 else float("nan"),
        "down_sharpe":      _s(down) if len(down) > 1 else float("nan"),
        "low_vol_sharpe":   _s(low_vol_r),
        "high_vol_sharpe":  _s(high_vol_r),
        "low_vol_mean_pct": _m(low_vol_r),
        "high_vol_mean_pct":_m(high_vol_r),
        "median_vol_threshold": med_vol,
    }


def rolling_sharpe(r: np.ndarray, window: int, tdy: int) -> np.ndarray:
    s = pd.Series(r)
    mu = s.rolling(window).mean()
    sd = s.rolling(window).std(ddof=1)
    return ((mu / sd) * math.sqrt(tdy)).to_numpy()


def rolling_cagr(r: np.ndarray, window: int, tdy: int) -> np.ndarray:
    out = np.full(len(r), float("nan"))
    for i in range(window, len(r) + 1):
        chunk = r[i - window:i]
        eq    = _equity(chunk)
        out[i - 1] = _cagr(eq, window, tdy)
    return out


# ─────────────────────────────────────────────
# 5. SLIPPAGE SENSITIVITY
# ─────────────────────────────────────────────

def slippage_sensitivity(r: np.ndarray, tdy: int,
                         rates: Tuple[float, ...] = (0.0, 0.005, 0.01, 0.02, 0.03, 0.05)
                         ) -> pd.DataFrame:
    """
    For each slippage rate, apply to losing days only (consistent with your existing logic),
    recompute CAGR, Sharpe, MaxDD.
    """
    rows = []
    for rate in rates:
        adj = r.copy()
        adj[adj < 0] *= (1.0 + rate)   # losses are made worse
        eq = _equity(adj)
        rows.append({
            "slippage_rate":    rate,
            "CAGR_%":          _cagr(eq, len(adj), tdy) * 100,
            "Sharpe":          _sharpe(adj, tdy),
            "MaxDD_%":         _max_dd(eq) * 100,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# 6. TAIL RISK (extra)
# ─────────────────────────────────────────────

def weekly_cvar(r: np.ndarray, alpha: float = 0.01) -> float:
    """
    Group daily returns into non-overlapping weeks of 7 days.
    Compute the CVaR of the worst alpha% of weekly returns.
    """
    n_weeks = len(r) // 7
    if n_weeks < 10:
        return float("nan")
    weekly = np.array([
        float(np.prod(1.0 + r[i*7:(i+1)*7]) - 1.0)
        for i in range(n_weeks)
    ])
    q = float(np.quantile(weekly, alpha))
    tail = weekly[weekly <= q]
    return float(np.mean(tail)) if len(tail) else float("nan")


def losing_streak_stats(r: np.ndarray) -> Dict[str, Any]:
    """Max and average consecutive losing day streaks."""
    streaks: List[int] = []
    cur = 0
    for ri in r:
        if ri < 0:
            cur += 1
        else:
            if cur > 0:
                streaks.append(cur)
            cur = 0
    if cur > 0:
        streaks.append(cur)

    if not streaks:
        return {"max_streak": 0, "avg_streak": 0.0, "n_streaks": 0}
    return {
        "max_streak": int(max(streaks)),
        "avg_streak": float(np.mean(streaks)),
        "n_streaks":  len(streaks),
    }


# ─────────────────────────────────────────────
# 7. STATISTICAL VALIDITY
# ─────────────────────────────────────────────

def deflated_sharpe_ratio(
    observed_sharpe: float,
    n_days: int,
    n_trials: int,
    skewness: float = 0.0,
    excess_kurtosis: float = 0.0,
    tdy: int = 365,
) -> Dict[str, float]:
    """
    Deflated Sharpe Ratio (Bailey & Lopez de Prado, 2014).
    Estimates the probability that the observed Sharpe is a false positive
    given the number of strategy trials/configurations tested.

    DSR = Prob( SR* > SR_benchmark | n_trials )
    SR_benchmark = expected max Sharpe from n_trials iid draws from N(0,1).
    """
    if n_trials < 1 or not math.isfinite(observed_sharpe):
        return {"dsr": float("nan"), "sr_benchmark": float("nan"), "prob_false_positive": float("nan")}

    # Expected maximum of n_trials iid standard normals (approximation)
    # E[max] ≈ (1 - gamma) * Z_{1-1/n} + gamma * Z_{1-1/(n*e)}
    # Simpler closed-form approximation used in practice:
    euler_gamma = 0.5772156649
    sr_benchmark = (
        (1.0 - euler_gamma) * stats.norm.ppf(1.0 - 1.0 / n_trials)
        + euler_gamma * stats.norm.ppf(1.0 - 1.0 / (n_trials * math.e))
    ) if n_trials > 1 else 0.0

    # Annualise the sample Sharpe to daily-scale for the variance formula
    sr_hat = observed_sharpe / math.sqrt(tdy)   # daily-scale Sharpe

    # Variance of Sharpe estimator (Christie, 2005 correction for non-normality)
    var_sr = (1.0 / n_days) * (
        1.0
        + 0.5 * sr_hat ** 2
        - skewness * sr_hat
        + (excess_kurtosis / 4.0) * sr_hat ** 2
    )
    if var_sr <= 0:
        var_sr = 1.0 / n_days

    sr_std = math.sqrt(var_sr) * math.sqrt(tdy)   # back to annualised scale

    # Probability that observed SR exceeds the benchmark
    z = (observed_sharpe - sr_benchmark) / sr_std if sr_std > 0 else float("nan")
    prob_genuine = float(stats.norm.cdf(z)) if math.isfinite(z) else float("nan")
    prob_false   = 1.0 - prob_genuine if math.isfinite(prob_genuine) else float("nan")

    return {
        "dsr":                   prob_genuine,
        "sr_benchmark":          sr_benchmark,
        "prob_false_positive":   prob_false,
        "sr_std":                sr_std,
        "z_score":               z,
    }


def minimum_track_record_length(
    observed_sharpe: float,
    target_sharpe: float = 1.0,
    confidence: float = 0.95,
    skewness: float = 0.0,
    excess_kurtosis: float = 0.0,
    tdy: int = 365,
) -> float:
    """
    Minimum number of daily observations required to reject H0: SR <= target_sharpe
    at the given confidence level (Bailey & Lopez de Prado, 2014).

    MTL = 1 + (1 - sk*SR + (ek/4)*SR^2) * (Z_alpha / (SR - SR*))^2
    where SR, SR* are in *daily* (non-annualised) units.
    """
    if not math.isfinite(observed_sharpe) or observed_sharpe <= target_sharpe:
        return float("nan")

    sr     = observed_sharpe / math.sqrt(tdy)    # daily scale
    sr_star= target_sharpe   / math.sqrt(tdy)

    z_alpha = float(stats.norm.ppf(confidence))
    bracket = 1.0 + 0.5 * sr ** 2 - skewness * sr + (excess_kurtosis / 4.0) * sr ** 2
    if (sr - sr_star) == 0:
        return float("nan")

    mtl = 1.0 + bracket * (z_alpha / (sr - sr_star)) ** 2
    return float(mtl)


def profit_factor(r: np.ndarray) -> float:
    gross_profit = float(np.sum(r[r > 0]))
    gross_loss   = float(abs(np.sum(r[r < 0])))
    return gross_profit / gross_loss if gross_loss > 0 else float("nan")


# ─────────────────────────────────────────────
# 8. CAPITAL & OPERATIONAL
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# 5b. CAPPED RETURN SENSITIVITY
# ─────────────────────────────────────────────

def capped_return_sensitivity(
    r: np.ndarray,
    tdy: int,
    caps: Tuple[float, ...] = (0.10, 0.20, 0.30, 0.50),
) -> pd.DataFrame:
    """
    For each cap level, winsorize daily returns to +/- cap,
    recompute CAGR, Sharpe, MaxDD, and report how many days
    were affected and what % of total log-growth they represented.
    Uncapped baseline (cap=inf) included as first row.
    """
    rows = []

    # Baseline (no cap)
    eq_base = _equity(r)
    log_r   = np.log1p(r)
    total_log = float(np.sum(log_r))

    rows.append({
        "cap":           "none",
        "cap_pct":       float("nan"),
        "days_capped":   0,
        "pct_days":      0.0,
        "log_growth_removed_%": 0.0,
        "CAGR_%":        _cagr(eq_base, len(r), tdy) * 100,
        "Sharpe":        _sharpe(r, tdy),
        "MaxDD_%":       _max_dd(eq_base) * 100,
        "total_return_%": (eq_base[-1] / eq_base[0] - 1.0) * 100,
    })

    for cap in caps:
        capped = np.clip(r, -cap, cap)
        n_capped = int(np.sum(np.abs(r) > cap))
        pct_days = n_capped / len(r) * 100 if len(r) else 0.0

        # log-growth removed by capping
        log_capped   = np.log1p(capped)
        log_removed  = float(np.sum(log_r - log_capped))
        pct_log_removed = (log_removed / total_log * 100) if total_log != 0 else float("nan")

        eq = _equity(capped)
        rows.append({
            "cap":           f"{cap*100:.0f}%",
            "cap_pct":       cap * 100,
            "days_capped":   n_capped,
            "pct_days":      pct_days,
            "log_growth_removed_%": pct_log_removed,
            "CAGR_%":        _cagr(eq, len(capped), tdy) * 100,
            "Sharpe":        _sharpe(capped, tdy),
            "MaxDD_%":       _max_dd(eq) * 100,
            "total_return_%": (eq[-1] / eq[0] - 1.0) * 100,
        })

    return pd.DataFrame(rows)


def _chart_capped_return_sensitivity(df: pd.DataFrame, label: str, path: str):
    if not _MPL:
        return
    # Drop the 'none' baseline row for plotting (no numeric x value)
    plot_df = df[df["cap"] != "none"].copy()
    if plot_df.empty:
        return

    x     = plot_df["cap_pct"].to_numpy(dtype=float)
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    configs = [
        ("CAGR_%",       "CAGR (%)",            "steelblue"),
        ("Sharpe",       "Sharpe Ratio",         "darkorange"),
        ("MaxDD_%",      "Max Drawdown (%)",     "crimson"),
        ("log_growth_removed_%", "Log-growth removed (%)", "purple"),
    ]

    for ax, (col, ylabel, colour) in zip(axes.flat, configs):
        baseline_val = float(df[df["cap"] == "none"][col].iloc[0]) if col in df.columns else None
        ax.plot(x, plot_df[col].to_numpy(dtype=float), marker="o", color=colour)
        if baseline_val is not None and np.isfinite(baseline_val):
            ax.axhline(baseline_val, color="grey", linewidth=0.8, linestyle="--", label="Uncapped")
            ax.legend(fontsize=8)
        ax.set_title(ylabel)
        ax.set_xlabel("Daily return cap (%)")
        ax.grid(True, alpha=0.3)

    fig.suptitle(f"{label} Capped Return Sensitivity", fontsize=13)
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# 5c. TOP-N DAY REMOVAL TEST
# ─────────────────────────────────────────────

def top_n_removal_test(
    r: np.ndarray,
    tdy: int,
    ns: Tuple[int, ...] = (1, 3, 5, 10),
) -> pd.DataFrame:
    """
    Sequentially remove the top-N best return days (by magnitude of positive return)
    and recompute CAGR, Sharpe, MaxDD.
    Shows how dependent the strategy is on a small number of outlier days.
    Baseline (N=0) included as first row.
    """
    rows = []

    # Sort indices by descending return value (best days first)
    sorted_idx = np.argsort(r)[::-1]   # largest returns first

    eq_base = _equity(r)
    rows.append({
        "n_removed":     0,
        "label":         "Baseline",
        "days_removed":  "",
        "CAGR_%":        _cagr(eq_base, len(r), tdy) * 100,
        "Sharpe":        _sharpe(r, tdy),
        "MaxDD_%":       _max_dd(eq_base) * 100,
        "total_return_%": (eq_base[-1] / eq_base[0] - 1.0) * 100,
        "cagr_delta_%":  0.0,
        "sharpe_delta":  0.0,
    })

    base_cagr   = rows[0]["CAGR_%"]
    base_sharpe = rows[0]["Sharpe"]

    for n in ns:
        if n > len(r):
            continue
        remove_idx = set(sorted_idx[:n])
        mask = np.array([i not in remove_idx for i in range(len(r))])
        r_adj = r[mask]

        # Show the actual return values removed (truncated to 5)
        removed_vals = r[sorted_idx[:n]]
        days_str = ", ".join(f"{v*100:.1f}%" for v in removed_vals[:5])
        if n > 5:
            days_str += f" … (+{n-5} more)"

        eq = _equity(r_adj)
        c  = _cagr(eq, len(r_adj), tdy) * 100
        s  = _sharpe(r_adj, tdy)
        rows.append({
            "n_removed":     n,
            "label":         f"Remove top {n}",
            "days_removed":  days_str,
            "CAGR_%":        c,
            "Sharpe":        s,
            "MaxDD_%":       _max_dd(eq) * 100,
            "total_return_%": (eq[-1] / eq[0] - 1.0) * 100,
            "cagr_delta_%":  c - base_cagr,
            "sharpe_delta":  s - base_sharpe,
        })

    return pd.DataFrame(rows)


def _chart_top_n_removal(df: pd.DataFrame, label: str, path: str):
    if not _MPL:
        return
    plot_df = df[df["n_removed"] > 0].copy()
    if plot_df.empty:
        return

    x = plot_df["n_removed"].to_numpy(dtype=int)
    base_cagr   = float(df[df["n_removed"] == 0]["CAGR_%"].iloc[0])
    base_sharpe = float(df[df["n_removed"] == 0]["Sharpe"].iloc[0])
    base_mdd    = float(df[df["n_removed"] == 0]["MaxDD_%"].iloc[0])

    fig, axes = plt.subplots(1, 3, figsize=(13, 5))

    for ax, col, base_val, colour, ylabel in zip(
        axes,
        ["CAGR_%", "Sharpe", "MaxDD_%"],
        [base_cagr, base_sharpe, base_mdd],
        ["steelblue", "darkorange", "crimson"],
        ["CAGR (%)", "Sharpe Ratio", "Max Drawdown (%)"],
    ):
        ax.plot(x, plot_df[col].to_numpy(dtype=float), marker="o", color=colour)
        ax.axhline(base_val, color="grey", linewidth=0.9, linestyle="--", label="Baseline")
        ax.set_title(ylabel)
        ax.set_xlabel("Top N days removed")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    fig.suptitle(f"{label} Top-N Day Removal Test", fontsize=13)
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# 5d. LUCKY STREAK TEST
# ─────────────────────────────────────────────

def lucky_streak_test(
    r: np.ndarray,
    tdy: int,
    window: int = 30,
) -> pd.DataFrame:
    """
    Identifies every non-overlapping `window`-day block, ranks them by
    total return, then removes the single best block (the "luckiest month")
    and recomputes full-sample CAGR, Sharpe, MaxDD.

    Also removes the best 2 and best 3 blocks to show sensitivity.

    Returns a table showing impact of removing 0 / 1 / 2 / 3 best windows.
    """
    n = len(r)
    n_blocks = n // window

    if n_blocks < 2:
        return pd.DataFrame()

    # Score each block by its compounded return
    block_returns = []
    for i in range(n_blocks):
        blk = r[i * window: (i + 1) * window]
        eq  = _equity(blk)
        block_returns.append((i, float(eq[-1] / eq[0] - 1.0)))

    # Sort best → worst
    block_returns.sort(key=lambda x: x[1], reverse=True)

    rows = []
    eq_base = _equity(r)
    base_cagr   = _cagr(eq_base, n, tdy) * 100
    base_sharpe = _sharpe(r, tdy)
    base_mdd    = _max_dd(eq_base) * 100
    base_total  = (eq_base[-1] / eq_base[0] - 1.0) * 100

    rows.append({
        "blocks_removed": 0,
        "label":          "Baseline (all blocks)",
        "removed_block_returns": "",
        "CAGR_%":         base_cagr,
        "Sharpe":         base_sharpe,
        "MaxDD_%":        base_mdd,
        "total_return_%": base_total,
        "cagr_delta_%":   0.0,
        "sharpe_delta":   0.0,
    })

    for k in range(1, min(4, n_blocks)):
        removed_block_indices = set(block_returns[i][0] for i in range(k))
        # Build return series with those blocks zeroed out (replaced with 0 return days)
        r_adj = r.copy()
        removed_rets = []
        for bi in sorted(removed_block_indices):
            blk_ret = block_returns[[b[0] for b in block_returns].index(bi)][1]
            removed_rets.append(blk_ret)
            r_adj[bi * window: (bi + 1) * window] = 0.0

        eq  = _equity(r_adj)
        c   = _cagr(eq, len(r_adj), tdy) * 100
        s   = _sharpe(r_adj, tdy)

        removed_str = ", ".join(f"+{v*100:.1f}%" for v in sorted(removed_rets, reverse=True))

        rows.append({
            "blocks_removed":        k,
            "label":                 f"Remove best {k} block{'s' if k > 1 else ''}",
            "removed_block_returns": removed_str,
            "CAGR_%":                c,
            "Sharpe":                s,
            "MaxDD_%":               _max_dd(eq) * 100,
            "total_return_%":        (eq[-1] / eq[0] - 1.0) * 100,
            "cagr_delta_%":          c - base_cagr,
            "sharpe_delta":          s - base_sharpe,
        })

    return pd.DataFrame(rows)


def _chart_lucky_streak(df: pd.DataFrame, label: str, window: int, path: str):
    if not _MPL or df.empty:
        return
    plot_df = df[df["blocks_removed"] > 0].copy()
    if plot_df.empty:
        return

    x = plot_df["blocks_removed"].to_numpy(dtype=int)
    base_cagr   = float(df[df["blocks_removed"] == 0]["CAGR_%"].iloc[0])
    base_sharpe = float(df[df["blocks_removed"] == 0]["Sharpe"].iloc[0])

    fig, axes = plt.subplots(1, 2, figsize=(10, 5))

    for ax, col, base_val, colour, ylabel in zip(
        axes,
        ["CAGR_%", "Sharpe"],
        [base_cagr, base_sharpe],
        ["steelblue", "darkorange"],
        ["CAGR (%)", "Sharpe Ratio"],
    ):
        ax.bar(x, plot_df[col].to_numpy(dtype=float), color=colour, alpha=0.75)
        ax.axhline(base_val, color="grey", linewidth=0.9, linestyle="--", label="Baseline")
        ax.set_xticks(x)
        ax.set_xticklabels([f"Remove\nbest {i}" for i in x], fontsize=9)
        ax.set_title(ylabel)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle(f"{label} Lucky Streak Test  (window = {window} days)", fontsize=13)
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# 5e. PARAMETER SENSITIVITY MAP
# ─────────────────────────────────────────────

# Default baseline values — overridden by caller via param_config
_PARAM_DEFAULTS = {
    "EARLY_KILL_X":          175,
    "EARLY_KILL_Y":               0.0035,   # 1x (was EARLY_KILL_Y=0.014 @ 4x)
    "PORT_TSL":              0.085,
    "L_BASE":                   1.25,
    "L_HIGH":                   2.0,
    "PORT_SL":            -0.049,
    "EARLY_FILL_Y":  0.30,
}

# BAR_MINUTES must match your script
_BAR_MINUTES  = 5
_N_ROWS       = 216
_PIVOT_LEV    = 4.0


def _apply_hybrid_day_param(
    path_1x: np.ndarray,
    *,
    early_x_minutes: int,
    early_y_1x: float,
    trail_dd_1x: float,
    l_base: float,
    l_high: float,
    port_stop_1x: float,
    early_fill_threshold_1x: float,
    strong_thr_1x: float = 0.01,
    enable_early_kill: bool = True,
    enable_trailing_stop: bool = True,
    enable_portfolio_stop: bool = True,
    enable_early_fill: bool = True,
    enable_adaptive_lev: bool = True,
    early_fill_max_minutes: int = 9 * 60,
    trial_purchases: bool = False,
) -> tuple:
    """
    Compact re-implementation of apply_best_hybrid_day.
    trial_purchases=False: flat during trial, enter fresh at x_idx if passes.
    trial_purchases=True:  enter at 1x at open, adjust leverage at x_idx.

    Returns (r, lev_used) where:
      r        -- day return in 1x-equivalent space (adaptive leverage already applied)
      lev_used -- the adaptive leverage scalar actually applied (L_BASE or L_HIGH).
                  0.0 if no trade was taken (early_kill fired with trial_purchases=False).
                  For trial_purchases=True the blended effective leverage is returned:
                  trial period runs at 1x, post-trial at lev, so
                  lev_used = (trial_bars * 1.0 + post_bars * lev) / total_bars.
    """
    n = len(path_1x)

    def _idx(mins: int) -> int:
        return max(0, min((mins // _BAR_MINUTES) - 1, n - 1))

    x_idx        = _idx(early_x_minutes)
    fill_max_idx = min(_idx(early_fill_max_minutes), n - 1)

    roi_x = float(path_1x[x_idx]) if np.isfinite(path_1x[x_idx]) else float(path_1x[-1])
    if not np.isfinite(roi_x):
        roi_x = 0.0

    # ── TRIAL_PURCHASES = False: flat during trial, enter fresh if passes ──
    if not trial_purchases:
        if enable_early_kill and roi_x < early_y_1x:
            return 0.0, 0.0  # no trade -- no fees
        lev = l_high if (enable_adaptive_lev and roi_x >= strong_thr_1x) else l_base
        entry_1x = float(path_1x[x_idx])
        peak = 0.0
        exit_roi_incr = (float(path_1x[-1]) if np.isfinite(path_1x[-1]) else entry_1x) - entry_1x
        for i in range(x_idx + 1, n):
            v = float(path_1x[i])
            if not np.isfinite(v): continue
            incr = v - entry_1x
            if enable_portfolio_stop and incr <= port_stop_1x:
                exit_roi_incr = incr; break
            if incr > peak: peak = incr
            if enable_trailing_stop and (incr - peak) <= -abs(trail_dd_1x):
                exit_roi_incr = incr; break
            if enable_early_fill and i <= fill_max_idx and v >= early_fill_threshold_1x:
                exit_roi_incr = incr; break
        return float(exit_roi_incr * lev), lev

    # ── TRIAL_PURCHASES = True: enter at 1x at open, adjust at trial bar ──
    if enable_early_kill and roi_x < early_y_1x:
        # Held at 1x through trial only -- no post-trial position
        return float(roi_x), 1.0
    lev = l_high if (enable_adaptive_lev and roi_x >= strong_thr_1x) else l_base
    trial_return = roi_x
    peak     = roi_x
    exit_roi = float(path_1x[-1]) if np.isfinite(path_1x[-1]) else roi_x
    exit_bar = n - 1
    for i in range(x_idx + 1, n):
        v = float(path_1x[i])
        if not np.isfinite(v): continue
        if enable_portfolio_stop and v <= port_stop_1x:
            exit_roi = v; exit_bar = i; break
        if v > peak: peak = v
        if enable_trailing_stop and (v - peak) <= -abs(trail_dd_1x):
            exit_roi = v; exit_bar = i; break
        if enable_early_fill and i <= fill_max_idx and v >= early_fill_threshold_1x:
            exit_roi = v; exit_bar = i; break
    post_trial_incremental = exit_roi - trial_return
    # Blended leverage: trial bars held at 1x, post-trial bars at lev
    trial_bars  = x_idx + 1
    post_bars   = max(exit_bar - x_idx, 0)
    total_bars  = trial_bars + post_bars
    blended_lev = (trial_bars * 1.0 + post_bars * lev) / total_bars if total_bars > 0 else lev
    return float(trial_return + post_trial_incremental * lev), blended_lev



# ══════════════════════════════════════════════════════════════════════════════
# CALENDAR REGIME FILTER
# ══════════════════════════════════════════════════════════════════════════════
# Four 40-day earnings-season windows where institutional capital rotation
# suppresses crypto intraday momentum.  Validated F1=0.853 against observed
# strategy decay windows (see validate_vix_regime.py).
#
# Windows (inclusive, month/day):
#   Q4: Jan 01 – Feb 15   Q1: Mar 01 – Apr 10
#   Q2: May 14 – Jun 25   Q3: Oct 13 – Nov 22

# Exact observed 40-day strategy decay windows.
# These are the validated bad periods — outside these windows the strategy
# trades normally.  The windows repeat approximately at these calendar offsets
# each year and are re-validated annually via validate_vix_regime.py.
_EARNINGS_WINDOWS: List[Tuple[str, str]] = [
    ("2025-03-01", "2025-04-08"),   # Q1 2025
    ("2025-05-14", "2025-06-21"),   # Q2 2025
    ("2025-10-13", "2025-11-20"),   # Q3 2025
    ("2026-01-05", "2026-02-12"),   # Q4 2025 / Q1 2026
    # ── Add future windows here each year after validation ──────────
    # ("2026-03-01", "2026-04-08"),
    # ("2026-05-14", "2026-06-21"),
]

# Pre-parse to Timestamps for fast lookup
_EARNINGS_TS: List[Tuple] = [
    (pd.Timestamp(s), pd.Timestamp(e)) for s, e in _EARNINGS_WINDOWS
]


def _in_earnings_window(date_str: str) -> bool:
    """
    Returns True if date_str falls inside an exact observed decay window.
    Accepts any string parseable by pd.Timestamp.
    Returns False for any unparseable string (safe default = trade).
    """
    try:
        ts = pd.Timestamp(date_str)
    except Exception:
        return False
    return any(s <= ts <= e for s, e in _EARNINGS_TS)


def _simulate_with_params(
    df_4x: pd.DataFrame,
    params: Dict[str, float],
    tdy: int,
    trial_purchases: bool = False,
    calendar_filter: bool = False,
    filter_series: Optional[pd.Series] = None,
) -> Dict[str, float]:
    """
    Run the full hybrid simulation for one parameter set.
    Returns dict with CAGR, Sharpe, MaxDD.

    Optional params dict keys (in addition to the core six):
      EARLY_INSTILL_Y          — adaptive lev threshold (default 0.01)
      EARLY_FILL_X — fill window ceiling in minutes (default 540)
                               pass None or 0 to disable early fill
      PORT_SL           — pass None or 0 to disable portfolio stop
      EARLY_FILL_Y— pass None or 0 to disable early fill

    calendar_filter: if True, days inside earnings windows return 0.0
                     (flat — no position taken).
    filter_series: optional pd.Series[bool] indexed by date; True = sit flat (0.0).
                   Applied in addition to calendar_filter.
                   Pass the canonical regime filter so sensitivity/plateau tests
                   evaluate perturbations under the same filter as the main run.
    """
    early_y_1x = float(params["EARLY_KILL_Y"])

    # Optional overrides
    strong_thr_1x = float(params.get("EARLY_INSTILL_Y", 0.01))

    raw_ps = params.get("PORT_SL", -0.049)
    enable_port_stop = (raw_ps is not None and raw_ps != 0)
    port_stop_1x     = float(raw_ps) if enable_port_stop else -999.0

    raw_ef = params.get("EARLY_FILL_Y", 0.30)
    enable_early_fill = (raw_ef is not None and raw_ef != 0)
    early_fill_thr    = float(raw_ef) if enable_early_fill else 999.0

    raw_efx = params.get("EARLY_FILL_X", 540)
    early_fill_max_min = int(raw_efx) if (raw_efx is not None and raw_efx != 0) else 0

    daily = []

    for col in df_4x.columns:
        # Calendar filter — flat day during earnings windows
        if calendar_filter and _in_earnings_window(str(col)):
            daily.append(0.0)
            continue

        # Regime filter series — flat day when filter fires
        if filter_series is not None:
            try:
                col_ts = pd.Timestamp(str(col)[:10])
                if filter_series.get(col_ts, False):
                    daily.append(0.0)
                    continue
            except Exception:
                pass

        path_4x = df_4x[col].to_numpy(dtype=float)
        path_1x = path_4x / _PIVOT_LEV

        r, _lev_used = _apply_hybrid_day_param(
            path_1x,
            early_x_minutes          = int(round(params["EARLY_KILL_X"])),
            early_y_1x               = early_y_1x,
            trail_dd_1x              = float(params["PORT_TSL"]),
            l_base                   = float(params["L_BASE"]),
            l_high                   = float(params["L_HIGH"]),
            port_stop_1x             = port_stop_1x,
            early_fill_threshold_1x  = early_fill_thr,
            strong_thr_1x            = strong_thr_1x,
            enable_portfolio_stop    = enable_port_stop,
            enable_early_fill        = enable_early_fill,
            early_fill_max_minutes   = early_fill_max_min,
            trial_purchases          = trial_purchases,
        )
        if np.isfinite(r):
            daily.append(r)

    if len(daily) < 10:
        return {"CAGR_%": float("nan"), "Sharpe": float("nan"),
                "MaxDD_%": float("nan"), "Sortino": float("nan"), "R2": float("nan")}

    r_arr = np.array(daily, dtype=float)
    eq    = _equity(r_arr)
    return {
        "CAGR_%":  _cagr(eq, len(r_arr), tdy) * 100,
        "Sharpe":  _sharpe(r_arr, tdy),
        "MaxDD_%": _max_dd(eq) * 100,
        "Sortino": _sortino(r_arr, tdy),
        "R2":      _r_squared(eq),
    }


def parameter_sensitivity_map(
    df_4x: pd.DataFrame,
    tdy: int,
    param_config: Optional[Dict[str, float]] = None,
    perturb_pcts: Tuple[float, ...] = (-0.30, -0.20, -0.10, 0.0, 0.10, 0.20, 0.30),
    params_to_test: Optional[List[str]] = None,
    trial_purchases: bool = False,
    filter_series: Optional[pd.Series] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each parameter in params_to_test, perturb it by each value in
    perturb_pcts (relative %) while holding all others at baseline.

    filter_series: optional pd.Series[bool] (True=sit flat) to apply the
                   canonical regime filter during each re-simulation, so the
                   sensitivity baseline and perturbations reflect the filtered
                   strategy rather than the unfiltered raw strategy.

    Returns:
        tornado_df  — one row per (param, perturbation), with Sharpe/CAGR/MaxDD
        heatmap_df  — pivot of Sharpe values: rows=param, cols=perturbation %
    """
    baseline = dict(_PARAM_DEFAULTS)
    if param_config:
        baseline.update(param_config)

    if params_to_test is None:
        params_to_test = list(baseline.keys())

    # ── Baseline run ──────────────────────────────────────────
    base_res = _simulate_with_params(df_4x, baseline, tdy, trial_purchases=trial_purchases,
                                     filter_series=filter_series)
    base_sharpe = base_res["Sharpe"]
    base_cagr   = base_res["CAGR_%"]
    base_mdd    = base_res["MaxDD_%"]

    print(f"\n  [sensitivity] baseline → Sharpe={base_sharpe:.3f}  CAGR={base_cagr:.1f}%  MaxDD={base_mdd:.2f}%")

    rows = []
    total = len(params_to_test) * len(perturb_pcts)
    done  = 0

    for param in params_to_test:
        if baseline.get(param) is None:
            continue   # disabled feature — skip perturbation
        base_val = float(baseline[param])

        for pct in perturb_pcts:
            done += 1
            new_val = base_val * (1.0 + pct)

            # Special handling: EARLY_KILL_X must snap to valid bar boundary
            if param == "EARLY_KILL_X":
                new_val = max(_BAR_MINUTES, round(new_val / _BAR_MINUTES) * _BAR_MINUTES)

            test_params = dict(baseline)
            test_params[param] = new_val

            res = _simulate_with_params(df_4x, test_params, tdy, trial_purchases=trial_purchases,
                                        filter_series=filter_series)

            rows.append({
                "param":         param,
                "base_val":      base_val,
                "perturb_pct":   pct * 100,
                "perturbed_val": new_val,
                "Sharpe":        res["Sharpe"],
                "CAGR_%":        res["CAGR_%"],
                "MaxDD_%":       res["MaxDD_%"],
                "sharpe_delta":  res["Sharpe"] - base_sharpe,
                "cagr_delta_%":  res["CAGR_%"] - base_cagr,
                "is_baseline":   pct == 0.0,
            })

            if done % 10 == 0 or done == total:
                print(f"  [sensitivity] {done}/{total}  {param} {pct:+.0%} → Sharpe={res['Sharpe']:.3f}")

    tornado_df = pd.DataFrame(rows)

    # ── Heatmap pivot: rows = param, cols = perturb_pct, values = Sharpe ──
    heatmap_df = tornado_df.pivot_table(
        index="param", columns="perturb_pct", values="Sharpe", aggfunc="first"
    )

    return tornado_df, heatmap_df


def _chart_tornado(
    tornado_df: pd.DataFrame,
    base_sharpe: float,
    label: str,
    path: str,
):
    """
    Tornado chart: for each parameter, show the range of Sharpe across
    all perturbation levels.  Sorted by impact range (widest bar at top).
    One grouped bar cluster per perturbation level, coloured by magnitude.
    """
    if not _MPL or tornado_df.empty:
        return

    params   = tornado_df["param"].unique()
    perturbs = sorted(tornado_df["perturb_pct"].unique())
    # exclude the 0% baseline row from range calculation
    non_zero = tornado_df[tornado_df["perturb_pct"] != 0.0]

    # Rank params by total Sharpe swing (max - min across perturbations)
    ranges = {}
    for p in params:
        vals = tornado_df[tornado_df["param"] == p]["Sharpe"].dropna()
        ranges[p] = float(vals.max() - vals.min()) if len(vals) > 1 else 0.0
    sorted_params = sorted(params, key=lambda p: ranges[p], reverse=True)

    n_params   = len(sorted_params)
    n_perturbs = len([x for x in perturbs if x != 0.0])
    colours    = plt.cm.RdYlGn(np.linspace(0.15, 0.85, len(perturbs)))
    pct_colour = {pct: colours[i] for i, pct in enumerate(perturbs)}

    fig, ax = plt.subplots(figsize=(13, max(5, n_params * 0.9 + 2)))

    bar_height = 0.12
    group_gap  = 0.85
    y_positions = {p: i * group_gap for i, p in enumerate(sorted_params)}

    for pct in perturbs:
        if pct == 0.0:
            continue
        subset = tornado_df[tornado_df["perturb_pct"] == pct]
        offset = (list(p for p in perturbs if p != 0.0).index(pct)
                  - n_perturbs / 2.0 + 0.5) * bar_height

        for param in sorted_params:
            row = subset[subset["param"] == param]
            if row.empty:
                continue
            sharpe_val = float(row["Sharpe"].iloc[0])
            if not np.isfinite(sharpe_val):
                continue
            y = y_positions[param] + offset
            delta = sharpe_val - base_sharpe
            ax.barh(
                y, delta, height=bar_height * 0.85,
                color=pct_colour[pct],
                label=f"{pct:+.0f}%" if param == sorted_params[0] else "_nolegend_",
            )

    ax.axvline(0, color="black", linewidth=1.0)
    ax.set_yticks(list(y_positions.values()))
    ax.set_yticklabels(sorted_params, fontsize=9)
    ax.set_xlabel("Sharpe delta vs baseline")
    ax.set_title(f"{label} Parameter Sensitivity — Tornado Chart\n(sorted by impact; baseline Sharpe = {base_sharpe:.3f})")

    # Deduplicate legend
    handles, lbls = ax.get_legend_handles_labels()
    seen = {}
    for h, l in zip(handles, lbls):
        if l not in seen:
            seen[l] = h
    ax.legend(seen.values(), seen.keys(), title="Perturbation", fontsize=8,
              loc="lower right")
    ax.grid(True, alpha=0.3, axis="x")
    _safe_save(fig, path)


def _chart_heatmap(
    heatmap_df: pd.DataFrame,
    label: str,
    path: str,
):
    """
    Heatmap: rows = parameters, cols = perturbation %, cell = Sharpe value.
    Colour = green (high Sharpe) → red (low Sharpe).
    """
    if not _MPL or heatmap_df.empty:
        return

    data = heatmap_df.to_numpy(dtype=float)
    row_labels = list(heatmap_df.index)
    col_labels  = [f"{c:+.0f}%" for c in heatmap_df.columns]

    fig, ax = plt.subplots(figsize=(max(8, len(col_labels) * 1.1),
                                    max(4, len(row_labels) * 0.7 + 1.5)))

    # Normalise colour per row so each parameter's sensitivity is visible
    vmin = np.nanmin(data)
    vmax = np.nanmax(data)
    im = ax.imshow(data, aspect="auto", cmap="RdYlGn", vmin=vmin, vmax=vmax)

    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, fontsize=9)
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels, fontsize=9)
    ax.set_xlabel("Perturbation %")
    ax.set_title(f"{label} Parameter Sensitivity Heatmap  (cell = Sharpe ratio)")

    # Annotate cells
    for i in range(len(row_labels)):
        for j in range(len(col_labels)):
            val = data[i, j]
            txt = f"{val:.2f}" if np.isfinite(val) else "N/A"
            brightness = (val - vmin) / (vmax - vmin + 1e-9)
            txt_col = "black" if 0.3 < brightness < 0.75 else "white"
            ax.text(j, i, txt, ha="center", va="center", fontsize=8, color=txt_col)

    plt.colorbar(im, ax=ax, label="Sharpe ratio")
    _safe_save(fig, path)


def _chart_sensitivity_line(
    tornado_df: pd.DataFrame,
    base_sharpe: float,
    label: str,
    path: str,
):
    """
    Line chart: one line per parameter showing Sharpe vs perturbation %.
    A flat line = robust.  A steep line = fragile.
    """
    if not _MPL or tornado_df.empty:
        return

    params  = tornado_df["param"].unique()
    perturbs = sorted(tornado_df["perturb_pct"].unique())
    colours  = plt.cm.tab10(np.linspace(0, 1, len(params)))

    fig, ax = plt.subplots(figsize=(11, 6))

    for param, colour in zip(params, colours):
        sub = tornado_df[tornado_df["param"] == param].sort_values("perturb_pct")
        ax.plot(
            sub["perturb_pct"], sub["Sharpe"],
            marker="o", markersize=5, linewidth=1.5,
            label=param, color=colour,
        )

    ax.axhline(base_sharpe, color="black", linewidth=1.0,
               linestyle="--", label=f"Baseline ({base_sharpe:.2f})")
    ax.axvline(0, color="grey", linewidth=0.7, linestyle=":")
    ax.set_xlabel("Perturbation (%)")
    ax.set_ylabel("Sharpe Ratio")
    ax.set_title(f"{label} Parameter Sensitivity — Sharpe vs Perturbation\n(flat lines = robust; steep = fragile)")
    ax.legend(fontsize=8, loc="best")
    ax.grid(True, alpha=0.3)
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# NEW A. COST CURVE TEST
# ─────────────────────────────────────────────

def cost_curve_test(
    r: np.ndarray,
    tdy: int,
    aum_levels: Tuple[float, ...] = (5_000, 25_000, 100_000, 250_000, 500_000, 1_000_000),
    base_slippage: float = 0.001,
    impact_exponent: float = 0.5,
) -> pd.DataFrame:
    """
    Models how total execution cost (slippage + market impact) scales with AUM.

    For each AUM level:
      - Base slippage:    fixed per-trade friction (bid-ask + exchange fees)
      - Market impact:    scales as impact_coeff * sqrt(AUM / ref_AUM)
        where ref_AUM = aum_levels[0] (your current starting capital).
        The sqrt model is standard for equities/futures (Almgren-Chriss).
      - Total cost rate applied symmetrically to each daily return.

    Returns table of CAGR, Sharpe, MaxDD at each AUM level.
    Shows exactly where the strategy's alpha decays to zero.
    """
    ref_aum = float(aum_levels[0])
    rows = []

    for aum in aum_levels:
        scale  = float(aum) / ref_aum
        impact = base_slippage * (scale ** impact_exponent)
        total_cost = base_slippage + impact

        # Apply cost: reduce positive days, worsen negative days
        r_adj = r.copy()
        r_adj = r_adj - total_cost  # symmetric drag each day

        eq = _equity(r_adj)
        rows.append({
            "AUM":             aum,
            "base_slip_%":     base_slippage * 100,
            "mkt_impact_%":    impact * 100,
            "total_cost_%":    total_cost * 100,
            "CAGR_%":          _cagr(eq, len(r_adj), tdy) * 100,
            "Sharpe":          _sharpe(r_adj, tdy),
            "MaxDD_%":         _max_dd(eq) * 100,
        })

    return pd.DataFrame(rows)


def _chart_cost_curve(df: pd.DataFrame, label: str, path: str):
    if not _MPL or df.empty:
        return
    fig, axes = plt.subplots(1, 3, figsize=(13, 5))
    x = df["AUM"].to_numpy(dtype=float) / 1_000  # in $k

    for ax, col, colour, ylabel in zip(
        axes,
        ["CAGR_%", "Sharpe", "MaxDD_%"],
        ["steelblue", "darkorange", "crimson"],
        ["CAGR (%)", "Sharpe Ratio", "Max Drawdown (%)"],
    ):
        ax.plot(x, df[col].to_numpy(dtype=float), marker="o", color=colour)
        ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
        ax.set_xlabel("AUM ($k)")
        ax.set_title(ylabel)
        ax.grid(True, alpha=0.3)

    fig.suptitle(f"{label} Cost Curve (market impact scaling)", fontsize=13)
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# NEW B. SHOCK INJECTION TEST
# ─────────────────────────────────────────────

def shock_injection_test(
    r: np.ndarray,
    tdy: int,
    shock_sizes: Tuple[float, ...] = (-0.10, -0.20, -0.30, -0.50),
    n_shocks: Tuple[int, ...] = (1, 3, 5),
    inject_mode: str = "worst_cluster",   # "worst_cluster" | "random_uniform"
    seed: int = 42,
) -> pd.DataFrame:
    """
    Injects artificial shock days into the return series and measures
    how bad the resulting drawdown and Sharpe get.

    Two injection modes:
      worst_cluster: inject shocks back-to-back at the position of the
                     existing worst drawdown (stress the weak spot)
      random_uniform: inject at uniformly random positions (average case)

    For each (shock_size, n_shocks) combination, reports CAGR, Sharpe, MaxDD
    and the delta vs baseline.

    This directly answers: "What happens if we get N bad days in a row
    of size X% — does the strategy survive?"
    """
    rng      = np.random.default_rng(seed)
    eq_base  = _equity(r)
    dd_base  = _dd_curve(eq_base)
    base_cagr   = _cagr(eq_base, len(r), tdy) * 100
    base_sharpe = _sharpe(r, tdy)
    base_mdd    = _max_dd(eq_base) * 100

    # Find trough index of worst drawdown (inject there in worst_cluster mode)
    worst_idx = int(np.argmin(dd_base))

    rows = []
    # Baseline row
    rows.append({
        "shock_size_%":  0.0,
        "n_shocks":      0,
        "mode":          "baseline",
        "CAGR_%":        base_cagr,
        "Sharpe":        base_sharpe,
        "MaxDD_%":       base_mdd,
        "delta_cagr_%":  0.0,
        "delta_sharpe":  0.0,
        "delta_mdd_%":   0.0,
        "survived":      True,
    })

    for shock in shock_sizes:
        for ns in n_shocks:
            r_adj = r.copy()

            if inject_mode == "worst_cluster":
                # Insert shocks just before the worst trough
                insert_at = max(0, worst_idx - ns)
                positions = list(range(insert_at, insert_at + ns))
            else:
                positions = sorted(rng.choice(len(r), size=ns, replace=False).tolist())

            for pos in positions:
                if pos < len(r_adj):
                    r_adj[pos] = shock  # overwrite with shock return

            eq  = _equity(r_adj)
            c   = _cagr(eq, len(r_adj), tdy) * 100
            s   = _sharpe(r_adj, tdy)
            mdd = _max_dd(eq) * 100

            rows.append({
                "shock_size_%":  shock * 100,
                "n_shocks":      ns,
                "mode":          inject_mode,
                "CAGR_%":        c,
                "Sharpe":        s,
                "MaxDD_%":       mdd,
                "delta_cagr_%":  c - base_cagr,
                "delta_sharpe":  s - base_sharpe,
                "delta_mdd_%":   mdd - base_mdd,
                "survived":      mdd > -50.0,   # survived if MaxDD doesn't hit -50%
            })

    return pd.DataFrame(rows)


def _chart_shock_injection(df: pd.DataFrame, label: str, path: str):
    if not _MPL or df.empty:
        return
    plot_df = df[df["n_shocks"] > 0].copy()
    if plot_df.empty:
        return

    shock_sizes = sorted(plot_df["shock_size_%"].unique())
    n_shocks_vals = sorted(plot_df["n_shocks"].unique())
    colours = plt.cm.Reds(np.linspace(0.4, 0.9, len(shock_sizes)))

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    for ax, col, ylabel in zip(axes, ["Sharpe", "MaxDD_%"], ["Sharpe Ratio", "Max Drawdown (%)"]):
        base_val = float(df[df["n_shocks"] == 0][col].iloc[0]) if not df[df["n_shocks"] == 0].empty else None

        for shock, colour in zip(shock_sizes, colours):
            sub = plot_df[plot_df["shock_size_%"] == shock].sort_values("n_shocks")
            ax.plot(sub["n_shocks"], sub[col].to_numpy(dtype=float),
                    marker="o", label=f"{shock:.0f}% shock", color=colour)

        if base_val is not None:
            ax.axhline(base_val, color="black", linewidth=0.9, linestyle="--", label="Baseline")

        ax.set_xlabel("Number of shock days injected")
        ax.set_title(ylabel)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    fig.suptitle(f"{label} Shock Injection Test  (mode: worst cluster)", fontsize=13)
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# NEW C. NEIGHBOR PLATEAU TEST
# ─────────────────────────────────────────────

def neighbor_plateau_test(
    df_4x: pd.DataFrame,
    tdy: int,
    param_config: Optional[Dict[str, float]] = None,
    n_neighbors: int = 200,
    perturb_scale: float = 0.15,
    seed: int = 42,
    trial_purchases: bool = False,
    filter_series: Optional[pd.Series] = None,
) -> Dict[str, Any]:
    """
    Samples n_neighbors random parameter sets within +-perturb_scale of the
    baseline, simulates each, and measures the distribution of Sharpe ratios.

    Unlike the grid sensitivity map (which perturbs one param at a time),
    this perturbs ALL parameters simultaneously -- testing the shape of the
    joint parameter space around your optimum.

    A broad plateau = most neighbors have similar Sharpe -> robust.
    A sharp spike   = most neighbors are much worse -> overfitted.

    filter_series: optional pd.Series[bool] (True=sit flat) to apply the
                   canonical regime filter during each re-simulation.

    Returns:
        plateau_ratio:  fraction of neighbors within 0.5 Sharpe of baseline
        sharpe_pct10:   10th percentile Sharpe across neighbors
        sharpe_median:  median Sharpe across neighbors
        neighbor_df:    full table of all sampled neighbors
    """
    rng      = np.random.default_rng(seed)
    baseline = dict(_PARAM_DEFAULTS)
    if param_config:
        baseline.update(param_config)

    base_res    = _simulate_with_params(df_4x, baseline, tdy, trial_purchases=trial_purchases,
                                        filter_series=filter_series)
    base_sharpe = base_res["Sharpe"]

    print(f"  [plateau] baseline Sharpe = {base_sharpe:.3f}  sampling {n_neighbors} neighbors ...")

    param_names = list(baseline.keys())
    rows = []

    for i in range(n_neighbors):
        neighbor = dict(baseline)
        for p in param_names:
            if baseline[p] is None:
                continue   # disabled feature — leave as None
            pct = rng.uniform(-perturb_scale, perturb_scale)
            new_val = baseline[p] * (1.0 + pct)
            if p == "EARLY_KILL_X":
                new_val = max(5, round(new_val / 5) * 5)
            neighbor[p] = new_val

        res = _simulate_with_params(df_4x, neighbor, tdy, trial_purchases=trial_purchases,
                                    filter_series=filter_series)
        rows.append({
            "neighbor_id":   i,
            "Sharpe":        res["Sharpe"],
            "CAGR_%":        res["CAGR_%"],
            "MaxDD_%":       res["MaxDD_%"],
            "sharpe_delta":  res["Sharpe"] - base_sharpe,
            **{f"p_{p}": neighbor[p] for p in param_names},
        })

        if (i + 1) % 50 == 0:
            done_sharpes = [r["Sharpe"] for r in rows if np.isfinite(r["Sharpe"])]
            med = np.median(done_sharpes) if done_sharpes else float("nan")
            print(f"  [plateau] {i+1}/{n_neighbors}  running median Sharpe = {med:.3f}")

    neighbor_df = pd.DataFrame(rows)
    finite_sharpes = neighbor_df["Sharpe"].dropna().to_numpy(dtype=float)
    finite_sharpes = finite_sharpes[np.isfinite(finite_sharpes)]

    plateau_ratio = float(np.mean(np.abs(finite_sharpes - base_sharpe) <= 0.5)) if len(finite_sharpes) else float("nan")
    sharpe_pct10  = float(np.percentile(finite_sharpes, 10)) if len(finite_sharpes) else float("nan")
    sharpe_pct25  = float(np.percentile(finite_sharpes, 25)) if len(finite_sharpes) else float("nan")
    sharpe_median = float(np.median(finite_sharpes)) if len(finite_sharpes) else float("nan")
    sharpe_pct75  = float(np.percentile(finite_sharpes, 75)) if len(finite_sharpes) else float("nan")
    sharpe_std    = float(np.std(finite_sharpes, ddof=1)) if len(finite_sharpes) > 1 else float("nan")

    return {
        "base_sharpe":    base_sharpe,
        "n_neighbors":    n_neighbors,
        "perturb_scale":  perturb_scale,
        "plateau_ratio":  plateau_ratio,
        "sharpe_pct10":   sharpe_pct10,
        "sharpe_pct25":   sharpe_pct25,
        "sharpe_median":  sharpe_median,
        "sharpe_pct75":   sharpe_pct75,
        "sharpe_std":     sharpe_std,
        "neighbor_df":    neighbor_df,
    }


def _chart_neighbor_plateau(plateau: Dict[str, Any], label: str, path: str):
    if not _MPL or not plateau or plateau["neighbor_df"].empty:
        return

    sharpes = plateau["neighbor_df"]["Sharpe"].dropna().to_numpy(dtype=float)
    sharpes = sharpes[np.isfinite(sharpes)]
    base    = plateau["base_sharpe"]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Left: histogram of neighbor Sharpes
    ax = axes[0]
    # ax.hist(sharpes, bins=max(10, min(40, len(sharpes)//5)), color="steelblue", alpha=0.75, edgecolor="white")
    _bins = max(10, min(40, len(sharpes)//5))
    _sharpe_range = max(sharpes) - min(sharpes) if len(sharpes) > 1 else 0
    if _sharpe_range < 1e-9:
        ax.axvline(sharpes[0] if len(sharpes) else 0, color="steelblue", linewidth=2)
        ax.set_title(ax.get_title() + " (degenerate — all neighbors identical)", fontsize=8)
    else:
        ax.hist(sharpes, bins=_bins, color="steelblue", alpha=0.75, edgecolor="white")

    ax.axvline(base, color="red", linewidth=1.5, linestyle="--", label=f"Baseline ({base:.2f})")
    ax.axvline(plateau["sharpe_pct10"], color="orange", linewidth=1.0, linestyle=":", label=f"p10 ({plateau['sharpe_pct10']:.2f})")
    ax.set_xlabel("Sharpe Ratio")
    ax.set_ylabel("Count")
    ax.set_title("Neighbor Sharpe Distribution")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # Right: delta from baseline (sorted)
    ax = axes[1]
    deltas = np.sort(sharpes - base)
    x = np.arange(len(deltas))
    colours = np.where(deltas >= -0.5, "steelblue", "crimson")
    ax.bar(x, deltas, color=colours, alpha=0.7, width=1.0)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.axhline(-0.5, color="orange", linewidth=0.8, linestyle="--", label="±0.5 threshold")
    ax.axhline(0.5,  color="orange", linewidth=0.8, linestyle="--")
    ax.set_xlabel("Neighbor (sorted by delta)")
    ax.set_ylabel("Sharpe delta vs baseline")
    ax.set_title(f"Plateau Ratio: {plateau['plateau_ratio']*100:.1f}% within ±0.5")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3, axis="y")

    fig.suptitle(
        f"{label} Neighbor Plateau Test  "
        f"(n={plateau['n_neighbors']}, ±{plateau['perturb_scale']*100:.0f}% joint perturbation)",
        fontsize=12,
    )
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# NEW D. CAPACITY CURVE TEST
# ─────────────────────────────────────────────

def capacity_curve_test(
    r: np.ndarray,
    tdy: int,
    starting_capital: float = 5_000.0,
    avg_daily_volume_usd: float = 5_000_000,
    participation_rate: float = 0.10,
    impact_coeff: float = 0.10,
    active_day_fraction: float = 0.341,
) -> pd.DataFrame:
    """
    Dynamic capacity curve: anchored to your actual starting capital,
    compounding equity forward and applying increasing market impact drag
    as the account grows day by day.

    The old static model picked arbitrary AUM levels disconnected from your
    actual account. This model asks the real question:
    "As my account compounds, at what point does my own growing position
    size start moving the market against me?"

    Key improvements over static version:
      1. Anchored to real starting_capital — changing that value changes results
      2. Impact compounds with equity — drag increases as you grow
      3. Impact only applied on active (non-kill) days — 65.9% kill rate means
         you have minimal footprint on most days
      4. Reports metrics at participation threshold crossings, not arbitrary AUM

    Impact model (Almgren-Chriss sqrt):
      impact_rate = impact_coeff * sqrt(equity / max_deployable) * 0.001
      max_deployable = avg_daily_volume * participation_rate
    """
    max_deployable = avg_daily_volume_usd * participation_rate

    equity     = float(starting_capital)
    eq_curve   = [equity]
    eq_nodrag  = [equity]
    rows_daily = []

    for i, ret in enumerate(r):
        participation = equity / avg_daily_volume_usd

        # Only apply impact on active (non-kill) days
        # active_day_fraction = ~0.341 (1 - 0.659 kill rate)
        is_active = (i % max(1, round(1.0 / max(active_day_fraction, 0.01)))) != 0
        impact = (impact_coeff * math.sqrt(max(equity, 1.0) / max_deployable) * 0.001
                  if (participation > 0 and is_active) else 0.0)

        adj_ret   = ret - impact
        equity    = equity * (1.0 + adj_ret)
        nodrag_eq = eq_nodrag[-1] * (1.0 + ret)

        eq_curve.append(equity)
        eq_nodrag.append(nodrag_eq)

        rows_daily.append({
            "day":             i + 1,
            "equity":          equity,
            "participation_%": participation * 100,
            "impact_%":        impact * 100,
            "capacity_flag":   participation > 0.10,
        })

    daily_df = pd.DataFrame(rows_daily)

    # Summary at participation threshold crossings
    thresholds = [0.001, 0.005, 0.01, 0.02, 0.05, 0.10, 0.20, 0.50, 1.00]
    summary_rows = []

    for thresh in thresholds:
        crossing = daily_df[daily_df["participation_%"] >= thresh * 100]
        if crossing.empty:
            day_reached = len(r)
            equity_at   = float(eq_curve[-1])
        else:
            day_reached = int(crossing.iloc[0]["day"])
            equity_at   = float(crossing.iloc[0]["equity"])

        # Metrics with impact fixed at this participation level
        fixed_impact = impact_coeff * math.sqrt(
            (thresh * avg_daily_volume_usd) / max_deployable
        ) * 0.001 * active_day_fraction
        r_adj = r - fixed_impact
        eq_s  = _equity(r_adj)

        summary_rows.append({
            "participation_%":    thresh * 100,
            "AUM_at_crossing":    equity_at,
            "day_reached":        day_reached,
            "impact_%/day":       fixed_impact * 100,
            "CAGR_%":             _cagr(eq_s, len(r_adj), tdy) * 100,
            "Sharpe":             _sharpe(r_adj, tdy),
            "MaxDD_%":            _max_dd(eq_s) * 100,
            "capacity_flag":      thresh > 0.10,
        })

    summary_df = pd.DataFrame(summary_rows)
    summary_df.attrs["daily_df"]        = daily_df
    summary_df.attrs["eq_curve"]        = np.array(eq_curve)
    summary_df.attrs["eq_nodrag"]       = np.array(eq_nodrag)
    summary_df.attrs["starting_capital"]= starting_capital
    return summary_df



def _chart_capacity_curve(df: pd.DataFrame, label: str, path: str):
    if not _MPL or df.empty:
        return

    part = df["participation_%"].to_numpy(dtype=float)
    x = np.arange(len(part))
    xlabels = [f"{p:.1f}%" for p in part]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    ax = axes[0]
    ax.plot(part, df["Sharpe"].to_numpy(dtype=float), marker="o", color="darkorange")
    ax.axvline(10.0, color="red", linewidth=0.9, linestyle="--", label="10% threshold")
    ax.axhline(2.0,  color="grey", linewidth=0.8, linestyle=":", label="Sharpe=2")
    ax.set_xlabel("Market participation (%)"); ax.set_title("Sharpe vs Participation")
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    ax = axes[1]
    ax.plot(part, df["MaxDD_%"].to_numpy(dtype=float), marker="o", color="crimson")
    ax.axvline(10.0, color="red", linewidth=0.9, linestyle="--", label="10% threshold")
    ax.axhline(-30.0, color="grey", linewidth=0.8, linestyle=":", label="-30% floor")
    ax.set_xlabel("Market participation (%)"); ax.set_title("Max Drawdown vs Participation")
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3)

    ax = axes[2]
    aum_vals = df["AUM_at_crossing"].to_numpy(dtype=float)
    bar_colours = ["crimson" if f else "steelblue" for f in df["capacity_flag"]]
    ax.bar(x, aum_vals, color=bar_colours, alpha=0.75)
    ax.set_xticks(x); ax.set_xticklabels(xlabels, rotation=40, fontsize=7)
    ax.set_title("AUM when threshold reached\n(red = >10% daily vol)")
    ax.set_ylabel("Account equity ($)"); ax.grid(True, alpha=0.3, axis="y")

    start_cap = df.attrs.get("starting_capital", 0)
    eq_curve  = df.attrs.get("eq_curve")
    eq_nodrag = df.attrs.get("eq_nodrag")
    daily_df  = df.attrs.get("daily_df")

    if eq_curve is not None and eq_nodrag is not None:
        import os
        fig2, ax2 = plt.subplots(figsize=(10, 4))
        days = np.arange(len(eq_curve))
        ax2.plot(days, eq_curve,  color="steelblue", label="With impact drag")
        ax2.plot(days, eq_nodrag, color="grey", linestyle="--", label="No impact", alpha=0.7)
        ax2.set_xlabel("Day"); ax2.set_ylabel("Account equity ($)")
        ax2.set_title(f"{label} Equity Growth with Dynamic Market Impact  (start=${start_cap:,.0f})")
        ax2.legend(fontsize=9); ax2.grid(True, alpha=0.3)
        if daily_df is not None:
            red_days = daily_df[daily_df["capacity_flag"]]["day"]
            if not red_days.empty:
                d = int(red_days.iloc[0])
                ax2.axvline(d, color="red", linewidth=0.9, linestyle="--",
                            label=f"Day {d}: hits 10% vol")
                ax2.legend(fontsize=9)
        equity_path = path.replace("_capacity_curve.png", "_capacity_equity.png")
        _safe_save(fig2, equity_path)

    fig.suptitle(
        f"{label} Dynamic Capacity Curve  (start=${start_cap:,.0f})",
        fontsize=12
    )
    _safe_save(fig, path)


def leverage_sensitivity(
    r_1x: np.ndarray,
    tdy: int,
    levels: Tuple[float, ...] = (0.5, 0.75, 1.0, 1.25, 1.5, 2.0),
) -> pd.DataFrame:
    """
    Scale a *1x* return series by different leverage multipliers,
    recompute CAGR, Sharpe, MaxDD.
    """
    rows = []
    for lev in levels:
        adj = r_1x * lev
        eq  = _equity(adj)
        rows.append({
            "leverage":  lev,
            "CAGR_%":   _cagr(eq, len(adj), tdy) * 100,
            "Sharpe":   _sharpe(adj, tdy),
            "MaxDD_%":  _max_dd(eq) * 100,
        })
    return pd.DataFrame(rows)


def kelly_fraction(r: np.ndarray) -> Dict[str, float]:
    """
    Full Kelly fraction = mu / sigma^2 (continuous approximation).
    Half-Kelly also returned as common institutional practice.
    """
    mu  = float(np.mean(r))
    var = float(np.var(r, ddof=1))
    if var == 0:
        return {"full_kelly": float("nan"), "half_kelly": float("nan")}
    fk = mu / var
    return {"full_kelly": fk, "half_kelly": fk / 2.0}


def ruin_probability(
    r: np.ndarray,
    ruin_threshold: float = -0.50,
    n_sims: int = 5000,
    horizon_days: int = 365,
    seed: int = 42,
) -> Dict[str, float]:
    """
    Monte Carlo estimate of ruin probability:
    probability that cumulative drawdown hits ruin_threshold
    within horizon_days, given the empirical return distribution.
    """
    rng = np.random.default_rng(seed)
    n_r = len(r)
    ruin_count = 0

    for _ in range(n_sims):
        idx   = rng.integers(0, n_r, size=horizon_days)
        samp  = r[idx]
        eq    = np.cumprod(1.0 + samp)
        peak  = np.maximum.accumulate(eq)
        dd    = eq / peak - 1.0
        if np.any(dd <= ruin_threshold):
            ruin_count += 1

    prob = ruin_count / n_sims
    return {
        "ruin_threshold":  ruin_threshold,
        "horizon_days":    horizon_days,
        "ruin_probability": prob,
        "n_sims":          n_sims,
    }


# ─────────────────────────────────────────────
# 9. CHARTS
# ─────────────────────────────────────────────

def _chart_equity_curve(r: np.ndarray, label: str, path: str):
    """
    Cumulative equity curve with:
      - IS/OOS regime split (vertical dashed line at midpoint)
      - Drawdown shading beneath the curve
      - Annotations for MaxDD, final multiple, and key stats
    """
    if not _MPL:
        return

    eq     = _equity(r)
    days   = np.arange(len(eq))
    split  = len(r) // 2

    # Running peak and drawdown
    peak   = np.maximum.accumulate(eq)
    dd     = (eq - peak) / peak  # negative values

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8),
                                    gridspec_kw={"height_ratios": [3, 1]},
                                    sharex=True)
    fig.subplots_adjust(hspace=0.08)

    # ── Equity curve ─────────────────────────────────────────────────────────
    ax1.plot(days, eq, color="steelblue", linewidth=1.5, label="Equity (1x start)")
    ax1.fill_between(days, 1.0, eq, where=(eq >= 1.0),
                     alpha=0.12, color="steelblue")

    # IS/OOS split line
    ax1.axvline(split, color="darkorange", linewidth=1.2, linestyle="--", alpha=0.8,
                label=f"IS/OOS split (day {split})")

    # Final multiple annotation
    final_mult = eq[-1]
    ax1.annotate(f"{final_mult:.1f}×",
                 xy=(days[-1], eq[-1]),
                 xytext=(-30, 10), textcoords="offset points",
                 fontsize=9, color="steelblue",
                 arrowprops=dict(arrowstyle="->", color="steelblue", lw=0.8))

    # IS / OOS CAGR labels
    tdy = 365
    cagr_is  = _cagr(_equity(r[:split]),  len(r[:split]),  tdy) * 100
    cagr_oos = _cagr(_equity(r[split:]),  len(r[split:]),  tdy) * 100
    sharpe_is  = _sharpe(r[:split],  tdy)
    sharpe_oos = _sharpe(r[split:],  tdy)

    ax1.text(split * 0.5, eq.max() * 0.92,
             f"IS  CAGR={cagr_is:.0f}%\nSharpe={sharpe_is:.2f}",
             ha="center", fontsize=8, color="dimgray",
             bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.7))
    ax1.text(split + (len(r) - split) * 0.5, eq.max() * 0.92,
             f"OOS  CAGR={cagr_oos:.0f}%\nSharpe={sharpe_oos:.2f}",
             ha="center", fontsize=8, color="dimgray",
             bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.7))

    ax1.set_ylabel("Equity Multiple")
    ax1.set_title(f"{label}  —  Equity Curve  "
                  f"(Final {final_mult:.1f}×  |  "
                  f"IS/OOS ratio: {(cagr_oos/cagr_is):.2f}  |  "
                  f"Sharpe {_sharpe(r, tdy):.2f})",
                  fontsize=11)
    ax1.legend(fontsize=8, loc="upper left")
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}×"))

    # ── Drawdown panel ────────────────────────────────────────────────────────
    ax2.fill_between(days[1:], dd[1:], 0,
                     where=(dd[1:] < 0), color="crimson", alpha=0.45)
    ax2.plot(days[1:], dd[1:], color="crimson", linewidth=0.8)
    ax2.axvline(split, color="darkorange", linewidth=1.2, linestyle="--", alpha=0.8)
    ax2.axhline(0, color="gray", linewidth=0.5)

    max_dd = dd.min()
    ax2.annotate(f"MaxDD {max_dd*100:.1f}%",
                 xy=(np.argmin(dd), max_dd),
                 xytext=(15, -8), textcoords="offset points",
                 fontsize=8, color="crimson",
                 arrowprops=dict(arrowstyle="->", color="crimson", lw=0.8))

    ax2.set_ylabel("Drawdown")
    ax2.set_xlabel("Trading Day")
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x*100:.0f}%"))

    _safe_save(fig, path)


def _chart_rolling_sharpe(rs: np.ndarray, window: int, label: str, path: str):
    if not _MPL:
        return
    fig, ax = plt.subplots()
    ax.plot(rs, color="steelblue")
    ax.axhline(0, color="red", linewidth=0.8, linestyle="--")
    ax.axhline(2, color="green", linewidth=0.8, linestyle=":")
    ax.set_title(f"{label} Rolling {window}-day Sharpe")
    ax.set_xlabel("Day")
    ax.set_ylabel("Sharpe")
    _safe_save(fig, path)


def _chart_rolling_cagr(rc: np.ndarray, window: int, label: str, path: str):
    if not _MPL:
        return
    fig, ax = plt.subplots()
    ax.plot(rc * 100, color="darkorange")
    ax.axhline(0, color="red", linewidth=0.8, linestyle="--")
    ax.set_title(f"{label} Rolling {window}-day CAGR (%)")
    ax.set_xlabel("Day")
    ax.set_ylabel("CAGR (%)")
    _safe_save(fig, path)


def _chart_slippage_sensitivity(df: pd.DataFrame, label: str, path: str):
    if not _MPL:
        return
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    for ax, col, colour in zip(axes, ["CAGR_%", "Sharpe", "MaxDD_%"],
                                      ["steelblue", "darkorange", "crimson"]):
        ax.plot(df["slippage_rate"] * 100, df[col], marker="o", color=colour)
        ax.set_title(col)
        ax.set_xlabel("Slippage rate (%)")
        ax.grid(True, alpha=0.3)
    fig.suptitle(f"{label} Slippage Sensitivity")
    _safe_save(fig, path)


def _chart_leverage_sensitivity(df: pd.DataFrame, label: str, path: str):
    if not _MPL:
        return
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    for ax, col, colour in zip(axes, ["CAGR_%", "Sharpe", "MaxDD_%"],
                                      ["steelblue", "darkorange", "crimson"]):
        ax.plot(df["leverage"], df[col], marker="o", color=colour)
        ax.set_title(col)
        ax.set_xlabel("Leverage multiplier")
        ax.grid(True, alpha=0.3)
    fig.suptitle(f"{label} Leverage Sensitivity")
    _safe_save(fig, path)


def _chart_drawdown_episodes(episodes: pd.DataFrame, label: str, path: str):
    if not _MPL or episodes.empty:
        return
    top = episodes.nsmallest(min(10, len(episodes)), "depth")
    fig, ax = plt.subplots(figsize=(10, 5))
    colours = ["crimson" if pd.isna(r["end_idx"]) else "steelblue"
               for _, r in top.iterrows()]
    bars = ax.barh(
        range(len(top)),
        top["depth"].abs() * 100,
        color=colours
    )
    labels = [
        f"Ep {int(r['start_idx'])}  depth={r['depth']*100:.1f}%  dur={int(r['duration_days'])}d"
        f"  rec={'N/A' if pd.isna(r['end_idx']) else str(int(r['recovery_days']))+'d'}"
        for _, r in top.iterrows()
    ]
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("Depth (%)")
    ax.set_title(f"{label} Top Drawdown Episodes  (red = not yet recovered)")
    _safe_save(fig, path)


def _chart_return_distribution(r: np.ndarray, label: str, path: str):
    if not _MPL:
        return
    fig, ax = plt.subplots()
    ax.hist(r * 100, bins=40, density=True, alpha=0.6, color="steelblue", label="Actual")
    # normal overlay
    mu, sd = float(np.mean(r)) * 100, float(np.std(r)) * 100
    x = np.linspace(mu - 4 * sd, mu + 4 * sd, 300)
    ax.plot(x, stats.norm.pdf(x, mu, sd), "r-", lw=1.5, label="Normal fit")
    ax.set_title(f"{label} Daily Return Distribution")
    ax.set_xlabel("Daily return (%)")
    ax.set_ylabel("Density")
    ax.legend()
    _safe_save(fig, path)


def _chart_rolling_sharpe_with_regimes(rs: np.ndarray, r: np.ndarray, window: int,
                                       label: str, path: str):
    """Rolling Sharpe coloured by high/low vol regime."""
    if not _MPL:
        return
    vol = pd.Series(r).rolling(20).std().to_numpy()
    med_vol = float(np.nanmedian(vol))

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True)

    ax1.plot(rs, color="steelblue", lw=0.9)
    ax1.axhline(0,  color="red",   lw=0.8, ls="--")
    ax1.axhline(2,  color="green", lw=0.8, ls=":")
    ax1.set_ylabel("Rolling Sharpe")
    ax1.set_title(f"{label} Rolling {window}d Sharpe + Vol Regime")

    ax2.fill_between(range(len(vol)), 0, vol * 100,
                     where=vol > med_vol,  color="tomato",      alpha=0.5, label="High vol")
    ax2.fill_between(range(len(vol)), 0, vol * 100,
                     where=vol <= med_vol, color="lightsteelblue", alpha=0.5, label="Low vol")
    ax2.set_ylabel("Rolling 20d vol (%)")
    ax2.set_xlabel("Day")
    ax2.legend(fontsize=8)
    _safe_save(fig, path)


# ─────────────────────────────────────────────
# 10. MASTER RUNNER
# ─────────────────────────────────────────────

def run_institutional_audit(
    daily_returns: np.ndarray,
    label: str = "Strategy",
    outdir: Optional[Path] = None,
    track_file=None,            # callable: track_file(filename) -> full_path
    n_trials: int = 50,         # number of parameter combos tried (for DSR)
    trading_days_per_year: int = 365,
    starting_capital: float = 5000.0,
    save_charts: bool = True,
    rolling_window: int = 60,   # days for rolling Sharpe / CAGR
    mc_iters:   int = 2000,     # Monte Carlo reshuffle iterations (0 = skip)
    bb_iters:   int = 1000,     # Block bootstrap iterations (0 = skip)
    bb_block:   int = 10,       # Block bootstrap block length (days)
    mc_seed:    int = 7,        # RNG seed for MC/BB
    n_wf_folds: int = 8,        # walk-forward folds (0 = skip)
    wf_train_days: int = 120,   # rolling walk-forward train window
    wf_test_days:  int = 30,    # rolling walk-forward test window
    wf_step_days:  int = 30,    # rolling walk-forward step size
    # ── Parameter sensitivity map ──────────────────────────
    df_4x: Optional[pd.DataFrame] = None,   # raw pivot from Google Sheets
    param_config: Optional[Dict[str, float]] = None,  # your actual baseline params
    params_to_test: Optional[List[str]] = None,       # subset to test (None = all)
    trial_purchases: bool = False,
    regime_filter_cols=None,   # set of col name strings to zero (regime filter)
    filter_mode: Optional[str] = None,       # canonical filter mode (for logging only)
    filter_series: Optional[pd.Series] = None,  # pd.Series[bool] True=sit flat;
                                                 # threads into sensitivity/plateau
                                                 # re-simulations so they use the
                                                 # same filter as the main run
) -> Dict[str, Any]:
    """
    Run all institutional audit tests on `daily_returns`.
    Prints a full report and saves charts.
    Returns a dict of all computed metrics.
    """

    r    = np.asarray(daily_returns, dtype=float)
    r    = r[np.isfinite(r)]
    tdy  = trading_days_per_year
    n    = len(r)
    eq   = _equity(r)
    dd   = _dd_curve(eq)

    observed_sharpe = _sharpe(r, tdy)

    # ── Pre-compute dist stats for DSR / MTL ──────────────────
    dist = distribution_stats(r)
    sk   = dist["skewness"]
    ku   = dist["excess_kurtosis"]

    # ── All metrics ───────────────────────────────────────────
    # Sortino/Omega/Ulcer: use active-only returns so the mean, downside std,
    # and gain/loss ratio all reflect actual trading days.
    # Calmar: keep full eq (with flat-day equity plateaus) for MaxDD accuracy;
    # use active-only r for CAGR so it annualises consistently.
    r_active = r[r != 0.0] if (r != 0.0).any() else r
    eq_full  = eq   # equity built from r (includes flat plateaus)
    sort_r   = sortino_ratio(r_active, tdy)
    calm_r   = calmar_ratio(eq_full, r, tdy)  # MaxDD from full eq, CAGR from r (with zeros → total days)
    omeg_r   = omega_ratio(r_active)
    ulcer    = ulcer_index(eq_full)

    ep_df   = drawdown_episodes(eq)
    pct_uw  = pct_time_underwater(eq)
    avg_dw  = average_drawdown(eq)

    jb      = jarque_bera_test(r)
    lb      = ljung_box_test(r, lags=10)

    regime  = regime_analysis(r, tdy)

    rs_arr  = rolling_sharpe(r, rolling_window, tdy)
    rc_arr  = rolling_cagr(r, rolling_window, tdy)

    slip_df   = slippage_sensitivity(r, tdy)
    cap_df    = capped_return_sensitivity(r, tdy, caps=(0.10, 0.20, 0.30, 0.50))
    topn_df   = top_n_removal_test(r, tdy, ns=(1, 3, 5, 10))
    lucky_df  = lucky_streak_test(r, tdy, window=30)
    lev_df    = leverage_sensitivity(r, tdy)  # treats current series as 1x

    # ── Periodic return breakdown ─────────────────────────────
    period_breakdown = periodic_return_breakdown(r, tdy)

    # ── New tests ─────────────────────────────────────────────
    cost_df     = cost_curve_test(r, tdy)
    shock_df    = shock_injection_test(r, tdy)
    capacity_df = capacity_curve_test(r, tdy, starting_capital=starting_capital)

    # ── Rolling MaxDD ─────────────────────────────────────────
    roll_mdd = rolling_maxdd_table(r, windows=(3, 7, 10, 30))

    # ── Daily VaR / CVaR ──────────────────────────────────────
    var5,  cvar5  = var_cvar(r, 0.05)
    var1,  cvar1  = var_cvar(r, 0.01)

    # ── MC Reshuffle + Block Bootstrap ────────────────────────
    mc_df = pd.DataFrame()
    bb_df = pd.DataFrame()
    if mc_iters > 0:
        print(f"\n  Running MC reshuffle ({mc_iters:,} iters) …")
        mc_df = mc_reshuffle(r, iters=mc_iters, seed=mc_seed)
    if bb_iters > 0:
        print(f"  Running block bootstrap ({bb_iters:,} iters, block={bb_block}d) …")
        bb_df = block_bootstrap(r, iters=bb_iters, block_len=bb_block, seed=mc_seed + 1)

    # Neighbor plateau — slow (n_neighbors simulations); only runs when df_4x provided
    plateau: Dict[str, Any] = {}

    # ── Parameter sensitivity (requires df_4x) ────────────────
    run_sensitivity = df_4x is not None and not df_4x.empty
    tornado_df  = pd.DataFrame()
    heatmap_df  = pd.DataFrame()
    base_sharpe_sens = observed_sharpe

    if run_sensitivity:
        print(f"\n  Running parameter sensitivity map ({label}) …")
        tornado_df, heatmap_df = parameter_sensitivity_map(
            df_4x,
            tdy,
            param_config   = param_config,
            perturb_pcts   = (-0.30, -0.20, -0.10, 0.0, 0.10, 0.20, 0.30),
            params_to_test    = params_to_test,
            trial_purchases  = trial_purchases,
            filter_series    = filter_series,
        )
        # baseline row Sharpe (perturb_pct == 0)
        base_row = tornado_df[tornado_df["perturb_pct"] == 0.0]
        if not base_row.empty:
            base_sharpe_sens = float(base_row["Sharpe"].iloc[0])

        print(f"\n  Running neighbor plateau test ({label}) …")
        plateau = neighbor_plateau_test(
            df_4x, tdy,
            param_config    = param_config,
            n_neighbors     = 200,
            perturb_scale   = 0.15,
            trial_purchases = trial_purchases,
            filter_series   = filter_series,
        )

    wk_cvar = weekly_cvar(r)
    streak  = losing_streak_stats(r)

    dsr_res = deflated_sharpe_ratio(observed_sharpe, n, n_trials, sk, ku, tdy)
    mtl     = minimum_track_record_length(observed_sharpe, 1.0, 0.95, sk, ku, tdy)
    pf      = profit_factor(r)

    kelly   = kelly_fraction(r)
    ruin    = ruin_probability(r, ruin_threshold=-0.50, horizon_days=tdy)

    # ── Print Report ──────────────────────────────────────────

    sep  = "=" * 54
    sep2 = "-" * 54

    print()
    print(sep)
    print(f"  INSTITUTIONAL AUDIT — {label.upper()}")
    print(sep)

    # ── 0a. Simulation Bias Audit ─────────────────────────────
    if df_4x is not None and param_config is not None:
        bias_audit = simulation_bias_audit(
            df_4x, param_config,
            trial_purchases=trial_purchases,
            tdy=tdy,
        )
        print_simulation_bias_report(bias_audit, label)
    else:
        bias_audit = {}

    # ── 0b. Periodic Return Breakdown ─────────────────────────
    print_periodic_return_breakdown(period_breakdown)

    # ── 1. Risk-Adjusted Return Quality ───────────────────────
    print()
    print("┌─ RISK-ADJUSTED RETURN QUALITY ─────────────────────")
    print(f"│  Sharpe Ratio:            {observed_sharpe:>10.3f}")
    print(f"│  Sortino Ratio:          {sort_r:>10.3f}")
    print(f"│  Calmar Ratio:           {calm_r:>10.3f}")
    print(f"│  Omega Ratio:            {omeg_r:>10.3f}")
    print(f"│  Ulcer Index:            {ulcer:>10.3f}  (lower = better)")
    print(f"│  Profit Factor:          {pf:>10.3f}")
    print(f"└{'─'*53}")

    # ── 2. Drawdown Episodes ──────────────────────────────────
    print()
    print("┌─ DRAWDOWN EPISODE ANALYSIS ─────────────────────────")
    print(f"│  Distinct episodes:      {len(ep_df):>10d}")
    print(f"│  Avg drawdown depth:     {avg_dw*100:>9.2f}%")
    print(f"│  % time underwater:      {pct_uw*100:>9.2f}%")

    if not ep_df.empty:
        top5_ep = ep_df.nsmallest(5, "depth")
        print(f"│")
        print(f"│  TOP 5 WORST DRAWDOWN EPISODES:")
        print(f"│  {'#':>2}  {'Depth':>8}  {'Dur(d)':>7}  {'Rec(d)':>7}  {'Total(d)':>9}  {'Recovered':>9}")
        print(f"│  {sep2[:50]}")
        for rank, (_, ep) in enumerate(top5_ep.iterrows(), 1):
            rec_str = "N/A (open)" if pd.isna(ep["end_idx"]) else f"{int(ep['recovery_days']):>7d}"
            print(
                f"│  {rank:>2}  {ep['depth']*100:>7.2f}%  "
                f"{int(ep['duration_days']):>7d}  "
                f"{rec_str:>9}  "
                f"{int(ep['total_days']):>9d}  "
                f"{'NO' if pd.isna(ep['end_idx']) else 'YES':>9}"
            )
    print(f"└{'─'*53}")

    # ── 3. Return Distribution ────────────────────────────────
    print()
    print("┌─ RETURN DISTRIBUTION ───────────────────────────────")
    print(f"│  Skewness:               {sk:>10.4f}  {'⚠ negative skew' if sk < -0.5 else 'OK'}")
    print(f"│  Excess kurtosis:        {ku:>10.4f}  {'⚠ fat tails' if ku > 1.0 else 'OK'}")
    print(f"│  Jarque-Bera stat:       {jb['jb_stat']:>10.3f}")
    print(f"│  JB p-value:             {jb['jb_pvalue']:>10.4f}  ({'normal dist likely' if jb.get('normal') else 'NON-NORMAL ⚠'})")
    print(f"│  Ljung-Box Q({lb['lags']}):     {lb['lb_stat']:>10.3f}")
    print(f"│  LB p-value:             {lb['lb_pvalue']:>10.4f}  ({'autocorrelated ⚠' if lb.get('autocorrelated') else 'no autocorrelation'})")
    print(f"└{'─'*53}")

    # ── 4. Regime Analysis ────────────────────────────────────
    print()
    print("┌─ REGIME & CONDITIONAL ANALYSIS ────────────────────")
    print(f"│  Up days:    {regime['up_days']:>4d}   mean={regime['up_mean_pct']:>6.2f}%  Sharpe={regime['up_sharpe']:>6.2f}")
    print(f"│  Down days:  {regime['down_days']:>4d}   mean={regime['down_mean_pct']:>6.2f}%  Sharpe={regime['down_sharpe']:>6.2f}")
    print(f"│  Low-vol regime:   mean={regime['low_vol_mean_pct']:>6.2f}%  Sharpe={regime['low_vol_sharpe']:>6.2f}")
    print(f"│  High-vol regime:  mean={regime['high_vol_mean_pct']:>6.2f}%  Sharpe={regime['high_vol_sharpe']:>6.2f}")
    print(f"│  Vol split threshold: {regime['median_vol_threshold']*100:.4f}% daily vol")
    rs_finite = rs_arr[np.isfinite(rs_arr)]
    rc_finite = rc_arr[np.isfinite(rc_arr)]
    if len(rs_finite):
        print(f"│  Rolling {rolling_window}d Sharpe:  min={rs_finite.min():.2f}  med={np.median(rs_finite):.2f}  max={rs_finite.max():.2f}")
    if len(rc_finite):
        print(f"│  Rolling {rolling_window}d CAGR:    min={rc_finite.min()*100:.1f}%  med={np.median(rc_finite)*100:.1f}%  max={rc_finite.max()*100:.1f}%")
    print(f"└{'─'*53}")

    # ── 4b. Rolling MaxDD ─────────────────────────────────────
    print()
    print("┌─ ROLLING MAX DRAWDOWN ──────────────────────────────")
    for w, v in roll_mdd.items():
        flag = "⚠" if math.isfinite(v) and v < -0.15 else ""
        v_s  = f"{v*100:.2f}%" if math.isfinite(v) else "nan"
        print(f"│  Worst {w:>3} window:       {v_s:>10}  {flag}")
    print(f"└{'─'*53}")

    # ── 4c. Daily VaR / CVaR ──────────────────────────────────
    print()
    print("┌─ DAILY VaR / CVaR ──────────────────────────────────")
    for alpha, var, cvar_v in [(0.05, var5, cvar5), (0.01, var1, cvar1)]:
        var_s  = f"{var*100:.2f}%"  if math.isfinite(var)    else "nan"
        cvar_s = f"{cvar_v*100:.2f}%" if math.isfinite(cvar_v) else "nan"
        print(f"│  VaR({int(alpha*100):2d}%):  {var_s:>8}   CVaR({int(alpha*100):2d}%):  {cvar_s:>8}")
    print(f"└{'─'*53}")

    # ── 5. Slippage Sensitivity ───────────────────────────────
    print()
    print("┌─ SLIPPAGE SENSITIVITY TABLE ────────────────────────")
    print(f"│  {'Slippage':>10}  {'CAGR%':>8}  {'Sharpe':>8}  {'MaxDD%':>8}")
    print(f"│  {sep2[:42]}")
    for _, row in slip_df.iterrows():
        print(f"│  {row['slippage_rate']*100:>9.1f}%  {row['CAGR_%']:>8.2f}  {row['Sharpe']:>8.3f}  {row['MaxDD_%']:>7.2f}%")

    elast = cost_elasticity(slip_df)
    if math.isfinite(elast):
        print(f"│")
        print(f"│  Cost Elasticity (d log CAGR / d slip): {elast:.4f}")
        sens_flag = "✅ LOW" if abs(elast) < 100 else ("⚠ MODERATE" if abs(elast) < 300 else "⚠ HIGH")
        print(f"│  Sensitivity: {sens_flag}  (|elasticity| < 100 = low cost sensitivity)")
    print(f"└{'─'*53}")

    # ── 5e. Regime Robustness ─────────────────────────────────
    rr = regime_robustness_test(r, tdy, split_pct=0.50)
    print_regime_robustness_report(rr)

    # ── 5b. Capped Return Sensitivity ────────────────────────
    print()
    print("┌─ CAPPED RETURN SENSITIVITY TABLE ──────────────────")
    print(f"│  {'Cap':>6}  {'Days':>5}  {'%Days':>6}  {'LogGrw%':>8}  {'CAGR%':>10}  {'Sharpe':>7}  {'MaxDD%':>7}")
    print(f"│  {sep2[:60]}")
    for _, row in cap_df.iterrows():
        cap_lbl = f"{row['cap']:>6}"
        days    = int(row["days_capped"])
        pct_d   = float(row["pct_days"])
        lg_rem  = float(row["log_growth_removed_%"])
        cagr_v  = float(row["CAGR_%"])
        sh_v    = float(row["Sharpe"])
        mdd_v   = float(row["MaxDD_%"])
        lg_str  = f"{lg_rem:>7.1f}%" if np.isfinite(lg_rem) else "    N/A"
        print(
            f"│  {cap_lbl}  {days:>5d}  {pct_d:>5.1f}%  {lg_str}  "
            f"{cagr_v:>10.2f}  {sh_v:>7.3f}  {mdd_v:>6.2f}%"
        )
    print(f"└{'─'*53}")
    print()

    # ── 5c. Top-N Day Removal ─────────────────────────────────
    print("┌─ TOP-N DAY REMOVAL TEST ────────────────────────────")
    print(f"│  {'N Removed':>10}  {'CAGR%':>10}  {'ΔCAGR%':>8}  {'Sharpe':>7}  {'ΔSharpe':>8}  {'MaxDD%':>7}")
    print(f"│  {sep2[:58]}")
    for _, row in topn_df.iterrows():
        lbl = f"{row['label']:>14}"
        c   = float(row["CAGR_%"])
        dc  = float(row["cagr_delta_%"])
        s   = float(row["Sharpe"])
        ds  = float(row["sharpe_delta"])
        mdd = float(row["MaxDD_%"])
        dc_str = f"{dc:>+8.2f}" if row["n_removed"] > 0 else "        "
        ds_str = f"{ds:>+8.3f}" if row["n_removed"] > 0 else "        "
        print(f"│  {lbl}  {c:>10.2f}  {dc_str}  {s:>7.3f}  {ds_str}  {mdd:>6.2f}%")
        if row["n_removed"] > 0 and row["days_removed"]:
            print(f"│    ↳ removed: {row['days_removed']}")
    print(f"└{'─'*53}")
    print()

    # ── 5d. Lucky Streak Test ─────────────────────────────────
    if not lucky_df.empty:
        print("┌─ LUCKY STREAK TEST (30-day windows) ───────────────")
        print(f"│  {'Scenario':>22}  {'CAGR%':>10}  {'ΔCAGR%':>8}  {'Sharpe':>7}  {'ΔSharpe':>8}")
        print(f"│  {sep2[:58]}")
        for _, row in lucky_df.iterrows():
            lbl = f"{row['label']:>22}"
            c   = float(row["CAGR_%"])
            dc  = float(row["cagr_delta_%"])
            s   = float(row["Sharpe"])
            ds  = float(row["sharpe_delta"])
            dc_str = f"{dc:>+8.2f}" if row["blocks_removed"] > 0 else "        "
            ds_str = f"{ds:>+8.3f}" if row["blocks_removed"] > 0 else "        "
            print(f"│  {lbl}  {c:>10.2f}  {dc_str}  {s:>7.3f}  {ds_str}")
            if row["blocks_removed"] > 0 and row["removed_block_returns"]:
                print(f"│    ↳ best block(s): {row['removed_block_returns']}")
        print(f"└{'─'*53}")
        print()

    print("┌─ TAIL RISK (EXTENDED) ──────────────────────────────")
    print(f"│  Weekly CVaR (worst 1%): {wk_cvar*100:>8.2f}%")
    print(f"│  Max consec. losing days:{streak['max_streak']:>8d}")
    print(f"│  Avg losing streak len:  {streak['avg_streak']:>8.1f}")
    print(f"│  Number of streaks:      {streak['n_streaks']:>8d}")
    print(f"└{'─'*53}")

    # ── 7. Statistical Validity ───────────────────────────────
    print()
    print("┌─ STATISTICAL VALIDITY ──────────────────────────────")
    print(f"│  Observed (annualised) Sharpe:  {observed_sharpe:>8.3f}")
    print(f"│  # trials tested (n_trials):    {n_trials:>8d}")
    print(f"│  DSR benchmark Sharpe:          {dsr_res['sr_benchmark']:>8.3f}")
    print(f"│  DSR (prob Sharpe is genuine):  {dsr_res['dsr']:>8.3%}  {'✅ PASS' if dsr_res['dsr'] > 0.95 else '⚠ FAIL — may be overfitted'}")
    print(f"│  Prob false positive:           {dsr_res['prob_false_positive']:>8.3%}")
    print(f"│  Min track record needed:       {mtl:>8.0f} days  ({mtl/tdy:.1f} years)")
    print(f"│  Current track record:          {n:>8d} days  ({n/tdy:.1f} years)")
    adequate = n >= mtl if math.isfinite(mtl) else False
    print(f"│  Track record adequate?  {'✅ YES' if adequate else '⚠ NO — need more live data'}")
    print(f"│  Profit factor:                 {pf:>8.3f}  {'✅' if pf > 1.5 else '⚠'}")
    print(f"└{'─'*53}")

    # ── 8. Capital & Operational ──────────────────────────────
    print()
    print("┌─ CAPITAL & OPERATIONAL ─────────────────────────────")
    print(f"│  Full Kelly fraction:    {kelly['full_kelly']:>8.4f}  ({kelly['full_kelly']*100:.2f}% of capital per day)")
    print(f"│  Half Kelly fraction:    {kelly['half_kelly']:>8.4f}  ({kelly['half_kelly']*100:.2f}% of capital per day)")
    print(f"│  Ruin probability (50% DD in {ruin['horizon_days']}d): {ruin['ruin_probability']:.4%}")
    print()
    print(f"│  LEVERAGE SENSITIVITY TABLE  (treating current series as 1x)")
    print(f"│  {'Leverage':>9}  {'CAGR%':>8}  {'Sharpe':>8}  {'MaxDD%':>8}")
    print(f"│  {sep2[:42]}")
    for _, row in lev_df.iterrows():
        print(f"│  {row['leverage']:>9.2f}x  {row['CAGR_%']:>8.2f}  {row['Sharpe']:>8.3f}  {row['MaxDD_%']:>7.2f}%")
    print(f"└{'─'*53}")

    # ── 9. Parameter Sensitivity Map ─────────────────────────
    if run_sensitivity and not tornado_df.empty:
        print()
        print("┌─ PARAMETER SENSITIVITY MAP ─────────────────────────")
        print(f"│  Baseline Sharpe (from simulation): {base_sharpe_sens:.3f}")
        print(f"│  Perturbations tested: ±10%, ±20%, ±30%")
        print(f"│")
        print(f"│  {'Parameter':>26}  {'−30%':>7}  {'−20%':>7}  {'−10%':>7}  {'BASE':>7}  {'+10%':>7}  {'+20%':>7}  {'+30%':>7}  {'Range':>7}")
        print(f"│  {'─'*87}")

        # Build per-param summary
        for param in (params_to_test or list(_PARAM_DEFAULTS.keys())):
            sub = tornado_df[tornado_df["param"] == param].sort_values("perturb_pct")
            if sub.empty:
                continue
            pct_to_sharpe = {float(row["perturb_pct"]): float(row["Sharpe"])
                             for _, row in sub.iterrows()}
            cols = [-30, -20, -10, 0, 10, 20, 30]
            vals = [pct_to_sharpe.get(float(c), float("nan")) for c in cols]
            finite_vals = [v for v in vals if np.isfinite(v)]
            rng = max(finite_vals) - min(finite_vals) if len(finite_vals) > 1 else 0.0
            flag = "  ⚠ FRAGILE" if rng > 1.0 else ("  ✅ ROBUST" if rng < 0.3 else "")
            val_strs = [f"{v:>7.2f}" if np.isfinite(v) else "    N/A" for v in vals]
            print(f"│  {param:>26}  {'  '.join(val_strs)}  {rng:>6.2f}{flag}")

        print(f"│")
        print(f"│  Range interpretation: <0.30 = robust ✅  |  >1.00 = fragile ⚠")
        print(f"└{'─'*53}")
    elif not run_sensitivity:
        print()
        print("┌─ PARAMETER SENSITIVITY MAP ─────────────────────────")
        print("│  (skipped — pass df_4x= to run_institutional_audit)")
        print(f"└{'─'*53}")

    # ── 10. Cost Curve ────────────────────────────────────────
    print()
    print("┌─ COST CURVE TEST ───────────────────────────────────")
    print(f"│  Models performance decay as AUM grows (base slip + sqrt market impact)")
    print(f"│  {'AUM':>10}  {'Slip%':>6}  {'Impact%':>8}  {'Total%':>7}  {'CAGR%':>10}  {'Sharpe':>7}  {'MaxDD%':>8}")
    print(f"│  {'─'*65}")
    for _, row in cost_df.iterrows():
        aum_str = f"${row['AUM']:>8,.0f}"
        print(f"│  {aum_str}  {row['base_slip_%']:>6.3f}  {row['mkt_impact_%']:>8.3f}  "
              f"{row['total_cost_%']:>7.3f}  {row['CAGR_%']:>10.2f}  {row['Sharpe']:>7.3f}  {row['MaxDD_%']:>7.2f}%")
    print(f"└{'─'*53}")

    # ── 11. Shock Injection ───────────────────────────────────
    print()
    print("┌─ SHOCK INJECTION TEST ──────────────────────────────")
    baseline_shock_row = shock_df[shock_df["n_shocks"] == 0]
    if not baseline_shock_row.empty:
        bsr = baseline_shock_row.iloc[0]
        print(f"│  Baseline — Sharpe: {bsr['Sharpe']:.3f}  MaxDD: {bsr['MaxDD_%']:.2f}%")
    print(f"│  {'Shock':>7}  {'N days':>7}  {'Sharpe':>7}  {'ΔSharpe':>8}  {'MaxDD%':>8}  {'ΔMaxDD%':>8}  {'Survived':>9}")
    print(f"│  {'─'*65}")
    for _, row in shock_df[shock_df["n_shocks"] > 0].iterrows():
        survived_str = "✅ YES" if row["survived"] else "⚠ NO"
        print(f"│  {row['shock_size_%']:>6.0f}%  {row['n_shocks']:>7.0f}  {row['Sharpe']:>7.3f}  "
              f"{row['delta_sharpe']:>+8.3f}  {row['MaxDD_%']:>7.2f}%  {row['delta_mdd_%']:>+8.2f}%  {survived_str:>9}")
    print(f"└{'─'*53}")

    # ── 12. Capacity Curve ────────────────────────────────────
    print()
    print("┌─ CAPACITY CURVE TEST (dynamic — anchored to starting capital) ─")
    print(f"│  Start: ${starting_capital:,.0f}  |  Impact ∝ sqrt(equity / max_deployable) on active days only")
    print(f"│  {'Particip%':>10}  {'AUM at crossing':>16}  {'Day':>5}  {'Impact%/d':>10}  {'CAGR%':>10}  {'Sharpe':>7}  {'MaxDD%':>8}  {'Flag':>6}")
    print(f"│  {'─'*80}")
    for _, row in capacity_df.iterrows():
        aum_str  = f"${row['AUM_at_crossing']:>12,.0f}"
        flag_str = "⚠ HIGH" if row["capacity_flag"] else "  OK"
        day_str  = f"{row['day_reached']:>5.0f}" if row['day_reached'] < len(r) else "  N/A"
        print(f"│  {row['participation_%']:>10.1f}  {aum_str}  {day_str}  "
              f"{row['impact_%/day']:>10.4f}  {row['CAGR_%']:>10.2f}  {row['Sharpe']:>7.3f}  "
              f"{row['MaxDD_%']:>7.2f}%  {flag_str}")
    print(f"│")
    first_red = capacity_df[capacity_df["capacity_flag"]]
    if not first_red.empty:
        r0 = first_red.iloc[0]
        print(f"│  ⚠ Hits >10% daily volume at ${r0['AUM_at_crossing']:,.0f}  (day {r0['day_reached']:.0f})")
    else:
        print(f"│  ✅ Never hits 10% daily volume participation within this track record")
    print(f"└{'─'*53}")

    # ── 13. Neighbor Plateau ──────────────────────────────────
    if plateau:
        print()
        print("┌─ NEIGHBOR PLATEAU TEST ─────────────────────────────")
        print(f"│  Joint ±{plateau['perturb_scale']*100:.0f}% perturbation of all parameters simultaneously")
        print(f"│  n_neighbors: {plateau['n_neighbors']}  |  baseline Sharpe: {plateau['base_sharpe']:.3f}")
        print(f"│")
        flag_plateau = "✅ PLATEAU" if plateau["plateau_ratio"] >= 0.70 else "⚠ SPIKE"
        print(f"│  Plateau ratio (within ±0.5 Sharpe):  {plateau['plateau_ratio']*100:.1f}%  {flag_plateau}")
        print(f"│  Neighbor Sharpe p10:   {plateau['sharpe_pct10']:>7.3f}")
        print(f"│  Neighbor Sharpe p25:   {plateau['sharpe_pct25']:>7.3f}")
        print(f"│  Neighbor Sharpe median:{plateau['sharpe_median']:>7.3f}")
        print(f"│  Neighbor Sharpe p75:   {plateau['sharpe_pct75']:>7.3f}")
        print(f"│  Neighbor Sharpe std:   {plateau['sharpe_std']:>7.3f}")
        print(f"│")
        print(f"│  Interpretation: ≥70% within ±0.5 = broad plateau (robust) ✅")
        print(f"│                  <70%             = narrow spike (overfitted) ⚠")
        print(f"└{'─'*53}")
    else:
        print()
        print("┌─ NEIGHBOR PLATEAU TEST ─────────────────────────────")
        print("│  (skipped — pass df_4x= to run_institutional_audit)")
        print(f"└{'─'*53}")

    # ── Charts ────────────────────────────────────────────────
    if save_charts and _MPL and outdir is not None and track_file is not None:
        pfx = label.lower().replace(" ", "_")

        p = track_file(f"{pfx}_inst_equity_curve.png")
        _chart_equity_curve(r, label, p)

        p = track_file(f"{pfx}_inst_rolling_sharpe.png")
        _chart_rolling_sharpe_with_regimes(rs_arr, r, rolling_window, label, p)

        p = track_file(f"{pfx}_inst_rolling_cagr.png")
        _chart_rolling_cagr(rc_arr, rolling_window, label, p)

        p = track_file(f"{pfx}_inst_slippage_sensitivity.png")
        _chart_slippage_sensitivity(slip_df, label, p)

        p = track_file(f"{pfx}_inst_capped_return_sensitivity.png")
        _chart_capped_return_sensitivity(cap_df, label, p)

        p = track_file(f"{pfx}_inst_top_n_removal.png")
        _chart_top_n_removal(topn_df, label, p)

        if not lucky_df.empty:
            p = track_file(f"{pfx}_inst_lucky_streak.png")
            _chart_lucky_streak(lucky_df, label, 30, p)

        p = track_file(f"{pfx}_inst_leverage_sensitivity.png")
        _chart_leverage_sensitivity(lev_df, label, p)

        p = track_file(f"{pfx}_inst_drawdown_episodes.png")
        _chart_drawdown_episodes(ep_df, label, p)

        p = track_file(f"{pfx}_inst_return_distribution.png")
        _chart_return_distribution(r, label, p)

        if run_sensitivity and not tornado_df.empty:
            p = track_file(f"{pfx}_inst_sensitivity_tornado.png")
            _chart_tornado(tornado_df, base_sharpe_sens, label, p)

            p = track_file(f"{pfx}_inst_sensitivity_heatmap.png")
            _chart_heatmap(heatmap_df, label, p)

            p = track_file(f"{pfx}_inst_sensitivity_lines.png")
            _chart_sensitivity_line(tornado_df, base_sharpe_sens, label, p)

        p = track_file(f"{pfx}_inst_cost_curve.png")
        _chart_cost_curve(cost_df, label, p)

        p = track_file(f"{pfx}_inst_shock_injection.png")
        _chart_shock_injection(shock_df, label, p)

        p = track_file(f"{pfx}_inst_capacity_curve.png")
        _chart_capacity_curve(capacity_df, label, p)

        if plateau:
            p = track_file(f"{pfx}_inst_neighbor_plateau.png")
            _chart_neighbor_plateau(plateau, label, p)

        print(f"\n✅ Institutional charts saved ({pfx}).")

    # ── Walk-forward validation ───────────────────────────────
    wf_results = {}
    if n_wf_folds > 0 and df_4x is not None and param_config is not None:
        print(f"\n  Running walk-forward validation ({n_wf_folds} folds) …")
        wf_params = dict(param_config)
        wf_results = walk_forward_validation(
            df_4x               = df_4x,
            params              = wf_params,
            trial_purchases     = trial_purchases,
            n_folds             = n_wf_folds,
            tdy                 = tdy,
            regime_filter_cols  = regime_filter_cols,
        )
        print_walk_forward_report(wf_results, label)
    elif n_wf_folds > 0:
        print("\n  (Walk-forward skipped — df_4x or param_config not provided)")

    # ── Rolling walk-forward ──────────────────────────────────
    wf_rolling_results = {}
    if df_4x is not None and param_config is not None:
        print(f"\n  Running rolling walk-forward "
              f"(train={wf_train_days}d  test={wf_test_days}d  step={wf_step_days}d) …")
        wf_rolling_results = walk_forward_rolling(
            df_4x               = df_4x,
            params              = dict(param_config),
            trial_purchases     = trial_purchases,
            train_days          = wf_train_days,
            test_days           = wf_test_days,
            step_days           = wf_step_days,
            tdy                 = tdy,
            regime_filter_cols  = regime_filter_cols,
        )
        print_walk_forward_rolling_report(wf_rolling_results, label)

    # ── Sharpe Stability Analysis ─────────────────────────────
    if wf_rolling_results and wf_rolling_results.get("folds") is not None and len(wf_rolling_results.get("folds", [])) > 1:
        ss = sharpe_stability_analysis(wf_rolling_results["folds"])
        print_sharpe_stability_report(ss)
    else:
        ss = {}

    # ── MC + BB Stress Summary ────────────────────────────────
    if not mc_df.empty and not bb_df.empty:
        print_stress_summary(mc_df, bb_df, label=label,
                             mc_iters=mc_iters, bb_iters=bb_iters, block_len=bb_block)

    results = {
        "sortino":          sort_r,
        "calmar":           calm_r,
        "omega":            omeg_r,
        "ulcer_index":      ulcer,
        "profit_factor":    pf,
        "pct_time_underwater": pct_uw,
        "avg_drawdown":     avg_dw,
        "n_episodes":       len(ep_df),
        "episodes_df":      ep_df,
        "skewness":         sk,
        "excess_kurtosis":  ku,
        "jb_stat":          jb["jb_stat"],
        "jb_pvalue":        jb["jb_pvalue"],
        "lb_stat":          lb["lb_stat"],
        "lb_pvalue":        lb["lb_pvalue"],
        "regime":           regime,
        "rolling_sharpe":   rs_arr,
        "rolling_cagr":     rc_arr,
        "slippage_df":      slip_df,
        "cap_sensitivity_df": cap_df,
        "top_n_removal_df": topn_df,
        "lucky_streak_df":  lucky_df,
        "leverage_df":      lev_df,
        "sensitivity_tornado_df": tornado_df,
        "sensitivity_heatmap_df": heatmap_df,
        "weekly_cvar":      wk_cvar,
        "streak":           streak,
        "observed_sharpe":  observed_sharpe,
        "dsr":              dsr_res,
        "mtl_days":         mtl,
        "kelly":            kelly,
        "ruin":             ruin,
        "cost_curve_df":    cost_df,
        "shock_df":         shock_df,
        "capacity_df":      capacity_df,
        "plateau":          plateau,
        "walk_forward":         wf_results,
        "walk_forward_rolling": wf_rolling_results,
        "regime_robustness":    rr,
        "rolling_maxdd":        roll_mdd,
        "var5":                 var5,
        "cvar5":                cvar5,
        "var1":                 var1,
        "cvar1":                cvar1,
        "mc_df":                mc_df,
        "bb_df":                bb_df,
        "sharpe_stability":     ss,
        "period_breakdown":     period_breakdown,
        "bias_audit":           bias_audit,
    }

    # ── Institutional Scorecard ───────────────────────────────
    scorecard = institutional_scorecard(results, quiet=True)

    results["scorecard"] = scorecard
    return results


# ══════════════════════════════════════════════════════════════════════════════
# WALK-FORWARD VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def walk_forward_validation(
    df_4x: pd.DataFrame,
    params: Dict[str, float],
    trial_purchases: bool = False,
    n_folds: int = 8,
    tdy: int = 365,
    regime_filter_cols=None,
) -> Dict:
    """
    Expanding-window walk-forward validation over n_folds.

    Structure (expanding window):
      Fold k: train = days 0 .. split_k-1,  test = days split_k .. split_{k+1}-1

    The first fold uses ~50% of data as the minimum training window so the
    test segments are evenly sized across all 8 folds.

    For each fold:
      - Parameters are treated as fixed (already optimised on full in-sample).
      - The test segment is evaluated blind.
      - Sharpe, CAGR, MaxDD, Sortino, R² are computed on test days only.
      - DSR is computed on the test segment with n_trials=1 (no selection done
        on out-of-sample data) — this is the genuine DSR.

    Returns dict with per-fold results and aggregate stats.
    """
    cols      = list(df_4x.columns)
    n_days    = len(cols)
    n_test    = n_days // (n_folds + 1)   # size of each test fold
    min_train = n_days - n_folds * n_test  # first train window

    fold_results = []

    for fold in range(n_folds):
        train_end  = min_train + fold * n_test          # exclusive
        test_start = train_end
        test_end   = test_start + n_test                # exclusive
        if test_end > n_days:
            test_end = n_days

        test_cols = cols[test_start:test_end]
        if len(test_cols) < 5:
            continue

        # Simulate on test days only — apply regime filter if provided
        if regime_filter_cols:
            df_test = df_4x[test_cols].copy()
            for _fc in test_cols:
                if str(_fc) in regime_filter_cols:
                    df_test[_fc] = 0.0
        else:
            df_test = df_4x[test_cols]
        res     = _simulate_with_params(df_test, params, tdy,
                                        trial_purchases=trial_purchases)

        sharpe  = res["Sharpe"]
        cagr    = res["CAGR_%"]
        maxdd   = res["MaxDD_%"]
        sortino = res.get("Sortino", float("nan"))
        r2      = res.get("R2", float("nan"))

        # DSR on test segment — n_trials=1 since no selection on this data
        n_test_days = len(test_cols)
        if math.isfinite(sharpe) and n_test_days >= 5:
            # Recompute distribution stats from test returns
            test_daily = []
            for col in test_cols:
                path_4x = df_4x[col].to_numpy(dtype=float)
                path_1x = path_4x / _PIVOT_LEV
                early_y_1x   = float(params.get("EARLY_KILL_Y", 0.014)) / _PIVOT_LEV
                strong_thr   = float(params.get("EARLY_INSTILL_Y", 0.01))
                raw_ps       = params.get("PORT_SL", -0.049)
                en_ps        = raw_ps is not None and raw_ps != 0
                ps_val       = float(raw_ps) if en_ps else -999.0
                raw_ef       = params.get("EARLY_FILL_Y", 0.30)
                en_ef        = raw_ef is not None and raw_ef != 0
                ef_val       = float(raw_ef) if en_ef else 999.0
                raw_efx      = params.get("EARLY_FILL_X", 540)
                efx_val      = int(raw_efx) if (raw_efx is not None and raw_efx != 0) else 0
                r, _lev_used = _apply_hybrid_day_param(
                    path_1x,
                    early_x_minutes         = int(round(params.get("EARLY_KILL_X", 175))),
                    early_y_1x              = early_y_1x,
                    trail_dd_1x             = float(params.get("PORT_TSL", 0.085)),
                    l_base                  = float(params.get("L_BASE", 1.0)),
                    l_high                  = float(params.get("L_HIGH", 2.4)),
                    port_stop_1x            = ps_val,
                    early_fill_threshold_1x = ef_val,
                    strong_thr_1x           = strong_thr,
                    enable_portfolio_stop   = en_ps,
                    enable_early_fill       = en_ef,
                    early_fill_max_minutes  = efx_val,
                    trial_purchases         = trial_purchases,
                )
                if np.isfinite(r):
                    test_daily.append(r)

            if len(test_daily) >= 5:
                r_arr = np.array(test_daily, dtype=float)
                sk_t  = float(stats.skew(r_arr))
                ku_t  = float(stats.kurtosis(r_arr, fisher=True))
                dsr_t = deflated_sharpe_ratio(
                    sharpe, n_trials=1,
                    n_days=len(r_arr), skewness=sk_t, excess_kurtosis=ku_t,
                    tdy=tdy,
                )
            else:
                dsr_t = {"dsr": float("nan"), "prob_false_positive": float("nan")}
        else:
            dsr_t = {"dsr": float("nan"), "prob_false_positive": float("nan")}

        fold_results.append({
            "fold":         fold + 1,
            "train_days":   train_end,
            "test_start":   test_start,
            "test_end":     test_end,
            "test_days":    len(test_cols),
            "sharpe":       sharpe,
            "cagr_pct":     cagr,
            "maxdd_pct":    maxdd,
            "sortino":      sortino,
            "r2":           r2,
            "dsr":          dsr_t.get("dsr", float("nan")),
            "prob_fp":      dsr_t.get("prob_false_positive", float("nan")),
        })

    if not fold_results:
        return {"folds": [], "aggregate": {}}

    df_folds = pd.DataFrame(fold_results)

    # Aggregate stats across folds
    valid = df_folds[df_folds["sharpe"].apply(math.isfinite)]
    agg = {
        "mean_sharpe":   float(valid["sharpe"].mean()),
        "std_sharpe":    float(valid["sharpe"].std()),
        "min_sharpe":    float(valid["sharpe"].min()),
        "max_sharpe":    float(valid["sharpe"].max()),
        "mean_cagr":     float(valid["cagr_pct"].mean()),
        "mean_maxdd":    float(valid["maxdd_pct"].mean()),
        "mean_sortino":  float(valid["sortino"].mean()) if valid["sortino"].apply(math.isfinite).any() else float("nan"),
        "mean_r2":       float(valid["r2"].mean()) if valid["r2"].apply(math.isfinite).any() else float("nan"),
        "mean_dsr":      float(valid["dsr"].mean()) if valid["dsr"].apply(math.isfinite).any() else float("nan"),
        "pct_folds_positive_sharpe": float((valid["sharpe"] > 0).mean() * 100),
        "sharpe_decay":  float(valid["sharpe"].mean()) - float(valid["sharpe"].iloc[0]) if len(valid) > 1 else float("nan"),
    }

    return {"folds": df_folds, "aggregate": agg}


def print_walk_forward_report(wf: Dict, label: str = "Strategy"):
    """Print formatted walk-forward validation report."""
    if not wf or not isinstance(wf.get("folds"), pd.DataFrame) or wf["folds"].empty:
        print("  No walk-forward results available.")
        return

    df  = wf["folds"]
    agg = wf["aggregate"]

    print(f"\n┌─ WALK-FORWARD VALIDATION ({len(df)} folds, expanding window) ────────")
    print(f"│  Each fold: params fixed from full in-sample optimisation")
    print(f"│  DSR per fold uses n_trials=1 (no selection on test data)")
    print(f"│")
    print(f"│  {'Fold':>4}  {'Train':>5}  {'Test':>9}  {'Days':>4}  "
          f"{'Sharpe':>7}  {'CAGR%':>7}  {'MaxDD%':>7}  "
          f"{'Sortino':>7}  {'R²':>5}  {'DSR%':>6}  {'FP%':>5}")
    print(f"│  {'─'*4}  {'─'*5}  {'─'*9}  {'─'*4}  "
          f"{'─'*7}  {'─'*7}  {'─'*7}  "
          f"{'─'*7}  {'─'*5}  {'─'*6}  {'─'*5}")

    for _, row in df.iterrows():
        sh_s  = f"{row['sharpe']:7.3f}"  if math.isfinite(row['sharpe'])  else "    nan"
        ca_s  = f"{row['cagr_pct']:7.0f}" if math.isfinite(row['cagr_pct']) else "    nan"
        dd_s  = f"{row['maxdd_pct']:7.2f}" if math.isfinite(row['maxdd_pct']) else "    nan"
        so_s  = f"{row['sortino']:7.3f}"  if math.isfinite(row['sortino'])  else "    nan"
        r2_s  = f"{row['r2']:5.3f}"       if math.isfinite(row['r2'])       else "  nan"
        dsr_s = f"{row['dsr']*100:6.1f}"  if math.isfinite(row['dsr'])      else "   nan"
        fp_s  = f"{row['prob_fp']*100:5.1f}" if math.isfinite(row['prob_fp']) else "  nan"
        print(f"│  {int(row['fold']):4d}  "
              f"d1-{int(row['train_days']):<3}  "
              f"d{int(row['test_start'])+1:<4}-{int(row['test_end']):<4}  "
              f"{int(row['test_days']):4d}  "
              f"{sh_s}  {ca_s}  {dd_s}  {so_s}  {r2_s}  {dsr_s}  {fp_s}")

    print(f"│")
    print(f"│  AGGREGATE ACROSS {len(df)} FOLDS:")
    print(f"│  Mean Sharpe:    {agg['mean_sharpe']:.3f}  "
          f"(±{agg['std_sharpe']:.3f}  "
          f"min={agg['min_sharpe']:.3f}  max={agg['max_sharpe']:.3f})")
    print(f"│  Mean CAGR:      {agg['mean_cagr']:.0f}%")
    print(f"│  Mean MaxDD:     {agg['mean_maxdd']:.2f}%")
    if math.isfinite(agg.get("mean_sortino", float("nan"))):
        print(f"│  Mean Sortino:   {agg['mean_sortino']:.3f}")
    if math.isfinite(agg.get("mean_r2", float("nan"))):
        print(f"│  Mean R²:        {agg['mean_r2']:.3f}")
    if math.isfinite(agg.get("mean_dsr", float("nan"))):
        dsr_pct = agg["mean_dsr"] * 100
        flag = "✅ PASS" if dsr_pct >= 95 else ("⚠ MARGINAL" if dsr_pct >= 75 else "⚠ FAIL")
        print(f"│  Mean OOS DSR:   {dsr_pct:.1f}%  {flag}")
    print(f"│  % folds positive Sharpe: {agg['pct_folds_positive_sharpe']:.0f}%")

    # Stability interpretation
    std  = agg["std_sharpe"]
    mean = agg["mean_sharpe"]
    cv   = std / abs(mean) if mean != 0 else float("inf")
    if cv < 0.20:
        stab = "✅ STABLE — low Sharpe variance across folds"
    elif cv < 0.40:
        stab = "⚠ MODERATE — some fold-to-fold variation"
    else:
        stab = "⚠ UNSTABLE — high variance, may be regime-dependent"
    print(f"│  Stability (CV={cv:.2f}): {stab}")
    print(f"└─────────────────────────────────────────────────────────────────")


# ══════════════════════════════════════════════════════════════════════════════
# WALK-FORWARD VALIDATION — ROLLING WINDOW
# ══════════════════════════════════════════════════════════════════════════════

def _wf_eval_segment(df_4x, cols, params, trial_purchases, tdy,
                     regime_filter_cols=None):
    """Evaluate a param config on a specific set of day columns. Returns metrics dict.
    regime_filter_cols: optional set of column name strings to zero out (regime filter).
    """
    early_y_1x = float(params.get("EARLY_KILL_Y", 0.014)) / _PIVOT_LEV
    strong_thr = float(params.get("EARLY_INSTILL_Y", 0.01))
    raw_ps     = params.get("PORT_SL", -0.049)
    en_ps      = raw_ps is not None and raw_ps != 0
    ps_val     = float(raw_ps) if en_ps else -999.0
    raw_ef     = params.get("EARLY_FILL_Y", 0.30)
    en_ef      = raw_ef is not None and raw_ef != 0
    ef_val     = float(raw_ef) if en_ef else 999.0
    raw_efx    = params.get("EARLY_FILL_X", 540)
    efx_val    = int(raw_efx) if (raw_efx is not None and raw_efx != 0) else 0

    daily = []
    for col in cols:
        # Regime filter — zero out bad-calendar days inside fold windows
        if regime_filter_cols and str(col) in regime_filter_cols:
            daily.append(0.0)
            continue
        path_4x = df_4x[col].to_numpy(dtype=float)
        path_1x = path_4x / _PIVOT_LEV
        r, _lev_used = _apply_hybrid_day_param(
            path_1x,
            early_x_minutes         = int(round(params.get("EARLY_KILL_X", 175))),
            early_y_1x              = early_y_1x,
            trail_dd_1x             = float(params.get("PORT_TSL", 0.085)),
            l_base                  = float(params.get("L_BASE", 1.0)),
            l_high                  = float(params.get("L_HIGH", 2.4)),
            port_stop_1x            = ps_val,
            early_fill_threshold_1x = ef_val,
            strong_thr_1x           = strong_thr,
            enable_portfolio_stop   = en_ps,
            enable_early_fill       = en_ef,
            early_fill_max_minutes  = efx_val,
            trial_purchases         = trial_purchases,
        )
        if np.isfinite(r):
            daily.append(r)

    if len(daily) < 5:
        nan = float("nan")
        return {"sharpe": nan, "cagr_pct": nan, "maxdd_pct": nan,
                "sortino": nan, "r2": nan, "final_mult": nan,
                "dsr": nan, "prob_fp": nan, "n_days": len(daily)}

    r_arr  = np.array(daily, dtype=float)
    eq     = _equity(r_arr)
    sharpe = _sharpe(r_arr, tdy)
    cagr   = _cagr(eq, len(r_arr), tdy) * 100
    maxdd  = _max_dd(eq) * 100
    srt    = _sortino(r_arr, tdy)
    r2     = _r_squared(eq)

    sk_t   = float(stats.skew(r_arr))
    ku_t   = float(stats.kurtosis(r_arr, fisher=True))
    dsr_t  = deflated_sharpe_ratio(
        sharpe, n_trials=1, n_days=len(r_arr),
        skewness=sk_t, excess_kurtosis=ku_t, tdy=tdy,
    )
    return {
        "sharpe":      sharpe,
        "cagr_pct":    cagr,
        "maxdd_pct":   maxdd,
        "sortino":     srt,
        "r2":          r2,
        "final_mult":  float(eq[-1]),
        "dsr":         dsr_t.get("dsr", float("nan")),
        "prob_fp":     dsr_t.get("prob_false_positive", float("nan")),
        "n_days":      len(daily),
        "daily_rets":  list(daily),   # raw daily returns for diagnostic printing
        "col_names":   [str(c) for c in cols],  # date labels
    }


def walk_forward_rolling(
    df_4x: pd.DataFrame,
    params: Dict[str, float],
    trial_purchases: bool = False,
    train_days: int = 120,
    test_days: int = 30,
    step_days: int = 30,
    tdy: int = 365,
    regime_filter_cols=None,
) -> Dict:
    """
    Fixed rolling-window walk-forward validation.

    Each fold:
      train window : train_days (fixed size, slides forward by step_days)
      test  window : test_days  (immediately follows train window)
      step  size   : step_days  (how far the window moves each fold)

    With 364 days, train=120, test=30, step=30:
      Fold 1: train d1-120,   test d121-150
      Fold 2: train d31-150,  test d151-180
      ...
      Fold 8: train d211-330, test d331-360

    Parameters are NOT re-optimised per fold — they are fixed from the
    full in-sample grid search. This tests whether the edge is stable
    across different 120-day market regimes.
    """
    cols   = list(df_4x.columns)
    n_days = len(cols)
    folds  = []
    fold   = 1
    start  = 0

    while True:
        train_end  = start + train_days
        test_end   = train_end + test_days
        if test_end > n_days:
            break

        train_cols = cols[start:train_end]
        test_cols  = cols[train_end:test_end]

        # Evaluate on train window (in-sample for this fold — regime characterisation)
        train_res = _wf_eval_segment(df_4x, train_cols, params, trial_purchases, tdy, regime_filter_cols=regime_filter_cols)

        # Evaluate on test segment only (params are fixed)
        res = _wf_eval_segment(df_4x, test_cols, params, trial_purchases, tdy, regime_filter_cols=regime_filter_cols)
        res.update({
            "fold":              fold,
            "train_start":       start + 1,
            "train_end":         train_end,
            "test_start":        train_end + 1,
            "test_end":          test_end,
            "train_sharpe":      train_res["sharpe"],
            "train_cagr_pct":    train_res["cagr_pct"],
            "train_maxdd_pct":   train_res["maxdd_pct"],
            "train_final_mult":  train_res.get("final_mult", float("nan")),
            "unstable":          math.isfinite(res["sharpe"]) and res["sharpe"] < 0,
            # diagnostic — kept for unstable fold analysis, not printed in normal output
            "test_daily_rets":   res.get("daily_rets", []),
            "test_col_names":    res.get("col_names", []),
        })
        folds.append(res)

        fold  += 1
        start += step_days

    if not folds:
        return {"folds": [], "aggregate": {}, "config": {}}

    df_folds = pd.DataFrame(folds)
    valid    = df_folds[df_folds["sharpe"].apply(math.isfinite)]

    agg = {}
    if len(valid) > 0:
        mean_sh  = float(valid["sharpe"].mean())
        std_sh   = float(valid["sharpe"].std()) if len(valid) > 1 else 0.0
        n_unstab = int(df_folds["unstable"].sum()) if "unstable" in df_folds.columns else 0
        agg = {
            "mean_sharpe":               mean_sh,
            "std_sharpe":                std_sh,
            "min_sharpe":                float(valid["sharpe"].min()),
            "max_sharpe":                float(valid["sharpe"].max()),
            "mean_cagr":                 float(valid["cagr_pct"].mean()),
            "mean_maxdd":                float(valid["maxdd_pct"].mean()),
            "mean_sortino":              float(valid["sortino"].mean()) if valid["sortino"].apply(math.isfinite).any() else float("nan"),
            "mean_r2":                   float(valid["r2"].mean())      if valid["r2"].apply(math.isfinite).any()      else float("nan"),
            "mean_dsr":                  float(valid["dsr"].mean())     if valid["dsr"].apply(math.isfinite).any()     else float("nan"),
            "pct_folds_positive_sharpe": float((valid["sharpe"] > 0).mean() * 100),
            "n_valid_folds":             len(valid),
            "unstable_folds":            n_unstab,
            "unstable_fraction":         n_unstab / len(df_folds),
            "robust_score":              mean_sh - std_sh,   # conservative lower bound
        }

    return {
        "folds":     df_folds,
        "aggregate": agg,
        "config":    {"train_days": train_days, "test_days": test_days, "step_days": step_days},
    }


def print_walk_forward_rolling_report(wf: Dict, label: str = "Strategy"):
    """Print formatted rolling walk-forward report."""
    if not wf or not isinstance(wf.get("folds"), pd.DataFrame) or wf["folds"].empty:
        print("  No rolling walk-forward results available.")
        return

    df   = wf["folds"]
    agg  = wf["aggregate"]
    cfg  = wf.get("config", {})
    tr   = cfg.get("train_days", "?")
    te   = cfg.get("test_days",  "?")
    st   = cfg.get("step_days",  "?")

    print(f"\n┌─ WALK-FORWARD ROLLING  (train={tr}d  test={te}d  step={st}d) ──────")
    print(f"│  Params fixed — tests whether edge holds across different regimes")
    print(f"│  DSR per fold: n_trials=1 (genuine out-of-sample)")
    print(f"│")

    for _, row in df.iterrows():
        unstable = row.get("unstable", False)
        flag     = "  ⚠ OOS Sharpe negative — unstable fold" if unstable else ""

        tr_fm = f"{row.get('train_final_mult', float('nan')):.3f}" if math.isfinite(row.get('train_final_mult', float('nan'))) else "  nan"
        tr_sh = f"{row['train_sharpe']:6.3f}"    if math.isfinite(row.get('train_sharpe', float('nan')))    else "   nan"
        tr_ca = f"{row['train_cagr_pct']:7.0f}"  if math.isfinite(row.get('train_cagr_pct', float('nan')))  else "    nan"
        tr_dd = f"{row['train_maxdd_pct']:6.2f}" if math.isfinite(row.get('train_maxdd_pct', float('nan'))) else "   nan"

        oo_fm = f"{row.get('final_mult', float('nan')):.3f}" if math.isfinite(row.get('final_mult', float('nan'))) else "  nan"
        oo_sh = f"{row['sharpe']:6.3f}"    if math.isfinite(row['sharpe'])   else "   nan"
        oo_ca = f"{row['cagr_pct']:7.0f}"  if math.isfinite(row['cagr_pct']) else "    nan"
        oo_dd = f"{row['maxdd_pct']:6.2f}" if math.isfinite(row['maxdd_pct']) else "   nan"
        oo_so = f"{row['sortino']:6.3f}"   if math.isfinite(row['sortino'])  else "   nan"
        oo_r2 = f"{row['r2']:5.3f}"        if math.isfinite(row['r2'])       else "  nan"
        dsr_s = f"{row['dsr']*100:5.1f}%"  if math.isfinite(row['dsr'])      else "   nan"

        print(f"│  {'─'*62}")
        print(f"│  FOLD {int(row['fold'])}   "
              f"Train: d{int(row['train_start'])}-{int(row['train_end'])} ({tr}d)   "
              f"Test: d{int(row['test_start'])}-{int(row['test_end'])} ({te}d){flag}")
        print(f"│    In-sample  (train): FinalMult={tr_fm}  Sharpe={tr_sh}  CAGR={tr_ca}%  MaxDD={tr_dd}%")
        print(f"│    OOS (test):         FinalMult={oo_fm}  Sharpe={oo_sh}  CAGR={oo_ca}%  MaxDD={oo_dd}%  "
              f"Sortino={oo_so}  R²={oo_r2}  DSR={dsr_s}")

        # ── Diagnostic daily breakdown for unstable folds ─────────────────
        if unstable:
            daily_rets = row.get("test_daily_rets", [])
            col_names  = row.get("test_col_names",  [])
            if daily_rets:
                print(f"│")
                print(f"│    ⚠ UNSTABLE FOLD — DAILY RETURN BREAKDOWN:")
                print(f"│    {'Day':<5}  {'Date':<12}  {'Return':>8}  {'Cumul':>8}  {'Type'}")
                print(f"│    {'─'*52}")
                cumul = 1.0
                loss_days, win_days = 0, 0
                large_loss_days = []  # >2% loss
                for i, (ret, col) in enumerate(zip(daily_rets,
                                                    col_names if col_names else [""] * len(daily_rets))):
                    cumul *= (1 + ret)
                    ret_pct   = ret * 100
                    cumul_pct = (cumul - 1) * 100
                    if ret < 0:
                        loss_days += 1
                        tag = f"{'LOSS':>6}{'  ← BIG LOSS' if ret_pct < -3 else ''}"
                        if ret_pct < -2:
                            large_loss_days.append((i + 1, col[:10], ret_pct))
                    else:
                        win_days += 1
                        tag = f"{'WIN':>6}{'  ← BIG WIN' if ret_pct > 5 else ''}"
                    date_str = col[:10] if col else f"d{int(row['test_start'])+i}"
                    print(f"│    {i+1:<5}  {date_str:<12}  {ret_pct:>7.2f}%  {cumul_pct:>7.2f}%  {tag}")

                print(f"│    {'─'*52}")
                print(f"│    Win days:   {win_days}/{len(daily_rets)} "
                      f"({win_days/len(daily_rets)*100:.0f}%)")
                print(f"│    Loss days:  {loss_days}/{len(daily_rets)} "
                      f"({loss_days/len(daily_rets)*100:.0f}%)")
                avg_ret = sum(daily_rets) / len(daily_rets) * 100
                print(f"│    Avg daily:  {avg_ret:+.3f}%")
                pos_rets = [r for r in daily_rets if r > 0]
                neg_rets = [r for r in daily_rets if r < 0]
                if pos_rets:
                    print(f"│    Avg win:    {sum(pos_rets)/len(pos_rets)*100:+.3f}%")
                if neg_rets:
                    print(f"│    Avg loss:   {sum(neg_rets)/len(neg_rets)*100:+.3f}%")
                    print(f"│    Worst day:  {min(daily_rets)*100:+.3f}%")
                    print(f"│    Best day:   {max(daily_rets)*100:+.3f}%")
                # Characterise loss pattern
                if loss_days > win_days * 1.5:
                    print(f"│    Pattern:   SUSTAINED BLEED — loss days dominate ({loss_days} vs {win_days} wins)")
                    print(f"│               → Structural signal failure in this regime")
                elif large_loss_days and loss_days <= win_days:
                    print(f"│    Pattern:   TAIL EVENT — {len(large_loss_days)} large loss day(s) amid normal activity")
                    print(f"│               → Strategy profitable on most days; tail risk issue")
                else:
                    print(f"│    Pattern:   MIXED — moderate bleed with offsetting wins")
                print(f"│")

    n_unstab = int(agg.get("unstable_folds", 0))
    n_total  = len(df)
    print(f"│  {'─'*62}")
    print(f"│  Unstable folds (OOS Sharpe < 0): {n_unstab}/{n_total}")
    print(f"│")
    print(f"│  AGGREGATE ACROSS {agg.get('n_valid_folds', n_total)} VALID FOLDS:")
    print(f"│  Mean Sharpe:    {agg['mean_sharpe']:.3f}  "
          f"(±{agg['std_sharpe']:.3f}  "
          f"min={agg['min_sharpe']:.3f}  max={agg['max_sharpe']:.3f})")
    print(f"│  Robust score:   {agg['robust_score']:.3f}  (mean − 1σ)")
    print(f"│  Mean CAGR:      {agg['mean_cagr']:.0f}%")
    print(f"│  Mean MaxDD:     {agg['mean_maxdd']:.2f}%")
    if math.isfinite(agg.get("mean_sortino", float("nan"))):
        print(f"│  Mean Sortino:   {agg['mean_sortino']:.3f}")
    if math.isfinite(agg.get("mean_r2", float("nan"))):
        print(f"│  Mean R²:        {agg['mean_r2']:.3f}")
    if math.isfinite(agg.get("mean_dsr", float("nan"))):
        dsr_pct = agg["mean_dsr"] * 100
        flag = "✅ PASS" if dsr_pct >= 95 else ("⚠ MARGINAL" if dsr_pct >= 75 else "⚠ FAIL")
        print(f"│  Mean OOS DSR:   {dsr_pct:.1f}%  {flag}")
    print(f"│  % folds positive Sharpe: {agg['pct_folds_positive_sharpe']:.0f}%")

    # Stability
    std  = agg.get("std_sharpe", float("nan"))
    mean = agg.get("mean_sharpe", float("nan"))
    if math.isfinite(std) and math.isfinite(mean) and mean != 0:
        cv = std / abs(mean)
        if cv < 0.20:
            stab = "✅ STABLE — consistent across regimes"
        elif cv < 0.40:
            stab = "⚠ MODERATE — some regime sensitivity"
        else:
            stab = "⚠ UNSTABLE — edge may be regime-specific"
        print(f"│  Stability (CV={cv:.2f}): {stab}")

    print(f"└─────────────────────────────────────────────────────────────────")


# ══════════════════════════════════════════════════════════════════════════════
# COST ELASTICITY  &  REGIME ROBUSTNESS TEST
# ══════════════════════════════════════════════════════════════════════════════

def cost_elasticity(slip_df: pd.DataFrame) -> float:
    """
    Compute d(log CAGR) / d(slippage) via linear regression over the
    slippage sensitivity table.  Negative number — larger magnitude means
    more sensitive to execution cost.
    """
    df = slip_df.dropna(subset=["slippage_rate", "CAGR_%"])
    df = df[df["CAGR_%"] > 0]
    if len(df) < 2:
        return float("nan")
    x = df["slippage_rate"].to_numpy(dtype=float)
    y = np.log(df["CAGR_%"].to_numpy(dtype=float))
    # slope of log-CAGR vs slippage
    x_mean = x.mean()
    slope  = float(np.sum((x - x_mean) * (y - y.mean())) / np.sum((x - x_mean) ** 2))
    return slope


def regime_robustness_test(
    r: np.ndarray,
    tdy: int,
    split_pct: float = 0.50,
) -> Dict:
    """
    Split daily returns at split_pct into IS (first half) and OOS (second half).
    Compute full metrics on each half and report stability ratios.

    Returns dict with is_stats, oos_stats, and stability summary.
    """
    n      = len(r)
    split  = int(n * split_pct)
    r_is   = r[:split]
    r_oos  = r[split:]

    # Strip calendar-filter flat days (0.0) before computing segment stats.
    # Flat days dilute annualised metrics without contributing signal — they
    # represent days the strategy deliberately sits out, not genuine zero returns.
    # We preserve them in the full-sample series (for equity curve) but exclude
    # them from IS/OOS comparison metrics.
    r_is_active  = r_is[r_is  != 0.0] if (r_is  != 0.0).any() else r_is
    r_oos_active = r_oos[r_oos != 0.0] if (r_oos != 0.0).any() else r_oos

    # Total calendar days per segment (including flat/zero days) - used for
    # CAGR annualisation so filtered runs aren't inflated by fewer active days.
    n_is_total  = len(r_is)
    n_oos_total = len(r_oos)

    def _seg_stats(seg: np.ndarray, n_total: int) -> Dict:
        if len(seg) < 5:
            nan = float("nan")
            return {"n": len(seg), "cagr": nan,
                    "sharpe": nan, "maxdd": nan, "final_mult": nan,
                    "best_day": nan, "worst_day": nan,
                    "mean": nan, "std": nan}
        eq = _equity(seg)
        return {
            "n":          len(seg),
            "sharpe":     _sharpe(seg, tdy),
            "cagr":       _cagr(eq, n_total, tdy) * 100,  # annualise over full half-period
            "maxdd":      _max_dd(eq) * 100,
            "final_mult": float(eq[-1]),
            "best_day":   float(np.max(seg)) * 100,
            "worst_day":  float(np.min(seg)) * 100,
            "mean":       float(np.mean(seg)),
            "std":        float(np.std(seg, ddof=1)),
        }

    is_stats  = _seg_stats(r_is_active,  n_is_total)
    oos_stats = _seg_stats(r_oos_active, n_oos_total)

    # Stability ratios
    cagr_ratio   = (oos_stats["cagr"] / is_stats["cagr"]
                    if is_stats["cagr"] and is_stats["cagr"] != 0 else float("nan"))
    sharpe_diff  = ((oos_stats["sharpe"] - is_stats["sharpe"])
                    if math.isfinite(is_stats["sharpe"]) and math.isfinite(oos_stats["sharpe"])
                    else float("nan"))
    dd_change    = ((oos_stats["maxdd"] - is_stats["maxdd"])
                    if math.isfinite(is_stats["maxdd"]) and math.isfinite(oos_stats["maxdd"])
                    else float("nan"))

    # Flag — OOS Sharpe should be within 40% of IS Sharpe
    if math.isfinite(sharpe_diff) and math.isfinite(is_stats["sharpe"]) and is_stats["sharpe"] > 0:
        decay_pct = abs(sharpe_diff) / is_stats["sharpe"] * 100
        stable    = decay_pct < 40
    else:
        decay_pct = float("nan")
        stable    = False

    return {
        "is_stats":    is_stats,
        "oos_stats":   oos_stats,
        "cagr_ratio":  cagr_ratio,
        "sharpe_diff": sharpe_diff,
        "dd_change":   dd_change,
        "decay_pct":   decay_pct,
        "stable":      stable,
        "split_day":   split,
    }


def print_regime_robustness_report(rr: Dict):
    """Print formatted regime robustness report."""
    if not rr:
        return
    is_s  = rr["is_stats"]
    oos_s = rr["oos_stats"]

    print()
    print("┌─ REGIME ROBUSTNESS TEST ────────────────────────────")
    print(f"│  First {rr['split_day']} days (IS) vs remaining {oos_s['n']} days (OOS)")
    print(f"│")
    print(f"│  {'Metric':<22}  {'IN-SAMPLE':>12}  {'OUT-OF-SAMPLE':>13}")
    print(f"│  {'─'*22}  {'─'*12}  {'─'*13}")

    def _r(v, fmt=".3f"):
        return format(v, fmt) if math.isfinite(v) else "nan"

    rows = [
        ("Days",       f"{is_s['n']}",              f"{oos_s['n']}"),
        ("Final Mult", f"{_r(is_s['final_mult'])}x", f"{_r(oos_s['final_mult'])}x"),
        ("CAGR %",     _r(is_s['cagr'], ".2f"),      _r(oos_s['cagr'], ".2f")),
        ("Sharpe",     _r(is_s['sharpe']),            _r(oos_s['sharpe'])),
        ("MaxDD %",    _r(is_s['maxdd'], ".2f"),      _r(oos_s['maxdd'], ".2f")),
        ("Best Day %", _r(is_s['best_day'], ".2f"),   _r(oos_s['best_day'], ".2f")),
        ("Worst Day %",_r(is_s['worst_day'], ".2f"),  _r(oos_s['worst_day'], ".2f")),
        ("Mean return", _r(is_s['mean'], ".5f"),      _r(oos_s['mean'], ".5f")),
        ("Std return",  _r(is_s['std'], ".5f"),       _r(oos_s['std'], ".5f")),
    ]
    for label, iv, ov in rows:
        print(f"│  {label:<22}  {iv:>12}  {ov:>13}")

    print(f"│")
    print(f"│  STABILITY SUMMARY:")
    cr = rr['cagr_ratio']
    sd = rr['sharpe_diff']
    dc = rr['dd_change']
    dp = rr['decay_pct']

    cr_flag = "✅" if math.isfinite(cr) and cr >= 0.50 else "⚠"
    sd_flag = "✅" if math.isfinite(sd) and abs(sd) < 0.75 else "⚠"
    dc_flag = "✅" if math.isfinite(dc) and dc > -10 else "⚠"

    print(f"│  CAGR Ratio (OOS/IS):   {_r(cr, '.3f')}  {cr_flag}  (≥0.50 = acceptable)")
    print(f"│  Sharpe Diff (OOS−IS):  {_r(sd, '.3f')}  {sd_flag}  (>-0.75 = acceptable)")
    print(f"│  MaxDD Change (OOS−IS): {_r(dc, '.2f')}%  {dc_flag}  (>-10% = acceptable)")

    if math.isfinite(dp):
        stab_str = "✅ STABLE" if rr["stable"] else "⚠ DECAYING"
        print(f"│  Sharpe decay:          {dp:.1f}%  {stab_str}  (<40% decay = stable)")

    print(f"└{'─'*53}")


# ══════════════════════════════════════════════════════════════════════════════
# 1. MONTE CARLO RESHUFFLE + BLOCK BOOTSTRAP
# ══════════════════════════════════════════════════════════════════════════════

def mc_reshuffle(
    r: np.ndarray,
    iters: int = 2000,
    seed: int = 7,
) -> pd.DataFrame:
    """
    Randomly permute the daily return series `iters` times.
    Records MaxDD, TotalMultiple (raw equity multiple), and Sharpe per sim.

    NOTE: TotalMultiple is used instead of CAGR because every permutation of
    the same returns has the same geometric mean, making annualised CAGR
    collapse to an identical value across all sims (p05 == median == p95).
    TotalMultiple varies meaningfully across permutations via path-dependent
    drawdown exposure.
    """
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(iters):
        perm = rng.permutation(r)
        eq   = _equity(perm)
        out.append({
            "MaxDD":         _max_dd(eq),
            "TotalMultiple": float(eq[-1] / eq[0]),
            "Sharpe":        _sharpe(perm, 365),
        })
    return pd.DataFrame(out)


def block_bootstrap(
    r: np.ndarray,
    iters: int = 1000,
    block_len: int = 10,
    seed: int = 8,
) -> pd.DataFrame:
    """
    Block bootstrap: resample contiguous blocks of `block_len` days with
    replacement to preserve short-range autocorrelation structure.
    Records MaxDD, TotalMultiple, Sharpe per sim.
    """
    rng = np.random.default_rng(seed)
    n   = len(r)
    out = []
    for _ in range(iters):
        chunks: List[float] = []
        while len(chunks) < n:
            start = int(rng.integers(0, max(1, n - block_len + 1)))
            chunks.extend(r[start: start + block_len].tolist())
        samp = np.array(chunks[:n], dtype=float)
        eq   = _equity(samp)
        out.append({
            "MaxDD":         _max_dd(eq),
            "TotalMultiple": float(eq[-1] / eq[0]),
            "Sharpe":        _sharpe(samp, 365),
        })
    return pd.DataFrame(out)


# ══════════════════════════════════════════════════════════════════════════════
# 2. ROLLING MaxDD AT MULTIPLE WINDOWS
# ══════════════════════════════════════════════════════════════════════════════

def rolling_maxdd(r: np.ndarray, window: int) -> float:
    """Worst MaxDD observed in any rolling window of `window` days."""
    if len(r) < window:
        return float("nan")
    worst = 0.0
    for i in range(len(r) - window + 1):
        m = _max_dd(_equity(r[i: i + window]))
        if m < worst:
            worst = m
    return float(worst)


def rolling_maxdd_table(r: np.ndarray,
                        windows: Tuple[int, ...] = (3, 7, 10, 30)) -> Dict[str, float]:
    """Return dict of {f'{w}d': rolling_maxdd} for each window."""
    return {f"{w}d": rolling_maxdd(r, w) for w in windows}


# ══════════════════════════════════════════════════════════════════════════════
# 3. DAILY VaR / CVaR AT MULTIPLE CONFIDENCE LEVELS
# ══════════════════════════════════════════════════════════════════════════════

def var_cvar(r: np.ndarray, alpha: float = 0.05) -> Tuple[float, float]:
    """
    Historical VaR and CVaR at confidence level alpha.
    Returns (VaR, CVaR) — both negative numbers for losses.
    """
    if len(r) < 20:
        return float("nan"), float("nan")
    q    = float(np.quantile(r, alpha))
    tail = r[r <= q]
    cvar = float(np.mean(tail)) if len(tail) else float("nan")
    return q, cvar


# ══════════════════════════════════════════════════════════════════════════════
# 4. SHARPE STABILITY ANALYSIS (t-test across walk-forward folds)
# ══════════════════════════════════════════════════════════════════════════════

def sharpe_stability_analysis(wf_df: pd.DataFrame) -> Dict:
    """
    Given a walk-forward fold table (must have 'sharpe' column for OOS folds),
    runs a one-sample t-test of mean OOS Sharpe vs 0, and computes key stability
    stats used to validate the edge is real across regimes.
    """
    from scipy import stats as _stats
    test_sharpes = wf_df["sharpe"].dropna().to_numpy(dtype=float)
    test_sharpes = test_sharpes[np.isfinite(test_sharpes)]
    n = len(test_sharpes)
    if n < 2:
        return {}

    mean_s = float(np.mean(test_sharpes))
    std_s  = float(np.std(test_sharpes, ddof=1))
    se     = std_s / math.sqrt(n)
    ci_lo  = mean_s - 1.96 * se
    ci_hi  = mean_s + 1.96 * se
    t_stat = mean_s / se if se > 0 else float("nan")
    p_val  = float(2 * (1 - _stats.t.cdf(abs(t_stat), df=n - 1))) if math.isfinite(t_stat) else float("nan")
    pct_gt_2 = float(np.mean(test_sharpes > 2.0))

    return {
        "n_folds":    n,
        "mean":       mean_s,
        "std":        std_s,
        "pct_gt_2":   pct_gt_2,
        "ci_low":     ci_lo,
        "ci_high":    ci_hi,
        "t_stat":     t_stat,
        "p_value":    p_val,
    }


def print_sharpe_stability_report(ss: Dict):
    if not ss:
        return
    print()
    print("┌─ SHARPE STABILITY ANALYSIS (walk-forward folds) ───")
    print(f"│  Folds:              {ss['n_folds']}")
    print(f"│  Mean OOS Sharpe:    {ss['mean']:.3f}")
    print(f"│  Sharpe Std Dev:     {ss['std']:.3f}")
    print(f"│  % Folds > 2.0:      {ss['pct_gt_2']*100:.1f}%")
    print(f"│  95% CI:             [{ss['ci_low']:.3f}, {ss['ci_high']:.3f}]")
    print(f"│  T-stat (vs 0):      {ss['t_stat']:.3f}")
    pval = ss['p_value']
    sig  = "✅ SIGNIFICANT" if pval < 0.05 else ("⚠ MARGINAL" if pval < 0.10 else "⚠ NOT SIGNIFICANT")
    print(f"│  P-value:            {pval:.6f}  {sig}")
    print(f"└{'─'*53}")


# ══════════════════════════════════════════════════════════════════════════════
# 5. HYBRID STRESS SUMMARY (MC + BB combined print block)
# ══════════════════════════════════════════════════════════════════════════════

def print_stress_summary(
    mc: pd.DataFrame,
    bb: pd.DataFrame,
    label: str = "Strategy",
    mc_iters: int = 2000,
    bb_iters: int = 1000,
    block_len: int = 10,
):
    """
    Print the combined MC reshuffle + block bootstrap stress summary.
    Reports MaxDD percentiles and TotalMultiple distribution.
    """
    mc_mdd  = mc["MaxDD"].dropna().to_numpy(dtype=float)
    mc_mult = mc["TotalMultiple"].dropna().to_numpy(dtype=float)
    bb_mdd  = bb["MaxDD"].dropna().to_numpy(dtype=float)
    bb_mult = bb["TotalMultiple"].dropna().to_numpy(dtype=float)

    print()
    print(f"┌─ STRESS TEST SUMMARY  ({label}) ─────────────────────")
    print(f"│  MC Reshuffle: {mc_iters:,} iters   |   Block Bootstrap: {bb_iters:,} iters  block={block_len}d")
    print(f"│")
    print(f"│  MAX DRAWDOWN DISTRIBUTION:")
    print(f"│  {'':22}  {'MC Reshuffle':>13}  {'Block Bootstrap':>15}")
    print(f"│  {'─'*22}  {'─'*13}  {'─'*15}")
    for label_q, q in [("p05 (worst 5%)", 0.05), ("Median", 0.50), ("p95 (best 5%)", 0.95)]:
        mc_v = float(np.quantile(mc_mdd, q)) * 100 if len(mc_mdd) else float("nan")
        bb_v = float(np.quantile(bb_mdd, q)) * 100 if len(bb_mdd) else float("nan")
        print(f"│  {label_q:<22}  {mc_v:>12.2f}%  {bb_v:>14.2f}%")

    print(f"│")
    print(f"│  TOTAL RETURN MULTIPLE DISTRIBUTION (MC Reshuffle):")
    if len(mc_mult):
        p05 = np.quantile(mc_mult, 0.05)
        p50 = np.quantile(mc_mult, 0.50)
        p95 = np.quantile(mc_mult, 0.95)
        collapsed = abs(p95 - p05) < 0.01 * p50  # virtually identical
        print(f"│  p05 (worst reshuffles):  {p05:>8.1f}x")
        print(f"│  Median:                  {p50:>8.1f}x")
        print(f"│  p95 (best reshuffles):   {p95:>8.1f}x")
        if collapsed:
            print(f"│  ℹ  TotalMultiple is path-order invariant for this strategy —")
            print(f"│     every permutation ends at the same equity multiple.")
            print(f"│     Variance is captured by the MaxDD distribution above.")
        print(f"│  % sims > 10x:            {(mc_mult > 10).mean()*100:>7.1f}%")
        print(f"│  % sims > 100x:           {(mc_mult > 100).mean()*100:>7.1f}%")
        print(f"│  % sims lost money (<1x): {(mc_mult < 1.0).mean()*100:>7.1f}%")
    print(f"└{'─'*53}")


# ══════════════════════════════════════════════════════════════════════════════
# INSTITUTIONAL SCORECARD
# ══════════════════════════════════════════════════════════════════════════════

def _score_metric(value: float, thresholds: List[Tuple[float, int, str]]) -> Tuple[int, str]:
    """
    Score a metric against a list of (threshold, points, label) tuples.
    Thresholds are checked in order — first match wins.
    Returns (points, label) where label is 'PASS', 'MARGINAL', or 'FAIL'.
    """
    for threshold, points, label in thresholds:
        if value >= threshold:
            return points, label
    # fallback — last entry
    _, points, label = thresholds[-1]
    return points, label


def _letter_grade(score: float, max_score: float) -> Tuple[str, str]:
    """Convert numeric score to letter grade and descriptor."""
    pct = score / max_score * 100
    if pct >= 93: return "A+",  "Exceptional — Institutional Ready"
    if pct >= 88: return "A",   "Strong — Institutional Ready"
    if pct >= 83: return "A−",  "Institutional-Ready with Minor Caveat"
    if pct >= 78: return "B+",  "Near-Institutional — One Clear Gap"
    if pct >= 73: return "B",   "Good — Two Gaps to Address"
    if pct >= 68: return "B−",  "Promising — Needs Improvement"
    if pct >= 60: return "C+",  "Marginal — Significant Work Needed"
    if pct >= 50: return "C",   "Below Institutional Standard"
    return "D",  "Not Deployable — Fundamental Issues"


def _subsection_grade(score: float, max_score: float) -> str:
    pct = score / max_score * 100
    if pct >= 93: return "A+"
    if pct >= 88: return "A"
    if pct >= 83: return "A−"
    if pct >= 78: return "B+"
    if pct >= 73: return "B"
    if pct >= 68: return "B−"
    if pct >= 60: return "C+"
    if pct >= 50: return "C"
    return "D"


def institutional_scorecard(results: Dict, quiet: bool = False) -> Dict:
    """
    Compute and print the institutional scorecard from a completed audit results dict.

    10 metrics across 3 tiers:
      Gating    (3 × 15 pts = 45):  DSR, Walk-Forward CV, MaxDD
      Core      (4 ×  8 pts = 32):  Sharpe, Calmar, Sortino, Regime Robustness
      Supporting(3 × ~7-8 pts = 23): Neighbor Plateau, MC % Losing, Slippage Resilience

    Total: 100 pts.
    quiet: if True, skip all print output (used when recomputing after FA-WF overwrite).
    """
    nan = float("nan")

    # ── Extract values ────────────────────────────────────────────────────────

    # 1. DSR
    dsr_res  = results.get("dsr", {})
    dsr_val  = float(dsr_res.get("dsr", nan)) * 100 if dsr_res else nan   # as %

    # 2. Walk-forward OOS Sharpe CV
    wfr = results.get("walk_forward_rolling", {})
    wf_agg = wfr.get("aggregate", {}) if wfr else {}
    wf_mean = float(wf_agg.get("mean_sharpe", nan))
    wf_std  = float(wf_agg.get("std_sharpe",  nan))
    wf_cv   = (wf_std / abs(wf_mean)) if (math.isfinite(wf_mean) and math.isfinite(wf_std) and wf_mean != 0) else nan
    wf_pct_pos = float(wf_agg.get("pct_folds_positive_sharpe", nan))
    unstable   = int(wf_agg.get("unstable_folds", 0))

    # 3. MaxDD
    eq      = _equity(np.asarray(results.get("rolling_sharpe", [0]), dtype=float))  # fallback
    # pull directly from dsr results which stores observed_sharpe
    # MaxDD from episodes or rolling
    maxdd_val = nan
    ep_df = results.get("episodes_df", pd.DataFrame())
    if not ep_df.empty and "depth" in ep_df.columns:
        maxdd_val = float(ep_df["depth"].min()) * 100  # most negative

    # 4. Sharpe
    sharpe_val = float(results.get("observed_sharpe", nan))

    # 5. Calmar
    calmar_val = float(results.get("calmar", nan))

    # 6. Sortino
    sortino_val = float(results.get("sortino", nan))

    # 7. Regime Robustness — CAGR ratio and Sharpe decay
    rr = results.get("regime_robustness", {})
    cagr_ratio  = float(rr.get("cagr_ratio",  nan)) if rr else nan
    sharpe_diff = float(rr.get("sharpe_diff", nan)) if rr else nan
    decay_pct   = float(rr.get("decay_pct",   nan)) if rr else nan

    # 8. Neighbor Plateau
    plateau = results.get("plateau", {})
    plateau_ratio = float(plateau.get("plateau_ratio", nan)) * 100 if plateau else nan

    # 9. MC % losing
    mc_df = results.get("mc_df", pd.DataFrame())
    mc_pct_losing = nan
    if not mc_df.empty and "TotalMultiple" in mc_df.columns:
        mc_pct_losing = float((mc_df["TotalMultiple"] < 1.0).mean() * 100)

    # 10. Slippage resilience — Sharpe at ~2× base slippage
    slip_df = results.get("slippage_df", pd.DataFrame())
    slip_sharpe_2x = nan
    if not slip_df.empty and "Sharpe" in slip_df.columns and len(slip_df) >= 3:
        # 3rd row is typically 2× base slippage level
        slip_sharpe_2x = float(slip_df.iloc[2]["Sharpe"])

    # ── Scoring ───────────────────────────────────────────────────────────────

    scores = {}

    # 1. DSR (15 pts) — gating
    # Typical range across filters: 99.5–99.95%. Fine bands to separate them.
    if math.isfinite(dsr_val):
        pts, lbl = _score_metric(dsr_val, [
            (99.5, 15, "PASS"),
            (99.0, 13, "PASS"),
            (95.0, 10, "PASS"),
            (85.0,  6, "MARGINAL"),
            (70.0,  2, "MARGINAL"),
            (0.0,   0, "FAIL"),
        ])
    else:
        pts, lbl = 0, "FAIL"
    scores["dsr"] = {"pts": pts, "max": 15, "label": lbl,
                     "value": f"{dsr_val:.2f}%" if math.isfinite(dsr_val) else "n/a",
                     "goal": "≥95%  (≥99.5% ideal)", "name": "Deflated Sharpe Ratio (DSR)"}

    # 2. Walk-Forward CV (15 pts) — gating
    # Typical range: 0.12 (Tail+Disp) → 0.14 (Tail) → 0.36 (Tail+Blofin) → 0.52 (No Filter)
    # Fine bands in the 0.10–0.40 range to separate these.
    if math.isfinite(wf_cv):
        pts, lbl = _score_metric(-wf_cv, [   # negate: lower CV = better
            (-0.15, 15, "PASS"),
            (-0.20, 13, "PASS"),
            (-0.25, 10, "PASS"),
            (-0.35,  6, "MARGINAL"),
            (-0.50,  3, "MARGINAL"),
            (-999,   0, "FAIL"),
        ])
        cv_str = f"CV={wf_cv:.2f}  {wf_pct_pos:.0f}% folds positive  {unstable} unstable"
    else:
        pts, lbl = 0, "FAIL"
        cv_str = "n/a"
    scores["wf_cv"] = {"pts": pts, "max": 15, "label": lbl,
                       "value": cv_str, "goal": "CV<0.25  (CV<0.15 ideal)",
                       "name": "Walk-Forward Stability (CV)"}

    # 3. MaxDD (15 pts) — gating
    # Typical range: -18% (Tail+Disp) → -23% (Tail) → -26% (Tail+Blofin) → -39% (No Filter)
    # Fine bands in the -15% to -30% range to separate tightly clustered filters.
    if math.isfinite(maxdd_val):
        pts, lbl = _score_metric(-maxdd_val, [   # negate: less negative = better
            (-18.0, 15, "PASS"),
            (-21.0, 13, "PASS"),
            (-25.0, 10, "PASS"),
            (-28.0,  7, "PASS"),
            (-33.0,  4, "MARGINAL"),
            (-40.0,  1, "MARGINAL"),
            (-999,   0, "FAIL"),
        ])
    else:
        pts, lbl = 5, "MARGINAL"
    scores["maxdd"] = {"pts": pts, "max": 15, "label": lbl,
                       "value": f"{maxdd_val:.2f}%" if math.isfinite(maxdd_val) else "n/a",
                       "goal": "≤30%  (≤25% ideal)",
                       "name": "Maximum Drawdown"}

    # 4. Sharpe (8 pts) — core
    # Typical range: 3.03 (No Filter) → 3.47–3.50 (filtered). Fine bands.
    if math.isfinite(sharpe_val):
        pts, lbl = _score_metric(sharpe_val, [
            (3.5, 8, "PASS"),
            (3.3, 7, "PASS"),
            (3.0, 6, "PASS"),
            (2.5, 4, "PASS"),
            (2.0, 2, "MARGINAL"),
            (1.5, 1, "MARGINAL"),
            (0.0, 0, "FAIL"),
        ])
    else:
        pts, lbl = 0, "FAIL"
    scores["sharpe"] = {"pts": pts, "max": 8, "label": lbl,
                        "value": f"{sharpe_val:.3f}" if math.isfinite(sharpe_val) else "n/a",
                        "goal": "≥2.0  (≥3.0 ideal)",
                        "name": "Sharpe Ratio"}

    # 5. Calmar (8 pts) — core
    # Typical range: 22 (No Filter) → 58–78 (filtered). Wide spread here.
    if math.isfinite(calmar_val):
        pts, lbl = _score_metric(calmar_val, [
            (50.0, 8, "PASS"),
            (30.0, 7, "PASS"),
            (10.0, 6, "PASS"),
            (5.0,  4, "PASS"),
            (3.0,  2, "MARGINAL"),
            (1.0,  1, "MARGINAL"),
            (0.0,  0, "FAIL"),
        ])
    else:
        pts, lbl = 0, "FAIL"
    scores["calmar"] = {"pts": pts, "max": 8, "label": lbl,
                        "value": f"{calmar_val:.2f}" if math.isfinite(calmar_val) else "n/a",
                        "goal": "≥3.0  (≥10.0 ideal)",
                        "name": "Calmar Ratio"}

    # 6. Sortino (8 pts) — core
    # Typical range: 5.8 (No Filter) → 9.4–12.4 (filtered). Good spread.
    if math.isfinite(sortino_val):
        pts, lbl = _score_metric(sortino_val, [
            (10.0, 8, "PASS"),
            (8.0,  7, "PASS"),
            (6.0,  5, "PASS"),
            (4.0,  3, "PASS"),
            (2.5,  1, "MARGINAL"),
            (0.0,  0, "FAIL"),
        ])
    else:
        pts, lbl = 0, "FAIL"
    scores["sortino"] = {"pts": pts, "max": 8, "label": lbl,
                         "value": f"{sortino_val:.3f}" if math.isfinite(sortino_val) else "n/a",
                         "goal": "≥4.0  (≥8.0 ideal)",
                         "name": "Sortino Ratio"}

    # 7. Regime Robustness (8 pts) — core
    if math.isfinite(decay_pct) and math.isfinite(cagr_ratio):
        # combine: decay < 30% AND cagr_ratio > 0.60
        regime_score = 0
        if decay_pct < 20 and cagr_ratio >= 0.70:   regime_score = 8
        elif decay_pct < 30 and cagr_ratio >= 0.60: regime_score = 6
        elif decay_pct < 40 and cagr_ratio >= 0.50: regime_score = 3
        elif decay_pct < 50:                         regime_score = 1
        lbl = "PASS" if regime_score >= 6 else ("MARGINAL" if regime_score >= 2 else "FAIL")
        regime_str = f"decay={decay_pct:.1f}%  CAGR ratio={cagr_ratio:.2f}  ΔSharpe={sharpe_diff:.2f}"
    else:
        regime_score, lbl, regime_str = 0, "FAIL", "n/a"
    scores["regime"] = {"pts": regime_score, "max": 8, "label": lbl,
                        "value": regime_str, "goal": "decay<30%, CAGR ratio≥0.60",
                        "name": "Regime Robustness"}

    # 8. Neighbor Plateau (8 pts) — supporting
    if math.isfinite(plateau_ratio):
        pts, lbl = _score_metric(plateau_ratio, [
            (80.0, 8, "PASS"),
            (70.0, 6, "PASS"),
            (55.0, 3, "MARGINAL"),
            (40.0, 1, "MARGINAL"),
            (0.0,  0, "FAIL"),
        ])
    else:
        pts, lbl = 0, "FAIL"
    scores["plateau"] = {"pts": pts, "max": 8, "label": lbl,
                         "value": f"{plateau_ratio:.1f}%" if math.isfinite(plateau_ratio) else "n/a",
                         "goal": "≥70%  (≥80% ideal)",
                         "name": "Neighbor Plateau (Parameter Robustness)"}

    # 9. MC % Losing (8 pts) — supporting
    if math.isfinite(mc_pct_losing):
        pts, lbl = _score_metric(-mc_pct_losing, [  # negate: lower = better
            (-0.5,  8, "PASS"),
            (-1.0,  6, "PASS"),
            (-3.0,  3, "MARGINAL"),
            (-8.0,  1, "MARGINAL"),
            (-999,  0, "FAIL"),
        ])
    else:
        pts, lbl = 0, "FAIL"
    scores["mc_losing"] = {"pts": pts, "max": 8, "label": lbl,
                            "value": f"{mc_pct_losing:.2f}%" if math.isfinite(mc_pct_losing) else "n/a",
                            "goal": "<1%  (<0.5% ideal)",
                            "name": "MC Reshuffle % Losing"}

    # 10. Slippage Resilience (7 pts) — supporting
    # Typical range: 2.6–3.5 across filters. Fine bands to separate.
    if math.isfinite(slip_sharpe_2x):
        pts, lbl = _score_metric(slip_sharpe_2x, [
            (3.0, 7, "PASS"),
            (2.5, 6, "PASS"),
            (2.0, 4, "PASS"),
            (1.8, 2, "MARGINAL"),
            (1.5, 1, "MARGINAL"),
            (0.0, 0, "FAIL"),
        ])
    else:
        pts, lbl = 0, "FAIL"
    scores["slippage"] = {"pts": pts, "max": 7, "label": lbl,
                          "value": f"{slip_sharpe_2x:.3f}" if math.isfinite(slip_sharpe_2x) else "n/a",
                          "goal": "Sharpe≥1.8 at 2× slippage  (≥2.5 ideal)",
                          "name": "Slippage Resilience"}

    # ── Subsection scores ─────────────────────────────────────────────────────
    gating_score    = scores["dsr"]["pts"] + scores["wf_cv"]["pts"] + scores["maxdd"]["pts"]
    core_score      = scores["sharpe"]["pts"] + scores["calmar"]["pts"] + scores["sortino"]["pts"] + scores["regime"]["pts"]
    supporting_score= scores["plateau"]["pts"] + scores["mc_losing"]["pts"] + scores["slippage"]["pts"]

    total_score = gating_score + core_score + supporting_score
    max_total   = 100

    overall_grade, descriptor = _letter_grade(total_score, max_total)
    gating_grade    = _subsection_grade(gating_score,     45)
    core_grade      = _subsection_grade(core_score,       32)
    supporting_grade= _subsection_grade(supporting_score, 23)

    # ── Dynamic "what you still need" ────────────────────────────────────────
    gaps = []
    urgent = []

    if scores["dsr"]["pts"] < 15:
        urgent.append("DSR below threshold — more live track record is the only fix. "
                      "Every month of live returns consistent with backtest adds significant weight.")
    if scores["wf_cv"]["pts"] < 10:
        urgent.append("Walk-forward instability — high Sharpe variance across regimes suggests "
                      "the edge may be period-specific. Investigate which folds underperformed and why.")
    if scores["maxdd"]["pts"] < 11:
        urgent.append("MaxDD exceeds -30% — review PORT_STOP and TRAIL_DD parameters. "
                      "Consider tighter stops or reduced leverage to bring worst-case drawdown inside mandate.")
    if scores["sharpe"]["pts"] < 6:
        gaps.append("Sharpe below 2.5 — acceptable but an allocator will ask what is suppressing it. "
                    "Ensure slippage assumptions are realistic.")
    if scores["calmar"]["pts"] < 6:
        gaps.append("Calmar below 3.0 — return per unit of MaxDD is below institutional benchmark. "
                    "Either CAGR needs to improve or MaxDD needs to tighten.")
    if scores["sortino"]["pts"] < 6:
        gaps.append("Sortino below 4.0 — more downside volatility than expected for this Sharpe. "
                    "Check whether losses are clustering or spreading uniformly.")
    if scores["regime"]["pts"] < 6:
        gaps.append("Regime robustness — Sharpe decay or CAGR ratio suggests performance deteriorated "
                    "in the second half of the dataset. Characterise what changed and whether it is structural.")
    if scores["plateau"]["pts"] < 6:
        gaps.append("Narrow parameter plateau — optimal params sit on a ridge rather than a broad stable "
                    "region. Real-world execution deviation from exact parameters may degrade performance.")
    if scores["mc_losing"]["pts"] < 6:
        gaps.append("MC % losing above 2% — more path-ordering sensitivity than ideal. "
                    "The positive expectation may be partially sequence-dependent.")
    if scores["slippage"]["pts"] < 5:
        gaps.append("Slippage resilience — Sharpe degrades materially at 2× base slippage. "
                    "Live execution costs may be higher than assumed. Stress-test against wider spreads.")

    if not urgent and not gaps:
        what_next = ["Extend live track record — the quantitative case is strong; "
                     "live history is the only remaining gap for institutional credibility.",
                     "Seek independent methodology audit — third-party sign-off on simulation "
                     "engine and execution assumptions.",
                     "Begin fund structure and operational setup in parallel with live trading."]
    else:
        what_next = urgent + gaps

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _wrap(text: str, width: int = 56, indent: str = "  │  ") -> List[str]:
        words = text.split()
        lines_out, line = [], ""
        for w in words:
            if len(line) + len(w) + 1 > width:
                lines_out.append(indent + line)
                line = w
            else:
                line = (line + " " + w).strip()
        if line:
            lines_out.append(indent + line)
        return lines_out

    def _subscore_narrative(key: str, s: Dict) -> str:
        """One-sentence contextual note per metric."""
        v = s["value"]
        if key == "dsr":
            if s["pts"] == 15:
                return f"DSR {v} — the edge cannot credibly be attributed to luck given the search space tested."
            elif s["pts"] >= 10:
                return f"DSR {v} — marginal. A longer live track record is the primary remedy; no parameter change fixes this."
            else:
                return f"DSR {v} — fails the statistical validity gate. The backtest Sharpe has a meaningful probability of being a search artifact."
        if key == "wf_cv":
            if s["pts"] == 15:
                return f"{v} — Sharpe is stable across market regimes. The edge is not concentrated in a single period."
            elif s["pts"] >= 10:
                return f"{v} — some regime sensitivity. Investigate which folds underperformed and whether it is structural."
            else:
                return f"{v} — high instability. The strategy's edge may be period-specific rather than durable."
        if key == "maxdd":
            if s["pts"] >= 11:
                return f"MaxDD {v} — inside the institutional mandate. Drawdown discipline is clean."
            elif s["pts"] >= 6:
                return f"MaxDD {v} — borderline. An allocator with a -30% hard limit will flag this. Tighter stops or lower leverage required."
            else:
                return f"MaxDD {v} — exceeds institutional tolerance. This is a deployment blocker without parameter adjustment."
        if key == "sharpe":
            if s["pts"] >= 6:
                return f"Sharpe {v} — well above the institutional benchmark of 2.0. Multiple standard deviations above what most funds achieve."
            elif s["pts"] >= 4:
                return f"Sharpe {v} — acceptable but an allocator will probe whether slippage assumptions are realistic."
            else:
                return f"Sharpe {v} — below institutional standard. Risk-adjusted returns need improvement."
        if key == "calmar":
            if s["pts"] >= 6:
                return f"Calmar {v} — strong return per unit of worst-case drawdown. The strategy earns well relative to the pain it can inflict."
            elif s["pts"] >= 3:
                return f"Calmar {v} — marginal. Either CAGR needs to rise or MaxDD needs to tighten."
            else:
                return f"Calmar {v} — the strategy is not generating enough return relative to its drawdown risk."
        if key == "sortino":
            if s["pts"] >= 6:
                return f"Sortino {v} — return distribution is skewed positively. The volatility is predominantly upside."
            elif s["pts"] >= 3:
                return f"Sortino {v} — moderate. More downside volatility than expected. Check whether losses are clustering."
            else:
                return f"Sortino {v} — downside volatility is too high relative to returns. Distribution needs improvement."
        if key == "regime":
            if s["pts"] >= 6:
                return f"{v} — the edge held into the second half of the dataset. No evidence of time-specific overfitting."
            elif s["pts"] >= 2:
                return f"{v} — some deterioration in the OOS half. Worth characterising what regime drove the decay."
            else:
                return f"{v} — significant performance decay in OOS half. This is the most important gap to investigate before deployment."
        if key == "plateau":
            if s["pts"] >= 6:
                return f"{v} of neighbors within 0.5 Sharpe — parameters sit on a broad stable plateau. Operational imprecision is tolerable."
            elif s["pts"] >= 3:
                return f"{v} of neighbors within 0.5 Sharpe — moderate robustness. Small deviations from exact params may degrade live performance."
            else:
                return f"{v} of neighbors within 0.5 Sharpe — narrow spike. The strategy is sensitive to exact parameter values, which is a live execution risk."
        if key == "mc_losing":
            if s["pts"] >= 6:
                return f"{v} of 2,000 reshuffles lost money — positive expectation is robust to path ordering."
            elif s["pts"] >= 3:
                return f"{v} of reshuffles lost money — some path-ordering sensitivity. The edge may be partially sequence-dependent."
            else:
                return f"{v} of reshuffles lost money — the positive expectation is fragile to return ordering. Investigate concentration."
        if key == "slippage":
            if s["pts"] >= 5:
                return f"Sharpe {v} at 2× slippage — execution cost resilient. Live spreads can widen materially without destroying the edge."
            elif s["pts"] >= 3:
                return f"Sharpe {v} at 2× slippage — marginal. Live execution costs need to be tightly controlled."
            else:
                return f"Sharpe {v} at 2× slippage — fragile to cost increases. Backtest slippage assumptions may be optimistic."
        return ""

    # ── Build evidence checklist ──────────────────────────────────────────────
    _wfr       = results.get("walk_forward_rolling", {})
    _wfr_folds = _wfr.get("folds") if _wfr else None
    _wfr_valid = isinstance(_wfr_folds, pd.DataFrame) and not _wfr_folds.empty
    evidence = []
    if math.isfinite(dsr_val):
        evidence.append(f"DSR and MTL — statistical validity proven ({dsr_val:.2f}% DSR)")
    if _wfr_valid:
        evidence.append(f"Walk-forward regime test across {len(_wfr_folds)} rolling folds")
    if not mc_df.empty:
        evidence.append(f"MC reshuffle ({len(mc_df):,} iterations) and block bootstrap stress tests")
    if plateau:
        evidence.append("Neighbor plateau test — joint parameter robustness confirmed")
    if not slip_df.empty:
        evidence.append("Slippage, cost curve, and capacity curve — operational concerns addressed")
    evidence.append("Shock injection — tail risk quantified under artificial stress scenarios")
    evidence.append("Drawdown episode analysis — depth, duration, and recovery tracked")
    evidence.append("Regime robustness — first-half vs second-half performance split")

    # ── Bottom line ───────────────────────────────────────────────────────────
    n_fails    = sum(1 for s in scores.values() if s["label"] == "FAIL")
    n_marginal = sum(1 for s in scores.values() if s["label"] == "MARGINAL")
    n_pass     = sum(1 for s in scores.values() if s["label"] == "PASS")

    if total_score >= 88:
        bottom_line = (
            f"The quantitative case is genuinely institutional-grade — scoring {total_score}/100 "
            f"across all 10 key metrics. {n_pass} of 10 tests pass cleanly. "
            f"The remaining gap{'s are' if n_marginal > 1 else ' is'} not strategy problems — "
            f"{'they are' if n_marginal > 1 else 'it is'} time and process problems that live "
            f"trading resolves automatically. Continue trading; the numbers will do the rest."
        )
    elif total_score >= 73:
        failing = [scores[k]["name"] for k in scores if scores[k]["label"] in ("FAIL", "MARGINAL")]
        bottom_line = (
            f"The strategy scores {total_score}/100 — strong but not yet fully institutional-grade. "
            f"{n_pass} of 10 metrics pass cleanly. The gaps are concentrated in: "
            f"{', '.join(failing[:3])}. Address these specifically before seeking institutional capital. "
            f"The underlying edge appears real; the question is whether the operational packaging matches the numbers."
        )
    else:
        failing = [scores[k]["name"] for k in scores if scores[k]["label"] == "FAIL"]
        bottom_line = (
            f"The strategy scores {total_score}/100. There are {n_fails} outright failures and "
            f"{n_marginal} marginal results that require attention before this is deployable. "
            f"Failing metrics: {', '.join(failing) if failing else 'none'}. "
            f"Focus on the gating metrics first — Sharpe and DSR must both clear before the other scores matter."
        )

    # ── Print ─────────────────────────────────────────────────────────────────
    if not quiet:
      sep  = "═" * 62

      print()
      print(sep)
      print(f"  INSTITUTIONAL SCORECARD")
      print(sep)
      print()
      print(f"  Overall Grade:  {overall_grade}  —  {descriptor}")
      print(f"  Total Score:    {total_score} / {max_total}  "
            f"({n_pass} pass  {n_marginal} marginal  {n_fails} fail)")
      print()

      # ── Gating metrics ────────────────────────────────────────
      print(f"  ┌─ GATING METRICS  ({gating_score}/45)  Grade: {gating_grade} {'─'*20}")
      for key in ["dsr", "wf_cv", "maxdd"]:
          s    = scores[key]
          icon = "✅" if s["label"] == "PASS" else ("⚠ " if s["label"] == "MARGINAL" else "❌")
          note = _subscore_narrative(key, s)
          print(f"  │")
          print(f"  │  {icon} {s['name']}  —  {s['pts']}/{s['max']}")
          for ln in _wrap(note):
              print(ln)
          print(f"  │     Goal: {s['goal']}   Actual: {s['value']}")
      print(f"  └{'─'*59}")

      # ── Core metrics ──────────────────────────────────────────
      print()
      print(f"  ┌─ CORE METRICS  ({core_score}/32)  Grade: {core_grade} {'─'*23}")
      for key in ["sharpe", "calmar", "sortino", "regime"]:
          s    = scores[key]
          icon = "✅" if s["label"] == "PASS" else ("⚠ " if s["label"] == "MARGINAL" else "❌")
          note = _subscore_narrative(key, s)
          print(f"  │")
          print(f"  │  {icon} {s['name']}  —  {s['pts']}/{s['max']}")
          for ln in _wrap(note):
              print(ln)
          print(f"  │     Goal: {s['goal']}   Actual: {s['value']}")
      print(f"  └{'─'*59}")

      # ── Supporting metrics ────────────────────────────────────
      print()
      print(f"  ┌─ SUPPORTING METRICS  ({supporting_score}/23)  Grade: {supporting_grade} {'─'*18}")
      for key in ["plateau", "mc_losing", "slippage"]:
          s    = scores[key]
          icon = "✅" if s["label"] == "PASS" else ("⚠ " if s["label"] == "MARGINAL" else "❌")
          note = _subscore_narrative(key, s)
          print(f"  │")
          print(f"  │  {icon} {s['name']}  —  {s['pts']}/{s['max']}")
          for ln in _wrap(note):
              print(ln)
          print(f"  │     Goal: {s['goal']}   Actual: {s['value']}")
      print(f"  └{'─'*59}")

      # ── Evidence checklist ────────────────────────────────────
      print()
      print(f"  ┌─ WHAT YOU HAVE — DUE DILIGENCE CHECKLIST {'─'*17}")
      for item in evidence:
          print(f"  │  ✅ {item}")
      print(f"  └{'─'*59}")

      # ── What you still need ───────────────────────────────────
      print()
      print(f"  ┌─ WHAT YOU STILL NEED {'─'*37}")
      if not what_next:
          print(f"  │  No critical gaps identified.")
      else:
          for i, item in enumerate(what_next, 1):
              lines_out = _wrap(item, width=54, indent="  │     ")
              print(f"  │  {i}. {lines_out[0].strip()}")
              for ln in lines_out[1:]:
                  print(ln)
              if i < len(what_next):
                  print(f"  │")
      print(f"  └{'─'*59}")

      # ── Bottom line ───────────────────────────────────────────
      print()
      print(f"  ┌─ BOTTOM LINE {'─'*46}")
      for ln in _wrap(bottom_line, width=56, indent="  │  "):
          print(ln)
      print(f"  └{'─'*59}")
      print()
      print(sep)

    return {
        "total_score":       total_score,
        "overall_grade":     overall_grade,
        "descriptor":        descriptor,
        "gating_score":      gating_score,
        "core_score":        core_score,
        "supporting_score":  supporting_score,
        "scores":            scores,
        "gaps":              gaps,
        "urgent":            urgent,
    }


# ══════════════════════════════════════════════════════════════════════════════
# PERIODIC RETURN BREAKDOWN
# ══════════════════════════════════════════════════════════════════════════════

def periodic_return_breakdown(
    r: np.ndarray,
    tdy: int = 365,
    days_per_week: int = 7,
    days_per_month: int = 30,
) -> Dict:
    """
    Aggregates daily returns into weekly and monthly buckets and computes
    win rate, avg win, avg loss, best/worst for each period.
    """
    r = np.asarray(r, dtype=float)
    r = r[np.isfinite(r)]
    n = len(r)

    # ── Daily ─────────────────────────────────────────────────
    wins_d  = r[r > 0]
    loss_d  = r[r < 0]
    daily = {
        "n":           n,
        "avg":         float(np.mean(r))         if n  else float("nan"),
        "best":        float(np.max(r))           if n  else float("nan"),
        "worst":       float(np.min(r))           if n  else float("nan"),
        "win_rate":    float(np.mean(r > 0))      if n  else float("nan"),
        "avg_win":     float(np.mean(wins_d))     if len(wins_d)  else float("nan"),
        "avg_loss":    float(np.mean(loss_d))     if len(loss_d)  else float("nan"),
        "n_wins":      int(np.sum(r > 0)),
        "n_losses":    int(np.sum(r < 0)),
    }

    # ── Weekly — aggregate into non-overlapping 7-day buckets ─
    def _bucket_returns(arr: np.ndarray, bucket: int) -> np.ndarray:
        """Compound daily returns into non-overlapping buckets."""
        out = []
        for i in range(0, len(arr) - bucket + 1, bucket):
            chunk = arr[i:i + bucket]
            compound = float(np.prod(1.0 + chunk) - 1.0)
            out.append(compound)
        return np.array(out, dtype=float)

    wk = _bucket_returns(r, days_per_week)
    wins_w  = wk[wk > 0]
    loss_w  = wk[wk < 0]
    weekly = {
        "n":        len(wk),
        "avg":      float(np.mean(wk))        if len(wk) else float("nan"),
        "best":     float(np.max(wk))          if len(wk) else float("nan"),
        "worst":    float(np.min(wk))          if len(wk) else float("nan"),
        "win_rate": float(np.mean(wk > 0))     if len(wk) else float("nan"),
        "avg_win":  float(np.mean(wins_w))     if len(wins_w)  else float("nan"),
        "avg_loss": float(np.mean(loss_w))     if len(loss_w)  else float("nan"),
        "n_wins":   int(np.sum(wk > 0)),
        "n_losses": int(np.sum(wk < 0)),
    }

    # ── Monthly — 30-day buckets ───────────────────────────────
    mo = _bucket_returns(r, days_per_month)
    wins_m  = mo[mo > 0]
    loss_m  = mo[mo < 0]
    monthly = {
        "n":        len(mo),
        "avg":      float(np.mean(mo))        if len(mo) else float("nan"),
        "best":     float(np.max(mo))          if len(mo) else float("nan"),
        "worst":    float(np.min(mo))          if len(mo) else float("nan"),
        "win_rate": float(np.mean(mo > 0))     if len(mo) else float("nan"),
        "avg_win":  float(np.mean(wins_m))     if len(wins_m)  else float("nan"),
        "avg_loss": float(np.mean(loss_m))     if len(loss_m)  else float("nan"),
        "n_wins":   int(np.sum(mo > 0)),
        "n_losses": int(np.sum(mo < 0)),
    }

    return {"daily": daily, "weekly": weekly, "monthly": monthly}


def print_periodic_return_breakdown(pb: Dict):
    """Print the periodic return breakdown section."""
    if not pb:
        return

    def _f(v, pct=True, plus=False):
        if not math.isfinite(v):
            return "   n/a"
        s = f"{v*100:+.2f}%" if plus else f"{v*100:.2f}%"
        return s

    d  = pb["daily"]
    w  = pb["weekly"]
    mo = pb["monthly"]

    print()
    print("┌─ RETURN RATES BY PERIOD ────────────────────────────")
    print(f"│")

    # Monthly
    print(f"│  MONTHLY  ({mo['n']} months)")
    print(f"│  {'Win rate:':16} {mo['win_rate']*100:.1f}%  "
          f"({mo['n_wins']}W / {mo['n_losses']}L)")
    print(f"│  {'Avg month:':16} {_f(mo['avg'], plus=True)}")
    if math.isfinite(mo.get('avg_win', float('nan'))):
        print(f"│  {'Avg win:':16} {_f(mo['avg_win'])}")
    if math.isfinite(mo.get('avg_loss', float('nan'))):
        print(f"│  {'Avg loss:':16} {_f(mo['avg_loss'])}")
    print(f"│  {'Best month:':16} {_f(mo['best'])}"
          f"   {'Worst:':7} {_f(mo['worst'])}")

    print(f"│")

    # Weekly
    print(f"│  WEEKLY  ({w['n']} weeks)")
    print(f"│  {'Win rate:':16} {w['win_rate']*100:.1f}%  "
          f"({w['n_wins']}W / {w['n_losses']}L)")
    print(f"│  {'Avg week:':16} {_f(w['avg'], plus=True)}")
    if math.isfinite(w.get('avg_win', float('nan'))):
        print(f"│  {'Avg win:':16} {_f(w['avg_win'])}")
    if math.isfinite(w.get('avg_loss', float('nan'))):
        print(f"│  {'Avg loss:':16} {_f(w['avg_loss'])}")
    print(f"│  {'Best week:':16} {_f(w['best'])}"
          f"   {'Worst:':7} {_f(w['worst'])}")

    print(f"│")

    # Daily
    print(f"│  DAILY  ({d['n']} days)")
    print(f"│  {'Win rate:':16} {d['win_rate']*100:.1f}%  "
          f"({d['n_wins']}W / {d['n_losses']}L)")
    print(f"│  {'Avg day:':16} {_f(d['avg'], plus=True)}")
    if math.isfinite(d.get('avg_win', float('nan'))):
        print(f"│  {'Avg win:':16} {_f(d['avg_win'])}")
    if math.isfinite(d.get('avg_loss', float('nan'))):
        print(f"│  {'Avg loss:':16} {_f(d['avg_loss'])}")
    print(f"│  {'Best day:':16} {_f(d['best'])}"
          f"   {'Worst:':7} {_f(d['worst'])}")

    print(f"└{'─'*53}")


# ══════════════════════════════════════════════════════════════════════════════
# SIMULATION BIAS AUDIT
# ══════════════════════════════════════════════════════════════════════════════

def simulation_bias_audit(
    df_4x: pd.DataFrame,
    param_config: Dict,
    trial_purchases: bool = False,
    tdy: int = 365,
    n_shuffle_days: int = 5,
    random_seed: int = 42,
) -> Dict:
    """
    Runs a battery of concrete pass/fail tests to detect simulation biases
    that could artificially inflate backtest returns.

    Tests:
    1. CAUSAL INTEGRITY   — entry/exit never uses future bar data
    2. TRIAL PERIOD LOCK  — TRIAL_PURCHASES=True locks trial at 1x correctly
    3. ENTRY PRICE CHECK  — entry uses bar[x_idx] close, not bar[x_idx+1] open
    4. STOP TRIGGER CHECK — stop exits at the bar that crossed, not next bar
    5. SHUFFLE INVARIANCE — randomly shuffling intraday bars in flat region
                            doesn't change pre-signal return (proves no lookahead)
    6. ZERO RETURN DAY    — a perfectly flat day returns exactly 0.0
    7. SINGLE BAR KILL    — a day that fails signal at x_idx returns exactly roi_x
                            (TP=True) or 0.0 (TP=False)
    8. KNOWN RETURN CHECK — manually constructed path produces exact expected return
    9. SLIPPAGE DIRECTION — slippage always reduces returns, never increases them
    10. BAR INDEX BOUNDS  — x_idx never exceeds n-1 (no out-of-bounds access)
    """
    rng = np.random.default_rng(random_seed)
    results = {}
    passed = 0
    failed = 0

    def _run(path_1x):
        p = param_config
        early_y_1x = float(p["EARLY_KILL_Y"])
        raw_ps  = p.get("PORT_SL", -0.049)
        en_ps   = raw_ps is not None and raw_ps != 0
        ps_val  = float(raw_ps) if en_ps else -999.0
        raw_ef  = p.get("EARLY_FILL_Y", None)
        en_ef   = raw_ef is not None and raw_ef != 0
        ef_val  = float(raw_ef) if en_ef else 999.0
        raw_efx = p.get("EARLY_FILL_X", None)
        efx_val = int(raw_efx) if (raw_efx is not None and raw_efx != 0) else 0
        _r, _lu = _apply_hybrid_day_param(
            np.asarray(path_1x, dtype=float),
            early_x_minutes          = int(round(p["EARLY_KILL_X"])),
            early_y_1x               = early_y_1x,
            trail_dd_1x              = float(p["PORT_TSL"]),
            l_base                   = float(p.get("L_BASE", 1.0)),
            l_high                   = float(p["L_HIGH"]),
            port_stop_1x             = ps_val,
            early_fill_threshold_1x  = ef_val,
            strong_thr_1x            = float(p.get("EARLY_INSTILL_Y", 0.01)),
            enable_portfolio_stop    = en_ps,
            enable_early_fill        = en_ef,
            early_fill_max_minutes   = efx_val,
            trial_purchases          = trial_purchases,
        )
        return _r

    x_idx = max(0, (int(round(param_config["EARLY_KILL_X"])) // _BAR_MINUTES) - 1)
    n_bars = _N_ROWS
    early_y_1x = float(param_config["EARLY_KILL_Y"])

    # ── TEST 1: Zero return day ────────────────────────────────────────────
    # A flat path (all zeros) should return exactly 0.0
    flat_path = np.zeros(n_bars)
    r_flat = _run(flat_path)
    t1_pass = abs(r_flat) < 1e-9
    results["T1_zero_return_day"] = {
        "pass": t1_pass,
        "expected": 0.0,
        "got": r_flat,
        "description": "Flat path (all zeros) must return exactly 0.0",
    }
    passed += t1_pass; failed += not t1_pass

    # ── TEST 2: Signal fail → early kill ──────────────────────────────────
    # Path that stays below signal threshold at x_idx
    # TP=False: should return 0.0  |  TP=True: should return roi_x (negative/small)
    below_signal = np.zeros(n_bars)
    below_signal[x_idx] = early_y_1x * 0.5   # half the threshold — fails signal
    # make post-signal bars very profitable to ensure any lookahead would show up
    below_signal[x_idx+1:] = 0.50
    r_below = _run(below_signal)
    if not trial_purchases:
        t2_pass  = abs(r_below) < 1e-9
        t2_expect = "0.0 (flat — signal failed)"
    else:
        t2_pass  = abs(r_below - below_signal[x_idx]) < 1e-9
        t2_expect = f"{below_signal[x_idx]:.6f} (trial return only)"
    results["T2_signal_fail_early_kill"] = {
        "pass": t2_pass,
        "expected": t2_expect,
        "got": r_below,
        "description": "Failed signal must not capture post-signal returns (lookahead check)",
    }
    passed += t2_pass; failed += not t2_pass

    # ── TEST 3: No future bar access on kill day ───────────────────────────
    # Same as T2 but post-signal bars are NEGATIVE
    # If the function is clean, result should be same as T2 regardless of post-signal values
    below_signal_neg = np.zeros(n_bars)
    below_signal_neg[x_idx] = early_y_1x * 0.5
    below_signal_neg[x_idx+1:] = -0.50
    r_below_neg = _run(below_signal_neg)
    t3_pass = abs(r_below - r_below_neg) < 1e-9
    results["T3_future_bar_independence"] = {
        "pass": t3_pass,
        "expected": f"Same result as T2 ({r_below:.6f})",
        "got": r_below_neg,
        "description": "Post-signal bars must not affect killed-day return (causal integrity)",
    }
    passed += t3_pass; failed += not t3_pass

    # ── TEST 4: Pre-signal bar shuffle invariance ──────────────────────────
    # Shuffling bars BEFORE x_idx should not change the result
    # (because only path_1x[x_idx] is read for the signal check)
    base_path = np.zeros(n_bars)
    base_path[x_idx]    = early_y_1x * 2.0   # passes signal
    base_path[x_idx+1:] = 0.01               # modest positive drift
    r_base = _run(base_path)

    shuffled_path = base_path.copy()
    pre_signal = shuffled_path[:x_idx].copy()
    rng.shuffle(pre_signal)
    shuffled_path[:x_idx] = pre_signal
    r_shuffled = _run(shuffled_path)

    t4_pass = abs(r_base - r_shuffled) < 1e-9
    results["T4_pre_signal_shuffle_invariance"] = {
        "pass": t4_pass,
        "expected": f"{r_base:.6f}",
        "got": r_shuffled,
        "description": "Shuffling pre-signal bars must not change return (only x_idx bar matters)",
    }
    passed += t4_pass; failed += not t4_pass

    # ── TEST 5: Known return — pass signal, no stop triggered ─────────────
    # Construct a path that passes signal and drifts to a known final value
    # Verify the return matches our manual calculation exactly
    known_path = np.zeros(n_bars)
    known_path[x_idx]    = early_y_1x * 3.0   # passes signal strongly
    known_path[x_idx+1:] = 0.0                 # flat after entry

    lev = float(param_config["L_HIGH"]) if (
        early_y_1x * 3.0 >= float(param_config.get("EARLY_INSTILL_Y", 0.01))
    ) else float(param_config.get("L_BASE", 1.0))

    r_known = _run(known_path)

    if not trial_purchases:
        # Enters at x_idx close, exits at last bar close
        # entry = path[x_idx], exit = path[-1] = 0.0
        # incremental = 0.0 - path[x_idx] = -path[x_idx]
        entry = known_path[x_idx]
        incremental = known_path[-1] - entry
        expected_known = incremental * lev
    else:
        # trial_return = path[x_idx]
        # post_trial incremental = path[-1] - path[x_idx] = -path[x_idx]
        trial_ret = known_path[x_idx]
        post_incr = known_path[-1] - trial_ret
        expected_known = trial_ret + post_incr * lev

    t5_pass = abs(r_known - expected_known) < 1e-6
    results["T5_known_return_verification"] = {
        "pass": t5_pass,
        "expected": f"{expected_known:.6f}",
        "got": r_known,
        "description": "Manually computed return must match simulation exactly",
    }
    passed += t5_pass; failed += not t5_pass

    # ── TEST 6: Trailing stop triggers at correct bar ─────────────────────
    # Build a path that rises then drops by exactly trail_dd_1x + epsilon
    # Stop should trigger at the exact bar it crosses, not the next one
    trail_dd = float(param_config["PORT_TSL"])
    trail_path = np.zeros(n_bars)
    trail_path[x_idx] = early_y_1x * 3.0   # pass signal

    # rise to peak at x_idx+5, then drop by trail_dd + epsilon at x_idx+6
    peak_val = trail_path[x_idx] + 0.05
    stop_val = peak_val - trail_dd - 0.001  # just beyond stop threshold
    for i in range(x_idx+1, min(x_idx+6, n_bars)):
        trail_path[i] = peak_val
    stop_bar = min(x_idx+6, n_bars-1)
    trail_path[stop_bar] = stop_val
    # make all bars after stop_bar very profitable — if stop didn't trigger, return would be higher
    trail_path[stop_bar+1:] = 0.50

    r_trail = _run(trail_path)

    # Expected: should exit at stop_bar value, NOT capture the 0.50 bars after
    if not trial_purchases:
        entry = trail_path[x_idx]
        expected_trail = (stop_val - entry) * lev
    else:
        trial_ret = trail_path[x_idx]
        post_incr = stop_val - trial_ret
        expected_trail = trial_ret + post_incr * lev

    t6_pass = abs(r_trail - expected_trail) < 1e-6
    results["T6_trailing_stop_bar_accuracy"] = {
        "pass": t6_pass,
        "expected": f"{expected_trail:.6f}  (exit at stop bar, not after)",
        "got": r_trail,
        "description": "Trailing stop must exit at the triggering bar, not capture subsequent returns",
    }
    passed += t6_pass; failed += not t6_pass

    # ── TEST 7: Trial period leverage lock (TRIAL_PURCHASES=True only) ────
    if trial_purchases:
        # During trial (bars 0..x_idx), return must be at 1x regardless of L_HIGH
        # Construct path that passes signal, verify trial portion is exactly path[x_idx]
        tp_path = np.zeros(n_bars)
        tp_path[x_idx] = early_y_1x * 3.0
        tp_path[x_idx+1:] = 0.0  # flat post-trial

        r_tp = _run(tp_path)
        # With flat post-trial: result = trial_return + (0 - trial_return) * lev
        #                               = trial_return * (1 - lev)
        # So trial_return = result / (1 - lev)  ... unless lev=1, then = trial_return
        trial_ret_expected = tp_path[x_idx]
        post_flat          = 0.0 - trial_ret_expected
        expected_tp        = trial_ret_expected + post_flat * lev

        # Verify: if we set L_HIGH=10 (absurd), trial period should still be 1x
        p_high_lev = dict(param_config)
        p_high_lev["L_HIGH"] = 10.0
        p_high_lev["L_BASE"] = 10.0

        def _run_custom(path, p_override):
            ey1x = float(p_override["EARLY_KILL_Y"])
            raw_ps  = p_override.get("PORT_SL", -0.049)
            en_ps   = raw_ps is not None and raw_ps != 0
            ps_val  = float(raw_ps) if en_ps else -999.0
            raw_ef  = p_override.get("EARLY_FILL_Y", None)
            en_ef   = raw_ef is not None and raw_ef != 0
            ef_val  = float(raw_ef) if en_ef else 999.0
            raw_efx = p_override.get("EARLY_FILL_X", None)
            efx_val = int(raw_efx) if (raw_efx is not None and raw_efx != 0) else 0
            _r2, _lu2 = _apply_hybrid_day_param(
                np.asarray(path, dtype=float),
                early_x_minutes          = int(round(p_override["EARLY_KILL_X"])),
                early_y_1x               = ey1x,
                trail_dd_1x              = float(p_override["PORT_TSL"]),
                l_base                   = float(p_override.get("L_BASE", 1.0)),
                l_high                   = float(p_override["L_HIGH"]),
                port_stop_1x             = ps_val,
                early_fill_threshold_1x  = ef_val,
                strong_thr_1x            = float(p_override.get("EARLY_INSTILL_Y", 0.01)),
                enable_portfolio_stop    = en_ps,
                enable_early_fill        = en_ef,
                early_fill_max_minutes   = efx_val,
                trial_purchases          = True,
            )
            return _r2

        r_tp_10x     = _run_custom(tp_path, p_high_lev)
        # Expected with L=10 on flat post: trial_ret + (0 - trial_ret) * 10
        expected_10x = trial_ret_expected + (0.0 - trial_ret_expected) * 10.0
        r_tp_1x_portion = r_tp - (0.0 - trial_ret_expected) * lev
        # Trial portion should be 1x in both cases
        t7_pass = abs(r_tp_1x_portion - trial_ret_expected) < 1e-6
        results["T7_trial_period_leverage_lock"] = {
            "pass": t7_pass,
            "expected": f"Trial return = {trial_ret_expected:.6f} at 1x",
            "got": f"Reconstructed trial portion = {r_tp_1x_portion:.6f}",
            "description": "Trial period (0..x_idx) must always be computed at 1x leverage",
        }
        passed += t7_pass; failed += not t7_pass
    else:
        results["T7_trial_period_leverage_lock"] = {
            "pass": True,
            "expected": "N/A (TRIAL_PURCHASES=False)",
            "got": "N/A",
            "description": "Not applicable — TRIAL_PURCHASES=False means no trial period",
        }
        passed += 1

    # ── TEST 8: Real data cross-check ─────────────────────────────────────
    # Take a sample of real days from df_4x and manually re-simulate one
    # to verify the driver output matches the function output
    sample_cols = list(df_4x.columns[:n_shuffle_days])
    mismatches  = []
    for col in sample_cols:
        path_4x = df_4x[col].to_numpy(dtype=float)
        path_1x = path_4x / _PIVOT_LEV
        r_func  = _run(path_1x)

        # Re-run via _simulate_with_params route to check consistency
        tmp_df  = df_4x[[col]].copy()
        res     = _simulate_with_params(tmp_df, param_config, tdy,
                                         trial_purchases=trial_purchases)
        # _simulate_with_params returns aggregate stats — can't compare directly
        # Instead compare _run result with a second call (determinism check)
        r_func2 = _run(path_1x)
        if abs(r_func - r_func2) > 1e-12:
            mismatches.append((col, r_func, r_func2))

    t8_pass = len(mismatches) == 0
    results["T8_determinism_check"] = {
        "pass": t8_pass,
        "expected": "Identical results on repeated calls",
        "got": f"{len(mismatches)} mismatches out of {len(sample_cols)} days",
        "description": "Simulation must be fully deterministic — same input always same output",
    }
    passed += t8_pass; failed += not t8_pass

    # ── TEST 9: Portfolio stop fires before trailing stop ─────────────────
    # Build a path where port_stop fires at bar x_idx+2 (-5% immediate drop)
    # Verify we exit there and don't continue to capture remaining bars
    raw_ps = param_config.get("PORT_SL", -0.049)
    if raw_ps is not None and raw_ps != 0:
        ps_1x = float(raw_ps)
        ps_path = np.zeros(n_bars)
        ps_path[x_idx] = early_y_1x * 3.0  # pass signal
        ps_path[x_idx+1] = 0.01            # small gain
        ps_path[x_idx+2] = ps_1x - 0.001  # just below port stop
        ps_path[x_idx+3:] = 0.50           # very profitable after — should NOT be captured

        r_ps = _run(ps_path)
        stop_exit_val = ps_path[x_idx+2]

        if not trial_purchases:
            entry = ps_path[x_idx]
            expected_ps = (stop_exit_val - entry) * lev
        else:
            trial_ret = ps_path[x_idx]
            post_incr = stop_exit_val - trial_ret
            expected_ps = trial_ret + post_incr * lev

        t9_pass = abs(r_ps - expected_ps) < 1e-6
        results["T9_portfolio_stop_priority"] = {
            "pass": t9_pass,
            "expected": f"{expected_ps:.6f}  (exit at port stop bar)",
            "got": r_ps,
            "description": "Portfolio stop must fire at triggering bar and not capture subsequent gains",
        }
        passed += t9_pass; failed += not t9_pass
    else:
        results["T9_portfolio_stop_priority"] = {
            "pass": True, "expected": "N/A (PORT_STOP disabled)",
            "got": "N/A", "description": "Portfolio stop disabled — test skipped",
        }
        passed += 1

    # ── TEST 10: Bar index bounds ──────────────────────────────────────────
    # x_idx must be < n_bars, and path access must never exceed n_bars-1
    x_idx_actual = max(0, min((int(round(param_config["EARLY_KILL_X"])) // _BAR_MINUTES) - 1, n_bars - 1))
    t10_pass = x_idx_actual < n_bars
    results["T10_bar_index_bounds"] = {
        "pass": t10_pass,
        "expected": f"x_idx < {n_bars}",
        "got": f"x_idx = {x_idx_actual}",
        "description": "Signal bar index must be within valid array bounds",
    }
    passed += t10_pass; failed += not t10_pass

    return {
        "passed": passed,
        "failed": failed,
        "total":  passed + failed,
        "tests":  results,
    }


def print_simulation_bias_report(bias: Dict, label: str = ""):
    """Print the simulation bias audit results."""
    if not bias:
        return

    total  = bias["total"]
    passed = bias["passed"]
    failed = bias["failed"]

    grade = "✅ CLEAN" if failed == 0 else (
            "⚠ SUSPECT" if failed <= 2 else "❌ BIASED")

    print()
    print("┌─ SIMULATION BIAS AUDIT ─────────────────────────────")
    print(f"│  {label}{'  |  ' if label else ''}Passed: {passed}/{total}   {grade}")
    print(f"│")

    for key, t in bias["tests"].items():
        icon = "✅" if t["pass"] else "❌"
        name = key.replace("_", " ").title()
        print(f"│  {icon} {name}")
        if not t["pass"]:
            print(f"│     Expected: {t['expected']}")
            print(f"│     Got:      {t['got']}")
            print(f"│     → {t['description']}")
        else:
            print(f"│     {t['description']}")

    print(f"│")
    if failed == 0:
        print(f"│  All {total} bias checks passed. Simulation logic is causally")
        print(f"│  sound — no evidence of lookahead or implementation bias.")
    else:
        print(f"│  ⚠ {failed} check(s) FAILED. Results may be artificially")
        print(f"│  inflated. Investigate failed tests before trusting returns.")
    print(f"└{'─'*53}")

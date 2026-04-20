"""Strategy-level VOL boost computation.

Sibling of compute_vol_boost() in trader_blofin.py. The host-trader version
reads from a flat CSV (blofin_returns_log.csv) and is account-level. This
module takes a list of daily returns directly and is strategy-level —
used by the nightly refresh to publish a per-strategy vol_boost into
strategy_version.current_metrics.

Math is identical across the two callers: same 4-stage clip, same
constants. The constants here are duplicated from trader_blofin.py's
VOL_LEV_* block; they stay in lockstep because both trace back to the
audit-matched baseline in /root/benji/trader-blofin.py. The legacy copy
in trader_blofin.py dies with the eventual CAPITAL_MODE cleanup; this
module becomes the sole canonical home.
"""
from __future__ import annotations

import math

import numpy as np

# Host-parity constants (lines 99-104 of /root/benji/trader-blofin.py).
VOL_LEV_TARGET_VOL   = 0.02
VOL_LEV_WINDOW       = 30
VOL_LEV_SHARPE_REF   = 3.3
VOL_LEV_MAX_BOOST    = 2.0     # 1.33x floor to 2.66x ceiling
VOL_LEV_DD_THRESHOLD = -0.15
VOL_LEV_DD_SCALE     = 1.0     # inactive (structurally present)


def compute_strategy_vol_boost(rets: list[float]) -> float:
    """Compute VOL-target leverage boost from strategy-simulated returns.

    Four-stage clip, identical math to compute_vol_boost():
      Stage 1: vc = clip(target_vol / realized_vol, 1.0, max_boost)
      Stage 2: sc = clip(sharpe_ref / rolling_sharpe, 0.5, 2.0)
      Stage 3: dg = DD_SCALE if running_DD < DD_THRESHOLD else 1.0
      Stage 4: boost = clip(vc * sc * dg, 1.0, max_boost)

    Expected input shape
    --------------------
    `rets` is the strategy's daily net-of-fees returns as decimals
    (e.g. 0.01 = 1%). One entry per calendar day the audit simulated,
    in chronological order. Flat/no-entry days MUST be included as 0.0
    — not omitted — so realized vol reflects the actual in-/out-of-market
    pattern. This matches the host trader's log_daily_return behavior
    (flat days are written to blofin_returns_log.csv with
    net_return_pct=0, not skipped).

    Window semantics match compute_vol_boost:
      - Volatility and rolling-Sharpe use the trailing VOL_LEV_WINDOW
        entries.
      - Equity/drawdown use the FULL history (not windowed) so the DD
        guard sees lifetime max-drawdown, not just trailing 30d.

    Returns
    -------
    float
        Boost scalar in [1.0, VOL_LEV_MAX_BOOST]. Degenerate input
        (empty, <5 entries, or numerical failure) returns 1.0 — matches
        compute_vol_boost's "Returns log not found / insufficient data"
        fallback. Callers relying on 1.0-as-sentinel should treat the
        value as "no boost" regardless of cause.
    """
    if not rets:
        return 1.0
    try:
        arr = np.asarray(rets, dtype=float)
        window = min(VOL_LEV_WINDOW, len(arr))
        if window < 5:
            return 1.0
        rets_w = arr[-window:]

        realized_vol = max(float(np.std(rets_w)), 1e-8)
        vc = float(np.clip(VOL_LEV_TARGET_VOL / realized_vol, 1.0, VOL_LEV_MAX_BOOST))

        mean_ret = float(np.mean(rets_w))
        rolling_sharpe = max(
            mean_ret / realized_vol * math.sqrt(365) if realized_vol > 0
            else VOL_LEV_SHARPE_REF,
            1e-8,
        )
        sc = float(np.clip(VOL_LEV_SHARPE_REF / rolling_sharpe, 0.5, 2.0))

        equity     = np.cumprod(1.0 + arr)
        running_dd = float(equity[-1] / np.max(equity) - 1.0)
        dg = VOL_LEV_DD_SCALE if running_dd < VOL_LEV_DD_THRESHOLD else 1.0

        boost = float(np.clip(vc * sc * dg, 1.0, VOL_LEV_MAX_BOOST))
        return boost
    except Exception:
        return 1.0

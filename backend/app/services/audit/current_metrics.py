"""Shared builder for the strategy_version.current_metrics JSONB payload.

Both Simulator promote (user-triggered) and refresh_strategy_metrics (nightly
cron) call this so the denormalized cache matches a single shape — and so the
Allocator API's `current_metrics->>'<key>'` reads always land on a consistent
key set.

Keys here mirror the columns the Allocator card surfaces (via the Part 11 SQL
swap). Values that aren't populated for this filter (e.g. top-level-only
metrics when the picked filter isn't best_filter) are emitted as None.
"""
from __future__ import annotations

from typing import Any

# Order here is cosmetic (JSONB doesn't preserve it), but the comment groups
# match the _FILTER_TO_RESULTS_COLUMN / _TOP_LEVEL_ONLY_COLUMNS maps in
# app.api.routes.simulator so future edits keep them synchronized.
_CURRENT_METRICS_KEYS: tuple[str, ...] = (
    # Filter-row columns (always populated when available)
    "sharpe",
    "cagr_pct",
    "max_dd_pct",
    "active_days",
    "fa_wf_cv",
    "cv",
    "total_return_pct",
    "worst_day_pct",
    "worst_month_pct",
    "dsr_pct",
    "grade",
    "scorecard_score",
    # Top-level-only (populated only when the picked filter is best_filter)
    "sortino",
    "calmar",
    "omega",
    "ulcer_index",
    "fa_oos_sharpe",
    "flat_days",
    "profit_factor",
    "win_rate_daily",
    "avg_daily_ret_pct",
    "best_month_pct",
    "equity_r2",
    # Capital (always populated when starting_capital is known)
    "starting_capital",
    "ending_capital",
    "equity_multiplier",
)


def build_current_metrics(result_row: dict[str, Any]) -> dict[str, Any]:
    """Build the JSONB payload stored on strategy_version.current_metrics.

    Given an `audit.results`-shaped row dict (the return of
    simulator._build_result_row), projects it to the fixed key set the
    Allocator card reads. Keys missing from result_row land as None —
    intentional; the card renders "—" for null.
    """
    return {k: result_row.get(k) for k in _CURRENT_METRICS_KEYS}

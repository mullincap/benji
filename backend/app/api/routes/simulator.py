"""
backend/app/api/routes/simulator.py
====================================
Simulator-specific endpoints that operate on the audit.* DB tables.

Currently hosts the promote-to-strategy flow:
  POST /api/simulator/audits/{job_id}/promote

Job CRUD + raw output streaming remain under /api/jobs/* (see jobs.py).
Admin-only: every route requires a valid admin session.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ...core.config import settings
from ...db import get_cursor
from ...services.audit.config_hash import hash_config
from ...services.audit.current_metrics import build_current_metrics
from .admin import require_admin

router = APIRouter(
    prefix="/api/simulator",
    tags=["simulator"],
    dependencies=[Depends(require_admin)],
)


# ─── Models ──────────────────────────────────────────────────────────────────

class PromoteRequest(BaseModel):
    strategy_name: str = Field(..., min_length=1)
    version_label: str = Field(..., min_length=1)
    description:   str | None = None
    filter_mode:   str = Field(..., min_length=1)


# ─── Helpers ────────────────────────────────────────────────────────────────

def _slugify(value: str) -> str:
    """Conservative slug: lowercase, hyphen-separated, [a-z0-9_-] only."""
    s = value.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "strategy"


# Map (filter entry → audit.results column) for the subset that appears in the
# per-filter metrics block. Keys present only at the top level (sortino /
# calmar / omega / ulcer_index / fa_oos_sharpe) are handled separately below.
_FILTER_TO_RESULTS_COLUMN = {
    "sharpe":        "sharpe",
    "cagr":          "cagr_pct",
    "max_dd":        "max_dd_pct",
    "active":        "active_days",
    "wf_cv":         "fa_wf_cv",
    "cv":            "cv",
    "tot_ret":       "total_return_pct",
    "wst_1d":        "worst_day_pct",
    "wst_1m":        "worst_month_pct",
    "dsr_pct":       "dsr_pct",
    "grade":         "grade",
    "grade_score":   "scorecard_score",
}

# Keys that only exist at metrics top level (best-filter view). Only applied
# when the promoted filter IS the best filter.
_TOP_LEVEL_ONLY_COLUMNS = {
    "sortino":           "sortino",
    "calmar":            "calmar",
    "omega":             "omega",
    "ulcer_index":       "ulcer_index",
    "fa_oos_sharpe":     "fa_oos_sharpe",
    "flat_days":         "flat_days",
    # Added alongside the Part 6 _parse_metrics regex extensions.
    "profit_factor":     "profit_factor",
    "win_rate_daily":    "win_rate_daily",
    "avg_daily_ret_pct": "avg_daily_ret_pct",
    "best_month_pct":    "best_month_pct",
    "equity_r2":         "equity_r2",
}


def _load_job_json(job_id: str) -> dict[str, Any] | None:
    """Read backend/jobs/{id}/job.json — source of truth for metrics payload."""
    path = Path(settings.JOBS_DIR) / job_id / "job.json"
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _pick_filter_metrics(metrics: dict, filter_mode: str) -> dict | None:
    """Find the per-filter dict in metrics['filters'] whose 'filter' matches."""
    for entry in (metrics.get("filters") or []):
        if isinstance(entry, dict) and entry.get("filter") == filter_mode:
            return entry
    return None


def _build_result_row(
    filter_entry: dict,
    metrics: dict,
    starting_capital: float | None,
    is_best_filter: bool,
) -> dict[str, Any]:
    """
    Assemble the column → value map for an audit.results INSERT. Missing keys
    land NULL (all metric columns are nullable in the schema).
    """
    row: dict[str, Any] = {}
    for src_key, col in _FILTER_TO_RESULTS_COLUMN.items():
        if src_key in filter_entry and filter_entry[src_key] is not None:
            row[col] = filter_entry[src_key]
    # Top-level-only metrics: valid when the promoted filter == best filter
    # (otherwise we'd misattribute another filter's sortino/calmar/etc).
    if is_best_filter:
        for src_key, col in _TOP_LEVEL_ONLY_COLUMNS.items():
            if src_key in metrics and metrics[src_key] is not None:
                row[col] = metrics[src_key]
    # Capital fields live outside metrics
    if starting_capital is not None:
        row["starting_capital"] = starting_capital
        # equity_multiplier = last equity_curve point if present
        curve = filter_entry.get("equity_curve") or []
        if curve:
            row["equity_multiplier"] = curve[-1]
            row["ending_capital"] = starting_capital * curve[-1]
    return row


# ─── Routes ─────────────────────────────────────────────────────────────────

# Canonical reference strategy for Simulator compare-to-canonical.
# Alpha Main is the default reference because Sharpe is leverage-invariant
# across the Alpha Low / Main / Max family (all three show identical Sharpe
# in the audit), so picking Alpha Main yields the same Sharpe comparison
# regardless of what leverage tier a candidate is testing. If it's ever
# absent, the endpoint errors rather than silently falling back — the
# Simulator's compare feature depends on a stable canonical reference.
CANONICAL_REFERENCE_STRATEGY = "Alpha Main"


@router.get("/canonical-reference")
def get_canonical_reference(cur=Depends(get_cursor)) -> dict[str, Any]:
    """Return the currently-canonical published strategy_version's metrics +
    config for side-by-side comparison against a Simulator candidate.

    Consumed by the Simulator UI's 'Compare to canonical' card (Stream D-small,
    Option 1 migration). The governance rule in docs/strategy_specification.md §5
    is applied client-side against the returned numbers.

    Returns:
        strategy_name, version_label, filter_mode, metrics_data_through,
        metrics (= audit.strategy_versions.current_metrics),
        config  (= audit.strategy_versions.config)
    """
    cur.execute(
        """
        SELECT s.display_name, s.filter_mode,
               sv.version_label, sv.current_metrics, sv.config,
               sv.metrics_data_through
        FROM audit.strategies s
        JOIN audit.strategy_versions sv USING (strategy_id)
        WHERE s.is_published = TRUE
          AND sv.is_active  = TRUE
          AND s.display_name = %s
        LIMIT 1
        """,
        (CANONICAL_REFERENCE_STRATEGY,),
    )
    row = cur.fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Canonical reference strategy {CANONICAL_REFERENCE_STRATEGY!r} "
                "not found. Expected one active, published strategy_version row. "
                "See docs/strategy_specification.md § 7 for migration status."
            ),
        )
    return {
        "strategy_name":        row["display_name"],
        "version_label":        row["version_label"],
        "filter_mode":          row["filter_mode"],
        "metrics_data_through": row["metrics_data_through"].isoformat()
                                if row["metrics_data_through"] else None,
        "metrics":              row["current_metrics"] or {},
        "config":               row["config"] or {},
    }


@router.post("/audits/{job_id}/promote")
def promote_audit(
    job_id: str,
    body: PromoteRequest,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Promote a completed audit as a published strategy version. Creates the
    strategy row (if the display_name is new), a new strategy_version, and a
    single audit.results row for the chosen filter_mode. All writes happen in
    one transaction (driven by get_cursor's commit-on-success).
    """
    # ── Load source data ────────────────────────────────────────────────
    job_json = _load_job_json(job_id)
    if job_json is None:
        raise HTTPException(status_code=404, detail="Audit not found")
    if str(job_json.get("status", "")).lower() != "complete":
        raise HTTPException(
            status_code=404,
            detail=f"Audit status is {job_json.get('status')!r}; only completed audits can be promoted",
        )

    results_blob = job_json.get("results") or {}
    metrics = results_blob.get("metrics") or {}
    starting_capital = results_blob.get("starting_capital")

    # Validate filter_mode is present in the audit's metrics
    filter_entry = _pick_filter_metrics(metrics, body.filter_mode)
    if filter_entry is None:
        available = [
            e.get("filter") for e in (metrics.get("filters") or []) if isinstance(e, dict)
        ]
        raise HTTPException(
            status_code=400,
            detail=f"filter_mode {body.filter_mode!r} not in audit metrics. Available: {available}",
        )

    # Ensure this job hasn't already been promoted
    cur.execute(
        "SELECT strategy_version_id FROM audit.jobs WHERE job_id = %s",
        (job_id,),
    )
    existing = cur.fetchone()
    if existing is None:
        # audit.jobs row doesn't exist yet — fabricate one using the same
        # finalize-hook path. Normally the worker persists at finalize, but if
        # the job ran before that code shipped there won't be a row.
        fees_table = metrics.get("fees_table") or []
        if not fees_table:
            raise HTTPException(
                status_code=500,
                detail="audit.jobs row missing and fees_table empty — cannot derive dates",
            )
        cur.execute(
            """
            INSERT INTO audit.jobs
                (job_id, strategy_version_id, status, completed_at,
                 date_from, date_to, config_overrides)
            VALUES (%s, NULL, 'complete', NOW(), %s, %s, %s::jsonb)
            """,
            (
                job_id,
                fees_table[0].get("date"),
                fees_table[-1].get("date"),
                json.dumps(job_json.get("params") or {}),
            ),
        )
    elif existing["strategy_version_id"] is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Audit already promoted as strategy_version_id={existing['strategy_version_id']}",
        )

    # ── Find or create audit.strategies row ─────────────────────────────
    cur.execute(
        "SELECT strategy_id, name FROM audit.strategies WHERE display_name = %s",
        (body.strategy_name,),
    )
    strat_row = cur.fetchone()
    if strat_row is not None:
        strategy_id = strat_row["strategy_id"]
    else:
        # Slugify for the UNIQUE `name` column; append -2, -3, ... on conflict
        base_slug = _slugify(body.strategy_name)
        slug = base_slug
        suffix = 1
        while True:
            cur.execute(
                "SELECT 1 FROM audit.strategies WHERE name = %s",
                (slug,),
            )
            if cur.fetchone() is None:
                break
            suffix += 1
            slug = f"{base_slug}-{suffix}"
        cur.execute(
            """
            INSERT INTO audit.strategies
                (name, display_name, description, filter_mode, is_published)
            VALUES (%s, %s, %s, %s, TRUE)
            RETURNING strategy_id
            """,
            (slug, body.strategy_name, body.description, body.filter_mode),
        )
        strategy_id = cur.fetchone()["strategy_id"]

    # ── Persist the user's filter selection into the config JSONB so
    #    executor paths that read sv.config->>'active_filter' find it
    #    without falling back to the factory default.
    #
    #    Stored as user-facing label form (e.g. "A - Tail Guardrail").
    #    Downstream consumers must normalize via TraderConfig._pick —
    #    the factory strips the "^[A-Z] - " prefix to get the canonical
    #    identifier matched against daily_signals.filter_name /
    #    live_deploys_signal.csv.
    config_to_persist = dict(job_json.get("params") or {})
    config_to_persist["active_filter"] = body.filter_mode
    new_hash = hash_config(config_to_persist)

    # ── INSERT-vs-UPDATE branch on config_hash match. If this strategy
    #    already has a version with an identical identity-bearing config,
    #    treat this promote as a manual refresh of that version rather
    #    than fabricating a duplicate.
    cur.execute(
        """
        SELECT strategy_version_id, version_label
        FROM audit.strategy_versions
        WHERE strategy_id = %s AND config_hash = %s
        """,
        (strategy_id, new_hash),
    )
    existing = cur.fetchone()

    if existing is not None:
        strategy_version_id = str(existing["strategy_version_id"])
        resolved_version_label = existing["version_label"]
        reused_existing_version = True
        # Deactivate any OTHER active versions for this strategy, then
        # re-activate the matching one. The partial unique index
        # idx_one_active_version_per_strategy enforces the invariant.
        cur.execute(
            """
            UPDATE audit.strategy_versions
            SET is_active = FALSE
            WHERE strategy_id = %s
              AND is_active = TRUE
              AND strategy_version_id <> %s
            """,
            (strategy_id, strategy_version_id),
        )
        cur.execute(
            """
            UPDATE audit.strategy_versions
            SET is_active    = TRUE,
                published_at = COALESCE(published_at, NOW())
            WHERE strategy_version_id = %s
            """,
            (strategy_version_id,),
        )
    else:
        resolved_version_label = body.version_label
        reused_existing_version = False
        # Reject duplicate version_label only on the new-version path. A
        # config_hash match above reuses whatever label the existing
        # version already has, so the label the user typed is irrelevant
        # in that branch.
        cur.execute(
            """
            SELECT 1 FROM audit.strategy_versions
            WHERE strategy_id = %s AND version_label = %s
            """,
            (strategy_id, body.version_label),
        )
        if cur.fetchone() is not None:
            raise HTTPException(
                status_code=409,
                detail=f"version_label {body.version_label!r} already exists for this strategy",
            )
        # Fresh version: deactivate all current actives, then insert.
        cur.execute(
            """
            UPDATE audit.strategy_versions
            SET is_active = FALSE
            WHERE strategy_id = %s AND is_active = TRUE
            """,
            (strategy_id,),
        )
        cur.execute(
            """
            INSERT INTO audit.strategy_versions
                (strategy_id, version_label, config, config_hash, notes, is_active, published_at)
            VALUES (%s, %s, %s::jsonb, %s, %s, TRUE, NOW())
            RETURNING strategy_version_id
            """,
            (
                strategy_id,
                body.version_label,
                json.dumps(config_to_persist),
                new_hash,
                body.description,
            ),
        )
        strategy_version_id = str(cur.fetchone()["strategy_version_id"])

    # ── Insert the result row ───────────────────────────────────────────
    is_best = metrics.get("best_filter") == body.filter_mode
    result_row = _build_result_row(
        filter_entry, metrics, starting_capital, is_best_filter=is_best,
    )
    columns = ["job_id", "filter_mode"] + list(result_row.keys())
    placeholders = ["%s"] * len(columns)
    values: list[Any] = [job_id, body.filter_mode] + list(result_row.values())
    cur.execute(
        f"""
        INSERT INTO audit.results ({", ".join(columns)})
        VALUES ({", ".join(placeholders)})
        RETURNING result_id
        """,
        values,
    )
    result_id = str(cur.fetchone()["result_id"])

    # ── Link audit.jobs row to the version (new or reused) ──────────────
    cur.execute(
        """
        UPDATE audit.jobs
        SET strategy_version_id = %s,
            promoted_at = NOW()
        WHERE job_id = %s
        """,
        (strategy_version_id, job_id),
    )

    # ── Refresh the denormalized card cache. Derive data_through from the
    #    fees_table's last row when available (same source the worker's
    #    audit.jobs persistence uses).
    fees_table = metrics.get("fees_table") or []
    data_through = None
    if fees_table and fees_table[-1].get("date"):
        data_through = fees_table[-1]["date"]
    current_metrics = build_current_metrics(result_row)
    cur.execute(
        """
        UPDATE audit.strategy_versions
        SET current_metrics       = %s::jsonb,
            metrics_updated_at    = NOW(),
            metrics_data_through  = %s
        WHERE strategy_version_id = %s
        """,
        (json.dumps(current_metrics), data_through, strategy_version_id),
    )

    return {
        "strategy_id":             strategy_id,
        "strategy_version_id":     strategy_version_id,
        "result_id":               result_id,
        "strategy_name":           body.strategy_name,
        "version_label":           resolved_version_label,
        "filter_mode":             body.filter_mode,
        "is_published":            True,
        "promoted_filter_is_best": is_best,
        "reused_existing_version": reused_existing_version,
        "result":                  {k: result_row[k] for k in result_row},
    }

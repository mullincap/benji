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
    "sortino":        "sortino",
    "calmar":         "calmar",
    "omega":          "omega",
    "ulcer_index":    "ulcer_index",
    "fa_oos_sharpe":  "fa_oos_sharpe",
    "flat_days":      "flat_days",
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

    # ── Reject duplicate version_label on this strategy ─────────────────
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

    # ── Deactivate other versions, then insert new active version ───────
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
            (strategy_id, version_label, config, notes, is_active, published_at)
        VALUES (%s, %s, %s::jsonb, %s, TRUE, NOW())
        RETURNING strategy_version_id
        """,
        (
            strategy_id,
            body.version_label,
            json.dumps(job_json.get("params") or {}),
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

    # ── Link audit.jobs row to the new version ──────────────────────────
    cur.execute(
        """
        UPDATE audit.jobs
        SET strategy_version_id = %s,
            promoted_at = NOW()
        WHERE job_id = %s
        """,
        (strategy_version_id, job_id),
    )

    return {
        "strategy_id":          strategy_id,
        "strategy_version_id":  strategy_version_id,
        "result_id":            result_id,
        "strategy_name":        body.strategy_name,
        "version_label":        body.version_label,
        "filter_mode":          body.filter_mode,
        "is_published":         True,
        "promoted_filter_is_best": is_best,
        "result":               {k: result_row[k] for k in result_row},
    }

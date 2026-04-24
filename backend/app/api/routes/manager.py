"""
backend/app/api/routes/manager.py
==================================
Manager module — Claude-powered portfolio intelligence layer.

Endpoints:
  GET  /api/manager/overview                          — full portfolio context
  GET  /api/manager/conversations                     — list all conversations
  POST /api/manager/conversations                     — create new conversation
  GET  /api/manager/conversations/{conversation_id}   — full history
  DELETE /api/manager/conversations/{conversation_id} — cascade delete
  POST /api/manager/conversations/{id}/messages       — send + Claude response
  POST /api/manager/briefing                          — auto-briefing (internal token)
"""

from __future__ import annotations

import os
import re
import json
import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from ...db import get_cursor
from ...core.config import settings, load_secrets
from .admin import require_admin
from .allocator import refresh_snapshots

load_secrets()

ANTHROPIC_API_KEY = settings.ANTHROPIC_API_KEY or os.environ.get("ANTHROPIC_API_KEY", "")
INTERNAL_API_TOKEN = settings.INTERNAL_API_TOKEN or os.environ.get("INTERNAL_API_TOKEN", "")

CLAUDE_MODEL = settings.CLAUDE_MODEL
CLAUDE_MAX_TOKENS = settings.CLAUDE_MAX_TOKENS

router = APIRouter(
    prefix="/api/manager",
    tags=["manager"],
    dependencies=[Depends(require_admin)],
)


# ── Request models ───────────────────────────────────────────────────────────

class MessageRequest(BaseModel):
    content: str


# ── Helpers ──────────────────────────────────────────────────────────────────

def _dec(val: Any) -> float | None:
    if val is None:
        return None
    return float(val)


def _iso(val: Any) -> str | None:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


def _fetch_portfolio_context(cur) -> dict[str, Any]:
    """Build the full portfolio context dict used by overview and chat."""

    # ── Allocations with strategy info ───────────────────────────────────
    # Active-only. Drives CURRENT-STATE aggregates: TOTAL AUM, the
    # allocations list rendered in the overview, today's P&L baseline.
    # Historical aggregates (Max DD, portfolio equity curve, total P&L
    # baseline) use the separate historical_alloc_ids set below so that
    # pausing an allocation doesn't silently rewrite the historical
    # denominator.
    cur.execute("""
        SELECT
            a.allocation_id, a.strategy_version_id, a.connection_id,
            a.capital_usd, a.status,
            sv.version_label, sv.strategy_id,
            s.display_name AS strategy_display_name, s.name AS strategy_name,
            s.filter_mode,
            ec.exchange, ec.label AS connection_label
        FROM user_mgmt.allocations a
        JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
        JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
        JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
        WHERE a.status = 'active'
        ORDER BY a.created_at
    """)
    allocations = cur.fetchall()

    total_aum = Decimal(0)
    alloc_list = []
    all_version_ids = []       # native UUIDs for SQL queries
    all_alloc_ids = []         # native UUIDs for SQL queries

    for a in allocations:
        total_aum += a["capital_usd"] or Decimal(0)
        all_version_ids.append(a["strategy_version_id"])
        all_alloc_ids.append(a["allocation_id"])
        alloc_list.append({
            "allocation_id": str(a["allocation_id"]),
            "strategy_version_id": str(a["strategy_version_id"]),
            "connection_id": str(a["connection_id"]),
            "capital_usd": _dec(a["capital_usd"]),
            "status": a["status"],
            "version_label": a["version_label"],
            "strategy_display_name": a["strategy_display_name"] or a["strategy_name"],
            "filter_mode": a["filter_mode"],
            "exchange": a["exchange"],
            "connection_label": a["connection_label"],
        })

    # ── Historical allocations (all statuses) ────────────────────────────
    # Drives HISTORICAL aggregates that should not migrate when the set
    # of currently-active allocations changes:
    #   - Max DD  — lifetime worst peak-to-trough equity
    #   - Portfolio Equity curve — historical 30D account equity
    #   - perf_by_alloc fallback denominator
    # Includes paused and closed allocations; excludes connections that
    # have never held strategy capital (exploratory links) so Max DD
    # doesn't pick up equity from wallets that weren't part of the
    # allocator's book.
    cur.execute("""
        SELECT DISTINCT allocation_id, connection_id
        FROM user_mgmt.allocations
    """)
    historical_rows = cur.fetchall()
    historical_alloc_ids = [r["allocation_id"] for r in historical_rows]
    historical_connection_uuids = list({r["connection_id"] for r in historical_rows})

    # ── Performance daily (last 30 days per allocation) ──────────────────
    perf_by_alloc: dict[str, list[dict]] = {}
    today_return_by_alloc: dict[str, float] = {}
    drawdown_by_alloc: dict[str, float] = {}
    today = datetime.date.today()

    # Historical: pull perf rows for every allocation that has ever been
    # strategy capital, not just active ones. The chart / Max DD / total P&L
    # baseline downstream all aggregate across perf_by_alloc — if we filtered
    # to active-only here, pausing an allocation would silently shorten the
    # 30D curve and move the DD peak.
    if historical_alloc_ids:
        cur.execute("""
            SELECT allocation_id, date, equity_usd, daily_return, drawdown
            FROM user_mgmt.performance_daily
            WHERE allocation_id = ANY(%s::uuid[])
              AND date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY allocation_id, date
        """, (historical_alloc_ids,))
        for row in cur.fetchall():
            aid = str(row["allocation_id"])
            perf_by_alloc.setdefault(aid, []).append({
                "date": row["date"].isoformat(),
                "equity_usd": _dec(row["equity_usd"]),
                "daily_return": _dec(row["daily_return"]),
                "drawdown": _dec(row["drawdown"]),
            })
            if row["date"] == today:
                today_return_by_alloc[aid] = float(row["daily_return"] or 0)
            dd = float(row["drawdown"] or 0)
            if dd < drawdown_by_alloc.get(aid, 0):
                drawdown_by_alloc[aid] = dd

    # ── Fallback: compute daily performance from exchange_snapshots ──────
    # When performance_daily is empty (no pipeline populates it yet),
    # build per-day equity + returns from the last exchange snapshot per day.
    # Same historical-scope rule as the primary path above.
    if not perf_by_alloc and historical_alloc_ids:
        cur.execute("""
            SELECT DISTINCT ON (a.allocation_id, date_trunc('day', es.snapshot_at))
                a.allocation_id,
                date_trunc('day', es.snapshot_at)::date AS day,
                es.total_equity_usd
            FROM user_mgmt.exchange_snapshots es
            JOIN user_mgmt.allocations a ON a.connection_id = es.connection_id
            WHERE a.allocation_id = ANY(%s::uuid[])
              AND es.fetch_ok = TRUE
              AND es.snapshot_at >= NOW() - INTERVAL '30 days'
            ORDER BY a.allocation_id, date_trunc('day', es.snapshot_at), es.snapshot_at DESC
        """, (historical_alloc_ids,))
        # Group by allocation
        snap_by_alloc: dict[str, list[tuple]] = {}
        for row in cur.fetchall():
            aid = str(row["allocation_id"])
            snap_by_alloc.setdefault(aid, []).append(
                (row["day"].isoformat(), float(row["total_equity_usd"] or 0))
            )
        for aid, snaps in snap_by_alloc.items():
            snaps.sort()
            peak = 0.0
            prev_eq = None
            for d, eq in snaps:
                daily_ret = ((eq / prev_eq - 1) * 100) if prev_eq and prev_eq > 0 else 0.0
                peak = max(peak, eq)
                dd = ((eq / peak - 1) * 100) if peak > 0 else 0.0
                prev_eq = eq
                perf_by_alloc.setdefault(aid, []).append({
                    "date": d,
                    "equity_usd": eq,
                    "daily_return": round(daily_ret, 4),
                    "drawdown": round(dd, 4),
                })
                if d == today.isoformat():
                    today_return_by_alloc[aid] = daily_ret
                if dd < drawdown_by_alloc.get(aid, 0):
                    drawdown_by_alloc[aid] = dd

    # ── Sharpe per strategy version (most recent audit result) ───────────
    sharpe_by_version: dict[str, float | None] = {}
    if all_version_ids:
        cur.execute("""
            SELECT DISTINCT ON (j.strategy_version_id)
                j.strategy_version_id, r.sharpe
            FROM audit.results r
            JOIN audit.jobs j ON r.job_id = j.job_id
            WHERE j.strategy_version_id = ANY(%s::uuid[])
              AND j.status = 'complete'
            ORDER BY j.strategy_version_id, j.completed_at DESC NULLS LAST
        """, (all_version_ids,))
        for row in cur.fetchall():
            sharpe_by_version[str(row["strategy_version_id"])] = _dec(row["sharpe"])

    # Enrich allocations with perf data
    for a in alloc_list:
        aid = a["allocation_id"]
        vid = a["strategy_version_id"]
        a["daily_return_today"] = today_return_by_alloc.get(aid, 0.0)
        a["max_drawdown"] = drawdown_by_alloc.get(aid, 0.0)
        a["sharpe"] = sharpe_by_version.get(vid)
        a["performance_30d"] = perf_by_alloc.get(aid, [])

    # ── Aggregated metrics ───────────────────────────────────────────────
    total_aum_f = float(total_aum)
    total_dollar_pnl_today = sum(
        (a["capital_usd"] or 0) * (today_return_by_alloc.get(a["allocation_id"], 0) / 100)
        for a in alloc_list
    )
    today_pct = (total_dollar_pnl_today / total_aum_f * 100) if total_aum_f > 0 else 0.0

    # WTD / MTD: weighted by capital
    wtd_dollar = 0.0
    mtd_dollar = 0.0
    week_start = today - datetime.timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    for a in alloc_list:
        aid = a["allocation_id"]
        cap = a["capital_usd"] or 0
        for p in perf_by_alloc.get(aid, []):
            d = datetime.date.fromisoformat(p["date"])
            ret = p["daily_return"] or 0
            if d >= week_start:
                wtd_dollar += cap * (ret / 100)
            if d >= month_start:
                mtd_dollar += cap * (ret / 100)
    wtd_pct = (wtd_dollar / total_aum_f * 100) if total_aum_f > 0 else 0.0
    mtd_pct = (mtd_dollar / total_aum_f * 100) if total_aum_f > 0 else 0.0

    # Lifetime account-wide max drawdown, computed from 5-min bucketed wallet
    # equity summed across connections. Intraday cadence is required so the
    # metric captures peak-to-trough losses that happen WITHIN a session and
    # recover before close — daily-close buckets would mask them entirely.
    # USD magnitude is the dollar gap at the worst-percentage moment.
    #
    # Historical metric — scope is all-ever connections. Pausing or closing
    # an allocation must NOT silently redefine the historical denominator.
    # Using historical_connection_uuids (connections that have ever held
    # strategy capital) instead of the active-only set fixes a 2026-04-22
    # bug where pausing Alpha Main made Max DD jump from -9.9% → -13.0%
    # because the calc silently switched from BloFin+Binance combined to
    # BloFin-only.
    connection_uuids = historical_connection_uuids
    max_dd = 0.0
    max_dd_usd = 0.0
    if connection_uuids:
        cur.execute("""
            WITH bucketed AS (
              SELECT connection_id,
                     snapshot_at,
                     to_timestamp(
                       floor(extract(epoch from snapshot_at) / 300) * 300
                     ) AS bucket,
                     total_equity_usd AS equity
              FROM user_mgmt.exchange_snapshots
              WHERE connection_id = ANY(%s::uuid[])
                AND fetch_ok = TRUE
                AND total_equity_usd IS NOT NULL
            ),
            latest_per_conn AS (
              SELECT DISTINCT ON (connection_id, bucket)
                     connection_id, bucket, equity
              FROM bucketed
              ORDER BY connection_id, bucket, snapshot_at DESC
            )
            SELECT bucket, SUM(equity) AS total_equity
            FROM latest_per_conn
            GROUP BY bucket
            ORDER BY bucket
        """, (connection_uuids,))
        intraday_equity_series = [float(r["total_equity"]) for r in cur.fetchall()]
        if intraday_equity_series:
            peak = intraday_equity_series[0]
            for eq in intraday_equity_series:
                peak = max(peak, eq)
                if peak > 0:
                    dd_pct = (eq - peak) / peak * 100.0
                    if dd_pct < max_dd:
                        max_dd = dd_pct
                        max_dd_usd = eq - peak

    # ── Portfolio equity curve (sum of equity_usd per date) ──────────────
    # Historical metric — iterate all perf rows (including paused/closed
    # allocations' history), not just alloc_list. Pausing an allocation
    # must not silently shorten or reshape the 30D curve.
    equity_by_date: dict[str, float] = {}
    for perf_list in perf_by_alloc.values():
        for p in perf_list:
            equity_by_date[p["date"]] = equity_by_date.get(p["date"], 0) + (p["equity_usd"] or 0)

    portfolio_equity = [
        {"date": d, "equity_usd": v}
        for d, v in sorted(equity_by_date.items())
    ]

    # ── Daily signals (latest per strategy version) ──────────────────────
    signals = []
    if all_version_ids:
        cur.execute("""
            SELECT DISTINCT ON (ds.strategy_version_id)
                ds.signal_batch_id, ds.signal_date, ds.strategy_version_id,
                ds.computed_at, ds.sit_flat, ds.filter_name,
                sv.version_label,
                s.display_name AS strategy_display_name
            FROM user_mgmt.daily_signals ds
            JOIN audit.strategy_versions sv ON sv.strategy_version_id = ds.strategy_version_id
            JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
            WHERE ds.strategy_version_id = ANY(%s::uuid[])
            ORDER BY ds.strategy_version_id, ds.signal_date DESC
        """, (all_version_ids,))
        signal_rows = cur.fetchall()

        batch_ids = [r["signal_batch_id"] for r in signal_rows]
        items_by_batch: dict[str, list[dict]] = {}
        if batch_ids:
            cur.execute("""
                SELECT dsi.signal_batch_id, dsi.rank, dsi.weight,
                       sym.base AS symbol
                FROM user_mgmt.daily_signal_items dsi
                JOIN market.symbols sym ON sym.symbol_id = dsi.symbol_id
                WHERE dsi.signal_batch_id = ANY(%s::uuid[])
                ORDER BY dsi.rank
            """, (batch_ids,))
            for row in cur.fetchall():
                bid = str(row["signal_batch_id"])
                items_by_batch.setdefault(bid, []).append({
                    "symbol": row["symbol"],
                    "rank": row["rank"],
                    "weight": _dec(row["weight"]),
                })

        for r in signal_rows:
            bid = str(r["signal_batch_id"])
            signals.append({
                "strategy_display_name": r["strategy_display_name"],
                "signal_date": r["signal_date"].isoformat(),
                "sit_flat": r["sit_flat"],
                "filter_name": r["filter_name"],
                "computed_at": _iso(r["computed_at"]),
                "symbols": items_by_batch.get(bid, []),
            })

    # ── Pipeline status ──────────────────────────────────────────────────
    cur.execute("""
        SELECT status, created_at, started_at, completed_at
        FROM market.compiler_jobs
        ORDER BY created_at DESC LIMIT 1
    """)
    compiler_job = cur.fetchone()

    cur.execute("""
        SELECT status, created_at, started_at, completed_at
        FROM market.indexer_jobs
        ORDER BY created_at DESC LIMIT 1
    """)
    indexer_job = cur.fetchone()

    def _job_status(job: dict | None) -> dict:
        if not job:
            return {"status": "unknown", "last_run": None}
        return {
            "status": job["status"],
            "last_run": _iso(job["completed_at"] or job["started_at"] or job["created_at"]),
        }

    signals_time = None
    if signals:
        times = [s["computed_at"] for s in signals if s["computed_at"]]
        if times:
            signals_time = max(times)

    # Trader: derive status from allocations.runtime_state — the durability
    # layer's source of truth for live trader subprocesses (see commit
    # f538b7c / trader_supervisor.py). The supervisor uses STALE_THRESHOLD_MIN
    # to decide whether a trader needs respawning; we reuse the same constant
    # so the Pipeline Status row and the supervisor never disagree on what
    # "stalled" means.
    #
    # Per-allocation classification:
    #   - phase='active'  AND updated within threshold → 'running'
    #   - phase='active'  AND updated past threshold   → 'stalled'
    #   - phase IN ('closed','no_entry','errored') AND same UTC day → 'complete'
    #   - updated yesterday or earlier                 → 'idle'
    #   - no runtime_state                             → (ignored in aggregation)
    #
    # Portfolio aggregation: worst-case wins (stalled > running > idle >
    # complete) so the most actionable state surfaces first. If no allocation
    # has a runtime_state at all → 'no_deployments'.
    from ...cli.trader_supervisor import STALE_THRESHOLD_MIN  # noqa: PLC0415
    cur.execute(f"""
        SELECT a.runtime_state->>'phase'                       AS phase,
               (a.runtime_state->>'updated_at')::timestamptz   AS updated_at,
               (a.runtime_state->>'date')                       AS rs_date,
               (NOW() - (a.runtime_state->>'updated_at')::timestamptz)
                 > INTERVAL '{STALE_THRESHOLD_MIN} minutes'     AS is_stale
        FROM user_mgmt.allocations a
        WHERE a.status IN ('active', 'paused')
          AND a.runtime_state IS NOT NULL
          AND a.runtime_state->>'updated_at' IS NOT NULL
    """)
    rs_rows = cur.fetchall()

    today_utc_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    per_alloc_states: list[str] = []
    last_run_ts = None
    for r in rs_rows:
        phase = r["phase"]
        updated_at = r["updated_at"]
        rs_date = r["rs_date"]
        is_stale = bool(r["is_stale"])
        if updated_at and (last_run_ts is None or updated_at > last_run_ts):
            last_run_ts = updated_at
        if phase == "active":
            per_alloc_states.append("stalled" if is_stale else "running")
        elif phase in ("closed", "no_entry", "errored") and rs_date == today_utc_str:
            per_alloc_states.append("complete")
        else:
            per_alloc_states.append("idle")

    if not per_alloc_states:
        trader_status = {"status": "no_deployments", "last_run": None}
    else:
        if "stalled" in per_alloc_states:
            agg = "stalled"
        elif "running" in per_alloc_states:
            agg = "running"
        elif "idle" in per_alloc_states:
            agg = "idle"
        else:
            agg = "complete"
        trader_status = {"status": agg, "last_run": _iso(last_run_ts)}

    pipeline = {
        "compiler": _job_status(compiler_job),
        "indexer": _job_status(indexer_job),
        "signals_last_generated": signals_time,
        "trader": trader_status,
    }

    # ── Exchange snapshots ───────────────────────────────────────────────
    cur.execute("""
        SELECT DISTINCT ON (ec.connection_id)
            ec.connection_id, ec.exchange, ec.label,
            es.snapshot_at, es.total_equity_usd, es.available_usd,
            es.used_margin_usd, es.unrealized_pnl, es.positions,
            es.fetch_ok
        FROM user_mgmt.exchange_connections ec
        LEFT JOIN user_mgmt.exchange_snapshots es
            ON es.connection_id = ec.connection_id
            AND es.fetch_ok = TRUE
        WHERE ec.status = 'active'
        ORDER BY ec.connection_id, es.snapshot_at DESC NULLS LAST
    """)
    snap_rows = cur.fetchall()

    exchange_snapshots = []
    total_live_equity = Decimal(0)
    total_unrealized = Decimal(0)
    has_snaps = False
    for r in snap_rows:
        if r["snapshot_at"] is not None:
            has_snaps = True
            total_live_equity += r["total_equity_usd"] or Decimal(0)
            total_unrealized += r["unrealized_pnl"] or Decimal(0)
        exchange_snapshots.append({
            "connection_id": str(r["connection_id"]),
            "exchange": r["exchange"],
            "label": r["label"],
            "snapshot_at": _iso(r["snapshot_at"]),
            "total_equity_usd": _dec(r["total_equity_usd"]),
            "available_usd": _dec(r["available_usd"]),
            "unrealized_pnl": _dec(r["unrealized_pnl"]),
            "positions": r["positions"] if r["positions"] is not None else [],
        })

    # ── Intraday equity curve (latest UTC day with data, 15-min intervals) ─
    # Find the most recent UTC day that has snapshots for any of these
    # allocations, then return that day's 15-min buckets. Mirrors the
    # Session Logs viewer's "auto-scope to most recent session" behaviour
    # so the chart keeps showing the last session between close and the
    # next open rather than a blank "no data" state.
    #
    # DISTINCT ON (allocation_id, bucket) + ORDER BY snapshot_at DESC →
    # take the LATEST snapshot per allocation within each 15-min bucket.
    # Then SUM across allocations in Python. (Prior AVG() bug collapsed
    # multi-allocation equity into a mean — preserved as DISTINCT ON + SUM.)
    intraday_date: str | None = None
    intraday_equity: list[dict[str, Any]] = []
    if all_alloc_ids:
        # Pick the latest UTC day that has at least one snapshot INSIDE the
        # 06:00+ session window — otherwise the cron's post-close snapshots
        # (00:00–05:59 UTC after 23:55 session close) would anchor us to
        # "today" with no buckets that land on the 06:00–23:45 frontend grid.
        #
        # 7-day lookback bounds the hypertable scan; LIMIT 1 makes the planner
        # walk the snapshot_at index backward and stop on first match, so the
        # extract(hour) filter is paid per-row only until a hit.
        cur.execute("""
            SELECT date_trunc('day', es.snapshot_at)::date AS day
            FROM user_mgmt.exchange_snapshots es
            JOIN user_mgmt.allocations a ON a.connection_id = es.connection_id
            WHERE a.allocation_id = ANY(%s::uuid[])
              AND es.fetch_ok = TRUE
              AND es.total_equity_usd IS NOT NULL
              AND es.snapshot_at >= NOW() - INTERVAL '7 days'
              AND extract(hour from es.snapshot_at) >= 6
            ORDER BY es.snapshot_at DESC
            LIMIT 1
        """, (all_alloc_ids,))
        day_row = cur.fetchone()
        if day_row:
            intraday_date = day_row["day"].isoformat()
            cur.execute("""
                SELECT DISTINCT ON (a.allocation_id, bucket)
                    date_trunc('hour', es.snapshot_at) +
                      INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM es.snapshot_at) / 15)
                      AS bucket,
                    a.allocation_id,
                    es.total_equity_usd AS equity_usd
                FROM user_mgmt.exchange_snapshots es
                JOIN user_mgmt.allocations a ON a.connection_id = es.connection_id
                WHERE a.allocation_id = ANY(%s::uuid[])
                  AND es.fetch_ok = TRUE
                  AND date_trunc('day', es.snapshot_at)::date = %s::date
                ORDER BY a.allocation_id, bucket, es.snapshot_at DESC
            """, (all_alloc_ids, intraday_date))

            from collections import defaultdict
            bucketed: dict = defaultdict(float)
            for r in cur.fetchall():
                if r["equity_usd"] is None:
                    continue
                bucketed[r["bucket"]] += float(r["equity_usd"])
            intraday_equity = [
                {"time": b.isoformat(), "equity_usd": v}
                for b, v in sorted(bucketed.items())
            ]

    # USD P&L is the actual dollar profit over the window. Given the period
    # % `p` and the current live equity `E`, the implied period-start equity
    # is E / (1 + p/100), so profit = E - E/(1+p/100). Applying p directly
    # to E is wrong — that would treat the % as being on the end value
    # instead of the start value. Example: E=$4,274.08 at +94.12% MTD
    # implies a start of $2,201.77 and profit of $2,072.31 (not $4,022.94
    # which is what pct × E would yield).
    def _usd_from_pct(pct: float, end_equity: float) -> float:
        denom = 1.0 + pct / 100.0
        if end_equity == 0 or denom == 0:
            return 0.0
        return end_equity - end_equity / denom

    usd_base = float(total_live_equity) if has_snaps else total_aum_f
    today_usd = _usd_from_pct(today_pct, usd_base)
    wtd_usd   = _usd_from_pct(wtd_pct,   usd_base)
    mtd_usd   = _usd_from_pct(mtd_pct,   usd_base)

    # Total P&L since first recorded day. Uses the earliest equity in the
    # portfolio_equity_30d series as the baseline — same anchor the chart
    # visually starts from, so the card value is consistent with the curve
    # above it.
    initial_equity = portfolio_equity[0]["equity_usd"] if portfolio_equity else 0.0
    total_pnl_usd = (usd_base - initial_equity) if initial_equity else 0.0
    total_pnl_pct = (
        (total_pnl_usd / initial_equity * 100) if initial_equity else 0.0
    )

    return {
        "allocations": alloc_list,
        "total_aum": total_aum_f,
        "today_pct": round(today_pct, 2),
        "today_usd": round(today_usd, 2),
        "wtd_pct": round(wtd_pct, 2),
        "wtd_usd": round(wtd_usd, 2),
        "mtd_pct": round(mtd_pct, 2),
        "mtd_usd": round(mtd_usd, 2),
        "total_pnl_usd": round(total_pnl_usd, 2),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "max_drawdown": round(max_dd, 1),
        "max_drawdown_usd": round(max_dd_usd, 2),
        "portfolio_equity_30d": portfolio_equity,
        "intraday_equity": intraday_equity,
        "intraday_date": intraday_date,
        "signals": signals,
        "pipeline": pipeline,
        "exchange_snapshots": exchange_snapshots,
        "total_live_equity_usd": float(total_live_equity) if has_snaps else None,
        "total_unrealized_pnl": float(total_unrealized) if has_snaps else None,
    }


def _format_portfolio_context(ctx: dict[str, Any]) -> str:
    """Format portfolio context dict into the text block for Claude's system prompt."""
    lines = []
    n = len(ctx["allocations"])
    lines.append(f"Total AUM: ${ctx['total_aum']:,.0f} across {n} active allocation{'s' if n != 1 else ''}")

    if ctx["total_live_equity_usd"] is not None:
        lines.append(
            f"Total live equity: ${ctx['total_live_equity_usd']:,.0f} "
            f"(unrealized P&L: ${ctx['total_unrealized_pnl']:+,.0f})"
        )

    lines.append("")
    for i, a in enumerate(ctx["allocations"], 1):
        ret = a["daily_return_today"]
        dd = a["max_drawdown"]
        sharpe = f"{a['sharpe']:.2f}" if a["sharpe"] is not None else "n/a"
        lines.append(
            f"Allocation {i}: ${a['capital_usd']:,.0f} · {a['exchange']} · "
            f"{a['strategy_display_name']} · today {ret:+.2f}% · "
            f"drawdown {dd:.1f}% · sharpe {sharpe}"
        )

    # Live exchange data
    snaps = [s for s in ctx["exchange_snapshots"] if s["snapshot_at"]]
    if snaps:
        lines.append("")
        snap_time = max(s["snapshot_at"] for s in snaps)
        lines.append(f"Live exchange data (as of {snap_time}):")
        for s in snaps:
            eq = s["total_equity_usd"] or 0
            av = s["available_usd"] or 0
            up = s["unrealized_pnl"] or 0
            lines.append(f"  {s['exchange']}: equity=${eq:,.0f} available=${av:,.0f} unrealized=${up:+,.0f}")
            positions = s.get("positions") or []
            if positions:
                pos_parts = [
                    f"{p['symbol']}: {p['side']} {p['size']} @ {p['entry_price']}"
                    for p in positions
                ]
                lines.append(f"  Open positions: {', '.join(pos_parts)}")
            else:
                lines.append("  Open positions: flat")

    # Signals
    if ctx["signals"]:
        lines.append("")
        lines.append("Today's signals:")
        for sig in ctx["signals"]:
            syms = [s["symbol"] for s in sig["symbols"]]
            sym_str = ", ".join(syms[:10])
            if len(syms) > 10:
                sym_str += f" (+{len(syms)-10} more)"
            filt = sig["filter_name"] or "none"
            if sig["sit_flat"]:
                lines.append(f"  {sig['strategy_display_name']}: SIT FLAT ({filt})")
            else:
                lines.append(f"  {sig['strategy_display_name']}: {sym_str} ({len(syms)} symbols, filter: {filt})")

    # Pipeline
    lines.append("")
    lines.append("Pipeline:")
    p = ctx["pipeline"]
    lines.append(f"  compiler: {p['compiler']['status']} · last run {p['compiler']['last_run'] or 'never'}")
    lines.append(f"  indexer: {p['indexer']['status']} · last run {p['indexer']['last_run'] or 'never'}")
    lines.append(f"  signals: last generated {p['signals_last_generated'] or 'never'}")

    return "\n".join(lines)


SYSTEM_PROMPT_TEMPLATE = """You are the portfolio intelligence layer for a quantitative crypto trading platform called 3M. You are speaking with a professional institutional allocator managing multiple strategies across multiple exchanges.

Be concise, precise, and analytical. Use exact numbers. Flag risks proactively. Lead responses with the total portfolio view, then break down per allocation when relevant. For any action that modifies the portfolio (pause allocation, adjust capital, add symbols to market.symbols), you MUST present a structured summary before executing — never act without explicit user confirmation.

When proposing an action, include a JSON block at the end of your response in this exact format:
{{"action": true, "type": "pause_allocation|adjust_capital|add_symbol", "params": {{}}, "summary": "one line description"}}

If no action is being proposed, do not include any JSON block.

Current portfolio context:
{portfolio_context}

Today: {today} UTC
Read-only mode: {read_only}"""


async def _call_claude(messages: list[dict], system_prompt: str) -> str:
    """Call the Anthropic Messages API via httpx."""
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured")

    api_messages = [{"role": m["role"], "content": m["content"]} for m in messages]

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": CLAUDE_MAX_TOKENS,
                "system": system_prompt,
                "messages": api_messages,
            },
        )
        if resp.status_code != 200:
            detail = resp.text[:200]
            raise Exception(f"Anthropic API {resp.status_code}: {detail}")
        data = resp.json()
        return data["content"][0]["text"]


# ── Routes ───────────────────────────────────────────────────────────────────

@router.get("/overview")
def overview(cur=Depends(get_cursor)) -> dict[str, Any]:
    return _fetch_portfolio_context(cur)


# ── Time-series for Portfolio Equity + Daily Returns charts ─────────────────
#
# Split out from /overview so the chart range tabs (1D | 1W | 1M | ALL) can
# refetch series data without reloading the whole overview payload. Overview
# still returns portfolio_equity_30d for backward compat; charts now read
# from this endpoint so they can offer multiple ranges.

@router.get("/portfolio-series")
def portfolio_series(
    range: str | None = None,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Return portfolio equity + daily-returns series for a given range.

    Range semantics:
      1D  : intraday, last 24h, 5-min granularity from exchange_snapshots.
            Daily returns are not meaningful at this cadence and return []
            — frontend disables the 1D tab on the Daily Returns card.
      1W  : daily close, last 7 calendar days.
      1M  : daily close, last 30 calendar days.
      ALL : daily close, full history available in performance_daily /
            exchange_snapshots, unbounded.

    Data source: performance_daily first; if empty, fall back to the
    last exchange_snapshots row per UTC day (matches the overview
    endpoint's fallback logic).

    Response:
      {
        range, granularity, first_data_date, real_days,
        portfolio_equity: [{date, equity_usd}],
        daily_returns:    [{date, return_pct, return_usd}]
      }

    Bars / points are only emitted for dates where real data exists.
    No forward-fill, no placeholder values — missing days are missing.
    """
    range_up = (range or "1M").upper()
    if range_up not in ("1D", "1W", "1M", "ALL"):
        range_up = "1M"

    # 1. Active allocations (any status that's not archived)
    cur.execute("""
        SELECT a.allocation_id, a.connection_id, a.capital_usd
        FROM user_mgmt.allocations a
        WHERE a.status IN ('active', 'paused')
    """)
    alloc_rows = cur.fetchall()
    if not alloc_rows:
        return {
            "range": range_up,
            "granularity": "intraday" if range_up == "1D" else "daily",
            "first_data_date": None,
            "real_days": 0,
            "portfolio_equity": [],
            "daily_returns": [],
        }

    connection_ids = list({r["connection_id"] for r in alloc_rows})

    # ── 1D: intraday from exchange_snapshots (per-bucket sum across
    # all of the user's active connections) ─────────────────────────────
    if range_up == "1D":
        cur.execute("""
            SELECT snapshot_at, connection_id, total_equity_usd
            FROM user_mgmt.exchange_snapshots
            WHERE connection_id = ANY(%s::uuid[])
              AND snapshot_at >= NOW() - INTERVAL '24 hours'
              AND fetch_ok = TRUE
              AND total_equity_usd IS NOT NULL
            ORDER BY snapshot_at
        """, (connection_ids,))
        # Bucket to 5-min and sum across connections per bucket. A
        # connection may miss a bucket (fetch failure, etc.) — in that
        # case we reuse that connection's last-known equity so the sum
        # stays comparable across buckets. This is within-bucket
        # forward-fill for missing connection readings, NOT synthetic
        # backfill for empty time windows (which the user explicitly
        # vetoed).
        series: dict[int, dict[str, float]] = {}
        for row in cur.fetchall():
            ts = row["snapshot_at"]
            cid = str(row["connection_id"])
            bucket_ts = int(ts.timestamp() // 300) * 300
            series.setdefault(bucket_ts, {})[cid] = float(row["total_equity_usd"])

        # Carry forward last-known per-connection equity when that conn
        # didn't tick in a given bucket (so sum reflects full AUM).
        bucket_sorted = sorted(series.keys())
        last_known: dict[str, float] = {}
        portfolio_points: list[dict[str, Any]] = []
        for b in bucket_sorted:
            for cid, eq in series[b].items():
                last_known[cid] = eq
            # Only emit bucket if we have at least one connection's value
            # — prevents phantom leading points before any snapshots exist.
            if last_known:
                portfolio_points.append({
                    "date": datetime.datetime.fromtimestamp(
                        b, tz=datetime.timezone.utc,
                    ).isoformat(),
                    "equity_usd": round(sum(last_known.values()), 2),
                })

        first_date = portfolio_points[0]["date"][:10] if portfolio_points else None
        return {
            "range": "1D",
            "granularity": "intraday",
            "first_data_date": first_date,
            "real_days": 1 if portfolio_points else 0,
            "portfolio_equity": portfolio_points,
            # Daily returns not meaningful intraday — frontend hides/disables.
            "daily_returns": [],
        }

    # ── 1W / 1M / ALL: bucketed wallet equity from exchange_snapshots ──────
    # Bucket sizes match the allocator's account-balance-series endpoint so
    # the Manager Overview charts and the Allocator Overview chart use the
    # same intervals (30-min on 1W, 3-hour on 1M, 1-day on ALL). Per-bucket
    # equity = SUM(DISTINCT ON (connection, bucket) latest snapshot).
    range_specs = {
        "1W":  {"bucket_seconds": 1800,   "lookback_interval": "7 days"},
        "1M":  {"bucket_seconds": 10800,  "lookback_interval": "30 days"},
        "ALL": {"bucket_seconds": 86400,  "lookback_interval": None},
    }
    spec = range_specs[range_up]
    bucket_s = spec["bucket_seconds"]
    lookback = spec["lookback_interval"]

    base_select = """
        SELECT s.connection_id,
               s.snapshot_at,
               to_timestamp(
                 floor(extract(epoch from s.snapshot_at) / %(bucket)s) * %(bucket)s
               ) AS bucket,
               s.total_equity_usd AS equity
        FROM user_mgmt.exchange_snapshots s
        WHERE s.connection_id = ANY(%(cids)s::uuid[])
          AND s.fetch_ok = TRUE
          AND s.total_equity_usd IS NOT NULL
    """
    if lookback:
        base_select += " AND s.snapshot_at >= NOW() - %(lookback)s::interval"

    query = f"""
        WITH bucketed AS ({base_select}),
        latest_per_conn AS (
          SELECT DISTINCT ON (connection_id, bucket)
                 connection_id, bucket, equity
          FROM bucketed
          ORDER BY connection_id, bucket, snapshot_at DESC
        )
        SELECT bucket AS ts, SUM(equity) AS equity_usd
        FROM latest_per_conn
        GROUP BY bucket
        ORDER BY bucket
    """
    params: dict[str, Any] = {"cids": connection_ids, "bucket": bucket_s}
    if lookback:
        params["lookback"] = lookback
    cur.execute(query, params)
    rows = cur.fetchall()
    portfolio_points = [
        {"date": r["ts"].isoformat(), "equity_usd": float(r["equity_usd"] or 0)}
        for r in rows
    ]

    # real_days = distinct UTC calendar days represented in the bucket series.
    distinct_days = sorted({p["date"][:10] for p in portfolio_points})
    first_date = distinct_days[0] if distinct_days else None

    # daily_returns: one per closed UTC session (last bucket of each day
    # minus last bucket of prior day). Frontend on 1W/1M may also compute
    # per-bucket deltas from portfolio_equity for a finer-grained P&L view.
    last_eq_by_day: dict[str, float] = {}
    for p in portfolio_points:
        day = p["date"][:10]
        last_eq_by_day[day] = p["equity_usd"]  # later rows overwrite — last of day wins
    days_sorted = sorted(last_eq_by_day.keys())
    daily_returns = []
    # NOTE: can't use range() here — the function's `range` parameter
    # shadows the built-in. enumerate() sidesteps the collision.
    for i, day in enumerate(days_sorted):
        if i == 0:
            continue
        prev_eq = last_eq_by_day[days_sorted[i - 1]]
        curr_eq = last_eq_by_day[day]
        ret_usd = curr_eq - prev_eq
        ret_pct = (ret_usd / prev_eq * 100.0) if prev_eq > 0 else 0.0
        daily_returns.append({
            "date": day,
            "return_usd": round(ret_usd, 2),
            "return_pct": round(ret_pct, 4),
        })

    return {
        "range": range_up,
        "granularity": "intraday" if range_up in ("1W", "1M") else "daily",
        "first_data_date": first_date,
        "real_days": len(distinct_days),
        "portfolio_equity": portfolio_points,
        "daily_returns": daily_returns,
    }


@router.get("/conversations")
def list_conversations(cur=Depends(get_cursor)) -> dict[str, Any]:
    cur.execute("""
        SELECT conversation_id, title, created_at, updated_at
        FROM user_mgmt.manager_conversations
        ORDER BY updated_at DESC
    """)
    rows = cur.fetchall()
    return {
        "conversations": [
            {
                "conversation_id": str(r["conversation_id"]),
                "title": r["title"],
                "created_at": _iso(r["created_at"]),
                "updated_at": _iso(r["updated_at"]),
            }
            for r in rows
        ]
    }


@router.post("/conversations")
def create_conversation(cur=Depends(get_cursor)) -> dict[str, Any]:
    cur.execute("""
        INSERT INTO user_mgmt.manager_conversations DEFAULT VALUES
        RETURNING conversation_id, title, created_at, updated_at
    """)
    r = cur.fetchone()
    return {
        "conversation_id": str(r["conversation_id"]),
        "title": r["title"],
        "created_at": _iso(r["created_at"]),
        "updated_at": _iso(r["updated_at"]),
        "messages": [],
    }


@router.get("/conversations/{conversation_id}")
def get_conversation(conversation_id: str, cur=Depends(get_cursor)) -> dict[str, Any]:
    cur.execute("""
        SELECT conversation_id, title, created_at, updated_at
        FROM user_mgmt.manager_conversations
        WHERE conversation_id = %s
    """, (conversation_id,))
    conv = cur.fetchone()
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    cur.execute("""
        SELECT message_id, role, content, created_at
        FROM user_mgmt.manager_messages
        WHERE conversation_id = %s
        ORDER BY created_at ASC
    """, (conversation_id,))
    messages = cur.fetchall()

    return {
        "conversation_id": str(conv["conversation_id"]),
        "title": conv["title"],
        "created_at": _iso(conv["created_at"]),
        "updated_at": _iso(conv["updated_at"]),
        "messages": [
            {
                "message_id": str(m["message_id"]),
                "role": m["role"],
                "content": m["content"],
                "created_at": _iso(m["created_at"]),
            }
            for m in messages
        ],
    }


@router.delete("/conversations/{conversation_id}")
def delete_conversation(conversation_id: str, cur=Depends(get_cursor)) -> dict[str, Any]:
    cur.execute("""
        DELETE FROM user_mgmt.manager_conversations
        WHERE conversation_id = %s
        RETURNING conversation_id
    """, (conversation_id,))
    deleted = cur.fetchone()
    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"ok": True, "deleted": str(deleted["conversation_id"])}


@router.post("/conversations/{conversation_id}/messages")
async def send_message(
    conversation_id: str,
    body: MessageRequest,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    # Verify conversation exists
    cur.execute(
        "SELECT conversation_id FROM user_mgmt.manager_conversations WHERE conversation_id = %s",
        (conversation_id,),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Save user message
    cur.execute("""
        INSERT INTO user_mgmt.manager_messages (conversation_id, role, content)
        VALUES (%s, 'user', %s)
        RETURNING message_id, created_at
    """, (conversation_id, body.content))
    user_msg = cur.fetchone()

    # Check if this is the first message (for auto-title)
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM user_mgmt.manager_messages WHERE conversation_id = %s",
        (conversation_id,),
    )
    msg_count = cur.fetchone()["cnt"]

    # Refresh live exchange data before building context
    refresh_snapshots(cur)

    # Fetch portfolio context
    ctx = _fetch_portfolio_context(cur)
    today_str = datetime.date.today().isoformat()
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        portfolio_context=_format_portfolio_context(ctx),
        today=today_str,
        read_only="true",
    )

    # Load full conversation history
    cur.execute("""
        SELECT role, content FROM user_mgmt.manager_messages
        WHERE conversation_id = %s
        ORDER BY created_at ASC
    """, (conversation_id,))
    history = [{"role": r["role"], "content": r["content"]} for r in cur.fetchall()]

    # Call Claude
    try:
        assistant_content = await _call_claude(history, system_prompt)
    except HTTPException:
        raise
    except Exception as e:
        assistant_content = f"[Error: Could not reach Claude — {e}]"

    # Save assistant message
    cur.execute("""
        INSERT INTO user_mgmt.manager_messages (conversation_id, role, content)
        VALUES (%s, 'assistant', %s)
        RETURNING message_id, created_at
    """, (conversation_id, assistant_content))
    asst_msg = cur.fetchone()

    # Update conversation updated_at
    cur.execute(
        "UPDATE user_mgmt.manager_conversations SET updated_at = NOW() WHERE conversation_id = %s",
        (conversation_id,),
    )

    # Auto-title: first message sets title
    if msg_count == 1:
        words = body.content.split()[:6]
        title = f"{today_str} · {' '.join(words)}"
        if len(body.content.split()) > 6:
            title += "..."
        cur.execute(
            "UPDATE user_mgmt.manager_conversations SET title = %s WHERE conversation_id = %s",
            (title, conversation_id),
        )

    return {
        "content": assistant_content,
        "message_id": str(asst_msg["message_id"]),
        "created_at": _iso(asst_msg["created_at"]),
    }


@router.post("/briefing")
async def briefing(cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Auto-create a conversation with a daily portfolio briefing.
    Accepts X-Internal-Token instead of session cookie.
    """
    today_str = datetime.date.today().isoformat()
    title = f"Portfolio briefing · {today_str}"

    # Create conversation
    cur.execute("""
        INSERT INTO user_mgmt.manager_conversations (title)
        VALUES (%s)
        RETURNING conversation_id
    """, (title,))
    conv = cur.fetchone()
    conversation_id = str(conv["conversation_id"])

    # Synthetic user message
    user_content = f"Give me my daily portfolio briefing for {today_str}"
    cur.execute("""
        INSERT INTO user_mgmt.manager_messages (conversation_id, role, content)
        VALUES (%s, 'user', %s)
    """, (conversation_id, user_content))

    # Refresh live exchange data before building context
    refresh_snapshots(cur)

    # Fetch context and call Claude
    ctx = _fetch_portfolio_context(cur)
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        portfolio_context=_format_portfolio_context(ctx),
        today=today_str,
        read_only="true",
    )

    try:
        assistant_content = await _call_claude(
            [{"role": "user", "content": user_content}],
            system_prompt,
        )
    except Exception as e:
        assistant_content = f"[Error: Could not reach Claude — {e}]"

    # Save assistant response
    cur.execute("""
        INSERT INTO user_mgmt.manager_messages (conversation_id, role, content)
        VALUES (%s, 'assistant', %s)
    """, (conversation_id, assistant_content))

    cur.execute(
        "UPDATE user_mgmt.manager_conversations SET updated_at = NOW() WHERE conversation_id = %s",
        (conversation_id,),
    )

    return {"conversation_id": conversation_id}


# ── Execution reports ────────────────────────────────────────────────────────

def _resolve_reports_dir() -> Path:
    """
    Resolve the BloFin execution reports directory.
    Override via env BLOFIN_REPORTS_DIR; otherwise use <project_root>/blofin_execution_reports
    where project_root is the parent of the backend/ directory (the same layout
    the trader writes to when invoked from the repo root).
    """
    override = os.environ.get("BLOFIN_REPORTS_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[4] / "blofin_execution_reports"


@router.get("/execution-reports")
def list_execution_reports() -> dict[str, Any]:
    """
    Return all BloFin execution reports sorted by date descending.
    Each report is the raw JSON written by trader-blofin.py.
    Malformed files are skipped (logged-silently) rather than failing the endpoint.
    """
    reports_dir = _resolve_reports_dir()
    if not reports_dir.exists():
        return {"reports": [], "source_dir": str(reports_dir)}

    reports: list[dict[str, Any]] = []
    for p in sorted(reports_dir.glob("*.json"), reverse=True):
        try:
            with open(p) as f:
                reports.append(json.load(f))
        except Exception:
            continue
    return {"reports": reports, "source_dir": str(reports_dir)}


# ── Multi-tenant execution summary (allocation_returns + portfolio_sessions) ──

@router.get("/execution-summary")
def execution_summary(
    allocation_ids: str | None = None,
    range: str | None = None,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Per-(allocation, day) execution summary for the Manager Execution tab.

    Reads allocation_returns LEFT JOIN portfolio_sessions (on allocation_id +
    session_date = signal_date) LEFT JOIN allocations (for exchange / strategy
    labels). Unifies what Session D shipped into `allocation_returns` with the
    session-level telemetry in `portfolio_sessions`.

    Several execution-quality fields (fill_rate, entry/exit_slip_bps, retries,
    signal_count, alerts) are not yet persisted for allocations — those are
    returned as null today. A Session E+ writer extension will populate them;
    response shape is stable across that change.

    Query params:
      allocation_ids: comma-separated UUIDs. Default = all active allocations.
      range:         "1W" | "1M" | "ALL". Default = ALL.

    Response:
      {
        "available_allocations": [{allocation_id, exchange, strategy_label, capital_usd}],
        "kpis": {avg_fill_rate, avg_entry_slip_bps, avg_exit_slip_bps,
                 avg_pnl_gap, retries_needed, sessions_traded, sessions_total},
        "daily": [{date, allocation_id, exchange, strategy_label, capital_usd,
                   signal_count, conviction{passed,return_pct}, filled, retried,
                   fill_rate, entry_slip_bps, exit_slip_bps,
                   est_return_pct, actual_return_pct, pnl_gap_pct_from_gross,
                   leverage_applied, bars_count, sym_stops_count,
                   peak_portfolio_return, max_dd_from_peak, exit_reason, alerts}, ...]
      }

    KPI aggregation: capital-weighted average for the *_slip / fill_rate / pnl_gap
    fields (weight = capital_deployed_usd). Count fields are plain sums.
    """
    # ── 1. Resolve the active-allocation universe (for filter + default set) ──
    cur.execute("""
        SELECT
            a.allocation_id, a.capital_usd,
            s.display_name AS strategy_display_name, s.name AS strategy_name,
            ec.exchange
        FROM user_mgmt.allocations a
        JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
        JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
        JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
        WHERE a.status = 'active'
        ORDER BY a.created_at
    """)
    active_rows = cur.fetchall()

    available_allocations = [
        {
            "allocation_id": str(r["allocation_id"]),
            "exchange": r["exchange"],
            "strategy_label": r["strategy_display_name"] or r["strategy_name"],
            "capital_usd": _dec(r["capital_usd"]),
        }
        for r in active_rows
    ]
    all_active_ids = [r["allocation_id"] for r in active_rows]

    # ── 2. Filter to user-selected allocation_ids if provided ─────────────────
    if allocation_ids:
        requested = {u.strip() for u in allocation_ids.split(",") if u.strip()}
        scope_ids = [aid for aid in all_active_ids if str(aid) in requested]
    else:
        scope_ids = list(all_active_ids)

    # ── 3. Date range ─────────────────────────────────────────────────────────
    range_days = {"1W": 7, "1M": 30}.get((range or "").upper())
    since_date = None
    if range_days is not None:
        since_date = datetime.date.today() - datetime.timedelta(days=range_days - 1)

    # ── 4. Pull daily rows ────────────────────────────────────────────────────
    # Even when scope_ids is empty (user filter matched no active allocations)
    # we still return the shape; the frontend renders the empty state.
    #
    # Principal anchor (migration 007): sessions before an allocation's
    # principal_anchor_at are "pre-history" and excluded. Falls back to
    # allocation.created_at when anchor is NULL (no effective filter). The
    # per-row filter in the SQL below (ar.session_date >= anchor::date)
    # handles the fallback uniformly via COALESCE.
    daily: list[dict[str, Any]] = []
    if scope_ids:
        params: list[Any] = [scope_ids]
        date_clause = ""
        if since_date is not None:
            date_clause = " AND ar.session_date >= %s"
            params.append(since_date)

        # Capital-events adjustment (compute-on-read, replaces the old
        # 23:56 reconcile cron). For each (allocation_id, session_date) we
        # net the deposits/withdrawals on the same UTC date, reconstruct
        # end_equity from the trader-written net, and re-denominator on the
        # capital-adjusted baseline:
        #   end_equity_X    = capital_deployed * (1 + stored_X_pct/100)
        #   adjusted_X_pct  = (end_equity_X - capital_deployed - ce_net)
        #                     / (capital_deployed + ce_net) * 100
        # Skipped (returns stored value) when capital_deployed = 0
        # (filtered/no_entry rows where dividing makes no sense) or
        # capital_deployed + ce_net <= 0 (would invert sign of denominator).
        cur.execute(f"""
            WITH ce AS (
                SELECT allocation_id,
                       event_at::date AS session_date,
                       SUM(CASE kind WHEN 'deposit'    THEN amount_usd
                                     WHEN 'withdrawal' THEN -amount_usd
                           END)::numeric AS session_net
                  FROM user_mgmt.allocation_capital_events
                 WHERE deleted_at IS NULL
                 GROUP BY allocation_id, event_at::date
            )
            SELECT
                ar.allocation_id,
                ar.session_date,
                CASE
                    WHEN ar.capital_deployed_usd > 0
                     AND COALESCE(ce.session_net, 0) <> 0
                     AND (ar.capital_deployed_usd + ce.session_net) > 0
                    THEN (ar.capital_deployed_usd * (1 + ar.net_return_pct/100)
                          - ar.capital_deployed_usd - ce.session_net)
                         / (ar.capital_deployed_usd + ce.session_net) * 100
                    ELSE ar.net_return_pct
                END AS net_return_pct,
                CASE
                    WHEN ar.capital_deployed_usd > 0
                     AND COALESCE(ce.session_net, 0) <> 0
                     AND (ar.capital_deployed_usd + ce.session_net) > 0
                    THEN (ar.capital_deployed_usd * (1 + ar.gross_return_pct/100)
                          - ar.capital_deployed_usd - ce.session_net)
                         / (ar.capital_deployed_usd + ce.session_net) * 100
                    ELSE ar.gross_return_pct
                END AS gross_return_pct,
                ar.exit_reason       AS ar_exit_reason,
                ar.effective_leverage,
                ar.capital_deployed_usd,
                ar.fill_rate         AS ar_fill_rate,
                ar.avg_entry_slip_bps,
                ar.avg_exit_slip_bps,
                ar.retries_used,
                ar.signal_count      AS ar_signal_count,
                ar.conviction_roi_x,
                ar.alerts_fired,
                ps.symbols,
                ps.entered,
                ps.bars_count,
                ps.final_portfolio_return,
                ps.peak_portfolio_return,
                ps.max_dd_from_peak,
                ps.sym_stops,
                ps.lev_int,
                ec.exchange,
                s.display_name       AS strategy_display_name,
                s.name               AS strategy_name,
                a.capital_usd
            FROM user_mgmt.allocation_returns ar
            LEFT JOIN ce
                   ON ce.allocation_id = ar.allocation_id
                  AND ce.session_date  = ar.session_date
            LEFT JOIN user_mgmt.portfolio_sessions ps
                   ON ps.allocation_id = ar.allocation_id
                  AND ps.signal_date   = ar.session_date
            JOIN user_mgmt.allocations a ON a.allocation_id = ar.allocation_id
            JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
            JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
            JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
            WHERE ar.allocation_id = ANY(%s::uuid[])
                  AND ar.session_date >=
                      (COALESCE(ec.principal_anchor_at, a.created_at))::date
                  {date_clause}
            ORDER BY ar.session_date DESC, ar.allocation_id ASC
        """, tuple(params))

        for r in cur.fetchall():
            er = r["ar_exit_reason"]
            entered_arr = r["entered"] or []
            symbols_arr = r["symbols"] or []
            filled = len(entered_arr) > 0

            # When portfolio_sessions shows a real session (entered positions
            # present) but allocation_returns carries an early 'filtered'/
            # 'no_entry_conviction' label, the `ar` row is stale — written by
            # the first-subprocess crash handler before the recovered trader
            # took over. Trust portfolio_sessions in that case: the session
            # actually traded. Common on sessions that were respawned mid-
            # morning (Gap 6 case — 2026-04-23 spawn recovery).
            ar_is_stale_filtered = (
                er in ("filtered", "no_entry_conviction")
                and len(entered_arr) > 0
            )
            if ar_is_stale_filtered:
                er_effective = None  # mid-session / recovered, not yet closed
                conviction_passed = True
            else:
                er_effective = er
                conviction_passed = er not in (
                    "filtered", "no_entry_conviction", "missed_window",
                    "stale_close_failed", "no_entry", "errored", "entry_failed",
                )

            # Leveraged estimated return proxy: final_portfolio_return is 1x,
            # so scale by lev_int × 100 to get pre-fee leveraged %.
            fpr = _dec(r["final_portfolio_return"])
            lev_int = int(r["lev_int"]) if r["lev_int"] is not None else None
            est_return_pct = (
                fpr * lev_int * 100 if fpr is not None and lev_int is not None else None
            )

            # For a stale-filtered row the ar net/gross are zero (wrote-before-
            # trading snapshot) — don't surface them as "actual return 0%".
            # The true actual_return is only known after session close.
            if ar_is_stale_filtered:
                net_ret = None
                gross_ret = None
            else:
                net_ret = _dec(r["net_return_pct"])
                gross_ret = _dec(r["gross_return_pct"])
            pnl_gap_pct_from_gross = (
                gross_ret - net_ret
                if gross_ret is not None and net_ret is not None
                else None
            )

            sym_stops_arr = r["sym_stops"] or []

            # signal_count: prefer the writer-persisted value EXCEPT when the
            # ar row is stale-filtered — in that case ar.signal_count is 0 but
            # portfolio_sessions.symbols has the real count.
            ar_sig = r["ar_signal_count"]
            if ar_is_stale_filtered and symbols_arr:
                signal_count = len(symbols_arr)
            else:
                signal_count = (
                    int(ar_sig) if ar_sig is not None
                    else (len(symbols_arr) if symbols_arr else None)
                )
            # conviction_roi_x is now persisted; use it when available.
            roi_x = _dec(r["conviction_roi_x"])
            alerts_list = r["alerts_fired"] or []

            daily.append({
                "date": r["session_date"].isoformat(),
                "allocation_id": str(r["allocation_id"]),
                "exchange": r["exchange"],
                "strategy_label": r["strategy_display_name"] or r["strategy_name"],
                "capital_usd": _dec(r["capital_usd"]),
                "signal_count": signal_count,
                "conviction": {
                    "passed": conviction_passed,
                    "return_pct": roi_x,
                },
                "filled": filled,
                # Persisted by the writer extension (commit adding fill_rate
                # et al. to allocation_returns). Null for pre-writer rows.
                "retried":          int(r["retries_used"]) if r["retries_used"] is not None else None,
                "fill_rate":        _dec(r["ar_fill_rate"]),
                "entry_slip_bps":   _dec(r["avg_entry_slip_bps"]),
                "exit_slip_bps":    _dec(r["avg_exit_slip_bps"]),
                "alerts":           len(alerts_list) if alerts_list else None,
                # Sourced from portfolio_sessions / ar.*
                "est_return_pct":             est_return_pct,
                "actual_return_pct":          net_ret,
                "pnl_gap_pct_from_gross":     pnl_gap_pct_from_gross,
                "leverage_applied":           _dec(r["effective_leverage"]),
                "bars_count":                 r["bars_count"],
                "sym_stops_count":            len(sym_stops_arr),
                "peak_portfolio_return":      _dec(r["peak_portfolio_return"]),
                "max_dd_from_peak":           _dec(r["max_dd_from_peak"]),
                "capital_deployed_usd":       _dec(r["capital_deployed_usd"]),
                "exit_reason":                er_effective,
            })

    # ── 5. KPIs — capital-weighted where averaged, plain sum/count where counted ─
    # Weighting rationale: averaging fill_rate / slippage across allocations of
    # wildly different capital gives a small allocation equal voice to a large
    # one, which isn't what a portfolio manager wants. capital_deployed_usd is
    # the natural weight — small-allocation noise shouldn't dominate.
    def _weighted_avg(field: str) -> float | None:
        num = 0.0
        den = 0.0
        for d in daily:
            v = d.get(field)
            w = d.get("capital_deployed_usd") or 0.0
            if v is not None and w > 0:
                num += float(v) * float(w)
                den += float(w)
        return (num / den) if den > 0 else None

    retries_needed = sum(d["retried"] or 0 for d in daily)
    sessions_traded = sum(1 for d in daily if d["filled"])
    sessions_total = len(daily)

    kpis = {
        "avg_fill_rate":      _weighted_avg("fill_rate"),
        "avg_entry_slip_bps": _weighted_avg("entry_slip_bps"),
        "avg_exit_slip_bps":  _weighted_avg("exit_slip_bps"),
        "avg_pnl_gap":        _weighted_avg("pnl_gap_pct_from_gross"),
        "retries_needed":     retries_needed,
        "sessions_traded":    sessions_traded,
        "sessions_total":     sessions_total,
    }

    return {
        "available_allocations": available_allocations,
        "kpis":                  kpis,
        "daily":                 daily,
    }


@router.get("/execution-summary/{session_date}/positions")
def execution_summary_positions(
    session_date: str,
    allocation_id: str,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """Per-symbol execution breakdown for a single (allocation × session_date).

    Backs the Manager Execution per-row expand. Pulls from
    user_mgmt.allocation_execution_symbols, which the trader writes at
    session close (see _log_allocation_execution_symbols in trader_blofin.py).

    Pre-trader-extension sessions have no rows — response returns an empty
    list and the frontend renders a "no per-symbol data" notice.
    """
    cur.execute(
        """
        SELECT inst_id, side,
               target_contracts, filled_contracts, fill_pct,
               est_entry_price, fill_entry_price, entry_slippage_bps,
               est_exit_price, fill_exit_price, exit_slippage_bps,
               pnl_usd, pnl_pct, exit_reason, retry_rounds, sym_stopped
          FROM user_mgmt.allocation_execution_symbols
         WHERE allocation_id = %s::uuid
           AND session_date  = %s::date
         ORDER BY sym_stopped DESC, inst_id ASC
        """,
        (allocation_id, session_date),
    )
    rows = [
        {
            "inst_id":            r["inst_id"],
            "side":               r["side"],
            "target_contracts":   _dec(r["target_contracts"]),
            "filled_contracts":   _dec(r["filled_contracts"]),
            "fill_pct":           _dec(r["fill_pct"]),
            "est_entry_price":    _dec(r["est_entry_price"]),
            "fill_entry_price":   _dec(r["fill_entry_price"]),
            "entry_slippage_bps": _dec(r["entry_slippage_bps"]),
            "est_exit_price":     _dec(r["est_exit_price"]),
            "fill_exit_price":    _dec(r["fill_exit_price"]),
            "exit_slippage_bps":  _dec(r["exit_slippage_bps"]),
            "pnl_usd":            _dec(r["pnl_usd"]),
            "pnl_pct":            _dec(r["pnl_pct"]),
            "exit_reason":        r["exit_reason"],
            "retry_rounds":       r["retry_rounds"],
            "sym_stopped":        r["sym_stopped"],
        }
        for r in cur.fetchall()
    ]
    return {
        "allocation_id": allocation_id,
        "session_date":  session_date,
        "positions":     rows,
    }


# ── Portfolio time series (SQL-backed) ──────────────────────────────────────
# The Manager UI reads exclusively from user_mgmt.portfolio_sessions +
# user_mgmt.portfolio_bars, which the trader writes live every 5 minutes.
# The trader also writes a parallel NDJSON backup at
# blofin_execution_reports/portfolios/YYYY-MM-DD.ndjson for recovery; those
# files are NOT read by this API.

def _fmt_utc(dt: Any) -> str | None:
    """Format a TIMESTAMPTZ or None as 'YYYY-MM-DD HH:MM:SS' in UTC."""
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime.datetime):
        if dt.tzinfo is None:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


@router.get("/portfolios")
def list_portfolios(
    allocation_ids: str | None = None,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Return one summary row per portfolio session, sorted by date descending.

    Multi-tenant aware: projects allocation_id + exchange + strategy_label
    so the frontend can render a per-allocation filter and disambiguate
    multi-allocation days. Also returns `available_allocations[]` (all active
    allocations) for the filter dropdown.

    Rows with allocation_id IS NULL (host-master cron entries) are filtered
    out — master portfolio history lives in NDJSON files at
    /root/benji/blofin_execution_reports/portfolios/*.ndjson and is not
    currently merged into this response. See Session F+ follow-up in
    docs/open_work_list.md for the NDJSON-overlay parallel to Execution tab's
    "Include master history" toggle.

    Query params:
      allocation_ids: comma-separated UUIDs. Default = all active allocations.
    """
    # Resolve active-allocation universe (powers filter dropdown + default scope).
    cur.execute("""
        SELECT
            a.allocation_id, a.capital_usd,
            s.display_name AS strategy_display_name, s.name AS strategy_name,
            ec.exchange
        FROM user_mgmt.allocations a
        JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
        JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
        JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
        WHERE a.status = 'active'
        ORDER BY a.created_at
    """)
    active_rows = cur.fetchall()

    available_allocations = [
        {
            "allocation_id": str(r["allocation_id"]),
            "exchange": r["exchange"],
            "strategy_label": r["strategy_display_name"] or r["strategy_name"],
            "capital_usd": _dec(r["capital_usd"]),
        }
        for r in active_rows
    ]
    all_active_ids = [r["allocation_id"] for r in active_rows]

    if allocation_ids:
        requested = {u.strip() for u in allocation_ids.split(",") if u.strip()}
        scope_ids = [aid for aid in all_active_ids if str(aid) in requested]
    else:
        scope_ids = list(all_active_ids)

    portfolios: list[dict[str, Any]] = []
    if scope_ids:
        cur.execute("""
            SELECT ps.signal_date,
                   ps.status,
                   ps.session_start_utc,
                   ps.exit_time_utc,
                   ps.exit_reason,
                   ps.eff_lev::double precision                         AS eff_lev,
                   ps.lev_int,
                   ps.symbols,
                   ps.entered,
                   ps.bars_count,
                   COALESCE(ps.final_portfolio_return, 0)::double precision AS final_incr,
                   COALESCE(ps.peak_portfolio_return,  0)::double precision AS peak,
                   COALESCE(ps.max_dd_from_peak,       0)::double precision AS max_dd_from_peak,
                   ps.sym_stops,
                   ps.allocation_id,
                   ec.exchange,
                   s.display_name AS strategy_display_name,
                   s.name         AS strategy_name
            FROM user_mgmt.portfolio_sessions ps
            JOIN user_mgmt.allocations a ON a.allocation_id = ps.allocation_id
            JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
            JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
            JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
            WHERE ps.allocation_id = ANY(%s::uuid[])
            ORDER BY ps.signal_date DESC, ps.allocation_id ASC
        """, (scope_ids,))
        for r in cur.fetchall():
            portfolios.append({
                "date":              r["signal_date"].isoformat(),
                "allocation_id":     str(r["allocation_id"]),
                "exchange":          r["exchange"],
                "strategy_label":    r["strategy_display_name"] or r["strategy_name"],
                "status":            r["status"],
                "session_start_utc": _fmt_utc(r["session_start_utc"]),
                "exit_time_utc":     _fmt_utc(r["exit_time_utc"]),
                "exit_reason":       r["exit_reason"],
                "eff_lev":           r["eff_lev"],
                "lev_int":           r["lev_int"],
                "symbols":           list(r["symbols"] or []),
                "entered":           list(r["entered"] or []),
                "bars_count":        r["bars_count"],
                "final_incr":        r["final_incr"],
                "peak":              r["peak"],
                "max_dd_from_peak":  r["max_dd_from_peak"],
                "sym_stops":         sorted(list(r["sym_stops"] or [])),
            })

    return {
        "available_allocations": available_allocations,
        "portfolios":            portfolios,
    }


@router.get("/portfolios/{date}")
def get_portfolio(
    date: str,
    allocation_id: str | None = None,
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Return {meta, bars[]} for one session, sorted by bar_number.
    Safe to poll while meta.status == "active"; the trader upserts bars every
    5 minutes.

    HIGH-severity multi-tenant fix: pre-refactor, this endpoint fetchone()'d by
    signal_date only, silently dropping all but one row on multi-allocation
    days. Now requires `allocation_id` query param when multiple rows share
    the date (returns 400 with the available options so the frontend can
    redirect to the correct URL).
    """
    # Basic guard against path-ish inputs — only YYYY-MM-DD shape allowed.
    if len(date) != 10 or date[4] != "-" or date[7] != "-":
        raise HTTPException(status_code=400, detail="invalid date")

    # Enumerate all sessions on this date (allocation-aware). We surface the
    # list back to the caller on ambiguity so they can pick one.
    cur.execute("""
        SELECT ps.portfolio_session_id, ps.signal_date, ps.status,
               ps.session_start_utc, ps.exit_time_utc, ps.exit_reason,
               ps.symbols, ps.entered,
               ps.eff_lev::double precision AS eff_lev,
               ps.lev_int,
               ps.allocation_id,
               ec.exchange,
               s.display_name AS strategy_display_name,
               s.name         AS strategy_name
        FROM user_mgmt.portfolio_sessions ps
        LEFT JOIN user_mgmt.allocations a ON a.allocation_id = ps.allocation_id
        LEFT JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
        LEFT JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
        LEFT JOIN user_mgmt.exchange_connections ec ON ec.connection_id = a.connection_id
        WHERE ps.signal_date = %s
        ORDER BY ps.allocation_id ASC
    """, (date,))
    candidates = cur.fetchall()

    if not candidates:
        raise HTTPException(status_code=404, detail="portfolio not found")

    if allocation_id is not None:
        match = next(
            (r for r in candidates if str(r["allocation_id"]) == allocation_id),
            None,
        )
        if match is None:
            raise HTTPException(
                status_code=404,
                detail=f"No portfolio session for allocation_id={allocation_id} on {date}",
            )
        session = match
    elif len(candidates) == 1:
        # Backward compat: exactly one session on this date, no ambiguity.
        session = candidates[0]
    else:
        # Multi-allocation day — caller must specify which one.
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Multiple allocations have sessions on this date; specify allocation_id query param",
                "available": [
                    {
                        "allocation_id": str(r["allocation_id"]) if r["allocation_id"] else None,
                        "exchange": r["exchange"],
                        "strategy_label": r["strategy_display_name"] or r["strategy_name"],
                    }
                    for r in candidates
                ],
            },
        )

    cur.execute("""
        SELECT bar_number,
               bar_timestamp_utc,
               portfolio_return::double precision AS incr,
               peak_return::double precision      AS peak,
               symbol_returns,
               stopped
        FROM user_mgmt.portfolio_bars
        WHERE portfolio_session_id = %s
        ORDER BY bar_number
    """, (session["portfolio_session_id"],))
    bar_rows = cur.fetchall()

    meta = {
        "date":              session["signal_date"].isoformat(),
        "allocation_id":     str(session["allocation_id"]) if session["allocation_id"] else None,
        "exchange":          session["exchange"],
        "strategy_label":    session["strategy_display_name"] or session["strategy_name"],
        "status":            session["status"],
        "session_start_utc": _fmt_utc(session["session_start_utc"]),
        "exit_time_utc":     _fmt_utc(session["exit_time_utc"]),
        "exit_reason":       session["exit_reason"],
        "symbols":           list(session["symbols"] or []),
        "entered":           list(session["entered"] or []),
        "eff_lev":           session["eff_lev"],
        "lev_int":           session["lev_int"],
    }
    bars = [{
        "bar":         b["bar_number"],
        "ts":          _fmt_utc(b["bar_timestamp_utc"]),
        "incr":        b["incr"],
        "peak":        b["peak"],
        "sym_returns": b["symbol_returns"] or {},
        "stopped":     list(b["stopped"] or []),
    } for b in bar_rows]

    return {"meta": meta, "bars": bars}


# ── Trader log stream (session-windowed, paginated) ──────────────────────────
# Reads the trader's append-only log file (blofin_executor.log), segments by
# SESSION {date} headers the trader emits at each session start, and returns
# a line-numbered page for the requested session. Designed for live polling:
# the frontend passes the last line number it has and gets only newer lines.
#
# Path is configurable via BLOFIN_LOG_FILE env var. In Docker, the backend
# container needs a bind mount pointing at the host's project dir — see
# docker-compose.yml (`/host_trader:/ro`).

_LOG_LINE_RE = re.compile(
    # "YYYY-MM-DD HH:MM:SS[,ms] [LEVEL] message..."
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:,\d+)?\s+\[(\w+)\]\s+(.*)$"
)
_SESSION_HEADER_RE = re.compile(r"^\s*SESSION\s+(\d{4}-\d{2}-\d{2})")


def _resolve_log_path() -> Path:
    """
    Master log path. BLOFIN_LOG_FILE env override, else project-root-relative
    blofin_executor.log (same project-root resolution as _resolve_reports_dir).
    """
    override = os.environ.get("BLOFIN_LOG_FILE")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[4] / "blofin_executor.log"


_ALLOC_LOG_DIR = Path("/mnt/quant-data/logs/trader")
_ALLOC_LOG_DATE_RE = re.compile(r"^allocation_[0-9a-f-]{36}_(\d{4}-\d{2}-\d{2})\.log$")


def _resolve_allocation_log_path(allocation_id: str, date: str) -> Path:
    """
    Per-allocation log path. Each spawn_traders subprocess redirects its
    stdout/stderr to /mnt/quant-data/logs/trader/allocation_<uuid>_<date>.log
    — see backend/app/cli/spawn_traders.py. Mount at /mnt/quant-data is
    shared with the host (docker-compose.yml).
    """
    return _ALLOC_LOG_DIR / f"allocation_{allocation_id}_{date}.log"


def _list_allocation_log_dates(allocation_id: str, limit: int = 3) -> list[str]:
    """
    Scan /mnt/quant-data/logs/trader/ for allocation_<id>_YYYY-MM-DD.log files
    and return up to `limit` most-recent date strings (descending). Empty list
    if no files exist for this allocation. Used for:
      - auto-resolving "show me the latest session I have" when the UI omits
        the date query param (picks [0] of the returned list)
      - populating the date-picker tabs in SessionLogs.tsx so the user can
        navigate back up to `limit - 1` days
    """
    if not _ALLOC_LOG_DIR.exists():
        return []
    try:
        dates: list[str] = []
        prefix = f"allocation_{allocation_id}_"
        for p in _ALLOC_LOG_DIR.iterdir():
            if not p.is_file() or not p.name.startswith(prefix):
                continue
            m = _ALLOC_LOG_DATE_RE.match(p.name)
            if m:
                dates.append(m.group(1))
        dates.sort(reverse=True)  # ISO dates sort lexicographically
        return dates[:limit]
    except OSError:
        return []


def _parse_log_line(raw: str) -> dict[str, str] | None:
    """Return {ts, level, text} for a well-formed line; None for continuations."""
    m = _LOG_LINE_RE.match(raw)
    if not m:
        return None
    return {"ts": m.group(1), "level": m.group(2), "text": m.group(3)}


def _read_trader_log() -> list[dict[str, str]]:
    """Parse the master log (blofin_executor.log). Delegates to _read_log_file."""
    return _read_log_file(_resolve_log_path())


def _session_window(
    entries: list[dict[str, str]],
    target_date: str | None,
) -> tuple[str | None, list[dict[str, str]]]:
    """
    Return (session_date, entries_in_window) for the requested session.

    Primary strategy: find the SESSION {date} header the trader writes at
    session start. Window is [that header .. next SESSION header or EOF].
    --resume emits a new SESSION header with the same date; we pick the
    latest matching marker so resumed sessions show the most recent window.

    Fallback when no matching header: all entries whose ts starts with the
    target date (UTC trading-day approximation).
    """
    markers: list[tuple[int, str]] = []
    for i, e in enumerate(entries):
        mm = _SESSION_HEADER_RE.match(e["text"])
        if mm:
            markers.append((i, mm.group(1)))

    if not markers:
        if target_date is None:
            return None, []
        return target_date, [e for e in entries if e["ts"].startswith(target_date)]

    if target_date is None:
        start_idx, date = markers[-1]
    else:
        matching = [m for m in markers if m[1] == target_date]
        if not matching:
            return target_date, [e for e in entries if e["ts"].startswith(target_date)]
        start_idx, date = matching[-1]

    next_starts = [m[0] for m in markers if m[0] > start_idx]
    end_idx = next_starts[0] if next_starts else len(entries)
    return date, entries[start_idx:end_idx]


def _read_log_file(path: Path) -> list[dict[str, str]]:
    """Parse one log file, folding continuation lines into the previous entry."""
    if not path.exists():
        return []
    out: list[dict[str, str]] = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            ent = _parse_log_line(raw)
            if ent is not None:
                out.append(ent)
            elif out:
                out[-1]["text"] = out[-1]["text"] + "\n" + raw
    return out


@router.get("/execution-logs")
def execution_logs(
    date: str | None = None,
    allocation_id: str | None = None,
    since_line: int | None = None,
    limit: int = Query(default=500, ge=1, le=2000),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Return trader log lines for one session.

    Two modes:
      (a) Master mode (default, no allocation_id): reads blofin_executor.log —
          a single continuous append-only file spanning multiple sessions.
          Uses SESSION header markers to segment into per-day windows.
      (b) Allocation mode (allocation_id provided): reads the per-allocation
          per-day file at /mnt/quant-data/logs/trader/allocation_<id>_<date>.log.
          One file = one session. When date is omitted, auto-resolves to the
          most recent log file for that allocation (glob on filename dates) —
          supports "show me the latest session I have" when today's hasn't
          opened yet. Returns empty response if no files exist at all.

    date:           YYYY-MM-DD. Omitted in master mode → most recent session.
                    Omitted in allocation mode → most recent existing log file.
    allocation_id:  Optional UUID. When provided, switches to allocation mode.
    since_line:     return only lines with n > since_line (live-polling cursor).
                    Omitted → tail the last `limit` lines.
    limit:          1..2000, default 500.

    Response: {date, total_lines, from_line, lines[], session_active,
               allocation_id}. session_active reads portfolio_sessions; in
    allocation mode the check is scoped to the allocation_id.
    """
    if date is not None:
        if len(date) != 10 or date[4] != "-" or date[7] != "-":
            raise HTTPException(status_code=400, detail="invalid date")

    available_dates: list[str] = []
    if allocation_id is not None:
        # Allocation mode — one file per (allocation, date).
        # Always enumerate available dates (last 3) so the UI can render
        # date-picker tabs. When `date` is omitted, auto-resolve to the
        # most recent — supports "show latest session" UX when the current
        # session hasn't opened yet.
        available_dates = _list_allocation_log_dates(allocation_id, limit=3)
        if not date:
            if not available_dates:
                # No log files exist for this allocation yet — return empty
                # response; frontend shows the "No sessions" placeholder.
                return {
                    "date":            None,
                    "allocation_id":   allocation_id,
                    "available_dates": [],
                    "total_lines":     0,
                    "from_line":       0,
                    "lines":           [],
                    "session_active":  False,
                }
            date = available_dates[0]
        window = _read_log_file(_resolve_allocation_log_path(allocation_id, date))
        session_date = date
    else:
        # Master mode — single continuous file, segment by SESSION header.
        entries = _read_trader_log()
        session_date, window = _session_window(entries, date)

    total = len(window)

    if since_line is None:
        start = max(0, total - limit)
    else:
        start = max(0, since_line + 1)
    end = min(total, start + limit)

    lines = [
        {"n": start + i,
         "ts": e["ts"],
         "level": e["level"],
         "text": e["text"]}
        for i, e in enumerate(window[start:end])
    ]

    session_active = False
    if session_date:
        if allocation_id is not None:
            cur.execute(
                """
                SELECT 1
                FROM user_mgmt.portfolio_sessions
                WHERE signal_date = %s AND status = 'active'
                  AND allocation_id = %s::uuid
                """,
                (session_date, allocation_id),
            )
        else:
            cur.execute(
                """
                SELECT 1
                FROM user_mgmt.portfolio_sessions
                WHERE signal_date = %s AND status = 'active'
                """,
                (session_date,),
            )
        session_active = cur.fetchone() is not None

    return {
        "date":            session_date,
        "allocation_id":   allocation_id,
        "available_dates": available_dates,
        "total_lines":     total,
        "from_line":       start,
        "lines":           lines,
        "session_active":  session_active,
    }

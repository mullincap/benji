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

    # ── Performance daily (last 30 days per allocation) ──────────────────
    perf_by_alloc: dict[str, list[dict]] = {}
    today_return_by_alloc: dict[str, float] = {}
    drawdown_by_alloc: dict[str, float] = {}
    today = datetime.date.today()

    if alloc_list:
        cur.execute("""
            SELECT allocation_id, date, equity_usd, daily_return, drawdown
            FROM user_mgmt.performance_daily
            WHERE allocation_id = ANY(%s::uuid[])
              AND date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY allocation_id, date
        """, (all_alloc_ids,))
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
    if not perf_by_alloc and all_alloc_ids:
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
        """, (all_alloc_ids,))
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

    max_dd = min((a["max_drawdown"] for a in alloc_list), default=0.0)

    # ── Portfolio equity curve (sum of equity_usd per date) ──────────────
    equity_by_date: dict[str, float] = {}
    for a in alloc_list:
        for p in perf_by_alloc.get(a["allocation_id"], []):
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

    # Trader: check most recent deployment for last execution
    cur.execute("""
        SELECT status, created_at, entry_at, exit_at
        FROM user_mgmt.deployments
        ORDER BY created_at DESC LIMIT 1
    """)
    trader_row = cur.fetchone()
    if trader_row:
        trader_status = {
            "status": trader_row["status"],
            "last_run": _iso(trader_row["entry_at"] or trader_row["created_at"]),
        }
    else:
        trader_status = {"status": "no_deployments", "last_run": None}

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

    # ── Intraday equity curve (today, 15-min intervals) ────────────────
    # DISTINCT ON (allocation_id, bucket) + ORDER BY snapshot_at DESC →
    # take the LATEST snapshot per allocation within each 15-min bucket.
    # Then SUM across allocations in Python. This replaces a previous AVG()
    # that incorrectly collapsed multi-allocation equity into a mean
    # (e.g., BloFin $3,950 + Binance $19 → $1,985 instead of $3,969).
    #
    # DISTINCT ON vs MAX: end-of-bucket semantic (what a user expects from
    # an intraday chart) rather than high-water-mark bias inside the bucket.
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
          AND es.snapshot_at >= CURRENT_DATE
        ORDER BY a.allocation_id, bucket, es.snapshot_at DESC
    """, (all_alloc_ids,))

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
        "portfolio_equity_30d": portfolio_equity,
        "intraday_equity": intraday_equity,
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
def list_portfolios(cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Return one summary row per portfolio session, sorted by date descending.
    Cached summary columns on portfolio_sessions (populated by the trader on
    each bar append) mean the list endpoint doesn't need to aggregate bars.
    Response shape matches the legacy NDJSON version so the frontend is
    unchanged.
    """
    cur.execute("""
        SELECT signal_date,
               status,
               session_start_utc,
               exit_time_utc,
               exit_reason,
               eff_lev::double precision                         AS eff_lev,
               lev_int,
               symbols,
               entered,
               bars_count,
               COALESCE(final_portfolio_return, 0)::double precision AS final_incr,
               COALESCE(peak_portfolio_return,  0)::double precision AS peak,
               COALESCE(max_dd_from_peak,       0)::double precision AS max_dd_from_peak,
               sym_stops
        FROM user_mgmt.portfolio_sessions
        ORDER BY signal_date DESC
    """)
    rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "date":              r["signal_date"].isoformat(),
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
    return {"portfolios": out}


@router.get("/portfolios/{date}")
def get_portfolio(date: str, cur=Depends(get_cursor)) -> dict[str, Any]:
    """
    Return {meta, bars[]} for one session, sorted by bar_number.
    Safe to poll while meta.status == "active"; the trader upserts bars every
    5 minutes.
    """
    # Basic guard against path-ish inputs — only YYYY-MM-DD shape allowed.
    if len(date) != 10 or date[4] != "-" or date[7] != "-":
        raise HTTPException(status_code=400, detail="invalid date")

    cur.execute("""
        SELECT portfolio_session_id, signal_date, status,
               session_start_utc, exit_time_utc, exit_reason,
               symbols, entered,
               eff_lev::double precision AS eff_lev,
               lev_int
        FROM user_mgmt.portfolio_sessions
        WHERE signal_date = %s
    """, (date,))
    session = cur.fetchone()
    if session is None:
        raise HTTPException(status_code=404, detail="portfolio not found")

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
    BLOFIN_LOG_FILE env override, else project-root-relative
    blofin_executor.log (same project-root resolution as _resolve_reports_dir).
    """
    override = os.environ.get("BLOFIN_LOG_FILE")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[4] / "blofin_executor.log"


def _parse_log_line(raw: str) -> dict[str, str] | None:
    """Return {ts, level, text} for a well-formed line; None for continuations."""
    m = _LOG_LINE_RE.match(raw)
    if not m:
        return None
    return {"ts": m.group(1), "level": m.group(2), "text": m.group(3)}


def _read_trader_log() -> list[dict[str, str]]:
    """
    Parse the entire log file, folding continuation lines (tracebacks, etc.)
    into the previous entry's text. Returns entries in file order.
    """
    path = _resolve_log_path()
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


@router.get("/execution-logs")
def execution_logs(
    date: str | None = None,
    since_line: int | None = None,
    limit: int = Query(default=500, ge=1, le=2000),
    cur=Depends(get_cursor),
) -> dict[str, Any]:
    """
    Return trader log lines for one session window.

    date:        YYYY-MM-DD; omitted → most recent session.
    since_line:  return only lines with n > since_line (live-polling cursor).
                 Omitted → tail the last `limit` lines.
    limit:       1..2000, default 500.

    Response: {date, total_lines, from_line, lines[], session_active}.
    session_active is true iff user_mgmt.portfolio_sessions shows status
    'active' for the session's date (covers the common "is trader still
    running?" UI-polling question).
    """
    if date is not None:
        if len(date) != 10 or date[4] != "-" or date[7] != "-":
            raise HTTPException(status_code=400, detail="invalid date")

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
        "date":           session_date,
        "total_lines":    total,
        "from_line":      start,
        "lines":          lines,
        "session_active": session_active,
    }

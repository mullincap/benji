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
import json
import datetime
from decimal import Decimal
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
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
    cur.execute("""
        SELECT
            date_trunc('hour', es.snapshot_at) +
              INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM es.snapshot_at) / 15)
              AS bucket,
            AVG(es.total_equity_usd) AS equity_usd
        FROM user_mgmt.exchange_snapshots es
        JOIN user_mgmt.allocations a ON a.connection_id = es.connection_id
        WHERE a.allocation_id = ANY(%s::uuid[])
          AND es.fetch_ok = TRUE
          AND es.snapshot_at >= CURRENT_DATE
        GROUP BY bucket
        ORDER BY bucket
    """, (all_alloc_ids,))
    intraday_rows = cur.fetchall()
    intraday_equity = [
        {"time": r["bucket"].isoformat(), "equity_usd": float(r["equity_usd"])}
        for r in intraday_rows if r["equity_usd"] is not None
    ]

    return {
        "allocations": alloc_list,
        "total_aum": total_aum_f,
        "today_pct": round(today_pct, 2),
        "wtd_pct": round(wtd_pct, 2),
        "mtd_pct": round(mtd_pct, 2),
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

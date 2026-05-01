"""
backend/app/api/routes/manager_live.py
========================================
Read endpoints for the Manager Live tab.

Single venue v1 (BloFin); the schema and helpers are multi-venue-keyed
so Binance trading can land later without endpoint changes. The venue
parameter defaults to whichever venue currently has an active strategy
session in window — for v1 that always resolves to BloFin.

  GET /api/manager/live/account    — KPI strip per Data Dictionary §3
  GET /api/manager/live/risk       — Risk Signals per §4
  GET /api/manager/live/positions  — Open Positions Table per §12

Caching: Redis with short TTLs (2s on /account + /positions, 5s on
/risk). Cache keys include venue + connection_id. The TPSL fetch (one
HTTP round-trip to BloFin's tpsl-pending endpoint) is cached separately
with 5s TTL and shared between /risk and /positions so opening the
page doesn't trigger duplicate fetches.

**Pure read, no side effects.** These endpoints serve from
exchange_snapshots (kept fresh by the every-5-min sync_exchange_snapshots
cron) plus an on-demand BloFin TPSL fetch. They do NOT call
refresh_snapshots — the live "tick 2s" promise belongs to the WebSocket
sidecar (later step), not these REST endpoints. The frontend can poll
freely without burning BloFin API budget or writing to the DB.

BloFin null-field derivation (used_margin, unrealized_pnl) and source
attribution (manual vs strategy) live in
`app.services.manager_live_state` so /api/manager/positions and these
endpoints produce identical numbers for the same connection. Anyone
adding a third reader should import from there too.
"""

from __future__ import annotations

import json
import logging
import statistics
from typing import Any, Literal

import redis as redis_lib
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ...core.config import settings
from ...db import get_cursor
from ...services.encryption import decrypt_key
from ...services.manager_live_state import (
    build_strategy_symbol_map,
    derive_position_side,
    derive_unrealized_pnl,
    derive_used_margin,
    parse_symbol_base,
)
from .admin import require_admin
from .allocator import (
    fetch_blofin_tpsl_orders,
)

log = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/manager/live",
    tags=["manager-live"],
    dependencies=[Depends(require_admin)],
)


# ── Redis client (module-level singleton) ─────────────────────────────────

_redis: redis_lib.Redis | None = None


def _get_redis() -> redis_lib.Redis:
    """Lazy-init a Redis client. Same DB the Celery broker uses; namespace
    is enforced via `live:*` key prefix."""
    global _redis
    if _redis is None:
        _redis = redis_lib.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis


def _cache_get(key: str) -> Any | None:
    """Return parsed JSON, or None on miss / Redis outage. Redis being
    unavailable should NOT break the Live endpoints — fall through to
    fresh compute."""
    try:
        raw = _get_redis().get(key)
    except redis_lib.RedisError as e:
        log.warning("Redis GET %s failed: %s", key, e)
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _cache_set(key: str, value: Any, *, ttl: int) -> None:
    """Best-effort cache write. Failures log but don't propagate."""
    try:
        _get_redis().setex(key, ttl, json.dumps(value, default=str))
    except redis_lib.RedisError as e:
        log.warning("Redis SETEX %s failed: %s", key, e)


# ── Venue / connection resolution ─────────────────────────────────────────

def _resolve_venue_connection(
    cur, venue: str | None, connection_id: str | None,
) -> tuple[str, str]:
    """Resolve to a concrete (venue, connection_id) pair.

    Defaulting rules:
      * If both supplied: use as-is.
      * If only venue: pick the most recent active connection on that venue.
      * If neither: pick the venue of the active strategy session, then
        the active connection on that venue. v1 always resolves to BloFin.

    Raises HTTPException(404) if no resolution is possible (no active
    strategy session AND no active connections at all).
    """
    if venue and connection_id:
        return venue, connection_id

    if not venue:
        # Find the active strategy session's venue. Pick the most-recently-
        # updated runtime_state from active allocations.
        cur.execute("""
            SELECT ec.exchange
              FROM user_mgmt.allocations a
              JOIN user_mgmt.exchange_connections ec
                ON ec.connection_id = a.connection_id
             WHERE a.status = 'active'
               AND a.runtime_state IS NOT NULL
             ORDER BY (a.runtime_state->>'updated_at')::timestamptz DESC NULLS LAST
             LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            venue = row["exchange"]
        else:
            # Fallback: any active connection (default to blofin if multiple).
            cur.execute("""
                SELECT exchange FROM user_mgmt.exchange_connections
                 WHERE status = 'active'
                 ORDER BY (exchange='blofin') DESC, created_at DESC
                 LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "No active connections")
            venue = row["exchange"]

    if not connection_id:
        cur.execute("""
            SELECT connection_id FROM user_mgmt.exchange_connections
             WHERE exchange = %s AND status = 'active'
             ORDER BY created_at DESC
             LIMIT 1
        """, (venue,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, f"No active {venue} connection")
        connection_id = str(row["connection_id"])

    return venue, connection_id


def _read_latest_snapshot(cur, venue: str, connection_id: str) -> dict | None:
    """Most recent fetch_ok=TRUE snapshot for this connection. None if
    no successful snapshot exists (e.g. fresh connection, all reads
    failing)."""
    cur.execute("""
        SELECT ec.connection_id, ec.exchange AS venue, ec.label,
               es.snapshot_at, es.total_equity_usd, es.available_usd,
               es.used_margin_usd, es.unrealized_pnl, es.positions
          FROM user_mgmt.exchange_connections ec
          JOIN user_mgmt.exchange_snapshots es
            ON es.connection_id = ec.connection_id
           AND es.fetch_ok = TRUE
         WHERE ec.connection_id = %s::uuid AND ec.exchange = %s
         ORDER BY es.snapshot_at DESC
         LIMIT 1
    """, (connection_id, venue))
    return cur.fetchone()


def _read_today_anchor(cur, venue: str, connection_id: str) -> dict | None:
    """Today's UTC anchor row, or None if not yet written. The Live tab
    surfaces missing anchors explicitly — UI shows '—' rather than
    fabricating today's PnL from an older anchor."""
    cur.execute("""
        SELECT anchor_date, total_equity_usd, captured_at,
               raw_payload->'synthesized' AS synthesized
          FROM user_mgmt.account_daily_anchors
         WHERE venue = %s AND connection_id = %s::uuid
           AND anchor_date = (NOW() AT TIME ZONE 'UTC')::date
         LIMIT 1
    """, (venue, connection_id))
    return cur.fetchone()


# ── Connection credentials (for TPSL fetch) ───────────────────────────────

def _get_connection_creds(cur, connection_id: str) -> tuple[str, str, str] | None:
    """Decrypt API credentials for a connection. Returns
    (api_key, api_secret, passphrase) or None if the row has nulls.

    BloFin requires all three. Binance won't be calling this in v1
    (account data is BloFin-only), so the passphrase requirement here
    is fine."""
    cur.execute("""
        SELECT api_key_enc, api_secret_enc, passphrase_enc
          FROM user_mgmt.exchange_connections
         WHERE connection_id = %s::uuid
    """, (connection_id,))
    row = cur.fetchone()
    if not row:
        return None
    if not row["api_key_enc"] or not row["api_secret_enc"]:
        return None
    api_key = decrypt_key(row["api_key_enc"])
    api_secret = decrypt_key(row["api_secret_enc"])
    passphrase = decrypt_key(row["passphrase_enc"]) if row["passphrase_enc"] else ""
    if not api_key or not api_secret:
        return None
    return api_key, api_secret, passphrase


def _get_tpsl_orders(cur, venue: str, connection_id: str) -> dict[str, dict]:
    """Cached TPSL fetch shared by /risk and /positions. 5s TTL. Returns
    empty dict on auth failure or non-BloFin venues (Binance fetch is
    out of v1 scope per the venue split)."""
    if venue != "blofin":
        return {}
    key = f"live:tpsl:{venue}:{connection_id}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    creds = _get_connection_creds(cur, connection_id)
    if not creds:
        log.warning("TPSL fetch skipped: no creds for %s/%s", venue, connection_id[:8])
        _cache_set(key, {}, ttl=5)
        return {}
    api_key, api_secret, passphrase = creds
    try:
        result = fetch_blofin_tpsl_orders(
            api_key=api_key, api_secret=api_secret, passphrase=passphrase,
        )
    except Exception as e:
        log.warning("TPSL fetch failed for %s/%s: %s", venue, connection_id[:8], e)
        result = {}
    _cache_set(key, result, ttl=5)
    return result


# ── Pydantic response models ──────────────────────────────────────────────

class AccountSnapshot(BaseModel):
    """KPI strip (Data Dictionary §3). All amounts USDT-denominated.

    `today_pnl_usd` is None when today's anchor row is missing — UI
    renders '—' rather than fabricating from a stale anchor.
    """
    venue: str
    connection_id: str
    connection_label: str | None
    snapshot_at: str | None

    # Equity column
    total_equity_usd: float
    available_usd: float
    today_pnl_usd: float | None
    today_pnl_pct: float | None
    today_anchor_date: str | None
    today_anchor_missing: bool

    # Deployed margin column
    used_margin_usd: float
    used_margin_pct: float
    open_position_count: int

    # Notional column
    total_notional_usd: float
    notional_to_equity: float
    long_notional_usd: float
    short_notional_usd: float
    long_count: int
    short_count: int

    # Net unrealized
    unrealized_pnl_usd: float
    unrealized_pnl_pct: float
    green_count: int

    # Per-position pct stats
    avg_pnl_pct: float
    median_pnl_pct: float
    pnl_pct_stdev: float

    # Leverage
    avg_leverage: float
    min_leverage: float
    max_leverage: float

    # Source counts
    strategy_count: int
    manual_count: int


class MarginLevel(BaseModel):
    ratio: float | None              # equity / maint_margin if computable
    liquidation_buffer_pct: float | None
    note: str | None = None          # e.g. "BloFin maint_margin not exposed"


class LargestPosition(BaseModel):
    symbol: str
    symbol_base: str
    side: Literal["long", "short"]
    notional_usd: float
    notional_share_pct: float
    leverage: float
    source: Literal["manual", "strategy"]
    strategy_name: str | None


class NearestStop(BaseModel):
    symbol: str | None
    symbol_base: str | None
    sl_price: float | None
    mark_price: float | None
    distance_pct: float | None        # signed: (sl − mark) / mark × 100


class UnhedgedConcentration(BaseModel):
    direction: Literal["long", "short", "balanced"]
    pct_of_book: float
    constituent_symbols: list[str]
    no_protective_stops: list[str]


class RiskSnapshot(BaseModel):
    """Risk Signals row (§4)."""
    venue: str
    connection_id: str
    snapshot_at: str | None

    margin_level: MarginLevel
    largest_position: LargestPosition | None
    nearest_stop: NearestStop
    concentration: UnhedgedConcentration | None


class LivePosition(BaseModel):
    """One row of the Open Positions table (§12)."""
    venue: str
    connection_id: str
    connection_label: str | None
    symbol: str
    symbol_base: str
    side: Literal["long", "short"]

    size: float
    notional_usd: float
    leverage: float | None
    margin_mode: str | None

    entry_price: float | None
    mark_price: float | None
    unrealized_pnl_usd: float
    unrealized_pnl_pct: float

    source: Literal["manual", "strategy"]
    strategy_name: str | None

    # Lifecycle (None until position_history writer ships)
    opened_at: str | None = None
    age_seconds: int | None = None

    # SL/TP
    sl_price: float | None
    sl_distance_pct: float | None
    tp_price: float | None
    tp_distance_pct: float | None
    risk_reward: float | None         # |TP − entry| / |SL − entry|


class PositionsResponse(BaseModel):
    venue: str
    connection_id: str
    connection_label: str | None
    snapshot_at: str | None
    positions: list[LivePosition]
    counts: dict[str, int] = Field(
        default_factory=dict,
        description="Counts per filter chip: total, strategy, manual, long, short",
    )


# ── Compute helpers ───────────────────────────────────────────────────────

def _iso(dt) -> str | None:
    return dt.isoformat() if dt else None


def _enrich_positions(
    snap_positions: list[dict],
    *, venue: str, connection_id: str, connection_label: str | None,
    strat_map: dict[str, str], tpsl_by_inst: dict[str, dict],
) -> list[LivePosition]:
    """Transform raw snapshot positions into LivePosition rows. Pulls
    SL/TP from the TPSL map keyed on raw instId form (BloFin's "BTC-USDT"
    with the dash) — snapshot positions have the dash stripped, so we
    re-insert it for the lookup."""
    out: list[LivePosition] = []
    for p in (snap_positions or []):
        sym_full = (p.get("symbol") or "").upper()
        sym_base = parse_symbol_base(sym_full)
        size_raw = p.get("size") or 0
        side = derive_position_side(p.get("side"), size_raw)
        size_abs = abs(float(size_raw))
        notional = float(p.get("notional_usd") or 0)
        entry = p.get("entry_price")
        mark = p.get("mark_price")
        upl = float(p.get("unrealized_pnl") or 0)
        # Pct = UPL / notional (notional already accounts for BloFin's
        # contract_value scaling; entry_price × size is wrong for
        # contract-denominated venues).
        upl_pct = (upl / notional * 100.0) if notional > 0 else 0.0

        # SL/TP lookup: BloFin TPSL response uses the dash form.
        if sym_base and sym_full.endswith("USDT"):
            inst_id = f"{sym_base}-USDT"
        else:
            inst_id = sym_full
        tpsl = tpsl_by_inst.get(inst_id) or {}
        sl_price = tpsl.get("sl_price")
        tp_price = tpsl.get("tp_price")
        sl_dist = (
            (sl_price - mark) / mark * 100.0
            if sl_price and mark else None
        )
        tp_dist = (
            (tp_price - mark) / mark * 100.0
            if tp_price and mark else None
        )
        rr = (
            abs(tp_price - entry) / abs(sl_price - entry)
            if (sl_price and tp_price and entry and abs(sl_price - entry) > 0)
            else None
        )

        strat_name = strat_map.get(sym_base)
        out.append(LivePosition(
            venue=venue,
            connection_id=connection_id,
            connection_label=connection_label,
            symbol=sym_full,
            symbol_base=sym_base,
            side=side,
            size=size_abs,
            notional_usd=notional,
            leverage=float(p.get("leverage")) if p.get("leverage") else None,
            margin_mode=p.get("margin_mode"),
            entry_price=float(entry) if entry else None,
            mark_price=float(mark) if mark else None,
            unrealized_pnl_usd=upl,
            unrealized_pnl_pct=upl_pct,
            source="strategy" if strat_name else "manual",
            strategy_name=strat_name,
            sl_price=sl_price,
            sl_distance_pct=sl_dist,
            tp_price=tp_price,
            tp_distance_pct=tp_dist,
            risk_reward=rr,
        ))
    return out


def _build_account_response(
    snap: dict, *, venue: str, connection_id: str,
    today_anchor: dict | None,
    enriched: list[LivePosition],
) -> AccountSnapshot:
    eq = float(snap["total_equity_usd"] or 0)
    av = float(snap["available_usd"] or 0)
    um = derive_used_margin(
        total_equity=eq,
        available=av,
        raw_used_margin=float(snap["used_margin_usd"] or 0),
    )
    un = derive_unrealized_pnl(
        raw_unrealized_pnl=float(snap["unrealized_pnl"] or 0),
        positions=snap["positions"] or [],
    )

    long_pos = [p for p in enriched if p.side == "long"]
    short_pos = [p for p in enriched if p.side == "short"]
    long_notional = sum(p.notional_usd for p in long_pos)
    short_notional = sum(p.notional_usd for p in short_pos)
    total_notional = long_notional + short_notional

    pcts = [p.unrealized_pnl_pct for p in enriched]
    avg_pct = statistics.fmean(pcts) if pcts else 0.0
    med_pct = statistics.median(pcts) if pcts else 0.0
    std_pct = statistics.pstdev(pcts) if len(pcts) > 1 else 0.0

    leverages = [p.leverage for p in enriched if p.leverage]
    avg_lev = (
        sum(p.notional_usd * p.leverage for p in enriched if p.leverage) / total_notional
        if total_notional and leverages else 0.0
    )
    min_lev = min(leverages) if leverages else 0.0
    max_lev = max(leverages) if leverages else 0.0

    today_pnl_usd: float | None = None
    today_pnl_pct: float | None = None
    today_anchor_date: str | None = None
    today_anchor_missing = today_anchor is None
    if today_anchor:
        anchor_eq = float(today_anchor["total_equity_usd"] or 0)
        today_pnl_usd = eq - anchor_eq
        today_pnl_pct = (today_pnl_usd / anchor_eq * 100.0) if anchor_eq > 0 else 0.0
        today_anchor_date = today_anchor["anchor_date"].isoformat()

    return AccountSnapshot(
        venue=venue,
        connection_id=connection_id,
        connection_label=snap.get("label"),
        snapshot_at=_iso(snap["snapshot_at"]),
        total_equity_usd=round(eq, 2),
        available_usd=round(av, 2),
        today_pnl_usd=round(today_pnl_usd, 2) if today_pnl_usd is not None else None,
        today_pnl_pct=round(today_pnl_pct, 4) if today_pnl_pct is not None else None,
        today_anchor_date=today_anchor_date,
        today_anchor_missing=today_anchor_missing,
        used_margin_usd=round(um, 2),
        used_margin_pct=round((um / eq * 100.0) if eq > 0 else 0.0, 2),
        open_position_count=len(enriched),
        total_notional_usd=round(total_notional, 2),
        notional_to_equity=round((total_notional / eq) if eq > 0 else 0.0, 4),
        long_notional_usd=round(long_notional, 2),
        short_notional_usd=round(short_notional, 2),
        long_count=len(long_pos),
        short_count=len(short_pos),
        unrealized_pnl_usd=round(un, 2),
        unrealized_pnl_pct=round((un / eq * 100.0) if eq > 0 else 0.0, 4),
        green_count=sum(1 for p in enriched if p.unrealized_pnl_usd > 0),
        avg_pnl_pct=round(avg_pct, 4),
        median_pnl_pct=round(med_pct, 4),
        pnl_pct_stdev=round(std_pct, 4),
        avg_leverage=round(avg_lev, 2),
        min_leverage=round(min_lev, 2),
        max_leverage=round(max_lev, 2),
        strategy_count=sum(1 for p in enriched if p.source == "strategy"),
        manual_count=sum(1 for p in enriched if p.source == "manual"),
    )


def _build_risk_response(
    snap: dict, *, venue: str, connection_id: str,
    enriched: list[LivePosition],
) -> RiskSnapshot:
    # SL/TP for the nearest_stop check is already attached to each
    # LivePosition by _enrich_positions, so we don't need a separate
    # tpsl_by_inst arg here.
    total_notional = sum(p.notional_usd for p in enriched)

    # Margin level: BloFin balance endpoint doesn't reliably expose
    # maintenance margin. Skip rather than fabricate. Once we have a
    # reliable maint_margin source (BloFin "/api/v1/account/balance" detail
    # rows or computed from positions × maint rate × notional), wire it in.
    margin_level = MarginLevel(
        ratio=None,
        liquidation_buffer_pct=None,
        note="BloFin maintenance margin not exposed at account level — wire in v2",
    )

    # Largest by absolute notional
    largest_position: LargestPosition | None = None
    if enriched:
        big = max(enriched, key=lambda p: p.notional_usd)
        largest_position = LargestPosition(
            symbol=big.symbol,
            symbol_base=big.symbol_base,
            side=big.side,
            notional_usd=big.notional_usd,
            notional_share_pct=round((big.notional_usd / total_notional * 100.0) if total_notional else 0, 2),
            leverage=big.leverage or 0.0,
            source=big.source,
            strategy_name=big.strategy_name,
        )

    # Nearest stop: positions with an SL only, picked by smallest |distance|
    sl_candidates = [p for p in enriched if p.sl_price and p.mark_price and p.sl_distance_pct is not None]
    nearest_stop = NearestStop(
        symbol=None, symbol_base=None,
        sl_price=None, mark_price=None, distance_pct=None,
    )
    if sl_candidates:
        nearest = min(sl_candidates, key=lambda p: abs(p.sl_distance_pct or 0))
        nearest_stop = NearestStop(
            symbol=nearest.symbol,
            symbol_base=nearest.symbol_base,
            sl_price=nearest.sl_price,
            mark_price=nearest.mark_price,
            distance_pct=round(nearest.sl_distance_pct or 0, 4),
        )

    # Unhedged concentration: max single-direction share
    long_notional = sum(p.notional_usd for p in enriched if p.side == "long")
    short_notional = sum(p.notional_usd for p in enriched if p.side == "short")
    concentration: UnhedgedConcentration | None = None
    if total_notional > 0:
        if long_notional > short_notional:
            direction: Literal["long", "short", "balanced"] = "long"
            cluster = [p for p in enriched if p.side == "long"]
            pct = round(long_notional / total_notional * 100.0, 2)
        elif short_notional > long_notional:
            direction = "short"
            cluster = [p for p in enriched if p.side == "short"]
            pct = round(short_notional / total_notional * 100.0, 2)
        else:
            direction = "balanced"
            cluster = list(enriched)
            pct = 50.0
        concentration = UnhedgedConcentration(
            direction=direction,
            pct_of_book=pct,
            constituent_symbols=[p.symbol_base for p in cluster],
            no_protective_stops=[p.symbol_base for p in cluster if not p.sl_price],
        )

    return RiskSnapshot(
        venue=venue,
        connection_id=connection_id,
        snapshot_at=_iso(snap["snapshot_at"]),
        margin_level=margin_level,
        largest_position=largest_position,
        nearest_stop=nearest_stop,
        concentration=concentration,
    )


# ── Endpoints ─────────────────────────────────────────────────────────────

CACHE_TTL_ACCOUNT_S = 2
CACHE_TTL_POSITIONS_S = 2
CACHE_TTL_RISK_S = 5


@router.get("/account", response_model=AccountSnapshot)
def get_account(
    venue: str | None = Query(None, pattern="^(blofin|binance)$"),
    connection_id: str | None = None,
    cur=Depends(get_cursor),
) -> AccountSnapshot:
    venue, connection_id = _resolve_venue_connection(cur, venue, connection_id)
    cache_key = f"live:account:{venue}:{connection_id}"
    cached = _cache_get(cache_key)
    if cached:
        return AccountSnapshot.model_validate(cached)

    snap = _read_latest_snapshot(cur, venue, connection_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {venue}/{connection_id}")

    strat_map = build_strategy_symbol_map(cur, [connection_id])
    conn_strat = strat_map.get(connection_id, {})
    # /account doesn't surface SL/TP — skip the TPSL fetch entirely so the
    # uncached path is dominated by the DB read (~10ms) rather than a
    # BloFin HTTP round-trip (~400ms).
    enriched = _enrich_positions(
        snap["positions"] or [],
        venue=venue, connection_id=connection_id,
        connection_label=snap.get("label"),
        strat_map=conn_strat, tpsl_by_inst={},
    )
    today_anchor = _read_today_anchor(cur, venue, connection_id)
    response = _build_account_response(
        snap, venue=venue, connection_id=connection_id,
        today_anchor=today_anchor, enriched=enriched,
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_ACCOUNT_S)
    return response


@router.get("/risk", response_model=RiskSnapshot)
def get_risk(
    venue: str | None = Query(None, pattern="^(blofin|binance)$"),
    connection_id: str | None = None,
    cur=Depends(get_cursor),
) -> RiskSnapshot:
    venue, connection_id = _resolve_venue_connection(cur, venue, connection_id)
    cache_key = f"live:risk:{venue}:{connection_id}"
    cached = _cache_get(cache_key)
    if cached:
        return RiskSnapshot.model_validate(cached)

    snap = _read_latest_snapshot(cur, venue, connection_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {venue}/{connection_id}")

    strat_map = build_strategy_symbol_map(cur, [connection_id])
    conn_strat = strat_map.get(connection_id, {})
    tpsl = _get_tpsl_orders(cur, venue, connection_id)
    enriched = _enrich_positions(
        snap["positions"] or [],
        venue=venue, connection_id=connection_id,
        connection_label=snap.get("label"),
        strat_map=conn_strat, tpsl_by_inst=tpsl,
    )
    response = _build_risk_response(
        snap, venue=venue, connection_id=connection_id,
        enriched=enriched,
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_RISK_S)
    return response


@router.get("/positions", response_model=PositionsResponse)
def get_positions(
    venue: str | None = Query(None, pattern="^(blofin|binance)$"),
    connection_id: str | None = None,
    cur=Depends(get_cursor),
) -> PositionsResponse:
    venue, connection_id = _resolve_venue_connection(cur, venue, connection_id)
    cache_key = f"live:positions:{venue}:{connection_id}"
    cached = _cache_get(cache_key)
    if cached:
        return PositionsResponse.model_validate(cached)

    snap = _read_latest_snapshot(cur, venue, connection_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {venue}/{connection_id}")

    strat_map = build_strategy_symbol_map(cur, [connection_id])
    conn_strat = strat_map.get(connection_id, {})
    tpsl = _get_tpsl_orders(cur, venue, connection_id)
    enriched = _enrich_positions(
        snap["positions"] or [],
        venue=venue, connection_id=connection_id,
        connection_label=snap.get("label"),
        strat_map=conn_strat, tpsl_by_inst=tpsl,
    )
    # Default sort: absolute notional descending — most-impactful row first.
    enriched.sort(key=lambda p: -p.notional_usd)

    response = PositionsResponse(
        venue=venue,
        connection_id=connection_id,
        connection_label=snap.get("label"),
        snapshot_at=_iso(snap["snapshot_at"]),
        positions=enriched,
        counts={
            "total": len(enriched),
            "strategy": sum(1 for p in enriched if p.source == "strategy"),
            "manual": sum(1 for p in enriched if p.source == "manual"),
            "long": sum(1 for p in enriched if p.side == "long"),
            "short": sum(1 for p in enriched if p.side == "short"),
        },
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_POSITIONS_S)
    return response

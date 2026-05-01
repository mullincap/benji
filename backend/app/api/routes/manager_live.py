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
import time
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
from ...services.ema_cache import (
    TF_ORDER,
    alignment_tier,
    compute_and_cache_ema20,
)
from ...services.boxplot_cache import (
    compute_and_cache_boxplot,
    mark_dot_class,
    trend_color,
)
from ...services.correlation_cache import get_coverage_matrix_cached
from ...services.factor_decomp_cache import get_factor_decomposition_cached
from ...services.exchanges.binance_market import BinanceMarketClient
from concurrent.futures import ThreadPoolExecutor

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
    failing).

    With the BloFin WebSocket sidecar running, the freshest data lives
    in Redis (writes-on-push, near-real-time). When sidecar is healthy
    we synthesize a snapshot row from Redis; when stale or absent we
    fall back to this DB-cached query (5-min cron staleness ceiling).

    The dict returned by this function carries two extra markers used
    by callers to populate the response's `stale_source` /
    `sidecar_stale` fields:
        snap['_stale_source']   ∈ {None, 'rest_fallback'}
        snap['_sidecar_stale']  ∈ {True, False}
    """
    redis_snap = _try_read_redis_snapshot(venue, connection_id)
    if redis_snap is not None:
        return redis_snap

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
    row = cur.fetchone()
    if row is None:
        return None
    out = dict(row)
    # DB read: by definition not fresh from sidecar.
    out["_stale_source"] = "rest_fallback"
    out["_sidecar_stale"] = True
    return out


# ── Redis-first read (sidecar fast path) ────────────────────────────


# Heartbeat staleness threshold — keeps in sync with the sidecar's
# heartbeat key TTL (30s). If the heartbeat is older than this we don't
# trust the Redis data and force the DB fallback.
_SIDECAR_HEARTBEAT_STALE_S = 30


def _try_read_redis_snapshot(venue: str, connection_id: str) -> dict | None:
    """Synthesize a snapshot row from Redis keys written by the sidecar.

    Returns a dict in the same shape as the DB-read row when:
      * venue == 'blofin' (v1 sidecar scope)
      * account snapshot key exists
      * positions list key exists
      * heartbeat key exists AND is younger than _SIDECAR_HEARTBEAT_STALE_S

    Returns None on any miss — caller falls through to the DB query.
    Redis errors are logged and treated as misses so a Redis hiccup
    never breaks the read path; the DB fallback always works.
    """
    if venue != "blofin":
        return None
    try:
        r = _get_redis()
        account_raw = r.get(f"account:blofin:{connection_id}:snapshot")
        positions_raw = r.get(f"positions:blofin:{connection_id}:list")
        heartbeat_raw = r.get(f"sidecar:blofin:{connection_id}:heartbeat")
    except redis_lib.RedisError as e:
        log.warning("Redis read failed in snapshot path: %s", e)
        return None
    if not account_raw or not positions_raw or not heartbeat_raw:
        return None
    try:
        heartbeat_ts = int(heartbeat_raw)
    except (TypeError, ValueError):
        return None
    age_s = int(time.time()) - heartbeat_ts
    if age_s > _SIDECAR_HEARTBEAT_STALE_S:
        # Sidecar isn't writing recently — let the caller fall back to
        # the DB row. Endpoints will mark sidecar_stale=true on that
        # response too because the DB path sets _sidecar_stale=True.
        return None
    try:
        account = json.loads(account_raw)
        positions = json.loads(positions_raw)
    except json.JSONDecodeError as e:
        log.warning("Redis snapshot JSON parse failed: %s", e)
        return None

    # Look up the connection label since Redis doesn't store it. This
    # is a tiny indexed query and the sidecar happy-path skips the rest
    # of the DB work, so net DB load drops dramatically vs the cron path.
    label = _get_connection_label(connection_id)

    return {
        "connection_id": connection_id,
        "venue": "blofin",
        "label": label,
        "snapshot_at": _heartbeat_to_datetime(heartbeat_ts),
        "total_equity_usd": account.get("total_equity_usd"),
        "available_usd": account.get("available_usd"),
        "used_margin_usd": account.get("used_margin_usd"),
        "unrealized_pnl": account.get("unrealized_pnl"),
        "positions": positions,
        "_stale_source": None,
        "_sidecar_stale": False,
    }


def _heartbeat_to_datetime(unix_s: int):
    """Convert sidecar heartbeat (epoch seconds) to a tz-aware UTC
    datetime, matching the type of exchange_snapshots.snapshot_at."""
    import datetime as _dt
    return _dt.datetime.fromtimestamp(unix_s, tz=_dt.timezone.utc)


_LABEL_CACHE: dict[str, tuple[str | None, float]] = {}


def _get_connection_label(connection_id: str) -> str | None:
    """Per-process cache of connection labels. Labels are user-edited
    rarely; a 60s TTL avoids hammering the DB on every hot-path read."""
    cached = _LABEL_CACHE.get(connection_id)
    now = time.time()
    if cached and (now - cached[1]) < 60:
        return cached[0]
    # No DB cursor passed in here; use a one-shot connection. This
    # only fires on Redis-hit cache miss — at most once per minute per
    # process, negligible.
    try:
        from ...db import get_worker_conn
        conn = get_worker_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT label FROM user_mgmt.exchange_connections "
                "WHERE connection_id = %s::uuid",
                (connection_id,),
            )
            row = cur.fetchone()
            label = row[0] if row else None
        finally:
            conn.close()
    except Exception as e:
        log.warning("label lookup failed for %s: %s", connection_id, e)
        label = cached[0] if cached else None
    _LABEL_CACHE[connection_id] = (label, now)
    return label


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


def _read_position_anchors_today(cur, venue: str, connection_id: str) -> dict[str, float]:
    """Today's per-position UPL anchors, indexed by position_id (= symbol
    in BloFin net mode). Returns {} if no anchors written for today.

    Drives waterfall today_pnl computation:
      today_pnl_per_position = current_unrealized_pnl − anchor_unrealized_pnl

    Picks the EARLIEST snapshot per position for today (the 00:00 UTC
    anchor) — the writer normally writes one row per position per day,
    but if a future intraday snapshotter ships, the earliest still wins
    as the day-anchor.
    """
    cur.execute("""
        SELECT DISTINCT ON (position_id)
               position_id, unrealized_pnl_usd
          FROM user_mgmt.position_snapshots
         WHERE venue = %s AND connection_id = %s::uuid
           AND snapshot_at::date = (NOW() AT TIME ZONE 'UTC')::date
         ORDER BY position_id, snapshot_at ASC
    """, (venue, connection_id))
    out: dict[str, float] = {}
    for row in cur.fetchall():
        if row["unrealized_pnl_usd"] is not None:
            out[row["position_id"]] = float(row["unrealized_pnl_usd"])
    return out


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

    # Sidecar freshness (BloFin WebSocket sidecar — see services/sidecars/).
    # `stale_source` is None when this response was synthesized from a
    # fresh Redis sidecar push; "rest_fallback" when the read fell
    # through to the 5-min cron-backed exchange_snapshots row.
    # `sidecar_stale` is True when the sidecar heartbeat is older than
    # 30s (sidecar dead or restarting); the frontend renders an amber
    # badge in either case.
    stale_source: Literal["rest_fallback"] | None = None
    sidecar_stale: bool = False


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

    # Sidecar freshness — same semantics as AccountSnapshot.
    stale_source: Literal["rest_fallback"] | None = None
    sidecar_stale: bool = False


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

    # Today's PnL contribution per position (drives waterfall §6).
    # Computed as `unrealized_pnl_usd − today_anchor_unrealized_pnl_usd`.
    # null when no anchor row exists for today; UI shows an "anchor
    # missing" badge on the bar in that case.
    today_pnl_usd: float | None
    today_anchor_missing: bool


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

    # Sidecar freshness — same semantics as AccountSnapshot.
    stale_source: Literal["rest_fallback"] | None = None
    sidecar_stale: bool = False


# ── MA Alignment Heatmap (Data Dictionary §11) ─────────────────────────

MaAlignmentTier = Literal[
    "aligned-strong", "aligned-mid", "aligned-soft",
    "neutral",
    "against-soft", "against-mid", "against-strong",
]


class MaCell(BaseModel):
    """One (symbol, timeframe) cell. `distance_pct` is null when the
    underlying kline data isn't available (insufficient history,
    fetch failure, or the symbol isn't listed on Binance USDM); the
    UI renders a neutral '—' in those cells."""
    distance_pct: float | None
    ema_value: float | None
    tier: MaAlignmentTier
    reason: str | None  # null on success; otherwise 'not_listed' /
                       # 'insufficient_history' / 'fetch_error'


class MaRow(BaseModel):
    """One open position's row of the heatmap."""
    symbol: str
    symbol_base: str
    side: Literal["long", "short"]
    binance_symbol: str | None        # resolved via market.symbols.binance_id
    mark_price: float | None
    cells: dict[str, MaCell]          # keyed by tf (5m/15m/30m/1h/4h/8h/1d)
    confluence_aligned: int           # # of tfs where alignment matches direction
    confluence_total: int             # # of tfs with usable data


class MaAlignmentResponse(BaseModel):
    venue: str
    connection_id: str
    snapshot_at: str | None
    timeframes: list[str]             # column order = TF_ORDER
    rows: list[MaRow]


# ── Box Plot Strip (Data Dictionary §10) ───────────────────────────────

# Mark-dot color classes per §10:
#   'good' = aligned with position direction
#   'bad'  = strongly counter-aligned
#   'neu'  = in the box's neutral mid-zone
BoxDotClass = Literal["good", "bad", "neu"]
TrendDirection = Literal["strong-up", "up", "flat", "down", "strong-down"]


class BoxPlotCell(BaseModel):
    """One position's 24h × 5m distribution, including the live mark dot
    + entry triangle the SVG renders. `reason` non-null means we can't
    render the box plot for this position — frontend shows
    'INSUFFICIENT DATA' instead."""
    symbol: str
    symbol_base: str
    side: Literal["long", "short"]
    binance_symbol: str | None

    # Distribution (null when reason != null)
    p5: float | None
    p25: float | None
    p50: float | None
    p75: float | None
    p95: float | None
    win_min: float | None  # named 'min' in cache; renamed for JSON clarity
    win_max: float | None

    # Live overlay
    mark_price: float | None
    entry_price: float | None
    mark_dot: BoxDotClass

    # Trend
    slope_sigma: float | None
    trend_direction: TrendDirection | None
    trend_color: BoxDotClass

    # Diagnostics
    last_close_ts: int | None
    reason: str | None


class BoxPlotsResponse(BaseModel):
    venue: str
    connection_id: str
    snapshot_at: str | None
    cells: list[BoxPlotCell]


# ── Coverage Matrix + Effective-N (§8 + §9a) ──────────────────────────

# Cell tier names mirror the mockup CSS classes (cm-strong-con etc.)
# and the `diag` / `insufficient` sentinels.
CovTier = Literal[
    "strong-con", "mid-con", "soft-con",
    "neutral",
    "soft-hedge", "mid-hedge", "strong-hedge",
    "diag", "insufficient",
]


class CoverageRow(BaseModel):
    symbol: str
    symbol_base: str
    side: Literal["long", "short"]
    notional_usd: float
    binance_symbol: str | None
    has_history: bool
    sigma_daily: float | None  # daily $ stdev of this position's PnL series


class CoverageMatrixResponse(BaseModel):
    venue: str
    connection_id: str
    snapshot_at: str | None
    rows: list[CoverageRow]
    # NxN matrix; cell value is the Pearson correlation (or null when
    # one side has insufficient history). Diagonal is 1.0.
    matrix: list[list[float | None]]
    # NxN tier names parallel to `matrix` so the frontend can color
    # without re-binning.
    tiers: list[list[CovTier]]
    effective_n: float | None
    diversification_benefit_pct: float | None
    nominal_count: int
    reasons: dict[str, str]  # symbol → 'not_listed' | 'insufficient_history'


# ── Factor Decomposition (§9b) ────────────────────────────────────────


class FactorVarPct(BaseModel):
    """Per-position variance attribution (sums to 100% across the three
    when present, or all-null when the position has no usable history)."""

    btc: float | None
    alt: float | None
    idio: float | None


class FactorPositionRow(BaseModel):
    symbol: str
    symbol_base: str
    side: Literal["long", "short"]
    notional_usd: float
    has_history: bool
    # β values are in dollar terms — a +1.0 (= +100%) move in the factor
    # would translate to $β of position PnL on average, holding the
    # other factor constant. Null when has_history=False.
    beta_btc: float | None
    beta_alt: float | None
    var_pct: FactorVarPct


class FactorPortfolio(BaseModel):
    beta_btc: float
    beta_alt: float
    var_btc_pct: float
    var_alt_pct: float
    var_idio_pct: float
    sigma_silo_usd: float
    sigma_portfolio_usd: float
    diversification_benefit_pct: float | None


class FactorDecompositionResponse(BaseModel):
    venue: str
    connection_id: str
    snapshot_at: str | None
    positions: list[FactorPositionRow]
    portfolio: FactorPortfolio | None
    alt_index_member_count: int
    alt_index_target_count: int
    n_days: int
    reasons: dict[str, str]


# ── Compute helpers ───────────────────────────────────────────────────────

def _iso(dt) -> str | None:
    return dt.isoformat() if dt else None


def _enrich_positions(
    snap_positions: list[dict],
    *, venue: str, connection_id: str, connection_label: str | None,
    strat_map: dict[str, str], tpsl_by_inst: dict[str, dict],
    position_anchors: dict[str, float] | None = None,
) -> list[LivePosition]:
    """Transform raw snapshot positions into LivePosition rows. Pulls
    SL/TP from the TPSL map keyed on raw instId form (BloFin's "BTC-USDT"
    with the dash) — snapshot positions have the dash stripped, so we
    re-insert it for the lookup.

    `position_anchors` is {position_id → today's anchor UPL} from
    `position_snapshots`. When None or missing for a position,
    today_pnl_usd is None and today_anchor_missing=True."""
    anchors = position_anchors or {}
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

        # Today's PnL contribution: current upl − today's anchor upl.
        # Anchor key = sym_full (matches position_snapshots.position_id
        # which the writer sets to the snapshot's symbol form).
        anchor_upl = anchors.get(sym_full)
        if anchor_upl is None:
            today_pnl: float | None = None
            anchor_missing = True
        else:
            today_pnl = upl - anchor_upl
            anchor_missing = False

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
            today_pnl_usd=today_pnl,
            today_anchor_missing=anchor_missing,
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
        stale_source=snap.get("_stale_source"),
        sidecar_stale=bool(snap.get("_sidecar_stale", False)),
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
        stale_source=snap.get("_stale_source"),
        sidecar_stale=bool(snap.get("_sidecar_stale", False)),
    )


# ── Endpoints ─────────────────────────────────────────────────────────────

CACHE_TTL_ACCOUNT_S = 2
CACHE_TTL_POSITIONS_S = 2
CACHE_TTL_RISK_S = 5
# /ma-alignment piggybacks on the EMA cache (which already has TF×2
# TTLs); a thin 30s response cache absorbs frontend polling without
# masking bar-close updates beyond the next minute. Frontend polls at
# 60s anyway, so this cache is mostly a thundering-herd guard.
CACHE_TTL_MA_ALIGNMENT_S = 30
# /boxplots — same logic as /ma-alignment: thin response cache absorbs
# duplicate frontend polls without masking the underlying 5m bar-close
# rotation in boxplot_cache.
CACHE_TTL_BOXPLOTS_S = 30
# /coverage-matrix piggybacks on the correlation_cache (1H-bar TTL).
# 60s response cache absorbs frontend polling without masking the
# hourly recompute window.
CACHE_TTL_COVERAGE_S = 60
# /factor-decomposition piggybacks on factor_decomp_cache (also 1H-bar
# TTL) — same 60s response cache rationale as coverage matrix.
CACHE_TTL_FACTOR_DECOMP_S = 60


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
    # Today's per-position UPL anchors (drives the waterfall today_pnl_usd
    # field). Cheap: one SQL read against position_snapshots filtered to
    # today's UTC date. Cached implicitly by the endpoint's 2s response
    # cache.
    pos_anchors = _read_position_anchors_today(cur, venue, connection_id)
    enriched = _enrich_positions(
        snap["positions"] or [],
        venue=venue, connection_id=connection_id,
        connection_label=snap.get("label"),
        strat_map=conn_strat, tpsl_by_inst=tpsl,
        position_anchors=pos_anchors,
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
        stale_source=snap.get("_stale_source"),
        sidecar_stale=bool(snap.get("_sidecar_stale", False)),
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_POSITIONS_S)
    return response


# ── MA Alignment Heatmap endpoint ─────────────────────────────────────

def _resolve_binance_ids(cur, symbol_bases: list[str]) -> dict[str, str | None]:
    """Look up market.symbols.binance_id for each base. Returns {base:
    binance_id_or_None}. The 1000-prefix tickers (1000PEPE etc.) are
    why we go through the symbol registry instead of naive
    `base + 'USDT'`."""
    if not symbol_bases:
        return {}
    cur.execute("""
        SELECT base, binance_id
          FROM market.symbols
         WHERE base = ANY(%s::text[])
    """, (list(set(symbol_bases)),))
    rows = cur.fetchall()
    out: dict[str, str | None] = {b: None for b in symbol_bases}
    for r in rows:
        out[r["base"]] = r["binance_id"]
    return out


@router.get("/ma-alignment", response_model=MaAlignmentResponse)
def get_ma_alignment(
    venue: str | None = Query(None, pattern="^(blofin|binance)$"),
    connection_id: str | None = None,
    cur=Depends(get_cursor),
) -> MaAlignmentResponse:
    """Per-position EMA20 distance heatmap across the seven canonical
    timeframes. Underlying EMA values come from a per-(symbol, tf,
    bar_close_ts) Redis cache populated on demand from Binance USDM
    klines (see services.ema_cache). Mark prices are sourced from the
    BloFin snapshot — the same mark the rest of the Live tab reads.

    Per-position fetches across timeframes are parallelized via a
    thread pool so a cold-cache page load completes in roughly one
    Binance-round-trip's worth of latency rather than 49×.
    """
    venue, connection_id = _resolve_venue_connection(cur, venue, connection_id)
    cache_key = f"live:ma-alignment:{venue}:{connection_id}"
    cached = _cache_get(cache_key)
    if cached:
        return MaAlignmentResponse.model_validate(cached)

    snap = _read_latest_snapshot(cur, venue, connection_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {venue}/{connection_id}")

    # Open positions only.
    open_positions: list[dict] = []
    for p in (snap["positions"] or []):
        sym = (p.get("symbol") or "").upper()
        if not sym:
            continue
        size = float(p.get("size") or 0)
        if size == 0:
            continue
        open_positions.append(p)

    # Resolve Binance ids per base symbol.
    bases = [parse_symbol_base(p.get("symbol") or "") for p in open_positions]
    binance_ids = _resolve_binance_ids(cur, bases)

    # Build the (binance_symbol, tf) work list. Skip cells whose
    # binance_symbol is None — those return synthetic 'not_listed'
    # cells without an HTTP call.
    client = BinanceMarketClient()

    def fetch_one(binance_symbol: str | None, tf: str) -> tuple[str, dict]:
        return tf, compute_and_cache_ema20(
            binance_symbol=binance_symbol, tf=tf, client=client,
        )

    rows: list[MaRow] = []
    # ThreadPoolExecutor lets the per-(position, tf) Binance fetches
    # run in parallel. With ~7 positions × 7 TFs = 49 fetches and 16
    # workers, a fully cold cache primes in ~3-6 round-trips × ~150ms.
    with ThreadPoolExecutor(max_workers=16) as ex:
        # Submit all (position, tf) tuples up front so all fetches run
        # in parallel rather than serializing per-row.
        futures: list[tuple[dict, str, "object"]] = []
        for p in open_positions:
            sym_full = (p.get("symbol") or "").upper()
            sym_base = parse_symbol_base(sym_full)
            binance_id = binance_ids.get(sym_base)
            for tf in TF_ORDER:
                fut = ex.submit(fetch_one, binance_id, tf)
                futures.append((p, tf, fut))

        # Group results by position.
        per_pos: dict[str, dict[str, dict]] = {}
        for p, tf, fut in futures:
            sym_full = (p.get("symbol") or "").upper()
            per_pos.setdefault(sym_full, {})[tf] = fut.result()[1]

    for p in open_positions:
        sym_full = (p.get("symbol") or "").upper()
        sym_base = parse_symbol_base(sym_full)
        binance_id = binance_ids.get(sym_base)
        side = derive_position_side(p.get("side"), p.get("size") or 0)
        mark = p.get("mark_price")
        mark_f = float(mark) if mark else None

        cells: dict[str, MaCell] = {}
        confluence_aligned = 0
        confluence_total = 0
        for tf in TF_ORDER:
            ema_result = per_pos.get(sym_full, {}).get(tf, {
                "ema_value": None, "reason": "fetch_error",
            })
            ema_val = ema_result.get("ema_value")
            reason = ema_result.get("reason")

            if ema_val is None or mark_f is None or mark_f <= 0:
                cells[tf] = MaCell(
                    distance_pct=None,
                    ema_value=None,
                    tier="neutral",
                    reason=reason or "no_mark",
                )
                continue

            dist = (mark_f - float(ema_val)) / mark_f * 100.0
            tier = alignment_tier(dist, side)
            cells[tf] = MaCell(
                distance_pct=round(dist, 4),
                ema_value=float(ema_val),
                tier=tier,
                reason=None,
            )
            confluence_total += 1
            if tier.startswith("aligned-"):
                confluence_aligned += 1

        rows.append(MaRow(
            symbol=sym_full,
            symbol_base=sym_base,
            side=side,
            binance_symbol=binance_id,
            mark_price=mark_f,
            cells=cells,
            confluence_aligned=confluence_aligned,
            confluence_total=confluence_total,
        ))

    response = MaAlignmentResponse(
        venue=venue,
        connection_id=connection_id,
        snapshot_at=_iso(snap["snapshot_at"]),
        timeframes=list(TF_ORDER),
        rows=rows,
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_MA_ALIGNMENT_S)
    return response


@router.get("/boxplots", response_model=BoxPlotsResponse)
def get_boxplots(
    venue: str | None = Query(None, pattern="^(blofin|binance)$"),
    connection_id: str | None = None,
    cur=Depends(get_cursor),
) -> BoxPlotsResponse:
    """24h × 5m close-price distribution per open position. Drives the
    box plot strip (Data Dictionary §10).

    Like /ma-alignment, fetches are parallelized across positions —
    one Binance klines round-trip per symbol on a cold cache. Mark
    price + entry price come from the BloFin snapshot; box / whisker
    / median come from the Binance kline distribution.
    """
    venue, connection_id = _resolve_venue_connection(cur, venue, connection_id)
    cache_key = f"live:boxplots:{venue}:{connection_id}"
    cached = _cache_get(cache_key)
    if cached:
        return BoxPlotsResponse.model_validate(cached)

    snap = _read_latest_snapshot(cur, venue, connection_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {venue}/{connection_id}")

    open_positions: list[dict] = []
    for p in (snap["positions"] or []):
        sym = (p.get("symbol") or "").upper()
        size = float(p.get("size") or 0)
        if not sym or size == 0:
            continue
        open_positions.append(p)

    bases = [parse_symbol_base(p.get("symbol") or "") for p in open_positions]
    binance_ids = _resolve_binance_ids(cur, bases)
    client = BinanceMarketClient()

    # Parallel fetch: one box plot computation per symbol.
    def fetch_one(p: dict) -> tuple[str, dict]:
        base = parse_symbol_base(p.get("symbol") or "")
        bid = binance_ids.get(base)
        return p.get("symbol") or "", compute_and_cache_boxplot(
            binance_symbol=bid, client=client,
        )

    results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, dist in ex.map(fetch_one, open_positions):
            results[sym] = dist

    cells: list[BoxPlotCell] = []
    for p in open_positions:
        sym_full = (p.get("symbol") or "").upper()
        sym_base = parse_symbol_base(sym_full)
        side = derive_position_side(p.get("side"), p.get("size") or 0)
        binance_id = binance_ids.get(sym_base)
        mark = float(p["mark_price"]) if p.get("mark_price") else None
        entry = float(p["entry_price"]) if p.get("entry_price") else None
        dist = results.get(sym_full, {})

        # Mark dot + trend color require both a mark and a valid
        # distribution. Fall back to 'neu' otherwise so the frontend
        # has a stable enum to render against.
        if (
            mark is not None
            and dist.get("p25") is not None
            and dist.get("p50") is not None
            and dist.get("p75") is not None
        ):
            mark_dot = mark_dot_class(
                mark=mark,
                p25=float(dist["p25"]),
                p50=float(dist["p50"]),
                p75=float(dist["p75"]),
                side=side,
            )
        else:
            mark_dot = "neu"

        trend = dist.get("trend")
        tc = trend_color(trend, side) if trend else "neu"

        cells.append(BoxPlotCell(
            symbol=sym_full,
            symbol_base=sym_base,
            side=side,
            binance_symbol=binance_id,
            p5=_opt_float(dist.get("p5")),
            p25=_opt_float(dist.get("p25")),
            p50=_opt_float(dist.get("p50")),
            p75=_opt_float(dist.get("p75")),
            p95=_opt_float(dist.get("p95")),
            win_min=_opt_float(dist.get("min")),
            win_max=_opt_float(dist.get("max")),
            mark_price=mark,
            entry_price=entry,
            mark_dot=mark_dot,
            slope_sigma=_opt_float(dist.get("slope_sigma")),
            trend_direction=trend,
            trend_color=tc,
            last_close_ts=dist.get("last_close_ts"),
            reason=dist.get("reason"),
        ))

    # Default sort: notional desc, same as /positions, so the box plot
    # strip's column order matches the rest of the page.
    cells.sort(
        key=lambda c: -(
            float(next(
                (p.get("notional_usd") or 0)
                for p in open_positions
                if (p.get("symbol") or "").upper() == c.symbol
            ))
        ),
    )

    response = BoxPlotsResponse(
        venue=venue,
        connection_id=connection_id,
        snapshot_at=_iso(snap["snapshot_at"]),
        cells=cells,
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_BOXPLOTS_S)
    return response


def _opt_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@router.get("/coverage-matrix", response_model=CoverageMatrixResponse)
def get_coverage_matrix(
    venue: str | None = Query(None, pattern="^(blofin|binance)$"),
    connection_id: str | None = None,
    cur=Depends(get_cursor),
) -> CoverageMatrixResponse:
    """7×7 (or N×N) pairwise position-PnL correlation matrix + effective-N.

    Reads open positions from the latest snapshot, resolves binance_id
    per base symbol via market.symbols, and pulls 30d daily-close
    series for each (cached per-symbol per-day in `daily_closes:*`).
    Pairwise Pearson on dollar-PnL series (notional × return × dir_sign)
    gives the matrix; effective-N comes from the diversification-ratio
    formula (Σσ)² / (Σ_ij σᵢσⱼρᵢⱼ) — see correlation_cache.py header
    for why this deviates from §9a Note B's literal Herfindahl form.
    """
    venue, connection_id = _resolve_venue_connection(cur, venue, connection_id)
    cache_key = f"live:coverage-matrix:{venue}:{connection_id}"
    cached = _cache_get(cache_key)
    if cached:
        return CoverageMatrixResponse.model_validate(cached)

    snap = _read_latest_snapshot(cur, venue, connection_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {venue}/{connection_id}")

    open_positions: list[dict] = []
    for p in (snap["positions"] or []):
        sym = (p.get("symbol") or "").upper()
        size = float(p.get("size") or 0)
        if not sym or size == 0:
            continue
        sym_base = parse_symbol_base(sym)
        side = derive_position_side(p.get("side"), p.get("size") or 0)
        notional = float(p.get("notional_usd") or 0)
        # Default sort by notional desc so matrix rows track the
        # positions table / treemap order.
        open_positions.append({
            "symbol": sym,
            "symbol_base": sym_base,
            "side": side,
            "notional_usd": notional,
        })
    open_positions.sort(key=lambda p: -p["notional_usd"])

    bases = [p["symbol_base"] for p in open_positions]
    binance_ids = _resolve_binance_ids(cur, bases)

    def resolver(base: str) -> str | None:
        return binance_ids.get(base)

    client = BinanceMarketClient()
    result = get_coverage_matrix_cached(
        open_positions, client=client, binance_id_resolver=resolver,
    )

    rows = [CoverageRow(
        symbol=r["symbol"],
        symbol_base=r["symbol_base"],
        side=r["side"],
        notional_usd=r["notional_usd"],
        binance_symbol=r.get("binance_symbol"),
        has_history=r["has_history"],
        sigma_daily=r.get("sigma_daily"),
    ) for r in result["rows"]]

    response = CoverageMatrixResponse(
        venue=venue,
        connection_id=connection_id,
        snapshot_at=_iso(snap["snapshot_at"]),
        rows=rows,
        matrix=result["matrix"],
        tiers=result["tiers"],
        effective_n=result["effective_n"],
        diversification_benefit_pct=result["diversification_benefit_pct"],
        nominal_count=result["nominal_count"],
        reasons=result["reasons"],
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_COVERAGE_S)
    return response


@router.get("/factor-decomposition", response_model=FactorDecompositionResponse)
def get_factor_decomposition(
    venue: str | None = Query(None, pattern="^(blofin|binance)$"),
    connection_id: str | None = None,
    cur=Depends(get_cursor),
) -> FactorDecompositionResponse:
    """Multivariate ridge regression of each open position's 30d daily-PnL
    series against (BTC factor, ALT factor) plus an aggregated portfolio
    regression. Drives the §9b Factor Decomposition card.

    Reuses the per-symbol daily-closes cache populated by /coverage-matrix
    (`daily_closes:*`). Compute result is cached at 1H bar boundary in
    `factor_decomp:{position_set_hash}:{1h_bar_close_ts}` plus a 60s thin
    response cache for frontend polling.
    """
    venue, connection_id = _resolve_venue_connection(cur, venue, connection_id)
    cache_key = f"live:factor-decomposition:{venue}:{connection_id}"
    cached = _cache_get(cache_key)
    if cached:
        return FactorDecompositionResponse.model_validate(cached)

    snap = _read_latest_snapshot(cur, venue, connection_id)
    if not snap:
        raise HTTPException(404, f"No snapshot for {venue}/{connection_id}")

    open_positions: list[dict] = []
    for p in (snap["positions"] or []):
        sym = (p.get("symbol") or "").upper()
        size = float(p.get("size") or 0)
        if not sym or size == 0:
            continue
        sym_base = parse_symbol_base(sym)
        side = derive_position_side(p.get("side"), p.get("size") or 0)
        notional = float(p.get("notional_usd") or 0)
        open_positions.append({
            "symbol": sym,
            "symbol_base": sym_base,
            "side": side,
            "notional_usd": notional,
        })
    open_positions.sort(key=lambda p: -p["notional_usd"])

    bases = [p["symbol_base"] for p in open_positions]
    binance_ids = _resolve_binance_ids(cur, bases)

    def resolver(base: str) -> str | None:
        return binance_ids.get(base)

    client = BinanceMarketClient()
    result = get_factor_decomposition_cached(
        open_positions, client=client, binance_id_resolver=resolver,
    )

    pos_rows = [FactorPositionRow(
        symbol=r["symbol"],
        symbol_base=r["symbol_base"],
        side=r["side"],
        notional_usd=r["notional_usd"],
        has_history=r["has_history"],
        beta_btc=r.get("beta_btc"),
        beta_alt=r.get("beta_alt"),
        var_pct=FactorVarPct(
            btc=r["var_pct"]["btc"],
            alt=r["var_pct"]["alt"],
            idio=r["var_pct"]["idio"],
        ),
    ) for r in result["positions"]]

    portfolio_dict = result.get("portfolio")
    portfolio = (
        FactorPortfolio(
            beta_btc=portfolio_dict["beta_btc"],
            beta_alt=portfolio_dict["beta_alt"],
            var_btc_pct=portfolio_dict["var_btc_pct"],
            var_alt_pct=portfolio_dict["var_alt_pct"],
            var_idio_pct=portfolio_dict["var_idio_pct"],
            sigma_silo_usd=portfolio_dict["sigma_silo_usd"],
            sigma_portfolio_usd=portfolio_dict["sigma_portfolio_usd"],
            diversification_benefit_pct=portfolio_dict["diversification_benefit_pct"],
        ) if portfolio_dict else None
    )

    response = FactorDecompositionResponse(
        venue=venue,
        connection_id=connection_id,
        snapshot_at=_iso(snap["snapshot_at"]),
        positions=pos_rows,
        portfolio=portfolio,
        alt_index_member_count=result["alt_index_member_count"],
        alt_index_target_count=result["alt_index_target_count"],
        n_days=result["n_days"],
        reasons=result["reasons"],
    )
    _cache_set(cache_key, response.model_dump(), ttl=CACHE_TTL_FACTOR_DECOMP_S)
    return response

"""
backend/app/services/manager_live_state.py
=============================================
Shared helpers for the Manager Live + Positions read paths.

This module is the **single source of truth** for two concerns that
otherwise drift between endpoints:

  1. BloFin null-field derivation. BloFin's /api/v1/account/balance
     returns null for `frozenBal` (used margin) and `upl` (account-
     level unrealized PnL) on some account modes. Both endpoints
     ( /api/manager/positions and /api/manager/live/* ) must derive
     identically, otherwise the same connection produces different
     numbers in two places.

  2. Source attribution per Data Dictionary §14. Each open position is
     tagged 'strategy' (symbol is in the latest is_selected=TRUE basket
     for an active allocation on that connection) or 'manual'. v1 reads
     directly from daily_signals + allocations; once the
     position_history writer ships, that table becomes the cache and
     this helper reads from there first.

Net-mode side derivation (BloFin returns positionSide='net' for net-
mode accounts) is also covered here so the same convention is used by
every consumer.

NOTE: position_history is the eventual home for source attribution
once the writer lands. Until then, this helper computes it on every
read by joining daily_signals — at which point we'll switch the
strategy_symbol_map() function to read position_history first and fall
back to daily_signals for historical positions.
"""

from __future__ import annotations

from typing import Any


# ── BloFin null-field derivation ──────────────────────────────────────────

def derive_used_margin(*, total_equity: float, available: float, raw_used_margin: float) -> float:
    """Used margin in USD. Prefer the exchange-reported value when
    non-zero; otherwise fall back to (equity − available), which is what
    the percentage view already shows.

    BloFin's `frozenBal` is the available-equity reservation, so this
    fallback is an upper-bound estimate. Worst case: cross-margin float
    counts toward the estimate even when it isn't strictly "used" — but
    the mismatch is typically <1% of equity, well within the precision
    of the KPI strip's 2-decimal display.
    """
    if raw_used_margin and raw_used_margin > 0:
        return raw_used_margin
    return max(total_equity - available, 0.0)


def derive_unrealized_pnl(*, raw_unrealized_pnl: float, positions: list[dict]) -> float:
    """Account-level unrealized PnL in USD. Prefer the exchange-reported
    field when non-zero; otherwise sum per-position upl.

    BloFin returns `upl=0.0` (or null) on some account modes, even when
    individual positions show non-zero upl. Summing per-position is the
    authoritative fallback — the exchange's own "totals at the bottom
    of the page" view is the same calculation.
    """
    if raw_unrealized_pnl and raw_unrealized_pnl != 0:
        return raw_unrealized_pnl
    return sum(float(p.get("unrealized_pnl") or 0) for p in (positions or []))


# ── Net-mode side derivation ──────────────────────────────────────────────

def derive_position_side(raw_side: str | None, size: float) -> str:
    """Return 'long' or 'short' for a position. BloFin net-mode accounts
    report positionSide='net'; the sign of `size` carries the direction
    (positive=long, negative=short). Hedge-mode accounts already give
    'long'/'short' explicitly."""
    s = (raw_side or "").lower()
    if s in ("net", ""):
        return "long" if (size or 0) >= 0 else "short"
    return s


# ── Symbol normalization ──────────────────────────────────────────────────

def parse_symbol_base(symbol: str | None) -> str:
    """Strip USDT quote suffix to get the base symbol.

    Matches BloFin storage form ("BTCUSDT" — already dash-stripped at
    snapshot-write time) and Binance ("BTCUSDT"). Caller passes the
    upper-cased symbol; we don't re-upper here so the caller controls
    casing for downstream joins.
    """
    if not symbol:
        return ""
    sym = symbol.upper()
    return sym[:-4] if sym.endswith("USDT") else sym


# ── Source attribution map ────────────────────────────────────────────────

def build_strategy_symbol_map(cur, connection_ids: list) -> dict[str, dict[str, str]]:
    """Build {connection_id_str: {SYMBOL_BASE: strategy_display_name}} for
    source attribution per Data Dictionary §14.

    Looks up:
      - Active allocations on each connection (status='active')
      - Latest daily_signals batch per allocation's strategy_version
      - is_selected=TRUE items in that batch — those are the symbols the
        strategy actually deployed (vs alternates the writer recorded
        for audit)

    For v1, position_history is empty so we always re-derive on read.
    Once position_history has a writer, this function should be
    refactored to read source from position_history first and fall
    back to this lookup only for positions without a history row.
    """
    if not connection_ids:
        return {}

    cur.execute("""
        SELECT a.connection_id, a.strategy_version_id,
               s.display_name AS strategy_display_name, s.name AS strategy_name
          FROM user_mgmt.allocations a
          JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
          JOIN audit.strategies s ON s.strategy_id = sv.strategy_id
         WHERE a.connection_id = ANY(%s::uuid[])
           AND a.status = 'active'
    """, (list(connection_ids),))
    alloc_rows = cur.fetchall()
    if not alloc_rows:
        return {}

    version_ids = list({a["strategy_version_id"] for a in alloc_rows})
    cur.execute("""
        SELECT DISTINCT ON (ds.strategy_version_id)
               ds.strategy_version_id, ds.signal_batch_id
          FROM user_mgmt.daily_signals ds
         WHERE ds.strategy_version_id = ANY(%s::uuid[])
         ORDER BY ds.strategy_version_id, ds.signal_date DESC
    """, (version_ids,))
    sig_rows = cur.fetchall()
    version_to_batch = {
        str(r["strategy_version_id"]): str(r["signal_batch_id"]) for r in sig_rows
    }

    sym_by_batch: dict[str, list[str]] = {}
    batch_ids = [r["signal_batch_id"] for r in sig_rows]
    if batch_ids:
        # is_selected=TRUE filters out alternates the writer recorded for
        # audit but didn't actually deploy. A position on a non-selected
        # symbol is genuinely manual.
        cur.execute("""
            SELECT dsi.signal_batch_id, sym.base AS symbol
              FROM user_mgmt.daily_signal_items dsi
              JOIN market.symbols sym ON sym.symbol_id = dsi.symbol_id
             WHERE dsi.signal_batch_id = ANY(%s::uuid[])
               AND dsi.is_selected = TRUE
        """, (batch_ids,))
        for row in cur.fetchall():
            sym_by_batch.setdefault(str(row["signal_batch_id"]), []).append(row["symbol"])

    result: dict[str, dict[str, str]] = {}
    for a in alloc_rows:
        cid = str(a["connection_id"])
        vid = str(a["strategy_version_id"])
        bid = version_to_batch.get(vid)
        if not bid:
            continue
        strat_name = a["strategy_display_name"] or a["strategy_name"]
        for sym in sym_by_batch.get(bid, []):
            result.setdefault(cid, {})[sym.upper()] = strat_name
    return result


# ── Position normalization ────────────────────────────────────────────────

def normalize_position(
    pos: dict,
    *,
    connection_id: str,
    venue: str,
    connection_label: str | None,
    strategy_symbol_map: dict[str, str],
) -> dict[str, Any]:
    """Apply net-mode side derivation, base-symbol parsing, and source
    attribution to a single position dict. Returns a flat dict suitable
    for both /api/manager/positions and the Live endpoint shapes."""
    sym_full = (pos.get("symbol") or "").upper()
    sym_base = parse_symbol_base(sym_full)
    raw_side = (pos.get("side") or "").lower()
    size = pos.get("size") or 0
    side = derive_position_side(raw_side, size)
    notional = float(pos.get("notional_usd") or 0)
    strat_name = strategy_symbol_map.get(sym_base)

    return {
        "connection_id": connection_id,
        "venue": venue,
        "connection_label": connection_label,
        "symbol": sym_full,
        "symbol_base": sym_base,
        "side": side,
        "size": abs(size),
        "entry_price": pos.get("entry_price"),
        "mark_price": pos.get("mark_price"),
        "unrealized_pnl_usd": float(pos.get("unrealized_pnl") or 0),
        "notional_usd": notional,
        "leverage": pos.get("leverage"),
        "margin_mode": pos.get("margin_mode"),
        "source": "strategy" if strat_name else "manual",
        "strategy_name": strat_name,
    }

"""Auto-generate preview portfolio_sessions for sit-flat allocations.

Cron entry point. Runs every 5 minutes from ~06:35 UTC onward. For each
active allocation whose trader has decided to sit out today (filtered,
no_entry_conviction, missed_window), maintains a synthetic portfolio_
sessions row + portfolio_bars timeline so the manager UI can render the
candidate basket's intraday performance just like a real session.

This unifies the operator workflow:
  - PASSED days  → real portfolio data (trader writes)
  - NO ENTRY days → synthetic preview (this script writes)
  - Either path → late-entry button override available

The script is idempotent: ON CONFLICT DO NOTHING on portfolio_bars so
re-runs only insert new 5-min bars. portfolio_sessions UPSERTs metadata
(symbols/entered/sym_stops) each run so per-symbol stop transitions are
captured as Binance prices roll forward.

Marker: exit_reason='preview_no_entry' — distinct from manual
'preview_late_entry' so cleanup can target only auto-generated rows.
The frontend's late-entry button condition checks for either.
"""
from __future__ import annotations

import calendar
import json
import sys
import uuid
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, "/app/backend")
from app.db import get_worker_conn

DEPLOYS_CSV = Path("/host_trader/live_deploys_signal.csv")
BINANCE_BASE = "https://fapi.binance.com"
ENTRY_REF_HOUR = 6
ENTRY_REF_MIN = 35
SESSION_END_HOUR = 23
SESSION_END_MIN = 55

# Stop threshold (per-symbol). Currently hardcoded at -6% to match every
# active strategy version's stop_raw_pct. If a strategy with a different
# threshold is deployed, this should read from sv.config.stop_raw_pct.
STOP_THRESHOLD = -0.06


def _kline_close_at(binance_sym: str, target_dt: datetime) -> float | None:
    """Return Binance perp 1m close at target_dt, or None on failure."""
    ts_ms = calendar.timegm(target_dt.timetuple()) * 1000
    try:
        r = requests.get(
            f"{BINANCE_BASE}/fapi/v1/klines",
            params={
                "symbol": binance_sym,
                "interval": "1m",
                "startTime": ts_ms - 60_000,
                "endTime": ts_ms + 60_000,
                "limit": 3,
            },
            timeout=10,
        )
        r.raise_for_status()
        bars = r.json()
        if not bars:
            return None
        return float(min(bars, key=lambda b: abs(b[0] - ts_ms))[4])
    except Exception:
        return None


def _klines_5m(binance_sym: str, start_dt: datetime, end_dt: datetime) -> list:
    """Return Binance perp 5m (open_ts_ms, close) tuples."""
    ts0 = calendar.timegm(start_dt.timetuple()) * 1000
    ts1 = calendar.timegm(end_dt.timetuple()) * 1000
    try:
        r = requests.get(
            f"{BINANCE_BASE}/fapi/v1/klines",
            params={
                "symbol": binance_sym,
                "interval": "5m",
                "startTime": ts0,
                "endTime": ts1,
                "limit": 200,
            },
            timeout=10,
        )
        r.raise_for_status()
        bars = r.json()
        return [(int(b[0]), float(b[4])) for b in bars] if bars else []
    except Exception:
        return []


def _read_csv_basket(date_str: str, filter_name: str) -> tuple[list[str], bool]:
    """Read live_deploys_signal.csv for a (date, filter) pair. Returns
    (symbols, sit_flat). Empty list when no matching row."""
    if not DEPLOYS_CSV.exists():
        return [], False
    import csv as _csv
    with open(DEPLOYS_CSV, newline="") as f:
        for row in _csv.DictReader(f):
            if row.get("date", "").strip() != date_str:
                continue
            if row.get("filter", "").strip().lower() != filter_name.strip().lower():
                continue
            symbols_str = row.get("symbols", "").strip()
            sit_flat = str(row.get("sit_flat", "")).strip().lower() == "true"
            symbols = [s.strip() for s in symbols_str.replace(",", " ").split() if s.strip()]
            return symbols, sit_flat
    return [], False


def _eligible_allocations() -> list[dict]:
    """Active allocations whose today is filtered/no_entry/missed_window."""
    today = datetime.now(timezone.utc).date()
    conn = get_worker_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT a.allocation_id::text,
                   a.runtime_state,
                   sv.config->>'active_filter' AS active_filter,
                   sv.config->>'l_high'        AS l_high
              FROM user_mgmt.allocations a
              JOIN audit.strategy_versions sv ON sv.strategy_version_id = a.strategy_version_id
             WHERE a.status = 'active'
            """
        )
        out = []
        for r in cur.fetchall():
            state = r[1] or {}
            if not isinstance(state, dict):
                continue
            if state.get("date") != today.isoformat():
                continue
            phase = state.get("phase")
            # Trader hasn't entered: any of these phases means we should
            # generate a preview.
            if phase not in ("filtered", "no_entry", "missed_window"):
                continue
            af = (r[2] or "").strip()
            # Strip "A - " / "B - " label prefixes the trader normalizes
            for prefix in ("A - ", "B - ", "C - ", "D - "):
                if af.startswith(prefix):
                    af = af[len(prefix):]
                    break
            out.append({
                "allocation_id": r[0],
                "filter_name":   af,
                "l_high":        float(r[3]) if r[3] else 1.0,
            })
        return out
    finally:
        conn.close()


def _binance_ticker_for(inst_id: str) -> str:
    """ENSO-USDT → ENSOUSDT."""
    return inst_id.replace("-", "")


def _existing_session_id(conn, allocation_id: str, signal_date: str) -> str | None:
    """Return preview portfolio_session_id if it already exists."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT portfolio_session_id::text
              FROM user_mgmt.portfolio_sessions
             WHERE allocation_id = %s::uuid
               AND signal_date  = %s::date
            """,
            (allocation_id, signal_date),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def _build_preview(allocation_id: str, basket: list[str], filter_name: str, l_high: float) -> int:
    """Build/update preview session + bars for one allocation. Returns
    bars-inserted count."""
    today = datetime.now(timezone.utc).date()
    today_str = today.isoformat()
    entry_ref = datetime.combine(today, dtime(ENTRY_REF_HOUR, ENTRY_REF_MIN, 0), tzinfo=timezone.utc)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if now < entry_ref:
        return 0  # Too early, entry ref not yet set

    inst_ids = [f"{s}-USDT" for s in basket]
    binance_syms = {f"{s}-USDT": _binance_ticker_for(f"{s}-USDT") for s in basket}

    # Fetch entry-ref prices (one per symbol). Skip symbols Binance can't price.
    entry_prices: dict[str, float] = {}
    for inst, bsym in binance_syms.items():
        p = _kline_close_at(bsym, entry_ref)
        if p is not None:
            entry_prices[inst] = p
    if not entry_prices:
        print(f"  alloc {allocation_id[:8]}: no Binance prices for any basket symbol, skipping")
        return 0

    # Pull 5m klines through current time per symbol
    series: dict[str, list[tuple[int, float]]] = {}
    for inst in entry_prices:
        bsym = binance_syms[inst]
        bars = _klines_5m(bsym, entry_ref, now)
        if bars:
            series[inst] = bars

    # Build per-bar timeline. Stop tracking per inst_id when its return
    # crosses STOP_THRESHOLD; subsequent bars hold the observed clamp.
    all_ts = sorted({t for s in series.values() for (t, _) in s})
    stopped_at: dict[str, int] = {}
    clamp_value: dict[str, float] = {}

    bars_payload: list[dict] = []
    for ts in all_ts:
        bar_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        # Bar number from session_start (00:00 UTC) at 5-min intervals.
        # Match the trader's numbering: bar 7 = 06:35 entry ref.
        offset_min = (bar_dt - entry_ref).total_seconds() / 60.0
        bar_number = 7 + int(offset_min / 5)

        sym_returns: dict[str, float] = {}
        stopped_now: list[str] = []
        for inst, bars in series.items():
            match = next((p for (t, p) in bars if t == ts), None)
            if match is None:
                continue
            entry_p = entry_prices[inst]
            if entry_p <= 0:
                continue
            raw_ret = (match - entry_p) / entry_p
            if inst in stopped_at:
                sym_returns[inst] = clamp_value[inst]
                stopped_now.append(inst)
            elif raw_ret <= STOP_THRESHOLD:
                stopped_at[inst] = bar_number
                clamp_value[inst] = raw_ret
                sym_returns[inst] = raw_ret
                stopped_now.append(inst)
            else:
                sym_returns[inst] = raw_ret

        if not sym_returns:
            continue
        portfolio_ret = sum(sym_returns.values()) / len(sym_returns)
        bars_payload.append({
            "bar_number": bar_number,
            "bar_dt":     bar_dt,
            "portfolio_return": portfolio_ret,
            "sym_returns": sym_returns,
            "stopped":     stopped_now,
        })

    # Compute running peak
    running_peak = 0.0
    for b in bars_payload:
        running_peak = max(running_peak, b["portfolio_return"])
        b["peak_return"] = running_peak

    final_ret = bars_payload[-1]["portfolio_return"] if bars_payload else 0.0
    peak_ret = max((b["portfolio_return"] for b in bars_payload), default=0.0)
    running_peak = 0.0
    max_dd = 0.0
    for b in bars_payload:
        running_peak = max(running_peak, b["portfolio_return"])
        dd = b["portfolio_return"] - running_peak
        if dd < max_dd:
            max_dd = dd
    stopped_full = [inst for inst, _ in stopped_at.items()]

    # UPSERT session + INSERT bars (idempotent)
    conn = get_worker_conn()
    try:
        cur = conn.cursor()
        existing = _existing_session_id(conn, allocation_id, today_str)
        if existing:
            session_id = existing
            cur.execute(
                """
                UPDATE user_mgmt.portfolio_sessions
                   SET symbols = %s, entered = %s,
                       bars_count = %s,
                       final_portfolio_return = %s,
                       peak_portfolio_return  = %s,
                       max_dd_from_peak       = %s,
                       sym_stops              = %s,
                       updated_at             = NOW()
                 WHERE portfolio_session_id = %s::uuid
                   AND exit_reason = 'preview_no_entry'
                """,
                (inst_ids, [], len(bars_payload),
                 final_ret, peak_ret, max_dd,
                 stopped_full, session_id),
            )
        else:
            session_id = str(uuid.uuid4())
            # Use 4x for eff_lev as a placeholder — preview rows aren't
            # tied to actual trader leverage, but the matrix uses lev_int
            # for some labels.
            cur.execute(
                """
                INSERT INTO user_mgmt.portfolio_sessions
                    (portfolio_session_id, allocation_id, signal_date,
                     session_start_utc, status, exit_reason,
                     symbols, entered, eff_lev, lev_int,
                     bars_count, final_portfolio_return,
                     peak_portfolio_return, max_dd_from_peak,
                     sym_stops, created_at, updated_at)
                VALUES (%s::uuid, %s::uuid, %s::date, %s,
                        'active', 'preview_no_entry',
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, NOW(), NOW())
                """,
                (session_id, allocation_id, today_str, entry_ref,
                 inst_ids, [], float(l_high * 2.0), int(round(l_high * 2.0)),
                 len(bars_payload), final_ret, peak_ret, max_dd,
                 stopped_full),
            )

        # Insert bars (skip if exists)
        inserted = 0
        for b in bars_payload:
            cur.execute(
                """
                INSERT INTO user_mgmt.portfolio_bars
                    (portfolio_session_id, bar_number, bar_timestamp_utc,
                     portfolio_return, peak_return, symbol_returns,
                     stopped, logged_at)
                VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (portfolio_session_id, bar_number) DO NOTHING
                """,
                (session_id, b["bar_number"], b["bar_dt"],
                 b["portfolio_return"], b["peak_return"],
                 json.dumps(b["sym_returns"]), b["stopped"]),
            )
            if cur.rowcount > 0:
                inserted += 1

        conn.commit()
        cur.close()
        return inserted
    except Exception as e:
        conn.rollback()
        print(f"  alloc {allocation_id[:8]}: write failed: {e}")
        return 0
    finally:
        conn.close()


def main() -> int:
    today = datetime.now(timezone.utc).date()
    print(f"=== generate_no_entry_previews {today.isoformat()} {datetime.now(timezone.utc):%H:%M:%S} UTC ===")
    eligibles = _eligible_allocations()
    print(f"eligible no-entry allocations: {len(eligibles)}")
    if not eligibles:
        return 0
    for alloc in eligibles:
        basket, sit_flat = _read_csv_basket(today.isoformat(), alloc["filter_name"])
        if not basket:
            print(f"  alloc {alloc['allocation_id'][:8]} ({alloc['filter_name']}): empty basket, skipping")
            continue
        n = _build_preview(
            allocation_id=alloc["allocation_id"],
            basket=basket,
            filter_name=alloc["filter_name"],
            l_high=alloc["l_high"],
        )
        print(f"  alloc {alloc['allocation_id'][:8]} ({alloc['filter_name']}): {len(basket)} symbols, +{n} bars")
    return 0


if __name__ == "__main__":
    sys.exit(main())

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
# Log dir for the session-logs panel — same path the live trader writes to,
# read by /api/manager/execution-logs. Each (allocation, date) gets its own
# file. Preview lines append to the existing file alongside the trader's
# own spawn/filter messages, so the panel reads as one continuous session.
# Inside the celery/backend containers this is bind-mounted at the same path.
TRADER_LOG_DIR = Path("/mnt/quant-data/logs/trader")
# Default deploy ratio used by the live trader (10% margin buffer holds
# 90% of capital in leveraged positions). expected_roi = incr × lev × 0.90.
DEPLOY_RATIO = 0.90
# Preview's actual_roi mirrors expected_roi — there's no real account
# equity to read since no positions opened. Setting actual = expected
# means the delta column reads as 0, which is the correct semantic for
# "if we had traded this basket, we'd be at the model-predicted P&L."

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


def _existing_logged_bars(log_path: Path) -> set[int]:
    """Scan an allocation log file for already-emitted preview bar lines.
    Idempotency guard: re-running the cron won't duplicate lines for bars
    we already logged. Looks for "Bar  N |" anchored at any depth past the
    timestamp+level prefix; trader-spawn block doesn't contain those, so
    matching in the body alone is safe."""
    if not log_path.exists():
        return set()
    seen: set[int] = set()
    try:
        with log_path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                # Cheap match — the BAR_UPDATE_RE is anchored after a
                # potential alloc prefix; we just need any bar number.
                idx = line.find("Bar ")
                if idx < 0:
                    continue
                rest = line[idx + 4:].lstrip()
                # bar number is up to first space or pipe
                end = 0
                while end < len(rest) and rest[end].isdigit():
                    end += 1
                if end == 0:
                    continue
                try:
                    seen.add(int(rest[:end]))
                except ValueError:
                    continue
    except Exception:
        # File-read failures shouldn't block bar generation; treat as
        # "no bars logged yet" so we re-emit (worst case = a few dupes
        # parsed identically by the frontend).
        return set()
    return seen


def _write_preview_log_lines(
    *,
    allocation_id: str,
    today_str: str,
    bars_payload: list[dict],
    inst_ids: list[str],
    capital_usd: float,
    eff_lev: float,
    fill_gate_dt: datetime,
) -> int:
    """Append trader-format log lines for each preview bar to the
    per-allocation log file at /mnt/quant-data/logs/trader/.

    The session-logs UI panel reads this file via the
    /api/manager/execution-logs endpoint and parses lines into typed
    events (`bar_update`, `roi_report`). Without these lines the panel
    on a sit-flat day shows only the 06:05 filter-decision messages
    even though portfolio_bars has 50+ rows of preview data.

    Two lines per bar — same shape the live trader emits, so the
    parser regexes (_BAR_UPDATE_RE / _ROI_REPORT_RE) classify them
    identically:
        2026-04-27 06:35:00,000 [INFO] [alloc XXXXXXXX] Bar   7 | incr=...
        2026-04-27 06:35:00,000 [INFO] [alloc XXXXXXXX]         | actual_roi=...

    Idempotent: skips bars already present in the file.
    """
    log_path = TRADER_LOG_DIR / f"allocation_{allocation_id}_{today_str}.log"
    already_logged = _existing_logged_bars(log_path)
    new_bars = [b for b in bars_payload if b["bar_number"] not in already_logged]
    if not new_bars:
        return 0

    # Trader's log_prefix uses the first 8 hex chars of the alloc id —
    # mirror that so the parser's _ALLOC_PREFIX_RE matches identically.
    short_alloc = allocation_id.split("-")[0][:8]
    n_universe = len(inst_ids)
    lines: list[str] = []
    for b in new_bars:
        bar_n  = b["bar_number"]
        ts_str = b["bar_dt"].strftime("%Y-%m-%d %H:%M:%S,000")
        incr   = float(b["portfolio_return"])
        peak   = float(b["peak_return"])
        tsl    = incr - peak
        sess   = incr  # open-anchored already; same as portfolio_return
        n_stop = len(b["stopped"])
        n_act  = n_universe - n_stop
        fill   = "open" if b["bar_dt"] <= fill_gate_dt else "closed"
        # Set actual = expected so the delta reads 0. Equity/pnl
        # computed from actual so the snapshot tile shows non-zero
        # numbers consistent with the model.
        expected = incr * eff_lev * DEPLOY_RATIO
        actual   = expected
        equity   = capital_usd * (1.0 + actual)
        pnl      = capital_usd * actual
        # Bar update line (incr/peak summary).
        lines.append(
            f"{ts_str} [INFO] [alloc {short_alloc}] "
            f"Bar {bar_n:3d} | incr={incr*100:+.3f}%  peak={peak*100:.3f}%  "
            f"tsl={tsl*100:+.3f}%  sess={sess*100:+.3f}%  "
            f"active={n_act}/{n_universe}  stopped={n_stop}  "
            f"fill={fill}\n"
        )
        # ROI report continuation line. Leading spaces + "|" — the
        # parser's _ROI_REPORT_RE anchors on the pipe (whitespace
        # absorbed by lstrip after alloc-prefix strip).
        pnl_sign = "+" if pnl >= 0 else "-"
        lines.append(
            f"{ts_str} [INFO] [alloc {short_alloc}]         "
            f"| actual_roi={actual*100:+.3f}%  "
            f"equity=${equity:,.2f}  "
            f"pnl=${pnl_sign}{abs(pnl):,.2f}  "
            f"expected={expected*100:+.3f}% (incr×lev×0.90)  "
            f"delta=+0.000%\n"
        )

    # Append (create-if-missing). Mode 'a' is the standard concurrent-
    # safe append on POSIX for short writes; cron runs single-threaded
    # so contention isn't a concern.
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        print(f"  alloc {allocation_id[:8]}: log-line append failed: {e}")
        return 0
    return len(new_bars)


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
    # Each run re-detects stops from live kline data — no seeding from
    # existing portfolio_bars. Tried that earlier (cross-run state for
    # cases where Binance is flaky on the stop-detection bar), but the
    # seed picked up stale clamp values written by buggy prior runs and
    # mis-attributed the stop bar. Live klines are authoritative;
    # missing-bar fallback is handled by within-run carry-forward
    # (last_observed_ret) below — sufficient for transient API hiccups.
    stopped_at: dict[str, int] = {}
    clamp_value: dict[str, float] = {}
    last_observed_ret: dict[str, float] = {}

    bars_payload: list[dict] = []
    for ts in all_ts:
        bar_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        # Bar number from session_start (00:00 UTC) at 5-min intervals.
        # Match the trader's numbering: bar 7 = 06:35 entry ref.
        offset_min = (bar_dt - entry_ref).total_seconds() / 60.0
        bar_number = 7 + int(offset_min / 5)

        sym_returns: dict[str, float] = {}
        stopped_now: list[str] = []
        # Iterate over the FULL inst_id list (not just series.keys()) so
        # symbols whose entire kline series is missing this run still
        # get their clamp/last-known value emitted. Without this, a
        # symbol that had Binance data yesterday but not today drops
        # out of the matrix entirely — even though we know it's
        # stopped from the seeded state above.
        for inst in inst_ids:
            entry_p = entry_prices.get(inst, 0)
            bars = series.get(inst, [])
            match = next((p for (t, p) in bars if t == ts), None) if bars else None

            # Stopped: emit clamp ONLY for bars at or after the stop
            # crossing. The seeded stopped_at[inst] holds the bar
            # number where the symbol first crossed -6%; bars before
            # that should still show their real pre-stop returns
            # (otherwise the matrix retroactively shows -7.21% at
            # 06:35 even though NOT didn't actually stop until 06:45).
            if inst in stopped_at and bar_number >= stopped_at[inst]:
                sym_returns[inst] = clamp_value[inst]
                stopped_now.append(inst)
                continue

            # Active with live kline data — compute fresh return.
            if match is not None and entry_p > 0:
                raw_ret = (match - entry_p) / entry_p
                # If raw_ret crosses threshold AND we don't already
                # have an earlier stop bar recorded, this is the new
                # stop-crossing bar. (The seeded value can be later
                # than the actual first crossing if a prior run's
                # kline-fetch missed the real first-cross bar.)
                if raw_ret <= STOP_THRESHOLD and (
                    inst not in stopped_at or bar_number <= stopped_at[inst]
                ):
                    stopped_at[inst] = bar_number
                    clamp_value[inst] = raw_ret
                    sym_returns[inst] = raw_ret
                    stopped_now.append(inst)
                else:
                    sym_returns[inst] = raw_ret
                last_observed_ret[inst] = sym_returns[inst]
                continue

            # No live data this bar — fall back to last-known. Keeps
            # the dict dense so the matrix doesn't have "—" gaps and
            # the portfolio mean isn't computed off a partial subset.
            if inst in last_observed_ret:
                sym_returns[inst] = last_observed_ret[inst]

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

        # Upsert bars. Was DO NOTHING; switched to DO UPDATE so re-runs
        # heal stale sparse-dict rows from the pre-fix code path. The
        # carry-forward + cumulative state above is the source of truth
        # — any prior row's symbol_returns/stopped should be overwritten
        # with the dense version. Cheap: same row count, same indexes,
        # and rowcount=1 fires for both new + updated so the "+0 bars"
        # log line stops being misleading.
        inserted = 0
        for b in bars_payload:
            cur.execute(
                """
                INSERT INTO user_mgmt.portfolio_bars
                    (portfolio_session_id, bar_number, bar_timestamp_utc,
                     portfolio_return, peak_return, symbol_returns,
                     stopped, logged_at)
                VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb, %s, NOW())
                ON CONFLICT (portfolio_session_id, bar_number) DO UPDATE
                   SET portfolio_return = EXCLUDED.portfolio_return,
                       peak_return      = EXCLUDED.peak_return,
                       symbol_returns   = EXCLUDED.symbol_returns,
                       stopped          = EXCLUDED.stopped,
                       logged_at        = NOW()
                """,
                (session_id, b["bar_number"], b["bar_dt"],
                 b["portfolio_return"], b["peak_return"],
                 json.dumps(b["sym_returns"]), b["stopped"]),
            )
            if cur.rowcount > 0:
                inserted += 1

        conn.commit()
        cur.close()

        # Append trader-format log lines so the session-logs panel
        # renders bar_update + roi_report events for the preview, not
        # just the morning's filter-decision message. Idempotent;
        # already-logged bars are skipped.
        try:
            cur2 = conn.cursor()
            cur2.execute(
                """
                SELECT capital_usd, sv.config->>'early_fill_x'
                  FROM user_mgmt.allocations a
                  JOIN audit.strategy_versions sv
                    ON sv.strategy_version_id = a.strategy_version_id
                 WHERE a.allocation_id = %s::uuid
                """,
                (allocation_id,),
            )
            row = cur2.fetchone()
            cur2.close()
            cap_usd = float(row[0]) if row and row[0] is not None else 0.0
            efx_min = int(row[1]) if row and row[1] not in (None, "") else 90
            session_open_dt = datetime.combine(
                today, dtime(0, 0, 0), tzinfo=timezone.utc,
            ).replace(hour=ENTRY_REF_HOUR - 0)  # session open = entry_ref hour
            # Trader uses session_open + early_fill_x (in minutes) as
            # the fill_gate. Mirror that here so the fill=open|closed
            # column reads the same on previews as it would live.
            session_open_dt = entry_ref.replace(
                hour=entry_ref.hour, minute=0, second=0, microsecond=0,
            )
            from datetime import timedelta as _td
            fill_gate_dt = session_open_dt + _td(minutes=efx_min)
            eff_lev = float(l_high * 2.0)
            n_logged = _write_preview_log_lines(
                allocation_id=allocation_id,
                today_str=today_str,
                bars_payload=bars_payload,
                inst_ids=inst_ids,
                capital_usd=cap_usd,
                eff_lev=eff_lev,
                fill_gate_dt=fill_gate_dt,
            )
            if n_logged:
                print(f"  alloc {allocation_id[:8]}: appended {n_logged} log line-pairs")
        except Exception as _e:
            # Best-effort — DB rows are the source of truth; log lines
            # are the cosmetic layer.
            print(f"  alloc {allocation_id[:8]}: log-line write skipped: {_e}")

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

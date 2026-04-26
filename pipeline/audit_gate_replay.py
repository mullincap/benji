#!/usr/bin/env python3
"""
audit_gate_replay.py — N-day backfill diff between audit verifier and live.

For each date in the window, recomputes the audit-canonical gate decision
using pipeline.audit_filters (the same code paths the verifier uses at
06:01 UTC) and diffs against what daily_signal_v2 wrote to
user_mgmt.daily_signals on that date.

Purpose: validate, before the trader ever gates on audit_status, that
the audit and live paths agree historically. Expected outcome on a
healthy codebase: zero mismatches.

Read-only — never writes to daily_signals.

Usage
-----
  python pipeline/audit_gate_replay.py --days 30           # last 30 days
  python pipeline/audit_gate_replay.py --start 2026-03-27 --end 2026-04-25
  python pipeline/audit_gate_replay.py --days 30 --json    # machine-readable

Runtime: each day takes 30-60s (compute_canonical_basket fetches klines +
OI for ~400 perps; compute_dispersion_filter fetches 33d of 1d klines).
30-day window ≈ 15-30 minutes. Run on the prod host (DB credentials +
Binance reachability assumed).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPT_DIR.parent))  # project root

from pipeline.intraday_audit import (  # noqa: E402
    _compute_audit_decisions,
    _diff,
    _fetch_live_rows,
)

logging.basicConfig(
    level=logging.WARNING,  # quiet by default — replay prints its own report
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("audit_gate_replay")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Replay the audit verifier across past dates and diff "
                    "against user_mgmt.daily_signals."
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--days", type=int, default=30,
                   help="Number of trailing days, ending yesterday UTC "
                        "(default: 30).")
    g.add_argument("--start", type=str, default=None,
                   help="Start date YYYY-MM-DD (inclusive).")
    p.add_argument("--end", type=str, default=None,
                   help="End date YYYY-MM-DD (inclusive). Required with --start.")
    p.add_argument("--json", action="store_true",
                   help="Emit summary as JSON instead of human-readable.")
    p.add_argument("--stop-on-mismatch", action="store_true",
                   help="Halt at the first mismatch (faster diagnostic loop).")
    return p.parse_args()


def _resolve_window(args: argparse.Namespace) -> list[dt.date]:
    today = dt.datetime.now(dt.timezone.utc).date()
    if args.start:
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end) if args.end else today - dt.timedelta(days=1)
    else:
        end = today - dt.timedelta(days=1)
        start = end - dt.timedelta(days=args.days - 1)
    if start > end:
        raise ValueError(f"start ({start}) > end ({end})")
    return [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]


def _replay_date(ref_date: dt.date) -> dict:
    """Recompute + diff one date. Returns:
        {date, dates_with_no_basket, n_strategies, mismatches: [...]}
    """
    out = {
        "date": ref_date.isoformat(),
        "n_strategies": 0,
        "n_live_rows": 0,
        "mismatches": [],
        "skipped_reason": None,
    }
    try:
        _basket, audit_decisions = _compute_audit_decisions(ref_date)
    except Exception as e:
        out["skipped_reason"] = f"audit_compute_failed: {type(e).__name__}: {e}"
        return out

    sv_ids = [d["sv_id"] for d in audit_decisions]
    out["n_strategies"] = len(sv_ids)

    try:
        live_rows = _fetch_live_rows(ref_date, sv_ids)
    except Exception as e:
        out["skipped_reason"] = f"live_fetch_failed: {type(e).__name__}: {e}"
        return out
    out["n_live_rows"] = len(live_rows)

    for d in audit_decisions:
        sv_id = d["sv_id"]
        live = live_rows.get(sv_id)
        if live is None:
            continue  # no live row for that (date, strategy) — can't diff
        audit_dec = {
            "sit_flat": bool(d["sit_flat"]),
            "filter_name": d.get("filter_name"),
        }
        diff_summary = _diff(audit_dec, live)
        if diff_summary:
            out["mismatches"].append({
                "strategy_version_id": sv_id,
                "display_name": d.get("display_name"),
                "diff": diff_summary,
                "audit": audit_dec,
                "live": {
                    "sit_flat": bool(live["sit_flat"]),
                    "filter_name": live.get("filter_name"),
                },
            })
    return out


def _print_human(report: dict) -> None:
    print()
    print(f"=== AUDIT GATE REPLAY · {report['window']['start']} → "
          f"{report['window']['end']} ({report['window']['days']} days) ===")
    print()

    rows_compared = 0
    rows_mismatch = 0
    dates_with_mismatch = 0
    dates_skipped = 0
    for day in report["days"]:
        if day["skipped_reason"]:
            dates_skipped += 1
            print(f"  {day['date']}  SKIPPED  {day['skipped_reason']}")
            continue
        rows_compared += day["n_live_rows"]
        rows_mismatch += len(day["mismatches"])
        if day["mismatches"]:
            dates_with_mismatch += 1
            for m in day["mismatches"]:
                name = m["display_name"] or m["strategy_version_id"][:8]
                print(f"  {day['date']}  MISMATCH  [{name}]  {m['diff']}")
        # Don't print clean days inline — the summary covers them.

    print()
    print("─" * 72)
    print(f"  Window:               {report['window']['days']} days")
    print(f"  Dates skipped:        {dates_skipped}")
    print(f"  Rows compared:        {rows_compared}")
    print(f"  Rows mismatched:      {rows_mismatch}")
    print(f"  Dates with mismatch:  {dates_with_mismatch}")
    print(f"  Elapsed:              {report['elapsed_s']:.1f}s")
    print()
    if rows_mismatch == 0 and dates_skipped < report["window"]["days"]:
        print("  RESULT: clean — audit and live paths agree on every "
              "comparable row.")
    elif rows_mismatch == 0:
        print("  RESULT: no mismatches but coverage is thin — review "
              "skipped dates.")
    else:
        print(f"  RESULT: {rows_mismatch} mismatch(es) — investigate before "
              "wiring trader to audit_status.")
    print()


def main() -> int:
    args = _parse_args()
    dates = _resolve_window(args)

    t0 = time.time()
    days_report: list[dict] = []
    for i, d in enumerate(dates, 1):
        sys.stderr.write(f"  [{i}/{len(dates)}] {d.isoformat()} ...\n")
        sys.stderr.flush()
        day_report = _replay_date(d)
        days_report.append(day_report)
        if args.stop_on_mismatch and day_report["mismatches"]:
            sys.stderr.write(f"  HALT — mismatch detected on {d}\n")
            break

    elapsed = time.time() - t0
    report = {
        "window": {
            "start": dates[0].isoformat(),
            "end": dates[-1].isoformat(),
            "days": len(dates),
        },
        "elapsed_s": elapsed,
        "days": days_report,
    }

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_human(report)

    # Exit 1 if any mismatch found (cron-friendly fail signal).
    any_mismatch = any(d["mismatches"] for d in days_report)
    return 1 if any_mismatch else 0


if __name__ == "__main__":
    sys.exit(main())

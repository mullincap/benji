#!/usr/bin/env python3
"""
audit_backtest_replay.py — diff audit_filters.py vs audit.py's stored
per-day filter decisions for one audit job.

Why this exists
---------------
The peer tool pipeline/audit_gate_replay.py diffs audit_filters.py
(extracted from daily_signal_v2) against what daily_signal_v2 wrote to
user_mgmt.daily_signals. Those two paths now share one implementation
(post-extraction), so that replay validates "the extraction didn't
break anything" — a low-bar check.

THIS replay is the higher-bar check: diff audit_filters.py against the
ORIGINAL pipeline/audit.py (build_tail_guardrail at audit.py:1950 and
build_dispersion_filter at audit.py:2765), as recorded in
audit.equity_curves.is_active for a completed audit job. If the two
implementations agree on the audit's full historical window
(typically 400+ days, including regime-fire days), the
audit_filters.py extraction is faithful to the audit code path.

Truth source:
  - audit.equity_curves.is_active per (result_id, date)
  - is_active = TRUE  → audit's filter cleared    → audit_sit_flat = FALSE
  - is_active = FALSE → audit's filter fired/flat → audit_sit_flat = TRUE

What the replay computes:
  - For each audit-day, recompute (sit_flat) via audit_filters using the
    strategy_version's config thresholds (tail_drop_pct, tail_vol_mult)
    pulled from audit.strategy_versions.config JSONB.
  - Diff against audit's recorded is_active.

Caveats:
  - is_active blends pure filter-fire with the audit's deploy schedule
    (e.g. days outside the deploy window will be is_active=FALSE).
    First run will reveal whether mismatches cluster on non-trading
    calendar days vs cluster on filter-decision days.
  - The audit filter (build_dispersion_filter in audit.py:2765) reads
    daily klines from a parquet. audit_filters.compute_dispersion_filter
    pulls from Binance FAPI live. Across long historical windows that
    Binance still serves, these should agree; if they diverge,
    audit-side-only data (delisted symbols, etc.) is the suspect.

Usage:
  python pipeline/audit_backtest_replay.py --audit-id <uuid>
  python pipeline/audit_backtest_replay.py --audit-id <uuid> --start 2026-01-01
  python pipeline/audit_backtest_replay.py --audit-id <uuid> --json
  python pipeline/audit_backtest_replay.py --audit-id <uuid> --stop-on-mismatch

Read-only — never writes to audit.* or user_mgmt.*.
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

from pipeline.audit_filters import (  # noqa: E402
    compute_tail_guardrail,
    compute_dispersion_filter,
    fetch_btc_daily_closes,
)
from pipeline.db.connection import get_conn  # noqa: E402

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("audit_backtest_replay")


# ==========================================================================
# CLI
# ==========================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Diff audit_filters.py vs audit.py's stored per-day "
                    "filter decisions for one audit job.",
    )
    p.add_argument("--audit-id", required=True, type=str,
                   help="Audit job_id (audit.jobs.job_id UUID).")
    p.add_argument("--filter-mode", default=None, type=str,
                   help="Override audit.results.filter_mode. Default: "
                        "match the strategy_version's active_filter from "
                        "audit.strategy_versions.config JSONB.")
    p.add_argument("--start", type=str, default=None,
                   help="Skip dates before this (YYYY-MM-DD). Useful for "
                        "diagnostic windows.")
    p.add_argument("--end", type=str, default=None,
                   help="Skip dates after this (YYYY-MM-DD).")
    p.add_argument("--json", action="store_true",
                   help="Emit JSON report instead of human-readable.")
    p.add_argument("--stop-on-mismatch", action="store_true",
                   help="Halt at the first mismatch (faster diagnostic).")
    p.add_argument("--limit", type=int, default=None,
                   help="Process at most N dates (debug / smoke test).")
    return p.parse_args()


# ==========================================================================
# AUDIT DATA FETCH
# ==========================================================================

def _resolve_audit_target(audit_id: str, override_filter_mode: str | None) -> dict:
    """Resolve audit_id → (result_id, filter_mode, sv_id, active_filter,
    config). Errors if no result_id matches the chosen filter_mode.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT j.strategy_version_id::text,
                   sv.config,
                   (sv.config->>'active_filter') AS active_filter
              FROM audit.jobs j
              JOIN audit.strategy_versions sv
                ON sv.strategy_version_id = j.strategy_version_id
             WHERE j.job_id = %s::uuid
            """,
            (audit_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"audit.jobs has no row for job_id={audit_id}")
        sv_id, config, active_filter = row
        target_filter_mode = override_filter_mode or active_filter
        if not target_filter_mode:
            raise RuntimeError(
                f"strategy_version {sv_id} has no active_filter in config; "
                f"pass --filter-mode explicitly."
            )

        cur.execute(
            """
            SELECT result_id::text, filter_mode
              FROM audit.results
             WHERE job_id = %s::uuid
               AND filter_mode = %s
            """,
            (audit_id, target_filter_mode),
        )
        result_row = cur.fetchone()
        if not result_row:
            cur.execute(
                """
                SELECT filter_mode FROM audit.results WHERE job_id = %s::uuid
                """,
                (audit_id,),
            )
            available = [r[0] for r in cur.fetchall()]
            raise RuntimeError(
                f"audit.results has no row for (job_id={audit_id}, "
                f"filter_mode={target_filter_mode!r}). Available: {available}"
            )
        cur.close()
        return {
            "audit_id": audit_id,
            "result_id": result_row[0],
            "filter_mode": result_row[1],
            "strategy_version_id": sv_id,
            "active_filter": active_filter,
            "config": config or {},
        }
    finally:
        conn.close()


def _fetch_audit_curve(result_id: str,
                       start: dt.date | None,
                       end: dt.date | None) -> list[tuple[dt.date, bool]]:
    """Return [(date, is_active), ...] in chronological order for the
    audit's stored equity curve.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        sql = """
            SELECT date, is_active
              FROM audit.equity_curves
             WHERE result_id = %s::uuid
        """
        params: list = [result_id]
        if start:
            sql += " AND date >= %s::date"
            params.append(start.isoformat())
        if end:
            sql += " AND date <= %s::date"
            params.append(end.isoformat())
        sql += " ORDER BY date"
        cur.execute(sql, tuple(params))
        rows = [(r[0], bool(r[1])) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    return rows


# ==========================================================================
# RECOMPUTE + DIFF
# ==========================================================================

def _recompute_filter_decision(ref_date: dt.date, config: dict) -> dict:
    """Recompute audit_filters' per-strategy decision for ref_date using
    the strategy_version's config. Returns:
        {sit_flat: bool, tg_fire: bool, dp_fire: bool,
         tg_reason: str|None, dp_reason: str|None,
         insufficient_history: bool, error: str|None}

    insufficient_history=True when Tail Guardrail fail-closes due to
    fewer than TAIL_VOL_LONG_WINDOW+1 prior BTC daily closes. These
    recompute outputs are NOT real filter decisions — they're a
    safety bypass — so the diff layer must skip them rather than
    silently agree with audit's actual FLAT days (false-positive
    agreement). Smoke-tested 2026-04-26: market.futures_1m's BTC
    coverage starts 2025-02-13, which is ALSO the audit's first
    date, so the first ~61 audit dates fail-close coincidentally.
    """
    out = {
        "sit_flat": None, "tg_fire": None, "dp_fire": None,
        "tg_reason": None, "dp_reason": None,
        "insufficient_history": False, "error": None,
    }
    try:
        tdp = config.get("tail_drop_pct")
        tvm = config.get("tail_vol_mult")
        btc_closes = fetch_btc_daily_closes(ref_date)
        tg_flat, tg_reason = compute_tail_guardrail(
            ref_date, closes=btc_closes,
            tail_drop_pct=float(tdp) if tdp is not None else None,
            tail_vol_mult=float(tvm) if tvm is not None else None,
        )
        if tg_reason and tg_reason.startswith("tail_guardrail_insufficient_history"):
            out["insufficient_history"] = True
            out["tg_reason"] = tg_reason
            return out
        dp_flat, dp_reason = compute_dispersion_filter(ref_date)
        out["tg_fire"] = bool(tg_flat)
        out["dp_fire"] = bool(dp_flat)
        out["tg_reason"] = tg_reason
        out["dp_reason"] = dp_reason
        # active_filter = "A - Tail + Dispersion": fire if either fires.
        out["sit_flat"] = bool(tg_flat) or bool(dp_flat)
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def _diff(audit_sit_flat: bool, recomputed: dict) -> str | None:
    if recomputed["error"]:
        return f"recompute_error: {recomputed['error']}"
    if recomputed["insufficient_history"]:
        return None  # caller checks insufficient_history before calling _diff
    if recomputed["sit_flat"] is None:
        return "recompute_returned_null"
    if bool(audit_sit_flat) != bool(recomputed["sit_flat"]):
        return (f"sit_flat: audit={audit_sit_flat} "
                f"recomputed={recomputed['sit_flat']} "
                f"tg_fire={recomputed['tg_fire']} "
                f"dp_fire={recomputed['dp_fire']}")
    return None


# ==========================================================================
# OUTPUT
# ==========================================================================

def _print_human(report: dict) -> None:
    print()
    print(f"=== AUDIT BACKTEST REPLAY · job={report['audit_id'][:8]} "
          f"filter={report['filter_mode']!r} ===")
    print(f"  strategy_version_id: {report['strategy_version_id']}")
    print(f"  active_filter:       {report['active_filter']}")
    print(f"  curve days:          {report['n_curve_days']}")
    print(f"  date range:          {report['first_date']} → {report['last_date']}")
    print()

    if report["mismatches"]:
        print(f"  MISMATCHES ({len(report['mismatches'])}):")
        for m in report["mismatches"][:50]:
            print(f"    {m['date']}  {m['diff']}")
        if len(report["mismatches"]) > 50:
            print(f"    ...{len(report['mismatches']) - 50} more")
        print()

    if report["errors"]:
        print(f"  RECOMPUTE ERRORS ({len(report['errors'])}):")
        for e in report["errors"][:20]:
            print(f"    {e['date']}  {e['error']}")
        if len(report["errors"]) > 20:
            print(f"    ...{len(report['errors']) - 20} more")
        print()

    print("─" * 72)
    print(f"  Curve days:           {report['n_curve_days']}")
    print(f"  Skipped (no history): {report['n_skipped_insufficient']}  "
          f"(Tail Guardrail needs 60d BTC history; "
          f"market.futures_1m starts 2025-02-13)")
    print(f"  Recompute errors:     {len(report['errors'])}")
    print(f"  Compared:             {report['n_compared']}")
    print(f"  Matched:              {report['n_matched']}")
    print(f"  Mismatched:           {len(report['mismatches'])}")
    print(f"  Audit PASS days:      {report['audit_active']}  "
          f"FLAT days: {report['audit_flat']}")
    print(f"  Elapsed:              {report['elapsed_s']:.1f}s")
    print()
    if report["mismatches"]:
        rate = 100.0 * len(report["mismatches"]) / max(report["n_compared"], 1)
        print(f"  RESULT: {len(report['mismatches'])} mismatch(es) "
              f"({rate:.2f}% mismatch rate over {report['n_compared']} "
              f"comparable dates). Investigate before trusting "
              f"audit_filters as audit-canonical.")
    elif report["n_compared"] == 0:
        print(f"  RESULT: 0 comparable dates — replay window has no "
              f"real comparisons. Coverage is empty.")
    elif report["errors"]:
        print(f"  RESULT: 0 mismatches over {report['n_compared']} "
              f"comparable dates, but {len(report['errors'])} "
              f"recompute errors — coverage incomplete.")
    else:
        print(f"  RESULT: clean — audit_filters and audit.py agree on "
              f"all {report['n_compared']} comparable dates.")
    print()


def _print_json(report: dict) -> None:
    print(json.dumps(report, default=str, indent=2, sort_keys=True))


# ==========================================================================
# MAIN
# ==========================================================================

def main() -> int:
    args = _parse_args()
    t0 = time.time()

    target = _resolve_audit_target(args.audit_id, args.filter_mode)
    log.warning(
        f"target: result_id={target['result_id'][:8]} "
        f"filter_mode={target['filter_mode']!r} "
        f"sv={target['strategy_version_id'][:8]}"
    )

    start_d = dt.date.fromisoformat(args.start) if args.start else None
    end_d = dt.date.fromisoformat(args.end) if args.end else None
    curve = _fetch_audit_curve(target["result_id"], start_d, end_d)
    if args.limit:
        curve = curve[: args.limit]

    if not curve:
        log.error("no equity_curves rows for given result_id + window")
        return 1

    n_compared = 0
    n_matched = 0
    n_skipped_insufficient = 0
    mismatches: list[dict] = []
    errors: list[dict] = []
    audit_active = sum(1 for _, a in curve if a)
    audit_flat = sum(1 for _, a in curve if not a)

    for i, (d, audit_active_flag) in enumerate(curve, 1):
        sys.stderr.write(f"  [{i}/{len(curve)}] {d.isoformat()}\n")
        sys.stderr.flush()
        audit_sit_flat = not audit_active_flag

        recomputed = _recompute_filter_decision(d, target["config"])
        if recomputed["error"]:
            errors.append({"date": d.isoformat(), "error": recomputed["error"]})
            continue
        if recomputed["insufficient_history"]:
            n_skipped_insufficient += 1
            continue

        diff_summary = _diff(audit_sit_flat, recomputed)
        n_compared += 1
        if diff_summary is None:
            n_matched += 1
        else:
            mismatches.append({
                "date": d.isoformat(),
                "audit_sit_flat": audit_sit_flat,
                "recomputed_sit_flat": recomputed["sit_flat"],
                "tg_fire": recomputed["tg_fire"],
                "dp_fire": recomputed["dp_fire"],
                "tg_reason": recomputed["tg_reason"],
                "dp_reason": recomputed["dp_reason"],
                "diff": diff_summary,
            })
            if args.stop_on_mismatch:
                sys.stderr.write(f"  HALT — mismatch on {d}\n")
                break

    elapsed = time.time() - t0
    report = {
        "audit_id": target["audit_id"],
        "filter_mode": target["filter_mode"],
        "strategy_version_id": target["strategy_version_id"],
        "active_filter": target["active_filter"],
        "n_curve_days": len(curve),
        "first_date": curve[0][0].isoformat() if curve else None,
        "last_date": curve[-1][0].isoformat() if curve else None,
        "n_compared": n_compared,
        "n_matched": n_matched,
        "n_skipped_insufficient": n_skipped_insufficient,
        "audit_active": audit_active,
        "audit_flat": audit_flat,
        "mismatches": mismatches,
        "errors": errors,
        "elapsed_s": elapsed,
    }

    if args.json:
        _print_json(report)
    else:
        _print_human(report)

    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())

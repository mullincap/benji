#!/usr/bin/env python3
"""compare_decisions_30day.py — verify peek's filter decisions match the audit.

For each of the last N days, computes peek's (audit_filters) Tail Guardrail
and Tail + Dispersion sit-flat decisions using the same thresholds the
audit ran with, and compares them to the audit's stored per-day decisions
(audit.equity_curves.is_active). Surfaces matches and divergences with
numeric breakdowns so any disagreement is debuggable.

Two filter modes are compared independently because the live strategy
versions can run different threshold packages:
  - "A - Tail Guardrail" alone  (latest audit defaults: tdp=0.04, tvm=1.4)
  - "A - Tail + Dispersion"     (latest audit defaults: tdp=0.03, tvm=1.4, dth=0.66)

Usage:
    python scripts/compare_decisions_30day.py
    python scripts/compare_decisions_30day.py --days 60
    python scripts/compare_decisions_30day.py --tg-result <uuid> --td-result <uuid>

When --tg-result / --td-result are omitted, the script picks the most
recent completed audit per filter_mode.
"""
from __future__ import annotations
import argparse
import datetime as _dt
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from pipeline.audit_filters import (  # noqa: E402
    compute_tail_guardrail_detail,
    compute_dispersion_filter_detail,
    fetch_btc_daily_closes,
)
from pipeline.db.connection import get_conn  # noqa: E402


def latest_result_for(mode: str) -> dict | None:
    """Pick the most recent audit.results row for a given filter_mode and
    return its result_id + the job's filter thresholds."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT r.result_id::text, r.job_id::text, j.created_at,
                   (j.config_overrides->>'tail_drop_pct')::float,
                   (j.config_overrides->>'tail_vol_mult')::float,
                   (j.config_overrides->>'dispersion_threshold')::float
              FROM audit.results r
              JOIN audit.jobs j ON j.job_id = r.job_id
             WHERE r.filter_mode = %s
             ORDER BY j.created_at DESC LIMIT 1
        """, (mode,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "result_id": row[0], "job_id": row[1], "created_at": row[2],
        "tail_drop_pct": row[3], "tail_vol_mult": row[4],
        "dispersion_threshold": row[5],
    }


def fetch_audit_decisions(result_id: str, days: int) -> dict[_dt.date, bool]:
    """Pull audit.equity_curves.is_active per date for the last N days.
    Returns {date: is_active}."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT date, is_active
              FROM audit.equity_curves
             WHERE result_id = %s::uuid
               AND date >= CURRENT_DATE - INTERVAL '%s days'
             ORDER BY date
        """, (result_id, days + 2))
        return {r[0]: bool(r[1]) for r in cur.fetchall()}
    finally:
        conn.close()


def fmt_dec(verdict: bool | None) -> str:
    if verdict is None: return "  ?  "
    return "TRADE" if verdict else "FLAT "


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30,
                    help="Trailing days to compare (default 30)")
    ap.add_argument("--tg-result", type=str, default=None,
                    help="Override Tail Guardrail audit result_id (uuid)")
    ap.add_argument("--td-result", type=str, default=None,
                    help="Override Tail + Dispersion audit result_id (uuid)")
    ap.add_argument("--end", type=str, default=None,
                    help="End date (YYYY-MM-DD); defaults to UTC today - 1")
    args = ap.parse_args()

    end = (_dt.date.fromisoformat(args.end) if args.end
           else _dt.datetime.now(_dt.timezone.utc).date() - _dt.timedelta(days=1))
    dates = [end - _dt.timedelta(days=i) for i in range(args.days - 1, -1, -1)]

    # Pick audit targets.
    tg_meta = latest_result_for("A - Tail Guardrail") if not args.tg_result else None
    td_meta = latest_result_for("A - Tail + Dispersion") if not args.td_result else None
    if args.tg_result:
        tg_meta = _resolve_explicit(args.tg_result)
    if args.td_result:
        td_meta = _resolve_explicit(args.td_result)
    if not tg_meta or not td_meta:
        print("error: could not resolve audit targets; pass --tg-result/--td-result")
        sys.exit(1)

    print(f"Tail Guardrail audit:")
    print(f"  result {tg_meta['result_id'][:8]}  job {tg_meta['job_id'][:8]}  "
          f"({tg_meta['created_at']:%Y-%m-%d %H:%M})  "
          f"tdp={tg_meta['tail_drop_pct']} tvm={tg_meta['tail_vol_mult']}")
    print(f"Tail + Dispersion audit:")
    print(f"  result {td_meta['result_id'][:8]}  job {td_meta['job_id'][:8]}  "
          f"({td_meta['created_at']:%Y-%m-%d %H:%M})  "
          f"tdp={td_meta['tail_drop_pct']} tvm={td_meta['tail_vol_mult']} "
          f"dth={td_meta['dispersion_threshold']}")

    audit_tg = fetch_audit_decisions(tg_meta["result_id"], args.days)
    audit_td = fetch_audit_decisions(td_meta["result_id"], args.days)

    # ── Run peek per day, comparing against audit ──────────────────────
    rows: list[dict] = []
    for d in dates:
        # TG using the TG audit's thresholds.
        tg = compute_tail_guardrail_detail(
            d,
            tail_drop_pct=tg_meta["tail_drop_pct"],
            tail_vol_mult=tg_meta["tail_vol_mult"],
        )
        # Disp + TG using the Tail+Dispersion audit's thresholds.
        tg_for_td = compute_tail_guardrail_detail(
            d,
            tail_drop_pct=td_meta["tail_drop_pct"],
            tail_vol_mult=td_meta["tail_vol_mult"],
        )
        disp_for_td = compute_dispersion_filter_detail(
            d, threshold=td_meta["dispersion_threshold"],
        )
        rows.append({
            "date": d,
            # Peek decisions: TRADE = not sit_flat
            "peek_tg_trade": (not tg["sit_flat"]) if tg["sit_flat"] is not None else None,
            "peek_tg_pdr": tg.get("prev_day_return"),
            "peek_tg_ratio": tg.get("rvol_ratio"),
            "peek_td_trade": (
                None if (tg_for_td["sit_flat"] is None or disp_for_td["sit_flat"] is None)
                else not (tg_for_td["sit_flat"] or disp_for_td["sit_flat"])
            ),
            "peek_td_tg_flat": tg_for_td["sit_flat"],
            "peek_td_disp_flat": disp_for_td["sit_flat"],
            "peek_td_disp_ratio": disp_for_td.get("dispersion_ratio"),
            "peek_td_disp_failopen": disp_for_td.get("fail_open_reason"),
            # Audit decisions
            "audit_tg_trade": audit_tg.get(d),
            "audit_td_trade": audit_td.get(d),
        })

    # ── Print TG comparison table ──────────────────────────────────────
    print(f"\n{'='*78}")
    print(f"  Tail Guardrail — last {args.days} days")
    print(f"  thresholds: drop=-{tg_meta['tail_drop_pct']*100:.1f}%  vol={tg_meta['tail_vol_mult']:g}×")
    print(f"{'='*78}")
    print(f"  date         peek    audit   match  prev_day  rvol_ratio")
    print(f"  ──────────   ─────   ─────   ─────  ────────  ──────────")
    tg_match = tg_mismatch = tg_missing = 0
    for r in rows:
        peek = r["peek_tg_trade"]; aud = r["audit_tg_trade"]
        if aud is None:
            mark = "  ?  "; tg_missing += 1
        elif peek == aud:
            mark = "  ✓  "; tg_match += 1
        else:
            mark = " ✗✗✗ "; tg_mismatch += 1
        pdr = (f"{r['peek_tg_pdr']*100:+7.3f}%" if r['peek_tg_pdr'] is not None else "   —   ")
        rr = (f"{r['peek_tg_ratio']:5.3f}×" if r['peek_tg_ratio'] is not None else "  —  ")
        print(f"  {r['date']}   {fmt_dec(peek)}   {fmt_dec(aud)}   {mark}  {pdr}  {rr}")
    print(f"\n  Summary: {tg_match} match, {tg_mismatch} mismatch, "
          f"{tg_missing} no audit row")

    # ── Print Tail+Disp comparison table ───────────────────────────────
    print(f"\n{'='*78}")
    print(f"  Tail + Dispersion — last {args.days} days")
    print(f"  TG: drop=-{td_meta['tail_drop_pct']*100:.1f}%  vol={td_meta['tail_vol_mult']:g}×    "
          f"Disp: thr={td_meta['dispersion_threshold']:g}")
    print(f"{'='*78}")
    print(f"  date         peek    audit   match  TGflat  DPflat  disp_ratio")
    print(f"  ──────────   ─────   ─────   ─────  ──────  ──────  ──────────")
    td_match = td_mismatch = td_missing = 0
    for r in rows:
        peek = r["peek_td_trade"]; aud = r["audit_td_trade"]
        if aud is None:
            mark = "  ?  "; td_missing += 1
        elif peek == aud:
            mark = "  ✓  "; td_match += 1
        else:
            mark = " ✗✗✗ "; td_mismatch += 1
        tgf = ("★" if r['peek_td_tg_flat'] else "·") if r['peek_td_tg_flat'] is not None else "?"
        dpf = ("★" if r['peek_td_disp_flat'] else "·") if r['peek_td_disp_flat'] is not None else "?"
        if r['peek_td_disp_failopen']:
            dr = f"open  "
        elif r['peek_td_disp_ratio'] is not None:
            dr = f"{r['peek_td_disp_ratio']:5.3f} "
        else:
            dr = "  —   "
        print(f"  {r['date']}   {fmt_dec(peek)}   {fmt_dec(aud)}   {mark}    {tgf}      {dpf}    {dr}")
    print(f"\n  Summary: {td_match} match, {td_mismatch} mismatch, "
          f"{td_missing} no audit row")

    # ── Final report ───────────────────────────────────────────────────
    print(f"\n{'='*78}")
    print(f"  Overall agreement (last {args.days} days)")
    print(f"{'='*78}")
    print(f"  Tail Guardrail:        {tg_match}/{tg_match+tg_mismatch} = "
          f"{100*tg_match/max(tg_match+tg_mismatch,1):.1f}% match  "
          f"({tg_mismatch} divergence{'s' if tg_mismatch != 1 else ''}, {tg_missing} no audit data)")
    print(f"  Tail + Dispersion:     {td_match}/{td_match+td_mismatch} = "
          f"{100*td_match/max(td_match+td_mismatch,1):.1f}% match  "
          f"({td_mismatch} divergence{'s' if td_mismatch != 1 else ''}, {td_missing} no audit data)")
    print()


def _resolve_explicit(result_id_prefix: str) -> dict | None:
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT r.result_id::text, r.job_id::text, j.created_at,
                   (j.config_overrides->>'tail_drop_pct')::float,
                   (j.config_overrides->>'tail_vol_mult')::float,
                   (j.config_overrides->>'dispersion_threshold')::float
              FROM audit.results r
              JOIN audit.jobs j ON j.job_id = r.job_id
             WHERE r.result_id::text LIKE %s
             LIMIT 1
        """, (result_id_prefix + "%",))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row: return None
    return {"result_id": row[0], "job_id": row[1], "created_at": row[2],
            "tail_drop_pct": row[3], "tail_vol_mult": row[4],
            "dispersion_threshold": row[5]}


if __name__ == "__main__":
    main()

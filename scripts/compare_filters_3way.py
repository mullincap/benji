#!/usr/bin/env python3
"""compare_filters_3way.py — 10-day per-day numeric cross-check.

For each of the last N days, pulls Tail Guardrail + Dispersion values from:
  1. audit_filters.py (live path — used by peek + daily_signal_v3)
  2. audit.py build_tail_guardrail / build_dispersion_filter (canonical
     audit math, run over a window ending each day)
  3. daily_signal_v3's historical cron.log (already-emitted values)

Prints a side-by-side table so divergences are obvious. The audit_filters
and audit.py values should be identical (or near-identical) on identical
inputs; daily_signal log values are slightly older formats and partial.
"""
from __future__ import annotations
import argparse
import datetime as _dt
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from pipeline.audit_filters import (  # noqa: E402
    compute_tail_guardrail_detail,
    compute_dispersion_filter_detail,
    fetch_btc_daily_closes,
)


SIGNAL_LOG = "/mnt/quant-data/logs/signal/cron.log"


def parse_signal_log(log_path: str, dates: list[_dt.date]) -> dict[_dt.date, dict]:
    """Parse daily_signal_v3 cron.log for the most recent log line for each
    target date. Returns dict[date] = {tg_pdr, tg_short, tg_long, tg_ratio,
    disp_yesterday, disp_baseline, disp_ratio} where present."""
    out: dict[_dt.date, dict] = {d: {} for d in dates}
    if not Path(log_path).exists():
        return out

    # Two TG line formats observed:
    #   "BTC prev-day: -1.05%  5d rvol: 1.59%  60d baseline: 2.42%  ratio: 0.656x"
    #   "BTC prev-day: 1.33%  rvol_short: 1.31%  rvol_long: 2.30%  ratio: 0.569x"
    tg_re_old = re.compile(
        r"^(\d{4}-\d{2}-\d{2}) .*BTC prev-day:\s*(-?[0-9.]+)%\s+"
        r"5d rvol:\s*([0-9.]+)%\s+60d baseline:\s*([0-9.]+)%\s+"
        r"ratio:\s*([0-9.]+)x"
    )
    tg_re_new = re.compile(
        r"^(\d{4}-\d{2}-\d{2}) .*BTC prev-day:\s*(-?[0-9.]+)%\s+"
        r"rvol_short:\s*([0-9.]+)%\s+rvol_long:\s*([0-9.]+)%\s+"
        r"ratio:\s*([0-9.]+)x"
    )
    disp_re = re.compile(
        r"^(\d{4}-\d{2}-\d{2}) .*Dispersion\[(\d{4}-\d{2}-\d{2})\]="
        r"([0-9.]+)\s+baseline_median=([0-9.]+)\s+ratio=([0-9.]+)"
    )

    with open(log_path) as f:
        for line in f:
            m = tg_re_old.match(line) or tg_re_new.match(line)
            if m:
                run_date = _dt.date.fromisoformat(m.group(1))
                if run_date in out:
                    out[run_date].update({
                        "tg_pdr": float(m.group(2)),
                        "tg_short": float(m.group(3)),
                        "tg_long": float(m.group(4)),
                        "tg_ratio": float(m.group(5)),
                    })
                continue
            m = disp_re.match(line)
            if m:
                run_date = _dt.date.fromisoformat(m.group(1))
                if run_date in out:
                    out[run_date].update({
                        "disp_yesterday_date": m.group(2),
                        "disp_yesterday": float(m.group(3)),
                        "disp_baseline": float(m.group(4)),
                        "disp_ratio": float(m.group(5)),
                    })
    return out


def audit_per_day_tg(end_date: _dt.date, lookback_days: int = 61) -> pd.DataFrame:
    """Replay audit.py's build_tail_guardrail per-day math but extract the
    per-day inputs (prev_ret, rvol_short, rvol_long, vol_ratio) for the
    window ending end_date. Uses the same BTC daily closes audit_filters uses
    (futures_1m DB) so the source matches.

    fetch_btc_daily_closes returns TAIL_VOL_LONG_WINDOW+6 = 66 days back from
    ref_date (exclusive), so closes[-1] = close[end_date]. Need >= 61 days
    for the 60d rvol window to populate."""
    closes = fetch_btc_daily_closes(end_date + _dt.timedelta(days=1))
    if len(closes) < lookback_days:
        return pd.DataFrame()

    # Mirror audit.py build_tail_guardrail (lines 1974-2020) but per-day:
    s = pd.Series(closes, index=pd.RangeIndex(len(closes)))
    lr = np.log(s / s.shift(1))
    # audit annualizes; we KEEP raw to match audit_filters output for
    # direct numeric comparison. Ratio cancels annualization anyway.
    rvol_short = lr.rolling(5, min_periods=3).std()
    rvol_long = lr.rolling(60, min_periods=30).std()
    vol_ratio = (rvol_short / rvol_long.replace(0, np.nan)).shift(1)
    prev_ret = lr.shift(1)

    # Indices: closes are oldest→newest, last index is end_date
    # closes[-1] is end_date, closes[-2] is end_date-1, etc.
    return pd.DataFrame({
        "prev_ret": prev_ret,
        "rvol_short": rvol_short,
        "rvol_long": rvol_long,
        "vol_ratio": vol_ratio,
    })


def fmt_pct(x) -> str:
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "  —    "
    return f"{x*100:+7.3f}%"


def fmt_ratio(x) -> str:
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "  —  "
    return f"{x:5.3f}"


def fmt_disp(x) -> str:
    if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))):
        return "   —   "
    return f"{x:.5f}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=10,
                    help="Number of trailing days to compare (default 10).")
    ap.add_argument("--end", type=str, default=None,
                    help="End date (YYYY-MM-DD); defaults to UTC today - 1.")
    ap.add_argument("--audit-id", type=str, default=None,
                    help="Optional audit job_id prefix. When set, pulls "
                         "tail_drop_pct/tail_vol_mult/dispersion_threshold "
                         "from audit.jobs.config_overrides for this run.")
    args = ap.parse_args()

    end_date = (
        _dt.date.fromisoformat(args.end) if args.end
        else _dt.datetime.now(_dt.timezone.utc).date() - _dt.timedelta(days=1)
    )
    dates = [end_date - _dt.timedelta(days=i) for i in range(args.days - 1, -1, -1)]

    overrides: dict = {}
    if args.audit_id:
        from pipeline.db.connection import get_conn
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT job_id::text, config_overrides FROM audit.jobs "
            "WHERE job_id::text LIKE %s ORDER BY created_at DESC LIMIT 1",
            (args.audit_id + "%",),
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            jid, cfg = row
            overrides = {k: cfg.get(k) for k in
                          ("tail_drop_pct", "tail_vol_mult",
                           "dispersion_threshold", "early_kill_y", "leverage")
                          if cfg.get(k) is not None}
            print(f"[overrides] audit {jid[:8]}: {overrides}")

    tdp = overrides.get("tail_drop_pct")
    tvm = overrides.get("tail_vol_mult")
    dth = overrides.get("dispersion_threshold")

    # Pull historical signal log values once.
    log_values = parse_signal_log(SIGNAL_LOG, dates)

    # ── Tail Guardrail table ────────────────────────────────────────
    print()
    print(f"════════════════════════════════════════════════════════════════════")
    print(f"  TAIL GUARDRAIL — last {args.days} days  "
          f"(thresholds: drop=-{(tdp or 0.04)*100:.1f}%  vol={tvm or 1.4:g}×)")
    print(f"════════════════════════════════════════════════════════════════════")
    print(f"  date         │ AF prev_day  ratio   fires │ A.PY prev_day ratio  Δratio │ LOG prev_day  ratio")
    print(f"  ─────────────┼─────────────────────────────┼─────────────────────────────┼──────────────────────")

    for d in dates:
        af = compute_tail_guardrail_detail(d, tail_drop_pct=tdp, tail_vol_mult=tvm)
        af_pdr = af.get("prev_day_return")
        af_ratio = af.get("rvol_ratio")
        af_fires = "★flat" if af.get("sit_flat") else "pass "
        if af.get("insufficient_history"):
            af_fires = "★hist"

        # Audit replay: closes[-1] corresponds to (d-1), so to get values
        # gating day d, we want last row of audit_per_day_tg(d-1) — but we
        # built closes ending at d (lookback fetches up through d). Easier:
        # rerun per-day with window ending d (uses same closes through d).
        ap_pdr = ap_ratio = float("nan")
        try:
            df = audit_per_day_tg(d)
            if not df.empty:
                # Last available row is for "day after d-1's close" = d
                # But prev_ret/rvol are .shift(1) so the row for d is the
                # last row in df.
                last = df.iloc[-1]
                # Convert log_return → simple
                ap_pdr = float(np.expm1(last["prev_ret"]))
                ap_ratio = float(last["vol_ratio"])
        except Exception:
            pass

        delta = (ap_ratio - af_ratio) if (af_ratio is not None and not np.isnan(ap_ratio)) else float("nan")

        log_pdr = log_values.get(d, {}).get("tg_pdr")
        log_ratio = log_values.get(d, {}).get("tg_ratio")

        print(f"  {d.isoformat()}   │ "
              f"{fmt_pct(af_pdr)}  {fmt_ratio(af_ratio)}×  {af_fires:<5} │ "
              f"{fmt_pct(ap_pdr)}  {fmt_ratio(ap_ratio)}×  {fmt_ratio(delta)}× │ "
              f"{(f'{log_pdr:+7.3f}%' if log_pdr is not None else '  —    '):>8}  "
              f"{(f'{log_ratio:5.3f}×' if log_ratio is not None else '  —  '):>6}")

    # ── Dispersion table ────────────────────────────────────────────
    print()
    print(f"════════════════════════════════════════════════════════════════════")
    print(f"  DISPERSION — last {args.days} days  (threshold={dth or 0.66:g})")
    print(f"════════════════════════════════════════════════════════════════════")
    print(f"  date         │ AF y_disp   baseline   ratio   fires │ LOG y_disp   baseline   ratio")
    print(f"  ─────────────┼────────────────────────────────────────┼──────────────────────────────────")

    for d in dates:
        af = compute_dispersion_filter_detail(d, threshold=dth)
        af_yd = af.get("yesterday_dispersion")
        af_bm = af.get("baseline_median")
        af_dr = af.get("dispersion_ratio")
        if af.get("fail_open_reason"):
            af_fires = f"★open({af['fail_open_reason'][:6]})"
        elif af.get("sit_flat"):
            af_fires = "★flat"
        else:
            af_fires = "pass "

        log_yd = log_values.get(d, {}).get("disp_yesterday")
        log_bm = log_values.get(d, {}).get("disp_baseline")
        log_dr = log_values.get(d, {}).get("disp_ratio")

        print(f"  {d.isoformat()}   │ "
              f"{fmt_disp(af_yd):>8}  {fmt_disp(af_bm):>8}  {fmt_ratio(af_dr)}  {af_fires:<6} │ "
              f"{fmt_disp(log_yd):>8}  {fmt_disp(log_bm):>8}  "
              f"{(f'{log_dr:5.3f}' if log_dr is not None else '  —  '):>6}")

    print()
    print("Legend:")
    print("  AF   = audit_filters.py (peek + daily_signal_v3 path)")
    print("  A.PY = audit.py replay against same DB BTC closes")
    print("  LOG  = parsed from /mnt/quant-data/logs/signal/cron.log (historical)")
    print("  ★flat = filter fires (sit-flat); ★hist = insufficient history;")
    print("  ★open = fail-open due to insufficient symbols/klines/baseline")
    print()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""compare_peek_vs_audit_30day.py — verify peek matches audit's filter math.

For each of the last N days, computes:
  - peek's filter decisions (audit_filters.compute_*_detail)
  - audit's truth decisions (audit.build_tail_guardrail | build_dispersion_filter)

Then diffs them. Both filter modes are evaluated independently:
  - Tail Guardrail alone
  - Tail + Dispersion (TG OR Disp fires → sit-flat)

This compares peek to the AUDIT METHODOLOGY truth — not to
audit.equity_curves.is_active, which is contaminated by deploy-schedule
and empty-basket factors (per audit_truth_replay's design notes). A clean
match here means peek would have produced identical sit-flat decisions
to the audit on identical inputs.

Usage:
    python scripts/compare_peek_vs_audit_30day.py
    python scripts/compare_peek_vs_audit_30day.py --days 60
    python scripts/compare_peek_vs_audit_30day.py --tdp 0.03 --dth 0.66
"""
from __future__ import annotations
import argparse
import datetime as _dt
import logging
import sys
import time
from pathlib import Path

# audit.py at /root/benji/pipeline/audit.py needs sibling modules on sys.path.
HERE = Path(__file__).resolve().parent
PIPELINE_DIR = HERE.parent / "pipeline"
sys.path.insert(0, str(HERE.parent))     # project root → from pipeline import …
sys.path.insert(0, str(PIPELINE_DIR))    # pipeline dir → import audit, config

import pandas as pd  # noqa: E402

from pipeline.audit_filters import (  # noqa: E402
    compute_tail_guardrail_detail,
    compute_dispersion_filter_detail,
)

# Heavy import: audit.py emits banner logs + runs DISPERSION_UNIVERSE setup.
logging.basicConfig(level=logging.WARNING, stream=sys.stdout, format="%(message)s")
import audit  # noqa: E402


def _fetch_btc_ohlcv() -> pd.DataFrame:
    """yfinance daily BTC OHLCV from 2014-11-01, normalized to lowercase
    cols + tz-naive index — matches audit_truth_replay._fetch_btc_ohlcv."""
    import yfinance as yf
    print("fetching BTC daily history via yfinance...", flush=True)
    btc_raw = yf.Ticker("BTC-USD").history(
        start="2014-11-01", end=None, interval="1d", auto_adjust=True,
    )
    btc_ohlcv = btc_raw[["Open", "High", "Low", "Close", "Volume"]].copy()
    btc_ohlcv.columns = ["open", "high", "low", "close", "volume"]
    btc_ohlcv.index = pd.to_datetime(btc_ohlcv.index).tz_localize(None).normalize()
    print(f"  {len(btc_ohlcv)} days, {btc_ohlcv.index.min().date()} → "
          f"{btc_ohlcv.index.max().date()}", flush=True)
    return btc_ohlcv


def _fetch_alt_returns(end_date: _dt.date, days: int) -> pd.DataFrame:
    """Use audit.fetch_altcoin_daily_returns. Window covers the comparison
    range plus generous warm-up so dispersion's baseline_win has history.

    Reuses audit's own DISPERSION_CACHE_FILE so both sides of the
    comparison (truth and peek-via-audit_filters) hit identical cached
    klines — eliminates cache-timing as a confound.

    When `strict_dynamic` is True, the returns universe is built from
    market.market_cap_daily (union of per-day top-N, lagged) instead of
    the legacy hardcoded DISPERSION_SYMBOLS list, matching the audit
    pipeline's strict-dynamic path. Live ALTS MAIN now runs with this
    flag set in stored config (locked 2026-04-29), so cron-context
    comparisons should use strict_dynamic=True from now on.
    """
    start = (end_date - _dt.timedelta(days=days + 200)).isoformat()
    end = (end_date + _dt.timedelta(days=1)).isoformat()
    print(f"fetching alt_returns {start} → {end} via audit helper...",
          flush=True)
    return audit.fetch_altcoin_daily_returns(start=start, end=end)


def _audit_truth_series(btc_ohlcv: pd.DataFrame, alt_returns: pd.DataFrame,
                        tdp: float, tvm: float,
                        dth: float, baseline_win: int,
                        n_symbols: int = 40,
                        lag_days: int | None = None) -> pd.DataFrame:
    """Run audit.build_tail_guardrail + build_dispersion_filter (with the
    dynamic mcap mask the real audit jobs use) over the full series.

    Returns DataFrame indexed by date with columns:
      - tail (bool: True = TG fires/sit-flat)
      - disp (bool: True = Disp fires/sit-flat — dynamic universe mode)
      - tail_or_disp (bool: union, for Tail+Dispersion mode)

    `lag_days` is forwarded to build_dynamic_symbol_mask. None → use
    audit's DISPERSION_UNIVERSE_LAG_DAYS env default. The audit's
    strict-dynamic path is implicit when alt_returns came from a
    dynamic-universe fetch (which is the case here when the caller
    used build_dynamic_returns_universe upstream).
    """
    print(f"computing audit truth: TG(drop={tdp}, vol={tvm}) | "
          f"Disp(thr={dth}, win={baseline_win}, n={n_symbols} dynamic)...",
          flush=True)
    tail = audit.build_tail_guardrail(btc_ohlcv, drop_pct=tdp, vol_mult=tvm)

    # Dispersion: real audit jobs run with DISPERSION_DYNAMIC_UNIVERSE=True,
    # so build the mcap mask and pass it. Without the mask, build_dispersion_filter
    # falls back to a static all-symbols universe and produces a different
    # (and non-canonical) ratio series.
    start_str = alt_returns.index.min().date().isoformat()
    end_str = alt_returns.index.max().date().isoformat()
    mcap_df = audit._load_mcap_from_db(start_str, end_str)
    mask_kwargs = {"n": n_symbols}
    if lag_days is not None:
        mask_kwargs["lag_days"] = lag_days
    mask_df = audit.build_dynamic_symbol_mask(mcap_df, alt_returns, **mask_kwargs)
    disp = audit.build_dispersion_filter(
        alt_returns, threshold=dth, baseline_win=baseline_win,
        symbol_mask_df=mask_df,
    )
    tail.index = pd.to_datetime(tail.index).tz_localize(None).normalize()
    disp.index = pd.to_datetime(disp.index).tz_localize(None).normalize()
    df = pd.concat([tail.rename("tail"), disp.rename("disp")], axis=1)
    df["tail"] = df["tail"].fillna(False).astype(bool)
    df["disp"] = df["disp"].fillna(False).astype(bool)
    df["tail_or_disp"] = df["tail"] | df["disp"]
    return df


def fmt_dec(sit_flat: bool | None) -> str:
    if sit_flat is None: return "  ?  "
    return "FLAT " if sit_flat else "TRADE"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30,
                    help="Trailing days to compare (default 30)")
    ap.add_argument("--end", type=str, default=None,
                    help="End date YYYY-MM-DD (default UTC today - 1)")
    ap.add_argument("--tdp", type=float, default=0.04,
                    help="tail_drop_pct (default 0.04)")
    ap.add_argument("--tvm", type=float, default=1.4,
                    help="tail_vol_mult (default 1.4)")
    ap.add_argument("--dth", type=float, default=0.66,
                    help="dispersion_threshold (default 0.66)")
    ap.add_argument("--baseline-win", type=int, default=33,
                    help="dispersion baseline_win (default 33)")
    ap.add_argument("--n", type=int, default=40,
                    help="dispersion_n (default 40; ALTS MAIN locked at 20)")
    ap.add_argument("--lag-days", type=int, default=None,
                    help="dispersion universe lag (default = audit env, =1)")
    ap.add_argument("--strict-dynamic", action="store_true",
                    help="Use strict_dynamic universe on BOTH peek and audit "
                         "sides — required to compare against the regime live "
                         "now runs (locked-in 2026-04-29 for ALTS MAIN). When "
                         "off, runs the legacy hardcoded-pool comparison.")
    args = ap.parse_args()

    end = (_dt.date.fromisoformat(args.end) if args.end
           else _dt.datetime.now(_dt.timezone.utc).date() - _dt.timedelta(days=1))
    dates = [end - _dt.timedelta(days=i) for i in range(args.days - 1, -1, -1)]

    print(f"\nComparing peek vs audit-py for {args.days} days "
          f"({dates[0]} → {dates[-1]})")
    print(f"Thresholds: tdp={args.tdp}  tvm={args.tvm}  dth={args.dth}  "
          f"baseline_win={args.baseline_win}  n={args.n}  "
          f"lag_days={args.lag_days}  strict_dynamic={args.strict_dynamic}\n")

    t0 = time.time()
    btc = _fetch_btc_ohlcv()
    alt = _fetch_alt_returns(end, args.days)

    # When strict_dynamic, override the alt_returns universe with the
    # mcap-derived dynamic universe BEFORE building the truth series.
    # This matches what live audit_filters now does per-strategy.
    if args.strict_dynamic:
        mcap_for_universe = audit._load_mcap_from_db(
            (end - _dt.timedelta(days=args.days + 200)).isoformat(),
            (end + _dt.timedelta(days=1)).isoformat(),
        )
        dyn_lag = args.lag_days if args.lag_days is not None else 1
        dyn_universe = audit.build_dynamic_returns_universe(
            mcap_for_universe, n=args.n, lag_days=dyn_lag,
        )
        print(f"strict_dynamic: built {len(dyn_universe)}-ticker universe "
              f"(union of top-{args.n} mcap, lagged {dyn_lag}d)\n",
              flush=True)
        # Re-fetch alt_returns scoped to the dynamic universe
        start_str = (end - _dt.timedelta(days=args.days + 200)).isoformat()
        end_str = (end + _dt.timedelta(days=1)).isoformat()
        alt = audit.fetch_altcoin_daily_returns(
            symbols=dyn_universe, start=start_str, end=end_str,
        )

    truth = _audit_truth_series(btc, alt, args.tdp, args.tvm,
                                  args.dth, args.baseline_win,
                                  n_symbols=args.n,
                                  lag_days=args.lag_days)
    print(f"audit truth ready ({time.time()-t0:.1f}s)\n", flush=True)

    # Compute peek per day.
    print("computing peek decisions per day (audit_filters)...", flush=True)
    rows = []
    for d in dates:
        tg = compute_tail_guardrail_detail(d, tail_drop_pct=args.tdp,
                                            tail_vol_mult=args.tvm)
        dp = compute_dispersion_filter_detail(
            d,
            threshold=args.dth,
            baseline_win=args.baseline_win,
            n_symbols=args.n,
            lag_days=args.lag_days,
            strict_dynamic=args.strict_dynamic,
        )
        ts = pd.Timestamp(d)
        rows.append({
            "date": d,
            "peek_tg_flat": tg["sit_flat"],
            "peek_disp_flat": dp["sit_flat"],
            "peek_tg_pdr": tg.get("prev_day_return"),
            "peek_tg_ratio": tg.get("rvol_ratio"),
            "peek_disp_ratio": dp.get("dispersion_ratio"),
            "peek_disp_failopen": dp.get("fail_open_reason"),
            "audit_tg_flat": (bool(truth.loc[ts, "tail"]) if ts in truth.index else None),
            "audit_disp_flat": (bool(truth.loc[ts, "disp"]) if ts in truth.index else None),
        })

    # ── TG comparison ───────────────────────────────────────────────────
    print(f"\n{'='*86}")
    print(f"  Tail Guardrail — peek vs audit (drop=-{args.tdp*100:.1f}% vol={args.tvm:g}×)")
    print(f"{'='*86}")
    print(f"  date         peek    audit   match  prev_day  rvol_ratio  audit_TG  peek_TG")
    print(f"  ──────────   ─────   ─────   ─────  ────────  ──────────  ────────  ───────")
    tg_match = tg_mis = tg_miss = 0
    for r in rows:
        peek_v = r["peek_tg_flat"]; aud_v = r["audit_tg_flat"]
        if aud_v is None:
            mark = "  ?  "; tg_miss += 1
        elif peek_v == aud_v:
            mark = "  ✓  "; tg_match += 1
        else:
            mark = " ✗✗✗ "; tg_mis += 1
        pdr = (f"{r['peek_tg_pdr']*100:+7.3f}%" if r['peek_tg_pdr'] is not None else "   —   ")
        rr = (f"{r['peek_tg_ratio']:5.3f}×" if r['peek_tg_ratio'] is not None else "  —  ")
        a_tag = "★FLAT" if aud_v else ("PASS " if aud_v is False else " ?   ")
        p_tag = "★FLAT" if peek_v else ("PASS " if peek_v is False else " ?   ")
        print(f"  {r['date']}   {fmt_dec(peek_v)}   {fmt_dec(aud_v)}   {mark}  "
              f"{pdr}  {rr}    {a_tag}    {p_tag}")
    print(f"\n  Tail Guardrail: {tg_match} match, {tg_mis} mismatch, "
          f"{tg_miss} no audit data")

    # ── Tail+Dispersion (Disp component) comparison ───────────────────
    print(f"\n{'='*86}")
    print(f"  Dispersion — peek vs audit (thr={args.dth:g}, win={args.baseline_win}d)")
    print(f"{'='*86}")
    print(f"  date         peek    audit   match  disp_ratio  audit_DP  peek_DP")
    print(f"  ──────────   ─────   ─────   ─────  ──────────  ────────  ───────")
    dp_match = dp_mis = dp_miss = 0
    for r in rows:
        peek_v = r["peek_disp_flat"]; aud_v = r["audit_disp_flat"]
        if aud_v is None:
            mark = "  ?  "; dp_miss += 1
        elif peek_v == aud_v:
            mark = "  ✓  "; dp_match += 1
        else:
            mark = " ✗✗✗ "; dp_mis += 1
        if r["peek_disp_failopen"]:
            dr = "  open  "
        elif r["peek_disp_ratio"] is not None:
            dr = f"  {r['peek_disp_ratio']:5.3f}  "
        else:
            dr = "    —   "
        a_tag = "★FLAT" if aud_v else ("PASS " if aud_v is False else " ?   ")
        p_tag = "★FLAT" if peek_v else ("PASS " if peek_v is False else " ?   ")
        print(f"  {r['date']}   {fmt_dec(peek_v)}   {fmt_dec(aud_v)}   {mark}  "
              f"{dr}    {a_tag}    {p_tag}")
    print(f"\n  Dispersion: {dp_match} match, {dp_mis} mismatch, "
          f"{dp_miss} no audit data")

    # ── Tail+Dispersion combined comparison ─────────────────────────────
    print(f"\n{'='*86}")
    print(f"  Tail + Dispersion (combined: TG OR Disp fires) — peek vs audit")
    print(f"{'='*86}")
    print(f"  date         peek    audit   match  TGflat-peek  DPflat-peek  TGflat-aud  DPflat-aud")
    print(f"  ──────────   ─────   ─────   ─────  ───────────  ───────────  ──────────  ──────────")
    td_match = td_mis = td_miss = 0
    for r in rows:
        peek_tg = r["peek_tg_flat"]; peek_dp = r["peek_disp_flat"]
        aud_tg = r["audit_tg_flat"]; aud_dp = r["audit_disp_flat"]
        peek_td = (None if (peek_tg is None or peek_dp is None) else (peek_tg or peek_dp))
        aud_td = (None if (aud_tg is None or aud_dp is None) else (aud_tg or aud_dp))
        if aud_td is None:
            mark = "  ?  "; td_miss += 1
        elif peek_td == aud_td:
            mark = "  ✓  "; td_match += 1
        else:
            mark = " ✗✗✗ "; td_mis += 1
        def _t(v): return "★flat" if v else (" pass" if v is False else " ?  ")
        print(f"  {r['date']}   {fmt_dec(peek_td)}   {fmt_dec(aud_td)}   {mark}  "
              f"  {_t(peek_tg):<10}   {_t(peek_dp):<10}    "
              f"{_t(aud_tg):<10}  {_t(aud_dp):<10}")
    print(f"\n  Tail + Dispersion: {td_match} match, {td_mis} mismatch, "
          f"{td_miss} no audit data")

    # ── Summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*86}")
    print(f"  Overall agreement (peek vs audit-py truth) — last {args.days} days")
    print(f"{'='*86}")
    pct = lambda m, t: 100*m/max(t, 1)
    print(f"  Tail Guardrail:        {tg_match}/{tg_match+tg_mis} = "
          f"{pct(tg_match, tg_match+tg_mis):.1f}%")
    print(f"  Dispersion (component):{dp_match}/{dp_match+dp_mis} = "
          f"{pct(dp_match, dp_match+dp_mis):.1f}%")
    print(f"  Tail + Dispersion:     {td_match}/{td_match+td_mis} = "
          f"{pct(td_match, td_match+td_mis):.1f}%")
    print()


if __name__ == "__main__":
    main()

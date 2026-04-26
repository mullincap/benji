#!/usr/bin/env python3
"""
audit_truth_replay.py — produce the audit-canonical filter "answer key"
by invoking pipeline/audit.py's actual filter functions, then diff
against audit.equity_curves.is_active to determine whether is_active
equals the filter decision or is contaminated by other factors
(deploy schedule, empty basket, no-data day, etc.).

Background (2026-04-26 design call)
-----------------------------------
The 437-day audit_backtest_replay.py reported 60% mismatch between
audit_filters.py outputs and audit.equity_curves.is_active. That
number is unsafe to act on because BOTH sides are suspects:
  - audit_filters.py may drift from audit.py (the question we want to answer)
  - audit.equity_curves.is_active may not equal the filter decision
    (it's a single boolean that combines whatever audit.py decided to
    record — could include deploy-schedule, empty-basket, etc.)

This script establishes the truth answer key by calling audit.py's
build_tail_guardrail and build_dispersion_filter directly with the
strategy_version's thresholds, then ORing per the active_filter
("A - Tail + Dispersion" → tail | disp).

Truth is computed per date, not stored. Two diffs:
  1. truth vs audit.equity_curves.is_active  → reveals contamination
     of is_active by non-filter factors. Clean agreement → is_active
     IS the filter decision; we can use it as comparison source.
  2. truth vs audit_filters.py (deferred — heavier; ~73 min)

Usage
-----
Must run inside celery container (audit.py needs /app/pipeline on
sys.path and yfinance/sklearn/etc. available). Invoke:

    docker exec benji-celery-1 python /app/pipeline/audit_truth_replay.py \
        --audit-id <uuid>

Read-only — never writes to audit.* tables.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import sys
import time
from pathlib import Path

# audit.py needs /app/pipeline on sys.path (sibling import of
# institutional_audit). The container's PIPELINE_DIR is /app/pipeline.
sys.path.insert(0, "/app/pipeline")
sys.path.insert(0, "/app")

# pipeline.db is at /app/pipeline/db/connection.py — already covered
# by the path above.
from pipeline.db.connection import get_conn  # noqa: E402

import pandas as pd  # noqa: E402

# Defer the audit import — it has heavy module-level work + emits
# DISPERSION_UNIVERSE_SIZE log lines on import.
import audit  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("audit_truth_replay")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compute the audit-canonical filter answer key via "
                    "audit.py's filter functions and diff against "
                    "audit.equity_curves.is_active."
    )
    p.add_argument("--audit-id", required=True, type=str,
                   help="audit.jobs.job_id UUID.")
    p.add_argument("--filter-mode", default=None, type=str,
                   help="Override audit.results.filter_mode (default: "
                        "match the strategy_version's active_filter).")
    p.add_argument("--limit-mismatches", type=int, default=50,
                   help="Print at most N mismatch rows in human output.")
    return p.parse_args()


# ==========================================================================
# AUDIT METADATA + INPUTS
# ==========================================================================

def _load_target(audit_id: str, override_filter_mode: str | None) -> dict:
    """Load audit metadata: result_id, filter_mode, sv_id, config, date range."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT j.job_id::text, j.date_from, j.date_to,
                   j.strategy_version_id::text, sv.config,
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
            raise RuntimeError(f"audit.jobs has no row for {audit_id}")
        job_id, date_from, date_to, sv_id, config, active_filter = row
        target_filter_mode = override_filter_mode or active_filter
        if not target_filter_mode:
            raise RuntimeError(
                f"No active_filter in config for {sv_id}; pass --filter-mode."
            )

        cur.execute(
            """
            SELECT result_id::text
              FROM audit.results
             WHERE job_id = %s::uuid AND filter_mode = %s
            """,
            (audit_id, target_filter_mode),
        )
        result_row = cur.fetchone()
        if not result_row:
            raise RuntimeError(
                f"No audit.results row for filter_mode={target_filter_mode!r}"
            )
        cur.close()
        return {
            "audit_id":    job_id,
            "result_id":   result_row[0],
            "filter_mode": target_filter_mode,
            "sv_id":       sv_id,
            "config":      config or {},
            "date_from":   date_from,
            "date_to":     date_to,
            "active_filter": active_filter,
        }
    finally:
        conn.close()


def _fetch_btc_ohlcv() -> pd.DataFrame:
    """Mirror audit.py's BTC fetch — yfinance daily OHLCV from 2014-11-01.
    Returns DataFrame with Open/High/Low/Close/Volume indexed by date.
    """
    import yfinance as yf
    log.info("yfinance: fetching BTC-USD daily history...")
    btc_raw = yf.Ticker("BTC-USD").history(
        start="2014-11-01", end=None, interval="1d", auto_adjust=True,
    )
    if btc_raw is None or btc_raw.empty:
        raise RuntimeError("yfinance returned empty BTC history")
    btc_ohlcv = btc_raw[["Open", "High", "Low", "Close", "Volume"]].copy()
    btc_ohlcv.index = pd.to_datetime(btc_ohlcv.index).tz_localize(None).normalize()
    log.info(f"  {len(btc_ohlcv)} days, "
             f"{btc_ohlcv.index.min().date()} → {btc_ohlcv.index.max().date()}")
    return btc_ohlcv


def _fetch_alt_returns(date_from: _dt.date, date_to: _dt.date) -> pd.DataFrame:
    """Use audit.fetch_altcoin_daily_returns with a window covering the
    audit's range plus baseline warm-up. Default symbols (top-90 by
    audit's DISPERSION_SYMBOLS) — sufficient for the canonical config.
    """
    log.info("fetching alt_returns via audit.fetch_altcoin_daily_returns...")
    # Add a generous warm-up so baseline_win has history.
    start = (date_from - _dt.timedelta(days=120)).isoformat()
    end = (date_to + _dt.timedelta(days=1)).isoformat()
    alt_returns = audit.fetch_altcoin_daily_returns(
        start=start, end=end,
        cache_file=str(Path(audit.__file__).parent / "dispersion_cache_audit_truth.csv"),
    )
    log.info(f"  shape={alt_returns.shape}")
    return alt_returns


# ==========================================================================
# TRUTH COMPUTATION
# ==========================================================================

def _compute_truth_series(
    btc_ohlcv: pd.DataFrame,
    alt_returns: pd.DataFrame,
    config: dict,
) -> pd.Series:
    """truth = build_tail_guardrail | build_dispersion_filter using the
    strategy's threshold config. True = filter fires (sit_flat).
    """
    drop_pct = float(config.get("tail_drop_pct", 0.05))
    vol_mult = float(config.get("tail_vol_mult", 2.0))
    disp_thresh = float(config.get("dispersion_threshold", 0.75))
    disp_baseline_win = int(config.get("dispersion_baseline_win", 20))

    log.info(
        f"computing truth: TG(drop={drop_pct}, vol_mult={vol_mult}) | "
        f"DP(threshold={disp_thresh}, baseline_win={disp_baseline_win})"
    )
    tail = audit.build_tail_guardrail(
        btc_ohlcv, drop_pct=drop_pct, vol_mult=vol_mult,
    )
    disp = audit.build_dispersion_filter(
        alt_returns, threshold=disp_thresh, baseline_win=disp_baseline_win,
    )

    # Align indexes by date (both produce daily Series).
    tail.index = pd.to_datetime(tail.index).tz_localize(None).normalize()
    disp.index = pd.to_datetime(disp.index).tz_localize(None).normalize()

    combined = pd.concat([tail.rename("tail"), disp.rename("disp")], axis=1)
    combined["truth"] = combined["tail"].fillna(False) | combined["disp"].fillna(False)
    return combined


# ==========================================================================
# AUDIT-SIDE EQUITY_CURVES FETCH
# ==========================================================================

def _fetch_audit_curve(result_id: str) -> dict:
    """Return {date: is_active} for the audit's equity curve."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT date, is_active
              FROM audit.equity_curves
             WHERE result_id = %s::uuid
             ORDER BY date
            """,
            (result_id,),
        )
        rows = {r[0]: bool(r[1]) for r in cur.fetchall()}
        cur.close()
    finally:
        conn.close()
    return rows


# ==========================================================================
# DAILY BASKETS (for cross-referencing with empty-basket days)
# ==========================================================================

def _fetch_baskets(audit_id: str) -> dict:
    """Return {date: basket_size} for the audit's daily_baskets."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT date, COALESCE(array_length(basket, 1), 0) AS sz
              FROM audit.daily_baskets
             WHERE job_id = %s::uuid
             ORDER BY date
            """,
            (audit_id,),
        )
        rows = {r[0]: int(r[1]) for r in cur.fetchall()}
        cur.close()
    finally:
        conn.close()
    return rows


# ==========================================================================
# DIFF + REPORT
# ==========================================================================

def main() -> int:
    args = _parse_args()
    t0 = time.time()

    target = _load_target(args.audit_id, args.filter_mode)
    log.info(
        f"target: audit_id={target['audit_id'][:8]} "
        f"filter_mode={target['filter_mode']!r} "
        f"sv={target['sv_id'][:8]} "
        f"window={target['date_from']} → {target['date_to']}"
    )

    btc_ohlcv = _fetch_btc_ohlcv()
    alt_returns = _fetch_alt_returns(target["date_from"], target["date_to"])
    truth_df = _compute_truth_series(btc_ohlcv, alt_returns, target["config"])

    audit_active_by_date = _fetch_audit_curve(target["result_id"])
    baskets_by_date = _fetch_baskets(target["audit_id"])

    # Walk audit's date window in the equity curve order. truth_df is
    # indexed by Timestamp; lookup via .normalize() date.
    start = target["date_from"]
    end   = target["date_to"]
    cursor_d = start
    rows: list[dict] = []
    while cursor_d <= end:
        ts = pd.Timestamp(cursor_d)
        if ts in truth_df.index:
            tail_v = bool(truth_df.loc[ts, "tail"])
            disp_v = bool(truth_df.loc[ts, "disp"])
            truth_v = bool(truth_df.loc[ts, "truth"])
        else:
            tail_v = disp_v = truth_v = None
        is_active = audit_active_by_date.get(cursor_d)
        audit_sit_flat = (not is_active) if is_active is not None else None
        basket_sz = baskets_by_date.get(cursor_d)
        rows.append({
            "date":           cursor_d.isoformat(),
            "tail_fire":      tail_v,
            "disp_fire":      disp_v,
            "truth_sit_flat": truth_v,
            "audit_sit_flat": audit_sit_flat,
            "basket_size":    basket_sz,
        })
        cursor_d += _dt.timedelta(days=1)

    # Comparison: count agreement / disagreement, segment by basket-size.
    n_compared = 0
    n_match = 0
    mismatches: list[dict] = []
    for r in rows:
        if r["truth_sit_flat"] is None or r["audit_sit_flat"] is None:
            continue
        n_compared += 1
        if r["truth_sit_flat"] == r["audit_sit_flat"]:
            n_match += 1
        else:
            mismatches.append(r)

    elapsed = time.time() - t0

    print()
    print(f"=== AUDIT TRUTH REPLAY · job={target['audit_id'][:8]} "
          f"filter={target['filter_mode']!r} ===")
    print(f"  strategy_version_id: {target['sv_id']}")
    print(f"  window: {target['date_from']} → {target['date_to']}  "
          f"({len(rows)} calendar days)")
    print()
    print(f"  Compared (truth vs audit.is_active): {n_compared}")
    print(f"  Matched:                              {n_match}")
    print(f"  Mismatched:                           {len(mismatches)}")
    rate = (100.0 * len(mismatches) / n_compared) if n_compared else 0.0
    print(f"  Mismatch rate:                        {rate:.2f}%")
    print(f"  Elapsed:                              {elapsed:.1f}s")
    print()

    if mismatches:
        # Segment mismatches by direction + basket-size to look for
        # patterns that hint at contamination.
        truth_T_audit_F = [m for m in mismatches if m["truth_sit_flat"] and not m["audit_sit_flat"]]
        truth_F_audit_T = [m for m in mismatches if not m["truth_sit_flat"] and m["audit_sit_flat"]]
        empty_basket = [m for m in mismatches if (m["basket_size"] or 0) == 0]
        print(f"  Direction breakdown:")
        print(f"    truth=fire,  audit=trade : {len(truth_T_audit_F)}")
        print(f"    truth=trade, audit=flat  : {len(truth_F_audit_T)}")
        print(f"  Of all mismatches, {len(empty_basket)} have empty basket "
              f"(basket-emptiness is a likely audit.is_active=False driver "
              f"unrelated to the filter)")
        print()
        print(f"  First {min(args.limit_mismatches, len(mismatches))} "
              f"mismatches (date, tail/disp/truth, audit, basket_size):")
        for m in mismatches[: args.limit_mismatches]:
            print(f"    {m['date']}  "
                  f"tail={m['tail_fire']!s:5} disp={m['disp_fire']!s:5} "
                  f"truth={m['truth_sit_flat']!s:5}  "
                  f"audit_sit_flat={m['audit_sit_flat']!s:5}  "
                  f"basket={m['basket_size']}")
        if len(mismatches) > args.limit_mismatches:
            print(f"    ...{len(mismatches) - args.limit_mismatches} more")
        print()

    if not mismatches and n_compared:
        print("  RESULT: clean — audit.equity_curves.is_active equals the "
              "filter decision. is_active is safe to use as a comparison "
              "source going forward.")
    elif n_compared == 0:
        print("  RESULT: no comparable rows.")
    else:
        print(f"  RESULT: {len(mismatches)} disagreement(s) between truth "
              f"and audit.is_active. is_active is contaminated by "
              f"non-filter factors. Identify the contaminating factor "
              f"(deploy schedule? basket emptiness? no-data day?) before "
              f"using is_active for any drift comparison.")
    print()
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())

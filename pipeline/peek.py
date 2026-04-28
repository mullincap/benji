#!/usr/bin/env python3
"""peek.py — single-day basket dry-run for the audit pipeline.

Usage:
    python pipeline/peek.py 2026-04-27
    python pipeline/peek.py 2026-04-27 --out /tmp/peek_2026-04-27.json
    python pipeline/peek.py 2026-04-27 --force-build

Mirrors the canonical (non --live-parity) basket selection that the audit's
overlap_analysis.py would produce for the given date. Prints the overlap of
top-20 price ∩ top-20 OI symbols across the [00:00, 06:00) UTC window.

Designed to be run between 06:00 and 06:35 UTC — after metl + indexer crons
have finished today's data, before the trader deploys at 06:35 — so you can
diff this basket against the one daily_signal_v2.py wrote to
live_deploys_signal.csv.

For past dates, market.leaderboards already has full coverage and peek skips
the rebuild. For today, peek will inline-build any missing metric via
build_intraday_leaderboard.py (price + OI, in parallel; ~2 min each).
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Set env vars BEFORE importing overlap_analysis so its module-level globals
# match the canonical audit configuration (job 235aa3d6 baseline).
os.environ.setdefault("INDEX_LOOKBACK", "6")
os.environ.setdefault("SORT_LOOKBACK", "6")
os.environ.setdefault("FREQ_CUTOFF", "20")
os.environ.setdefault("SAMPLE_INTERVAL", "5")
os.environ.setdefault("LEADERBOARD_TOP_N", "333")

# Make pipeline.* importable whether peek is run from repo root, from
# pipeline/, or as docker exec ... python /app/pipeline/peek.py.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

from pipeline import overlap_analysis as oa  # noqa: E402
from pipeline.db.connection import get_conn  # noqa: E402

import pandas as pd  # noqa: E402

INDEXER = _HERE / "indexer" / "build_intraday_leaderboard.py"

# Canonical knobs — match audit job 235aa3d6 params exactly.
FREQ_WIDTH = 20
FREQ_CUTOFF = 20
SAMPLE_INTERVAL = 5
MODE = "snapshot"
MIN_MCAP = 0.0
SORT_BY = "price"
OVERLAP_DIMENSIONS = "price_oi"
DEPLOYMENT_START_HOUR = 6  # also used as the freq window upper bound


def coverage_minutes(metric: str, peek_date: datetime.date) -> int:
    """Distinct-minute count in market.leaderboards for [00:00, deployment_start) UTC."""
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    start_dt = datetime.datetime.combine(peek_date, datetime.time(0, 0))
    end_dt = datetime.datetime.combine(peek_date, datetime.time(DEPLOYMENT_START_HOUR, 0))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT timestamp_utc)
        FROM market.leaderboards
        WHERE metric = %s
          AND anchor_hour = %s
          AND variant = 'close'
          AND timestamp_utc >= %s
          AND timestamp_utc <  %s
    """, (metric, anchor_hour, start_dt, end_dt))
    n = cur.fetchone()[0]
    cur.close()
    conn.close()
    return int(n or 0)


def spawn_indexer(metric: str, peek_date: datetime.date) -> subprocess.Popen:
    """Launch build_intraday_leaderboard.py for one metric, one date."""
    date_str = peek_date.isoformat()
    cmd = [
        sys.executable, str(INDEXER),
        "--metric", metric,
        "--source", "db",
        "--start", date_str,
        "--end", date_str,
        "--triggered-by", "cli",
        "--run-tag", f"peek_{date_str}",
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def latest_bar_in_window(peek_date: datetime.date) -> "datetime.datetime | None":
    """Most recent timestamp present for BOTH price and OI inside
    [00:00, deployment_start) UTC of peek_date. Used as the snapshot anchor
    when the deployment_start bar doesn't exist yet (intra-day peek)."""
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    start_dt = datetime.datetime.combine(peek_date, datetime.time(0, 0))
    end_dt = datetime.datetime.combine(peek_date, datetime.time(DEPLOYMENT_START_HOUR, 0))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(timestamp_utc) FROM (
            SELECT DISTINCT timestamp_utc FROM market.leaderboards
            WHERE metric = 'price' AND anchor_hour = %s AND variant = 'close'
              AND timestamp_utc >= %s AND timestamp_utc < %s
            INTERSECT
            SELECT DISTINCT timestamp_utc FROM market.leaderboards
            WHERE metric = 'open_interest' AND anchor_hour = %s AND variant = 'close'
              AND timestamp_utc >= %s AND timestamp_utc < %s
        ) t
    """, (anchor_hour, start_dt, end_dt, anchor_hour, start_dt, end_dt))
    ts = cur.fetchone()[0]
    cur.close()
    conn.close()
    return ts


def freq_at_exact_timestamp(metric: str, ts: "datetime.datetime",
                             peek_date: datetime.date) -> dict:
    """dict[date, Counter] for `metric` at exact timestamp `ts`. Same shape as
    oa._load_frequency_from_db so compute_overlap consumes it identically."""
    from collections import Counter
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.binance_id
        FROM market.leaderboards l
        JOIN market.symbols s ON s.symbol_id = l.symbol_id
        WHERE l.metric = %s
          AND l.anchor_hour = %s
          AND l.variant = 'close'
          AND l.rank <= %s
          AND l.timestamp_utc = %s
    """, (metric, anchor_hour, FREQ_WIDTH, ts))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    counter: Counter = Counter()
    for (raw_sym,) in rows:
        base = oa.normalize_symbol(raw_sym) if raw_sym else None
        if base is None:
            continue
        counter[base] += 1
    return {peek_date: counter}


def rank_table_at_timestamp(metric: str, ts: "datetime.datetime") -> list:
    """Return ordered top-FREQ_WIDTH ranks for metric at exact timestamp ts.

    Each entry: (rank, base, pct_change_decimal). Sorted by rank ascending.
    Used by --diagnostics to surface which symbols ranked where, before the
    intersection — so divergence vs daily_signal_v2's basket can be traced
    to a specific axis (OI granularity, snapshot timing, etc)."""
    anchor_hour = (DEPLOYMENT_START_HOUR - oa.INDEX_LOOKBACK) % 24
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT l.rank, s.binance_id, l.pct_change
        FROM market.leaderboards l
        JOIN market.symbols s ON s.symbol_id = l.symbol_id
        WHERE l.metric = %s
          AND l.anchor_hour = %s
          AND l.variant = 'close'
          AND l.rank <= %s
          AND l.timestamp_utc = %s
        ORDER BY l.rank
    """, (metric, anchor_hour, FREQ_WIDTH, ts))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    out = []
    for rank, raw_sym, pct in rows:
        base = oa.normalize_symbol(raw_sym) if raw_sym else None
        if base is None:
            continue
        out.append((int(rank), base, float(pct) if pct is not None else 0.0))
    return out


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("date", type=str,
                    help="Date YYYY-MM-DD whose basket to peek")
    ap.add_argument("--out", type=str, default=None,
                    help="Optional JSON output path for scripted diff against "
                         "live_deploys_signal.csv")
    ap.add_argument("--force-build", action="store_true",
                    help="Rebuild leaderboards even if [00:00, 06:00) coverage "
                         "is already complete")
    ap.add_argument("--diagnostics", action="store_true",
                    help="Print top-FREQ_WIDTH rank tables (price + OI) at the "
                         "snapshot timestamp, before the intersection. Useful for "
                         "diagnosing divergence vs daily_signal_v2's basket — "
                         "mark whether each top-ranked symbol survived the "
                         "intersection. Also adds a 'diagnostics' block to JSON "
                         "output if --out is set.")
    args = ap.parse_args()

    try:
        peek_dt = datetime.date.fromisoformat(args.date)
    except ValueError:
        ap.error(f"date must be YYYY-MM-DD, got {args.date!r}")
    if peek_dt > datetime.date.today() + datetime.timedelta(days=1):
        ap.error(f"date {peek_dt} is in the future; no data possible")

    expected = DEPLOYMENT_START_HOUR * 60  # 360 minutes for [00:00, 06:00)
    metrics = ["price", "open_interest"]

    # 1. Cache check
    print(f"[peek] target date: {peek_dt.isoformat()}  window: [00:00, "
          f"{DEPLOYMENT_START_HOUR:02d}:00) UTC")
    to_build = []
    for m in metrics:
        n = coverage_minutes(m, peek_dt)
        mark = "✓" if n >= expected else "✗"
        print(f"  coverage: {m:14s} {n:>4}/{expected} minutes {mark}")
        if args.force_build or n < expected:
            to_build.append(m)

    # 2. Inline build (parallel)
    build_t0 = time.time()
    if to_build:
        print(f"[peek] building {to_build} for {peek_dt} (parallel)")
        procs = {m: spawn_indexer(m, peek_dt) for m in to_build}
        failed = []
        for m, p in procs.items():
            out, _ = p.communicate()
            if p.returncode != 0:
                failed.append(m)
                sys.stderr.write(
                    f"[peek] {m} build FAILED rc={p.returncode}\n"
                    f"--- last 2000 chars of output ---\n"
                    f"{(out or '')[-2000:]}\n"
                )
            else:
                print(f"  build done: {m}")
        if failed:
            print(f"[peek] aborting — failed metrics: {failed}", file=sys.stderr)
            sys.exit(2)
    build_seconds = time.time() - build_t0

    # 3. Freq + overlap (canonical knobs, mirrors audit job 235aa3d6).
    # _load_frequency_from_db returns dict[date, Counter] for ALL dates in
    # the table; we filter to peek_dt in Python (cheap — snapshot-mode SQL
    # is the chunk-pruned 0.1s/call rewrite).
    freq_t0 = time.time()
    price_all = oa._load_frequency_from_db(
        "price", FREQ_WIDTH, SAMPLE_INTERVAL, MODE, min_mcap=MIN_MCAP,
    )
    oi_all = oa._load_frequency_from_db(
        "open_interest", FREQ_WIDTH, SAMPLE_INTERVAL, MODE, min_mcap=MIN_MCAP,
    )
    price_freq = {peek_dt: price_all.get(peek_dt, {})}
    oi_freq = {peek_dt: oi_all.get(peek_dt, {})}

    # Fallback: if the deployment_start bar isn't in market.leaderboards yet
    # (peek run before the window completes), snapshot from the latest bar
    # available in [00:00, deployment_start) instead. Useful for smoke tests
    # and intra-window dry runs.
    fallback_ts = None
    if not price_freq[peek_dt] and not oi_freq[peek_dt]:
        fallback_ts = latest_bar_in_window(peek_dt)
        if fallback_ts:
            print(f"  [peek] {DEPLOYMENT_START_HOUR:02d}:00 UTC bar not in "
                  f"market.leaderboards yet — falling back to latest available: "
                  f"{fallback_ts:%Y-%m-%d %H:%M:%S} UTC")
            price_freq = freq_at_exact_timestamp("price", fallback_ts, peek_dt)
            oi_freq = freq_at_exact_timestamp("open_interest", fallback_ts, peek_dt)

    overlap_df = oa.compute_overlap(
        price_freq, oi_freq,
        freq_cutoff=FREQ_CUTOFF,
        sort_by=SORT_BY,
        overlap_dimensions=OVERLAP_DIMENSIONS,
    )
    freq_seconds = time.time() - freq_t0

    row = overlap_df[overlap_df["date"] == pd.Timestamp(peek_dt)]
    syms = list(row["overlap_symbols"].iloc[0]) if len(row) else []

    # 4. Output
    print()
    snap_label = (
        f"snapshot @ {fallback_ts:%H:%M:%S} UTC (partial)"
        if fallback_ts
        else f"snapshot @ {DEPLOYMENT_START_HOUR:02d}:00:00 UTC"
    )
    print(f"[peek-date {peek_dt.isoformat()}] {len(syms)} symbols  ({snap_label})")
    print(f"  → {' '.join(syms) if syms else '(empty basket)'}")
    print(f"  build={build_seconds:.1f}s  freq={freq_seconds:.1f}s  "
          f"total={build_seconds + freq_seconds:.1f}s")

    # Diagnostics: rank table dump
    diag_block = None
    if args.diagnostics:
        diag_ts = fallback_ts or datetime.datetime.combine(
            peek_dt,
            datetime.time(DEPLOYMENT_START_HOUR, 0, 0),
            tzinfo=datetime.timezone.utc,
        )
        price_top = rank_table_at_timestamp("price", diag_ts)
        oi_top = rank_table_at_timestamp("open_interest", diag_ts)
        basket_set = set(syms)

        def _print_table(label, rows):
            print(f"\n[diagnostics] {label} top-{FREQ_WIDTH} @ "
                  f"{diag_ts:%Y-%m-%d %H:%M:%S} UTC  "
                  f"(★ = in basket)")
            if not rows:
                print(f"  (no rows in market.leaderboards at this timestamp)")
                return
            for rank, base, pct in rows:
                marker = "★" if base in basket_set else " "
                print(f"  {rank:>3}.  {base:<12}  {pct * 100:+8.3f}%  {marker}")

        _print_table("price", price_top)
        _print_table("open_interest", oi_top)

        diag_block = {
            "snapshot_timestamp_utc": diag_ts.isoformat(),
            "price_top": [
                {"rank": r, "base": b, "pct_change": pct,
                 "in_basket": b in basket_set}
                for r, b, pct in price_top
            ],
            "oi_top": [
                {"rank": r, "base": b, "pct_change": pct,
                 "in_basket": b in basket_set}
                for r, b, pct in oi_top
            ],
        }

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        json_payload = {
            "date": peek_dt.isoformat(),
            "symbols": syms,
            "basket_size": len(syms),
            "snapshot_timestamp_utc": (
                fallback_ts.isoformat()
                if fallback_ts
                else f"{peek_dt.isoformat()}T{DEPLOYMENT_START_HOUR:02d}:00:00"
            ),
            "is_partial": fallback_ts is not None,
            "build_seconds": round(build_seconds, 1),
            "freq_seconds": round(freq_seconds, 1),
            "run_config": {
                "freq_width": FREQ_WIDTH,
                "freq_cutoff": FREQ_CUTOFF,
                "mode": MODE,
                "deployment_start_hour": DEPLOYMENT_START_HOUR,
                "index_lookback": oa.INDEX_LOOKBACK,
                "sort_lookback": oa._resolve_sort_lookback(),
                "sample_interval": SAMPLE_INTERVAL,
                "min_mcap": MIN_MCAP,
                "overlap_dimensions": OVERLAP_DIMENSIONS,
                "sort_by": SORT_BY,
                "price_ranking_metric": "pct_change",
                "oi_ranking_metric": "pct_change",
            },
        }
        if diag_block is not None:
            json_payload["diagnostics"] = diag_block
        with open(out_path, "w") as f:
            json.dump(json_payload, f, indent=2)
        print(f"  wrote {out_path}")


if __name__ == "__main__":
    main()

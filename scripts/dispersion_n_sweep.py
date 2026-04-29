#!/usr/bin/env python3
"""dispersion_n_sweep.py — sweep dispersion_n for ALTS MAIN.

Submits a JobRequest per N value via the celery pipeline_worker, using
ALTS MAIN's stored strategy_versions.config as the base + an override
on dispersion_n. Polls celery + audit.jobs until all complete, then
pulls headline metrics from audit.results into a side-by-side table.

Run inside the celery container (where pipeline_worker + DB conn are
on path):

    docker compose exec -T celery python /tmp/dispersion_n_sweep.py

Options:
    --n-values 20,30,40,60,90,120     comma-separated sweep values
    --strategy alts_main              audit.strategies.name to base on
    --poll-interval 30                seconds between status checks
"""
from __future__ import annotations
import argparse
import sys
import time
import uuid

sys.path.insert(0, "/app")

from app.workers.pipeline_worker import run_pipeline
from app.services.job_store import create_job, update_job
from pipeline.db.connection import get_conn


def fetch_base_config(strategy_name: str) -> dict:
    """Pull the published strategy_version.config for the named strategy."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT sv.strategy_version_id::text, sv.config
          FROM audit.strategies s
          JOIN audit.strategy_versions sv ON sv.strategy_id = s.strategy_id
         WHERE s.is_published = TRUE AND s.name = %s
         LIMIT 1
    """, (strategy_name,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        raise RuntimeError(f"No published strategy_version for name={strategy_name!r}")
    return {"sv_id": row[0], "config": row[1] or {}}


def submit_one(base_params: dict, dispersion_n: int) -> str:
    """Override dispersion_n + write jobstore entry + enqueue celery task.
    Returns job_id. Mirrors what /api/jobs POST does so the simulator UI
    surfaces this audit just like a normal user submission."""
    params = dict(base_params)
    params["dispersion_n"] = int(dispersion_n)
    # Belt-and-suspenders: explicitly assert the dynamic-universe + mcap
    # source we want for this sweep (these should already be in the
    # base config from the lock-in earlier today, but make them
    # immune to surprises).
    params["dispersion_dynamic_universe"]    = True
    params["dispersion_universe_mode"]       = "all"
    params.setdefault("dispersion_universe_lag_days", 0)
    params.setdefault("dispersion_universe_strict_dynamic", True)

    job_id = str(uuid.uuid4())
    # 1. Write JSON jobstore entry — simulator UI's GET /api/jobs reads this
    create_job(job_id, params)
    # 2. Stamp a friendly display name so the UI list is readable
    update_job(
        job_id,
        display_name=f"sweep n={dispersion_n} (ALTS MAIN, strict dynamic)",
    )
    # 3. Enqueue celery task; record task_id once celery accepts it
    task = run_pipeline.delay(job_id, params)
    update_job(job_id, task_id=task.id)
    print(f"  → enqueue n={dispersion_n}  job_id={job_id[:8]}  task_id={task.id[:8]}",
          flush=True)
    return job_id


def fetch_metrics(job_id: str) -> dict | None:
    """Pull headline metrics from audit.results once the audit completes.
    Returns None if no results row exists yet."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT filter_mode, sharpe, sortino, cagr_pct, max_dd_pct,
                   calmar, win_rate_daily, avg_daily_ret_pct,
                   total_return_pct, grade, scorecard_score
              FROM audit.results WHERE job_id = %s::uuid
             ORDER BY filter_mode
        """, (job_id,))
        rows = cur.fetchall()
        if not rows:
            return None
        # ALTS MAIN uses 'A - Tail + Dispersion' as its active_filter — pick
        # that row; fall back to the highest-Sharpe row when the filter
        # mode label isn't recorded.
        target = None
        for r in rows:
            if r[0] == "A - Tail + Dispersion":
                target = r; break
        if target is None:
            target = max(rows, key=lambda r: r[1] or 0)
        return {
            "filter_mode": target[0],
            "sharpe":      target[1],
            "sortino":     target[2],
            "cagr_pct":    target[3],
            "max_dd_pct":  target[4],
            "calmar":      target[5],
            "win_rate":    target[6],
            "avg_daily":   target[7],
            "total_ret":   target[8],
            "grade":       target[9],
            "score":       target[10],
        }
    finally:
        cur.close(); conn.close()


def fetch_status(job_id: str) -> str:
    """Pull audit.jobs.status if a row exists, else 'pending' (celery worker
    writes the row at completion)."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM audit.jobs WHERE job_id = %s::uuid", (job_id,))
        row = cur.fetchone()
        return row[0] if row else "pending"
    finally:
        cur.close(); conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n-values", type=str, default="20,30,40,60,90,120",
                    help="comma-separated dispersion_n values")
    ap.add_argument("--strategy", type=str, default="alts_main",
                    help="audit.strategies.name to base config on")
    ap.add_argument("--poll-interval", type=int, default=30,
                    help="seconds between status polls")
    ap.add_argument("--max-wait-min", type=int, default=180,
                    help="abort if audits haven't finished after N minutes")
    args = ap.parse_args()

    n_values = [int(x) for x in args.n_values.split(",")]
    base = fetch_base_config(args.strategy)
    base_cfg = base["config"]

    print(f"{'='*78}")
    print(f"  GRID SEARCH SUMMARY — dispersion_n sweep")
    print(f"{'='*78}")
    print(f"  Sweeping over:    dispersion_n ∈ {n_values}  ({len(n_values)} runs)")
    print(f"  Base strategy:    {args.strategy}  (sv_id {base['sv_id'][:8]})")
    print(f"")
    print(f"  Held constant across all runs (from stored config + sweep overrides):")
    print(f"    active_filter:                  {base_cfg.get('active_filter')!r}")
    print(f"    dispersion_threshold:           {base_cfg.get('dispersion_threshold')}")
    print(f"    dispersion_baseline_win:        {base_cfg.get('dispersion_baseline_win')} days")
    print(f"    dispersion_universe_mode:       {base_cfg.get('dispersion_universe_mode')!r} (full mcap table)")
    print(f"    dispersion_universe_lag_days:   {base_cfg.get('dispersion_universe_lag_days')}")
    print(f"    dispersion_universe_strict_dynamic: True (forced)")
    print(f"    dispersion_dynamic_universe:    True (forced)")
    print(f"    tail_drop_pct / vol_mult:       {base_cfg.get('tail_drop_pct')} / {base_cfg.get('tail_vol_mult')}")
    print(f"    leverage:                       {base_cfg.get('leverage')}")
    print(f"    port_sl / port_tsl:             {base_cfg.get('port_sl')} / {base_cfg.get('port_tsl')}")
    print(f"    early_kill_y:                   {base_cfg.get('early_kill_y')}")
    print(f"")
    print(f"  Each run varies ONLY dispersion_n (top-N mcap symbols per day).")
    print(f"  Universe = union of all distinct tickers ever in lagged top-N")
    print(f"  across the audit window, queried from market.market_cap_daily.")
    print(f"  Visible in simulator UI under display_name = 'sweep n=X (ALTS MAIN, strict dynamic)'.")
    print(f"{'='*78}\n")

    # Submit all jobs upfront. With celery concurrency=2, two run in parallel;
    # the rest queue and process FIFO. ~10 min per audit.
    job_map: dict[int, str] = {}
    for n in n_values:
        job_map[n] = submit_one(base_cfg, n)

    print(f"\nSubmitted {len(job_map)} jobs. Polling every {args.poll_interval}s "
          f"(max wait {args.max_wait_min}min)...\n")

    start = time.time()
    while True:
        elapsed = time.time() - start
        if elapsed / 60 > args.max_wait_min:
            print(f"  ⚠ {args.max_wait_min}min wait elapsed; some audits may "
                  f"still be running. Continuing with what we have.")
            break
        statuses = {n: fetch_status(jid) for n, jid in job_map.items()}
        done = sum(1 for s in statuses.values() if s == "complete")
        line = "  ".join(f"n={n}:{s[:6]}" for n, s in statuses.items())
        print(f"  [{elapsed/60:5.1f}min] {done}/{len(job_map)} done  |  {line}",
              flush=True)
        if done == len(job_map):
            print(f"\n  All audits complete in {elapsed/60:.1f} min")
            break
        time.sleep(args.poll_interval)

    # Pull metrics + tabulate
    print(f"\n{'='*98}")
    print(f"  Dispersion N sweep — base={args.strategy}, "
          f"filter='Tail + Dispersion', strict_dynamic universe")
    print(f"{'='*98}")
    hdr = (f"  {'N':>4}  {'Sharpe':>7}  {'Sortino':>8}  {'CAGR':>9}  "
           f"{'MaxDD':>9}  {'Calmar':>7}  {'WinRate':>8}  {'AvgDay':>9}  "
           f"{'TotalRet':>10}  {'Grade':>6}  {'Score':>5}")
    print(hdr)
    print(f"  {'-'*4}  {'-'*7}  {'-'*8}  {'-'*9}  {'-'*9}  {'-'*7}  "
          f"{'-'*8}  {'-'*9}  {'-'*10}  {'-'*6}  {'-'*5}")
    rows: list[tuple[int, dict]] = []
    for n in n_values:
        m = fetch_metrics(job_map[n])
        if m is None:
            print(f"  {n:>4}  (no audit.results — job {job_map[n][:8]} status "
                  f"= {fetch_status(job_map[n])})")
            continue
        rows.append((n, m))
        f = lambda v, fmt: "—" if v is None else fmt.format(v)
        print(f"  {n:>4}  {f(m['sharpe'], '{:7.3f}'):>7}  "
              f"{f(m['sortino'], '{:8.3f}'):>8}  "
              f"{f(m['cagr_pct'], '{:8.2f}%'):>9}  "
              f"{f(m['max_dd_pct'], '{:8.2f}%'):>9}  "
              f"{f(m['calmar'], '{:7.3f}'):>7}  "
              f"{f(m['win_rate'], '{:7.2f}%'):>8}  "
              f"{f(m['avg_daily'], '{:8.4f}%'):>9}  "
              f"{f(m['total_ret'], '{:9.2f}%'):>10}  "
              f"{f(m['grade'], '{}'):>6}  "
              f"{f(m['score'], '{:5}'):>5}")

    if rows:
        # Highlight winners
        best_sharpe = max(rows, key=lambda r: r[1]["sharpe"] or -1e9)
        best_calmar = max(rows, key=lambda r: r[1]["calmar"] or -1e9)
        best_score  = max(rows, key=lambda r: r[1]["score"] or -1e9)
        print(f"\n  ★ Best Sharpe:  N={best_sharpe[0]}  ({best_sharpe[1]['sharpe']:.3f})")
        print(f"  ★ Best Calmar:  N={best_calmar[0]}  ({best_calmar[1]['calmar']:.3f})")
        print(f"  ★ Best Score:   N={best_score[0]}  ({best_score[1]['score']})")

    print(f"\nJob IDs (for follow-up SQL):")
    for n, jid in job_map.items():
        print(f"  n={n:>3}: {jid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

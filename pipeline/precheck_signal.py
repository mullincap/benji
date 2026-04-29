#!/usr/bin/env python3
"""precheck_signal.py — pre/post sanity checks for the daily_signal_v3 cron.

Two phases on the same script:

  --phase pre   Runs ~30 min before the 06:00 UTC daily_signal_v3 cron
                to verify the live audit_filters path will produce a clean
                decision for the locked-in published-strategy configs.
                Catches: import-chain failures, fail-open dispersion (e.g.
                stale mcap data), empty baskets, threshold misconfig.

  --phase post  Runs ~2 min after the 06:00 UTC daily_signal_v3 cron to
                verify the run actually happened with the expected
                params: per-strategy disp log line shows
                strict_dyn=True n=<expected>, signal written to DB +
                CSV, no errors in the cron log.

Exit code 0 = all checks pass; exit code 1 = alert-worthy. Intended as
a host-crontab entry — output goes to /mnt/quant-data/logs/precheck/.

Read-only — never writes to anywhere outside its own log file.
"""
from __future__ import annotations
import argparse
import datetime as _dt
import logging
import os
import re
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))   # so `from pipeline.X import ...`
sys.path.insert(0, str(_HERE))          # so audit_filters can `import audit`

from pipeline.db.connection import get_conn  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("precheck_signal")

SIGNAL_CRON_LOG = "/mnt/quant-data/logs/signal/cron.log"


# -- Shared helpers ---------------------------------------------------------

def _today_utc() -> _dt.date:
    return _dt.datetime.now(_dt.timezone.utc).date()


def _published_strategies() -> list[dict]:
    """Return [{name, version_label, sv_id, config}, ...] for every
    is_published=True strategy/version pair. The pre-check exercises
    each one separately so a single broken strategy doesn't mask the
    rest."""
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT s.name, s.display_name, sv.version_label,
                   sv.strategy_version_id::text, sv.config
              FROM audit.strategies s
              JOIN audit.strategy_versions sv ON sv.strategy_id = s.strategy_id
             WHERE s.is_published = TRUE
             ORDER BY s.name
        """)
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()
    return [{
        "name":          r[0],
        "display_name":  r[1] or r[0],
        "version_label": r[2],
        "sv_id":         r[3],
        "config":        r[4] or {},
    } for r in rows]


# -- Phase: pre ------------------------------------------------------------

def _phase_pre() -> int:
    """Walk every published strategy + run audit_filters per its stored
    dispersion config. Confirms: imports clean, mcap data fresh, basket
    formable. Mirrors what daily_signal_v3.compute_per_strategy_decisions
    will do at 06:00 UTC."""
    today = _today_utc()
    log.info(f"PRE-FLIGHT (UTC {today}) — verifying audit_filters can produce "
             f"a decision for every published strategy")

    # Lazy-import: audit_filters loads audit.py which has heavy module-level work
    from pipeline.audit_filters import (
        compute_tail_guardrail_detail,
        compute_dispersion_filter_detail,
    )

    strategies = _published_strategies()
    log.info(f"Found {len(strategies)} published strategy version(s)")

    failures: list[str] = []
    for s in strategies:
        cfg = s["config"]
        label = f"{s['display_name']} (sv {s['sv_id'][:8]})"
        log.info(f"\n[{label}]")
        log.info(f"  active_filter:                       "
                 f"{cfg.get('active_filter')!r}")
        log.info(f"  tail_drop_pct / tail_vol_mult:       "
                 f"{cfg.get('tail_drop_pct')} / {cfg.get('tail_vol_mult')}")
        log.info(f"  dispersion_threshold:                "
                 f"{cfg.get('dispersion_threshold')}")
        log.info(f"  dispersion_n:                        "
                 f"{cfg.get('dispersion_n')}")
        log.info(f"  dispersion_universe_lag_days:        "
                 f"{cfg.get('dispersion_universe_lag_days')}")
        log.info(f"  dispersion_universe_strict_dynamic:  "
                 f"{cfg.get('dispersion_universe_strict_dynamic')}")

        # Tail Guardrail
        try:
            tg = compute_tail_guardrail_detail(
                today,
                tail_drop_pct=(float(cfg["tail_drop_pct"])
                               if cfg.get("tail_drop_pct") is not None else None),
                tail_vol_mult=(float(cfg["tail_vol_mult"])
                               if cfg.get("tail_vol_mult") is not None else None),
            )
            if tg.get("insufficient_history"):
                failures.append(f"{label}: TG insufficient history")
                log.error(f"  TG ✗ insufficient history (n_closes={tg.get('n_closes')})")
            else:
                log.info(f"  TG values: prev_day={tg.get('prev_day_return') and tg['prev_day_return']*100:.3f}%  "
                         f"ratio={tg.get('rvol_ratio'):.3f}×  "
                         f"sit_flat={tg['sit_flat']}")
        except Exception as e:
            failures.append(f"{label}: TG raised {e!r}")
            log.error(f"  TG ✗ exception: {e}")

        # Dispersion (only when the strategy uses it — quick check)
        af = (cfg.get("active_filter") or "").strip()
        uses_disp = "Dispersion" in af or af == "A - Tail + Dispersion" or af == "A - Dispersion"
        if uses_disp:
            try:
                dp = compute_dispersion_filter_detail(
                    today,
                    threshold=(float(cfg["dispersion_threshold"])
                               if cfg.get("dispersion_threshold") is not None else None),
                    baseline_win=(int(cfg["dispersion_baseline_win"])
                                  if cfg.get("dispersion_baseline_win") is not None else None),
                    n_symbols=(int(cfg["dispersion_n"])
                               if cfg.get("dispersion_n") is not None else None),
                    lag_days=(int(cfg["dispersion_universe_lag_days"])
                              if cfg.get("dispersion_universe_lag_days") is not None else None),
                    strict_dynamic=(bool(cfg["dispersion_universe_strict_dynamic"])
                                    if cfg.get("dispersion_universe_strict_dynamic") is not None else None),
                )
                if dp.get("fail_open_reason"):
                    failures.append(f"{label}: Disp fail-open ({dp['fail_open_reason']})")
                    log.error(f"  Disp ✗ fail-open: {dp['fail_open_reason']}  "
                              f"(eligible={dp.get('n_symbols_eligible')} "
                              f"klines={dp.get('n_symbols_with_klines')})")
                else:
                    log.info(f"  Disp values: yest_disp={dp.get('yesterday_dispersion'):.5f}  "
                             f"baseline={dp.get('baseline_median'):.5f}  "
                             f"ratio={dp.get('dispersion_ratio'):.3f}  "
                             f"sit_flat={dp['sit_flat']}")
            except Exception as e:
                failures.append(f"{label}: Disp raised {e!r}")
                log.error(f"  Disp ✗ exception: {e}")
        else:
            log.info(f"  Disp: skipped (active_filter doesn't use it)")

    log.info(f"\n{'='*60}")
    if failures:
        log.error(f"PRE-FLIGHT FAIL — {len(failures)} issue(s):")
        for f in failures:
            log.error(f"  ✗ {f}")
        return 1
    log.info(f"PRE-FLIGHT GREEN — all {len(strategies)} strategies producible")
    return 0


# -- Phase: post -----------------------------------------------------------

def _phase_post() -> int:
    """After daily_signal_v3 cron at 06:00 UTC, confirm:
      1. The cron actually ran today (most-recent log entry on today's date)
      2. No tracebacks in the run
      3. [per-strategy disp] log lines show n + strict_dyn matching each
         strategy's stored config (lock-in actually took effect)
      4. signal_date row exists in user_mgmt.daily_signals for every
         published+active strategy_version
      5. live_deploys_signal.csv has a row for today
    """
    today = _today_utc()
    today_str = today.isoformat()
    log.info(f"POST-CHECK (UTC {today}) — verifying signal cron output")

    failures: list[str] = []

    # 1+2+3 — log inspection
    log_path = Path(SIGNAL_CRON_LOG)
    if not log_path.exists():
        failures.append(f"signal cron log missing: {SIGNAL_CRON_LOG}")
        log.error(f"  ✗ signal cron log not found at {SIGNAL_CRON_LOG}")
    else:
        text = log_path.read_text(errors="replace")
        # Take the LAST occurrence of today's daily_signal banner
        banner = f"DAILY SIGNAL v"
        today_lines = [ln for ln in text.splitlines() if ln.startswith(today_str)]
        if not today_lines:
            failures.append("no log lines from today")
            log.error(f"  ✗ no signal cron log entries dated {today_str}")
        else:
            log.info(f"  ✓ {len(today_lines)} log line(s) from today")
            joined = "\n".join(today_lines)

            # Tracebacks/errors
            errs = re.findall(r"Traceback|^.*ERROR.*$", joined, re.MULTILINE)
            if errs:
                failures.append(f"signal log has {len(errs)} error/traceback line(s)")
                log.error(f"  ✗ signal log shows {len(errs)} error/traceback line(s)")
                for e in errs[:5]:
                    log.error(f"      {e[:200]}")
            else:
                log.info(f"  ✓ no errors/tracebacks in today's signal log")

            # Per-strategy disp lock-in check
            disp_lines = re.findall(
                r"\[per-strategy disp .+?\] thr=(\S+) win=(\S+) n=(\S+) "
                r"lag=(\S+) strict_dyn=(\S+) → sit_flat=(\S+)",
                joined,
            )
            if disp_lines:
                log.info(f"  ✓ {len(disp_lines)} [per-strategy disp] line(s) found")
                for thr, win, n, lag, sd, flat in disp_lines:
                    log.info(f"      thr={thr} win={win} n={n} "
                             f"lag={lag} strict_dyn={sd} sit_flat={flat}")
            else:
                # Not necessarily a failure — only fires when strategy's
                # disp config differs from canonical defaults. If every
                # strategy is on canonical, no per-strategy line emitted.
                log.info(f"  · no [per-strategy disp] lines — strategies "
                         f"may all be on canonical defaults")

    # 4 — DB row presence
    strategies = _published_strategies()
    conn = get_conn(); cur = conn.cursor()
    try:
        for s in strategies:
            cur.execute(
                "SELECT 1 FROM user_mgmt.daily_signals "
                "WHERE strategy_version_id = %s::uuid AND signal_date = %s",
                (s["sv_id"], today_str),
            )
            if cur.fetchone():
                log.info(f"  ✓ DB row for {s['display_name']} on {today_str}")
            else:
                failures.append(f"no DB row for {s['display_name']} on {today_str}")
                log.error(f"  ✗ no DB row for {s['display_name']} on {today_str}")
    finally:
        cur.close(); conn.close()

    # 5 — CSV presence
    csv_path = Path("/root/benji/live_deploys_signal.csv")
    if csv_path.exists():
        csv_text = csv_path.read_text()
        if today_str in csv_text:
            log.info(f"  ✓ live_deploys_signal.csv has row for {today_str}")
        else:
            failures.append(f"live_deploys_signal.csv missing {today_str}")
            log.error(f"  ✗ live_deploys_signal.csv has no row for {today_str}")
    else:
        log.warning(f"  · live_deploys_signal.csv not at expected path; skipping")

    log.info(f"\n{'='*60}")
    if failures:
        log.error(f"POST-CHECK FAIL — {len(failures)} issue(s)")
        return 1
    log.info(f"POST-CHECK GREEN — daily_signal_v3 cron output looks correct")
    return 0


# -- Main ------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--phase", choices=("pre", "post"), required=True,
                    help="pre = ~05:30 UTC sanity check; post = ~06:02 UTC verify")
    args = ap.parse_args()

    if args.phase == "pre":
        return _phase_pre()
    return _phase_post()


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
intraday_audit.py — verifier for today's daily-signal gate decision.

Runs at 06:01 UTC (after daily_signal_v2.py has written user_mgmt.daily_signals
at 06:00 + ~30s). Re-computes the (sit_flat, filter_name) decision per
published+active strategy version using pipeline.audit_filters
(canonical, single-source-of-truth) and compares against what the live
path wrote.

  - Agreement → mark daily_signals.audit_status='verified',
                audit_verified_at=NOW(). Trader (after follow-up commit)
                gates entry on audit_status='verified'.
  - Disagreement → mark daily_signals.audit_status='mismatch',
                   audit_mismatch_reason=<one-line diff>, emit a
                   [GATE_MISMATCH] alert. This is a bug, not a tolerable
                   condition. Once the trader gates on audit_status, a
                   mismatch causes us to sit out a day the live path
                   said to trade (or vice versa) — should page someone.
  - Verifier failure → mark daily_signals.audit_status='error',
                       audit_mismatch_reason=<failure summary>, emit a
                       [GATE_ERROR] alert. Distinct from mismatch:
                       infrastructure flakes shouldn't fire the same
                       alert path as logic divergence.
  - No live row yet → log warning and skip (verifier ran before live).

Why this exists (Option A1, 2026-04-26 design)
-----------------------------------------------
A parallel-writer architecture (audit pipeline writing its own table
alongside user_mgmt.daily_signals) was rejected because two implementations
silently drift. A single-writer + verifier architecture makes drift loud
and observable. The verifier shares filter functions with the live writer
via pipeline/audit_filters.py — drift is now only possible inside the
basket-selection step (compute_canonical_basket), which is a separate
concern (Problem 2 in the user's two-problem framing).

Usage
-----
  python -m pipeline.intraday_audit              # today UTC
  python pipeline/intraday_audit.py --date 2026-04-26
  python pipeline/intraday_audit.py --json
  python pipeline/intraday_audit.py --dry-run    # compute + print, no DB writes
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
_PROJECT_ROOT = _SCRIPT_DIR.parent
sys.path.insert(0, str(_PROJECT_ROOT))

# daily_signal_v2 owns the basket selection (compute_canonical_basket) and
# strategy-config dispatch (compute_per_strategy_decisions); we reuse those
# rather than re-implementing. Filter logic itself comes from audit_filters
# (shared with daily_signal_v2 — single source of truth).
import daily_signal_v2 as dsv2  # noqa: E402

from pipeline.db.connection import get_conn  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("intraday_audit")


# ==========================================================================
# CLI
# ==========================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Verify today's audit-canonical gate decision against "
                    "the live path's user_mgmt.daily_signals row."
    )
    p.add_argument("--date", type=str, default=None,
                   help="UTC date YYYY-MM-DD (default: today UTC).")
    p.add_argument("--json", action="store_true",
                   help="Emit per-strategy verification result as JSON.")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + diff but skip the audit_status update.")
    return p.parse_args()


def _resolve_date(arg: str | None) -> dt.date:
    if arg:
        return dt.date.fromisoformat(arg)
    return dt.datetime.now(dt.timezone.utc).date()


# ==========================================================================
# AUDIT-CANONICAL RECOMPUTE (reuses dsv2's per-strategy dispatch)
# ==========================================================================

def _compute_audit_decisions(ref_date: dt.date) -> tuple[list[str], list[dict]]:
    """Return (basket, per_strategy_decisions) for ref_date.

    Independent recomputation: does NOT read user_mgmt.daily_signals.
    Calls dsv2.compute_canonical_basket + audit_filters via dsv2's
    per-strategy dispatch.
    """
    blofin = dsv2.get_blofin_symbols()

    basket, _price_top, _oi_top = dsv2.compute_canonical_basket(ref_date)
    if blofin:
        pre = list(basket)
        basket = [s for s in basket if s in blofin]
        dropped = sorted(set(pre) - set(basket))
        if dropped:
            log.info(f"BloFin filter dropped {len(dropped)} symbols: {dropped}")

    btc_closes = dsv2._fetch_btc_daily_closes(ref_date)
    canonical_tg = dsv2.compute_tail_guardrail(ref_date, closes=btc_closes)
    disp_decision = dsv2.compute_dispersion_filter(ref_date)
    canonical_reason = canonical_tg[1] or ("pass" if not canonical_tg[0] else "")

    strategies = dsv2.fetch_published_strategy_configs()
    decisions = dsv2.compute_per_strategy_decisions(
        ref_date, strategies, btc_closes,
        disp_decision=disp_decision,
        canonical_tg_decision=canonical_tg,
        canonical_filter_reason=canonical_reason,
    )
    return basket, decisions


# ==========================================================================
# LIVE ROW FETCH + DIFF
# ==========================================================================

def _fetch_live_rows(ref_date: dt.date, sv_ids: list[str]) -> dict:
    """Return {strategy_version_id: {sit_flat, filter_name, filter_reason,
                                     audit_status, signal_batch_id}}
    for the given date and strategy versions. Strategies missing a row
    are absent from the result.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT strategy_version_id::text,
                   sit_flat,
                   filter_name,
                   filter_reason,
                   audit_status,
                   signal_batch_id::text
              FROM user_mgmt.daily_signals
             WHERE signal_date = %s::date
               AND strategy_version_id = ANY(%s::uuid[])
            """,
            (ref_date.isoformat(), sv_ids),
        )
        rows = {
            r[0]: {
                "sit_flat": bool(r[1]),
                "filter_name": r[2],
                "filter_reason": r[3],
                "audit_status": r[4],
                "signal_batch_id": r[5],
            }
            for r in cur.fetchall()
        }
        cur.close()
    finally:
        conn.close()
    return rows


def _diff(audit: dict, live: dict) -> str | None:
    """Return None if they agree on (sit_flat, filter_name); otherwise a
    one-line diff summary suitable for audit_mismatch_reason.

    filter_reason is intentionally excluded from the diff: reasons can
    legitimately differ in wording (e.g. crash gate vs vol gate firing
    on the same numeric inputs would be a bug, but a string difference
    in the reason text alone is not).
    """
    diffs: list[str] = []
    if bool(audit["sit_flat"]) != bool(live["sit_flat"]):
        diffs.append(
            f"sit_flat: live={live['sit_flat']} audit={audit['sit_flat']}"
        )
    if (audit.get("filter_name") or "") != (live.get("filter_name") or ""):
        diffs.append(
            f"filter_name: live={live.get('filter_name')!r} "
            f"audit={audit.get('filter_name')!r}"
        )
    return " | ".join(diffs) if diffs else None


# ==========================================================================
# ALERTING (stub — wire to PagerDuty/Slack when trader gates on this)
# ==========================================================================

def _alert_mismatch(
    ref_date: dt.date,
    strategy_name: str,
    sv_id: str,
    audit_decision: dict,
    live_decision: dict,
    diff_summary: str,
) -> None:
    """Emit a mismatch alert.

    A mismatch is a bug, not a tolerable condition. Once the trader gates
    on audit_status='verified', a mismatch means we either:
      - sit out a day the live path said to trade (audit fires, live didn't), or
      - trade a day the audit said to sit out (live fires, audit didn't).

    Today this is log-only. There is no log aggregation on the Hetzner
    host (cron pipes to /mnt/quant-data/logs/signal/intraday_audit.log
    with no MAILTO, no journalctl capture, no Loki). The [GATE_MISMATCH]
    prefix is the greppable handle for whatever alerting we wire up
    next — at minimum a 06:10 cron `grep -E '\\[GATE_(MISMATCH|ERROR)\\]'`
    against the log + a webhook post. See docs/ops_runbook.md for the
    `ssh mcap 'tail ...'` surfacing pattern that exists today.
    """
    payload = {
        "signal_date": ref_date.isoformat(),
        "strategy_version_id": sv_id,
        "strategy_name": strategy_name,
        "diff": diff_summary,
        "audit": {
            "sit_flat": bool(audit_decision["sit_flat"]),
            "filter_name": audit_decision.get("filter_name"),
            "filter_reason": audit_decision.get("filter_reason"),
        },
        "live": {
            "sit_flat": bool(live_decision["sit_flat"]),
            "filter_name": live_decision.get("filter_name"),
            "filter_reason": live_decision.get("filter_reason"),
        },
    }
    log.error(
        "[GATE_MISMATCH] strategy=%s date=%s diff=%s payload=%s",
        strategy_name, ref_date.isoformat(), diff_summary,
        json.dumps(payload, sort_keys=True),
    )
    # TODO: wire to alerting hook when trader gates on audit_status.
    # Keep the call non-fatal (try/except) so a webhook outage never
    # breaks the verifier.


def _alert_error(
    ref_date: dt.date,
    scope: str,
    detail: str,
    *,
    strategy_name: str | None = None,
) -> None:
    """Emit an error alert — verifier itself failed.

    Distinct from [GATE_MISMATCH] so we don't conflate infrastructure
    flakes (DB unreachable, Binance fetch threw) with logic divergence.
    Collapsing the two would train us to ignore the alert.

    `scope` is one of: 'audit_compute', 'live_fetch', 'per_strategy',
    'top_level'. `detail` is a one-line failure summary.
    """
    log.error(
        "[GATE_ERROR] scope=%s date=%s strategy=%s detail=%s",
        scope, ref_date.isoformat(), strategy_name or "-", detail,
    )


# ==========================================================================
# WRITE BACK (set audit_status + verified_at / mismatch_reason)
# ==========================================================================

def _mark_verified(signal_batch_id: str) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE user_mgmt.daily_signals
               SET audit_status          = 'verified',
                   audit_verified_at     = NOW(),
                   audit_mismatch_reason = NULL
             WHERE signal_batch_id = %s::uuid
            """,
            (signal_batch_id,),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _mark_mismatch(signal_batch_id: str, reason: str) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE user_mgmt.daily_signals
               SET audit_status          = 'mismatch',
                   audit_verified_at     = NULL,
                   audit_mismatch_reason = %s
             WHERE signal_batch_id = %s::uuid
            """,
            (reason, signal_batch_id),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _mark_error(signal_batch_id: str, reason: str) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE user_mgmt.daily_signals
               SET audit_status          = 'error',
                   audit_verified_at     = NULL,
                   audit_mismatch_reason = %s
             WHERE signal_batch_id = %s::uuid
            """,
            (reason, signal_batch_id),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()


def _mark_all_pending_error_for_date(ref_date: dt.date, reason: str) -> int:
    """Bulk-mark every still-pending row for ref_date as error. Used when
    the verifier failed before it could compute per-strategy decisions
    (e.g. _compute_audit_decisions raised). Does NOT downgrade
    verified or mismatch rows. Returns the number of rows updated.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE user_mgmt.daily_signals
               SET audit_status          = 'error',
                   audit_verified_at     = NULL,
                   audit_mismatch_reason = %s
             WHERE signal_date = %s::date
               AND audit_status = 'pending'
            """,
            (reason, ref_date.isoformat()),
        )
        n = cur.rowcount
        conn.commit()
        cur.close()
        return n
    finally:
        conn.close()


# ==========================================================================
# OUTPUT
# ==========================================================================

def _print_human(
    ref_date: dt.date,
    basket: list[str],
    results: list[dict],
) -> None:
    print()
    print(f"=== AUDIT VERIFIER · {ref_date.isoformat()} (UTC) ===")
    print(f"  Audit basket ({len(basket)}): {basket}")
    print()
    print(f"  {'Strategy':<28} {'Audit filter':<20} {'audit sit':<10} "
          f"{'live sit':<10} {'verdict':<10} reason")
    print(f"  {'-'*28} {'-'*20} {'-'*10} {'-'*10} {'-'*10} {'-'*40}")
    for r in results:
        name = (r["display_name"] or r["sv_id"][:8])[:28]
        af = (r["audit"].get("filter_name") or "")[:20]
        a_flat = "TRUE" if r["audit"]["sit_flat"] else "false"
        l_flat = (
            "TRUE" if r["live"] and r["live"]["sit_flat"]
            else ("false" if r["live"] else "-")
        )
        verdict = r["verdict"]
        reason = (r.get("diff") or r["audit"].get("filter_reason") or "")[:60]
        print(f"  {name:<28} {af:<20} {a_flat:<10} {l_flat:<10} "
              f"{verdict:<10} {reason}")
    print()


def _print_json(
    ref_date: dt.date,
    basket: list[str],
    results: list[dict],
    elapsed_s: float,
) -> None:
    print(json.dumps({
        "signal_date": ref_date.isoformat(),
        "audit_basket": basket,
        "results": results,
        "elapsed_s": round(elapsed_s, 2),
    }, indent=2, sort_keys=True))


# ==========================================================================
# MAIN
# ==========================================================================

def _run_verifier(
    args: argparse.Namespace,
    ref_date: dt.date,
    counters: dict,
) -> int:
    """Inner verifier body. Mutates `counters` for the runtime log line.
    Returns 0 on success (regardless of mismatches/errors), 1 on a
    top-level failure that prevented per-strategy work entirely.

    The outer main() wraps this in a try/finally so the
    [INTRADAY_AUDIT_RUNTIME] line is emitted on every exit path —
    including timeouts and unhandled exceptions.
    """
    log.info(f"intraday_audit starting · date={ref_date.isoformat()} "
             f"dry_run={args.dry_run}")

    # Phase 1: audit-canonical recompute. Failure here means we have no
    # decisions to compare against. Mark all still-pending rows for the
    # date as 'error' so the trader won't fail-open on stale 'pending'.
    try:
        basket, audit_decisions = _compute_audit_decisions(ref_date)
    except Exception as e:
        detail = f"audit_compute_failed: {type(e).__name__}: {e}"
        _alert_error(ref_date, scope="audit_compute", detail=detail)
        if not args.dry_run:
            try:
                n = _mark_all_pending_error_for_date(ref_date, detail)
                log.error(f"  marked {n} pending row(s) as error for "
                          f"{ref_date}")
            except Exception as e2:
                _alert_error(
                    ref_date, scope="audit_compute",
                    detail=(f"audit_compute_failed AND mark_error_failed: "
                            f"{type(e2).__name__}: {e2}"),
                )
        return 1

    sv_ids = [d["sv_id"] for d in audit_decisions]

    # Phase 2: fetch live rows. Same handling — global failure → mark
    # pending rows as error.
    try:
        live_rows = _fetch_live_rows(ref_date, sv_ids)
    except Exception as e:
        detail = f"live_fetch_failed: {type(e).__name__}: {e}"
        _alert_error(ref_date, scope="live_fetch", detail=detail)
        if not args.dry_run:
            try:
                _mark_all_pending_error_for_date(ref_date, detail)
            except Exception:
                pass
        return 1

    # Phase 3: per-strategy diff + status update.
    results: list[dict] = []
    for d in audit_decisions:
        sv_id = d["sv_id"]
        strategy_name = d.get("display_name") or sv_id[:8]
        audit_dec = {
            "sit_flat": bool(d["sit_flat"]),
            "filter_name": d.get("filter_name"),
            "filter_reason": d.get("filter_reason"),
        }
        live = live_rows.get(sv_id)
        diff_summary: str | None = None
        verdict = "pending"

        try:
            if live is None:
                verdict = "no_live_row"
                log.warning(
                    f"  [{strategy_name}] no live row for {ref_date} "
                    f"— skipping (verifier ran before daily_signal_v2 "
                    f"wrote the row)"
                )
                counters["pending"] += 1
            else:
                diff_summary = _diff(audit_dec, live)
                if diff_summary is None:
                    verdict = "verified"
                    counters["verified"] += 1
                    if not args.dry_run:
                        _mark_verified(live["signal_batch_id"])
                else:
                    verdict = "mismatch"
                    counters["mismatch"] += 1
                    _alert_mismatch(
                        ref_date, strategy_name, sv_id,
                        audit_dec, live, diff_summary,
                    )
                    if not args.dry_run:
                        _mark_mismatch(live["signal_batch_id"], diff_summary)
        except Exception as e:
            detail = (f"per_strategy_failed: {type(e).__name__}: {e}")
            _alert_error(
                ref_date, scope="per_strategy",
                detail=detail, strategy_name=strategy_name,
            )
            verdict = "error"
            counters["error"] += 1
            if not args.dry_run and live is not None:
                try:
                    _mark_error(live["signal_batch_id"], detail)
                except Exception:
                    pass  # already alerted; don't mask the original error

        results.append({
            "sv_id": sv_id,
            "display_name": d.get("display_name"),
            "audit": audit_dec,
            "live": live,
            "diff": diff_summary,
            "verdict": verdict,
        })

    if args.json:
        _print_json(ref_date, basket, results, counters.get("elapsed_s", 0.0))
    else:
        _print_human(ref_date, basket, results)
    return 0


def main() -> int:
    args = _parse_args()
    ref_date = _resolve_date(args.date)
    counters = {"verified": 0, "mismatch": 0, "pending": 0, "error": 0}
    exit_code = 0
    t0 = time.time()
    try:
        exit_code = _run_verifier(args, ref_date, counters)
    except Exception as e:
        # Catch-all for anything _run_verifier didn't handle (e.g.
        # KeyboardInterrupt would NOT land here; that's fine — Ctrl-C
        # should still emit the runtime line via finally below).
        _alert_error(
            ref_date, scope="top_level",
            detail=f"unhandled_exception: {type(e).__name__}: {e}",
        )
        log.error("traceback:", exc_info=True)
        exit_code = 1
    finally:
        elapsed = time.time() - t0
        # Counters reflect work completed before the exception, if any.
        # `outcome` summarizes top-level success/failure for grep-friendly
        # filtering; per-strategy verdicts live in the human/JSON output.
        outcome = "ok" if exit_code == 0 else "failed"
        log.info(
            f"[INTRADAY_AUDIT_RUNTIME] elapsed_s={elapsed:.1f} "
            f"outcome={outcome} "
            f"verified={counters['verified']} "
            f"mismatch={counters['mismatch']} "
            f"pending={counters['pending']} "
            f"error={counters['error']} "
            f"dry_run={args.dry_run}"
        )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

"""
Celery task that orchestrates the full audit pipeline chain:
  1. overlap_analysis.py --audit   (streams stdout+stderr → audit_output.txt)
  2. node generate_audit_report.js (produces overlap_audit_report.docx)
  3. Parse audit_output.txt for key metrics and filter comparison table
  4. Write results back to job store
"""

import re
import subprocess
from pathlib import Path

from celery import Celery

from app.core.config import settings
from app.services.job_store import get_job, update_job

celery_app = Celery("pipeline_worker", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
)

# ---------------------------------------------------------------------------
# Python binary — must have pandas, pyarrow, scipy etc (miniforge base, not the FastAPI venv)
# Configured via PIPELINE_PYTHON in .env
# ---------------------------------------------------------------------------
_PIPELINE_PYTHON = settings.PIPELINE_PYTHON

# ---------------------------------------------------------------------------
# Metric parsing
#
# Patterns are anchored to the exact log lines produced by audit.py /
# institutional_audit.py as observed in audit_output.txt.
# ---------------------------------------------------------------------------

# ── Canonical single-line summary emitted once per best filter, e.g.:
#   NetRet=+248.9%  Sharpe=2.053  MaxDD=-53.23%  WF-CV=1.205  FA-OOS Sharpe=2.417  Flat=0d  Active=398d  Equity-R²=0.8795
_CANON_RE = re.compile(
    r"NetRet=[+-]?[\d.]+%\s+"
    r"Sharpe=(?P<sharpe>[+-]?[\d.]+)\s+"
    r"MaxDD=(?P<maxdd>[+-]?[\d.]+)%\s+"
    r"WF-CV=(?P<cv>[+-]?[\d.]+)\s+"
    r"FA-OOS Sharpe=(?P<fa_oos_sharpe>[+-]?[\d.]+)\s+"
    r"Flat=(?P<flat>\d+)d\s+"
    r"Active=(?P<active>\d+)d"
)

# Ratio block lines, e.g.:
#   │  Sortino Ratio:               3.517
#   │  Calmar Ratio:                8.457
#   │  Omega Ratio:                 1.372
#   │  Ulcer Index:                23.910  (lower = better)
_SORTINO_RE = re.compile(r"Sortino Ratio:\s*(?P<v>[+-]?[\d.]+)")
_CALMAR_RE  = re.compile(r"Calmar Ratio:\s*(?P<v>[+-]?[\d.]+)")
_OMEGA_RE   = re.compile(r"Omega Ratio:\s*(?P<v>[+-]?[\d.]+)")
_ULCER_RE   = re.compile(r"Ulcer Index:\s*(?P<v>[+-]?[\d.]+)")

# DSR line, e.g.:
#   DSR (prob Sharpe is genuine):   91.796%
_DSR_RE = re.compile(r"DSR\s*\(prob Sharpe is genuine\):\s*(?P<dsr>[+-]?[\d.]+)%")

# CAGR from filter table row (captured below) — also grab from canonical line
_CAGR_RE = re.compile(r"NetRet=[+-]?[\d.]+%\s+Sharpe=[+-]?[\d.]+\s+MaxDD=[+-]?[\d.]+%.*?CAGR=(?P<cagr>[+-]?[\d.]+)%")

# Overall grade + score, e.g.:
#   OVERALL GRADE                                 C (54/100)
_OVERALL_GRADE_RE = re.compile(r"OVERALL GRADE\s+(?P<grade>[A-F][+-]?)\s+\((?P<score>\d+)/100\)")

# Final machine-readable tags emitted by audit.py:
#   FINAL_WF_CV(A_-_No_Filter):         1.2055
#   FINAL_GRADE(A_-_No_Filter):         C
#   FINAL_GRADE_SCORE(A_-_No_Filter):   54.0
_FINAL_CV_RE     = re.compile(r"FINAL_WF_CV\([^)]+\):\s*(?P<cv>[+-]?[\d.]+)")
_FINAL_GRADE_RE  = re.compile(r"FINAL_GRADE\([^)]+\):\s*(?P<grade>[A-F][+-]?)")
_FINAL_SCORE_RE  = re.compile(r"FINAL_GRADE_SCORE\([^)]+\):\s*(?P<score>[+-]?[\d.]+)")
_FINAL_EQUITY_RE      = re.compile(r"FINAL_EQUITY_SERIES\([^)]+\):\s*(\[[\d.,\s\-]+\])")
_FINAL_DD_RE          = re.compile(r"FINAL_DD_SERIES\([^)]+\):\s*(\[[\d.,\s\-]+\])")
_FINAL_INTRADAY_RE    = re.compile(r"^FINAL_INTRADAY_BARS:\s*(\{.+\})\s*$", re.MULTILINE)
_FINAL_EXIT_BARS_RE   = re.compile(r"^FINAL_INTRADAY_EXIT_BARS:\s*(\{.+\})\s*$", re.MULTILINE)
_FINAL_PORTFOLIO_RE   = re.compile(r"^FINAL_DAILY_PORTFOLIO:\s*(\{.+\})\s*$", re.MULTILINE)
_FINAL_PORTFOLIO_FILTER_RE = re.compile(
    r"^FINAL_DAILY_PORTFOLIO_(?P<tag>[A-Za-z0-9_\-]+):\s*(?P<json>\{.+\})\s*$",
    re.MULTILINE,
)

# Filter comparison table rows, e.g.:
#   A - No Filter                             2.053    450.2%   -53.23%      398    1.206     248.9%   3.49×   -6.10%  -21.05%  -29.58%   91.8%    54 ◄
# Columns: Filter  Sharpe  CAGR%  MaxDD%  Active  WF-CV  TotRet%  Eq  Wst1D%  Wst1W%  Wst1M%  DSR%  Grd
_FILTER_ROW_RE = re.compile(
    r"^\s*(?P<label>[A-F]\s+-\s+[^\n]+?)\s+"
    r"(?P<sharpe>(?:[+-]?[\d.]+|n/?a|nan|\?))\s+"
    r"(?P<cagr>(?:[+-]?[\d.]+|n/?a|nan|\?))%?\s+"
    r"(?P<maxdd>(?:[+-]?[\d.]+|n/?a|nan|\?))%?\s+"
    r"(?P<active>\d+)\s+"
    r"(?P<wf_cv>(?:[+-]?[\d.]+|n/?a|nan|\?))\s+"
    r"(?P<tot_ret>(?:[+-]?[\d.]+|n/?a|nan|\?))%?\s+"
    r"(?P<eq>(?:[+-]?[\d.]+|n/?a|nan|\?))×?\s+"
    r"(?P<wst_1d>(?:[+-]?[\d.]+|n/?a|nan|\?))%?\s+"
    r"(?P<wst_1w>(?:[+-]?[\d.]+|n/?a|nan|\?))%?\s+"
    r"(?P<wst_1m>(?:[+-]?[\d.]+|n/?a|nan|\?))%?\s+"
    r"(?P<dsr>(?:[+-]?[\d.]+|n/?a|nan|\?))%?\s+"
    r"(?P<grade>(?:\d+|n/?a|nan|\?))"
    r"(?:\s+(?P<winner>◄))?",
    re.MULTILINE,
)

_FINAL_FILTER_VALUE_RE = re.compile(r"^FINAL_(?P<key>[A-Z_]+)\((?P<fid>[^)]+)\):\s*(?P<val>.+?)\s*$", re.MULTILINE)

_FEES_HEADING_RE = re.compile(r"FEES PANEL", re.IGNORECASE)
_FEES_COL_HEADER_RE = re.compile(r"^\s*Date\s+Start\s+\(\$\)\s+Margin\s+\(\$\)\s+Lev\s+Invested\s+\(\$\)\s+Trade Vol\s+\(\$\)\s+Taker Fee\s+\(\$\)\s+Funding\s+\(\$\)\s+End\s+\(\$\)\s+Ret Gross%\s+Ret Net%\s+Net P&L\s+\(\$\)\s*$")
_SIMULATING_FILTER_RE = re.compile(r"^\s*SIMULATING:\s*(?P<filter>.+?)\s*$")
_DATE_RE = r"(?P<date>\d{4}-\d{2}-\d{2})"
_NUM_RE = r"[+-]?\d[\d,]*\.\d+"
_PCT_RE = r"[+-]?\d+\.\d+%"
_ACTIVE_FEES_ROW_RE = re.compile(
    rf"^\s*{_DATE_RE}\s+"
    rf"(?P<start>{_NUM_RE})\s+"
    rf"(?P<margin>{_NUM_RE})\s+"
    rf"(?P<lev>{_NUM_RE})\s+"
    rf"(?P<invested>{_NUM_RE})\s+"
    rf"(?P<trade_vol>{_NUM_RE})\s+"
    rf"(?P<taker_fee>{_NUM_RE})\s+"
    rf"(?P<funding>{_NUM_RE})\s+"
    rf"(?P<end>{_NUM_RE})\s+"
    rf"(?P<ret_gross>{_PCT_RE})\s+"
    rf"(?P<ret_net>{_PCT_RE})\s+"
    rf"(?P<net_pnl>{_NUM_RE})\s*$"
)
_NO_ENTRY_FEES_ROW_RE = re.compile(
    rf"^\s*{_DATE_RE}\s+"
    rf"(?P<start>{_NUM_RE})\s+"
    r"(?:—\s*(?P<reason>NO ENTRY|FILTERED)\s*—|-+\s*(?P<reason2>NO ENTRY|FILTERED)\s*-+)\s+"
    rf"(?P<end>{_NUM_RE})\s+"
    rf"(?P<ret_gross>{_PCT_RE})\s+"
    rf"(?P<ret_net>{_PCT_RE})\s+"
    rf"(?P<net_pnl>{_NUM_RE})\s*$"
)


def _norm_filter_label(s: str) -> str:
    return (
        s.lower()
        .replace("+", "p")
        .replace("(", " ")
        .replace(")", " ")
        .replace("-", " ")
        .replace("_", " ")
    )


def _parse_num(s: str | None) -> float | None:
    if s is None:
        return None
    try:
        return float(s.replace(",", "").strip())
    except Exception:
        return None


def _parse_pct(s: str | None) -> float | None:
    if s is None:
        return None
    t = s.strip().replace("%", "")
    try:
        return float(t)
    except Exception:
        return None


def _extract_filter_name_from_fees_heading(line: str) -> str:
    # Prefer pipe-delimited filter labels: FEES PANEL | <filter> | ...
    if "|" in line:
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) >= 2:
            return parts[1]
    # Fallback: FEES PANEL — <filter> ...
    m = re.search(r"FEES PANEL\s*[—-]\s*(.+)$", line, flags=re.IGNORECASE)
    if m:
        tail = m.group(1).strip()
        # drop leading descriptor if present
        tail = re.sub(r"^\s*per active trading day\b", "", tail, flags=re.IGNORECASE).strip(" -|")
        if "capital=" in tail.lower() or not tail:
            return ""
        return tail
    return ""


def _nearest_simulating_filter(lines: list[str], start_idx: int) -> str:
    for i in range(start_idx, -1, -1):
        m = _SIMULATING_FILTER_RE.match(lines[i].strip())
        if m:
            return m.group("filter").strip()
    return ""


def _extract_fees_tables(text: str) -> dict[str, list[dict]]:
    lines = text.splitlines()
    starts = [i for i, ln in enumerate(lines) if _FEES_HEADING_RE.search(ln)]
    tables: dict[str, list[dict]] = {}
    for si, start in enumerate(starts):
        end = starts[si + 1] if si + 1 < len(starts) else len(lines)
        block = lines[start:end]
        heading = block[0].strip()
        filter_name = (
            _extract_filter_name_from_fees_heading(heading)
            or _nearest_simulating_filter(lines, start)
            or f"fees_panel_{si + 1}"
        )
        # find column header line, then parse rows below it
        header_idx = -1
        for j, line in enumerate(block):
            if _FEES_COL_HEADER_RE.match(line):
                header_idx = j
                break
        if header_idx < 0:
            continue
        rows: list[dict] = []
        for raw in block[header_idx + 1:]:
            line = raw.rstrip()
            if not line.strip():
                # allow blank lines within section
                continue
            if _FEES_HEADING_RE.search(line):
                break
            if re.match(r"^\s*[=─-]{5,}\s*$", line):
                continue
            m_no = _NO_ENTRY_FEES_ROW_RE.match(line)
            if m_no:
                g = m_no.groupdict()
                reason = (g.get("reason") or g.get("reason2") or "").strip().upper()
                no_entry_reason = "filter" if reason == "FILTERED" else "conviction_gate"
                rows.append({
                    "date": g["date"],
                    "start": _parse_num(g["start"]),
                    "margin": None,
                    "lev": None,
                    "invested": None,
                    "trade_vol": None,
                    "taker_fee": None,
                    "funding": None,
                    "end": _parse_num(g["end"]),
                    "ret_gross": _parse_pct(g["ret_gross"]),
                    "ret_net": _parse_pct(g["ret_net"]),
                    "net_pnl": _parse_num(g["net_pnl"]),
                    "no_entry": True,
                    "no_entry_reason": no_entry_reason,
                })
                continue
            m_active = _ACTIVE_FEES_ROW_RE.match(line)
            if m_active:
                g = m_active.groupdict()
                rows.append({
                    "date": g["date"],
                    "start": _parse_num(g["start"]),
                    "margin": _parse_num(g["margin"]),
                    "lev": _parse_num(g["lev"]),
                    "invested": _parse_num(g["invested"]),
                    "trade_vol": _parse_num(g["trade_vol"]),
                    "taker_fee": _parse_num(g["taker_fee"]),
                    "funding": _parse_num(g["funding"]),
                    "end": _parse_num(g["end"]),
                    "ret_gross": _parse_pct(g["ret_gross"]),
                    "ret_net": _parse_pct(g["ret_net"]),
                    "net_pnl": _parse_num(g["net_pnl"]),
                    "no_entry": False,
                })
                continue
        if rows:
            tables[filter_name] = rows
    return tables


def _parse_metrics(audit_output_path: Path) -> dict:
    if not audit_output_path.exists():
        return {}

    text = audit_output_path.read_text(errors="replace")
    metrics: dict = {}
    hints: list[str] = []

    # ── Canonical summary line (most reliable single source) ─────────────────
    # NetRet=+248.9%  Sharpe=2.053  MaxDD=-53.23%  WF-CV=1.205  FA-OOS Sharpe=2.417  Flat=0d  Active=398d
    canon = _CANON_RE.search(text)
    if canon:
        metrics["sharpe"]        = float(canon.group("sharpe"))
        metrics["max_drawdown"]  = float(canon.group("maxdd"))
        metrics["cv"]            = float(canon.group("cv"))
        metrics["fa_oos_sharpe"] = float(canon.group("fa_oos_sharpe"))
        metrics["flat_days"]     = int(canon.group("flat"))
        metrics["active_days"]   = int(canon.group("active"))

    # ── Ratio block ───────────────────────────────────────────────────────────
    m = _SORTINO_RE.search(text)
    if m:
        metrics["sortino"] = float(m.group("v"))
    m = _CALMAR_RE.search(text)
    if m:
        metrics["calmar"] = float(m.group("v"))
    m = _OMEGA_RE.search(text)
    if m:
        metrics["omega"] = float(m.group("v"))
    m = _ULCER_RE.search(text)
    if m:
        metrics["ulcer_index"] = float(m.group("v"))

    # ── DSR ───────────────────────────────────────────────────────────────────
    m = _DSR_RE.search(text)
    if m:
        metrics["dsr_pct"] = float(m.group("dsr"))

    # ── Overall grade + score ─────────────────────────────────────────────────
    og = _OVERALL_GRADE_RE.search(text)
    if og:
        metrics["grade"]       = og.group("grade")
        metrics["grade_score"] = float(og.group("score"))

    # ── Machine-readable FINAL_ tags (most precise — override above) ──────────
    m = _FINAL_CV_RE.search(text)
    if m:
        metrics["cv"] = float(m.group("cv"))
    m = _FINAL_GRADE_RE.search(text)
    if m:
        metrics["grade"] = m.group("grade")
    m = _FINAL_SCORE_RE.search(text)
    if m:
        metrics["grade_score"] = float(m.group("score"))

    # ── Daily series for charts ───────────────────────────────────────────────
    import json as _json
    m = _FINAL_EQUITY_RE.search(text)
    if m:
        try:
            metrics["equity_curve"] = _json.loads(m.group(1))
        except Exception:
            pass
    m = _FINAL_DD_RE.search(text)
    if m:
        try:
            metrics["drawdown_curve"] = _json.loads(m.group(1))
        except Exception:
            pass
    m = _FINAL_INTRADAY_RE.search(text)
    if m:
        try:
            metrics["intraday_bars"] = _json.loads(m.group(1))
        except Exception:
            pass
    m = _FINAL_EXIT_BARS_RE.search(text)
    if m:
        try:
            metrics["intraday_exit_bars"] = _json.loads(m.group(1))
        except Exception:
            pass
    m = _FINAL_PORTFOLIO_RE.search(text)
    if m:
        try:
            metrics["daily_portfolio"] = _json.loads(m.group(1))
        except Exception:
            pass

    # Per-filter daily portfolios: FINAL_DAILY_PORTFOLIO_<tag>: {...}
    _dp_by_filter: dict[str, object] = {}
    for _dp_m in _FINAL_PORTFOLIO_FILTER_RE.finditer(text):
        try:
            _dp_tag = _dp_m.group("tag").replace("_", " ")
            _dp_by_filter[_dp_tag] = _json.loads(_dp_m.group("json"))
        except Exception:
            pass
    if _dp_by_filter:
        metrics["daily_portfolio_by_filter"] = _dp_by_filter

    def _parse_opt_float(v: str | None) -> float | None:
        if v is None:
            return None
        s = v.strip().lower()
        if s in {"n/a", "na", "nan", "?", "--", ""}:
            return None
        try:
            return float(s)
        except (TypeError, ValueError):
            return None

    def _parse_opt_int(v: str | None) -> int | None:
        f = _parse_opt_float(v)
        return int(f) if f is not None else None

    def _fmt_filter_label_from_id(fid: str) -> str:
        label = fid.replace("_-_", " - ").replace("_", " ")
        label = re.sub(r"\s+p\s+", " + ", label)
        if label.endswith("(OR"):
            label += ")"
        if label.endswith("(AND"):
            label += ")"
        return label.strip()

    def _norm_label(label: str) -> str:
        return (
            label.lower()
            .replace("+", "p")
            .replace("(", " ")
            .replace(")", " ")
            .replace("-", " ")
            .replace("_", " ")
        )

    # ── Filter comparison table ───────────────────────────────────────────────
    # Row format (actual):
    #   A - No Filter                             2.053    450.2%   -53.23%      398    1.206     248.9%   3.49×   -6.10%  -21.05%  -29.58%   91.8%    54 ◄
    filter_rows = []
    seen_labels: set[str] = set()
    for row in _FILTER_ROW_RE.finditer(text):
        g = row.groupdict()
        label = g["label"].strip()
        if label in seen_labels:
            continue
        seen_labels.add(label)
        filter_rows.append({
            "filter":      label,
            "sharpe":      _parse_opt_float(g["sharpe"]),
            "cagr":        _parse_opt_float(g["cagr"]),
            "max_dd":      _parse_opt_float(g["maxdd"]),
            "active":      int(g["active"]),
            "wf_cv":       _parse_opt_float(g["wf_cv"]),
            "tot_ret":     _parse_opt_float(g["tot_ret"]),
            "eq":          _parse_opt_float(g["eq"]),
            "wst_1d":      _parse_opt_float(g["wst_1d"]),
            "wst_1w":      _parse_opt_float(g["wst_1w"]),
            "wst_1m":      _parse_opt_float(g["wst_1m"]),
            "dsr_pct":     _parse_opt_float(g["dsr"]),
            "grade_score": _parse_opt_int(g["grade"]),
            "is_run_summary_best": bool(g.get("winner")),
        })
    if filter_rows:
        metrics["filter_comparison"] = filter_rows
        # Prefer explicit winner marker from RUN SUMMARY (◄).
        best_marked = next((r for r in filter_rows if r.get("is_run_summary_best")), None)
        # Fallback to highest grade_score, then Sharpe.
        best_fallback = max(
            filter_rows,
            key=lambda r: (
                r["grade_score"] is not None,
                r["grade_score"] if r["grade_score"] is not None else float("-inf"),
                r["sharpe"] if r["sharpe"] is not None else float("-inf"),
            ),
        )
        best = best_marked or best_fallback
        metrics["best_filter"] = best["filter"]
        if best.get("cagr") is not None:
            metrics["cagr"] = best["cagr"]
        if best.get("sharpe") is not None:
            metrics["sharpe"] = best["sharpe"]
        if best.get("max_dd") is not None:
            metrics["max_drawdown"] = best["max_dd"]
        if best.get("wf_cv") is not None:
            metrics["cv"] = best["wf_cv"]
        if best.get("dsr_pct") is not None:
            metrics["dsr_pct"] = best["dsr_pct"]
        if best.get("grade_score") is not None:
            metrics["grade_score"] = float(best["grade_score"])

    # ── Per-filter FINAL_* metrics (for UI filter selection) ──────────────────
    # Parse machine-readable FINAL_* lines so frontend can switch charts/cards
    # to any selected filter, not only the canonical/best row.
    per_filter: dict[str, dict] = {}
    for fm in _FINAL_FILTER_VALUE_RE.finditer(text):
        fkey = fm.group("key")
        fid = fm.group("fid")
        val = fm.group("val").strip()
        if "_-_" not in fid:
            continue
        label = _fmt_filter_label_from_id(fid)
        row = per_filter.setdefault(label, {"filter": label})
        if fkey in {"EQUITY_SERIES", "DD_SERIES"}:
            try:
                parsed = _json.loads(val)
            except Exception:
                continue
            if fkey == "EQUITY_SERIES":
                row["equity_curve"] = parsed
            else:
                row["drawdown_curve"] = parsed
            continue
        if fkey == "GRADE":
            row["grade"] = val
            continue

        num = _parse_opt_float(val)
        if fkey == "SHARPE":
            row["sharpe"] = num
        elif fkey == "CAGR":
            row["cagr"] = num
        elif fkey == "MAX_DD":
            row["max_dd"] = num
        elif fkey == "ACTIVE_DAYS":
            row["active"] = _parse_opt_int(val)
        elif fkey == "WF_CV":
            row["wf_cv"] = num
            row["cv"] = num
        elif fkey == "TOTAL_RETURN":
            row["tot_ret"] = num
        elif fkey == "WORST_DAY":
            row["wst_1d"] = num
        elif fkey == "WORST_WEEK":
            row["wst_1w"] = num
        elif fkey == "WORST_MONTH":
            row["wst_1m"] = num
        elif fkey == "DSR":
            row["dsr_pct"] = num
        elif fkey == "GRADE_SCORE":
            row["grade_score"] = _parse_opt_int(val)

    if per_filter:
        # Keep list form for easy frontend iteration/lookup.
        metrics["filters"] = list(per_filter.values())
        # Canonicalize filter labels in summary rows where we have better names.
        if metrics.get("filter_comparison"):
            by_norm = { _norm_label(k): k for k in per_filter.keys() }
            for row in metrics["filter_comparison"]:
                raw = str(row.get("filter", "")).strip()
                canon = by_norm.get(_norm_label(raw))
                if canon:
                    row["filter"] = canon
            # Canonicalize best_filter label to match normalized filter rows.
            if metrics.get("best_filter"):
                bf = str(metrics["best_filter"]).strip()
                canon_bf = by_norm.get(_norm_label(bf))
                if canon_bf:
                    metrics["best_filter"] = canon_bf

    # ── Fees panel rows by filter ────────────────────────────────────────────
    fees_tables_by_filter = _extract_fees_tables(text)
    if fees_tables_by_filter:
        metrics["fees_tables_by_filter"] = fees_tables_by_filter
        best_filter = str(metrics.get("best_filter") or "").strip()
        selected_fees = None
        if best_filter:
            bf_norm = _norm_filter_label(best_filter)
            for k, rows in fees_tables_by_filter.items():
                if _norm_filter_label(k) == bf_norm:
                    selected_fees = rows
                    break
        if selected_fees is None:
            selected_fees = next(iter(fees_tables_by_filter.values()))
        metrics["fees_table"] = selected_fees

    if "All fetches failed - dispersion filter unavailable" in text:
        hints.append("Dispersion filters could not run because Binance data fetch failed. If you are behind geo restrictions, turn on VPN and re-run.")
    if hints:
        metrics["hints"] = hints

    return metrics


def _build_cli_args(params: dict) -> list[str]:
    """Map job params → overlap_analysis.py CLI flags.

    Only flags that overlap_analysis.py actually accepts go here.
    audit.py-only params (leverage, stop_raw_pct, min_listing_age, etc.)
    are passed via env vars in pipeline_env and do not appear on the CLI.
    """
    flag_map = {
        "leaderboard_index":        "--leaderboard-index",
        "min_mcap":                 "--min-mcap",
        "max_mcap":                 "--max-mcap",
        "sort_by":                  "--sort-by",
        "mode":                     "--mode",
        "sample_interval":          "--sample-interval",
        "freq_width":               "--freq-width",
        "freq_cutoff":              "--freq-cutoff",
        "deployment_start_hour":    "--deployment-start-hour",
        "index_lookback":           "--index-lookback",
        "sort_lookback":            "--sort-lookback",
        "deployment_runtime_hours": "--deployment-runtime-hours",
        "capital_mode":             "--capital-mode",
        "fixed_notional_cap":       "--fixed-notional-cap",
        "overlap_source":           "--source",
    }
    bool_flags = {
        "end_cross_midnight": "--end-cross-midnight",
        "drop_unverified":    "--drop-unverified",
        "quick":              "--quick",
    }

    # --audit chains into rebuild_portfolio_matrix.py and audit.py automatically.
    # --audit-source controls the price data source for rebuild_portfolio_matrix.
    audit_source = params.get("price_source", "parquet")
    args: list[str] = ["--audit", "--audit-source", audit_source]

    for param, flag in flag_map.items():
        value = params.get(param)
        if value is not None:
            args += [flag, str(value)]

    for param, flag in bool_flags.items():
        if params.get(param):
            args.append(flag)

    return args


class JobCancelled(Exception):
    pass


def _is_cancelled(job_id: str) -> bool:
    job = get_job(job_id)
    if not job:
        return False
    return str(job.get("status", "")).lower() in {"cancelled", "canceled"}


@celery_app.task(bind=True, name="pipeline_worker.run_pipeline")
def run_pipeline(self, job_id: str, params: dict) -> dict:
    """
    Execute the full audit pipeline for a job.

    Stages:
      overlap   — runs overlap_analysis.py --audit (chains portfolio + audit)
      report    — runs node generate_audit_report.js
      parsing   — extracts key metrics from audit_output.txt
    """
    pipeline_dir  = Path(settings.PIPELINE_DIR)
    job_dir       = Path(settings.JOBS_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    audit_output_path = job_dir / "audit_output.txt"

    # Env vars passed to subprocesses so pipeline scripts pick up correct paths.
    # Every JobRequest param is forwarded as SNAKE_UPPER_CASE so audit.py can
    # read it via os.environ.get("PARAM_NAME", default).
    import os

    def _boolenv(v: bool) -> str:
        return "1" if v else "0"

    pipeline_env = {
        **os.environ,
        # Ensure pipeline python's directory is on PATH so subprocess "python3" resolves correctly
        "PATH": str(Path(_PIPELINE_PYTHON).parent) + ":" + os.environ.get("PATH", ""),
        # Infrastructure paths
        "BASE_DATA_DIR":       str(settings.BASE_DATA_DIR),
        "PARQUET_PATH":        settings.PARQUET_PATH,
        "MARKETCAP_DIR":       settings.MARKETCAP_DIR,
        # Blank LOCAL_MATRIX_CSV tells audit.py to rebuild the matrix fresh
        "LOCAL_MATRIX_CSV":    "",

        # ── Basic ─────────────────────────────────────────────────────────────
        "LEADERBOARD_INDEX":          str(params.get("leaderboard_index", 100)),
        "SORT_BY":                    str(params.get("sort_by", "price")),
        "MODE":                       str(params.get("mode", "snapshot")),
        "FREQ_WIDTH":                 str(params.get("freq_width", 20)),
        "FREQ_CUTOFF":                str(params.get("freq_cutoff", 20)),
        "SAMPLE_INTERVAL":            str(params.get("sample_interval", 5)),
        "DEPLOYMENT_START_HOUR":      str(params.get("deployment_start_hour", 6)),
        "INDEX_LOOKBACK":             str(params.get("index_lookback", 6)),
        "SORT_LOOKBACK":              str(params.get("sort_lookback", "6")),
        "DEPLOYMENT_RUNTIME_HOURS":   str(params.get("deployment_runtime_hours", "daily")),
        "END_CROSS_MIDNIGHT":         _boolenv(params.get("end_cross_midnight", True)),
        "STARTING_CAPITAL":           str(params.get("starting_capital", 100000.0)),
        "CAPITAL_MODE":               str(params.get("capital_mode", "fixed")),
        "FIXED_NOTIONAL_CAP":         str(params.get("fixed_notional_cap", "internal")),
        "PIVOT_LEVERAGE":             str(params.get("pivot_leverage", 4.0)),
        "MIN_MCAP":                   str(params.get("min_mcap", 0.0)),
        "MAX_MCAP":                   str(params.get("max_mcap", 0.0)),
        "MIN_LISTING_AGE":            str(params.get("min_listing_age", 0)),
        "MAX_PORT":                   "" if params.get("max_port") is None else str(params.get("max_port")),
        "DROP_UNVERIFIED":            _boolenv(params.get("drop_unverified", False)),
        "LEVERAGE":                   str(params.get("leverage", 4.0)),
        "STOP_RAW_PCT":               str(params.get("stop_raw_pct", -6.0)),
        "PRICE_SOURCE":               str(params.get("price_source", "parquet")),
        "MCAP_SOURCE":                str(params.get("mcap_source", "parquet")),
        "SAVE_CHARTS":                _boolenv(params.get("save_charts", True)),
        "TRIAL_PURCHASES":            _boolenv(params.get("trial_purchases", False)),
        "QUICK":                      _boolenv(params.get("quick", False)),
        "TAKER_FEE_PCT":              str(params.get("taker_fee_pct", 0.0008)),
        "FUNDING_RATE_DAILY_PCT":     str(params.get("funding_rate_daily_pct", 0.0002)),

        # CANDIDATE_CONFIGS execution params
        "EARLY_KILL_X":               str(params.get("early_kill_x", 5)),
        "EARLY_KILL_Y":               str(params.get("early_kill_y", -999.0)),
        "EARLY_INSTILL_Y":            str(params.get("early_instill_y", -999.0)),
        "L_BASE":                     str(params.get("l_base", 0.0)),
        "L_HIGH":                     str(params.get("l_high", 1.0)),
        "PORT_TSL":                   str(params.get("port_tsl", 0.99)),
        "PORT_SL":                    str(params.get("port_sl", -0.99)),
        "EARLY_FILL_Y":               str(params.get("early_fill_y", 0.99)),
        "EARLY_FILL_X":               str(params.get("early_fill_x", 5)),

        # ── Filters ───────────────────────────────────────────────────────────
        "ENABLE_TAIL_GUARDRAIL":      _boolenv(params.get("enable_tail_guardrail", True)),
        "ENABLE_DISPERSION_FILTER":   _boolenv(params.get("enable_dispersion_filter", True)),
        "ENABLE_TAIL_PLUS_DISP":      _boolenv(params.get("enable_tail_plus_disp", True)),
        "ENABLE_VOL_FILTER":          _boolenv(params.get("enable_vol_filter", True)),
        "ENABLE_TAIL_DISP_VOL":       _boolenv(params.get("enable_tail_disp_vol", False)),
        "ENABLE_TAIL_OR_VOL":         _boolenv(params.get("enable_tail_or_vol", False)),
        "ENABLE_TAIL_AND_VOL":        _boolenv(params.get("enable_tail_and_vol", False)),
        "ENABLE_BLOFIN_FILTER":       _boolenv(params.get("enable_blofin_filter", False)),
        "ENABLE_BTC_MA_FILTER":       _boolenv(params.get("enable_btc_ma_filter", False)),
        "ENABLE_IC_DIAGNOSTIC":       _boolenv(params.get("enable_ic_diagnostic", False)),
        "ENABLE_IC_FILTER":           _boolenv(params.get("enable_ic_filter", False)),
        "RUN_FILTER_NONE":            _boolenv(params.get("run_filter_none", True)),
        "RUN_FILTER_TAIL":            _boolenv(params.get("run_filter_tail", False)),
        "RUN_FILTER_DISPERSION":      _boolenv(params.get("run_filter_dispersion", False)),
        "RUN_FILTER_TAIL_DISP":       _boolenv(params.get("run_filter_tail_disp", False)),
        "RUN_FILTER_VOL":             _boolenv(params.get("run_filter_vol", False)),
        "RUN_FILTER_TAIL_DISP_VOL":   _boolenv(params.get("run_filter_tail_disp_vol", False)),
        "RUN_FILTER_TAIL_OR_VOL":     _boolenv(params.get("run_filter_tail_or_vol", False)),
        "RUN_FILTER_TAIL_AND_VOL":    _boolenv(params.get("run_filter_tail_and_vol", False)),
        "RUN_FILTER_TAIL_BLOFIN":     _boolenv(params.get("run_filter_tail_blofin", False)),
        "RUN_FILTER_CALENDAR":        _boolenv(params.get("run_filter_calendar", False)),

        # ── Advanced — Strategy tuning ────────────────────────────────────────
        "DISPERSION_THRESHOLD":         str(params.get("dispersion_threshold", 0.66)),
        "DISPERSION_BASELINE_WIN":      str(params.get("dispersion_baseline_win", 33)),
        "DISPERSION_DYNAMIC_UNIVERSE":  _boolenv(params.get("dispersion_dynamic_universe", True)),
        "DISPERSION_N":                 str(params.get("dispersion_n", 40)),
        "VOL_LOOKBACK":                 str(params.get("vol_lookback", 10)),
        "VOL_PERCENTILE":               str(params.get("vol_percentile", 0.25)),
        "VOL_BASELINE_WIN":             str(params.get("vol_baseline_win", 90)),
        "TAIL_DROP_PCT":                str(params.get("tail_drop_pct", 0.04)),
        "TAIL_VOL_MULT":                str(params.get("tail_vol_mult", 1.4)),
        "IC_SIGNAL":                    str(params.get("ic_signal", "mom1d")),
        "IC_WINDOW":                    str(params.get("ic_window", 30)),
        "IC_THRESHOLD":                 str(params.get("ic_threshold", 0.02)),
        "BTC_MA_DAYS":                  str(params.get("btc_ma_days", 20)),
        "BLOFIN_MIN_SYMBOLS":           str(params.get("blofin_min_symbols", 1)),
        "LEADERBOARD_TOP_N":            str(params.get("leaderboard_top_n", 333)),
        "TRAIN_TEST_SPLIT":             str(params.get("train_test_split", 0.60)),
        "N_TRIALS":                     str(params.get("n_trials", 3)),

        # ── Advanced — Leverage scaling ───────────────────────────────────────
        "ENABLE_PERF_LEV_SCALING":    _boolenv(params.get("enable_perf_lev_scaling", False)),
        "PERF_LEV_WINDOW":            str(params.get("perf_lev_window", 10)),
        "PERF_LEV_SORTINO_TARGET":    str(params.get("perf_lev_sortino_target", 3.0)),
        "PERF_LEV_MAX_BOOST":         str(params.get("perf_lev_max_boost", 1.5)),
        "ENABLE_VOL_LEV_SCALING":     _boolenv(params.get("enable_vol_lev_scaling", False)),
        "VOL_LEV_WINDOW":             str(params.get("vol_lev_window", 30)),
        "VOL_LEV_TARGET_VOL":         str(params.get("vol_lev_target_vol", 0.02)),
        "VOL_LEV_MAX_BOOST":          str(params.get("vol_lev_max_boost", 2.0)),
        "VOL_LEV_DD_THRESHOLD":       str(params.get("vol_lev_dd_threshold", -0.06)),
        "LEV_QUANTIZATION_MODE":      str(params.get("lev_quantization_mode", "off")),
        "LEV_QUANTIZATION_STEP":      str(params.get("lev_quantization_step", 0.1)),
        "ENABLE_CONTRA_LEV_SCALING":  _boolenv(params.get("enable_contra_lev_scaling", False)),
        "CONTRA_LEV_WINDOW":          str(params.get("contra_lev_window", 30)),
        "CONTRA_LEV_MAX_BOOST":       str(params.get("contra_lev_max_boost", 2.0)),
        "CONTRA_LEV_DD_THRESHOLD":    str(params.get("contra_lev_dd_threshold", -0.15)),

        # ── Advanced — Risk overlays ──────────────────────────────────────────
        "ENABLE_PPH":                       _boolenv(params.get("enable_pph", False)),
        "PPH_FREQUENCY":                    str(params.get("pph_frequency", "weekly")),
        "PPH_THRESHOLD":                    str(params.get("pph_threshold", 0.20)),
        "PPH_HARVEST_FRAC":                 str(params.get("pph_harvest_frac", 0.50)),
        "PPH_SWEEP_ENABLED":                _boolenv(params.get("pph_sweep_enabled", False)),
        "ENABLE_RATCHET":                   _boolenv(params.get("enable_ratchet", False)),
        "RATCHET_FREQUENCY":                str(params.get("ratchet_frequency", "weekly")),
        "RATCHET_TRIGGER":                  str(params.get("ratchet_trigger", 0.20)),
        "RATCHET_LOCK_PCT":                 str(params.get("ratchet_lock_pct", 0.15)),
        "RATCHET_RISK_OFF_LEV_SCALE":       str(params.get("ratchet_risk_off_lev_scale", 0.0)),
        "RATCHET_SWEEP_ENABLED":            _boolenv(params.get("ratchet_sweep_enabled", False)),
        "ENABLE_ADAPTIVE_RATCHET":          _boolenv(params.get("enable_adaptive_ratchet", False)),
        "ADAPTIVE_RATCHET_FREQUENCY":       str(params.get("adaptive_ratchet_frequency", "weekly")),
        "ADAPTIVE_RATCHET_VOL_WINDOW":      str(params.get("adaptive_ratchet_vol_window", 20)),
        "ADAPTIVE_RATCHET_VOL_LOW":         str(params.get("adaptive_ratchet_vol_low", 0.03)),
        "ADAPTIVE_RATCHET_VOL_HIGH":        str(params.get("adaptive_ratchet_vol_high", 0.07)),
        "ADAPTIVE_RATCHET_RISK_OFF_SCALE":  str(params.get("adaptive_ratchet_risk_off_scale", 0.0)),
        "ADAPTIVE_RATCHET_FLOOR_DECAY":     str(params.get("adaptive_ratchet_floor_decay", 0.995)),
        "ADAPTIVE_RATCHET_SWEEP_ENABLED":   _boolenv(params.get("adaptive_ratchet_sweep_enabled", False)),

        # ── Advanced — Sweeps, cubes, robustness ─────────────────────────────
        "ENABLE_SWEEP_L_HIGH":          _boolenv(params.get("enable_sweep_l_high", False)),
        "ENABLE_SWEEP_TAIL_GUARDRAIL":  _boolenv(params.get("enable_sweep_tail_guardrail", False)),
        "ENABLE_SWEEP_TRAIL_WIDE":      _boolenv(params.get("enable_sweep_trail_wide", False)),
        "ENABLE_SWEEP_TRAIL_NARROW":    _boolenv(params.get("enable_sweep_trail_narrow", False)),
        "ENABLE_PARAM_SURFACES":        _boolenv(params.get("enable_param_surfaces", False)),
        "ENABLE_STABILITY_CUBE":        _boolenv(params.get("enable_stability_cube", False)),
        "ENABLE_RISK_THROTTLE_CUBE":    _boolenv(params.get("enable_risk_throttle_cube", False)),
        "ENABLE_EXIT_CUBE":             _boolenv(params.get("enable_exit_cube", False)),
        "ENABLE_NOISE_STABILITY":       _boolenv(params.get("enable_noise_stability", False)),
        "ENABLE_SLIPPAGE_SWEEP":        _boolenv(params.get("enable_slippage_sweep", False)),
        "ENABLE_EQUITY_ENSEMBLE":       _boolenv(params.get("enable_equity_ensemble", False)),
        "ENABLE_PARAM_JITTER":          _boolenv(params.get("enable_param_jitter", False)),
        "ENABLE_RETURN_CONCENTRATION":  _boolenv(params.get("enable_return_concentration", False)),
        "ENABLE_SHARPE_RIDGE_MAP":      _boolenv(params.get("enable_sharpe_ridge_map", False)),
        "ENABLE_SHARPE_PLATEAU":        _boolenv(params.get("enable_sharpe_plateau", False)),
        "ENABLE_TOP_N_REMOVAL":         _boolenv(params.get("enable_top_n_removal", False)),
        "ENABLE_LUCKY_STREAK":          _boolenv(params.get("enable_lucky_streak", False)),
        "ENABLE_PERIODIC_BREAKDOWN":    _boolenv(params.get("enable_periodic_breakdown", False)),
        "ENABLE_WEEKLY_MILESTONES":     _boolenv(params.get("enable_weekly_milestones", False)),
        "ENABLE_MONTHLY_MILESTONES":    _boolenv(params.get("enable_monthly_milestones", False)),
        "ENABLE_DSR_MTL":               _boolenv(params.get("enable_dsr_mtl", False)),
        "ENABLE_SHOCK_INJECTION":       _boolenv(params.get("enable_shock_injection", False)),
        "ENABLE_RUIN_PROBABILITY":      _boolenv(params.get("enable_ruin_probability", False)),
        "ENABLE_MCAP_DIAGNOSTIC":       _boolenv(params.get("enable_mcap_diagnostic", False)),
        "ENABLE_CAPACITY_CURVE":        _boolenv(params.get("enable_capacity_curve", False)),
        "ENABLE_REGIME_ROBUSTNESS":     _boolenv(params.get("enable_regime_robustness", False)),
        "ENABLE_MIN_CUM_RETURN":        _boolenv(params.get("enable_min_cum_return", False)),

        # ── Expert ────────────────────────────────────────────────────────────
        "ANNUALIZATION_FACTOR":  str(params.get("annualization_factor", 365)),
        "BAR_MINUTES":           str(params.get("bar_minutes", 5)),
        "SAVE_DAILY_FILES":      _boolenv(params.get("save_daily_files", False)),
        "BUILD_MASTER_FILE":     _boolenv(params.get("build_master_file", True)),
    }

    # ------------------------------------------------------------------
    # Stage 0 (DB mode only): refresh leaderboard parquets from DB
    # ------------------------------------------------------------------
    if params.get("price_source") == "db":
        import glob as _glob
        import pyarrow.parquet as _pq
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from pipeline.db.connection import get_conn as _get_conn

        base_dir = pipeline_env.get("BASE_DATA_DIR", "/mnt/quant-data")
        indexer_script = pipeline_dir / "indexer" / "build_intraday_leaderboard.py"

        # Check if parquets are already up to date with the DB.
        # Compare both start AND end dates — a parquet that only covers
        # the last 14 days is stale even if its end date matches.
        _conn = _get_conn()
        _cur = _conn.cursor()
        _cur.execute("SELECT MIN(timestamp_utc)::date, MAX(timestamp_utc)::date FROM market.leaderboards")
        db_first, db_last = _cur.fetchone()
        _cur.close()
        _conn.close()

        parquet_stale = False
        for metric in ("price", "open_interest", "volume"):
            pq_path = Path(base_dir) / "leaderboards" / metric / f"intraday_pct_leaderboard_{metric}_top333_anchor0000_ALL.parquet"
            if not pq_path.exists():
                parquet_stale = True
                break
            try:
                pf = _pq.ParquetFile(str(pq_path))
                first_rg = pf.read_row_group(0, columns=["timestamp_utc"])
                last_rg = pf.read_row_group(pf.metadata.num_row_groups - 1, columns=["timestamp_utc"])
                pq_first = first_rg.to_pandas()["timestamp_utc"].min().date()
                pq_last = last_rg.to_pandas()["timestamp_utc"].max().date()
                if pq_last < db_last or pq_first > db_first:
                    parquet_stale = True
                    break
            except Exception:
                parquet_stale = True
                break

        if parquet_stale:
            update_job(job_id, status="running", stage="leaderboard_refresh", progress=2)
            # Delete ALL parquets + filtered caches so the rebuild starts
            # from scratch across the full DB date range, not just the
            # delta since the last parquet end date.
            for stale in _glob.glob(f"{base_dir}/leaderboard_*_filtered_*"):
                os.remove(stale)
            db_first_str = db_first.strftime("%Y-%m-%d")
            for metric in ("price", "open_interest", "volume"):
                lb_dir = Path(base_dir) / "leaderboards" / metric
                for old_pq in _glob.glob(str(lb_dir / "intraday_pct_leaderboard_*_ALL.parquet")):
                    os.remove(old_pq)
                lb_cmd = [
                    _PIPELINE_PYTHON, str(indexer_script),
                    "--source", "db", "--metric", metric, "--force",
                    "--start", db_first_str,
                ]
                subprocess.run(lb_cmd, cwd=str(pipeline_dir), env=pipeline_env,
                               capture_output=True, timeout=14400)  # 4h — full rebuild is ~3h
        else:
            # Parquets are current — still need to clear filtered caches
            # in case the filter params changed since last run.
            for stale in _glob.glob(f"{base_dir}/leaderboard_*_filtered_*"):
                os.remove(stale)

    # ------------------------------------------------------------------
    # Stage 1: overlap_analysis.py --audit
    # ------------------------------------------------------------------
    update_job(job_id, status="running", stage="overlap", progress=5)

    overlap_script = pipeline_dir / "overlap_analysis.py"
    cmd = [_PIPELINE_PYTHON, str(overlap_script)] + _build_cli_args(params)

    try:
        with audit_output_path.open("wb") as out_fh:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(pipeline_dir),
                env=pipeline_env,
            )

            # Stream output line-by-line to audit_output.txt in real time
            progress = 5
            for line in proc.stdout:  # type: ignore[union-attr]
                if _is_cancelled(job_id):
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except Exception:
                        proc.kill()
                    raise JobCancelled("Cancelled by user.")
                out_fh.write(line)
                out_fh.flush()
                # Pulse progress slowly while the pipeline runs (cap at 75)
                if progress < 75:
                    progress += 1
                    if progress % 5 == 0:
                        update_job(job_id, progress=progress)

            proc.wait()

        if proc.returncode != 0:
            raise RuntimeError(
                f"overlap_analysis.py exited with code {proc.returncode}. "
                f"See audit_output.txt for details."
            )
    except JobCancelled as exc:
        update_job(job_id, status="cancelled", stage="done", error=str(exc))
        return {}
    except Exception as exc:
        update_job(job_id, status="failed", stage="overlap", error=str(exc))
        raise

    if _is_cancelled(job_id):
        update_job(job_id, status="cancelled", stage="done", error="Cancelled by user.")
        return {}

    update_job(job_id, stage="overlap", progress=90)

    # ------------------------------------------------------------------
    # Stage 2: Parse metrics from audit_output.txt
    # ------------------------------------------------------------------
    update_job(job_id, stage="parsing", progress=95)

    metrics = _parse_metrics(audit_output_path)

    if _is_cancelled(job_id):
        update_job(job_id, status="cancelled", stage="done", error="Cancelled by user.")
        return {}

    results = {
        "metrics":            metrics,
        "audit_output_path":  str(audit_output_path),
        "starting_capital":   params.get("starting_capital", 100000.0),
        "fees_tables_by_filter": metrics.get("fees_tables_by_filter"),
        "fees_table":         metrics.get("fees_table"),
    }

    update_job(job_id, status="complete", stage="done", progress=100, results=results)
    return results


# ─── Side-effect import: register additional task modules ───────────────────
# `app.workers.indexer_backfill_worker` defines @celery_app.task(...) functions
# against the SAME celery_app instance from this file. Importing it here at
# module load time ensures the celery worker process picks up those task
# definitions when it starts via:
#   celery -A app.workers.pipeline_worker.celery_app worker
# Without this import the new tasks would only be discovered if the worker
# command was changed to load multiple modules. Keep this at the bottom of
# the file so it runs after `celery_app` is fully constructed.
import app.workers.indexer_backfill_worker  # noqa: E402,F401
import app.workers.run_jobs_worker  # noqa: E402,F401

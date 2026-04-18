"""
Celery task that orchestrates the full audit pipeline chain:
  1. overlap_analysis.py --audit   (streams stdout+stderr → audit_output.txt)
  2. node generate_audit_report.js (produces overlap_audit_report.docx)
  3. Parse audit_output.txt for key metrics and filter comparison table
  4. Write results back to job store
"""

import json
import logging
import re
from pathlib import Path

from celery import Celery

from app.core.config import settings
from app.db import get_worker_conn
from app.services.audit.pipeline_runner import (
    JobCancelled,
    build_cli_args,
    build_pipeline_env,
    prestage_parquet,
    run_audit_subprocess,
)
from app.services.job_store import get_job, update_job

_worker_log = logging.getLogger("pipeline_worker")

celery_app = Celery("pipeline_worker", broker=settings.REDIS_URL, backend=settings.REDIS_URL)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
)

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
    r"(?:\s+Equity-R²=(?P<equity_r2>[+-]?[\d.]+))?"
)

# Ratio block lines, e.g.:
#   │  Sortino Ratio:               3.517
#   │  Calmar Ratio:                8.457
#   │  Omega Ratio:                 1.372
#   │  Ulcer Index:                23.910  (lower = better)
#   │  Profit Factor:               2.025
_SORTINO_RE         = re.compile(r"Sortino Ratio:\s*(?P<v>[+-]?[\d.]+)")
_CALMAR_RE          = re.compile(r"Calmar Ratio:\s*(?P<v>[+-]?[\d.]+)")
_OMEGA_RE           = re.compile(r"Omega Ratio:\s*(?P<v>[+-]?[\d.]+)")
_ULCER_RE           = re.compile(r"Ulcer Index:\s*(?P<v>[+-]?[\d.]+)")
_PROFIT_FACTOR_RE   = re.compile(r"Profit Factor:\s*(?P<v>[+-]?[\d.]+)")

# MONTHLY period-return-stats block, e.g.:
#   │  Best month:      124.49%   Worst:  -22.15%
_BEST_MONTH_RE = re.compile(r"Best month:\s*(?P<v>[+-]?[\d.]+)%")

# Scorecard rows (one per metric, best-filter only). Anchored to goal column
# so we don't match informational/N/A rows with the same metric name.
_SCORECARD_WIN_RATE_RE  = re.compile(r"^\s*Win Rate\s+>\d+%\s+(?P<v>[+-]?[\d.]+)%", re.MULTILINE)
_SCORECARD_AVG_DAILY_RE = re.compile(r"^\s*Avg Daily Return %\s+>\d+%\s+(?P<v>[+-]?[\d.]+)%", re.MULTILINE)

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
    r"^FINAL_DAILY_PORTFOLIO_(?P<tag>[A-Za-z0-9_\-+.]+):\s*(?P<json>\{.+\})\s*$",
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
        if canon.group("equity_r2") is not None:
            metrics["equity_r2"] = float(canon.group("equity_r2"))

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
    m = _PROFIT_FACTOR_RE.search(text)
    if m:
        metrics["profit_factor"] = float(m.group("v"))

    # ── MONTHLY period stats (best filter) ───────────────────────────────────
    m = _BEST_MONTH_RE.search(text)
    if m:
        metrics["best_month_pct"] = float(m.group("v"))

    # ── Scorecard rows (best filter only) ────────────────────────────────────
    m = _SCORECARD_WIN_RATE_RE.search(text)
    if m:
        metrics["win_rate_daily"] = float(m.group("v"))
    m = _SCORECARD_AVG_DAILY_RE.search(text)
    if m:
        metrics["avg_daily_ret_pct"] = float(m.group("v"))

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

    pipeline_env = build_pipeline_env(params)

    # ------------------------------------------------------------------
    # Stage 0 (DB mode only): refresh leaderboard parquets from DB
    # ------------------------------------------------------------------
    prestage_parquet(
        params,
        pipeline_env=pipeline_env,
        pipeline_dir=pipeline_dir,
        on_rebuild_start=lambda: update_job(
            job_id, status="running", stage="leaderboard_refresh", progress=2,
        ),
    )

    # ------------------------------------------------------------------
    # Stage 1: overlap_analysis.py --audit
    # ------------------------------------------------------------------
    update_job(job_id, status="running", stage="overlap", progress=5)

    overlap_script = pipeline_dir / "overlap_analysis.py"
    cmd = [settings.PIPELINE_PYTHON, str(overlap_script)] + build_cli_args(params)

    # Pulse progress slowly while the pipeline runs (cap at 75).
    progress = [5]

    def _bump_progress(_line: bytes) -> None:
        if progress[0] < 75:
            progress[0] += 1
            if progress[0] % 5 == 0:
                update_job(job_id, progress=progress[0])

    try:
        run_audit_subprocess(
            cmd=cmd,
            output_path=audit_output_path,
            cwd=pipeline_dir,
            env=pipeline_env,
            on_line=_bump_progress,
            cancelled=lambda: _is_cancelled(job_id),
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

    # Best-effort: also persist a lightweight audit.jobs row. This seeds the
    # allocator-visible history without populating audit.results (promotion is
    # a separate explicit action). Any failure here is logged but must not
    # disrupt the JSON-file write above — that's still the source of truth
    # for in-progress jobs and crash recovery.
    _persist_audit_job_row(job_id, params, metrics)

    return results


def _persist_audit_job_row_at_cursor(
    cur,
    job_id: str,
    params: dict,
    metrics: dict,
    *,
    strategy_version_id: str | None = None,
) -> bool:
    """Issue the audit.jobs INSERT/UPSERT against an external cursor.

    Returns True if the row was written; False if required data (fees_table
    date range) is missing — in which case the caller should NOT commit on
    our behalf. Caller owns the enclosing transaction and must commit.

    strategy_version_id defaults to NULL (user-driven audits; promote fills
    it later). The nightly CLI passes the version it's refreshing so the
    audit.jobs row is linked from birth.
    """
    fees_table = metrics.get("fees_table") or []
    if not fees_table:
        _worker_log.warning(
            "audit.jobs insert skipped for %s: metrics.fees_table empty "
            "(cannot derive date_from/date_to)", job_id,
        )
        return False
    try:
        date_from = fees_table[0].get("date")
        date_to   = fees_table[-1].get("date")
    except (AttributeError, IndexError, TypeError):
        _worker_log.warning(
            "audit.jobs insert skipped for %s: fees_table entries lack a date key",
            job_id,
        )
        return False
    if not date_from or not date_to:
        _worker_log.warning(
            "audit.jobs insert skipped for %s: date_from=%r date_to=%r",
            job_id, date_from, date_to,
        )
        return False

    cur.execute(
        """
        INSERT INTO audit.jobs
            (job_id, strategy_version_id, status,
             completed_at, date_from, date_to, config_overrides)
        VALUES (%s, %s, 'complete', NOW(), %s, %s, %s::jsonb)
        ON CONFLICT (job_id) DO UPDATE SET
            status           = 'complete',
            completed_at     = EXCLUDED.completed_at,
            date_from        = EXCLUDED.date_from,
            date_to          = EXCLUDED.date_to,
            config_overrides = EXCLUDED.config_overrides
        """,
        (job_id, strategy_version_id, date_from, date_to, json.dumps(params)),
    )
    return True


def _persist_audit_job_row(job_id: str, params: dict, metrics: dict) -> None:
    """
    INSERT (or re-assert) the audit.jobs row at finalize. strategy_version_id
    is NULL until the admin explicitly promotes this audit as a strategy via
    POST /api/simulator/audits/{job_id}/promote.

    Date range is derived from metrics["fees_table"] (first + last row's
    `date` key). If fees_table is empty or missing both dates, we skip the
    insert — date_from/date_to are NOT NULL in the schema and we refuse to
    fabricate values.

    Thin wrapper over `_persist_audit_job_row_at_cursor` that owns its own
    connection + commit. Used by the worker's best-effort finalize hook.
    """
    try:
        conn = get_worker_conn()
        try:
            with conn.cursor() as cur:
                if _persist_audit_job_row_at_cursor(cur, job_id, params, metrics):
                    conn.commit()
        finally:
            conn.close()
    except Exception as e:
        _worker_log.warning("audit.jobs insert failed for %s: %s", job_id, e)


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

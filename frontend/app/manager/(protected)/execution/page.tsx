"use client";

/**
 * frontend/app/manager/(protected)/execution/page.tsx
 * ====================================================
 * Execution-quality dashboard for the BloFin live trader.
 * Reads per-session JSON reports written by trader-blofin.py via
 *   GET /api/manager/execution-reports
 * and renders:
 *   - 6 summary KPI cards (aggregate across all reports)
 *   - Daily summary table (one row per session, click to expand)
 *   - Expanded per-symbol detail (fills + exits merged)
 */

import { useCallback, useEffect, useMemo, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Types ──────────────────────────────────────────────────────────────────

interface FillSymbol {
  inst_id: string;
  target_contracts: number;
  filled_contracts: number;
  fill_pct: number;
  est_entry_price: number;
  fill_entry_price: number | null;
  entry_slippage_bps: number | null;
  order_ids: string[];
  retry_rounds: number;
  skipped_reason: string | null;
  lev_int?: number;
  eff_lev?: number;
  notional_usd?: number;
}

interface ExitSymbol {
  inst_id: string;
  est_exit_price: number | null;
  fill_exit_price: number | null;
  exit_slippage_bps: number | null;
}

interface ExecutionReport {
  date: string;
  session_start_utc?: string;
  session_end_utc?: string;
  resumed?: boolean;
  signal?: {
    symbols_signaled: string[];
    filter: string;
    count: number;
  };
  conviction?: {
    roi_x_pct: number;
    kill_y_pct: number;
    passed: boolean;
  } | null;
  leverage?: {
    l_high?: number;
    vol_boost?: number;
    eff_lev: number;
    lev_int?: number;
  };
  capital?: {
    account_balance?: number;
    balance_pre_entry?: number | null;
    balance_post_exit?: number | null;
    usdt_total?: number;
    margin_buffer_pct?: number;
    usdt_deployable?: number;
    usdt_per_symbol?: number;
  };
  fills?: {
    total_symbols: number;
    filled_first_pass: number;
    filled_via_retry: number;
    failed: number;
    fill_rate_pct: number;
    avg_entry_slippage_bps?: number | null;
    symbols: FillSymbol[];
  };
  monitoring?: {
    bars_monitored: number;
    peak_pct: number;
    sym_stops_fired: string[];
    sym_stops_count: number;
  };
  exit?: {
    reason: string;
    est_return_1x_pct?: number | null;
    eff_lev?: number;
    est_net_return_pct?: number | null;
    close_failures?: string[];
    close_failure_count?: number;
    symbols?: ExitSymbol[];
    avg_exit_slippage_bps?: number | null;
    actual_pnl_usd?: number | null;
    actual_return_pct?: number | null;
    pnl_vs_est_pct?: number | null;
  };
  alerts_fired?: number;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

const FLAT_REASONS = new Set(["filtered", "no_entry_conviction", "missed_window"]);
const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";

function prettyReason(reason: string): string {
  return reason
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function fmt(n: number | null | undefined, digits = 2): string {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return n.toFixed(digits);
}

function fmtPct(v: number | null | undefined, digits = 2, withSign = true): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = withSign && v > 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}%`;
}

function fmtBps(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(1)}bps`;
}

function fmtUsd(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  if (Math.abs(v) >= 1000)
    return `$${Math.round(v).toLocaleString()}`;
  return `$${v.toFixed(2)}`;
}

function fmtPrice(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  if (v >= 1000) return `$${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  if (v >= 1) return `$${v.toFixed(4)}`;
  return `$${v.toFixed(6)}`;
}

// Color thresholds ----------------------------------------------------------

function entrySlipColor(bps: number | null | undefined): string {
  if (bps === null || bps === undefined) return "var(--t2)";
  const abs = Math.abs(bps);
  if (abs <= 5) return "var(--green)";
  if (abs <= 15) return "var(--amber)";
  return "var(--red)";
}

function exitSlipColor(bps: number | null | undefined): string {
  if (bps === null || bps === undefined) return "var(--t2)";
  if (bps <= 0) return "var(--green)";
  if (bps <= 10) return "var(--amber)";
  return "var(--red)";
}

function pnlGapColor(pct: number | null | undefined): string {
  if (pct === null || pct === undefined) return "var(--t2)";
  const abs = Math.abs(pct);
  if (abs <= 0.5) return "var(--green)";
  if (abs <= 1.5) return "var(--amber)";
  return "var(--red)";
}

function fillRateColor(pct: number | null | undefined): string {
  if (pct === null || pct === undefined) return "var(--t2)";
  if (pct >= 100) return "var(--green)";
  if (pct >= 75) return "var(--amber)";
  return "var(--red)";
}

function fillPctColor(pct: number | null | undefined): string {
  if (pct === null || pct === undefined || pct === 0) return "var(--red)";
  if (pct >= 100) return "var(--green)";
  return "var(--amber)";
}

// ─── Shared cell styles ─────────────────────────────────────────────────────

const thStyle: React.CSSProperties = {
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: "0.12em",
  color: "var(--t3)",
  textTransform: "uppercase",
  textAlign: "left",
  padding: "4px 8px 6px 0",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
};

const tdStyle: React.CSSProperties = {
  fontSize: 11,
  color: "var(--t1)",
  padding: "8px 8px 8px 0",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
  fontFamily: FONT_MONO,
};

const sectionLabel: React.CSSProperties = {
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: "0.12em",
  color: "var(--t3)",
  textTransform: "uppercase",
  marginBottom: 10,
};

// ─── Window selector ────────────────────────────────────────────────────────

const WINDOW_PRESETS = [
  { label: "1W",  days: 7 },
  { label: "1M",  days: 30 },
  { label: "All", days: null as number | null },
] as const;
type WindowPreset = (typeof WINDOW_PRESETS)[number];
const DEFAULT_WINDOW: WindowPreset = WINDOW_PRESETS[0]; // 1W

function WindowSegmentControl({
  value,
  onChange,
}: {
  value: WindowPreset;
  onChange: (next: WindowPreset) => void;
}) {
  return (
    <div
      role="tablist"
      aria-label="Window range"
      style={{
        display: "inline-flex",
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 4,
        overflow: "hidden",
      }}
    >
      {WINDOW_PRESETS.map((preset, i) => {
        const active = preset.label === value.label;
        return (
          <button
            key={preset.label}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => onChange(preset)}
            style={{
              background: active ? "var(--bg4)" : "transparent",
              color: active ? "var(--t0)" : "var(--t2)",
              border: "none",
              borderLeft: i === 0 ? "none" : "1px solid var(--line)",
              padding: "5px 12px",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              fontFamily: FONT_MONO,
              cursor: "pointer",
              transition: "background 0.15s ease, color 0.15s ease",
            }}
            onMouseEnter={(e) => {
              if (!active) e.currentTarget.style.color = "var(--t1)";
            }}
            onMouseLeave={(e) => {
              if (!active) e.currentTarget.style.color = "var(--t2)";
            }}
          >
            {preset.label}
          </button>
        );
      })}
    </div>
  );
}

// ─── KPI Card ───────────────────────────────────────────────────────────────

function KpiCard({
  label,
  value,
  color,
  subtitle,
}: {
  label: string;
  value: string;
  color?: string;
  subtitle?: string;
}) {
  return (
    <div
      style={{
        flex: 1,
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 5,
        padding: "14px 16px",
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          color: "var(--t3)",
          textTransform: "uppercase",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 18,
          fontWeight: 700,
          color: color || "var(--t0)",
          fontFamily: FONT_MONO,
        }}
      >
        {value}
      </div>
      {subtitle && (
        <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
          {subtitle}
        </div>
      )}
    </div>
  );
}

// ─── Badges ─────────────────────────────────────────────────────────────────

function Badge({
  label,
  bg,
  color,
}: {
  label: string;
  bg: string;
  color: string;
}) {
  return (
    <span
      style={{
        display: "inline-block",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.06em",
        padding: "2px 6px",
        borderRadius: 3,
        background: bg,
        color,
        fontFamily: FONT_MONO,
      }}
    >
      {label}
    </span>
  );
}

function convictionBadge(
  conv: ExecutionReport["conviction"],
  exitReason?: string,
) {
  // No conviction record OR no roi_x value computed => the check never ran
  // (e.g. filtered days where the signal was blocked before bar-6).
  if (
    !conv ||
    conv.roi_x_pct === null ||
    conv.roi_x_pct === undefined
  ) {
    return <span style={{ color: "var(--t3)" }}>—</span>;
  }

  const roiTxt = `${conv.roi_x_pct.toFixed(2)}%`;

  // Conviction passed but execution window had already closed — show MISS
  // (amber) instead of PASS (green) so it's clear no trade went through.
  if (conv.passed && exitReason === "missed_window") {
    return (
      <Badge
        label={`MISS ${roiTxt}`}
        bg="var(--amber-dim)"
        color="var(--amber)"
      />
    );
  }

  const bg = conv.passed ? "var(--green-dim)" : "var(--red-dim)";
  const fg = conv.passed ? "var(--green)" : "var(--red)";
  return (
    <Badge
      label={`${conv.passed ? "PASS" : "FAIL"} ${roiTxt}`}
      bg={bg}
      color={fg}
    />
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function ExecutionPage() {
  const [reports, setReports] = useState<ExecutionReport[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [windowPreset, setWindowPreset] = useState<WindowPreset>(DEFAULT_WINDOW);

  const load = useCallback(() => {
    fetch(`${API_BASE}/api/manager/execution-reports`, {
      credentials: "include",
    })
      .then((r) => {
        if (r.status === 401) throw new Error("Session expired");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => setReports(d.reports || []))
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const toggle = useCallback((date: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(date)) next.delete(date);
      else next.add(date);
      return next;
    });
  }, []);

  // Aggregate KPIs across all reports
  const kpis = useMemo(() => {
    if (!reports) {
      return {
        avgFillRate: null as number | null,
        avgEntrySlip: null as number | null,
        avgExitSlip: null as number | null,
        avgPnlGap: null as number | null,
        totalRetries: 0,
        traded: 0,
        total: 0,
      };
    }
    const fillRates: number[] = [];
    const entrySlips: number[] = [];
    const exitSlips: number[] = [];
    const pnlGaps: number[] = [];
    let totalRetries = 0;
    let traded = 0;

    for (const r of reports) {
      if (r.fills) {
        fillRates.push(r.fills.fill_rate_pct);
        if (
          r.fills.avg_entry_slippage_bps !== null &&
          r.fills.avg_entry_slippage_bps !== undefined
        )
          entrySlips.push(r.fills.avg_entry_slippage_bps);
        totalRetries += r.fills.filled_via_retry || 0;
        traded += 1;
      }
      if (r.exit) {
        if (
          r.exit.avg_exit_slippage_bps !== null &&
          r.exit.avg_exit_slippage_bps !== undefined
        )
          exitSlips.push(r.exit.avg_exit_slippage_bps);
        if (
          r.exit.pnl_vs_est_pct !== null &&
          r.exit.pnl_vs_est_pct !== undefined
        )
          pnlGaps.push(r.exit.pnl_vs_est_pct);
      }
    }
    const avg = (xs: number[]) =>
      xs.length === 0 ? null : xs.reduce((a, b) => a + b, 0) / xs.length;
    return {
      avgFillRate: avg(fillRates),
      avgEntrySlip: avg(entrySlips),
      avgExitSlip: avg(exitSlips),
      avgPnlGap: avg(pnlGaps),
      totalRetries,
      traded,
      total: reports.length,
    };
  }, [reports]);

  if (error) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--red)" }}>
        Error: {error}
      </div>
    );
  }

  if (!reports) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--t2)" }}>
        Loading execution reports…
      </div>
    );
  }

  // Apply window filter for the table only — KPIs above are universal.
  const visibleReports = (() => {
    if (windowPreset.days === null) return reports;
    const cutoff = new Date(Date.now() - windowPreset.days * 86400000);
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    return reports.filter((r) => r.date >= cutoffStr);
  })();

  return (
    <div
      style={{
        padding: 20,
        display: "flex",
        flexDirection: "column",
        gap: 14,
        height: "100%",
        overflow: "auto",
      }}
    >
      {/* Row 1: KPI cards */}
      <div style={{ display: "flex", gap: 10 }}>
        <KpiCard
          label="Avg Fill Rate"
          value={kpis.avgFillRate === null ? "—" : `${kpis.avgFillRate.toFixed(1)}%`}
          color="var(--green)"
        />
        <KpiCard
          label="Avg Entry Slip"
          value={fmtBps(kpis.avgEntrySlip)}
          color={entrySlipColor(kpis.avgEntrySlip)}
        />
        <KpiCard
          label="Avg Exit Slip"
          value={fmtBps(kpis.avgExitSlip)}
          color={exitSlipColor(kpis.avgExitSlip)}
        />
        <KpiCard
          label="Avg PnL Gap"
          value={fmtPct(kpis.avgPnlGap)}
          color={pnlGapColor(kpis.avgPnlGap)}
        />
        <KpiCard
          label="Retries Needed"
          value={`${kpis.totalRetries}`}
          color={kpis.totalRetries > 0 ? "var(--amber)" : "var(--t0)"}
        />
        <KpiCard
          label="Sessions Traded"
          value={`${kpis.traded} / ${kpis.total}`}
        />
      </div>

      {/* Row 2: Daily summary table */}
      <div
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          padding: "12px 16px",
          overflow: "auto",
          flex: 1,
          minHeight: 0,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: 10,
          }}
        >
          <div style={{ ...sectionLabel, marginBottom: 0 }}>
            Daily Execution Summary
            {windowPreset.days !== null && (
              <span
                style={{
                  marginLeft: 10,
                  color: "var(--t2)",
                  fontWeight: 400,
                  letterSpacing: "0.06em",
                }}
              >
                · {visibleReports.length} of {reports.length}
              </span>
            )}
          </div>
          <WindowSegmentControl
            value={windowPreset}
            onChange={setWindowPreset}
          />
        </div>
        {reports.length === 0 ? (
          <div style={{ fontSize: 11, color: "var(--t3)" }}>
            No execution reports yet. The first session report will appear
            here after the trader runs.
          </div>
        ) : visibleReports.length === 0 ? (
          <div style={{ fontSize: 11, color: "var(--t3)" }}>
            No reports in the selected window.
          </div>
        ) : (
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontFamily: FONT_MONO,
            }}
          >
            <thead>
              <tr>
                {[
                  "",
                  "Date",
                  "Signal",
                  "Conviction",
                  "Filled",
                  "Retried",
                  "Fill Rate",
                  "Entry Slip",
                  "Exit Slip",
                  "Est Ret",
                  "Actual Ret",
                  "PnL Gap",
                  "Lev",
                  "Exit",
                  "Alerts",
                ].map((h, i) => (
                  <th key={i} style={thStyle}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {visibleReports.map((r) => (
                <DayRow
                  key={r.date}
                  report={r}
                  expanded={expanded.has(r.date)}
                  onToggle={() => toggle(r.date)}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

// ─── Day Row ────────────────────────────────────────────────────────────────

function DayRow({
  report,
  expanded,
  onToggle,
}: {
  report: ExecutionReport;
  expanded: boolean;
  onToggle: () => void;
}) {
  const exitReason = report.exit?.reason ?? "";
  const flat = FLAT_REASONS.has(exitReason);

  if (flat) {
    return (
      <tr style={{ opacity: 0.5 }}>
        <td style={tdStyle}></td>
        <td style={{ ...tdStyle, color: "var(--t2)" }}>{report.date}</td>
        <td style={tdStyle}>{report.signal?.count ?? 0}</td>
        <td style={tdStyle}>{convictionBadge(report.conviction, exitReason)}</td>
        <td
          colSpan={11}
          style={{
            ...tdStyle,
            color: "var(--t2)",
            fontStyle: "italic",
          }}
        >
          {prettyReason(exitReason)}
        </td>
      </tr>
    );
  }

  const fills = report.fills;
  const exitBlk = report.exit;
  const lev = report.leverage;
  const alerts = report.alerts_fired ?? 0;

  const filledCount =
    (fills?.filled_first_pass ?? 0) + (fills?.filled_via_retry ?? 0);
  const totalSyms = fills?.total_symbols ?? 0;
  const retried = fills?.filled_via_retry ?? 0;

  return (
    <>
      <tr
        onClick={onToggle}
        style={{
          cursor: "pointer",
          background: expanded ? "var(--bg3)" : undefined,
        }}
      >
        <td style={{ ...tdStyle, width: 18, color: "var(--t2)" }}>
          {expanded ? "▾" : "▸"}
        </td>
        <td style={{ ...tdStyle, color: "var(--t0)" }}>{report.date}</td>
        <td style={tdStyle}>{report.signal?.count ?? 0}</td>
        <td style={tdStyle}>{convictionBadge(report.conviction, exitReason)}</td>
        <td
          style={{
            ...tdStyle,
            color: filledCount < totalSyms ? "var(--amber)" : "var(--t1)",
          }}
        >
          {filledCount}/{totalSyms}
        </td>
        <td
          style={{
            ...tdStyle,
            color: retried > 0 ? "var(--amber)" : "var(--t1)",
          }}
        >
          {retried}
        </td>
        <td
          style={{
            ...tdStyle,
            color: fillRateColor(fills?.fill_rate_pct),
          }}
        >
          {fills ? `${fills.fill_rate_pct.toFixed(0)}%` : "—"}
        </td>
        <td
          style={{
            ...tdStyle,
            color: entrySlipColor(fills?.avg_entry_slippage_bps),
          }}
        >
          {fmtBps(fills?.avg_entry_slippage_bps)}
        </td>
        <td
          style={{
            ...tdStyle,
            color: exitSlipColor(exitBlk?.avg_exit_slippage_bps),
          }}
        >
          {fmtBps(exitBlk?.avg_exit_slippage_bps)}
        </td>
        <td style={tdStyle}>{fmtPct(exitBlk?.est_net_return_pct)}</td>
        <td style={tdStyle}>{fmtPct(exitBlk?.actual_return_pct)}</td>
        <td
          style={{
            ...tdStyle,
            color: pnlGapColor(exitBlk?.pnl_vs_est_pct),
          }}
        >
          {fmtPct(exitBlk?.pnl_vs_est_pct)}
        </td>
        <td style={tdStyle}>{lev?.eff_lev ? `${lev.eff_lev.toFixed(2)}x` : "—"}</td>
        <td style={{ ...tdStyle, color: "var(--t2)" }}>
          {prettyReason(exitReason || "—")}
        </td>
        <td
          style={{
            ...tdStyle,
            color: alerts > 0 ? "var(--red)" : "var(--t2)",
            fontWeight: alerts > 0 ? 700 : 400,
          }}
        >
          {alerts}
        </td>
      </tr>
      {expanded && (
        <tr style={{ background: "var(--bg1)" }}>
          <td colSpan={15} style={{ padding: "10px 12px 18px 32px" }}>
            <SymbolDetails report={report} />
          </td>
        </tr>
      )}
    </>
  );
}

// ─── Expanded per-symbol detail ─────────────────────────────────────────────

function SymbolDetails({ report }: { report: ExecutionReport }) {
  const fillBy: Record<string, FillSymbol> = {};
  for (const s of report.fills?.symbols ?? []) fillBy[s.inst_id] = s;

  const exitBy: Record<string, ExitSymbol> = {};
  for (const s of report.exit?.symbols ?? []) exitBy[s.inst_id] = s;

  const symStopped = new Set(report.monitoring?.sym_stops_fired ?? []);

  // Union of all inst_ids so skipped / never-filled symbols still show
  const allIds = new Set<string>([
    ...Object.keys(fillBy),
    ...Object.keys(exitBy),
  ]);
  const rows = Array.from(allIds).map((id) => ({
    inst_id: id,
    fill: fillBy[id],
    exit: exitBy[id],
    sym_stopped: symStopped.has(id),
  }));
  rows.sort((a, b) => a.inst_id.localeCompare(b.inst_id));

  if (rows.length === 0) {
    return (
      <div style={{ fontSize: 11, color: "var(--t3)" }}>
        No per-symbol data for this session.
      </div>
    );
  }

  const columns = [
    "Symbol",
    "Target",
    "Filled",
    "Fill %",
    "Est Entry",
    "Fill Entry",
    "Entry Slip",
    "Est Exit",
    "Fill Exit",
    "Exit Slip",
    "Leverage",
    "Notional",
    "Retry",
    "Sym Stop",
    "Skip Reason",
  ];

  return (
    <table
      style={{
        width: "100%",
        borderCollapse: "collapse",
        fontFamily: FONT_MONO,
      }}
    >
      <thead>
        <tr>
          {columns.map((c) => (
            <th key={c} style={thStyle}>
              {c}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const f = row.fill;
          const e = row.exit;
          const entryBps = f?.entry_slippage_bps ?? null;
          const exitBps = e?.exit_slippage_bps ?? null;
          const retry = f?.retry_rounds ?? 0;
          const fillPct = f?.fill_pct ?? 0;

          return (
            <tr key={row.inst_id}>
              <td style={{ ...tdStyle, color: "var(--t0)" }}>{row.inst_id}</td>
              <td style={tdStyle}>{f ? fmt(f.target_contracts, 4) : "—"}</td>
              <td style={tdStyle}>{f ? fmt(f.filled_contracts, 4) : "—"}</td>
              <td style={{ ...tdStyle, color: fillPctColor(fillPct) }}>
                {f ? `${fillPct.toFixed(0)}%` : "—"}
              </td>
              <td style={tdStyle}>{fmtPrice(f?.est_entry_price)}</td>
              <td style={tdStyle}>{fmtPrice(f?.fill_entry_price)}</td>
              <td style={{ ...tdStyle, color: entrySlipColor(entryBps) }}>
                {fmtBps(entryBps)}
              </td>
              <td style={tdStyle}>{fmtPrice(e?.est_exit_price)}</td>
              <td style={tdStyle}>{fmtPrice(e?.fill_exit_price)}</td>
              <td style={{ ...tdStyle, color: exitSlipColor(exitBps) }}>
                {fmtBps(exitBps)}
              </td>
              <td style={tdStyle}>
                {f?.lev_int ? (
                  <>
                    <span style={{ color: "var(--t0)" }}>{f.lev_int}x</span>
                    <span style={{ color: "var(--t2)" }}>
                      {" "}
                      ({f.eff_lev?.toFixed(2)}x)
                    </span>
                  </>
                ) : (
                  "—"
                )}
              </td>
              <td style={tdStyle}>{fmtUsd(f?.notional_usd)}</td>
              <td style={tdStyle}>
                {retry > 0 ? (
                  <Badge
                    label={`R${retry}`}
                    bg="var(--amber-dim)"
                    color="var(--amber)"
                  />
                ) : (
                  <span style={{ color: "var(--t3)" }}>—</span>
                )}
              </td>
              <td style={tdStyle}>
                {row.sym_stopped ? (
                  <Badge label="-6.0%" bg="var(--red-dim)" color="var(--red)" />
                ) : (
                  <span style={{ color: "var(--t3)" }}>—</span>
                )}
              </td>
              <td style={{ ...tdStyle, color: "var(--t2)", fontSize: 10 }}>
                {f?.skipped_reason ?? "—"}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

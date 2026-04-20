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
import SessionLogs from "./SessionLogs";

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

// ─── Multi-tenant summary types (new endpoint) ──────────────────────────────
// Fed by GET /api/manager/execution-summary (reads allocation_returns joined
// with portfolio_sessions). Execution-quality fields (fill_rate, slip_bps,
// retried, alerts) are null today — writer extension tracked for Session E+.

interface AvailableAlloc {
  allocation_id: string;
  exchange: string;
  strategy_label: string;
  capital_usd: number | null;
}

interface SummaryKpis {
  avg_fill_rate:      number | null;
  avg_entry_slip_bps: number | null;
  avg_exit_slip_bps:  number | null;
  avg_pnl_gap:        number | null;
  retries_needed:     number;
  sessions_traded:    number;
  sessions_total:     number;
}

interface SummaryDaily {
  date: string;
  allocation_id: string;
  exchange: string;
  strategy_label: string;
  capital_usd: number | null;
  signal_count: number | null;
  conviction: { passed: boolean; return_pct: number | null } | null;
  filled: boolean;
  retried: number | null;
  fill_rate: number | null;
  entry_slip_bps: number | null;
  exit_slip_bps: number | null;
  est_return_pct: number | null;
  actual_return_pct: number | null;
  pnl_gap_pct_from_gross: number | null;
  leverage_applied: number | null;
  bars_count: number | null;
  sym_stops_count: number | null;
  peak_portfolio_return: number | null;
  max_dd_from_peak: number | null;
  capital_deployed_usd: number | null;
  exit_reason: string | null;
  alerts: number | null;
}

interface ExecutionSummary {
  available_allocations: AvailableAlloc[];
  kpis:                  SummaryKpis;
  daily:                 SummaryDaily[];
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

// ─── Allocation filter (single-select for v1) ──────────────────────────────
// TODO: lift to page-level or URL query param if Overview / Portfolios adopt
// the same control. Multi-select is a follow-up if users ask.

function AllocationFilter({
  value,
  onChange,
  options,
}: {
  value: "all" | string;
  onChange: (next: "all" | string) => void;
  options: AvailableAlloc[];
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value as "all" | string)}
      style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 4,
        color: "var(--t1)",
        fontFamily: FONT_MONO,
        fontSize: 10,
        padding: "5px 10px",
        cursor: "pointer",
      }}
    >
      <option value="all">All allocations</option>
      {options.map((a) => (
        <option key={a.allocation_id} value={a.allocation_id}>
          {a.exchange} · {a.strategy_label}
        </option>
      ))}
    </select>
  );
}

// ─── Master-history toggle (temporary) ─────────────────────────────────────
// REMOVE after master cron retirement per docs/open_work_list.md Phase 2 gate
// (earliest 2026-04-28).

function IncludeMasterToggle({
  on,
  onChange,
}: {
  on: boolean;
  onChange: (next: boolean) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onChange(!on)}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 8,
        background: "transparent",
        border: "1px solid var(--line)",
        borderRadius: 4,
        padding: "4px 10px",
        fontFamily: FONT_MONO,
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        color: on ? "var(--t0)" : "var(--t2)",
        cursor: "pointer",
      }}
    >
      <span
        style={{
          width: 22,
          height: 12,
          borderRadius: 6,
          background: on ? "var(--green)" : "var(--bg4)",
          position: "relative",
          display: "inline-block",
          transition: "background 0.15s ease",
        }}
      >
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: on ? "var(--bg0)" : "var(--t2)",
            position: "absolute",
            top: 2,
            left: on ? 12 : 2,
            transition: "left 0.15s ease",
          }}
        />
      </span>
      Include master history
    </button>
  );
}

// ─── Empty-state banner ────────────────────────────────────────────────────

function TabBanner({ text }: { text: string }) {
  return (
    <div
      style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderLeft: "3px solid var(--green)",
        borderRadius: 4,
        padding: "10px 14px",
        fontSize: 10,
        color: "var(--t1)",
        fontFamily: FONT_MONO,
      }}
    >
      {text}
    </div>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function ExecutionPage() {
  const [reports, setReports] = useState<ExecutionReport[] | null>(null);
  const [summary, setSummary] = useState<ExecutionSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [windowPreset, setWindowPreset] = useState<WindowPreset>(DEFAULT_WINDOW);
  const [allocFilter, setAllocFilter] = useState<"all" | string>("all");
  const [includeMaster, setIncludeMaster] = useState(false);
  // null → SessionLogs shows the most recent session. Any row click sets this
  // to the clicked row's date so the log viewer correlates with the table.
  const [selectedLogDate, setSelectedLogDate] = useState<string | null>(null);
  // Mutually-exclusive section expansion: at most one of the two detail
  // panels (table / logs) is open at a time so each gets full vertical
  // space when active. null = both collapsed.
  const [activeSection, setActiveSection] = useState<"table" | "logs" | null>("table");
  const toggleSection = useCallback((name: "table" | "logs") => {
    setActiveSection((cur) => (cur === name ? null : name));
  }, []);

  // Master-history fetch: only active when "Include master history" is ON.
  // Legacy /api/manager/execution-reports path — retire once master cron
  // is retired per docs/open_work_list.md Phase 2 gate.
  useEffect(() => {
    if (!includeMaster) { setReports(null); return; }
    let cancelled = false;
    fetch(`${API_BASE}/api/manager/execution-reports`, { credentials: "include" })
      .then((r) => {
        if (r.status === 401) throw new Error("Session expired");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => { if (!cancelled) setReports(d.reports || []); })
      .catch((e) => { if (!cancelled) setError(e.message); });
    return () => { cancelled = true; };
  }, [includeMaster]);

  // Multi-tenant summary fetch: drives KPIs + daily table + filter dropdown.
  // Re-runs when allocation filter or window preset changes.
  useEffect(() => {
    let cancelled = false;
    const params = new URLSearchParams();
    params.set("range", windowPreset.label);
    if (allocFilter !== "all") params.set("allocation_ids", allocFilter);
    fetch(`${API_BASE}/api/manager/execution-summary?${params}`, { credentials: "include" })
      .then((r) => {
        if (r.status === 401) throw new Error("Session expired");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: ExecutionSummary) => { if (!cancelled) setSummary(d); })
      .catch((e) => { if (!cancelled) setError(e.message); });
    return () => { cancelled = true; };
  }, [allocFilter, windowPreset]);

  const toggle = useCallback((date: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(date)) next.delete(date);
      else next.add(date);
      return next;
    });
    // Correlate the log viewer with the clicked row.
    setSelectedLogDate(date);
  }, []);

  // KPIs now come from the backend summary (capital-weighted when multiple
  // allocations in scope). Execution-quality fields are null until the
  // telemetry writer extension ships (Session E+).
  const kpis = summary?.kpis ?? {
    avg_fill_rate:      null,
    avg_entry_slip_bps: null,
    avg_exit_slip_bps:  null,
    avg_pnl_gap:        null,
    retries_needed:     0,
    sessions_traded:    0,
    sessions_total:     0,
  };

  if (error) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--red)" }}>
        Error: {error}
      </div>
    );
  }

  if (!summary) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--t2)" }}>
        Loading execution summary…
      </div>
    );
  }

  // Master-history rows filtered by window (same semantic the legacy page had).
  const visibleReports = (() => {
    if (!reports) return [];
    if (windowPreset.days === null) return reports;
    const cutoff = new Date(Date.now() - windowPreset.days * 86400000);
    const cutoffStr = cutoff.toISOString().slice(0, 10);
    return reports.filter((r) => r.date >= cutoffStr);
  })();

  // Banner logic — two distinct empty-state conditions.
  // Condition A: zero sessions ever (no data yet — first session pending)
  // Condition B: sessions exist but execution-quality metrics are universally null
  //              (telemetry writer hasn't shipped yet).
  let bannerText: string | null = null;
  if (summary.daily.length === 0) {
    bannerText =
      "Execution telemetry begins with the first session close (~23:55 UTC 2026-04-21).";
  } else if (summary.daily.every((d) => d.fill_rate === null)) {
    bannerText =
      "Execution-quality metrics (fill rate, slippage, retries) populate once the per-allocation telemetry writer ships. Tracked for Session E/F.";
  }

  // When a single allocation is selected, hide the ALLOCATION column in the
  // table — it would be redundant. Hide master rows too since they're not
  // scoped to a specific allocation.
  const showAllocCol = allocFilter === "all";
  const showMasterRows = includeMaster && allocFilter === "all";

  // Unified row list: allocation rows from summary.daily, master rows from
  // visibleReports. Sorted by date DESC (summary.daily already is); master
  // rows merge by date order.
  type CombinedRow =
    | { kind: "alloc"; date: string; data: SummaryDaily }
    | { kind: "master"; date: string; data: ExecutionReport };

  const combinedRows: CombinedRow[] = [
    ...summary.daily.map(
      (d) => ({ kind: "alloc" as const, date: d.date, data: d }),
    ),
    ...(showMasterRows
      ? visibleReports.map(
          (r) => ({ kind: "master" as const, date: r.date, data: r }),
        )
      : []),
  ];
  combinedRows.sort((a, b) => (a.date < b.date ? 1 : a.date > b.date ? -1 : 0));

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
      {/* Row 0: filter + master toggle (controls row) */}
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <AllocationFilter
          value={allocFilter}
          onChange={setAllocFilter}
          options={summary.available_allocations}
        />
        <IncludeMasterToggle on={includeMaster} onChange={setIncludeMaster} />
      </div>

      {/* Row 0b: banner (Condition A / B / none) */}
      {bannerText && <TabBanner text={bannerText} />}

      {/* Row 1: KPI cards — static titles, values from summary.kpis */}
      <div style={{ display: "flex", gap: 10 }}>
        <KpiCard
          label="Avg Fill Rate"
          value={kpis.avg_fill_rate === null ? "—" : `${kpis.avg_fill_rate.toFixed(1)}%`}
          color={kpis.avg_fill_rate === null ? "var(--t2)" : "var(--green)"}
        />
        <KpiCard
          label="Avg Entry Slip"
          value={fmtBps(kpis.avg_entry_slip_bps)}
          color={entrySlipColor(kpis.avg_entry_slip_bps)}
        />
        <KpiCard
          label="Avg Exit Slip"
          value={fmtBps(kpis.avg_exit_slip_bps)}
          color={exitSlipColor(kpis.avg_exit_slip_bps)}
        />
        <KpiCard
          label="Avg PnL Gap"
          value={fmtPct(kpis.avg_pnl_gap)}
          color={pnlGapColor(kpis.avg_pnl_gap)}
        />
        <KpiCard
          label="Retries Needed"
          value={`${kpis.retries_needed}`}
          color={kpis.retries_needed > 0 ? "var(--amber)" : "var(--t0)"}
        />
        <KpiCard
          label="Sessions Traded"
          value={`${kpis.sessions_traded} / ${kpis.sessions_total}`}
        />
      </div>

      {/* Row 2: Daily summary table (collapsible; mutually exclusive with logs) */}
      <div
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          overflow: "hidden",
          // Take remaining vertical space only when expanded so the
          // collapsed header sits snug above the next section.
          flex: activeSection === "table" ? 1 : "0 0 auto",
          minHeight: 0,
          display: "flex",
          flexDirection: "column",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "12px 16px",
          }}
        >
          <button
            type="button"
            onClick={() => toggleSection("table")}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 10,
              background: "transparent",
              border: "none",
              padding: 0,
              cursor: "pointer",
              fontFamily: FONT_MONO,
              color: "var(--t3)",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
            }}
          >
            <span style={{ color: "var(--t2)", width: 14 }}>
              {activeSection === "table" ? "▾" : "▸"}
            </span>
            <span>Daily Execution Summary</span>
            {activeSection === "table" && (
              <span
                style={{
                  color: "var(--t2)",
                  fontWeight: 400,
                  letterSpacing: "0.06em",
                }}
              >
                · {combinedRows.length} row{combinedRows.length === 1 ? "" : "s"}
              </span>
            )}
            {activeSection !== "table" && combinedRows.length > 0 && (
              <span
                style={{
                  color: "var(--t2)",
                  fontWeight: 400,
                  letterSpacing: "0.06em",
                }}
              >
                · {combinedRows.length} sessions
              </span>
            )}
          </button>
          {activeSection === "table" && (
            <WindowSegmentControl
              value={windowPreset}
              onChange={setWindowPreset}
            />
          )}
        </div>
        {activeSection === "table" && (
        <div style={{
          padding: "0 16px 12px",
          overflow: "auto",
          flex: 1,
          minHeight: 0,
        }}>
        {combinedRows.length === 0 ? (
          <div style={{ fontSize: 11, color: "var(--t3)" }}>
            {summary.daily.length === 0
              ? "No sessions in the selected range for this allocation."
              : "No reports in the selected window."}
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
                  ...(showAllocCol ? ["Allocation"] : []),
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
              {combinedRows.map((row, idx) =>
                row.kind === "alloc" ? (
                  <AllocationDayRow
                    key={`alloc-${row.data.allocation_id}-${row.date}-${idx}`}
                    row={row.data}
                    showAllocCol={showAllocCol}
                  />
                ) : (
                  <DayRow
                    key={`master-${row.date}-${idx}`}
                    report={row.data}
                    expanded={expanded.has(row.date)}
                    onToggle={() => toggle(row.date)}
                    showAllocCol={showAllocCol}
                  />
                ),
              )}
            </tbody>
          </table>
        )}
        </div>
        )}
      </div>

      {/* Row 3: Session logs. Multi-tenant mode shows an informational panel
          (per-allocation logs deferred to the telemetry writer extension);
          master rows continue to show the existing SessionLogs component
          when "Include master history" is on. */}
      <div
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          overflow: "hidden",
          flex: activeSection === "logs" ? 1 : "0 0 auto",
          minHeight: 0,
          display: "flex",
          flexDirection: "column",
        }}
      >
        <button
          type="button"
          onClick={() => toggleSection("logs")}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            background: "transparent",
            border: "none",
            padding: "12px 16px",
            cursor: "pointer",
            fontFamily: FONT_MONO,
            color: "var(--t3)",
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            textTransform: "uppercase",
          }}
        >
          <span style={{ color: "var(--t2)", width: 14 }}>
            {activeSection === "logs" ? "▾" : "▸"}
          </span>
          <span>Session Logs</span>
        </button>
        {activeSection === "logs" && (
          showMasterRows && selectedLogDate ? (
            // Master-row selected — existing SessionLogs component.
            <SessionLogs
              selectedDate={selectedLogDate}
              expanded
              onToggle={() => toggleSection("logs")}
            />
          ) : (
            // Multi-tenant rows OR no master row selected — info panel.
            <div style={{ padding: "0 16px 18px", fontSize: 10, color: "var(--t2)", lineHeight: 1.6 }}>
              Per-allocation session logs will be available once the execution
              telemetry writer ships (tracked for Session E/F). Master session
              logs remain accessible via the &quot;Include master history&quot;
              toggle above — expand a master row to view them here.
            </div>
          )
        )}
      </div>
    </div>
  );
}

// ─── Allocation day row (multi-tenant) ─────────────────────────────────────
// Renders one row in the daily summary table for an allocation_returns entry.
// Execution-quality columns (fill rate, slip, retries, alerts) render "—"
// until the writer extension populates them. Not expandable — per-symbol
// detail for allocations is part of the deferred writer extension.

function AllocationDayRow({
  row,
  showAllocCol,
}: {
  row: SummaryDaily;
  showAllocCol: boolean;
}) {
  const exitReason = row.exit_reason ?? "";
  const flat = FLAT_REASONS.has(exitReason);

  const allocCell = showAllocCol ? (
    <td style={{ ...tdStyle, color: "var(--t1)" }}>
      {row.exchange} · {row.strategy_label}
    </td>
  ) : null;

  // Conviction badge synthesis: backend gives us {passed, return_pct};
  // convictionBadge expects the master-report shape, so adapt.
  const convBadge = row.conviction ? (
    <Badge
      label={`${row.conviction.passed ? "PASS" : "FAIL"}${
        row.conviction.return_pct !== null
          ? ` ${row.conviction.return_pct.toFixed(2)}%`
          : ""
      }`}
      bg={row.conviction.passed ? "var(--green-dim)" : "var(--red-dim)"}
      color={row.conviction.passed ? "var(--green)" : "var(--red)"}
    />
  ) : (
    <span style={{ color: "var(--t3)" }}>—</span>
  );

  if (flat) {
    return (
      <tr style={{ opacity: 0.6 }}>
        <td style={tdStyle}></td>
        <td style={{ ...tdStyle, color: "var(--t2)" }}>{row.date}</td>
        {allocCell}
        <td style={tdStyle}>{row.signal_count ?? "—"}</td>
        <td style={tdStyle}>{convBadge}</td>
        <td
          colSpan={showAllocCol ? 11 : 11}
          style={{ ...tdStyle, color: "var(--t2)", fontStyle: "italic" }}
        >
          {prettyReason(exitReason)}
        </td>
      </tr>
    );
  }

  return (
    <tr>
      <td style={tdStyle}></td>
      <td style={{ ...tdStyle, color: "var(--t0)" }}>{row.date}</td>
      {allocCell}
      <td style={tdStyle}>{row.signal_count ?? "—"}</td>
      <td style={tdStyle}>{convBadge}</td>
      <td style={{ ...tdStyle, color: "var(--t1)" }}>
        {row.filled ? "yes" : "no"}
      </td>
      <td style={tdStyle}>{row.retried ?? "—"}</td>
      <td style={{ ...tdStyle, color: fillRateColor(row.fill_rate) }}>
        {row.fill_rate === null ? "—" : `${row.fill_rate.toFixed(0)}%`}
      </td>
      <td style={{ ...tdStyle, color: entrySlipColor(row.entry_slip_bps) }}>
        {fmtBps(row.entry_slip_bps)}
      </td>
      <td style={{ ...tdStyle, color: exitSlipColor(row.exit_slip_bps) }}>
        {fmtBps(row.exit_slip_bps)}
      </td>
      <td style={tdStyle}>{fmtPct(row.est_return_pct)}</td>
      <td style={tdStyle}>{fmtPct(row.actual_return_pct)}</td>
      <td style={{ ...tdStyle, color: pnlGapColor(row.pnl_gap_pct_from_gross) }}>
        {fmtPct(row.pnl_gap_pct_from_gross)}
      </td>
      <td style={tdStyle}>
        {row.leverage_applied ? `${row.leverage_applied.toFixed(2)}x` : "—"}
      </td>
      <td style={{ ...tdStyle, color: "var(--t2)" }}>
        {prettyReason(exitReason || "—")}
      </td>
      <td style={{ ...tdStyle, color: "var(--t2)" }}>{row.alerts ?? "—"}</td>
    </tr>
  );
}

// ─── Day Row ────────────────────────────────────────────────────────────────

function DayRow({
  report,
  expanded,
  onToggle,
  showAllocCol,
}: {
  report: ExecutionReport;
  expanded: boolean;
  onToggle: () => void;
  showAllocCol: boolean;
}) {
  const exitReason = report.exit?.reason ?? "";
  const flat = FLAT_REASONS.has(exitReason);
  const masterLabel = (
    <span style={{ color: "var(--t3)", fontStyle: "italic" }}>
      Master (pre-multi-tenant)
    </span>
  );
  const flatColSpan = showAllocCol ? 11 : 11; // allocation col is inserted pre-fill/conviction; fill run starts after conviction
  const detailColSpan = showAllocCol ? 16 : 15;

  if (flat) {
    return (
      <tr
        onClick={onToggle}
        style={{ opacity: 0.6, cursor: "pointer" }}
      >
        <td style={tdStyle}></td>
        <td style={{ ...tdStyle, color: "var(--t2)" }}>{report.date}</td>
        {showAllocCol && <td style={tdStyle}>{masterLabel}</td>}
        <td style={tdStyle}>{report.signal?.count ?? 0}</td>
        <td style={tdStyle}>{convictionBadge(report.conviction, exitReason)}</td>
        <td
          colSpan={flatColSpan}
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
        {showAllocCol && <td style={tdStyle}>{masterLabel}</td>}
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
          <td colSpan={detailColSpan} style={{ padding: "10px 12px 18px 32px" }}>
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

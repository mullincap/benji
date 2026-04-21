"use client";

import { useCallback, useEffect, useState, useRef } from "react";
import Skeleton, { KPIGridSkeleton, TableSkeleton } from "../../../components/Skeleton";
import { RangeTabs, TimeRange } from "../../../components/RangeTabs";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Tooltip,
} from "chart.js";
import { Line, Bar } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Tooltip
);

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Types ──────────────────────────────────────────────────────────────────

interface PerfDay {
  date: string;
  equity_usd: number | null;
  daily_return: number | null;
  drawdown: number | null;
}

interface Allocation {
  allocation_id: string;
  strategy_version_id: string;
  connection_id: string;
  capital_usd: number;
  status: string;
  version_label: string;
  strategy_display_name: string;
  filter_mode: string;
  exchange: string;
  connection_label: string;
  daily_return_today: number;
  max_drawdown: number;
  sharpe: number | null;
  performance_30d: PerfDay[];
}

interface Snapshot {
  connection_id: string;
  exchange: string;
  label: string;
  snapshot_at: string | null;
  total_equity_usd: number | null;
  available_usd: number | null;
  unrealized_pnl: number | null;
  positions: unknown[];
}

interface PipelineJob {
  status: string;
  last_run: string | null;
}

interface OverviewData {
  allocations: Allocation[];
  total_aum: number;
  today_pct: number;
  today_usd: number;
  wtd_pct: number;
  wtd_usd: number;
  mtd_pct: number;
  mtd_usd: number;
  total_pnl_usd: number;
  total_pnl_pct: number;
  max_drawdown: number;
  portfolio_equity_30d: { date: string; equity_usd: number }[];
  intraday_equity: { time: string; equity_usd: number }[];
  pipeline: {
    compiler: PipelineJob;
    indexer: PipelineJob;
    trader: PipelineJob;
    signals_last_generated: string | null;
  };
  exchange_snapshots: Snapshot[];
  total_live_equity_usd: number | null;
  total_unrealized_pnl: number | null;
}

interface PortfolioSeries {
  range: TimeRange;
  granularity: "intraday" | "daily";
  first_data_date: string | null;
  real_days: number;
  portfolio_equity: { date: string; equity_usd: number }[];
  daily_returns: { date: string; return_pct: number; return_usd: number }[];
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function pctColor(v: number): string {
  if (v > 0) return "var(--green)";
  if (v < 0) return "var(--red)";
  return "var(--t1)";
}

function fmtPct(v: number): string {
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(2)}%`;
}

function fmtUsdSigned(v: number): string {
  const sign = v > 0 ? "+" : v < 0 ? "−" : "";
  const abs = Math.abs(v);
  return `${sign}$${abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

/** Human "Apr 20" from an ISO date string. Returns "—" for falsy input. */
function shortDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function equitySubtitle(range: TimeRange, s: PortfolioSeries | null): string {
  if (!s) return "Loading…";
  if (range === "1D") {
    return s.portfolio_equity.length > 0
      ? `Today · ${s.portfolio_equity.length} snapshots`
      : "Today · no snapshots yet";
  }
  const expected = range === "1W" ? 7 : range === "1M" ? 30 : null;
  if (expected !== null && s.real_days >= expected) {
    return `Last ${expected} days`;
  }
  if (s.first_data_date && s.real_days > 0) {
    const unit = s.real_days === 1 ? "day" : "days";
    return `Since ${shortDate(s.first_data_date)} (${s.real_days} ${unit})`;
  }
  return "No recorded data yet";
}

function returnsSubtitle(range: TimeRange, s: PortfolioSeries | null): string {
  if (!s) return "Loading…";
  if (range === "1D") {
    return s.daily_returns.length > 0
      ? "1 session (today)"
      : "No session yet today";
  }
  const expected = range === "1W" ? 7 : range === "1M" ? 30 : null;
  const n = s.daily_returns.length;
  if (expected !== null && n >= expected) {
    return `Last ${expected} sessions`;
  }
  if (n > 0) {
    const unit = n === 1 ? "session" : "sessions";
    return `${n} ${unit} so far`;
  }
  return "No sessions recorded yet";
}

function emptyCopyForRange(range: TimeRange, kind: "equity" | "returns"): string {
  const unit = kind === "equity" ? "data" : "sessions";
  switch (range) {
    case "1D":  return kind === "equity"
      ? "No snapshots captured today yet."
      : "Daily returns are a per-session metric — first bar appears after today's 23:55 UTC session close.";
    case "1W":  return `No ${unit} in the last 7 days yet.`;
    case "1M":  return `No ${unit} in the last 30 days yet.`;
    case "ALL": return `No ${unit} recorded yet.`;
  }
}

function statusBadge(status: string) {
  const map: Record<string, { bg: string; color: string; label: string }> = {
    complete: { bg: "var(--green-dim)", color: "var(--green)", label: "COMPLETE" },
    running: { bg: "var(--amber-dim)", color: "var(--amber)", label: "RUNNING" },
    failed: { bg: "var(--red-dim)", color: "var(--red)", label: "FAILED" },
    stale: { bg: "var(--amber-dim)", color: "var(--amber)", label: "STALE" },
    unknown: { bg: "var(--bg3)", color: "var(--t3)", label: "UNKNOWN" },
  };
  const s = map[status] || map.unknown;
  return (
    <span
      style={{
        display: "inline-block",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.06em",
        padding: "2px 6px",
        borderRadius: 3,
        background: s.bg,
        color: s.color,
      }}
    >
      {s.label}
    </span>
  );
}

function relTime(iso: string | null): string {
  if (!iso) return "never";
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function staleCheck(job: PipelineJob): string {
  if (job.status === "unknown") return "unknown";
  if (!job.last_run) return "unknown";
  const diff = Date.now() - new Date(job.last_run).getTime();
  if (job.status === "running") return "running";
  if (job.status === "complete" && diff > 48 * 3600000) return "stale";
  return job.status;
}

// ─── KPI Card ───────────────────────────────────────────────────────────────

function KpiCard({
  label,
  value,
  color,
  subvalue,
  subvalueColor,
}: {
  label: string;
  value: string;
  color?: string;
  subvalue?: string;
  /** Override color for the subvalue line only. Defaults to the main `color`. */
  subvalueColor?: string;
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
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        {value}
      </div>
      {subvalue && (
        <div
          style={{
            fontSize: 11,
            color: subvalueColor || color || "var(--t2)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            marginTop: 4,
            opacity: 0.8,
          }}
        >
          {subvalue}
        </div>
      )}
    </div>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function OverviewPage() {
  const [data, setData] = useState<OverviewData | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Each chart owns its own range — they're independent controls.
  const [equityRange, setEquityRange] = useState<TimeRange>("1M");
  const [returnsRange, setReturnsRange] = useState<TimeRange>("1M");
  const [equitySeries, setEquitySeries] = useState<PortfolioSeries | null>(null);
  const [returnsSeries, setReturnsSeries] = useState<PortfolioSeries | null>(null);

  const loadOverview = useCallback(() => {
    fetch(`${API_BASE}/api/allocator/snapshots/refresh`, {
      method: "POST",
      credentials: "include",
    })
      .catch(() => {})
      .finally(() => {
        fetch(`${API_BASE}/api/manager/overview`, { credentials: "include" })
          .then((r) => {
            if (r.status === 401) throw new Error("Session expired");
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
          })
          .then(setData)
          .catch((e) => setError(e.message));
      });
  }, []);

  // Fetch series for each chart independently when its range changes.
  // Daily Returns on 1D falls back to the server's empty response —
  // render path shows a "not meaningful at intraday cadence" hint.
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/manager/portfolio-series?range=${equityRange}`, {
      credentials: "include",
    })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((d: PortfolioSeries) => { if (!cancelled) setEquitySeries(d); })
      .catch(() => { if (!cancelled) setEquitySeries(null); });
    return () => { cancelled = true; };
  }, [equityRange]);

  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/manager/portfolio-series?range=${returnsRange}`, {
      credentials: "include",
    })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((d: PortfolioSeries) => { if (!cancelled) setReturnsSeries(d); })
      .catch(() => { if (!cancelled) setReturnsSeries(null); });
    return () => { cancelled = true; };
  }, [returnsRange]);

  useEffect(() => {
    loadOverview();
    // Auto-refresh every 5 minutes for live intraday data
    const interval = setInterval(loadOverview, 5 * 60 * 1000);
    return () => clearInterval(interval);
  }, [loadOverview]);

  if (error) {
    return (
      <div
        style={{
          padding: 28,
          fontSize: 10,
          color: "var(--red)",
        }}
      >
        Error: {error}
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{
        padding: 20,
        display: "flex",
        flexDirection: "column",
        gap: 10,
        height: "100%",
        overflow: "hidden",
      }}>
        <KPIGridSkeleton count={5} />
        {/* Two charts side-by-side */}
        <div style={{ display: "flex", gap: 10, flex: "0 0 200px" }}>
          {Array.from({ length: 2 }).map((_, i) => (
            <div key={i} style={{
              flex: 1,
              background: "var(--bg2)",
              border: "1px solid var(--line)",
              borderRadius: 5,
              padding: "12px 16px",
              display: "flex",
              flexDirection: "column",
              gap: 10,
            }}>
              <Skeleton width={140} height={9} />
              <Skeleton width="100%" height={150} borderRadius={4} />
            </div>
          ))}
        </div>
        {/* Two cards side-by-side: allocation table + pipeline status */}
        <div style={{ display: "flex", gap: 10, flex: 1, minHeight: 0 }}>
          <div style={{ flex: 1 }}>
            <TableSkeleton rows={5} columns={[80, 100, 70, 80, 60, 50, 60]} />
          </div>
          <div style={{ flex: 1 }}>
            <TableSkeleton rows={4} columns={[100, 80, 80]} />
          </div>
        </div>
      </div>
    );
  }

  // Chart series are fetched server-side per selected range. Format
  // x-axis labels based on granularity:
  //   intraday (1D)  → HH:MM
  //   daily (others) → MM-DD
  const fmtEquityLabel = (iso: string) =>
    equitySeries?.granularity === "intraday"
      ? new Date(iso).toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false })
      : iso.slice(5); // MM-DD

  const eqDates = equitySeries?.portfolio_equity.map((d) => fmtEquityLabel(d.date)) ?? [];
  const eqValues = equitySeries?.portfolio_equity.map((d) => d.equity_usd) ?? [];

  const dailyReturnDates = returnsSeries?.daily_returns.map((d) => d.date.slice(5)) ?? [];
  const dailyReturnPcts = returnsSeries?.daily_returns.map((d) => d.return_pct) ?? [];

  const hasEquityData = eqValues.length > 0;
  const hasDailyData = dailyReturnDates.length > 0;

  return (
    <div
      style={{
        padding: 20,
        display: "flex",
        flexDirection: "column",
        gap: 10,
        height: "100%",
        overflow: "hidden",
      }}
    >
      {/* Row 1: KPI Cards */}
      <div style={{ display: "flex", gap: 10 }}>
        <KpiCard
          label="Total AUM"
          value={`$${(data.total_live_equity_usd ?? data.total_aum).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
          subvalue={`${fmtUsdSigned(data.total_pnl_usd)} (${fmtPct(data.total_pnl_pct)})`}
          subvalueColor={
            data.total_pnl_usd > 0 ? "var(--green)"
            : data.total_pnl_usd < 0 ? "var(--red)"
            : "var(--t1)"
          }
        />
        <KpiCard
          label="Today"
          value={fmtPct(data.today_pct)}
          subvalue={fmtUsdSigned(data.today_usd)}
          color={pctColor(data.today_pct)}
        />
        <KpiCard
          label="WTD"
          value={fmtPct(data.wtd_pct)}
          subvalue={fmtUsdSigned(data.wtd_usd)}
          color={pctColor(data.wtd_pct)}
        />
        <KpiCard
          label="MTD"
          value={fmtPct(data.mtd_pct)}
          subvalue={fmtUsdSigned(data.mtd_usd)}
          color={pctColor(data.mtd_pct)}
        />
        <KpiCard
          label="Max Drawdown"
          value={data.max_drawdown === 0 ? "0.0%" : `${data.max_drawdown.toFixed(1)}%`}
          color={data.max_drawdown < 0 ? "var(--red)" : "var(--t1)"}
        />
      </div>

      {/* Row 2: Charts */}
      <div style={{ display: "flex", gap: 10, flex: "0 0 300px" }}>
        {/* Equity Curve */}
        <div
          style={{
            flex: 1,
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            display: "flex",
            flexDirection: "column",
          }}
        >
          {/* Header: title + dynamic subtitle + range tabs */}
          <div
            style={{
              display: "flex",
              alignItems: "flex-start",
              justifyContent: "space-between",
              gap: 10,
              marginBottom: 8,
            }}
          >
            <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              <span style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                color: "var(--t3)",
                textTransform: "uppercase",
              }}>Portfolio Equity</span>
              <span style={{ fontSize: 9, color: "var(--t3)", fontWeight: 400 }}>
                {equitySubtitle(equityRange, equitySeries)}
              </span>
            </div>
            <RangeTabs value={equityRange} onChange={setEquityRange} />
          </div>
          {hasEquityData ? (
            <div style={{ flex: 1, position: "relative" }}>
              <Line
                data={{
                  labels: eqDates,
                  datasets: [
                    {
                      data: eqValues,
                      borderColor: "#00c896",
                      backgroundColor: "rgba(0, 200, 150, 0.08)",
                      fill: true,
                      tension: 0.3,
                      // Show a visible marker when there's only one point
                      // so the user can tell it's intentional, not a glitch.
                      pointRadius: eqValues.length === 1 ? 4 : 0,
                      pointBackgroundColor: "#00c896",
                      borderWidth: 1.5,
                    },
                  ],
                }}
                options={{
                  responsive: true,
                  maintainAspectRatio: false,
                  interaction: { mode: "index" as const, intersect: false },
                  scales: {
                    x: {
                      display: true,
                      ticks: { color: "#5a5754", font: { size: 8 }, maxTicksLimit: 6 },
                      grid: { display: false },
                    },
                    y: {
                      display: true,
                      ticks: { color: "#5a5754", font: { size: 8, family: "Space Mono" }, maxTicksLimit: 4,
                        callback: (v: unknown) => `$${Number(v).toLocaleString("en-US")}`,
                      },
                      grid: { color: "rgba(50,50,59,0.3)" },
                    },
                  },
                  plugins: {
                    legend: { display: false },
                    tooltip: {
                      enabled: true,
                      backgroundColor: "rgba(20,20,22,0.95)",
                      titleFont: { family: "Space Mono", size: 10 },
                      bodyFont: { family: "Space Mono", size: 11 },
                      padding: 10,
                      cornerRadius: 4,
                      callbacks: {
                        label: (ctx: { raw: unknown }) => `$${Number(ctx.raw).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
                      },
                    },
                  },
                }}
              />
            </div>
          ) : (
            <div
              style={{
                flex: 1,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: 9,
                color: "var(--t3)",
                textAlign: "center",
                padding: "0 20px",
              }}
            >
              {emptyCopyForRange(equityRange, "equity")}
            </div>
          )}
        </div>

        {/* Daily Returns */}
        <div
          style={{
            flex: 1,
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            display: "flex",
            flexDirection: "column",
          }}
        >
          {/* Header: title + dynamic subtitle + range tabs.
              1D is present but disabled — daily returns are a per-session
              metric, one bar per UTC day, so an intraday view is not
              meaningful. Keep the tab visible for affordance consistency
              with the Equity chart but block clicks. */}
          <div
            style={{
              display: "flex",
              alignItems: "flex-start",
              justifyContent: "space-between",
              gap: 10,
              marginBottom: 8,
            }}
          >
            <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              <span style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                color: "var(--t3)",
                textTransform: "uppercase",
              }}>Daily Returns</span>
              <span style={{ fontSize: 9, color: "var(--t3)", fontWeight: 400 }}>
                {returnsSubtitle(returnsRange, returnsSeries)}
              </span>
            </div>
            <RangeTabs
              value={returnsRange}
              onChange={setReturnsRange}
              disabled={["1D"]}
            />
          </div>
          {hasDailyData ? (
            <div style={{ flex: 1, position: "relative" }}>
              <Bar
                data={{
                  labels: dailyReturnDates,
                  datasets: [
                    {
                      data: dailyReturnPcts,
                      backgroundColor: dailyReturnPcts.map((v) =>
                        v >= 0 ? "rgba(0, 200, 150, 0.6)" : "rgba(255, 77, 77, 0.6)"
                      ),
                      borderRadius: 2,
                    },
                  ],
                }}
                options={{
                  responsive: true,
                  maintainAspectRatio: false,
                  scales: {
                    x: {
                      display: true,
                      ticks: { color: "#5a5754", font: { size: 8 }, maxTicksLimit: 6 },
                      grid: { display: false },
                    },
                    y: {
                      display: true,
                      ticks: {
                        color: "#5a5754", font: { size: 8 }, maxTicksLimit: 4,
                        callback: (v: unknown) => `${Number(v).toFixed(2)}%`,
                      },
                      grid: { color: "rgba(50,50,59,0.3)" },
                    },
                  },
                  plugins: {
                    tooltip: {
                      enabled: true,
                      backgroundColor: "rgba(20,20,22,0.95)",
                      titleFont: { family: "Space Mono", size: 10 },
                      bodyFont: { family: "Space Mono", size: 11 },
                      padding: 10,
                      cornerRadius: 4,
                      callbacks: {
                        label: (ctx: { raw: unknown }) => {
                          const v = Number(ctx.raw);
                          const sign = v >= 0 ? "+" : "";
                          return `${sign}${v.toFixed(2)}%`;
                        },
                      },
                    },
                    legend: { display: false },
                  },
                }}
              />
            </div>
          ) : (
            <div
              style={{
                flex: 1,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: 9,
                color: "var(--t3)",
                textAlign: "center",
                padding: "0 20px",
              }}
            >
              {emptyCopyForRange(returnsRange, "returns")}
            </div>
          )}
        </div>
      </div>

      {/* Intraday Equity (today) */}
      {(() => {
        // Build full deployment window grid: 06:00 → 00:00 UTC at 15-min intervals
        const grid: string[] = [];
        for (let h = 6; h < 24; h++) {
          for (let m = 0; m < 60; m += 15) {
            grid.push(`${h.toString().padStart(2, "0")}:${m.toString().padStart(2, "0")}`);
          }
        }

        // Map actual data onto the grid
        const dataMap: Record<string, number> = {};
        for (const p of data.intraday_equity ?? []) {
          const d = new Date(p.time);
          const label = `${d.getUTCHours().toString().padStart(2, "0")}:${d.getUTCMinutes().toString().padStart(2, "0")}`;
          dataMap[label] = p.equity_usd;
        }
        const values = grid.map((label) => dataMap[label] ?? null);
        const hasAny = values.some((v) => v !== null);

        // Linear regression projection for future empty points
        const realPoints: { x: number; y: number }[] = [];
        values.forEach((v, i) => { if (v !== null) realPoints.push({ x: i, y: v }); });
        let projected: (number | null)[] = values.map(() => null);
        if (realPoints.length >= 2) {
          const lastReal = realPoints[realPoints.length - 1];
          // 3 hours = 12 fifteen-minute buckets
          const hasEnoughHistory = realPoints.length >= 12;

          let slope = 0;
          if (hasEnoughHistory) {
            const n = realPoints.length;
            const sumX = realPoints.reduce((s, p) => s + p.x, 0);
            const sumY = realPoints.reduce((s, p) => s + p.y, 0);
            const sumXY = realPoints.reduce((s, p) => s + p.x * p.y, 0);
            const sumX2 = realPoints.reduce((s, p) => s + p.x * p.x, 0);
            slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
          }
          // slope = 0 → flat line; slope > 0 → linear projection
          projected = grid.map((_, i) => {
            if (i === lastReal.x) return lastReal.y;
            if (i > lastReal.x) return lastReal.y + slope * (i - lastReal.x);
            return null;
          });
        }

        return (
          <div style={{
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            height: 330,
          }}>
            <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginBottom: 8 }}>
              Intraday Equity (today) · 06:00–00:00 UTC
            </div>
            <div style={{ height: "calc(100% - 24px)" }}>
              {hasAny ? (
                <Line
                  data={{
                    labels: grid,
                    datasets: [
                      {
                        label: "Equity",
                        data: values,
                        borderColor: "#00c896",
                        backgroundColor: "rgba(0, 200, 150, 0.15)",
                        fill: true,
                        tension: 0.3,
                        pointRadius: values.map((v) => v !== null ? 2 : 0),
                        pointBackgroundColor: "#00c896",
                        borderWidth: 2,
                        spanGaps: false,
                      },
                      {
                        label: "Projected",
                        data: projected,
                        borderColor: "rgba(160, 157, 150, 0.4)",
                        backgroundColor: "rgba(160, 157, 150, 0.06)",
                        fill: true,
                        tension: 0.3,
                        pointRadius: 0,
                        borderWidth: 1.5,
                        borderDash: [6, 4],
                        spanGaps: true,
                      },
                    ],
                  }}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: "index" as const, intersect: false },
                    plugins: {
                      legend: { display: false },
                      tooltip: {
                        enabled: true,
                        backgroundColor: "rgba(20,20,22,0.95)",
                        titleFont: { family: "Space Mono", size: 10 },
                        bodyFont: { family: "Space Mono", size: 11 },
                        padding: 10,
                        cornerRadius: 4,
                        filter: (item: { raw: unknown; datasetIndex: number }) => item.raw !== null && item.datasetIndex === 0,
                        callbacks: {
                          label: (ctx: { raw: unknown; datasetIndex: number }) => {
                            if (ctx.datasetIndex === 1) return "";
                            return ctx.raw !== null
                              ? `$${Number(ctx.raw).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                              : "";
                          },
                        },
                      },
                    },
                    scales: {
                      x: { grid: { display: false }, ticks: { color: "#5a5754", font: { size: 8, family: "Space Mono" }, maxTicksLimit: 12 } },
                      y: { grid: { color: "rgba(50,50,59,0.3)" }, ticks: { color: "#5a5754", font: { size: 8, family: "Space Mono" }, maxTicksLimit: 4,
                        callback: (v: unknown) => `$${Number(v).toLocaleString("en-US")}`,
                      }},
                    },
                  }}
                />
              ) : (
                <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100%", fontSize: 10, color: "var(--t2)" }}>
                  No intraday data yet
                </div>
              )}
            </div>
          </div>
        );
      })()}

      {/* Row 3: Allocation table + Pipeline status */}
      <div style={{ display: "flex", gap: 10, flex: 1, minHeight: 0 }}>
        {/* Allocation breakdown */}
        <div
          style={{
            flex: 1,
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            overflow: "auto",
          }}
        >
          <div
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            Allocations
          </div>
          {data.allocations.length === 0 ? (
            <div style={{ fontSize: 10, color: "var(--t3)" }}>
              No active allocations
            </div>
          ) : (
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
              }}
            >
              <thead>
                <tr>
                  {["Exchange", "Strategy", "Capital", "Live Equity", "Today", "DD", "Status"].map(
                    (h) => (
                      <th
                        key={h}
                        style={{
                          fontSize: 9,
                          fontWeight: 700,
                          letterSpacing: "0.1em",
                          color: "var(--t3)",
                          textTransform: "uppercase",
                          textAlign: "left",
                          padding: "4px 8px 6px 0",
                          borderBottom: "1px solid var(--line)",
                        }}
                      >
                        {h}
                      </th>
                    )
                  )}
                </tr>
              </thead>
              <tbody>
                {data.allocations.map((a) => {
                  // Find matching snapshot for live equity
                  const snap = data.exchange_snapshots.find(
                    (s) => s.connection_id === a.connection_id
                  );
                  const liveEq = snap?.total_equity_usd;
                  return (
                    <tr key={a.allocation_id}>
                      <td style={tdStyle}>{a.exchange}</td>
                      <td style={tdStyle}>
                        {a.strategy_display_name}
                      </td>
                      <td style={tdStyle}>
                        ${a.capital_usd.toLocaleString()}
                      </td>
                      <td style={tdStyle}>
                        {liveEq !== null && liveEq !== undefined
                          ? `$${liveEq.toLocaleString()}`
                          : "--"}
                      </td>
                      <td
                        style={{
                          ...tdStyle,
                          color: pctColor(a.daily_return_today),
                        }}
                      >
                        {fmtPct(a.daily_return_today)}
                      </td>
                      <td
                        style={{
                          ...tdStyle,
                          color:
                            a.max_drawdown < 0 ? "var(--red)" : "var(--t1)",
                        }}
                      >
                        {a.max_drawdown.toFixed(1)}%
                      </td>
                      <td style={tdStyle}>
                        <span
                          style={{
                            fontSize: 9,
                            fontWeight: 700,
                            padding: "2px 6px",
                            borderRadius: 3,
                            background: "var(--green-dim)",
                            color: "var(--green)",
                          }}
                        >
                          ACTIVE
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        {/* Pipeline status */}
        <div
          style={{
            flex: 1,
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            overflow: "auto",
          }}
        >
          <div
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            Pipeline Status
          </div>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            <thead>
              <tr>
                {["Component", "Status", "Last Run"].map((h) => (
                  <th
                    key={h}
                    style={{
                      fontSize: 9,
                      fontWeight: 700,
                      letterSpacing: "0.1em",
                      color: "var(--t3)",
                      textTransform: "uppercase",
                      textAlign: "left",
                      padding: "4px 8px 6px 0",
                      borderBottom: "1px solid var(--line)",
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style={tdStyle}>Compiler</td>
                <td style={tdStyle}>
                  {statusBadge(staleCheck(data.pipeline.compiler))}
                </td>
                <td style={tdStyle}>
                  {relTime(data.pipeline.compiler.last_run)}
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>Indexer</td>
                <td style={tdStyle}>
                  {statusBadge(staleCheck(data.pipeline.indexer))}
                </td>
                <td style={tdStyle}>
                  {relTime(data.pipeline.indexer.last_run)}
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>Daily Signal</td>
                <td style={tdStyle}>
                  {statusBadge(
                    data.pipeline.signals_last_generated
                      ? "complete"
                      : "unknown"
                  )}
                </td>
                <td style={tdStyle}>
                  {relTime(data.pipeline.signals_last_generated)}
                </td>
              </tr>
              <tr>
                <td style={tdStyle}>Trader</td>
                <td style={tdStyle}>
                  {statusBadge(staleCheck(data.pipeline.trader))}
                </td>
                <td style={tdStyle}>
                  {relTime(data.pipeline.trader.last_run)}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

const tdStyle: React.CSSProperties = {
  fontSize: 10,
  color: "var(--t1)",
  padding: "6px 8px 6px 0",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
};

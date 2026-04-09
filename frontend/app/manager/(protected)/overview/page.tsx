"use client";

import { useEffect, useState, useRef } from "react";
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
  wtd_pct: number;
  mtd_pct: number;
  max_drawdown: number;
  portfolio_equity_30d: { date: string; equity_usd: number }[];
  pipeline: {
    compiler: PipelineJob;
    indexer: PipelineJob;
    signals_last_generated: string | null;
  };
  exchange_snapshots: Snapshot[];
  total_live_equity_usd: number | null;
  total_unrealized_pnl: number | null;
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
}: {
  label: string;
  value: string;
  color?: string;
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
    </div>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function OverviewPage() {
  const [data, setData] = useState<OverviewData | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    // Refresh live exchange data first, then load overview
    fetch(`${API_BASE}/api/allocator/snapshots/refresh`, {
      method: "POST",
      credentials: "include",
    })
      .catch(() => {}) // non-critical — overview still works with stale snapshots
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
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "100%",
          fontSize: 9,
          color: "var(--t3)",
          textTransform: "uppercase",
          letterSpacing: "0.12em",
        }}
      >
        Loading overview...
      </div>
    );
  }

  // Build daily return array from allocations' perf data
  const dailyReturnsByDate: Record<string, number> = {};
  for (const a of data.allocations) {
    for (const p of a.performance_30d) {
      const ret = (a.capital_usd || 0) * ((p.daily_return || 0) / 100);
      dailyReturnsByDate[p.date] = (dailyReturnsByDate[p.date] || 0) + ret;
    }
  }
  const dailyReturnDates = Object.keys(dailyReturnsByDate).sort();
  const dailyReturnPcts = dailyReturnDates.map((d) =>
    data.total_aum > 0
      ? (dailyReturnsByDate[d] / data.total_aum) * 100
      : 0
  );

  // Equity curve
  const eqDates = data.portfolio_equity_30d.map((d) => d.date.slice(5)); // MM-DD
  const eqValues = data.portfolio_equity_30d.map((d) => d.equity_usd);

  const hasEquityData = eqValues.length > 0;
  const hasDailyData = dailyReturnDates.length > 0;

  return (
    <div
      style={{
        padding: 20,
        display: "flex",
        flexDirection: "column",
        gap: 14,
        height: "100%",
        overflow: "hidden",
      }}
    >
      {/* Row 1: KPI Cards */}
      <div style={{ display: "flex", gap: 10 }}>
        <KpiCard
          label="Today"
          value={fmtPct(data.today_pct)}
          color={pctColor(data.today_pct)}
        />
        <KpiCard
          label="WTD"
          value={fmtPct(data.wtd_pct)}
          color={pctColor(data.wtd_pct)}
        />
        <KpiCard
          label="MTD"
          value={fmtPct(data.mtd_pct)}
          color={pctColor(data.mtd_pct)}
        />
        <KpiCard
          label="Max Drawdown"
          value={data.max_drawdown === 0 ? "0.0%" : `${data.max_drawdown.toFixed(1)}%`}
          color={data.max_drawdown < 0 ? "var(--red)" : "var(--t1)"}
        />
        <KpiCard
          label="Total AUM"
          value={`$${(data.total_live_equity_usd ?? data.total_aum).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
        />
      </div>

      {/* Row 2: Charts */}
      <div style={{ display: "flex", gap: 10, flex: "0 0 200px" }}>
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
          <div
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
              marginBottom: 8,
            }}
          >
            Portfolio Equity (30d)
          </div>
          {hasEquityData ? (
            <div style={{ flex: 1, position: "relative" }}>
              <Line
                data={{
                  labels: eqDates,
                  datasets: [
                    {
                      data: eqValues,
                      borderColor: "var(--green)",
                      backgroundColor: "rgba(0, 200, 150, 0.08)",
                      fill: true,
                      tension: 0.3,
                      pointRadius: 0,
                      borderWidth: 1.5,
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
                      ticks: { color: "#5a5754", font: { size: 8 }, maxTicksLimit: 4 },
                      grid: { color: "rgba(50,50,59,0.3)" },
                    },
                  },
                  plugins: { tooltip: { enabled: true }, legend: { display: false } },
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
              }}
            >
              No data yet
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
          <div
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
              marginBottom: 8,
            }}
          >
            Daily Returns (30d)
          </div>
          {hasDailyData ? (
            <div style={{ flex: 1, position: "relative" }}>
              <Bar
                data={{
                  labels: dailyReturnDates.map((d) => d.slice(5)),
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
                      ticks: { color: "#5a5754", font: { size: 8 }, maxTicksLimit: 4 },
                      grid: { color: "rgba(50,50,59,0.3)" },
                    },
                  },
                  plugins: { tooltip: { enabled: true }, legend: { display: false } },
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
              }}
            >
              No data yet
            </div>
          )}
        </div>
      </div>

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
                  {statusBadge("unknown")}
                </td>
                <td style={tdStyle}>--</td>
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

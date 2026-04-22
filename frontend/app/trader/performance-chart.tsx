"use client";

import { useState, useRef, useEffect } from "react";
import {
  Chart as ChartJS,
  CategoryScale, LinearScale, PointElement, LineElement, BarElement,
  Filler, Tooltip,
} from "chart.js";
import { Line, Bar } from "react-chartjs-2";
import { allocatorApi, ApiBalanceHistory, parseApiError } from "./api";
import { RangeTabs, TimeRange } from "../components/RangeTabs";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Filler, Tooltip);

interface PerformanceChartProps {
  // Omit instanceId to render the aggregate account-balance series (sum
  // across all of the user's exchange connections). Pass instanceId to
  // render the per-allocation curve.
  instanceId?: string;
  title?: string;
  containerStyle?: React.CSSProperties;
  equityHeight?: number;
}

interface ChartData {
  equity: number[];
  pnl: number[];
  labels: string[];
}

// ─── Transform API response → chart data shape ──────────────────────────────

// Label format per range — matches the bucket granularity chosen by the
// backend (see RANGE_SPECS in allocator.py):
//   1D  → 5-min buckets        → "HH:MM"
//   1W  → 30-min buckets       → "Mon DD HH:MM" (cross-day spans need both)
//   1M  → 3-hour buckets       → "Mon DD HH:MM"
//   ALL → 1-day buckets        → "Mon DD"
function formatLabel(d: Date, range: TimeRange): string {
  const time = d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", hour12: false });
  const date = d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  switch (range) {
    case "1D":  return time;
    case "1W":  return `${date} ${time}`;
    case "1M":  return `${date} ${time}`;
    case "ALL": return date;
  }
}

function transformHistory(history: ApiBalanceHistory[], range: TimeRange): ChartData {
  const equity: number[] = [];
  const pnl: number[] = [];
  const labels: string[] = [];

  for (let i = 0; i < history.length; i++) {
    const row = history[i];
    equity.push(row.equity_usd);
    // Per-row P&L = equity change between consecutive buckets. First row
    // has no prior reference → 0 baseline.
    pnl.push(i === 0 ? 0 : row.equity_usd - history[i - 1].equity_usd);
    labels.push(formatLabel(new Date(row.date), range));
  }

  return { equity, pnl, labels };
}

// ─── Shared tooltip state ────────────────────────────────────────────────────

function useSharedHover() {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);
  return { hoverIdx, setHoverIdx, tooltipPos, setTooltipPos };
}

// ─── CSS variable resolver ───────────────────────────────────────────────────

function getCssVar(name: string): string {
  if (typeof document === "undefined") return "";
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// ─── Empty / loading / error states ─────────────────────────────────────────

function StateMessage({ main, sub }: { main: string; sub?: string }) {
  return (
    <div style={{
      height: 260,
      display: "flex", flexDirection: "column",
      alignItems: "center", justifyContent: "center",
      gap: 4,
    }}>
      <div style={{ fontSize: 10, color: "var(--t2)", fontWeight: 400 }}>{main}</div>
      {sub && <div style={{ fontSize: 9, color: "var(--t3)" }}>{sub}</div>}
    </div>
  );
}

// ─── Component ───────────────────────────────────────────────────────────────

export default function PerformanceChart({ instanceId, title = "PERFORMANCE", containerStyle, equityHeight = 180 }: PerformanceChartProps) {
  const [range, setRange] = useState<TimeRange>("1M");
  const [data, setData] = useState<ChartData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { hoverIdx, setHoverIdx, tooltipPos, setTooltipPos } = useSharedHover();
  const containerRef = useRef<HTMLDivElement>(null);

  // Fetch history whenever the instance or range changes.
  useEffect(() => {
    let cancelled = false;
    async function fetchHistory() {
      setLoading(true);
      setError(null);
      try {
        const result = instanceId
          ? await allocatorApi.getBalanceHistory(instanceId, range)
          : await allocatorApi.getAccountBalanceSeries(range);
        if (cancelled) return;
        setData(transformHistory(result.history, range));
      } catch (err) {
        if (cancelled) return;
        const { detail } = parseApiError(err);
        setError(detail || "Unable to load history");
        setData(null);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    fetchHistory();
    return () => { cancelled = true; };
  }, [instanceId, range]);

  // Resolve CSS vars for Chart.js (needs actual hex values)
  const green = getCssVar("--green") || "#00c896";
  const red = getCssVar("--red") || "#ff4d4d";
  const t2 = getCssVar("--t2") || "#5a5754";
  const t3 = getCssVar("--t3") || "#bbbbbb";

  // Header (always rendered — tabs remain visible across loading/empty/error).
  const header = (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
      <span style={{ fontSize: 9, color: "var(--t3)", fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase" }}>
        {title}
      </span>
      <RangeTabs value={range} onChange={setRange} />
    </div>
  );

  const wrapperStyle: React.CSSProperties = {
    background: "var(--bg1)", border: "1px solid var(--line)",
    borderRadius: 6, padding: "12px 14px", marginBottom: 20,
    position: "relative",
    ...containerStyle,
  };

  // Loading: show header + skeleton.
  if (loading && data === null) {
    return (
      <div style={wrapperStyle}>
        {header}
        <StateMessage main="Loading…" />
      </div>
    );
  }

  // Error: honest error text, no fallback to mock.
  if (error) {
    return (
      <div style={wrapperStyle}>
        {header}
        <StateMessage main="Unable to load history" sub={error} />
      </div>
    );
  }

  // Empty: no snapshot rows in range yet. sync_exchange_snapshots runs
  // every 5 min so this should resolve within a cron tick of a new
  // connection going live.
  if (!data || data.equity.length === 0) {
    return (
      <div style={wrapperStyle}>
        {header}
        <StateMessage
          main="No balance snapshots yet"
          sub={range === "1D"
            ? "Snapshots appear every 5 min once the exchange connection is live."
            : "Snapshots captured every 5 min; daily rollups start after the first UTC day."}
        />
      </div>
    );
  }

  // Single-point: still too sparse for a line. Render the single value as
  // a plain stat until the next snapshot/close lands.
  if (data.equity.length === 1) {
    return (
      <div style={wrapperStyle}>
        {header}
        <StateMessage
          main={`$${data.equity[0].toLocaleString()} at ${data.labels[0]}`}
          sub="Only one snapshot so far — line appears after the next 5-min tick."
        />
      </div>
    );
  }

  // ── Full chart rendering (data.equity.length >= 2) ──────────────────────

  // How many x-axis labels to show
  const labelCounts: Record<TimeRange, number> = { "1D": 6, "1W": 7, "1M": 6, "ALL": 6 };
  const maxLabels = labelCounts[range];
  const step = Math.max(1, Math.floor(data.labels.length / maxLabels));
  const xLabelIndices = data.labels.map((_, i) => i % step === 0 || i === data.labels.length - 1);

  // Shared hover handler
  const hoverIdxRef = { current: null as number | null };
  const handleHover = (chart: ChartJS, event: MouseEvent) => {
    const points = chart.getElementsAtEventForMode(event, "index", { intersect: false }, false);
    if (points.length > 0 && containerRef.current) {
      const idx = points[0].index;
      if (hoverIdxRef.current === idx) return;
      hoverIdxRef.current = idx;
      const rect = containerRef.current.getBoundingClientRect();
      setHoverIdx(idx);
      setTooltipPos({ x: event.clientX - rect.left, y: event.clientY - rect.top });
    } else {
      if (hoverIdxRef.current === null) return;
      hoverIdxRef.current = null;
      setHoverIdx(null);
      setTooltipPos(null);
    }
  };

  const handleLeave = () => {
    hoverIdxRef.current = null;
    setHoverIdx(null);
    setTooltipPos(null);
  };

  // Equity chart config
  const equityData = {
    labels: data.labels,
    datasets: [{
      data: data.equity,
      borderColor: green,
      borderWidth: 1.5,
      backgroundColor: (ctx: { chart: ChartJS }) => {
        const chart = ctx.chart;
        const { ctx: c, chartArea } = chart;
        if (!chartArea) return green;
        const gradient = c.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
        gradient.addColorStop(0, green + "33");
        gradient.addColorStop(1, green + "03");
        return gradient;
      },
      fill: true,
      pointRadius: 0,
      pointHoverRadius: 0,
      tension: 0.3,
    }],
  };

  const equityOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 } as const,
    layout: { padding: { left: 0, right: 0, top: 0, bottom: 0 } },
    scales: {
      x: { display: false },
      y: {
        position: "right" as const,
        grid: { display: false },
        border: { display: false },
        afterFit: (axis: { width: number }) => { axis.width = 60; },
        ticks: {
          maxTicksLimit: 2,
          font: { family: "'Space Mono', monospace", size: 9 },
          color: t3,
          callback: (v: number | string) => "$" + Number(v).toLocaleString(),
        },
      },
    },
    plugins: {
      tooltip: { enabled: false },
      legend: { display: false },
    },
    onHover: (_event: unknown, _elements: unknown, chart: ChartJS) => {
      const e = (_event as { native?: MouseEvent })?.native;
      if (e) handleHover(chart, e);
    },
  };

  // P&L bars config
  const pnlData = {
    labels: data.labels,
    datasets: [{
      data: data.pnl,
      backgroundColor: data.pnl.map(v => v >= 0 ? green + "B3" : red + "B3"),
      borderRadius: 1,
      borderSkipped: false as const,
      barPercentage: 0.5,
      categoryPercentage: 0.7,
    }],
  };

  const pnlOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 } as const,
    layout: { padding: { left: 0, right: 0, top: 0, bottom: 0 } },
    scales: {
      x: { display: false },
      y: {
        position: "right" as const,
        display: true,
        grid: { display: false },
        border: { display: false },
        afterFit: (axis: { width: number }) => { axis.width = 60; },
        ticks: { display: false },
        min: Math.min(0, ...data.pnl) * 1.2,
        max: Math.max(0, ...data.pnl) * 1.2,
      },
    },
    plugins: {
      tooltip: { enabled: false },
      legend: { display: false },
    },
    onHover: (_event: unknown, _elements: unknown, chart: ChartJS) => {
      const e = (_event as { native?: MouseEvent })?.native;
      if (e) handleHover(chart, e);
    },
  };

  // Zero line plugin for P&L chart
  const zeroLinePlugin = {
    id: "zeroLine",
    afterDraw: (chart: ChartJS) => {
      const { ctx, chartArea, scales } = chart;
      const yScale = scales.y;
      if (!yScale || !chartArea) return;
      const zeroY = yScale.getPixelForValue(0);
      if (zeroY < chartArea.top || zeroY > chartArea.bottom) return;
      ctx.save();
      ctx.setLineDash([3, 3]);
      ctx.strokeStyle = t2 + "4D";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(chartArea.left, zeroY);
      ctx.lineTo(chartArea.right, zeroY);
      ctx.stroke();
      ctx.restore();
    },
  };

  // Crosshair plugin for both charts
  const crosshairPlugin = {
    id: "crosshair",
    afterDraw: (chart: ChartJS) => {
      if (hoverIdx === null) return;
      const { ctx, chartArea, scales } = chart;
      const xScale = scales.x;
      if (!xScale || !chartArea) return;
      const x = xScale.getPixelForValue(hoverIdx);
      ctx.save();
      ctx.setLineDash([2, 2]);
      ctx.strokeStyle = t2 + "66";
      ctx.lineWidth = 0.5;
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.stroke();
      ctx.restore();
    },
  };

  return (
    <div
      ref={containerRef}
      onMouseLeave={handleLeave}
      style={wrapperStyle}
    >
      {header}

      {/* Equity curve */}
      <div style={{ height: equityHeight }}>
        <Line data={equityData} options={equityOptions as never} plugins={[crosshairPlugin]} />
      </div>

      {/* P&L bars */}
      <div style={{ height: 80 }}>
        <Bar data={pnlData} options={pnlOptions as never} plugins={[zeroLinePlugin, crosshairPlugin]} />
      </div>

      {/* X-axis labels */}
      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
        {data.labels.map((l, i) => (
          xLabelIndices[i] ? (
            <span key={i} style={{ fontSize: 9, color: "var(--t3)" }}>{l}</span>
          ) : null
        )).filter(Boolean)}
      </div>

      {/* Shared tooltip */}
      {hoverIdx !== null && tooltipPos && (
        <div style={{
          position: "absolute",
          left: Math.min(tooltipPos.x + 12, (containerRef.current?.offsetWidth ?? 400) - 160),
          top: Math.max(0, tooltipPos.y - 60),
          background: "var(--bg2)", border: "1px solid var(--line)",
          borderRadius: 4, padding: "6px 10px",
          fontSize: 9, pointerEvents: "none",
          zIndex: 50, whiteSpace: "nowrap",
        }}>
          <div style={{ color: "var(--t3)", marginBottom: 2 }}>{data.labels[hoverIdx]}</div>
          <div style={{ color: "var(--t0)" }}>Equity: ${data.equity[hoverIdx]?.toLocaleString()}</div>
          <div style={{ color: data.pnl[hoverIdx] >= 0 ? "var(--green)" : "var(--red)" }}>
            P&L: {data.pnl[hoverIdx] >= 0 ? "+" : ""}${data.pnl[hoverIdx]?.toLocaleString()}
          </div>
        </div>
      )}
    </div>
  );
}

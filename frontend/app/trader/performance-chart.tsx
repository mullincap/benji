"use client";

import { useState, useRef, useEffect } from "react";
import {
  Chart as ChartJS,
  CategoryScale, LinearScale, PointElement, LineElement, BarElement,
  Filler, Tooltip,
} from "chart.js";
import { Line, Bar } from "react-chartjs-2";
import { allocatorApi, ApiBalanceHistory, parseApiError } from "./api";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Filler, Tooltip);

// ─── Types ───────────────────────────────────────────────────────────────────

type TimeRange = "1D" | "1W" | "1M" | "ALL";

interface PerformanceChartProps {
  instanceId: string;
  title?: string;
}

interface ChartData {
  equity: number[];
  pnl: number[];
  labels: string[];
}

// ─── Transform API response → chart data shape ──────────────────────────────

function transformHistory(history: ApiBalanceHistory[]): ChartData {
  const equity: number[] = [];
  const pnl: number[] = [];
  const labels: string[] = [];

  for (let i = 0; i < history.length; i++) {
    const row = history[i];
    equity.push(row.equity_usd);
    // Per-day P&L = equity change since previous row.
    // First row's P&L is unknown (no prior equity reference) → use 0 as the
    // visual baseline rather than a fake value; tooltip will show the raw row.
    pnl.push(i === 0 ? 0 : row.equity_usd - history[i - 1].equity_usd);
    const d = new Date(row.date);
    labels.push(d.toLocaleDateString("en-US", { month: "short", day: "numeric" }));
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

export default function PerformanceChart({ instanceId, title = "PERFORMANCE" }: PerformanceChartProps) {
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
        const result = await allocatorApi.getBalanceHistory(instanceId, range);
        if (cancelled) return;
        setData(transformHistory(result.history));
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

  const ranges: TimeRange[] = ["1D", "1W", "1M", "ALL"];

  // Header (always rendered — tabs remain visible across loading/empty/error).
  const header = (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
      <span style={{ fontSize: 9, color: "var(--t3)", fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase" }}>
        {title}
      </span>
      <div style={{ display: "flex", border: "1px solid var(--line)", borderRadius: 4, overflow: "hidden" }}>
        {ranges.map(r => (
          <button
            key={r}
            onClick={() => setRange(r)}
            style={{
              padding: "3px 8px", fontSize: 9, fontWeight: 700,
              background: range === r ? "var(--bg4)" : "transparent",
              color: range === r ? "var(--t0)" : "var(--t3)",
              border: "none", cursor: "pointer",
              borderRight: r !== "ALL" ? "1px solid var(--line)" : "none",
            }}
          >
            {r}
          </button>
        ))}
      </div>
    </div>
  );

  const wrapperStyle = {
    background: "var(--bg1)", border: "1px solid var(--line)",
    borderRadius: 6, padding: "12px 14px", marginBottom: 20,
    position: "relative" as const,
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

  // Empty: no history rows for this allocation / range yet.
  if (!data || data.equity.length === 0) {
    return (
      <div style={wrapperStyle}>
        {header}
        <StateMessage
          main="No trading history yet"
          sub="First data point appears after the next session closes (~23:55 UTC)"
        />
      </div>
    );
  }

  // Single-point: still too sparse to draw a line or meaningful bar chart.
  // Render the single value as a plain stat with the same empty-state chrome.
  if (data.equity.length === 1) {
    return (
      <div style={wrapperStyle}>
        {header}
        <StateMessage
          main={`$${data.equity[0].toLocaleString()} on ${data.labels[0]}`}
          sub="Only one data point so far — line chart appears after two closes"
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
      <div style={{ height: 180 }}>
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

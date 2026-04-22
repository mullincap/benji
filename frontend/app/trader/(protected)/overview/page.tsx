"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useTrader, Position, Exchange, StrategyInstance, StrategyType, STRATEGY_CATALOG, fmt, GHOST_CURVE, RISK_COLOR, RISK_DIM, StrategyCatalogEntry } from "../../context";
import EquityCurveSvg from "../../equity-curve";
import PerformanceChart from "../../performance-chart";
import TraderCard from "../../components/TraderCard";
import {
  Chart as ChartJS,
  RadialLinearScale, PointElement, LineElement, Filler, Tooltip,
} from "chart.js";
import { Radar } from "react-chartjs-2";

ChartJS.register(RadialLinearScale, PointElement, LineElement, Filler, Tooltip);

const RADAR_DATA: Record<StrategyType, { data: number[]; fill: string; border: string; point: string }> = {
  "alpha-low":  { data: [28, 37, 61, 90, 72], fill: "#00c89615", border: "#00c896", point: "#00c896" },
  "alpha-mid":  { data: [54, 53, 63, 72, 64], fill: "#f0a50015", border: "#f0a500", point: "#f0a500" },
  "alpha-high": { data: [91, 78, 67, 44, 58], fill: "#ff4d4d10", border: "#ff4d4d", point: "#ff4d4d" },
};
// ─── Ghost mock data ─────────────────────────────────────────────────────────

const GHOST_POSITIONS: (Position & { strategy: string; exchange: string })[] = [
  { symbol: "BTCUSDT", side: "LONG",  size: 0.25, entry: 61840, mark: 63420, pnl: 395.0,  pnlPct: 2.55, strategy: "Alpha Mid", exchange: "Binance" },
  { symbol: "ETHUSDT", side: "SHORT", size: 1.80, entry: 3280,  mark: 3195,  pnl: 153.0,  pnlPct: 2.59, strategy: "Alpha Mid", exchange: "Bybit" },
  { symbol: "SOLUSDT", side: "LONG",  size: 12.0, entry: 148.5, mark: 144.2, pnl: -51.6,  pnlPct: -2.89, strategy: "Alpha Low", exchange: "Binance" },
];

// ─── Metric card ─────────────────────────────────────────────────────────────

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 16px" }}>
      <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: color ?? "var(--t0)" }}>{value}</div>
    </div>
  );
}

// ─── Dashboard content ───────────────────────────────────────────────────────

function DashboardContent({ equity, dailyPnl, allTimePnl, sharpe, allocated, activeCount, totalAvailable, positions, showCurve, showAggregate, exchanges, instances }: {
  equity: number; dailyPnl: number; allTimePnl: number; sharpe: number;
  allocated: number; activeCount: number; totalAvailable: number;
  positions: (Position & { strategy: string; exchange: string })[];
  showCurve?: boolean;
  showAggregate?: boolean;
  exchanges?: Exchange[];
  instances?: StrategyInstance[];
}) {
  const availableBalance = Math.max(0, totalAvailable - allocated);
  return (
    <>
      {/* Hero cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 10, marginBottom: 20 }}>
        <MetricCard label="TOTAL EQUITY" value={`$${fmt(equity, 0)}`} />
        <MetricCard label="ALL-TIME P&L" value={allTimePnl === 0 ? "\u2014" : `${allTimePnl >= 0 ? "+" : ""}$${fmt(allTimePnl, 0)}`} color={allTimePnl === 0 ? "var(--t2)" : allTimePnl > 0 ? "var(--green)" : "var(--red)"} />
        <MetricCard label="DAILY P&L" value={dailyPnl === 0 ? "\u2014" : `${dailyPnl >= 0 ? "+" : ""}$${fmt(dailyPnl, 0)}`} color={dailyPnl === 0 ? "var(--t2)" : dailyPnl > 0 ? "var(--green)" : "var(--red)"} />
        <MetricCard label="SHARPE" value={sharpe > 0 ? fmt(sharpe, 2) : "\u2014"} color={sharpe > 0 ? "var(--t0)" : "var(--t2)"} />
        <MetricCard label="ACTIVE TRADERS" value={String(activeCount)} />
      </div>

      {/* Capital deployed bar */}
      <div style={{
        background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 6,
        padding: "10px 14px", marginBottom: 20,
      }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
          <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>CAPITAL DEPLOYED</span>
          <span style={{ fontSize: 10, whiteSpace: "nowrap" }}>
            <span style={{ color: "var(--t2)" }}>${fmt(allocated, 0)} allocated</span>
            <span style={{ color: "var(--t3)", margin: "0 6px" }}>|</span>
            <span style={{ color: "var(--t0)", fontWeight: 700 }}>${fmt(availableBalance, 0)} available</span>
            <span style={{ color: "var(--t3)", margin: "0 6px" }}>|</span>
            <span style={{ color: "var(--t2)" }}>{totalAvailable > 0 ? Math.round((allocated / totalAvailable) * 100) : 0}%</span>
          </span>
        </div>
        <div style={{ height: 5, background: "var(--bg3)", borderRadius: 3, overflow: "hidden" }}>
          <div style={{
            height: "100%",
            width: `${totalAvailable > 0 ? Math.min(100, (allocated / totalAvailable) * 100) : 0}%`,
            background: "var(--green)",
            borderRadius: 3,
          }} />
        </div>
      </div>

      {/* Total account balance — aggregate across all connections */}
      {showAggregate && (
        <PerformanceChart title="TOTAL ACCOUNT BALANCE" />
      )}

      {/* Equity curve — ghost only */}
      {showCurve && (
        <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, overflow: "hidden", marginBottom: 20 }}>
          <div style={{
            padding: "10px 16px", borderBottom: "1px solid var(--line)",
            display: "flex", alignItems: "center", justifyContent: "space-between",
          }}>
            <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>EQUITY CURVE</span>
            <span style={{ fontSize: 10, color: "var(--green)" }}>+38.2% · Sharpe 1.84 · Max DD -11.3%</span>
          </div>
          <div style={{ padding: "8px 0" }}>
            <EquityCurveSvg data={GHOST_CURVE} />
          </div>
        </div>
      )}

      {/* Account status block */}
      <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 20 }}>


      </div>{/* end account status block */}
    </>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function OverviewPage() {
  const router = useRouter();
  const { instances, exchanges, loading, error } = useTrader();
  const empty = instances.length === 0;
  const exchangeBalance = exchanges.reduce((s, e) => s + e.balance, 0);
  const totalAllocatedRaw = instances.reduce((s, i) => s + (i.allocation ?? 0), 0);
  const totalAvailable = exchangeBalance > 0 ? exchangeBalance : totalAllocatedRaw;

  const totalEquity = instances.reduce((s, i) => s + i.equity, 0);
  const dailyPnl = instances.reduce((s, i) => s + i.dailyPnl, 0);
  const totalAllocated = instances.reduce((s, i) => s + (i.allocation ?? 0), 0);
  const activeCount = instances.filter(i => i.status === "live").length;

  // All-time P&L: equity above the originally allocated capital, summed across instances
  const allTimePnl = instances.reduce((s, i) => s + (i.equity - (i.allocation ?? 0)), 0);

  // Portfolio Sharpe: equity-weighted average of per-strategy Sharpe from STRATEGY_CATALOG
  const sharpe = totalEquity > 0
    ? instances.reduce((s, i) => s + ((STRATEGY_CATALOG[i.strategyType]?.sharpe ?? 0) * i.equity), 0) / totalEquity
    : 0;

  const allPositions: (Position & { strategy: string; exchange: string })[] = [];
  for (const inst of instances) {
    for (const p of inst.positions) {
      allPositions.push({ ...p, strategy: inst.strategyName, exchange: inst.exchangeName ?? "" });
    }
  }

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>

        {/* Section label — always visible, outside blur */}
        <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginBottom: 16 }}>
          CAPITAL
        </div>

        {loading && (
          <div style={{ textAlign: "center", padding: "40px 0", color: "var(--t2)", fontSize: 10 }}>Loading...</div>
        )}

        {error && (
          <div style={{ background: "var(--bg2)", border: "1px solid var(--red-dim)", borderRadius: 5, padding: "12px 16px", marginBottom: 16, fontSize: 10, color: "var(--red)" }}>
            {error}
          </div>
        )}

        <div style={{ position: "relative" }}>

          <div style={empty ? { filter: "blur(3.5px) brightness(0.75)", pointerEvents: "none" } : undefined}>
            {empty ? (
              <DashboardContent equity={293593} dailyPnl={14092} allTimePnl={62847} sharpe={2.41} allocated={94000} activeCount={3} totalAvailable={totalAvailable} positions={GHOST_POSITIONS} showCurve />
            ) : (
              <DashboardContent equity={totalEquity} dailyPnl={dailyPnl} allTimePnl={allTimePnl} sharpe={sharpe} allocated={totalAllocated} activeCount={activeCount} totalAvailable={totalAvailable} positions={allPositions} exchanges={exchanges} instances={instances} showAggregate />
            )}
          </div>

          {empty && (
            <div style={{
              position: "absolute", inset: 0,
              display: "flex", flexDirection: "column",
              alignItems: "center", justifyContent: "center",
              borderRadius: 5,
            }}>
              <div style={{ fontSize: 9, color: "var(--t2)", textTransform: "uppercase", letterSpacing: "0.12em", marginBottom: 14 }}>
                SETUP TAKES APPROXIMATELY 3-5 MINUTES
              </div>
              <button
                onClick={() => router.push("/trader/strategies")}
                style={{
                  background: "var(--green)", color: "var(--bg0)",
                  border: "none", borderRadius: 5,
                  padding: "10px 18px", fontSize: 10, fontWeight: 700,
                  letterSpacing: "0.08em", textTransform: "uppercase",
                  cursor: "pointer",
                }}
              >
                SELECT A STRATEGY
              </button>
            </div>
          )}
        </div>

        {/* Traders treemap — only when instances exist */}
        {!empty && instances.length > 0 && (() => {
          const activeInstances = instances.filter(i => i.status === "live" || i.status === "paused");
          const unlinkedInstances = instances.filter(i => i.status === "unlinked");
          const treemapTotal = totalAvailable > 0 ? totalAvailable : totalAllocated;
          const unallocated = Math.max(0, totalAvailable - totalAllocated);

          return (
            <>
              <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginTop: 24, marginBottom: 10 }}>
                TRADERS
              </div>

              {/* Allocation breakdown bands */}
              {activeInstances.length > 0 && (() => {
                const BAND_COLORS: Record<string, { bg: string; border: string; text: string }> = {
                  low:    { bg: "var(--green-dim)", border: "var(--green-mid)", text: "var(--green)" },
                  medium: { bg: "#f0a50015", border: "#f0a50030", text: "var(--amber)" },
                  high:   { bg: "#ff4d4d10", border: "#ff4d4d25", text: "var(--red)" },
                };
                const BAR_FILL: Record<string, string> = { low: "var(--green)", medium: "var(--amber)", high: "var(--red)" };
                const totalAlloc = activeInstances.reduce((s, i) => s + (i.allocation ?? 0), 0);
                const unalloc = Math.max(0, treemapTotal - totalAlloc);
                const fmtAbbrev = (n: number) => n >= 1000000 ? `$${(n / 1000000).toFixed(1)}m` : n >= 1000 ? `$${Math.round(n / 1000)}k` : `$${n}`;

                return (
                  <div style={{
                    background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6,
                    padding: "10px 14px", marginBottom: 8,
                  }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                      <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>ALLOCATION BREAKDOWN</span>
                      <span style={{ fontSize: 9, color: "var(--t3)" }}>${fmt(treemapTotal, 0)} total</span>
                    </div>
                    <div style={{ display: "flex", gap: 4, height: 36, borderRadius: 5, overflow: "hidden", marginBottom: 6 }}>
                      {activeInstances.map(inst => {
                        const alloc = inst.allocation ?? 0;
                        const pctWidth = treemapTotal > 0 ? (alloc / treemapTotal) * 100 : 0;
                        const bc = BAND_COLORS[inst.risk] ?? BAND_COLORS.low;
                        const showText = pctWidth >= 8;
                        const pctOfTotal = treemapTotal > 0 ? ((alloc / treemapTotal) * 100).toFixed(1) : "0.0";
                        return (
                          <div key={inst.id} style={{
                            width: `${pctWidth}%`, minWidth: 4,
                            background: bc.bg, border: `0.5px solid ${bc.border}`,
                            borderRadius: 3, padding: showText ? "0 10px" : 0,
                            display: "flex", alignItems: "center", overflow: "hidden",
                          }}>
                            {showText && (
                              <div style={{ overflow: "hidden" }}>
                                <div style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", color: bc.text, whiteSpace: "nowrap" }}>{inst.strategyName}</div>
                                <div style={{ fontSize: 8, color: bc.text, opacity: 0.5, whiteSpace: "nowrap" }}>${fmt(alloc, 0)} &middot; {pctOfTotal}%</div>
                              </div>
                            )}
                          </div>
                        );
                      })}
                      {unalloc > 0 && (
                        <div style={{
                          flex: 1, minWidth: 4,
                          background: "var(--bg3)", border: "0.5px solid var(--line)",
                          borderRadius: 3, padding: "0 8px",
                          display: "flex", alignItems: "center",
                        }}>
                          <span style={{ fontSize: 9, color: "var(--t3)", whiteSpace: "nowrap" }}>IDLE</span>
                        </div>
                      )}
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 10 }}>
                      <span style={{ fontSize: 9, color: "var(--t3)" }}>$0</span>
                      <span style={{ fontSize: 9, color: "var(--t3)" }}>{fmtAbbrev(Math.round(treemapTotal / 2))}</span>
                      <span style={{ fontSize: 9, color: "var(--t3)" }}>{fmtAbbrev(treemapTotal)}</span>
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                      {activeInstances.map(inst => {
                        const alloc = inst.allocation ?? 0;
                        const pct = treemapTotal > 0 ? ((alloc / treemapTotal) * 100).toFixed(1) : "0.0";
                        const fillColor = BAR_FILL[inst.risk] ?? "var(--green)";
                        return (
                          <div key={inst.id} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <span style={{ width: 7, height: 7, borderRadius: 2, background: fillColor, flexShrink: 0 }} />
                            <span style={{ fontSize: 10, color: "var(--t2)", width: 80, flexShrink: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{inst.strategyName}</span>
                            <div style={{ flex: 1, height: 4, background: "var(--bg3)", borderRadius: 2, overflow: "hidden" }}>
                              <div style={{ height: "100%", width: `${treemapTotal > 0 ? (alloc / treemapTotal) * 100 : 0}%`, background: fillColor, borderRadius: 2 }} />
                            </div>
                            <span style={{ fontSize: 10, fontWeight: 700, color: "var(--t0)", flexShrink: 0, textAlign: "right", width: 65 }}>${fmt(alloc, 0)}</span>
                            <span style={{ fontSize: 9, color: "var(--t3)", flexShrink: 0, width: 35, textAlign: "right" }}>{pct}%</span>
                          </div>
                        );
                      })}
                      {unalloc > 0 && (
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ width: 7, height: 7, borderRadius: 2, background: "var(--bg3)", flexShrink: 0 }} />
                          <span style={{ fontSize: 10, color: "var(--t3)", width: 80, flexShrink: 0 }}>Unallocated</span>
                          <div style={{ flex: 1, height: 4, background: "var(--bg3)", borderRadius: 2, overflow: "hidden" }}>
                            <div style={{ height: "100%", width: `${treemapTotal > 0 ? (unalloc / treemapTotal) * 100 : 0}%`, background: "var(--bg3)", borderRadius: 2 }} />
                          </div>
                          <span style={{ fontSize: 10, fontWeight: 700, color: "var(--t3)", flexShrink: 0, textAlign: "right", width: 65 }}>${fmt(unalloc, 0)}</span>
                          <span style={{ fontSize: 9, color: "var(--t3)", flexShrink: 0, width: 35, textAlign: "right" }}>{treemapTotal > 0 ? ((unalloc / treemapTotal) * 100).toFixed(1) : "0.0"}%</span>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })()}

              {/* Treemap */}
              {activeInstances.length > 0 && (
                <div style={{ display: "flex", gap: 4, height: 100, marginBottom: unlinkedInstances.length > 0 ? 10 : 0 }}>
                  {activeInstances.map(inst => {
                    const isLive = inst.status === "live";
                    const alloc = inst.allocation ?? 0;
                    const pct = treemapTotal > 0 ? (alloc / treemapTotal) * 100 : 0;
                    const allocPct = totalAllocated > 0 ? Math.round((alloc / totalAllocated) * 100) : 0;
                    return (
                      <div
                        key={inst.id}
                        onClick={() => router.push(`/trader/traders/${inst.id}`)}
                        style={{
                          width: `${pct}%`, minWidth: 60,
                          background: isLive ? "var(--green-dim)" : "var(--bg3)",
                          border: isLive ? "0.5px solid var(--green-mid)" : "0.5px solid var(--line)",
                          borderRadius: 5, padding: 12,
                          display: "flex", flexDirection: "column", justifyContent: "space-between",
                          overflow: "hidden", cursor: "pointer",
                          transition: "border-color 0.15s ease",
                        }}
                        onMouseEnter={e => { e.currentTarget.style.borderColor = isLive ? "var(--green)" : "var(--line2)"; }}
                        onMouseLeave={e => { e.currentTarget.style.borderColor = isLive ? "var(--green-mid)" : "var(--line)"; }}
                      >
                        {/* Top: name + risk badge */}
                        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                          <span style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", color: isLive ? "var(--green)" : "var(--t2)", letterSpacing: "0.06em", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{inst.strategyName}</span>
                          <span style={{ fontSize: 8, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", padding: "1px 5px", borderRadius: 2, background: RISK_DIM[inst.risk], color: RISK_COLOR[inst.risk], flexShrink: 0 }}>{inst.risk}</span>
                        </div>
                        {/* Bottom: allocation + pct + P&L */}
                        <div>
                          <div style={{ fontSize: 18, fontWeight: 700, color: isLive ? "var(--t0)" : "var(--t2)" }}>${fmt(alloc, 0)}</div>
                          <div style={{ fontSize: 9, color: "var(--t3)" }}>{allocPct}% of total</div>
                          <div style={{ fontSize: 9, color: !inst.dailyPnl ? "var(--t3)" : inst.dailyPnl > 0 ? "var(--green)" : "var(--red)" }}>
                            {!inst.dailyPnl ? "\u2014" : `${inst.dailyPnl >= 0 ? "+" : ""}$${fmt(inst.dailyPnl)} today`}
                          </div>
                        </div>
                      </div>
                    );
                  })}

                  {/* Unallocated block */}
                  {unallocated > 0 && (
                    <div style={{
                      width: `${(unallocated / treemapTotal) * 100}%`, minWidth: 50,
                      background: "var(--bg2)", border: "0.5px solid var(--line)",
                      borderRadius: 5, padding: 12,
                      display: "flex", flexDirection: "column", justifyContent: "space-between",
                      overflow: "hidden",
                    }}>
                      <span style={{ fontSize: 9, fontWeight: 700, textTransform: "uppercase", color: "var(--t3)", letterSpacing: "0.06em" }}>UNALLOCATED</span>
                      <div>
                        <div style={{ fontSize: 18, fontWeight: 700, color: "var(--t3)" }}>${fmt(unallocated, 0)}</div>
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* Radar chart — strategy profile */}
              {activeInstances.length > 0 && (() => {
                const liveTypes = new Set(activeInstances.filter(i => i.status === "live").map(i => i.strategyType));
                const datasets = Array.from(liveTypes).map(type => {
                  const rd = RADAR_DATA[type as StrategyType];
                  if (!rd) return null;
                  const inst = activeInstances.find(i => i.strategyType === type);
                  return {
                    label: inst?.strategyName ?? type,
                    data: rd.data,
                    backgroundColor: rd.fill,
                    borderColor: rd.border,
                    borderWidth: 1.5,
                    pointBackgroundColor: rd.point,
                    pointRadius: 3,
                    pointHoverRadius: 4,
                  };
                }).filter(Boolean) as any[];
                if (datasets.length === 0) return null;

                return (
                  <div style={{
                    background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6,
                    padding: "12px 16px", marginTop: 8,
                    display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, alignItems: "center",
                  }}>
                    {/* Left: radar */}
                    <div>
                      <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>STRATEGY PROFILE</div>
                      <div style={{ position: "relative", height: 180 }}>
                        <Radar
                          data={{
                            labels: ["Return", "Sharpe", "Win Rate", "Stability", "Consistency"],
                            datasets,
                          }}
                          options={{
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {
                              r: {
                                min: 0, max: 100,
                                ticks: { display: false },
                                grid: { color: "#1a1a1d" },
                                angleLines: { color: "#1a1a1d" },
                                pointLabels: {
                                  color: "#35332f",
                                  font: { size: 8, family: "'Space Mono', monospace" },
                                },
                              },
                            },
                            plugins: {
                              legend: { display: false },
                              tooltip: {
                                backgroundColor: "#141416",
                                borderColor: "#242428",
                                borderWidth: 1,
                                titleFont: { size: 0 },
                                bodyFont: { size: 9, family: "'Space Mono', monospace" },
                                padding: 8,
                                displayColors: false,
                                callbacks: {
                                  title: () => "",
                                  label: (ctx) => `${ctx.dataset.label}: ${ctx.parsed.r}`,
                                },
                              },
                            },
                          }}
                        />
                      </div>
                    </div>
                    {/* Right: legend + axes */}
                    <div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 12 }}>
                        {datasets.map((ds: any) => (
                          <div key={ds.label} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <span style={{ width: 8, height: 8, borderRadius: 2, background: ds.borderColor, flexShrink: 0 }} />
                            <span style={{ fontSize: 10, color: "var(--t2)" }}>{ds.label}</span>
                          </div>
                        ))}
                      </div>
                      <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>AXES</div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                        {[
                          { name: "Return", desc: "YTD %" },
                          { name: "Sharpe", desc: "risk-adj return" },
                          { name: "Win Rate", desc: "% winning days" },
                          { name: "Stability", desc: "inverse of max DD" },
                          { name: "Consistency", desc: "CAGR / volatility" },
                        ].map(ax => (
                          <div key={ax.name} style={{ display: "flex", gap: 6 }}>
                            <span style={{ fontSize: 9, color: "var(--t2)" }}>{ax.name}</span>
                            <span style={{ fontSize: 9, color: "var(--t3)" }}>&mdash; {ax.desc}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                );
              })()}

              {/* Unlinked instances still shown as cards */}
              {unlinkedInstances.length > 0 && (
                <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: activeInstances.length > 0 ? 10 : 0 }}>
                  {unlinkedInstances.map(inst => (
                    <TraderCard key={inst.id} inst={inst} />
                  ))}
                </div>
              )}
            </>
          );
        })()}
      </div>
    </div>
  );
}

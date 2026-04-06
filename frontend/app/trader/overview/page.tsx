"use client";

import { useState, useMemo } from "react";
import { useRouter } from "next/navigation";
import { useTrader, Position, Exchange, StrategyInstance, StrategyType, STRATEGY_CATALOG, fmt, GHOST_CURVE, RISK_COLOR, RISK_DIM } from "../context";
import EquityCurveSvg from "../equity-curve";
import PerformanceChart from "../performance-chart";
import TraderCard from "../components/TraderCard";
import {
  Chart as ChartJS,
  LinearScale, PointElement, Tooltip,
} from "chart.js";
import { Bubble } from "react-chartjs-2";

ChartJS.register(LinearScale, PointElement, Tooltip);

// ─── Bubble chart strategy data ──────────────────────────────────────────────

const BUBBLE_STRATEGY_DATA: Record<StrategyType, { x: number; y: number; fill: string; border: string }> = {
  "alpha-low":  { x: 8.2,  y: 14.2, fill: "#00c89625", border: "#00c896" },
  "alpha-mid":  { x: 14.6, y: 38.2, fill: "#f0a50025", border: "#f0a500" },
  "alpha-high": { x: 19.9, y: 91.4, fill: "#ff4d4d20", border: "#ff4d4d" },
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

function DashboardContent({ equity, dailyPnl, allocated, activeCount, totalAvailable, positions, showCurve, exchanges, instances }: {
  equity: number; dailyPnl: number; allocated: number; activeCount: number; totalAvailable: number;
  positions: (Position & { strategy: string; exchange: string })[];
  showCurve?: boolean;
  exchanges?: Exchange[];
  instances?: StrategyInstance[];
}) {
  const availableBalance = Math.max(0, totalAvailable - allocated);
  const [exchangesOpen, setExchangesOpen] = useState(false);
  const [positionsOpen, setPositionsOpen] = useState(positions.length > 0);
  return (
    <>
      {/* Hero cards */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 8 }}>
        <MetricCard label="TOTAL EQUITY" value={`$${fmt(equity, 0)}`} />
        <MetricCard label="DAILY P&L" value={dailyPnl === 0 ? "\u2014" : `${dailyPnl >= 0 ? "+" : ""}$${fmt(dailyPnl, 0)}`} color={dailyPnl === 0 ? "var(--t2)" : dailyPnl > 0 ? "var(--green)" : "var(--red)"} />
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

      {/* Account status block — chart, exchanges, positions grouped tightly */}
      <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 20 }}>

      {/* Combined performance chart — live data only */}
      {!showCurve && allocated > 0 && (
        <div style={{ marginBottom: -14 }}>
          <PerformanceChart allocation={allocated} ytdReturn={28} title="COMBINED PERFORMANCE" />
        </div>
      )}

      {/* Exchange accounts table — collapsible */}
      {exchanges && exchanges.length > 0 && instances && (
        <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6, overflow: "hidden" }}>
          <div
            onClick={() => setExchangesOpen(v => !v)}
            style={{
              padding: "10px 14px",
              borderBottom: exchangesOpen ? "1px solid var(--line)" : "none",
              display: "flex", alignItems: "center", justifyContent: "space-between",
              cursor: "pointer",
            }}
          >
            <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>EXCHANGE ACCOUNTS</span>
            <span style={{ fontSize: 9, color: "var(--t3)", transition: "transform 0.2s ease", display: "inline-block", transform: exchangesOpen ? "rotate(90deg)" : "rotate(0deg)" }}>{"\u25B6"}</span>
          </div>
          <div style={{ overflow: "hidden", maxHeight: exchangesOpen ? 1000 : 0, transition: "max-height 0.2s ease" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--line)" }}>
                  {["EXCHANGE", "ACCOUNT BALANCE", "ALLOCATED", "TRADERS", "STATUS"].map(h => (
                    <th key={h} style={{ padding: "7px 14px", textAlign: "left", fontSize: 9, color: "var(--t3)", fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {exchanges.map((ex, idx) => {
                  const exInstances = instances.filter(i => i.exchangeName === ex.name && i.status === "live");
                  const exAllocated = exInstances.reduce((s, i) => s + (i.allocation ?? 0), 0);
                  const pctDeployed = ex.balance > 0 ? Math.min(100, (exAllocated / ex.balance) * 100).toFixed(1) : "0.0";
                  return (
                    <tr key={ex.id} style={{ borderBottom: idx < exchanges.length - 1 ? "1px solid var(--line)" : "none" }}>
                      <td style={{ padding: "10px 14px" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--green)", flexShrink: 0 }} />
                          <span style={{ fontSize: 10, fontWeight: 700, color: "var(--t0)" }}>{ex.name}</span>
                        </div>
                      </td>
                      <td style={{ padding: "10px 14px" }}>
                        <div style={{ fontSize: 10, color: "var(--t1)" }}>${ex.balance.toLocaleString("en-US")}</div>
                        <div style={{ fontSize: 9, color: "var(--t3)" }}>{ex.maskedKey}</div>
                      </td>
                      <td style={{ padding: "10px 14px" }}>
                        <div style={{ fontSize: 10, color: "var(--t1)" }}>${exAllocated.toLocaleString("en-US")}</div>
                        <div style={{ fontSize: 9, color: "var(--t3)" }}>{exInstances.length} traders &middot; {pctDeployed}% deployed</div>
                      </td>
                      <td style={{ padding: "10px 14px", fontSize: 10, color: "var(--t1)" }}>{exInstances.length}</td>
                      <td style={{ padding: "10px 14px" }}>
                        <span style={{ fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 3, background: "var(--green-dim)", color: "var(--green)", border: "1px solid var(--green-mid)" }}>Connected</span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Positions table — collapsible */}
      <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6, overflow: "hidden" }}>

        <div
          onClick={() => setPositionsOpen(v => !v)}
          style={{
            padding: "10px 14px",
            borderBottom: positionsOpen ? "1px solid var(--line)" : "none",
            display: "flex", alignItems: "center", justifyContent: "space-between",
            cursor: "pointer",
          }}
        >
          <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>OPEN POSITIONS</span>
          <span style={{ fontSize: 9, color: "var(--t3)", transition: "transform 0.2s ease", display: "inline-block", transform: positionsOpen ? "rotate(90deg)" : "rotate(0deg)" }}>{"\u25B6"}</span>
        </div>
        <div style={{ overflow: "hidden", maxHeight: positionsOpen ? 1000 : 0, transition: "max-height 0.2s ease" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--line)" }}>
                {["SYMBOL", "STRATEGY", "EXCHANGE", "SIDE", "SIZE", "P&L"].map(h => (
                  <th key={h} style={{ padding: "7px 14px", textAlign: h === "P&L" ? "right" : "left", color: "var(--t3)", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {positions.length === 0 ? (
                <tr><td colSpan={6} style={{ padding: "20px 14px", textAlign: "center", color: "var(--t2)", fontSize: 10 }}>No open positions</td></tr>
              ) : positions.map((p, i) => (
                <tr key={`${p.symbol}-${p.strategy}-${i}`} style={{ borderBottom: i < positions.length - 1 ? "1px solid var(--line)" : "none" }}>
                  <td style={{ padding: "10px 14px", color: "var(--t0)" }}>{p.symbol}</td>
                  <td style={{ padding: "10px 14px", color: "var(--t1)" }}>{p.strategy}</td>
                  <td style={{ padding: "10px 14px", color: "var(--t1)" }}>{p.exchange}</td>
                  <td style={{ padding: "10px 14px" }}>
                    <span style={{ fontSize: 9, padding: "2px 6px", borderRadius: 2, background: p.side === "LONG" ? "var(--green-dim)" : "var(--red-dim)", color: p.side === "LONG" ? "var(--green)" : "var(--red)" }}>{p.side}</span>
                  </td>
                  <td style={{ padding: "10px 14px", color: "var(--t1)" }}>{p.size}</td>
                  <td style={{ padding: "10px 14px", textAlign: "right", color: p.pnl >= 0 ? "var(--green)" : "var(--red)" }}>
                    {p.pnl >= 0 ? "+" : ""}${fmt(p.pnl)}
                    <span style={{ fontSize: 9, opacity: 0.7, marginLeft: 4 }}>({p.pnlPct >= 0 ? "+" : ""}{fmt(p.pnlPct)}%)</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      </div>{/* end account status block */}
    </>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function OverviewPage() {
  const router = useRouter();
  const { instances, exchanges } = useTrader();
  const empty = instances.length === 0;
  const exchangeBalance = exchanges.reduce((s, e) => s + e.balance, 0);
  const totalAllocatedRaw = instances.reduce((s, i) => s + (i.allocation ?? 0), 0);
  const totalAvailable = exchangeBalance > 0 ? exchangeBalance : totalAllocatedRaw;

  const totalEquity = instances.reduce((s, i) => s + i.equity, 0);
  const dailyPnl = instances.reduce((s, i) => s + i.dailyPnl, 0);
  const totalAllocated = instances.reduce((s, i) => s + (i.allocation ?? 0), 0);
  const activeCount = instances.filter(i => i.status === "live").length;

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
          OVERVIEW
        </div>

        <div style={{ position: "relative" }}>

          <div style={empty ? { filter: "blur(3.5px) brightness(0.75)", pointerEvents: "none" } : undefined}>
            {empty ? (
              <DashboardContent equity={293593} dailyPnl={14092} allocated={94000} activeCount={3} totalAvailable={totalAvailable} positions={GHOST_POSITIONS} showCurve />
            ) : (
              <DashboardContent equity={totalEquity} dailyPnl={dailyPnl} allocated={totalAllocated} activeCount={activeCount} totalAvailable={totalAvailable} positions={allPositions} exchanges={exchanges} instances={instances} />
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

              {/* Treemap */}
              {activeInstances.length > 0 && (
                <div style={{ display: "flex", gap: 4, height: 200, marginBottom: unlinkedInstances.length > 0 ? 10 : 0 }}>
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

              {/* Bubble chart — risk/return */}
              {activeInstances.length > 0 && (() => {
                // Group instances by strategy, only live
                const liveByStrategy: Record<string, { alloc: number; inst: typeof activeInstances[0] }> = {};
                for (const inst of activeInstances) {
                  if (inst.status !== "live") continue;
                  const key = inst.strategyType;
                  if (!liveByStrategy[key]) liveByStrategy[key] = { alloc: 0, inst };
                  liveByStrategy[key].alloc += inst.allocation ?? 0;
                }
                const datasets = Object.entries(liveByStrategy).map(([type, { alloc, inst }]) => {
                  const bd = BUBBLE_STRATEGY_DATA[type as StrategyType];
                  if (!bd) return null;
                  const r = Math.max(6, Math.min(28, Math.sqrt(alloc / 1000)));
                  return {
                    label: inst.strategyName,
                    data: [{ x: bd.x, y: bd.y, r }],
                    backgroundColor: bd.fill,
                    borderColor: bd.border,
                    borderWidth: 1.5,
                    hoverBackgroundColor: bd.border + "40",
                    hoverBorderColor: bd.border,
                    // stash for tooltip
                    _meta: { name: inst.strategyName, exchange: inst.exchangeName, alloc },
                  };
                }).filter(Boolean) as any[];

                return (
                  <div style={{
                    background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6,
                    padding: "10px 14px", marginTop: 8,
                  }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 6 }}>
                      <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>RISK / RETURN</span>
                      <span style={{ fontSize: 9, color: "var(--t3)" }}>bubble size = allocation</span>
                    </div>
                    <div style={{ height: 180 }}>
                      <Bubble
                        data={{ datasets }}
                        options={{
                          responsive: true,
                          maintainAspectRatio: false,
                          scales: {
                            x: {
                              reverse: true,
                              min: 0, max: 25,
                              ticks: {
                                color: "#35332f",
                                font: { size: 9, family: "'Space Mono', monospace" },
                                callback: (v) => `${v}%`,
                              },
                              grid: { color: "#141416" },
                              border: { display: false },
                            },
                            y: {
                              min: 0, max: 110,
                              ticks: {
                                color: "#35332f",
                                font: { size: 9, family: "'Space Mono', monospace" },
                                callback: (v) => `+${v}%`,
                              },
                              grid: { color: "#141416" },
                              border: { display: false },
                            },
                          },
                          plugins: {
                            legend: { display: false },
                            tooltip: {
                              backgroundColor: "#141416",
                              borderColor: "#242428",
                              borderWidth: 1,
                              titleFont: { size: 9, family: "'Space Mono', monospace" },
                              bodyFont: { size: 9, family: "'Space Mono', monospace" },
                              padding: 8,
                              callbacks: {
                                label: (ctx) => {
                                  const meta = (ctx.dataset as any)._meta;
                                  return `${meta.name} · ${meta.exchange} · $${meta.alloc.toLocaleString("en-US")}`;
                                },
                              },
                            },
                          },
                        }}
                      />
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

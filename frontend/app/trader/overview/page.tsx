"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useTrader, Position, Exchange, StrategyInstance, fmt, GHOST_CURVE, RISK_COLOR, RISK_DIM } from "../context";
import EquityCurveSvg from "../equity-curve";
import PerformanceChart from "../performance-chart";
import TraderCard from "../components/TraderCard";

// ─── Toggle ─────────────────────────────────────────────────────────────────

function Toggle({ on, onColor, offColor, onToggle, label }: {
  on: boolean; onColor: string; offColor: string; onToggle: () => void; label: string;
}) {
  return (
    <button onClick={e => { e.stopPropagation(); onToggle(); }} style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      background: "transparent", border: "none", cursor: "pointer",
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
      color: on ? onColor : offColor,
    }}>
      <span style={{
        width: 28, height: 14, borderRadius: 7,
        background: on ? onColor : "var(--bg4)",
        position: "relative", display: "inline-block", transition: "background 0.2s ease",
      }}>
        <span style={{
          width: 10, height: 10, borderRadius: "50%",
          background: on ? "var(--bg0)" : "var(--t2)",
          position: "absolute", top: 2, left: on ? 16 : 2,
          transition: "left 0.2s ease",
        }} />
      </span>
      {label}
    </button>
  );
}

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
  const availableBalance = totalAvailable - allocated;
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
  const { instances, exchanges, updateInstance } = useTrader();
  const empty = instances.length === 0;
  const totalAvailable = exchanges.reduce((s, e) => s + e.balance, 0);

  const [editingId, setEditingId] = useState<string | null>(null);
  const [editValue, setEditValue] = useState("");
  const [flashId, setFlashId] = useState<string | null>(null);
  const blurTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const totalEquity = instances.reduce((s, i) => s + i.equity, 0);
  const dailyPnl = instances.reduce((s, i) => s + i.dailyPnl, 0);
  const totalAllocated = instances.reduce((s, i) => s + (i.allocation ?? 0), 0);
  const activeCount = instances.filter(i => i.status === "live").length;

  // Preview: swap the editing instance's allocation with editValue
  const editNum = parseInt(editValue) || 0;
  const previewAllocated = editingId
    ? totalAllocated - (instances.find(i => i.id === editingId)?.allocation ?? 0) + editNum
    : totalAllocated;

  const allPositions: (Position & { strategy: string; exchange: string })[] = [];
  for (const inst of instances) {
    for (const p of inst.positions) {
      allPositions.push({ ...p, strategy: inst.strategyName, exchange: inst.exchangeName ?? "" });
    }
  }

  function startEdit(inst: StrategyInstance) {
    setEditingId(inst.id);
    setEditValue(String(inst.allocation ?? 0));
  }

  function confirmEdit() {
    if (!editingId) return;
    const inst = instances.find(i => i.id === editingId);
    if (!inst) return;
    const val = parseInt(editValue) || 0;
    const maxAlloc = totalAvailable - totalAllocated + (inst.allocation ?? 0);
    if (val <= 0 || val > maxAlloc) return;
    updateInstance(editingId, { allocation: val });
    setFlashId(editingId);
    setEditingId(null);
    setEditValue("");
    setTimeout(() => setFlashId(null), 500);
  }

  function cancelEdit() {
    setEditingId(null);
    setEditValue("");
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
              <DashboardContent equity={totalEquity} dailyPnl={dailyPnl} allocated={previewAllocated} activeCount={activeCount} totalAvailable={totalAvailable} positions={allPositions} exchanges={exchanges} instances={instances} />
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
                LAUNCH YOUR FIRST STRATEGY
              </button>
            </div>
          )}
        </div>

        {/* Traders list — only when instances exist */}
        {!empty && instances.length > 0 && (
          <>
            <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase", marginTop: 24, marginBottom: 10 }}>
              TRADERS
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              {instances.map(inst => {
                const isEditing = editingId === inst.id;
                const isFlash = flashId === inst.id;
                const isLiveOrPaused = inst.status === "live" || inst.status === "paused";
                const maxAlloc = totalAvailable - totalAllocated + (inst.allocation ?? 0);
                const currentEditVal = parseInt(editValue) || 0;
                const editValid = isEditing && currentEditVal > 0 && currentEditVal <= maxAlloc;

                if (inst.status === "unlinked") {
                  return <TraderCard key={inst.id} inst={inst} />;
                }

                return (
                  <div key={inst.id}>
                    <div
                      onClick={() => { if (!isEditing) router.push(`/trader/traders/${inst.id}`); }}
                      style={{
                        background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5,
                        padding: "16px 18px", cursor: isEditing ? "default" : "pointer",
                        opacity: inst.status === "live" ? 1 : 0.6,
                        transition: "border-color 0.15s ease, opacity 0.2s ease",
                      }}
                      onMouseEnter={e => { if (!isEditing) e.currentTarget.style.borderColor = "var(--line2)"; }}
                      onMouseLeave={e => (e.currentTarget.style.borderColor = "var(--line)")}
                    >
                      {/* Top row: name, badges, toggle */}
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ fontSize: 14, fontWeight: 700, color: "var(--t0)" }}>{inst.strategyName}</span>
                          {inst.exchangeName && (
                            <span style={{ fontSize: 9, padding: "2px 6px", borderRadius: 2, background: "var(--bg3)", color: "var(--t1)", border: "1px solid var(--line)" }}>{inst.exchangeName}</span>
                          )}
                          <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", padding: "2px 6px", borderRadius: 2, background: RISK_DIM[inst.risk], color: RISK_COLOR[inst.risk] }}>{inst.risk}</span>
                        </div>
                        <Toggle
                          on={inst.status === "live"}
                          onColor="var(--green)"
                          offColor="var(--t2)"
                          onToggle={() => updateInstance(inst.id, { status: inst.status === "live" ? "paused" : "live" })}
                          label={inst.status === "live" ? "Live" : "Paused"}
                        />
                      </div>
                      {/* Bottom row: allocation (editable) + stats */}
                      <div style={{ display: "flex", gap: 20, fontSize: 10, color: "var(--t2)", alignItems: "center" }}>
                        {/* Allocation — editable */}
                        <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
                          <span style={{ fontSize: 9, color: "var(--t3)" }}>allocation</span>
                          {isEditing ? (
                            <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }} onClick={e => e.stopPropagation()}>
                              <input
                                type="text"
                                autoFocus
                                value={editValue}
                                onChange={e => {
                                  const raw = e.target.value.replace(/[^0-9]/g, "");
                                  setEditValue(raw);
                                }}
                                onBlur={() => { blurTimerRef.current = setTimeout(cancelEdit, 150); }}
                                onKeyDown={e => { if (e.key === "Enter" && editValid) confirmEdit(); if (e.key === "Escape") cancelEdit(); }}
                                style={{
                                  width: 80, background: "var(--bg0)", border: "1px solid var(--green)",
                                  borderRadius: 3, padding: "3px 6px", color: "var(--t0)",
                                  fontSize: 10, outline: "none",
                                }}
                              />
                              <span
                                onMouseDown={e => { e.preventDefault(); if (blurTimerRef.current) clearTimeout(blurTimerRef.current); }}
                                onClick={e => { e.stopPropagation(); if (editValid) confirmEdit(); }}
                                style={{
                                  fontSize: 11, color: "var(--green)", cursor: "pointer",
                                  pointerEvents: editValid ? "auto" : "none",
                                  opacity: editValid ? 1 : 0.3,
                                }}
                              >{"\u2713"}</span>
                              <span
                                onMouseDown={e => { e.preventDefault(); if (blurTimerRef.current) clearTimeout(blurTimerRef.current); }}
                                onClick={e => { e.stopPropagation(); cancelEdit(); }}
                                style={{ fontSize: 11, color: "var(--t3)", cursor: "pointer" }}
                              >{"\u2715"}</span>
                            </span>
                          ) : (
                            <span
                              onClick={e => { e.stopPropagation(); startEdit(inst); }}
                              style={{
                                color: isFlash ? "var(--green)" : "var(--t2)",
                                borderBottom: "1px dashed var(--line)",
                                cursor: "pointer",
                                transition: "color 0.3s ease",
                              }}
                            >${fmt(inst.allocation ?? 0, 0)}</span>
                          )}
                        </span>
                        <span style={{ color: !inst.dailyPnl ? "var(--t2)" : inst.dailyPnl > 0 ? "var(--green)" : "var(--red)" }}>
                          {!inst.dailyPnl ? "\u2014" : `${inst.dailyPnl >= 0 ? "+" : ""}$${fmt(inst.dailyPnl)}`} today
                        </span>
                        <span>{inst.positions.length} open</span>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

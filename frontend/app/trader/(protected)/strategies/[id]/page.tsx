"use client";

import { useState, useEffect, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import { useTrader, STRATEGY_CATALOG, CAPACITY_DATA, StrategyType, StrategyInstance, fmt, RISK_COLOR, RISK_DIM, GHOST_CURVE } from "../../../context";
import { allocatorApi } from "../../../api";
import EquityCurveSvg from "../../../equity-curve";
import SetupWizard from "../../../components/SetupWizard";

// ─── Metric card ─────────────────────────────────────────────────────────────

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 16px" }}>
      <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: color ?? "var(--t0)" }}>{value}</div>
    </div>
  );
}

// ─── Calendar returns heatmap ────────────────────────────────────────────────

interface DayData { date: string; day: number; dow: number; ret: number | null; future?: boolean; }

// TODO: replace with real backtest daily returns from backend
function generateMockCalendarData(): Record<string, DayData[]> {
  const months: Record<string, DayData[]> = {};
  const now = new Date();
  // Go back 11 months from current month, plus current month = 12 months
  for (let m = 11; m >= 0; m--) {
    const d0 = new Date(now.getFullYear(), now.getMonth() - m, 1);
    const year = d0.getFullYear();
    const month = d0.getMonth();
    const key = `${year}-${String(month + 1).padStart(2, "0")}`;
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    const days: DayData[] = [];
    for (let d = 1; d <= daysInMonth; d++) {
      const date = new Date(year, month, d);
      const dow = date.getDay();
      const isFuture = date > now;
      const isWeekend = dow === 0 || dow === 6;
      const seed = Math.sin(d * 7919 + month * 1031 + year) * 10000;
      const rand = seed - Math.floor(seed);
      const ret = isFuture ? null : isWeekend ? null : Math.round(((rand - 0.38) * 4) * 100) / 100;
      days.push({ date: date.toISOString().slice(0, 10), day: d, dow, ret, future: isFuture });
    }
    months[key] = days;
  }
  return months;
}

const MOCK_CALENDAR = generateMockCalendarData();

function getReturnColor(ret: number | null): string {
  if (ret === null) return "var(--bg3)";
  if (ret > 2) return "var(--green)";
  if (ret > 0.5) return "var(--green-mid)";
  if (ret > -0.5) return "var(--bg4)";
  if (ret > -2) return "var(--red-dim)";
  return "var(--red)";
}

function getReturnTextColor(ret: number | null): string {
  if (ret === null) return "var(--t3)";
  if (ret > 0.5) return "var(--green)";
  if (ret < -0.5) return "var(--red)";
  return "var(--t2)";
}

function CalendarHeatmap() {
  const [mode, setMode] = useState<"GRID" | "CHART">("GRID");
  const [expanded, setExpanded] = useState(false);

  const monthKeys = Object.keys(MOCK_CALENDAR).sort();
  const visibleKeys = expanded ? monthKeys : monthKeys.slice(-3);
  const DOW = ["S", "M", "T", "W", "T", "F", "S"];

  return (
    <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, overflow: "hidden", marginBottom: 20 }}>
      <div style={{ padding: "10px 16px", borderBottom: "1px solid var(--line)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>CALENDAR RETURNS</span>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {(["GRID", "CHART"] as const).map((m, i) => (
            <span key={m} style={{ display: "inline-flex", alignItems: "center", gap: 12 }}>
              {i > 0 && <span style={{ color: "var(--t3)", fontSize: 9 }}>{"\u00B7"}</span>}
              <button onClick={() => setMode(m)} style={{
                padding: 0, fontSize: 9, fontWeight: 700, letterSpacing: "0.08em",
                background: "transparent", color: mode === m ? "var(--t0)" : "var(--t3)",
                border: "none", cursor: "pointer",
              }}>{m}</button>
            </span>
          ))}
        </div>
      </div>
      <div style={{ padding: "12px 16px" }}>
        {mode === "GRID" ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16 }}>
            {visibleKeys.map(key => {
              const days = MOCK_CALENDAR[key];
              const label = new Date(key + "-01").toLocaleDateString("en-US", { month: "short", year: "numeric" });
              const parts = key.split("-");
              const yr = parseInt(parts[0]);
              const mo = parseInt(parts[1]) - 1;
              const startDow = new Date(yr, mo, 1).getDay();
              const cells: (DayData | null)[] = [];
              for (let i = 0; i < startDow; i++) cells.push(null);
              for (const d of days) cells.push(d);
              return (
                <div key={key} style={{ width: "100%", overflow: "hidden", display: "flex", flexDirection: "column", alignItems: "center" }}>
                  <div style={{ fontSize: 9, color: "var(--t2)", fontWeight: 700, marginBottom: 6, textAlign: "center" }}>{label}</div>
                  {/* Day-of-week headers — exactly 7 letters */}
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 20px)", gap: 3, marginBottom: 3 }}>
                    <div style={{ width: 20, fontSize: 8, color: "var(--t3)", textAlign: "center", lineHeight: "14px" }}>S</div>
                    <div style={{ width: 20, fontSize: 8, color: "var(--t3)", textAlign: "center", lineHeight: "14px" }}>M</div>
                    <div style={{ width: 20, fontSize: 8, color: "var(--t3)", textAlign: "center", lineHeight: "14px" }}>T</div>
                    <div style={{ width: 20, fontSize: 8, color: "var(--t3)", textAlign: "center", lineHeight: "14px" }}>W</div>
                    <div style={{ width: 20, fontSize: 8, color: "var(--t3)", textAlign: "center", lineHeight: "14px" }}>T</div>
                    <div style={{ width: 20, fontSize: 8, color: "var(--t3)", textAlign: "center", lineHeight: "14px" }}>F</div>
                    <div style={{ width: 20, fontSize: 8, color: "var(--t3)", textAlign: "center", lineHeight: "14px" }}>S</div>
                  </div>
                  {/* Calendar grid */}
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 20px)", gap: 3 }}>
                    {cells.map((cell, i) => (
                      <div
                        key={i}
                        title={cell ? (cell.future ? `${cell.date}: future` : `${cell.date}: ${cell.ret !== null ? (cell.ret >= 0 ? "+" : "") + cell.ret.toFixed(2) + "%" : "weekend"}`) : ""}
                        style={{
                          width: 20, height: 20, borderRadius: 2,
                          background: cell ? (cell.future ? "var(--bg3)" : getReturnColor(cell.ret)) : "transparent",
                          display: "flex", alignItems: "center", justifyContent: "center",
                          fontSize: 7, color: cell ? (cell.future ? "var(--t3)" : getReturnTextColor(cell.ret)) : "transparent",
                          opacity: cell?.future ? 0.4 : 0.6,
                        }}
                      >
                        {cell ? cell.day : ""}
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {visibleKeys.map(key => {
              const days = MOCK_CALENDAR[key].filter(d => d.ret !== null);
              const label = new Date(key + "-01").toLocaleDateString("en-US", { month: "short", year: "numeric" });
              const cumReturns = days.reduce<number[]>((acc, d) => { acc.push((acc.length > 0 ? acc[acc.length - 1] : 0) + (d.ret ?? 0)); return acc; }, []);
              const peak = Math.max(...cumReturns);
              const W = 800, H = 60;
              const minV = Math.min(0, ...cumReturns); const maxV = Math.max(0, ...cumReturns) || 1;
              const range = maxV - minV || 1;
              const zeroY = H - ((0 - minV) / range) * H;
              const xs = cumReturns.map((_, i) => (i / Math.max(1, cumReturns.length - 1)) * W);
              const ys = cumReturns.map(v => H - ((v - minV) / range) * H);
              const linePath = xs.map((x, i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${ys[i].toFixed(1)}`).join(" ");
              const abovePath = `${linePath} L${W},${zeroY.toFixed(1)} L0,${zeroY.toFixed(1)} Z`;
              const belowPath = abovePath;
              const finalReturn = cumReturns[cumReturns.length - 1] ?? 0;
              const lineColor = finalReturn >= 0 ? "var(--green)" : "var(--red)";
              return (
                <div key={key}>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
                    <span style={{ fontSize: 9, color: "var(--t2)", letterSpacing: "0.08em" }}>{label}</span>
                    <span style={{ fontSize: 9, color: "var(--green)" }}>+{peak.toFixed(1)}% peak</span>
                  </div>
                  <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: "block" }}>
                    <defs>
                      <clipPath id={`above-${key}`}><rect x="0" y="0" width={W} height={zeroY} /></clipPath>
                      <clipPath id={`below-${key}`}><rect x="0" y={zeroY} width={W} height={H - zeroY} /></clipPath>
                    </defs>
                    <line x1="0" y1={zeroY} x2={W} y2={zeroY} stroke="var(--t2)" strokeWidth={0.5} strokeDasharray="4 3" />
                    <path d={abovePath} fill="var(--green)" opacity={0.08} clipPath={`url(#above-${key})`} />
                    <path d={belowPath} fill="var(--red)" opacity={0.1} clipPath={`url(#below-${key})`} />
                    <path d={linePath} fill="none" stroke={lineColor} strokeWidth={1.2} />
                  </svg>
                </div>
              );
            })}
          </div>
        )}
        {!expanded && monthKeys.length > 3 && (
          <button onClick={() => setExpanded(true)} style={{
            display: "block", width: "100%", marginTop: 12, paddingTop: 10, padding: "10px 0 0",
            borderTop: "0.5px solid var(--line)", background: "transparent", border: "none",
            borderTopWidth: "0.5px", borderTopStyle: "solid", borderTopColor: "var(--line)",
            color: "var(--t2)", fontSize: 9, fontWeight: 700, letterSpacing: "0.08em",
            cursor: "pointer", textTransform: "uppercase", textAlign: "center",
          }}>
            {"\u25BC"} VIEW MORE
          </button>
        )}
        {expanded && (
          <button onClick={() => setExpanded(false)} style={{
            display: "block", width: "100%", marginTop: 12, paddingTop: 10, padding: "10px 0 0",
            background: "transparent", border: "none",
            borderTopWidth: "0.5px", borderTopStyle: "solid", borderTopColor: "var(--line)",
            color: "var(--t2)", fontSize: 9, fontWeight: 700, letterSpacing: "0.08em",
            cursor: "pointer", textTransform: "uppercase", textAlign: "center",
          }}>
            {"\u25B2"} SHOW LESS
          </button>
        )}
      </div>
    </div>
  );
}

// ─── Page (read-only, no wizard) ─────────────────────────────────────────────

export default function MarketplaceDetailPage() {
  const params = useParams();
  const router = useRouter();
  const id = typeof params.id === "string" ? params.id : "alpha-mid";
  const cat = STRATEGY_CATALOG[id as StrategyType];
  const { instances, addInstance, updateInstance, removeInstance, refresh } = useTrader();

  if (!cat) {
    return <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%", color: "var(--t2)", fontSize: 10 }}>Strategy not found.</div>;
  }

  const hasInstance = instances.some(i => i.strategyType === id);
  const hasLiveInstance = instances.some(i => i.strategyType === id && i.status === "live");
  const unlinkedInstance = instances.find(i => i.strategyType === id && i.status === "unlinked");
  const hasLiveOrPausedInstance = instances.some(i => i.strategyType === id && (i.status === "live" || i.status === "paused"));
  const liveOrPausedInstance = instances.find(i => i.strategyType === id && (i.status === "live" || i.status === "paused"));
  const [isStuck, setIsStuck] = useState(false);
  const [wizardOpen, setWizardOpen] = useState(!!unlinkedInstance);
  const [wizardInstanceId, setWizardInstanceId] = useState<string | null>(unlinkedInstance?.id ?? null);
  const [confirmRemove, setConfirmRemove] = useState(false);
  const [addedHover, setAddedHover] = useState(false);
  const sentinelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => setIsStuck(!entry.isIntersecting),
      { threshold: 0 }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  function handleAdd() {
    const newId = `${id}-${Date.now()}`;
    const newInst: StrategyInstance = {
      id: newId,
      strategyType: id as StrategyType,
      strategyName: cat.name,
      exchangeId: null,
      exchangeName: null,
      risk: cat.risk,
      status: "unlinked",
      alerts: false,
      allocation: null,
      equity: 0,
      dailyPnl: 0,
      positions: [],
    };
    addInstance(newInst);
    setWizardInstanceId(newId);
    setWizardOpen(true);
  }

  async function handleWizardActivate(exchangeId: string, exchangeName: string, allocation: number) {
    if (!wizardInstanceId) return;

    // Create allocation via API
    const stratVersionId = cat.strategyVersionId;
    let realAllocationId = wizardInstanceId;

    if (stratVersionId) {
      try {
        const result = await allocatorApi.createAllocation({
          strategy_version_id: stratVersionId,
          connection_id: exchangeId,
          capital_usd: allocation,
        });
        realAllocationId = result.allocation_id;
      } catch (err) {
        console.error("Failed to create allocation:", err);
        // Fall through — update local state anyway so UI isn't stuck
      }
    }

    // Remove the temporary unlinked instance
    if (wizardInstanceId !== realAllocationId) {
      removeInstance(wizardInstanceId);
    }

    // Update or add the real instance
    updateInstance(realAllocationId, {
      id: realAllocationId,
      status: "live",
      exchangeId,
      exchangeName,
      allocation,
      equity: allocation,
      strategyVersionId: stratVersionId,
      connectionId: exchangeId,
    });

    setWizardOpen(false);
    setWizardInstanceId(null);

    // Refresh data from backend
    refresh();

    setTimeout(() => {
      const fadeOut = (window as any).__celebrationFadeOut;
      if (fadeOut) { fadeOut(); delete (window as any).__celebrationFadeOut; }
      setTimeout(() => router.push(`/trader/traders/${realAllocationId}`), 200);
    }, 1800);
  }

  function handleWizardCancel() {
    if (wizardInstanceId) {
      removeInstance(wizardInstanceId);
    }
    setWizardOpen(false);
    setWizardInstanceId(null);
  }

  function handleRemove() {
    const inst = instances.find(i => i.strategyType === id && i.status === "unlinked");
    if (inst) removeInstance(inst.id);
    setWizardOpen(false);
    setWizardInstanceId(null);
    setConfirmRemove(false);
    setAddedHover(false);
  }

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>

        {/* Sentinel for IntersectionObserver */}
        <div ref={sentinelRef} style={{ height: 0 }} />

        {/* Sticky header */}
        <div style={{
          position: "sticky", top: 0, zIndex: 40,
          background: "var(--bg0)",
          padding: "12px 0",
          borderBottom: isStuck ? "0.5px solid var(--line)" : "0.5px solid transparent",
          transition: "border-color 0.2s ease",
          marginBottom: 12,
        }}>
          {/* Breadcrumb */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 10, marginBottom: 8 }}>
            <span
              onClick={() => router.push("/trader/strategies")}
              style={{ color: "var(--t2)", cursor: "pointer", transition: "color 0.15s ease", textDecoration: "none" }}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t0)"; e.currentTarget.style.textDecoration = "underline"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t2)"; e.currentTarget.style.textDecoration = "none"; }}
            >Strategies</span>
            <span style={{ color: "var(--t3)" }}>{"\u203A"}</span>
            <span style={{ color: "var(--t3)" }}>{cat.name}</span>
          </div>

          {/* Header */}
          <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <h1 style={{ fontSize: 24, fontWeight: 700, color: "var(--t0)", margin: 0 }}>{cat.name}</h1>
              <span style={{
                fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                color: RISK_COLOR[cat.risk], background: RISK_DIM[cat.risk], borderRadius: 3, padding: "3px 8px",
              }}>{cat.risk}</span>
              {hasLiveInstance && (
                <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                  <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--green)" }} />
                  <span style={{ fontSize: 9, color: "var(--green)", fontWeight: 700, letterSpacing: "0.06em", whiteSpace: "nowrap" }}>Active</span>
                </div>
              )}
            </div>
            {hasLiveOrPausedInstance ? (
              /* Live/paused — VIEW TRADER → */
              <button
                onClick={() => liveOrPausedInstance && router.push(`/trader/traders/${liveOrPausedInstance.id}`)}
                onMouseEnter={() => setAddedHover(true)}
                onMouseLeave={() => setAddedHover(false)}
                style={{
                  display: "inline-flex", alignItems: "center",
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                  whiteSpace: "nowrap",
                  background: "transparent",
                  border: addedHover ? "0.5px solid var(--green-mid)" : "0.5px solid var(--line)",
                  color: addedHover ? "var(--green)" : "var(--t1)",
                  borderRadius: 3, padding: "9px 18px",
                  cursor: "pointer",
                  transition: "all 0.15s ease",
                }}
              >
                VIEW TRADER &rarr;
              </button>
            ) : (hasInstance || wizardOpen) ? (
              /* Unlinked — removable ADDED button */
              <div style={{ position: "relative", display: "flex", flexDirection: "column", alignItems: "flex-end" }}>
                <button
                  onClick={() => setConfirmRemove(true)}
                  onMouseEnter={() => setAddedHover(true)}
                  onMouseLeave={() => { setAddedHover(false); }}
                  style={{
                    display: "inline-flex", alignItems: "center",
                    fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                    whiteSpace: "nowrap",
                    background: "transparent",
                    border: addedHover ? "0.5px solid #ff4d4d40" : "0.5px solid var(--line)",
                    color: addedHover ? "var(--red)" : "var(--t3)",
                    borderRadius: 3, padding: "9px 18px",
                    cursor: "pointer",
                    transition: "all 0.15s ease",
                  }}
                >
                  {addedHover ? (
                    "REMOVE"
                  ) : (
                    <>
                      <svg width="10" height="10" viewBox="0 0 10 10" fill="none" style={{ marginRight: 5 }}><polyline points="1.5,5 4,7.5 8.5,2.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
                      ADDED
                    </>
                  )}
                </button>
                {confirmRemove && (
                  <div style={{ marginTop: 6, fontSize: 9, display: "flex", alignItems: "center", gap: 6 }}>
                    <span style={{ color: "var(--t2)" }}>Remove this strategy?</span>
                    <button onClick={handleRemove} style={{ background: "transparent", border: "none", color: "var(--red)", fontSize: 9, cursor: "pointer", padding: 0 }}>Yes, remove</button>
                    <span style={{ color: "var(--t3)" }}>&middot;</span>
                    <button onClick={() => { setConfirmRemove(false); setAddedHover(false); }} style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0 }}>Cancel</button>
                  </div>
                )}
              </div>
            ) : (
              /* No instance — SYNC CAPITAL */
              <button onClick={handleAdd} style={{
                background: "var(--green)", color: "var(--bg0)",
                border: "none", borderRadius: 3,
                padding: "9px 18px", fontSize: 9, fontWeight: 700,
                letterSpacing: "0.12em", textTransform: "uppercase",
                cursor: "pointer", whiteSpace: "nowrap",
              }}>
                SYNC CAPITAL
              </button>
            )}
          </div>
        </div>

        {/* Capacity + social proof */}
        {(() => {
          const cap = CAPACITY_DATA[id] ?? { allocators: 0, deployed: 0, capacity: 1 };
          const remaining = cap.capacity - cap.deployed;
          const remainingPct = remaining / cap.capacity;
          const color = remainingPct > 0.5 ? "var(--green)" : remainingPct >= 0.2 ? "var(--amber)" : "var(--red)";
          const isLow = remainingPct < 0.2;
          const fmtFull = (n: number) => "$" + n.toLocaleString("en-US");
          return (
            <div style={{ marginBottom: 20 }}>
              <div style={{ display: "flex", alignItems: "center", fontSize: 9 }}>
                <span style={{ color: "var(--t2)" }}>{cap.allocators} allocators</span>
                <span style={{ color: "var(--t3)", margin: "0 5px" }}>&middot;</span>
                <span style={{ color: "var(--t2)" }}>{fmtFull(cap.deployed)} deployed</span>
                <span style={{ color: "var(--t3)", margin: "0 5px" }}>&middot;</span>
                <span style={{ color, fontWeight: 700 }}>{isLow ? `Only ${fmtFull(remaining)} left` : `${fmtFull(remaining)} max limit`}</span>
              </div>
              <div style={{ height: 4, background: "var(--bg3)", borderRadius: 2, marginTop: 6, overflow: "hidden" }}>
                <div style={{ height: "100%", width: `${(cap.deployed / cap.capacity) * 100}%`, background: "#6a6a6a", borderRadius: 2 }} />
              </div>
            </div>
          );
        })()}

        {/* Stats strip — 6 cards */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 8, marginBottom: 20 }}>
          <MetricCard label="YTD RETURN" value={`+${fmt(cat.ytd, 1)}%`} color="var(--green)" />
          <MetricCard label="SHARPE" value={fmt(cat.sharpe, 2)} />
          <MetricCard label="MAX DD" value={`-${fmt(cat.maxDd, 1)}%`} color="var(--red)" />
          <MetricCard label="WIN RATE" value={`${fmt(cat.winRate, 0)}%`} />
          <MetricCard label="CAGR" value={`${fmt(cat.cagr, 1)}%`} color="var(--green)" />
          <MetricCard label="PROFIT FACTOR" value={`${fmt(cat.profitFactor, 2)}x`} />
        </div>

        {/* Inline wizard panel — slides down when open */}
        <div style={{
          maxHeight: wizardOpen ? 600 : 0,
          overflow: "hidden",
          transition: "max-height 0.3s ease",
        }}>
          <div style={{
            background: "var(--bg1)", border: "1px solid var(--line)",
            borderRadius: 8, padding: "20px 24px", marginBottom: 24,
          }}>
            <SetupWizard
              strategyName={cat.name}
              onActivate={handleWizardActivate}
              onCancel={handleWizardCancel}
            />
          </div>
        </div>

        {/* Content below wizard — dims when wizard is open */}
        <div style={{
          opacity: wizardOpen ? 0.25 : 1,
          pointerEvents: wizardOpen ? "none" : "auto",
          transition: "opacity 0.2s ease",
        }}>
          {/* Equity curve */}
          <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, overflow: "hidden", marginBottom: 20 }}>
            <div style={{ padding: "10px 16px", borderBottom: "1px solid var(--line)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>EQUITY CURVE</span>
              <span style={{ fontSize: 10 }}>
                <span style={{ color: "var(--green)" }}>+{fmt(cat.ytd, 1)}%</span>
                <span style={{ color: "var(--t2)" }}> YTD &middot; </span>
                <span style={{ color: "var(--t2)" }}>Sharpe </span><span style={{ color: "var(--green)" }}>{fmt(cat.sharpe, 2)}</span>
                <span style={{ color: "var(--t2)" }}> &middot; Max DD </span><span style={{ color: "var(--red)" }}>-{fmt(cat.maxDd, 1)}%</span>
                <span style={{ color: "var(--t2)" }}> &middot; Vol </span><span style={{ color: "var(--t1)" }}>{fmt(cat.vol, 1)}%</span>
              </span>
            </div>
            <div style={{ padding: "8px 0", height: 250 }}><EquityCurveSvg data={GHOST_CURVE} /></div>
          </div>

          {/* Calendar returns heatmap */}
          <CalendarHeatmap />

          {/* HOW IT WORKS */}
          <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "16px 18px", marginBottom: 20 }}>
            <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 10 }}>HOW IT WORKS</div>
            <p style={{ fontSize: 11, color: "var(--t2)", margin: 0, lineHeight: 1.8 }}>{cat.description}</p>
          </div>

          {/* Second stats row */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 10 }}>
            <MetricCard label="SIMPLE RETURN" value={`+${fmt(cat.simpleReturn, 1)}%`} color="var(--green)" />
            <MetricCard label="COMPOUNDED RETURN" value={`+${fmt(cat.compoundedReturn, 1)}%`} color="var(--green)" />
            <MetricCard label="AVG WIN/LOSS" value={`${fmt(cat.avgWinLoss, 2)}x`} />
            <MetricCard label="ACTIVE DAYS" value={`${cat.activeDays}`} />
          </div>
        </div>
      </div>
    </div>
  );
}

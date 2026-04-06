"use client";

import { useState, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import { useTrader, STRATEGY_CATALOG, StrategyType, fmt, RISK_COLOR, RISK_DIM } from "../../context";
import PerformanceChart from "../../performance-chart";
import SetupWizard from "../../components/SetupWizard";

// ─── Toggle ──────────────────────────────────────────────────────────────────

function Toggle({ on, onColor, offColor, onToggle, label }: {
  on: boolean; onColor: string; offColor: string; onToggle: () => void; label: string;
}) {
  return (
    <button onClick={onToggle} style={{
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

// ─── Metric card ─────────────────────────────────────────────────────────────

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 16px" }}>
      <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: color ?? "var(--t0)" }}>{value}</div>
    </div>
  );
}

// ─── Mode A: Unlinked setup wizard ───────────────────────────────────────────

function UnlinkedMode({ instanceId }: { instanceId: string }) {
  const router = useRouter();
  const { instances, updateInstance } = useTrader();
  const inst = instances.find(i => i.id === instanceId)!;
  const cat = STRATEGY_CATALOG[inst.strategyType];

  function handleActivate(exchangeId: string, exchangeName: string, allocation: number) {
    updateInstance(inst.id, {
      status: "live",
      exchangeId,
      exchangeName,
      allocation,
      equity: Math.round(allocation * 1.034),
    });
    setTimeout(() => {
      const fadeOut = (window as any).__celebrationFadeOut;
      if (fadeOut) { fadeOut(); delete (window as any).__celebrationFadeOut; }
    }, 1800);
  }

  function handleCancel() {
    router.push("/trader/traders");
  }

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>

        {/* Breadcrumb */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 20, fontSize: 10, color: "var(--t2)" }}>
          <span onClick={() => router.push("/trader/strategies")} style={{ cursor: "pointer" }}
            onMouseEnter={e => (e.currentTarget.style.color = "var(--t1)")} onMouseLeave={e => (e.currentTarget.style.color = "var(--t2)")}>Traders</span>
          <span>{"\u203A"}</span>
          <span style={{ color: "var(--t1)" }}>{inst.strategyName}</span>
        </div>

        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 24 }}>
          <h1 style={{ fontSize: 24, fontWeight: 700, color: "var(--t0)", margin: 0 }}>{inst.strategyName}</h1>
          <span style={{
            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
            color: RISK_COLOR[inst.risk], background: RISK_DIM[inst.risk], borderRadius: 3, padding: "3px 8px",
          }}>{inst.risk}</span>
        </div>

        {/* Reference stats */}
        <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 8 }}>
          STRATEGY REFERENCE STATS
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 10, marginBottom: 24 }}>
          <MetricCard label="YTD RETURN" value={`+${fmt(cat.ytd, 1)}%`} color="var(--green)" />
          <MetricCard label="SHARPE" value={fmt(cat.sharpe, 2)} />
          <MetricCard label="MAX DD" value={`-${fmt(cat.maxDd, 1)}%`} color="var(--red)" />
          <MetricCard label="WIN RATE" value={`${fmt(cat.winRate, 0)}%`} />
        </div>

        {/* Setup panel */}
        <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 8, padding: 20 }}>
          <SetupWizard
            strategyName={inst.strategyName}
            onActivate={handleActivate}
            onCancel={handleCancel}
          />
        </div>
      </div>
    </div>
  );
}

// ─── Mode B: Live/Paused dashboard ───────────────────────────────────────────

function LiveMode({ instanceId }: { instanceId: string }) {
  const router = useRouter();
  const { exchanges, instances, updateInstance, removeInstance } = useTrader();
  const inst = instances.find(i => i.id === instanceId)!;
  const totalBalance = exchanges.reduce((s, e) => s + e.balance, 0);
  const editMaxAllocation = totalBalance - instances.filter(i => i.id !== instanceId).reduce((s, i) => s + (i.allocation ?? 0), 0);

  const [confirmPause, setConfirmPause] = useState(false);
  const [exchangeLost, setExchangeLost] = useState(false);
  const [confirmClose, setConfirmClose] = useState(false);
  const [confirmRemoveTrader, setConfirmRemoveTrader] = useState(false);
  const [positionsOpen, setPositionsOpen] = useState(inst.positions.length > 0);
  const [settingsOpen, setSettingsOpen] = useState(true);
  const [editing, setEditing] = useState(false);
  const [editAllocation, setEditAllocation] = useState("");
  const [editAlerts, setEditAlerts] = useState(false);
  const positionsContentRef = useRef<HTMLDivElement>(null);
  const settingsContentRef = useRef<HTMLDivElement>(null);

  const isLive = inst.status === "live";

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>

        {/* Breadcrumb */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 20, fontSize: 10, color: "var(--t2)" }}>
          <span onClick={() => router.push("/trader/strategies")} style={{ cursor: "pointer" }}
            onMouseEnter={e => (e.currentTarget.style.color = "var(--t1)")} onMouseLeave={e => (e.currentTarget.style.color = "var(--t2)")}>Traders</span>
          <span>{"\u203A"}</span>
          <span style={{ color: "var(--t1)" }}>{inst.strategyName} &middot; {inst.exchangeName}</span>
        </div>

        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 24 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <h1 style={{ fontSize: 24, fontWeight: 700, color: "var(--t0)", margin: 0 }}>{inst.strategyName}</h1>
            <span style={{ fontSize: 9, padding: "2px 6px", borderRadius: 2, background: "var(--bg3)", color: "var(--t1)", border: "1px solid var(--line)" }}>{inst.exchangeName}</span>
            {isLive && (
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ position: "relative", display: "inline-flex", width: 6, height: 6 }}>
                  <span style={{ position: "absolute", inset: 0, borderRadius: "50%", background: "var(--green)", opacity: 0.3, animation: "pulse-dot 1.2s ease-in-out infinite" }} />
                  <span style={{ position: "absolute", inset: 0, borderRadius: "50%", background: "var(--green)" }} />
                </span>
                <span style={{ fontSize: 10, color: "var(--t2)" }}>live &middot; synced 8s ago</span>
              </div>
            )}
          </div>
          <div style={{ display: "flex", gap: 14 }}>
            <Toggle on={isLive} onColor="var(--green)" offColor="var(--t2)" onToggle={() => {
              if (isLive) { setConfirmPause(true); } else {
                const exExists = exchanges.some(e => e.name === inst.exchangeName);
                if (exExists) { updateInstance(inst.id, { status: "live" }); setExchangeLost(false); }
                else { setExchangeLost(true); }
              }
            }} label={isLive ? "Live" : "Paused"} />
          </div>
        </div>

        {/* Pause confirmation */}
        {confirmPause && (
          <div style={{
            background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 6,
            padding: "10px 12px", marginTop: -16, marginBottom: 24,
          }}>
            <div style={{ fontSize: 10, color: "var(--t1)", marginBottom: 2 }}>Pause this trader?</div>
            <div style={{ fontSize: 9, color: "var(--t3)", lineHeight: 1.6, marginBottom: 8 }}>Existing positions will be held. No new signals will be processed.</div>
            <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 9 }}>
              <button onClick={() => { updateInstance(inst.id, { status: "paused" }); setConfirmPause(false); }} style={{ background: "transparent", border: "none", color: "var(--t1)", fontSize: 9, cursor: "pointer", padding: 0 }}>Yes, pause</button>
              <span style={{ color: "var(--t3)" }}>&middot;</span>
              <button onClick={() => setConfirmPause(false)} style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0 }}>Cancel</button>
            </div>
          </div>
        )}

        {/* Exchange lost error */}
        {exchangeLost && (
          <div style={{
            background: "var(--bg2)", border: "0.5px solid #ff4d4d30", borderRadius: 6,
            padding: "10px 12px", marginTop: -16, marginBottom: 24,
          }}>
            <div style={{ fontSize: 10, color: "var(--t2)", lineHeight: 1.6, marginBottom: 8 }}>
              Exchange connection lost. The API keys for {inst.exchangeName} were removed. Re-link this exchange in Settings to resume.
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span onClick={() => router.push("/trader/settings")} style={{ fontSize: 10, color: "var(--green)", cursor: "pointer" }}>Go to Settings &rarr;</span>
              <button onClick={() => setExchangeLost(false)} style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0 }}>Dismiss</button>
            </div>
          </div>
        )}

        {/* Metric cards */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 10, marginBottom: 20 }}>
          <MetricCard label="TOTAL ACCOUNT EQUITY" value={`$${fmt(exchanges.find(e => e.name === inst.exchangeName)?.balance ?? 0, 0)}`} />
          <MetricCard label="ALLOCATION" value={`$${fmt(inst.allocation ?? 0, 0)}`} />
          <MetricCard label="OPEN POSITIONS" value={String(inst.positions.length)} />
          <MetricCard label="DAILY P&L" value={!inst.dailyPnl ? "\u2014" : `${inst.dailyPnl >= 0 ? "+" : ""}$${fmt(inst.dailyPnl)}`} color={!inst.dailyPnl ? "var(--t2)" : inst.dailyPnl > 0 ? "var(--green)" : "var(--red)"} />
        </div>

        {/* Performance chart */}
        <PerformanceChart allocation={inst.allocation ?? 0} ytdReturn={STRATEGY_CATALOG[inst.strategyType]?.ytd ?? 0} />

        {/* Positions table — collapsible */}
        <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6, overflow: "hidden", marginBottom: 20 }}>
          <div
            onClick={() => setPositionsOpen(v => !v)}
            style={{
              padding: "12px 18px",
              borderBottom: positionsOpen ? "1px solid var(--line)" : "none",
              display: "flex", alignItems: "center", justifyContent: "space-between",
              cursor: "pointer",
              background: positionsOpen ? "transparent" : "var(--bg2)",
            }}
          >
            <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>OPEN POSITIONS</span>
            <span style={{ fontSize: 10, color: "var(--t2)", transition: "transform 0.2s ease", display: "inline-block", transform: positionsOpen ? "rotate(0deg)" : "rotate(-90deg)" }}>{"\u25BC"}</span>
          </div>
          <div ref={positionsContentRef} style={{
            overflow: "hidden",
            height: positionsOpen ? "auto" : 0,
            transition: "height 0.2s ease",
          }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--line)" }}>
                  {["SYMBOL", "SIDE", "SIZE", "ENTRY", "MARK", "P&L"].map(h => (
                    <th key={h} style={{ padding: "8px 16px", textAlign: h === "P&L" ? "right" : "left", color: "var(--t3)", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {inst.positions.map((p, i) => (
                  <tr key={`${p.symbol}-${i}`} style={{ borderBottom: i < inst.positions.length - 1 ? "1px solid var(--bg3)" : "none" }}>
                    <td style={{ padding: "9px 16px", color: "var(--t1)" }}>{p.symbol}</td>
                    <td style={{ padding: "9px 16px" }}>
                      <span style={{ fontSize: 9, padding: "2px 6px", borderRadius: 2, background: p.side === "LONG" ? "var(--green-dim)" : "var(--red-dim)", color: p.side === "LONG" ? "var(--green)" : "var(--red)" }}>{p.side}</span>
                    </td>
                    <td style={{ padding: "9px 16px", color: "var(--t1)" }}>{p.size}</td>
                    <td style={{ padding: "9px 16px", color: "var(--t1)" }}>${fmt(p.entry, 0)}</td>
                    <td style={{ padding: "9px 16px", color: "var(--t1)" }}>${fmt(p.mark, 0)}</td>
                    <td style={{ padding: "9px 16px", textAlign: "right", color: p.pnl >= 0 ? "var(--green)" : "var(--red)" }}>
                      {p.pnl >= 0 ? "+" : ""}${fmt(p.pnl)}
                      <span style={{ fontSize: 9, opacity: 0.7, marginLeft: 4 }}>({p.pnlPct >= 0 ? "+" : ""}{fmt(p.pnlPct)}%)</span>
                    </td>
                  </tr>
                ))}
                {inst.positions.length === 0 && (
                  <tr><td colSpan={6} style={{ padding: "20px 16px", textAlign: "center", color: "var(--t2)", fontSize: 10 }}>No open positions</td></tr>
                )}
              </tbody>
            </table>
            {/* Close all positions */}
            <div style={{ borderTop: "0.5px solid var(--line)", padding: "10px 16px", display: "flex", justifyContent: "flex-end" }}>
              {!confirmClose ? (
                <button
                  onClick={() => setConfirmClose(true)}
                  onMouseEnter={e => { e.currentTarget.style.color = "var(--red)"; }}
                  onMouseLeave={e => { e.currentTarget.style.color = "var(--t3)"; }}
                  style={{
                    background: "transparent", color: "var(--t3)",
                    border: "none", padding: 0,
                    fontSize: 9, cursor: "pointer",
                    transition: "color 0.15s ease",
                  }}
                >CLOSE ALL POSITIONS</button>
              ) : (
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: 9, color: "var(--red)" }}>Close all {inst.positions.length} positions?</span>
                  <button onClick={() => { updateInstance(inst.id, { positions: [], status: "paused" }); setConfirmClose(false); }}
                    style={{ background: "var(--red)", color: "var(--bg0)", border: "none", borderRadius: 3, padding: "5px 10px", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>CONFIRM</button>
                  <button onClick={() => setConfirmClose(false)}
                    style={{ background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, padding: "5px 10px", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>CANCEL</button>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Settings — collapsible with read-only / edit mode */}
        <div style={{ background: "var(--bg1)", border: "1px solid var(--line)", borderRadius: 6, overflow: "hidden" }}>
          <div
            style={{
              padding: "12px 18px",
              borderBottom: settingsOpen ? "1px solid var(--line)" : "none",
              display: "flex", alignItems: "center", justifyContent: "space-between",
              cursor: "pointer",
              background: settingsOpen ? "transparent" : "var(--bg2)",
            }}
          >
            <span onClick={() => setSettingsOpen(v => !v)} style={{ flex: 1, fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>SETTINGS</span>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              {settingsOpen && !editing && (
                <button onClick={e => { e.stopPropagation(); setEditing(true); setEditAllocation(String(inst.allocation ?? 0)); setEditAlerts(inst.alerts); }}
                  style={{ background: "transparent", border: "none", color: "var(--t2)", fontSize: 9, cursor: "pointer", padding: 0 }}>Edit</button>
              )}
              {settingsOpen && editing && (
                <>
                  {(() => {
                    const val = parseInt(editAllocation) || 0;
                    const valid = val > 0 && val <= editMaxAllocation;
                    return <button onClick={e => { e.stopPropagation(); if (valid) { updateInstance(inst.id, { allocation: val, alerts: editAlerts }); setEditing(false); } else { updateInstance(inst.id, { alerts: editAlerts }); setEditing(false); } }}
                      style={{ background: "transparent", border: "none", color: valid ? "var(--green)" : "var(--t2)", fontSize: 9, cursor: valid ? "pointer" : "not-allowed", padding: 0, opacity: valid ? 1 : 0.4 }}>Save</button>;
                  })()}
                  <button onClick={e => { e.stopPropagation(); setEditing(false); }}
                    style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0 }}>Cancel</button>
                </>
              )}
              <span onClick={() => setSettingsOpen(v => !v)} style={{ fontSize: 10, color: "var(--t2)", transition: "transform 0.2s ease", display: "inline-block", transform: settingsOpen ? "rotate(0deg)" : "rotate(-90deg)" }}>{"\u25BC"}</span>
            </div>
          </div>
          <div ref={settingsContentRef} style={{
            overflow: "hidden",
            height: settingsOpen ? "auto" : 0,
            transition: "height 0.2s ease",
          }}>
            <div style={{ padding: "16px 18px" }}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 14, marginBottom: 16 }}>
                {/* USD Allocation */}
                <div>
                  <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>USD ALLOCATION</div>
                  {editing ? (
                    <>
                    <input type="text"
                      value={editAllocation}
                      onChange={e => {
                        const raw = e.target.value.replace(/[^0-9]/g, "");
                        const num = parseInt(raw) || 0;
                        if (num > editMaxAllocation) return;
                        setEditAllocation(raw);
                      }}
                      autoFocus
                      style={{ width: "100%", background: "var(--bg3)", border: "1px solid var(--line)", borderRadius: 3, padding: "8px 10px", color: "var(--t1)", fontSize: 10, outline: "none" }}
                      onFocus={e => (e.target.style.borderColor = "var(--green)")}
                      onBlur={e => (e.target.style.borderColor = "var(--line)")} />
                    <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 3 }}>Max: ${editMaxAllocation.toLocaleString("en-US")}</div>
                    </>
                  ) : (
                    <div style={{ fontSize: 10, color: "var(--t1)", padding: "8px 0" }}>${(inst.allocation ?? 0).toLocaleString("en-US")}</div>
                  )}
                </div>
                {/* Exchange — always read-only */}
                <div>
                  <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>EXCHANGE</div>
                  <div style={{ fontSize: 10, color: "var(--t1)", padding: "8px 0" }}>{inst.exchangeName}</div>
                </div>
                {/* Strategy — always read-only */}
                <div>
                  <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>STRATEGY</div>
                  <div style={{ fontSize: 10, color: "var(--t1)", padding: "8px 0" }}>{inst.strategyName}</div>
                </div>
                {/* Alerts */}
                <div>
                  <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>ALERTS</div>
                  {editing ? (
                    <div style={{ padding: "6px 0" }}>
                      <Toggle on={editAlerts} onColor="var(--green)" offColor="var(--t2)" onToggle={() => setEditAlerts(v => !v)} label={editAlerts ? "On" : "Off"} />
                    </div>
                  ) : (
                    <div style={{ fontSize: 10, color: "var(--t1)", padding: "8px 0" }}>{inst.alerts ? "On" : "Off"}</div>
                  )}
                </div>
              </div>

              {/* Remove trader */}
              <div style={{ borderTop: "0.5px solid var(--line)", paddingTop: 12, display: "flex", justifyContent: "flex-end" }}>
                {!confirmRemoveTrader ? (
                  <button
                    onClick={() => setConfirmRemoveTrader(true)}
                    onMouseEnter={e => { e.currentTarget.style.color = "var(--red)"; }}
                    onMouseLeave={e => { e.currentTarget.style.color = "var(--t3)"; }}
                    style={{
                      background: "transparent", color: "var(--t3)",
                      border: "none", padding: 0,
                      fontSize: 9, cursor: "pointer",
                      transition: "color 0.15s ease",
                    }}
                  >REMOVE TRADER</button>
                ) : (
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ fontSize: 9, color: "var(--red)" }}>Remove this trader?</span>
                    <button onClick={() => { removeInstance(inst.id); router.push("/trader/traders"); }}
                      style={{ background: "var(--red)", color: "var(--bg0)", border: "none", borderRadius: 3, padding: "5px 10px", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>CONFIRM</button>
                    <button onClick={() => setConfirmRemoveTrader(false)}
                      style={{ background: "transparent", color: "var(--t2)", border: "1px solid var(--line)", borderRadius: 3, padding: "5px 10px", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer" }}>CANCEL</button>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Page router ─────────────────────────────────────────────────────────────

export default function StrategyDashboardPage() {
  const params = useParams();
  const id = typeof params.id === "string" ? params.id : "";
  const { instances } = useTrader();
  const inst = instances.find(i => i.id === id);

  if (!inst) {
    return <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%", color: "var(--t2)", fontSize: 10 }}>Strategy not found.</div>;
  }

  if (inst.status === "unlinked") {
    return <UnlinkedMode instanceId={inst.id} />;
  }

  return <LiveMode instanceId={inst.id} />;
}

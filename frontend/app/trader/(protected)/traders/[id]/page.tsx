"use client";

import { useState, useEffect, useRef } from "react";
import { useParams, useRouter } from "next/navigation";
import { useTrader, STRATEGY_CATALOG, StrategyType, Position, fmt, RISK_COLOR, RISK_DIM } from "../../../context";
import { allocatorApi, ApiPnl, ApiPosition } from "../../../api";
import PerformanceChart from "../../../performance-chart";
import SetupWizard from "../../../components/SetupWizard";

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

function MetricCard({ label, value, color, subtitle, subtitleColor }: {
  label: string;
  value: string;
  color?: string;
  subtitle?: string;
  subtitleColor?: string;
}) {
  return (
    <div style={{ background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5, padding: "14px 16px" }}>
      <div style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: color ?? "var(--t0)" }}>{value}</div>
      {subtitle && (
        <div style={{
          fontSize: 10,
          marginTop: 4,
          color: subtitleColor ?? "var(--t2)",
          fontWeight: 700,
          letterSpacing: "0.02em",
        }}>{subtitle}</div>
      )}
    </div>
  );
}

// Settings list row — label on the left, control on the right.
// Read-only rows get slightly muted value text to visually distinguish
// from interactive rows.
function SettingsRow({
  label,
  control,
  readOnly,
}: {
  label: React.ReactNode;
  control: React.ReactNode;
  readOnly?: boolean;
}) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "180px 1fr",
        alignItems: "center",
        gap: 16,
        padding: "10px 0",
      }}
    >
      <div style={{
        fontSize: 9,
        color: readOnly ? "var(--t3)" : "var(--t2)",
        letterSpacing: "0.12em",
        fontWeight: 700,
        textTransform: "uppercase",
      }}>
        {label}
      </div>
      <div>{control}</div>
    </div>
  );
}

// ─── PnL formatting helpers ──────────────────────────────────────────────────

function fmtUsdSigned(n: number): string {
  const sign = n >= 0 ? "+" : "-";
  return `${sign}$${fmt(Math.abs(n))}`;
}

function pnlColor(n: number | null | undefined): string {
  if (n === null || n === undefined) return "var(--t2)";
  if (n > 0) return "var(--green)";
  if (n < 0) return "var(--red)";
  return "var(--t0)";
}

function pnlSubtitle(pct: number | null | undefined): string | undefined {
  if (pct === null || pct === undefined) return undefined;
  const chevron = pct > 0 ? "▲" : pct < 0 ? "▼" : "●";
  return `${chevron} ${pct >= 0 ? "+" : ""}${fmt(pct, 2)}%`;
}

// ─── Mode A: Unlinked setup wizard ───────────────────────────────────────────

function UnlinkedMode({ instanceId }: { instanceId: string }) {
  const router = useRouter();
  const { instances, updateInstance, removeInstance, refresh } = useTrader();
  const inst = instances.find(i => i.id === instanceId)!;
  const cat = STRATEGY_CATALOG[inst.strategyType];

  async function handleActivate(exchangeId: string, exchangeName: string, allocation: number) {
    const stratVersionId = cat?.strategyVersionId;
    let realId = inst.id;

    if (stratVersionId) {
      try {
        const result = await allocatorApi.createAllocation({
          strategy_version_id: stratVersionId,
          connection_id: exchangeId,
          capital_usd: allocation,
        });
        realId = result.allocation_id;
      } catch (err) {
        console.error("Failed to create allocation:", err);
      }
    }

    if (realId !== inst.id) removeInstance(inst.id);
    updateInstance(realId, {
      id: realId,
      status: "live",
      exchangeId,
      exchangeName,
      allocation,
      equity: allocation,
      strategyVersionId: stratVersionId,
      connectionId: exchangeId,
    });

    refresh();

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
          <MetricCard label="YTD RETURN" value={cat ? `+${fmt(cat.ytd, 1)}%` : "\u2014"} color="var(--green)" />
          <MetricCard label="SHARPE" value={cat ? fmt(cat.sharpe, 2) : "\u2014"} />
          <MetricCard label="MAX DD" value={cat ? `-${fmt(cat.maxDd, 1)}%` : "\u2014"} color="var(--red)" />
          <MetricCard label="WIN RATE" value={cat ? `${fmt(cat.winRate, 0)}%` : "\u2014"} />
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

function mapApiPosition(p: ApiPosition): Position {
  const entry = p.entry_price;
  const mark = p.mark_price;
  const lev = p.leverage || 1;
  // Leveraged ROI: the % move a user actually sees on the exchange UI
  // for the margin committed to this position. Raw 1x move × leverage.
  // When entry is missing, fall back to 0 rather than showing NaN.
  const pnlPct = entry > 0 ? ((mark - entry) / entry) * 100 * lev : 0;
  return {
    symbol: p.symbol,
    side: (p.side || "net").toUpperCase() as "LONG" | "SHORT" | "NET",
    size: p.size,
    entry,
    mark,
    pnl: p.unrealized_pnl,
    pnlPct: Math.round(pnlPct * 100) / 100,
    notionalUsd: p.notional_usd,
    leverage: p.leverage,
  };
}

// Adaptive price formatting — same number picks readable precision based
// on magnitude, so $92.45 and $0.006122 both render without lying.
function fmtPrice(n: number): string {
  if (!isFinite(n) || n === 0) return "$0";
  const abs = Math.abs(n);
  let decimals: number;
  if (abs >= 1000) decimals = 0;
  else if (abs >= 1)   decimals = 2;
  else if (abs >= 0.01) decimals = 4;
  else if (abs >= 0.0001) decimals = 6;
  else decimals = 8;
  return `$${n.toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`;
}

// Strip the quote suffix so "BASUSDT" renders as "BAS". Every position
// in this view is USDT-quoted; the suffix adds noise without info.
function stripQuote(sym: string): string {
  if (sym.endsWith("USDT")) return sym.slice(0, -4);
  return sym;
}

function fmtNotional(n: number | undefined): string {
  if (n == null || !isFinite(n)) return "—";
  return `$${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function LiveMode({ instanceId }: { instanceId: string }) {
  const router = useRouter();
  const { exchanges, instances, updateInstance, removeInstance, refresh } = useTrader();
  const inst = instances.find(i => i.id === instanceId)!;
  const totalBalance = exchanges.reduce((s, e) => s + e.balance, 0);
  const editMaxAllocation = totalBalance - instances.filter(i => i.id !== instanceId).reduce((s, i) => s + (i.allocation ?? 0), 0);

  const [confirmPause, setConfirmPause] = useState(false);
  const [exchangeLost, setExchangeLost] = useState(false);
  const [confirmClose, setConfirmClose] = useState(false);
  const [confirmRemoveTrader, setConfirmRemoveTrader] = useState(false);
  const [livePositions, setLivePositions] = useState<Position[]>(inst.positions);
  const [positionsLoading, setPositionsLoading] = useState(false);
  const [snapshotAt, setSnapshotAt] = useState<string | null>(null);
  const [syncedAgo, setSyncedAgo] = useState<string>("never synced");
  const [pnl, setPnl] = useState<ApiPnl | null>(null);
  const [positionsOpen, setPositionsOpen] = useState(true);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editAllocation, setEditAllocation] = useState("");
  // Two-step confirmation when editing capital_usd during a live session —
  // surfaces the risk inline without a modal.
  const [capitalEditConfirm, setCapitalEditConfirm] = useState(false);
  // Compounding + Alerts both auto-save on click; the transient "✓ saved"
  // pill next to their labels fades out after 1.5s.
  const [compoundingSavedAt, setCompoundingSavedAt] = useState<number | null>(null);
  const [alertsSavedAt, setAlertsSavedAt] = useState<number | null>(null);
  useEffect(() => {
    if (compoundingSavedAt === null) return;
    const t = setTimeout(() => setCompoundingSavedAt(null), 1500);
    return () => clearTimeout(t);
  }, [compoundingSavedAt]);
  useEffect(() => {
    if (alertsSavedAt === null) return;
    const t = setTimeout(() => setAlertsSavedAt(null), 1500);
    return () => clearTimeout(t);
  }, [alertsSavedAt]);
  const positionsContentRef = useRef<HTMLDivElement>(null);
  const settingsContentRef = useRef<HTMLDivElement>(null);

  const isLive = inst.status === "live";

  // Fetch live positions from backend
  useEffect(() => {
    let cancelled = false;
    async function fetchPositions() {
      setPositionsLoading(true);
      try {
        const result = await allocatorApi.getPositions(instanceId);
        if (!cancelled) {
          setLivePositions(result.positions.map(mapApiPosition));
          setSnapshotAt(result.snapshot_at);
        }
      } catch {
        // Fall back to context positions
        if (!cancelled) setLivePositions(inst.positions);
      } finally {
        if (!cancelled) setPositionsLoading(false);
      }
    }
    fetchPositions();
    return () => { cancelled = true; };
  }, [instanceId, inst.positions]);

  // Fetch Session + Total PnL from /pnl. Re-polls every 15s so the KPI
  // cards track the account as the trader loop's bar writes + the
  // every-5-min exchange_snapshots tick advance the equity number.
  useEffect(() => {
    let cancelled = false;
    async function fetchPnl() {
      try {
        const result = await allocatorApi.getPnl(instanceId);
        if (!cancelled) setPnl(result);
      } catch {
        // Silent: keep previous value; next tick will retry.
      }
    }
    fetchPnl();
    const id = setInterval(fetchPnl, 15000);
    return () => { cancelled = true; clearInterval(id); };
  }, [instanceId]);

  // Re-compute the "synced Xs ago" display every 5s from the snapshot_at
  // timestamp returned by getPositions. Pure UI tick — does not re-fetch.
  useEffect(() => {
    function compute() {
      if (!snapshotAt) { setSyncedAgo("never synced"); return; }
      const ts = new Date(snapshotAt).getTime();
      if (Number.isNaN(ts)) { setSyncedAgo("never synced"); return; }
      const secondsAgo = Math.max(0, Math.floor((Date.now() - ts) / 1000));
      let text: string;
      if (secondsAgo < 60)        text = `${secondsAgo}s ago`;
      else if (secondsAgo < 3600) text = `${Math.floor(secondsAgo / 60)}m ago`;
      else if (secondsAgo < 86400) text = `${Math.floor(secondsAgo / 3600)}h ago`;
      else                         text = `${Math.floor(secondsAgo / 86400)}d ago`;
      setSyncedAgo(`synced ${text}`);
    }
    compute();
    const id = setInterval(compute, 5000);
    return () => clearInterval(id);
  }, [snapshotAt]);

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
                <span style={{ fontSize: 10, color: "var(--t2)" }}>live &middot; {syncedAgo}</span>
              </div>
            )}
          </div>
          <div style={{ display: "flex", gap: 14 }}>
            <Toggle on={isLive} onColor="var(--green)" offColor="var(--t2)" onToggle={() => {
              if (isLive) { setConfirmPause(true); } else {
                const exExists = exchanges.some(e => e.id === inst.exchangeId);
                if (exExists) {
                  allocatorApi.updateAllocation(inst.id, { status: "active" }).catch(() => {});
                  updateInstance(inst.id, { status: "live" }); setExchangeLost(false);
                }
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
              <button onClick={async () => { await allocatorApi.updateAllocation(inst.id, { status: "paused" }).catch(() => {}); updateInstance(inst.id, { status: "paused" }); setConfirmPause(false); }} style={{ background: "transparent", border: "none", color: "var(--t1)", fontSize: 9, cursor: "pointer", padding: 0 }}>Yes, pause</button>
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

        {/* Metric cards — live equity + allocation + open positions + Session/Total P&L.
            Session + Total cards show USD as the hero number (same styling as Total
            Account Equity) with ROI% + chevron as the secondary subtitle. */}
        <div style={{ display: "grid", gridTemplateColumns: "1.1fr 1fr 0.9fr 1.1fr 1.1fr", gap: 10, marginBottom: 20 }}>
          <MetricCard
            label="TOTAL ACCOUNT EQUITY"
            value={`$${fmt(pnl?.equity_usd ?? exchanges.find(e => e.name === inst.exchangeName)?.balance ?? 0, 0)}`}
          />
          <MetricCard label="ALLOCATION" value={`$${fmt(inst.allocation ?? 0, 0)}`} />
          <MetricCard label="OPEN POSITIONS" value={positionsLoading && !pnl ? "..." : String(livePositions.length)} />
          <MetricCard
            label="SESSION P&L"
            value={pnl?.session_pnl_usd == null ? "\u2014" : fmtUsdSigned(pnl.session_pnl_usd)}
            color={pnlColor(pnl?.session_pnl_usd)}
            subtitle={pnlSubtitle(pnl?.session_return_pct)}
            subtitleColor={pnlColor(pnl?.session_return_pct)}
          />
          <MetricCard
            label="TOTAL P&L"
            value={pnl == null ? "\u2014" : fmtUsdSigned(pnl.total_pnl_usd)}
            color={pnlColor(pnl?.total_pnl_usd)}
            subtitle={pnlSubtitle(pnl?.total_return_pct)}
            subtitleColor={pnlColor(pnl?.total_return_pct)}
          />
        </div>

        {/* Performance chart */}
        <PerformanceChart instanceId={instanceId} />

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
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <span style={{ fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase" }}>OPEN POSITIONS</span>
              <span style={{ fontSize: 9, color: "var(--t3)" }}>·</span>
              <span style={{ fontSize: 9, color: "var(--t2)" }}>{livePositions.length} open</span>
              {snapshotAt && (
                <>
                  <span style={{ fontSize: 9, color: "var(--t3)" }}>·</span>
                  <span style={{ fontSize: 9, color: "var(--t3)" }}>{syncedAgo}</span>
                </>
              )}
            </div>
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
                  {([
                    { label: "SYMBOL",   align: "left"  },
                    { label: "SIDE",     align: "left"  },
                    { label: "SIZE",     align: "right" },
                    { label: "NOTIONAL", align: "right" },
                    { label: "ENTRY",    align: "right" },
                    { label: "MARK",     align: "right" },
                    { label: "LEV",      align: "right" },
                    { label: "P&L",      align: "right" },
                  ] as const).map(h => (
                    <th key={h.label} style={{ padding: "8px 16px", textAlign: h.align, color: "var(--t3)", fontSize: 9, fontWeight: 700, letterSpacing: "0.12em" }}>{h.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {livePositions.map((p, i) => (
                  <tr
                    key={`${p.symbol}-${i}`}
                    style={{
                      borderBottom: i < livePositions.length - 1 ? "1px solid var(--bg3)" : "none",
                      transition: "background 0.1s ease",
                    }}
                    onMouseEnter={e => { e.currentTarget.style.background = "var(--bg2)"; }}
                    onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}
                  >
                    <td style={{ padding: "9px 16px", color: "var(--t0)", fontWeight: 700 }}>{stripQuote(p.symbol)}</td>
                    <td style={{ padding: "9px 16px" }}>
                      <span style={{
                        fontSize: 9,
                        letterSpacing: "0.06em",
                        color: p.side === "LONG" ? "var(--green)" : p.side === "SHORT" ? "var(--red)" : "var(--t2)",
                      }}>{p.side}</span>
                    </td>
                    <td style={{ padding: "9px 16px", color: "var(--t1)", textAlign: "right" }}>{p.size.toLocaleString("en-US")}</td>
                    <td style={{ padding: "9px 16px", color: "var(--t1)", textAlign: "right" }}>{fmtNotional(p.notionalUsd)}</td>
                    <td style={{ padding: "9px 16px", color: "var(--t1)", textAlign: "right" }}>{fmtPrice(p.entry)}</td>
                    <td style={{ padding: "9px 16px", color: "var(--t1)", textAlign: "right" }}>{fmtPrice(p.mark)}</td>
                    <td style={{ padding: "9px 16px", color: "var(--t2)", textAlign: "right" }}>{p.leverage ? `${p.leverage}×` : "—"}</td>
                    <td style={{ padding: "9px 16px", textAlign: "right", color: p.pnl >= 0 ? "var(--green)" : "var(--red)" }}>
                      {p.pnl >= 0 ? "+" : ""}${fmt(p.pnl)}
                      <span style={{ fontSize: 9, opacity: 0.7, marginLeft: 4 }}>({p.pnlPct >= 0 ? "+" : ""}{fmt(p.pnlPct)}%)</span>
                    </td>
                  </tr>
                ))}
                {livePositions.length === 0 && !positionsLoading && (
                  <tr><td colSpan={8} style={{ padding: "20px 16px", textAlign: "center", color: "var(--t2)", fontSize: 10 }}>No open positions</td></tr>
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
                  <span style={{ fontSize: 9, color: "var(--red)" }}>Close all {livePositions.length} positions?</span>
                  <button onClick={async () => { await allocatorApi.updateAllocation(inst.id, { status: "paused" }).catch(() => {}); updateInstance(inst.id, { positions: [], status: "paused" }); setLivePositions([]); setConfirmClose(false); }}
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
<span onClick={() => setSettingsOpen(v => !v)} style={{ fontSize: 10, color: "var(--t2)", transition: "transform 0.2s ease", display: "inline-block", transform: settingsOpen ? "rotate(0deg)" : "rotate(-90deg)" }}>{"\u25BC"}</span>
            </div>
          </div>
          <div ref={settingsContentRef} style={{
            overflow: "hidden",
            height: settingsOpen ? "auto" : 0,
            transition: "height 0.2s ease",
          }}>
            <div style={{ padding: "4px 18px 16px" }}>
              {/* ── Editable config ─────────────────────────────────── */}

              {/* USD Allocation */}
              <SettingsRow
                label="USD ALLOCATION"
                control={
                  editing ? (
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <input type="text"
                        value={editAllocation}
                        onChange={e => {
                          const raw = e.target.value.replace(/[^0-9]/g, "");
                          const num = parseInt(raw) || 0;
                          if (num > editMaxAllocation) return;
                          setEditAllocation(raw);
                        }}
                        autoFocus
                        style={{ width: 120, background: "var(--bg3)", border: "1px solid var(--line)", borderRadius: 3, padding: "6px 10px", color: "var(--t1)", fontSize: 10, outline: "none" }}
                        onFocus={e => (e.target.style.borderColor = "var(--green)")}
                        onBlur={e => (e.target.style.borderColor = "var(--line)")} />
                      {(() => {
                        const val = parseInt(editAllocation) || 0;
                        const valid = val > 0 && val <= editMaxAllocation;
                        return (
                          <>
                            <button onClick={async () => {
                              if (!valid) return;
                              if (isLive && !capitalEditConfirm) { setCapitalEditConfirm(true); return; }
                              await allocatorApi.updateAllocation(inst.id, { capital_usd: val }).catch(() => {});
                              updateInstance(inst.id, { allocation: val });
                              setEditing(false); setCapitalEditConfirm(false);
                            }}
                              style={{ background: "transparent", border: "none", color: valid ? (isLive && !capitalEditConfirm ? "var(--amber)" : "var(--green)") : "var(--t3)", fontSize: 9, cursor: valid ? "pointer" : "not-allowed", padding: 0, opacity: valid ? 1 : 0.4, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase" }}>
                              {isLive && !capitalEditConfirm ? "Update anyway?" : "Save"}
                            </button>
                            <button onClick={() => { setEditing(false); setCapitalEditConfirm(false); }}
                              style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase" }}>Cancel</button>
                          </>
                        );
                      })()}
                      <span style={{ fontSize: 9, color: "var(--t3)" }}>Max ${editMaxAllocation.toLocaleString("en-US")}</span>
                    </div>
                  ) : (
                    <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                      <span style={{ fontSize: 11, color: "var(--t1)" }}>${(inst.allocation ?? 0).toLocaleString("en-US")}</span>
                      <button
                        onClick={() => { setEditing(true); setEditAllocation(String(inst.allocation ?? 0)); setCapitalEditConfirm(false); }}
                        onMouseEnter={e => { e.currentTarget.style.color = "var(--t1)"; }}
                        onMouseLeave={e => { e.currentTarget.style.color = "var(--t3)"; }}
                        style={{ background: "transparent", border: "none", color: "var(--t3)", fontSize: 9, cursor: "pointer", padding: 0, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", transition: "color 0.15s" }}>Edit</button>
                    </div>
                  )
                }
              />

              {/* Compounding — segmented pill, auto-saves on click */}
              <SettingsRow
                label={
                  <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <span>COMPOUNDING</span>
                    {compoundingSavedAt !== null && (
                      <span style={{ color: "var(--green)", fontSize: 9, letterSpacing: 0, textTransform: "none", opacity: 0.9 }}>✓ saved</span>
                    )}
                  </span>
                }
                control={
                  <div>
                    <div style={{ display: "inline-flex", gap: 4 }}>
                      {(["compound", "fixed"] as const).map((mode) => {
                        const active = inst.compoundingMode === mode;
                        return (
                          <button
                            key={mode}
                            type="button"
                            onClick={async () => {
                              if (active) return;
                              const prev = inst.compoundingMode;
                              updateInstance(inst.id, { compoundingMode: mode });
                              try {
                                await allocatorApi.updateAllocation(inst.id, { compounding_mode: mode });
                                setCompoundingSavedAt(Date.now());
                              } catch {
                                updateInstance(inst.id, { compoundingMode: prev });
                              }
                            }}
                            style={{
                              padding: "5px 14px",
                              fontSize: 9,
                              fontWeight: 700,
                              letterSpacing: "0.08em",
                              textTransform: "uppercase",
                              background: active ? "var(--green-dim)" : "transparent",
                              color: active ? "var(--green)" : "var(--t2)",
                              border: "1px solid " + (active ? "var(--green-mid)" : "var(--line)"),
                              borderRadius: 3,
                              cursor: active ? "default" : "pointer",
                              transition: "background 0.15s, color 0.15s, border-color 0.15s",
                            }}
                            onMouseEnter={(e) => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
                            onMouseLeave={(e) => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
                          >
                            {mode}
                          </button>
                        );
                      })}
                    </div>
                    <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 6, lineHeight: 1.5, maxWidth: 520 }}>
                      {inst.compoundingMode === "compound"
                        ? "Next session's capital follows your wallet equity at session close."
                        : "Allocation stays constant. Profits and losses accumulate in your wallet as idle capital."}
                    </div>
                  </div>
                }
              />

              {/* Alerts */}
              <SettingsRow
                label={
                  <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <span>ALERTS</span>
                    {alertsSavedAt !== null && (
                      <span style={{ color: "var(--green)", fontSize: 9, letterSpacing: 0, textTransform: "none", opacity: 0.9 }}>✓ saved</span>
                    )}
                  </span>
                }
                control={
                  <div style={{ display: "inline-flex", gap: 4 }}>
                    {([false, true] as const).map((val) => {
                      const active = inst.alerts === val;
                      const label = val ? "On" : "Off";
                      return (
                        <button
                          key={label}
                          type="button"
                          onClick={() => {
                            if (active) return;
                            updateInstance(inst.id, { alerts: val });
                            setAlertsSavedAt(Date.now());
                          }}
                          style={{
                            padding: "5px 14px",
                            fontSize: 9,
                            fontWeight: 700,
                            letterSpacing: "0.08em",
                            textTransform: "uppercase",
                            background: active ? "var(--green-dim)" : "transparent",
                            color: active ? "var(--green)" : "var(--t2)",
                            border: "1px solid " + (active ? "var(--green-mid)" : "var(--line)"),
                            borderRadius: 3,
                            cursor: active ? "default" : "pointer",
                            transition: "background 0.15s, color 0.15s, border-color 0.15s",
                          }}
                          onMouseEnter={(e) => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
                          onMouseLeave={(e) => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
                        >
                          {label}
                        </button>
                      );
                    })}
                  </div>
                }
              />

              {/* ── Read-only identity (divider above) ────────────── */}
              <div style={{ borderTop: "0.5px solid var(--line)", margin: "14px 0" }} />

              <SettingsRow
                label="EXCHANGE"
                readOnly
                control={
                  <span style={{ fontSize: 11, color: "var(--t1)" }}>{inst.exchangeName}</span>
                }
              />
              <SettingsRow
                label="STRATEGY"
                readOnly
                control={
                  <span style={{ fontSize: 11, color: "var(--t1)" }}>{inst.strategyName}</span>
                }
              />

              {/* Remove trader */}
              <div style={{ borderTop: "0.5px solid var(--line)", marginTop: 14, paddingTop: 12, display: "flex", justifyContent: "flex-end" }}>
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
                    <button onClick={async () => { await allocatorApi.deleteAllocation(inst.id).catch(() => {}); removeInstance(inst.id); router.push("/trader/traders"); }}
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

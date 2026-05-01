"use client";

/**
 * Manager → Live tab (skeleton).
 *
 * Step 5 ships the structural shell only — page header, all section
 * placeholders in collapsible cards, sticky right-side chat panel.
 * Data wiring lands in step 6+; until then sections show skeleton
 * loaders or an empty-state caption.
 *
 * The REFRESH button is functional: it hits all three Live endpoints
 * and updates a "last refreshed" timestamp on success. EXPORT and
 * FLATTEN ALL are placeholder UI — the hold-to-confirm visual works,
 * but neither performs any action in v1 (per scope).
 */

import { CSSProperties, ReactNode, useCallback, useEffect, useState } from "react";
import Collapsible from "../../../components/Collapsible";
import Skeleton from "../../../components/Skeleton";
import type {
  AccountSnapshot,
  RiskSnapshot,
  PositionsResponse,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Page header ─────────────────────────────────────────────────────────

interface HeaderProps {
  venue: string;
  positionsCount: number | null;
  lastRefreshAt: Date | null;
  refreshing: boolean;
  onRefresh: () => void;
}

function PageHeader({ venue, positionsCount, lastRefreshAt, refreshing, onRefresh }: HeaderProps) {
  const [, setTick] = useState(0);
  // 1-Hz tick to keep the "updated Xs ago" stamp current without
  // re-fetching data.
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const updatedLabel = lastRefreshAt
    ? `UPDATED ${lastRefreshAt.toISOString().slice(11, 19)} UTC`
    : "UPDATED —";

  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-end",
        justifyContent: "space-between",
        gap: 16,
        flexWrap: "wrap",
        marginBottom: 18,
      }}
    >
      <div>
        <div
          style={{
            fontSize: 22,
            fontWeight: 700,
            letterSpacing: "0.02em",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          Live Account Status
        </div>
        <div
          style={{
            display: "flex",
            gap: 14,
            alignItems: "center",
            color: "var(--t2)",
            fontSize: 11,
            letterSpacing: "0.06em",
            marginTop: 4,
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          <LiveTag />
          <span>VENUE · {venue.toUpperCase()}</span>
          <span>{positionsCount === null ? "— OPEN POSITIONS" : `${positionsCount} OPEN POSITIONS`}</span>
          <span>{updatedLabel}</span>
        </div>
      </div>

      <div style={{ display: "flex", gap: 8 }}>
        <HeaderButton onClick={() => { /* placeholder — out of v1 scope */ }}>
          EXPORT
        </HeaderButton>
        <FlattenAllButton />
        <HeaderButton onClick={onRefresh} primary disabled={refreshing}>
          {refreshing ? "REFRESHING…" : "REFRESH ↻"}
        </HeaderButton>
      </div>
    </div>
  );
}

function LiveTag() {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        color: "var(--green)",
        border: "1px solid var(--green)",
        padding: "3px 8px",
        borderRadius: 3,
        fontSize: 10,
        letterSpacing: "0.12em",
      }}
    >
      <span
        aria-hidden
        style={{
          width: 6,
          height: 6,
          borderRadius: "50%",
          background: "var(--green)",
          boxShadow: "0 0 6px rgba(0, 200, 150, 0.45)",
          animation: "pulse-dot 1.6s ease-in-out infinite",
        }}
      />
      LIVE · TICK 2s
    </span>
  );
}

function HeaderButton({
  children, onClick, primary, danger, disabled,
}: {
  children: ReactNode;
  onClick?: () => void;
  primary?: boolean;
  danger?: boolean;
  disabled?: boolean;
}) {
  const base: CSSProperties = {
    border: "1px solid var(--line)",
    padding: "6px 12px",
    borderRadius: 3,
    background: "transparent",
    color: "var(--t2)",
    fontSize: 11,
    letterSpacing: "0.08em",
    cursor: disabled ? "default" : "pointer",
    fontFamily: "var(--font-space-mono), Space Mono, monospace",
    opacity: disabled ? 0.5 : 1,
  };
  const variant: CSSProperties = primary
    ? { borderColor: "var(--green)", color: "var(--green)", background: "var(--green-dim)" }
    : danger
      ? { borderColor: "var(--red)", color: "var(--red)", background: "var(--red-dim)" }
      : {};
  return (
    <button onClick={onClick} disabled={disabled} style={{ ...base, ...variant }}>
      {children}
    </button>
  );
}

/**
 * Hold-to-confirm placeholder. Pressing the button starts a 2-second
 * fill animation; releasing before the timer completes resets. Reaching
 * 2s flashes a "READY" state. v1 wires NO action on completion — the
 * UI is here so the design is reviewable end-to-end.
 */
function FlattenAllButton() {
  const [progress, setProgress] = useState(0);
  const [active, setActive] = useState(false);

  const start = useCallback(() => {
    setActive(true);
  }, []);

  const stop = useCallback(() => {
    setActive(false);
    setProgress(0);
  }, []);

  useEffect(() => {
    if (!active) return;
    const STEP_MS = 50;
    const TOTAL_MS = 2000;
    const id = setInterval(() => {
      setProgress((p) => {
        const next = p + STEP_MS;
        if (next >= TOTAL_MS) {
          clearInterval(id);
          // Reached "ready" — placeholder, no action in v1.
          setTimeout(() => {
            setActive(false);
            setProgress(0);
          }, 400);
          return TOTAL_MS;
        }
        return next;
      });
    }, STEP_MS);
    return () => clearInterval(id);
  }, [active]);

  const fillPct = active ? Math.min(100, (progress / 2000) * 100) : 0;

  return (
    <button
      onMouseDown={start}
      onMouseUp={stop}
      onMouseLeave={stop}
      onTouchStart={start}
      onTouchEnd={stop}
      title="Hold 2s to confirm — placeholder, no action wired in v1"
      style={{
        position: "relative",
        border: "1px solid var(--red)",
        padding: "6px 12px",
        borderRadius: 3,
        background: "var(--red-dim)",
        color: "var(--red)",
        fontSize: 11,
        letterSpacing: "0.08em",
        cursor: "pointer",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        overflow: "hidden",
      }}
    >
      <span
        aria-hidden
        style={{
          position: "absolute",
          inset: 0,
          background: "rgba(255, 77, 77, 0.25)",
          width: `${fillPct}%`,
          transition: active ? "none" : "width 200ms ease",
        }}
      />
      <span style={{ position: "relative" }}>
        {fillPct >= 100 ? "READY" : "⚠ FLATTEN ALL · HOLD"}
      </span>
    </button>
  );
}

// ─── Section placeholders ────────────────────────────────────────────────

function Placeholder({ height = 80, label }: { height?: number; label?: string }) {
  return (
    <div
      style={{
        height,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "var(--t3)",
        fontSize: 10,
        letterSpacing: "0.06em",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {label ?? "Loading…"}
    </div>
  );
}

function KpiStripSkeleton() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 1, background: "var(--line)", borderRadius: 4, overflow: "hidden", marginTop: 4 }}>
      {Array.from({ length: 6 }).map((_, i) => (
        <div key={i} style={{ background: "var(--bg2)", padding: "14px 16px" }}>
          <Skeleton width={80} height={9} />
          <div style={{ height: 8 }} />
          <Skeleton width={120} height={20} />
          <div style={{ height: 8 }} />
          <Skeleton width={140} height={10} />
        </div>
      ))}
    </div>
  );
}

function RiskRowSkeleton() {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 1, background: "var(--line)", borderRadius: 4, overflow: "hidden", marginTop: 4 }}>
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} style={{ background: "var(--bg2)", padding: "12px 16px" }}>
          <Skeleton width={80} height={9} />
          <div style={{ height: 6 }} />
          <Skeleton width={150} height={14} />
          <div style={{ height: 4 }} />
          <Skeleton width={180} height={9} />
        </div>
      ))}
    </div>
  );
}

// ─── Right chat panel (read-only scaffold per step 5) ────────────────────

const CTX_CHIPS = [
  "OPEN POSITIONS",
  "90D ACCOUNT HISTORY",
  "LIVE FUNDING",
  "OI · L/S SKEW",
  "TA · 1m–1d",
  "STRATEGY METADATA",
];

function ChatPanel({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  return (
    <aside
      style={{
        width: collapsed ? 36 : 400,
        flexShrink: 0,
        borderLeft: "1px solid var(--line)",
        background: "var(--bg0)",
        position: "sticky",
        top: 0,
        height: "calc(100vh - 44px)",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        transition: "width 200ms ease",
      }}
    >
      {/* Header / collapse handle */}
      <div
        style={{
          height: 40,
          borderBottom: "1px solid var(--line)",
          display: "flex",
          alignItems: "center",
          justifyContent: collapsed ? "center" : "space-between",
          padding: collapsed ? 0 : "0 12px",
          flexShrink: 0,
        }}
      >
        {!collapsed && (
          <span
            style={{
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "0.08em",
              color: "var(--t1)",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            PM <span style={{ color: "var(--purple, #7B5FFF)" }}>ASSISTANT</span>
          </span>
        )}
        <button
          onClick={onToggle}
          title={collapsed ? "Expand chat" : "Collapse chat"}
          style={{
            width: 24,
            height: 24,
            border: "1px solid var(--line2)",
            borderRadius: 3,
            background: "transparent",
            color: "var(--t1)",
            fontSize: 11,
            cursor: "pointer",
          }}
        >
          {collapsed ? "«" : "»"}
        </button>
      </div>

      {collapsed ? (
        <div
          style={{
            flex: 1,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            pointerEvents: "none",
          }}
        >
          <span
            style={{
              writingMode: "vertical-rl",
              transform: "rotate(180deg)",
              fontSize: 9,
              letterSpacing: "0.2em",
              color: "var(--t3)",
            }}
          >
            PM ASSISTANT · CTX LIVE
          </span>
        </div>
      ) : (
        <>
          {/* Context chips */}
          <div
            style={{
              padding: "10px 12px",
              borderBottom: "1px solid var(--line)",
              display: "flex",
              flexWrap: "wrap",
              gap: 4,
              flexShrink: 0,
            }}
          >
            {CTX_CHIPS.map((c) => (
              <span
                key={c}
                style={{
                  fontSize: 9,
                  letterSpacing: "0.08em",
                  color: "var(--t3)",
                  padding: "3px 7px",
                  border: "1px solid var(--line)",
                  borderRadius: 2,
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 5,
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                }}
              >
                <span
                  aria-hidden
                  style={{
                    width: 5,
                    height: 5,
                    borderRadius: "50%",
                    background: "var(--green)",
                  }}
                />
                {c}
              </span>
            ))}
          </div>

          {/* Messages — empty in v1 (LLM not wired) */}
          <div
            style={{
              flex: 1,
              overflowY: "auto",
              padding: "20px 14px",
              fontSize: 11,
              color: "var(--t3)",
              letterSpacing: "0.04em",
              textAlign: "center",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            Chat is read-only in v1. The PM-grade conversation engine
            ships in a separate task.
          </div>

          {/* Suggested prompts */}
          <div style={{ borderTop: "1px solid var(--line)", padding: "10px 12px 0" }}>
            <div
              style={{
                fontSize: 9,
                letterSpacing: "0.16em",
                color: "var(--t3)",
                marginBottom: 8,
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
              }}
            >
              SUGGESTED
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
              {[
                "Where is my biggest tail risk right now?",
                "What's pushing the portfolio today?",
                "Show me the BTC/SOL drawdown overlap",
              ].map((q) => (
                <span
                  key={q}
                  style={{
                    fontSize: 10,
                    color: "var(--t2)",
                    background: "var(--bg2)",
                    border: "1px solid var(--line)",
                    padding: "5px 9px",
                    borderRadius: 3,
                    cursor: "default",
                    fontFamily: "var(--font-space-mono), Space Mono, monospace",
                  }}
                >
                  {q}
                </span>
              ))}
            </div>
          </div>

          {/* Input — disabled in v1 */}
          <div style={{ borderTop: "1px solid var(--line)", padding: "10px 12px 14px" }}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                background: "var(--bg2)",
                border: "1px solid var(--line2)",
                borderRadius: 3,
                padding: "0 10px",
              }}
            >
              <input
                disabled
                placeholder="Ask the PM…"
                style={{
                  flex: 1,
                  background: "transparent",
                  border: "none",
                  color: "var(--t2)",
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                  fontSize: 12,
                  padding: "10px 6px",
                  outline: "none",
                }}
              />
              <span style={{ color: "var(--t3)", fontSize: 11 }}>SEND ↵</span>
            </div>
          </div>
        </>
      )}
    </aside>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────

export default function LivePage() {
  const [account, setAccount] = useState<AccountSnapshot | null>(null);
  const [risk, setRisk] = useState<RiskSnapshot | null>(null);
  const [positions, setPositions] = useState<PositionsResponse | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [lastRefreshAt, setLastRefreshAt] = useState<Date | null>(null);
  const [chatCollapsed, setChatCollapsed] = useState(false);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    try {
      const [a, r, p] = await Promise.all([
        fetch(`${API_BASE}/api/manager/live/account`, { credentials: "include" }).then((x) => (x.ok ? x.json() : null)),
        fetch(`${API_BASE}/api/manager/live/risk`, { credentials: "include" }).then((x) => (x.ok ? x.json() : null)),
        fetch(`${API_BASE}/api/manager/live/positions`, { credentials: "include" }).then((x) => (x.ok ? x.json() : null)),
      ]);
      if (a) setAccount(a);
      if (r) setRisk(r);
      if (p) setPositions(p);
      setLastRefreshAt(new Date());
    } catch {
      // Step 6 will handle errors visibly; for the skeleton we just
      // swallow so the shell always renders.
    } finally {
      setRefreshing(false);
    }
  }, []);

  // Initial load
  useEffect(() => {
    refresh();
  }, [refresh]);

  const venue = account?.venue ?? "blofin";
  const positionsCount = positions?.counts?.total ?? null;

  return (
    <div style={{ display: "flex", height: "100%" }}>
      {/* Main content */}
      <main style={{ flex: 1, overflow: "auto", padding: "16px 20px 32px" }}>
        <PageHeader
          venue={venue}
          positionsCount={positionsCount}
          lastRefreshAt={lastRefreshAt}
          refreshing={refreshing}
          onRefresh={refresh}
        />

        {/* Account snapshot — KPI strip */}
        <Collapsible
          id="live:account"
          title="Account Snapshot"
          summary={
            account ? (
              <>
                EQUITY{" "}
                <strong style={{ color: "var(--t1)" }}>
                  ${account.total_equity_usd.toLocaleString()}
                </strong>
              </>
            ) : (
              "loading…"
            )
          }
        >
          <KpiStripSkeleton />
        </Collapsible>

        {/* Risk signals */}
        <Collapsible
          id="live:risk"
          title="Risk Signals"
          summary={risk ? <>NEAREST STOP <strong style={{ color: "var(--t1)" }}>{risk.nearest_stop.symbol_base ?? "—"}</strong></> : "loading…"}
        >
          <RiskRowSkeleton />
        </Collapsible>

        {/* Position map (treemap) */}
        <Collapsible
          id="live:position-map"
          title="Position Map · Notional × PnL"
          summary={positions ? <>{positions.counts.total} POSITIONS</> : undefined}
        >
          <Placeholder height={240} label="Treemap renders in step 7" />
        </Collapsible>

        {/* PnL Attribution + Exposure (split row) */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <Collapsible
            id="live:waterfall"
            title="PnL Attribution · Today"
          >
            <Placeholder height={200} label="Waterfall renders in step 7" />
          </Collapsible>
          <Collapsible
            id="live:exposure"
            title="Exposure · Long vs Short"
          >
            <Placeholder height={200} label="Exposure bar renders in step 7" />
          </Collapsible>
        </div>

        {/* Coverage matrix + Factor decomp (split row) */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <Collapsible
            id="live:coverage-matrix"
            title="Coverage Matrix · 30D Rolling"
          >
            <Placeholder height={260} label="Pairwise correlation matrix renders in step 10" />
          </Collapsible>
          <Collapsible
            id="live:factor-decomp"
            title="Factor Decomposition · 30D Rolling"
          >
            <Placeholder height={260} label="Effective-N + factor bar render in step 11" />
          </Collapsible>
        </div>

        {/* Box plot strip */}
        <Collapsible
          id="live:box-plots"
          title="Trailing Distribution · 24H Window"
        >
          <Placeholder height={170} label="Per-position box plots render in step 9" />
        </Collapsible>

        {/* MA alignment heatmap */}
        <Collapsible
          id="live:ma-heatmap"
          title="MA Alignment · Distance from EMA"
        >
          <Placeholder height={250} label="EMA-distance heatmap renders in step 8" />
        </Collapsible>

        {/* Open positions table */}
        <Collapsible
          id="live:positions-table"
          title={`Open Positions · ${positions?.counts.total ?? "—"}`}
        >
          <Placeholder height={120} label="Positions table renders in step 6" />
        </Collapsible>
      </main>

      {/* Right chat panel */}
      <ChatPanel collapsed={chatCollapsed} onToggle={() => setChatCollapsed((v) => !v)} />
    </div>
  );
}

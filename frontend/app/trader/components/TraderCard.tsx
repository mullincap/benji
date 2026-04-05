"use client";

import { useRouter } from "next/navigation";
import { useTrader, StrategyInstance, fmt, RISK_COLOR, RISK_DIM } from "../context";

// ─── Toggle ──────────────────────────────────────────────────────────────────

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

// ─── Unlinked card ───────────────────────────────────────────────────────────

function UnlinkedCard({ inst }: { inst: StrategyInstance }) {
  const router = useRouter();
  return (
    <div
      onClick={() => router.push(`/trader/traders/${inst.id}`)}
      style={{
        background: "var(--bg2)", border: "1px solid var(--green-dim)", borderRadius: 5,
        padding: "16px 18px", cursor: "pointer",
        transition: "border-color 0.15s ease",
      }}
      onMouseEnter={e => (e.currentTarget.style.borderColor = "var(--green)")}
      onMouseLeave={e => (e.currentTarget.style.borderColor = "var(--green-dim)")}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 14, fontWeight: 700, color: "var(--t0)" }}>{inst.strategyName}</span>
          <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", padding: "2px 6px", borderRadius: 2, background: RISK_DIM[inst.risk], color: RISK_COLOR[inst.risk] }}>{inst.risk}</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
          <span style={{ width: 5, height: 5, borderRadius: "50%", background: "transparent", border: "1px solid var(--green)" }} />
          <span style={{ fontSize: 9, color: "var(--green)", fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase" }}>SETUP REQUIRED</span>
        </div>
      </div>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: 10, color: "var(--t3)" }}>No exchange linked &middot; not yet active</span>
        <button
          onClick={e => { e.stopPropagation(); router.push(`/trader/traders/${inst.id}`); }}
          style={{
            background: "var(--green)", color: "var(--bg0)",
            border: "none", borderRadius: 3,
            padding: "6px 14px", fontSize: 9, fontWeight: 700,
            letterSpacing: "0.12em", textTransform: "uppercase",
            cursor: "pointer",
          }}
        >
          BEGIN SETUP &rarr;
        </button>
      </div>
    </div>
  );
}

// ─── Live/Paused card ────────────────────────────────────────────────────────

function LiveCard({ inst }: { inst: StrategyInstance }) {
  const router = useRouter();
  const { updateInstance } = useTrader();
  const isLive = inst.status === "live";

  return (
    <div
      onClick={() => router.push(`/trader/traders/${inst.id}`)}
      style={{
        background: "var(--bg2)", border: "1px solid var(--line)", borderRadius: 5,
        padding: "16px 18px", cursor: "pointer",
        opacity: isLive ? 1 : 0.6,
        transition: "border-color 0.15s ease, opacity 0.2s ease",
      }}
      onMouseEnter={e => (e.currentTarget.style.borderColor = "var(--line2)")}
      onMouseLeave={e => (e.currentTarget.style.borderColor = "var(--line)")}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 14, fontWeight: 700, color: "var(--t0)" }}>{inst.strategyName}</span>
          {inst.exchangeName && (
            <span style={{ fontSize: 9, padding: "2px 6px", borderRadius: 2, background: "var(--bg3)", color: "var(--t1)", border: "1px solid var(--line)" }}>{inst.exchangeName}</span>
          )}
          <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", padding: "2px 6px", borderRadius: 2, background: RISK_DIM[inst.risk], color: RISK_COLOR[inst.risk] }}>{inst.risk}</span>
        </div>
        <Toggle
          on={isLive}
          onColor="var(--green)"
          offColor="var(--t2)"
          onToggle={() => updateInstance(inst.id, { status: isLive ? "paused" : "live" })}
          label={isLive ? "Live" : "Paused"}
        />
      </div>
      <div style={{ display: "flex", gap: 20, fontSize: 10, color: "var(--t2)" }}>
        <span>${fmt(inst.allocation ?? 0, 0)} allocated</span>
        <span style={{ color: !inst.dailyPnl ? "var(--t2)" : inst.dailyPnl > 0 ? "var(--green)" : "var(--red)" }}>
          {!inst.dailyPnl ? "\u2014" : `${inst.dailyPnl >= 0 ? "+" : ""}$${fmt(inst.dailyPnl)}`} today
        </span>
        <span>{inst.positions.length} open</span>
        <span>Alerts {inst.alerts ? "on" : "off"}</span>
      </div>
    </div>
  );
}

// ─── Exported card ───────────────────────────────────────────────────────────

export default function TraderCard({ inst }: { inst: StrategyInstance }) {
  if (inst.status === "unlinked") return <UnlinkedCard inst={inst} />;
  return <LiveCard inst={inst} />;
}

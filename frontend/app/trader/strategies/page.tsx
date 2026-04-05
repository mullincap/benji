"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useTrader, STRATEGY_CATALOG, StrategyType, StrategyInstance, fmt, RISK_COLOR, RISK_DIM } from "../context";

const CATALOG_ENTRIES = Object.entries(STRATEGY_CATALOG) as [StrategyType, typeof STRATEGY_CATALOG[StrategyType]][];

// ─── View toggle icons ───────────────────────────────────────────────────────

function ListIcon({ color }: { color: string }) {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <rect x="1" y="2" width="12" height="1.5" rx="0.5" fill={color} />
      <rect x="1" y="6" width="12" height="1.5" rx="0.5" fill={color} />
      <rect x="1" y="10" width="12" height="1.5" rx="0.5" fill={color} />
    </svg>
  );
}

function GridIcon({ color }: { color: string }) {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <rect x="1" y="1" width="5" height="5" rx="1" fill={color} />
      <rect x="8" y="1" width="5" height="5" rx="1" fill={color} />
      <rect x="1" y="8" width="5" height="5" rx="1" fill={color} />
      <rect x="8" y="8" width="5" height="5" rx="1" fill={color} />
    </svg>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function StrategiesPage() {
  const router = useRouter();
  const { instances } = useTrader();
  const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
  const [view, setView] = useState<"list" | "grid">("list");

  function isDominant(index: number) {
    return hoveredIndex === null ? index === 0 : hoveredIndex === index;
  }

  const trans = "all 0.15s ease";

  return (
    <div style={{ background: "var(--bg0)", padding: "28px", minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>

        {/* Header row: label + view toggle */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
          <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase" }}>
            STRATEGIES
          </div>
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <button
              onClick={() => setView("list")}
              style={{
                padding: 0, border: "none", background: "transparent",
                cursor: "pointer", display: "inline-flex", alignItems: "center",
                transition: "color 0.15s ease",
              }}
              onMouseEnter={e => { if (view !== "list") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t1)"); }}
              onMouseLeave={e => { if (view !== "list") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t3)"); }}
            ><ListIcon color={view === "list" ? "var(--t1)" : "var(--t2)"} /></button>
            <button
              onClick={() => setView("grid")}
              style={{
                padding: 0, border: "none", background: "transparent",
                cursor: "pointer", display: "inline-flex", alignItems: "center",
                transition: "color 0.15s ease",
              }}
              onMouseEnter={e => { if (view !== "grid") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t1)"); }}
              onMouseLeave={e => { if (view !== "grid") e.currentTarget.querySelector("svg")?.setAttribute("fill", "var(--t3)"); }}
            ><GridIcon color={view === "grid" ? "var(--t1)" : "var(--t2)"} /></button>
          </div>
        </div>

        {/* Cards container */}
        <div
          onMouseLeave={() => setHoveredIndex(null)}
          style={view === "list"
            ? { display: "flex", flexDirection: "column", gap: 8 }
            : { display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 12 }
          }
        >
          {CATALOG_ENTRIES.map(([type, cat], index) => {
            const hasInstance = instances.some(i => i.strategyType === type);
            const dominant = isDominant(index);

            if (view === "list") {
              return (
                <div
                  key={type}
                  onMouseEnter={() => setHoveredIndex(index)}
                  style={{
                    background: dominant ? "var(--bg2)" : "var(--bg1)",
                    border: `1px solid ${hasInstance && dominant ? "var(--green-mid)" : "var(--line)"}`,
                    borderRadius: 5, padding: "20px 22px",
                    transition: trans, cursor: "pointer",
                  }}
                  onClick={() => router.push(`/trader/strategies/${type}`)}
                >
                  {/* Top row */}
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 16 }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                        <h3 style={{ fontSize: 13, fontWeight: 700, color: dominant ? "var(--t0)" : "var(--t2)", margin: 0, transition: trans }}>{cat.name}</h3>
                        <span style={{
                          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                          color: RISK_COLOR[cat.risk], background: RISK_DIM[cat.risk],
                          borderRadius: 3, padding: "3px 8px",
                        }}>{cat.risk}</span>
                      </div>
                      <p style={{ fontSize: 10, color: dominant ? "var(--t1)" : "var(--t3)", margin: 0, lineHeight: 1.5, maxWidth: 560, transition: trans }}>
                        {cat.description.split(".")[0]}.
                      </p>
                    </div>

                    <div style={{ display: "flex", alignItems: "center", gap: 16, flexShrink: 0, marginLeft: 24 }}>
                      <span style={{ fontSize: 18, fontWeight: 700, color: dominant ? "var(--green)" : "var(--t2)", transition: trans }}>
                        +{fmt(cat.ytd, 1)}%
                        <span style={{ fontSize: 9, fontWeight: 400, color: "var(--t2)", marginLeft: 4 }}>YTD</span>
                      </span>

                      {hasInstance ? (
                        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                          <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--green)" }} />
                          <span style={{ fontSize: 10, color: "var(--green)", whiteSpace: "nowrap" }}>In your traders</span>
                        </div>
                      ) : (
                        <button
                          onClick={e => { e.stopPropagation(); router.push(`/trader/strategies/${type}`); }}
                          style={{
                            padding: "8px 16px",
                            background: dominant ? "var(--green)" : "transparent",
                            color: dominant ? "var(--bg0)" : "var(--t2)",
                            border: dominant ? "none" : "1px solid var(--line)",
                            borderRadius: 3,
                            fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                            cursor: "pointer", whiteSpace: "nowrap",
                            transition: trans,
                          }}
                        >
                          VIEW STRATEGY
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Stats row */}
                  <div style={{ display: "flex", gap: 14, paddingTop: 14, borderTop: "1px solid var(--line)" }}>
                    {[
                      { label: "SHARPE", value: fmt(cat.sharpe, 2), first: true },
                      { label: "CAGR", value: `${fmt(cat.cagr, 1)}%` },
                      { label: "MAX DD", value: `-${fmt(cat.maxDd, 1)}%` },
                      { label: "WIN RATE", value: `${fmt(cat.winRate, 0)}%` },
                      { label: "AVG 1M", value: `+${fmt(cat.avg1m, 1)}%` },
                    ].map(s => (
                      <div key={s.label} style={{ borderLeft: s.first ? "none" : "1px solid var(--line)", paddingLeft: s.first ? 0 : 14 }}>
                        <div style={{ fontSize: 9, color: "var(--t2)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 3 }}>{s.label}</div>
                        <div style={{ fontSize: 12, fontWeight: 700, color: dominant ? "var(--t1)" : "var(--t3)", transition: trans }}>{s.value}</div>
                      </div>
                    ))}
                  </div>
                </div>
              );
            }

            // Grid view
            return (
              <div
                key={type}
                onMouseEnter={() => setHoveredIndex(index)}
                onClick={() => router.push(`/trader/strategies/${type}`)}
                style={{
                  background: dominant ? "var(--bg2)" : "var(--bg1)",
                  border: `1px solid ${hasInstance && dominant ? "var(--green-mid)" : "var(--line)"}`,
                  borderRadius: 5, padding: "20px 18px",
                  display: "flex", flexDirection: "column",
                  transition: trans, cursor: "pointer",
                }}
              >
                {/* Name + badge */}
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                  <h3 style={{ fontSize: 13, fontWeight: 700, color: dominant ? "var(--t0)" : "var(--t2)", margin: 0, transition: trans }}>{cat.name}</h3>
                  <span style={{
                    fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                    color: RISK_COLOR[cat.risk], background: RISK_DIM[cat.risk],
                    borderRadius: 3, padding: "3px 8px",
                  }}>{cat.risk}</span>
                </div>

                {/* Description */}
                <p style={{ fontSize: 10, color: dominant ? "var(--t1)" : "var(--t3)", margin: "0 0 14px", lineHeight: 1.5, flex: 1, transition: trans }}>
                  {cat.description.split(".")[0]}.
                </p>

                {/* Stats — vertical in grid mode, no dividers */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8, paddingTop: 12, borderTop: "1px solid var(--line)", marginBottom: 12 }}>
                  {[
                    { label: "SHARPE", value: fmt(cat.sharpe, 2) },
                    { label: "MAX DD", value: `-${fmt(cat.maxDd, 1)}%` },
                    { label: "WIN RATE", value: `${fmt(cat.winRate, 0)}%` },
                  ].map(s => (
                    <div key={s.label}>
                      <div style={{ fontSize: 9, color: "var(--t2)", letterSpacing: "0.12em", fontWeight: 700, textTransform: "uppercase", marginBottom: 3 }}>{s.label}</div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: dominant ? "var(--t1)" : "var(--t3)", transition: trans }}>{s.value}</div>
                    </div>
                  ))}
                </div>

                {/* YTD */}
                <div style={{ fontSize: 18, fontWeight: 700, color: dominant ? "var(--green)" : "var(--t2)", marginBottom: 14, transition: trans }}>
                  +{fmt(cat.ytd, 1)}%
                  <span style={{ fontSize: 9, fontWeight: 400, color: "var(--t2)", marginLeft: 4 }}>YTD</span>
                </div>

                {/* CTA */}
                {hasInstance ? (
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 6, padding: "8px 0" }}>
                    <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--green)" }} />
                    <span style={{ fontSize: 10, color: "var(--green)" }}>In your traders</span>
                  </div>
                ) : (
                  <button
                    onClick={e => { e.stopPropagation(); router.push(`/trader/strategies/${type}`); }}
                    style={{
                      width: "100%", padding: "9px 0",
                      background: dominant ? "var(--green)" : "transparent",
                      color: dominant ? "var(--bg0)" : "var(--t2)",
                      border: dominant ? "none" : "1px solid var(--line)",
                      borderRadius: 3,
                      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
                      cursor: "pointer",
                      transition: trans,
                    }}
                  >
                    VIEW STRATEGY
                  </button>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

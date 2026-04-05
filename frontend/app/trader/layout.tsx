"use client";

import { useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import Topbar from "../components/Topbar";
import { TraderProvider, useTrader, STRATEGY_CATALOG, StrategyType } from "./context";

// ─── Nav item ────────────────────────────────────────────────────────────────

function NavItem({ label, href, active }: { label: string; href: string; active: boolean }) {
  const router = useRouter();
  return (
    <button
      onClick={() => router.push(href)}
      style={{
        display: "block", width: "100%",
        padding: "5px 8px 5px 16px",
        background: "transparent",
        borderLeft: "none",
        borderRight: "none", borderTop: "none", borderBottom: "none",
        color: active ? "var(--t0)" : "var(--t2)",
        fontSize: 10, fontWeight: active ? 700 : 400,
        textAlign: "left", cursor: "pointer",
        transition: "all 0.15s ease",
        marginBottom: 2,
      }}
      onMouseEnter={e => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
      onMouseLeave={e => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
    >
      {label}
    </button>
  );
}

function SubItem({ label, href, active, dot, allocationLabel }: { label: string; href: string; active: boolean; dot?: { color: string; border?: string }; allocationLabel?: { text: string; color: string } }) {
  const router = useRouter();
  return (
    <button
      onClick={() => router.push(href)}
      style={{
        display: "flex", alignItems: "center", justifyContent: "space-between",
        width: "100%",
        padding: "3px 8px 3px 28px",
        background: "transparent",
        border: "none",
        color: active ? "var(--t1)" : "var(--t2)",
        fontSize: 10, fontWeight: 400,
        textAlign: "left", cursor: "pointer",
        transition: "color 0.15s ease",
        whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
      }}
      onMouseEnter={e => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
      onMouseLeave={e => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
    >
      <span style={{ overflow: "hidden", textOverflow: "ellipsis", flex: 1 }}>{label}</span>
      <span style={{ display: "flex", alignItems: "center", gap: 6, flexShrink: 0 }}>
        {allocationLabel && (
          <span style={{ fontSize: 9, color: allocationLabel.color, fontWeight: 400 }}>{allocationLabel.text}</span>
        )}
        {dot && (
          <span style={{
            width: 5, height: 5, borderRadius: "50%", flexShrink: 0,
            background: dot.color,
            border: dot.border ?? "none",
            marginRight: 4,
            opacity: 0.5,
          }} />
        )}
      </span>
    </button>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      padding: "10px 16px 6px",
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase",
    }}>
      {children}
    </div>
  );
}

// ─── Sidebar ─────────────────────────────────────────────────────────────────

const CATALOG_ENTRIES = Object.entries(STRATEGY_CATALOG) as [StrategyType, typeof STRATEGY_CATALOG[StrategyType]][];

function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const { instances } = useTrader();
  const [collapsed, setCollapsed] = useState(false);
  const [strategiesOpen, setStrategiesOpen] = useState(true);
  const [tradersOpen, setTradersOpen] = useState(true);

  return (
    <div style={{
      width: collapsed ? 38 : 288,
      flexShrink: 0,
      background: "var(--bg0)",
      borderRight: "1px solid var(--line)",
      display: "flex", flexDirection: "column",
      overflow: "hidden",
      transition: "width 0.2s ease",
    }}>
      {/* Header */}
      <div style={{
        height: 40,
        borderBottom: "1px solid var(--line)",
        display: "flex", alignItems: "center",
        justifyContent: collapsed ? "center" : "space-between",
        padding: collapsed ? 0 : "0 8px 0 16px",
        flexShrink: 0,
      }}>
        {!collapsed && (
          <span style={{ fontSize: 9, textTransform: "uppercase", letterSpacing: "0.12em", color: "var(--t3)", fontWeight: 700 }}>
            Deployment Panel
          </span>
        )}
        <button
          onClick={() => setCollapsed(v => !v)}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          style={{
            width: 24, height: 24,
            border: "1px solid var(--line2)", borderRadius: 3,
            background: "transparent", color: "var(--t1)",
            fontSize: 11, cursor: "pointer",
          }}
        >
          {collapsed ? "\u00BB" : "\u00AB"}
        </button>
      </div>

      {/* Expanded content */}
      {!collapsed && (
        <div style={{ flex: 1, overflowY: "auto", paddingTop: 4 }}>
          {/* TRADER section */}
          <NavItem label="Overview" href="/trader/overview" active={pathname === "/trader/overview"} />

          {/* Divider */}
          <div style={{ borderTop: "1px solid var(--line)", margin: "8px 14px" }} />

          {/* Strategies — collapsible, default closed */}
          {(() => {
            const active = pathname.startsWith("/trader/strategies");
            return (
              <div style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                padding: "5px 8px 5px 0",
                borderLeft: "none",
                background: "transparent",
                marginBottom: 2,
              }}>
                <button onClick={() => router.push("/trader/strategies")} style={{
                  flex: 1, background: "transparent", border: "none", textAlign: "left",
                  padding: "0 0 0 14px", fontSize: 10, cursor: "pointer",
                  color: active ? "var(--t0)" : "var(--t2)", fontWeight: active ? 700 : 400,
                }}
                  onMouseEnter={e => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
                  onMouseLeave={e => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
                >Strategies</button>
                <button onClick={() => setStrategiesOpen(v => !v)} style={{
                  background: "transparent", border: "none", padding: "2px 6px",
                  cursor: "pointer", display: "inline-flex", alignItems: "center",
                }}>
                  <span style={{ fontSize: 8, color: "var(--t2)", transition: "transform 0.2s ease", display: "inline-block", transform: strategiesOpen ? "rotate(0deg)" : "rotate(-90deg)" }}>{"\u25BC"}</span>
                </button>
              </div>
            );
          })()}
          {strategiesOpen && CATALOG_ENTRIES.map(([type, cat]) => (
            <SubItem
              key={type}
              label={cat.name}
              href={`/trader/strategies/${type}`}
              active={pathname === `/trader/strategies/${type}`}
            />
          ))}

          {/* Divider */}
          <div style={{ borderTop: "1px solid var(--line)", margin: "8px 14px" }} />

          {/* Traders — collapsible, default open */}
          {(() => {
            const active = pathname.startsWith("/trader/traders");
            return (
              <div style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                padding: "5px 8px 5px 0",
                borderLeft: "none",
                background: "transparent",
                marginBottom: 2,
              }}>
                <button onClick={() => router.push("/trader/traders")} style={{
                  flex: 1, background: "transparent", border: "none", textAlign: "left",
                  padding: "0 0 0 14px", fontSize: 10, cursor: "pointer",
                  color: active ? "var(--t0)" : "var(--t2)", fontWeight: active ? 700 : 400,
                }}
                  onMouseEnter={e => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
                  onMouseLeave={e => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
                >Traders</button>
                <button onClick={() => setTradersOpen(v => !v)} style={{
                  background: "transparent", border: "none", padding: "2px 6px",
                  cursor: "pointer", display: "inline-flex", alignItems: "center",
                }}>
                  <span style={{ fontSize: 8, color: "var(--t2)", transition: "transform 0.2s ease", display: "inline-block", transform: tradersOpen ? "rotate(0deg)" : "rotate(-90deg)" }}>{"\u25BC"}</span>
                </button>
              </div>
            );
          })()}
          {tradersOpen && instances.map(inst => {
            const dotStyle = inst.status === "live"
              ? { color: "var(--green)" }
              : inst.status === "paused"
              ? { color: "var(--amber)" }
              : { color: "transparent", border: "1px solid var(--t1)" };
            const label = inst.exchangeName
              ? `${inst.strategyName} \u00B7 ${inst.exchangeName}`
              : inst.strategyName;
            const allocLabel = (() => {
              if (!inst.allocation) return { text: "\u2014", color: "var(--t3)" };
              const v = inst.allocation;
              const text = v >= 1000000 ? `$${(v / 1000000).toFixed(1)}m` : v >= 1000 ? `$${Math.round(v / 1000)}k` : `$${v}`;
              const color = inst.status === "live" ? "var(--t2)" : "var(--t3)";
              return { text, color };
            })();
            return (
              <SubItem
                key={inst.id}
                label={label}
                href={`/trader/traders/${inst.id}`}
                active={pathname === `/trader/traders/${inst.id}`}
                dot={dotStyle}
                allocationLabel={allocLabel}
              />
            );
          })}
        </div>
      )}

      {/* Collapsed label */}
      {collapsed && (
        <div style={{
          flex: 1, display: "flex", alignItems: "flex-end",
          justifyContent: "center", paddingBottom: 8, pointerEvents: "none",
        }}>
          <div style={{ fontSize: 8, color: "var(--t3)", transform: "rotate(-90deg)", whiteSpace: "nowrap", letterSpacing: "0.08em" }}>
            SIDEBAR
          </div>
        </div>
      )}

      {/* Bottom pinned — Settings */}
      {!collapsed && (
        <div style={{ borderTop: "1px solid var(--line)", paddingTop: 4, paddingBottom: 4 }}>
          <NavItem label="Settings" href="/trader/settings" active={pathname === "/trader/settings"} />
        </div>
      )}
    </div>
  );
}

// ─── Layout ──────────────────────────────────────────────────────────────────

export default function TraderLayout({ children }: { children: React.ReactNode }) {
  return (
    <TraderProvider>
      <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
        <Topbar />
        <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
          <Sidebar />
          <div style={{ flex: 1, overflow: "auto" }}>
            {children}
          </div>
        </div>
      </div>
    </TraderProvider>
  );
}

"use client";

/**
 * frontend/app/trader/(protected)/layout.tsx
 * ==========================================
 * Protected layout for all /trader/* pages except /trader/login.
 *
 * On mount, calls GET /api/auth/me. If unauthenticated (401), redirects to
 * /trader/login. Renders nothing until the auth check resolves so we never
 * flash protected content to an unauthenticated user.
 *
 * Once authenticated, renders: TraderProvider > Topbar > Sidebar > children.
 */

import { useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import Topbar from "../../components/Topbar";
import { TraderProvider, useTrader, STRATEGY_CATALOG, StrategyCatalogEntry } from "../context";
import { Chart as ChartJS, ArcElement } from "chart.js";
import { Doughnut } from "react-chartjs-2";

ChartJS.register(ArcElement);

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type AuthState = "loading" | "authed" | "unauthed";

// ─── Sidebar icons ──────────────────────────────────────────────────────────

function IconOverview({ color }: { color: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
      <rect x="1" y="1" width="4" height="4" rx="1" stroke={color} strokeWidth="1.2" />
      <rect x="7" y="1" width="4" height="4" rx="1" stroke={color} strokeWidth="1.2" />
      <rect x="1" y="7" width="4" height="4" rx="1" stroke={color} strokeWidth="1.2" />
      <rect x="7" y="7" width="4" height="4" rx="1" stroke={color} strokeWidth="1.2" />
    </svg>
  );
}

function IconStrategies({ color }: { color: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
      <path d="M2 2L6 1L10 2V8L6 11L2 8V2Z" stroke={color} strokeWidth="1.2" strokeLinejoin="round" />
      <path d="M6 1V11" stroke={color} strokeWidth="1" opacity="0.4" />
    </svg>
  );
}

function IconTraders({ color }: { color: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
      <polyline points="1,9 3.5,4 6,6.5 8.5,2 11,5" stroke={color} strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" fill="none" />
      <line x1="1" y1="11" x2="11" y2="11" stroke={color} strokeWidth="1" opacity="0.3" />
    </svg>
  );
}

function IconSettings({ color }: { color: string }) {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
      <circle cx="6" cy="6" r="2" stroke={color} strokeWidth="1.2" />
      <path d="M6 1V2.5M6 9.5V11M1 6H2.5M9.5 6H11M2.5 2.5L3.5 3.5M8.5 8.5L9.5 9.5M9.5 2.5L8.5 3.5M3.5 8.5L2.5 9.5" stroke={color} strokeWidth="1" strokeLinecap="round" />
    </svg>
  );
}

// ─── Nav item ────────────────────────────────────────────────────────────────

function NavItem({ label, href, active, icon }: { label: string; href: string; active: boolean; icon?: React.ReactNode }) {
  const router = useRouter();
  return (
    <button
      onClick={() => router.push(href)}
      style={{
        display: "flex", alignItems: "center", gap: 7,
        width: "100%",
        padding: "5px 8px 5px 14px",
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
      {icon}
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

// ─── Sidebar ─────────────────────────────────────────────────────────────────

function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const { instances, exchanges } = useTrader();
  const [collapsed, setCollapsed] = useState(false);
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
        <div style={{ flex: 1, overflowY: "auto" }}>
          {/* Sidebar donut */}
          {(() => {
            // Only active (validated + read-only) exchanges contribute to the balance donut.
            // pending_validation / invalid / errored rows are excluded — they don't represent real capital.
            const activeExchanges = exchanges.filter(e => e.status === "active");
            const totalBalance = activeExchanges.reduce((s, e) => s + e.balance, 0);
            const totalAllocated = instances.filter(i => i.status === "live" || i.status === "paused").reduce((s, i) => s + (i.allocation ?? 0), 0);
            const available = Math.max(0, totalBalance - totalAllocated);
            const hasExchanges = activeExchanges.length > 0;
            const fmtAbbrev = (n: number) => n >= 1000000 ? `$${(n / 1000000).toFixed(1)}m` : n >= 1000 ? `$${Math.round(n / 1000)}k` : `$${n}`;

            const donutData = hasExchanges ? {
              datasets: [{
                data: [totalAllocated, available],
                backgroundColor: ["#00c89630", "#1a1a1d"],
                borderColor: ["#00c896", "#242428"],
                borderWidth: 1.5,
                hoverOffset: 0,
              }],
            } : {
              datasets: [{
                data: [1],
                backgroundColor: ["#1a1a1d"],
                borderColor: ["#242428"],
                borderWidth: 1.5,
                hoverOffset: 0,
              }],
            };

            const fmtFull = (n: number) => "$" + Math.round(n).toLocaleString("en-US");

            return (
              <div style={{ padding: "14px 12px 10px", borderBottom: "0.5px solid var(--line)", display: "flex", flexDirection: "column", alignItems: "center" }}>
                <div style={{ position: "relative", width: 100, height: 100 }}>
                  <Doughnut
                    data={donutData}
                    width={100}
                    height={100}
                    options={{
                      responsive: false,
                      maintainAspectRatio: false,
                      cutout: "72%",
                      animation: { duration: 800 },
                      plugins: { legend: { display: false }, tooltip: { enabled: false } },
                    }}
                  />
                  <div style={{ position: "absolute", inset: 0, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", pointerEvents: "none" }}>
                    <span style={{ fontSize: 14, fontWeight: 700, color: "var(--t0)" }}>{fmtAbbrev(totalBalance)}</span>
                    <span style={{ fontSize: 8, color: "var(--t3)", marginTop: 1 }}>balance</span>
                  </div>
                </div>
                {hasExchanges ? (
                  <div style={{ width: "100%", marginTop: 10 }}>
                    {(() => {
                      const deployedPct = totalBalance > 0 ? totalAllocated / totalBalance : 0;
                      const deployedColor = deployedPct >= 0.8 ? "var(--green)" : "var(--amber)";
                      return [
                        { label: "DEPLOYED", value: fmtFull(totalAllocated), color: deployedColor },
                        { label: "AVAILABLE", value: fmtFull(available), color: "var(--t3)" },
                      ];
                    })().map((row, i) => (
                      <div key={row.label} style={{
                        display: "flex", alignItems: "center", justifyContent: "space-between",
                        padding: "5px 2px",
                        borderTop: i > 0 ? "0.5px solid var(--line)" : "none",
                      }}>
                        <span style={{ fontSize: 8, color: "var(--t3)", textTransform: "uppercase", letterSpacing: "0.06em" }}>{row.label}</span>
                        <span style={{ fontSize: 10, fontWeight: 700, color: row.color }}>{row.value}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <span style={{ fontSize: 8, color: "var(--t3)", marginTop: 8 }}>Link an exchange</span>
                )}
              </div>
            );
          })()}

          {/* TRADER section */}
          <div style={{ paddingTop: 4 }} />
          <NavItem label="Overview" href="/trader/overview" active={pathname === "/trader/overview"} icon={<IconOverview color={pathname === "/trader/overview" ? "var(--t0)" : "var(--t2)"} />} />

          {/* Divider */}
          <div style={{ borderTop: "1px solid var(--line)", margin: "8px 14px" }} />

          {/* Strategies — flat nav entry. Previously a collapsible
              with a chevron that expanded a sub-list of uninstantiated
              catalog strategies; removed per UX cleanup 2026-04-21.
              The Strategies index page at /trader/strategies still
              lists the same catalog inline. */}
          {(() => {
            const active = pathname.startsWith("/trader/strategies");
            return (
              <NavItem
                label="Strategies"
                href="/trader/strategies"
                active={active}
                icon={<IconStrategies color={active ? "var(--t0)" : "var(--t2)"} />}
              />
            );
          })()}

          {/* Divider */}
          <div style={{ borderTop: "1px solid var(--line)", margin: "8px 14px" }} />

          {/* Traders — collapsible */}
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
                  display: "flex", alignItems: "center", gap: 7,
                }}
                  onMouseEnter={e => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
                  onMouseLeave={e => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
                ><IconTraders color={active ? "var(--t0)" : "var(--t2)"} />Traders</button>
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
          <NavItem label="Settings" href="/trader/settings" active={pathname === "/trader/settings"} icon={<IconSettings color={pathname === "/trader/settings" ? "var(--t0)" : "var(--t2)"} />} />
        </div>
      )}
    </div>
  );
}

// ─── Protected layout with auth guard ───────────────────────────────────────

export default function TraderProtectedLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const [authState, setAuthState] = useState<AuthState>("loading");

  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/auth/me`, { credentials: "include" })
      .then((r) => {
        if (r.ok) return r.json();
        throw new Error("not authed");
      })
      .then((data) => {
        if (cancelled) return;
        if (data?.user_id) {
          setAuthState("authed");
        } else {
          setAuthState("unauthed");
          router.replace("/trader/login");
        }
      })
      .catch(() => {
        if (cancelled) return;
        setAuthState("unauthed");
        router.replace("/trader/login");
      });
    return () => { cancelled = true; };
  }, [router]);

  if (authState === "loading") {
    return (
      <div style={{
        minHeight: "100vh",
        background: "var(--bg0)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 9, color: "var(--t3)",
        textTransform: "uppercase", letterSpacing: "0.12em",
      }}>
        Verifying session...
      </div>
    );
  }

  if (authState === "unauthed") {
    return null;
  }

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

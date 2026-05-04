"use client";

import { useConfirm } from "../components/ConfirmDialog";
import { useSidebarCollapsed } from "../components/useSidebarCollapsed";
import { useAuth } from "../lib/auth";

const ALLOCATOR_PURPLE = "#a78bff";
const ALLOCATOR_PURPLE_SOFT = "rgba(167, 139, 255, 0.08)";
const RED_SOFT = "rgba(239, 68, 68, 0.06)";

// Topbar is sticky at top: 0 with height 46px (see Topbar.tsx). Sidebar
// sticks below it.
const TOPBAR_H = 46;

type NavId = "identity" | "profile" | "security" | "danger-zone";

const NAV: Array<{ id: NavId; label: string; danger?: boolean }> = [
  { id: "identity",     label: "Identity" },
  { id: "profile",      label: "Profile" },
  { id: "security",     label: "Security" },
  { id: "danger-zone",  label: "Danger Zone", danger: true },
];

export default function AccountSidebar({
  active,
  onNavigate,
}: {
  active: NavId;
  onNavigate: (id: NavId) => void;
}) {
  const [collapsed, setCollapsed] = useSidebarCollapsed();
  const { signout } = useAuth();
  const confirm = useConfirm();

  async function handleSignOut() {
    const ok = await confirm({
      eyebrow: "Session · Confirm",
      title: "Sign out?",
      description: "You'll need to sign back in to access your account.",
      confirmLabel: "Sign out",
    });
    if (ok) void signout();
  }

  return (
    <aside
      style={{
        width: collapsed ? 38 : 220,
        borderRight: "1px solid var(--line)",
        background: "var(--bg0)",
        // Sticky below the sticky topbar so the nav stays anchored on
        // long pages. align-self: flex-start because the parent is a
        // flex row and we don't want the sidebar to stretch to the
        // main column's height.
        position: "sticky",
        top: TOPBAR_H,
        alignSelf: "flex-start",
        height: `calc(100vh - ${TOPBAR_H}px)`,
        flexShrink: 0,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        transition: "width 0.2s ease",
      }}
    >
      <div
        style={{
          height: 40,
          borderBottom: "1px solid var(--line)",
          display: "flex",
          alignItems: "center",
          justifyContent: collapsed ? "center" : "space-between",
          padding: collapsed ? 0 : "0 8px 0 16px",
          flexShrink: 0,
        }}
      >
        {!collapsed && (
          <span
            style={{
              fontSize: 9,
              textTransform: "uppercase",
              letterSpacing: "0.12em",
              color: "var(--t3)",
              fontWeight: 700,
            }}
          >
            Account
          </span>
        )}
        <button
          type="button"
          onClick={() => setCollapsed((v) => !v)}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          style={{
            width: 24,
            height: 24,
            border: "1px solid var(--line2)",
            borderRadius: 3,
            background: "transparent",
            color: "var(--t1)",
            fontSize: 11,
            cursor: "pointer",
            fontFamily: "inherit",
          }}
        >
          {collapsed ? "»" : "«"}
        </button>
      </div>

      {!collapsed && (
        <>
          <nav style={{ flex: 1, overflowY: "auto", paddingTop: 8 }}>
            {NAV.map((item) => {
              const isActive = item.id === active;
              const accent = item.danger ? "var(--red)" : ALLOCATOR_PURPLE;
              const accentSoft = item.danger ? RED_SOFT : ALLOCATOR_PURPLE_SOFT;
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => onNavigate(item.id)}
                  style={{
                    display: "block",
                    width: "100%",
                    background: isActive ? accentSoft : "transparent",
                    border: "none",
                    borderLeft: `2px solid ${isActive ? accent : "transparent"}`,
                    color: isActive ? accent : "var(--t2)",
                    fontSize: 10,
                    fontWeight: 700,
                    letterSpacing: "0.14em",
                    textTransform: "uppercase",
                    textAlign: "left",
                    padding: "11px 18px 11px 16px",
                    cursor: "pointer",
                    fontFamily: "inherit",
                    transition: "color 0.12s ease, background 0.12s ease",
                  }}
                  onMouseEnter={(e) => {
                    if (isActive) return;
                    e.currentTarget.style.color = "var(--t0)";
                    e.currentTarget.style.background = "var(--bg2)";
                  }}
                  onMouseLeave={(e) => {
                    if (isActive) return;
                    e.currentTarget.style.color = "var(--t2)";
                    e.currentTarget.style.background = "transparent";
                  }}
                >
                  {item.label}
                </button>
              );
            })}
          </nav>

          <div style={{ borderTop: "1px solid var(--line)", padding: "12px 16px" }}>
            <button
              type="button"
              onClick={handleSignOut}
              onMouseEnter={(e) => {
                e.currentTarget.style.color = "var(--red)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.color = "var(--t3)";
              }}
              style={{
                background: "transparent",
                border: 0,
                color: "var(--t3)",
                fontSize: 9,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
                fontWeight: 700,
                cursor: "pointer",
                fontFamily: "inherit",
                padding: 0,
                transition: "color 0.12s ease",
              }}
            >
              Sign out
            </button>
          </div>
        </>
      )}

      {collapsed && (
        <div
          style={{
            flex: 1,
            display: "flex",
            alignItems: "flex-end",
            justifyContent: "center",
            paddingBottom: 8,
            pointerEvents: "none",
          }}
        >
          <div style={{
            fontSize: 8, color: "var(--t3)",
            transform: "rotate(-90deg)", whiteSpace: "nowrap",
            letterSpacing: "0.08em",
          }}>
            ACCOUNT
          </div>
        </div>
      )}
    </aside>
  );
}

export type { NavId };

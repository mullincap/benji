"use client";

/**
 * frontend/app/indexer/(protected)/layout.tsx
 * ===========================================
 * Protected layout for /indexer/* (everything except /indexer/login).
 *
 * On mount, calls GET /api/admin/whoami. If unauthenticated, replaces the
 * route with /indexer/login. Renders nothing visible until the auth check
 * resolves so we never flash protected content to an unauthenticated user.
 *
 * The layout chrome:
 *   - Topbar (shared component; module accent comes from the active theme via
 *     var(--module-accent), set by Topbar.applyAccent() on every pathname change)
 *   - IndexerSidebar (text-only nav: COVERAGE / JOBS / SIGNALS / STRATEGIES,
 *     active item left border uses var(--module-accent) so it follows the theme)
 *   - Content area (children render here)
 *
 * Sibling layout: app/indexer/(public)/layout.tsx wraps /indexer/login
 * with NO auth check and NO chrome. The route group parens "(protected)"
 * and "(public)" don't appear in the URL — both still serve under /indexer/.
 *
 * Auth is shared with the Compiler module: same admin_session cookie, same
 * require_admin dependency on the FastAPI side.
 */

import { useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import Topbar from "../../components/Topbar";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type AuthState = "loading" | "authed" | "unauthed";

// ─── Sidebar nav item ────────────────────────────────────────────────────────

function NavItem({ label, href, active }: { label: string; href: string; active: boolean }) {
  const router = useRouter();
  return (
    <button
      onClick={() => router.push(href)}
      style={{
        display: "block",
        width: "100%",
        background: "transparent",
        border: "none",
        borderLeft: `2px solid ${active ? "var(--module-accent)" : "transparent"}`,
        color: active ? "var(--t0)" : "var(--t2)",
        fontSize: 10,
        fontWeight: active ? 700 : 400,
        letterSpacing: "0.08em",
        textTransform: "uppercase",
        textAlign: "left",
        padding: "8px 14px 8px 16px",
        cursor: "pointer",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        transition: "color 0.15s ease",
      }}
      onMouseEnter={(e) => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
      onMouseLeave={(e) => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
    >
      {label}
    </button>
  );
}

// ─── Indexer sidebar — text-only, no icons ──────────────────────────────────

function IndexerSidebar() {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);
  return (
    <div
      style={{
        width: collapsed ? 38 : 288,
        borderRight: "1px solid var(--line)",
        overflow: "hidden",
        flexShrink: 0,
        transition: "width 0.2s ease",
        background: "var(--bg0)",
        display: "flex",
        flexDirection: "column",
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
            Indexer
          </span>
        )}
        <button
          onClick={() => setCollapsed((v) => !v)}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
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
          {collapsed ? "»" : "«"}
        </button>
      </div>
      {!collapsed && (
        <div style={{ flex: 1, overflowY: "auto", paddingTop: 8, display: "flex", flexDirection: "column" }}>
          <div>
            <NavItem label="Coverage"   href="/indexer/coverage"   active={pathname === "/indexer/coverage"} />
            <NavItem label="Signals"    href="/indexer/signals"    active={pathname === "/indexer/signals"} />
            <NavItem label="Strategies" href="/indexer/strategies" active={pathname === "/indexer/strategies"} />
          </div>
          <div style={{ marginTop: "auto", borderTop: "1px solid var(--line)", paddingTop: 4 }}>
            <NavItem label="Jobs"       href="/indexer/jobs"       active={pathname === "/indexer/jobs"} />
          </div>
        </div>
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
          <div style={{ fontSize: 8, color: "var(--t3)", transform: "rotate(-90deg)", whiteSpace: "nowrap", letterSpacing: "0.08em" }}>
            SIDEBAR
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Layout ──────────────────────────────────────────────────────────────────

export default function IndexerProtectedLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const [authState, setAuthState] = useState<AuthState>("loading");

  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/admin/whoami`, { credentials: "include" })
      .then((r) => r.json())
      .then((data) => {
        if (cancelled) return;
        if (data?.authenticated) {
          setAuthState("authed");
        } else {
          setAuthState("unauthed");
          router.replace("/indexer/login");
        }
      })
      .catch(() => {
        if (cancelled) return;
        setAuthState("unauthed");
        router.replace("/indexer/login");
      });
    return () => {
      cancelled = true;
    };
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
        Verifying session…
      </div>
    );
  }

  if (authState === "unauthed") {
    // Redirect is in flight — render nothing rather than flashing layout chrome
    return null;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Topbar />
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        <IndexerSidebar />
        <div style={{ flex: 1, overflow: "auto" }}>
          {children}
        </div>
      </div>
    </div>
  );
}

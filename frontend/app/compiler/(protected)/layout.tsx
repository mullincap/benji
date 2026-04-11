"use client";

/**
 * frontend/app/compiler/(protected)/layout.tsx
 * ============================================
 * Protected layout for /compiler/* (everything except /compiler/login).
 *
 * On mount, calls GET /api/admin/whoami. If unauthenticated, replaces the
 * route with /compiler/login. Renders nothing visible until the auth check
 * resolves so we never flash protected content to an unauthenticated user.
 *
 * The layout chrome:
 *   - Topbar (shared component; module accent comes from the active theme via
 *     var(--module-accent), set by Topbar.applyAccent() on every pathname change)
 *   - CompilerSidebar (text-only nav: COVERAGE / JOBS / SYMBOLS, active item
 *     left border uses var(--module-accent) so it follows the theme)
 *   - Content area (children render here)
 *
 * Sibling layout: app/compiler/(public)/layout.tsx wraps /compiler/login
 * with NO auth check and NO chrome. The route group parens "(protected)"
 * and "(public)" don't appear in the URL — both still serve under /compiler/.
 */

import { useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import Topbar from "../../components/Topbar";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// The compiler module's accent comes from the active theme via the
// `--module-accent` CSS variable that Topbar.tsx sets on :root whenever the
// active route is under /compiler/*. We don't hardcode a hex here — that
// would diverge from the theme system the user controls.

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

// ─── Compiler sidebar — text-only, no icons ─────────────────────────────────

function CompilerSidebar() {
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
            Compiler
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
        <div style={{ flex: 1, overflowY: "auto", paddingTop: 8 }}>
          <NavItem label="Coverage" href="/compiler/coverage" active={pathname === "/compiler/coverage"} />
          <NavItem label="Jobs"     href="/compiler/jobs"     active={pathname === "/compiler/jobs"} />
          <NavItem label="Symbols"  href="/compiler/symbols"  active={pathname === "/compiler/symbols"} />
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

export default function CompilerProtectedLayout({ children }: { children: React.ReactNode }) {
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
          router.replace("/compiler/login");
        }
      })
      .catch(() => {
        if (cancelled) return;
        setAuthState("unauthed");
        router.replace("/compiler/login");
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
        <CompilerSidebar />
        <div style={{ flex: 1, overflow: "auto" }}>
          {children}
        </div>
      </div>
    </div>
  );
}

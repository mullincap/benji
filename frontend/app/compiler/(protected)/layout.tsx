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
 *   - Topbar (shared component, hardcoded amber accent for compiler module)
 *   - CompilerSidebar (text-only nav: COVERAGE / JOBS / SYMBOLS, amber active border)
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
  return (
    <div style={{
      width: 200,
      flexShrink: 0,
      background: "var(--bg0)",
      borderRight: "1px solid var(--line)",
      display: "flex",
      flexDirection: "column",
      paddingTop: 14,
    }}>
      <div style={{
        padding: "0 16px 10px",
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase",
      }}>
        Compiler
      </div>
      <NavItem label="Coverage" href="/compiler/coverage" active={pathname === "/compiler/coverage"} />
      <NavItem label="Jobs"     href="/compiler/jobs"     active={pathname === "/compiler/jobs"} />
      <NavItem label="Symbols"  href="/compiler/symbols"  active={pathname === "/compiler/symbols"} />
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

"use client";

/**
 * frontend/app/admin/layout.tsx
 * ==============================
 * Admin Console shell.
 *
 * Three gates:
 *   1. Middleware (frontend/middleware.ts) bounces unauthenticated users
 *      to /auth/signin?next=<path>. So by the time we render here, the
 *      caller has at least a session cookie.
 *   2. This layout reads /api/auth/me (via auth context) and verifies
 *      `is_admin === true`. Non-admins get router.replace('/'). Cookie
 *      can't tell us is_admin alone — that's why this layout exists.
 *   3. Page components inside still wait for `loading === false` before
 *      firing admin API calls. The auth-bootstrap race noted in the
 *      Phase 1b spec hits here hardest because admin pages fire on
 *      mount.
 *
 * Children get a light-touch wrapper (max-width, padding, footer);
 * the topbar comes from the existing app-wide Topbar component which
 * surfaces the ADMIN tab when user.is_admin === true.
 */

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";

import Topbar from "../components/Topbar";
import { useAuth } from "../lib/auth";

const BUILD_HASH = process.env.NEXT_PUBLIC_BUILD_HASH || "dev";

export default function AdminLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const { user, loading } = useAuth();

  // Redirect non-admins to /. Middleware already kicked unauthed callers
  // to /auth/signin, so by the time we get here we either have an
  // admin user OR a regular user who shouldn't be seeing this.
  useEffect(() => {
    if (loading) return;
    if (!user) {
      // Edge case: cookie present but /me returned 401 (e.g. expired
      // server-side). Bounce to signin with a next-param.
      router.replace("/auth/signin?next=/admin");
      return;
    }
    if (!user.is_admin) {
      router.replace("/");
    }
  }, [loading, user, router]);

  // Render nothing until we've confirmed the user is an admin. Avoids
  // the admin-bootstrap race where pages fire admin API calls before
  // /me has resolved.
  if (loading || !user || !user.is_admin) {
    return (
      <div
        style={{
          minHeight: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--t3)",
          fontSize: 11,
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        Verifying admin access…
      </div>
    );
  }

  return (
    <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>
      <Topbar />
      <AdminSubNav />
      <main
        style={{
          flex: 1,
          padding: "24px 32px",
          maxWidth: 1600,
          width: "100%",
          margin: "0 auto",
        }}
      >
        {children}
      </main>
      <footer
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "16px 32px",
          borderTop: "1px solid var(--line)",
          color: "var(--t3)",
          fontSize: 10,
          letterSpacing: "0.06em",
        }}
      >
        <span>mullincap.com · admin · build {BUILD_HASH}</span>
        <span>signed in as {user.email}</span>
      </footer>
    </div>
  );
}

// ─── Cross-section sub-nav ────────────────────────────────────────────────
//
// Sibling-sibling nav between the three admin surfaces (Users /
// Invitations / Audit). Lives in the layout so every /admin/* page
// gets it without per-page wiring.
//
// Active-tab logic uses startsWith so detail routes
// (/admin/users/{user_id}) keep "Users" highlighted.
//
// Visual: full-width strip below the topbar, left-aligned to the
// content edge via the same padding as the page main.

const TABS: Array<{ key: string; label: string; href: string; match: (p: string) => boolean }> = [
  { key: "users",       label: "Users",       href: "/admin/users",       match: (p) => p === "/admin" || p.startsWith("/admin/users") },
  { key: "invitations", label: "Invitations", href: "/admin/invitations", match: (p) => p.startsWith("/admin/invitations") },
  { key: "audit",       label: "Audit Log",   href: "/admin/audit",       match: (p) => p.startsWith("/admin/audit") },
];

function AdminSubNav() {
  const router = useRouter();
  const pathname = usePathname();

  return (
    <nav
      aria-label="Admin sections"
      style={{
        // Sticky directly below the topbar (PR #46 made the topbar sticky
        // at top:0; topbar height = 46px). z-40 sits below the topbar's
        // z-50 so the topbar always wins on overlap, and above page
        // content's default stacking. Background must be opaque so page
        // content scrolling underneath doesn't bleed through.
        position: "sticky",
        top: 46,
        zIndex: 40,
        display: "flex",
        gap: 0,
        padding: "0 32px",
        borderBottom: "1px solid var(--line)",
        background: "var(--bg1)",
      }}
    >
      {TABS.map((tab) => {
        const isActive = tab.match(pathname);
        return (
          <button
            key={tab.key}
            type="button"
            onClick={() => router.push(tab.href)}
            style={{
              padding: "12px 16px",
              fontSize: 10,
              letterSpacing: "0.16em",
              textTransform: "uppercase",
              fontWeight: 700,
              color: isActive ? "var(--amber)" : "var(--t3)",
              background: "transparent",
              border: 0,
              borderBottom: isActive ? "2px solid var(--amber)" : "2px solid transparent",
              cursor: "pointer",
              fontFamily: "inherit",
              marginBottom: -1,
              transition: "color 0.12s ease",
            }}
            onMouseEnter={(e) => {
              if (!isActive) e.currentTarget.style.color = "var(--t0)";
            }}
            onMouseLeave={(e) => {
              if (!isActive) e.currentTarget.style.color = "var(--t3)";
            }}
          >
            {tab.label}
          </button>
        );
      })}
    </nav>
  );
}

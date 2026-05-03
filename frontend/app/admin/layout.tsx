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
import { useRouter } from "next/navigation";

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

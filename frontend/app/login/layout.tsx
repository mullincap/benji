/**
 * frontend/app/login/layout.tsx
 * =============================
 * Neutral admin login chrome — Topbar + centered content.
 *
 * Shared login for compiler / indexer / manager. No module chrome, no module
 * accent (the Topbar detects no active module from the path and falls back to
 * --t0 for --module-accent, which the login form uses neutrally).
 */

import Topbar from "../components/Topbar";

export default function LoginLayout({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      minHeight: "100vh",
      background: "var(--bg0)",
      display: "flex",
      flexDirection: "column",
    }}>
      <Topbar />
      <div style={{
        flex: 1,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}>
        {children}
      </div>
    </div>
  );
}

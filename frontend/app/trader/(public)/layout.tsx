/**
 * frontend/app/trader/(public)/layout.tsx
 * =======================================
 * Layout for the public sub-tree of /trader/* — currently only /trader/login.
 *
 * No auth check, no sidebar. Renders the shared Topbar at the top, then
 * centers the children (login form) in the remaining space.
 *
 * Sibling layout: app/trader/(protected)/layout.tsx wraps all other /trader/*
 * pages with an auth guard + sidebar. The route group parens "(public)" do not
 * appear in the URL — the login page is reachable at /trader/login.
 */

import Topbar from "../../components/Topbar";

export default function TraderPublicLayout({ children }: { children: React.ReactNode }) {
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

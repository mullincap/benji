/**
 * frontend/app/compiler/(public)/layout.tsx
 * =========================================
 * Layout for the public sub-tree of /compiler/* — currently only /compiler/login.
 *
 * Deliberately minimal: no auth check, no sidebar, no topbar. Just renders
 * children inside a full-viewport flex container so the login form can be
 * centered. This sibling-of-(protected) route group exists specifically so
 * the login page is NOT wrapped by the protected layout (which would create
 * an infinite redirect loop).
 *
 * The route group parens "(public)" do not appear in the URL — the login
 * page is reachable at /compiler/login.
 */

export default function CompilerPublicLayout({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      minHeight: "100vh",
      background: "var(--bg0)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
    }}>
      {children}
    </div>
  );
}

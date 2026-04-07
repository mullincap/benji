/**
 * frontend/app/compiler/(public)/layout.tsx
 * =========================================
 * Layout for the public sub-tree of /compiler/* — currently only /compiler/login.
 *
 * No auth check, no sidebar. Renders the shared Topbar at the top so the user
 * can see they're in the Compiler module, then centers the children (login form)
 * in the remaining space. The Topbar's accent-resolution logic sets the
 * --module-accent CSS variable to the active theme's compiler color, which the
 * login form's input/button then consume.
 *
 * This sibling-of-(protected) route group exists specifically so the login page
 * is NOT wrapped by the protected layout (which would create an infinite
 * redirect loop). The route group parens "(public)" do not appear in the URL —
 * the login page is reachable at /compiler/login.
 */

import Topbar from "../../components/Topbar";

export default function CompilerPublicLayout({ children }: { children: React.ReactNode }) {
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

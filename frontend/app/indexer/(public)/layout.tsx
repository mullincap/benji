/**
 * frontend/app/indexer/(public)/layout.tsx
 * ========================================
 * Layout for the public sub-tree of /indexer/* — currently only /indexer/login.
 *
 * No auth check, no sidebar. Renders the shared Topbar at the top so the user
 * can see they're in the Indexer module (and so Topbar.applyAccent() sets the
 * --module-accent CSS variable for the active theme's indexer color), then
 * centers the children in the remaining space.
 *
 * In practice the indexer login page is just a stub that redirects to
 * /compiler/login (auth is shared via the admin_session cookie), but this
 * sibling-of-(protected) route group still exists so the redirect path is
 * NOT wrapped by the protected layout — which would create an infinite
 * redirect loop. The route group parens "(public)" do not appear in the URL.
 */

import Topbar from "../../components/Topbar";

export default function IndexerPublicLayout({ children }: { children: React.ReactNode }) {
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

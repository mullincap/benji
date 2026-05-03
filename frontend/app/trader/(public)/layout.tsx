/**
 * frontend/app/trader/(public)/layout.tsx
 * =======================================
 * Layout for the public sub-tree of /trader/* — currently only the
 * /trader/login redirect stub.
 *
 * Pre-Phase-1a this layout wrapped a real signin form. Phase 1a
 * unified signin at /auth/signin (PR #15); the only remaining child
 * here is the redirect-only page that bounces stale bookmarks to
 * /auth/signin. The layout itself is largely vestigial — kept until
 * the /trader/login URL is decommissioned entirely.
 *
 * No auth check, no sidebar. Renders the shared Topbar at the top
 * so a user who lands here briefly during the redirect doesn't see
 * a chrome-less page, then centers the (effectively empty) child.
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

/**
 * frontend/app/(auth)/layout.tsx
 * ==============================
 * Wraps every /auth/* route. Applies the auth-shell palette scope
 * (defined in globals.css), renders the topbar (3M mark + status pill)
 * and the page footer (build hash + legal links), and centers a single
 * column of card content via {children}.
 *
 * Server Component — no client-side state needed at the layout level.
 * Pages inside the group ("use client") handle their own form state.
 */

import Link from "next/link";

const BUILD_HASH = process.env.NEXT_PUBLIC_BUILD_HASH || "dev";

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="auth-shell"
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        padding: "32px 24px 32px",
        minHeight: "100vh",
      }}
    >
      <header
        style={{
          width: "100%",
          maxWidth: 1200,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 8px",
          marginBottom: 56,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <Link
            href="/"
            style={{
              fontWeight: 700,
              fontSize: 14,
              letterSpacing: "0.04em",
              color: "var(--t0)",
              textDecoration: "none",
            }}
          >
            3M
          </Link>
          <span
            style={{
              color: "var(--t3)",
              fontSize: 10,
              letterSpacing: "0.18em",
              textTransform: "uppercase",
              paddingLeft: 12,
              borderLeft: "1px solid var(--line)",
            }}
          >
            Authentication
          </span>
        </div>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            color: "var(--t2)",
            fontSize: 11,
            letterSpacing: "0.04em",
          }}
        >
          <span
            style={{
              width: 6,
              height: 6,
              borderRadius: "50%",
              background: "var(--green)",
              boxShadow: "0 0 8px var(--green)",
              animation: "pulse-dot 2s ease-in-out infinite",
            }}
          />
          auth.mullincap.com
        </div>
      </header>

      <main
        style={{
          flex: 1,
          width: "100%",
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "center",
          padding: "16px 0",
        }}
      >
        {children}
      </main>

      <footer
        style={{
          width: "100%",
          maxWidth: 1200,
          marginTop: 56,
          padding: "0 8px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          color: "var(--t3)",
          fontSize: 10,
          letterSpacing: "0.06em",
        }}
      >
        <span>mullincap.com · auth · build {BUILD_HASH}</span>
        <div style={{ display: "flex", gap: 20 }}>
          <FootLink href="/health">Status</FootLink>
          <FootLink href="/security">Security</FootLink>
          <FootLink href="/privacy">Privacy</FootLink>
          <FootLink href="/terms">Terms</FootLink>
        </div>
      </footer>
    </div>
  );
}

function FootLink({ href, children }: { href: string; children: React.ReactNode }) {
  return (
    <Link
      href={href}
      style={{
        color: "var(--t3)",
        textDecoration: "none",
      }}
    >
      {children}
    </Link>
  );
}

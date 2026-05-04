"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

import Topbar from "../components/Topbar";
import { useAuth } from "../lib/auth";

export default function AccountShell({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const { user, loading } = useAuth();

  useEffect(() => {
    if (loading) return;
    if (!user) router.replace("/auth/signin?next=/account");
  }, [loading, user, router]);

  if (loading || !user) {
    return (
      <div style={{ background: "var(--bg0)", minHeight: "100vh" }} />
    );
  }

  return (
    <div
      style={{
        background: "var(--bg0)",
        // Subtle radial dot grid — same pattern auth pages use via the
        // .auth-shell class. Applied here too because the account page
        // is wide + sparse on large screens; flat solid bg felt empty.
        // Inline (not a shared class) so the trader/admin data-dashboard
        // surfaces stay solid — they want a flat backdrop for charts.
        backgroundImage:
          "radial-gradient(circle, rgba(36, 36, 40, 0.5) 1px, transparent 1px)",
        backgroundSize: "24px 24px",
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <Topbar />
      {children}
    </div>
  );
}

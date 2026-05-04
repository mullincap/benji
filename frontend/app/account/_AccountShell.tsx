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
    // Loading branch: keep the dot-grid backdrop so the post-auth handoff
    // doesn't briefly flash a flat solid bg before the post-auth shell
    // mounts. Same gradient values as the post-auth render below.
    return (
      <div
        style={{
          background: "var(--bg0)",
          backgroundImage:
            "radial-gradient(circle, rgba(36, 36, 40, 0.5) 1px, transparent 1px)",
          backgroundSize: "24px 24px",
          minHeight: "100vh",
        }}
      />
    );
  }

  return (
    <div
      style={{
        background: "var(--bg0)",
        // Subtle radial dot grid — matches the .auth-shell pattern in
        // globals.css. Sidebar overrides this with an opaque var(--bg0)
        // background so the dotted body shows only behind the main
        // content area where the sparse layout would otherwise feel
        // empty on wide viewports.
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

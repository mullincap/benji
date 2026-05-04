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

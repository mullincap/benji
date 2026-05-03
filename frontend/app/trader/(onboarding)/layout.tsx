"use client";

/**
 * frontend/app/trader/(onboarding)/layout.tsx
 * ===========================================
 * Layout for /trader/get-started — the 1-step onboarding hero page.
 *
 * Auth-gates the same way (protected)/layout.tsx does (its own /me
 * fetch, since the Trader tab does not consume useAuth() — see the
 * AUTH_CONTEXT_DUAL_SOURCE polish item filed for a later session).
 *
 * Bidirectional redirect partner: (protected)/layout.tsx sends users
 * to /trader/get-started when has_exchange is false. This layout
 * sends them BACK to /trader/overview once has_exchange flips true,
 * so a user who completes the link flow doesn't sit on the hero
 * page after returning from the OAuth/keys round-trip.
 *
 * No sidebar — the hero page is intentionally chrome-light. Just
 * Topbar + centered children.
 */

import { useEffect, useRef, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import Topbar from "../../components/Topbar";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type AuthState = "loading" | "authed" | "unauthed";

export default function TraderOnboardingLayout({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const [authState, setAuthState] = useState<AuthState>("loading");

  const initialPathRef = useRef(pathname);

  useEffect(() => {
    let cancelled = false;
    const signinHref = "/auth/signin?next=" + encodeURIComponent(initialPathRef.current);
    fetch(`${API_BASE}/api/auth/me`, { credentials: "include" })
      .then((r) => {
        if (r.ok) return r.json();
        throw new Error("not authed");
      })
      .then((data) => {
        if (cancelled) return;
        if (data?.user_id) {
          // If the user already has an exchange linked, the onboarding
          // hero is no longer the right destination — bounce back to
          // /trader/overview where the contextual nudge banner takes
          // over the "next step" guidance.
          if (data.has_exchange) {
            setAuthState("unauthed"); // suppress flash; layout returns null
            router.replace("/trader/overview");
            return;
          }
          setAuthState("authed");
        } else {
          setAuthState("unauthed");
          router.replace(signinHref);
        }
      })
      .catch(() => {
        if (cancelled) return;
        setAuthState("unauthed");
        router.replace(signinHref);
      });
    return () => { cancelled = true; };
  }, [router]);

  if (authState === "loading") {
    return (
      <div style={{
        minHeight: "100vh",
        background: "var(--bg0)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 9, color: "var(--t3)",
        textTransform: "uppercase", letterSpacing: "0.12em",
      }}>
        Verifying session...
      </div>
    );
  }

  if (authState === "unauthed") {
    return null;
  }

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
        alignItems: "flex-start",
        justifyContent: "center",
      }}>
        {children}
      </div>
    </div>
  );
}

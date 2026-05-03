"use client";

/**
 * frontend/app/lib/auth.tsx
 * =========================
 * Top-level auth context. AuthProvider wraps the root layout's children
 * and makes the current user available everywhere via useAuth().
 *
 * Shape:
 *   const { user, loading, refetch, signout } = useAuth();
 *
 * - `user` is null when unauthenticated or while loading.
 * - `loading` is true on initial mount until the first /me response.
 * - `refetch()` re-fetches /me on demand (e.g. after profile edit).
 * - `signout()` POSTs /logout, clears local state, redirects to /auth/signin.
 *
 * SSR note: this is "use client" so the provider hydrates with
 * { user: null, loading: true }. Consumers must handle the loading state.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { useRouter } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

export type AuthUser = {
  user_id: string;
  email: string;
  first_login: boolean;
  first_name: string | null;
  last_name: string | null;
  firm: string | null;
  role: string | null;
  is_admin: boolean;
  password_is_temporary: boolean;
  has_exchange: boolean;
};

type AuthContextValue = {
  user: AuthUser | null;
  loading: boolean;
  refetch: () => Promise<void>;
  signout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  const refetch = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(API_BASE + "/api/auth/me", { credentials: "include" });
      if (res.ok) {
        setUser((await res.json()) as AuthUser);
      } else {
        setUser(null);
      }
    } catch {
      setUser(null);
    } finally {
      setLoading(false);
    }
  }, []);

  const signout = useCallback(async () => {
    try {
      await fetch(API_BASE + "/api/auth/logout", {
        method: "POST",
        credentials: "include",
      });
    } catch {
      // Best-effort — even if the call fails we still clear local state.
    }
    // Clear the per-session trader onboarding flags so the next
    // sign-in in this tab gets a fresh onboarding experience:
    //   - onboarding_skipped: the get-started "Skip — explore first" flag
    //   - onboarding_nudge_dismissed:<kind>: per-session dismiss for the
    //     "pick a strategy" / "deploy capital" purple banners
    // The localStorage permanent-dismiss for the "you're live" green
    // banner is namespaced by user_id and intentionally preserved here.
    // (Source of truth: frontend/app/trader/_lib/onboarding.ts and
    // frontend/app/trader/components/OnboardingNudge.tsx.)
    if (typeof window !== "undefined") {
      sessionStorage.removeItem("onboarding_skipped");
      sessionStorage.removeItem("onboarding_nudge_dismissed:pick");
      sessionStorage.removeItem("onboarding_nudge_dismissed:deploy");
    }
    setUser(null);
    router.replace("/auth/signin");
  }, [router]);

  useEffect(() => {
    refetch();
  }, [refetch]);

  return (
    <AuthContext.Provider value={{ user, loading, refetch, signout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth must be used within an <AuthProvider>");
  }
  return ctx;
}

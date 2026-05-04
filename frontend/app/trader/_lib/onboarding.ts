"use client";

/**
 * frontend/app/trader/_lib/onboarding.ts
 * =======================================
 * useOnboardingState() — typed hook over GET /api/onboarding/state.
 *
 * Plain useState + useEffect (no SWR / React Query in this project).
 * Exposes a manual `refetch()` so callers can trigger a re-fetch after
 * mutations — exchange-link success, strategy selection, allocation
 * creation. Without explicit refetch, the AuthProvider-staleness bug
 * pattern from PR #21 reappears here.
 *
 * Skip flag — a sessionStorage entry that pauses the redirect from
 * /trader/overview → /trader/get-started. Set by the "Skip — explore
 * first" link on the get-started page; cleared on logout. Honored by
 * the trader (protected) layout's redirect logic.
 */

import { useCallback, useEffect, useState } from "react";

import { apiFetch } from "../../lib/api-fetch";

export type OnboardingState = {
  has_exchange: boolean;
  has_selected_strategy: boolean;
  selected_strategy_id: string | null;
  // selected_strategy_slug is the catalog slug used to build
  // /trader/strategies/<slug> deep links. selected_strategy_id is a
  // UUID and would 404 if used as the URL path. The two fields can
  // currently equal selected_strategy_name (all sourced from the same
  // audit.strategies.name column), but they're surfaced under
  // dedicated keys so future schema changes don't silently break
  // routing.
  selected_strategy_slug: string | null;
  selected_strategy_name: string | null;
  selected_strategy_version: string | null;
  selected_strategy_sharpe: number | null;
  has_active_allocation: boolean;
  // active_allocation_strategy_name is the strategy on the user's most
  // recent active allocation — populated only when has_active_allocation
  // is true. Distinct from selected_strategy_name (which is cleared at
  // allocation-create time); the live banner reads this field instead.
  active_allocation_strategy_name: string | null;
};

export const SKIP_FLAG_KEY = "onboarding_skipped";

export function isOnboardingSkipped(): boolean {
  if (typeof window === "undefined") return false;
  return sessionStorage.getItem(SKIP_FLAG_KEY) === "true";
}

export function setOnboardingSkipped(): void {
  if (typeof window === "undefined") return;
  sessionStorage.setItem(SKIP_FLAG_KEY, "true");
}

export function clearOnboardingSkipped(): void {
  if (typeof window === "undefined") return;
  sessionStorage.removeItem(SKIP_FLAG_KEY);
}

// ─── Mutations ──────────────────────────────────────────────────────────────

// Window event used as a tiny pub/sub so every mounted useOnboardingState()
// instance refetches after a mutation. Without this, the OnboardingNudge
// (mounted in the trader layout) and the strategy detail page each have
// their own useState — the detail page can refetch but the banner stays
// stale until pathname changes. The event bus is broadcast at mutation
// site (selectStrategy) and listened to inside the hook.
export const ONBOARDING_REFETCH_EVENT = "onboarding-state:refetch";

function broadcastOnboardingRefetch(): void {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new CustomEvent(ONBOARDING_REFETCH_EVENT));
}

export async function selectStrategy(strategyVersionId: string): Promise<void> {
  const res = await apiFetch("/api/onboarding/select-strategy", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ strategy_version_id: strategyVersionId }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`select-strategy failed (HTTP ${res.status}): ${detail || res.statusText}`);
  }
  broadcastOnboardingRefetch();
}

type Status = "loading" | "ready" | "error";

export function useOnboardingState() {
  const [state, setState] = useState<OnboardingState | null>(null);
  const [status, setStatus] = useState<Status>("loading");
  const [error, setError] = useState<string | null>(null);

  const refetch = useCallback(async () => {
    setStatus("loading");
    setError(null);
    try {
      const res = await apiFetch("/api/onboarding/state");
      if (!res.ok) {
        // 401s on this hook are common (page mounts before auth resolves);
        // leave state null so consumers can render the "we don't know yet"
        // path. apiFetch handles redirecting to /auth/signin for true
        // 401s on app routes.
        setState(null);
        setStatus("error");
        setError(`HTTP ${res.status}`);
        return;
      }
      const data = (await res.json()) as OnboardingState;
      setState(data);
      setStatus("ready");
    } catch (err) {
      setStatus("error");
      setError(err instanceof Error ? err.message : String(err));
      setState(null);
    }
  }, []);

  useEffect(() => {
    // Initial fetch on mount. The lint rule (set-state-in-effect) flags
    // this because refetch() begins with setStatus("loading") + setError(null);
    // those resets are intentional — they make refetch reusable post-mount
    // (after exchange link, strategy select, etc.) without keeping a stale
    // error/state from the previous call. Same shape as the AuthProvider's
    // refetch in app/lib/auth.tsx.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    refetch();
  }, [refetch]);

  // Subscribe to mutation broadcasts so every hook instance refetches
  // when selectStrategy (or any future mutation that calls
  // broadcastOnboardingRefetch) lands. Without this, clicking ALLOCATE
  // on the strategy detail page mutates `selected_strategy_id` but the
  // OnboardingNudge banner — whose own useState is independent — stays
  // on the previous "Pick a fund" copy until the user navigates away.
  useEffect(() => {
    if (typeof window === "undefined") return;
    const handler = () => { void refetch(); };
    window.addEventListener(ONBOARDING_REFETCH_EVENT, handler);
    return () => window.removeEventListener(ONBOARDING_REFETCH_EVENT, handler);
  }, [refetch]);

  return { state, status, error, refetch };
}

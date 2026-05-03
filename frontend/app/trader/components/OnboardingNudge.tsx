"use client";

/**
 * frontend/app/trader/components/OnboardingNudge.tsx
 * ===================================================
 * Contextual banner shown on /trader/overview that walks a freshly-
 * linked user through the remaining onboarding steps:
 *
 *   has_exchange + !selected_strategy + !active_allocation
 *     → "Pick a strategy" (allocator-purple)
 *
 *   has_exchange + selected_strategy + !active_allocation
 *     → "Finish setup" (allocator-purple, interpolates strategy name/version)
 *     Frames the state as "you've added it to your traders, the wizard
 *     just isn't done yet" — which is what SYNC CAPITAL leaves behind
 *     when the user opens the wizard and navigates away mid-flow.
 *
 *   has_active_allocation
 *     → "You're live" (green, permanent dismiss)
 *
 * State source: useOnboardingState() — exclusively. While the hook
 * is loading or in error, the banner renders nothing rather than
 * flashing a stale or broken state to the user.
 *
 * Path gating: only mounts on /trader/overview. The component lives
 * in the (protected) layout (so allocator state is fetched once
 * across nav), but the banner only renders when the user is on the
 * dashboard route — pointing them to the catalog while they're
 * already in the catalog would be redundant.
 *
 * Dismiss persistence:
 *   - Purple banners (pick / deploy) → sessionStorage, keyed per
 *     banner kind. Dismissing "Pick a strategy" does NOT carry over
 *     to "Finish setup" (different key); the next banner shows
 *     fresh after the user adds a strategy via SYNC CAPITAL.
 *   - Green banner (live) → localStorage, namespaced by user_id from
 *     useAuth(). Survives sign-out and sign-back-in for the SAME
 *     user (one-time "you made it" celebration), but a different
 *     user signing in on the same browser still gets their own
 *     celebration. Skip flag clearing on logout (auth.tsx) does NOT
 *     touch this key.
 *
 * Refetch on path change to /trader/overview: the hook fires once
 * at component mount, but the layout-mounted nudge stays alive
 * across nav. When the user returns from /trader/strategies (e.g.
 * after picking a strategy in commit 5), we re-fetch so the banner
 * reflects the new state. Mutation sites in commits 5/6 will also
 * call refetch() directly to avoid the round-trip-via-overview
 * dependency.
 */

import { useCallback, useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";

import { useAuth } from "../../lib/auth";
import { useOnboardingState, type OnboardingState } from "../_lib/onboarding";

type BannerKind = "pick" | "deploy" | "live";

const SESSION_DISMISS_PREFIX = "onboarding_nudge_dismissed:";
const PERMANENT_DISMISS_PREFIX = "onboarding_nudge_dismissed:live:";
const NUDGE_PATH = "/trader/overview";

function deriveKind(state: OnboardingState): BannerKind | null {
  if (!state.has_exchange) return null;          // get-started page handles this case
  if (state.has_active_allocation) return "live";
  if (state.has_selected_strategy) return "deploy";
  return "pick";
}

// The "live" banner's permanent dismiss is namespaced by user_id so a
// second user signing in on the same browser still gets the celebration.
// userId may be null while AuthProvider is resolving — we treat null
// reads as "not dismissed" (banner can show; once auth resolves we'll
// re-render with the real value) and null writes as no-ops (the banner
// will simply reappear once auth resolves; acceptable for v1).
function readDismissed(kind: BannerKind, userId: string | null): boolean {
  if (typeof window === "undefined") return false;
  if (kind === "live") {
    if (!userId) return false;
    return localStorage.getItem(PERMANENT_DISMISS_PREFIX + userId) === "true";
  }
  return sessionStorage.getItem(SESSION_DISMISS_PREFIX + kind) === "true";
}

function writeDismissed(kind: BannerKind, userId: string | null): void {
  if (typeof window === "undefined") return;
  if (kind === "live") {
    if (!userId) return;
    localStorage.setItem(PERMANENT_DISMISS_PREFIX + userId, "true");
  } else {
    sessionStorage.setItem(SESSION_DISMISS_PREFIX + kind, "true");
  }
}

function readAllDismissed(userId: string | null): Set<BannerKind> {
  const s = new Set<BannerKind>();
  if (readDismissed("pick",   userId)) s.add("pick");
  if (readDismissed("deploy", userId)) s.add("deploy");
  if (readDismissed("live",   userId)) s.add("live");
  return s;
}

// ─── Component ──────────────────────────────────────────────────────────────

export default function OnboardingNudge() {
  const pathname = usePathname();
  const { state, status, refetch } = useOnboardingState();
  const { user } = useAuth();
  const userId = user?.user_id ?? null;

  // Hydrate-safely lazy-init from storage. SSR returns an empty Set;
  // the effect below pulls in the real values once the window exists,
  // so the first paint never reads stale localStorage on the server.
  // Re-pulls when userId resolves so the live-banner localStorage key
  // (namespaced per user) is read with the correct user context.
  // (set-state-in-effect is intentional here — we are syncing FROM
  // an external system (Web Storage) into React state, which is one
  // of the rule's documented "OK" cases.)
  const [dismissed, setDismissed] = useState<Set<BannerKind>>(() => new Set());
  useEffect(() => {
    setDismissed(readAllDismissed(userId));
  }, [userId]);

  const dismiss = useCallback((kind: BannerKind) => {
    writeDismissed(kind, userId);
    setDismissed(prev => {
      const next = new Set(prev);
      next.add(kind);
      return next;
    });
  }, [userId]);

  // Refetch when the user returns to /trader/overview from another
  // protected route (e.g. /trader/strategies after picking a strategy).
  // Mutation sites in commits 5/6 will also call refetch() directly,
  // but this covers the layout-stays-mounted-across-nav case.
  useEffect(() => {
    if (pathname === NUDGE_PATH) {
      refetch();
    }
  }, [pathname, refetch]);

  if (pathname !== NUDGE_PATH) return null;
  if (status !== "ready" || !state) return null;

  const kind = deriveKind(state);
  if (!kind) return null;
  if (dismissed.has(kind)) return null;

  if (kind === "pick")   return <PickStrategyBanner   onDismiss={() => dismiss("pick")}   />;
  if (kind === "deploy") return <FinishSetupBanner    onDismiss={() => dismiss("deploy")} state={state} />;
  return                       <YoureLiveBanner      onDismiss={() => dismiss("live")}   state={state} />;
}

// ─── Banners ────────────────────────────────────────────────────────────────

function PickStrategyBanner({ onDismiss }: { onDismiss: () => void }) {
  const router = useRouter();
  return (
    <BannerShell tone="allocator">
      <BannerText>
        <Accent tone="green">✓ Exchange connected.</Accent>{" "}
        <Accent tone="allocator">Next: pick a strategy from the catalog.</Accent>{" "}
        Audited tear sheets show Sharpe, drawdown, walk-forward stability.
      </BannerText>
      <BannerActions>
        <PrimaryButton onClick={() => router.push("/trader/strategies")}>
          View catalog →
        </PrimaryButton>
        <GhostButton onClick={onDismiss}>Dismiss</GhostButton>
      </BannerActions>
    </BannerShell>
  );
}

function FinishSetupBanner({ state, onDismiss }: { state: OnboardingState; onDismiss: () => void }) {
  const router = useRouter();
  const name = state.selected_strategy_name ?? "Strategy";
  const version = state.selected_strategy_version ?? "";
  const versionText = version ? ` ${version}` : "";

  function gotoFinishSetup() {
    if (state.selected_strategy_id) {
      router.push(`/trader/strategies/${state.selected_strategy_id}`);
    } else {
      router.push("/trader/strategies");
    }
  }

  // Copy frames the state accurately: the strategy is already in the user's
  // Traders sidebar (SYNC CAPITAL added it), the wizard just hasn't been
  // completed yet. "Finish setup" is what's actually left to do, not a
  // brand-new "deploy capital" decision.
  return (
    <BannerShell tone="allocator">
      <BannerText>
        <Accent tone="green">✓ {name}{versionText} added to your traders.</Accent>{" "}
        <Accent tone="allocator">Finish setup to start trading.</Accent>
      </BannerText>
      <BannerActions>
        <PrimaryButton onClick={gotoFinishSetup}>Finish setup →</PrimaryButton>
        <GhostButton onClick={onDismiss}>Dismiss</GhostButton>
      </BannerActions>
    </BannerShell>
  );
}

function YoureLiveBanner({ state, onDismiss }: { state: OnboardingState; onDismiss: () => void }) {
  const name = state.selected_strategy_name ?? "Your strategy";
  const version = state.selected_strategy_version ?? "";
  const versionText = version ? ` ${version}` : "";
  return (
    <BannerShell tone="green">
      <BannerText>
        <Accent tone="green">✓ You&apos;re live.</Accent>{" "}
        First allocation deploying · {name}{versionText} · alerts on.
      </BannerText>
      <BannerActions>
        <GhostButton onClick={onDismiss}>Got it</GhostButton>
      </BannerActions>
    </BannerShell>
  );
}

// ─── Pieces ─────────────────────────────────────────────────────────────────

function BannerShell({ tone, children }: { tone: "allocator" | "green"; children: React.ReactNode }) {
  const accent = tone === "allocator" ? "var(--allocator)" : "var(--green)";
  const surface = tone === "allocator" ? "var(--allocator-soft)" : "var(--green-dim)";
  return (
    <div style={{
      margin: "16px 28px 0",
      padding: "14px 20px",
      background: surface,
      border: `1px solid ${accent}`,
      borderLeft: `3px solid ${accent}`,
      borderRadius: 2,
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: 24,
      flexWrap: "wrap",
    }}>
      {children}
    </div>
  );
}

function BannerText({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ color: "var(--t0)", fontSize: 13, lineHeight: 1.5, flex: "1 1 320px" }}>
      {children}
    </div>
  );
}

function BannerActions({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ display: "flex", gap: 10, flexShrink: 0 }}>{children}</div>
  );
}

function Accent({ tone, children }: { tone: "allocator" | "green"; children: React.ReactNode }) {
  return (
    <span style={{
      color: tone === "allocator" ? "var(--allocator)" : "var(--green)",
      fontWeight: 700,
    }}>
      {children}
    </span>
  );
}

function PrimaryButton({ onClick, children }: { onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "8px 14px",
        fontFamily: "inherit",
        fontSize: 10,
        fontWeight: 700,
        letterSpacing: "0.14em",
        textTransform: "uppercase",
        background: "var(--allocator)",
        color: "#0d0518",
        border: "1px solid var(--allocator)",
        borderRadius: 2,
        cursor: "pointer",
        transition: "background 0.12s ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.background = "#c0a8ff"; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = "var(--allocator)"; }}
    >
      {children}
    </button>
  );
}

function GhostButton({ onClick, children }: { onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "8px 14px",
        fontFamily: "inherit",
        fontSize: 10,
        fontWeight: 700,
        letterSpacing: "0.14em",
        textTransform: "uppercase",
        background: "transparent",
        color: "var(--t1)",
        border: "1px solid var(--border-bright)",
        borderRadius: 2,
        cursor: "pointer",
        transition: "color 0.12s ease, border-color 0.12s ease",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.color = "var(--t0)";
        e.currentTarget.style.borderColor = "var(--t1)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.color = "var(--t1)";
        e.currentTarget.style.borderColor = "var(--border-bright)";
      }}
    >
      {children}
    </button>
  );
}


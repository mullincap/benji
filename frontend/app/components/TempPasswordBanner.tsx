"use client";

/**
 * TempPasswordBanner — site-wide amber strip rendered above all page
 * content while the current user has `password_is_temporary = true`.
 *
 * Lifecycle:
 *   - admin issues temp password → DB sets password_is_temporary=true
 *   - user signs in (forced sessions revoked) → banner appears
 *   - user clicks the link → /settings/security → changes password
 *   - DB sets password_is_temporary=false → banner gone permanently
 *
 * Per-session dismissal via sessionStorage so the user can hide it for
 * the current tab if they're mid-task; on next page load (or new tab),
 * the banner re-appears. The "permanent" gate is the
 * password_is_temporary flag itself.
 *
 * The banner consumes useAuth() so it rerenders when the auth state
 * refetches (e.g. after change-password succeeds and we call refetch).
 */

import { useState } from "react";
import { usePathname } from "next/navigation";
import Link from "next/link";

import { useAuth } from "../lib/auth";

const DISMISS_KEY = "3m-temp-pw-banner-dismissed";

export default function TempPasswordBanner() {
  const { user } = useAuth();
  const pathname = usePathname();

  // Lazy initial state reads sessionStorage at first render. No
  // hydration mismatch risk because the banner is gated behind
  // `if (!user) return null` and `user` is null during SSR (auth
  // context starts unloaded server-side), so this component never
  // emits markup that would conflict with client hydration.
  const [dismissed, setDismissed] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return sessionStorage.getItem(DISMISS_KEY) === "true";
  });

  // Banner is suppressed:
  //   - while loading or unauthed
  //   - on auth flow pages (signin/invite/welcome/forgot) — pre-app context
  //   - when password is not temporary
  //   - when user has dismissed for the session
  //   - when already on the change-password page (don't tell them to go
  //     where they already are)
  if (!user) return null;
  if (!user.password_is_temporary) return null;
  if (pathname.startsWith("/auth/")) return null;
  if (pathname.startsWith("/settings/security")) return null;
  if (dismissed) return null;

  return (
    <div
      role="alert"
      aria-live="polite"
      style={{
        background: "rgba(240, 165, 0, 0.12)",
        borderBottom: "1px solid var(--amber)",
        padding: "8px 24px",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: 14,
        fontSize: 12,
        color: "var(--amber)",
        letterSpacing: "0.04em",
      }}
    >
      <span style={{ fontWeight: 700 }}>You&apos;re using a temporary password.</span>
      <Link
        href="/settings/security"
        style={{
          color: "var(--amber)",
          textDecoration: "underline",
          textDecorationThickness: "1px",
          textUnderlineOffset: "2px",
          fontWeight: 700,
        }}
      >
        Change it now →
      </Link>
      <button
        type="button"
        onClick={() => {
          if (typeof window !== "undefined") {
            sessionStorage.setItem(DISMISS_KEY, "true");
          }
          setDismissed(true);
        }}
        aria-label="Dismiss"
        title="Dismiss for this session"
        style={{
          background: "transparent",
          border: 0,
          color: "var(--amber)",
          fontSize: 14,
          cursor: "pointer",
          padding: "0 8px",
          lineHeight: 1,
        }}
      >
        ×
      </button>
    </div>
  );
}

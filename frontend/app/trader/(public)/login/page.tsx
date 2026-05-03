"use client";

/**
 * frontend/app/trader/(public)/login/page.tsx
 * ============================================
 * REDIRECT STUB.
 *
 * Pre-Phase-1a this was a 212-line standalone signin page. Phase 1a
 * unified signin at /auth/signin (PR #15). This route survives only
 * for stale bookmarks/external links — the actual form is gone.
 *
 * Behavior:
 *   - Forwards any incoming ?next= to /auth/signin
 *   - When ?next= is missing, defaults to /trader/overview so a user
 *     hitting /trader/login from a bookmark lands in the trader
 *     workspace after signing in (the historical intent of this URL)
 *
 * Safe to delete in a future commit once we've verified nothing
 * external is linking here for ~30 days.
 */

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function TraderLoginRedirect() {
  const router = useRouter();
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const next = params.get("next") || "/trader/overview";
    // Sanitize: same-origin paths only (matches /auth/signin's own
    // sanitizeNext rule).
    const safe = next.startsWith("/") && !next.startsWith("//") ? next : "/trader/overview";
    router.replace("/auth/signin?next=" + encodeURIComponent(safe));
  }, [router]);

  return null;
}

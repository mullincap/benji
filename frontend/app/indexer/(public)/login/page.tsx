"use client";

/**
 * frontend/app/indexer/(public)/login/page.tsx
 * ============================================
 * Indexer login stub — redirects to /compiler/login.
 *
 * Auth is shared between Compiler and Indexer (same admin_session cookie,
 * same require_admin dependency, same POST /api/admin/login). The locked
 * decision in docs/builds/indexer-page-build.md is to reuse the existing
 * compiler login page rather than duplicating the form. After authenticating
 * at /compiler/login the user lands on /compiler — they can then navigate to
 * /indexer manually. This is acceptable for a single-admin tool. A future
 * enhancement could thread a ?return_to=/indexer query param through the
 * compiler login page; out of scope for this round.
 */

import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function IndexerLoginRedirect() {
  const router = useRouter();
  useEffect(() => {
    router.replace("/compiler/login");
  }, [router]);

  return (
    <div style={{
      fontSize: 9, color: "var(--t3)",
      textTransform: "uppercase", letterSpacing: "0.12em",
    }}>
      Redirecting to admin login…
    </div>
  );
}

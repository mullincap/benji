"use client";

/**
 * frontend/app/trader/(onboarding)/get-started/page.tsx
 * =====================================================
 * Placeholder — replaced in Commit 3 of the get-started onboarding
 * series with the full hero card (link exchange / try demo / skip).
 *
 * Exists in Commit 2 only so that the redirect from
 * (protected)/layout.tsx → /trader/get-started resolves to a real
 * route while the rest of the onboarding wiring is being landed.
 */

export default function GetStartedPage() {
  return (
    <div style={{
      padding: 32,
      fontSize: 10,
      color: "var(--t2)",
      textTransform: "uppercase",
      letterSpacing: "0.12em",
    }}>
      Get started — placeholder
    </div>
  );
}

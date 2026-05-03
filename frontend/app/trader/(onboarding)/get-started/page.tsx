"use client";

/**
 * frontend/app/trader/(onboarding)/get-started/page.tsx
 * =====================================================
 * 1-step onboarding hero — the destination the (protected) trader
 * layout redirects to when has_exchange is false.
 *
 * Two modes on a single URL (no query params, no sub-routes):
 *
 *   1. Hero card — status strip → eyebrow → title → subtitle →
 *      2-col exchange picker (BloFin / Binance) → trust bullets →
 *      footer help line + "Skip — explore first" link.
 *
 *   2. Inline wizard — clicking either exchange card swaps the hero
 *      out for <ExchangeLinkWizard /> with the picked exchange
 *      pre-selected. The wizard's Back-on-Step-1 returns to the
 *      hero (state-only swap, URL unchanged). Wizard onSuccess
 *      routes to /trader/overview where the OnboardingNudge picks
 *      up the freshly-linked exchange.
 *
 * Pre-Phase-1c the cards deep-linked to /trader/settings?openLink=…
 * which yanked the user out of the focused single-page hero into the
 * full settings page (sidebar + linked-exchanges list + capital-events
 * section). The inline-wizard model keeps the user on the same URL
 * and same layout throughout the link flow.
 *
 * Mockup deviations:
 *   - The Figma uses a purple "allocator" accent for the eyebrow /
 *     arrows / hover. The trader workspace's design system originally
 *     gated only on --green / --amber / --red, so commit 3 of PR #24
 *     used green for the eyebrow + title accent. The exchange CARDS
 *     now use --allocator (introduced in commit 4 of PR #24 for the
 *     OnboardingNudge banners) — they're the page's primary action
 *     and reading them as purple "next step" affordances differentiates
 *     them from the green status text and trust-bullet checks.
 *
 * Skip link: stamps sessionStorage via setOnboardingSkipped() and
 * pushes /trader/overview. The (protected) layout's redirect honors
 * the skip flag so the user is not bounced back here on next nav.
 * Flag is cleared on logout (see app/lib/auth.tsx signout).
 */

import { useState } from "react";
import { useRouter } from "next/navigation";

import { setOnboardingSkipped } from "../../_lib/onboarding";
import { ExchangeLinkWizard } from "../../components/ExchangeLinkWizard";
import type { ExchangeSlug } from "../../api";

export default function GetStartedPage() {
  const router = useRouter();
  const [linkingExchange, setLinkingExchange] = useState<ExchangeSlug | null>(null);

  function onSkip(e: React.MouseEvent<HTMLAnchorElement>) {
    e.preventDefault();
    setOnboardingSkipped();
    router.push("/trader/overview");
  }

  // ─── Wizard mode ──────────────────────────────────────────────────────────
  if (linkingExchange !== null) {
    return (
      <div style={{ width: "100%", padding: "48px 24px", display: "flex", justifyContent: "center" }}>
        <div style={{ width: "100%", maxWidth: 720 }}>
          <ExchangeLinkWizard
            initialExchange={linkingExchange}
            onSuccess={() => router.push("/trader/overview")}
            onCancel={() => setLinkingExchange(null)}
          />
        </div>
      </div>
    );
  }

  // ─── Hero mode ────────────────────────────────────────────────────────────
  return (
    <div style={{ width: "100%", padding: "48px 24px", display: "flex", justifyContent: "center" }}>
      <div style={{
        width: "100%",
        maxWidth: 680,
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 3,
      }}>
        {/* status strip */}
        <div style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "12px 22px",
          borderBottom: "1px solid var(--line)",
          color: "var(--t1)",
          fontSize: 11,
          letterSpacing: "0.04em",
        }}>
          <span>
            &gt; setup.exchange = <span style={{ color: "var(--green)" }}>required</span>
          </span>
          <span style={{ color: "var(--t3)" }}>welcome to the trader workspace</span>
        </div>

        {/* body */}
        <div style={{ padding: "40px 32px 32px" }}>
          <div style={{
            color: "var(--green)",
            fontSize: 11,
            letterSpacing: "0.18em",
            textTransform: "uppercase",
            marginBottom: 16,
          }}>
            [ Trader · Get Started ]
          </div>

          <h1 style={{
            fontSize: 30,
            fontWeight: 700,
            color: "var(--t0)",
            lineHeight: 1.2,
            letterSpacing: "-0.01em",
            marginBottom: 14,
          }}>
            Link an <span style={{ color: "var(--green)" }}>exchange</span> to begin
          </h1>

          <p style={{
            color: "var(--t1)",
            fontSize: 14,
            lineHeight: 1.6,
            marginBottom: 28,
          }}>
            3M deploys audited quantitative strategies against your own exchange account.
            We never custody your capital — your funds stay on the exchange. Once linked,
            the platform will guide you to a strategy and your first allocation.
          </p>

          {/* exchange options */}
          <div style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 12,
            marginBottom: 24,
          }}>
            {/* Binance is the only exchange we promote in user-facing
                onboarding right now. BloFin's card stays rendered but
                visually-disabled — looks unavailable to demo users
                (e.g. Juan), still clickable for J's internal testing
                via the same wizard flow. Flip BloFin's
                visuallyDisabled to false to re-promote when ready. */}
            <ExchangeCard
              name="Binance"
              meta="USDT-M perpetuals · institutional"
              onClick={() => setLinkingExchange("binance")}
            />
            <ExchangeCard
              name="BloFin"
              meta="USDT-M perpetuals · coming soon"
              onClick={() => setLinkingExchange("blofin")}
              visuallyDisabled
            />
          </div>

          {/* trust bullets */}
          <ul style={{
            listStyle: "none",
            margin: 0,
            padding: "16px 0 0",
          }}>
            <TrustBullet
              strong="Read + trading permissions only."
              rest="No withdrawal access required."
            />
            <TrustBullet
              strong="API keys encrypted at rest."
              rest="Stored with Fernet, decrypted only at execution time."
            />
            <TrustBullet
              strong="You stay in control."
              rest="Pause, close, or revoke at any moment from the dashboard."
            />
          </ul>
        </div>

        {/* foot */}
        <div style={{
          padding: "16px 22px",
          borderTop: "1px solid var(--line)",
          background: "var(--bg2)",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 16,
          flexWrap: "wrap",
        }}>
          <div style={{ color: "var(--t1)", fontSize: 11, lineHeight: 1.5 }}>
            Need help getting an exchange set up?
            <a
              href="mailto:hello@mullincap.com"
              style={{ color: "var(--green)", textDecoration: "none", marginLeft: 8 }}
            >
              Book a call
            </a>
          </div>
          <a
            href="/trader/overview"
            onClick={onSkip}
            style={{
              color: "var(--t2)",
              textDecoration: "none",
              fontSize: 10,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              borderBottom: "1px dashed var(--line2)",
              paddingBottom: 1,
              whiteSpace: "nowrap",
            }}
          >
            Skip — explore first →
          </a>
        </div>
      </div>
    </div>
  );
}

// ─── Pieces ─────────────────────────────────────────────────────────────────

// `visuallyDisabled` makes the card LOOK unavailable (muted surface,
// no hover affordances, "[ COMING SOON ]" badge instead of the arrow)
// while keeping onClick wired. The behavioral departure from a real
// disable is intentional — it lets J click through the BloFin link
// path internally for testing without users perceiving it as a
// promoted option. Flip back to false when ready to ship.
function ExchangeCard({
  name, meta, onClick, visuallyDisabled = false,
}: {
  name: string;
  meta: string;
  onClick: () => void;
  visuallyDisabled?: boolean;
}) {
  const [hover, setHover] = useState(false);
  const showHover = hover && !visuallyDisabled;
  const arrowColor = showHover ? "var(--allocator)" : "var(--t2)";
  return (
    <button
      type="button"
      onClick={onClick}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        padding: "18px",
        background: visuallyDisabled ? "var(--bg2)" : "var(--allocator-soft)",
        border: `1px solid ${
          visuallyDisabled
            ? "var(--line)"
            : showHover ? "#c0a8ff" : "var(--allocator)"
        }`,
        borderRadius: 2,
        cursor: visuallyDisabled ? "default" : "pointer",
        opacity: visuallyDisabled ? 0.45 : 1,
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 14,
        textAlign: "left",
        font: "inherit",
        color: "inherit",
        transform: showHover ? "translateY(-2px)" : "translateY(0)",
        transition: "border-color 0.15s ease, transform 0.15s ease",
      }}
    >
      <div>
        <div style={{ color: "var(--t0)", fontWeight: 700, fontSize: 14, marginBottom: 4 }}>
          {name}
        </div>
        <div style={{ color: "var(--t2)", fontSize: 10, letterSpacing: "0.04em", lineHeight: 1.4 }}>
          {meta}
        </div>
      </div>
      {visuallyDisabled ? (
        <span style={{
          color: "var(--t2)",
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          whiteSpace: "nowrap",
        }}>
          [ Coming soon ]
        </span>
      ) : (
        <span style={{ color: arrowColor, fontSize: 14, transition: "color 0.15s ease" }}>→</span>
      )}
    </button>
  );
}

function TrustBullet({ strong, rest }: { strong: string; rest: string }) {
  return (
    <li style={{
      display: "flex",
      alignItems: "flex-start",
      gap: 10,
      padding: "5px 0",
      color: "var(--t1)",
      fontSize: 12,
      lineHeight: 1.5,
    }}>
      <span style={{ color: "var(--green)", fontWeight: 700, flexShrink: 0, marginTop: 1 }}>✓</span>
      <span>
        <span style={{ color: "var(--t0)", fontWeight: 700 }}>{strong}</span> {rest}
      </span>
    </li>
  );
}

"use client";

/**
 * frontend/app/trader/(onboarding)/get-started/page.tsx
 * =====================================================
 * 1-step onboarding hero — the destination the (protected) trader
 * layout redirects to when has_exchange is false.
 *
 * Layout: a single centered hero card. Status bar → eyebrow → title →
 * subtitle → 2-col exchange picker (BloFin / Binance) → trust bullets
 * → footer help line + "Skip — explore first" link.
 *
 * Mockup deviation: the Figma uses a purple "allocator" accent for
 * the eyebrow / link arrows / hover. The trader workspace's locked
 * design system has only --green / --amber / --red as accents, so
 * we substitute --green here. Visual hierarchy stays intact and the
 * page reads coherently with the rest of the trader chrome the user
 * sees post-link.
 *
 * Card behavior: clicking either exchange option routes to
 * /trader/settings?openLink=<slug>, which the settings page reads on
 * mount to auto-open the LinkWizard. The wizard's exchange dropdown
 * still requires a manual selection in v1 — pre-selection is a
 * commit-6 polish item.
 *
 * Skip link: stamps sessionStorage via setOnboardingSkipped() and
 * pushes /trader/overview. The (protected) layout's redirect honors
 * the skip flag so the user is not bounced back here on next nav.
 * Flag is cleared on logout (see app/lib/auth.tsx signout).
 */

import Link from "next/link";
import { useRouter } from "next/navigation";
import { setOnboardingSkipped } from "../../_lib/onboarding";

export default function GetStartedPage() {
  const router = useRouter();

  function onSkip(e: React.MouseEvent<HTMLAnchorElement>) {
    e.preventDefault();
    setOnboardingSkipped();
    router.push("/trader/overview");
  }

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
            <ExchangeCard
              name="BloFin"
              meta="USDT-M perpetuals · recommended"
              href="/trader/settings?openLink=blofin"
            />
            <ExchangeCard
              name="Binance"
              meta="USDT-M perpetuals · institutional"
              href="/trader/settings?openLink=binance"
            />
          </div>

          {/* trust bullets */}
          <ul style={{
            listStyle: "none",
            margin: 0,
            padding: "16px 18px",
            background: "var(--bg0)",
            border: "1px solid var(--line)",
            borderRadius: 2,
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

function ExchangeCard({ name, meta, href }: { name: string; meta: string; href: string }) {
  return (
    <Link
      href={href}
      style={{
        padding: "18px",
        background: "var(--bg0)",
        border: "1px solid var(--line)",
        borderRadius: 2,
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 14,
        textDecoration: "none",
        transition: "border-color 0.15s ease, background 0.15s ease, transform 0.15s ease",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.borderColor = "var(--green)";
        e.currentTarget.style.background = "var(--green-dim)";
        e.currentTarget.style.transform = "translateY(-1px)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.borderColor = "var(--line)";
        e.currentTarget.style.background = "var(--bg0)";
        e.currentTarget.style.transform = "translateY(0)";
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
      <span style={{ color: "var(--t2)", fontSize: 14 }}>→</span>
    </Link>
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

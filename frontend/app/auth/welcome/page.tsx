"use client";

/**
 * frontend/app/(auth)/welcome/page.tsx
 * ====================================
 * /auth/welcome — first-run onboarding.
 *
 * Auth + first_login gate is currently per-page (mount-effect GETs
 * /api/auth/me). The middleware in commit 6 will hoist this check
 * to the request level; until then, this page handles its own
 * redirect logic:
 *
 *   401              → /auth/signin?next=/auth/welcome
 *   first_login=false → /
 *   first_login=true  → render
 *
 * "Enter platform" CTA POSTs /api/auth/welcome/complete then redirects
 * to /. "View documentation" goes to a placeholder anchor (no /docs
 * route exists yet — to be wired when the docs site lands).
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";

import { AuthCard, Button } from "../_components";
import { useAuth } from "../../lib/auth";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type Me = {
  user_id: string;
  email: string;
  first_login: boolean;
  first_name: string | null;
};

const MODULES: { name: string; desc: string; color: string }[] = [
  { name: "Compiler",  desc: "Strategy intake & canonical builds", color: "#00C2FF" },
  { name: "Indexer",   desc: "Market snapshots & leaderboards",     color: "#00E5C8" },
  { name: "Simulator", desc: "Backtest, audit & stress test",       color: "#39FF85" },
  { name: "Allocator", desc: "Live deployment & capital sizing",    color: "#A78BFF" },
  { name: "Manager",   desc: "Portfolio & performance oversight",   color: "#7B5FFF" },
];

const QUICKSTART: { num: string; title: string; desc: string; href: string }[] = [
  {
    num: "01",
    title: "Connect an exchange",
    desc: "BloFin and Binance supported. Read-only or trading scope.",
    href: "/trader/settings",
  },
  {
    num: "02",
    title: "Browse the strategy catalog",
    desc: "Audited strategies are available in the Allocator module.",
    href: "/trader/strategies",
  },
  {
    num: "03",
    title: "Read the platform guide",
    // Placeholder — no /docs route exists yet. Replace when docs site lands.
    desc: "How the Compiler / Indexer / Simulator / Allocator / Manager pipeline fits together.",
    href: "#",
  },
];

export default function WelcomePage() {
  const router = useRouter();
  const { refetch } = useAuth();
  const [me, setMe] = useState<Me | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Auth + first-login gate
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(API_BASE + "/api/auth/me", { credentials: "include" });
        if (cancelled) return;
        if (res.status === 401) {
          router.replace("/auth/signin?next=/auth/welcome");
          return;
        }
        if (!res.ok) {
          setError(`Couldn't load your account (${res.status}).`);
          return;
        }
        const data = (await res.json()) as Me;
        if (!data.first_login) {
          router.replace("/");
          return;
        }
        setMe(data);
      } catch (err) {
        if (!cancelled) {
          setError(`Network error: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [router]);

  async function handleEnterPlatform() {
    if (submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch(API_BASE + "/api/auth/welcome/complete", {
        method: "POST",
        credentials: "include",
      });
      if (res.ok) {
        // Use the server-computed default_landing (same rule signin uses).
        // Brand-new users have no allocations yet → /trader/overview;
        // welcome screen is only ever shown on first_login=true so this
        // is almost always the trader path.
        const data = await res.json().catch(() => ({}));
        // Refresh AuthProvider state so the next page sees the cleared
        // first_login flag and topbar UI gated on `user` (e.g. SIGN OUT)
        // is populated. AuthProvider only fetches on mount; without
        // this, the next page renders with a stale user that's either
        // null or still has first_login=true.
        await refetch();
        const target =
          (typeof data?.default_landing === "string" && data.default_landing) ||
          "/trader/overview";
        router.replace(target);
        return;
      }
      setError(`Couldn't complete onboarding (${res.status}). Try again.`);
    } catch (err) {
      setError(`Network error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSubmitting(false);
    }
  }

  if (!me) {
    return (
      <AuthCard
        wide
        statusLeft={
          <>&gt; session.scope = <span style={{ color: "var(--amber)" }}>verifying</span></>
        }
        eyebrow="[ Onboarding · First Run ]"
        title="Loading…"
      >
        {error ? (
          <div role="alert" style={{ color: "var(--red)", fontSize: 12 }}>
            {error}
          </div>
        ) : (
          <div style={{ height: 60 }} />
        )}
      </AuthCard>
    );
  }

  const greeting = me.first_name?.trim() ? `Welcome, ${me.first_name}` : "Welcome to 3M";

  return (
    <AuthCard
      wide
      statusLeft={
        <>
          &gt; user.first_login ={" "}
          <span style={{ color: "var(--green)" }}>true</span> · session.scope ={" "}
          <span style={{ color: "var(--green)" }}>full</span>
        </>
      }
      statusRight="tier: allocator"
      eyebrow="[ Onboarding · First Run ]"
      title={greeting}
      subtitle="You're verified and ready to go. Here's a quick map of the five modules that make up the 3M platform."
    >
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(5, 1fr)",
          gap: 8,
          margin: "24px 0 28px",
        }}
      >
        {MODULES.map((m) => (
          <ModuleTile key={m.name} {...m} />
        ))}
      </div>

      <div
        style={{
          color: "var(--green)",
          fontSize: 10,
          letterSpacing: "0.18em",
          textTransform: "uppercase",
          fontWeight: 700,
          marginBottom: 0,
        }}
      >
        [ Next Steps ]
      </div>
      <ol
        style={{
          listStyle: "none",
          margin: "16px 0 24px",
          padding: 0,
        }}
      >
        {QUICKSTART.map((step, i, arr) => (
          <li
            key={step.num}
            style={{
              display: "flex",
              alignItems: "flex-start",
              gap: 12,
              padding: "12px 0",
              borderBottom: i === arr.length - 1 ? "0" : "1px solid var(--line)",
              color: "var(--t2)",
              fontSize: 13,
            }}
          >
            <span
              style={{
                color: "var(--green)",
                fontWeight: 700,
                fontSize: 11,
                letterSpacing: "0.1em",
                flexShrink: 0,
                width: 24,
              }}
            >
              {step.num}
            </span>
            <span>
              <Link
                href={step.href}
                style={{
                  color: "var(--t0)",
                  fontWeight: 700,
                  textDecoration: "none",
                }}
              >
                {step.title}
              </Link>
              <br />
              {step.desc}
            </span>
          </li>
        ))}
      </ol>

      {error && (
        <div role="alert" style={{ color: "var(--red)", fontSize: 12, marginBottom: 12 }}>
          {error}
        </div>
      )}

      <div style={{ display: "flex", gap: 10, marginTop: 20 }}>
        <Link
          href="#"
          style={{
            flex: 1,
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "13px 16px",
            fontSize: 12,
            fontWeight: 700,
            letterSpacing: "0.14em",
            textTransform: "uppercase",
            border: "1px solid var(--border-bright)",
            borderRadius: 2,
            background: "transparent",
            color: "var(--t0)",
            textDecoration: "none",
          }}
        >
          View documentation
        </Link>
        <div style={{ flex: 1 }}>
          <Button
            variant="primary"
            trailingArrow={!submitting}
            loading={submitting}
            onClick={handleEnterPlatform}
          >
            Enter platform
          </Button>
        </div>
      </div>
    </AuthCard>
  );
}

function ModuleTile({ name, desc, color }: { name: string; desc: string; color: string }) {
  return (
    <div
      style={{
        padding: "14px 12px",
        background: "var(--bg3)",
        border: "1px solid var(--line)",
        borderTop: `2px solid ${color}`,
        borderRadius: "0 0 2px 2px",
      }}
    >
      <div
        style={{
          color,
          fontSize: 11,
          fontWeight: 700,
          letterSpacing: "0.1em",
          textTransform: "uppercase",
          marginBottom: 4,
        }}
      >
        {name}
      </div>
      <div style={{ color: "var(--t2)", fontSize: 10, lineHeight: 1.4 }}>{desc}</div>
    </div>
  );
}

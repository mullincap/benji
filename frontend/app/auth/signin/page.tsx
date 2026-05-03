"use client";

/**
 * frontend/app/(auth)/signin/page.tsx
 * ===================================
 * /auth/signin — primary user sign-in.
 *
 * Posts to POST /api/auth/login with { email, password, remember }.
 *  - 200 → redirect to ?next= (sanitized) or /
 *  - 401 → "Invalid credentials" inline under password
 *  - 423 → card-level lock banner with retry_after countdown
 *  - 429 → card-level rate-limit banner with retry_after countdown
 *  - other → generic banner
 *
 * "Forgot password?" → /auth/forgot (Phase 1a stub)
 * Footer "Request access" → /#waitlist (same-origin landing waitlist anchor)
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";

import {
  AuthCard,
  Button,
  Checkbox,
  Field,
  Input,
} from "../_components";
import { useAuth } from "../../lib/auth";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const BUILD_HASH = process.env.NEXT_PUBLIC_BUILD_HASH || "dev";

/** Returns the validated ?next= path, or null if none was provided.
 *  Returning null (vs. defaulting to "/") lets the caller distinguish
 *  "user explicitly wants to land on X" from "no preference, use the
 *  server-side default_landing rule". */
function sanitizeNext(raw: string | null): string | null {
  if (!raw) return null;
  // Same-origin paths only — reject protocol-relative ("//host") and absolute URLs.
  if (!raw.startsWith("/") || raw.startsWith("//")) return null;
  return raw;
}

type LockState = {
  kind: "locked" | "rate_limited";
  retryAfter: number;
};

export default function SignInPage() {
  const router = useRouter();
  const { refetch } = useAuth();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [bannerError, setBannerError] = useState<string | null>(null);
  const [lock, setLock] = useState<LockState | null>(null);
  // Null when no explicit ?next= was provided. Falsy = let server's
  // default_landing decide where to route post-signin.
  const [nextPath, setNextPath] = useState<string | null>(null);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    setNextPath(sanitizeNext(params.get("next")));
  }, []);

  // Tick the lock countdown once per second.
  useEffect(() => {
    if (!lock) return;
    if (lock.retryAfter <= 0) {
      setLock(null);
      return;
    }
    const id = window.setInterval(() => {
      setLock((cur) => (cur ? { ...cur, retryAfter: cur.retryAfter - 1 } : cur));
    }, 1000);
    return () => window.clearInterval(id);
  }, [lock]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (submitting || lock) return;
    if (!email || !password) {
      setFieldError("Email and password are required.");
      return;
    }
    setSubmitting(true);
    setFieldError(null);
    setBannerError(null);

    try {
      const res = await fetch(API_BASE + "/api/auth/login", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password, remember }),
      });

      if (res.ok) {
        const data = await res.json().catch(() => ({}));
        // Refresh AuthProvider state BEFORE navigating. AuthProvider
        // only fetches /me on mount; without this, the next page
        // would render with a stale null user and any topbar UI gated
        // on user (e.g. SIGN OUT button) would stay hidden until the
        // first hard reload. See PR addressing the empty-state
        // sign-out bug — gate is already correct, the staleness is
        // what was breaking it.
        await refetch();
        // First-login users land on /auth/welcome regardless of ?next.
        if (data?.first_login) {
          router.replace("/auth/welcome");
        } else {
          // Routing precedence:
          //   1. explicit ?next= (deep link, auth-bounce, etc.) — user intent wins
          //   2. server-side default_landing rule (active allocations? → manager
          //      else trader)
          //   3. /trader/overview as the generic app home if both are missing
          //
          // The fallback used to be "/" which dropped users on the marketing
          // landing page — surprising for someone who just signed in.
          // /trader/overview is the conservative app home; users with active
          // allocations would normally hit case (2) and land on /manager
          // /overview directly.
          const target =
            nextPath ||
            (typeof data?.default_landing === "string" ? data.default_landing : null) ||
            "/trader/overview";
          router.replace(target);
        }
        return;
      }

      const body = await res.json().catch(() => ({}));

      if (res.status === 401) {
        setFieldError("Invalid email or password.");
      } else if (res.status === 403) {
        setBannerError("This account is deactivated. Contact support.");
      } else if (res.status === 423) {
        setLock({
          kind: "locked",
          retryAfter: Number(body?.detail?.retry_after) || 900,
        });
      } else if (res.status === 429) {
        setLock({
          kind: "rate_limited",
          retryAfter: Number(body?.detail?.retry_after) || 60,
        });
      } else {
        setBannerError(`Sign-in failed (${res.status}). Try again.`);
      }
    } catch (err) {
      setBannerError(
        `Network error: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <AuthCard
      statusLeft={
        <>
          &gt; session.state ={" "}
          <span style={{ color: "var(--green)" }}>unauthenticated</span>
        </>
      }
      statusRight={`v${BUILD_HASH}`}
      eyebrow="[ Auth · Sign In ]"
      title="Sign in"
      subtitle="Access your quantitative trading workspace."
      footer={
        <>
          Don&apos;t have an account?
          <Link
            href="/#waitlist"
            style={{
              color: "var(--green)",
              textDecoration: "none",
              marginLeft: 6,
            }}
          >
            Request access
          </Link>
        </>
      }
    >
      {bannerError && <Banner kind="error">{bannerError}</Banner>}
      {lock && <LockBanner state={lock} />}

      <form onSubmit={handleSubmit} noValidate>
        <Field label="Email" htmlFor="signin-email">
          <Input
            id="signin-email"
            type="email"
            placeholder="you@firm.com"
            autoComplete="email"
            autoFocus
            value={email}
            onChange={(e) => {
              setEmail(e.target.value);
              setFieldError(null);
            }}
            disabled={submitting || !!lock}
            required
          />
        </Field>

        <Field
          label="Password"
          htmlFor="signin-password"
          rightSlot={
            <Link
              href="/auth/forgot"
              style={{
                color: "var(--t3)",
                fontSize: 10,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                textDecoration: "none",
              }}
            >
              Forgot password?
            </Link>
          }
          helper={fieldError ?? undefined}
          helperKind={fieldError ? "error" : "default"}
        >
          <Input
            id="signin-password"
            type="password"
            placeholder="••••••••••••"
            autoComplete="current-password"
            value={password}
            onChange={(e) => {
              setPassword(e.target.value);
              setFieldError(null);
            }}
            invalid={!!fieldError}
            disabled={submitting || !!lock}
            required
          />
        </Field>

        <Checkbox
          checked={remember}
          onChange={setRemember}
          disabled={submitting || !!lock}
        >
          Remember this device for 14 days
        </Checkbox>

        <Button
          type="submit"
          variant="primary"
          trailingArrow={!submitting}
          loading={submitting}
          disabled={!!lock}
        >
          Sign in
        </Button>
      </form>
    </AuthCard>
  );
}

// ─── Local presentational helpers ──────────────────────────────────────────

function Banner({
  kind,
  children,
}: {
  kind: "error" | "warning";
  children: React.ReactNode;
}) {
  const color = kind === "error" ? "var(--red)" : "var(--amber)";
  return (
    <div
      role="alert"
      style={{
        marginBottom: 20,
        padding: "10px 14px",
        background: "var(--bg3)",
        border: `1px solid ${color}`,
        borderLeft: `2px solid ${color}`,
        borderRadius: 2,
        color: "var(--t0)",
        fontSize: 12,
      }}
    >
      {children}
    </div>
  );
}

function LockBanner({ state }: { state: LockState }) {
  const heading =
    state.kind === "locked"
      ? "Too many failed attempts"
      : "Too many requests";
  const detail =
    state.kind === "locked"
      ? "Your account is temporarily locked. Try again in"
      : "Slow down for a moment. Try again in";
  return (
    <Banner kind="warning">
      <div style={{ fontWeight: 700, marginBottom: 4 }}>{heading}</div>
      <div style={{ color: "var(--t2)" }}>
        {detail}{" "}
        <span style={{ color: "var(--amber)", fontVariantNumeric: "tabular-nums" }}>
          {formatRetry(state.retryAfter)}
        </span>
        .
      </div>
    </Banner>
  );
}

function formatRetry(seconds: number): string {
  if (seconds <= 0) return "0s";
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return s === 0 ? `${m}m` : `${m}m ${s.toString().padStart(2, "0")}s`;
}

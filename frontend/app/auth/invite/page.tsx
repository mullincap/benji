"use client";

/**
 * frontend/app/(auth)/invite/page.tsx
 * ===================================
 * /auth/invite?token=… — accept invite & register.
 *
 * Mount: GET /api/auth/invite/{token}.
 *   - 200 → render the registration form with locked email + inviter banner
 *   - 404 → render expired/invalid error state
 *   - other → render generic error
 *
 * Submit: POST /api/auth/invite/{token}/accept with profile + password.
 *   - 200 → redirect to /auth/welcome (fresh user, first_login=true)
 *   - 400 → inline helper text under the offending field
 *   - 409 → card-level "email already in use" banner
 *   - 429 → rate-limit banner with retry_after countdown
 *
 * Password validation: client mirrors backend _password_score exactly.
 * Submit is disabled until score >= 3 ("Good") and terms checkbox is checked.
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";

import { useAuth } from "../../lib/auth";
import {
  AuthCard,
  Button,
  Checkbox,
  Field,
  Input,
  PasswordStrengthMeter,
  Select,
  scorePassword,
} from "../_components";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// Trader leads (the platform's primary persona today), Allocator second
// (next most common), then alphabetical for the rest. The first entry
// is the form's default role on initial render.
const ROLE_OPTIONS = [
  "Trader",
  "Allocator",
  "Analyst",
  "Fund Manager",
  "Other",
  "Quant / Researcher",
] as const;

type InviteDetail = {
  inviter_name: string;
  inviter_firm: string;
  invited_email: string;
  expires_at: string;
  // Non-binding hints from the admin's "New Invitation" form. Used to
  // prefill Firm + Role on first render. Invitee can override either
  // field — the persisted user record uses what they actually submit,
  // not these. NULL on invitations issued before migration 025 OR via
  // the CLI tool (which doesn't capture them).
  suggested_firm: string | null;
  suggested_role: string | null;
};

type LoadState =
  | { kind: "loading" }
  | { kind: "ready"; invite: InviteDetail }
  | { kind: "missing_token" }
  | { kind: "invalid_or_expired" }
  | { kind: "fetch_error"; status: number };

export default function AcceptInvitePage() {
  const router = useRouter();
  const { refetch } = useAuth();
  const [token, setToken] = useState<string | null>(null);

  const [state, setState] = useState<LoadState>({ kind: "loading" });

  // Form state — only meaningful in `ready`.
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [firm, setFirm] = useState("");
  const [role, setRole] = useState<string>(ROLE_OPTIONS[0]); // Trader default
  const [showPassword, setShowPassword] = useState(false);
  const [password, setPassword] = useState("");
  const [agreedToTerms, setAgreedToTerms] = useState(false);

  const [submitting, setSubmitting] = useState(false);
  const [bannerError, setBannerError] = useState<string | null>(null);
  const [fieldError, setFieldError] = useState<{ field: string; msg: string } | null>(null);
  const [retryAfter, setRetryAfter] = useState<number>(0);

  // Read token from URL and validate it once on mount. Combined into a
  // single effect so we don't flash an "invalid" state on the initial
  // render before useSearchParams-equivalent logic runs.
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const t = params.get("token");
    setToken(t);
    if (!t) {
      setState({ kind: "missing_token" });
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(API_BASE + `/api/auth/invite/${encodeURIComponent(t)}`, {
          credentials: "include",
        });
        if (cancelled) return;
        if (res.status === 200) {
          const data = (await res.json()) as InviteDetail;
          setState({ kind: "ready", invite: data });
          // Prefill Firm + Role from the admin's New Invitation hints
          // (migration 025). Both stay editable — invitee can override.
          // Null/missing firm: leave blank (firm is optional). Null or
          // unknown role: fall back to the existing default (Trader).
          // Defensive against suggested_role values that aren't in the
          // current ROLE_OPTIONS — happens if the role list changes
          // after an invitation is issued; pick the default instead of
          // landing on an unselectable dropdown value.
          if (data.suggested_firm) {
            setFirm(data.suggested_firm);
          }
          if (data.suggested_role && (ROLE_OPTIONS as readonly string[]).includes(data.suggested_role)) {
            setRole(data.suggested_role);
          }
        } else if (res.status === 404) {
          setState({ kind: "invalid_or_expired" });
        } else {
          setState({ kind: "fetch_error", status: res.status });
        }
      } catch {
        if (!cancelled) setState({ kind: "fetch_error", status: 0 });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Rate-limit countdown
  useEffect(() => {
    if (retryAfter <= 0) return;
    const id = window.setInterval(() => setRetryAfter((r) => Math.max(0, r - 1)), 1000);
    return () => window.clearInterval(id);
  }, [retryAfter]);

  const passwordResult = scorePassword(password);
  // firm intentionally omitted — it's an optional field. backend accepts
  // null/empty (Pydantic schema is `firm: str | None = None`, the
  // user_mgmt.users.firm column has been nullable since the schema was
  // first laid down).
  const canSubmit =
    state.kind === "ready" &&
    !submitting &&
    retryAfter === 0 &&
    firstName.trim() &&
    lastName.trim() &&
    role &&
    passwordResult.meetsMinimum &&
    agreedToTerms;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (state.kind !== "ready" || !canSubmit || !token) return;
    setSubmitting(true);
    setBannerError(null);
    setFieldError(null);

    try {
      const res = await fetch(
        API_BASE + `/api/auth/invite/${encodeURIComponent(token)}/accept`,
        {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            first_name: firstName.trim(),
            last_name: lastName.trim(),
            // firm is optional; whitespace-only counts as "no firm" too.
            firm: firm.trim() || null,
            role,
            password,
          }),
        },
      );

      if (res.ok) {
        // Refresh AuthProvider state so /auth/welcome (and onward
        // pages) see the just-created user. AuthProvider only fetches
        // on mount; without this, downstream topbar UI gated on
        // `user` (e.g. SIGN OUT button) stays hidden until first
        // hard reload.
        await refetch();
        router.replace("/auth/welcome");
        return;
      }

      const body = await res.json().catch(() => ({}));
      const detail: unknown = body?.detail;
      const detailStr = typeof detail === "string" ? detail : "";

      if (res.status === 400 && detailStr.toLowerCase().includes("password")) {
        setFieldError({ field: "password", msg: detailStr });
      } else if (res.status === 400) {
        setBannerError(detailStr || "Please fill in all required fields.");
      } else if (res.status === 404) {
        setState({ kind: "invalid_or_expired" });
      } else if (res.status === 409) {
        setBannerError(detailStr || "An account with that email already exists.");
      } else if (res.status === 429) {
        const retry = Number(
          (typeof detail === "object" && detail && "retry_after" in detail
            ? (detail as { retry_after: number }).retry_after
            : 0),
        );
        setRetryAfter(retry || 60);
        setBannerError("Too many attempts. Please slow down.");
      } else {
        setBannerError(`Sign-up failed (${res.status}). Try again.`);
      }
    } catch (err) {
      setBannerError(`Network error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSubmitting(false);
    }
  }

  // ─── Loading / error states ─────────────────────────────────────────────
  if (state.kind === "loading") {
    return (
      <AuthCard
        statusLeft={
          <>&gt; invite.token = <span style={{ color: "var(--amber)" }}>verifying</span></>
        }
        statusRight="step 1 / 2"
        eyebrow="[ Onboard · Accept Invite ]"
        title="Verifying invitation…"
        subtitle="Hang tight — checking your invite."
      >
        <div style={{ height: 60 }} />
      </AuthCard>
    );
  }

  if (state.kind === "missing_token" || state.kind === "invalid_or_expired") {
    return (
      <AuthCard
        statusLeft={
          <>&gt; invite.token = <span style={{ color: "var(--red)" }}>invalid</span></>
        }
        statusRight="step 1 / 2"
        eyebrow="[ Onboard · Accept Invite ]"
        title="Invite expired or invalid"
        subtitle={
          state.kind === "missing_token"
            ? "This URL is missing an invite token. Use the full link from your invitation email."
            : "This invite link is invalid or has expired. Contact your inviter for a new one."
        }
        footer={
          <>
            Already have an account?
            <Link
              href="/auth/signin"
              style={{ color: "var(--green)", textDecoration: "none", marginLeft: 6 }}
            >
              Sign in
            </Link>
          </>
        }
      >
        <Link
          href="/"
          style={{
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            width: "100%",
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
          Back to mullincap.com
        </Link>
      </AuthCard>
    );
  }

  if (state.kind === "fetch_error") {
    return (
      <AuthCard
        statusLeft={
          <>&gt; invite.token = <span style={{ color: "var(--red)" }}>error</span></>
        }
        statusRight={`http ${state.status || "0"}`}
        eyebrow="[ Onboard · Accept Invite ]"
        title="Couldn't verify invitation"
        subtitle="There was a problem reaching the server. Refresh to retry."
      >
        <button
          onClick={() => location.reload()}
          style={{
            width: "100%",
            padding: "13px 16px",
            fontFamily: "inherit",
            fontSize: 12,
            fontWeight: 700,
            letterSpacing: "0.14em",
            textTransform: "uppercase",
            border: "1px solid var(--border-bright)",
            borderRadius: 2,
            background: "transparent",
            color: "var(--t0)",
            cursor: "pointer",
          }}
        >
          Reload
        </button>
      </AuthCard>
    );
  }

  // ─── Ready: render the form ─────────────────────────────────────────────
  const { invite } = state;
  const expiresIn = formatExpiresIn(invite.expires_at);

  return (
    <AuthCard
      statusLeft={
        <>&gt; invite.token = <span style={{ color: "var(--green)" }}>valid</span></>
      }
      statusRight={`expires in ${expiresIn}`}
      eyebrow="[ Onboard · Accept Invite ]"
      title="Welcome to 3M"
      subtitle="Complete your account to access institutional-grade quantitative tooling."
      footer={
        <>
          Already have an account?
          <Link
            href="/auth/signin"
            style={{ color: "var(--green)", textDecoration: "none", marginLeft: 6 }}
          >
            Sign in
          </Link>
        </>
      }
    >
      <InviterBanner invite={invite} />

      {bannerError && (
        <div
          role="alert"
          style={{
            marginBottom: 20,
            padding: "10px 14px",
            background: "var(--bg3)",
            border: "1px solid var(--red)",
            borderLeft: "2px solid var(--red)",
            borderRadius: 2,
            color: "var(--t0)",
            fontSize: 12,
          }}
        >
          {bannerError}
          {retryAfter > 0 && (
            <>
              {" "}Try again in{" "}
              <span style={{ color: "var(--amber)", fontVariantNumeric: "tabular-nums" }}>
                {retryAfter}s
              </span>.
            </>
          )}
        </div>
      )}

      <form onSubmit={handleSubmit} noValidate>
        <Field label="Email">
          <Input value={invite.invited_email} locked />
          <div style={{ marginTop: 6, fontSize: 11, color: "var(--t3)" }}>
            Locked to invite. Contact your inviter to change.
          </div>
        </Field>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <Field label="First name" htmlFor="invite-first">
            <Input
              id="invite-first"
              value={firstName}
              onChange={(e) => setFirstName(e.target.value)}
              autoComplete="given-name"
              placeholder="Robert"
              required
            />
          </Field>
          <Field label="Last name" htmlFor="invite-last">
            <Input
              id="invite-last"
              value={lastName}
              onChange={(e) => setLastName(e.target.value)}
              autoComplete="family-name"
              placeholder="Leonard"
              required
            />
          </Field>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <Field label="Firm (optional)" htmlFor="invite-firm">
            <Input
              id="invite-firm"
              value={firm}
              onChange={(e) => setFirm(e.target.value)}
              autoComplete="organization"
              placeholder="Colonial Capital LLC"
            />
          </Field>
          <Field label="Role" htmlFor="invite-role">
            <Select id="invite-role" value={role} onChange={(e) => setRole(e.target.value)}>
              {ROLE_OPTIONS.map((opt) => (
                <option key={opt} value={opt}>{opt}</option>
              ))}
            </Select>
          </Field>
        </div>

        <Field
          label="Create password"
          htmlFor="invite-password"
          helper={
            fieldError?.field === "password"
              ? fieldError.msg
              : "Min 12 chars · mixed case · number · symbol"
          }
          helperKind={fieldError?.field === "password" ? "error" : "default"}
        >
          <div style={{ position: "relative" }}>
            <Input
              id="invite-password"
              type={showPassword ? "text" : "password"}
              value={password}
              onChange={(e) => {
                setPassword(e.target.value);
                if (fieldError?.field === "password") setFieldError(null);
              }}
              autoComplete="new-password"
              placeholder="••••••••••••"
              required
              invalid={fieldError?.field === "password"}
              style={{ paddingRight: 44 }}
            />
            <button
              type="button"
              onClick={() => setShowPassword(v => !v)}
              aria-label={showPassword ? "Hide password" : "Show password"}
              aria-pressed={showPassword}
              style={{
                position: "absolute",
                right: 6,
                top: "50%",
                transform: "translateY(-50%)",
                width: 32,
                height: 32,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                background: "transparent",
                border: "none",
                color: "var(--t3)",
                cursor: "pointer",
                padding: 0,
                borderRadius: 2,
                transition: "color 0.12s ease",
              }}
              onMouseEnter={e => { e.currentTarget.style.color = "var(--t1)"; }}
              onMouseLeave={e => { e.currentTarget.style.color = "var(--t3)"; }}
            >
              {showPassword ? (
                /* Eye-off (password visible — click to hide) */
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M9.88 9.88a3 3 0 0 0 4.24 4.24" />
                  <path d="M10.73 5.08A10.43 10.43 0 0 1 12 5c7 0 11 7 11 7a13.16 13.16 0 0 1-1.67 2.68" />
                  <path d="M6.61 6.61A13.526 13.526 0 0 0 1 12s4 7 11 7a9.74 9.74 0 0 0 5.39-1.61" />
                  <line x1="2" y1="2" x2="22" y2="22" />
                </svg>
              ) : (
                /* Eye (password hidden — click to show) */
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7-11-7-11-7Z" />
                  <circle cx="12" cy="12" r="3" />
                </svg>
              )}
            </button>
          </div>
          {password.length > 0 && (
            <PasswordStrengthMeter
              password={password}
              helper="Strength"
            />
          )}
        </Field>

        <Checkbox checked={agreedToTerms} onChange={setAgreedToTerms}>
          I agree to the{" "}
          <Link href="/terms" style={{ color: "var(--t0)", textDecoration: "underline" }}>
            Terms of Service
          </Link>{" "}
          and{" "}
          <Link href="/privacy" style={{ color: "var(--t0)", textDecoration: "underline" }}>
            Privacy Policy
          </Link>
          , and acknowledge the platform handles real capital execution.
        </Checkbox>

        <Button
          type="submit"
          variant="primary"
          trailingArrow={!submitting}
          loading={submitting}
          disabled={!canSubmit}
        >
          Create account
        </Button>
      </form>
    </AuthCard>
  );
}

// ─── Helpers ───────────────────────────────────────────────────────────────

function InviterBanner({ invite }: { invite: InviteDetail }) {
  return (
    <div
      style={{
        marginBottom: 24,
        padding: "14px 16px",
        background: "var(--bg3)",
        border: "1px solid var(--line)",
        borderLeft: "2px solid var(--green)",
        borderRadius: 2,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 4,
        }}
      >
        <span
          style={{
            fontSize: 9,
            letterSpacing: "0.16em",
            textTransform: "uppercase",
            color: "var(--green)",
            fontWeight: 700,
          }}
        >
          Invited by
        </span>
      </div>
      <div style={{ fontSize: 12, color: "var(--t0)" }}>
        <span style={{ fontWeight: 700 }}>{invite.inviter_name}</span>
        <span style={{ color: "var(--t2)" }}> · {invite.inviter_firm}</span>
      </div>
    </div>
  );
}

function formatExpiresIn(iso: string): string {
  const ms = new Date(iso).getTime() - Date.now();
  if (ms <= 0) return "expired";
  const days = Math.floor(ms / (24 * 60 * 60 * 1000));
  const hours = Math.floor((ms % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000));
  if (days > 0) return `${days}d`;
  if (hours > 0) return `${hours}h`;
  return "<1h";
}

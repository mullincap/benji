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

const ROLE_OPTIONS = [
  "Fund Manager",
  "Allocator",
  "Quant / Researcher",
  "Analyst",
  "Other",
] as const;

type InviteDetail = {
  inviter_name: string;
  inviter_firm: string;
  invited_email: string;
  expires_at: string;
};

type LoadState =
  | { kind: "loading" }
  | { kind: "ready"; invite: InviteDetail }
  | { kind: "missing_token" }
  | { kind: "invalid_or_expired" }
  | { kind: "fetch_error"; status: number };

export default function AcceptInvitePage() {
  const router = useRouter();
  const [token, setToken] = useState<string | null>(null);

  const [state, setState] = useState<LoadState>({ kind: "loading" });

  // Form state — only meaningful in `ready`.
  const [firstName, setFirstName] = useState("");
  const [lastName, setLastName] = useState("");
  const [firm, setFirm] = useState("");
  const [role, setRole] = useState<string>(ROLE_OPTIONS[1]); // Allocator default
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
  const canSubmit =
    state.kind === "ready" &&
    !submitting &&
    retryAfter === 0 &&
    firstName.trim() &&
    lastName.trim() &&
    firm.trim() &&
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
            firm: firm.trim(),
            role,
            password,
          }),
        },
      );

      if (res.ok) {
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
          <Field label="Firm" htmlFor="invite-firm">
            <Input
              id="invite-firm"
              value={firm}
              onChange={(e) => setFirm(e.target.value)}
              autoComplete="organization"
              placeholder="Colonial Capital LLC"
              required
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
          <Input
            id="invite-password"
            type="password"
            value={password}
            onChange={(e) => {
              setPassword(e.target.value);
              if (fieldError?.field === "password") setFieldError(null);
            }}
            autoComplete="new-password"
            placeholder="••••••••••••"
            required
            invalid={fieldError?.field === "password"}
          />
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

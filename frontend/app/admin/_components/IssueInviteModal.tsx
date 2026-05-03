"use client";

/**
 * IssueInviteModal — admin issues a new invitation.
 *
 * Two-stage UI matching ResetPasswordModal:
 *   form  → email + firm + role + expires-in select, "Generate Invite Link"
 *   shown → green-bordered URL display with copy button
 *
 * The invite URL is shown ONCE per call. If the admin loses it, they
 * regenerate (which leaves the OLD invite revocable but inactive in
 * practice — there's no way to re-display the URL once minted, that's
 * the security trade we accepted in Phase 1a).
 */

import { useState } from "react";

import Modal from "./Modal";
import { issueInvitation } from "../_lib/api";

// Mirrors the acceptance-form list (frontend/app/auth/invite/page.tsx).
// Trader leads (platform's primary persona today), Allocator next, then
// alphabetical. First entry is the form's default.
const ROLE_OPTIONS = [
  "Trader",
  "Allocator",
  "Analyst",
  "Fund Manager",
  "Other",
  "Quant / Researcher",
] as const;

const EXPIRES_OPTIONS: Array<{ value: number; label: string }> = [
  { value: 3,  label: "3 days" },
  { value: 7,  label: "7 days" },
  { value: 14, label: "14 days" },
  { value: 30, label: "30 days" },
];

type Props = {
  onClose: () => void;
  /** Called after successful issuance — parent refetches the list. */
  onIssued?: () => void;
};

export default function IssueInviteModal({ onClose, onIssued }: Props) {
  const [email, setEmail] = useState("");
  const [firm, setFirm] = useState("");
  const [role, setRole] = useState<string>(ROLE_OPTIONS[0]); // Trader default
  const [expires, setExpires] = useState<number>(7);

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [inviteUrl, setInviteUrl] = useState<string | null>(null);
  const [expiresAt, setExpiresAt] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  async function handleGenerate() {
    setError(null);
    if (!email.trim()) {
      setError("Email is required.");
      return;
    }
    setSubmitting(true);
    try {
      const res = await issueInvitation({
        email: email.trim(),
        // firm is optional; whitespace-only collapses to null.
        firm: firm.trim() || null,
        role,
        expires_in_days: expires,
      });
      setInviteUrl(res.invite_url);
      setExpiresAt(res.expires_at);
      try {
        await navigator.clipboard.writeText(res.invite_url);
        setCopied(true);
      } catch {
        // No-op — admin can manually select-copy
      }
      onIssued?.();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleCopy() {
    if (!inviteUrl) return;
    try {
      await navigator.clipboard.writeText(inviteUrl);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2500);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  if (inviteUrl) {
    // ─── Stage 2: shown ──────────────────────────────────────────────────
    const expiresHuman = expiresAt
      ? new Date(expiresAt).toLocaleString(undefined, {
          dateStyle: "medium",
          timeStyle: "short",
        })
      : "—";

    return (
      <Modal
        eyebrow="[ Invitation Issued ]"
        title="Send this link out-of-band"
        onClose={onClose}
        footer={<SecondaryButton onClick={onClose}>Done</SecondaryButton>}
      >
        <p style={{ color: "var(--t2)", fontSize: 12, marginBottom: 16, lineHeight: 1.6 }}>
          Send the link below to <strong style={{ color: "var(--t0)" }}>{email}</strong> via Signal,
          encrypted email, or in person. The token is shown <strong style={{ color: "var(--amber)" }}>only once</strong>;
          if you lose it, regenerate (and revoke the old one if needed).
        </p>

        <div
          style={{
            background: "var(--bg3)",
            border: "1px solid var(--green)",
            borderRadius: 2,
            padding: 14,
            marginBottom: 12,
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 8,
            }}
          >
            <span
              style={{
                color: "var(--green)",
                fontSize: 10,
                letterSpacing: "0.16em",
                textTransform: "uppercase",
                fontWeight: 700,
              }}
            >
              ↳ One-Time Invite Link
            </span>
            <span style={{ color: "var(--t3)", fontSize: 10 }}>expires {expiresHuman}</span>
          </div>
          <div
            style={{
              background: "var(--bg2)",
              padding: "10px 12px",
              border: "1px solid var(--line)",
              color: "var(--t0)",
              fontSize: 11,
              wordBreak: "break-all",
              borderRadius: 2,
              lineHeight: 1.6,
            }}
          >
            {inviteUrl}
          </div>
          <button
            type="button"
            onClick={handleCopy}
            style={{
              marginTop: 10,
              background: "transparent",
              border: "1px solid var(--line2)",
              color: copied ? "var(--green)" : "var(--t0)",
              padding: "5px 10px",
              fontSize: 10,
              fontFamily: "inherit",
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              borderRadius: 2,
              cursor: "pointer",
            }}
          >
            {copied ? "✓ Copied to clipboard" : "Copy"}
          </button>
        </div>

        {error && (
          <div role="alert" style={{ color: "var(--red)", fontSize: 11 }}>
            {error}
          </div>
        )}
      </Modal>
    );
  }

  // ─── Stage 1: form ─────────────────────────────────────────────────────
  return (
    <Modal
      eyebrow="[ Action · Issue Invitation ]"
      title="New Invitation"
      onClose={onClose}
      footer={
        <>
          <SecondaryButton onClick={onClose} disabled={submitting}>
            Cancel
          </SecondaryButton>
          <AmberButton onClick={handleGenerate} loading={submitting}>
            Generate Invite Link →
          </AmberButton>
        </>
      }
    >
      <p style={{ color: "var(--t2)", fontSize: 12, marginBottom: 18, lineHeight: 1.6 }}>
        Generates a one-time, token-based invite link. Email delivery is not
        wired in v1 — the link is shown once after generation; copy it and
        deliver via your preferred secure channel.
      </p>

      <Field label="Email Address">
        <Input
          type="email"
          value={email}
          onChange={setEmail}
          placeholder="invitee@firm.com"
          disabled={submitting}
        />
      </Field>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <Field label="Firm (optional)">
          <Input
            value={firm}
            onChange={setFirm}
            placeholder="Firm name"
            disabled={submitting}
          />
        </Field>
        <Field label="Role">
          <Select value={role} onChange={setRole} disabled={submitting}>
            {ROLE_OPTIONS.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </Select>
        </Field>
      </div>

      <Field label="Expires In">
        <Select
          value={String(expires)}
          onChange={(v) => setExpires(parseInt(v, 10))}
          disabled={submitting}
        >
          {EXPIRES_OPTIONS.map((o) => (
            <option key={o.value} value={String(o.value)}>
              {o.label}
            </option>
          ))}
        </Select>
      </Field>

      {error && (
        <div role="alert" style={{ color: "var(--red)", fontSize: 11, marginTop: 12 }}>
          {error}
        </div>
      )}
    </Modal>
  );
}

// ─── Tiny inline form primitives (admin-flavored, distinct from auth/_components) ──

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={{ marginBottom: 14 }}>
      <label
        style={{
          display: "block",
          color: "var(--t2)",
          fontSize: 10,
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          marginBottom: 6,
        }}
      >
        {label}
      </label>
      {children}
    </div>
  );
}

function Input({
  value,
  onChange,
  placeholder,
  disabled,
  type = "text",
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  disabled?: boolean;
  type?: string;
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      disabled={disabled}
      style={{
        width: "100%",
        background: "var(--bg3)",
        border: "1px solid var(--line)",
        color: "var(--t0)",
        fontFamily: "inherit",
        fontSize: 13,
        padding: "10px 12px",
        borderRadius: 2,
        outline: "none",
      }}
      onFocus={(e) => {
        e.currentTarget.style.borderColor = "var(--amber)";
      }}
      onBlur={(e) => {
        e.currentTarget.style.borderColor = "var(--line)";
      }}
    />
  );
}

function Select({
  value,
  onChange,
  disabled,
  children,
}: {
  value: string;
  onChange: (v: string) => void;
  disabled?: boolean;
  children: React.ReactNode;
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      disabled={disabled}
      style={{
        width: "100%",
        background: "var(--bg3)",
        border: "1px solid var(--line)",
        color: "var(--t0)",
        fontFamily: "inherit",
        fontSize: 13,
        padding: "10px 36px 10px 12px",
        borderRadius: 2,
        appearance: "none",
        outline: "none",
        cursor: "pointer",
      }}
      onFocus={(e) => {
        e.currentTarget.style.borderColor = "var(--amber)";
      }}
      onBlur={(e) => {
        e.currentTarget.style.borderColor = "var(--line)";
      }}
    >
      {children}
    </select>
  );
}

function AmberButton({
  onClick,
  loading,
  children,
}: {
  onClick: () => void;
  loading?: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={loading}
      style={{
        background: "var(--amber)",
        color: "#1a1100",
        border: "1px solid var(--amber)",
        borderRadius: 2,
        padding: "9px 14px",
        fontFamily: "inherit",
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        cursor: loading ? "not-allowed" : "pointer",
        opacity: loading ? 0.6 : 1,
      }}
    >
      {loading ? "Working…" : children}
    </button>
  );
}

function SecondaryButton({
  onClick,
  disabled,
  children,
}: {
  onClick: () => void;
  disabled?: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      style={{
        background: "transparent",
        color: "var(--t0)",
        border: "1px solid var(--line2)",
        borderRadius: 2,
        padding: "9px 14px",
        fontFamily: "inherit",
        fontSize: 11,
        fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.6 : 1,
      }}
    >
      {children}
    </button>
  );
}

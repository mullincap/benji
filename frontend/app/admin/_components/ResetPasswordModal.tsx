"use client";

/**
 * ResetPasswordModal — admin issues a temp password for a target user.
 *
 * Two-stage UI:
 *   stage === 'confirm' → "Generate" button + warning copy
 *   stage === 'shown'   → green-bordered token display, copy button,
 *                          "Done" + "Regenerate" footer
 *
 * The temp password is shown ONCE per call. Closing the modal discards
 * the local copy; if the admin loses it, they generate a new one
 * (which invalidates the previous, since the endpoint replaces the
 * stored hash on each call).
 */

import { useState } from "react";

import Modal from "./Modal";
import { adminResetPassword } from "../_lib/api";

type Props = {
  userId: string;
  userEmail: string;
  userName: string;
  onClose: () => void;
};

export default function ResetPasswordModal({ userId, userEmail, userName, onClose }: Props) {
  const [stage, setStage] = useState<"confirm" | "shown">("confirm");
  const [tempPassword, setTempPassword] = useState<string | null>(null);
  const [sessionsRevoked, setSessionsRevoked] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  async function handleGenerate() {
    setSubmitting(true);
    setError(null);
    try {
      const res = await adminResetPassword(userId);
      setTempPassword(res.temp_password);
      setSessionsRevoked(res.sessions_revoked);
      setStage("shown");
      // Auto-copy on display.
      try {
        await navigator.clipboard.writeText(res.temp_password);
        setCopied(true);
      } catch {
        // Some contexts don't have clipboard access — render the
        // value anyway, admin can select-copy manually.
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleCopy() {
    if (!tempPassword) return;
    try {
      await navigator.clipboard.writeText(tempPassword);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2500);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  if (stage === "confirm") {
    return (
      <Modal
        eyebrow="[ Action · Generate Temp Password ]"
        title={`Reset password for ${userName}`}
        onClose={onClose}
        footer={
          <>
            <SecondaryButton onClick={onClose} disabled={submitting}>
              Cancel
            </SecondaryButton>
            <AmberButton onClick={handleGenerate} loading={submitting}>
              Generate Temp Password
            </AmberButton>
          </>
        }
      >
        <p style={{ color: "var(--t2)", fontSize: 12, marginBottom: 16, lineHeight: 1.6 }}>
          Generates a one-time temporary password for{" "}
          <strong style={{ color: "var(--t0)" }}>{userEmail}</strong>. The
          password is shown <strong style={{ color: "var(--amber)" }}>only once</strong>;
          copy it before closing this dialog and send it via your preferred
          secure channel.
        </p>
        <div
          style={{
            background: "rgba(239, 68, 68, 0.06)",
            borderLeft: "2px solid var(--red)",
            padding: "10px 12px",
            color: "var(--t2)",
            fontSize: 11,
            lineHeight: 1.55,
            borderRadius: 2,
          }}
        >
          <strong style={{ color: "var(--t0)" }}>Side effects:</strong> all of
          this user&apos;s active sessions will be revoked, and{" "}
          <code>password_is_temporary</code> is set to true (the user sees a
          banner prompting them to change it). Any active lockout is cleared.
        </div>
        {error && (
          <div role="alert" style={{ color: "var(--red)", fontSize: 11, marginTop: 12 }}>
            {error}
          </div>
        )}
      </Modal>
    );
  }

  // stage === 'shown'
  return (
    <Modal
      eyebrow="[ Temporary Password ]"
      title={`Send this to ${userName}`}
      onClose={onClose}
      footer={
        <>
          <SecondaryButton onClick={onClose}>Done</SecondaryButton>
          <AmberButton onClick={handleGenerate} loading={submitting}>
            ↻ Regenerate
          </AmberButton>
        </>
      }
    >
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
            ↳ One-Time Password
          </span>
          <button
            type="button"
            onClick={handleCopy}
            style={{
              background: "transparent",
              border: "1px solid var(--line2)",
              color: copied ? "var(--green)" : "var(--t2)",
              padding: "3px 8px",
              fontSize: 9,
              fontFamily: "inherit",
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              borderRadius: 2,
              cursor: "pointer",
            }}
          >
            {copied ? "✓ Copied" : "Copy"}
          </button>
        </div>
        <div
          style={{
            background: "var(--bg2)",
            padding: "10px 12px",
            border: "1px solid var(--line)",
            color: "var(--t0)",
            fontSize: 16,
            fontWeight: 700,
            wordBreak: "break-all",
            borderRadius: 2,
            lineHeight: 1.4,
            fontFamily: "inherit",
          }}
        >
          {tempPassword}
        </div>
        {copied && (
          <div style={{ color: "var(--green)", fontSize: 10, marginTop: 8, letterSpacing: "0.04em" }}>
            ✓ Copied to clipboard
          </div>
        )}
      </div>

      <div
        style={{
          background: "rgba(239, 68, 68, 0.06)",
          borderLeft: "2px solid var(--red)",
          padding: "10px 12px",
          color: "var(--t2)",
          fontSize: 11,
          lineHeight: 1.55,
          borderRadius: 2,
        }}
      >
        <strong style={{ color: "var(--t0)" }}>Send via secure channel only.</strong>{" "}
        Never paste into a public channel or unencrypted email.{" "}
        {sessionsRevoked > 0 && (
          <>
            {sessionsRevoked} active session
            {sessionsRevoked === 1 ? " was" : "s were"} revoked.
          </>
        )}
      </div>

      {error && (
        <div role="alert" style={{ color: "var(--red)", fontSize: 11, marginTop: 12 }}>
          {error}
        </div>
      )}
    </Modal>
  );
}

// ─── Buttons (admin amber + secondary outline) ────────────────────────────

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

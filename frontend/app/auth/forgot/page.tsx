/**
 * frontend/app/(auth)/forgot/page.tsx
 * ===================================
 * /auth/forgot — Phase 1a stub.
 *
 * Self-service password reset (token email + new-password form) lands in
 * Phase 1b. For now this page just tells the user to contact J directly.
 * No form, no API call. Replace with the real reset flow when the email
 * infra is in place.
 */

import Link from "next/link";
import { AuthCard } from "../_components";

const SUPPORT_EMAIL = "j@mullincap.com";

const SECONDARY_LINK_BUTTON: React.CSSProperties = {
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
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
  textDecoration: "none",
  transition: "all 0.12s ease",
};

export default function ForgotPasswordPage() {
  return (
    <AuthCard
      statusLeft={
        <>
          &gt; reset.flow ={" "}
          <span style={{ color: "var(--amber)" }}>manual</span>
        </>
      }
      statusRight="phase 1a"
      eyebrow="[ Auth · Password Reset ]"
      title="Reset your password"
      subtitle={
        <>
          Self-service password reset is coming soon. To reset your password
          now, email{" "}
          <a
            href={`mailto:${SUPPORT_EMAIL}?subject=${encodeURIComponent(
              "3M password reset",
            )}`}
            style={{ color: "var(--green)", textDecoration: "none" }}
          >
            {SUPPORT_EMAIL}
          </a>{" "}
          and we&apos;ll get you a new password within one business day.
        </>
      }
      footer={
        <>
          Remember it after all?
          <Link
            href="/auth/signin"
            style={{
              color: "var(--green)",
              textDecoration: "none",
              marginLeft: 6,
            }}
          >
            Back to sign in
          </Link>
        </>
      }
    >
      <Link href="/auth/signin" style={SECONDARY_LINK_BUTTON}>
        Back to sign in
      </Link>
    </AuthCard>
  );
}

"use client";

/**
 * frontend/app/settings/security/page.tsx
 * =========================================
 * /settings/security — user-facing change-password page.
 *
 * NEW TOP-LEVEL ROUTE flag (raised at scope time):
 *   No top-level /settings/ shell exists today — only the trader-portal
 *   /trader/(protected)/settings/. This route lives at the app root so
 *   it's reachable from the temp-password banner regardless of which
 *   module the user happens to be in.
 *
 *   When/if a unified /settings/ shell ships (Phase 1c?), this page
 *   should slot in under that shell. For Phase 1, it's intentionally
 *   minimal — single auth-style card, no left-rail navigation.
 *
 * Two flow paths:
 *   1. user.password_is_temporary === true
 *      → no current-password field; the temp was already verified at login
 *   2. user.password_is_temporary === false
 *      → current-password required, verified server-side
 *
 * Reuses the Phase 1a PasswordStrengthMeter + scorePassword helpers so
 * client-side validation matches the server's _password_score().
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";

import {
  AuthCard,
  Button,
  Field,
  Input,
  PasswordStrengthMeter,
  scorePassword,
} from "../../auth/_components";
import { useAuth } from "../../lib/auth";
import { changePassword } from "../../admin/_lib/api";

const BUILD_HASH = process.env.NEXT_PUBLIC_BUILD_HASH || "dev";

export default function SecurityPage() {
  const router = useRouter();
  const { user, loading, refetch } = useAuth();

  // Bounce unauthed users. Middleware doesn't gate /settings yet
  // (this is a brand-new route); page-level redirect is fine while
  // the route is alone in the /settings tree.
  useEffect(() => {
    if (loading) return;
    if (!user) {
      router.replace("/auth/signin?next=/settings/security");
    }
  }, [loading, user, router]);

  const isTemp = user?.password_is_temporary === true;

  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [bannerError, setBannerError] = useState<string | null>(null);
  const [fieldError, setFieldError] = useState<{ field: string; msg: string } | null>(null);
  const [success, setSuccess] = useState(false);

  const passwordResult = scorePassword(newPassword);
  const passwordsMatch = newPassword.length > 0 && newPassword === confirmPassword;
  const canSubmit =
    !submitting &&
    !success &&
    passwordResult.meetsMinimum &&
    passwordsMatch &&
    (isTemp || currentPassword.length > 0);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!canSubmit) return;
    if (!passwordsMatch) {
      setFieldError({ field: "confirm", msg: "Passwords do not match." });
      return;
    }
    setSubmitting(true);
    setBannerError(null);
    setFieldError(null);
    try {
      await changePassword(newPassword, isTemp ? undefined : currentPassword);
      setSuccess(true);
      // Refetch /me so password_is_temporary flips false in context →
      // banner disappears site-wide on next render.
      await refetch().catch(() => {});
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      // Heuristic mapping of server errors to fields.
      if (msg.includes("Current password incorrect") || msg.includes("Current password required")) {
        setFieldError({ field: "current", msg: "Current password is incorrect or missing." });
      } else if (msg.toLowerCase().includes("password too weak")) {
        setFieldError({ field: "new", msg: "Password too weak. Use 12+ chars with mixed case, number, and symbol." });
      } else {
        setBannerError(msg);
      }
    } finally {
      setSubmitting(false);
    }
  }

  // ─── Render ───────────────────────────────────────────────────────────
  // Reuse the auth-shell visual treatment for visual consistency with
  // /auth/signin and /auth/welcome.
  return (
    <div
      className="auth-shell"
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        padding: "32px 24px",
        minHeight: "100vh",
      }}
    >
      <header
        style={{
          width: "100%",
          maxWidth: 1200,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 8px",
          marginBottom: 56,
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <Link href="/" style={{ fontWeight: 700, fontSize: 14, color: "var(--t0)", textDecoration: "none" }}>
            3M
          </Link>
          <span
            style={{
              color: "var(--t3)",
              fontSize: 10,
              letterSpacing: "0.18em",
              textTransform: "uppercase",
              paddingLeft: 12,
              borderLeft: "1px solid var(--line)",
            }}
          >
            Settings · Security
          </span>
        </div>
        {user && (
          <span style={{ color: "var(--t2)", fontSize: 11 }}>{user.email}</span>
        )}
      </header>

      <main style={{ flex: 1, width: "100%", display: "flex", justifyContent: "center" }}>
        {success ? (
          <AuthCard
            statusLeft={
              <>
                &gt; password.set ={" "}
                <span style={{ color: "var(--green)" }}>complete</span>
              </>
            }
            eyebrow="[ Settings · Security ]"
            title="Password changed"
            subtitle="Your password has been updated. The temporary-password banner is gone."
            footer={
              <>
                Take me to{" "}
                <Link
                  href="/"
                  style={{ color: "var(--green)", textDecoration: "none", marginLeft: 6 }}
                >
                  the dashboard
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
                border: "1px solid var(--green)",
                background: "var(--green)",
                color: "#001a14",
                borderRadius: 2,
                textDecoration: "none",
              }}
            >
              Continue
            </Link>
          </AuthCard>
        ) : (
          <AuthCard
            statusLeft={
              isTemp ? (
                <>
                  &gt; password ={" "}
                  <span style={{ color: "var(--amber)" }}>temporary</span>
                </>
              ) : (
                <>
                  &gt; settings ={" "}
                  <span style={{ color: "var(--green)" }}>security</span>
                </>
              )
            }
            statusRight={`build ${BUILD_HASH}`}
            eyebrow="[ Settings · Security ]"
            title={isTemp ? "Set a permanent password" : "Change password"}
            subtitle={
              isTemp
                ? "An admin issued you a temporary password. Choose a permanent one now — once changed, your sessions on other devices stay active."
                : "Update your account password. Other devices stay signed in unless you also revoke their sessions."
            }
          >
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
              </div>
            )}

            <form onSubmit={handleSubmit} noValidate>
              {!isTemp && (
                <Field
                  label="Current password"
                  htmlFor="current-pw"
                  helper={fieldError?.field === "current" ? fieldError.msg : undefined}
                  helperKind={fieldError?.field === "current" ? "error" : "default"}
                >
                  <Input
                    id="current-pw"
                    type="password"
                    autoComplete="current-password"
                    value={currentPassword}
                    onChange={(e) => {
                      setCurrentPassword(e.target.value);
                      if (fieldError?.field === "current") setFieldError(null);
                    }}
                    invalid={fieldError?.field === "current"}
                    required
                  />
                </Field>
              )}

              <Field
                label="New password"
                htmlFor="new-pw"
                helper={
                  fieldError?.field === "new"
                    ? fieldError.msg
                    : "Min 12 chars · mixed case · number · symbol"
                }
                helperKind={fieldError?.field === "new" ? "error" : "default"}
              >
                <Input
                  id="new-pw"
                  type="password"
                  autoComplete="new-password"
                  value={newPassword}
                  onChange={(e) => {
                    setNewPassword(e.target.value);
                    if (fieldError?.field === "new") setFieldError(null);
                  }}
                  invalid={fieldError?.field === "new"}
                  required
                />
                {newPassword.length > 0 && (
                  <PasswordStrengthMeter password={newPassword} />
                )}
              </Field>

              <Field
                label="Confirm new password"
                htmlFor="confirm-pw"
                helper={
                  fieldError?.field === "confirm"
                    ? fieldError.msg
                    : confirmPassword.length > 0 && passwordsMatch
                    ? "✓ Passwords match"
                    : confirmPassword.length > 0 && !passwordsMatch
                    ? "Passwords do not match"
                    : undefined
                }
                helperKind={
                  fieldError?.field === "confirm" || (confirmPassword.length > 0 && !passwordsMatch)
                    ? "error"
                    : confirmPassword.length > 0 && passwordsMatch
                    ? "success"
                    : "default"
                }
              >
                <Input
                  id="confirm-pw"
                  type="password"
                  autoComplete="new-password"
                  value={confirmPassword}
                  onChange={(e) => {
                    setConfirmPassword(e.target.value);
                    if (fieldError?.field === "confirm") setFieldError(null);
                  }}
                  invalid={
                    fieldError?.field === "confirm" ||
                    (confirmPassword.length > 0 && !passwordsMatch)
                  }
                  required
                />
              </Field>

              <Button
                type="submit"
                variant="primary"
                trailingArrow={!submitting}
                loading={submitting}
                disabled={!canSubmit}
              >
                Update password
              </Button>
            </form>
          </AuthCard>
        )}
      </main>
    </div>
  );
}

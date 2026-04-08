"use client";

/**
 * frontend/app/compiler/(public)/login/page.tsx
 * =============================================
 * Compiler admin login page.
 *
 * UX:
 *   - Single passphrase input field, centered, dark theme
 *   - Submit POSTs to /api/admin/login (credentials: include) so the
 *     admin_session cookie is captured by the browser
 *   - On success → router.push('/compiler') which redirects to /coverage
 *   - On 401 → shows "Invalid passphrase" in --red below the field
 *   - On mount, calls /api/admin/whoami; if already authed, redirects
 *     immediately to /compiler so reloading after a successful login
 *     doesn't strand the user on the login page
 *
 * The form is intentionally minimal — no logo, no help text, no rememberme.
 * This is an admin tool, not a marketing surface.
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
// The accent color comes from the active theme via the --module-accent CSS
// variable. Topbar.tsx sets this on :root when the active route is under
// /compiler/*. We deliberately do NOT hardcode a hex here — the compiler
// module follows the user's selected theme like every other module.

export default function CompilerLoginPage() {
  const router = useRouter();
  const [passphrase, setPassphrase] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [checkingExisting, setCheckingExisting] = useState(true);

  // If the user is already authenticated, skip the login form
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/admin/whoami`, { credentials: "include" })
      .then((r) => r.json())
      .then((data) => {
        if (cancelled) return;
        if (data?.authenticated) {
          router.replace("/compiler");
        } else {
          setCheckingExisting(false);
        }
      })
      .catch(() => {
        if (!cancelled) setCheckingExisting(false);
      });
    return () => {
      cancelled = true;
    };
  }, [router]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!passphrase || submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/api/admin/login`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ passphrase }),
      });
      if (res.status === 200) {
        router.replace("/compiler");
      } else if (res.status === 401) {
        setError("Invalid passphrase");
      } else if (res.status === 503) {
        setError("Admin login is not configured on the server");
      } else {
        setError(`Unexpected error (${res.status})`);
      }
    } catch (err) {
      setError(`Network error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSubmitting(false);
    }
  }

  if (checkingExisting) {
    return (
      <div style={{
        fontSize: 9, color: "var(--t3)",
        textTransform: "uppercase", letterSpacing: "0.12em",
      }}>
        Checking session…
      </div>
    );
  }

  return (
    <form
      onSubmit={handleSubmit}
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "32px 36px",
        width: 360,
        display: "flex",
        flexDirection: "column",
        gap: 16,
      }}
    >
      <div style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase",
      }}>
        Compiler · Admin
      </div>

      <div style={{
        fontSize: 18, fontWeight: 700, color: "var(--t0)",
        marginBottom: 4,
      }}>
        Enter passphrase
      </div>

      <input
        type="password"
        value={passphrase}
        onChange={(e) => { setPassphrase(e.target.value); setError(null); }}
        placeholder="••••••••"
        autoFocus
        autoComplete="current-password"
        disabled={submitting}
        style={{
          background: "var(--bg3)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          padding: "10px 12px",
          color: "var(--t0)",
          fontSize: 12,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          outline: "none",
        }}
        onFocus={(e) => { e.currentTarget.style.borderColor = "var(--module-accent)"; }}
        onBlur={(e) => { e.currentTarget.style.borderColor = "var(--line)"; }}
      />

      <button
        type="submit"
        disabled={!passphrase || submitting}
        style={{
          background: passphrase && !submitting ? "var(--module-accent)" : "var(--bg3)",
          color: passphrase && !submitting ? "#0a0a0a" : "var(--t2)",
          border: "none",
          borderRadius: 6,
          padding: "10px 14px",
          fontSize: 10,
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          cursor: passphrase && !submitting ? "pointer" : "not-allowed",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          transition: "all 0.15s ease",
        }}
      >
        {submitting ? "Authenticating…" : "Enter"}
      </button>

      {error && (
        <div style={{
          fontSize: 10, color: "var(--red)",
          letterSpacing: "0.04em",
        }}>
          {error}
        </div>
      )}
    </form>
  );
}

"use client";

/**
 * frontend/app/trader/(public)/login/page.tsx
 * ============================================
 * Allocator user login page.
 *
 * UX:
 *   - Email + password inputs, centered, dark theme
 *   - Submit POSTs to /api/auth/login (credentials: include) so the
 *     user_session cookie is captured by the browser
 *   - On success -> router.push('/trader/overview')
 *   - On 401 -> shows "Invalid credentials" in --red below the fields
 *   - On mount, calls /api/auth/me; if already authed, redirects
 *     immediately so reloading doesn't strand the user on login
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

export default function TraderLoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [checkingExisting, setCheckingExisting] = useState(true);

  // If already authenticated, skip login
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/auth/me`, { credentials: "include" })
      .then((r) => {
        if (r.ok) return r.json();
        throw new Error("not authed");
      })
      .then((data) => {
        if (cancelled) return;
        if (data?.user_id) {
          router.replace("/trader/overview");
        } else {
          setCheckingExisting(false);
        }
      })
      .catch(() => {
        if (!cancelled) setCheckingExisting(false);
      });
    return () => { cancelled = true; };
  }, [router]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!email || !password || submitting) return;
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/api/auth/login`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      if (res.status === 200) {
        router.replace("/trader/overview");
      } else if (res.status === 401) {
        setError("Invalid credentials");
      } else if (res.status === 403) {
        setError("Account deactivated");
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
        Checking session...
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
        Trader
      </div>

      <div style={{
        fontSize: 18, fontWeight: 700, color: "var(--t0)",
        marginBottom: 4,
      }}>
        Sign in
      </div>

      <div>
        <div style={{
          fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em",
          fontWeight: 700, textTransform: "uppercase", marginBottom: 6,
        }}>
          EMAIL
        </div>
        <input
          type="email"
          value={email}
          onChange={(e) => { setEmail(e.target.value); setError(null); }}
          placeholder="you@example.com"
          autoFocus
          autoComplete="email"
          disabled={submitting}
          style={{
            width: "100%",
            background: "var(--bg3)",
            border: "1px solid var(--line)",
            borderRadius: 6,
            padding: "10px 12px",
            color: "var(--t0)",
            fontSize: 12,
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            outline: "none",
          }}
          onFocus={(e) => { e.currentTarget.style.borderColor = "var(--green)"; }}
          onBlur={(e) => { e.currentTarget.style.borderColor = "var(--line)"; }}
        />
      </div>

      <div>
        <div style={{
          fontSize: 9, color: "var(--t3)", letterSpacing: "0.12em",
          fontWeight: 700, textTransform: "uppercase", marginBottom: 6,
        }}>
          PASSWORD
        </div>
        <input
          type="password"
          value={password}
          onChange={(e) => { setPassword(e.target.value); setError(null); }}
          placeholder="********"
          autoComplete="current-password"
          disabled={submitting}
          style={{
            width: "100%",
            background: "var(--bg3)",
            border: "1px solid var(--line)",
            borderRadius: 6,
            padding: "10px 12px",
            color: "var(--t0)",
            fontSize: 12,
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            outline: "none",
          }}
          onFocus={(e) => { e.currentTarget.style.borderColor = "var(--green)"; }}
          onBlur={(e) => { e.currentTarget.style.borderColor = "var(--line)"; }}
        />
      </div>

      <button
        type="submit"
        disabled={!email || !password || submitting}
        style={{
          background: email && password && !submitting ? "var(--green)" : "var(--bg3)",
          color: email && password && !submitting ? "#0a0a0a" : "var(--t2)",
          border: "none",
          borderRadius: 6,
          padding: "10px 14px",
          fontSize: 10,
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          cursor: email && password && !submitting ? "pointer" : "not-allowed",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          transition: "all 0.15s ease",
        }}
      >
        {submitting ? "Authenticating..." : "Sign In"}
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

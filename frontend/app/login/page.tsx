"use client";

/**
 * frontend/app/login/page.tsx
 * ===========================
 * Neutral admin login page. Shared across compiler / indexer / manager via the
 * `admin_session` cookie.
 *
 * Accepts ?next=<path> to return the user to their originating route after
 * login. `next` is sanitized to same-origin paths ("/..." but not "//...") —
 * anything else falls back to /compiler.
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const DEFAULT_NEXT = "/compiler";

function sanitizeNext(raw: string | null): string {
  if (!raw) return DEFAULT_NEXT;
  // Must be a same-origin path; reject protocol-relative (//host) and absolute URLs
  if (!raw.startsWith("/") || raw.startsWith("//")) return DEFAULT_NEXT;
  return raw;
}

export default function LoginPage() {
  const router = useRouter();
  const [passphrase, setPassphrase] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [checkingExisting, setCheckingExisting] = useState(true);
  const [nextPath, setNextPath] = useState(DEFAULT_NEXT);

  // Read ?next= from the URL on mount (client-only; avoids Suspense boundary)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    setNextPath(sanitizeNext(params.get("next")));
  }, []);

  // If already authenticated, skip the form
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/admin/whoami`, { credentials: "include" })
      .then((r) => r.json())
      .then((data) => {
        if (cancelled) return;
        if (data?.authenticated) {
          router.replace(nextPath);
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
  }, [router, nextPath]);

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
        router.replace(nextPath);
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
        Admin
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

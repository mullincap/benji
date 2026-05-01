"use client";

/**
 * frontend/app/components/Collapsible.tsx
 * ==========================================
 * Reusable section wrapper. Click the head to toggle; chevron rotates;
 * body hides when collapsed. State persists in localStorage keyed by
 * `id` so a refresh restores the user's last layout.
 *
 * Conventions:
 *   * <Collapsible id="..." title="..." summary={...}> ...children... </Collapsible>
 *   * Title is the all-caps section label (small mono)
 *   * `summary` renders inline on the head and is shown ONLY in the
 *     collapsed state — match the mockup pattern where collapsing
 *     swaps the body for a one-line summary.
 *   * `defaultOpen` is the initial state if no localStorage entry exists.
 */

import { ReactNode, useEffect, useState } from "react";

interface CollapsibleProps {
  id: string;
  /** Section label (string for plain titles; ReactNode lets callers
   *  splice in a stale tag or other inline status indicator). */
  title: ReactNode;
  summary?: ReactNode;
  defaultOpen?: boolean;
  children: ReactNode;
}

const STORAGE_PREFIX = "benji3m:collapsible:";

export default function Collapsible({
  id,
  title,
  summary,
  defaultOpen = true,
  children,
}: CollapsibleProps) {
  // Hydration-safe initial state: render with defaultOpen on first paint,
  // then sync from localStorage in an effect. Avoids SSR/CSR mismatch.
  const [open, setOpen] = useState(defaultOpen);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(STORAGE_PREFIX + id);
      if (stored !== null) setOpen(stored === "1");
    } catch {
      // localStorage may be disabled — fall back to defaultOpen
    }
    setHydrated(true);
  }, [id]);

  useEffect(() => {
    if (!hydrated) return;
    try {
      window.localStorage.setItem(STORAGE_PREFIX + id, open ? "1" : "0");
    } catch {
      // ignore
    }
  }, [open, id, hydrated]);

  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 4,
        marginBottom: 12,
      }}
    >
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          width: "100%",
          padding: "10px 14px",
          background: "transparent",
          border: "none",
          color: "var(--t1)",
          cursor: "pointer",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          textAlign: "left",
        }}
        aria-expanded={open}
      >
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.16em",
            color: "var(--t3)",
            textTransform: "uppercase",
          }}
        >
          {title}
        </span>

        {/* Summary line — only visible when collapsed; mockup shows
            "EQUITY $X · NET +$Y · DEPLOYED $Z" beside the chevron. */}
        {!open && summary && (
          <span
            style={{
              fontSize: 10,
              color: "var(--t3)",
              letterSpacing: "0.06em",
              marginLeft: "auto",
              marginRight: 12,
              fontVariantNumeric: "tabular-nums",
            }}
          >
            {summary}
          </span>
        )}

        {/* Chevron — rotates 90° on collapsed */}
        <span
          aria-hidden
          style={{
            display: "inline-block",
            width: 0,
            height: 0,
            borderLeft: "5px solid transparent",
            borderRight: "5px solid transparent",
            borderTop: "6px solid var(--t3)",
            transition: "transform 200ms ease",
            transform: open ? "rotate(0deg)" : "rotate(-90deg)",
            marginLeft: open ? "auto" : 0,
          }}
        />
      </button>

      {open && (
        <div style={{ padding: "0 14px 14px" }}>
          {children}
        </div>
      )}
    </div>
  );
}

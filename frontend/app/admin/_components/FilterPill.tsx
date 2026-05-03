"use client";

/**
 * FilterPill — small toggle pill matching the mockup's `.filter-pill`
 * block. Used in the Users / Invitations / Audit toolbars to filter
 * by status, action type, etc.
 *
 * Active state uses the admin amber accent (matches the locked module
 * color); inactive uses dim borders + muted text.
 */

import type { ReactNode } from "react";

type Props = {
  active?: boolean;
  onClick?: () => void;
  children: ReactNode;
};

export default function FilterPill({ active, onClick, children }: Props) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "6px 10px",
        background: active ? "rgba(240, 165, 0, 0.12)" : "var(--bg3)",
        border: `1px solid ${active ? "var(--amber)" : "var(--line)"}`,
        color: active ? "var(--amber)" : "var(--t2)",
        fontSize: 10,
        letterSpacing: "0.08em",
        textTransform: "uppercase",
        borderRadius: 2,
        cursor: "pointer",
        fontFamily: "inherit",
        transition: "all 0.12s ease",
      }}
      onMouseEnter={(e) => {
        if (!active) {
          e.currentTarget.style.borderColor = "var(--line2)";
          e.currentTarget.style.color = "var(--t0)";
        }
      }}
      onMouseLeave={(e) => {
        if (!active) {
          e.currentTarget.style.borderColor = "var(--line)";
          e.currentTarget.style.color = "var(--t2)";
        }
      }}
    >
      {children}
    </button>
  );
}

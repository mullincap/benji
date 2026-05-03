/**
 * StatusPill — small colored pill matching the mockup `.pill` block.
 *
 * Accepts one of the canonical status tones. The mockup's status enum
 * maps directly to the user_status field returned by GET /api/admin/users.
 */

import type { ReactNode } from "react";

export type PillTone = "green" | "amber" | "red" | "dim" | "admin";

const TONES: Record<PillTone, { color: string; border: string; bg: string; dot: string }> = {
  green: {
    color: "var(--green)",
    border: "rgba(0, 200, 150, 0.4)",
    bg: "rgba(0, 200, 150, 0.12)",
    dot: "var(--green)",
  },
  amber: {
    color: "var(--amber)",
    border: "rgba(240, 165, 0, 0.4)",
    bg: "rgba(240, 165, 0, 0.12)",
    dot: "var(--amber)",
  },
  red: {
    color: "var(--red)",
    border: "rgba(239, 68, 68, 0.4)",
    bg: "rgba(239, 68, 68, 0.12)",
    dot: "var(--red)",
  },
  dim: {
    color: "var(--t2)",
    border: "var(--line2)",
    bg: "var(--bg3)",
    dot: "var(--t3)",
  },
  admin: {
    color: "var(--amber)",
    border: "var(--amber)",
    bg: "rgba(240, 165, 0, 0.12)",
    dot: "var(--amber)",
  },
};

type Props = {
  tone: PillTone;
  children: ReactNode;
  /** Hide the leading dot for a cleaner look on tags vs status. */
  noDot?: boolean;
};

export default function StatusPill({ tone, children, noDot }: Props) {
  const t = TONES[tone];
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        padding: "3px 8px",
        borderRadius: 2,
        fontSize: 9,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        fontWeight: 700,
        border: `1px solid ${t.border}`,
        color: t.color,
        background: t.bg,
      }}
    >
      {!noDot && (
        <span
          style={{ width: 5, height: 5, borderRadius: "50%", background: t.dot }}
        />
      )}
      {children}
    </span>
  );
}

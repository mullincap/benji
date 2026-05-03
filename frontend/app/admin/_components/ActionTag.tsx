/**
 * ActionTag — color-coded label for the audit log's action_type column.
 *
 * Tone derives from the action_type string. Falls back to neutral.
 */

import type { ReactNode } from "react";

type ActionTone = "reset" | "invite" | "lock" | "create" | "neutral";

const TONES: Record<ActionTone, { color: string; border: string; bg: string }> = {
  reset:   { color: "var(--amber)",   border: "rgba(240, 165, 0, 0.4)",   bg: "rgba(240, 165, 0, 0.12)" },
  invite:  { color: "var(--green)",   border: "rgba(0, 200, 150, 0.4)",   bg: "rgba(0, 200, 150, 0.12)" },
  lock:    { color: "var(--red)",     border: "rgba(239, 68, 68, 0.4)",   bg: "rgba(239, 68, 68, 0.12)" },
  create:  { color: "#a78bff",        border: "rgba(167, 139, 255, 0.4)", bg: "rgba(167, 139, 255, 0.12)" },
  neutral: { color: "var(--t0)",      border: "var(--line2)",             bg: "var(--bg3)" },
};

/** Map a server action_type string to a tone. */
export function toneForAction(actionType: string): ActionTone {
  if (actionType.includes("password_reset")) return "reset";
  if (actionType.includes("invitation"))     return "invite";
  if (actionType.includes("lock") || actionType.includes("denied")) return "lock";
  if (actionType.includes("user_created"))   return "create";
  return "neutral";
}

type Props = {
  /** Label to display — usually the action_type pretty-printed. */
  children: ReactNode;
  /** Explicit tone, or pass via toneForAction(actionType) at the call site. */
  tone?: ActionTone;
};

export default function ActionTag({ children, tone = "neutral" }: Props) {
  const t = TONES[tone];
  return (
    <span
      style={{
        fontSize: 9,
        letterSpacing: "0.14em",
        textTransform: "uppercase",
        padding: "3px 7px",
        borderRadius: 2,
        fontWeight: 700,
        background: t.bg,
        color: t.color,
        border: `1px solid ${t.border}`,
        flexShrink: 0,
        display: "inline-block",
      }}
    >
      {children}
    </span>
  );
}

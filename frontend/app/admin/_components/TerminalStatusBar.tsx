/**
 * TerminalStatusBar — the slim "> session.scope = ... · last refresh ..."
 * bar that sits above content blocks per the mockup. Echoes the
 * Phase 1a auth-shell terminal status flourish in the admin context.
 */

import type { ReactNode } from "react";

type Props = {
  /** Left-side content. Usually a "> key = value" line; the value can
   *  use <span style={{color:'var(--amber)'}}> for the accent color. */
  left: ReactNode;
  /** Right-side meta — last refresh timestamp, build version, etc. */
  right?: ReactNode;
};

export default function TerminalStatusBar({ left, right }: Props) {
  return (
    <div
      style={{
        background: "var(--bg3)",
        border: "1px solid var(--line)",
        borderRadius: 2,
        padding: "8px 14px",
        color: "var(--t2)",
        fontSize: 11,
        marginBottom: 16,
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        letterSpacing: "0.04em",
      }}
    >
      <span>{left}</span>
      {right && <span style={{ color: "var(--t3)" }}>{right}</span>}
    </div>
  );
}

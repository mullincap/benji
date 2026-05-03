/**
 * AdminCard — page section panel matching the mockup's `.panel` block.
 *
 * Used for the user-detail identity card and the right-side tab content.
 * Not the same as Phase 1a's AuthCard (no terminal status bar; admin
 * uses TerminalStatusBar separately).
 */

import type { ReactNode } from "react";

type Props = {
  title?: ReactNode;
  meta?: ReactNode;
  /** Right-side slot in the panel head (e.g. small action buttons). */
  rightSlot?: ReactNode;
  children?: ReactNode;
  /** Skip body padding when the child is itself a full-bleed table. */
  flush?: boolean;
};

export default function AdminCard({ title, meta, rightSlot, children, flush }: Props) {
  return (
    <div
      style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 2,
      }}
    >
      {(title || meta || rightSlot) && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "12px 16px",
            borderBottom: "1px solid var(--line)",
          }}
        >
          <div>
            {title && (
              <div
                style={{
                  color: "var(--t1)",
                  fontSize: 10,
                  letterSpacing: "0.16em",
                  textTransform: "uppercase",
                  fontWeight: 700,
                }}
              >
                {title}
              </div>
            )}
            {meta && (
              <div style={{ color: "var(--t3)", fontSize: 11, marginTop: 2 }}>
                {meta}
              </div>
            )}
          </div>
          {rightSlot && <div>{rightSlot}</div>}
        </div>
      )}
      <div style={{ padding: flush ? 0 : 16 }}>{children}</div>
    </div>
  );
}

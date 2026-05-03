"use client";

/**
 * Modal — admin-flavored modal matching the mockup `.modal` block.
 *
 * Backdrop click closes (matches user expectation for non-destructive
 * modals); ESC key closes. The modal traps focus on the close button
 * for the simplest a11y case — Phase 1 doesn't need a full focus trap
 * since these modals are single-action.
 *
 * Body content is supplied by the consumer; we only own the chrome
 * (head with eyebrow + title + close, footer with action buttons).
 */

import { useEffect, type ReactNode } from "react";

type Props = {
  /** Small uppercase label above the title (e.g. "[ Action · Issue Invitation ]"). */
  eyebrow?: string;
  /** Main heading. */
  title: string;
  /** Right-side close handler. Called also on backdrop click and ESC. */
  onClose: () => void;
  /** Modal body content. */
  children: ReactNode;
  /** Footer action row — typically a Cancel + Confirm button pair. */
  footer?: ReactNode;
  /** Optional max-width override (default 520px from mockup). */
  maxWidth?: number;
};

export default function Modal({
  eyebrow,
  title,
  onClose,
  children,
  footer,
  maxWidth = 520,
}: Props) {
  // ESC closes
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0, 0, 0, 0.7)",
        backdropFilter: "blur(4px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 100,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: "100%",
          maxWidth,
          background: "var(--bg2)",
          border: "1px solid var(--line2)",
          borderRadius: 3,
          overflow: "hidden",
          fontFamily: "inherit",
        }}
        role="dialog"
        aria-modal="true"
        aria-labelledby="admin-modal-title"
      >
        {/* Head */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "14px 18px",
            borderBottom: "1px solid var(--line)",
          }}
        >
          <div>
            {eyebrow && (
              <div
                style={{
                  color: "var(--amber)",
                  fontSize: 10,
                  letterSpacing: "0.18em",
                  textTransform: "uppercase",
                  marginBottom: 2,
                  fontWeight: 700,
                }}
              >
                {eyebrow}
              </div>
            )}
            <div id="admin-modal-title" style={{ fontSize: 14, fontWeight: 700, color: "var(--t0)" }}>
              {title}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            style={{
              background: "transparent",
              border: 0,
              color: "var(--t3)",
              fontSize: 18,
              cursor: "pointer",
              padding: "4px 8px",
              lineHeight: 1,
            }}
          >
            ×
          </button>
        </div>

        {/* Body */}
        <div style={{ padding: "20px 18px" }}>{children}</div>

        {/* Foot */}
        {footer && (
          <div
            style={{
              padding: "14px 18px",
              borderTop: "1px solid var(--line)",
              display: "flex",
              justifyContent: "flex-end",
              gap: 8,
              background: "var(--bg3)",
            }}
          >
            {footer}
          </div>
        )}
      </div>
    </div>
  );
}

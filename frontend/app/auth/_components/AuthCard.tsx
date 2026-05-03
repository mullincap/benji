/**
 * AuthCard — the dark-surface card with a terminal status bar at the top.
 *
 * The terminal bar is the visual signature of the auth flow — left side
 * shows session state ("> session.state = unauthenticated"), right side
 * shows step indicator or build version. statusLeft/statusRight accept
 * ReactNode so callers can color-code values via inline spans.
 */

import type { ReactNode } from "react";

type Props = {
  statusLeft?: ReactNode;
  statusRight?: ReactNode;
  eyebrow?: string;
  title?: ReactNode;
  subtitle?: ReactNode;
  children?: ReactNode;
  footer?: ReactNode;
  /** Wider card variant for the welcome screen (max-width 720 vs 440). */
  wide?: boolean;
  /** Center-align eyebrow/title/subtitle (used by confirmation/empty states). */
  centered?: boolean;
};

export default function AuthCard({
  statusLeft,
  statusRight,
  eyebrow,
  title,
  subtitle,
  children,
  footer,
  wide,
  centered,
}: Props) {
  return (
    <div
      style={{
        width: "100%",
        maxWidth: wide ? 720 : 440,
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 3,
      }}
    >
      {(statusLeft || statusRight) && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "12px 20px",
            borderBottom: "1px solid var(--line)",
            color: "var(--t3)",
            fontSize: 10,
            letterSpacing: "0.06em",
          }}
        >
          <span style={{ color: "var(--t2)" }}>{statusLeft}</span>
          <span style={{ color: "var(--t3)" }}>{statusRight}</span>
        </div>
      )}

      <div
        style={{
          padding: "32px 28px 28px",
          textAlign: centered ? "center" : "left",
        }}
      >
        {eyebrow && (
          <div
            style={{
              color: "var(--green)",
              fontSize: 10,
              letterSpacing: "0.18em",
              textTransform: "uppercase",
              fontWeight: 700,
              marginBottom: 12,
            }}
          >
            {eyebrow}
          </div>
        )}
        {title && (
          <h1
            style={{
              fontSize: 24,
              fontWeight: 700,
              lineHeight: 1.2,
              color: "var(--t0)",
              marginBottom: 8,
              letterSpacing: "-0.01em",
            }}
          >
            {title}
          </h1>
        )}
        {subtitle && (
          <div
            style={{
              color: "var(--t2)",
              fontSize: 13,
              lineHeight: 1.55,
              marginBottom: 28,
            }}
          >
            {subtitle}
          </div>
        )}
        {children}
      </div>

      {footer && (
        <div
          style={{
            padding: "16px 28px",
            borderTop: "1px solid var(--line)",
            textAlign: "center",
            color: "var(--t2)",
            fontSize: 12,
          }}
        >
          {footer}
        </div>
      )}
    </div>
  );
}

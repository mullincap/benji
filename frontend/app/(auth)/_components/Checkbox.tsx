"use client";

/**
 * Checkbox — controlled checkbox + clickable label row.
 *
 * Renders a custom box (no native checkbox) that we paint with CSS based
 * on the `checked` prop, plus the children as the clickable label. The
 * checked-state checkmark is drawn with a rotated CSS border (no SVG
 * dependency).
 */

import type { ReactNode } from "react";

type Props = {
  checked: boolean;
  onChange: (next: boolean) => void;
  children: ReactNode;
  disabled?: boolean;
  /** Render-anchor for the label — auto-generated id when not supplied. */
  id?: string;
};

export default function Checkbox({ checked, onChange, children, disabled, id }: Props) {
  const inputId = id || `checkbox-${stableIdFrom(children)}`;
  return (
    <label
      htmlFor={inputId}
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 10,
        margin: "8px 0 20px",
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
      }}
    >
      <input
        id={inputId}
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
        style={{
          /* Hide the native checkbox without removing it from a11y tree. */
          position: "absolute",
          opacity: 0,
          width: 0,
          height: 0,
          margin: 0,
        }}
      />
      <span
        aria-hidden
        style={{
          width: 16,
          height: 16,
          border: `1px solid ${checked ? "var(--green)" : "var(--line2)"}`,
          background: checked ? "var(--green)" : "var(--bg3)",
          borderRadius: 2,
          flexShrink: 0,
          marginTop: 1,
          position: "relative",
          transition: "all 0.12s ease",
        }}
      >
        {checked && (
          <span
            style={{
              position: "absolute",
              left: 4,
              top: 1,
              width: 4,
              height: 8,
              border: "solid #001a14",
              borderWidth: "0 2px 2px 0",
              transform: "rotate(45deg)",
              display: "block",
            }}
          />
        )}
      </span>
      <span style={{ fontSize: 12, color: "var(--t2)", lineHeight: 1.5 }}>
        {children}
      </span>
    </label>
  );
}

/** Hash-based stable id for label/input pairing; only used when no id is passed.
 *  Not security-sensitive — collisions just mean two checkboxes share an id,
 *  which is harmless given each instance is in its own form. */
function stableIdFrom(children: ReactNode): string {
  const text = typeof children === "string" ? children : "cb";
  let hash = 0;
  for (let i = 0; i < text.length; i++) hash = (hash * 31 + text.charCodeAt(i)) | 0;
  return Math.abs(hash).toString(36);
}

/**
 * Field — labeled wrapper for an input/select/checkbox.
 *
 * Renders the standard label row (10px uppercase muted) and an optional
 * helper line below the input. Supports a `rightSlot` in the label row
 * for inline links like "Forgot password?". `helperKind` switches the
 * helper color: default (dim), error (red), success (green).
 */

import type { ReactNode } from "react";

type HelperKind = "default" | "error" | "success";

type Props = {
  label: string;
  htmlFor?: string;
  helper?: ReactNode;
  helperKind?: HelperKind;
  rightSlot?: ReactNode;
  children: ReactNode;
};

const HELPER_COLOR: Record<HelperKind, string> = {
  default: "var(--t3)",
  error: "var(--red)",
  success: "var(--green)",
};

export default function Field({
  label,
  htmlFor,
  helper,
  helperKind = "default",
  rightSlot,
  children,
}: Props) {
  return (
    <div style={{ marginBottom: 16 }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 8,
        }}
      >
        <label
          htmlFor={htmlFor}
          style={{
            color: "var(--t2)",
            fontSize: 10,
            letterSpacing: "0.12em",
            textTransform: "uppercase",
          }}
        >
          {label}
        </label>
        {rightSlot}
      </div>
      {children}
      {helper && (
        <div
          style={{
            marginTop: 6,
            fontSize: 11,
            color: HELPER_COLOR[helperKind],
            letterSpacing: "0.02em",
          }}
        >
          {helper}
        </div>
      )}
    </div>
  );
}

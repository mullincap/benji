/**
 * Field — labeled wrapper for an input/select/checkbox.
 *
 * Renders the standard label row (10px uppercase muted) and an optional
 * helper line below the input. Supports a `rightSlot` in the label row
 * for inline links like "Forgot password?". `helperKind` switches the
 * helper color: default (dim), error (red), success (green).
 *
 * a11y: when helper text is present and htmlFor is set, Field injects
 * aria-describedby on the child input pointing to the helper element's
 * id. This requires the child to be a single element that accepts
 * aria-* props (Input/Select/Checkbox here) — the cloneElement is a
 * no-op for fragments and string children, falling back gracefully.
 */

import { Children, cloneElement, isValidElement, type ReactElement, type ReactNode } from "react";

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
  const helperId = helper && htmlFor ? `${htmlFor}-helper` : undefined;

  // Inject aria-describedby on the first valid element child when helper
  // is present. Subsequent siblings (e.g. PasswordStrengthMeter) pass
  // through unchanged. Computed up-front so the render function stays
  // pure (no mutation inside Children.map).
  const childArray = Children.toArray(children);
  const firstElementIndex = helperId
    ? childArray.findIndex((c) => isValidElement(c))
    : -1;
  const decorated = childArray.map((child, i) => {
    if (i !== firstElementIndex || !helperId || !isValidElement(child)) {
      return child;
    }
    const existing = (child.props as { "aria-describedby"?: string })["aria-describedby"];
    const next = existing ? `${existing} ${helperId}` : helperId;
    return cloneElement(child as ReactElement<{ "aria-describedby"?: string }>, {
      "aria-describedby": next,
    });
  });

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
      {decorated}
      {helper && (
        <div
          id={helperId}
          role={helperKind === "error" ? "alert" : undefined}
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

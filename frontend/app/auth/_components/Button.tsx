"use client";

/**
 * Button — primary (green fill) and secondary (outline) variants.
 *
 * `loading` keeps the button mounted (no DOM swap) and shows an inline
 * spinner — required by the spec so submit buttons don't reset focus.
 * `trailingArrow` appends → after the label.
 */

import { forwardRef, type ButtonHTMLAttributes } from "react";

type Variant = "primary" | "secondary";

type Props = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: Variant;
  trailingArrow?: boolean;
  loading?: boolean;
};

const Button = forwardRef<HTMLButtonElement, Props>(function Button(
  { variant = "primary", trailingArrow, loading, disabled, children, style, onMouseEnter, onMouseLeave, ...rest },
  ref,
) {
  const isPrimary = variant === "primary";
  const isDisabled = disabled || loading;

  const base = isPrimary
    ? {
        background: "var(--green)",
        color: "#001a14",
        borderColor: "var(--green)",
      }
    : {
        background: "transparent",
        color: "var(--t0)",
        borderColor: "var(--border-bright)",
      };

  return (
    <button
      ref={ref}
      disabled={isDisabled}
      onMouseEnter={(e) => {
        if (isDisabled) return;
        if (isPrimary) {
          e.currentTarget.style.background = "var(--green-bright)";
          e.currentTarget.style.borderColor = "var(--green-bright)";
        } else {
          e.currentTarget.style.borderColor = "var(--t2)";
          e.currentTarget.style.background = "var(--bg2)";
        }
        onMouseEnter?.(e);
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = base.background;
        e.currentTarget.style.borderColor = base.borderColor;
        onMouseLeave?.(e);
      }}
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        gap: 8,
        width: "100%",
        padding: "13px 16px",
        fontFamily: "inherit",
        fontSize: 12,
        fontWeight: 700,
        letterSpacing: "0.14em",
        textTransform: "uppercase",
        border: "1px solid",
        borderRadius: 2,
        cursor: isDisabled ? "not-allowed" : "pointer",
        opacity: isDisabled ? 0.6 : 1,
        transition: "all 0.12s ease",
        textDecoration: "none",
        ...base,
        ...style,
      }}
      {...rest}
    >
      {loading ? (
        <Spinner color={isPrimary ? "#001a14" : "var(--t0)"} />
      ) : (
        <>
          <span>{children}</span>
          {trailingArrow && <span aria-hidden>→</span>}
        </>
      )}
    </button>
  );
});

function Spinner({ color }: { color: string }) {
  return (
    <span
      aria-hidden
      style={{
        width: 14,
        height: 14,
        border: `2px solid ${color}`,
        borderTopColor: "transparent",
        borderRadius: "50%",
        display: "inline-block",
        animation: "spin 0.8s linear infinite",
      }}
    />
  );
}

export default Button;

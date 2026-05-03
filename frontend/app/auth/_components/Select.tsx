"use client";

/**
 * Select — drop-down primitive. Matches Input visually (same border, focus
 * halo, padding) plus a custom chevron drawn via background-image so we
 * don't need an icon dependency.
 */

import { forwardRef, type SelectHTMLAttributes } from "react";

type Props = SelectHTMLAttributes<HTMLSelectElement> & {
  invalid?: boolean;
};

const CHEVRON_BG =
  "linear-gradient(45deg, transparent 50%, var(--t2) 50%)," +
  "linear-gradient(135deg, var(--t2) 50%, transparent 50%)";

const Select = forwardRef<HTMLSelectElement, Props>(function Select(
  { invalid, style, onFocus, onBlur, children, ...rest },
  ref,
) {
  const baseBorder = invalid ? "var(--red)" : "var(--line)";

  return (
    <select
      ref={ref}
      onFocus={(e) => {
        if (!invalid) {
          e.currentTarget.style.borderColor = "var(--green)";
          e.currentTarget.style.boxShadow = "0 0 0 3px var(--green-soft)";
        }
        onFocus?.(e);
      }}
      onBlur={(e) => {
        e.currentTarget.style.borderColor = baseBorder;
        e.currentTarget.style.boxShadow = "none";
        onBlur?.(e);
      }}
      style={{
        width: "100%",
        background: "var(--bg3)",
        backgroundImage: CHEVRON_BG,
        backgroundPosition: "calc(100% - 16px) 50%, calc(100% - 11px) 50%",
        backgroundSize: "5px 5px, 5px 5px",
        backgroundRepeat: "no-repeat",
        border: `1px solid ${baseBorder}`,
        color: "var(--t0)",
        fontFamily: "inherit",
        fontSize: 14,
        padding: "12px 36px 12px 14px",
        borderRadius: 2,
        appearance: "none",
        WebkitAppearance: "none",
        outline: "none",
        cursor: "pointer",
        transition: "border-color 0.12s ease, box-shadow 0.12s ease",
        ...style,
      }}
      {...rest}
    >
      {children}
    </select>
  );
});

export default Select;

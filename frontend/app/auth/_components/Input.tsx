"use client";

/**
 * Input — text/email/password primitive matching the auth-flow mockup.
 *
 * Focus state shows green border + soft halo via inline onFocus/onBlur
 * handlers (matching the existing /login pattern — no styled-jsx, no CSS
 * modules). `invalid` swaps the border to red. `locked` renders a
 * read-only display variant for the email field on the accept-invite
 * screen.
 */

import { forwardRef, type InputHTMLAttributes } from "react";

type Props = InputHTMLAttributes<HTMLInputElement> & {
  invalid?: boolean;
  locked?: boolean;
};

const Input = forwardRef<HTMLInputElement, Props>(function Input(
  { invalid, locked, style, onFocus, onBlur, disabled, ...rest },
  ref,
) {
  const baseBorder = invalid
    ? "var(--red)"
    : "var(--line)";

  return (
    <input
      ref={ref}
      disabled={locked || disabled}
      onFocus={(e) => {
        if (!invalid && !locked) {
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
        background: locked ? "transparent" : "var(--bg3)",
        border: `1px solid ${baseBorder}`,
        color: locked ? "var(--t2)" : "var(--t0)",
        fontFamily: "inherit",
        fontSize: 14,
        padding: "12px 14px",
        borderRadius: 2,
        transition: "border-color 0.12s ease, box-shadow 0.12s ease",
        outline: "none",
        cursor: locked ? "not-allowed" : "text",
        ...style,
      }}
      {...rest}
    />
  );
});

export default Input;

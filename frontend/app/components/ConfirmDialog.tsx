"use client";

/**
 * ConfirmDialog + ConfirmProvider + useConfirm()
 * ==============================================
 * In-app replacement for window.confirm() / window.alert().
 *
 * Usage:
 *   const confirm = useConfirm();
 *   const ok = await confirm({
 *     title: "Sign out?",
 *     description: "You'll need to sign back in to access your account.",
 *     confirmLabel: "Sign out",
 *     destructive: false,
 *   });
 *   if (ok) signout();
 *
 * Provider lives at the root layout (above AuthProvider) so dialogs are
 * available to every surface — pre-auth, in-app, admin, marketing.
 *
 * Race policy: a second confirm() call while one is already open replaces
 * the first (resolves it to false). Keeps the queue at exactly one — the
 * UI only ever shows one modal so a stack would be invisible anyway.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";

const ALLOCATOR_PURPLE = "#a78bff";
const ALLOCATOR_PURPLE_HOVER = "#c0a8ff";

export type ConfirmOptions = {
  title: string;
  description?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  /** Red primary button when true, allocator-purple when false. */
  destructive?: boolean;
  /** Small uppercase eyebrow above the title. Hidden when omitted. */
  eyebrow?: string;
};

type Resolver = (ok: boolean) => void;

type State =
  | { open: false }
  | { open: true; opts: ConfirmOptions; resolve: Resolver };

const ConfirmContext = createContext<((opts: ConfirmOptions) => Promise<boolean>) | null>(null);

export function ConfirmProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<State>({ open: false });

  // Hold the latest resolver in a ref so close handlers don't capture a
  // stale closure when state changes between handler creation and fire.
  const resolverRef = useRef<Resolver | null>(null);

  const confirm = useCallback((opts: ConfirmOptions): Promise<boolean> => {
    // If another confirm is already pending, resolve the previous one to
    // false (cancelled by the new request) so callers don't dangle.
    if (resolverRef.current) {
      resolverRef.current(false);
      resolverRef.current = null;
    }
    return new Promise<boolean>((resolve) => {
      resolverRef.current = resolve;
      setState({ open: true, opts, resolve });
    });
  }, []);

  const close = useCallback((value: boolean) => {
    const r = resolverRef.current;
    resolverRef.current = null;
    setState({ open: false });
    if (r) r(value);
  }, []);

  return (
    <ConfirmContext.Provider value={confirm}>
      {children}
      {state.open && (
        <ConfirmDialog
          opts={state.opts}
          onConfirm={() => close(true)}
          onCancel={() => close(false)}
        />
      )}
    </ConfirmContext.Provider>
  );
}

export function useConfirm() {
  const ctx = useContext(ConfirmContext);
  if (!ctx) {
    throw new Error("useConfirm must be used within a <ConfirmProvider>");
  }
  return ctx;
}

// ─── Dialog UI ──────────────────────────────────────────────────────────────

function ConfirmDialog({
  opts,
  onConfirm,
  onCancel,
}: {
  opts: ConfirmOptions;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const confirmRef = useRef<HTMLButtonElement | null>(null);
  const cancelRef = useRef<HTMLButtonElement | null>(null);
  const dialogRef = useRef<HTMLDivElement | null>(null);

  const confirmLabel = opts.confirmLabel ?? "Confirm";
  const cancelLabel = opts.cancelLabel ?? "Cancel";
  const accent = opts.destructive ? "var(--red)" : ALLOCATOR_PURPLE;
  const accentHover = opts.destructive ? "#ff6b6b" : ALLOCATOR_PURPLE_HOVER;
  const accentText = opts.destructive ? "#1a0202" : "#0d0518";

  // Focus the primary action on mount; remember the previously-focused
  // element so we can restore it on close. Standard accessibility pattern.
  useEffect(() => {
    const previouslyFocused = (typeof document !== "undefined" ? document.activeElement : null) as HTMLElement | null;
    const t = window.setTimeout(() => confirmRef.current?.focus(), 0);

    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") {
        e.preventDefault();
        onCancel();
        return;
      }
      if (e.key === "Enter") {
        // Enter confirms unless focus is on the cancel button (then cancel
        // wins — matches standard OS dialog behavior).
        if (document.activeElement === cancelRef.current) return;
        e.preventDefault();
        onConfirm();
        return;
      }
      if (e.key === "Tab") {
        // Two-button focus trap: bounce between confirm and cancel.
        const focusable = [cancelRef.current, confirmRef.current].filter(Boolean) as HTMLElement[];
        if (focusable.length < 2) return;
        const active = document.activeElement;
        if (e.shiftKey) {
          if (active === focusable[0]) {
            e.preventDefault();
            focusable[focusable.length - 1].focus();
          }
        } else {
          if (active === focusable[focusable.length - 1]) {
            e.preventDefault();
            focusable[0].focus();
          }
        }
      }
    }

    document.addEventListener("keydown", onKey);
    return () => {
      window.clearTimeout(t);
      document.removeEventListener("keydown", onKey);
      previouslyFocused?.focus?.();
    };
  }, [onCancel, onConfirm]);

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="confirm-dialog-title"
      onMouseDown={(e) => {
        // Backdrop click dismisses. Filter to mousedown on the backdrop
        // itself — clicks that started inside the panel and dragged out
        // shouldn't dismiss (stops accidental close on text-selection).
        if (e.target === e.currentTarget) onCancel();
      }}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(8, 8, 9, 0.7)",
        backdropFilter: "blur(2px)",
        WebkitBackdropFilter: "blur(2px)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
        padding: 20,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      <div
        ref={dialogRef}
        style={{
          width: "100%",
          maxWidth: 400,
          background: "var(--bg1)",
          border: "1px solid var(--line2)",
          borderRadius: 2,
          boxShadow: "0 12px 40px rgba(0, 0, 0, 0.7)",
          padding: "20px 22px 18px",
        }}
      >
        {opts.eyebrow && (
          <div
            style={{
              color: opts.destructive ? "var(--red)" : ALLOCATOR_PURPLE,
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.18em",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            [ {opts.eyebrow} ]
          </div>
        )}

        <h2
          id="confirm-dialog-title"
          style={{
            color: "var(--t0)",
            fontSize: 16,
            fontWeight: 700,
            letterSpacing: "-0.01em",
            margin: 0,
            marginBottom: opts.description ? 8 : 18,
          }}
        >
          {opts.title}
        </h2>

        {opts.description && (
          <p
            style={{
              color: "var(--t1)",
              fontSize: 12,
              lineHeight: 1.55,
              margin: 0,
              marginBottom: 22,
            }}
          >
            {opts.description}
          </p>
        )}

        <div
          style={{
            display: "flex",
            justifyContent: "flex-end",
            gap: 10,
          }}
        >
          <button
            ref={cancelRef}
            type="button"
            onClick={onCancel}
            onMouseEnter={(e) => {
              e.currentTarget.style.color = "var(--t0)";
              e.currentTarget.style.borderColor = "var(--t2)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.color = "var(--t1)";
              e.currentTarget.style.borderColor = "var(--line2)";
            }}
            style={{
              display: "inline-flex",
              alignItems: "center",
              padding: "9px 16px",
              fontFamily: "inherit",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              border: "1px solid var(--line2)",
              borderRadius: 2,
              background: "transparent",
              color: "var(--t1)",
              cursor: "pointer",
              whiteSpace: "nowrap",
              transition: "all 0.12s ease",
            }}
          >
            {cancelLabel}
          </button>
          <button
            ref={confirmRef}
            type="button"
            onClick={onConfirm}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = accentHover;
              e.currentTarget.style.borderColor = accentHover;
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = accent;
              e.currentTarget.style.borderColor = accent;
            }}
            onFocus={(e) => {
              e.currentTarget.style.boxShadow = `0 0 0 3px ${opts.destructive ? "rgba(255, 77, 77, 0.25)" : "rgba(167, 139, 255, 0.3)"}`;
            }}
            onBlur={(e) => {
              e.currentTarget.style.boxShadow = "none";
            }}
            style={{
              display: "inline-flex",
              alignItems: "center",
              padding: "9px 16px",
              fontFamily: "inherit",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              border: `1px solid ${accent}`,
              borderRadius: 2,
              background: accent,
              color: accentText,
              cursor: "pointer",
              whiteSpace: "nowrap",
              outline: "none",
              transition: "all 0.12s ease",
            }}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

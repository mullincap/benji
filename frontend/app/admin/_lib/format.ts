/**
 * frontend/app/admin/_lib/format.ts
 * ==================================
 * Display formatters used across admin screens.
 *
 * Date convention (from spec): RELATIVE for "last login" style fields,
 * ABSOLUTE for "joined" style fields. The relative formatter does not
 * promise sub-minute precision — for "just now" callers should pass
 * an absolute timestamp and let this helper round.
 */

const USD = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 0,
});

const USD_PRECISE = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

export function formatUsd(amount: number | null, precise = false): string {
  if (amount == null) return "—";
  return (precise ? USD_PRECISE : USD).format(amount);
}

export function formatPct(value: number | null): string {
  if (value == null) return "—";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

const DATE_ABS = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  year: "numeric",
});

export function formatAbsoluteDate(iso: string | null): string {
  if (!iso) return "—";
  return DATE_ABS.format(new Date(iso));
}

/** "2m ago", "5h ago", "3d ago", "Apr 28" — falls back to absolute > 30d. */
export function formatRelative(iso: string | null): string {
  if (!iso) return "never";
  const now = Date.now();
  const t = new Date(iso).getTime();
  const diffMs = now - t;
  if (diffMs < 0) {
    // Future timestamp — render as absolute.
    return DATE_ABS.format(new Date(iso));
  }
  const sec = Math.floor(diffMs / 1000);
  if (sec < 60) return "just now";
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  if (day < 30) return `${day}d ago`;
  return DATE_ABS.format(new Date(iso));
}

/** Convert UserStatus to a display label + StatusPill tone. */
export function statusDisplay(status: string): { label: string; tone: "green" | "amber" | "red" | "dim" | "admin" } {
  switch (status) {
    case "active":      return { label: "Active",      tone: "green" };
    case "locked":      return { label: "Locked",      tone: "red" };
    case "pending":     return { label: "Pending",     tone: "amber" };
    case "idle":        return { label: "Idle",        tone: "amber" };
    case "no_activity": return { label: "No Activity", tone: "dim" };
    case "admin":       return { label: "Admin",       tone: "admin" };
    default:            return { label: status,        tone: "dim" };
  }
}

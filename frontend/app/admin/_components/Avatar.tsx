/**
 * Avatar — initials-only square avatar matching the mockup. Used in
 * user list rows and the user detail identity card.
 *
 * No image rendering yet — Phase 1 doesn't ingest profile photos.
 */

type Props = {
  /** Initials to render (typically derived from first/last name). */
  initials: string;
  /** "sm" (28px) for table rows, "lg" (64px) for detail page. */
  size?: "sm" | "lg";
};

const SIZES = {
  sm: { box: 28, font: 11 },
  lg: { box: 64, font: 24 },
};

export default function Avatar({ initials, size = "sm" }: Props) {
  const s = SIZES[size];
  return (
    <span
      style={{
        width: s.box,
        height: s.box,
        borderRadius: 2,
        background: "var(--bg2)",
        border: "1px solid var(--line2)",
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        color: size === "lg" ? "var(--amber)" : "var(--t2)",
        fontSize: s.font,
        fontWeight: 700,
        flexShrink: 0,
      }}
    >
      {initials.slice(0, 2).toUpperCase()}
    </span>
  );
}

/** Helper: derive initials from first/last name with email fallback. */
export function deriveInitials(args: {
  first_name?: string | null;
  last_name?: string | null;
  email: string;
}): string {
  const f = (args.first_name || "").trim();
  const l = (args.last_name || "").trim();
  if (f && l) return (f[0] + l[0]).toUpperCase();
  if (f) return f.slice(0, 2).toUpperCase();
  if (args.email) return args.email.slice(0, 2).toUpperCase();
  return "??";
}

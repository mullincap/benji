"use client";

/**
 * RangeTabs — shared 1D|1W|1M|ALL time-range selector.
 *
 * Lifted from the inline pattern originally in
 * app/trader/performance-chart.tsx so the Manager Overview charts + any
 * future chart panels can reuse the same UX + styling.
 */

export type TimeRange = "1D" | "1W" | "1M" | "ALL";
export const ALL_RANGES: TimeRange[] = ["1D", "1W", "1M", "ALL"];

export function RangeTabs({
  value,
  onChange,
  ranges = ALL_RANGES,
  disabled,
}: {
  value: TimeRange;
  onChange: (next: TimeRange) => void;
  /** Optional subset of ranges to render. Defaults to all four. */
  ranges?: TimeRange[];
  /** Ranges that render greyed-out and are unclickable (e.g. "1D" on a daily-only chart). */
  disabled?: TimeRange[];
}) {
  return (
    <div
      style={{
        display: "flex",
        border: "1px solid var(--line)",
        borderRadius: 4,
        overflow: "hidden",
      }}
    >
      {ranges.map((r, i) => {
        const isActive = r === value;
        const isDisabled = disabled?.includes(r) ?? false;
        return (
          <button
            key={r}
            type="button"
            onClick={() => { if (!isDisabled) onChange(r); }}
            disabled={isDisabled}
            style={{
              padding: "3px 8px",
              fontSize: 9,
              fontWeight: 700,
              background: isActive ? "var(--bg4)" : "transparent",
              color: isDisabled
                ? "var(--t3)"
                : isActive
                  ? "var(--t0)"
                  : "var(--t2)",
              border: "none",
              borderLeft: i === 0 ? "none" : "1px solid var(--line)",
              cursor: isDisabled ? "default" : "pointer",
              opacity: isDisabled ? 0.4 : 1,
              transition: "background 0.15s, color 0.15s",
            }}
            onMouseEnter={(e) => {
              if (!isActive && !isDisabled) e.currentTarget.style.color = "var(--t1)";
            }}
            onMouseLeave={(e) => {
              if (!isActive && !isDisabled) e.currentTarget.style.color = "var(--t2)";
            }}
          >
            {r}
          </button>
        );
      })}
    </div>
  );
}

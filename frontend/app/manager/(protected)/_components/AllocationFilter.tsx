"use client";

/**
 * frontend/app/manager/(protected)/_components/AllocationFilter.tsx
 * ==================================================================
 * Shared allocation-filter control for Manager tabs. Lifted from
 * execution/page.tsx + portfolios/page.tsx — previously duplicated with
 * identical logic per 2026-04-20 sprint.
 *
 * Props:
 *   selected              "all" or an allocation_id
 *   onChange              called with the new value
 *   options               backend-provided {allocation_id, exchange, strategy_label, capital_usd}[]
 *   includeMaster         optional — when provided, renders a toggle alongside
 *   onIncludeMasterChange optional — required if includeMaster is provided
 *
 * The "Include master history" toggle is OPT-IN: only rendered when the
 * includeMaster prop is non-undefined. Execution tab uses it; Portfolios
 * tab does not (master portfolio history is NDJSON-only — see
 * docs/open_work_list.md Session F+ queue).
 *
 * Single-select for v1. Multi-select is a separate follow-up if users ask.
 */

const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";

export interface AvailableAlloc {
  allocation_id: string;
  exchange: string;
  strategy_label: string;
  capital_usd: number | null;
}

export interface AllocationFilterProps {
  selected: "all" | string;
  onChange: (next: "all" | string) => void;
  options: AvailableAlloc[];
  includeMaster?: boolean;
  onIncludeMasterChange?: (next: boolean) => void;
}

export function AllocationFilter({
  selected,
  onChange,
  options,
  includeMaster,
  onIncludeMasterChange,
}: AllocationFilterProps) {
  const showToggle =
    includeMaster !== undefined && onIncludeMasterChange !== undefined;

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <select
        value={selected}
        onChange={(e) => onChange(e.target.value as "all" | string)}
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 4,
          color: "var(--t1)",
          fontFamily: FONT_MONO,
          fontSize: 10,
          padding: "5px 10px",
          cursor: "pointer",
        }}
      >
        <option value="all">All allocations</option>
        {options.map((a) => (
          <option key={a.allocation_id} value={a.allocation_id}>
            {a.exchange} · {a.strategy_label}
          </option>
        ))}
      </select>

      {showToggle && (
        <button
          type="button"
          onClick={() => onIncludeMasterChange!(!includeMaster)}
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 8,
            background: "transparent",
            border: "1px solid var(--line)",
            borderRadius: 4,
            padding: "4px 10px",
            fontFamily: FONT_MONO,
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            textTransform: "uppercase",
            color: includeMaster ? "var(--t0)" : "var(--t2)",
            cursor: "pointer",
          }}
        >
          <span
            style={{
              width: 22,
              height: 12,
              borderRadius: 6,
              background: includeMaster ? "var(--green)" : "var(--bg4)",
              position: "relative",
              display: "inline-block",
              transition: "background 0.15s ease",
            }}
          >
            <span
              style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: includeMaster ? "var(--bg0)" : "var(--t2)",
                position: "absolute",
                top: 2,
                left: includeMaster ? 12 : 2,
                transition: "left 0.15s ease",
              }}
            />
          </span>
          Include master history
        </button>
      )}
    </div>
  );
}

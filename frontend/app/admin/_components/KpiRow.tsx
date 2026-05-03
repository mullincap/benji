/**
 * KpiRow — page-head 5-tile KPI strip from the mockup.
 *
 * Each KPI accepts a label, value, optional delta (small caption
 * underneath), and optional `tone` to color the value (default neutral,
 * 'amber' for module-accent values, 'green' for positives, 'red' for
 * negatives).
 */

import type { ReactNode } from "react";

export type Kpi = {
  label: string;
  value: ReactNode;
  delta?: ReactNode;
  tone?: "default" | "amber" | "green" | "red";
};

const VALUE_COLOR: Record<NonNullable<Kpi["tone"]>, string> = {
  default: "var(--t0)",
  amber:   "var(--amber)",
  green:   "var(--green)",
  red:     "var(--red)",
};

type Props = {
  kpis: Kpi[];
  /** Override the column count (default 5 to match mockup). */
  columns?: number;
};

export default function KpiRow({ kpis, columns = 5 }: Props) {
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: `repeat(${columns}, 1fr)`,
        gap: 12,
        marginBottom: 24,
      }}
    >
      {kpis.map((kpi, i) => (
        <div
          key={kpi.label || `k${i}`}
          style={{
            padding: "14px 16px",
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 2,
          }}
        >
          <div
            style={{
              color: "var(--t3)",
              fontSize: 10,
              letterSpacing: "0.14em",
              textTransform: "uppercase",
              marginBottom: 6,
            }}
          >
            {kpi.label}
          </div>
          <div
            style={{
              color: VALUE_COLOR[kpi.tone || "default"],
              fontSize: 22,
              fontWeight: 700,
              lineHeight: 1.1,
            }}
          >
            {kpi.value}
          </div>
          {kpi.delta != null && (
            <div
              style={{
                color: "var(--t2)",
                fontSize: 10,
                marginTop: 4,
                letterSpacing: "0.04em",
              }}
            >
              {kpi.delta}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

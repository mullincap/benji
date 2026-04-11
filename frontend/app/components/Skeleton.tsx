/**
 * frontend/app/components/Skeleton.tsx
 * =====================================
 * Reusable shimmer placeholder block + a few preset compositions for
 * the recurring layouts (KPI grid, table, generic card).
 *
 * Backed by the .skeleton CSS class in globals.css — a left-to-right
 * gradient sweep, ~1.6s loop, no transform (so blocks don't grow/shrink
 * and disturb sibling layout).
 *
 * Use the primitives directly when a page has a unique layout, or use
 * the presets to drop a standard shape into a loading branch.
 */

import { CSSProperties } from "react";

interface SkeletonProps {
  width?: number | string;
  height?: number | string;
  borderRadius?: number | string;
  style?: CSSProperties;
  className?: string;
}

export default function Skeleton({
  width = "100%",
  height = 14,
  borderRadius = 3,
  style,
  className = "",
}: SkeletonProps) {
  return (
    <div
      className={`skeleton ${className}`.trim()}
      style={{
        width,
        height,
        borderRadius,
        display: "inline-block",
        ...style,
      }}
    />
  );
}

// ─── Presets ────────────────────────────────────────────────────────────────

/**
 * KPIGridSkeleton — rectangular grid of KPI cards. Each card is the same
 * dimensions as <KPICard> on the live pages (label + value + hint).
 */
export function KPIGridSkeleton({ count = 4 }: { count?: number }) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: `repeat(${count}, 1fr)`,
      gap: 10,
      marginBottom: 24,
    }}>
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          padding: "14px 16px",
          display: "flex",
          flexDirection: "column",
          gap: 8,
        }}>
          <Skeleton width={70} height={9} />
          <Skeleton width={90} height={20} />
          <Skeleton width={120} height={9} />
        </div>
      ))}
    </div>
  );
}

/**
 * TableSkeleton — bordered card containing N "rows" of placeholder
 * blocks. Each row has a configurable column shape: pass an array of
 * widths (numbers = px, strings = css width) and the row renders that
 * many blocks side by side.
 */
export function TableSkeleton({
  rows = 6,
  columns = [80, 60, "auto", 70, 50],
  withHeader = true,
}: {
  rows?: number;
  columns?: Array<number | string>;
  withHeader?: boolean;
}) {
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "16px 18px",
    }}>
      {withHeader && <Skeleton width={140} height={9} style={{ marginBottom: 14 }} />}
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} style={{
          display: "flex",
          alignItems: "center",
          gap: 16,
          paddingTop: 10,
          paddingBottom: 10,
          borderTop: i === 0 && !withHeader ? "none" : i === 0 ? "none" : "1px solid var(--line)",
        }}>
          {columns.map((w, j) => (
            <div key={j} style={{ flex: w === "auto" ? 1 : "0 0 auto" }}>
              <Skeleton width={w === "auto" ? "100%" : w} height={10} />
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

/**
 * CardSkeleton — single bordered card with a section label and N
 * blocks of skeleton text below. Used for "loading symbol detail" /
 * "loading conversation" / generic single-record pages.
 */
export function CardSkeleton({ lines = 4 }: { lines?: number }) {
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "20px 24px",
    }}>
      <Skeleton width={140} height={9} style={{ marginBottom: 14 }} />
      {Array.from({ length: lines }).map((_, i) => (
        <div key={i} style={{ marginBottom: 12 }}>
          <Skeleton width={i === 0 ? "60%" : i === lines - 1 ? "40%" : "100%"} height={12} />
        </div>
      ))}
    </div>
  );
}

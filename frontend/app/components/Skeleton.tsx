/**
 * frontend/app/components/Skeleton.tsx
 * =====================================
 * Reusable shimmer placeholder block. Use during data loading to render
 * the page chrome with gray pulsing rectangles where the values will
 * eventually be — keeps the layout stable and avoids the
 * "blank → loading text → full reflow" flash.
 *
 * Uses the .skeleton class defined in globals.css. The shimmer is a
 * left-to-right gradient sweep, ~1.6s loop, no transform (so the block
 * doesn't grow/shrink and disturb sibling layout).
 *
 * Example:
 *   <Skeleton width={120} height={18} />
 *   <Skeleton width="100%" height={6} borderRadius={2} />
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

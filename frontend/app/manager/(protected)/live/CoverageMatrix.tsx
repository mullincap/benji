"use client";

/**
 * frontend/app/manager/(protected)/live/CoverageMatrix.tsx
 * ============================================================
 * Coverage Matrix · PnL Correlation · 30D Rolling (Data Dictionary §8).
 *
 * NxN grid where N = number of open positions. Cell value is the
 * 30-day Pearson correlation of dollar-PnL series. Cells colored by
 * tier (cm-strong-con through cm-strong-hedge in the mockup CSS).
 * Diagonal renders as an em-dash on a faint background.
 *
 * Click a cell → highlights both its row and column so the user can
 * trace which two positions form the pair. Click again (or click a
 * different cell) to reset.
 *
 * For our long-only book: most cells will be on the red end of the
 * scale (positions move together). That's an honest read of the book.
 */

import { useState } from "react";
import type { CoverageMatrixResponse, CovTier } from "./types";

const TIER_BG: Record<CovTier, string> = {
  // Concentration scale (red — they move together)
  "strong-con": "rgba(255, 77, 77, 0.55)",
  "mid-con":    "rgba(255, 77, 77, 0.32)",
  "soft-con":   "rgba(255, 77, 77, 0.16)",
  // Neutral
  "neutral":    "transparent",
  // Hedge scale (blue — they cover each other)
  "soft-hedge":   "rgba(0, 194, 255, 0.13)",
  "mid-hedge":    "rgba(0, 194, 255, 0.26)",
  "strong-hedge": "rgba(0, 194, 255, 0.42)",
  // Sentinels
  "diag":         "var(--bg1)",
  "insufficient": "var(--bg1)",
};

const TIER_COLOR: Record<CovTier, string> = {
  "strong-con":   "#fff",
  "mid-con":      "var(--t0)",
  "soft-con":     "var(--t1)",
  "neutral":      "var(--t2)",
  "soft-hedge":   "var(--t1)",
  "mid-hedge":    "var(--t0)",
  "strong-hedge": "#001a26",
  "diag":         "var(--t3)",
  "insufficient": "var(--t3)",
};

interface Props {
  data: CoverageMatrixResponse | null;
}

export default function CoverageMatrix({ data }: Props) {
  const [highlight, setHighlight] = useState<[number, number] | null>(null);

  if (!data) {
    return <Skeleton />;
  }

  const n = data.rows.length;
  if (n === 0) {
    return (
      <Empty msg="No open positions" />
    );
  }
  if (n < 2) {
    return (
      <Empty msg="Need ≥ 2 positions for a correlation matrix" />
    );
  }

  // Row-label column auto-sizes to the longest ticker so 10-char names like
  // JELLYJELLY don't clip to "LLYJELLY". Space Mono 700 at 10px renders at
  // ~7px/char; +14px absorbs left/right padding plus a couple px breathing
  // room. Floor at 60px so a short-ticker book (e.g. all 3-4 char bases)
  // still has a visually balanced label column.
  const longestLabel = Math.max(0, ...data.rows.map((r) => r.symbol_base.length));
  const labelColPx = Math.max(60, longestLabel * 7 + 14);
  const labelCol = `${labelColPx}px`;
  const dataCol = "minmax(46px, 1fr)";
  const cols = `${labelCol} ${Array(n).fill(dataCol).join(" ")}`;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
      <Legend />
      <div
        style={{
          display: "grid",
          gridTemplateColumns: cols,
          gap: 1,
          background: "var(--line)",
          border: "1px solid var(--line)",
          borderRadius: 2,
          overflow: "hidden",
        }}
      >
        {/* Header row: corner + N column headers */}
        <Corner />
        {data.rows.map((r, j) => (
          <ColumnHeader
            key={`hdr-${j}`}
            symbol={r.symbol_base}
            highlighted={highlight !== null && (highlight[0] === j || highlight[1] === j)}
          />
        ))}
        {/* Body rows */}
        {data.rows.map((r, i) => (
          <Row
            key={`row-${i}`}
            i={i}
            label={r.symbol_base}
            sigma={r.sigma_daily}
            cells={data.matrix[i]}
            tiers={data.tiers[i]}
            n={n}
            highlight={highlight}
            onCellClick={(j) =>
              setHighlight((cur) =>
                cur && cur[0] === i && cur[1] === j ? null : [i, j]
              )
            }
          />
        ))}
      </div>
      {/* Reasons line for any insufficient-history rows */}
      {Object.keys(data.reasons).length > 0 && (
        <div
          style={{
            fontSize: 9,
            color: "var(--t3)",
            letterSpacing: "0.04em",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          {Object.entries(data.reasons).map(([sym, reason]) => (
            <span key={sym} style={{ marginRight: 12 }}>
              {sym.replace("USDT", "")}: {reason.replace(/_/g, " ")}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function Legend() {
  const items: { tier: CovTier; label: string }[] = [
    { tier: "strong-con", label: "concentration · move together" },
    { tier: "neutral",    label: "independent" },
    { tier: "strong-hedge", label: "hedge · cover each other" },
  ];
  return (
    <div
      style={{
        display: "flex",
        gap: 14,
        flexWrap: "wrap",
        alignItems: "center",
        fontSize: 10,
        color: "var(--t3)",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {items.map((it) => (
        <span key={it.tier} style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
          <span
            style={{
              display: "inline-block",
              width: 14,
              height: 12,
              borderRadius: 2,
              border: "1px solid var(--line2)",
              background: TIER_BG[it.tier] === "transparent" ? "var(--bg2)" : TIER_BG[it.tier],
            }}
          />
          {it.label}
        </span>
      ))}
    </div>
  );
}

function Corner() {
  return <div style={{ background: "var(--bg1)", height: 32 }} />;
}

function ColumnHeader({ symbol, highlighted }: { symbol: string; highlighted: boolean }) {
  return (
    <div
      style={{
        background: "var(--bg1)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        color: highlighted ? "var(--t0)" : "var(--t2)",
        height: 32,
        letterSpacing: "0.04em",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        borderBottom: highlighted ? "1px solid var(--green)" : "none",
        transition: "color 120ms ease",
      }}
    >
      {symbol}
    </div>
  );
}

function Row({
  i, label, sigma, cells, tiers, n, highlight, onCellClick,
}: {
  i: number;
  label: string;
  sigma: number | null;
  cells: (number | null)[];
  tiers: CovTier[];
  n: number;
  highlight: [number, number] | null;
  onCellClick: (j: number) => void;
}) {
  const rowHighlighted = highlight !== null && (highlight[0] === i || highlight[1] === i);
  return (
    <>
      <RowHeader
        label={label}
        sigma={sigma}
        highlighted={rowHighlighted}
      />
      {Array.from({ length: n }).map((_, j) => {
        const corr = cells[j];
        const tier = tiers[j];
        const cellHighlighted =
          highlight !== null &&
          (highlight[0] === i || highlight[1] === i ||
           highlight[0] === j || highlight[1] === j);
        const isIntersection =
          highlight !== null &&
          (highlight[0] === i || highlight[1] === i) &&
          (highlight[0] === j || highlight[1] === j);
        return (
          <Cell
            key={j}
            corr={corr}
            tier={tier}
            highlighted={cellHighlighted}
            intersection={isIntersection}
            onClick={() => onCellClick(j)}
          />
        );
      })}
    </>
  );
}

function RowHeader({
  label, sigma, highlighted,
}: { label: string; sigma: number | null; highlighted: boolean }) {
  return (
    <div
      title={sigma !== null ? `daily σ ≈ $${sigma.toFixed(2)}` : undefined}
      style={{
        background: "var(--bg1)",
        display: "flex",
        alignItems: "center",
        justifyContent: "flex-end",
        padding: "0 10px",
        fontSize: 10,
        fontWeight: 700,
        color: highlighted ? "var(--t0)" : "var(--t2)",
        height: 32,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        borderRight: highlighted ? "1px solid var(--green)" : "none",
        transition: "color 120ms ease",
      }}
    >
      {label}
    </div>
  );
}

function Cell({
  corr, tier, highlighted, intersection, onClick,
}: {
  corr: number | null;
  tier: CovTier;
  highlighted: boolean;
  intersection: boolean;
  onClick: () => void;
}) {
  if (tier === "diag") {
    return (
      <div
        style={{
          background: TIER_BG.diag,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: 32,
          fontSize: 10,
          color: TIER_COLOR.diag,
        }}
      >
        —
      </div>
    );
  }
  if (tier === "insufficient" || corr === null) {
    return (
      <div
        title="insufficient kline history (<14 days)"
        style={{
          background: TIER_BG.insufficient,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: 32,
          fontSize: 10,
          color: TIER_COLOR.insufficient,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        —
      </div>
    );
  }
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        background: TIER_BG[tier],
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: 32,
        fontSize: 10,
        fontWeight: 700,
        color: TIER_COLOR[tier],
        fontVariantNumeric: "tabular-nums",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        cursor: "pointer",
        border: intersection ? "1.5px solid var(--green)" : "none",
        outline: highlighted && !intersection ? "1px solid var(--line2)" : "none",
        outlineOffset: -1,
        filter: highlighted ? "brightness(1.18)" : "none",
        transition: "filter 120ms ease, border 120ms ease",
        padding: 0,
      }}
    >
      {fmtCorr(corr)}
    </button>
  );
}

function fmtCorr(v: number): string {
  const sign = v > 0 ? "+" : v < 0 ? "−" : "";
  return `${sign}${Math.abs(v).toFixed(2)}`;
}

function Skeleton() {
  return (
    <div style={{ height: 280, display: "flex", flexDirection: "column", gap: 10 }}>
      {Array.from({ length: 7 }).map((_, i) => (
        <div key={i} style={{ display: "flex", gap: 1 }}>
          {Array.from({ length: 8 }).map((_, j) => (
            <div
              key={j}
              style={{
                flex: j === 0 ? `0 0 60px` : 1,
                background: "var(--bg2)",
                height: 32,
              }}
            />
          ))}
        </div>
      ))}
    </div>
  );
}

function Empty({ msg }: { msg: string }) {
  return (
    <div
      style={{
        height: 200,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        color: "var(--t3)",
        fontSize: 11,
        letterSpacing: "0.06em",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {msg}
    </div>
  );
}

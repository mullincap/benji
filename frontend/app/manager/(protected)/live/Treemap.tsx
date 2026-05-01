"use client";

/**
 * frontend/app/manager/(protected)/live/Treemap.tsx
 * ====================================================
 * Position Map (Data Dictionary §5).
 *
 * Tile area ∝ notional, color tier ∝ unrealized PnL %. Click any tile
 * to scroll to + pulse the matching row in the Open Positions Table.
 *
 * Layout (per §5):
 *   * Top 1–2 positions whose share > 20% of book render as full-height
 *     column tiles, each with `flex: share_pct`.
 *   * Remaining positions go into a sub-grid on the right (2 columns
 *     for ≤4 sub-grid positions, 3 columns for 5–8, repeat(ceil(N/3))
 *     for 9+).
 *   * Floor on tile dimensions: 60×40 px so tiny positions stay legible.
 *
 * Hysteresis (per §5 "Mid-PnL flickering"): a tile's color tier only
 * changes when the new candidate tier has been observed for 2
 * consecutive ticks. Implemented via a ref-tracked pending-tier counter.
 */

import { CSSProperties, useEffect, useRef, useState } from "react";
import type { LivePosition } from "./types";

type Tier = "strong-green" | "soft-green" | "soft-red" | "mid-red" | "strong-red";

const TIER_STYLES: Record<Tier, CSSProperties> = {
  "strong-green": { background: "rgba(0, 200, 150, 0.22)", border: "1px solid rgba(0, 200, 150, 0.50)" },
  "soft-green":   { background: "rgba(0, 200, 150, 0.10)", border: "1px solid rgba(0, 200, 150, 0.30)" },
  "soft-red":     { background: "rgba(255, 77, 77, 0.10)",  border: "1px solid rgba(255, 77, 77, 0.30)" },
  "mid-red":      { background: "rgba(255, 77, 77, 0.20)",  border: "1px solid rgba(255, 77, 77, 0.50)" },
  "strong-red":   { background: "rgba(255, 77, 77, 0.32)",  border: "1px solid rgba(255, 77, 77, 0.65)" },
};

function computeTier(pct: number): Tier {
  if (pct > 5) return "strong-green";
  if (pct >= 0) return "soft-green";
  if (pct >= -3) return "soft-red";
  if (pct >= -10) return "mid-red";
  return "strong-red";
}

/** 2-tick hysteresis: a tile's tier only switches after the new candidate
 *  has held for two consecutive ticks. State persists per-symbol via ref;
 *  effect updates the displayed map when positions change. */
function useTreemapTiers(positions: LivePosition[]): Record<string, Tier> {
  const [displayed, setDisplayed] = useState<Record<string, Tier>>({});
  const pendingRef = useRef<Record<string, { tier: Tier; count: number }>>({});

  useEffect(() => {
    setDisplayed((prev) => {
      const next: Record<string, Tier> = { ...prev };
      const seen = new Set<string>();
      for (const p of positions) {
        seen.add(p.symbol);
        const desired = computeTier(p.unrealized_pnl_pct);
        const cur = next[p.symbol];
        if (cur === undefined) {
          // First observation: snap to current tier, no pending state.
          next[p.symbol] = desired;
          delete pendingRef.current[p.symbol];
          continue;
        }
        if (desired === cur) {
          delete pendingRef.current[p.symbol];
          continue;
        }
        const pending = pendingRef.current[p.symbol];
        if (pending && pending.tier === desired) {
          pending.count += 1;
          if (pending.count >= 2) {
            next[p.symbol] = desired;
            delete pendingRef.current[p.symbol];
          }
        } else {
          pendingRef.current[p.symbol] = { tier: desired, count: 1 };
        }
      }
      // Garbage-collect entries for positions that closed.
      for (const sym of Object.keys(next)) {
        if (!seen.has(sym)) {
          delete next[sym];
          delete pendingRef.current[sym];
        }
      }
      return next;
    });
  }, [positions]);

  return displayed;
}

interface Props {
  positions: LivePosition[];
  onTileClick?: (symbol: string) => void;
}

export default function Treemap({ positions, onTileClick }: Props) {
  const sorted = [...positions].sort((a, b) => b.notional_usd - a.notional_usd);
  const totalNotional = sorted.reduce((acc, p) => acc + p.notional_usd, 0);
  const tiers = useTreemapTiers(sorted);

  if (sorted.length === 0 || totalNotional === 0) {
    return (
      <div
        style={{
          height: 240,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--t3)",
          fontSize: 11,
          letterSpacing: "0.06em",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        No open positions
      </div>
    );
  }

  // Partition: top 1-2 positions where share > 20% → full-height column.
  // Remaining → sub-grid.
  const fullHeight: LivePosition[] = [];
  const subGrid: LivePosition[] = [];
  for (let i = 0; i < sorted.length; i++) {
    const p = sorted[i];
    const share = (p.notional_usd / totalNotional) * 100;
    if (i < 2 && share > 20) {
      fullHeight.push(p);
    } else {
      subGrid.push(p);
    }
  }

  const subGridShare = subGrid.reduce((acc, p) => acc + p.notional_usd, 0);
  const subGridPct = totalNotional > 0 ? (subGridShare / totalNotional) * 100 : 0;

  return (
    <div style={{ display: "flex", height: 240, gap: 3 }}>
      {fullHeight.map((p) => {
        const share = (p.notional_usd / totalNotional) * 100;
        return (
          <Tile
            key={p.symbol}
            position={p}
            tier={tiers[p.symbol] ?? computeTier(p.unrealized_pnl_pct)}
            share={share}
            full
            onClick={onTileClick}
            flex={share}
          />
        );
      })}
      {subGrid.length > 0 && (
        <SubGrid
          positions={subGrid}
          tiers={tiers}
          subGridShare={subGridShare}
          flex={subGridPct}
          onTileClick={onTileClick}
          totalNotional={totalNotional}
        />
      )}
    </div>
  );
}

function SubGrid({
  positions, tiers, subGridShare, flex, onTileClick, totalNotional,
}: {
  positions: LivePosition[];
  tiers: Record<string, Tier>;
  subGridShare: number;
  flex: number;
  onTileClick?: (symbol: string) => void;
  totalNotional: number;
}) {
  // 2 cols for ≤4, 3 cols for 5–8, ceil(N/3) cols for 9+.
  const cols = positions.length <= 4 ? 2 : positions.length <= 8 ? 3 : Math.ceil(positions.length / 3);
  return (
    <div
      style={{
        flex,
        minWidth: 60,
        display: "grid",
        gridTemplateColumns: `repeat(${cols}, 1fr)`,
        gridAutoRows: "1fr",
        gap: 3,
      }}
    >
      {positions.map((p) => {
        // Within-subgrid share for label clarity (vs book-wide share).
        const shareOfBook = (p.notional_usd / totalNotional) * 100;
        return (
          <Tile
            key={p.symbol}
            position={p}
            tier={tiers[p.symbol] ?? computeTier(p.unrealized_pnl_pct)}
            share={shareOfBook}
            full={false}
            onClick={onTileClick}
          />
        );
      })}
    </div>
  );
}

function Tile({
  position, tier, share, full, flex, onClick,
}: {
  position: LivePosition;
  tier: Tier;
  share: number;
  full: boolean;
  flex?: number;
  onClick?: (symbol: string) => void;
}) {
  const tierStyle = TIER_STYLES[tier];
  const upl = position.unrealized_pnl_usd;
  const uplPct = position.unrealized_pnl_pct;
  const isPos = uplPct >= 0;

  return (
    <button
      type="button"
      onClick={() => onClick?.(position.symbol)}
      title={`${position.symbol_base} · ${position.side.toUpperCase()} · ${position.source.toUpperCase()}`}
      style={{
        ...tierStyle,
        flex: flex !== undefined ? flex : undefined,
        minWidth: 60,
        minHeight: 40,
        borderRadius: 3,
        padding: full ? "14px 16px" : "10px 12px",
        display: "flex",
        flexDirection: "column",
        justifyContent: "space-between",
        cursor: "pointer",
        position: "relative",
        overflow: "hidden",
        textAlign: "left",
        transition: "filter 120ms ease, transform 120ms ease, background 220ms ease, border-color 220ms ease",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        color: "var(--t1)",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.filter = "brightness(1.15)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.filter = "none";
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          gap: 8,
        }}
      >
        <div>
          <div
            style={{
              fontSize: full ? 22 : 13,
              fontWeight: 700,
              letterSpacing: "0.02em",
              color: "var(--t0)",
              lineHeight: 1.05,
            }}
          >
            {position.symbol_base}
          </div>
          {full && (
            <div
              style={{
                fontSize: 8,
                letterSpacing: "0.16em",
                color: "var(--t3)",
                marginTop: 2,
                textTransform: "uppercase",
              }}
            >
              {position.source === "strategy" ? (position.strategy_name ?? "STRATEGY") : "MANUAL"}
              {" · "}
              {position.leverage ? `${position.leverage.toFixed(1)}×` : "—"}
            </div>
          )}
        </div>
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.14em",
            padding: "2px 6px",
            borderRadius: 2,
            color: position.side === "long" ? "var(--green)" : "var(--red)",
            background:
              position.side === "long"
                ? "rgba(0, 200, 150, 0.18)"
                : "rgba(255, 77, 77, 0.18)",
          }}
        >
          {full ? (position.side === "long" ? "LONG" : "SHORT") : (position.side === "long" ? "L" : "S")}
        </span>
      </div>
      <div
        style={{
          fontSize: full ? 30 : 16,
          fontWeight: 700,
          lineHeight: 1,
          margin: full ? "8px 0" : "4px 0",
          fontVariantNumeric: "tabular-nums",
          color: isPos ? "var(--green)" : "var(--red)",
        }}
      >
        {isPos ? "+" : ""}{uplPct.toFixed(1)}%
      </div>
      <div
        style={{
          fontSize: full ? 10 : 9,
          color: "var(--t3)",
          display: "flex",
          justifyContent: "space-between",
          gap: 8,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        <span>
          <span style={{ color: "var(--t0)", fontWeight: 700 }}>
            ${position.notional_usd.toLocaleString(undefined, { maximumFractionDigits: 0 })}
          </span>
          {" · "}
          {share.toFixed(1)}%
        </span>
        <span style={{ color: isPos ? "var(--green)" : "var(--red)", fontWeight: 700 }}>
          {isPos ? "+" : "−"}${Math.abs(upl).toFixed(2)}
        </span>
      </div>
    </button>
  );
}

"use client";

/**
 * frontend/app/manager/(protected)/live/MAAlignmentHeatmap.tsx
 * ===============================================================
 * MA Alignment · Distance from EMA · By Timeframe (Data Dictionary §11).
 *
 * Rows = open positions. Columns = (label, side, 5m, 15m, 30m, 1h, 4h,
 * 8h, 1D, CONFLUENCE). Cell value is the signed % distance between
 * mark price and EMA20 on that timeframe; cell color reflects whether
 * that distance is aligned with the position's direction.
 *
 * Confluence = count of timeframes where mark is aligned with position
 * direction. Color-tiered: 7/7 strong green, 5-6 soft green, 3-4 amber,
 * 1-2 red, 0 strong red.
 *
 * Cells with null distance (insufficient history, symbol not listed on
 * Binance USDM, fetch failure) render as a neutral '—'. The reason
 * surfaces in the hover tooltip so the operator can tell symbol-not-
 * listed from a transient fetch hiccup.
 */

import { useEffect, useRef, useState } from "react";
import type { MaAlignmentResponse, MaCell, MaAlignmentTier, Side } from "./types";

// Project tokens used inline. Same colors as the mockup's CSS classes.
const TIER_BG: Record<MaAlignmentTier, string> = {
  "aligned-strong": "rgba(0, 200, 150, 0.50)",
  "aligned-mid":    "rgba(0, 200, 150, 0.28)",
  "aligned-soft":   "rgba(0, 200, 150, 0.13)",
  "neutral":        "transparent",
  "against-soft":   "rgba(255, 77, 77, 0.13)",
  "against-mid":    "rgba(255, 77, 77, 0.30)",
  "against-strong": "rgba(255, 77, 77, 0.55)",
};

const TIER_COLOR: Record<MaAlignmentTier, string> = {
  "aligned-strong": "#002418",  // dark green text on bright bg
  "aligned-mid":    "var(--t0)",
  "aligned-soft":   "var(--t1)",
  "neutral":        "var(--t3)",
  "against-soft":   "var(--t1)",
  "against-mid":    "var(--t1)",
  "against-strong": "#fff",
};

interface Props {
  data: MaAlignmentResponse | null;
}

export default function MAAlignmentHeatmap({ data }: Props) {
  if (!data) {
    return <Skeleton timeframes={DEFAULT_TFS} rowCount={6} />;
  }

  if (data.rows.length === 0) {
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
        No open positions
      </div>
    );
  }

  const tfs = data.timeframes;
  // TF columns flex with available width via 1fr; floor 56px keeps the
  // numbers legible at the narrow end. Container maxWidth caps total span
  // so on a wide viewport with few timeframes the TF cells don't balloon
  // into oversized rectangles. On tight layouts (chat panel open) the
  // maxWidth is far above the actual width, so 1fr still fills.
  const cols = `80px 36px ${tfs.map(() => "minmax(56px, 1fr)").join(" ")} 70px`;
  const MAX_TF_WIDTH = 140;
  const containerMaxWidth = 80 + 36 + 70 + tfs.length * MAX_TF_WIDTH + (tfs.length + 2) * 1;

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
          maxWidth: containerMaxWidth,
        }}
      >
        {/* Header row */}
        <Header />
        <Header />
        {tfs.map((tf) => (
          <HeaderCell key={tf}>{tf}</HeaderCell>
        ))}
        <HeaderCell>CONFLUENCE</HeaderCell>

        {/* Body rows */}
        {data.rows.map((row) => (
          <Row key={row.symbol} row={row} tfs={tfs} />
        ))}
      </div>
    </div>
  );
}

const DEFAULT_TFS = ["5m", "15m", "30m", "1h", "4h", "8h", "1d"];

function Legend() {
  const items: { tier: MaAlignmentTier; label: string }[] = [
    { tier: "aligned-strong", label: "strongly aligned" },
    { tier: "aligned-soft",   label: "soft aligned" },
    { tier: "neutral",        label: "neutral" },
    { tier: "against-soft",   label: "soft against" },
    { tier: "against-strong", label: "strongly against" },
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
              verticalAlign: "middle",
            }}
          />
          {it.label}
        </span>
      ))}
    </div>
  );
}

function Header() {
  return (
    <div
      style={{
        background: "var(--bg1)",
        height: 32,
      }}
    />
  );
}

function HeaderCell({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        background: "var(--bg1)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        letterSpacing: "0.08em",
        color: "var(--t3)",
        height: 32,
        padding: "0 4px",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {children}
    </div>
  );
}

function Row({ row, tfs }: { row: MaAlignmentResponse["rows"][number]; tfs: string[] }) {
  return (
    <>
      <PosLabel symbol={row.symbol_base} />
      <SideTag side={row.side} />
      {tfs.map((tf) => {
        const cell = row.cells[tf] ?? {
          distance_pct: null, ema_value: null, tier: "neutral", reason: "missing",
        } as MaCell;
        return <Cell key={tf} cell={cell} symbol={row.symbol_base} tf={tf} />;
      })}
      <ConfluenceCell aligned={row.confluence_aligned} total={row.confluence_total} />
    </>
  );
}

function PosLabel({ symbol }: { symbol: string }) {
  return (
    <div
      style={{
        background: "var(--bg1)",
        display: "flex",
        alignItems: "center",
        justifyContent: "flex-end",
        padding: "0 10px",
        fontSize: 11,
        fontWeight: 700,
        color: "var(--t0)",
        letterSpacing: "0.02em",
        height: 32,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {symbol}
    </div>
  );
}

function SideTag({ side }: { side: Side }) {
  return (
    <div
      style={{
        background: "var(--bg1)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.1em",
        color: side === "long" ? "var(--green)" : "var(--red)",
        height: 32,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {side === "long" ? "L" : "S"}
    </div>
  );
}

function Cell({ cell, symbol, tf }: { cell: MaCell; symbol: string; tf: string }) {
  const isNull = cell.distance_pct === null || cell.ema_value === null;
  const tier = cell.tier;
  const bg = TIER_BG[tier];
  const color = TIER_COLOR[tier];
  const tooltip = isNull
    ? `${symbol} · ${tf} · ${cell.reason ?? "no data"}`
    : `${symbol} · ${tf} · EMA ${formatEma(cell.ema_value!)} · distance ${formatPct(cell.distance_pct!)}`;

  // Bar-close pulse: when this cell's distance_pct changes, briefly outline
  // the cell with a green border so the operator sees that the bar just
  // closed and the value updated. The pulse lives 800ms (300ms in, 500ms
  // fade) — short enough to feel like a pulse, long enough to register at
  // a glance during the 60s polling cadence. First render is suppressed by
  // tracking previous via a ref so the whole grid doesn't pulse on mount.
  const prev = useRef<number | null>(null);
  const [pulseKey, setPulseKey] = useState(0);
  useEffect(() => {
    if (prev.current !== null && prev.current !== cell.distance_pct) {
      setPulseKey((k) => k + 1);
    }
    prev.current = cell.distance_pct;
  }, [cell.distance_pct]);

  return (
    <div
      title={tooltip}
      style={{
        background: bg,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        color,
        height: 32,
        fontVariantNumeric: "tabular-nums",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        cursor: "default",
        transition: "background 220ms ease",
        position: "relative",
      }}
    >
      {pulseKey > 0 && (
        <span
          key={pulseKey}
          aria-hidden
          style={{
            position: "absolute",
            inset: 0,
            border: "1.5px solid var(--green)",
            borderRadius: 1,
            pointerEvents: "none",
            animation: "live-cell-pulse 800ms ease-out forwards",
          }}
        />
      )}
      {isNull ? (
        <span style={{ color: "var(--t3)" }}>—</span>
      ) : (
        formatPct(cell.distance_pct!)
      )}
    </div>
  );
}

function ConfluenceCell({ aligned, total }: { aligned: number; total: number }) {
  let color = "var(--t1)";
  if (total === 0) color = "var(--t3)";
  else if (aligned === total) color = "var(--green)";
  else if (aligned >= 5) color = "var(--green)";
  else if (aligned >= 3) color = "var(--amber)";
  else if (aligned >= 1) color = "var(--red)";
  else color = "var(--red)";

  return (
    <div
      style={{
        background: "var(--bg1)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 10,
        fontWeight: 700,
        letterSpacing: "0.08em",
        color,
        height: 32,
        fontVariantNumeric: "tabular-nums",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {total === 0 ? "—" : `${aligned} / ${total}`}
    </div>
  );
}

function formatPct(v: number): string {
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(1)}%`;
}

function formatEma(v: number): string {
  if (v >= 1000) return `$${v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  if (v >= 1) return `$${v.toFixed(4)}`;
  return `$${v.toFixed(6)}`;
}

function Skeleton({ timeframes, rowCount }: { timeframes: string[]; rowCount: number }) {
  const cols = `80px 36px ${timeframes.map(() => "minmax(56px, 1fr)").join(" ")} 70px`;
  const maxWidth = 80 + 36 + 70 + timeframes.length * 140 + (timeframes.length + 2) * 1;
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: cols,
        gap: 1,
        background: "var(--line)",
        border: "1px solid var(--line)",
        borderRadius: 2,
        overflow: "hidden",
        maxWidth,
      }}
    >
      <Header />
      <Header />
      {timeframes.map((tf) => (
        <HeaderCell key={tf}>{tf}</HeaderCell>
      ))}
      <HeaderCell>CONFLUENCE</HeaderCell>
      {Array.from({ length: rowCount }).map((_, ri) => (
        <SkeletonRow key={ri} cols={2 + timeframes.length + 1} />
      ))}
    </div>
  );
}

function SkeletonRow({ cols }: { cols: number }) {
  return (
    <>
      {Array.from({ length: cols }).map((_, i) => (
        <div
          key={i}
          style={{
            background: "var(--bg2)",
            height: 32,
          }}
        />
      ))}
    </>
  );
}

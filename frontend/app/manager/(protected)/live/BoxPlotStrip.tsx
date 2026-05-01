"use client";

/**
 * frontend/app/manager/(protected)/live/BoxPlotStrip.tsx
 * =========================================================
 * Trailing Distribution · 24H Window (Data Dictionary §10).
 *
 * One SVG per open position laid out in a responsive grid. Each cell
 * shows:
 *   * Whisker: y-axis line spanning [win_min, win_max] of the 24h
 *     close distribution.
 *   * Box: p25 → p75
 *   * Median line: p50
 *   * Mark line + dot: live mark, dashed horizontal reference
 *   * Entry triangle: position's entry price
 *   * High/low price labels above/below the SVG
 *   * Mark + entry numeric labels in the foot
 *   * Trend arrow + σ value (regression-slope-σ over the same window)
 *
 * Color rules:
 *   * Mark dot: 'good' (aligned), 'bad' (counter-aligned), 'neu' (mid)
 *   * Trend arrow: 'good' (aligned with position direction), 'bad'
 *     (counter), 'neu' (flat regardless of side)
 *
 * Click on a cell scrolls + pulses the matching row in the Open
 * Positions Table — same affordance the Treemap uses (shared
 * onCellClick callback).
 */

import type { BoxPlotCell, BoxDotClass, TrendDirection } from "./types";

// SVG viewBox is the inner box-plot canvas only — the cell's header/sub/foot
// rows live in HTML around it. Whiskers are intentionally compressed (Y_TOP /
// Y_BOTTOM occupy ~78% of the view) so the box (p25→p75) and median line read
// loud at narrow widths; the SVG itself stretches via aspect-ratio on its
// container, not a fixed pixel height.
const VIEW = { w: 100, h: 130 } as const;
const Y_TOP = 15;
const Y_BOTTOM = 115;
const Y_LABEL_TOP = 10;
const Y_LABEL_BOTTOM = 125;

const COLOR_GOOD = "var(--green)";
const COLOR_BAD = "var(--red)";
const COLOR_NEU = "var(--amber)";

interface Props {
  cells: BoxPlotCell[] | null;
  onCellClick?: (symbol: string) => void;
}

export default function BoxPlotStrip({ cells, onCellClick }: Props) {
  if (!cells) {
    return (
      <div style={gridStyle(6)}>
        {Array.from({ length: 6 }).map((_, i) => (
          <CellShell key={i} />
        ))}
      </div>
    );
  }

  if (cells.length === 0) {
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

  return (
    <div style={gridStyle(cells.length)}>
      {cells.map((c) => (
        <Cell key={c.symbol} cell={c} onClick={onCellClick} />
      ))}
    </div>
  );
}

// Cells fill available width via 1fr (auto-fit + 1fr). The container
// maxWidth caps total span at MAX_CELL_WIDTH × N so a low-count book on a
// wide viewport doesn't balloon individual cells (with aspect-ratio 1:1.4
// a 600px-wide cell would be 840px tall — visually wrong). On a tight
// layout (e.g. with the chat panel open) the maxWidth is far above the
// available width, so it has no effect and 1fr still fills.
const GRID_GAP = 8;
const MIN_CELL_WIDTH = 200;
const MAX_CELL_WIDTH = 360;

function gridStyle(count: number): React.CSSProperties {
  return {
    display: "grid",
    gridTemplateColumns: `repeat(auto-fit, minmax(${MIN_CELL_WIDTH}px, 1fr))`,
    gap: GRID_GAP,
    maxWidth: count * MAX_CELL_WIDTH + (count - 1) * GRID_GAP,
  };
}

function CellShell({ children }: { children?: React.ReactNode }) {
  return (
    <div
      style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: "10px 10px 8px",
        display: "flex",
        flexDirection: "column",
        gap: 4,
        minWidth: 0,
        width: "100%",
        aspectRatio: "1 / 1.4",
      }}
    >
      {children}
    </div>
  );
}

function Cell({ cell, onClick }: { cell: BoxPlotCell; onClick?: (s: string) => void }) {
  const upl = cell.mark_price !== null && cell.entry_price !== null
    ? ((cell.mark_price - cell.entry_price) / cell.entry_price) * 100 *
      (cell.side === "long" ? 1 : -1)
    : null;

  return (
    <button
      type="button"
      onClick={() => onClick?.(cell.symbol)}
      style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 3,
        padding: "10px 10px 8px",
        display: "flex",
        flexDirection: "column",
        gap: 4,
        minWidth: 0,
        width: "100%",
        aspectRatio: "1 / 1.4",
        cursor: "pointer",
        textAlign: "left",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        color: "var(--t1)",
        transition: "filter 120ms ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.filter = "brightness(1.08)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.filter = "none"; }}
    >
      {/* Header row: ticker + UPL pct */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: "var(--t0)", letterSpacing: "0.04em" }}>
          {cell.symbol_base}
        </span>
        <span
          style={{
            fontSize: 11,
            fontWeight: 700,
            fontVariantNumeric: "tabular-nums",
            color: upl === null ? "var(--t3)" : upl >= 0 ? "var(--green)" : "var(--red)",
          }}
        >
          {upl === null ? "—" : `${upl >= 0 ? "+" : ""}${upl.toFixed(1)}%`}
        </span>
      </div>

      {/* Sub-row: side tag + current mark */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          fontSize: 9,
          letterSpacing: "0.08em",
          color: "var(--t3)",
          marginBottom: 2,
        }}
      >
        <span style={{ color: cell.side === "long" ? "var(--green)" : "var(--red)" }}>
          {cell.side.toUpperCase()}
        </span>
        <span>{formatPrice(cell.mark_price)}</span>
      </div>

      {/* SVG zone — or fallback caption when distribution missing */}
      <div
        style={{
          flex: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          marginTop: 2,
        }}
      >
        {cell.reason ? (
          <SvgFallback reason={cell.reason} />
        ) : (
          <BoxPlotSvg cell={cell} />
        )}
      </div>

      {/* Footer: entry label + trend */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          fontSize: 10,
          fontVariantNumeric: "tabular-nums",
          borderTop: "1px dashed var(--line)",
          paddingTop: 6,
          marginTop: 2,
        }}
      >
        <span style={{ color: "var(--t3)", fontSize: 9, letterSpacing: "0.06em" }}>
          entry {formatPrice(cell.entry_price)}
        </span>
        <span
          style={{
            display: "flex",
            alignItems: "center",
            gap: 4,
            fontWeight: 700,
            color: trendCssColor(cell.trend_color),
          }}
        >
          {trendArrow(cell.trend_direction)}{" "}
          {cell.slope_sigma === null
            ? "—"
            : `${cell.slope_sigma >= 0 ? "+" : ""}${cell.slope_sigma.toFixed(1)}σ`}
        </span>
      </div>
    </button>
  );
}

function SvgFallback({ reason }: { reason: string }) {
  const text = reason === "insufficient_data"
    ? "INSUFFICIENT DATA"
    : reason === "not_listed"
      ? "NOT LISTED ON BINANCE"
      : reason === "fetch_error"
        ? "FETCH ERROR"
        : reason.toUpperCase();
  return (
    <div
      style={{
        textAlign: "center",
        color: "var(--t3)",
        fontSize: 9,
        letterSpacing: "0.1em",
        padding: "0 8px",
      }}
    >
      {text}
    </div>
  );
}

function BoxPlotSvg({ cell }: { cell: BoxPlotCell }) {
  // Bail-out guard — endpoint should set reason in this case but
  // double-check; better to render fallback than crash on null math.
  if (
    cell.win_min === null ||
    cell.win_max === null ||
    cell.p25 === null ||
    cell.p50 === null ||
    cell.p75 === null
  ) {
    return <SvgFallback reason="insufficient_data" />;
  }

  const win_min = cell.win_min;
  const win_max = cell.win_max;
  const range = win_max - win_min;

  // y(price): linear map [win_max, win_min] → [Y_TOP, Y_BOTTOM]. When
  // the window is degenerate (max == min, e.g. fresh listing with 1
  // price), pin everything to the middle.
  const y = (p: number): number => {
    if (range === 0) return (Y_TOP + Y_BOTTOM) / 2;
    return Y_TOP + ((win_max - p) / range) * (Y_BOTTOM - Y_TOP);
  };

  const yClamp = (p: number): number => Math.max(Y_TOP, Math.min(Y_BOTTOM, y(p)));

  const yP25 = y(cell.p25);
  const yP75 = y(cell.p75);
  const yP50 = y(cell.p50);
  const yMark = cell.mark_price !== null ? yClamp(cell.mark_price) : null;
  const yEntry = cell.entry_price !== null ? yClamp(cell.entry_price) : null;

  const dotColor = dotCssColor(cell.mark_dot);

  return (
    <svg
      viewBox={`0 0 ${VIEW.w} ${VIEW.h}`}
      preserveAspectRatio="none"
      style={{ width: "100%", height: "100%", display: "block" }}
    >
      {/* High / low labels */}
      <text
        x="50"
        y={Y_LABEL_TOP}
        textAnchor="middle"
        style={{ fill: "var(--t3)", fontSize: 7.5, fontFamily: "Space Mono, monospace", letterSpacing: "0.08em" }}
      >
        {formatPrice(win_max)}
      </text>
      <text
        x="50"
        y={Y_LABEL_BOTTOM}
        textAnchor="middle"
        style={{ fill: "var(--t3)", fontSize: 7.5, fontFamily: "Space Mono, monospace", letterSpacing: "0.08em" }}
      >
        {formatPrice(win_min)}
      </text>

      {/* Whisker (full window) + caps */}
      <line x1="50" y1={Y_TOP} x2="50" y2={Y_BOTTOM} stroke="var(--t3)" strokeWidth="1" />
      <line x1="42" y1={Y_TOP} x2="58" y2={Y_TOP} stroke="var(--t3)" strokeWidth="1" />
      <line x1="42" y1={Y_BOTTOM} x2="58" y2={Y_BOTTOM} stroke="var(--t3)" strokeWidth="1" />

      {/* Box (p25 → p75) */}
      <rect
        x="30"
        y={yP75}
        width="40"
        height={Math.max(1, yP25 - yP75)}
        fill="rgba(255,255,255,0.06)"
        stroke="var(--line2)"
        strokeWidth="1"
      />
      {/* Median (p50) */}
      <line x1="30" y1={yP50} x2="70" y2={yP50} stroke="var(--t1)" strokeWidth="1.5" />

      {/* Mark line (dashed horizontal) + dot */}
      {yMark !== null && (
        <>
          <line
            x1="14" y1={yMark} x2="86" y2={yMark}
            stroke={dotColor} strokeWidth="1" strokeDasharray="3 3" fill="none"
          />
          <circle cx="50" cy={yMark} r="4" fill={dotColor} stroke="var(--bg0, #080809)" strokeWidth="1" />
        </>
      )}

      {/* Entry triangle (right side, pointing left) */}
      {yEntry !== null && (
        <polygon
          points={`74,${yEntry} 80,${yEntry - 3} 80,${yEntry + 3}`}
          fill="var(--amber)"
          stroke="var(--bg0, #080809)"
          strokeWidth="0.5"
        />
      )}
    </svg>
  );
}

// ── Helpers ───────────────────────────────────────────────────────────

function formatPrice(p: number | null): string {
  if (p === null) return "—";
  if (p >= 1000) return `$${(p / 1000).toFixed(1)}K`;
  if (p >= 1) return `$${p.toFixed(p >= 100 ? 2 : 4)}`;
  if (p >= 0.001) return `$${p.toFixed(5)}`;
  return `$${p.toExponential(2)}`;
}

function dotCssColor(c: BoxDotClass): string {
  return c === "good" ? COLOR_GOOD : c === "bad" ? COLOR_BAD : COLOR_NEU;
}

function trendCssColor(c: BoxDotClass): string {
  return c === "good" ? COLOR_GOOD : c === "bad" ? COLOR_BAD : COLOR_NEU;
}

function trendArrow(d: TrendDirection | null): string {
  switch (d) {
    case "strong-up":   return "↗";
    case "up":          return "↗";
    case "flat":        return "→";
    case "down":        return "↘";
    case "strong-down": return "↘";
    default:            return "—";
  }
}

"use client";

/**
 * PortfolioMatrix.tsx
 * ====================
 * Two-theme renderer for the 5-MIN ROI MATRIX on the portfolio detail
 * page. SIMPLE theme is the long-standing tabular view; ADVANCED is the
 * heatmap + sparkline + statistics footer treatment specified in the
 * matrix style brief. Theme + scope persist in localStorage, scoped per
 * browser, independent of allocation/session.
 *
 * Layout split (per the spec's "implementation pattern suggestion"):
 *   MatrixContainer
 *     ├─ MatrixHeader          (title + theme toggle, persistent)
 *     ├─ ScopePills            (1H/4H/ALL — Advanced only)
 *     └─ SimpleMatrix | AdvancedMatrix   (data body)
 *
 * Both children receive the same `bars` prop. Each handles its own
 * row order, column ranges, and statistics — no Simple-leaks-into-
 * Advanced behavior.
 */

import { useEffect, useMemo, useState } from "react";

const FONT_MONO = `var(--font-space-mono), ui-monospace, "SF Mono", Menlo, Consolas, monospace`;
const PORTFOLIO_COLOR = "#00c896";

const STORAGE_KEYS = {
  theme: "portfolio.matrix.theme",
  scope: "portfolio.matrix.scope",
};

type Theme = "simple" | "advanced";
type Scope = "1H" | "4H" | "ALL";

interface PortfolioBar {
  bar: number;
  ts: string;
  incr: number;
  peak: number;
  sym_returns: Record<string, number>;
  stopped: string[];
}

interface Props {
  bars: PortfolioBar[];
  symbolsOrdered: string[];
  stoppedAtBar: Map<string, number>;
  symStopsCount: number;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}%`;
}

function fmtPctPlain(v: number, digits = 2): string {
  // Negative is rendered with a regular minus (no leading "+"); positive
  // gets a leading "+". Used for stats footer where the delta is more
  // readable without leading whitespace.
  return `${v >= 0 ? "+" : ""}${v.toFixed(digits)}`;
}

function fmtTimeShort(ts: string): string {
  return ts.split(" ")[1]?.slice(0, 5) ?? ts;
}

// Heatmap tint — per-column normalized, capped opacity. Cap chosen by the
// brief: 0.18 default, 0.12 for the PORTFOLIO column where it composites
// over the column's own faint green tint. Higher caps trade polish for
// noise — non-negotiable per the brief.
function cellTint(
  value: number,
  rangeAbsMax: number,
  cap = 0.18,
): string {
  if (value === 0 || rangeAbsMax === 0) return "transparent";
  const normalized = Math.abs(value) / rangeAbsMax;
  const opacity = Math.min(normalized * cap, cap);
  if (value < 0) return `rgba(239, 68, 68, ${opacity.toFixed(3)})`;
  return `rgba(0, 200, 150, ${opacity.toFixed(3)})`;
}

// Sparkline color — average sentiment for the column. Stopped columns
// always use a flat dim red to reinforce "no movement," even if the
// historical pre-stop trajectory was positive.
function sparkColor(values: number[], isStopped: boolean): string {
  if (isStopped) return "#835656";
  if (values.length === 0) return "#5e8a73";
  const avg = values.reduce((a, b) => a + b, 0) / values.length;
  return avg < 0 ? "#a07474" : "#5e8a73";
}

function sparkPath(values: number[]): string {
  if (values.length === 0) return "";
  if (values.length === 1) return `M0,7 L80,7`;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const xStep = 80 / (values.length - 1);
  return values
    .map((v, i) => {
      const x = (i * xStep).toFixed(1);
      const y = (14 - ((v - min) / range) * 12 - 1).toFixed(1);
      return `${i === 0 ? "M" : "L"}${x},${y}`;
    })
    .join(" ");
}

// Population stddev — analyst-credibility per-character. Sample stddev
// (/(N-1)) is wrong here: the "population" is all visible bars in scope,
// not a sample drawn from a larger set.
function populationStdDev(values: number[]): number {
  if (values.length === 0) return 0;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance =
    values.reduce((s, v) => s + (v - mean) * (v - mean), 0) / values.length;
  return Math.sqrt(variance);
}

// Bar-number gap detection. Returns the count of "missing" stretches
// (consecutive runs of skipped bar numbers). Surfaces in the title for
// both themes — operators noticing gaps without explanation is bad.
function countGaps(bars: PortfolioBar[]): number {
  let n = 0;
  for (let i = 0; i < bars.length - 1; i++) {
    if (bars[i + 1].bar - bars[i].bar > 1) n++;
  }
  return n;
}

// ── Container ───────────────────────────────────────────────────────────────

export default function MatrixContainer({
  bars,
  symbolsOrdered,
  stoppedAtBar,
  symStopsCount,
}: Props) {
  const [theme, setTheme] = useState<Theme>(() => {
    if (typeof window === "undefined") return "simple";
    const v = window.localStorage.getItem(STORAGE_KEYS.theme);
    return v === "advanced" ? "advanced" : "simple";
  });
  const [scope, setScope] = useState<Scope>(() => {
    if (typeof window === "undefined") return "ALL";
    const v = window.localStorage.getItem(STORAGE_KEYS.scope);
    return v === "1H" || v === "4H" ? v : "ALL";
  });

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(STORAGE_KEYS.theme, theme);
  }, [theme]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    window.localStorage.setItem(STORAGE_KEYS.scope, scope);
  }, [scope]);

  // Scope filter — only applies in Advanced. Latest-N-bars window.
  const scopedBars = useMemo(() => {
    if (theme !== "advanced") return bars;
    if (scope === "ALL") return bars;
    const n = scope === "1H" ? 12 : 48; // bars × 5 min
    return bars.slice(Math.max(0, bars.length - n));
  }, [bars, theme, scope]);

  const totalGaps = useMemo(() => countGaps(scopedBars), [scopedBars]);

  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 5,
        padding: "12px 16px",
        minHeight: 0,
        display: "flex",
        flexDirection: "column",
        fontFamily: FONT_MONO,
      }}
    >
      <MatrixHeader
        visibleCount={scopedBars.length}
        totalCount={bars.length}
        stoppedCount={symStopsCount}
        gapCount={totalGaps}
        theme={theme}
        onThemeChange={setTheme}
      />
      {theme === "advanced" && <ScopePills scope={scope} onChange={setScope} />}
      {theme === "simple" ? (
        <SimpleMatrix
          bars={bars}
          symbolsOrdered={symbolsOrdered}
          stoppedAtBar={stoppedAtBar}
        />
      ) : (
        <AdvancedMatrix
          bars={scopedBars}
          allBars={bars}
          symbolsOrdered={symbolsOrdered}
          stoppedAtBar={stoppedAtBar}
        />
      )}
    </div>
  );
}

// ── Header (title + theme toggle) ───────────────────────────────────────────

function MatrixHeader({
  visibleCount,
  totalCount,
  stoppedCount,
  gapCount,
  theme,
  onThemeChange,
}: {
  visibleCount: number;
  totalCount: number;
  stoppedCount: number;
  gapCount: number;
  theme: Theme;
  onThemeChange: (next: Theme) => void;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        marginBottom: 10,
      }}
    >
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          color: "var(--t3)",
          textTransform: "uppercase",
        }}
      >
        ROI Matrix
        <span
          style={{
            marginLeft: 10,
            color: "var(--t2)",
            fontWeight: 400,
            letterSpacing: "0.06em",
          }}
        >
          · {visibleCount} of {totalCount} bars
        </span>
        {gapCount > 0 && (
          <span
            style={{
              marginLeft: 6,
              color: "var(--amber)",
              fontWeight: 400,
              letterSpacing: "0.06em",
            }}
          >
            · {gapCount} gap{gapCount > 1 ? "s" : ""}
          </span>
        )}
        {stoppedCount > 0 && (
          <span
            style={{
              marginLeft: 6,
              color: "var(--t2)",
              fontWeight: 400,
              letterSpacing: "0.06em",
            }}
          >
            · {stoppedCount} stopped
          </span>
        )}
      </div>
      <ThemeToggle value={theme} onChange={onThemeChange} />
    </div>
  );
}

function ThemeToggle({
  value,
  onChange,
}: {
  value: Theme;
  onChange: (next: Theme) => void;
}) {
  const opts: { key: Theme; label: string }[] = [
    { key: "simple", label: "SIMPLE" },
    { key: "advanced", label: "ADV" },
  ];
  return (
    <div
      role="tablist"
      aria-label="Matrix theme"
      style={{
        display: "inline-flex",
        border: "1px solid rgba(255,255,255,0.08)",
        borderRadius: 3,
        overflow: "hidden",
        fontFamily: FONT_MONO,
      }}
    >
      {opts.map((opt, i) => {
        const active = opt.key === value;
        return (
          <button
            key={opt.key}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => onChange(opt.key)}
            style={{
              padding: "4px 10px",
              fontSize: 9,
              letterSpacing: "0.10em",
              background: active ? "rgba(0,200,150,0.10)" : "transparent",
              color: active ? PORTFOLIO_COLOR : "#52525b",
              border: 0,
              borderLeft: i === 0 ? "none" : "1px solid rgba(255,255,255,0.08)",
              cursor: "pointer",
              fontFamily: FONT_MONO,
              transition: "background 0.12s, color 0.12s",
            }}
            onMouseEnter={(e) => {
              if (!active) (e.currentTarget as HTMLButtonElement).style.color = "#a1a1aa";
            }}
            onMouseLeave={(e) => {
              if (!active) (e.currentTarget as HTMLButtonElement).style.color = "#52525b";
            }}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

// ── Scope pills (Advanced only) ─────────────────────────────────────────────

function ScopePills({
  scope,
  onChange,
}: {
  scope: Scope;
  onChange: (next: Scope) => void;
}) {
  const opts: Scope[] = ["1H", "4H", "ALL"];
  return (
    <div
      style={{
        display: "flex",
        gap: 4,
        padding: "4px 0 10px 0",
      }}
    >
      {opts.map((s) => {
        const active = scope === s;
        return (
          <button
            key={s}
            type="button"
            onClick={() => onChange(s)}
            style={{
              fontSize: 9.5,
              letterSpacing: "0.06em",
              padding: "3px 9px",
              borderRadius: 3,
              background: active ? "rgba(0,200,150,0.10)" : "transparent",
              color: active ? PORTFOLIO_COLOR : "#71717a",
              border: `1px solid ${active ? "rgba(0,200,150,0.28)" : "rgba(255,255,255,0.08)"}`,
              cursor: "pointer",
              fontFamily: FONT_MONO,
            }}
          >
            {s}
          </button>
        );
      })}
    </div>
  );
}

// ── Simple matrix ───────────────────────────────────────────────────────────
// Verbatim port of the inline matrix that lived on page.tsx pre-extract.
// Italic on stopped symbols, oldest-at-top, no heatmap, no sparklines, no
// stats footer. Don't add anything here without an explicit ask.

function SimpleMatrix({
  bars,
  symbolsOrdered,
  stoppedAtBar,
}: {
  bars: PortfolioBar[];
  symbolsOrdered: string[];
  stoppedAtBar: Map<string, number>;
}) {
  const matrixThStyle: React.CSSProperties = {
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: "0.12em",
    color: "var(--t3)",
    textTransform: "uppercase",
    textAlign: "right",
    padding: "8px 10px",
    borderBottom: "1px solid var(--line)",
    whiteSpace: "nowrap",
  };
  const matrixTdStyle: React.CSSProperties = {
    fontSize: 11,
    padding: "5px 10px",
    textAlign: "right",
    borderBottom: "1px solid var(--line)",
    fontFamily: FONT_MONO,
    fontVariantNumeric: "tabular-nums",
  };

  return (
    <div
      style={{
        overflow: "auto",
        maxHeight: 420,
        border: "1px solid var(--line)",
        borderRadius: 4,
      }}
    >
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontFamily: FONT_MONO,
        }}
      >
        <thead
          style={{
            position: "sticky",
            top: 0,
            background: "var(--bg2)",
            zIndex: 1,
          }}
        >
          <tr>
            <th style={matrixThStyle}>Bar</th>
            {/* Time column gets a thick right border that mirrors the
                PORTFOLIO column's left border — gives the index columns
                (Bar/Time) a visual gutter from the symbol grid. */}
            <th
              style={{
                ...matrixThStyle,
                borderRight: "2px solid var(--line2)",
              }}
            >
              Time
            </th>
            {symbolsOrdered.map((sym) => (
              <th key={sym} style={matrixThStyle}>
                {sym.replace("-USDT", "")}
              </th>
            ))}
            <th
              style={{
                ...matrixThStyle,
                borderLeft: "2px solid var(--line2)",
                color: PORTFOLIO_COLOR,
              }}
            >
              Portfolio
            </th>
          </tr>
        </thead>
        <tbody>
          {bars.map((b) => (
            <tr key={b.bar}>
              <td style={{ ...matrixTdStyle, color: "var(--t2)" }}>{b.bar}</td>
              <td
                style={{
                  ...matrixTdStyle,
                  color: "var(--t2)",
                  borderRight: "2px solid var(--line2)",
                }}
              >
                {fmtTimeShort(b.ts)}
              </td>
              {symbolsOrdered.map((sym) => {
                const v = b.sym_returns[sym];
                const stopBar = stoppedAtBar.get(sym);
                const isStopped = stopBar !== undefined && b.bar >= stopBar;
                const pct = v !== undefined ? v * 100 : null;
                return (
                  <td
                    key={sym}
                    style={{
                      ...matrixTdStyle,
                      color:
                        pct === null
                          ? "var(--t3)"
                          : isStopped
                          ? "var(--t3)"
                          : pct >= 0
                          ? "var(--green)"
                          : "var(--red)",
                      fontStyle: isStopped ? "italic" : undefined,
                    }}
                  >
                    {pct === null ? "—" : fmtPct(pct, 2)}
                  </td>
                );
              })}
              <td
                style={{
                  ...matrixTdStyle,
                  borderLeft: "2px solid var(--line2)",
                  fontWeight: 700,
                  color: b.incr >= 0 ? "var(--green)" : "var(--red)",
                }}
              >
                {fmtPct(b.incr * 100)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Advanced matrix ─────────────────────────────────────────────────────────

function AdvancedMatrix({
  bars,
  allBars: _allBars,
  symbolsOrdered,
  stoppedAtBar,
}: {
  bars: PortfolioBar[];
  allBars: PortfolioBar[];
  symbolsOrdered: string[];
  stoppedAtBar: Map<string, number>;
}) {
  // Latest-first. Sparkline data still walks the chronological array
  // separately so the time-series chart goes left-to-right oldest→newest
  // regardless of table direction. Bloomberg convention.
  const reversedBars = useMemo(() => [...bars].reverse(), [bars]);

  // Per-column ranges + sparkline data. Memoize on `bars` so they don't
  // recompute on every keystroke / unrelated rerender.
  const columnStats = useMemo(() => {
    const out: Record<
      string,
      {
        values: number[];          // chronological order, % units
        absMax: number;
        high: number;
        low: number;
        sigma: number;
        isStopped: boolean;
      }
    > = {};
    for (const sym of symbolsOrdered) {
      const stopBar = stoppedAtBar.get(sym);
      const isStopped = stopBar !== undefined;
      const values: number[] = [];
      for (const b of bars) {
        const v = b.sym_returns[sym];
        if (v !== undefined) values.push(v * 100);
      }
      const absMax = values.length
        ? Math.max(...values.map((v) => Math.abs(v)))
        : 0;
      const high = values.length ? Math.max(...values) : 0;
      const low = values.length ? Math.min(...values) : 0;
      out[sym] = {
        values,
        absMax,
        high,
        low,
        sigma: populationStdDev(values),
        isStopped,
      };
    }
    // Portfolio column
    const portValues = bars.map((b) => b.incr * 100);
    out["__portfolio__"] = {
      values: portValues,
      absMax: portValues.length
        ? Math.max(...portValues.map((v) => Math.abs(v)))
        : 0,
      high: portValues.length ? Math.max(...portValues) : 0,
      low: portValues.length ? Math.min(...portValues) : 0,
      sigma: populationStdDev(portValues),
      isStopped: false,
    };
    return out;
  }, [bars, symbolsOrdered, stoppedAtBar]);

  // Column widths — tighter than Simple to fit at ~880px main content
  // when the session-logs panel is open. BAR/TIME 60px, symbol 80px,
  // PORTFOLIO 100px. Total = 60+60+8*80+100 = 860 for an 8-symbol day.
  const COL_W = {
    bar: 60,
    time: 60,
    sym: 80,
    portfolio: 100,
  };

  const portTint = "rgba(0,200,150,0.04)";
  const portDivider = "rgba(0,200,150,0.20)";

  // Shared cell padding tokens so all sections (header, sparkline, tbody,
  // tfoot) line up on the column grid without drift.
  const CELL_PAD_X = 10;

  return (
    <div
      style={{
        overflow: "auto",
        maxHeight: 460,
        border: "1px solid var(--line)",
        borderRadius: 4,
        background: "#080809",
      }}
    >
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          tableLayout: "fixed",
          fontFamily: FONT_MONO,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        <colgroup>
          <col style={{ width: COL_W.bar }} />
          <col style={{ width: COL_W.time }} />
          {symbolsOrdered.map((sym) => (
            <col key={sym} style={{ width: COL_W.sym }} />
          ))}
          <col style={{ width: COL_W.portfolio }} />
        </colgroup>
        {/* Sticky head: column labels + sparkline strip */}
        <thead
          style={{
            position: "sticky",
            top: 0,
            background: "#0a0a0b",
            zIndex: 2,
          }}
        >
          <tr>
            <th style={advHeadCellStyle({ align: "left", padX: CELL_PAD_X })}>
              BAR
            </th>
            <th style={advHeadCellStyle({ align: "left", padX: CELL_PAD_X })}>
              TIME
            </th>
            {symbolsOrdered.map((sym) => {
              const stats = columnStats[sym];
              const display = sym.replace("-USDT", "");
              return (
                <th
                  key={sym}
                  style={advHeadCellStyle({
                    align: "right",
                    padX: CELL_PAD_X,
                    color: stats?.isStopped ? "#a07474" : undefined,
                  })}
                >
                  {stats?.isStopped && <StopChip />}
                  {display}
                </th>
              );
            })}
            <th
              style={{
                ...advHeadCellStyle({ align: "right", padX: CELL_PAD_X }),
                color: PORTFOLIO_COLOR,
                background: portTint,
                position: "relative",
                boxShadow: `inset 1px 0 0 ${portDivider}`,
              }}
            >
              PORTFOLIO
            </th>
          </tr>
          {/* Sparkline strip */}
          <tr>
            <td style={sparkCellStyle()} colSpan={2} />
            {symbolsOrdered.map((sym) => {
              const stats = columnStats[sym];
              return (
                <td key={sym} style={sparkCellStyle()}>
                  <Sparkline values={stats?.values ?? []} stopped={stats?.isStopped ?? false} />
                </td>
              );
            })}
            <td
              style={{
                ...sparkCellStyle(),
                background: portTint,
                boxShadow: `inset 1px 0 0 ${portDivider}`,
              }}
            >
              <Sparkline
                values={columnStats["__portfolio__"]?.values ?? []}
                stopped={false}
                emphasis
              />
            </td>
          </tr>
        </thead>
        {/* Body: latest-first */}
        <tbody>
          {reversedBars.map((b) => (
            <tr key={b.bar}>
              <td style={advBodyCellStyle({ align: "left", color: "#a1a1aa", fontSize: 10.5, padX: CELL_PAD_X })}>
                {b.bar}
              </td>
              <td style={advBodyCellStyle({ align: "left", color: "#52525b", fontSize: 10, padX: CELL_PAD_X })}>
                {fmtTimeShort(b.ts)}
              </td>
              {symbolsOrdered.map((sym) => {
                const v = b.sym_returns[sym];
                const stats = columnStats[sym];
                const stopBar = stoppedAtBar.get(sym);
                const isStopped = stopBar !== undefined && b.bar >= stopBar;
                const pct = v !== undefined ? v * 100 : null;
                const tint =
                  pct === null
                    ? "transparent"
                    : cellTint(pct, stats?.absMax ?? 0, 0.18);
                let color: string;
                if (pct === null) color = "#52525b";
                else if (isStopped) color = "#a07474";
                else if (pct < 0) color = "#ef4444";
                else if (pct > 0) color = PORTFOLIO_COLOR;
                else color = "#71717a";
                return (
                  <td
                    key={sym}
                    style={{
                      ...advBodyCellStyle({ align: "right", color, padX: CELL_PAD_X }),
                      background: tint,
                      opacity: isStopped ? 0.65 : 1,
                    }}
                  >
                    {pct === null ? "—" : fmtPct(pct, 2)}
                  </td>
                );
              })}
              {/* Portfolio cell: own column tint + heatmap composited on top
                  with a lower cap (0.12) so the two layers don't fight. */}
              {(() => {
                const portStats = columnStats["__portfolio__"];
                const pct = b.incr * 100;
                const tint = cellTint(pct, portStats?.absMax ?? 0, 0.12);
                const color = pct < 0 ? "#ef4444" : pct > 0 ? PORTFOLIO_COLOR : "#d4d4d8";
                return (
                  <td
                    style={{
                      ...advBodyCellStyle({ align: "right", color, padX: CELL_PAD_X, fontWeight: 500 }),
                      background: tint,
                      // Composite over the column tint via a layered
                      // gradient so the heatmap reads on top of the
                      // faint green wash without overriding it.
                      backgroundImage: `linear-gradient(${portTint}, ${portTint})`,
                      boxShadow: `inset 1px 0 0 ${portDivider}`,
                    }}
                  >
                    {fmtPct(pct, 2)}
                  </td>
                );
              })()}
            </tr>
          ))}
        </tbody>
        {/* Stats footer */}
        <tfoot>
          <tr>
            <td
              colSpan={2}
              style={advFootLabelStyle({ align: "left", padX: CELL_PAD_X })}
            >
              SUMMARY
            </td>
            {symbolsOrdered.map((sym) => (
              <td key={sym} style={advFootLabelStyle({ align: "right", padX: CELL_PAD_X })}>
                HIGH / LOW / σ
              </td>
            ))}
            <td
              style={{
                ...advFootLabelStyle({ align: "right", padX: CELL_PAD_X }),
                background: portTint,
                boxShadow: `inset 1px 0 0 ${portDivider}`,
              }}
            >
              HIGH / LOW / σ
            </td>
          </tr>
          <tr>
            <td
              colSpan={2}
              style={advFootValueStyle({ align: "left", padX: CELL_PAD_X })}
            >
              {bars.length} BARS
            </td>
            {symbolsOrdered.map((sym) => {
              const s = columnStats[sym];
              if (!s)
                return (
                  <td key={sym} style={advFootValueStyle({ align: "right", padX: CELL_PAD_X })}>
                    —
                  </td>
                );
              return (
                <td key={sym} style={advFootValueStyle({ align: "right", padX: CELL_PAD_X })}>
                  <b style={{ color: "#a1a1aa", fontWeight: 400 }}>
                    {fmtPctPlain(s.high, 2)}
                  </b>{" "}
                  / {fmtPctPlain(s.low, 2)} / {s.sigma.toFixed(2)}
                </td>
              );
            })}
            {(() => {
              const s = columnStats["__portfolio__"];
              if (!s)
                return (
                  <td
                    style={{
                      ...advFootValueStyle({ align: "right", padX: CELL_PAD_X }),
                      background: portTint,
                      boxShadow: `inset 1px 0 0 ${portDivider}`,
                    }}
                  >
                    —
                  </td>
                );
              return (
                <td
                  style={{
                    ...advFootValueStyle({ align: "right", padX: CELL_PAD_X }),
                    background: portTint,
                    boxShadow: `inset 1px 0 0 ${portDivider}`,
                    color: "#71717a",
                  }}
                >
                  <b style={{ color: "#d4d4d8", fontWeight: 400 }}>
                    {fmtPctPlain(s.high, 2)}
                  </b>{" "}
                  / {fmtPctPlain(s.low, 2)} / {s.sigma.toFixed(2)}
                </td>
              );
            })()}
          </tr>
        </tfoot>
      </table>
    </div>
  );
}

// ── Advanced cell-style factories ───────────────────────────────────────────

function advHeadCellStyle({
  align,
  padX,
  color,
}: {
  align: "left" | "right";
  padX: number;
  color?: string;
}): React.CSSProperties {
  return {
    fontSize: 9,
    letterSpacing: "0.12em",
    color: color ?? "#71717a",
    fontWeight: 400,
    padding: `9px ${padX}px 6px ${padX}px`,
    textAlign: align,
    textTransform: "uppercase",
    background: "#0a0a0b",
    borderBottom: "1px solid rgba(255,255,255,0.05)",
    whiteSpace: "nowrap",
  };
}

function sparkCellStyle(): React.CSSProperties {
  return {
    padding: "0 8px 8px 8px",
    background: "#0a0a0b",
    borderBottom: "1px solid rgba(255,255,255,0.06)",
    height: 14,
  };
}

function advBodyCellStyle({
  align,
  color,
  padX,
  fontSize = 11,
  fontWeight = 400,
}: {
  align: "left" | "right";
  color: string;
  padX: number;
  fontSize?: number;
  fontWeight?: number;
}): React.CSSProperties {
  return {
    fontSize,
    fontWeight,
    color,
    padding: `5px ${padX}px`,
    textAlign: align,
    borderBottom: "1px solid rgba(255,255,255,0.025)",
    fontVariantNumeric: "tabular-nums",
    whiteSpace: "nowrap",
  };
}

function advFootLabelStyle({
  align,
  padX,
}: {
  align: "left" | "right";
  padX: number;
}): React.CSSProperties {
  return {
    padding: `6px ${padX}px 1px`,
    fontSize: 8,
    letterSpacing: "0.14em",
    color: "#42424a",
    textAlign: align,
    background: "#0a0a0b",
    fontVariantNumeric: "tabular-nums",
  };
}

function advFootValueStyle({
  align,
  padX,
}: {
  align: "left" | "right";
  padX: number;
}): React.CSSProperties {
  return {
    padding: `8px ${padX}px 9px`,
    fontSize: 9.5,
    letterSpacing: "0.06em",
    color: "#52525b",
    textAlign: align,
    background: "#0a0a0b",
    borderTop: "1px solid rgba(255,255,255,0.06)",
    fontVariantNumeric: "tabular-nums",
  };
}

// ── Sub-components ──────────────────────────────────────────────────────────

function StopChip() {
  return (
    <span
      style={{
        display: "inline-block",
        fontSize: 7.5,
        letterSpacing: "0.10em",
        padding: "1px 4px",
        background: "rgba(239,68,68,0.10)",
        color: "#a07474",
        borderRadius: 2,
        marginRight: 5,
        verticalAlign: "middle",
      }}
    >
      STOP
    </span>
  );
}

function Sparkline({
  values,
  stopped,
  emphasis = false,
}: {
  values: number[];
  stopped: boolean;
  emphasis?: boolean;
}) {
  // Stopped: draw a flat horizontal at y-mid in dim red. Reinforces
  // "no movement" without parsing the repeated post-clamp numbers.
  // Emphasis (PORTFOLIO): always green palette + slightly thicker stroke.
  const path = stopped ? "M0,7 L80,7" : sparkPath(values);
  const stroke = emphasis
    ? "#5e8a73"
    : sparkColor(values, stopped);
  return (
    <svg
      width="100%"
      height={14}
      viewBox="0 0 80 14"
      preserveAspectRatio="none"
      style={{ display: "block", opacity: 0.85 }}
      aria-hidden
    >
      <path
        d={path}
        fill="none"
        stroke={stroke}
        strokeWidth={emphasis ? 1.2 : 1}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

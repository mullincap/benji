"use client";

/**
 * frontend/app/manager/(protected)/portfolios/[date]/page.tsx
 * ============================================================
 * Per-bar detail for one trading session. Renders:
 *   - Header with summary line + back link
 *   - 6 KPI cards
 *   - Cumulative ROI chart (one line per symbol + thicker portfolio line)
 *   - 5-min ROI matrix table (symbols across, bars down, Portfolio col last)
 *
 * Polls /api/manager/portfolios/{date} every 30s while session.status ===
 * "active" to surface new bars in real time. When status flips to "closed"
 * the polling stops automatically.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useParams, useSearchParams, useRouter } from "next/navigation";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
  type ChartOptions,
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
);

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";
const POLL_MS = 30_000;

const PORTFOLIO_COLOR = "#00c896";
const SYMBOL_COLORS: Record<string, string> = {
  "BTC-USDT":  "#f7931a",
  "ETH-USDT":  "#627eea",
  "SOL-USDT":  "#00d18c",
  "DOGE-USDT": "#c3a634",
  "AVAX-USDT": "#e84142",
  "LINK-USDT": "#2a5ada",
};
const FALLBACK_PALETTE = ["#9945ff", "#f0a500", "#ed93b1", "#bb86fc", "#ff7a59", "#5fb6ff"];

function colorFor(inst: string, idx: number): string {
  return SYMBOL_COLORS[inst] ?? FALLBACK_PALETTE[idx % FALLBACK_PALETTE.length];
}

// ─── Types ──────────────────────────────────────────────────────────────────

interface PortfolioMeta {
  date: string;
  allocation_id: string | null;    // null for master rows (pre-multi-tenant)
  exchange: string | null;
  strategy_label: string | null;
  status: "active" | "closed";
  session_start_utc: string | null;
  exit_time_utc: string | null;
  exit_reason: string | null;
  symbols: string[];
  entered: string[];
  eff_lev: number;
  lev_int: number;
}

// When the backend detects multiple allocations on the requested date and no
// allocation_id query param was provided, it returns 400 with a picker payload.
// The detail page renders a chooser so the user can redirect to the correct URL.
interface AmbiguityDetail {
  message: string;
  available: Array<{
    allocation_id: string | null;
    exchange: string | null;
    strategy_label: string | null;
  }>;
}

interface PortfolioBar {
  bar: number;
  ts: string;
  incr: number;
  peak: number;
  sym_returns: Record<string, number>;
  stopped: string[];
}

interface PortfolioDetail {
  meta: PortfolioMeta;
  bars: PortfolioBar[];
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}%`;
}

function fmtTime(ts: string | null | undefined): string {
  if (!ts) return "—";
  // "2026-04-17 06:35:02" → "06:35"
  const part = ts.split(" ")[1] ?? "";
  return part.slice(0, 5);
}

function exitBadge(reason: string | null, status: string) {
  if (status === "active") {
    return (
      <span
        style={{
          display: "inline-block",
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.06em",
          padding: "2px 6px",
          borderRadius: 3,
          background: "var(--green-dim)",
          color: "var(--green)",
          fontFamily: FONT_MONO,
        }}
      >
        ACTIVE
      </span>
    );
  }
  if (!reason) return <span style={{ color: "var(--t3)" }}>—</span>;
  const map: Record<string, { bg: string; fg: string }> = {
    port_sl:       { bg: "var(--red-dim)",   fg: "var(--red)" },
    port_tsl:      { bg: "var(--red-dim)",   fg: "var(--red)" },
    early_fill:    { bg: "var(--green-dim)", fg: "var(--green)" },
    session_close: { bg: "var(--bg3)",       fg: "var(--t1)" },
    sym_stop:      { bg: "var(--amber-dim)", fg: "var(--amber)" },
  };
  const s = map[reason] || { bg: "var(--bg3)", fg: "var(--t2)" };
  return (
    <span
      style={{
        display: "inline-block",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.06em",
        padding: "2px 6px",
        borderRadius: 3,
        background: s.bg,
        color: s.fg,
        fontFamily: FONT_MONO,
      }}
    >
      {reason.replace(/_/g, " ").toUpperCase()}
    </span>
  );
}

function LivePulse() {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        color: "var(--green)",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.12em",
        fontFamily: FONT_MONO,
      }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: "50%",
          background: "var(--green)",
          animation: "portfolio-pulse 1.6s ease-in-out infinite",
        }}
      />
      LIVE
    </span>
  );
}

// ─── KPI Card ───────────────────────────────────────────────────────────────

function KpiCard({
  label,
  value,
  color,
  subtitle,
}: {
  label: string;
  value: React.ReactNode;
  color?: string;
  subtitle?: string;
}) {
  return (
    <div
      style={{
        flex: 1,
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 5,
        padding: "14px 16px",
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          color: "var(--t3)",
          textTransform: "uppercase",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 18,
          fontWeight: 700,
          color: color || "var(--t0)",
          fontFamily: FONT_MONO,
        }}
      >
        {value}
      </div>
      {subtitle && (
        <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
          {subtitle}
        </div>
      )}
    </div>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function PortfolioDetailPage() {
  const params = useParams<{ date: string }>();
  const searchParams = useSearchParams();
  const router = useRouter();
  const date = params.date;
  const allocationId = searchParams.get("allocation_id");

  const [data, setData] = useState<PortfolioDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [ambiguity, setAmbiguity] = useState<AmbiguityDetail | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const matrixRef = useRef<HTMLDivElement>(null);
  const wasAtBottomRef = useRef(true);

  const load = useCallback(async () => {
    try {
      const url = allocationId
        ? `${API_BASE}/api/manager/portfolios/${date}?allocation_id=${allocationId}`
        : `${API_BASE}/api/manager/portfolios/${date}`;
      const r = await fetch(url, { credentials: "include" });
      if (r.status === 401) throw new Error("Session expired");
      if (r.status === 404) throw new Error("Portfolio not found for this date");
      if (r.status === 400) {
        // Multi-allocation ambiguity — surface the picker instead of an error.
        const body = await r.json().catch(() => null);
        const detail = body?.detail;
        if (detail && typeof detail === "object" && Array.isArray(detail.available)) {
          setAmbiguity(detail as AmbiguityDetail);
          setError(null);
          return;
        }
        throw new Error(typeof detail === "string" ? detail : `HTTP 400`);
      }
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const json = (await r.json()) as PortfolioDetail;
      // Capture scroll position before state update so we can restore it.
      const el = matrixRef.current;
      if (el) {
        wasAtBottomRef.current =
          el.scrollHeight - el.scrollTop - el.clientHeight < 24;
      }
      setAmbiguity(null);
      setData(json);
      setLastUpdated(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [date, allocationId]);

  // Initial load + polling for active sessions
  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (!data) return;
    if (data.meta.status !== "active") return;
    const id = setInterval(load, POLL_MS);
    return () => clearInterval(id);
  }, [data, load]);

  // Auto-scroll matrix to bottom when new bars arrive (only if user was already at bottom)
  useEffect(() => {
    if (!data) return;
    const el = matrixRef.current;
    if (!el) return;
    if (wasAtBottomRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [data]);

  const symbolsOrdered = useMemo(() => {
    if (!data) return [] as string[];
    // Order by entered first, then any extras present in symbols
    const known = new Set(data.meta.entered);
    const ordered: string[] = [...data.meta.entered];
    for (const s of data.meta.symbols) {
      if (!known.has(s)) ordered.push(s);
    }
    return ordered;
  }, [data]);

  const stoppedAtBar = useMemo(() => {
    // For each symbol, the bar number where it first appears in `stopped`.
    if (!data) return new Map<string, number>();
    const m = new Map<string, number>();
    for (const b of data.bars) {
      for (const s of b.stopped || []) {
        if (!m.has(s)) m.set(s, b.bar);
      }
    }
    return m;
  }, [data]);

  if (ambiguity) {
    // Multiple allocations on this date. Render a picker that redirects to
    // the disambiguated URL. Reached when the user lands on /portfolios/{date}
    // with no allocation_id query param AND ≥2 sessions exist for that date.
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--t1)", maxWidth: 520 }}>
        <div style={{ marginBottom: 12, color: "var(--t0)", fontSize: 13, fontWeight: 700 }}>
          Multiple allocations for {date}
        </div>
        <div style={{ marginBottom: 16, fontSize: 10, color: "var(--t2)", lineHeight: 1.6 }}>
          {ambiguity.message}
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {ambiguity.available.map((opt) => (
            <button
              key={opt.allocation_id ?? "master"}
              type="button"
              onClick={() => {
                const suffix = opt.allocation_id
                  ? `?allocation_id=${opt.allocation_id}`
                  : "";
                router.push(`/manager/portfolios/${date}${suffix}`);
              }}
              style={{
                background: "var(--bg2)",
                border: "1px solid var(--line)",
                borderRadius: 4,
                padding: "10px 14px",
                fontFamily: FONT_MONO,
                fontSize: 10,
                color: "var(--t1)",
                textAlign: "left",
                cursor: "pointer",
              }}
            >
              {opt.allocation_id
                ? `${opt.exchange} · ${opt.strategy_label}`
                : "Master (pre-multi-tenant)"}
            </button>
          ))}
        </div>
        <div style={{ marginTop: 18 }}>
          <Link href="/manager/portfolios" style={{ color: "var(--t2)", fontSize: 10 }}>
            ← BACK TO PORTFOLIOS
          </Link>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--red)" }}>
        Error: {error}
        <div style={{ marginTop: 14 }}>
          <Link href="/manager/portfolios" style={{ color: "var(--t2)", fontSize: 10 }}>
            ← BACK TO PORTFOLIOS
          </Link>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--t2)" }}>
        Loading portfolio…
      </div>
    );
  }

  const { meta, bars } = data;
  const live = meta.status === "active";
  const lastBar = bars[bars.length - 1];
  const final = lastBar ? lastBar.incr * 100 : 0;
  const peak = lastBar ? lastBar.peak * 100 : 0;
  const dd = bars.length
    ? Math.min(...bars.map((b) => b.incr - b.peak)) * 100
    : 0;
  const startTime = fmtTime(meta.session_start_utc);
  const endTime = live ? "LIVE" : fmtTime(meta.exit_time_utc);
  const symStopsCount = stoppedAtBar.size;
  const enteredCount = meta.entered.length;

  // Chart datasets: one per symbol + portfolio line
  const labels = bars.map((b) => fmtTime(b.ts));
  const symbolDatasets = symbolsOrdered.map((sym, idx) => {
    const stoppedBar = stoppedAtBar.get(sym);
    const data = bars.map((b) => {
      const v = b.sym_returns[sym];
      return v !== undefined ? v * 100 : null;
    });
    return {
      label: sym.replace("-USDT", ""),
      data,
      borderColor: colorFor(sym, idx),
      backgroundColor: colorFor(sym, idx),
      borderWidth: 1.5,
      pointRadius: 0,
      pointHoverRadius: 4,
      tension: 0.15,
      // Dash the segment after the stop bar.
      segment: stoppedBar !== undefined
        ? {
            borderDash: (ctx: { p1DataIndex: number }) => {
              const barNum = bars[ctx.p1DataIndex]?.bar ?? 0;
              return barNum >= stoppedBar ? [4, 4] : undefined;
            },
          }
        : undefined,
      _isSymbol: true,
    };
  });
  const portfolioDataset = {
    label: "Portfolio",
    data: bars.map((b) => b.incr * 100),
    borderColor: PORTFOLIO_COLOR,
    backgroundColor: PORTFOLIO_COLOR,
    borderWidth: 3,
    pointRadius: 0,
    pointHoverRadius: 5,
    tension: 0.15,
    _isSymbol: false,
  };

  const chartOptions: ChartOptions<"line"> = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    animation: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "#0e0e10",
        borderColor: "#2a2a2e",
        borderWidth: 1,
        titleColor: "#f0ede6",
        bodyColor: "#a09d96",
        titleFont: { family: "Space Mono", size: 10, weight: "bold" },
        bodyFont:  { family: "Space Mono", size: 10 },
        padding: 8,
        callbacks: {
          label: (ctx) => {
            const v = ctx.parsed.y;
            if (v === null || v === undefined) return `${ctx.dataset.label}: —`;
            return `${ctx.dataset.label}: ${v >= 0 ? "+" : ""}${v.toFixed(3)}%`;
          },
        },
      },
    },
    scales: {
      x: {
        ticks: {
          color: "#5a5754",
          font: { family: "Space Mono", size: 9 },
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 12,
        },
        grid: { color: "#1a1a1d" },
      },
      y: {
        ticks: {
          color: "#5a5754",
          font: { family: "Space Mono", size: 9 },
          callback: (val) => `${Number(val).toFixed(1)}%`,
        },
        grid: { color: "#1a1a1d" },
      },
    },
  };

  // Matrix shows every bar at 5-min resolution. The previous 30-min
  // subsample toggle was removed — users wanted full fidelity by default
  // and the table already scrolls inside a fixed-height container.
  const matrixBars = bars;

  return (
    <>
      <style jsx global>{`
        @keyframes portfolio-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%      { opacity: 0.35; transform: scale(0.85); }
        }
      `}</style>
      <div
        style={{
          padding: 20,
          display: "flex",
          flexDirection: "column",
          gap: 14,
          height: "100%",
          overflow: "auto",
          fontFamily: FONT_MONO,
        }}
      >
        {/* Back link + header */}
        <div>
          <Link
            href="/manager/portfolios"
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
              textDecoration: "none",
            }}
          >
            ← Back to Portfolios
          </Link>
          <div
            style={{
              display: "flex",
              alignItems: "baseline",
              gap: 14,
              marginTop: 6,
            }}
          >
            <span
              style={{
                fontSize: 18,
                fontWeight: 700,
                color: "var(--t0)",
              }}
            >
              {meta.date}
            </span>
            <span style={{ fontSize: 11, color: "var(--t2)" }}>
              {startTime} → {live ? "" : endTime}
              {live && <LivePulse />}
              {" · "}
              {bars.length} bars
              {meta.exit_reason && ` · ${meta.exit_reason.replace(/_/g, " ")} exit`}
              {" · "}
              {enteredCount} symbols active
              {symStopsCount > 0 && ` (${symStopsCount} stopped)`}
            </span>
          </div>
          {lastUpdated && live && (
            <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
              LAST UPDATED · {lastUpdated.toISOString().slice(11, 19)} UTC
            </div>
          )}
        </div>

        {/* KPI cards */}
        <div style={{ display: "flex", gap: 10 }}>
          <KpiCard
            label="Portfolio ROI"
            value={fmtPct(final)}
            color={final >= 0 ? "var(--green)" : "var(--red)"}
          />
          <KpiCard label="Peak ROI" value={fmtPct(peak)} />
          <KpiCard
            label="Max Drawdown"
            value={fmtPct(dd)}
            color={dd <= -2 ? "var(--red)" : undefined}
          />
          <KpiCard
            label="Symbols"
            value={`${enteredCount} / ${meta.symbols.length}`}
            subtitle={symStopsCount > 0 ? `${symStopsCount} stopped` : undefined}
          />
          <KpiCard label="Eff Leverage" value={`${meta.eff_lev.toFixed(2)}x`} />
          <KpiCard label="Exit" value={exitBadge(meta.exit_reason, meta.status)} />
        </div>

        {/* Chart */}
        <div
          style={{
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 10,
              gap: 16,
              flexWrap: "wrap",
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
              Cumulative ROI by Symbol
            </div>
            {/* Custom legend */}
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 12,
                fontSize: 10,
                color: "var(--t1)",
              }}
            >
              <LegendChip label="Portfolio" color={PORTFOLIO_COLOR} thick />
              {symbolsOrdered.map((sym, idx) => (
                <LegendChip
                  key={sym}
                  label={sym.replace("-USDT", "")}
                  color={colorFor(sym, idx)}
                  dashed={stoppedAtBar.has(sym)}
                />
              ))}
            </div>
          </div>
          <div style={{ height: 320 }}>
            <Line
              data={{ labels, datasets: [...symbolDatasets, portfolioDataset] }}
              options={chartOptions}
            />
          </div>
        </div>

        {/* Matrix */}
        <div
          style={{
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            minHeight: 0,
            display: "flex",
            flexDirection: "column",
          }}
        >
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
              5-min ROI Matrix
              <span
                style={{
                  marginLeft: 10,
                  color: "var(--t2)",
                  fontWeight: 400,
                  letterSpacing: "0.06em",
                }}
              >
                · {matrixBars.length} of {bars.length} bars
              </span>
            </div>
          </div>
          <div
            ref={matrixRef}
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
                  <th style={matrixThStyle}>Time</th>
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
                {matrixBars.map((b) => (
                  <tr key={b.bar}>
                    <td style={{ ...matrixTdStyle, color: "var(--t2)" }}>{b.bar}</td>
                    <td style={{ ...matrixTdStyle, color: "var(--t2)" }}>
                      {fmtTime(b.ts)}
                    </td>
                    {symbolsOrdered.map((sym) => {
                      const v = b.sym_returns[sym];
                      const stopBar = stoppedAtBar.get(sym);
                      const isStopped =
                        stopBar !== undefined && b.bar >= stopBar;
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
        </div>
      </div>
    </>
  );
}

// ─── Legend chip ────────────────────────────────────────────────────────────

function LegendChip({
  label,
  color,
  thick,
  dashed,
}: {
  label: string;
  color: string;
  thick?: boolean;
  dashed?: boolean;
}) {
  return (
    <div style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
      <span
        style={{
          display: "inline-block",
          width: 14,
          height: thick ? 4 : 2,
          background: dashed
            ? `repeating-linear-gradient(to right, ${color} 0 4px, transparent 4px 7px)`
            : color,
          borderRadius: 1,
        }}
      />
      <span
        style={{
          color: "var(--t1)",
          fontSize: 10,
          fontWeight: thick ? 700 : 400,
        }}
      >
        {label}
      </span>
    </div>
  );
}

// ─── Matrix cell styles ─────────────────────────────────────────────────────

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
  background: "var(--bg2)",
};

const matrixTdStyle: React.CSSProperties = {
  fontSize: 11,
  color: "var(--t1)",
  padding: "6px 10px",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
  textAlign: "right",
  fontFamily: FONT_MONO,
};

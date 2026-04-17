"use client";

/**
 * frontend/app/manager/(protected)/portfolios/page.tsx
 * =====================================================
 * List of all live-trader portfolio sessions, one per traded day.
 * Reads from GET /api/manager/portfolios. Click a row to drill into the
 * per-bar detail view.
 *
 * Active sessions sort to the top by date (today is always largest) and
 * render with a pulsing green LIVE indicator.
 */

import { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";

interface PortfolioSummary {
  date: string;
  status: "active" | "closed";
  session_start_utc: string | null;
  exit_time_utc: string | null;
  exit_reason: string | null;
  eff_lev: number;
  lev_int: number;
  symbols: string[];
  entered: string[];
  bars_count: number;
  final_incr: number;
  peak: number;
  max_dd_from_peak: number;
  sym_stops: string[];
}

const thStyle: React.CSSProperties = {
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: "0.12em",
  color: "var(--t3)",
  textTransform: "uppercase",
  textAlign: "left",
  padding: "4px 8px 6px 0",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
};

const tdStyle: React.CSSProperties = {
  fontSize: 11,
  color: "var(--t1)",
  padding: "8px 8px 8px 0",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
  fontFamily: FONT_MONO,
};

const sectionLabel: React.CSSProperties = {
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: "0.12em",
  color: "var(--t3)",
  textTransform: "uppercase",
  marginBottom: 10,
};

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}%`;
}

function exitBadge(reason: string | null) {
  if (!reason) return <span style={{ color: "var(--t3)" }}>—</span>;
  const map: Record<string, { bg: string; fg: string }> = {
    port_sl:       { bg: "var(--red-dim)",   fg: "var(--red)" },
    port_tsl:      { bg: "var(--red-dim)",   fg: "var(--red)" },
    early_fill:    { bg: "var(--green-dim)", fg: "var(--green)" },
    session_close: { bg: "var(--bg3)",       fg: "var(--t1)" },
    sym_stop:      { bg: "var(--amber-dim)", fg: "var(--amber)" },
  };
  const s = map[reason] || { bg: "var(--bg3)", fg: "var(--t2)" };
  const label = reason.replace(/_/g, " ").toUpperCase();
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
      {label}
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
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.12em",
        color: "var(--green)",
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

export default function PortfoliosListPage() {
  const router = useRouter();
  const [data, setData] = useState<PortfolioSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    fetch(`${API_BASE}/api/manager/portfolios`, { credentials: "include" })
      .then((r) => {
        if (r.status === 401) throw new Error("Session expired");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => setData(d.portfolios || []))
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  if (error) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--red)" }}>
        Error: {error}
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--t2)" }}>
        Loading portfolios…
      </div>
    );
  }

  return (
    <>
      {/* CSS keyframes for the live pulse. Inlined here so the indicator is
          self-contained — no need to touch globals.css. */}
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
        }}
      >
        <div
          style={{
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            overflow: "auto",
            flex: 1,
            minHeight: 0,
          }}
        >
          <div style={sectionLabel}>Portfolios</div>
          {data.length === 0 ? (
            <div style={{ fontSize: 11, color: "var(--t3)" }}>
              No portfolios yet. Once the trader runs and enters positions,
              the per-bar timeline will appear here.
            </div>
          ) : (
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontFamily: FONT_MONO,
              }}
            >
              <thead>
                <tr>
                  {[
                    "Date",
                    "Symbols",
                    "Lev",
                    "Portfolio ROI",
                    "Peak",
                    "Max DD",
                    "Bars",
                    "Exit",
                    "Status",
                  ].map((h) => (
                    <th key={h} style={thStyle}>
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map((p) => {
                  const final = p.final_incr * 100;
                  const peakPct = p.peak * 100;
                  const ddPct = p.max_dd_from_peak * 100;
                  const live = p.status === "active";
                  const enteredCt = p.entered.length;
                  const symCt = p.symbols.length;
                  const partial = enteredCt < symCt;
                  return (
                    <tr
                      key={p.date}
                      onClick={() =>
                        router.push(`/manager/portfolios/${p.date}`)
                      }
                      style={{ cursor: "pointer" }}
                      onMouseEnter={(e) => {
                        e.currentTarget.style.background = "var(--bg3)";
                      }}
                      onMouseLeave={(e) => {
                        e.currentTarget.style.background = "";
                      }}
                    >
                      <td style={{ ...tdStyle, color: "var(--t0)" }}>
                        {p.date}
                      </td>
                      <td
                        style={{
                          ...tdStyle,
                          color: partial ? "var(--amber)" : "var(--t1)",
                        }}
                      >
                        {enteredCt} / {symCt}
                      </td>
                      <td style={tdStyle}>{p.eff_lev?.toFixed(2)}x</td>
                      <td
                        style={{
                          ...tdStyle,
                          color: final >= 0 ? "var(--green)" : "var(--red)",
                          fontWeight: 700,
                        }}
                      >
                        {fmtPct(final)}
                      </td>
                      <td style={{ ...tdStyle, color: "var(--t1)" }}>
                        {fmtPct(peakPct)}
                      </td>
                      <td
                        style={{
                          ...tdStyle,
                          color: ddPct < -2 ? "var(--red)" : "var(--t1)",
                        }}
                      >
                        {fmtPct(ddPct)}
                      </td>
                      <td style={tdStyle}>{p.bars_count}</td>
                      <td style={tdStyle}>
                        {live ? (
                          <span style={{ color: "var(--t3)" }}>—</span>
                        ) : (
                          exitBadge(p.exit_reason)
                        )}
                      </td>
                      <td style={tdStyle}>
                        {live ? <LivePulse /> : (
                          <span style={{ color: "var(--t3)" }}>—</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </>
  );
}

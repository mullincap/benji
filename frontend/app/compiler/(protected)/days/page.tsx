"use client";

/**
 * frontend/app/compiler/(protected)/days/page.tsx
 * ================================================
 * Recent Days list — quick-scan view of the last 10 days, each row
 * clickable to drill into the daily detail page.
 *
 * Sits between the Coverage Map (whole-window heatmap) and the per-day
 * detail (/compiler/days/[date]) as a fast nav surface for "show me
 * yesterday at a glance."
 *
 * Uses the existing GET /api/compiler/coverage?days=10 endpoint — no
 * new backend.
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { TableSkeleton } from "../../../components/Skeleton";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Response shapes (subset — match coverage endpoint) ─────────────────────

type EndpointCoverage = {
  close: number;
  volume: number;
  open_interest: number;
  funding_rate: number;
  long_short_ratio: number;
  market_cap_usd: number;
};

type CoverageDay = {
  date: string;
  symbols_complete: number;
  symbols_partial: number;
  symbols_with_data: number;
  symbols_missing: number;
  expected_symbols: number;
  has_job_truth: boolean;
  completeness_pct: number;
  endpoints: EndpointCoverage;
};

type CoverageResponse = {
  source_id: number;
  lookback_days: number;
  total_active_symbols: number;
  expected_today: number;
  days_returned: number;
  days: CoverageDay[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; data: CoverageResponse };

// ─── Helpers ────────────────────────────────────────────────────────────────

function statusFor(day: CoverageDay): "complete" | "partial" | "missing" {
  if (day.completeness_pct >= 95) return "complete";
  if (day.completeness_pct >= 5) return "partial";
  return "missing";
}

function relativeDay(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00Z");
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  const diffMs = today.getTime() - d.getTime();
  const diffDays = Math.round(diffMs / 86400000);
  if (diffDays === 0) return "today";
  if (diffDays === 1) return "yesterday";
  if (diffDays < 7) return `${diffDays}d ago`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
  return `${Math.floor(diffDays / 30)}mo ago`;
}

function dayOfWeek(dateStr: string): string {
  const d = new Date(dateStr + "T00:00:00Z");
  return d.toLocaleDateString("en-US", { weekday: "short", timeZone: "UTC" });
}

// ─── Sub-components ─────────────────────────────────────────────────────────

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase", marginBottom: 8,
    }}>
      {children}
    </div>
  );
}

function StatusBadge({ status }: { status: "complete" | "partial" | "missing" }) {
  const map = {
    complete: { bg: "var(--green-dim)", color: "var(--green)", label: "COMPLETE" },
    partial:  { bg: "var(--amber-dim)", color: "var(--amber)", label: "PARTIAL" },
    missing:  { bg: "var(--red-dim)",   color: "var(--red)",   label: "MISSING" },
  } as const;
  const s = map[status];
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      padding: "2px 6px",
      borderRadius: 3,
      background: s.bg,
      color: s.color,
      letterSpacing: "0.06em",
    }}>
      {s.label}
    </span>
  );
}

function CoverageBar({ pct }: { pct: number }) {
  const color = pct >= 95 ? "var(--green)" : pct >= 5 ? "var(--amber)" : "var(--red)";
  return (
    <div style={{
      position: "relative",
      width: 120,
      height: 6,
      background: "var(--bg3)",
      borderRadius: 2,
      overflow: "hidden",
    }}>
      <div style={{
        position: "absolute",
        top: 0, left: 0,
        width: `${Math.min(100, Math.max(0, pct))}%`,
        height: "100%",
        background: color,
      }} />
    </div>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function CompilerDaysListPage() {
  const router = useRouter();
  const [state, setState] = useState<LoadState>({ kind: "loading" });

  useEffect(() => {
    let cancelled = false;
    setState({ kind: "loading" });
    fetch(`${API_BASE}/api/compiler/coverage?days=10`, { credentials: "include" })
      .then(async (r) => {
        if (cancelled) return;
        if (r.status === 401) {
          setState({ kind: "error", message: "Session expired. Please log in again." });
          return;
        }
        if (!r.ok) {
          setState({ kind: "error", message: `Coverage endpoint returned ${r.status}` });
          return;
        }
        const data = (await r.json()) as CoverageResponse;
        if (cancelled) return;
        setState({ kind: "ready", data });
      })
      .catch((err) => {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setState({ kind: "error", message: `Network error: ${message}` });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <SectionLabel>Compiler · Days</SectionLabel>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Recent Days
        </h1>

        {state.kind === "loading" && (
          <TableSkeleton rows={10} columns={[80, 100, 60, 120, 100, 50, 12]} />
        )}

        {state.kind === "error" && (
          <div style={{
            background: "var(--red-dim)",
            border: "1px solid var(--red)",
            borderRadius: 6,
            padding: "14px 18px",
            fontSize: 10,
            color: "var(--red)",
          }}>
            {state.message}
          </div>
        )}

        {state.kind === "ready" && (
          <DaysList data={state.data} onDayClick={(date) => router.push(`/compiler/days/${date}`)} />
        )}
      </div>
    </div>
  );
}

function DaysList({ data, onDayClick }: { data: CoverageResponse; onDayClick: (date: string) => void }) {
  if (data.days.length === 0) {
    return (
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "24px",
        textAlign: "center",
        fontSize: 10,
        color: "var(--t3)",
      }}>
        No days returned from the coverage endpoint.
      </div>
    );
  }

  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      overflow: "hidden",
    }}>
      <table style={{
        width: "100%",
        borderCollapse: "collapse",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--line)" }}>
            <th style={thStyle}>Date</th>
            <th style={thStyle}>Day</th>
            <th style={thStyle}>Status</th>
            <th style={{ ...thStyle, textAlign: "right" }}>%</th>
            <th style={{ ...thStyle, textAlign: "right" }} title={`Price · ${endpointDayCount(data.days, "close")} / ${data.days.length} days complete`}>Px</th>
            <th style={{ ...thStyle, textAlign: "right" }} title={`Open Interest · ${endpointDayCount(data.days, "open_interest")} / ${data.days.length} days complete`}>OI</th>
            <th style={{ ...thStyle, textAlign: "right" }} title={`Volume · ${endpointDayCount(data.days, "volume")} / ${data.days.length} days complete`}>Vol</th>
            <th style={{ ...thStyle, textAlign: "right" }} title={`Market Cap · ${endpointDayCount(data.days, "market_cap_usd")} / ${data.days.length} days complete`}>MC</th>
            <th style={thStyle}></th>
          </tr>
        </thead>
        <tbody>
          {data.days.map((day) => {
            const status = statusFor(day);
            return (
              <tr
                key={day.date}
                onClick={() => onDayClick(day.date)}
                style={{
                  borderBottom: "1px solid var(--line)",
                  cursor: "pointer",
                  transition: "background 0.1s ease",
                }}
                onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg3)"; }}
                onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
                title={`Drill into ${day.date}`}
              >
                <td style={tdStyle}>
                  <span style={{ color: "var(--t0)", fontWeight: 700 }}>{day.date}</span>
                </td>
                <td style={tdStyle}>
                  <span style={{ color: "var(--t2)" }}>{dayOfWeek(day.date)}</span>
                  <span style={{ color: "var(--t3)", marginLeft: 8 }}>{relativeDay(day.date)}</span>
                </td>
                <td style={tdStyle}>
                  <StatusBadge status={status} />
                </td>
                <td style={{ ...tdStyle, textAlign: "right" }}>
                  <span style={{ color: "var(--t0)", fontWeight: 700 }}>
                    {day.completeness_pct.toFixed(1)}%
                  </span>
                </td>
                <td style={{ ...tdStyle, textAlign: "right" }}>
                  <EndpointPctBadge pct={day.endpoints?.close ?? 0} />
                </td>
                <td style={{ ...tdStyle, textAlign: "right" }}>
                  <EndpointPctBadge pct={day.endpoints?.open_interest ?? 0} />
                </td>
                <td style={{ ...tdStyle, textAlign: "right" }}>
                  <EndpointPctBadge pct={day.endpoints?.volume ?? 0} />
                </td>
                <td style={{ ...tdStyle, textAlign: "right" }}>
                  <EndpointPctBadge pct={day.endpoints?.market_cap_usd ?? 0} />
                </td>
                <td style={{ ...tdStyle, textAlign: "right", paddingRight: 14 }}>
                  <span style={{ color: "var(--t3)", fontSize: 12 }}>›</span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// Count how many days in a list are "complete" (>= 95%) for a given
// endpoint. Used by column header tooltips.
function endpointDayCount(days: CoverageDay[], key: keyof EndpointCoverage): number {
  return days.filter((d) => (d.endpoints?.[key] ?? 0) >= 95).length;
}

// Compact per-endpoint percentage badge — green/amber/red color-coded.
function EndpointPctBadge({ pct }: { pct: number }) {
  const color =
    pct >= 95 ? "var(--green)" :
    pct >= 5  ? "var(--amber)" :
                "var(--red)";
  return (
    <span style={{
      fontSize: 9,
      fontWeight: 700,
      color,
      fontFamily: "var(--font-space-mono), Space Mono, monospace",
    }}>
      {pct.toFixed(1)}%
    </span>
  );
}

const thStyle: React.CSSProperties = {
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: "0.08em",
  color: "var(--t3)",
  textTransform: "uppercase",
  textAlign: "left",
  padding: "10px 10px 12px",
};

const tdStyle: React.CSSProperties = {
  padding: "10px 10px",
  fontSize: 10,
  color: "var(--t1)",
  verticalAlign: "middle",
};

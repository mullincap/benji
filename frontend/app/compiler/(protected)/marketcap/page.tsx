"use client";

/**
 * frontend/app/compiler/(protected)/marketcap/page.tsx
 * ====================================================
 * Marketcap coverage overview — mirrors the futures Coverage page but
 * sourced from market.market_cap_daily (CoinGecko snapshots).
 *
 * KPI strip + heatmap + recent days table. Click a day to drill into
 * /compiler/marketcap/[date] and see every mcap row for that day.
 */

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Skeleton from "../../../components/Skeleton";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Types ──────────────────────────────────────────────────────────────────

type MarketcapDay = {
  date: string;
  mcap_rows: number;
  ranked_rows: number;
  total_mcap_usd: number | null;
  status: "complete" | "missing";
};

type MarketcapCoverageResponse = {
  lookback_days: number;
  days_returned: number;
  days_with_data: number;
  days_missing: number;
  days: MarketcapDay[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; data: MarketcapCoverageResponse };

// ─── Helpers ────────────────────────────────────────────────────────────────

function formatUsd(n: number | null): string {
  if (n === null || n === undefined) return "—";
  if (n >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
  if (n >= 1e9)  return `$${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6)  return `$${(n / 1e6).toFixed(2)}M`;
  return `$${n.toLocaleString("en-US")}`;
}

function heatmapColor(day: MarketcapDay): { bg: string; border: string } {
  if (day.status === "missing") {
    return { bg: "var(--red-dim)", border: "var(--red)" };
  }
  return { bg: "var(--green-mid)", border: "var(--green)" };
}

// ─── Lookback presets ───────────────────────────────────────────────────────

const LOOKBACK_PRESETS = [
  { label: "30",  days: 30 },
  { label: "90",  days: 90 },
  { label: "365", days: 365 },
  { label: "ALL", days: 10000 },
] as const;
type LookbackPreset = (typeof LOOKBACK_PRESETS)[number];
const DEFAULT_PRESET: LookbackPreset = LOOKBACK_PRESETS[3];

// ─── Subcomponents ──────────────────────────────────────────────────────────

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

function KPICard({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "14px 16px",
      display: "flex",
      flexDirection: "column",
      gap: 6,
    }}>
      <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase" }}>
        {label}
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color: "var(--t0)" }}>
        {value}
      </div>
      {hint && <div style={{ fontSize: 9, color: "var(--t2)" }}>{hint}</div>}
    </div>
  );
}

function LookbackSegmentControl({
  value, onChange, disabled,
}: {
  value: LookbackPreset;
  onChange: (next: LookbackPreset) => void;
  disabled: boolean;
}) {
  return (
    <div role="tablist" aria-label="Lookback range" style={{
      display: "inline-flex",
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 4,
      overflow: "hidden",
      opacity: disabled ? 0.5 : 1,
    }}>
      {LOOKBACK_PRESETS.map((preset) => {
        const active = preset.label === value.label;
        return (
          <button
            key={preset.label}
            type="button"
            role="tab"
            aria-selected={active}
            disabled={disabled}
            onClick={() => onChange(preset)}
            style={{
              background: active ? "var(--bg4)" : "transparent",
              color: active ? "var(--t0)" : "var(--t2)",
              border: "none",
              borderLeft: preset === LOOKBACK_PRESETS[0] ? "none" : "1px solid var(--line)",
              padding: "6px 14px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              cursor: disabled ? "not-allowed" : "pointer",
            }}
          >
            {preset.label}
          </button>
        );
      })}
    </div>
  );
}

function HeatmapCell({ day, onClick }: { day: MarketcapDay; onClick: () => void }) {
  const c = heatmapColor(day);
  const tooltip =
    `${day.date}\n${day.status.toUpperCase()}\n` +
    `${day.mcap_rows.toLocaleString("en-US")} mcap rows\n` +
    `Click to view all rows`;
  return (
    <div
      title={tooltip}
      onClick={onClick}
      style={{
        width: 14, height: 14,
        background: c.bg,
        border: `1px solid ${c.border}`,
        borderRadius: 2,
        cursor: "pointer",
        transition: "transform 0.1s ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.transform = "scale(1.15)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.transform = "scale(1)"; }}
    />
  );
}

function Heatmap({ days, onDayClick }: { days: MarketcapDay[]; onDayClick: (date: string) => void }) {
  const ordered = [...days].reverse();
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "16px 18px",
    }}>
      <SectionLabel>Marketcap Map · last {days.length} days</SectionLabel>
      <div style={{
        display: "flex",
        flexWrap: "wrap",
        gap: 4,
        marginBottom: 12,
      }}>
        {ordered.map((day) => (
          <HeatmapCell key={day.date} day={day} onClick={() => onDayClick(day.date)} />
        ))}
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 14, fontSize: 9, color: "var(--t2)" }}>
        <LegendSwatch color="var(--green-mid)" border="var(--green)" label="has data" />
        <LegendSwatch color="var(--red-dim)" border="var(--red)" label="no data" />
      </div>
    </div>
  );
}

function LegendSwatch({ color, border, label }: { color: string; border: string; label: string }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
      <div style={{
        width: 10, height: 10,
        background: color,
        border: `1px solid ${border}`,
        borderRadius: 2,
      }} />
      <span>{label}</span>
    </div>
  );
}

function RecentTable({ days, onDayClick }: { days: MarketcapDay[]; onDayClick: (date: string) => void }) {
  if (days.length === 0) {
    return null;
  }
  // Most recent 30 days, newest first
  const recent = days.slice(0, 30);
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      overflow: "hidden",
    }}>
      <div style={{ padding: "16px 18px 0" }}>
        <SectionLabel>Recent Days · {recent.length}</SectionLabel>
      </div>
      <table style={{
        width: "100%",
        borderCollapse: "collapse",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}>
        <thead>
          <tr>
            <Th>Date</Th>
            <Th>Status</Th>
            <Th align="right">Mcap Rows</Th>
            <Th align="right">Ranked</Th>
            <Th align="right">Total Mcap (USD)</Th>
            <Th align="right" />
          </tr>
        </thead>
        <tbody>
          {recent.map((d) => (
            <tr
              key={d.date}
              onClick={() => onDayClick(d.date)}
              style={{
                borderTop: "1px solid var(--line)",
                cursor: "pointer",
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg3)"; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
              title="Click to view all mcap rows for this day"
            >
              <Td><span style={{ color: "var(--t0)", fontWeight: 700 }}>{d.date}</span></Td>
              <Td><StatusBadge status={d.status} /></Td>
              <Td align="right">{d.mcap_rows.toLocaleString("en-US")}</Td>
              <Td align="right">{d.ranked_rows.toLocaleString("en-US")}</Td>
              <Td align="right">{formatUsd(d.total_mcap_usd)}</Td>
              <Td align="right"><span style={{ color: "var(--t3)", fontSize: 12 }}>›</span></Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Th({ children, align = "left" }: { children?: React.ReactNode; align?: "left" | "right" }) {
  return (
    <th style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase",
      textAlign: align,
      padding: "10px 14px 12px",
    }}>
      {children}
    </th>
  );
}

function Td({ children, align = "left" }: { children: React.ReactNode; align?: "left" | "right" }) {
  return (
    <td style={{
      fontSize: 10,
      color: "var(--t1)",
      textAlign: align,
      padding: "10px 14px",
    }}>
      {children}
    </td>
  );
}

function StatusBadge({ status }: { status: "complete" | "missing" }) {
  const isMissing = status === "missing";
  const color = isMissing ? "var(--red)" : "var(--green)";
  const bg = isMissing ? "var(--red-dim)" : "var(--green-dim)";
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color,
      background: bg,
      border: `1px solid ${color}`,
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      {status}
    </span>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function CompilerMarketcapPage() {
  const router = useRouter();
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [preset, setPreset] = useState<LookbackPreset>(DEFAULT_PRESET);

  const goToDay = (date: string) => router.push(`/compiler/marketcap/${date}`);

  useEffect(() => {
    let cancelled = false;
    setState({ kind: "loading" });
    fetch(`${API_BASE}/api/compiler/marketcap/coverage?days=${preset.days}`, { credentials: "include" })
      .then(async (r) => {
        if (cancelled) return;
        if (r.status === 401) {
          setState({ kind: "error", message: "Session expired. Please log in again." });
          return;
        }
        if (!r.ok) {
          setState({ kind: "error", message: `Marketcap coverage endpoint returned ${r.status}` });
          return;
        }
        const data = (await r.json()) as MarketcapCoverageResponse;
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
  }, [preset]);

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 24 }}>
          <div>
            <SectionLabel>Compiler · Marketcap</SectionLabel>
            <h1 style={{
              fontSize: 24, fontWeight: 700, color: "var(--t0)",
              margin: 0, letterSpacing: "-0.01em",
            }}>
              Marketcap Coverage
            </h1>
          </div>
          <LookbackSegmentControl
            value={preset}
            onChange={setPreset}
            disabled={state.kind === "loading"}
          />
        </div>

        {state.kind === "loading" && (
          <div style={{ color: "var(--t3)", fontSize: 10, padding: 24 }}>
            <Skeleton width={200} height={16} />
          </div>
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
          <MarketcapContent data={state.data} onDayClick={goToDay} />
        )}
      </div>
    </div>
  );
}

function MarketcapContent({ data, onDayClick }: { data: MarketcapCoverageResponse; onDayClick: (date: string) => void }) {
  const totalMcap = data.days[0]?.total_mcap_usd ?? null;
  const latestRows = data.days[0]?.mcap_rows ?? 0;

  return (
    <>
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(4, 1fr)",
        gap: 10,
        marginBottom: 24,
      }}>
        <KPICard
          label="Days With Data"
          value={data.days_with_data.toLocaleString("en-US")}
          hint={`of ${data.days_returned} days`}
        />
        <KPICard
          label="Days Missing"
          value={data.days_missing.toLocaleString("en-US")}
          hint="zero mcap rows"
        />
        <KPICard
          label="Latest Day Rows"
          value={latestRows.toLocaleString("en-US")}
          hint={data.days[0]?.date ?? "—"}
        />
        <KPICard
          label="Latest Total Mcap"
          value={formatUsd(totalMcap)}
          hint="sum across all bases"
        />
      </div>

      <div style={{ marginBottom: 24 }}>
        <Heatmap days={data.days} onDayClick={onDayClick} />
      </div>

      <RecentTable days={data.days} onDayClick={onDayClick} />
    </>
  );
}

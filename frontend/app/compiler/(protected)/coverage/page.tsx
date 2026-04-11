"use client";

/**
 * frontend/app/compiler/(protected)/coverage/page.tsx
 * ===================================================
 * Compiler Coverage page — read-only monitor for futures_1m ingest health.
 *
 * Data sources:
 *   GET /api/compiler/coverage?days=90  → per-day completeness for last 90 days
 *   GET /api/compiler/gaps?days=90      → days where symbols_complete < total
 *
 * Renders:
 *   1. 4 KPI cards (Total Symbols / Days Complete / Days With Gaps / Days Missing)
 *   2. Calendar heatmap — one cell per day, color-coded by completeness_pct
 *   3. Gap table — most recent gap days first
 *
 * Both API calls are made in parallel via Promise.all. Loading state shows a
 * minimal "Loading…" label. Error state shows the error message in --red.
 * No mock data anywhere.
 */

import { useEffect, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Response shapes ─────────────────────────────────────────────────────────
// Mirror the FastAPI router exactly. If the backend shape changes these
// types must change too — they are the contract.

type CoverageDay = {
  date: string;                 // 'YYYY-MM-DD'
  symbols_complete: number;
  symbols_partial: number;
  symbols_missing: number;
  total_active_symbols: number;
  fetched_universe: number;     // peak symbols_complete across the window
  completeness_pct: number;     // 0.0 - 100.0, rounded to 1 decimal
};

type CoverageResponse = {
  source_id: number;
  lookback_days: number;
  total_active_symbols: number;
  fetched_universe: number;
  days_returned: number;
  days: CoverageDay[];
};

type GapDay = {
  date: string;
  symbols_complete: number;
  symbols_total: number;
  completeness_pct: number;
  status: "missing" | "partial";
};

type GapsResponse = {
  source_id: number;
  lookback_days: number;
  total_active_symbols: number;
  fetched_universe: number;
  gaps_returned: number;
  gaps: GapDay[];
};

// ─── Lookback presets ────────────────────────────────────────────────────────
// Segment control shown above the KPI cards. "ALL" sends 10000 days upstream;
// the API clamps to whatever's actually in market.futures_1m via its
// `>= NOW() - <interval>` filter, so 10000 effectively means "everything".
const LOOKBACK_PRESETS = [
  { label: "30",  days: 30 },
  { label: "90",  days: 90 },
  { label: "365", days: 365 },
  { label: "ALL", days: 10000 },
] as const;
type LookbackPreset = (typeof LOOKBACK_PRESETS)[number];
const DEFAULT_PRESET: LookbackPreset = LOOKBACK_PRESETS[1]; // 90

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; coverage: CoverageResponse; gaps: GapsResponse };

// ─── Heatmap thresholds ──────────────────────────────────────────────────────
// Locked decision (per spec): a day is "green" if >= 95% of active symbols
// reach 1440 rows. Below that we use --amber for partial and --red for empty,
// with a darker no-data variant for days where the API returned no row at all.

function heatmapColor(pct: number | null): { bg: string; border: string } {
  if (pct === null) {
    return { bg: "var(--bg2)", border: "var(--line)" };
  }
  if (pct >= 95) {
    return { bg: "var(--green-mid)", border: "var(--green)" };
  }
  if (pct >= 5) {
    return { bg: "var(--amber-dim)", border: "var(--amber)" };
  }
  return { bg: "var(--red-dim)", border: "var(--red)" };
}

// ─── Subcomponents ───────────────────────────────────────────────────────────

function LookbackSegmentControl({
  value,
  onChange,
  disabled,
}: {
  value: LookbackPreset;
  onChange: (next: LookbackPreset) => void;
  disabled: boolean;
}) {
  return (
    <div
      role="tablist"
      aria-label="Lookback range"
      style={{
        display: "inline-flex",
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 4,
        overflow: "hidden",
        opacity: disabled ? 0.5 : 1,
      }}
    >
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
              borderLeft: preset === LOOKBACK_PRESETS[0]
                ? "none"
                : "1px solid var(--line)",
              padding: "6px 14px",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              cursor: disabled ? "not-allowed" : "pointer",
              transition: "background 0.15s ease, color 0.15s ease",
            }}
            onMouseEnter={(e) => {
              if (!active && !disabled) {
                e.currentTarget.style.color = "var(--t1)";
              }
            }}
            onMouseLeave={(e) => {
              if (!active && !disabled) {
                e.currentTarget.style.color = "var(--t2)";
              }
            }}
          >
            {preset.label}
          </button>
        );
      })}
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase",
      marginBottom: 8,
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
      <div style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase",
      }}>
        {label}
      </div>
      <div style={{
        fontSize: 18, fontWeight: 700, color: "var(--t0)",
      }}>
        {value}
      </div>
      {hint && (
        <div style={{ fontSize: 9, color: "var(--t2)" }}>
          {hint}
        </div>
      )}
    </div>
  );
}

function HeatmapCell({ day }: { day: CoverageDay }) {
  const colors = heatmapColor(day.completeness_pct);
  const tooltip =
    `${day.date}\n` +
    `${day.symbols_complete} complete · ${day.symbols_partial} partial · ${day.symbols_missing} missing\n` +
    `${day.completeness_pct}% of ${day.total_active_symbols} active symbols`;
  return (
    <div
      title={tooltip}
      style={{
        width: 14,
        height: 14,
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: 2,
        cursor: "default",
      }}
    />
  );
}

function Heatmap({ days }: { days: CoverageDay[] }) {
  // Display oldest-first left-to-right so the most recent day is bottom-right,
  // matching the convention of GitHub's contribution graph. The API returns
  // newest-first so we reverse here.
  const ordered = [...days].reverse();

  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "16px 18px",
    }}>
      <SectionLabel>Coverage Map · last {days.length} days</SectionLabel>
      <div style={{
        display: "flex",
        flexWrap: "wrap",
        gap: 4,
        marginBottom: 12,
      }}>
        {ordered.map((day) => (
          <HeatmapCell key={day.date} day={day} />
        ))}
      </div>
      <div style={{
        display: "flex",
        alignItems: "center",
        gap: 14,
        fontSize: 9,
        color: "var(--t2)",
      }}>
        <LegendSwatch color="var(--green-mid)" border="var(--green)" label="≥ 95% complete" />
        <LegendSwatch color="var(--amber-dim)" border="var(--amber)" label="5–94% complete" />
        <LegendSwatch color="var(--red-dim)" border="var(--red)" label="< 5% complete" />
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

function GapTable({ gaps }: { gaps: GapDay[] }) {
  if (gaps.length === 0) {
    return (
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "16px 18px",
      }}>
        <SectionLabel>Gap Days</SectionLabel>
        <div style={{ fontSize: 10, color: "var(--t2)" }}>
          No gaps detected — every day in the lookback window is fully covered.
        </div>
      </div>
    );
  }

  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "16px 18px",
    }}>
      <SectionLabel>Gap Days · {gaps.length} day{gaps.length === 1 ? "" : "s"}</SectionLabel>
      <table style={{
        width: "100%",
        borderCollapse: "collapse",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}>
        <thead>
          <tr>
            <Th>Date</Th>
            <Th>Status</Th>
            <Th align="right">Complete / Total</Th>
            <Th align="right">Completeness</Th>
          </tr>
        </thead>
        <tbody>
          {gaps.map((g) => (
            <tr key={g.date} style={{ borderTop: "1px solid var(--line)" }}>
              <Td>{g.date}</Td>
              <Td><StatusBadge status={g.status} /></Td>
              <Td align="right">{g.symbols_complete} / {g.symbols_total}</Td>
              <Td align="right">{g.completeness_pct.toFixed(1)}%</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Th({ children, align = "left" }: { children: React.ReactNode; align?: "left" | "right" }) {
  return (
    <th style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase",
      textAlign: align,
      padding: "8px 10px",
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
      padding: "8px 10px",
    }}>
      {children}
    </td>
  );
}

function StatusBadge({ status }: { status: "missing" | "partial" }) {
  const isPartial = status === "partial";
  const color = isPartial ? "var(--amber)" : "var(--red)";
  const bg = isPartial ? "var(--amber-dim)" : "var(--red-dim)";
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

// ─── Page ────────────────────────────────────────────────────────────────────

export default function CompilerCoveragePage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [preset, setPreset] = useState<LookbackPreset>(DEFAULT_PRESET);

  useEffect(() => {
    let cancelled = false;
    setState({ kind: "loading" });
    async function load() {
      try {
        const [covRes, gapsRes] = await Promise.all([
          fetch(`${API_BASE}/api/compiler/coverage?days=${preset.days}`, { credentials: "include" }),
          fetch(`${API_BASE}/api/compiler/gaps?days=${preset.days}`, { credentials: "include" }),
        ]);
        if (cancelled) return;

        if (covRes.status === 401 || gapsRes.status === 401) {
          setState({ kind: "error", message: "Session expired. Please log in again." });
          return;
        }
        if (!covRes.ok) {
          setState({ kind: "error", message: `Coverage endpoint returned ${covRes.status}` });
          return;
        }
        if (!gapsRes.ok) {
          setState({ kind: "error", message: `Gaps endpoint returned ${gapsRes.status}` });
          return;
        }

        const coverage = (await covRes.json()) as CoverageResponse;
        const gaps = (await gapsRes.json()) as GapsResponse;
        if (cancelled) return;
        setState({ kind: "ready", coverage, gaps });
      } catch (err) {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setState({ kind: "error", message: `Network error: ${message}` });
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [preset]);

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <div style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          marginBottom: 24,
        }}>
          <div>
            <SectionLabel>Compiler · Coverage</SectionLabel>
            <h1 style={{
              fontSize: 24, fontWeight: 700, color: "var(--t0)",
              margin: 0,
              letterSpacing: "-0.01em",
            }}>
              Coverage Map
            </h1>
          </div>
          <LookbackSegmentControl
            value={preset}
            onChange={setPreset}
            disabled={state.kind === "loading"}
          />
        </div>

        {state.kind === "loading" && (
          <div style={{
            fontSize: 9, color: "var(--t3)",
            textTransform: "uppercase", letterSpacing: "0.12em",
            padding: "40px 0",
          }}>
            Loading coverage data…
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
          <CoverageContent coverage={state.coverage} gaps={state.gaps} />
        )}
      </div>
    </div>
  );
}

function CoverageContent({ coverage, gaps }: { coverage: CoverageResponse; gaps: GapsResponse }) {
  // KPI math. "Fetched universe" = peak symbols_complete across the window.
  // It's a more honest denominator than `active`, which drifts stale when
  // Binance lists/delists instruments.
  const fetchedUniverse = coverage.fetched_universe;
  const daysComplete = coverage.days.filter((d) => d.completeness_pct >= 95).length;
  const daysWithGaps = gaps.gaps.filter((g) => g.status === "partial").length;
  const daysMissing = gaps.gaps.filter((g) => g.status === "missing").length;

  return (
    <>
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(4, 1fr)",
        gap: 10,
        marginBottom: 24,
      }}>
        <KPICard
          label="Fetched Universe"
          value={fetchedUniverse.toLocaleString("en-US")}
          hint="peak symbols complete in window"
        />
        <KPICard
          label="Days Complete"
          value={daysComplete.toLocaleString("en-US")}
          hint={`of ${coverage.days_returned} days · ≥ 95% coverage`}
        />
        <KPICard
          label="Days With Gaps"
          value={daysWithGaps.toLocaleString("en-US")}
          hint="partial coverage"
        />
        <KPICard
          label="Days Missing"
          value={daysMissing.toLocaleString("en-US")}
          hint="zero symbols complete"
        />
      </div>

      <div style={{ marginBottom: 24 }}>
        <Heatmap days={coverage.days} />
      </div>

      <GapTable gaps={gaps.gaps} />
    </>
  );
}

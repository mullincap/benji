"use client";

/**
 * frontend/app/indexer/(protected)/coverage/page.tsx
 * ==================================================
 * Indexer Coverage page — read-only monitor for market.leaderboards build
 * health, broken down by metric (price · open_interest · volume).
 *
 * Data source:
 *   GET /api/indexer/coverage?days=90
 *
 * Response shape (flat array — newest first, only includes days that have
 * rows for at least one metric):
 *   {
 *     lookback_days: 90,
 *     rows_per_full_day: 479520,    // 1440 × 333
 *     metrics: ["price","open_interest","volume"],
 *     days_returned: N,
 *     days: [{ date, metric, rows_actual, rows_expected, completeness_pct }]
 *   }
 *
 * The API returns NO row for a (date, metric) pair where the indexer never
 * ran. The frontend synthesizes the full date axis locally so empty days
 * for empty metrics still render as red cells — honest "this is broken"
 * visualization, per the build doc.
 *
 * Renders three blocks per metric:
 *   1. A row of 4 KPI cards (Days Complete / Days Partial / Days Missing /
 *      Most Recent Day) for that metric
 *   2. A calendar heatmap (one cell per day, oldest-first left-to-right)
 *   3. A gap table at the bottom of the page, grouped by metric
 */

import { useEffect, useMemo, useState } from "react";
import Skeleton, { KPIGridSkeleton } from "../../../components/Skeleton";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// Lookback presets shown in the segment control. The "ALL" preset sends a
// large numeric value upstream — the API clamps to whatever's actually in
// market.leaderboards via the SQL `>= NOW() - <interval>` filter, so a value
// of 10000 effectively means "everything we have."
const LOOKBACK_PRESETS = [
  { label: "30",  days: 30 },
  { label: "90",  days: 90 },
  { label: "365", days: 365 },
  { label: "ALL", days: 10000 },
] as const;
type LookbackPreset = (typeof LOOKBACK_PRESETS)[number];
const DEFAULT_PRESET: LookbackPreset = LOOKBACK_PRESETS[1]; // 90

const COMPLETE_THRESHOLD_PCT = 95;
const PARTIAL_THRESHOLD_PCT = 5;

// Display order — matches the API's `metrics` array. Hard-coded so the page
// renders even if the API ever returns metrics in a different order.
const METRICS = ["price", "open_interest", "volume"] as const;
type Metric = (typeof METRICS)[number];

const METRIC_LABELS: Record<Metric, string> = {
  price: "Price",
  open_interest: "Open Interest",
  volume: "Volume",
};

// ─── Response shapes ─────────────────────────────────────────────────────────

type CoverageDay = {
  date: string;          // 'YYYY-MM-DD'
  metric: string;        // 'price' | 'open_interest' | 'volume'
  rows_actual: number;
  rows_expected: number;
  completeness_pct: number;
};

type CoverageResponse = {
  lookback_days: number;
  rows_per_full_day: number;
  metrics: string[];
  days_returned: number;
  days: CoverageDay[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; coverage: CoverageResponse };

// ─── Date helpers ────────────────────────────────────────────────────────────

/** Returns ['YYYY-MM-DD', ...] for the last N days, OLDEST first. */
function buildDateAxis(days: number): string[] {
  const out: string[] = [];
  const today = new Date();
  // Use UTC to match the API which buckets by UTC date
  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(Date.UTC(
      today.getUTCFullYear(),
      today.getUTCMonth(),
      today.getUTCDate() - i,
    ));
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

/** Build the date axis bounded by what the API actually returned. Used for
 * the "ALL" preset where requesting the literal lookback (e.g. 10000 days)
 * would produce a 27-year axis with mostly empty cells. We anchor on the
 * earliest date the API returned and walk forward to today. */
function buildDateAxisFromData(coverageDays: { date: string }[]): string[] {
  if (coverageDays.length === 0) return buildDateAxis(0);
  let earliest = coverageDays[0].date;
  for (const d of coverageDays) {
    if (d.date < earliest) earliest = d.date;
  }
  // Compute number of days between `earliest` and today (inclusive, UTC)
  const start = new Date(earliest + "T00:00:00Z");
  const today = new Date();
  const todayUtc = Date.UTC(
    today.getUTCFullYear(),
    today.getUTCMonth(),
    today.getUTCDate(),
  );
  const diffDays = Math.floor((todayUtc - start.getTime()) / (24 * 60 * 60 * 1000)) + 1;
  return buildDateAxis(Math.max(diffDays, 1));
}

// ─── Heatmap colour ──────────────────────────────────────────────────────────

type CellState = "complete" | "partial" | "missing" | "no-data";

function classify(pct: number | null): CellState {
  if (pct === null) return "no-data";
  if (pct >= COMPLETE_THRESHOLD_PCT) return "complete";
  if (pct >= PARTIAL_THRESHOLD_PCT) return "partial";
  return "missing";
}

function cellColors(state: CellState): { bg: string; border: string } {
  switch (state) {
    case "complete":
      return { bg: "var(--green-mid)", border: "var(--green)" };
    case "partial":
      return { bg: "var(--amber-dim)", border: "var(--amber)" };
    case "missing":
      return { bg: "var(--red-dim)", border: "var(--red)" };
    case "no-data":
      return { bg: "var(--bg2)", border: "var(--line)" };
  }
}

// ─── Subcomponents ───────────────────────────────────────────────────────────

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

function HeatmapCell({ date, day }: { date: string; day: CoverageDay | null }) {
  const pct = day?.completeness_pct ?? null;
  const state = classify(pct);
  const colors = cellColors(state);
  const tooltip = day
    ? `${date}\n` +
      `${day.rows_actual.toLocaleString("en-US")} / ${day.rows_expected.toLocaleString("en-US")} rows\n` +
      `${day.completeness_pct}% complete`
    : `${date}\nNo rows in market.leaderboards`;
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

function MetricBlock({
  metric,
  dateAxis,
  byDate,
}: {
  metric: Metric;
  dateAxis: string[];
  byDate: Map<string, CoverageDay>;
}) {
  // KPI math against the FULL date axis (so missing dates count as missing)
  let daysComplete = 0;
  let daysPartial = 0;
  let daysMissing = 0;
  let mostRecentWithData: CoverageDay | null = null;

  for (let i = dateAxis.length - 1; i >= 0; i--) {
    const date = dateAxis[i];
    const day = byDate.get(date) ?? null;
    const state = classify(day?.completeness_pct ?? null);
    if (state === "complete") daysComplete++;
    else if (state === "partial") daysPartial++;
    else daysMissing++; // "missing" + "no-data" both count as missing here

    if (day && mostRecentWithData === null) {
      mostRecentWithData = day;
    }
  }

  const mostRecentLabel = mostRecentWithData
    ? `${mostRecentWithData.date} · ${mostRecentWithData.completeness_pct}%`
    : "—";

  return (
    <div style={{ marginBottom: 28 }}>
      <SectionLabel>{METRIC_LABELS[metric]}</SectionLabel>

      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(4, 1fr)",
        gap: 10,
        marginBottom: 12,
      }}>
        <KPICard
          label="Days Complete"
          value={daysComplete.toLocaleString("en-US")}
          hint={`of ${dateAxis.length} · ≥ ${COMPLETE_THRESHOLD_PCT}%`}
        />
        <KPICard
          label="Days Partial"
          value={daysPartial.toLocaleString("en-US")}
          hint={`${PARTIAL_THRESHOLD_PCT}–${COMPLETE_THRESHOLD_PCT - 1}% complete`}
        />
        <KPICard
          label="Days Missing"
          value={daysMissing.toLocaleString("en-US")}
          hint={`< ${PARTIAL_THRESHOLD_PCT}% or no rows`}
        />
        <KPICard
          label="Most Recent Day"
          value={mostRecentLabel}
          hint="latest row in market.leaderboards"
        />
      </div>

      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "16px 18px",
      }}>
        <div style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 4,
        }}>
          {dateAxis.map((date) => (
            <HeatmapCell key={date} date={date} day={byDate.get(date) ?? null} />
          ))}
        </div>
      </div>
    </div>
  );
}

function GapTable({
  byMetric,
  dateAxis,
}: {
  byMetric: Map<Metric, Map<string, CoverageDay>>;
  dateAxis: string[];
}) {
  // Collect all (metric, date) pairs that aren't complete, newest first
  type GapRow = {
    date: string;
    metric: Metric;
    state: CellState;
    rows_actual: number;
    completeness_pct: number;
  };
  const gaps: GapRow[] = [];
  // Walk newest -> oldest
  for (let i = dateAxis.length - 1; i >= 0; i--) {
    const date = dateAxis[i];
    for (const metric of METRICS) {
      const day = byMetric.get(metric)?.get(date) ?? null;
      const state = classify(day?.completeness_pct ?? null);
      if (state === "complete") continue;
      gaps.push({
        date,
        metric,
        state,
        rows_actual: day?.rows_actual ?? 0,
        completeness_pct: day?.completeness_pct ?? 0,
      });
    }
  }

  if (gaps.length === 0) {
    return (
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "16px 18px",
        marginTop: 8,
      }}>
        <SectionLabel>Gap Days</SectionLabel>
        <div style={{ fontSize: 10, color: "var(--t2)" }}>
          No gaps detected — every (metric, day) pair in the lookback window is fully covered.
        </div>
      </div>
    );
  }

  // Show only the most recent 50 gap rows so the page doesn't balloon
  const visible = gaps.slice(0, 50);

  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "16px 18px",
      marginTop: 8,
    }}>
      <SectionLabel>
        Gap Days · {gaps.length} total · showing {visible.length}
      </SectionLabel>
      <table style={{
        width: "100%",
        borderCollapse: "collapse",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}>
        <thead>
          <tr>
            <Th>Date</Th>
            <Th>Metric</Th>
            <Th>Status</Th>
            <Th align="right">Rows</Th>
            <Th align="right">Completeness</Th>
          </tr>
        </thead>
        <tbody>
          {visible.map((g) => (
            <tr key={`${g.date}-${g.metric}`} style={{ borderTop: "1px solid var(--line)" }}>
              <Td>{g.date}</Td>
              <Td>{METRIC_LABELS[g.metric]}</Td>
              <Td><StatusBadge state={g.state} /></Td>
              <Td align="right">{g.rows_actual.toLocaleString("en-US")}</Td>
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

function StatusBadge({ state }: { state: CellState }) {
  const label = state === "no-data" ? "missing" : state;
  const colors = cellColors(state);
  // Use the border colour as the text/border colour for legibility
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: colors.border,
      background: colors.bg,
      border: `1px solid ${colors.border}`,
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      {label}
    </span>
  );
}

// ─── Lookback segment control ────────────────────────────────────────────────

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

// ─── Page ────────────────────────────────────────────────────────────────────

export default function IndexerCoveragePage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [preset, setPreset] = useState<LookbackPreset>(DEFAULT_PRESET);

  useEffect(() => {
    let cancelled = false;
    setState({ kind: "loading" });
    async function load() {
      try {
        const res = await fetch(
          `${API_BASE}/api/indexer/coverage?days=${preset.days}`,
          { credentials: "include" },
        );
        if (cancelled) return;
        if (res.status === 401) {
          setState({ kind: "error", message: "Session expired. Please log in again." });
          return;
        }
        if (!res.ok) {
          setState({ kind: "error", message: `Coverage endpoint returned ${res.status}` });
          return;
        }
        const coverage = (await res.json()) as CoverageResponse;
        if (cancelled) return;
        setState({ kind: "ready", coverage });
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
        <SectionLabel>Indexer · Coverage</SectionLabel>
        <div style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          gap: 16,
          marginBottom: 8,
        }}>
          <h1 style={{
            fontSize: 24, fontWeight: 700, color: "var(--t0)",
            margin: 0,
            letterSpacing: "-0.01em",
          }}>
            Coverage Map
          </h1>
          <LookbackSegmentControl
            value={preset}
            onChange={setPreset}
            disabled={state.kind === "loading"}
          />
        </div>
        <div style={{
          fontSize: 10, color: "var(--t2)",
          marginBottom: 24,
        }}>
          Per-metric leaderboard completeness · {preset.label === "ALL"
            ? "all available days"
            : `last ${preset.label} days`} · expected{" "}
          {(1440 * 333).toLocaleString("en-US")} rows/day (1440 × 333)
        </div>

        {state.kind === "loading" && (
          <>
            {/* 3 metric blocks (price / OI / volume), each = KPI strip + heatmap */}
            {Array.from({ length: 3 }).map((_, m) => (
              <div key={m} style={{ marginBottom: 28 }}>
                <Skeleton width={120} height={9} style={{ marginBottom: 12 }} />
                <KPIGridSkeleton count={4} />
                <div style={{
                  background: "var(--bg2)",
                  border: "1px solid var(--line)",
                  borderRadius: 6,
                  padding: "16px 18px",
                  display: "flex",
                  flexWrap: "wrap",
                  gap: 4,
                }}>
                  {Array.from({ length: 90 }).map((_, i) => (
                    <Skeleton key={i} width={14} height={14} borderRadius={2} />
                  ))}
                </div>
              </div>
            ))}
          </>
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

        {state.kind === "ready" && <CoverageContent coverage={state.coverage} />}
      </div>
    </div>
  );
}

function CoverageContent({ coverage }: { coverage: CoverageResponse }) {
  const { dateAxis, byMetric } = useMemo(() => {
    // If the lookback is unbounded ("ALL", > 1 year), bound the axis to the
    // actual data window so we don't render thousands of empty cells.
    const dateAxis = coverage.lookback_days > 365
      ? buildDateAxisFromData(coverage.days)
      : buildDateAxis(coverage.lookback_days);
    const byMetric = new Map<Metric, Map<string, CoverageDay>>();
    for (const m of METRICS) byMetric.set(m, new Map());
    for (const d of coverage.days) {
      const m = d.metric as Metric;
      if (!METRICS.includes(m)) continue; // ignore unexpected metrics
      byMetric.get(m)!.set(d.date, d);
    }
    return { dateAxis, byMetric };
  }, [coverage]);

  return (
    <>
      {METRICS.map((metric) => (
        <MetricBlock
          key={metric}
          metric={metric}
          dateAxis={dateAxis}
          byDate={byMetric.get(metric)!}
        />
      ))}

      <div style={{
        display: "flex",
        alignItems: "center",
        gap: 14,
        fontSize: 9,
        color: "var(--t2)",
        marginTop: 4,
        marginBottom: 16,
      }}>
        <LegendSwatch color="var(--green-mid)" border="var(--green)" label={`≥ ${COMPLETE_THRESHOLD_PCT}% complete`} />
        <LegendSwatch color="var(--amber-dim)" border="var(--amber)" label={`${PARTIAL_THRESHOLD_PCT}–${COMPLETE_THRESHOLD_PCT - 1}% partial`} />
        <LegendSwatch color="var(--red-dim)" border="var(--red)" label={`< ${PARTIAL_THRESHOLD_PCT}% missing`} />
        <LegendSwatch color="var(--bg2)" border="var(--line)" label="no rows in DB" />
      </div>

      <GapTable byMetric={byMetric} dateAxis={dateAxis} />
    </>
  );
}

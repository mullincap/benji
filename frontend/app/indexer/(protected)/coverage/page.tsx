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

import { useEffect, useMemo, useRef, useState } from "react";
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

type FillStatus = {
  job_id: string;
  state: "queued" | "running" | "done" | "completed_with_errors" | "failed" | "cancelled";
  dates_total: number;
  dates_completed: number;
  dates_failed: number;
  current_date: string | null;
  current_state: string | null;
  elapsed_seconds: number;
  summary: string | null;
  errors?: string[];
  dates?: { date: string; state: "pending" | "running" | "done" | "failed";
            metrics_done?: string[]; metrics_failed?: string[] }[];
};

// Confirm-phase metadata captured from the POST response — shown in the
// modal before the user opts to track progress, and persisted while the
// modal is open so a quick re-poll doesn't lose the dates list.
type FillModalInit = { dates: string[]; estMin: number };

export default function IndexerCoveragePage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [preset, setPreset] = useState<LookbackPreset>(DEFAULT_PRESET);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [fillStatus, setFillStatus] = useState<FillStatus | null>(null);
  const [fillError, setFillError] = useState<string | null>(null);
  // Modal openness + the initial confirm-phase data (dates list + ETA).
  // null means closed; setting to an object opens the modal. The modal
  // renders confirm / progress / done internally based on fillStatus.
  const [fillModal, setFillModal] = useState<FillModalInit | null>(null);
  // Live tail of the worker's log for in-modal streaming.
  const [fillLogText, setFillLogText] = useState<string>("");
  const fillLogOffsetRef = useRef<number>(0);
  // POST in-flight guard so a double-click can't race two submissions.
  const [postingFill, setPostingFill] = useState(false);

  const fillIsTerminal = (s: FillStatus | null) =>
    s !== null && (s.state === "done" || s.state === "failed" ||
                   s.state === "completed_with_errors" ||
                   s.state === "cancelled");
  const fillIsRunning = (s: FillStatus | null) =>
    s !== null && (s.state === "queued" || s.state === "running");

  // Poll status while a job is in-flight; stop on terminal.
  useEffect(() => {
    if (!fillStatus || fillIsTerminal(fillStatus)) return;
    const job_id = fillStatus.job_id;
    const interval = window.setInterval(async () => {
      try {
        const res = await fetch(
          `${API_BASE}/api/indexer/coverage/fill-missing/status?job_id=${job_id}`,
          { credentials: "include" },
        );
        if (res.status === 404 || res.status === 503) return;
        if (!res.ok) return;
        const next = (await res.json()) as FillStatus;
        setFillStatus(next);
        if (fillIsTerminal(next)) {
          setRefreshNonce((n) => n + 1);
        }
      } catch { /* network blip — keep polling */ }
    }, 3000);
    return () => window.clearInterval(interval);
  }, [fillStatus]);

  // Poll worker's log file at the same cadence as /status.
  useEffect(() => {
    if (!fillStatus) {
      setFillLogText("");
      fillLogOffsetRef.current = 0;
      return;
    }
    const job_id = fillStatus.job_id;
    setFillLogText("");
    fillLogOffsetRef.current = 0;

    let cancelled = false;
    const tick = async () => {
      if (cancelled) return;
      try {
        const res = await fetch(
          `${API_BASE}/api/indexer/coverage/fill-missing/log?job_id=${job_id}&since=${fillLogOffsetRef.current}`,
          { credentials: "include" },
        );
        if (!res.ok) return;
        const body = await res.json() as
          { end: number; bytes: number; truncated: boolean; text: string };
        if (cancelled) return;
        fillLogOffsetRef.current = body.end;
        if (body.bytes > 0) {
          setFillLogText((prev) => {
            const next = body.truncated ? body.text : prev + body.text;
            const MAX = 80_000;
            return next.length > MAX ? next.slice(next.length - MAX) : next;
          });
        }
      } catch { /* swallow blips */ }
    };
    void tick();
    const id = window.setInterval(tick, 3000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, [fillStatus?.job_id]);

  // On page load, rehydrate any active job
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/indexer/coverage/fill-missing/active`,
          { credentials: "include" })
      .then((r) => r.ok ? r.json() : null)
      .then((doc) => {
        if (cancelled || !doc || !doc.job_id) return;
        setFillStatus(doc as FillStatus);
      })
      .catch(() => {});
    return () => { cancelled = true; };
  }, []);

  async function handleFillMissing() {
    if (fillIsRunning(fillStatus)) return;
    if (postingFill) return;
    setFillError(null);
    setPostingFill(true);

    let initial: { job_id: string; state: string; dates_total: number;
                   dates?: string[]; summary?: string };
    try {
      const res = await fetch(
        `${API_BASE}/api/indexer/coverage/fill-missing?days_back=30`,
        { method: "POST", credentials: "include" },
      );
      if (res.status === 409) {
        // Another indexer job already running — rehydrate from /status
        // and reopen the modal in progress phase instead of erroring.
        try {
          const body = await res.json();
          const existingJobId = body?.detail?.job_id;
          if (existingJobId) {
            const sres = await fetch(
              `${API_BASE}/api/indexer/coverage/fill-missing/status?job_id=${existingJobId}`,
              { credentials: "include" },
            );
            if (sres.ok) {
              const live = (await sres.json()) as FillStatus;
              setFillStatus(live);
              setFillModal({
                dates: (live.dates ?? []).map((d) => d.date),
                estMin: 0,
              });
            }
          }
        } catch { /* fall through */ }
        return;
      }
      if (!res.ok) {
        const text = await res.text().catch(() => `HTTP ${res.status}`);
        setFillError(text.slice(0, 200));
        return;
      }
      initial = await res.json();
    } catch (err) {
      setFillError(err instanceof Error ? err.message : String(err));
      return;
    } finally {
      setPostingFill(false);
    }

    if (initial.state === "done") {
      setFillStatus({
        job_id: initial.job_id,
        state: "done",
        dates_total: 0,
        dates_completed: 0,
        dates_failed: 0,
        current_date: null,
        current_state: null,
        elapsed_seconds: 0,
        summary: initial.summary || "No partial days found",
      });
      setRefreshNonce((n) => n + 1);
      setTimeout(() => setFillStatus(null), 6000);
      return;
    }

    const dates = initial.dates || [];
    // Indexer rebuild ~30-50s per metric per day, 3 metrics per day.
    // So ~2-3 min per day, much faster than compiler's metl-driven topup.
    const estMin = Math.ceil((dates.length * 2.5));
    // Open the modal in confirm phase. Tracking only starts when the
    // user clicks Watch — Dismiss leaves the job running detached with
    // no chip, matching the prior window.confirm flow.
    setFillModal({ dates, estMin });
    pendingFillJobIdRef.current = initial.job_id;
    pendingFillDatesTotalRef.current = dates.length;
  }

  // Refs hold the POST-returned job_id + dates_total between opening the
  // confirm modal and the user clicking Watch. Stored in refs (not state)
  // because they don't drive any rendering — they're consumed once and
  // then forwarded into fillStatus via setFillStatus.
  const pendingFillJobIdRef = useRef<string | null>(null);
  const pendingFillDatesTotalRef = useRef<number>(0);

  function handleStartWatching() {
    const job_id = pendingFillJobIdRef.current;
    const total = pendingFillDatesTotalRef.current;
    if (!job_id) return;
    setFillStatus({
      job_id,
      state: "queued",
      dates_total: total,
      dates_completed: 0,
      dates_failed: 0,
      current_date: null,
      current_state: "queued",
      elapsed_seconds: 0,
      summary: null,
    });
    // Modal stays open; it will switch to progress phase on next poll.
  }

  function handleDismissModal() {
    // Dismiss = "run in background" — close the modal but kick off
    // chip tracking so the operator still has a visible signal that
    // a worker is alive. See compiler/coverage/page.tsx for context.
    if (pendingFillJobIdRef.current && fillStatus === null) {
      handleStartWatching();
    }
    setFillModal(null);
    pendingFillJobIdRef.current = null;
  }

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
  }, [preset, refreshNonce]);

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
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {fillStatus && fillIsTerminal(fillStatus) && fillStatus.summary && (
              <span style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
                color: fillStatus.state === "done" ? "var(--green)"
                       : fillStatus.state === "failed" ? "var(--red)"
                       : "var(--amber)",
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
                maxWidth: 280,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }} title={fillStatus.summary}>
                {fillStatus.state === "done" ? "✓" :
                 fillStatus.state === "failed" ? "✗" : "⚠"} {fillStatus.summary}
              </span>
            )}
            {fillError && (
              <span title={fillError} style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
                color: "var(--red)",
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
              }}>
                ✗ {fillError.slice(0, 60)}
              </span>
            )}
            <button
              type="button"
              onClick={() => {
                // While a job is in flight, the chip becomes a re-opener
                // for the progress modal — surfaces live per-date status
                // again after the user minimized. Otherwise it kicks off
                // a fresh fill-missing detection.
                if (fillIsRunning(fillStatus)) {
                  // Reuse the dates list from the live status so the
                  // modal has rows to show even before init data is set.
                  setFillModal({
                    dates: (fillStatus?.dates ?? []).map((d) => d.date),
                    estMin: 0,
                  });
                } else {
                  handleFillMissing();
                }
              }}
              disabled={state.kind === "loading" || postingFill}
              title={
                postingFill
                  ? "Detecting partial days…"
                  : fillIsRunning(fillStatus)
                    ? "Click to re-open the progress modal"
                    : "Detect partial days in market.leaderboards, force-rebuild each metric (price/OI/volume) per day, then refresh the continuous aggregate. Today (UTC) is always excluded from auto-detect."
              }
              style={{
                background: fillIsRunning(fillStatus) ? "var(--bg4)" : "var(--bg2)",
                color: fillIsRunning(fillStatus) ? "var(--amber)" : "var(--t1)",
                border: "1px solid var(--line)",
                borderRadius: 4,
                padding: "6px 14px",
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
                cursor: state.kind === "loading" ? "not-allowed" : "pointer",
                transition: "background 0.15s ease, color 0.15s ease",
                minWidth: 120,
                whiteSpace: "nowrap",
              }}
              onMouseEnter={(e) => {
                if (state.kind !== "loading") {
                  e.currentTarget.style.background = "var(--bg4)";
                  if (!fillIsRunning(fillStatus)) e.currentTarget.style.color = "var(--t0)";
                }
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = fillIsRunning(fillStatus) ? "var(--bg4)" : "var(--bg2)";
                if (!fillIsRunning(fillStatus)) e.currentTarget.style.color = "var(--t1)";
              }}
            >
              {(() => {
                if (postingFill) return "detecting…";
                if (!fillStatus || !fillIsRunning(fillStatus)) return "fill missing";
                const total = fillStatus.dates_total;
                const done = fillStatus.dates_completed + fillStatus.dates_failed;
                const cur = fillStatus.current_date;
                const elapsed = fillStatus.elapsed_seconds;
                const mm = Math.floor(elapsed / 60);
                const ss = String(elapsed % 60).padStart(2, "0");
                if (cur && fillStatus.current_state) {
                  // current_state is e.g. "rebuilding price"
                  return `${done + 1}/${total} ${cur} ${fillStatus.current_state.replace("rebuilding ", "")} ${mm}:${ss}`;
                }
                if (cur) {
                  return `filling ${done + 1}/${total}: ${cur} ${mm}:${ss}`;
                }
                if (fillStatus.current_state === "refreshing_cagg") {
                  return `refreshing cagg ${mm}:${ss}`;
                }
                return `queued ${mm}:${ss}`;
              })()}
            </button>
            <LookbackSegmentControl
              value={preset}
              onChange={setPreset}
              disabled={state.kind === "loading"}
            />
          </div>
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
      {fillModal && (
        <FillMissingModal
          init={fillModal}
          status={fillStatus}
          watching={fillStatus !== null}
          logText={fillLogText}
          onWatch={handleStartWatching}
          onDismiss={handleDismissModal}
          onMinimize={() => setFillModal(null)}
          onCancel={async () => {
            // Real cancel — hit the DELETE endpoint to SIGTERM the
            // worker subprocess. Job stops where it is; user can
            // confirm via the next poll showing state=cancelled.
            if (!fillStatus?.job_id) return;
            try {
              await fetch(
                `${API_BASE}/api/indexer/coverage/fill-missing/${fillStatus.job_id}`,
                { method: "DELETE", credentials: "include" },
              );
            } catch (err) {
              setFillError(err instanceof Error ? err.message : String(err));
            }
          }}
        />
      )}
    </div>
  );
}

function FillMissingModal({
  init,
  status,
  watching,
  logText,
  onWatch,
  onDismiss,
  onMinimize,
  onCancel,
}: {
  init: FillModalInit;
  status: FillStatus | null;
  watching: boolean;
  logText: string;
  onWatch: () => void;
  onDismiss: () => void;
  onMinimize: () => void;
  onCancel: () => Promise<void>;
}) {
  // Phase derives from status: confirm before tracking, progress while
  // running, done after terminal. Confirm phase is the only one shown
  // pre-watch — other phases imply fillStatus is set and being polled.
  const isTerminal = status !== null &&
    (status.state === "done" || status.state === "failed" ||
     status.state === "completed_with_errors");
  const phase: "confirm" | "progress" | "done" =
    !watching ? "confirm"
    : isTerminal ? "done"
    : "progress";

  const elapsed = status?.elapsed_seconds ?? 0;
  const mm = Math.floor(elapsed / 60);
  const ss = String(elapsed % 60).padStart(2, "0");

  // Per-date progress lines. Prefer the live `status.dates` array (worker
  // updates it as metrics complete); fall back to init.dates with a
  // pending placeholder when the first poll hasn't landed yet.
  const dateRows = status?.dates ?? init.dates.map((d) => ({
    date: d, state: "pending" as const, metrics_done: [], metrics_failed: [],
  }));

  const titleLabel =
    phase === "confirm" ? "Fill Missing — Confirm" :
    phase === "progress" ? "Fill Missing — Running" :
    status?.state === "done" ? "Fill Missing — Done" :
    status?.state === "failed" ? "Fill Missing — Failed" :
    status?.state === "cancelled" ? "Fill Missing — Cancelled" :
    "Fill Missing — Done with Errors";

  const titleColor =
    phase === "done" && status?.state === "done" ? "var(--green)" :
    phase === "done" && status?.state === "failed" ? "var(--red)" :
    phase === "done" && status?.state === "cancelled" ? "var(--amber)" :
    phase === "done" ? "var(--amber)" :
    "var(--t3)";

  const [cancelling, setCancelling] = useState(false);
  async function handleCancelClick() {
    setCancelling(true);
    try { await onCancel(); } finally { /* polling will mark cancelled */ }
  }

  return (
    <div
      onClick={phase === "confirm" ? onDismiss : onMinimize}
      style={{
        position: "fixed", inset: 0,
        background: "rgba(0,0,0,0.7)",
        display: "flex", alignItems: "center", justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: "var(--bg2)",
          border: "1px solid var(--line2)",
          borderRadius: 6, padding: "20px 24px",
          width: 560, maxWidth: "92vw",
          maxHeight: "84vh", overflowY: "auto",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: titleColor, textTransform: "uppercase",
          marginBottom: 14,
        }}>
          {titleLabel}
        </div>

        {phase === "confirm" && (
          <>
            <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 12 }}>
              Found <span style={{ color: "var(--t0)", fontWeight: 700 }}>{init.dates.length}</span>{" "}
              partial day{init.dates.length === 1 ? "" : "s"} in <code style={{ color: "var(--t0)" }}>market.leaderboards</code>.
              The job has already been queued in the background.
            </div>
            <div style={{
              background: "var(--bg3)",
              border: "1px solid var(--line)",
              borderRadius: 4,
              padding: "10px 12px",
              marginBottom: 12,
              maxHeight: 180, overflowY: "auto",
            }}>
              {init.dates.map((d) => (
                <div key={d} style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.7 }}>
                  · {d}
                </div>
              ))}
            </div>
            <div style={{ fontSize: 10, color: "var(--t2)", marginBottom: 18 }}>
              Estimated time: ~{init.estMin} min (3 metrics × ~45s per day).
            </div>
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
              <ModalButton variant="ghost" onClick={onDismiss}>Run in background</ModalButton>
              <ModalButton variant="primary" onClick={onWatch}>Watch progress</ModalButton>
            </div>
          </>
        )}

        {phase === "progress" && (
          <>
            <div style={{
              display: "flex", alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 14, gap: 12,
            }}>
              <div style={{ fontSize: 11, color: "var(--t1)" }}>
                {status?.current_date ? (
                  <>
                    <span style={{ color: "var(--amber)", fontWeight: 700 }}>
                      {status.current_date}
                    </span>
                    {status.current_state && (
                      <span style={{ color: "var(--t2)" }}> · {status.current_state}</span>
                    )}
                  </>
                ) : status?.current_state === "refreshing_cagg" ? (
                  <span style={{ color: "var(--amber)" }}>refreshing continuous aggregate…</span>
                ) : (
                  <span style={{ color: "var(--t2)" }}>queued</span>
                )}
              </div>
              <div style={{
                fontSize: 10, color: "var(--t2)",
                fontVariantNumeric: "tabular-nums",
              }}>
                {mm}:{ss}
                {status && status.dates_total > 0 && (
                  <span style={{ marginLeft: 10, color: "var(--t3)" }}>
                    {status.dates_completed + status.dates_failed}/{status.dates_total}
                  </span>
                )}
              </div>
            </div>
            <ProgressList rows={dateRows} currentDate={status?.current_date ?? null} currentState={status?.current_state ?? null} />
            <LogConsole text={logText} />
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 18 }}>
              <ModalButton variant="danger" onClick={handleCancelClick} disabled={cancelling}>
                {cancelling ? "Cancelling…" : "Cancel job"}
              </ModalButton>
              <ModalButton variant="primary" onClick={onMinimize}>Minimize</ModalButton>
            </div>
          </>
        )}

        {phase === "done" && (
          <>
            <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 12 }}>
              {status?.summary || "Done."}
            </div>
            <ProgressList rows={dateRows} currentDate={null} currentState={null} />
            {status?.errors && status.errors.length > 0 && (
              <div style={{
                marginTop: 12,
                background: "var(--red-dim)",
                border: "1px solid var(--red)",
                borderRadius: 4,
                padding: "8px 12px",
                fontSize: 10,
                color: "var(--red)",
                maxHeight: 120, overflowY: "auto",
              }}>
                {status.errors.map((e, i) => (
                  <div key={i} style={{ lineHeight: 1.6 }}>· {e}</div>
                ))}
              </div>
            )}
            <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 18 }}>
              <ModalButton variant="primary" onClick={onMinimize}>Close</ModalButton>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function ProgressList({
  rows,
  currentDate,
  currentState,
}: {
  rows: { date: string; state: "pending" | "running" | "done" | "failed";
          metrics_done?: string[]; metrics_failed?: string[] }[];
  currentDate: string | null;
  currentState: string | null;
}) {
  return (
    <div style={{
      background: "var(--bg3)",
      border: "1px solid var(--line)",
      borderRadius: 4,
      padding: "10px 12px",
      maxHeight: 240, overflowY: "auto",
    }}>
      {rows.map((row) => {
        const isCurrent = row.date === currentDate;
        const dateColor =
          row.state === "done" ? "var(--green)" :
          row.state === "failed" ? "var(--red)" :
          isCurrent ? "var(--amber)" :
          "var(--t2)";
        const marker =
          row.state === "done" ? "✓" :
          row.state === "failed" ? "✗" :
          isCurrent ? "▸" :
          "·";
        return (
          <div key={row.date} style={{
            display: "flex", alignItems: "center",
            gap: 10, fontSize: 10, lineHeight: 1.9,
          }}>
            <span style={{ color: dateColor, width: 12 }}>{marker}</span>
            <span style={{ color: dateColor, minWidth: 88 }}>{row.date}</span>
            <span style={{ color: "var(--t3)", display: "inline-flex", gap: 6 }}>
              {METRICS.map((m) => {
                const done = row.metrics_done?.includes(m);
                const failed = row.metrics_failed?.includes(m);
                const running = isCurrent && currentState === `rebuilding ${m}`;
                const c = failed ? "var(--red)" :
                          done ? "var(--green)" :
                          running ? "var(--amber)" :
                          "var(--t3)";
                const sym = failed ? "✗" : done ? "✓" : running ? "▸" : "·";
                return (
                  <span key={m} style={{ color: c, display: "inline-flex", gap: 3 }}>
                    <span>{sym}</span>
                    <span>{METRIC_LABELS[m].toLowerCase()}</span>
                  </span>
                );
              })}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function LogConsole({ text }: { text: string }) {
  // Auto-scroll-to-bottom when sticky to the tail; lets the operator
  // scroll up to read older lines without the panel snapping back.
  // See compiler/coverage/page.tsx for the same component + comments.
  const ref = useRef<HTMLDivElement | null>(null);
  const stickRef = useRef(true);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (stickRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [text]);
  const onScroll = () => {
    const el = ref.current;
    if (!el) return;
    stickRef.current = el.scrollTop + el.clientHeight >= el.scrollHeight - 24;
  };
  if (!text) {
    return (
      <div
        style={{
          marginTop: 12,
          padding: "10px 12px",
          background: "var(--bg0)",
          border: "1px solid var(--line)",
          borderRadius: 4,
          fontSize: 9,
          color: "var(--t3)",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          fontStyle: "italic",
        }}
      >
        waiting for worker output…
      </div>
    );
  }
  return (
    <div
      ref={ref}
      onScroll={onScroll}
      style={{
        marginTop: 12,
        height: 200,
        overflowY: "auto",
        padding: "10px 12px",
        background: "var(--bg0)",
        border: "1px solid var(--line)",
        borderRadius: 4,
        fontSize: 9,
        lineHeight: 1.45,
        color: "var(--t1)",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        whiteSpace: "pre-wrap",
        wordBreak: "break-word",
      }}
    >
      {text}
    </div>
  );
}

function ModalButton({
  children,
  onClick,
  variant,
  disabled = false,
}: {
  children: React.ReactNode;
  onClick: () => void;
  variant: "primary" | "ghost" | "danger";
  disabled?: boolean;
}) {
  const palette = variant === "primary"
    ? { bg: "var(--green-dim)", bgHover: "var(--green-mid)", color: "var(--green)", border: "var(--green)" }
    : variant === "danger"
    ? { bg: "var(--red-dim)",   bgHover: "var(--red)",       color: "var(--red)",   border: "var(--red)" }
    : { bg: "var(--bg3)",       bgHover: "var(--bg4)",       color: "var(--t1)",    border: "var(--line)" };
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      style={{
        background: palette.bg,
        color: palette.color,
        border: `1px solid ${palette.border}`,
        borderRadius: 4,
        padding: "6px 14px",
        fontSize: 9, fontWeight: 700,
        letterSpacing: "0.12em",
        textTransform: "uppercase",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        cursor: disabled ? "not-allowed" : "pointer",
        opacity: disabled ? 0.5 : 1,
      }}
      onMouseEnter={(e) => {
        if (!disabled) e.currentTarget.style.background = palette.bgHover;
      }}
      onMouseLeave={(e) => {
        if (!disabled) e.currentTarget.style.background = palette.bg;
      }}
    >
      {children}
    </button>
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

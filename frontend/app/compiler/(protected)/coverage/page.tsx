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

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Skeleton from "../../../components/Skeleton";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Response shapes ─────────────────────────────────────────────────────────
// Mirror the FastAPI router exactly. If the backend shape changes these
// types must change too — they are the contract.

type EndpointCoverage = {
  close: number;
  volume: number;
  open_interest: number;
  funding_rate: number;
  long_short_ratio: number;
  market_cap_usd: number;
};

type DayStatus = "complete" | "partial" | "missing";

type CoverageDay = {
  date: string;                 // 'YYYY-MM-DD'
  symbols_complete: number;
  symbols_partial: number;
  symbols_with_data: number;
  symbols_missing: number;
  expected_symbols: number;     // per-day denominator (job truth or fallback)
  has_job_truth: boolean;       // true if expected_symbols came from compiler_jobs
  completeness_pct: number;     // legacy: symbols_complete / expected (close-only)
  min_critical_pct: number;     // min over critical endpoints (close/vol/OI/mcap)
  status: DayStatus;            // unified day health bucket
  endpoints: EndpointCoverage;  // per-endpoint pcts (close/volume/OI/funding/LS/mcap)
};

type CoverageResponse = {
  source_id: number;
  lookback_days: number;
  total_active_symbols: number;
  expected_today: number;       // latest day's expected_symbols (KPI headline)
  days_returned: number;
  days: CoverageDay[];
};

type GapDay = {
  date: string;
  symbols_complete: number;
  symbols_total: number;
  completeness_pct: number;
  has_job_truth: boolean;
  status: "missing" | "partial";
  endpoints: EndpointCoverage;
};

type GapsResponse = {
  source_id: number;
  lookback_days: number;
  total_active_symbols: number;
  expected_today: number;
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
const DEFAULT_PRESET: LookbackPreset = LOOKBACK_PRESETS[3]; // ALL

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; coverage: CoverageResponse; gaps: GapsResponse };

// ─── Heatmap thresholds ──────────────────────────────────────────────────────
// Mirror the backend day-status rule: a day is green if min(close, OI) > 90 pct
// across the day's expected symbols. amber for any non-zero data below that
// bar, red for zero. Keep this in sync with COMPLETE_THRESHOLD_PCT in
// backend/app/api/routes/compiler.py.

function heatmapColor(pct: number | null): { bg: string; border: string } {
  if (pct === null) {
    return { bg: "var(--bg2)", border: "var(--line)" };
  }
  if (pct > 90) {
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

function HeatmapCell({ day, onClick }: { day: CoverageDay; onClick: () => void }) {
  const colors = heatmapColor(day.min_critical_pct);
  const source = day.has_job_truth ? "job truth" : "fallback: symbols_with_data";
  const tooltip =
    `${day.date}\n` +
    `${day.status.toUpperCase()} · min critical ${day.min_critical_pct}%\n` +
    `close ${day.endpoints.close}% · vol ${day.endpoints.volume}% · OI ${day.endpoints.open_interest}% · mcap ${day.endpoints.market_cap_usd}%\n` +
    `expected ${day.expected_symbols} (${source})\n` +
    `Click to drill into this day`;
  return (
    <div
      title={tooltip}
      onClick={onClick}
      style={{
        width: 14,
        height: 14,
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: 2,
        cursor: "pointer",
        transition: "transform 0.1s ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.transform = "scale(1.15)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.transform = "scale(1)"; }}
    />
  );
}

function Heatmap({ days, onDayClick }: { days: CoverageDay[]; onDayClick: (date: string) => void }) {
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
          <HeatmapCell key={day.date} day={day} onClick={() => onDayClick(day.date)} />
        ))}
      </div>
      <div style={{
        display: "flex",
        alignItems: "center",
        gap: 14,
        fontSize: 9,
        color: "var(--t2)",
      }}>
        <LegendSwatch color="var(--green-mid)" border="var(--green)" label="> 90% complete" />
        <LegendSwatch color="var(--amber-dim)" border="var(--amber)" label="5–90% complete" />
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

function GapTable({ gaps, onDayClick }: { gaps: GapDay[]; onDayClick: (date: string) => void }) {
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
            <Th align="right">%</Th>
            <Th align="right" title={`Price · ${endpointDayCount(gaps, "close")} / ${gaps.length} days complete`}>Px</Th>
            <Th align="right" title={`Open Interest · ${endpointDayCount(gaps, "open_interest")} / ${gaps.length} days complete`}>OI</Th>
            <Th align="right" title={`Volume · ${endpointDayCount(gaps, "volume")} / ${gaps.length} days complete`}>Vol</Th>
            <Th align="right" title={`Daily Market Cap (CoinGecko → market.market_cap_daily) · ${endpointDayCount(gaps, "market_cap_usd")} / ${gaps.length} days complete`}>MC</Th>
          </tr>
        </thead>
        <tbody>
          {gaps.map((g) => (
            <tr
              key={g.date}
              onClick={() => onDayClick(g.date)}
              style={{
                borderTop: "1px solid var(--line)",
                cursor: "pointer",
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg3)"; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
              title="Click to drill into this day"
            >
              <Td>{g.date}</Td>
              <Td><StatusBadge status={g.status} /></Td>
              <Td align="right">{g.symbols_complete} / {g.symbols_total}</Td>
              <Td align="right">{g.completeness_pct.toFixed(1)}%</Td>
              <Td align="right"><EndpointPctBadge pct={g.endpoints?.close ?? 0} /></Td>
              <Td align="right"><EndpointPctBadge pct={g.endpoints?.open_interest ?? 0} /></Td>
              <Td align="right"><EndpointPctBadge pct={g.endpoints?.volume ?? 0} /></Td>
              <Td align="right"><EndpointPctBadge pct={g.endpoints?.market_cap_usd ?? 0} /></Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Count how many days in a list are "complete" (>= 95%) for a given
// endpoint. Used by column header tooltips so you can see the rolled-up
// "X / N days have full coverage" alongside the per-row percentages.
function endpointDayCount(gaps: GapDay[], key: keyof EndpointCoverage): number {
  return gaps.filter((g) => (g.endpoints?.[key] ?? 0) >= 95).length;
}

// Compact per-endpoint percentage badge — green/amber/red color-coded.
// Used in the Gap Days table and Days list page so a row scan immediately
// shows which endpoint is the limiting factor on a partial day.
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

function Th({ children, align = "left", title }: { children: React.ReactNode; align?: "left" | "right"; title?: string }) {
  return (
    <th title={title} style={{
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
  dates?: { date: string; state: "pending" | "deleting" | "fetching" | "done" | "failed" }[];
};

// Confirm-phase metadata captured from the POST response — passed into
// the modal so it can render the dates list + estimate before the user
// opts in to live tracking.
type FillModalInit = { dates: string[]; estMin: number };

export default function CompilerCoveragePage() {
  const router = useRouter();
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [preset, setPreset] = useState<LookbackPreset>(DEFAULT_PRESET);
  const [refreshNonce, setRefreshNonce] = useState(0);
  // fillStatus: null = idle. Otherwise the latest poll result.
  const [fillStatus, setFillStatus] = useState<FillStatus | null>(null);
  const [fillError, setFillError] = useState<string | null>(null);
  // Modal openness + the initial confirm-phase data (dates list + ETA).
  // null means closed.
  const [fillModal, setFillModal] = useState<FillModalInit | null>(null);
  const pendingFillJobIdRef = useRef<string | null>(null);
  const pendingFillDatesTotalRef = useRef<number>(0);
  // Live tail of the worker's metl log, streamed in via the new
  // /coverage/fill-missing/log endpoint at the same 3s cadence as
  // the status JSON. Stored as one text blob; the frontend renders
  // it inside the modal's progress phase as a scrolling console.
  const [fillLogText, setFillLogText] = useState<string>("");
  const fillLogOffsetRef = useRef<number>(0);
  // Disabled-button flag for the in-flight POST window so a double-
  // click can't race two submissions through before the first lands.
  const [postingFill, setPostingFill] = useState(false);

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
  }

  function handleDismissModal() {
    // Dismiss = "run in background" — close the modal but kick off
    // chip tracking so the operator still has a visible signal that
    // a worker is alive. Without this, the page looked idle right
    // after dismiss and a second Fill Missing click would race the
    // running worker (was a real footgun pre-2026-04-29).
    if (pendingFillJobIdRef.current && fillStatus === null) {
      handleStartWatching();
    }
    setFillModal(null);
    pendingFillJobIdRef.current = null;
  }

  const goToDay = (date: string) => router.push(`/compiler/days/${date}`);

  const fillIsTerminal = (s: FillStatus | null) =>
    s !== null && (s.state === "done" || s.state === "failed" ||
                   s.state === "completed_with_errors" ||
                   s.state === "cancelled");
  const fillIsRunning = (s: FillStatus | null) =>
    s !== null && (s.state === "queued" || s.state === "running");

  // Poll the worker's status JSON every 3s while the job is in-flight.
  // Stops when state becomes terminal. Restarts if a new job is kicked off.
  useEffect(() => {
    if (!fillStatus || fillIsTerminal(fillStatus)) return;
    const job_id = fillStatus.job_id;
    const interval = window.setInterval(async () => {
      try {
        const res = await fetch(
          `${API_BASE}/api/compiler/coverage/fill-missing/status?job_id=${job_id}`,
          { credentials: "include" },
        );
        if (res.status === 404 || res.status === 503) return; // brief race
        if (!res.ok) return;
        const next = (await res.json()) as FillStatus;
        setFillStatus(next);
        if (fillIsTerminal(next)) {
          // Refresh the page data once the job completes
          setRefreshNonce((n) => n + 1);
        }
      } catch {
        // network blips: ignore, keep polling
      }
    }, 3000);
    return () => window.clearInterval(interval);
  }, [fillStatus]);

  // Poll the worker's log file at the same cadence. Tails new bytes
  // since `fillLogOffsetRef` and appends to the in-memory text blob,
  // capping the blob to the last ~64KB so a long-running job doesn't
  // bloat memory. Resets when the watched job_id changes.
  useEffect(() => {
    if (!fillStatus) {
      setFillLogText("");
      fillLogOffsetRef.current = 0;
      return;
    }
    const job_id = fillStatus.job_id;
    // Reset on job_id change
    setFillLogText("");
    fillLogOffsetRef.current = 0;

    let cancelled = false;
    const tick = async () => {
      if (cancelled) return;
      try {
        const res = await fetch(
          `${API_BASE}/api/compiler/coverage/fill-missing/log?job_id=${job_id}&since=${fillLogOffsetRef.current}`,
          { credentials: "include" },
        );
        if (!res.ok) return;
        const body = await res.json() as
          { end: number; bytes: number; truncated: boolean; text: string };
        if (cancelled) return;
        fillLogOffsetRef.current = body.end;
        if (body.bytes > 0) {
          setFillLogText((prev) => {
            // If server told us it skipped a chunk, replace; otherwise append.
            const next = body.truncated ? body.text : prev + body.text;
            // Keep the tail bounded (~80KB) so the modal doesn't
            // grow without limit on a long job.
            const MAX = 80_000;
            return next.length > MAX ? next.slice(next.length - MAX) : next;
          });
        }
      } catch { /* swallow blips */ }
    };
    // Fire once immediately so the panel has content fast, then poll.
    void tick();
    const id = window.setInterval(tick, 3000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, [fillStatus?.job_id]);

  // On page load, check for an active fill job and rehydrate state.
  useEffect(() => {
    let cancelled = false;
    fetch(`${API_BASE}/api/compiler/coverage/fill-missing/active`,
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
    if (postingFill) return; // double-click guard
    setFillError(null);
    setPostingFill(true);

    // 1. Hit the endpoint with no `dates` arg — backend auto-detects gaps.
    //    The endpoint will (a) return done immediately with dates_total=0
    //    if no gaps, (b) return queued with a list of dates, or (c) 409
    //    if another job is already running — in that case we rehydrate
    //    from /status and reopen the modal in progress phase.
    let initial: { job_id: string; state: string; dates_total: number;
                   dates?: string[]; summary?: string };
    try {
      const res = await fetch(
        `${API_BASE}/api/compiler/coverage/fill-missing?days_back=30`,
        { method: "POST", credentials: "include" },
      );
      if (res.status === 409) {
        // Another job already running. Server returns the existing
        // job_id in the detail; pull the current status so the modal
        // re-attaches to it instead of showing a confusing error.
        try {
          const body = await res.json();
          const existingJobId = body?.detail?.job_id;
          if (existingJobId) {
            const sres = await fetch(
              `${API_BASE}/api/compiler/coverage/fill-missing/status?job_id=${existingJobId}`,
              { credentials: "include" },
            );
            if (sres.ok) {
              const live = (await sres.json()) as FillStatus;
              setFillStatus(live);
              // Open modal in progress phase by reusing the dates
              // already in the status payload. estMin is irrelevant
              // post-confirm; pass 0.
              setFillModal({
                dates: (live.dates ?? []).map((d) => d.date),
                estMin: 0,
              });
            }
          }
        } catch { /* fall through to generic */ }
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

    // 2. Zero-gap fast path: state=done, just show the summary.
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

    // 3. Gaps found — open the styled confirm modal. The worker is
    //    already running (POST kicked it off); the modal is informational.
    //    Watching starts the polling chip + live progress; Dismissing
    //    leaves the worker running detached.
    const dates = initial.dates || [];
    const estMin = Math.ceil((dates.length * 7) / 1) || 1; // ~7 min/day for metl
    setFillModal({ dates, estMin });
    pendingFillJobIdRef.current = initial.job_id;
    pendingFillDatesTotalRef.current = dates.length;
  }

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
  }, [preset, refreshNonce]);

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
                if (fillIsRunning(fillStatus)) {
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
                    : "Detect days with incomplete data in market.futures_1m, delete + re-run metl for each, then refresh the continuous aggregates. Today (UTC) is always excluded from auto-detect."
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
                cursor: (state.kind === "loading" || postingFill) ? "not-allowed" : "pointer",
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
                // Same intra-day % parse as the modal's ProgressMetrics
                // — keep the chip's number consistent with what the
                // operator sees when they re-open the modal.
                const overallPct = computeOverallPct(fillStatus, fillLogText);
                const pctStr = overallPct === null ? "" : `${overallPct.toFixed(0)}% · `;
                if (cur) {
                  return `${pctStr}${done + 1}/${total}: ${cur} ${mm}:${ss}`;
                }
                if (fillStatus.current_state === "refreshing_caggs") {
                  return `refreshing caggs ${mm}:${ss}`;
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

        {state.kind === "loading" && <CoverageSkeleton />}

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
          <CoverageContent coverage={state.coverage} gaps={state.gaps} onDayClick={goToDay} />
        )}
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
            // Real cancel — DELETE endpoint SIGTERMs the worker subprocess
            // (PID is stamped into status JSON at startup). Polling will
            // observe state=cancelled on the next tick.
            if (!fillStatus?.job_id) return;
            try {
              await fetch(
                `${API_BASE}/api/compiler/coverage/fill-missing/${fillStatus.job_id}`,
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

// Per-day-stage modal (compiler version: each day moves through
// pending → fetching → done/failed; no per-metric breakdown).
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
  // Live tail of the worker's metl log — rendered as a scrolling
  // console inside the progress phase so the operator can see
  // per-symbol fetch progress as it happens.
  logText: string;
  onWatch: () => void;
  onDismiss: () => void;
  onMinimize: () => void;
  onCancel: () => Promise<void>;
}) {
  const isTerminal = status !== null &&
    (status.state === "done" || status.state === "failed" ||
     status.state === "completed_with_errors" ||
     status.state === "cancelled");
  const phase: "confirm" | "progress" | "done" =
    !watching ? "confirm"
    : isTerminal ? "done"
    : "progress";

  const elapsed = status?.elapsed_seconds ?? 0;
  const mm = Math.floor(elapsed / 60);
  const ss = String(elapsed % 60).padStart(2, "0");

  const dateRows = status?.dates ?? init.dates.map((d) => ({
    date: d, state: "pending" as const,
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
              partial day{init.dates.length === 1 ? "" : "s"} in{" "}
              <code style={{ color: "var(--t0)" }}>market.futures_1m</code>.
              For each, the cached CSV is deleted (forces metl to re-fetch
              from Amberdata) and missing rows are inserted with
              ON&nbsp;CONFLICT&nbsp;DO&nbsp;NOTHING — existing rows stay
              intact if a fetch fails partway.
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
              Estimated time: ~{init.estMin} min (~7 min per day for metl re-fetch).
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
                ) : status?.current_state === "refreshing_caggs" ? (
                  <span style={{ color: "var(--amber)" }}>refreshing continuous aggregates…</span>
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
            <ProgressMetrics
              status={status}
              logText={logText}
              elapsedSec={elapsed}
            />
            <DayProgressList rows={dateRows} currentDate={status?.current_date ?? null} />
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
            <DayProgressList rows={dateRows} currentDate={null} />
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

function DayProgressList({
  rows,
  currentDate,
}: {
  rows: { date: string; state: "pending" | "deleting" | "fetching" | "done" | "failed" }[];
  currentDate: string | null;
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
        const c =
          row.state === "done" ? "var(--green)" :
          row.state === "failed" ? "var(--red)" :
          isCurrent || row.state === "deleting" || row.state === "fetching" ? "var(--amber)" :
          "var(--t2)";
        const marker =
          row.state === "done" ? "✓" :
          row.state === "failed" ? "✗" :
          isCurrent || row.state === "deleting" || row.state === "fetching" ? "▸" :
          "·";
        return (
          <div key={row.date} style={{
            display: "flex", alignItems: "center",
            gap: 10, fontSize: 10, lineHeight: 1.9,
          }}>
            <span style={{ color: c, width: 12 }}>{marker}</span>
            <span style={{ color: c, minWidth: 88 }}>{row.date}</span>
            <span style={{ color: "var(--t3)" }}>
              {row.state === "pending" ? "" : row.state}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function LogConsole({ text }: { text: string }) {
  // Auto-scroll to bottom on every text update unless the operator
  // has scrolled up — that "stuck to bottom" UX matches what tail -f
  // feels like in a terminal. Sticky-bottom is detected by checking
  // whether the prior render had the scroll within ~24px of the end.
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
    // Empty placeholder while the worker hasn't logged anything yet —
    // gives the panel a stable footprint so the modal doesn't jump
    // when the first chunk arrives.
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

// Two-tier progress calc shared by ProgressMetrics (modal) and the
// chip's running label (page header). Returns null when there's no
// running job to compute against.
//
//   1. Day count from /status (dates_completed / dates_total)
//   2. Within the in-flight day, parse the latest `(N.N%)` from the
//      streamed metl log — metl prints this per symbol, e.g.
//      "✅ BTCUSDT done → 0:00:07.776 (22.07%)".
//
// Then: overallPct = (days_done + intraDayPct/100) / total_days * 100
function computeOverallPct(
  status: FillStatus | null,
  logText: string,
): number | null {
  if (!status) return null;
  const total = status.dates_total;
  if (total <= 0) return null;
  const daysDone = status.dates_completed + status.dates_failed;

  let intraDayPct: number | null = null;
  if (logText) {
    const matches = logText.match(/\(([\d.]+)%\)/g);
    if (matches && matches.length > 0) {
      const last = matches[matches.length - 1];
      const m = last.match(/\(([\d.]+)%\)/);
      if (m) {
        const v = Number(m[1]);
        if (!Number.isNaN(v) && v >= 0 && v <= 100) intraDayPct = v;
      }
    }
  }
  const intraDayFrac = intraDayPct === null ? 0 : intraDayPct / 100;

  // Don't double-count: between-day moments still have last day's
  // 100% in the log. Only credit intra-day when current_date is set
  // and we're actively running.
  const isMidDay = !!status.current_date &&
                   status.state === "running" &&
                   status.current_state !== "refreshing_caggs";
  const completedFraction = (daysDone + (isMidDay ? intraDayFrac : 0)) / total;
  return Math.min(100, Math.max(0, completedFraction * 100));
}

function ProgressMetrics({
  status,
  logText,
  elapsedSec,
}: {
  status: FillStatus | null;
  logText: string;
  elapsedSec: number;
}) {
  if (!status) return null;
  const overallPctRaw = computeOverallPct(status, logText);
  const overallPct = overallPctRaw ?? 0;
  const remainingPct = 100 - overallPct;

  // ETA: linear extrapolation. Suppressed when:
  //   - <10s elapsed (regression too noisy)
  //   - <2% complete (denominator too small, ETA jitters wildly)
  //   - we're on the cagg-refresh tail (no log percentages to project)
  let etaText: string | null = null;
  if (elapsedSec >= 10 && overallPct >= 2 && overallPct < 100) {
    const remainingSec = elapsedSec * (100 - overallPct) / overallPct;
    if (Number.isFinite(remainingSec) && remainingSec >= 0) {
      const rmm = Math.floor(remainingSec / 60);
      const rss = Math.floor(remainingSec % 60);
      etaText = rmm > 0 ? `${rmm}m ${rss}s` : `${rss}s`;
    }
  }

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr 1fr",
        gap: 12,
        marginBottom: 12,
        padding: "10px 12px",
        background: "var(--bg3)",
        border: "1px solid var(--line)",
        borderRadius: 4,
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        fontVariantNumeric: "tabular-nums",
      }}
    >
      <Metric label="DONE" value={`${overallPct.toFixed(1)}%`} color="var(--green)" />
      <Metric label="REMAINING" value={`${remainingPct.toFixed(1)}%`} color="var(--amber)" />
      <Metric
        label="ETA"
        value={etaText ?? "computing…"}
        color={etaText ? "var(--t1)" : "var(--t3)"}
      />
    </div>
  );
}

function Metric({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <span style={{
        fontSize: 8, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase",
      }}>
        {label}
      </span>
      <span style={{ fontSize: 13, fontWeight: 700, color }}>
        {value}
      </span>
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

function CoverageContent({ coverage, gaps, onDayClick }: { coverage: CoverageResponse; gaps: GapsResponse; onDayClick: (date: string) => void }) {
  // KPI math sources from the unified per-day status the backend computes
  // (status is the bucket where min over critical endpoints — close, vol,
  // OI, mcap — clears the 95% threshold). complete + partial + missing
  // adds up to days_returned exactly.
  const expectedToday = coverage.expected_today;
  const daysComplete = coverage.days.filter((d) => d.status === "complete").length;
  const daysWithGaps = coverage.days.filter((d) => d.status === "partial").length;
  const daysMissing = coverage.days.filter((d) => d.status === "missing").length;

  // Headline coverage % is the average of min_critical_pct across the
  // window — answers "on the average day, what fraction of the universe
  // is fully usable by the simulator." Honest because it factors in
  // mcap; the old close-only weighted average overstated health.
  const coveragePct = coverage.days.length > 0
    ? coverage.days.reduce((sum, d) => sum + d.min_critical_pct, 0) / coverage.days.length
    : 0;

  return (
    <>
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, 1fr)",
        gap: 10,
        marginBottom: 24,
      }}>
        <KPICard
          label="Coverage"
          value={`${coveragePct.toFixed(1)}%`}
          hint={`avg min(close/OI) across ${coverage.days_returned} days`}
        />
        <KPICard
          label="Expected Today"
          value={expectedToday.toLocaleString("en-US")}
          hint="symbols Binance listed on latest day"
        />
        <KPICard
          label="Days Complete"
          value={daysComplete.toLocaleString("en-US")}
          hint={`of ${coverage.days_returned} days · min(close, OI) > 90%`}
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
        <Heatmap days={coverage.days} onDayClick={onDayClick} />
      </div>

      <GapTable gaps={gaps.gaps} onDayClick={onDayClick} />
    </>
  );
}

// ─── Skeleton (loading placeholder) ─────────────────────────────────────────

function CoverageSkeleton() {
  // 5 KPI cards + heatmap card + gap table card. Same dimensions/spacing
  // as the real components so the page doesn't reflow when data lands.
  return (
    <>
      {/* KPI strip */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, 1fr)",
        gap: 10,
        marginBottom: 24,
      }}>
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} style={{
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 6,
            padding: "14px 16px",
            display: "flex",
            flexDirection: "column",
            gap: 8,
          }}>
            <Skeleton width={70} height={9} />
            <Skeleton width={90} height={20} />
            <Skeleton width={120} height={9} />
          </div>
        ))}
      </div>

      {/* Heatmap card */}
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "16px 18px",
        marginBottom: 24,
      }}>
        <Skeleton width={180} height={9} style={{ marginBottom: 14 }} />
        <div style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 4,
          marginBottom: 14,
        }}>
          {Array.from({ length: 90 }).map((_, i) => (
            <Skeleton key={i} width={14} height={14} borderRadius={2} />
          ))}
        </div>
        <div style={{ display: "flex", gap: 14 }}>
          <Skeleton width={100} height={9} />
          <Skeleton width={100} height={9} />
          <Skeleton width={100} height={9} />
        </div>
      </div>

      {/* Gap table card */}
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "16px 18px",
      }}>
        <Skeleton width={140} height={9} style={{ marginBottom: 14 }} />
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} style={{
            display: "flex",
            alignItems: "center",
            gap: 16,
            paddingTop: 10,
            paddingBottom: 10,
            borderTop: i === 0 ? "none" : "1px solid var(--line)",
          }}>
            <Skeleton width={80} height={10} />
            <Skeleton width={60} height={14} borderRadius={3} />
            <div style={{ flex: 1 }} />
            <Skeleton width={70} height={10} />
            <Skeleton width={50} height={10} />
          </div>
        ))}
      </div>
    </>
  );
}

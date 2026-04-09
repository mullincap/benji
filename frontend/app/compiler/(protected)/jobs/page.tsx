"use client";

/**
 * frontend/app/compiler/(protected)/jobs/page.tsx
 * ===============================================
 * Compiler Jobs page — read-only monitor for market.compiler_jobs runs.
 *
 * Data source:
 *   GET /api/compiler/jobs?limit=50  (sorted DESC by created_at)
 *
 * Polling behavior:
 *   - Initial fetch on mount
 *   - If any job in the latest batch has status === "running", schedule the
 *     next fetch in 10 seconds. Otherwise stop polling until the user navigates.
 *   - Pauses entirely when document.visibilityState === "hidden". Resumes with
 *     an immediate fetch when the tab becomes visible again.
 *   - is_stale jobs (running but no heartbeat for 2h) still trigger polling —
 *     a stale job might catch up, and the user wants to see that update live.
 *
 * Live duration ticker:
 *   - Independent 1s setInterval that bumps a `nowMs` state, ONLY active when
 *     the tab is visible AND at least one job is running.
 *   - The duration cells read `nowMs - started_at` so they tick up live for
 *     in-flight jobs without needing to refetch the API every second.
 *
 * Status badges:
 *   COMPLETE   → --green
 *   RUNNING    → --amber + pulsing dot (uses globals.css @keyframes pulse-dot)
 *   FAILED     → --red
 *   STALE      → --red with explicit "STALE" label (overrides RUNNING)
 *   QUEUED     → --t2
 *   CANCELLED  → --t2
 */

import { useCallback, useEffect, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const POLL_INTERVAL_MS = 10_000;

// ─── Source registry ────────────────────────────────────────────────────────
// Hardcoded mirror of market.sources. The table is essentially static (6 rows,
// changes once per year at most). If we ever need it dynamic, expose
// GET /api/compiler/sources and replace this with a fetch.
const SOURCE_NAMES: Record<number, string> = {
  1: "amberdata_binance",
  2: "binance_direct",
  3: "blofin_direct",
  4: "coingecko",
  5: "amberdata_spot",
  6: "amberdata_options",
};

// ─── Response types ─────────────────────────────────────────────────────────
// Mirrors the FastAPI router exactly. See backend/app/api/routes/compiler.py
// _serialize_job() — these types are the contract.

type JobStatus = "queued" | "running" | "complete" | "failed" | "cancelled";

type CompilerJob = {
  job_id: string;
  source_id: number;
  status: JobStatus;
  date_from: string | null;
  date_to: string | null;
  endpoints_enabled: string[] | null;
  symbols_total: number | null;
  symbols_done: number | null;
  rows_written: number | null;
  started_at: string | null;
  completed_at: string | null;
  last_heartbeat: string | null;
  error_msg: string | null;
  triggered_by: string | null;
  run_tag: string | null;
  created_at: string | null;
  is_stale: boolean;
};

type JobsResponse = {
  jobs_returned: number;
  jobs: CompilerJob[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; jobs: CompilerJob[] };

// ─── Duration formatter ─────────────────────────────────────────────────────
// Format relative time spans the way the build doc shows them in smoke test
// results: "12s" / "2m 14s" / "1h 22m" / "1d 4h". The biggest unit always
// shows two parts of detail, smaller units show whole seconds.

function formatDuration(ms: number): string {
  if (ms < 0) return "—";
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remSec = seconds % 60;
  if (minutes < 60) return remSec === 0 ? `${minutes}m` : `${minutes}m ${remSec}s`;
  const hours = Math.floor(minutes / 60);
  const remMin = minutes % 60;
  if (hours < 24) return remMin === 0 ? `${hours}h` : `${hours}h ${remMin}m`;
  const days = Math.floor(hours / 24);
  const remHr = hours % 24;
  return remHr === 0 ? `${days}d` : `${days}d ${remHr}h`;
}

function jobDuration(job: CompilerJob, nowMs: number): string {
  if (!job.started_at) return "—";
  const startMs = Date.parse(job.started_at);
  if (Number.isNaN(startMs)) return "—";
  const endMs = job.completed_at ? Date.parse(job.completed_at) : nowMs;
  if (Number.isNaN(endMs)) return "—";
  return formatDuration(endMs - startMs);
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

function StatusBadge({ job }: { job: CompilerJob }) {
  // Stale takes precedence over running
  const display: { label: string; color: string; bg: string; pulse: boolean } = (() => {
    if (job.is_stale) {
      return { label: "STALE", color: "var(--red)", bg: "var(--red-dim)", pulse: false };
    }
    switch (job.status) {
      case "complete":
        return { label: "COMPLETE", color: "var(--green)", bg: "var(--green-dim)", pulse: false };
      case "running":
        return { label: "RUNNING", color: "var(--amber)", bg: "var(--amber-dim)", pulse: true };
      case "failed":
        return { label: "FAILED", color: "var(--red)", bg: "var(--red-dim)", pulse: false };
      case "cancelled":
        return { label: "CANCELLED", color: "var(--t2)", bg: "var(--bg3)", pulse: false };
      case "queued":
      default:
        return { label: "QUEUED", color: "var(--t2)", bg: "var(--bg3)", pulse: false };
    }
  })();

  return (
    <span style={{
      display: "inline-flex",
      alignItems: "center",
      gap: 5,
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: display.color,
      background: display.bg,
      border: `1px solid ${display.color}`,
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      {display.pulse && (
        <span style={{
          width: 5, height: 5,
          borderRadius: "50%",
          background: display.color,
          animation: "pulse-dot 1.4s ease-in-out infinite",
        }} />
      )}
      {display.label}
    </span>
  );
}

function ProgressBar({ done, total }: { done: number; total: number }) {
  if (total <= 0) {
    return <span style={{ color: "var(--t3)" }}>—</span>;
  }
  const pct = Math.min(100, (done / total) * 100);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, minWidth: 120 }}>
      <div style={{
        flex: 1,
        height: 4,
        background: "var(--bg3)",
        borderRadius: 2,
        overflow: "hidden",
      }}>
        <div style={{
          height: "100%",
          width: `${pct}%`,
          background: "var(--amber)",
          borderRadius: 2,
          transition: "width 0.3s ease",
        }} />
      </div>
      <span style={{ fontSize: 9, color: "var(--t2)", whiteSpace: "nowrap" }}>
        {done.toLocaleString("en-US")} / {total.toLocaleString("en-US")}
      </span>
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
      borderBottom: "1px solid var(--line)",
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
      padding: "10px 10px",
      verticalAlign: "middle",
    }}>
      {children}
    </td>
  );
}

function JobRow({ job, nowMs }: { job: CompilerJob; nowMs: number }) {
  const sourceName = SOURCE_NAMES[job.source_id] ?? `source ${job.source_id}`;
  const dateRange =
    job.date_from && job.date_to
      ? job.date_from === job.date_to
        ? job.date_from
        : `${job.date_from} → ${job.date_to}`
      : "—";
  const done = job.symbols_done ?? 0;
  const total = job.symbols_total ?? 0;
  const rowsWritten = (job.rows_written ?? 0).toLocaleString("en-US");
  const triggeredBy = job.triggered_by ?? "—";
  const isInFlight = job.status === "running";

  return (
    <tr style={{ borderTop: "1px solid var(--line)" }}>
      <Td>{dateRange}</Td>
      <Td>{sourceName}</Td>
      <Td><StatusBadge job={job} /></Td>
      <Td>
        {isInFlight
          ? <ProgressBar done={done} total={total} />
          : <span>{done.toLocaleString("en-US")} / {total.toLocaleString("en-US")}</span>}
      </Td>
      <Td align="right">{rowsWritten}</Td>
      <Td align="right">{jobDuration(job, nowMs)}</Td>
      <Td>{triggeredBy}</Td>
    </tr>
  );
}

// ─── Run panel — POST /api/compiler/runs + recent runs from /runs ─────────
// Generic UI surface for triggering pipeline scripts. Shipped scripts:
//   - metl                 → Amberdata ETL (metl.py --start <yesterday>)
//   - coingecko_marketcap  → Daily marketcap snapshot
//   - backfill_futures_1m  → Bulk loader from master parquet (slow!)
// Each click opens a confirmation modal. Recent runs panel polls every 10s
// while any run is active so the user can watch status without leaving
// the page.

type RunRow = {
  run_id:       string;
  script_name:  string;
  module:       string;
  status:       "queued" | "running" | "complete" | "failed" | "cancelled";
  triggered_by: string;
  params:       Record<string, unknown>;
  started_at:   string | null;
  completed_at: string | null;
  exit_code:    number | null;
  rows_written: number;
  error_msg:    string | null;
  stdout_tail:  string | null;
  stderr_tail:  string | null;
  created_at:   string | null;
};

const RUN_BUTTONS: { script: string; label: string; warning: string }[] = [
  {
    script: "metl",
    label:  "Run Amberdata ETL",
    warning: "Runs metl.py for yesterday's date. Reads from Amberdata API, writes to market.futures_1m. Typically completes in ~10–20 min. Idempotent — re-running is safe.",
  },
  {
    script: "coingecko_marketcap",
    label:  "Run CoinGecko Daily",
    warning: "Runs coingecko_marketcap.py --mode daily. Fetches the top 2000 coin universe and writes a daily snapshot to /mnt/quant-data/raw/coingecko. Takes ~2 min. Safe to re-run.",
  },
  {
    script: "backfill_futures_1m",
    label:  "Backfill futures_1m",
    warning: "⚠ HEAVY OPERATION. Runs backfill_futures_1m.py against the full master parquet (~312M rows). Can take HOURS and will compete with the simulator + other queries for DB resources. Only run if you know futures_1m has missing data. Idempotent (ON CONFLICT DO NOTHING) but expensive.",
  },
];

function RunPanel() {
  const [pending, setPending] = useState<{ script: string; warning: string } | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [runs, setRuns] = useState<RunRow[]>([]);

  const fetchRuns = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/compiler/runs?limit=10`, { credentials: "include" });
      if (!res.ok) return;
      const data = await res.json();
      setRuns(data.runs || []);
    } catch {
      // ignore — non-fatal
    }
  }, []);

  // Initial load + 10s poll while any run is active
  useEffect(() => {
    let cancelled = false;
    let timer: number | null = null;
    async function loop() {
      if (cancelled) return;
      await fetchRuns();
      const anyActive = runs.some((r) => r.status === "running" || r.status === "queued");
      timer = window.setTimeout(loop, anyActive ? 5000 : 30000);
    }
    loop();
    return () => {
      cancelled = true;
      if (timer !== null) window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fetchRuns]);

  async function trigger(script: string) {
    setSubmitting(true);
    setFeedback(null);
    try {
      const res = await fetch(`${API_BASE}/api/compiler/runs`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ script, params: {} }),
      });
      if (res.status === 401) {
        setFeedback("Session expired. Please log in again.");
        return;
      }
      if (!res.ok) {
        const txt = await res.text();
        setFeedback(`POST returned ${res.status}: ${txt.slice(0, 200)}`);
        return;
      }
      const data = await res.json();
      setFeedback(`✓ Enqueued ${script} — task ${data.celery_task_id?.slice(0, 8) ?? "?"}…`);
      await fetchRuns();
    } catch (err) {
      setFeedback(`Network error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setSubmitting(false);
      setPending(null);
    }
  }

  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "14px 16px",
      marginBottom: 16,
    }}>
      <div style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase",
        marginBottom: 10,
      }}>
        Run Manager
      </div>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginBottom: 12 }}>
        {RUN_BUTTONS.map((b) => (
          <button
            key={b.script}
            type="button"
            disabled={submitting}
            onClick={() => setPending({ script: b.script, warning: b.warning })}
            style={{
              background: "var(--bg3)",
              border: "1px solid var(--line2)",
              borderRadius: 4,
              padding: "8px 14px",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: "var(--t1)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              transition: "all 0.15s ease",
            }}
            onMouseEnter={(e) => { if (!submitting) e.currentTarget.style.color = "var(--t0)"; }}
            onMouseLeave={(e) => { if (!submitting) e.currentTarget.style.color = "var(--t1)"; }}
          >
            {b.label}
          </button>
        ))}
        {feedback && (
          <span style={{
            fontSize: 10,
            color: feedback.startsWith("✓") ? "var(--green)" : "var(--red)",
            marginLeft: 4,
          }}>
            {feedback}
          </span>
        )}
      </div>

      {runs.length > 0 && (
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 6,
        }}>
          Recent Runs
        </div>
      )}
      {runs.length > 0 && (
        <table style={{
          width: "100%",
          borderCollapse: "collapse",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}>
          <thead>
            <tr>
              <Th>Script</Th>
              <Th>Status</Th>
              <Th align="right">Exit</Th>
              <Th>Started</Th>
              <Th>Triggered</Th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.run_id} style={{ borderTop: "1px solid var(--line)" }}>
                <Td>{r.script_name}</Td>
                <Td><RunStatusBadge status={r.status} /></Td>
                <Td align="right">{r.exit_code !== null ? r.exit_code : "—"}</Td>
                <Td>{r.started_at ? r.started_at.slice(11, 19) + " UTC" : "—"}</Td>
                <Td>{r.triggered_by}</Td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {pending && (
        <RunConfirmModal
          script={pending.script}
          warning={pending.warning}
          submitting={submitting}
          onCancel={() => setPending(null)}
          onConfirm={() => trigger(pending.script)}
        />
      )}
    </div>
  );
}

function RunStatusBadge({ status }: { status: RunRow["status"] }) {
  const map: Record<RunRow["status"], { color: string; bg: string }> = {
    running:   { color: "var(--amber)", bg: "var(--amber-dim)" },
    queued:    { color: "var(--t2)",    bg: "var(--bg3)" },
    complete:  { color: "var(--green)", bg: "var(--green-dim)" },
    failed:    { color: "var(--red)",   bg: "var(--red-dim)" },
    cancelled: { color: "var(--t2)",    bg: "var(--bg3)" },
  };
  const c = map[status] ?? map.queued;
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: c.color,
      background: c.bg,
      border: `1px solid ${c.color}`,
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      {status}
    </span>
  );
}

function RunConfirmModal({
  script, warning, submitting, onCancel, onConfirm,
}: {
  script: string;
  warning: string;
  submitting: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={onCancel}
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
          borderRadius: 6,
          padding: "20px 24px",
          maxWidth: 480,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 10,
        }}>
          Confirm Run
        </div>
        <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
          {warning}
        </div>
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
          <button
            type="button"
            disabled={submitting}
            onClick={onCancel}
            style={{
              background: "transparent",
              border: "1px solid var(--line2)",
              borderRadius: 4,
              padding: "8px 16px",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: "var(--t2)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={submitting}
            onClick={onConfirm}
            style={{
              background: "var(--amber-dim)",
              border: "1px solid var(--amber)",
              borderRadius: 4,
              padding: "8px 16px",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase",
              color: "var(--amber)",
              cursor: submitting ? "not-allowed" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            {submitting ? "Enqueueing…" : `Run ${script}`}
          </button>
        </div>
      </div>
    </div>
  );
}

function JobsTable({ jobs, nowMs }: { jobs: CompilerJob[]; nowMs: number }) {
  if (jobs.length === 0) {
    return (
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "20px 24px",
        fontSize: 10,
        color: "var(--t2)",
      }}>
        No compiler jobs have run yet. Trigger one via{" "}
        <code style={{ color: "var(--t1)" }}>
          python pipeline/compiler/metl.py --start &lt;date&gt; --end &lt;date&gt;
        </code>{" "}
        on the server.
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
          <tr>
            <Th>Date Range</Th>
            <Th>Source</Th>
            <Th>Status</Th>
            <Th>Symbols Done / Total</Th>
            <Th align="right">Rows Written</Th>
            <Th align="right">Duration</Th>
            <Th>Triggered By</Th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <JobRow key={job.job_id} job={job} nowMs={nowMs} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function CompilerJobsPage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [nowMs, setNowMs] = useState(() => Date.now());
  const pollTimerRef = useRef<number | null>(null);
  const tickerRef = useRef<number | null>(null);

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/compiler/jobs?limit=50`, {
        credentials: "include",
      });
      if (res.status === 401) {
        setState({ kind: "error", message: "Session expired. Please log in again." });
        return null;
      }
      if (!res.ok) {
        setState({ kind: "error", message: `Jobs endpoint returned ${res.status}` });
        return null;
      }
      const data = (await res.json()) as JobsResponse;
      setState({ kind: "ready", jobs: data.jobs });
      setNowMs(Date.now());
      return data.jobs;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setState({ kind: "error", message: `Network error: ${message}` });
      return null;
    }
  }, []);

  // Polling loop: re-fetch every 10s if (a) any job is running and
  // (b) the tab is visible. Pauses entirely on tab hide; resumes with an
  // immediate fetch on tab show.
  useEffect(() => {
    let cancelled = false;

    function clearPoll() {
      if (pollTimerRef.current !== null) {
        window.clearTimeout(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    }

    function clearTicker() {
      if (tickerRef.current !== null) {
        window.clearInterval(tickerRef.current);
        tickerRef.current = null;
      }
    }

    function schedulePollIfNeeded(jobs: CompilerJob[] | null) {
      clearPoll();
      if (cancelled) return;
      if (document.visibilityState === "hidden") return;
      if (!jobs) return;
      const anyRunning = jobs.some((j) => j.status === "running");
      if (!anyRunning) {
        clearTicker();
        return;
      }
      // Live elapsed-time ticker — bump nowMs every second so duration
      // cells re-render. Only runs while visible AND running.
      if (tickerRef.current === null) {
        tickerRef.current = window.setInterval(() => {
          if (document.visibilityState === "visible") {
            setNowMs(Date.now());
          }
        }, 1000);
      }
      pollTimerRef.current = window.setTimeout(async () => {
        const next = await fetchJobs();
        schedulePollIfNeeded(next);
      }, POLL_INTERVAL_MS);
    }

    async function initialLoad() {
      const jobs = await fetchJobs();
      schedulePollIfNeeded(jobs);
    }

    function onVisibilityChange() {
      if (document.visibilityState === "visible") {
        // Tab came back into focus — fetch fresh and resume polling
        fetchJobs().then((jobs) => schedulePollIfNeeded(jobs));
      } else {
        // Tab hidden — stop both timers
        clearPoll();
        clearTicker();
      }
    }

    initialLoad();
    document.addEventListener("visibilitychange", onVisibilityChange);

    return () => {
      cancelled = true;
      clearPoll();
      clearTicker();
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [fetchJobs]);

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <SectionLabel>Compiler · Jobs</SectionLabel>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 16,
          letterSpacing: "-0.01em",
        }}>
          Job Runs
        </h1>

        <RunPanel />

        {state.kind === "loading" && (
          <div style={{
            fontSize: 9, color: "var(--t3)",
            textTransform: "uppercase", letterSpacing: "0.12em",
            padding: "40px 0",
          }}>
            Loading jobs…
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
          <JobsTable jobs={state.jobs} nowMs={nowMs} />
        )}
      </div>
    </div>
  );
}

"use client";

/**
 * frontend/app/indexer/(protected)/jobs/page.tsx
 * ==============================================
 * Indexer Jobs page — read-only monitor for market.indexer_jobs runs.
 *
 * Data source:
 *   GET /api/indexer/jobs?limit=50  (sorted DESC by created_at)
 *
 * Polling, ticker, status badge, duration formatter — all lifted verbatim
 * from frontend/app/compiler/(protected)/jobs/page.tsx so the two pages
 * stay structurally identical and the user gets the same UX. Only the
 * row-shape and the job_type filter chips are indexer-specific.
 *
 * Polling behavior:
 *   - Initial fetch on mount
 *   - If any job has status === "running", schedule the next fetch in 10s
 *   - Pauses entirely when document.visibilityState === "hidden", resumes
 *     with an immediate fetch when the tab becomes visible again
 *   - Stale running jobs still trigger polling — they may catch up
 *
 * Live duration ticker: 1s setInterval bumps `nowMs` so duration cells
 * tick up live for in-flight jobs without re-hitting the API.
 *
 * Empty state: market.indexer_jobs is currently 0 rows because no script
 * writes to it yet (see Phase 0 cron diagnostic in the build doc). The
 * empty-state message explains this honestly rather than implying user error.
 *
 * Filter chips: ALL · LEADERBOARD · OVERLAP · FULL — client-side only,
 * no API round-trip per chip.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const POLL_INTERVAL_MS = 10_000;

// ─── Response types ─────────────────────────────────────────────────────────
// Mirror backend/app/api/routes/indexer.py _serialize_indexer_job()

type JobStatus = "queued" | "running" | "complete" | "failed" | "cancelled";
type JobType = "leaderboard" | "overlap" | "full";

type IndexerJob = {
  job_id: string;
  job_type: JobType;
  status: JobStatus;
  metric: string | null;
  date_from: string | null;
  date_to: string | null;
  params: Record<string, unknown> | null;
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
  jobs: IndexerJob[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; jobs: IndexerJob[] };

type FilterKey = "all" | JobType;

const FILTER_CHIPS: { key: FilterKey; label: string }[] = [
  { key: "all",         label: "All" },
  { key: "leaderboard", label: "Leaderboard" },
  { key: "overlap",     label: "Overlap" },
  { key: "full",        label: "Full" },
];

// ─── Duration formatter (lifted from compiler/jobs) ─────────────────────────

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

function jobDuration(job: IndexerJob, nowMs: number): string {
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

function StatusBadge({ job }: { job: IndexerJob }) {
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

function JobTypeBadge({ jobType }: { jobType: JobType }) {
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: "var(--t1)",
      background: "var(--bg3)",
      border: "1px solid var(--line2)",
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      {jobType}
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

function FilterChip({
  label,
  active,
  count,
  onClick,
}: {
  label: string;
  active: boolean;
  count: number;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        background: active ? "var(--bg4)" : "var(--bg2)",
        border: `1px solid ${active ? "var(--module-accent)" : "var(--line)"}`,
        borderRadius: 4,
        padding: "6px 12px",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.08em",
        textTransform: "uppercase",
        color: active ? "var(--t0)" : "var(--t2)",
        cursor: "pointer",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        transition: "all 0.15s ease",
      }}
      onMouseEnter={(e) => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
      onMouseLeave={(e) => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
    >
      <span>{label}</span>
      <span style={{
        fontSize: 9,
        color: active ? "var(--t1)" : "var(--t3)",
        fontWeight: 400,
      }}>
        {count}
      </span>
    </button>
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

function JobRow({ job, nowMs }: { job: IndexerJob; nowMs: number }) {
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
      <Td><JobTypeBadge jobType={job.job_type} /></Td>
      <Td>{job.metric ?? "—"}</Td>
      <Td>{dateRange}</Td>
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

// ─── Backfill panel — POST /api/indexer/runs ────────────────────────────────

const BACKFILL_METRICS: { metric: "price" | "open_interest" | "volume"; label: string }[] = [
  { metric: "price",         label: "Price" },
  { metric: "open_interest", label: "Open Interest" },
  { metric: "volume",        label: "Volume" },
];

function BackfillPanel({ onJobCreated }: { onJobCreated: () => void }) {
  const [pending, setPending] = useState<string | null>(null); // metric awaiting confirmation
  const [submitting, setSubmitting] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);

  async function trigger(metric: string) {
    setSubmitting(true);
    setFeedback(null);
    try {
      const res = await fetch(`${API_BASE}/api/indexer/runs`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ metric }),
      });
      if (res.status === 401) {
        setFeedback("Session expired. Please log in again.");
        return;
      }
      if (!res.ok) {
        const txt = await res.text();
        setFeedback(`POST /api/indexer/runs returned ${res.status}: ${txt.slice(0, 200)}`);
        return;
      }
      const data = await res.json();
      setFeedback(`✓ Backfill enqueued for ${metric} — task ${data.celery_task_id?.slice(0, 8) ?? "?"}…`);
      onJobCreated();
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
        Backfill Leaderboards
      </div>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
        {BACKFILL_METRICS.map((m) => (
          <button
            key={m.metric}
            type="button"
            disabled={submitting}
            onClick={() => setPending(m.metric)}
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
            Backfill {m.label}
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
      {pending && (
        <ConfirmModal
          metric={pending}
          submitting={submitting}
          onCancel={() => setPending(null)}
          onConfirm={() => trigger(pending)}
        />
      )}
    </div>
  );
}

function ConfirmModal({
  metric, submitting, onCancel, onConfirm,
}: {
  metric: string;
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
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.7)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
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
          maxWidth: 440,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase",
          marginBottom: 10,
        }}>
          Confirm Backfill
        </div>
        <div style={{ fontSize: 11, color: "var(--t1)", lineHeight: 1.6, marginBottom: 16 }}>
          This will run <code style={{ color: "var(--t0)" }}>backfill_leaderboards_bulk.py --metric {metric}</code> on the server.
          The job typically takes 60–90 minutes and writes ~190M rows. The Indexer Coverage page will be slow to query during the run.
          The script is idempotent — re-running is safe.
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
            {submitting ? "Enqueueing…" : `Run Backfill ${metric}`}
          </button>
        </div>
      </div>
    </div>
  );
}

function EmptyState() {
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "24px 28px",
    }}>
      <SectionLabel>No Jobs Recorded</SectionLabel>
      <div style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.7 }}>
        <code style={{ color: "var(--t0)" }}>market.indexer_jobs</code> is
        currently empty. The nightly indexer cron writes parquet files but
        does not yet record runs to this table — wiring{" "}
        <code style={{ color: "var(--t0)" }}>build_intraday_leaderboard.py</code>{" "}
        to insert into <code style={{ color: "var(--t0)" }}>market.indexer_jobs</code>{" "}
        is a follow-up phase that mirrors what was done for{" "}
        <code style={{ color: "var(--t0)" }}>metl.py → compiler_jobs</code>.
        Once that lands, this page will populate automatically.
      </div>
    </div>
  );
}

function JobsTable({ jobs, nowMs }: { jobs: IndexerJob[]; nowMs: number }) {
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
        No jobs match the active filter.
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
            <Th>Type</Th>
            <Th>Metric</Th>
            <Th>Date Range</Th>
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

export default function IndexerJobsPage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [nowMs, setNowMs] = useState(() => Date.now());
  const [filter, setFilter] = useState<FilterKey>("all");
  const pollTimerRef = useRef<number | null>(null);
  const tickerRef = useRef<number | null>(null);

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/indexer/jobs?limit=50`, {
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

  // Polling loop — same shape as compiler/jobs
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

    function schedulePollIfNeeded(jobs: IndexerJob[] | null) {
      clearPoll();
      if (cancelled) return;
      if (document.visibilityState === "hidden") return;
      if (!jobs) return;
      const anyRunning = jobs.some((j) => j.status === "running");
      if (!anyRunning) {
        clearTicker();
        return;
      }
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
        fetchJobs().then((jobs) => schedulePollIfNeeded(jobs));
      } else {
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

  // Counts per chip — computed from the full job list, not the filtered one
  const counts = useMemo(() => {
    const result: Record<FilterKey, number> = { all: 0, leaderboard: 0, overlap: 0, full: 0 };
    if (state.kind !== "ready") return result;
    result.all = state.jobs.length;
    for (const job of state.jobs) {
      if (job.job_type in result) {
        result[job.job_type as FilterKey]++;
      }
    }
    return result;
  }, [state]);

  const filteredJobs = useMemo(() => {
    if (state.kind !== "ready") return [];
    if (filter === "all") return state.jobs;
    return state.jobs.filter((j) => j.job_type === filter);
  }, [state, filter]);

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 1100, margin: "0 auto" }}>
        <SectionLabel>Indexer · Jobs</SectionLabel>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 16,
          letterSpacing: "-0.01em",
        }}>
          Indexer Jobs
        </h1>

        <BackfillPanel onJobCreated={() => fetchJobs()} />


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

        {state.kind === "ready" && state.jobs.length === 0 && <EmptyState />}

        {state.kind === "ready" && state.jobs.length > 0 && (
          <>
            <div style={{
              display: "flex",
              gap: 8,
              marginBottom: 16,
              flexWrap: "wrap",
            }}>
              {FILTER_CHIPS.map((chip) => (
                <FilterChip
                  key={chip.key}
                  label={chip.label}
                  active={filter === chip.key}
                  count={counts[chip.key]}
                  onClick={() => setFilter(chip.key)}
                />
              ))}
            </div>
            <JobsTable jobs={filteredJobs} nowMs={nowMs} />
          </>
        )}
      </div>
    </div>
  );
}

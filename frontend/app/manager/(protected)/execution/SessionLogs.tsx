"use client";

/**
 * SessionLogs.tsx
 * ================
 * Collapsible terminal-style viewer for the trader log, shown below the
 * Daily Execution Summary on the Execution tab.
 *
 * Behavior:
 *   - Default collapsed. Click header to expand.
 *   - `selectedDate` prop (from the parent page) chooses which session to
 *     display. null → server returns the most recent session.
 *   - Expanded + session_active=true → polls /execution-logs every 15s with
 *     `since_line` so only new lines cross the wire.
 *   - Auto-scrolls to the latest line when new lines arrive, UNLESS the user
 *     has scrolled up (we detect via the bottom-threshold check on scroll).
 *   - "Load earlier lines" button appears when from_line > 0; prepends older
 *     lines while preserving the visual scroll position via a scroll-height
 *     delta adjustment.
 */

import { useCallback, useEffect, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";
const POLL_INTERVAL_MS = 15_000;
const BOTTOM_THRESHOLD_PX = 20;
const LOAD_LIMIT = 500;

// Build a relative or absolute URL safely. Using `new URL()` directly would
// throw on prod where API_BASE is "" (nginx proxies same-origin requests),
// because URL requires an absolute base. String concat + URLSearchParams
// matches the convention used by the other Manager endpoints.
function buildLogsUrl(params: Record<string, string | number | undefined>): string {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null) continue;
    qs.set(k, String(v));
  }
  const q = qs.toString();
  return `${API_BASE}/api/manager/execution-logs${q ? `?${q}` : ""}`;
}

interface LogLine {
  n: number;
  ts: string;
  level: string;
  text: string;
}

interface LogResponse {
  date: string | null;
  allocation_id?: string | null;
  available_dates?: string[];  // last 3 available dates, DESC (alloc mode only)
  total_lines: number;
  from_line: number;
  lines: LogLine[];
  session_active: boolean;
}

function levelColor(level: string): string {
  switch (level) {
    case "ERROR":
    case "CRITICAL":
      return "var(--red)";
    case "WARNING":
    case "WARN":
      return "var(--amber)";
    default:
      return "#e0e0e0";
  }
}

// Extract HH:MM:SS from "YYYY-MM-DD HH:MM:SS"
function shortTime(ts: string): string {
  return ts.split(" ")[1] ?? ts;
}

function LivePulse() {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        color: "var(--green)",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.12em",
        fontFamily: FONT_MONO,
      }}
    >
      <span
        style={{
          width: 7,
          height: 7,
          borderRadius: "50%",
          background: "var(--green)",
          animation: "portfolio-pulse 1.6s ease-in-out infinite",
        }}
      />
      LIVE
    </span>
  );
}

export default function SessionLogs({
  selectedDate,
  allocationId,
  expanded,
  onToggle,
}: {
  selectedDate: string | null;
  /**
   * When provided, fetch per-allocation log file at
   * /mnt/quant-data/logs/trader/allocation_<allocationId>_<selectedDate>.log.
   * When null/undefined, fall back to master's continuous blofin_executor.log
   * (legacy behavior). `selectedDate` is REQUIRED when allocationId is set.
   */
  allocationId?: string | null;
  /**
   * Parent controls open/closed so the Execution tab can enforce
   * mutually-exclusive expansion with the Daily Execution Summary.
   */
  expanded: boolean;
  onToggle: () => void;
}) {
  const collapsed = !expanded;
  const [lines, setLines] = useState<LogLine[]>([]);
  const [total, setTotal] = useState(0);
  const [fromLine, setFromLine] = useState(0);
  const [sessionActive, setSessionActive] = useState(false);
  const [sessionDate, setSessionDate] = useState<string | null>(null);
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [loadingEarlier, setLoadingEarlier] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Override for the date query param. Set when the user clicks one of the
  // date-picker tabs. Cleared whenever the parent-provided selectedDate or
  // allocationId changes — those indicate a new context and the user's prior
  // tab selection no longer applies.
  const [userSelectedDate, setUserSelectedDate] = useState<string | null>(null);
  useEffect(() => {
    setUserSelectedDate(null);
  }, [selectedDate, allocationId]);
  const effectiveDate = userSelectedDate ?? selectedDate;

  const scrollRef = useRef<HTMLDivElement>(null);
  const bottomedOutRef = useRef(true);
  // Keep the latest n without retriggering pollNew on every lines update.
  const lastNRef = useRef(-1);

  useEffect(() => {
    lastNRef.current = lines.length ? lines[lines.length - 1].n : -1;
  }, [lines]);

  const fetchInitial = useCallback(async (date: string | null) => {
    setLoading(true);
    setError(null);
    try {
      const url = buildLogsUrl({
        date: date ?? undefined,
        allocation_id: allocationId ?? undefined,
        limit: LOAD_LIMIT,
      });
      const res = await fetch(url, { credentials: "include" });
      if (res.status === 401) throw new Error("Session expired");
      if (res.status === 400) {
        // Likely: allocation mode needs both date + allocationId. Surface as
        // a user-visible message rather than a crash.
        const body = await res.json().catch(() => ({}));
        throw new Error(typeof body?.detail === "string" ? body.detail : "HTTP 400");
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: LogResponse = await res.json();
      setLines(data.lines);
      setTotal(data.total_lines);
      setFromLine(data.from_line);
      setSessionActive(data.session_active);
      setSessionDate(data.date);
      setAvailableDates(data.available_dates ?? []);
      bottomedOutRef.current = true;
      // Scroll to bottom after layout.
      requestAnimationFrame(() => {
        const el = scrollRef.current;
        if (el) el.scrollTop = el.scrollHeight;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [allocationId]);

  const pollNew = useCallback(async () => {
    try {
      const url = buildLogsUrl({
        date: sessionDate ?? undefined,
        allocation_id: allocationId ?? undefined,
        since_line: lastNRef.current,
        limit: LOAD_LIMIT,
      });
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) return;
      const data: LogResponse = await res.json();
      setTotal(data.total_lines);
      setSessionActive(data.session_active);
      if (data.lines.length === 0) return;
      setLines((prev) => {
        const byN = new Map<number, LogLine>();
        for (const l of prev) byN.set(l.n, l);
        for (const l of data.lines) byN.set(l.n, l);
        return Array.from(byN.values()).sort((a, b) => a.n - b.n);
      });
      if (bottomedOutRef.current) {
        requestAnimationFrame(() => {
          const el = scrollRef.current;
          if (el) el.scrollTop = el.scrollHeight;
        });
      }
    } catch {
      // Swallow transient polling errors silently; the next tick will retry.
    }
  }, [sessionDate, allocationId]);

  const loadEarlier = useCallback(async () => {
    if (fromLine <= 0 || loadingEarlier) return;
    setLoadingEarlier(true);
    try {
      const el = scrollRef.current;
      const prevScrollHeight = el?.scrollHeight ?? 0;
      const prevScrollTop = el?.scrollTop ?? 0;

      // Request the chunk of `LOAD_LIMIT` lines ending just before fromLine.
      // API returns lines with n > since_line, so since_line = start - 1.
      const targetStart = Math.max(0, fromLine - LOAD_LIMIT);
      const sinceParam = targetStart - 1;

      const url = buildLogsUrl({
        date: sessionDate ?? undefined,
        allocation_id: allocationId ?? undefined,
        since_line: sinceParam,
        limit: LOAD_LIMIT,
      });
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: LogResponse = await res.json();
      setLines((prev) => {
        const byN = new Map<number, LogLine>();
        for (const l of data.lines) byN.set(l.n, l);
        for (const l of prev) byN.set(l.n, l);
        return Array.from(byN.values()).sort((a, b) => a.n - b.n);
      });
      setFromLine(data.from_line);
      setTotal(data.total_lines);

      // Preserve the user's view: when we prepend, scrollHeight grows.
      // Keep scrollTop offset by the growth so the currently-visible lines
      // stay where they are on screen.
      requestAnimationFrame(() => {
        const nextEl = scrollRef.current;
        if (!nextEl) return;
        const delta = nextEl.scrollHeight - prevScrollHeight;
        nextEl.scrollTop = prevScrollTop + delta;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoadingEarlier(false);
    }
  }, [fromLine, sessionDate, loadingEarlier, allocationId]);

  const handleScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    bottomedOutRef.current =
      el.scrollHeight - el.scrollTop - el.clientHeight <= BOTTOM_THRESHOLD_PX;
  };

  // (Re)fetch when expanded OR when the effective date / allocationId
  // changes while expanded. effectiveDate folds in userSelectedDate so
  // clicking a date-tab triggers a refetch.
  useEffect(() => {
    if (collapsed) return;
    fetchInitial(effectiveDate);
  }, [collapsed, effectiveDate, fetchInitial]);

  // Poll while expanded + active.
  useEffect(() => {
    if (collapsed || !sessionActive) return;
    const id = setInterval(pollNew, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [collapsed, sessionActive, pollNew]);

  // Header summary text on the right side
  const headerRight = (() => {
    if (loading && lines.length === 0) {
      return <span style={{ color: "var(--t3)" }}>loading…</span>;
    }
    if (error) {
      return <span style={{ color: "var(--red)" }}>{error}</span>;
    }
    if (total > 0) {
      return (
        <span style={{ color: "var(--t2)", fontSize: 10 }}>
          {total.toLocaleString()} lines
        </span>
      );
    }
    return null;
  })();

  return (
    <div
      style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 5,
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
        // Fill available vertical space only when expanded so the collapsed
        // header sits snug.
        flex: expanded ? 1 : "0 0 auto",
        minHeight: 0,
      }}
    >
      {/* Header / toggle */}
      <button
        type="button"
        onClick={onToggle}
        style={{
          width: "100%",
          display: "flex",
          alignItems: "center",
          gap: 10,
          background: "transparent",
          border: "none",
          padding: "12px 16px",
          cursor: "pointer",
          textAlign: "left",
          fontFamily: FONT_MONO,
          color: "var(--t3)",
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          textTransform: "uppercase",
        }}
      >
        <span style={{ color: "var(--t2)", width: 14 }}>
          {collapsed ? "▸" : "▾"}
        </span>
        <span>Session Logs</span>
        {sessionDate && (
          <span style={{ color: "var(--t2)", fontWeight: 400, letterSpacing: "0.06em" }}>
            — {sessionDate}
          </span>
        )}
        <span style={{ flex: 1 }} />
        {sessionActive && <LivePulse />}
        {headerRight}
      </button>

      {/* Body */}
      {!collapsed && (
        <div style={{
          borderTop: "1px solid var(--line)",
          display: "flex",
          flexDirection: "column",
          flex: 1,
          minHeight: 0,
        }}>
          {/* Date picker — allocation mode only; renders when we have ≥2
              dates available (nothing to switch if we only have one). */}
          {allocationId && availableDates.length >= 2 && (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                padding: "8px 16px",
                borderBottom: "1px solid var(--line)",
                background: "var(--bg1)",
                fontFamily: FONT_MONO,
              }}
            >
              <span
                style={{
                  fontSize: 9,
                  fontWeight: 700,
                  letterSpacing: "0.12em",
                  color: "var(--t3)",
                  textTransform: "uppercase",
                }}
              >
                Date
              </span>
              <div style={{ display: "flex", border: "1px solid var(--line)", borderRadius: 4, overflow: "hidden" }}>
                {availableDates.map((d, i) => {
                  const active = d === sessionDate;
                  return (
                    <button
                      key={d}
                      type="button"
                      onClick={() => setUserSelectedDate(d)}
                      style={{
                        background: active ? "var(--bg4)" : "transparent",
                        color: active ? "var(--t0)" : "var(--t2)",
                        border: "none",
                        borderLeft: i === 0 ? "none" : "1px solid var(--line)",
                        padding: "4px 10px",
                        fontSize: 10,
                        fontWeight: 700,
                        letterSpacing: "0.04em",
                        fontFamily: FONT_MONO,
                        cursor: "pointer",
                        transition: "background 0.15s ease, color 0.15s ease",
                      }}
                      onMouseEnter={(e) => {
                        if (!active) e.currentTarget.style.color = "var(--t1)";
                      }}
                      onMouseLeave={(e) => {
                        if (!active) e.currentTarget.style.color = "var(--t2)";
                      }}
                    >
                      {d}
                    </button>
                  );
                })}
              </div>
              <span style={{ fontSize: 9, color: "var(--t3)" }}>
                (last {availableDates.length} available)
              </span>
            </div>
          )}

          {fromLine > 0 && (
            <div
              style={{
                display: "flex",
                justifyContent: "center",
                padding: "8px",
                borderBottom: "1px solid var(--line)",
                background: "var(--bg1)",
              }}
            >
              <button
                type="button"
                onClick={loadEarlier}
                disabled={loadingEarlier}
                style={{
                  background: "transparent",
                  border: "1px solid var(--line)",
                  borderRadius: 4,
                  color: "var(--t2)",
                  fontSize: 9,
                  fontWeight: 700,
                  letterSpacing: "0.12em",
                  textTransform: "uppercase",
                  padding: "4px 14px",
                  cursor: loadingEarlier ? "default" : "pointer",
                  fontFamily: FONT_MONO,
                  opacity: loadingEarlier ? 0.6 : 1,
                }}
              >
                {loadingEarlier
                  ? "Loading…"
                  : `Load earlier lines (${fromLine.toLocaleString()} hidden)`}
              </button>
            </div>
          )}

          <div
            ref={scrollRef}
            onScroll={handleScroll}
            style={{
              flex: 1,
              minHeight: 320,
              overflowY: "auto",
              background: "var(--bg1)",
              fontFamily: FONT_MONO,
              fontSize: 10,
              lineHeight: "18px",
              padding: "8px 0",
            }}
          >
            {lines.length === 0 && !loading && !error && (
              <div
                style={{
                  padding: "24px",
                  textAlign: "center",
                  color: "var(--t3)",
                  fontSize: 10,
                }}
              >
                {allocationId
                  ? "No sessions in the selected range for this allocation."
                  : "No log lines for this session yet."}
              </div>
            )}
            {lines.map((line) => (
              <LogRow key={line.n} line={line} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function LogRow({ line }: { line: LogLine }) {
  const color = levelColor(line.level);
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "48px 72px 48px 1fr",
        gap: 8,
        padding: "0 14px",
        whiteSpace: "pre-wrap",
        wordBreak: "break-word",
      }}
    >
      <span style={{ color: "var(--t3)", textAlign: "right" }}>{line.n}</span>
      <span style={{ color: "var(--t3)" }}>{shortTime(line.ts)}</span>
      <span style={{ color: color, fontWeight: 700 }}>
        {line.level === "WARNING" ? "WARN" : line.level}
      </span>
      <span style={{ color: color }}>{line.text}</span>
    </div>
  );
}

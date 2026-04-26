"use client";

/**
 * PortfolioSessionLogs.tsx
 * =========================
 * Right-edge collapsible sidebar that surfaces the live trader log
 * stream for the active portfolio's primary allocation. Mounts on
 * the portfolio detail page (/manager/portfolios/[date]) — independent
 * of the existing SessionLogs.tsx on the Execution tab, which keeps
 * working unchanged.
 *
 * Architecture notes:
 *   - Backend semantics: each line carries an optional {kind, data}
 *     classified by manager.py:_annotate_event. The sidebar renders
 *     typed rows — tick rows for bar_update + roi_report, collapsible
 *     stop-event groups for sym_stop sequences, generic rows for
 *     unmatched lines. No regex on raw strings here.
 *   - Allocation scoping: lines that carry data.allocation_id (short
 *     8-char prefix) are filtered against the page's primary
 *     allocationId. Today's allocation log files are per-allocation
 *     by file path so the filter is mostly defensive.
 *   - Polling: 15s cadence, gated on `expanded && session_active`.
 *     since_line cursor avoids re-shipping the full window each tick.
 *   - State: persisted across reloads in localStorage (expand state,
 *     last viewed line, animation paused).
 *   - Alert state: when a sym_stop arrives with n > lastViewedLineN
 *     while the panel is collapsed, the edge handle goes red. Opens
 *     reset the watermark even if the operator didn't scroll to the
 *     event — they were given the chance to see it.
 *   - Fetch URL: string-concat (NOT new URL()), since API_BASE is
 *     "" in prod under the nginx same-origin proxy.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

// ── Constants ───────────────────────────────────────────────────────────────

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const FONT_MONO = `var(--font-space-mono), ui-monospace, "SF Mono", Menlo, Consolas, monospace`;

const PANEL_WIDTH_PX = 640;
// Collapsed handle target. Bumped from 16 → 28 for discoverability —
// 16 was visible but read as a thin border, hard to spot as a control.
// 28 gives a real click target while still reading as "edge tab" not
// "panel". Page wrapper's reflow margin in page.tsx must match.
const HANDLE_WIDTH_PX = 28;
const POLL_INTERVAL_MS = 15_000;
const BOTTOM_THRESHOLD_PX = 24;
const LOAD_LIMIT = 500;
const BAR_INTERVAL_S = 300;

const STORAGE_KEYS = {
  expanded: "portfolio.sessionLogs.expanded",
  lastViewedLineN: "portfolio.sessionLogs.lastViewedLineN",
  animatePaused: "portfolio.sessionLogs.animatePaused",
};

const TYPING_MESSAGES_BASE = [
  "awaiting next bar",
  "polling blofin api",
  "trailing stops armed",
  "monitoring dispersion",
  "session window stable",
  "watching market depth",
  "computing tick deltas",
];

// ── Types ───────────────────────────────────────────────────────────────────

interface BarUpdateData {
  bar: number;
  incr: number;
  peak: number;
  tsl: number;
  sess: number;
  active: number;
  universe: number;
  stopped: number;
  fill: string;
  allocation_id?: string | null;
}

interface RoiReportData {
  bar: number | null;
  incr: number | null;
  actual: number;
  expected: number;
  delta: number;
  equity: number;
  pnl: number;
  allocation_id?: string | null;
}

interface LogLine {
  n: number;
  ts: string;
  level: string;
  text: string;
  kind?: string;
  data?: Record<string, unknown>;
}

interface LogResponse {
  date: string | null;
  allocation_id?: string | null;
  total_lines: number;
  from_line: number;
  lines: LogLine[];
  session_active: boolean;
}

type FilterKey = "all" | "bars" | "events" | "stops" | "warn";

type Item =
  | {
      type: "tick";
      bar: number;
      ts: string;
      n: number;
      bar_update?: BarUpdateData;
      roi_report?: RoiReportData;
    }
  | { type: "stop_event"; ts: string; n: number; lines: LogLine[] }
  | { type: "system"; line: LogLine }
  | { type: "generic"; line: LogLine };

interface Props {
  date: string;
  allocationId: string | null;
  exchange: string | null;
  strategyLabel: string | null;
  sessionActive: boolean;
  // Controlled expansion state — lifted to the page so it can reflow
  // its main content (margin-right transition) instead of overlaying
  // the panel over the chart and matrix. Page reads the same localStorage
  // key on mount so initial render carries no flash.
  expanded: boolean;
  onExpandedChange: (next: boolean) => void;
}

// ── URL builder ────────────────────────────────────────────────────────────
// String-concat + URLSearchParams. `new URL()` throws when API_BASE is "".
function buildLogsUrl(params: Record<string, string | number | undefined>): string {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null) continue;
    qs.set(k, String(v));
  }
  const q = qs.toString();
  return `${API_BASE}/api/manager/execution-logs${q ? `?${q}` : ""}`;
}

// ── Formatters ──────────────────────────────────────────────────────────────

// Heatmap tint — per-column normalized, capped opacity. Mirrors the
// Advanced matrix's cellTint so the ACT column in the panel reads
// with the same intensity grammar the operator already learned there.
// Cap stays at 0.18 (non-negotiable per the matrix brief — higher
// trades polish for noise).
function cellTint(value: number, rangeAbsMax: number, cap = 0.18): string {
  if (value === 0 || rangeAbsMax === 0) return "transparent";
  const normalized = Math.abs(value) / rangeAbsMax;
  const opacity = Math.min(normalized * cap, cap);
  if (value < 0) return `rgba(239, 68, 68, ${opacity.toFixed(3)})`;
  return `rgba(0, 200, 150, ${opacity.toFixed(3)})`;
}

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v >= 0 ? "+" : "−";
  return `${sign}${Math.abs(v).toFixed(digits)}%`;
}

function fmtUSD(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v < 0 ? "−" : "";
  return `${sign}$${Math.abs(v).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function fmtTimeShort(ts: string): string {
  // "YYYY-MM-DD HH:MM:SS" -> "HH:MM"
  return ts.split(" ")[1]?.slice(0, 5) ?? ts;
}

function fmtTimeFull(ts: string): string {
  return ts.split(" ")[1] ?? ts;
}

// Bar boundary countdown — anchored to UTC 5-min boundaries (real wall clock,
// not a JS interval). Operator can open the panel mid-bar and the countdown
// stays honest.
function nextBarSeconds(): number {
  const now = Math.floor(Date.now() / 1000);
  const next = Math.ceil(now / BAR_INTERVAL_S) * BAR_INTERVAL_S;
  return Math.max(0, next - now);
}

function fmtCountdown(secs: number): string {
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

// ── localStorage helpers ────────────────────────────────────────────────────

function readBoolStorage(key: string, fallback: boolean): boolean {
  if (typeof window === "undefined") return fallback;
  const raw = window.localStorage.getItem(key);
  if (raw === null) return fallback;
  return raw === "true";
}

function writeBoolStorage(key: string, val: boolean): void {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(key, String(val));
}

function readIntStorage(key: string): number | null {
  if (typeof window === "undefined") return null;
  const raw = window.localStorage.getItem(key);
  if (raw === null) return null;
  const n = parseInt(raw, 10);
  return Number.isNaN(n) ? null : n;
}

function writeIntStorage(key: string, val: number): void {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(key, String(val));
}

// ── Item assembly: tick rows, stop events, generic ──────────────────────────

function buildItems(lines: LogLine[]): Item[] {
  // Pass 1: index tick data by bar number. bar_update + roi_report for
  // the same bar collapse into the same tick item.
  const tickByBar = new Map<number, Item & { type: "tick" }>();
  for (const l of lines) {
    if (l.kind === "bar_update" && l.data) {
      const d = l.data as unknown as BarUpdateData;
      if (d.bar == null) continue;
      const slot: Item & { type: "tick" } =
        tickByBar.get(d.bar) ??
        { type: "tick", bar: d.bar, ts: l.ts, n: l.n };
      slot.bar_update = d;
      // Use the earliest n for ordering, latest ts for display.
      slot.n = Math.min(slot.n, l.n);
      slot.ts = l.ts;
      tickByBar.set(d.bar, slot);
    } else if (l.kind === "roi_report" && l.data) {
      const d = l.data as unknown as RoiReportData;
      if (d.bar == null) continue;
      const slot: Item & { type: "tick" } =
        tickByBar.get(d.bar) ?? { type: "tick", bar: d.bar, ts: l.ts, n: l.n };
      slot.roi_report = d;
      tickByBar.set(d.bar, slot);
    }
  }

  // Pass 2: walk linearly and emit items in chronological order.
  // Stop events accumulate sym_stop + reconciliation lines until any
  // unrelated line breaks the sequence.
  const out: Item[] = [];
  const emittedBars = new Set<number>();
  let currentStop: (Item & { type: "stop_event" }) | null = null;

  const flushStop = () => {
    if (currentStop) {
      out.push(currentStop);
      currentStop = null;
    }
  };

  for (const l of lines) {
    if (l.kind === "bar_update" || l.kind === "roi_report") {
      const bar = (l.data as { bar?: number | null } | undefined)?.bar ?? null;
      if (bar != null && !emittedBars.has(bar)) {
        flushStop();
        const slot = tickByBar.get(bar);
        if (slot) {
          out.push(slot);
          emittedBars.add(bar);
        }
      }
      continue;
    }
    if (l.kind === "sym_stop") {
      if (!currentStop) {
        currentStop = { type: "stop_event", ts: l.ts, n: l.n, lines: [] };
      }
      currentStop.lines.push(l);
      continue;
    }
    if (
      currentStop &&
      ["close_all", "position_check", "size_mismatch", "close_confirm"].includes(
        l.kind || "",
      )
    ) {
      currentStop.lines.push(l);
      continue;
    }
    flushStop();
    // system_event rows render compact via SystemRow; everything else
    // unmatched falls back to GenericRow with multi-line wrap.
    if (l.kind === "system_event") {
      out.push({ type: "system", line: l });
    } else {
      out.push({ type: "generic", line: l });
    }
  }
  flushStop();
  return out;
}

// ── Main component ──────────────────────────────────────────────────────────

export default function PortfolioSessionLogs({
  date,
  allocationId,
  exchange,
  strategyLabel,
  sessionActive,
  expanded,
  onExpandedChange,
}: Props) {
  // Local setter that also persists to localStorage. The controlled
  // prop drives rendering; the page reads the same key on mount.
  const setExpanded = useCallback(
    (next: boolean) => {
      writeBoolStorage(STORAGE_KEYS.expanded, next);
      onExpandedChange(next);
    },
    [onExpandedChange],
  );
  const [lines, setLines] = useState<LogLine[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<FilterKey>("all");
  // Independent SYS toggle — orthogonal to the exclusive filter chips.
  // Off by default so bootstrap noise (TraderConfig, allocation init,
  // rehydrate, session boundaries) doesn't dominate the viewport. The
  // chip count reveals what's hidden so the operator gets a "there's
  // stuff here you're not seeing" cue without having to look.
  const [sysShown, setSysShown] = useState<boolean>(false);
  const [hasUnreadStop, setHasUnreadStop] = useState<boolean>(false);
  const [paused, setPaused] = useState<boolean>(() =>
    readBoolStorage(STORAGE_KEYS.animatePaused, false),
  );
  const [collapsedStops, setCollapsedStops] = useState<Set<number>>(new Set());
  const [tabHidden, setTabHidden] = useState<boolean>(false);

  const lastNRef = useRef<number>(-1);
  const scrollRef = useRef<HTMLDivElement>(null);
  const wasAtBottomRef = useRef<boolean>(true);
  const lastViewedRef = useRef<number | null>(readIntStorage(STORAGE_KEYS.lastViewedLineN));

  // Persist paused state. (Expanded persistence is handled by the
  // controlled setter so the page mounts with the right margin-right
  // before the panel even renders, avoiding a layout flash.)
  useEffect(() => writeBoolStorage(STORAGE_KEYS.animatePaused, paused), [paused]);

  // Tab visibility tracking — animations auto-pause when hidden.
  useEffect(() => {
    if (typeof document === "undefined") return;
    const onVis = () => setTabHidden(document.visibilityState === "hidden");
    onVis();
    document.addEventListener("visibilitychange", onVis);
    return () => document.removeEventListener("visibilitychange", onVis);
  }, []);

  // Initial fetch — fires on mount + whenever allocation/date change.
  // Polling fires only when expanded + sessionActive (15s cadence).
  const fetchInitial = useCallback(async () => {
    if (!allocationId || !date) return;
    try {
      setError(null);
      const url = buildLogsUrl({
        date,
        allocation_id: allocationId,
        limit: LOAD_LIMIT,
      });
      const res = await fetch(url, { credentials: "include" });
      if (res.status === 401) throw new Error("Session expired");
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: LogResponse = await res.json();
      setLines(data.lines);
      lastNRef.current = data.lines.length
        ? data.lines[data.lines.length - 1].n
        : -1;
      // First load: if no watermark stored, seed with the latest n so
      // historical stop events don't trigger a phantom alert state.
      if (lastViewedRef.current === null && data.lines.length > 0) {
        const latestN = data.lines[data.lines.length - 1].n;
        lastViewedRef.current = latestN;
        writeIntStorage(STORAGE_KEYS.lastViewedLineN, latestN);
      }
      requestAnimationFrame(() => {
        const el = scrollRef.current;
        if (el) el.scrollTop = el.scrollHeight;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [date, allocationId]);

  const pollNew = useCallback(async () => {
    if (!allocationId || !date) return;
    try {
      const url = buildLogsUrl({
        date,
        allocation_id: allocationId,
        since_line: lastNRef.current,
        limit: LOAD_LIMIT,
      });
      const res = await fetch(url, { credentials: "include" });
      if (!res.ok) return;
      const data: LogResponse = await res.json();
      if (data.lines.length === 0) return;

      // Alert detection: any sym_stop past the watermark while collapsed.
      const lvn = lastViewedRef.current ?? -1;
      const hasNewStop = data.lines.some(
        (l) => l.kind === "sym_stop" && l.n > lvn,
      );
      if (hasNewStop && !expanded) setHasUnreadStop(true);

      setLines((prev) => {
        const byN = new Map<number, LogLine>();
        for (const l of prev) byN.set(l.n, l);
        for (const l of data.lines) byN.set(l.n, l);
        return Array.from(byN.values()).sort((a, b) => a.n - b.n);
      });
      lastNRef.current = data.lines[data.lines.length - 1].n;
    } catch {
      // best-effort; silent on poll errors so transient blips don't
      // surface a red error state to the operator.
    }
  }, [date, allocationId, expanded]);

  useEffect(() => {
    fetchInitial();
  }, [fetchInitial]);

  useEffect(() => {
    if (!expanded || !sessionActive || !allocationId) return;
    const id = setInterval(pollNew, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  }, [expanded, sessionActive, allocationId, pollNew]);

  // Auto-scroll on new lines if we were at bottom.
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    if (wasAtBottomRef.current) el.scrollTop = el.scrollHeight;
  }, [lines.length]);

  // On expand: clear unread + bump watermark.
  useEffect(() => {
    if (expanded && lines.length > 0) {
      const latestN = lines[lines.length - 1].n;
      lastViewedRef.current = latestN;
      writeIntStorage(STORAGE_KEYS.lastViewedLineN, latestN);
      setHasUnreadStop(false);
    }
  }, [expanded, lines.length]);

  const onScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    wasAtBottomRef.current =
      el.scrollHeight - el.scrollTop - el.clientHeight < BOTTOM_THRESHOLD_PX;
  }, []);

  // ── Filter + scope to active allocation ──────────────────────────────
  // Lines emit allocation_id as the SHORT id (first 8 chars from the
  // [alloc XXX] log prefix); the prop carries the full UUID.
  const allocShort = allocationId ? allocationId.slice(0, 8) : null;

  const scopedLines = useMemo(
    () =>
      lines.filter((l) => {
        const lineAlloc = (l.data as { allocation_id?: string | null } | undefined)
          ?.allocation_id;
        if (lineAlloc && allocShort && lineAlloc !== allocShort) return false;
        return true;
      }),
    [lines, allocShort],
  );

  const filteredLines = useMemo(() => {
    return scopedLines.filter((l) => {
      // SYS gate first — independent of the exclusive filter chip.
      // system_events stay hidden unless the operator opts in.
      if (l.kind === "system_event" && !sysShown) return false;
      // Exclusive filter chip
      if (filter === "all") return true;
      if (filter === "bars") return l.kind === "bar_update";
      if (filter === "events") {
        // EVENTS = non-tick, non-system events. The SYS chip is the
        // dedicated path for system_events; don't double-count them.
        return (
          l.kind &&
          !["bar_update", "roi_report", "system_event"].includes(l.kind)
        );
      }
      if (filter === "stops") return l.kind === "sym_stop";
      if (filter === "warn") {
        const lvl = (l.level || "").toUpperCase();
        return (
          lvl === "WARN" ||
          lvl === "WARNING" ||
          l.kind === "sym_stop" ||
          l.kind === "size_mismatch"
        );
      }
      return true;
    });
  }, [scopedLines, filter, sysShown]);

  const items = useMemo(() => buildItems(filteredLines), [filteredLines]);

  // Column abs-max for the ACT cell heatmap. Derived from the visible
  // tick rows' roi_report.actual values; recomputes when the filter or
  // line set changes. Matches the Advanced matrix's per-column scaling.
  const actAbsMax = useMemo(() => {
    let m = 0;
    for (const item of items) {
      if (item.type === "tick" && item.roi_report) {
        const v = Math.abs(item.roi_report.actual);
        if (v > m) m = v;
      }
    }
    return m;
  }, [items]);

  // Chip counts — computed against scopedLines (not filteredLines), so
  // each chip's count answers "how many would I see if I clicked this".
  // Note: ALL excludes system_events (they're the SYS chip's domain).
  const counts = useMemo(() => {
    let bars = 0;
    let events = 0;
    let stopGroups = 0;
    let warn = 0;
    let sys = 0;
    let allCount = 0;
    let inStop = false;
    for (const l of scopedLines) {
      if (l.kind === "system_event") {
        sys++;
        // system_events don't count against ALL — operator opts in via SYS.
        inStop = false;
        continue;
      }
      allCount++;
      if (l.kind === "bar_update") {
        bars++;
        inStop = false;
      } else if (l.kind === "roi_report") {
        inStop = false;
      } else if (l.kind === "sym_stop") {
        if (!inStop) {
          stopGroups++;
          inStop = true;
        }
      } else if (
        ["close_all", "position_check", "size_mismatch", "close_confirm"].includes(
          l.kind || "",
        )
      ) {
        // continues stop event if any
      } else {
        inStop = false;
      }
      if (
        l.kind &&
        !["bar_update", "roi_report"].includes(l.kind)
      ) {
        events++;
      }
      const lvl = (l.level || "").toUpperCase();
      if (
        lvl === "WARN" ||
        lvl === "WARNING" ||
        l.kind === "sym_stop" ||
        l.kind === "size_mismatch"
      ) {
        warn++;
      }
    }
    return { all: allCount, bars, events, stops: stopGroups, warn, sys };
  }, [scopedLines]);

  // Latest-bar metrics for header + snapshot + footer typing template.
  const latestSnapshot = useMemo(() => {
    let lastBar: BarUpdateData | undefined;
    let lastRoi: RoiReportData | undefined;
    for (const item of items) {
      if (item.type === "tick") {
        if (item.bar_update) lastBar = item.bar_update;
        if (item.roi_report) lastRoi = item.roi_report;
      }
    }
    return { lastBar, lastRoi };
  }, [items]);

  const toggleStop = useCallback((n: number) => {
    setCollapsedStops((prev) => {
      const next = new Set(prev);
      if (next.has(n)) next.delete(n);
      else next.add(n);
      return next;
    });
  }, []);

  // Animations gate: paused if user paused, OR tab hidden, OR collapsed,
  // OR session ended. Pulse + countdown stay running on session end —
  // they convey aliveness, not decoration; only the typing freezes.
  const animationsOff = paused || tabHidden || !expanded || !sessionActive;

  // Header label
  const headerLeft = exchange && strategyLabel
    ? `${exchange.toUpperCase()} · ${strategyLabel.toUpperCase()}`
    : strategyLabel?.toUpperCase() ?? "";

  return (
    <>
      <style jsx global>{`
        @keyframes pls-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%      { opacity: 0.4; transform: scale(0.8); }
        }
        @keyframes pls-blink {
          0%, 50% { opacity: 1; }
          51%, 100% { opacity: 0; }
        }
      `}</style>

      {/* Edge handle — collapsed-state trigger. Always visible at the
          viewport's right edge when the panel is closed. */}
      {!expanded && (
        <EdgeHandle
          alert={hasUnreadStop}
          sessionActive={sessionActive}
          tabHidden={tabHidden}
          onOpen={() => setExpanded(true)}
        />
      )}

      {/* Expanded panel — fixed right edge, full viewport height. */}
      {expanded && (
        <div
          style={{
            position: "fixed",
            top: 0,
            right: 0,
            bottom: 0,
            width: PANEL_WIDTH_PX,
            background: "#080809",
            borderLeft: "1px solid rgba(255,255,255,0.08)",
            boxShadow: "-1px 0 0 rgba(255,255,255,0.04)",
            display: "flex",
            flexDirection: "column",
            fontFamily: FONT_MONO,
            fontSize: 11,
            color: "#d4d4d8",
            zIndex: 50,
          }}
        >
          <PanelHeader
            label={headerLeft}
            allocId={allocationId}
            sessionActive={sessionActive}
            onClose={() => setExpanded(false)}
          />
          <ChipToolbar
            filter={filter}
            setFilter={setFilter}
            sysShown={sysShown}
            setSysShown={setSysShown}
            counts={counts}
          />
          <Snapshot
            lastBar={latestSnapshot.lastBar}
            lastRoi={latestSnapshot.lastRoi}
          />
          <Viewport
            scrollRef={scrollRef}
            onScroll={onScroll}
            items={items}
            collapsedStops={collapsedStops}
            toggleStop={toggleStop}
            error={error}
            actAbsMax={actAbsMax}
          />
          <Footer
            paused={paused}
            setPaused={setPaused}
            animationsOff={animationsOff}
            sessionActive={sessionActive}
            lastBar={latestSnapshot.lastBar}
          />
        </div>
      )}
    </>
  );
}

// ── Edge handle (collapsed) ─────────────────────────────────────────────────

function EdgeHandle({
  alert,
  sessionActive,
  tabHidden: _tabHidden,
  onOpen,
}: {
  alert: boolean;
  sessionActive: boolean;
  tabHidden: boolean;
  onOpen: () => void;
}) {
  const [hover, setHover] = useState(false);
  const dotColor = alert ? "#ef4444" : sessionActive ? "#00c896" : "#71717a";
  const pulseAnim = sessionActive
    ? `pls-pulse ${alert ? "1.4s" : "2s"} ease-in-out infinite`
    : "none";

  return (
    <div
      role="button"
      tabIndex={0}
      aria-label={alert ? "Open session logs (new stop event)" : "Open session logs"}
      onClick={onOpen}
      onKeyDown={(e) => {
        if (e.key === " " || e.key === "Enter") {
          e.preventDefault();
          onOpen();
        }
      }}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      style={{
        position: "fixed",
        top: 0,
        right: 0,
        bottom: 0,
        width: HANDLE_WIDTH_PX,
        background: hover ? "#0e0e11" : "#0a0a0b",
        borderLeft: alert
          ? "1px solid rgba(239,68,68,0.40)"
          : "1px solid rgba(255,255,255,0.06)",
        cursor: "pointer",
        userSelect: "none",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        zIndex: 40,
        transition: "background 0.15s",
      }}
    >
      {/* Red spine when in alert state. */}
      {alert && (
        <div
          style={{
            position: "absolute",
            top: 0,
            bottom: 0,
            left: 0,
            width: 1,
            background: "#ef4444",
            opacity: 0.4,
          }}
        />
      )}
      {/* Top status dot */}
      <div
        style={{
          width: 5,
          height: 5,
          borderRadius: "50%",
          background: dotColor,
          boxShadow: sessionActive
            ? `0 0 5px ${alert ? "rgba(239,68,68,0.7)" : "rgba(0,200,150,0.6)"}`
            : "none",
          marginTop: 10,
          flexShrink: 0,
          animation: pulseAnim,
        }}
      />
      {/* Center chevron */}
      <div
        style={{
          marginTop: "auto",
          marginBottom: "auto",
          color: alert ? "#ef4444" : hover ? "#d4d4d8" : "#71717a",
          fontSize: 18,
          fontWeight: 500,
          lineHeight: 1,
          fontFamily: FONT_MONO,
        }}
      >
        ‹
      </div>
    </div>
  );
}

// ── Panel header ────────────────────────────────────────────────────────────

function PanelHeader({
  label,
  allocId,
  sessionActive,
  onClose,
}: {
  label: string;
  allocId: string | null;
  sessionActive: boolean;
  onClose: () => void;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        // Match the web app's topbar height (Topbar.tsx → 46px) so the
        // panel header sits flush with the topbar baseline. Vertical
        // centering carries the content; padding-x stays the same.
        height: 46,
        padding: "0 12px",
        borderBottom: "1px solid rgba(255,255,255,0.08)",
        background: "#0c0c0e",
        flexShrink: 0,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
        <span
          style={{
            width: 6,
            height: 6,
            borderRadius: "50%",
            background: sessionActive ? "#00c896" : "#71717a",
            boxShadow: sessionActive ? "0 0 6px rgba(0,200,150,0.7)" : "none",
            flexShrink: 0,
          }}
        />
        <span
          style={{
            fontSize: 10.5,
            letterSpacing: "0.06em",
            color: "#71717a",
            fontWeight: 500,
          }}
        >
          {label}
        </span>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <span
          style={{
            fontSize: 9.5,
            letterSpacing: "0.08em",
            color: "#52525b",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          {allocId ? allocId.slice(0, 8) : ""}
        </span>
        <button
          type="button"
          aria-label="Close session logs"
          onClick={onClose}
          style={{
            background: "transparent",
            border: "none",
            color: "#71717a",
            fontSize: 16,
            lineHeight: 1,
            cursor: "pointer",
            padding: 0,
            fontFamily: FONT_MONO,
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.color = "#d4d4d8";
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.color = "#71717a";
          }}
        >
          ›
        </button>
      </div>
    </div>
  );
}

// ── Chip toolbar ────────────────────────────────────────────────────────────

function ChipToolbar({
  filter,
  setFilter,
  sysShown,
  setSysShown,
  counts,
}: {
  filter: FilterKey;
  setFilter: (k: FilterKey) => void;
  sysShown: boolean;
  setSysShown: (next: boolean | ((prev: boolean) => boolean)) => void;
  counts: { all: number; bars: number; events: number; stops: number; warn: number; sys: number };
}) {
  const chips: { key: FilterKey; label: string; count: number; tone: "neutral" | "stop" | "warn" }[] = [
    { key: "all", label: "ALL", count: counts.all, tone: "neutral" },
    { key: "bars", label: "BARS", count: counts.bars, tone: "neutral" },
    { key: "events", label: "EVENTS", count: counts.events, tone: "neutral" },
    { key: "stops", label: "STOPS", count: counts.stops, tone: "stop" },
    { key: "warn", label: "WARN", count: counts.warn, tone: "warn" },
  ];

  return (
    <div
      style={{
        display: "flex",
        flexWrap: "wrap",
        alignItems: "center",
        gap: 4,
        padding: "7px 12px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        background: "#0a0a0b",
      }}
    >
      {chips.map((c) => {
        const active = filter === c.key;
        const palette = active
          ? c.tone === "stop"
            ? { bg: "rgba(239,68,68,0.10)", fg: "#ef4444", bd: "rgba(239,68,68,0.30)", n: "rgba(239,68,68,0.7)" }
            : c.tone === "warn"
            ? { bg: "rgba(240,165,0,0.10)", fg: "#f0a500", bd: "rgba(240,165,0,0.30)", n: "rgba(240,165,0,0.7)" }
            : { bg: "rgba(0,200,150,0.10)", fg: "#00c896", bd: "rgba(0,200,150,0.28)", n: "rgba(0,200,150,0.7)" }
          : {
              bg: "transparent",
              fg: "#71717a",
              bd: "rgba(255,255,255,0.08)",
              n: "rgba(255,255,255,0.4)",
            };
        return (
          <button
            key={c.key}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => setFilter(c.key)}
            style={{
              fontFamily: FONT_MONO,
              fontSize: 9.5,
              letterSpacing: "0.04em",
              padding: "3px 6px",
              borderRadius: 3,
              background: palette.bg,
              color: palette.fg,
              border: `1px solid ${palette.bd}`,
              cursor: "pointer",
              whiteSpace: "nowrap",
            }}
          >
            {c.label}
            <span style={{ marginLeft: 4, color: palette.n, fontWeight: 400 }}>
              {c.count}
            </span>
          </button>
        );
      })}
      {/* SYS — independent toggle (NOT mutually exclusive with the
          filter chips above). Off by default so bootstrap noise stays
          collapsed; the count exposes how much is hidden. */}
      <button
        type="button"
        role="switch"
        aria-checked={sysShown}
        title={
          sysShown
            ? "Hide system bootstrap & config lines"
            : "Show system bootstrap & config lines"
        }
        onClick={() => setSysShown((s) => !s)}
        style={{
          fontFamily: FONT_MONO,
          fontSize: 9.5,
          letterSpacing: "0.04em",
          padding: "3px 6px",
          borderRadius: 3,
          background: sysShown ? "rgba(255,255,255,0.06)" : "transparent",
          color: sysShown ? "#d4d4d8" : "#71717a",
          border: `1px solid ${
            sysShown ? "rgba(255,255,255,0.16)" : "rgba(255,255,255,0.08)"
          }`,
          cursor: "pointer",
          whiteSpace: "nowrap",
        }}
      >
        SYS
        <span
          style={{
            marginLeft: 4,
            color: sysShown ? "rgba(255,255,255,0.6)" : "rgba(255,255,255,0.4)",
            fontWeight: 400,
          }}
        >
          {counts.sys}
        </span>
      </button>
    </div>
  );
}

// ── Snapshot strip ──────────────────────────────────────────────────────────

function Snapshot({
  lastBar,
  lastRoi,
}: {
  lastBar?: BarUpdateData;
  lastRoi?: RoiReportData;
}) {
  const cells: {
    label: string;
    value: React.ReactNode;
    tone: "default" | "down" | "hl";
  }[] = [
    {
      label: "EQUITY",
      value: lastRoi ? fmtUSD(lastRoi.equity) : "—",
      tone: "default",
    },
    {
      label: "PNL",
      value: lastRoi ? fmtUSD(lastRoi.pnl) : "—",
      tone: lastRoi && lastRoi.pnl < 0 ? "down" : "default",
    },
    {
      label: "ROI",
      // Live 1x portfolio incr — entry-anchored, what the chart's NOW
      // line tracks against. Switched from sess (open-anchored window
      // return) to keep the snapshot aligned with the chart + KPI tile
      // above it. sess still drives the WIN column in the log table.
      value: lastBar ? fmtPct(lastBar.incr) : "—",
      tone: lastBar && lastBar.incr < 0 ? "down" : "default",
    },
    {
      label: "PEAK / TSL",
      // Two-color rendering: PEAK in the default tone, TSL in amber so
      // it pops as the live trailing-stop trigger floor. Both still
      // share the same cell so the strip stays at 5 columns.
      value: lastBar ? (
        <>
          {fmtPct(lastBar.peak, 2)}
          <span style={{ color: "#52525b" }}> / </span>
          <span style={{ color: "var(--amber)" }}>
            {fmtPct(lastBar.tsl, 2)}
          </span>
        </>
      ) : (
        "—"
      ),
      tone: "default",
    },
    {
      label: "Δ",
      value: lastRoi ? fmtPct(lastRoi.delta) : "—",
      tone: "hl",
    },
  ];

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, 1fr)",
        gap: 8,
        padding: "11px 12px",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
        background: "#0a0a0b",
      }}
    >
      {cells.map((c) => (
        <div
          key={c.label}
          style={{ display: "flex", flexDirection: "column", gap: 3, minWidth: 0 }}
        >
          <span
            style={{
              fontSize: 9,
              letterSpacing: "0.10em",
              color: "#52525b",
              fontFamily: FONT_MONO,
            }}
          >
            {c.label}
          </span>
          <span
            style={{
              fontSize: 12,
              color:
                c.tone === "down"
                  ? "#ef4444"
                  : c.tone === "hl"
                  ? "#00c896"
                  : "#d4d4d8",
              fontWeight: c.tone === "hl" ? 500 : 400,
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              fontVariantNumeric: "tabular-nums",
              fontFamily: FONT_MONO,
            }}
          >
            {c.value}
          </span>
        </div>
      ))}
    </div>
  );
}

// ── Viewport (scrollable rows) ──────────────────────────────────────────────

function Viewport({
  scrollRef,
  onScroll,
  items,
  collapsedStops,
  toggleStop,
  error,
  actAbsMax,
}: {
  scrollRef: React.RefObject<HTMLDivElement | null>;
  onScroll: () => void;
  items: Item[];
  collapsedStops: Set<number>;
  toggleStop: (n: number) => void;
  error: string | null;
  actAbsMax: number;
}) {
  return (
    <div
      ref={scrollRef}
      onScroll={onScroll}
      style={{
        flex: 1,
        overflowY: "auto",
        padding: "0 0 8px 0",
        scrollbarWidth: "thin",
        scrollbarColor: "#2c2c30 transparent",
        position: "relative",
      }}
    >
      <div
        style={{
          position: "relative",
          padding: "0 12px 0 32px",
          fontFeatureSettings: '"tnum"',
        }}
      >
        {/* Spine */}
        <div
          style={{
            position: "absolute",
            left: 18,
            top: 30,
            bottom: 0,
            width: 1,
            background: "rgba(255,255,255,0.06)",
          }}
        />

        {/* Sticky column header */}
        <ColumnHeader />

        {error && (
          <div
            style={{
              padding: "12px 0",
              color: "#ef4444",
              fontSize: 10,
              fontFamily: FONT_MONO,
            }}
          >
            {error}
          </div>
        )}

        {items.length === 0 && !error && (
          <div
            style={{
              padding: "16px 0",
              color: "#52525b",
              fontSize: 10,
              fontFamily: FONT_MONO,
            }}
          >
            No log lines for this allocation yet.
          </div>
        )}

        {items.map((item, idx) => {
          if (item.type === "tick") {
            return (
              <TickRow
                key={`tick-${item.bar}-${idx}`}
                item={item}
                actAbsMax={actAbsMax}
              />
            );
          }
          if (item.type === "stop_event") {
            return (
              <StopEventGroup
                key={`stop-${item.n}`}
                event={item}
                collapsed={collapsedStops.has(item.n)}
                onToggle={() => toggleStop(item.n)}
              />
            );
          }
          if (item.type === "system") {
            return <SystemRow key={`sys-${item.line.n}`} line={item.line} />;
          }
          return <GenericRow key={`g-${item.line.n}`} line={item.line} />;
        })}
      </div>
    </div>
  );
}

// ── Sticky column header ────────────────────────────────────────────────────

function ColumnHeader() {
  const headerCellStyle: React.CSSProperties = {
    fontSize: 8.5,
    letterSpacing: "0.12em",
    // Bumped from #52525b (very dim) to the panel's primary text tone
    // — column labels are referenced often enough that the readability
    // win outweighs the visual hierarchy "let data dominate" argument.
    color: "#d4d4d8",
    textTransform: "uppercase",
    fontFamily: FONT_MONO,
  };
  return (
    <div
      style={{
        position: "sticky",
        top: 0,
        zIndex: 3,
        background: "#080809",
        display: "flex",
        alignItems: "center",
        gap: 5,
        padding: "8px 0 7px 0",
        borderBottom: "1px solid rgba(255,255,255,0.06)",
      }}
    >
      <span style={{ ...headerCellStyle, width: 38 }}>TIME</span>
      <span style={{ ...headerCellStyle, width: 50 }}>BAR</span>
      <span style={{ ...headerCellStyle, width: 54, textAlign: "right" }}>WIN</span>
      <span style={{ ...headerCellStyle, width: 56, textAlign: "right" }}>LIVE</span>
      <span style={{ ...headerCellStyle, width: 56, textAlign: "right" }}>EXP</span>
      <span style={{ ...headerCellStyle, width: 56, textAlign: "right" }}>ACT</span>
      <span style={{ ...headerCellStyle, flex: 1, textAlign: "right" }}>Δ</span>
    </div>
  );
}

// ── Tick row ────────────────────────────────────────────────────────────────

function TickRow({
  item,
  actAbsMax,
}: {
  item: Item & { type: "tick" };
  actAbsMax: number;
}) {
  const bu = item.bar_update;
  const ri = item.roi_report;

  const sess = bu?.sess ?? null;
  const incr = bu?.incr ?? null;
  const expected = ri?.expected ?? null;
  const actual = ri?.actual ?? null;
  const delta = ri?.delta ?? null;

  return (
    <div
      style={{
        position: "relative",
        display: "flex",
        alignItems: "center",
        gap: 5,
        padding: "3px 0",
        lineHeight: 1.5,
        fontFamily: FONT_MONO,
      }}
    >
      <span style={{ width: 38, color: "#52525b", fontSize: 10 }}>
        {fmtTimeShort(item.ts)}
      </span>
      <span
        style={{
          width: 50,
          display: "flex",
          alignItems: "baseline",
          gap: 5,
        }}
      >
        <span
          style={{
            color: "#a1a1aa",
            fontWeight: 400,
            fontSize: 10.5,
            letterSpacing: "0.04em",
          }}
        >
          {item.bar}
        </span>
        <span style={{ color: "#52525b", fontSize: 9 }}>
          {bu ? `${bu.active}/${bu.universe}` : ""}
        </span>
      </span>

      {/* PEAK + TSL columns removed — surfaced only in the snapshot
          strip above. Keeps the row narrower and lets WIN/LIVE/EXP/ACT/Δ
          breathe. */}

      <span
        style={{
          width: 54,
          textAlign: "right",
          fontSize: 10.5,
          fontVariantNumeric: "tabular-nums",
          color:
            sess === null
              ? "#52525b"
              : sess < 0
              ? "#a07474"
              : sess > 0
              ? "#71b88a"
              : "#71717a",
        }}
      >
        {sess !== null ? fmtPct(sess, 2) : "—"}
      </span>
      <span
        style={{
          width: 56,
          textAlign: "right",
          fontSize: 10.5,
          fontVariantNumeric: "tabular-nums",
          color: incr !== null && incr > 0 ? "#71b88a" : "#71717a",
        }}
      >
        {incr !== null ? fmtPct(incr, 2) : "—"}
      </span>
      <span
        style={{
          width: 56,
          textAlign: "right",
          fontSize: 10.5,
          fontVariantNumeric: "tabular-nums",
          color: expected !== null && expected > 0 ? "#9ad6b5" : "#a1a1aa",
        }}
      >
        {expected !== null ? fmtPct(expected, 2) : "—"}
      </span>
      {/* ACT — column-normalized red/green heatmap tint, same intensity
          grammar as the Advanced matrix's symbol columns. Cap is 0.18
          (matches matrix); padding tightens vs the other cells so the
          tint reads as a contained pill rather than bleeding out. */}
      <span
        style={{
          width: 56,
          textAlign: "right",
          fontSize: 10.5,
          fontWeight: 500,
          fontVariantNumeric: "tabular-nums",
          color: actual === null ? "#52525b" : actual >= 0 ? "#00c896" : "#ef4444",
          background:
            actual === null ? "transparent" : cellTint(actual, actAbsMax, 0.18),
          borderRadius: 2,
          padding: "1px 4px",
          marginRight: -4,  // compensate for the padding so column widths line up
        }}
      >
        {actual !== null ? fmtPct(actual, 2) : "—"}
      </span>
      <span style={{ flex: 1, textAlign: "right" }}>
        {delta !== null ? (
          <span
            style={{
              display: "inline-block",
              padding: "2px 8px",
              borderRadius: 2,
              fontSize: 10.5,
              fontWeight: 500,
              fontVariantNumeric: "tabular-nums",
              background:
                delta >= 0 ? "rgba(0,200,150,0.10)" : "rgba(239,68,68,0.10)",
              color: delta >= 0 ? "#00c896" : "#ef4444",
            }}
          >
            {fmtPct(delta, 2)}
          </span>
        ) : (
          <span style={{ color: "#52525b", fontSize: 10.5 }}>—</span>
        )}
      </span>
    </div>
  );
}

// ── Stop event group ────────────────────────────────────────────────────────

function StopEventGroup({
  event,
  collapsed,
  onToggle,
}: {
  event: Item & { type: "stop_event" };
  collapsed: boolean;
  onToggle: () => void;
}) {
  // Compute meta: closed count + duration.
  const closedCount = event.lines.filter((l) => l.kind === "close_confirm").length;
  const symStops = event.lines.filter((l) => l.kind === "sym_stop");
  const lastConfirm = [...event.lines]
    .reverse()
    .find((l) => l.kind === "close_confirm");
  const startTs = symStops[0]?.ts ?? event.ts;
  const endTs = lastConfirm?.ts ?? startTs;
  const durSec = (() => {
    try {
      const a = Date.parse(startTs.replace(" ", "T") + "Z");
      const b = Date.parse(endTs.replace(" ", "T") + "Z");
      if (Number.isNaN(a) || Number.isNaN(b)) return null;
      return Math.round((b - a) / 1000);
    } catch {
      return null;
    }
  })();

  const meta = `${closedCount} closed${durSec !== null ? ` · ${durSec} sec` : ""}`;

  // Group lines by phase
  const phaseTrigger = event.lines.filter((l) => l.kind === "sym_stop");
  const phaseRecon = event.lines.filter((l) =>
    ["close_all", "position_check", "size_mismatch"].includes(l.kind || ""),
  );
  const phaseResolved = event.lines.filter((l) => l.kind === "close_confirm");

  return (
    <div
      style={{
        position: "relative",
        margin: "5px 0 6px 0",
        padding: "2px 0",
        fontFamily: FONT_MONO,
      }}
    >
      {/* Diamond marker on spine */}
      <span
        style={{
          position: "absolute",
          left: -20,
          top: 7,
          width: 11,
          height: 11,
          background: "#ef4444",
          transform: "rotate(45deg)",
          zIndex: 1,
        }}
      />
      {/* Header */}
      <div
        role="button"
        tabIndex={0}
        onClick={onToggle}
        onKeyDown={(e) => {
          if (e.key === " " || e.key === "Enter") {
            e.preventDefault();
            onToggle();
          }
        }}
        style={{
          display: "flex",
          alignItems: "baseline",
          gap: 8,
          cursor: "pointer",
          padding: "3px 0",
          userSelect: "none",
        }}
      >
        <span style={{ color: "#ef4444", fontSize: 10, width: 10, flexShrink: 0 }}>
          {collapsed ? "▸" : "▾"}
        </span>
        <span
          style={{
            color: "#ef4444",
            fontWeight: 500,
            letterSpacing: "0.06em",
            fontSize: 10.5,
            flexShrink: 0,
          }}
        >
          STOP EVENT
        </span>
        <span style={{ color: "#52525b", fontSize: 9.5 }}>
          {fmtTimeFull(startTs)}
        </span>
        <span style={{ color: "#71717a", fontSize: 9.5, marginLeft: "auto" }}>
          {meta}
        </span>
      </div>
      {/* Children */}
      {!collapsed && (
        <div
          style={{
            position: "relative",
            marginLeft: 8,
            padding: "5px 0 5px 16px",
          }}
        >
          {/* Sub-spine */}
          <div
            style={{
              position: "absolute",
              left: 0,
              top: 0,
              bottom: 0,
              width: 1,
              background: "rgba(239,68,68,0.30)",
            }}
          />
          {phaseTrigger.length > 0 && (
            <>
              <PhaseHeading label="TRIGGER" />
              {phaseTrigger.map((l) => (
                <StopEventLine key={l.n} line={l} />
              ))}
            </>
          )}
          {phaseRecon.length > 0 && (
            <>
              <PhaseHeading label="RECONCILIATION" />
              {phaseRecon.map((l) => (
                <StopEventLine key={l.n} line={l} />
              ))}
            </>
          )}
          {phaseResolved.length > 0 && (
            <>
              <PhaseHeading label="RESOLVED" />
              {phaseResolved.map((l) => (
                <StopEventLine key={l.n} line={l} />
              ))}
            </>
          )}
        </div>
      )}
    </div>
  );
}

function PhaseHeading({ label }: { label: string }) {
  return (
    <div
      style={{
        fontSize: 8.5,
        letterSpacing: "0.14em",
        color: "#52525b",
        textTransform: "uppercase",
        marginTop: 6,
        marginBottom: 3,
        paddingLeft: 2,
        fontFamily: FONT_MONO,
      }}
    >
      {label}
    </div>
  );
}

function StopEventLine({ line }: { line: LogLine }) {
  // Choose icon + content shape per kind.
  let icon = "·";
  let iconColor = "#5f5f66";
  let body: React.ReactNode;
  if (line.kind === "sym_stop") {
    const d = line.data as { symbol?: string; ret?: number; threshold?: number } | undefined;
    icon = "⚠";
    iconColor = "#ef4444";
    body = (
      <span>
        <span style={{ color: "#f4a4a4", fontWeight: 500 }}>{d?.symbol}</span>{" "}
        · ret <span style={{ color: "#ef4444" }}>{d?.ret !== undefined ? fmtPct(d.ret, 2) : "—"}</span>{" "}
        → clamp <span style={{ color: "#ef4444" }}>{d?.threshold !== undefined ? fmtPct(d.threshold, 1) : "—"}</span>
      </span>
    );
  } else if (line.kind === "close_all") {
    const d = line.data as { reason?: string } | undefined;
    icon = "↓";
    iconColor = "#71717a";
    body = (
      <span style={{ color: "#a1a1aa" }}>
        close all · reason <span style={{ color: "#d4d4d8" }}>{d?.reason}</span>
      </span>
    );
  } else if (line.kind === "position_check") {
    const d = line.data as { symbol?: string; size?: number } | undefined;
    body = (
      <span style={{ color: "#a1a1aa" }}>
        <span style={{ color: "#f4a4a4", fontWeight: 500 }}>{d?.symbol}</span>{" "}
        · size <span style={{ color: "#d4d4d8" }}>{d?.size}</span>
      </span>
    );
  } else if (line.kind === "size_mismatch") {
    const d = line.data as { symbol?: string; state?: number; blofin?: number } | undefined;
    icon = "≠";
    iconColor = "#f0a500";
    body = (
      <span style={{ color: "#a1a1aa" }}>
        <span style={{ color: "#f4a4a4", fontWeight: 500 }}>{d?.symbol}</span>{" "}
        · state <span style={{ color: "#d4d4d8" }}>{d?.state}</span> ≠ blofin{" "}
        <span style={{ color: "#d4d4d8" }}>{d?.blofin}</span>
      </span>
    );
  } else if (line.kind === "close_confirm") {
    const d = line.data as { symbol?: string } | undefined;
    icon = "✓";
    iconColor = "#00c896";
    body = (
      <span style={{ color: "#a1a1aa" }}>
        <span style={{ color: "#f4a4a4", fontWeight: 500 }}>{d?.symbol}</span>{" "}
        closed via blofin
      </span>
    );
  } else {
    body = <span style={{ color: "#a1a1aa" }}>{line.text}</span>;
  }

  return (
    <div
      style={{
        position: "relative",
        padding: "1px 0",
        display: "flex",
        alignItems: "baseline",
        gap: 7,
        fontSize: 10.5,
        lineHeight: 1.5,
        color: "#c1c1c6",
        fontFamily: FONT_MONO,
      }}
    >
      <span
        style={{
          position: "absolute",
          left: -16,
          top: 8,
          width: 12,
          height: 1,
          background: "rgba(239,68,68,0.30)",
        }}
      />
      <span
        style={{
          width: 10,
          flexShrink: 0,
          textAlign: "center",
          fontSize: 10,
          color: iconColor,
        }}
      >
        {icon}
      </span>
      <span
        style={{
          color: "#52525b",
          fontSize: 9.5,
          width: 50,
          flexShrink: 0,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {fmtTimeFull(line.ts)}
      </span>
      {body}
    </div>
  );
}

// ── Generic row (unmatched lines) ───────────────────────────────────────────

// Compact dim row for system_event lines (TraderConfig, allocation
// init/resume, rehydrate, session boundary). Single-line layout: time,
// dim SYS pill, truncated body. Click to expand the full text. Stays
// out of the column grid so bootstrap noise never claims tick-row
// real estate even when SYS visibility is toggled on.
function SystemRow({ line }: { line: LogLine }) {
  const [expanded, setExpanded] = useState(false);
  const subtype =
    (line.data as { subtype?: string } | undefined)?.subtype ?? "sys";
  return (
    <div
      role="button"
      tabIndex={0}
      onClick={() => setExpanded((v) => !v)}
      onKeyDown={(e) => {
        if (e.key === " " || e.key === "Enter") {
          e.preventDefault();
          setExpanded((v) => !v);
        }
      }}
      style={{
        display: "flex",
        alignItems: "baseline",
        gap: 7,
        padding: "1px 0",
        fontSize: 9.5,
        color: "#71717a",
        fontFamily: FONT_MONO,
        lineHeight: 1.55,
        cursor: "pointer",
        userSelect: "none",
      }}
    >
      <span
        style={{
          width: 38,
          color: "#42424a",
          fontSize: 9.5,
          flexShrink: 0,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {fmtTimeShort(line.ts)}
      </span>
      <span
        style={{
          background: "#1a1a1d",
          color: "#a1a1aa",
          fontSize: 8,
          letterSpacing: "0.10em",
          padding: "1px 5px",
          borderRadius: 2,
          flexShrink: 0,
          textTransform: "uppercase",
          fontFamily: FONT_MONO,
        }}
        title={`subtype: ${subtype}`}
      >
        SYS
      </span>
      <span
        style={{
          flex: 1,
          color: "#71717a",
          minWidth: 0,
          ...(expanded
            ? { whiteSpace: "pre-wrap", wordBreak: "break-word" }
            : {
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }),
        }}
      >
        {line.text}
      </span>
    </div>
  );
}

function GenericRow({ line }: { line: LogLine }) {
  const lvlColor =
    (line.level || "").toUpperCase() === "ERROR"
      ? "#ef4444"
      : (line.level || "").toUpperCase().startsWith("WARN")
      ? "#f0a500"
      : "#52525b";
  return (
    <div
      style={{
        display: "flex",
        alignItems: "baseline",
        gap: 8,
        padding: "2px 0",
        fontSize: 10.5,
        color: "#71717a",
        fontFamily: FONT_MONO,
        lineHeight: 1.5,
      }}
    >
      <span style={{ width: 38, color: "#52525b", fontSize: 10 }}>
        {fmtTimeShort(line.ts)}
      </span>
      <span style={{ color: lvlColor, fontSize: 9, letterSpacing: "0.10em", width: 36 }}>
        {(line.level || "").toUpperCase()}
      </span>
      <span style={{ flex: 1, whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
        {line.text}
      </span>
    </div>
  );
}

// ── Footer (typing animation + countdown + click-to-pause) ──────────────────

function Footer({
  paused,
  setPaused,
  animationsOff,
  sessionActive,
  lastBar,
}: {
  paused: boolean;
  setPaused: (next: boolean | ((prev: boolean) => boolean)) => void;
  animationsOff: boolean;
  sessionActive: boolean;
  lastBar?: BarUpdateData;
}) {
  // Build the message list — substitute live state into the template.
  const messages = useMemo(() => {
    return TYPING_MESSAGES_BASE.map((m) => {
      if (lastBar) {
        return m
          .replace("{active}", String(lastBar.active))
          .replace("{universe}", String(lastBar.universe));
      }
      return m;
    }).concat(
      lastBar
        ? [`${lastBar.active}/${lastBar.universe} positions active`]
        : [],
    );
  }, [lastBar]);

  // Typing state — track current message index, displayed length, phase.
  const [msgIdx, setMsgIdx] = useState(0);
  const [displayLen, setDisplayLen] = useState(0);
  const [phase, setPhase] = useState<"typing" | "hold" | "erasing" | "pause">("typing");

  useEffect(() => {
    if (animationsOff) return;
    const msg = messages[msgIdx % messages.length] ?? "";
    let timeout: ReturnType<typeof setTimeout>;
    if (phase === "typing") {
      if (displayLen < msg.length) {
        timeout = setTimeout(
          () => setDisplayLen((n) => n + 1),
          40 + Math.random() * 40,
        );
      } else {
        timeout = setTimeout(() => setPhase("hold"), 1700);
      }
    } else if (phase === "hold") {
      timeout = setTimeout(() => setPhase("erasing"), 0);
    } else if (phase === "erasing") {
      if (displayLen > 0) {
        timeout = setTimeout(() => setDisplayLen((n) => n - 1), 18);
      } else {
        timeout = setTimeout(() => setPhase("pause"), 0);
      }
    } else {
      // pause
      timeout = setTimeout(() => {
        setMsgIdx((i) => (i + 1) % messages.length);
        setPhase("typing");
      }, 250);
    }
    return () => clearTimeout(timeout);
  }, [animationsOff, phase, displayLen, msgIdx, messages]);

  // Reset typing on message-list change (e.g. lastBar update).
  useEffect(() => {
    setMsgIdx(0);
    setDisplayLen(0);
    setPhase("typing");
  }, [messages.length]);

  // Countdown — anchored to real bar boundary, ticks once per second.
  const [countdown, setCountdown] = useState(nextBarSeconds());
  useEffect(() => {
    const id = setInterval(() => setCountdown(nextBarSeconds()), 1000);
    return () => clearInterval(id);
  }, []);

  const togglePause = useCallback(() => {
    setPaused((p) => !p);
  }, [setPaused]);

  const currentMsg =
    messages[msgIdx % messages.length]?.slice(0, displayLen) ?? "";

  // Pulse + countdown stay live even when typing pauses on session-end.
  // Only the typing animation freezes; pulse turns dim if the session
  // is closed for the day.
  const dotColor = sessionActive ? "#00c896" : "#71717a";
  const pulseAnim = sessionActive ? "pls-pulse 2s ease-in-out infinite" : "none";
  const cursorOpacity = paused ? 0.35 : 1;
  const cursorAnim = paused ? "none" : "pls-blink 1.05s step-end infinite";

  return (
    <div
      role="button"
      tabIndex={0}
      aria-label={paused ? "Resume typing animation" : "Pause typing animation"}
      onClick={togglePause}
      onKeyDown={(e) => {
        if (e.key === " " || e.key === "Enter") {
          e.preventDefault();
          togglePause();
        }
      }}
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 10,
        padding: "10px 12px",
        borderTop: "1px solid rgba(255,255,255,0.08)",
        background: "#0c0c0e",
        fontSize: 10,
        color: "#71717a",
        cursor: "pointer",
        userSelect: "none",
        fontFamily: FONT_MONO,
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          flex: 1,
          minWidth: 0,
          overflow: "hidden",
        }}
      >
        <span
          style={{
            width: 5,
            height: 5,
            borderRadius: "50%",
            background: dotColor,
            boxShadow: sessionActive
              ? "0 0 5px rgba(0,200,150,0.6)"
              : "none",
            animation: pulseAnim,
            flexShrink: 0,
          }}
        />
        <span style={{ color: "#52525b", fontSize: 10, flexShrink: 0 }}>›</span>
        <span
          style={{
            color: "#a1a1aa",
            whiteSpace: "nowrap",
            overflow: "hidden",
            letterSpacing: "0.02em",
            minWidth: 0,
          }}
        >
          {currentMsg}
        </span>
        <span
          style={{
            color: "#00c896",
            display: "inline-block",
            animation: cursorAnim,
            marginLeft: 1,
            lineHeight: 1,
            opacity: cursorOpacity,
            transition: "opacity 0.2s",
          }}
        >
          ▊
        </span>
      </div>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 5,
          fontSize: 9,
          letterSpacing: "0.10em",
          color: "#52525b",
          flexShrink: 0,
        }}
      >
        NEXT BAR{" "}
        <b
          style={{
            color: "#a1a1aa",
            fontWeight: 400,
            fontVariantNumeric: "tabular-nums",
            letterSpacing: "0.04em",
            fontSize: 10,
          }}
        >
          {fmtCountdown(countdown)}
        </b>
      </div>
    </div>
  );
}

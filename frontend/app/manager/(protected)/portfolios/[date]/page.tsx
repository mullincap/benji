"use client";

/**
 * frontend/app/manager/(protected)/portfolios/[date]/page.tsx
 * ============================================================
 * Per-bar detail for one trading session. Renders:
 *   - Header with summary line + back link
 *   - 6 KPI cards
 *   - Cumulative ROI chart (one line per symbol + thicker portfolio line)
 *   - 5-min ROI matrix table (symbols across, bars down, Portfolio col last)
 *
 * Polls /api/manager/portfolios/{date} every 30s while session.status ===
 * "active" to surface new bars in real time. When status flips to "closed"
 * the polling stops automatically.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useParams, useSearchParams, useRouter } from "next/navigation";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
  type ChartOptions,
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
);

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const FONT_MONO = "var(--font-space-mono), Space Mono, monospace";
const POLL_MS = 30_000;

const PORTFOLIO_COLOR = "#00c896";
const SYMBOL_COLORS: Record<string, string> = {
  "BTC-USDT":  "#f7931a",
  "ETH-USDT":  "#627eea",
  "SOL-USDT":  "#00d18c",
  "DOGE-USDT": "#c3a634",
  "AVAX-USDT": "#e84142",
  "LINK-USDT": "#2a5ada",
};
const FALLBACK_PALETTE = ["#9945ff", "#f0a500", "#ed93b1", "#bb86fc", "#ff7a59", "#5fb6ff"];

function colorFor(inst: string, idx: number): string {
  return SYMBOL_COLORS[inst] ?? FALLBACK_PALETTE[idx % FALLBACK_PALETTE.length];
}

// ─── Types ──────────────────────────────────────────────────────────────────

interface PortfolioMeta {
  date: string;
  allocation_id: string | null;    // null for master rows (pre-multi-tenant)
  exchange: string | null;
  strategy_label: string | null;
  status: "active" | "closed";
  session_start_utc: string | null;
  exit_time_utc: string | null;
  exit_reason: string | null;
  symbols: string[];
  entered: string[];
  eff_lev: number;
  lev_int: number;
  // Early-fill trigger params from strategy config. early_fill_y is a
  // decimal fraction (0.09 = 9%); early_fill_x is minutes from session
  // open. session_start_hour drives the elapsed/remaining countdown.
  early_fill_y: number;
  early_fill_x: number;
  session_start_hour: number;
  // Latest open-anchored session_ret from the trader's runtime_state.
  // This is the exact value EARLY_FILL compares against. Prefer this
  // over computing from sym_returns — the running pre-fix trader writes
  // entry-anchored values to portfolio_bars.symbol_returns, so averaging
  // them gives `incr` (06:35 entry-anchored) instead of `sess`
  // (06:00 open-anchored). null for closed master sessions or allocations
  // missing runtime_state; in that case we fall back to the sym_returns
  // mean for visual continuity, accepting it'll match `incr` until celery
  // ships post-d9eac53 code.
  current_session_ret: number | null;
  // session_ret snapshot at the first bar past the fill window. Set once
  // the window closes; null while still open. The bar freezes its fill at
  // this ratio post-window so the user can see how close the portfolio
  // came to firing — a historical "score at the buzzer".
  fill_window_close_ret: number | null;
  fill_window_close_bar: number | null;
}

// When the backend detects multiple allocations on the requested date and no
// allocation_id query param was provided, it returns 400 with a picker payload.
// The detail page renders a chooser so the user can redirect to the correct URL.
interface AmbiguityDetail {
  message: string;
  available: Array<{
    allocation_id: string | null;
    exchange: string | null;
    strategy_label: string | null;
  }>;
}

interface PortfolioBar {
  bar: number;
  ts: string;
  incr: number;
  peak: number;
  sym_returns: Record<string, number>;
  stopped: string[];
}

interface PortfolioDetail {
  meta: PortfolioMeta;
  bars: PortfolioBar[];
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}%`;
}

function fmtTime(ts: string | null | undefined): string {
  if (!ts) return "—";
  // "2026-04-17 06:35:02" → "06:35"
  const part = ts.split(" ")[1] ?? "";
  return part.slice(0, 5);
}

// Whitelist of legitimate "final" exit reasons. subprocess_died,
// stale_close_failed, errored, etc. are transient/error states that
// shouldn't surface as a user-facing exit — render as LIVE if the
// session is still being written, otherwise as a generic ENDED label.
const TERMINAL_EXIT_REASONS: Record<string, { label: string; bg: string; fg: string }> = {
  port_sl:                         { label: "PORT SL",        bg: "var(--red-dim)",   fg: "var(--red)" },
  port_tsl:                        { label: "PORT TSL",       bg: "var(--red-dim)",   fg: "var(--red)" },
  early_fill:                      { label: "EARLY FILL",     bg: "var(--green-dim)", fg: "var(--green)" },
  session_close:                   { label: "SESSION CLOSE",  bg: "var(--bg3)",       fg: "var(--t1)" },
  sym_stop:                        { label: "SYM STOP",       bg: "var(--amber-dim)", fg: "var(--amber)" },
  filtered:                        { label: "FILTERED",       bg: "var(--bg3)",       fg: "var(--t2)" },
  no_entry_conviction:             { label: "NO CONVICTION",  bg: "var(--bg3)",       fg: "var(--t2)" },
  missed_window:                   { label: "MISSED WINDOW",  bg: "var(--bg3)",       fg: "var(--t2)" },
  late_entry_no_eligible_symbols:  { label: "ALL STOPPED",    bg: "var(--amber-dim)", fg: "var(--amber)" },
};

function exitBadge(reason: string | null, status: string) {
  // Treat the session as LIVE whenever status is active OR the recorded
  // exit_reason is one of the transient/error states (subprocess_died,
  // stale_close_failed, errored). Those almost always mean the trader
  // got interrupted but a respawn is writing bars, and surfacing a
  // permanent-looking exit label is misleading.
  const transientErrors = new Set(["subprocess_died", "stale_close_failed", "errored"]);
  if (status === "active" || (reason && transientErrors.has(reason))) {
    return (
      <span
        style={{
          display: "inline-block",
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.06em",
          padding: "2px 6px",
          borderRadius: 3,
          background: "var(--green-dim)",
          color: "var(--green)",
          fontFamily: FONT_MONO,
        }}
      >
        LIVE
      </span>
    );
  }
  if (!reason) return <span style={{ color: "var(--t3)" }}>—</span>;
  const known = TERMINAL_EXIT_REASONS[reason];
  const s = known ?? { label: reason.replace(/_/g, " ").toUpperCase(), bg: "var(--bg3)", fg: "var(--t2)" };
  return (
    <span
      style={{
        display: "inline-block",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.06em",
        padding: "2px 6px",
        borderRadius: 3,
        background: s.bg,
        color: s.fg,
        fontFamily: FONT_MONO,
      }}
    >
      {s.label}
    </span>
  );
}

// Open-anchored, equal-weight, stopped-clamped session return from a bar's
// symbol_returns map. The trader writes this exact map (already with stopped
// symbols clamped at stop_raw_pct) to portfolio_bars.symbol_returns —
// see backend/app/cli/trader_blofin.py:2511-2524 — so taking the mean of the
// values reproduces session_ret without persisting a separate column.
// Mirrors equal_weight_return() in trader_blofin.py:969 by skipping symbols
// missing from the map (delisted / fetch failure) rather than fabricating
// zeros, so the denominator shrinks the same way.
function sessionReturnFromBar(bar: PortfolioBar): number | null {
  const vals = Object.values(bar.sym_returns);
  if (vals.length === 0) return null;
  let sum = 0;
  for (const v of vals) sum += v;
  return sum / vals.length;
}

// Minutes elapsed since session_start_hour:00 UTC on the given date. The
// trader's fill window boundaries are UTC (fill_gate_dt at trader_blofin.py:
// 2270), so the countdown must be UTC-anchored — local time would mis-show
// "remaining" by the user's tz offset.
function minutesSinceSessionOpen(
  dateISO: string,
  sessionStartHourUTC: number,
  now: Date,
): number | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateISO);
  if (!m) return null;
  const openMs = Date.UTC(
    Number(m[1]), Number(m[2]) - 1, Number(m[3]),
    sessionStartHourUTC, 0, 0, 0,
  );
  return (now.getTime() - openMs) / 60_000;
}

function fmtMinutesRemaining(mins: number): string {
  const total = Math.max(0, Math.floor(mins));
  const h = Math.floor(total / 60);
  const m = total % 60;
  if (h <= 0) return `${m}m`;
  return `${h}h ${m}m`;
}

function LivePulse() {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        color: "var(--green)",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.12em",
        fontFamily: FONT_MONO,
      }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: "50%",
          background: "var(--green)",
          animation: "portfolio-pulse 1.6s ease-in-out infinite",
        }}
      />
      LIVE
    </span>
  );
}

// ─── Early-Fill Progress Bar ────────────────────────────────────────────────
//
// Visualizes how close the portfolio is to the early-fill take-profit trigger.
// The trigger fires when session_ret >= early_fill_y AND we're still inside
// [session_open, session_open + early_fill_x]. See trader_blofin.py:2614.
//
// We compute session_ret from the latest bar's sym_returns (open-anchored,
// stopped-clamped — same map the trader writes), NOT account-actual ROI, so
// the bar tracks what the trader actually compares against.

function EarlyFillProgressBar({
  sessionRet,
  earlyFillY,
  earlyFillX,
  date,
  sessionStartHourUTC,
  isActive,
  fillWindowCloseRet,
}: {
  sessionRet: number | null;
  earlyFillY: number;
  earlyFillX: number;
  date: string;
  sessionStartHourUTC: number;
  isActive: boolean;
  fillWindowCloseRet: number | null;
}) {
  // Live-tick the countdown each second so "remaining" stays current
  // between 30s polls. Skip the interval once the session is closed —
  // there's nothing left to count down.
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    if (!isActive) return;
    const id = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(id);
  }, [isActive]);

  const elapsed = minutesSinceSessionOpen(date, sessionStartHourUTC, now);
  const remaining = elapsed !== null ? earlyFillX - elapsed : null;
  const windowOpen = remaining !== null && remaining > 0;

  // Past the fill gate, render TWO layered semi-transparent bars: the frozen
  // close-ROI (where the portfolio landed at the buzzer — trigger reference)
  // and the live session_ret (still moving even though the trigger is dead).
  // A vertical tick at the close-ROI mark anchors the comparison so the user
  // can read "how much did the portfolio drift since the window expired".
  // Pre-close, only the live bar renders (single-bar progress to target).
  const showLayered = !windowOpen && fillWindowCloseRet !== null;

  const liveCur = sessionRet ?? 0;
  const closeCur = fillWindowCloseRet ?? 0;

  const liveFired  = sessionRet !== null && liveCur >= earlyFillY;
  const closeFired = fillWindowCloseRet !== null && closeCur >= earlyFillY;

  const liveRatio  = earlyFillY > 0 ? liveCur  / earlyFillY : 0;
  const closeRatio = earlyFillY > 0 ? closeCur / earlyFillY : 0;
  const liveWidthPct  = Math.min(100, Math.max(0, liveRatio  * 100));
  const closeWidthPct = Math.min(100, Math.max(0, closeRatio * 100));

  // The "primary" value used for the headline number + gap-to-target text.
  // Pre-close: live (we want to see live progress toward the trigger).
  // Post-close: close (the historical buzzer value is the canonical fact;
  // the live drift since shows separately as the SINCE delta below).
  const primaryRet  = showLayered ? fillWindowCloseRet : sessionRet;
  const primaryCur  = primaryRet ?? 0;
  const primaryFired = showLayered ? closeFired : liveFired;
  const gap = earlyFillY - primaryCur;

  // Color: green if the relevant value crossed threshold, amber otherwise.
  // In layered mode, picking close-bar color drives the badge (the trigger
  // reference); the directional delta sliver between close and live picks
  // its own color (green=gain, red=loss) inline at render time.
  const fillColor = primaryFired ? "var(--green)" : "var(--amber)";
  const fillBg    = primaryFired ? "var(--green-dim)" : "var(--amber-dim)";

  const liveCurPctStr  = sessionRet === null
    ? "—"
    : `${liveCur >= 0 ? "+" : ""}${(liveCur * 100).toFixed(2)}%`;
  const closeCurPctStr = fillWindowCloseRet === null
    ? "—"
    : `${closeCur >= 0 ? "+" : ""}${(closeCur * 100).toFixed(2)}%`;
  const targetPctStr = `${(earlyFillY * 100).toFixed(2)}%`;

  // Drift since window close: live - close. Positive = portfolio kept
  // climbing; negative = drifted back. Only meaningful in layered mode.
  const sinceDelta = (sessionRet !== null && fillWindowCloseRet !== null)
    ? liveCur - closeCur
    : null;
  const sinceStr = sinceDelta === null
    ? null
    : `${sinceDelta >= 0 ? "+" : ""}${(sinceDelta * 100).toFixed(2)}% SINCE`;
  const sinceColor = sinceDelta === null
    ? "var(--t2)"
    : sinceDelta >= 0 ? "var(--green)" : "var(--red)";

  let timeBadge: string;
  if (showLayered) {
    timeBadge = closeFired ? "TARGET HIT AT CLOSE" : "WINDOW CLOSED";
  } else if (!isActive) {
    timeBadge = "SESSION CLOSED";
  } else if (!windowOpen) {
    timeBadge = "FILL WINDOW CLOSED";
  } else {
    timeBadge = `${fmtMinutesRemaining(remaining as number)} REMAINING`;
  }

  let gapStr: string;
  if (primaryRet === null) {
    gapStr = "—";
  } else if (primaryFired) {
    gapStr = showLayered ? "TARGET HIT" : "TARGET REACHED";
  } else if (showLayered) {
    gapStr = `${(gap * 100).toFixed(2)}% SHORT`;
  } else {
    gapStr = `${(gap * 100).toFixed(2)}% TO TARGET`;
  }

  const badgeMuted = !isActive || (!windowOpen && !showLayered);

  return (
    <div
      style={{
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 5,
        padding: "12px 16px",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 10,
          gap: 12,
          flexWrap: "wrap",
        }}
      >
        <div
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "var(--t3)",
            textTransform: "uppercase",
          }}
        >
          Early-Fill Progress
          <span
            style={{
              marginLeft: 10,
              color: "var(--t2)",
              fontWeight: 400,
              letterSpacing: "0.06em",
              textTransform: "none",
            }}
          >
            · {showLayered
                ? "session ROI at fill-window close vs live"
                : "session ROI vs portfolio take-profit"}
          </span>
        </div>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 14,
            fontSize: 10,
            fontFamily: FONT_MONO,
            flexWrap: "wrap",
          }}
        >
          {showLayered ? (
            <span style={{ fontWeight: 700 }}>
              <span style={{ color: "var(--t2)", fontSize: 9, letterSpacing: "0.08em", marginRight: 4 }}>CLOSE</span>
              <span style={{ color: closeFired ? "var(--green)" : "var(--t1)" }}>{closeCurPctStr}</span>
              <span style={{ color: "var(--t3)", margin: "0 8px" }}>·</span>
              <span style={{ color: "var(--t2)", fontSize: 9, letterSpacing: "0.08em", marginRight: 4 }}>NOW</span>
              <span style={{ color: liveFired ? "var(--green)" : "var(--t1)" }}>{liveCurPctStr}</span>
              <span style={{ color: "var(--t3)", margin: "0 8px" }}>/</span>
              <span style={{ color: "var(--t2)" }}>{targetPctStr}</span>
            </span>
          ) : (
            <span style={{ color: primaryFired ? "var(--green)" : "var(--t1)", fontWeight: 700 }}>
              {liveCurPctStr} <span style={{ color: "var(--t3)" }}>/</span>{" "}
              <span style={{ color: "var(--t2)" }}>{targetPctStr}</span>
            </span>
          )}
          <span style={{ color: primaryFired ? "var(--green)" : "var(--amber)", fontWeight: 700 }}>
            {gapStr}
          </span>
          {showLayered && sinceStr && (
            <span style={{ color: sinceColor, fontWeight: 700 }}>
              {sinceStr}
            </span>
          )}
          <span
            style={{
              padding: "2px 6px",
              borderRadius: 3,
              background: badgeMuted ? "var(--bg3)" : fillBg,
              color:      badgeMuted ? "var(--t2)"  : fillColor,
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.06em",
            }}
          >
            {timeBadge}
          </span>
        </div>
      </div>
      <div
        style={{
          position: "relative",
          width: "100%",
          height: 10,
          background: "var(--bg3)",
          borderRadius: 3,
          overflow: "hidden",
          border: "1px solid var(--line)",
        }}
      >
        {showLayered ? (
          <>
            {/* Common-ground base: bar fills to min(close, live). Color
                reflects close-trigger state (the historical anchor) — green
                if the trigger fired at the buzzer, amber otherwise. */}
            <div
              style={{
                position: "absolute",
                top: 0, left: 0,
                width: `${Math.min(closeWidthPct, liveWidthPct)}%`,
                height: "100%",
                background: closeFired ? "var(--green)" : "var(--amber)",
                opacity: 0.7,
                transition: "width 0.4s ease",
              }}
            />
            {/* Directional delta sliver between live and close. Green when
                live > close (gained since buzzer), red when live < close
                (drifted back). Sits between min(close,live) and max(close,live). */}
            {Math.abs(closeWidthPct - liveWidthPct) > 0.01 && (
              <div
                style={{
                  position: "absolute",
                  top: 0,
                  left: `${Math.min(closeWidthPct, liveWidthPct)}%`,
                  width: `${Math.abs(closeWidthPct - liveWidthPct)}%`,
                  height: "100%",
                  background: liveCur >= closeCur ? "var(--green)" : "var(--red)",
                  opacity: 0.7,
                  transition: "width 0.4s ease, left 0.4s ease",
                }}
              />
            )}
            {/* Vertical tick anchoring the close-ROI mark. Always sits at
                closeWidthPct so the user sees the buzzer reference even
                when the live bar overshoots or undershoots. */}
            <div
              style={{
                position: "absolute",
                top: 0,
                left: `calc(${closeWidthPct}% - 1px)`,
                width: 2,
                height: "100%",
                background: "var(--t0)",
                opacity: 0.85,
              }}
            />
          </>
        ) : (
          <div
            style={{
              width: `${liveWidthPct}%`,
              height: "100%",
              background: fillColor,
              transition: "width 0.4s ease",
            }}
          />
        )}
      </div>
    </div>
  );
}

// ─── KPI Card ───────────────────────────────────────────────────────────────

function KpiCard({
  label,
  value,
  color,
  subtitle,
}: {
  label: string;
  value: React.ReactNode;
  color?: string;
  subtitle?: string;
}) {
  return (
    <div
      style={{
        flex: 1,
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 5,
        padding: "14px 16px",
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          color: "var(--t3)",
          textTransform: "uppercase",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 18,
          fontWeight: 700,
          color: color || "var(--t0)",
          fontFamily: FONT_MONO,
        }}
      >
        {value}
      </div>
      {subtitle && (
        <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
          {subtitle}
        </div>
      )}
    </div>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

export default function PortfolioDetailPage() {
  const params = useParams<{ date: string }>();
  const searchParams = useSearchParams();
  const router = useRouter();
  const date = params.date;
  const allocationId = searchParams.get("allocation_id");

  const [data, setData] = useState<PortfolioDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [ambiguity, setAmbiguity] = useState<AmbiguityDetail | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  // Late-entry override state
  const [lateEntryFiring, setLateEntryFiring] = useState(false);
  const [lateEntrySpawning, setLateEntrySpawning] = useState(false);
  const [lateEntryError, setLateEntryError] = useState<string | null>(null);

  const fireLateEntry = useCallback(async () => {
    if (!data || !allocationId) return;
    const lastBar = data.bars.length > 0 ? data.bars[data.bars.length - 1] : null;
    const previewPct = lastBar ? lastBar.incr * 100 : 0;
    if (!confirm(
      `Spawn the trader in late-entry mode now?\n\n` +
      `Allocation: ${allocationId.slice(0, 8)}\n` +
      `Date: ${date}\n` +
      `Current preview portfolio: ${previewPct.toFixed(2)}%\n\n` +
      `The trader will skip conviction gates and enter at current marks. ` +
      `Symbols already past the per-symbol stop threshold will be excluded automatically. ` +
      `Your live performance will diverge from the audit's backtest for today.`
    )) return;
    setLateEntryFiring(true);
    setLateEntryError(null);
    try {
      const resp = await fetch(
        `${API_BASE}/api/manager/portfolios/${date}/late-entry`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify({
            allocation_id: allocationId,
            confirm: true,
          }),
        },
      );
      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(`HTTP ${resp.status}: ${txt}`);
      }
      // Hand off to the fast-poll effect which watches for the trader's
      // first runtime_state write (exit_reason transitions away from
      // preview_late_entry). Avoid window.location.reload() — it dropped
      // users on a "not found" page when the URL lost its allocation_id
      // search param.
      setLateEntrySpawning(true);
    } catch (e) {
      setLateEntryError(e instanceof Error ? e.message : String(e));
    } finally {
      setLateEntryFiring(false);
    }
  }, [data, allocationId, date]);

  // Chart view mode persisted in the URL so users can share links to a
  // specific layout. Hidden-symbol state is local-only and resets on
  // navigation — too granular to belong in the URL.
  const view: "portfolio" | "symbols" =
    searchParams.get("view") === "symbols" ? "symbols" : "portfolio";
  const [hiddenSymbols, setHiddenSymbols] = useState<Set<string>>(new Set());

  const setView = useCallback((next: "portfolio" | "symbols") => {
    const params = new URLSearchParams(searchParams.toString());
    if (next === "symbols") params.set("view", "symbols");
    else params.delete("view");
    const qs = params.toString();
    router.replace(qs ? `?${qs}` : "?", { scroll: false });
  }, [router, searchParams]);

  const toggleSymbolHidden = useCallback((label: string) => {
    setHiddenSymbols((prev) => {
      const next = new Set(prev);
      if (next.has(label)) next.delete(label);
      else next.add(label);
      return next;
    });
  }, []);

  const matrixRef = useRef<HTMLDivElement>(null);
  const wasAtBottomRef = useRef(true);

  const load = useCallback(async () => {
    try {
      const url = allocationId
        ? `${API_BASE}/api/manager/portfolios/${date}?allocation_id=${allocationId}`
        : `${API_BASE}/api/manager/portfolios/${date}`;
      const r = await fetch(url, { credentials: "include" });
      if (r.status === 401) throw new Error("Session expired");
      if (r.status === 404) throw new Error("Portfolio not found for this date");
      if (r.status === 400) {
        // Multi-allocation ambiguity — surface the picker instead of an error.
        const body = await r.json().catch(() => null);
        const detail = body?.detail;
        if (detail && typeof detail === "object" && Array.isArray(detail.available)) {
          setAmbiguity(detail as AmbiguityDetail);
          setError(null);
          return;
        }
        throw new Error(typeof detail === "string" ? detail : `HTTP 400`);
      }
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const json = (await r.json()) as PortfolioDetail;
      // Capture scroll position before state update so we can restore it.
      const el = matrixRef.current;
      if (el) {
        wasAtBottomRef.current =
          el.scrollHeight - el.scrollTop - el.clientHeight < 24;
      }
      setAmbiguity(null);
      setData(json);
      setLastUpdated(new Date());
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [date, allocationId]);

  // Initial load + polling for active sessions
  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (!data) return;
    if (data.meta.status !== "active") return;
    const id = setInterval(load, POLL_MS);
    return () => clearInterval(id);
  }, [data, load]);

  // Late-entry fast-poll: while the trader is starting, hit the detail
  // endpoint every 3s for up to 3 minutes. Trader entry typically takes
  // 60-120s (signal load + pre-filter + entry order placement). When
  // exit_reason flips off "preview_late_entry", spawn is complete and the
  // regular active-session 30s poll takes over.
  useEffect(() => {
    if (!lateEntrySpawning) return;
    const start = Date.now();
    const id = setInterval(() => {
      if (Date.now() - start > 180_000) {
        setLateEntryError(
          "Trader didn't transition to live within 3 min. Check the trader log for errors."
        );
        setLateEntrySpawning(false);
        return;
      }
      load();
    }, 3000);
    return () => clearInterval(id);
  }, [lateEntrySpawning, load]);

  // Detect spawn-complete transition (any "preview_*" → live).
  useEffect(() => {
    if (!lateEntrySpawning) return;
    const er = data?.meta.exit_reason;
    const isPreview = er === "preview_late_entry" || er === "preview_no_entry";
    if (data && !isPreview) {
      setLateEntrySpawning(false);
    }
  }, [data, lateEntrySpawning]);

  // Auto-scroll matrix to bottom when new bars arrive (only if user was already at bottom)
  useEffect(() => {
    if (!data) return;
    const el = matrixRef.current;
    if (!el) return;
    if (wasAtBottomRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [data]);

  const symbolsOrdered = useMemo(() => {
    if (!data) return [] as string[];
    // Order by entered first, then any extras present in symbols
    const known = new Set(data.meta.entered);
    const ordered: string[] = [...data.meta.entered];
    for (const s of data.meta.symbols) {
      if (!known.has(s)) ordered.push(s);
    }
    return ordered;
  }, [data]);

  const stoppedAtBar = useMemo(() => {
    // For each symbol, the bar number where it first appears in `stopped`.
    if (!data) return new Map<string, number>();
    const m = new Map<string, number>();
    for (const b of data.bars) {
      for (const s of b.stopped || []) {
        if (!m.has(s)) m.set(s, b.bar);
      }
    }
    return m;
  }, [data]);

  if (ambiguity) {
    // Multiple allocations on this date. Render a picker that redirects to
    // the disambiguated URL. Reached when the user lands on /portfolios/{date}
    // with no allocation_id query param AND ≥2 sessions exist for that date.
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--t1)", maxWidth: 520 }}>
        <div style={{ marginBottom: 12, color: "var(--t0)", fontSize: 13, fontWeight: 700 }}>
          Multiple allocations for {date}
        </div>
        <div style={{ marginBottom: 16, fontSize: 10, color: "var(--t2)", lineHeight: 1.6 }}>
          {ambiguity.message}
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {ambiguity.available.map((opt) => (
            <button
              key={opt.allocation_id ?? "master"}
              type="button"
              onClick={() => {
                const suffix = opt.allocation_id
                  ? `?allocation_id=${opt.allocation_id}`
                  : "";
                router.push(`/manager/portfolios/${date}${suffix}`);
              }}
              style={{
                background: "var(--bg2)",
                border: "1px solid var(--line)",
                borderRadius: 4,
                padding: "10px 14px",
                fontFamily: FONT_MONO,
                fontSize: 10,
                color: "var(--t1)",
                textAlign: "left",
                cursor: "pointer",
              }}
            >
              {opt.allocation_id
                ? `${opt.exchange} · ${opt.strategy_label}`
                : "Master (pre-multi-tenant)"}
            </button>
          ))}
        </div>
        <div style={{ marginTop: 18 }}>
          <Link href="/manager/portfolios" style={{ color: "var(--t2)", fontSize: 10 }}>
            ← BACK TO PORTFOLIOS
          </Link>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--red)" }}>
        Error: {error}
        <div style={{ marginTop: 14 }}>
          <Link href="/manager/portfolios" style={{ color: "var(--t2)", fontSize: 10 }}>
            ← BACK TO PORTFOLIOS
          </Link>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: 28, fontSize: 11, color: "var(--t2)" }}>
        Loading portfolio…
      </div>
    );
  }

  const { meta, bars } = data;
  const live = meta.status === "active";
  const lastBar = bars[bars.length - 1];
  const final = lastBar ? lastBar.incr * 100 : 0;
  const peak = lastBar ? lastBar.peak * 100 : 0;
  // Open-anchored portfolio return — what the trader's EARLY_FILL trigger
  // compares to early_fill_y. Prefer the meta value (the trader's own
  // runtime_state.session_ret, exactly the trigger's input). Fall back to
  // averaging the latest bar's sym_returns only if the meta value is
  // missing (closed master rows / allocations without runtime_state).
  const currentSessionRet =
    meta.current_session_ret !== null && meta.current_session_ret !== undefined
      ? meta.current_session_ret
      : lastBar
      ? sessionReturnFromBar(lastBar)
      : null;
  const dd = bars.length
    ? Math.min(...bars.map((b) => b.incr - b.peak)) * 100
    : 0;
  const startTime = fmtTime(meta.session_start_utc);
  const endTime = live ? "LIVE" : fmtTime(meta.exit_time_utc);
  const symStopsCount = stoppedAtBar.size;
  const enteredCount = meta.entered.length;

  // Chart datasets: one per symbol + portfolio line
  const labels = bars.map((b) => fmtTime(b.ts));
  const symbolDatasets = symbolsOrdered.map((sym, idx) => {
    const stoppedBar = stoppedAtBar.get(sym);
    const displayLabel = sym.replace("-USDT", "");
    const data = bars.map((b) => {
      const v = b.sym_returns[sym];
      return v !== undefined ? v * 100 : null;
    });
    return {
      label: displayLabel,
      data,
      borderColor: colorFor(sym, idx),
      backgroundColor: colorFor(sym, idx),
      borderWidth: 1.5,
      pointRadius: 0,
      pointHoverRadius: 4,
      tension: 0.15,
      hidden: hiddenSymbols.has(displayLabel),
      // Dash the segment after the stop bar.
      segment: stoppedBar !== undefined
        ? {
            borderDash: (ctx: { p1DataIndex: number }) => {
              const barNum = bars[ctx.p1DataIndex]?.bar ?? 0;
              return barNum >= stoppedBar ? [4, 4] : undefined;
            },
          }
        : undefined,
      _isSymbol: true,
    };
  });
  // Portfolio series always renders as a filled area anchored to the zero
  // axis (not the chart min) — matches the trader-page Performance chart's
  // gradient treatment so the pair reads as part of the same design system.
  const portfolioDataset = {
    label: "Portfolio",
    data: bars.map((b) => b.incr * 100),
    borderColor: PORTFOLIO_COLOR,
    backgroundColor: (ctx: { chart: ChartJS }) => {
      const c = ctx.chart.ctx;
      const area = ctx.chart.chartArea;
      if (!area) return PORTFOLIO_COLOR + "26";
      const gradient = c.createLinearGradient(0, area.top, 0, area.bottom);
      gradient.addColorStop(0, PORTFOLIO_COLOR + "33");
      gradient.addColorStop(1, PORTFOLIO_COLOR + "03");
      return gradient;
    },
    fill: "origin" as const,
    borderWidth: 3,
    pointRadius: 0,
    pointHoverRadius: 5,
    tension: 0.15,
    _isSymbol: false,
  };

  // Datasets shown to Chart.js depend on the mode. Portfolio-only mode strips
  // symbol lines entirely (cleaner view); All Symbols mode stacks them under
  // the portfolio area. The `hidden` flag on each symbol dataset is driven by
  // React state so toggle visibility survives mode switches.
  const chartDatasets = view === "symbols"
    ? [portfolioDataset, ...symbolDatasets]
    : [portfolioDataset];

  const chartOptions: ChartOptions<"line"> = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: "index", intersect: false },
    animation: { duration: 250 },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "#0e0e10",
        borderColor: "#2a2a2e",
        borderWidth: 1,
        titleColor: "#f0ede6",
        bodyColor: "#a09d96",
        titleFont: { family: "Space Mono", size: 10, weight: "bold" },
        bodyFont:  { family: "Space Mono", size: 10 },
        padding: 8,
        itemSort: (a, b) => {
          // Portfolio row always first so it anchors the tooltip.
          const aPortfolio = a.dataset.label === "Portfolio" ? -1 : 0;
          const bPortfolio = b.dataset.label === "Portfolio" ? -1 : 0;
          return aPortfolio - bPortfolio;
        },
        callbacks: {
          labelColor: (ctx) => {
            const isPortfolio = ctx.dataset.label === "Portfolio";
            return {
              borderColor: PORTFOLIO_COLOR,
              backgroundColor: isPortfolio ? PORTFOLIO_COLOR : (ctx.dataset.borderColor as string),
              borderWidth: isPortfolio ? 0 : 2,
              borderRadius: isPortfolio ? 2 : 0,
            };
          },
          label: (ctx) => {
            const v = ctx.parsed.y;
            const name = ctx.dataset.label ?? "";
            if (v === null || v === undefined) return `${name}: —`;
            const pct = `${v >= 0 ? "+" : ""}${v.toFixed(3)}%`;
            // Emphasize Portfolio row with an arrow bullet so the line stands
            // out among the symbol rows in All Symbols mode.
            return name === "Portfolio" ? `▸ ${name}: ${pct}` : `  ${name}: ${pct}`;
          },
        },
      },
    },
    scales: {
      x: {
        ticks: {
          color: "#5a5754",
          font: { family: "Space Mono", size: 9 },
          maxRotation: 0,
          autoSkip: true,
          maxTicksLimit: 12,
        },
        grid: { color: "#1a1a1d" },
      },
      y: {
        ticks: {
          color: "#5a5754",
          font: { family: "Space Mono", size: 9 },
          callback: (val) => `${Number(val).toFixed(1)}%`,
        },
        grid: { color: "#1a1a1d" },
      },
    },
  };

  // Matrix shows every bar at 5-min resolution. The previous 30-min
  // subsample toggle was removed — users wanted full fidelity by default
  // and the table already scrolls inside a fixed-height container.
  const matrixBars = bars;

  return (
    <>
      <style jsx global>{`
        @keyframes portfolio-pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%      { opacity: 0.35; transform: scale(0.85); }
        }
      `}</style>
      <div
        style={{
          padding: 20,
          display: "flex",
          flexDirection: "column",
          gap: 14,
          height: "100%",
          overflow: "auto",
          fontFamily: FONT_MONO,
        }}
      >
        {/* Back link + header */}
        <div>
          <Link
            href="/manager/portfolios"
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
              textDecoration: "none",
            }}
          >
            ← Back to Portfolios
          </Link>
          <div
            style={{
              display: "flex",
              alignItems: "baseline",
              gap: 14,
              marginTop: 6,
            }}
          >
            <span
              style={{
                fontSize: 18,
                fontWeight: 700,
                color: "var(--t0)",
              }}
            >
              {meta.date}
            </span>
            <span style={{ fontSize: 11, color: "var(--t2)" }}>
              {startTime} → {live ? "" : endTime}
              {live && <LivePulse />}
              {" · "}
              {bars.length} bars
              {(() => {
                // Suppress stale/transient exit_reason text when the
                // session is still being written. subprocess_died is a
                // crash marker; if a respawn is updating bars, the
                // session hasn't actually ended.
                const transient = new Set(["subprocess_died", "stale_close_failed", "errored"]);
                if (live) return null;
                if (meta.exit_reason && transient.has(meta.exit_reason)) return null;
                return meta.exit_reason ? ` · ${meta.exit_reason.replace(/_/g, " ")} exit` : null;
              })()}
              {" · "}
              {enteredCount} symbols active
              {symStopsCount > 0 && ` (${symStopsCount} stopped)`}
            </span>
          </div>
          {lastUpdated && live && (
            <div style={{ fontSize: 9, color: "var(--t3)", marginTop: 4 }}>
              LAST UPDATED · {lastUpdated.toISOString().slice(11, 19)} UTC
            </div>
          )}
          {(meta.exit_reason === "preview_late_entry" || meta.exit_reason === "preview_no_entry") && (
            <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 6 }}>
              <button
                onClick={fireLateEntry}
                disabled={lateEntryFiring || lateEntrySpawning}
                style={{
                  background: lateEntrySpawning ? "var(--amber)" : "var(--green)",
                  color: "var(--bg0)",
                  fontWeight: 700,
                  fontSize: 11,
                  letterSpacing: "0.08em",
                  textTransform: "uppercase",
                  padding: "10px 18px",
                  border: "none",
                  cursor: (lateEntryFiring || lateEntrySpawning) ? "wait" : "pointer",
                  opacity: lateEntryFiring ? 0.5 : 1,
                  fontFamily: "inherit",
                }}
              >
                {lateEntryFiring
                  ? "Sending request…"
                  : lateEntrySpawning
                    ? "Trader spawning… (waiting for first bar)"
                    : "▸ Manual Override · Enter Now"}
              </button>
              <div style={{ fontSize: 9, color: "var(--t3)", maxWidth: 360 }}>
                {lateEntrySpawning
                  ? "Polling every 3s. Page updates automatically when the trader writes its first bar (~60-120s)."
                  : "Preview portfolio. Click to spawn the trader in late-entry mode. The trader will bypass conviction gates and enter at current marks."}
              </div>
              {lateEntryError && (
                <div style={{ fontSize: 10, color: "var(--red)" }}>{lateEntryError}</div>
              )}
            </div>
          )}
        </div>

        {/* KPI cards */}
        <div style={{ display: "flex", gap: 10 }}>
          <KpiCard
            label="Portfolio ROI"
            value={fmtPct(final)}
            color={final >= 0 ? "var(--green)" : "var(--red)"}
          />
          <KpiCard label="Peak ROI" value={fmtPct(peak)} />
          <KpiCard
            label="Max Drawdown"
            value={fmtPct(dd)}
            color={dd <= -2 ? "var(--red)" : undefined}
          />
          <KpiCard
            label="Symbols"
            value={`${enteredCount - symStopsCount} / ${enteredCount}`}
            subtitle={symStopsCount > 0 ? `${symStopsCount} stopped` : undefined}
          />
          <KpiCard label="Eff Leverage" value={`${meta.eff_lev.toFixed(2)}x`} />
          <KpiCard label="Exit" value={exitBadge(meta.exit_reason, meta.status)} />
        </div>

        {/* Early-fill take-profit progress */}
        <EarlyFillProgressBar
          sessionRet={currentSessionRet}
          earlyFillY={meta.early_fill_y}
          earlyFillX={meta.early_fill_x}
          date={meta.date}
          sessionStartHourUTC={meta.session_start_hour}
          isActive={live}
          fillWindowCloseRet={meta.fill_window_close_ret}
        />

        {/* Chart */}
        <div
          style={{
            background: "var(--bg1)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 10,
              gap: 16,
              flexWrap: "wrap",
            }}
          >
            <div
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                color: "var(--t3)",
                textTransform: "uppercase",
              }}
            >
              Cumulative ROI {view === "symbols" ? "by Symbol" : ""}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 14, flexWrap: "wrap" }}>
              {/* Custom legend — only shown in All Symbols mode. Symbols are
                  clickable to toggle visibility; Portfolio is always visible
                  and rendered non-interactive. */}
              {view === "symbols" && (
                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 12,
                    fontSize: 10,
                    color: "var(--t1)",
                  }}
                >
                  <LegendChip label="Portfolio" color={PORTFOLIO_COLOR} thick />
                  {symbolsOrdered.map((sym, idx) => {
                    const display = sym.replace("-USDT", "");
                    return (
                      <LegendChip
                        key={sym}
                        label={display}
                        color={colorFor(sym, idx)}
                        dashed={stoppedAtBar.has(sym)}
                        hidden={hiddenSymbols.has(display)}
                        onClick={() => toggleSymbolHidden(display)}
                      />
                    );
                  })}
                </div>
              )}
              {/* Mode toggle */}
              <ModeToggle value={view} onChange={setView} />
            </div>
          </div>
          <div style={{ height: 320 }}>
            <Line
              data={{ labels, datasets: chartDatasets }}
              options={chartOptions}
            />
          </div>
        </div>

        {/* Matrix */}
        <div
          style={{
            background: "var(--bg1)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            padding: "12px 16px",
            minHeight: 0,
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 10,
            }}
          >
            <div
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                color: "var(--t3)",
                textTransform: "uppercase",
              }}
            >
              5-min ROI Matrix
              <span
                style={{
                  marginLeft: 10,
                  color: "var(--t2)",
                  fontWeight: 400,
                  letterSpacing: "0.06em",
                }}
              >
                · {matrixBars.length} of {bars.length} bars
              </span>
            </div>
          </div>
          <div
            ref={matrixRef}
            style={{
              overflow: "auto",
              maxHeight: 420,
              border: "1px solid var(--line)",
              borderRadius: 4,
            }}
          >
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontFamily: FONT_MONO,
              }}
            >
              <thead
                style={{
                  position: "sticky",
                  top: 0,
                  background: "var(--bg2)",
                  zIndex: 1,
                }}
              >
                <tr>
                  <th style={matrixThStyle}>Bar</th>
                  <th style={matrixThStyle}>Time</th>
                  {symbolsOrdered.map((sym) => (
                    <th key={sym} style={matrixThStyle}>
                      {sym.replace("-USDT", "")}
                    </th>
                  ))}
                  <th
                    style={{
                      ...matrixThStyle,
                      borderLeft: "2px solid var(--line2)",
                      color: PORTFOLIO_COLOR,
                    }}
                  >
                    Portfolio
                  </th>
                </tr>
              </thead>
              <tbody>
                {matrixBars.map((b) => (
                  <tr key={b.bar}>
                    <td style={{ ...matrixTdStyle, color: "var(--t2)" }}>{b.bar}</td>
                    <td style={{ ...matrixTdStyle, color: "var(--t2)" }}>
                      {fmtTime(b.ts)}
                    </td>
                    {symbolsOrdered.map((sym) => {
                      const v = b.sym_returns[sym];
                      const stopBar = stoppedAtBar.get(sym);
                      const isStopped =
                        stopBar !== undefined && b.bar >= stopBar;
                      const pct = v !== undefined ? v * 100 : null;
                      return (
                        <td
                          key={sym}
                          style={{
                            ...matrixTdStyle,
                            color:
                              pct === null
                                ? "var(--t3)"
                                : isStopped
                                ? "var(--t3)"
                                : pct >= 0
                                ? "var(--green)"
                                : "var(--red)",
                            fontStyle: isStopped ? "italic" : undefined,
                          }}
                        >
                          {pct === null ? "—" : fmtPct(pct, 2)}
                        </td>
                      );
                    })}
                    <td
                      style={{
                        ...matrixTdStyle,
                        borderLeft: "2px solid var(--line2)",
                        fontWeight: 700,
                        color: b.incr >= 0 ? "var(--green)" : "var(--red)",
                      }}
                    >
                      {fmtPct(b.incr * 100)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </>
  );
}

// ─── Legend chip ────────────────────────────────────────────────────────────

function ModeToggle({
  value,
  onChange,
}: {
  value: "portfolio" | "symbols";
  onChange: (next: "portfolio" | "symbols") => void;
}) {
  const opts: { key: "portfolio" | "symbols"; label: string }[] = [
    { key: "portfolio", label: "PORTFOLIO" },
    { key: "symbols",   label: "ALL SYMBOLS" },
  ];
  return (
    <div
      role="tablist"
      aria-label="Chart view mode"
      style={{
        display: "inline-flex",
        border: "1px solid var(--line)",
        borderRadius: 4,
        overflow: "hidden",
        fontFamily: FONT_MONO,
      }}
    >
      {opts.map((opt, i) => {
        const active = opt.key === value;
        return (
          <button
            key={opt.key}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => onChange(opt.key)}
            style={{
              padding: "4px 10px",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.08em",
              background: active ? "var(--bg4)" : "transparent",
              color: active ? "var(--t0)" : "var(--t2)",
              border: "none",
              borderLeft: i === 0 ? "none" : "1px solid var(--line)",
              cursor: "pointer",
              transition: "background 0.15s, color 0.15s",
            }}
            onMouseEnter={(e) => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
            onMouseLeave={(e) => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

function LegendChip({
  label,
  color,
  thick,
  dashed,
  hidden,
  onClick,
}: {
  label: string;
  color: string;
  thick?: boolean;
  dashed?: boolean;
  hidden?: boolean;
  onClick?: () => void;
}) {
  const isClickable = !!onClick;
  const content = (
    <>
      <span
        style={{
          display: "inline-block",
          width: 14,
          height: thick ? 4 : 2,
          background: dashed
            ? `repeating-linear-gradient(to right, ${color} 0 4px, transparent 4px 7px)`
            : color,
          borderRadius: 1,
        }}
      />
      <span
        style={{
          color: "var(--t1)",
          fontSize: 10,
          fontWeight: thick ? 700 : 400,
          textDecoration: hidden ? "line-through" : undefined,
        }}
      >
        {label}
      </span>
    </>
  );
  if (!isClickable) {
    return (
      <div style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
        {content}
      </div>
    );
  }
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        background: "transparent",
        border: "none",
        padding: 0,
        cursor: "pointer",
        opacity: hidden ? 0.4 : 1,
        transition: "opacity 0.15s",
      }}
    >
      {content}
    </button>
  );
}

// ─── Matrix cell styles ─────────────────────────────────────────────────────

const matrixThStyle: React.CSSProperties = {
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: "0.12em",
  color: "var(--t3)",
  textTransform: "uppercase",
  textAlign: "right",
  padding: "8px 10px",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
  background: "var(--bg2)",
};

const matrixTdStyle: React.CSSProperties = {
  fontSize: 11,
  color: "var(--t1)",
  padding: "6px 10px",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
  textAlign: "right",
  fontFamily: FONT_MONO,
};

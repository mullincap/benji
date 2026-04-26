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
  // Risk-floor params for the chart's reference-line overlays. Decimal
  // fractions (e.g. -0.075 = -7.5% hard portfolio floor). Both negative.
  // port_tsl_pct is the offset from peak — live trigger is peak+port_tsl_pct.
  port_sl_pct?: number | null;
  port_tsl_pct?: number | null;
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

function exitBadge(reason: string | null, status: string, isStale: boolean = false) {
  // Treat the session as LIVE whenever status is active OR the recorded
  // exit_reason is one of the transient/error states (subprocess_died,
  // stale_close_failed, errored). Those almost always mean the trader
  // got interrupted but a respawn is writing bars, and surfacing a
  // permanent-looking exit label is misleading.
  const transientErrors = new Set(["subprocess_died", "stale_close_failed", "errored"]);
  const isLiveBySession = status === "active" || (reason && transientErrors.has(reason));
  if (isLiveBySession && isStale) {
    // Status says active but bars haven't flowed in > STALE_THRESHOLD_SEC.
    // Supervisor will respawn shortly; surface this honestly so the operator
    // doesn't think they're seeing live data.
    return (
      <span
        style={{
          display: "inline-block",
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.06em",
          padding: "2px 6px",
          borderRadius: 3,
          background: "var(--amber-dim)",
          color: "var(--amber)",
          fontFamily: FONT_MONO,
        }}
        title="No new bars in the last 7 minutes; auto-recovery is queued."
      >
        STALE
      </span>
    );
  }
  if (isLiveBySession) {
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

  // Chart view mode is local state, defaulting to "portfolio" on every
  // page load. Earlier this was URL-persisted via ?view=symbols, but
  // bookmarking/sharing a URL while in symbols mode meant fresh visitors
  // landed on the busier all-symbols chart instead of the cleaner
  // portfolio-only view. Forcing local state ensures the default never
  // drifts from "portfolio" regardless of how the page is reached.
  const [view, setViewState] = useState<"portfolio" | "symbols">("portfolio");
  const [hiddenSymbols, setHiddenSymbols] = useState<Set<string>>(new Set());
  // When true, the pace trendline extends across the historical region
  // too (bar 0 → end of session) instead of only the projection segment.
  // Default off so the trendline reads as a forecast; on, it doubles as
  // a "best-fit pace" overlay across the full path so the user can spot
  // bars sitting above/below the line at a glance.
  const [trendlineExtended, setTrendlineExtended] = useState(false);
  // Tracks the (date, allocationId) we've already seeded the hidden-set
  // for, so polling refreshes don't keep clobbering the user's legend
  // clicks. Reset when the user navigates to a different portfolio.
  const seededHiddenRef = useRef<string | null>(null);
  // Overlay visibility toggles. All start on so a fresh load is the most
  // informative view; user can dim any individual layer they don't need.
  const [overlays, setOverlays] = useState({
    portSl: true,
    portTsl: true,
    now: true,
    fillWindow: true,
  });

  const setView = useCallback((next: "portfolio" | "symbols") => {
    setViewState(next);
  }, []);

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

  // Auto-hide stopped symbols on first load per (date, allocation). Stopped
  // symbols are still rendered as dashed segments by default — but on a
  // basket of 8+ symbols where 3 are flat-lined at their clamp values, the
  // dashed lines stack up and crowd the live ones. Seeding hiddenSymbols
  // with the stopped set defaults them to off; clicking the legend chip
  // toggles them back on, same UX as any other symbol. Polling refreshes
  // skip the seed (seededHiddenRef guard) so the user's choices persist.
  useEffect(() => {
    if (!data) return;
    const key = `${date}|${allocationId ?? "master"}`;
    if (seededHiddenRef.current === key) return;
    seededHiddenRef.current = key;
    const stoppedDisplayLabels = new Set<string>();
    for (const sym of stoppedAtBar.keys()) {
      stoppedDisplayLabels.add(sym.replace("-USDT", ""));
    }
    setHiddenSymbols(stoppedDisplayLabels);
  }, [data, date, allocationId, stoppedAtBar]);

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
  const lastBar = bars[bars.length - 1];
  // Liveness = "bars are actually flowing right now". Bar interval is 5 min,
  // so anything older than ~7 min means the trader stopped writing — even
  // if portfolio_sessions.status is still 'active', we don't claim LIVE.
  // The supervisor (STALE_THRESHOLD_MIN=7) treats the same age as stale and
  // schedules a respawn; the UI signal mirrors that exact decision so the
  // operator sees STALE the moment auto-recovery is queued.
  const STALE_THRESHOLD_SEC = 7 * 60;
  const lastBarAgeSec = (() => {
    if (!lastBar?.ts) return null;
    // ts format: "YYYY-MM-DD HH:MM:SS" (UTC, no timezone marker).
    const parsed = Date.parse(lastBar.ts.replace(" ", "T") + "Z");
    if (Number.isNaN(parsed)) return null;
    return Math.max(0, (Date.now() - parsed) / 1000);
  })();
  const isFresh = lastBarAgeSec !== null && lastBarAgeSec <= STALE_THRESHOLD_SEC;
  const live = meta.status === "active" && isFresh;
  const isStale =
    meta.status === "active" &&
    lastBarAgeSec !== null &&
    lastBarAgeSec > STALE_THRESHOLD_SEC;
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
  const endTime = live
    ? "LIVE"
    : isStale
      ? "STALE"
      : fmtTime(meta.exit_time_utc);
  const symStopsCount = stoppedAtBar.size;
  const enteredCount = meta.entered.length;

  // Chart spans the full deployment window: from session_start_hour:00 UTC
  // through 23:55 UTC at 5-min cadence (matches the trader's bar interval).
  // Building all 216-ish slots up front means the chart reserves the right-
  // hand space for projections/forecasts after the last data point — instead
  // of the x-axis ending at "now" and shifting as bars arrive. Bars that
  // haven't fired yet sit as null entries; Chart.js renders them as gaps.
  const BAR_INTERVAL_MIN = 5;
  const SESSION_END_HOUR_UTC_EXCLUSIVE = 24;  // last bar timestamp 23:55
  const sessionStartHour = meta.session_start_hour ?? 6;
  const totalSlots =
    ((SESSION_END_HOUR_UTC_EXCLUSIVE - sessionStartHour) * 60) / BAR_INTERVAL_MIN;
  const labels: string[] = Array.from({ length: totalSlots }, (_, i) => {
    const totalMin = i * BAR_INTERVAL_MIN;
    const h = sessionStartHour + Math.floor(totalMin / 60);
    const m = totalMin % 60;
    return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
  });
  // Index bars into slots by parsing HH:MM out of bar.ts (UTC). Bars before
  // session_start_hour or after 23:55 fall outside the chart and are dropped.
  const barAtSlot: (PortfolioBar | undefined)[] = new Array(totalSlots);
  for (const b of bars) {
    const m = /(\d{2}):(\d{2}):/.exec(b.ts);
    if (!m) continue;
    const totalMin = (Number(m[1]) - sessionStartHour) * 60 + Number(m[2]);
    if (totalMin < 0) continue;
    const idx = Math.round(totalMin / BAR_INTERVAL_MIN);
    if (idx >= 0 && idx < totalSlots) barAtSlot[idx] = b;
  }
  let lastFilledIdx = -1;
  for (let i = totalSlots - 1; i >= 0; i--) {
    if (barAtSlot[i] !== undefined) { lastFilledIdx = i; break; }
  }

  const symbolDatasets = symbolsOrdered.map((sym, idx) => {
    const stoppedBar = stoppedAtBar.get(sym);
    const displayLabel = sym.replace("-USDT", "");
    const data = barAtSlot.map((b) => {
      if (!b) return null;
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
      // Bridge gaps caused by missing bars (e.g. trader-down windows that
      // the supervisor hadn't yet recovered). Each gap renders as a single
      // straight segment between the surrounding bars, instead of a hard
      // break — visually cleaner and the matrix below the chart still shows
      // exactly which bars are missing for anyone who needs the truth.
      spanGaps: true,
      // Dash the segment after the stop bar.
      segment: stoppedBar !== undefined
        ? {
            borderDash: (ctx: { p1DataIndex: number }) => {
              const barNum = barAtSlot[ctx.p1DataIndex]?.bar ?? 0;
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
    data: barAtSlot.map((b) => (b ? b.incr * 100 : null)),
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
    // Same gap-bridging rationale as the symbol datasets — missing bars
    // (trader-down windows, etc.) are stitched across visually.
    spanGaps: true,
    _isSymbol: false,
  };

  // Forward projection — linear-regression extrapolation from the latest
  // bar to the deployment window close. Mirrors the Manager Overview's
  // Intraday Equity projection (overview/page.tsx:816). Slope is OLS on
  // all real points once we have ≥1h of history (12 bars at 5-min cadence);
  // before that the line is flat at the latest value (insufficient signal
  // for a directional forecast). The projection only covers slots AFTER
  // lastFilledIdx so the dashed line lives entirely in the empty right-
  // hand space — it doesn't overlay the actual path.
  const realPoints: { x: number; y: number }[] = [];
  for (let i = 0; i <= lastFilledIdx; i++) {
    const b = barAtSlot[i];
    if (b !== undefined) realPoints.push({ x: i, y: b.incr * 100 });
  }
  const trendData: (number | null)[] = new Array(totalSlots).fill(null);
  if (realPoints.length >= 2) {
    const lastReal = realPoints[realPoints.length - 1];
    const REGRESSION_MIN_POINTS = 12;  // ~1h of 5-min bars
    let slope = 0;
    if (realPoints.length >= REGRESSION_MIN_POINTS) {
      const n = realPoints.length;
      const sumX = realPoints.reduce((s, p) => s + p.x, 0);
      const sumY = realPoints.reduce((s, p) => s + p.y, 0);
      const sumXY = realPoints.reduce((s, p) => s + p.x * p.y, 0);
      const sumX2 = realPoints.reduce((s, p) => s + p.x * p.x, 0);
      const denom = n * sumX2 - sumX * sumX;
      slope = denom === 0 ? 0 : (n * sumXY - sumX * sumY) / denom;
    }
    // Bidirectional toggle: forward-only (default) vs full-span. Same
    // slope + anchor either way; only the rendering range differs.
    const startX = trendlineExtended ? 0 : lastReal.x;
    for (let i = startX; i < totalSlots; i++) {
      trendData[i] = lastReal.y + slope * (i - lastReal.x);
    }
  }
  const trendlineDataset = {
    label: "Pace",
    data: trendData,
    borderColor: "rgba(160, 157, 150, 0.55)",
    backgroundColor: "rgba(160, 157, 150, 0.06)",
    borderWidth: 1.4,
    borderDash: [6, 4],
    pointRadius: 0,
    pointHoverRadius: 0,
    // Subtle wedge under the projection — same depth treatment as the
    // Manager Overview's intraday equity forecast. Anchors the projection
    // visually without competing with the live portfolio gradient. Drop
    // the fill in extended mode so we don't paint a second wedge on top
    // of the live path's own gradient.
    fill: (trendlineExtended ? false : "origin") as "origin" | false,
    tension: 0.15,
    spanGaps: false,
    _isSymbol: false,
  };

  // ── Risk-threshold reference lines ──────────────────────────────────────
  // Faint horizontal/dynamic markers showing where the trader's exit
  // triggers fire. Operator can read distance-to-trigger off the chart
  // without cross-checking the KPI cards.
  //   port_sl: hard floor — static horizontal line at port_sl_pct.
  //   port_tsl: trailing stop — dynamic, tracks peak[i] + port_tsl_pct
  //     per bar so the user sees the live trigger floor rising with peak.
  //     Where bars are missing the value falls back to the most recent peak.
  const portSlPct = meta.port_sl_pct ?? null;
  const portTslPct = meta.port_tsl_pct ?? null;
  const portSlData: (number | null)[] = portSlPct !== null
    ? new Array(totalSlots).fill(portSlPct * 100)
    : new Array(totalSlots).fill(null);
  let portTslData: (number | null)[] = new Array(totalSlots).fill(null);
  if (portTslPct !== null) {
    let runningPeak = 0;
    for (let i = 0; i < totalSlots; i++) {
      const b = barAtSlot[i];
      if (b !== undefined) runningPeak = b.peak;
      // Show the trigger floor from session start onwards (peak starts at 0).
      // After the last real bar we extend the latest known peak forward —
      // this is the level the trader will trip if the current peak holds.
      if (i <= lastFilledIdx || lastFilledIdx === -1) {
        portTslData[i] = (runningPeak + portTslPct) * 100;
      } else {
        portTslData[i] = (runningPeak + portTslPct) * 100;
      }
    }
  }
  const portSlDataset = {
    label: "Port SL",
    data: portSlData,
    borderColor: "rgba(255, 77, 77, 0.4)",
    backgroundColor: "transparent",
    borderWidth: 1,
    borderDash: [2, 4],
    pointRadius: 0,
    pointHoverRadius: 0,
    fill: false as const,
    tension: 0,
    spanGaps: true,
    hidden: !overlays.portSl,
    _isSymbol: false,
  };
  const portTslDataset = {
    label: "Port TSL",
    data: portTslData,
    borderColor: "rgba(240, 165, 0, 0.4)",
    backgroundColor: "transparent",
    borderWidth: 1,
    borderDash: [2, 4],
    pointRadius: 0,
    pointHoverRadius: 0,
    fill: false as const,
    tension: 0,
    spanGaps: true,
    stepped: "before" as const,
    hidden: !overlays.portTsl,
    _isSymbol: false,
  };

  // ── Drawdown wedge ──────────────────────────────────────────────────────
  // Subtle red overlay between the running peak and the current portfolio
  // path — visualizes how deep below peak the path has drifted. Rendered
  // as the running-peak series with a fill back to the portfolio dataset
  // (filling the underwater region only when path < peak).
  const peakData: (number | null)[] = barAtSlot.map((b) =>
    b !== undefined ? b.peak * 100 : null,
  );
  const drawdownDataset = {
    label: "Peak",
    data: peakData,
    borderColor: "rgba(255, 77, 77, 0.18)",
    backgroundColor: "rgba(255, 77, 77, 0.07)",
    borderWidth: 1,
    pointRadius: 0,
    pointHoverRadius: 0,
    // Fill back to the portfolio dataset (index 0). When peak > portfolio
    // (always true for stopped/trailing-down sessions), the area between
    // peak and path fills red — instant read of drawdown depth.
    fill: { target: "0", below: "rgba(255, 77, 77, 0.10)" } as const,
    tension: 0.15,
    spanGaps: true,
    _isSymbol: false,
  };

  // ── Current-value reference line ────────────────────────────────────────
  // Flat horizontal at the latest portfolio incr; lets the user eyeball
  // the gap to PORT SL / PORT TSL without translating between two y-values.
  // Same dashed-line style as the threshold lines but in the warm primary
  // text color so it reads as "where you are right now" rather than a
  // trigger boundary.
  const currentIncrPct = lastFilledIdx >= 0
    ? (barAtSlot[lastFilledIdx]!.incr) * 100
    : null;
  const currentDataset = {
    label: "Current",
    data: currentIncrPct !== null
      ? new Array(totalSlots).fill(currentIncrPct)
      : new Array(totalSlots).fill(null),
    borderColor: "rgba(240, 237, 230, 0.6)",
    backgroundColor: "transparent",
    borderWidth: 1,
    borderDash: [2, 4],
    pointRadius: 0,
    pointHoverRadius: 0,
    fill: false as const,
    tension: 0,
    spanGaps: true,
    hidden: !overlays.now,
    _isSymbol: false,
  };

  // Datasets shown to Chart.js depend on the mode. Portfolio-only mode strips
  // symbol lines entirely (cleaner view); All Symbols mode stacks them under
  // the portfolio area. The `hidden` flag on each symbol dataset is driven by
  // React state so toggle visibility survives mode switches. The pace
  // trendline appears in both modes (faint + dashed; reads as scaffolding
  // rather than a competing line). The drawdown wedge layers between
  // portfolio and trendline so it reads behind both lines.
  const chartDatasets = view === "symbols"
    ? [portfolioDataset, drawdownDataset, trendlineDataset, portSlDataset, portTslDataset, currentDataset, ...symbolDatasets]
    : [portfolioDataset, drawdownDataset, trendlineDataset, portSlDataset, portTslDataset, currentDataset];

  // Right-edge value labels for the threshold + current reference lines.
  // Inline Chart.js plugin avoids a chartjs-plugin-annotation dependency;
  // draws on canvas after datasets so the labels float over the right
  // margin where there's empty space (post-last-bar projection region).
  const latestPortTslPct = lastFilledIdx >= 0 && portTslPct !== null
    ? (barAtSlot[lastFilledIdx]!.peak + portTslPct) * 100
    : (portTslPct !== null ? portTslPct * 100 : null);
  const refLineLabels: { y: number; text: string; color: string }[] = [];
  if (portSlPct !== null && overlays.portSl) {
    refLineLabels.push({
      y: portSlPct * 100,
      text: `PORT SL: ${(portSlPct * 100).toFixed(1)}%`,
      color: "rgba(255, 77, 77, 0.95)",
    });
  }
  if (latestPortTslPct !== null && overlays.portTsl) {
    refLineLabels.push({
      y: latestPortTslPct,
      text: `PORT TSL: ${latestPortTslPct.toFixed(1)}%`,
      color: "rgba(240, 165, 0, 0.95)",
    });
  }
  if (currentIncrPct !== null && overlays.now) {
    const sign = currentIncrPct >= 0 ? "+" : "";
    refLineLabels.push({
      y: currentIncrPct,
      text: `NOW: ${sign}${currentIncrPct.toFixed(2)}%`,
      color: "rgba(240, 237, 230, 0.95)",
    });
  }
  // Slot index where the early-fill window closes. After this bar, the
  // EARLY_FILL trigger can no longer fire (trader_blofin.py:2614 —
  // `while utcnow() <= session_open + early_fill_x`). Drawing a vertical
  // dashed marker here gives the operator a visual deadline: anything
  // past this x is "no early fill possible" territory.
  const fillWindowSlot = meta.early_fill_x !== null && meta.early_fill_x !== undefined
    ? meta.early_fill_x / BAR_INTERVAL_MIN
    : null;
  const refLinePlugin = {
    id: "refLineLabels",
    afterDatasetsDraw(chart: ChartJS) {
      const { ctx, chartArea, scales } = chart;
      const yScale = scales.y;
      const xScale = scales.x;
      if (!yScale || !xScale) return;
      ctx.save();
      // ── Vertical marker: fill-window close ──────────────────────────
      if (overlays.fillWindow && fillWindowSlot !== null && fillWindowSlot >= 0 && fillWindowSlot < totalSlots) {
        const xPx = xScale.getPixelForValue(fillWindowSlot);
        if (xPx >= chartArea.left && xPx <= chartArea.right) {
          ctx.strokeStyle = "rgba(0, 200, 150, 0.45)";
          ctx.lineWidth = 1;
          ctx.setLineDash([3, 3]);
          ctx.beginPath();
          ctx.moveTo(xPx, chartArea.top);
          ctx.lineTo(xPx, chartArea.bottom);
          ctx.stroke();
          ctx.setLineDash([]);
          // Top-anchored label so it doesn't collide with the right-edge
          // y-value pills (which sit centered at their respective rows).
          ctx.font = 'bold 9px "Space Mono", monospace';
          ctx.textAlign = "left";
          ctx.textBaseline = "top";
          const text = "FILL WINDOW CLOSE";
          const metrics = ctx.measureText(text);
          const padX = 5;
          const padY = 2;
          const w = metrics.width + padX * 2;
          const h = 12 + padY * 2;
          ctx.fillStyle = "rgba(14, 14, 16, 0.85)";
          ctx.fillRect(xPx + 4, chartArea.top + 4, w, h);
          ctx.fillStyle = "rgba(0, 200, 150, 0.95)";
          ctx.fillText(text, xPx + 4 + padX, chartArea.top + 4 + padY);
        }
      }
      // ── Right-edge value pills for horizontal lines ─────────────────
      ctx.font = 'bold 9px "Space Mono", monospace';
      ctx.textAlign = "left";
      ctx.textBaseline = "middle";
      // Sort by y so we can offset overlapping labels vertically. With a
      // 12-px label height + 2-px padding, anything within 14 px is a
      // visual collision; nudge later labels down by half-height.
      const placed: { y: number; text: string; color: string; px: number }[] = [];
      const sorted = [...refLineLabels].sort((a, b) => b.y - a.y);
      for (const lbl of sorted) {
        let px = yScale.getPixelForValue(lbl.y);
        // Avoid rendering off-canvas if the value falls outside the y-range.
        px = Math.max(chartArea.top + 8, Math.min(chartArea.bottom - 8, px));
        // Push down if overlapping an already-placed label.
        for (const p of placed) {
          if (Math.abs(px - p.px) < 14) {
            px = p.px + 14;
          }
        }
        placed.push({ ...lbl, px });
      }
      // Render. Right-aligned just inside the chart's right edge.
      const padX = 6;
      const padY = 2;
      const labelHeight = 12;
      for (const { px, text, color } of placed) {
        const metrics = ctx.measureText(text);
        const w = metrics.width + padX * 2;
        const x = chartArea.right - w - 4;
        ctx.fillStyle = "rgba(14, 14, 16, 0.85)";
        ctx.fillRect(x, px - labelHeight / 2 - padY, w, labelHeight + padY * 2);
        ctx.fillStyle = color;
        ctx.fillText(text, x + padX, px);
      }
      ctx.restore();
    },
  };

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
        // Decorative overlays (pace trendline, threshold lines, peak/drawdown
        // wedge, current-value reference) are visual scaffolding; suppress
        // them from the tooltip so it stays focused on the actual symbol +
        // portfolio rows.
        filter: (item) =>
          item.dataset.label !== "Pace" &&
          item.dataset.label !== "Port SL" &&
          item.dataset.label !== "Port TSL" &&
          item.dataset.label !== "Peak" &&
          item.dataset.label !== "Current",
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
          <KpiCard label="Exit" value={exitBadge(meta.exit_reason, meta.status, isStale)} />
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
              {/* Trendline extent toggle — when off, the dashed pace line
                  only spans the empty future region; when on, it extends
                  backwards across the historical path too so the user can
                  spot bars sitting above/below the line at a glance. */}
              <button
                type="button"
                onClick={() => setTrendlineExtended((v) => !v)}
                style={{
                  fontFamily: FONT_MONO,
                  fontSize: 9,
                  fontWeight: 700,
                  letterSpacing: "0.12em",
                  textTransform: "uppercase",
                  padding: "4px 10px",
                  borderRadius: 3,
                  border: `1px solid ${trendlineExtended ? "var(--line2)" : "var(--line)"}`,
                  background: trendlineExtended ? "var(--bg3)" : "transparent",
                  color: trendlineExtended ? "var(--t0)" : "var(--t2)",
                  cursor: "pointer",
                }}
                title={
                  trendlineExtended
                    ? "Trendline spans the full session. Click to limit to the projection segment only."
                    : "Trendline currently covers only the projection segment. Click to extend it backwards across the historical path too."
                }
              >
                {trendlineExtended ? "Trendline · Full" : "Trendline · Projection"}
              </button>
              {/* Overlay toggles — color-coded chip group lets the operator
                  dim individual reference layers without losing the others.
                  Active = filled background; inactive = ghosted outline. */}
              <div style={{ display: "flex", gap: 4 }}>
                <OverlayChip
                  label="SL"
                  active={overlays.portSl}
                  color="var(--red)"
                  title={overlays.portSl ? "Hide PORT SL line" : "Show PORT SL line"}
                  onClick={() => setOverlays((o) => ({ ...o, portSl: !o.portSl }))}
                />
                <OverlayChip
                  label="TSL"
                  active={overlays.portTsl}
                  color="var(--amber)"
                  title={overlays.portTsl ? "Hide PORT TSL line" : "Show PORT TSL line"}
                  onClick={() => setOverlays((o) => ({ ...o, portTsl: !o.portTsl }))}
                />
                <OverlayChip
                  label="NOW"
                  active={overlays.now}
                  color="var(--t0)"
                  title={overlays.now ? "Hide NOW line" : "Show NOW line"}
                  onClick={() => setOverlays((o) => ({ ...o, now: !o.now }))}
                />
                <OverlayChip
                  label="WIN"
                  active={overlays.fillWindow}
                  color="var(--green)"
                  title={overlays.fillWindow ? "Hide fill-window marker" : "Show fill-window marker"}
                  onClick={() => setOverlays((o) => ({ ...o, fillWindow: !o.fillWindow }))}
                />
              </div>
              {/* Mode toggle */}
              <ModeToggle value={view} onChange={setView} />
            </div>
          </div>
          <div style={{ height: 320 }}>
            <Line
              data={{ labels, datasets: chartDatasets }}
              options={chartOptions}
              plugins={[refLinePlugin]}
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

        {/* Subtle deep-link to the session log viewer for this allocation+date */}
        {allocationId && (
          <div style={{
            display: "flex",
            justifyContent: "flex-end",
            marginTop: 16,
            paddingTop: 12,
            borderTop: "1px solid var(--line)",
          }}>
            <a
              href={`/manager/execution?date=${date}&allocation_id=${allocationId}`}
              style={{
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
                color: "var(--t3)",
                textDecoration: "none",
                fontFamily: FONT_MONO,
              }}
              onMouseEnter={(e) => { e.currentTarget.style.color = "var(--t1)"; }}
              onMouseLeave={(e) => { e.currentTarget.style.color = "var(--t3)"; }}
            >
              View session logs →
            </a>
          </div>
        )}
      </div>
    </>
  );
}

// ─── Legend chip ────────────────────────────────────────────────────────────

// Compact color-coded toggle chip for overlay layers (SL / TSL / NOW /
// WIN). Active state lights the dot in the layer's color and the label
// in primary text; inactive ghosts both. Same monospace + tight tracking
// as the rest of the chart toolbar so the chips read as part of the
// same control row, not a competing UI.
function OverlayChip({
  label,
  active,
  color,
  title,
  onClick,
}: {
  label: string;
  active: boolean;
  color: string;
  title?: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={active}
      title={title}
      onClick={onClick}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 5,
        padding: "3px 8px",
        fontFamily: FONT_MONO,
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.10em",
        textTransform: "uppercase",
        borderRadius: 3,
        border: `1px solid ${active ? "var(--line2)" : "var(--line)"}`,
        background: active ? "var(--bg3)" : "transparent",
        color: active ? "var(--t0)" : "var(--t3)",
        cursor: "pointer",
        transition: "background 0.15s, color 0.15s, border-color 0.15s",
      }}
      onMouseEnter={(e) => {
        if (!active) e.currentTarget.style.color = "var(--t1)";
      }}
      onMouseLeave={(e) => {
        if (!active) e.currentTarget.style.color = "var(--t3)";
      }}
    >
      <span
        style={{
          width: 6,
          height: 6,
          borderRadius: "50%",
          background: active ? color : "transparent",
          border: `1px solid ${color}`,
          opacity: active ? 1 : 0.4,
          flexShrink: 0,
        }}
      />
      {label}
    </button>
  );
}

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

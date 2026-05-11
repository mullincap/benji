"use client";
/**
 * usePlayback — animates the Summary-tab equity curve "playing back" the
 * simulation day-by-day so the operator can feel the trade days unfold.
 *
 * Pace: `msPerDay` (default 50ms/day) capped at `maxDurationMs` (default
 * 10s) so a 100-day strategy lasts 5s and a 5000-day strategy fast-
 * forwards instead of dragging. Frame loop uses requestAnimationFrame —
 * the caller throttles by reading `progress` which advances monotonically.
 *
 * Returned `progress` is null when stopped, otherwise a number in [0, 1]
 * mapped to "how far through the totalDays we are." Sliced indices are
 * `Math.max(1, Math.ceil(progress * totalDays))` so the curve always
 * shows at least one point and lands on the full series at progress=1.
 *
 * Live-metric helpers compute Sharpe / Max DD / Total Return / CAGR /
 * Vol / Profit Factor / Avg Win-Loss / Underwater streak / Avg 1M
 * return from the sliced equity series. They mirror the static formulas
 * already in ResultsView; kept here so the playback path doesn't depend
 * on private symbols inside that 17k-line monolith.
 */
import { useCallback, useEffect, useRef, useState } from "react";

export type Point = { x: number | string | Date; y: number } | number;

export type PlaybackState = {
  /** null = stopped; 0..1 = playing (or just finished at 1) */
  progress: number | null;
  /** true while raf loop is active */
  isPlaying: boolean;
  /** true once the playback has run end-to-end at least once in this session */
  hasPlayed: boolean;
  play: () => void;
  stop: () => void;
};

const DEFAULT_MS_PER_DAY = 50;
const DEFAULT_MAX_DURATION_MS = 10_000;

export function usePlayback(
  totalDays: number,
  msPerDay: number = DEFAULT_MS_PER_DAY,
  maxDurationMs: number = DEFAULT_MAX_DURATION_MS,
): PlaybackState {
  const [progress, setProgress] = useState<number | null>(null);
  const [hasPlayed, setHasPlayed] = useState(false);
  const rafRef = useRef<number | null>(null);
  const startRef = useRef<number | null>(null);

  // Total duration clamps to maxDurationMs so very long backtests don't
  // drag — a 1000-day strategy at 50ms/day would be 50s otherwise.
  const durationMs = Math.min(maxDurationMs, Math.max(500, totalDays * msPerDay));

  const stop = useCallback(() => {
    if (rafRef.current !== null) {
      cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    }
    startRef.current = null;
    setProgress(null);
  }, []);

  const tick = useCallback((now: number) => {
    if (startRef.current === null) startRef.current = now;
    const elapsed = now - startRef.current;
    const p = Math.min(1, elapsed / durationMs);
    setProgress(p);
    if (p < 1) {
      rafRef.current = requestAnimationFrame(tick);
    } else {
      rafRef.current = null;
      startRef.current = null;
      setHasPlayed(true);
      // Hold at 1 for a moment so the eye lands on the final state,
      // then clear so static metrics resume. The hold also gives the
      // user a moment to read final values before the fade returns.
      window.setTimeout(() => setProgress(null), 800);
    }
  }, [durationMs]);

  const play = useCallback(() => {
    if (totalDays < 2) return;
    stop();
    startRef.current = null;
    rafRef.current = requestAnimationFrame(tick);
  }, [totalDays, stop, tick]);

  // Clean up if the component unmounts mid-playback.
  useEffect(() => () => {
    if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
  }, []);

  return {
    progress,
    isPlaying: progress !== null && progress < 1,
    hasPlayed,
    play,
    stop,
  };
}

/** Convert progress (0..1) to the sliced length for an array of `total`
 *  elements. Always returns at least 2 (so derived metrics have at least
 *  one daily return) and at most `total`. */
export function progressToSliceLen(progress: number | null, total: number): number {
  if (progress === null || total < 2) return total;
  return Math.max(2, Math.min(total, Math.ceil(progress * total)));
}

/** Slice a Point[] (or null/undef) to the playback length. Returns null
 *  when input is null so consumers can pass through directly. */
export function slicePoints<T extends Point>(
  src: T[] | null | undefined,
  sliceLen: number,
): T[] | null | undefined {
  if (!src) return src;
  if (sliceLen >= src.length) return src;
  return src.slice(0, sliceLen);
}

// ───────────────────────────────────────────────────────────────────────
// Live metric recompute helpers
//
// All take the sliced equity series (post-multiplied by starting capital
// — i.e. `equityCurveDollars`-style values) and return the same shape
// the static ResultsView code derives from `selectedRow.*`. Kept dumb +
// pure so the hook file has zero React deps beyond useState/useRef.
// ───────────────────────────────────────────────────────────────────────

function yOf(p: Point): number {
  return typeof p === "number" ? p : p.y;
}

/** Extract daily-return percentages from an equity series. */
export function dailyReturnPcts(equity: Point[] | null | undefined): number[] {
  if (!equity || equity.length < 2) return [];
  const out: number[] = [];
  let prev = yOf(equity[0]);
  for (let i = 1; i < equity.length; i += 1) {
    const cur = yOf(equity[i]);
    if (Number.isFinite(prev) && prev !== 0 && Number.isFinite(cur)) {
      out.push(((cur / prev) - 1) * 100);
    }
    prev = cur;
  }
  return out;
}

/** Sharpe ratio (annualized, daily-bar input, 252 trading days). */
export function liveSharpe(equity: Point[] | null | undefined): number | null {
  const rets = dailyReturnPcts(equity);
  if (rets.length < 2) return null;
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
  const variance = rets.reduce((a, b) => a + ((b - mean) ** 2), 0) / (rets.length - 1);
  const sd = Math.sqrt(Math.max(0, variance));
  if (sd === 0) return null;
  return (mean / sd) * Math.sqrt(252);
}

/** Annualized volatility % (daily stdev). Matches dailyVolatilityPct in
 *  ResultsView (which is daily stdev, not annualized — kept consistent
 *  so the stats-bar reading doesn't jump when playback ends). */
export function liveDailyVolPct(equity: Point[] | null | undefined): number | null {
  const rets = dailyReturnPcts(equity);
  if (rets.length < 2) return null;
  const mean = rets.reduce((a, b) => a + b, 0) / rets.length;
  const variance = rets.reduce((a, b) => a + ((b - mean) ** 2), 0) / (rets.length - 1);
  return Math.sqrt(Math.max(0, variance));
}

/** Max drawdown % (negative number, e.g. -26.36 means -26.36%). */
export function liveMaxDDPct(equity: Point[] | null | undefined): number | null {
  if (!equity || equity.length < 2) return null;
  let peak = -Infinity;
  let worst = 0;
  for (const p of equity) {
    const v = yOf(p);
    if (!Number.isFinite(v)) continue;
    if (v > peak) peak = v;
    if (peak > 0) {
      const dd = ((v / peak) - 1) * 100;
      if (dd < worst) worst = dd;
    }
  }
  return worst;
}

/** Simple total-return % from start to end. */
export function liveTotalReturnPct(equity: Point[] | null | undefined): number | null {
  if (!equity || equity.length < 2) return null;
  const first = yOf(equity[0]);
  const last = yOf(equity[equity.length - 1]);
  if (!Number.isFinite(first) || !Number.isFinite(last) || first === 0) return null;
  return ((last / first) - 1) * 100;
}

/** CAGR % computed from elapsed calendar years between first and last
 *  dated points. Falls back to assuming 252-day year when points are
 *  plain numbers. */
export function liveCagrPct(equity: Point[] | null | undefined): number | null {
  if (!equity || equity.length < 2) return null;
  const first = yOf(equity[0]);
  const last = yOf(equity[equity.length - 1]);
  if (!Number.isFinite(first) || !Number.isFinite(last) || first <= 0 || last <= 0) return null;

  let years: number;
  const a = equity[0];
  const b = equity[equity.length - 1];
  const dateA = typeof a === "number" ? null : parseDateMaybe(a.x);
  const dateB = typeof b === "number" ? null : parseDateMaybe(b.x);
  if (dateA && dateB) {
    years = (dateB.getTime() - dateA.getTime()) / (365.25 * 24 * 3600 * 1000);
  } else {
    years = (equity.length - 1) / 252;
  }
  if (years <= 0) return null;
  return (Math.pow(last / first, 1 / years) - 1) * 100;
}

function parseDateMaybe(x: number | string | Date): Date | null {
  if (x instanceof Date) return Number.isFinite(x.getTime()) ? x : null;
  if (typeof x === "number") {
    const d = new Date(x);
    return Number.isFinite(d.getTime()) ? d : null;
  }
  const d = new Date(x);
  return Number.isFinite(d.getTime()) ? d : null;
}

/** Profit factor = sum(positive daily returns) / |sum(negative daily returns)|. */
export function liveProfitFactor(equity: Point[] | null | undefined): number | null {
  const rets = dailyReturnPcts(equity);
  if (rets.length === 0) return null;
  let pos = 0;
  let neg = 0;
  for (const r of rets) {
    if (r > 0) pos += r;
    else if (r < 0) neg += r;
  }
  if (neg === 0) return pos > 0 ? Infinity : null;
  return pos / Math.abs(neg);
}

/** Avg win / avg loss ratio (mean of positive daily returns over |mean of
 *  negative|). Returns null when one side is empty. */
export function liveAvgWinLoss(equity: Point[] | null | undefined): number | null {
  const rets = dailyReturnPcts(equity);
  if (rets.length === 0) return null;
  const wins = rets.filter((r) => r > 0);
  const losses = rets.filter((r) => r < 0);
  if (wins.length === 0 || losses.length === 0) return null;
  const meanWin = wins.reduce((a, b) => a + b, 0) / wins.length;
  const meanLoss = losses.reduce((a, b) => a + b, 0) / losses.length;
  if (meanLoss === 0) return null;
  return meanWin / Math.abs(meanLoss);
}

/** Longest underwater streak (consecutive days below the running peak). */
export function liveLongestUnderwaterStreak(equity: Point[] | null | undefined): number {
  if (!equity || equity.length < 2) return 0;
  let peak = -Infinity;
  let cur = 0;
  let best = 0;
  for (const p of equity) {
    const v = yOf(p);
    if (!Number.isFinite(v)) continue;
    if (v >= peak) { peak = v; cur = 0; }
    else { cur += 1; if (cur > best) best = cur; }
  }
  return best;
}

/** Average 1-month (~21 trading day) rolling return %. */
export function liveAvg1MReturnPct(equity: Point[] | null | undefined): number | null {
  if (!equity || equity.length < 22) return null;
  const window = 21;
  const out: number[] = [];
  for (let i = window; i < equity.length; i += 1) {
    const prev = yOf(equity[i - window]);
    const cur = yOf(equity[i]);
    if (Number.isFinite(prev) && prev !== 0 && Number.isFinite(cur)) {
      out.push(((cur / prev) - 1) * 100);
    }
  }
  if (out.length === 0) return null;
  return out.reduce((a, b) => a + b, 0) / out.length;
}

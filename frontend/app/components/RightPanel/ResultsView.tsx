'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceDot,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import MetricCard from '../ui/MetricCard';
import FilterTable from '../ui/FilterTable';

interface ResultsViewProps {
  results: Record<string, unknown> | null;
  jobId?: string | null;
  startingCapital?: number | null;
  params?: Record<string, unknown> | null;
}

function fmtMetric(v: unknown, isInt = false): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return isInt ? String(Math.round(v)) : v.toFixed(3);
  return String(v);
}

function fmtCurrency(v: number): string {
  return new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(v);
}

function fmtUsdCompact(v: number): string {
  const n = Number(v);
  if (!Number.isFinite(n)) return 'N/A';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (abs >= 1_000) return `$${(n / 1_000).toFixed(0)}k`;
  return `$${Math.round(n)}`;
}

function fmtPercent2(v: unknown): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return `${v.toFixed(2)}%`;
  const n = Number(v);
  if (Number.isFinite(n)) return `${n.toFixed(2)}%`;
  return String(v);
}

function fmtSignedPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return 'N/A';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(digits)}%`;
}

function fmtDateLong(d: Date): string {
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
}

function fmtDateShortYear(d: Date): string {
  const month = d.toLocaleDateString(undefined, { month: 'short' });
  const day = String(d.getDate()).padStart(2, '0');
  const yy = String(d.getFullYear()).slice(-2);
  return `${month} ${day} '${yy}`;
}

function asPct(v: unknown): number | null {
  const n = asNum(v);
  return n === null ? null : n;
}

function metricColor(key: string, value: unknown): string {
  if (typeof value !== 'number') return 'var(--t2)';
  const heuristics: Record<string, (v: number) => string> = {
    sharpe: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    cagr: (v) => (v > 0 ? 'var(--green)' : v > -5 ? 'var(--amber)' : 'var(--red)'),
    max_dd: (v) => (v > -20 ? 'var(--green)' : v > -30 ? 'var(--amber)' : 'var(--red)'),
    active: (v) => (v > 90 ? 'var(--green)' : v > 30 ? 'var(--amber)' : 'var(--red)'),
    tot_ret: (v) => (v > 0 ? 'var(--green)' : v > -10 ? 'var(--amber)' : 'var(--red)'),
    grade: (v) => (v >= 80 ? 'var(--green)' : v >= 65 ? 'var(--amber)' : 'var(--red)'),
    sortino: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar_ratio: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    omega: (v) => (v > 1.5 ? 'var(--green)' : v > 1 ? 'var(--amber)' : 'var(--red)'),
    ulcer_index: (v) => (v < 5 ? 'var(--green)' : v < 15 ? 'var(--amber)' : 'var(--red)'),
    fa_oos_sharpe: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    dsr_pct: (v) => (v > 95 ? 'var(--green)' : v > 80 ? 'var(--amber)' : 'var(--red)'),
    cv: (v) => (v < 0.25 ? 'var(--green)' : v < 0.5 ? 'var(--amber)' : 'var(--red)'),
    flat_days: (v) => (v < 30 ? 'var(--green)' : v < 60 ? 'var(--amber)' : 'var(--red)'),
    avg_win_loss: (v) => (v >= 1.5 ? 'var(--green)' : v >= 1 ? 'var(--amber)' : 'var(--red)'),
    profit_factor: (v) => (v >= 1.5 ? 'var(--green)' : v >= 1 ? 'var(--amber)' : 'var(--red)'),
    uw_streak: (v) => (v <= 20 ? 'var(--green)' : v <= 60 ? 'var(--amber)' : 'var(--red)'),
    avg_1m: (v) => (v > 0 ? 'var(--green)' : v > -5 ? 'var(--amber)' : 'var(--red)'),
  };
  return heuristics[key]?.(value) ?? 'var(--t0)';
}

type XValue = number | string | Date;
type Point = { x: XValue; y: number } | number;

function parseDateLike(v: XValue | undefined): Date | null {
  if (v === undefined || v === null) return null;
  if (v instanceof Date && !Number.isNaN(v.getTime())) return v;
  if (typeof v === 'string') {
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  if (typeof v === 'number') {
    const ms = v > 1e12 ? v : v > 1e9 ? v * 1000 : null;
    if (!ms) return null;
    const d = new Date(ms);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}

function fmtDateLabel(d: Date): string {
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function sampleNumericSeriesToLength(series: number[], targetLength: number): number[] {
  if (!Array.isArray(series) || series.length === 0 || targetLength <= 0) return [];
  if (series.length === targetLength) return [...series];
  if (series.length === 1) return Array.from({ length: targetLength }, () => series[0]);
  return Array.from({ length: targetLength }, (_, i) => {
    const t = (i / Math.max(1, targetLength - 1)) * (series.length - 1);
    const lo = Math.floor(t);
    const hi = Math.min(series.length - 1, Math.ceil(t));
    const w = t - lo;
    return series[lo] * (1 - w) + series[hi] * w;
  });
}

function sampleMinSeriesToLength(series: number[], targetLength: number): number[] {
  if (!Array.isArray(series) || series.length === 0 || targetLength <= 0) return [];
  if (series.length === targetLength) return [...series];
  if (series.length === 1) return Array.from({ length: targetLength }, () => series[0]);
  const out: number[] = [];
  for (let i = 0; i < targetLength; i += 1) {
    const start = Math.floor((i / targetLength) * series.length);
    const end = Math.floor(((i + 1) / targetLength) * series.length);
    const s = Math.max(0, Math.min(series.length - 1, start));
    const e = Math.max(s + 1, Math.min(series.length, end));
    let min = Number.POSITIVE_INFINITY;
    for (let j = s; j < e; j += 1) {
      if (series[j] < min) min = series[j];
    }
    out.push(Number.isFinite(min) ? min : series[s]);
  }
  // Keep exact first/last points aligned with timeline endpoints.
  out[0] = series[0];
  out[out.length - 1] = series[series.length - 1];
  return out;
}

function normalizeDrawdownDecimal(v: number): number {
  // Backend may emit drawdown as percent points (e.g. -22.1) or decimals (e.g. -0.221).
  return Math.abs(v) > 1.5 ? v / 100 : v;
}

function normalizeDrawdownSeries(raw: number[], expectedMaxDdPct: number | null): number[] {
  if (raw.length === 0) return [];
  const expected = expectedMaxDdPct !== null && Number.isFinite(expectedMaxDdPct)
    ? -Math.abs(expectedMaxDdPct) / 100
    : null;
  const candidateA = raw.map((v) => Math.max(-1, Math.min(0, v)));
  const candidateB = raw.map((v) => Math.max(-1, Math.min(0, v / 100)));
  if (expected === null) {
    // Fall back to single-point normalization heuristic when we do not have target max DD.
    return raw.map((v) => Math.max(-1, Math.min(0, normalizeDrawdownDecimal(v))));
  }
  const minA = Math.min(...candidateA);
  const minB = Math.min(...candidateB);
  const errA = Math.abs(minA - expected);
  const errB = Math.abs(minB - expected);
  return errB <= errA ? candidateB : candidateA;
}

function percentile(sorted: number[], p: number): number {
  if (!sorted.length) return NaN;
  const idx = (sorted.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const w = idx - lo;
  return sorted[lo] * (1 - w) + sorted[hi] * w;
}

type BoxStats = {
  min: number;
  q1: number;
  median: number;
  q3: number;
  max: number;
  mean: number;
};

function computeBoxStats(values: number[]): BoxStats | null {
  const clean = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (clean.length === 0) return null;
  const mean = clean.reduce((a, b) => a + b, 0) / clean.length;
  return {
    min: clean[0],
    q1: percentile(clean, 0.25),
    median: percentile(clean, 0.5),
    q3: percentile(clean, 0.75),
    max: clean[clean.length - 1],
    mean,
  };
}

function weekStartKey(d: Date): string {
  const dt = new Date(d);
  const day = dt.getDay();
  const diff = (day + 6) % 7; // Monday-start week
  dt.setDate(dt.getDate() - diff);
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, '0');
  const dd = String(dt.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

function periodReturnsFromEquity(
  eq: Array<{ d: Date; y: number }>,
  keyFn: (d: Date) => string,
): number[] {
  if (eq.length < 2) return [];
  const grouped = new Map<string, { first: number; last: number }>();
  for (const p of eq) {
    const key = keyFn(p.d);
    if (!grouped.has(key)) grouped.set(key, { first: p.y, last: p.y });
    const cur = grouped.get(key)!;
    cur.last = p.y;
  }
  return Array.from(grouped.values())
    .map((r) => (r.first > 0 ? ((r.last / r.first) - 1) * 100 : NaN))
    .filter((v) => Number.isFinite(v));
}

type CalendarCell = {
  date: Date | null;
  key: string;
  day: number | null;
  ret: number | null;
};

type CalendarMonth = {
  monthKey: string;
  label: string;
  cells: CalendarCell[];
};

function dateKey(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

function buildCalendarMonths(eqSeries: Array<{ d: Date; y: number }>): CalendarMonth[] {
  if (eqSeries.length < 2) return [];
  const byDay = new Map<string, number>();
  for (let i = 1; i < eqSeries.length; i += 1) {
    const prev = eqSeries[i - 1].y;
    const cur = eqSeries[i].y;
    if (prev <= 0 || !Number.isFinite(cur)) continue;
    byDay.set(dateKey(eqSeries[i].d), ((cur / prev) - 1) * 100);
  }

  const start = new Date(eqSeries[0].d.getFullYear(), eqSeries[0].d.getMonth(), 1);
  const end = new Date(eqSeries[eqSeries.length - 1].d.getFullYear(), eqSeries[eqSeries.length - 1].d.getMonth(), 1);
  const out: CalendarMonth[] = [];
  const cursor = new Date(start);

  while (cursor <= end) {
    const y = cursor.getFullYear();
    const m = cursor.getMonth();
    const first = new Date(y, m, 1);
    const daysInMonth = new Date(y, m + 1, 0).getDate();
    const lead = first.getDay(); // Sunday start.
    const cells: CalendarCell[] = [];
    for (let i = 0; i < lead; i += 1) {
      cells.push({ date: null, key: `${y}-${m + 1}-lead-${i}`, day: null, ret: null });
    }
    for (let d = 1; d <= daysInMonth; d += 1) {
      const dt = new Date(y, m, d);
      const k = dateKey(dt);
      cells.push({
        date: dt,
        key: k,
        day: d,
        ret: byDay.get(k) ?? null,
      });
    }
    while (cells.length % 7 !== 0) {
      cells.push({ date: null, key: `${y}-${m + 1}-trail-${cells.length}`, day: null, ret: null });
    }
    out.push({
      monthKey: `${y}-${String(m + 1).padStart(2, '0')}`,
      label: first.toLocaleDateString(undefined, { month: 'short', year: 'numeric' }),
      cells,
    });
    cursor.setMonth(cursor.getMonth() + 1);
  }
  return out;
}

type XYPoint = { x: number; y: number };

function sampleSorted(values: number[], target = 240): number[] {
  if (values.length <= target) return values;
  const out: number[] = [];
  for (let i = 0; i < target; i += 1) {
    const idx = Math.round((i / Math.max(1, target - 1)) * (values.length - 1));
    out.push(values[idx]);
  }
  return out;
}

function computeCdfPoints(values: number[]): XYPoint[] {
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 0) return [];
  const sampled = sampleSorted(sorted);
  return sampled.map((x, i) => ({ x, y: (i + 1) / sampled.length }));
}

function computeEqfPoints(values: number[]): XYPoint[] {
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 0) return [];
  const sampled = sampleSorted(sorted);
  return sampled.map((y, i) => ({ x: i / Math.max(1, sampled.length - 1), y }));
}

function computePdfPoints(values: number[]): XYPoint[] {
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  if (n < 2) return [];
  const min = sorted[0];
  const max = sorted[n - 1];
  const range = Math.max(1e-9, max - min);
  const q1 = percentile(sorted, 0.25);
  const q3 = percentile(sorted, 0.75);
  const iqr = Math.max(1e-9, q3 - q1);
  const fdWidth = 2 * iqr / Math.cbrt(Math.max(2, n));
  const width = Math.max(1e-9, Number.isFinite(fdWidth) ? fdWidth : (range / 24));
  const totalBins = Math.max(18, Math.min(72, Math.ceil(range / width)));

  // Pass 1: uniform estimate to locate peak.
  const baseBins = new Array(totalBins).fill(0);
  for (const v of sorted) {
    const idx = Math.min(totalBins - 1, Math.max(0, Math.floor((v - min) / width)));
    baseBins[idx] += 1;
  }
  const baseSmooth = baseBins.map((_, i) => {
    const a = baseBins[Math.max(0, i - 1)];
    const b = baseBins[i];
    const c = baseBins[Math.min(totalBins - 1, i + 1)];
    return (a + (2 * b) + c) / 4;
  });
  let peakIdx = 0;
  for (let i = 1; i < baseSmooth.length; i += 1) {
    if (baseSmooth[i] > baseSmooth[peakIdx]) peakIdx = i;
  }
  const peakX = min + (peakIdx + 0.5) * width;

  // Pass 2: asymmetric bins (more left of peak, fewer right of peak).
  const leftSpan = peakX - min;
  const rightSpan = max - peakX;
  if (leftSpan <= 1e-9 || rightSpan <= 1e-9) {
    return baseSmooth.map((count, i) => {
      const center = min + (i + 0.5) * width;
      const density = count / (n * width);
      return { x: center, y: density };
    });
  }
  const leftBins = Math.max(10, Math.min(totalBins - 4, Math.round(totalBins * 0.68)));
  const rightBins = Math.max(4, totalBins - leftBins);
  const edges: number[] = [min];
  for (let i = 1; i <= leftBins; i += 1) {
    edges.push(min + (leftSpan * i) / leftBins);
  }
  for (let i = 1; i <= rightBins; i += 1) {
    edges.push(peakX + (rightSpan * i) / rightBins);
  }

  const counts = new Array(edges.length - 1).fill(0);
  for (const v of sorted) {
    let idx = edges.length - 2;
    for (let i = 0; i < edges.length - 1; i += 1) {
      const lo = edges[i];
      const hi = edges[i + 1];
      const inBin = (v >= lo && v < hi) || (i === edges.length - 2 && v <= hi);
      if (inBin) {
        idx = i;
        break;
      }
    }
    counts[idx] += 1;
  }

  const densityRaw = counts.map((count, i) => {
    const bw = Math.max(1e-9, edges[i + 1] - edges[i]);
    return count / (n * bw);
  });
  const densitySmooth = densityRaw.map((_, i) => {
    const a = densityRaw[Math.max(0, i - 1)];
    const b = densityRaw[i];
    const c = densityRaw[Math.min(densityRaw.length - 1, i + 1)];
    return (a + (2 * b) + c) / 4;
  });
  return densitySmooth.map((d, i) => ({
    x: (edges[i] + edges[i + 1]) / 2,
    y: d,
  }));
}

function inverseNormalCdf(p: number): number {
  // Acklam's approximation for inverse standard normal CDF.
  const pp = Math.min(1 - 1e-12, Math.max(1e-12, p));
  const a = [-39.6968302866538, 220.946098424521, -275.928510446969, 138.357751867269, -30.6647980661472, 2.50662827745924];
  const b = [-54.4760987982241, 161.585836858041, -155.698979859887, 66.8013118877197, -13.2806815528857];
  const c = [-0.00778489400243029, -0.322396458041136, -2.40075827716184, -2.54973253934373, 4.37466414146497, 2.93816398269878];
  const d = [0.00778469570904146, 0.32246712907004, 2.445134137143, 3.75440866190742];
  const plow = 0.02425;
  const phigh = 1 - plow;
  if (pp < plow) {
    const q = Math.sqrt(-2 * Math.log(pp));
    return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
      / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  if (pp > phigh) {
    const q = Math.sqrt(-2 * Math.log(1 - pp));
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
      / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  const q = pp - 0.5;
  const r = q * q;
  return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
}

function DistributionCard({
  title,
  subtitle,
  points,
  mode,
  xMinLabel,
  xMidLabel,
  xMaxLabel,
  xAxisLabel,
  yAxisLabel,
  yTickFormatter,
  xMarkers,
  guideX = null,
  hoverXFormatter,
  hoverYFormatter,
  summary,
  chips,
  splitColorBySign = false,
  signAxis = 'x',
  splitThreshold = 0,
}: {
  title: string;
  subtitle: string;
  points: XYPoint[];
  mode: 'line' | 'bar' | 'step_area';
  xMinLabel: string;
  xMidLabel: string;
  xMaxLabel: string;
  xAxisLabel: string;
  yAxisLabel: string;
  yTickFormatter: (v: number) => string;
  xMarkers?: Array<{ x: number; label: string; color?: string }>;
  guideX?: number | null;
  hoverXFormatter: (v: number) => string;
  hoverYFormatter: (v: number) => string;
  summary?: string;
  chips?: Array<{ label: string; value: string; color?: string }>;
  splitColorBySign?: boolean;
  signAxis?: 'x' | 'y';
  splitThreshold?: number;
}) {
  const H = 190;
  const W = 280;
  const padL = 42;
  const padR = 14;
  const padT = 10;
  const padB = 22;
  const xs = points.map((p) => p.x);
  const ys = points.map((p) => p.y);
  const minX = xs.length ? Math.min(...xs) : 0;
  const maxX = xs.length ? Math.max(...xs) : 1;
  const minY = ys.length ? Math.min(...ys) : 0;
  const maxY = ys.length ? Math.max(...ys) : 1;
  const rangeX = Math.max(1e-9, maxX - minX);
  const rangeY = Math.max(1e-9, maxY - minY);
  const toPx = (x: number) => padL + ((x - minX) / rangeX) * (W - padL - padR);
  const toPy = (y: number) => H - padB - ((y - minY) / rangeY) * (H - padT - padB);
  const yTicks = Array.from({ length: 5 }, (_, i) => minY + ((maxY - minY) * i) / 4);
  const guideXInside = guideX !== null && guideX !== undefined && guideX >= minX && guideX <= maxX;
  const guideXPx = guideXInside ? toPx(guideX as number) : null;
  const zeroYInside = minY <= 0 && maxY >= 0;
  const barBaselineY = zeroYInside ? toPy(0) : toPy(minY);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const pointPixels = points.map((p) => ({ px: toPx(p.x), py: toPy(p.y), x: p.x, y: p.y }));
  const hoverPoint = hoverIdx !== null ? pointPixels[hoverIdx] : null;
  const signValueOf = (p: XYPoint) => (signAxis === 'x' ? p.x : p.y);
  const lineColorFor = (v: number) => (v < splitThreshold ? 'rgba(255, 77, 77, 0.96)' : 'rgba(0, 200, 150, 0.96)');
  const barFillFor = (v: number) => (v < splitThreshold ? 'rgba(255, 77, 77, 0.35)' : 'rgba(0, 200, 150, 0.35)');
  const barStrokeFor = (v: number) => (v < splitThreshold ? 'rgba(255, 77, 77, 0.78)' : 'rgba(0, 200, 150, 0.78)');
  const linePath = pointPixels.length > 1
    ? pointPixels.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.px.toFixed(2)} ${p.py.toFixed(2)}`).join(' ')
    : '';
  const stepPath = pointPixels.length > 1
    ? (() => {
      const segs = [`M ${pointPixels[0].px.toFixed(2)} ${pointPixels[0].py.toFixed(2)}`];
      for (let i = 1; i < pointPixels.length; i += 1) {
        const prev = pointPixels[i - 1];
        const cur = pointPixels[i];
        segs.push(`L ${cur.px.toFixed(2)} ${prev.py.toFixed(2)}`);
        segs.push(`L ${cur.px.toFixed(2)} ${cur.py.toFixed(2)}`);
      }
      return segs.join(' ');
    })()
    : '';
  const stepAreaPath = (mode === 'step_area' && stepPath && pointPixels.length > 1)
    ? `${stepPath} L ${pointPixels[pointPixels.length - 1].px.toFixed(2)} ${barBaselineY.toFixed(2)} L ${pointPixels[0].px.toFixed(2)} ${barBaselineY.toFixed(2)} Z`
    : '';
  const barWidth = (() => {
    if (pointPixels.length < 2) return 4;
    let minDx = Number.POSITIVE_INFINITY;
    for (let i = 1; i < pointPixels.length; i += 1) {
      const dx = pointPixels[i].px - pointPixels[i - 1].px;
      if (dx > 0 && dx < minDx) minDx = dx;
    }
    if (!Number.isFinite(minDx)) return 4;
    return Math.max(1, Math.min(12, minDx * 0.7));
  })();
  return (
    <div
      style={{
        background: 'var(--bg2)',
        border: '1px solid var(--line)',
        borderRadius: 3,
        padding: 12,
        minWidth: 0,
        position: 'relative',
      }}
    >
      <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 8 }}>
        {title}
      </div>
      <div
        style={{
          fontSize: 9,
          color: 'var(--t2)',
          marginBottom: 6,
          fontFamily: 'var(--font-space-mono), Space Mono, monospace',
        }}
      >
        {subtitle}
      </div>
      {points.length > 1 ? (
        <svg
          ref={svgRef}
          viewBox={`0 0 ${W} ${H}`}
          style={{ width: '100%', height: H, display: 'block' }}
          preserveAspectRatio="none"
          onMouseMove={(e) => {
            if (!svgRef.current || pointPixels.length === 0) return;
            const rect = svgRef.current.getBoundingClientRect();
            const x = ((e.clientX - rect.left) / rect.width) * W;
            let best = 0;
            let bestDist = Math.abs(pointPixels[0].px - x);
            for (let i = 1; i < pointPixels.length; i += 1) {
              const d = Math.abs(pointPixels[i].px - x);
              if (d < bestDist) {
                bestDist = d;
                best = i;
              }
            }
            setHoverIdx(best);
          }}
          onMouseLeave={() => setHoverIdx(null)}
        >
          {yTicks.map((t, i) => {
            const y = toPy(t);
            return (
              <g key={`y-${i}`}>
                <line x1={padL} y1={y} x2={W - padR} y2={y} stroke="var(--line2)" strokeDasharray="2 2" opacity={0.65} />
                <text x={4} y={y + 3} fill="var(--t1)" fontSize="8.5" fontFamily="var(--font-space-mono), Space Mono, monospace">
                  {yTickFormatter(t)}
                </text>
              </g>
            );
          })}
          <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke="var(--line2)" />
          <line x1={padL} y1={padT} x2={padL} y2={H - padB} stroke="var(--line2)" />
          <text
            x={8}
            y={padT - 2}
            fill="var(--t2)"
            fontSize="8"
            fontFamily="var(--font-space-mono), Space Mono, monospace"
          >
            {yAxisLabel}
          </text>
          <text
            x={(padL + (W - padR)) / 2}
            y={H - 4}
            textAnchor="middle"
            fill="var(--t2)"
            fontSize="8"
            fontFamily="var(--font-space-mono), Space Mono, monospace"
          >
            {xAxisLabel}
          </text>
          {zeroYInside && (
            <line
              x1={padL}
              y1={barBaselineY}
              x2={W - padR}
              y2={barBaselineY}
              stroke="rgba(255,255,255,0.35)"
              strokeDasharray="2 2"
            />
          )}
          {guideXInside && guideXPx !== null && (
            <line
              x1={guideXPx}
              y1={padT}
              x2={guideXPx}
              y2={H - padB}
              stroke="rgba(255,255,255,0.5)"
              strokeWidth="0.8"
            />
          )}
          {mode === 'bar' ? pointPixels.map((p, i) => {
            const top = Math.min(barBaselineY, p.py);
            const h = Math.max(1, Math.abs(p.py - barBaselineY));
            const signV = signValueOf(points[i]);
            return (
              <rect
                key={`bar-${i}`}
                x={p.px - (barWidth / 2)}
                y={top}
                width={barWidth}
                height={h}
                fill={splitColorBySign ? barFillFor(signV) : 'rgba(0, 200, 150, 0.35)'}
                stroke={splitColorBySign ? barStrokeFor(signV) : 'rgba(0, 200, 150, 0.75)'}
                strokeWidth="0.4"
              />
            );
          }) : mode === 'step_area' ? (
            <>
              {stepAreaPath && (
                <path
                  d={stepAreaPath}
                  fill="rgba(0, 200, 150, 0.22)"
                  stroke="none"
                />
              )}
              <path
                d={stepPath}
                fill="none"
                stroke="rgba(0, 200, 150, 0.95)"
                strokeWidth="1.6"
                strokeLinejoin="miter"
                strokeLinecap="square"
              />
            </>
          ) : (
            splitColorBySign ? (
              <>
                {pointPixels.slice(1).map((p, i) => {
                  const prev = pointPixels[i];
                  const midSignVal = (signValueOf(points[i]) + signValueOf(points[i + 1])) / 2;
                  return (
                    <line
                      key={`seg-${i}`}
                      x1={prev.px}
                      y1={prev.py}
                      x2={p.px}
                      y2={p.py}
                      stroke={lineColorFor(midSignVal)}
                      strokeWidth="1.8"
                      strokeLinejoin="round"
                      strokeLinecap="round"
                    />
                  );
                })}
              </>
            ) : (
              <path
                d={linePath}
                fill="none"
                stroke="#00c896"
                strokeWidth="1.8"
                strokeLinejoin="round"
                strokeLinecap="round"
              />
            )
          )}
          {mode === 'bar' && (
            <path
              d={linePath}
              fill="none"
              stroke="rgba(0, 200, 150, 0.95)"
              strokeWidth="1.2"
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          )}
          {(xMarkers ?? []).map((m, i) => {
            if (!Number.isFinite(m.x)) return null;
            const x = toPx(m.x);
            return (
              <g key={`mk-${i}`}>
                <line x1={x} y1={padT} x2={x} y2={H - padB} stroke={m.color ?? 'rgba(0,200,150,0.45)'} strokeDasharray="2 2" />
                <text
                  x={Math.min(W - padR - 16, Math.max(padL + 2, x + 2))}
                  y={H - padB + 12}
                  fill={m.color ?? 'var(--t1)'}
                  fontSize="8"
                  fontFamily="var(--font-space-mono), Space Mono, monospace"
                >
                  {m.label}
                </text>
              </g>
            );
          })}
          {hoverPoint && (
            <>
              <line x1={hoverPoint.px} y1={padT} x2={hoverPoint.px} y2={H - padB} stroke="rgba(255,255,255,0.45)" strokeDasharray="2 2" />
              <circle cx={hoverPoint.px} cy={hoverPoint.py} r={2.8} fill="#ffba4d" />
            </>
          )}
        </svg>
      ) : (
        <div style={{ height: H, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, color: 'var(--t3)' }}>
          Not enough data
        </div>
      )}
      {hoverPoint && (
        <div
          style={{
            position: 'absolute',
            left: `${Math.min(92, Math.max(8, (hoverPoint.px / W) * 100))}%`,
            top: 54,
            transform: 'translateX(-50%)',
            background: 'var(--bg1)',
            border: '1px solid var(--line2)',
            borderRadius: 3,
            padding: '4px 6px',
            fontSize: 9,
            color: 'var(--t1)',
            whiteSpace: 'nowrap',
            pointerEvents: 'none',
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          }}
        >
          x: {hoverXFormatter(hoverPoint.x)} | y: {hoverYFormatter(hoverPoint.y)}
        </div>
      )}
      {(chips && chips.length > 0) && (
        <div style={{ marginTop: 8, display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {chips.map((chip) => (
            <span
              key={`${chip.label}-${chip.value}`}
              style={{
                border: '1px solid var(--line2)',
                background: 'var(--bg1)',
                borderRadius: 3,
                padding: '2px 6px',
                fontSize: 8,
                color: chip.color ?? 'var(--t1)',
                fontFamily: 'var(--font-space-mono), Space Mono, monospace',
              }}
            >
              {chip.label}: {chip.value}
            </span>
          ))}
        </div>
      )}
      {summary && (
        <div
          style={{
            marginTop: 6,
            fontSize: 9,
            color: 'var(--t2)',
            lineHeight: 1.35,
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          }}
        >
          {summary}
        </div>
      )}
      <div
        style={{
          marginTop: 8,
          display: 'grid',
          gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
          gap: 6,
          fontSize: 9,
          color: 'var(--t2)',
          fontFamily: 'var(--font-space-mono), Space Mono, monospace',
        }}
      >
        <span>{xMinLabel}</span>
        <span style={{ textAlign: 'center' }}>{xMidLabel}</span>
        <span style={{ textAlign: 'right' }}>{xMaxLabel}</span>
      </div>
    </div>
  );
}

function QqPlotCard({
  points,
  summary,
}: {
  points: XYPoint[];
  summary?: string;
}) {
  const H = 190;
  const W = 280;
  const padL = 42;
  const padR = 14;
  const padT = 10;
  const padB = 22;
  const xs = points.map((p) => p.x);
  const ys = points.map((p) => p.y);
  const minX = xs.length ? Math.min(...xs) : 0;
  const maxX = xs.length ? Math.max(...xs) : 1;
  const minY = ys.length ? Math.min(...ys) : 0;
  const maxY = ys.length ? Math.max(...ys) : 1;
  const low = Math.min(minX, minY);
  const high = Math.max(maxX, maxY);
  const range = Math.max(1e-9, high - low);
  const toPx = (v: number) => padL + ((v - low) / range) * (W - padL - padR);
  const toPy = (v: number) => H - padB - ((v - low) / range) * (H - padT - padB);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const pixels = points.map((p) => ({ px: toPx(p.x), py: toPy(p.y), x: p.x, y: p.y }));
  const hover = hoverIdx !== null ? pixels[hoverIdx] : null;
  const pointColor = (v: number) => (v < 0 ? 'rgba(255,77,77,0.85)' : 'rgba(0,200,150,0.85)');
  const tickVals = Array.from({ length: 5 }, (_, i) => low + ((high - low) * i) / 4);

  return (
    <div style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: 12, minWidth: 0, position: 'relative' }}>
      <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 8 }}>
        Q-Q Plot vs Normal Distribution
      </div>
      <div style={{ fontSize: 9, color: 'var(--t2)', marginBottom: 6, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
        x: theoretical normal quantiles, y: observed returns
      </div>
      {points.length > 1 ? (
        <svg
          ref={svgRef}
          viewBox={`0 0 ${W} ${H}`}
          style={{ width: '100%', height: H, display: 'block' }}
          preserveAspectRatio="none"
          onMouseMove={(e) => {
            if (!svgRef.current || pixels.length === 0) return;
            const rect = svgRef.current.getBoundingClientRect();
            const x = ((e.clientX - rect.left) / rect.width) * W;
            const y = ((e.clientY - rect.top) / rect.height) * H;
            let best = 0;
            let bestD = Number.POSITIVE_INFINITY;
            for (let i = 0; i < pixels.length; i += 1) {
              const dx = pixels[i].px - x;
              const dy = pixels[i].py - y;
              const d = (dx * dx) + (dy * dy);
              if (d < bestD) {
                bestD = d;
                best = i;
              }
            }
            setHoverIdx(best);
          }}
          onMouseLeave={() => setHoverIdx(null)}
        >
          {tickVals.map((t, i) => {
            const x = toPx(t);
            const y = toPy(t);
            return (
              <g key={`qq-tick-${i}`}>
                <line x1={padL} y1={y} x2={W - padR} y2={y} stroke="var(--line2)" strokeDasharray="2 2" opacity={0.65} />
                <line x1={x} y1={padT} x2={x} y2={H - padB} stroke="var(--line2)" strokeDasharray="2 2" opacity={0.35} />
                <text x={4} y={y + 3} fill="var(--t1)" fontSize="8.5" fontFamily="var(--font-space-mono), Space Mono, monospace">
                  {t.toFixed(2)}%
                </text>
              </g>
            );
          })}
          <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke="var(--line2)" />
          <line x1={padL} y1={padT} x2={padL} y2={H - padB} stroke="var(--line2)" />
          {low <= 0 && high >= 0 && (
            <line
              x1={toPx(0)}
              y1={padT}
              x2={toPx(0)}
              y2={H - padB}
              stroke="rgba(255,255,255,0.5)"
              strokeWidth="0.8"
            />
          )}
          <line
            x1={toPx(low)}
            y1={toPy(low)}
            x2={toPx(high)}
            y2={toPy(high)}
            stroke="rgba(255,255,255,0.5)"
            strokeWidth="0.9"
          />
          {pixels.map((p, i) => (
            <circle key={`qq-point-${i}`} cx={p.px} cy={p.py} r={2.1} fill={pointColor(p.y)} />
          ))}
          {hover && (
            <>
              <line x1={hover.px} y1={padT} x2={hover.px} y2={H - padB} stroke="rgba(255,255,255,0.45)" strokeDasharray="2 2" />
              <line x1={padL} y1={hover.py} x2={W - padR} y2={hover.py} stroke="rgba(255,255,255,0.45)" strokeDasharray="2 2" />
              <circle cx={hover.px} cy={hover.py} r={2.8} fill="#ffba4d" />
            </>
          )}
          <text x={(padL + (W - padR)) / 2} y={H - 4} textAnchor="middle" fill="var(--t2)" fontSize="8" fontFamily="var(--font-space-mono), Space Mono, monospace">
            theoretical quantile return %
          </text>
          <text x={8} y={padT - 2} fill="var(--t2)" fontSize="8" fontFamily="var(--font-space-mono), Space Mono, monospace">
            observed return %
          </text>
        </svg>
      ) : (
        <div style={{ height: H, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, color: 'var(--t3)' }}>
          Not enough data
        </div>
      )}
      {hover && (
        <div
          style={{
            position: 'absolute',
            left: `${Math.min(92, Math.max(8, (hover.px / W) * 100))}%`,
            top: 56,
            transform: 'translateX(-50%)',
            background: 'var(--bg1)',
            border: '1px solid var(--line2)',
            borderRadius: 3,
            padding: '4px 6px',
            fontSize: 9,
            color: 'var(--t1)',
            whiteSpace: 'nowrap',
            pointerEvents: 'none',
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          }}
        >
          theo: {hover.x.toFixed(2)}% | obs: {hover.y.toFixed(2)}%
        </div>
      )}
      {summary && (
        <div style={{ marginTop: 8, fontSize: 9, color: 'var(--t2)', lineHeight: 1.35, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
          {summary}
        </div>
      )}
    </div>
  );
}

function DailyReturnBarStatCard({
  values,
}: {
  values: number[];
}) {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const H = 190;
  const W = 280;
  const padL = 42;
  const padR = 14;
  const padT = 10;
  const padB = 22;
  const clean = values.filter((v) => Number.isFinite(v));
  if (clean.length === 0) {
    return (
      <div style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: 12, minWidth: 0 }}>
        <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 8 }}>
          Daily Returns Bar Chart
        </div>
        <div style={{ height: H, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, color: 'var(--t3)' }}>
          No return data
        </div>
      </div>
    );
  }

  // Preserve original chronological order (active-day sequence).
  const sampled = (() => {
    if (clean.length <= 160) return clean;
    return Array.from({ length: 160 }, (_, i) => {
      const idx = Math.round((i / 159) * (clean.length - 1));
      return clean[idx];
    });
  })();
  const mean = sampled.reduce((a, b) => a + b, 0) / sampled.length;
  const median = percentile(sampled, 0.5);
  const minVal = Math.min(...sampled, 0);
  const maxVal = Math.max(...sampled, 0);
  const range = Math.max(1e-9, maxVal - minVal);
  const toPy = (v: number) => H - padB - ((v - minVal) / range) * (H - padT - padB);
  const yMedian = toPy(median);
  const yMean = toPy(mean);
  const yZero = toPy(0);
  const plotW = W - padL - padR;
  const barW = Math.max(1, Math.min(3.5, plotW / Math.max(1, sampled.length)));
  const ticks = Array.from({ length: 5 }, (_, i) => minVal + ((maxVal - minVal) * i) / 4);
  const hoverVal = hoverIdx !== null ? sampled[hoverIdx] : null;
  const hoverX = hoverIdx !== null ? (padL + ((hoverIdx + 0.5) / sampled.length) * plotW) : null;
  const mappedOriginalIdx = (i: number) => {
    if (clean.length <= sampled.length) return i + 1;
    return Math.round((i / Math.max(1, sampled.length - 1)) * (clean.length - 1)) + 1;
  };

  return (
    <div style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: 12, minWidth: 0, position: 'relative' }}>
      <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 8 }}>
        Daily Returns Bar Chart
      </div>
      <div style={{ fontSize: 9, color: 'var(--t2)', marginBottom: 6, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
        Chronological active-day returns
      </div>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        style={{ width: '100%', height: H, display: 'block' }}
        preserveAspectRatio="none"
        onMouseMove={(e) => {
          if (!svgRef.current || sampled.length === 0) return;
          const rect = svgRef.current.getBoundingClientRect();
          const x = ((e.clientX - rect.left) / rect.width) * W;
          let idx = Math.floor(((x - padL) / Math.max(1e-9, plotW)) * sampled.length);
          idx = Math.max(0, Math.min(sampled.length - 1, idx));
          setHoverIdx(idx);
        }}
        onMouseLeave={() => setHoverIdx(null)}
      >
        {ticks.map((t, i) => {
          const y = toPy(t);
          return (
            <g key={`dret-tick-${i}`}>
              <line x1={padL} y1={y} x2={W - padR} y2={y} stroke="var(--line2)" strokeDasharray="2 2" opacity={0.65} />
              <text x={4} y={y + 3} fill="var(--t1)" fontSize="8.5" fontFamily="var(--font-space-mono), Space Mono, monospace">
                {t.toFixed(2)}%
              </text>
            </g>
          );
        })}
        <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke="var(--line2)" />
        <line x1={padL} y1={padT} x2={padL} y2={H - padB} stroke="var(--line2)" />
        <line x1={padL} y1={yZero} x2={W - padR} y2={yZero} stroke="rgba(255,255,255,0.35)" strokeDasharray="2 2" />
        {sampled.map((v, i) => {
          const x = padL + ((i + 0.5) / sampled.length) * plotW;
          const y = toPy(v);
          const top = Math.min(yZero, y);
          const h = Math.max(1, Math.abs(y - yZero));
          const isNeg = v < 0;
          const fill = isNeg ? 'rgba(255, 77, 77, 0.38)' : 'rgba(0, 200, 150, 0.35)';
          const stroke = isNeg ? 'rgba(255, 77, 77, 0.82)' : 'rgba(0, 200, 150, 0.75)';
          const isHover = hoverIdx === i;
          return (
            <rect
              key={`dbar-${i}`}
              x={x - (barW / 2)}
              y={top}
              width={barW}
              height={h}
              fill={isHover ? (isNeg ? 'rgba(255,77,77,0.62)' : 'rgba(0,200,150,0.58)') : fill}
              stroke={isHover ? '#ffba4d' : stroke}
              strokeWidth={isHover ? 0.8 : 0.35}
            />
          );
        })}
        {hoverX !== null && (
          <line
            x1={hoverX}
            y1={padT}
            x2={hoverX}
            y2={H - padB}
            stroke="rgba(255,255,255,0.45)"
            strokeDasharray="2 2"
          />
        )}

        <line
          x1={padL}
          y1={yMedian}
          x2={W - padR}
          y2={yMedian}
          stroke="rgba(255,255,255,0.9)"
          strokeWidth="1"
          strokeDasharray="3 2"
        />
        <line
          x1={padL}
          y1={yMean}
          x2={W - padR}
          y2={yMean}
          stroke="rgba(255,186,77,0.95)"
          strokeWidth="1"
          strokeDasharray="3 2"
        />
        <text
          x={W - padR - 2}
          y={yMean - 3}
          textAnchor="end"
          fill="rgba(255,186,77,0.95)"
          fontSize="8.5"
          fontFamily="var(--font-space-mono), Space Mono, monospace"
        >
          mean {mean.toFixed(2)}%
        </text>
        <text
          x={(padL + (W - padR)) / 2}
          y={H - 4}
          textAnchor="middle"
          fill="var(--t2)"
          fontSize="8"
          fontFamily="var(--font-space-mono), Space Mono, monospace"
        >
          active day index (chronological)
        </text>
      </svg>
      {hoverIdx !== null && hoverVal !== null && (
        <div
          style={{
            position: 'absolute',
            left: `${Math.min(92, Math.max(8, ((hoverX ?? 0) / W) * 100))}%`,
            top: 56,
            transform: 'translateX(-50%)',
            background: 'var(--bg1)',
            border: '1px solid var(--line2)',
            borderRadius: 3,
            padding: '4px 6px',
            fontSize: 9,
            color: 'var(--t1)',
            whiteSpace: 'nowrap',
            pointerEvents: 'none',
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          }}
        >
          idx: {mappedOriginalIdx(hoverIdx)} | ret: {hoverVal.toFixed(2)}%
        </div>
      )}
      <div style={{ marginTop: 8, fontSize: 9, color: 'var(--t2)', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
        n: {clean.length}
      </div>
    </div>
  );
}

function ReturnBoxPlotCard({
  title,
  stats,
  count,
}: {
  title: string;
  stats: BoxStats | null;
  count: number;
}) {
  const H = 340;
  const W = 240;
  const yAxisPadLeft = 44;
  const yAxisPadRight = 16;
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hovering, setHovering] = useState(false);
  if (!stats) {
    return (
      <div
        style={{
          background: 'var(--bg2)',
          border: '1px solid var(--line)',
          borderRadius: 3,
          padding: 12,
          flex: 1,
          minWidth: 0,
          height: H + 52,
          display: 'flex',
          flexDirection: 'column',
          gap: 8,
        }}
      >
        <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
          {title}
        </div>
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, color: 'var(--t3)' }}>
          No return data
        </div>
      </div>
    );
  }

  const minVal = Math.min(stats.min, 0);
  const maxVal = Math.max(stats.max, 0);
  const range = Math.max(1e-9, maxVal - minVal);
  const yFor = (v: number) => 10 + (1 - (v - minVal) / range) * (H - 20);
  const xMid = yAxisPadLeft + ((W - yAxisPadLeft - yAxisPadRight) / 2);
  const boxTop = yFor(stats.q3);
  const boxBottom = yFor(stats.q1);
  const medianY = yFor(stats.median);
  const minY = yFor(stats.min);
  const maxY = yFor(stats.max);
  const meanY = yFor(stats.mean);
  const zeroY = yFor(0);
  const zeroInside = 0 >= minVal && 0 <= maxVal;
  const tickCount = 5;
  const tickValues = Array.from({ length: tickCount }, (_, i) => {
    if (tickCount === 1) return minVal;
    return minVal + ((maxVal - minVal) * i) / (tickCount - 1);
  });

  return (
    <div
      style={{
        background: 'var(--bg2)',
        border: '1px solid var(--line)',
        borderRadius: 3,
        padding: 12,
        flex: 1,
        minWidth: 0,
        position: 'relative',
      }}
    >
      <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 8 }}>
        {title}
      </div>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        style={{ width: '100%', height: H, display: 'block' }}
        preserveAspectRatio="none"
        onMouseEnter={() => setHovering(true)}
        onMouseLeave={() => setHovering(false)}
      >
        {tickValues.map((tick, idx) => {
          const y = yFor(tick);
          const label = `${tick.toFixed(2)}%`;
          return (
            <g key={`tick-${idx}`}>
              <line
                x1={yAxisPadLeft}
                y1={y}
                x2={W - yAxisPadRight}
                y2={y}
                stroke="var(--line2)"
                strokeDasharray="2 2"
                opacity={0.65}
              />
              <text
                x={8}
                y={y + 3}
                fill="var(--t3)"
                fontSize="9"
                fontFamily="var(--font-space-mono), Space Mono, monospace"
              >
                {label}
              </text>
            </g>
          );
        })}
        {zeroInside && (
          <line
            x1={yAxisPadLeft}
            y1={zeroY}
            x2={W - yAxisPadRight}
            y2={zeroY}
            stroke="var(--line2)"
            strokeDasharray="3 2"
            opacity={1}
          />
        )}
        <line x1={xMid} y1={maxY} x2={xMid} y2={boxTop} stroke="var(--t2)" />
        <line x1={xMid} y1={boxBottom} x2={xMid} y2={minY} stroke="var(--t2)" />
        <line x1={xMid - 18} y1={maxY} x2={xMid + 18} y2={maxY} stroke="var(--t2)" />
        <line x1={xMid - 18} y1={minY} x2={xMid + 18} y2={minY} stroke="var(--t2)" />
        <rect
          x={xMid - 26}
          y={boxTop}
          width={52}
          height={Math.max(1, boxBottom - boxTop)}
          fill="rgba(0, 200, 150, 0.18)"
          stroke="rgba(0, 200, 150, 0.7)"
        />
        <line x1={xMid - 26} y1={medianY} x2={xMid + 26} y2={medianY} stroke="#00c896" strokeWidth={1.5} />
        <line
          x1={yAxisPadLeft}
          y1={meanY}
          x2={W - yAxisPadRight}
          y2={meanY}
          stroke="rgba(255,186,77,0.95)"
          strokeWidth="1"
          strokeDasharray="3 2"
        />
        <circle cx={xMid} cy={meanY} r={3.2} fill="#ffba4d" />
        <text
          x={W - yAxisPadRight - 2}
          y={meanY - 3}
          textAnchor="end"
          fill="rgba(255,186,77,0.95)"
          fontSize="8.5"
          fontFamily="var(--font-space-mono), Space Mono, monospace"
        >
          mean {stats.mean.toFixed(2)}%
        </text>
      </svg>
      {hovering && (
        <div
          style={{
            position: 'absolute',
            right: 10,
            top: 28,
            background: 'var(--bg1)',
            border: '1px solid var(--line2)',
            borderRadius: 3,
            padding: '6px 8px',
            fontSize: 9,
            color: 'var(--t1)',
            whiteSpace: 'nowrap',
            pointerEvents: 'none',
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
            lineHeight: 1.45,
          }}
        >
          <div>min: {stats.min.toFixed(2)}%</div>
          <div>q1: {stats.q1.toFixed(2)}%</div>
          <div>median: {stats.median.toFixed(2)}%</div>
          <div>mean: {stats.mean.toFixed(2)}%</div>
          <div>q3: {stats.q3.toFixed(2)}%</div>
          <div>max: {stats.max.toFixed(2)}%</div>
        </div>
      )}
      <div
        style={{
          marginTop: 8,
          display: 'grid',
          gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
          gap: 6,
          fontSize: 9,
          color: 'var(--t2)',
          fontFamily: 'var(--font-space-mono), Space Mono, monospace',
        }}
      >
        <span>n: {count}</span>
        <span>mean: {stats.mean.toFixed(2)}%</span>
        <span>med: {stats.median.toFixed(2)}%</span>
      </div>
    </div>
  );
}

function normalizeFilterLabel(s: string): string {
  return s
    .toLowerCase()
    .replace(/\+/g, 'p')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function extractAlerts(text: string): string[] {
  return text
    .split('\n')
    .filter((line) => /warning|error|failed|unavailable|exception/i.test(line))
    .filter((line) => line.trim().length > 0)
    .slice(-24);
}

function normalizeLoose(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function parseHeadingLine(line: string): string | null {
  if (/^FEES PANEL\b/i.test(line)) {
    return 'FEES PANEL';
  }
  if (!line.startsWith('┌─')) return null;
  return line.replace(/^┌─\s*/, '').replace(/\s*─+$/, '').trim();
}

type ParsedSection = { title: string; body: string };

type FullReportCategoryKey =
  | 'executive_summary'
  | 'core_performance'
  | 'risk_profile'
  | 'signal_quality'
  | 'robustness_stress'
  | 'parameter_exploration'
  | 'stability_cubes'
  | 'capacity_diagnostics'
  | 'milestones_appendices'
  | 'artifact_outputs';

type FullReportCategoryDef = {
  key: FullReportCategoryKey;
  title: string;
  defaultOpen: boolean;
};

const FULL_REPORT_CATEGORIES: FullReportCategoryDef[] = [
  { key: 'executive_summary', title: 'Executive Summary', defaultOpen: true },
  { key: 'core_performance', title: 'Core Performance', defaultOpen: true },
  { key: 'risk_profile', title: 'Risk Profile', defaultOpen: true },
  { key: 'signal_quality', title: 'Signal Quality', defaultOpen: false },
  { key: 'robustness_stress', title: 'Robustness & Stress', defaultOpen: false },
  { key: 'parameter_exploration', title: 'Parameter Exploration', defaultOpen: false },
  { key: 'stability_cubes', title: 'Stability Cubes', defaultOpen: false },
  { key: 'capacity_diagnostics', title: 'Capacity & Diagnostics', defaultOpen: false },
  { key: 'milestones_appendices', title: 'Milestones & Appendices', defaultOpen: false },
  { key: 'artifact_outputs', title: 'Artifact Outputs', defaultOpen: false },
];

function fullReportCategoryForTitle(title: string): FullReportCategoryKey {
  const t = title.toUpperCase();
  if (/^RUN SUMMARY\b/.test(t) || /^BEST FILTER HEADLINE STATS\b/.test(t)) return 'executive_summary';

  if (
    /^DAILY SERIES AUDIT\b/.test(t)
    || /^RETURN RATES BY PERIOD\b/.test(t)
    || /^RETURN DISTRIBUTION\b/.test(t)
    || /^RETURN \+ CONDITIONAL ANALYSIS\b/.test(t)
    || /^ROLLING MAX DRAWDOWN\b/.test(t)
  ) {
    return 'core_performance';
  }

  if (
    /^RISK-ADJUSTED RETURN QUALITY\b/.test(t)
    || /^DAILY VAR \/ CVAR\b/.test(t)
    || /^DRAWDOWN EPISODE ANALYSIS\b/.test(t)
    || /^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/.test(t)
    || /^RUIN PROBABILITY\b/.test(t)
  ) {
    return 'risk_profile';
  }

  if (
    /^SIGNAL PREDICTIVENESS\b/.test(t)
    || /^ALLOCATOR VIEW SCORECARD\b/.test(t)
    || /^TECHNICAL APPENDIX SCORECARD\b/.test(t)
  ) {
    return 'signal_quality';
  }

  if (
    /^SLIPPAGE IMPACT SWEEP\b/.test(t)
    || /^NOISE PERTURBATION STABILITY TEST\b/.test(t)
    || /^PARAM JITTER \/ SHARPE STABILITY TEST\b/.test(t)
    || /^RETURN CONCENTRATION ANALYSIS\b/.test(t)
    || /^PERIODIC RETURN BREAKDOWN\b/.test(t)
    || /^SHOCK INJECTION TEST\b/.test(t)
    || /^REGIME ROBUSTNESS TEST\b/.test(t)
    || /^REGIME CONSISTENCY SUMMARY\b/.test(t)
  ) {
    return 'robustness_stress';
  }

  if (
    /^TAIL GUARDRAIL GRID SWEEP\b/.test(t)
    || /^PARAMETER SWEEP\b/.test(t)
    || /^L_HIGH SURFACE - RANKED BY SHARPE\b/.test(t)
    || /^PARAMETER SURFACE MAP\b/.test(t)
    || /^SHARPE RIDGE MAP\b/.test(t)
    || /^SHARPE PLATEAU DETECTOR\b/.test(t)
  ) {
    return 'parameter_exploration';
  }

  if (
    /^PARAMETRIC STABILITY CUBE\b/.test(t)
    || /^RISK THROTTLE STABILITY CUBE\b/.test(t)
    || /^EXIT ARCHITECTURE STABILITY CUBE\b/.test(t)
  ) {
    return 'stability_cubes';
  }

  if (
    /^LIQUIDITY CAPACITY CURVE\b/.test(t)
    || /^MINIMUM CUMULATIVE RETURN\b/.test(t)
    || /^MARKET CAP DIAGNOSTIC\b/.test(t)
    || /^MARKET CAP UNIVERSE SUMMARY\b/.test(t)
  ) {
    return 'capacity_diagnostics';
  }

  if (
    /^WEEKLY MILESTONES\b/.test(t)
    || /^MONTHLY MILESTONES\b/.test(t)
    || /^WHAT YOU HAVE\b/.test(t)
    || /^WHAT YOU STILL NEED\b/.test(t)
    || /^BOTTOM LINE\b/.test(t)
  ) {
    return 'milestones_appendices';
  }

  if (
    /^EQUITY ENSEMBLE\b/.test(t)
    || /OUTPUT FILES/i.test(t)
    || /SAVED:/i.test(t)
  ) {
    return 'artifact_outputs';
  }

  return 'core_performance';
}

function extractRunSummarySection(text: string): ParsedSection | null {
  if (!text) return null;
  const lines = text.split('\n');
  const start = lines.findIndex((l) => /\bRUN SUMMARY\b/i.test(l));
  if (start < 0) return null;
  let end = lines.length;
  for (let i = start + 1; i < lines.length; i += 1) {
    const t = lines[i].trim();
    if (parseHeadingLine(t)) {
      end = i;
      break;
    }
    if (isDividerLine(t)) {
      const next = (lines[i + 1] ?? '').trim();
      if (
        parseHeadingLine(next)
        || isSpecialSectionTitleLine(next)
        || /^OUTPUT FILES$/i.test(next)
        || /^RUN INPUTS SUMMARY$/i.test(next)
      ) {
        end = i;
        break;
      }
    }
  }
  const body = lines.slice(start + 1, end).join('\n').trim();
  if (!body) return null;
  return { title: 'RUN SUMMARY', body };
}

function extractSelectedFilterAdvancedSections(text: string, selectedFilter: string | null): ParsedSection[] {
  if (!text || !selectedFilter) return [];
  const lines = text.split('\n');
  const headings: Array<{ idx: number; title: string }> = [];
  for (let i = 0; i < lines.length; i += 1) {
    const title = parseHeadingLine(lines[i].trim());
    if (title) headings.push({ idx: i, title });
  }
  if (headings.length === 0) return [];

  const filterVariants = [
    selectedFilter,
    selectedFilter.replace(/\+/g, 'p'),
    selectedFilter.replace(/\+/g, ''),
    selectedFilter.replace(/\bp\b/gi, ''),
    selectedFilter.replace(/\s+/g, '_'),
  ].map(normalizeLoose);

  const stressCandidates = headings.filter((h) => h.title.includes('STRESS TEST SUMMARY'));
  const matchingStress = stressCandidates.find((h) => filterVariants.some((fv) => fv.length > 0 && normalizeLoose(h.title).includes(fv)));
  const stressRef = matchingStress ?? stressCandidates[stressCandidates.length - 1];
  if (!stressRef) return [];

  const blockStart = [...headings]
    .reverse()
    .find((h) => h.idx < stressRef.idx && (h.title.includes('FEES PANEL') || h.title.includes('DAILY SERIES AUDIT')))?.idx ?? 0;
  const nextFeesIdx = headings.find((h) => h.idx > stressRef.idx && h.title.includes('FEES PANEL'))?.idx;
  const nextDailyAuditIdx = headings.find((h) => h.idx > stressRef.idx && h.title.includes('DAILY SERIES AUDIT'))?.idx;
  const runSummaryIdx = lines.findIndex((l, i) => i > blockStart && l.includes('RUN SUMMARY'));
  const endCandidates = [nextFeesIdx, nextDailyAuditIdx, runSummaryIdx >= 0 ? runSummaryIdx : undefined]
    .filter((v): v is number => typeof v === 'number' && v > blockStart)
    .sort((a, b) => a - b);
  const blockEnd = endCandidates[0] ?? lines.length;

  const blockHeadings = headings.filter((h) => h.idx >= blockStart && h.idx < blockEnd);
  const sections: ParsedSection[] = [];
  for (let i = 0; i < blockHeadings.length; i += 1) {
    const cur = blockHeadings[i];
    const next = blockHeadings[i + 1];
    const start = cur.idx + 1;
    const end = next ? next.idx : blockEnd;
    let sectionLines = lines.slice(start, end);
    if (cur.title.includes('BOTTOM LINE')) {
      const cutoff = sectionLines.findIndex((line) => {
        const t = line.trim();
        return (
          t.startsWith('════════')
          || t.startsWith('[equity overwrite]')
          || t.startsWith('SIGNAL PREDICTIVENESS')
          || t.startsWith('RUN INPUTS SUMMARY')
          || t.startsWith('OUTPUT FILES')
          || /^\d{4}-\d{2}-\d{2}/.test(t)
        );
      });
      if (cutoff >= 0) {
        sectionLines = sectionLines.slice(0, cutoff);
      }
    }
    const body = sectionLines.join('\n').trim();
    if (!body) continue;
    sections.push({ title: cur.title, body });
  }
  return sections;
}

function extractFullReportSections(text: string): ParsedSection[] {
  if (!text) return [];
  const lines = text.split('\n');
  const headings: Array<{ idx: number; title: string }> = [];
  for (let i = 0; i < lines.length; i += 1) {
    const title = parseHeadingLine(lines[i].trim());
    if (title) headings.push({ idx: i, title });
  }
  if (headings.length === 0) {
    return [{ title: 'FULL REPORT', body: text }];
  }
  const sections: ParsedSection[] = [];
  for (let i = 0; i < headings.length; i += 1) {
    const cur = headings[i];
    const next = headings[i + 1];
    const start = cur.idx + 1;
    const end = next ? next.idx : lines.length;
    const body = lines.slice(start, end).join('\n').trim();
    if (!body) continue;
    sections.push({ title: cur.title, body });
  }
  return sections;
}

function filterSectionsForSelectedFilter(sections: ParsedSection[], selectedFilter: string | null): ParsedSection[] {
  if (!selectedFilter) return sections;
  const variants = [
    selectedFilter,
    selectedFilter.replace(/\+/g, 'p'),
    selectedFilter.replace(/\+/g, ''),
    selectedFilter.replace(/\bp\b/gi, ''),
    selectedFilter.replace(/\s+/g, '_'),
    selectedFilter.replace(/^A\s*-\s*/i, ''),
  ]
    .map(normalizeLoose)
    .filter((v) => v.length > 0);
  const filtered = sections.filter((section) => {
    const hay = normalizeLoose(`${section.title}\n${section.body}`);
    return variants.some((v) => hay.includes(v));
  });
  return filtered.length > 0 ? filtered : sections;
}

function isDividerLine(line: string): boolean {
  const t = line.trim();
  return /^={20,}$/.test(t) || /^═{20,}$/.test(t);
}

function parseSignalFilterName(titleLine: string): string {
  const parts = titleLine.split('|').map((p) => p.trim()).filter(Boolean);
  return parts.length >= 2 ? parts[1] : '';
}

function parseMilestoneFilterName(titleLine: string): string {
  const parts = titleLine.split('|').map((p) => p.trim()).filter(Boolean);
  return parts.length >= 2 ? parts[1] : '';
}

function parseFilterFromDelimitedTitle(titleLine: string): string {
  const parts = titleLine.split('|').map((p) => p.trim()).filter(Boolean);
  if (parts.length >= 2) return parts[1];
  const paren = titleLine.match(/\(([^)]+)\)/);
  return paren ? paren[1] : '';
}

function canonicalizeFilterLabel(s: string): string {
  const t = normalizeLoose(s);
  if (!t) return '';
  if (t.includes('nofilter')) return 'no_filter';
  if (t.includes('calendarfilter')) return 'calendar';
  if (t.includes('tailguardrail')) return 'tail_guardrail';
  if (t.includes('tail') && t.includes('disp') && t.includes('vol')) return 'tail_disp_vol';
  if (t.includes('tail') && t.includes('vol') && t.includes('or')) return 'tail_vol_or';
  if (t.includes('tail') && t.includes('vol') && t.includes('and')) return 'tail_vol_and';
  if (t.includes('tail') && t.includes('blofin')) return 'tail_blofin';
  if (t.includes('tail') && (t.includes('dispersion') || t.includes('disp'))) return 'tail_dispersion';
  if (t === 'dispersion' || (t.includes('dispersion') && !t.includes('tail'))) return 'dispersion';
  if (t.includes('volatility') && !t.includes('tail')) return 'volatility';
  return t;
}

function matchesSelectedFilter(filterName: string, selectedFilter: string | null): boolean {
  if (!selectedFilter) return true;
  const canonA = canonicalizeFilterLabel(filterName);
  const canonB = canonicalizeFilterLabel(selectedFilter);
  if (canonA && canonB) return canonA === canonB;
  const variants = [
    selectedFilter,
    selectedFilter.replace(/^A\s*-\s*/i, ''),
    selectedFilter.replace(/\+/g, 'p'),
    selectedFilter.replace(/\+/g, ''),
    selectedFilter.replace(/\bp\b/gi, ''),
    selectedFilter.replace(/\s+/g, '_'),
  ].map(normalizeLoose);
  const hay = normalizeLoose(filterName);
  return variants.some((v) => v.length > 0 && (hay.includes(v) || v.includes(hay)));
}

function isSpecialSectionTitleLine(line: string): boolean {
  const t = line.trim();
  return (
    t.includes('WEEKLY MILESTONES')
    || t.includes('MONTHLY MILESTONES')
    || t.includes('SIGNAL PREDICTIVENESS')
    || t.includes('ALLOCATOR VIEW SCORECARD')
    || t.includes('TECHNICAL APPENDIX SCORECARD')
    || /^TAIL GUARDRAIL GRID SWEEP\b/i.test(t)
    || /^PARAMETER SWEEP\b/i.test(t)
    || /^L_HIGH SURFACE - RANKED BY SHARPE\b/i.test(t)
    || /^PARAMETER SURFACE MAP\b/i.test(t)
    || /^SLIPPAGE IMPACT SWEEP\b/i.test(t)
    || /^NOISE PERTURBATION STABILITY TEST\b/i.test(t)
    || /^PARAM JITTER \/ SHARPE STABILITY TEST\b/i.test(t)
    || /^RETURN CONCENTRATION ANALYSIS\b/i.test(t)
    || /^PERIODIC RETURN BREAKDOWN\b/i.test(t)
    || /^MINIMUM CUMULATIVE RETURN\b/i.test(t)
    || /^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/i.test(t)
    || /^SHOCK INJECTION TEST\s+\|\s+Filter:/i.test(t)
    || /^RUIN PROBABILITY\s+\|\s+Filter:/i.test(t)
    || /^LIQUIDITY CAPACITY CURVE\s+\|\s+Filter:/i.test(t)
    || /^PARAMETRIC STABILITY CUBE\b/i.test(t)
    || /^RISK THROTTLE STABILITY CUBE(?! SUMMARY)\b/i.test(t)
    || /^EXIT ARCHITECTURE STABILITY CUBE(?! SUMMARY)\b/i.test(t)
    || /^REGIME ROBUSTNESS TEST\s+\|\s+Filter:/i.test(t)
    || /^REGIME CONSISTENCY SUMMARY\b/i.test(t)
    || /^MARKET CAP DIAGNOSTIC\b/i.test(t)
    || /^MARKET CAP UNIVERSE SUMMARY\b/i.test(t)
  );
}

function findDividerBoundedEnd(lines: string[], start: number): number {
  for (let i = start + 1; i < lines.length; i += 1) {
    if (isDividerLine(lines[i])) {
      const after = (lines[i + 1] ?? '').trim();
      if (isSpecialSectionTitleLine(after) || /^OUTPUT FILES$/i.test(after) || /^RUN INPUTS SUMMARY$/i.test(after)) {
        return i;
      }
    }
  }
  return lines.length;
}

function extractSpecialFullReportSections(text: string, selectedFilter: string | null): ParsedSection[] {
  if (!text) return [];
  const lines = text.split('\n');
  const sections: ParsedSection[] = [];

  const signalStarts: number[] = [];
  for (let i = 0; i < lines.length; i += 1) {
    if (lines[i].includes('SIGNAL PREDICTIVENESS')) {
      signalStarts.push(i);
    }
  }
  for (let i = 0; i < signalStarts.length; i += 1) {
    const start = signalStarts[i];
    const nextSignal = signalStarts[i + 1] ?? lines.length;
    const line = lines[start].trim().replace(/\s{2,}/g, ' ');
    const filterName = parseSignalFilterName(line);
    if (!matchesSelectedFilter(filterName, selectedFilter)) continue;
    const end = Math.min(
      nextSignal,
      lines.findIndex((l, idx) => idx > start && l.includes('ALLOCATOR VIEW SCORECARD')) > -1
        ? lines.findIndex((l, idx) => idx > start && l.includes('ALLOCATOR VIEW SCORECARD'))
        : lines.length,
    );
    const body = lines.slice(start + 1, end).join('\n').trim();
    if (!body) continue;
    sections.push({ title: line, body });
  }

  const scorecardTitles = ['ALLOCATOR VIEW SCORECARD', 'TECHNICAL APPENDIX SCORECARD'];
  for (const scorecardTitle of scorecardTitles) {
    const start = lines.findIndex((l) => l.includes(scorecardTitle));
    if (start < 0) continue;
    let end = lines.length;
    for (let i = start + 1; i < lines.length; i += 1) {
      const t = lines[i].trim();
      if (t.includes('ALLOCATOR VIEW SCORECARD') || t.includes('TECHNICAL APPENDIX SCORECARD')) {
        end = i;
        break;
      }
      if (isDividerLine(t)) {
        const next = (lines[i + 1] ?? '').trim();
        if (/^OUTPUT FILES$/i.test(next) || /^RUN INPUTS SUMMARY$/i.test(next)) {
          end = i;
          break;
        }
      }
    }
    const body = lines.slice(start + 1, end).join('\n').trim();
    if (!body) continue;
    sections.push({ title: scorecardTitle, body });
  }

  const milestoneTitles = ['WEEKLY MILESTONES', 'MONTHLY MILESTONES'];
  for (const milestoneTitle of milestoneTitles) {
    const starts: number[] = [];
    for (let i = 0; i < lines.length; i += 1) {
      if (lines[i].includes(milestoneTitle)) starts.push(i);
    }
    let matchedForSelected = 0;
    for (const start of starts) {
      const titleLine = lines[start].trim().replace(/\s{2,}/g, ' ');
      const filterName = parseMilestoneFilterName(titleLine);
      const isMatch = matchesSelectedFilter(filterName, selectedFilter);
      if (!isMatch) continue;
      matchedForSelected += 1;
      const end = findDividerBoundedEnd(lines, start);
      const body = lines.slice(start + 1, end).join('\n').trim();
      if (!body) continue;
      sections.push({ title: titleLine, body });
    }
    // Do not fallback to all filters here; milestones should follow selected/best filter only.
    void matchedForSelected;
  }

  const sweepTitleMatchers = [
    /^TAIL GUARDRAIL GRID SWEEP\b/i,
    /^PARAMETER SWEEP\b/i,
    /^L_HIGH SURFACE - RANKED BY SHARPE\b/i,
    /^PARAMETER SURFACE MAP\b/i,
    /^SLIPPAGE IMPACT SWEEP\b/i,
    /^NOISE PERTURBATION STABILITY TEST\b/i,
    /^PARAM JITTER \/ SHARPE STABILITY TEST\b/i,
    /^RETURN CONCENTRATION ANALYSIS\b/i,
    /^PERIODIC RETURN BREAKDOWN\b/i,
    /^MINIMUM CUMULATIVE RETURN\b/i,
    /^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/i,
    /^SHOCK INJECTION TEST\s+\|\s+Filter:/i,
    /^RUIN PROBABILITY\s+\|\s+Filter:/i,
    /^LIQUIDITY CAPACITY CURVE\s+\|\s+Filter:/i,
    /^PARAMETRIC STABILITY CUBE\b/i,
    /^RISK THROTTLE STABILITY CUBE(?! SUMMARY)\b/i,
    /^EXIT ARCHITECTURE STABILITY CUBE(?! SUMMARY)\b/i,
    /^REGIME ROBUSTNESS TEST\s+\|\s+Filter:/i,
    /^REGIME CONSISTENCY SUMMARY\b/i,
    /^MARKET CAP DIAGNOSTIC\b/i,
    /^MARKET CAP UNIVERSE SUMMARY\b/i,
  ];
  const byFamilyMatched = new Map<number, ParsedSection[]>();
  const byFamilyAll = new Map<number, ParsedSection[]>();
  for (let i = 0; i < lines.length; i += 1) {
    const lineRaw = lines[i];
    const line = lineRaw.trim();
    const familyIdx = sweepTitleMatchers.findIndex((rx) => rx.test(line));
    if (familyIdx < 0) continue;
    const titleLine = line.replace(/\s{2,}/g, ' ');
    const end = findDividerBoundedEnd(lines, i);
    const body = lines.slice(i + 1, end).join('\n').trim();
    if (!body) continue;
    const parsed: ParsedSection = { title: titleLine, body };
    const parenFilter = line.match(/\(([^)]+?)\s+filter\)/i)?.[1] ?? '';
    const inlineFilter = line.match(/Filter:\s*([^|]+)$/i)?.[1]?.trim() ?? '';
    const filterName = parenFilter || inlineFilter || parseFilterFromDelimitedTitle(line);
    if (!filterName) {
      sections.push(parsed);
      continue;
    }
    const allForFamily = byFamilyAll.get(familyIdx) ?? [];
    allForFamily.push(parsed);
    byFamilyAll.set(familyIdx, allForFamily);
    if (!matchesSelectedFilter(filterName, selectedFilter)) continue;
    const matchedForFamily = byFamilyMatched.get(familyIdx) ?? [];
    matchedForFamily.push(parsed);
    byFamilyMatched.set(familyIdx, matchedForFamily);
  }
  for (const [familyIdx, allForFamily] of byFamilyAll.entries()) {
    const matchedForFamily = byFamilyMatched.get(familyIdx);
    const chosen = matchedForFamily && matchedForFamily.length > 0 ? matchedForFamily : allForFamily;
    sections.push(...chosen);
  }

  // Single-line section for equity ensemble output when only a chart path is emitted.
  for (const raw of lines) {
    const t = raw.trim();
    if (!/^Equity ensemble chart saved:/i.test(t)) continue;
    sections.push({
      title: 'EQUITY ENSEMBLE',
      body: t,
    });
  }
  const sharpeRidgeLines = lines
    .map((raw) => raw.trim())
    .filter((t) => /^Sharpe ridge map saved\s*:/i.test(t));
  if (sharpeRidgeLines.length > 0) {
    sections.push({
      title: 'SHARPE RIDGE MAP',
      body: sharpeRidgeLines.join('\n'),
    });
  }
  const sharpePlateauLines = lines
    .map((raw) => raw.trim())
    .filter((t) => /^Sharpe plateau detector\s*:/i.test(t));
  if (sharpePlateauLines.length > 0) {
    sections.push({
      title: 'SHARPE PLATEAU DETECTOR',
      body: sharpePlateauLines.join('\n'),
    });
  }

  return sections;
}

function fullReportOrderRank(title: string): number {
  const t = title.toUpperCase();
  const order: Array<[RegExp, number]> = [
    [/^DAILY SERIES AUDIT\b/, 10],
    [/^SIMULATION BIAS AUDIT\b/, 20],
    [/^RETURN RATES BY PERIOD\b/, 30],
    [/^RISK-ADJUSTED RETURN QUALITY\b/, 40],
    [/^DRAWDOWN EPISODE ANALYSIS\b/, 50],
    [/^RETURN DISTRIBUTION\b/, 60],
    [/^RETURN \+ CONDITIONAL ANALYSIS\b/, 70],
    [/^ROLLING MAX DRAWDOWN\b/, 80],
    [/^DAILY VAR \/ CVAR\b/, 90],

    [/^SIGNAL PREDICTIVENESS\b/, 120],
    [/^ALLOCATOR VIEW SCORECARD\b/, 130],
    [/^TECHNICAL APPENDIX SCORECARD\b/, 140],

    [/^WEEKLY MILESTONES\b/, 160],
    [/^MONTHLY MILESTONES\b/, 170],

    [/^TAIL GUARDRAIL GRID SWEEP\b/, 200],
    [/^PARAMETER SWEEP\b/, 210],
    [/^L_HIGH SURFACE - RANKED BY SHARPE\b/, 220],
    [/^PARAMETER SURFACE MAP\b/, 230],
    [/^SHARPE RIDGE MAP\b/, 240],
    [/^SHARPE PLATEAU DETECTOR\b/, 250],

    [/^PARAMETRIC STABILITY CUBE\b/, 300],
    [/^RISK THROTTLE STABILITY CUBE\b/, 310],
    [/^EXIT ARCHITECTURE STABILITY CUBE\b/, 320],

    [/^SLIPPAGE IMPACT SWEEP\b/, 350],
    [/^NOISE PERTURBATION STABILITY TEST\b/, 360],
    [/^PARAM JITTER \/ SHARPE STABILITY TEST\b/, 370],
    [/^RETURN CONCENTRATION ANALYSIS\b/, 380],
    [/^PERIODIC RETURN BREAKDOWN\b/, 390],
    [/^SHOCK INJECTION TEST\b/, 400],
    [/^RUIN PROBABILITY\b/, 410],
    [/^REGIME ROBUSTNESS TEST\b/, 420],
    [/^REGIME CONSISTENCY SUMMARY\b/, 430],

    [/^LIQUIDITY CAPACITY CURVE\b/, 450],
    [/^MINIMUM CUMULATIVE RETURN\b/, 460],
    [/^MARKET CAP DIAGNOSTIC\b/, 470],
    [/^MARKET CAP UNIVERSE SUMMARY\b/, 480],
    [/^EQUITY ENSEMBLE\b/, 490],

    [/^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/, 520],
    [/^WHAT YOU HAVE\b/, 530],
    [/^WHAT YOU STILL NEED\b/, 540],
    [/^BOTTOM LINE\b/, 550],
  ];
  for (const [rx, rank] of order) {
    if (rx.test(t)) return rank;
  }
  return 999;
}

function buildFullReportSections(text: string, selectedFilter: string | null): ParsedSection[] {
  const all = extractFullReportSections(text);
  const stress = extractSelectedFilterAdvancedSections(text, selectedFilter);
  const signalAndScorecardsFromHeadings = filterSectionsForSelectedFilter(all, selectedFilter).filter((s) => {
    const t = s.title.toUpperCase();
    return (
      t.includes('SIGNAL PREDICTIVENESS')
      || t.includes('ALLOCATOR VIEW SCORECARD')
      || t.includes('TECHNICAL APPENDIX SCORECARD')
    );
  });
  const signalAndScorecardsFromText = extractSpecialFullReportSections(text, selectedFilter);
  const merged = [...stress, ...signalAndScorecardsFromHeadings, ...signalAndScorecardsFromText];
  const seen = new Set<string>();
  const deduped: ParsedSection[] = [];
  for (const s of merged) {
    const key = `${s.title}\n${s.body}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(s);
  }
  const folded: ParsedSection[] = [];
  for (const s of deduped) {
    const t = s.title.toUpperCase();
    const prev = folded[folded.length - 1];
    if (
      prev
      && t.startsWith('RISK THROTTLE STABILITY CUBE SUMMARY')
      && prev.title.toUpperCase().startsWith('RISK THROTTLE STABILITY CUBE')
    ) {
      prev.body = `${prev.body}\n\n${s.title}\n${s.body}`.trim();
      continue;
    }
    if (
      prev
      && t.startsWith('EXIT ARCHITECTURE STABILITY CUBE SUMMARY')
      && prev.title.toUpperCase().startsWith('EXIT ARCHITECTURE STABILITY CUBE')
    ) {
      prev.body = `${prev.body}\n\n${s.title}\n${s.body}`.trim();
      continue;
    }
    folded.push(s);
  }
  const sortSections = (sections: ParsedSection[]) => sections
    .map((s, idx) => ({ s, idx, rank: fullReportOrderRank(s.title) }))
    .sort((a, b) => (a.rank - b.rank) || (a.idx - b.idx))
    .map((x) => x.s);

  if (folded.length > 0) return sortSections(folded);
  return sortSections(filterSectionsForSelectedFilter(all, selectedFilter));
}

function syntheticDateAt(index: number, total: number): Date {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - (total - 1 - index));
  return d;
}

function CurveCard({
  title,
  data,
  color,
  gradientId,
  height = 160,
  fillAbove = false,
  showMonthlyGridlines = false,
  showAthLine = false,
  showMovingAverage = false,
  movingAverageWindow = 20,
  valueFormatter,
  showTitle = true,
  annotateMin = false,
  annotationLabel = 'Min',
  baselineValue = null,
  compactCurrencyTicks = false,
  statsBar,
  backgroundColor = 'var(--bg0)',
  showBorder = true,
}: {
  title: string;
  data: Point[] | null | undefined;
  color: string;
  gradientId: string;
  height?: number;
  fillAbove?: boolean;
  showMonthlyGridlines?: boolean;
  showAthLine?: boolean;
  showMovingAverage?: boolean;
  movingAverageWindow?: number;
  valueFormatter?: (v: number) => string;
  showTitle?: boolean;
  annotateMin?: boolean;
  annotationLabel?: string;
  baselineValue?: number | null;
  compactCurrencyTicks?: boolean;
  statsBar?: Array<{ label: string; value: string; color: string }>;
  backgroundColor?: string;
  showBorder?: boolean;
}) {
  const rows = useMemo(() => {
    const src = data ?? [];
    return src
      .map((p, idx) => {
        const y = typeof p === 'number' ? p : p.y;
        if (!Number.isFinite(y)) return null;
        const d = typeof p === 'number'
          ? syntheticDateAt(idx, src.length)
          : (parseDateLike(p.x) ?? syntheticDateAt(idx, src.length));
        return {
          idx,
          ts: d.getTime(),
          date: d,
          y,
        };
      })
      .filter((r): r is { idx: number; ts: number; date: Date; y: number } => r !== null);
  }, [data]);
  const isDrawdownPanel = fillAbove || /drawdown/i.test(title);
  const withSeries = useMemo(() => {
    if (rows.length === 0) return [] as Array<{
      idx: number;
      ts: number;
      date: Date;
      y: number;
      dailyRetPct: number | null;
      ath?: number;
      ma?: number | null;
    }>;
    const out = rows.map((r, i) => {
      const prev = i > 0 ? rows[i - 1].y : null;
      const dailyRetPct = prev && prev !== 0 ? ((r.y / prev) - 1) * 100 : null;
      const ath = showAthLine ? Math.max(...rows.slice(0, i + 1).map((x) => x.y)) : undefined;
      return {
        ...r,
        dailyRetPct,
        ath,
        ma: null as number | null,
      };
    });
    if (showMovingAverage) {
      const w = Math.max(2, Math.floor(movingAverageWindow));
      let sum = 0;
      for (let i = 0; i < out.length; i += 1) {
        sum += out[i].y;
        if (i >= w) sum -= out[i - w].y;
        if (i >= w - 1) out[i].ma = sum / w;
      }
    }
    return out;
  }, [rows, showAthLine, showMovingAverage, movingAverageWindow]);
  const monthRefs = useMemo(() => {
    if (!showMonthlyGridlines || withSeries.length < 2) return [] as number[];
    const out: number[] = [];
    let prevKey = `${withSeries[0].date.getFullYear()}-${withSeries[0].date.getMonth()}`;
    for (let i = 1; i < withSeries.length; i += 1) {
      const key = `${withSeries[i].date.getFullYear()}-${withSeries[i].date.getMonth()}`;
      if (key !== prevKey) {
        out.push(withSeries[i].ts);
        prevKey = key;
      }
    }
    return out;
  }, [withSeries, showMonthlyGridlines]);
  const minPointInfo = useMemo(() => {
    if (!annotateMin || withSeries.length === 0) return null;
    let minIdx = 0;
    for (let i = 1; i < withSeries.length; i += 1) {
      if (withSeries[i].y < withSeries[minIdx].y) minIdx = i;
    }
    return withSeries[minIdx];
  }, [annotateMin, withSeries]);
  const valueFmt = valueFormatter ?? ((v: number) => v.toFixed(3));
  const yTickFmt = (v: number) => (compactCurrencyTicks ? fmtUsdCompact(v) : valueFmt(v));

  return (
    <div
      style={{
        background: backgroundColor,
        border: showBorder ? '1px solid var(--line)' : 'none',
        borderRadius: 3,
        padding: 12,
        flex: 1,
        minWidth: 0,
      }}
    >
      {statsBar && statsBar.length > 0 && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 12,
            marginBottom: 8,
            fontSize: 9,
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
            letterSpacing: '0.06em',
            textTransform: 'uppercase',
          }}
        >
          {statsBar.map((s) => (
            <span key={`s-${s.label}`} style={{ color: 'var(--t2)' }}>
              {s.label}: <span style={{ color: s.color, fontWeight: 700 }}>{s.value}</span>
            </span>
          ))}
        </div>
      )}
      {showTitle && (
        <div
          style={{
            fontSize: 9,
            textTransform: 'uppercase',
            letterSpacing: '0.12em',
            color: 'var(--t3)',
            fontWeight: 700,
            marginBottom: 8,
          }}
        >
          {title}
        </div>
      )}
      {withSeries.length > 1 ? (
        <div style={{ width: '100%', height }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={withSeries} margin={{ top: 6, right: 10, bottom: 12, left: 8 }}>
              <defs>
                <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={color} stopOpacity={isDrawdownPanel ? 0.188 : 0.125} />
                  <stop offset="100%" stopColor={color} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid stroke="var(--line)" strokeOpacity={0.2} vertical={false} />
              <XAxis
                dataKey="ts"
                type="number"
                domain={['dataMin', 'dataMax']}
                tickFormatter={(ts) => fmtDateLabel(new Date(Number(ts)))}
                tick={{ fill: 'var(--t2)', fontSize: 9, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}
                axisLine={{ stroke: 'var(--line)', strokeOpacity: 0.35 }}
                tickLine={{ stroke: 'var(--line)', strokeOpacity: 0.35 }}
              />
              <YAxis
                tickFormatter={(v) => yTickFmt(Number(v))}
                tick={{ fill: 'var(--t2)', fontSize: 9, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}
                axisLine={{ stroke: 'var(--line)', strokeOpacity: 0.35 }}
                tickLine={{ stroke: 'var(--line)', strokeOpacity: 0.35 }}
                width={54}
              />
              <Tooltip
                cursor={{ stroke: 'var(--line2)', strokeDasharray: '2 2', strokeOpacity: 0.4 }}
                content={({ active, payload }) => {
                  if (!active || !payload || payload.length === 0) return null;
                  const row = payload[0]?.payload as { date?: Date; y?: number; dailyRetPct?: number | null } | undefined;
                  if (!row || typeof row.y !== 'number') return null;
                  return (
                    <div
                      style={{
                        background: '#141416',
                        border: '1px solid #242428',
                        borderRadius: 3,
                        color: 'var(--t0)',
                        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                        fontSize: 10,
                        padding: '6px 8px',
                        lineHeight: 1.45,
                      }}
                    >
                      <div>{row.date ? fmtDateLong(new Date(row.date)) : ''}</div>
                      <div>{title}: {valueFmt(row.y)}</div>
                      <div>Daily Return %: {row.dailyRetPct !== null && row.dailyRetPct !== undefined ? fmtPercent2(row.dailyRetPct) : 'N/A'}</div>
                    </div>
                  );
                }}
              />
              {monthRefs.map((ts, i) => (
                <ReferenceLine
                  key={`month-ref-${i}`}
                  x={ts}
                  stroke="var(--line)"
                  strokeOpacity={0.35}
                  strokeDasharray="2 3"
                />
              ))}
              {baselineValue !== null && Number.isFinite(baselineValue) && (
                <ReferenceLine
                  y={baselineValue}
                  stroke="var(--line2)"
                  strokeDasharray="4 4"
                  strokeOpacity={0.8}
                />
              )}
              <Area
                type="monotone"
                dataKey="y"
                stroke={color}
                strokeWidth={1.5}
                fill={`url(#${gradientId})`}
                dot={false}
                isAnimationActive={false}
              />
              {!isDrawdownPanel && showAthLine && (
                <Line
                  type="monotone"
                  dataKey="ath"
                  stroke="rgba(255,255,255,0.45)"
                  strokeDasharray="3 3"
                  strokeWidth={1}
                  dot={false}
                  isAnimationActive={false}
                />
              )}
              {!isDrawdownPanel && showMovingAverage && (
                <Line
                  type="monotone"
                  dataKey="ma"
                  stroke="rgba(255,186,77,0.92)"
                  strokeDasharray="4 2"
                  strokeWidth={1.1}
                  dot={false}
                  connectNulls
                  isAnimationActive={false}
                />
              )}
              {minPointInfo && (
                <ReferenceDot
                  x={minPointInfo.ts}
                  y={minPointInfo.y}
                  r={2.5}
                  fill="#ff4d4d"
                  stroke="var(--bg0)"
                  strokeWidth={1}
                  label={{
                    value: `${annotationLabel}: ${valueFmt(minPointInfo.y)}`,
                    fill: 'var(--t2)',
                    fontSize: 9,
                    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                    position: 'top',
                  }}
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <span style={{ fontSize: 10, color: 'var(--t3)' }}>No chart data</span>
        </div>
      )}
    </div>
  );
}

type FilterRow = Record<string, unknown> & {
  filter?: string;
  not_run?: boolean;
  sharpe?: number | null;
  cagr?: number | null;
  max_dd?: number | null;
  active?: number | null;
  wf_cv?: number | null;
  cv?: number | null;
  dsr_pct?: number | null;
  tot_ret?: number | null;
  grade?: string | null;
  grade_score?: number | null;
  equity_curve?: Point[];
  drawdown_curve?: Point[];
};
type ReportTab = 'summary' | 'breakdown' | 'stress_tests' | 'raw_output' | 'tear_sheet' | 'full_report';

function asNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

export default function ResultsView({ results, jobId, startingCapital, params }: ResultsViewProps) {
  const m = useMemo(() => ((results?.metrics ?? {}) as Record<string, unknown>), [results]);
  const equityCurve = (m.equity_curve ?? results?.equity_curve) as Point[] | null | undefined;
  const drawdownCurve = (m.drawdown_curve ?? results?.drawdown_curve) as Point[] | null | undefined;
  const filterComparison = (m.filter_comparison ?? results?.filter_comparison) as FilterRow[] | null | undefined;
  const perFilterMetrics = (m.filters ?? []) as FilterRow[];
  const hints = (m.hints ?? results?.hints) as string[] | null | undefined;
  const filterComparisonWithExpected: FilterRow[] = [...(filterComparison ?? [])];
  if (params) {
    const expected: Array<{ key: string; label: string; aliases: string[] }> = [
      { key: 'run_filter_none', label: 'A - No Filter', aliases: ['A - No Filter'] },
      { key: 'run_filter_calendar', label: 'A - Calendar Filter', aliases: ['A - Calendar Filter'] },
      { key: 'run_filter_tail', label: 'A - Tail Guardrail', aliases: ['A - Tail Guardrail'] },
      { key: 'run_filter_dispersion', label: 'A - Dispersion', aliases: ['A - Dispersion'] },
      { key: 'run_filter_tail_disp', label: 'A - Tail + Dispersion', aliases: ['A - Tail + Dispersion', 'A - Tail p Dispersion'] },
      { key: 'run_filter_vol', label: 'A - Volatility', aliases: ['A - Volatility'] },
      { key: 'run_filter_tail_disp_vol', label: 'A - Tail + Disp + Vol', aliases: ['A - Tail + Disp + Vol', 'A - Tail p Disp p Vol'] },
      { key: 'run_filter_tail_or_vol', label: 'A - Tail + Vol (OR)', aliases: ['A - Tail + Vol (OR)', 'A - Tail p Vol (OR'] },
      { key: 'run_filter_tail_and_vol', label: 'A - Tail + Vol (AND)', aliases: ['A - Tail + Vol (AND)', 'A - Tail p Vol (AND'] },
      { key: 'run_filter_tail_blofin', label: 'A - Tail + Blofin', aliases: ['A - Tail + Blofin', 'A - Tail p Blofin'] },
    ];
    const existing = new Set(
      filterComparisonWithExpected.map((r) => normalizeFilterLabel(String(r.filter ?? '').trim())),
    );
    for (const entry of expected) {
      if (!params[entry.key]) continue;
      const hasAlias = entry.aliases.some((alias) => existing.has(normalizeFilterLabel(alias)));
      if (!hasAlias) {
        filterComparisonWithExpected.push({ filter: entry.label, not_run: true });
      }
    }
  }
  const dispersionRequested = !!params?.run_filter_dispersion || !!params?.run_filter_tail_disp || !!params?.run_filter_tail_disp_vol;
  const dispersionMissing =
    filterComparisonWithExpected.some((r) => r.filter === 'A - Dispersion' && r.not_run)
    || filterComparisonWithExpected.some((r) => r.filter === 'A - Tail + Dispersion' && r.not_run)
    || filterComparisonWithExpected.some((r) => r.filter === 'A - Tail + Disp + Vol' && r.not_run);
  const showDispersionVpnHint = dispersionRequested && dispersionMissing;
  const runStartingCapital =
    typeof results?.starting_capital === 'number'
      ? results.starting_capital
      : typeof startingCapital === 'number'
        ? startingCapital
        : 100000;

  const mergedFilters: FilterRow[] = (() => {
    const byLabel = new Map<string, FilterRow>();
    for (const pf of perFilterMetrics) {
      const key = normalizeFilterLabel(String(pf.filter ?? ''));
      if (key) byLabel.set(key, pf);
    }
    return filterComparisonWithExpected.map((row) => {
      const key = normalizeFilterLabel(String(row.filter ?? ''));
      const pf = byLabel.get(key);
      if (!pf) return row;
      return { ...row, ...pf, filter: String(pf.filter ?? row.filter ?? '') };
    });
  })();

  const [manualSelectedFilter, setManualSelectedFilter] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<ReportTab>('summary');
  const [auditOutput, setAuditOutput] = useState<string>('');
  const [outputLoading, setOutputLoading] = useState(false);
  const [outputError, setOutputError] = useState<string | null>(null);
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'error'>('idle');
  const [tearTemplate, setTearTemplate] = useState('');
  const [calendarHover, setCalendarHover] = useState<{
    x: number;
    y: number;
    text: string;
  } | null>(null);
  const [calendarRowHoverKey, setCalendarRowHoverKey] = useState<string | null>(null);
  const [calendarViewMode, setCalendarViewMode] = useState<'grid' | 'chart'>('grid');
  const [openFullReportCategories, setOpenFullReportCategories] = useState<Record<FullReportCategoryKey, boolean>>(() => (
    FULL_REPORT_CATEGORIES.reduce((acc, cat) => {
      acc[cat.key] = cat.defaultOpen;
      return acc;
    }, {} as Record<FullReportCategoryKey, boolean>)
  ));
  const [openFullReportSectionKeys, setOpenFullReportSectionKeys] = useState<Record<string, boolean>>({});
  const filterComparisonRef = useRef<HTMLDetailsElement | null>(null);
  const monthlyHeatmapRailRef = useRef<HTMLDivElement | null>(null);
  const defaultSelectedFilter = (() => {
    if (mergedFilters.length === 0) return null;
    const candidates = mergedFilters.filter((r) => !r.not_run);
    const ranked = [...(candidates.length > 0 ? candidates : mergedFilters)].sort(
      (a, b) => (asNum(b.sharpe) ?? Number.NEGATIVE_INFINITY) - (asNum(a.sharpe) ?? Number.NEGATIVE_INFINITY),
    );
    return String(ranked[0]?.filter ?? mergedFilters[0]?.filter ?? '');
  })();

  const selectedFilter =
    manualSelectedFilter
      && mergedFilters.some((r) => normalizeFilterLabel(String(r.filter ?? '')) === normalizeFilterLabel(manualSelectedFilter))
      ? manualSelectedFilter
      : defaultSelectedFilter;
  const selectedRow =
    selectedFilter
      ? mergedFilters.find((r) => normalizeFilterLabel(String(r.filter ?? '')) === normalizeFilterLabel(selectedFilter)) ?? null
      : null;
  const alertLines = useMemo(() => extractAlerts(auditOutput), [auditOutput]);
  const advancedSections = useMemo(
    () => extractSelectedFilterAdvancedSections(auditOutput, selectedFilter),
    [auditOutput, selectedFilter],
  );
  const fullReportSectionsSelected = useMemo(() => {
    return buildFullReportSections(auditOutput, selectedFilter);
  }, [auditOutput, selectedFilter]);
  const runSummarySection = useMemo(() => extractRunSummarySection(auditOutput), [auditOutput]);
  const bestFilterHeadlineSection = useMemo<ParsedSection | null>(() => {
    const sharpe = selectedRow?.sharpe ?? m.sharpe;
    const cagr = selectedRow?.cagr ?? m.cagr;
    const maxDd = selectedRow?.max_dd ?? m.max_drawdown;
    const dsr = selectedRow?.dsr_pct ?? m.dsr_pct;
    const grade = selectedRow?.grade_score ?? selectedRow?.grade ?? m.grade_score;
    const body = [
      `Filter: ${selectedFilter ?? 'N/A'}`,
      `Sharpe: ${fmtMetric(sharpe)}`,
      `CAGR %: ${fmtPercent2(cagr)}`,
      `Max DD %: ${fmtPercent2(maxDd)}`,
      `DSR %: ${fmtPercent2(dsr)}`,
      `Grade: ${grade ?? 'N/A'}`,
    ].join('\n');
    return { title: 'BEST FILTER HEADLINE STATS', body };
  }, [selectedRow, selectedFilter, m]);
  const fullReportSections = useMemo(() => {
    const withExecutive = [
      ...(runSummarySection ? [runSummarySection] : []),
      ...(bestFilterHeadlineSection ? [bestFilterHeadlineSection] : []),
      ...fullReportSectionsSelected,
    ];
    const seen = new Set<string>();
    const out: ParsedSection[] = [];
    for (const s of withExecutive) {
      const key = `${s.title}\n${s.body}`;
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(s);
    }
    return out;
  }, [
    fullReportSectionsSelected,
    runSummarySection,
    bestFilterHeadlineSection,
  ]);
  const fullReportCategoryGroups = useMemo(() => {
    const byKey = FULL_REPORT_CATEGORIES.reduce((acc, cat) => {
      acc[cat.key] = [];
      return acc;
    }, {} as Record<FullReportCategoryKey, ParsedSection[]>);
    for (const section of fullReportSections) {
      const cat = fullReportCategoryForTitle(section.title);
      byKey[cat].push(section);
    }
    return FULL_REPORT_CATEGORIES
      .map((cat) => ({ ...cat, sections: byKey[cat.key] }))
      .filter((cat) => cat.sections.length > 0);
  }, [fullReportSections]);
  const fullReportSectionCount = useMemo(
    () => fullReportCategoryGroups.reduce((acc, cat) => acc + cat.sections.length, 0),
    [fullReportCategoryGroups],
  );
  const fullReportKpis = useMemo(() => ([
    { label: 'Sharpe', key: 'sharpe', value: fmtMetric(selectedRow?.sharpe ?? m.sharpe), colorValue: selectedRow?.sharpe ?? m.sharpe },
    { label: 'CAGR %', key: 'cagr', value: fmtPercent2(selectedRow?.cagr ?? m.cagr), colorValue: selectedRow?.cagr ?? m.cagr },
    { label: 'Max DD %', key: 'max_dd', value: fmtPercent2(selectedRow?.max_dd ?? m.max_drawdown), colorValue: selectedRow?.max_dd ?? m.max_drawdown },
    { label: 'DSR %', key: 'dsr_pct', value: fmtPercent2(selectedRow?.dsr_pct ?? m.dsr_pct), colorValue: selectedRow?.dsr_pct ?? m.dsr_pct },
    {
      label: 'Grade',
      key: 'grade',
      value: String((selectedRow?.grade_score ?? selectedRow?.grade ?? m.grade_score) ?? 'N/A'),
      colorValue: selectedRow?.grade_score ?? m.grade_score,
    },
  ]), [selectedRow, m]);

  useEffect(() => {
    setOpenFullReportSectionKeys((prev) => {
      const next: Record<string, boolean> = {};
      for (const cat of fullReportCategoryGroups) {
        for (let idx = 0; idx < cat.sections.length; idx += 1) {
          const key = `${cat.key}-${idx}-${cat.sections[idx].title}`;
          next[key] = prev[key] ?? false;
        }
      }
      const prevKeys = Object.keys(prev);
      const nextKeys = Object.keys(next);
      if (prevKeys.length !== nextKeys.length) return next;
      for (const k of nextKeys) {
        if (prev[k] !== next[k]) return next;
      }
      return prev;
    });
  }, [fullReportCategoryGroups]);

  const selectedEquityCurve = ((selectedRow?.equity_curve as Point[] | undefined) ?? equityCurve) as Point[] | null | undefined;
  const selectedDrawdownCurve = ((selectedRow?.drawdown_curve as Point[] | undefined) ?? drawdownCurve) as Point[] | null | undefined;
  const selectedTotalDays = selectedEquityCurve?.length ?? 0;
  const selectedActiveDays = asNum(selectedRow?.active);
  const scorecard = useMemo(
    () => ((results?.scorecard ?? []) as Array<{ label?: string; status?: string; value?: unknown }>),
    [results],
  );
  const activeDaysMain = (
    selectedActiveDays !== null && Number.isFinite(selectedActiveDays)
  ) ? String(Math.round(selectedActiveDays)) : fmtMetric(selectedRow?.active, true);
  const activeDaysSuffix = (
    selectedActiveDays !== null
    && Number.isFinite(selectedActiveDays)
    && selectedTotalDays > 0
  ) ? `/${selectedTotalDays} days` : undefined;
  const selectedActivePct = (
    selectedActiveDays !== null
    && Number.isFinite(selectedActiveDays)
    && selectedTotalDays > 0
  ) ? (selectedActiveDays / selectedTotalDays) * 100 : null;
  const equityCurveDollars = selectedEquityCurve?.map((p) => (
    typeof p === 'number'
      ? p * runStartingCapital
      : { ...p, y: p.y * runStartingCapital }
  ));
  const equitySeriesDated = useMemo(() => {
    const src = equityCurveDollars ?? [];
    return src.map((p, idx) => {
      if (typeof p === 'number') return { d: syntheticDateAt(idx, src.length), y: p };
      return { d: parseDateLike(p.x) ?? syntheticDateAt(idx, src.length), y: p.y };
    }).filter((p) => Number.isFinite(p.y));
  }, [equityCurveDollars]);
  const returnProfile = useMemo(() => {
    if (equitySeriesDated.length < 2) {
      return {
        daily: [] as number[],
        weekly: [] as number[],
        monthly: [] as number[],
        quarterly: [] as number[],
      };
    }
    const daily: number[] = [];
    for (let i = 1; i < equitySeriesDated.length; i += 1) {
      const prev = equitySeriesDated[i - 1].y;
      const cur = equitySeriesDated[i].y;
      if (prev > 0 && Number.isFinite(cur)) {
        const r = ((cur / prev) - 1) * 100;
        // Daily profile uses active-day returns so the quartiles are informative.
        if (Math.abs(r) > 1e-12) daily.push(r);
      }
    }
    const weekly = periodReturnsFromEquity(equitySeriesDated, weekStartKey);
    const monthly = periodReturnsFromEquity(
      equitySeriesDated,
      (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`,
    );
    const quarterly = periodReturnsFromEquity(
      equitySeriesDated,
      (d) => `${d.getFullYear()}-Q${Math.floor(d.getMonth() / 3) + 1}`,
    );
    return { daily, weekly, monthly, quarterly };
  }, [equitySeriesDated]);
  const derivedSummaryStats = useMemo(() => {
    const daily = returnProfile.daily.filter((v) => Number.isFinite(v));
    const wins = daily.filter((v) => v > 0);
    const losses = daily.filter((v) => v < 0);
    const avgWin = wins.length ? wins.reduce((a, b) => a + b, 0) / wins.length : null;
    const avgLoss = losses.length ? losses.reduce((a, b) => a + b, 0) / losses.length : null;
    const avgWinLoss = (
      avgWin !== null
      && avgLoss !== null
      && avgLoss < 0
    ) ? (avgWin / Math.abs(avgLoss)) : null;
    const profitFactor = (
      wins.length > 0
      && losses.length > 0
    ) ? (wins.reduce((a, b) => a + b, 0) / Math.abs(losses.reduce((a, b) => a + b, 0))) : null;
    const avg1M = returnProfile.monthly.length
      ? returnProfile.monthly.reduce((a, b) => a + b, 0) / returnProfile.monthly.length
      : null;
    const drawdownValues = (Array.isArray(selectedDrawdownCurve) ? selectedDrawdownCurve : [])
      .map((p) => (typeof p === 'number' ? p : p.y))
      .filter((v) => Number.isFinite(v))
      .map((v) => normalizeDrawdownDecimal(v));
    let longestUnderwaterStreak = 0;
    let streak = 0;
    for (const dd of drawdownValues) {
      if (dd < 0) {
        streak += 1;
        if (streak > longestUnderwaterStreak) longestUnderwaterStreak = streak;
      } else {
        streak = 0;
      }
    }
    return { avgWinLoss, profitFactor, avg1M, longestUnderwaterStreak };
  }, [returnProfile.daily, returnProfile.monthly, selectedDrawdownCurve]);
  const monthlyHeatmap = useMemo(() => {
    const monthMap = new Map<string, { label: string; first: number; last: number; d: Date }>();
    for (const p of equitySeriesDated) {
      const y = p.d.getFullYear();
      const m = p.d.getMonth();
      const key = `${y}-${String(m + 1).padStart(2, '0')}`;
      if (!monthMap.has(key)) {
        monthMap.set(key, {
          label: p.d.toLocaleDateString(undefined, { month: 'short', year: '2-digit' }).replace(' ', " '"),
          first: p.y,
          last: p.y,
          d: new Date(y, m, 1),
        });
      }
      const row = monthMap.get(key)!;
      row.last = p.y;
    }
    const rows = Array.from(monthMap.values())
      .sort((a, b) => a.d.getTime() - b.d.getTime())
      .map((r) => ({
        label: r.label.toUpperCase(),
        pct: r.first > 0 ? ((r.last / r.first) - 1) * 100 : 0,
      }));
    const absVals = rows.map((r) => Math.abs(r.pct)).filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
    const scale = absVals.length > 0 ? Math.max(1, percentile(absVals, 0.95)) : 1;
    const positive = rows.filter((r) => r.pct > 0).length;
    const negative = rows.filter((r) => r.pct < 0).length;
    const winRate = rows.length > 0 ? (positive / rows.length) * 100 : 0;
    const mean = rows.length > 0 ? (rows.reduce((a, b) => a + b.pct, 0) / rows.length) : 0;
    return { rows, scale, positive, negative, winRate, mean };
  }, [equitySeriesDated]);
  const returnProfileStats = useMemo(() => ({
    daily: computeBoxStats(returnProfile.daily),
    weekly: computeBoxStats(returnProfile.weekly),
    monthly: computeBoxStats(returnProfile.monthly),
    quarterly: computeBoxStats(returnProfile.quarterly),
  }), [returnProfile]);
  const dailyVolatilityPct = useMemo(() => {
    const vals = returnProfile.daily.filter((v) => Number.isFinite(v));
    if (vals.length < 2) return null;
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const variance = vals.reduce((a, b) => a + ((b - mean) ** 2), 0) / (vals.length - 1);
    return Math.sqrt(Math.max(0, variance));
  }, [returnProfile.daily]);
  const calendarMonths = useMemo(() => buildCalendarMonths(equitySeriesDated), [equitySeriesDated]);
  const calendarScale = useMemo(() => {
    const vals = calendarMonths
      .flatMap((mth) => mth.cells)
      .map((c) => c.ret)
      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
      .map((v) => Math.abs(v))
      .sort((a, b) => a - b);
    if (vals.length === 0) return 1;
    return Math.max(1, percentile(vals, 0.95));
  }, [calendarMonths]);
  const statisticalCharts = useMemo(() => {
    const source = returnProfile.daily;
    const sorted = [...source].sort((a, b) => a - b);
    const cdf = computeCdfPoints(source);
    const eqf = computeEqfPoints(source);
    const pdf = computePdfPoints(source);
    const qq = (() => {
      if (sorted.length < 2) return [] as XYPoint[];
      const mean = sorted.reduce((a, b) => a + b, 0) / sorted.length;
      const variance = sorted.reduce((a, b) => a + (b - mean) ** 2, 0) / sorted.length;
      const sigma = Math.sqrt(Math.max(variance, 1e-12));
      const sampled = sampleSorted(sorted);
      return sampled.map((obs, i) => {
        const q = (i + 0.5) / sampled.length;
        const z = inverseNormalCdf(q);
        const theo = mean + (sigma * z);
        return { x: theo, y: obs };
      });
    })();
    const min = sorted.length ? sorted[0] : NaN;
    const max = sorted.length ? sorted[sorted.length - 1] : NaN;
    const median = sorted.length ? percentile(sorted, 0.5) : NaN;
    const p10 = sorted.length ? percentile(sorted, 0.1) : NaN;
    const p90 = sorted.length ? percentile(sorted, 0.9) : NaN;
    const percentileMarkers = [0.01, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
      .map((q) => ({
        q,
        value: sorted.length ? percentile(sorted, q) : NaN,
      }))
      .filter((m) => Number.isFinite(m.value));
    return { cdf, eqf, pdf, qq, min, max, median, p10, p90, percentileMarkers, n: source.length };
  }, [returnProfile.daily]);

  useEffect(() => {
    let mounted = true;
    fetch('/tear_sheet12.html')
      .then((r) => (r.ok ? r.text() : ''))
      .then((txt) => {
        if (mounted) setTearTemplate(txt);
      })
      .catch(() => {
        if (mounted) setTearTemplate('');
      });
    return () => {
      mounted = false;
    };
  }, []);

  const tearSheetHtml = useMemo(() => {
    if (!tearTemplate) return '';
    const eq = (equityCurveDollars ?? []).map((p, idx) => {
      if (typeof p === 'number') return { d: syntheticDateAt(idx, (equityCurveDollars ?? []).length), y: p };
      return { d: parseDateLike(p.x) ?? syntheticDateAt(idx, (equityCurveDollars ?? []).length), y: p.y };
    }).filter((p) => Number.isFinite(p.y));
    if (eq.length < 2) return tearTemplate;

    const first = eq[0];
    const last = eq[eq.length - 1];
    const days = eq.length;
    const totRet = ((last.y / first.y) - 1) * 100;
    const cagr = asPct(selectedRow?.cagr ?? m.cagr) ?? 0;
    const sharpe = asNum(selectedRow?.sharpe ?? m.sharpe) ?? 0;
    const sortino = asNum(m.sortino) ?? 0;
    const calmar = asNum(m.calmar ?? m.calmar_ratio) ?? 0;
    const maxDd = asPct(selectedRow?.max_dd ?? m.max_drawdown) ?? 0;
    const dsr = asPct(selectedRow?.dsr_pct ?? m.dsr_pct) ?? 0;
    const cv = asNum(selectedRow?.wf_cv ?? selectedRow?.cv ?? m.cv) ?? 0;
    const r2 = asNum(m.r2 ?? m.r_squared ?? m.equity_r2);
    const grade = asNum(selectedRow?.grade_score ?? m.grade_score ?? selectedRow?.grade);
    const activeDays = Math.round(asNum(selectedRow?.active) ?? 0);
    const dailyRets: number[] = [];
    for (let i = 1; i < eq.length; i += 1) {
      dailyRets.push((eq[i].y / eq[i - 1].y) - 1);
    }
    const dailySorted = [...dailyRets].sort((a, b) => a - b);
    const bestDay = dailyRets.length > 0 ? Math.max(...dailyRets) * 100 : 0;
    const worstDay = dailyRets.length > 0 ? Math.min(...dailyRets) * 100 : 0;
    const avgDaily = dailyRets.length > 0 ? (dailyRets.reduce((a, b) => a + b, 0) / dailyRets.length) * 100 : 0;
    const equityMultiplier = first.y > 0 ? last.y / first.y : 0;
    const omega = asNum(m.omega);
    const ulcer = asNum(m.ulcer_index);
    const q05 = dailySorted.length > 0 ? percentile(dailySorted, 0.05) : NaN;
    const q95 = dailySorted.length > 0 ? percentile(dailySorted, 0.95) : NaN;
    const cvar5 = dailySorted.length > 0 ? dailySorted.slice(0, Math.max(1, Math.floor(dailySorted.length * 0.05))).reduce((a, b) => a + b, 0) / Math.max(1, Math.floor(dailySorted.length * 0.05)) : NaN;
    const cvar1 = dailySorted.length > 0 ? dailySorted.slice(0, Math.max(1, Math.floor(dailySorted.length * 0.01))).reduce((a, b) => a + b, 0) / Math.max(1, Math.floor(dailySorted.length * 0.01)) : NaN;
    const tailRatio = Number.isFinite(q95) && Number.isFinite(q05) && q05 !== 0 ? q95 / Math.abs(q05) : NaN;
    const mean = dailyRets.length > 0 ? dailyRets.reduce((a, b) => a + b, 0) / dailyRets.length : 0;
    const variance = dailyRets.length > 0 ? dailyRets.reduce((a, b) => a + (b - mean) ** 2, 0) / dailyRets.length : 0;
    const sigma = Math.sqrt(Math.max(variance, 0));
    const skew = sigma > 0 && dailyRets.length > 0 ? dailyRets.reduce((a, b) => a + ((b - mean) / sigma) ** 3, 0) / dailyRets.length : NaN;
    const kurt = sigma > 0 && dailyRets.length > 0 ? (dailyRets.reduce((a, b) => a + ((b - mean) / sigma) ** 4, 0) / dailyRets.length) - 3 : NaN;

    const monthlyMap = new Map<string, { d: Date; first: number; last: number }>();
    for (const p of eq) {
      const key = `${p.d.getFullYear()}-${String(p.d.getMonth() + 1).padStart(2, '0')}`;
      if (!monthlyMap.has(key)) monthlyMap.set(key, { d: new Date(p.d), first: p.y, last: p.y });
      const cur = monthlyMap.get(key)!;
      cur.last = p.y;
      cur.d = new Date(p.d);
    }
    const monthlyRows = Array.from(monthlyMap.values()).map((v) => ({
      label: v.d.toLocaleDateString(undefined, { month: 'short', year: '2-digit' }).replace(' ', " '"),
      pct: ((v.last / v.first) - 1) * 100,
    }));
    const monthlyWinRate = monthlyRows.length > 0 ? (monthlyRows.filter((r) => r.pct > 0).length / monthlyRows.length) * 100 : 0;
    const avgMonthly = monthlyRows.length > 0 ? monthlyRows.reduce((a, b) => a + b.pct, 0) / monthlyRows.length : 0;
    const bestMonth = monthlyRows.length > 0 ? Math.max(...monthlyRows.map((r) => r.pct)) : NaN;
    const worstMonth = monthlyRows.length > 0 ? Math.min(...monthlyRows.map((r) => r.pct)) : NaN;

    const sampleN = Math.min(58, eq.length);
    const weeklyData = Array.from({ length: sampleN }).map((_, i) => {
      const idx = Math.round((i / Math.max(1, sampleN - 1)) * (eq.length - 1));
      return { date: fmtDateShortYear(eq[idx].d), bal: Math.round(eq[idx].y) };
    });
    const rawDrawdownSeries = Array.isArray(selectedDrawdownCurve)
      ? selectedDrawdownCurve
        .map((p) => {
          if (typeof p === 'number') return p;
          if (p && typeof p === 'object') {
            const y = (p as { y?: unknown }).y;
            if (typeof y === 'number' && Number.isFinite(y)) return y;
            if (typeof y === 'string') {
              const n = Number(y);
              return Number.isFinite(n) ? n : null;
            }
          }
          return null;
        })
        .filter((n): n is number => typeof n === 'number' && Number.isFinite(n))
      : [];
    const expectedMaxDdPct = asNum(selectedRow?.max_dd ?? m.max_drawdown);
    const drawdownWeekly = rawDrawdownSeries.length >= 2
      ? normalizeDrawdownSeries(sampleMinSeriesToLength(rawDrawdownSeries, sampleN), expectedMaxDdPct)
      : [];
    const drawdownFull = rawDrawdownSeries.length >= 2
      ? normalizeDrawdownSeries(rawDrawdownSeries, expectedMaxDdPct)
      : (() => {
        let peak = eq[0]?.y ?? 1;
        return eq.map((p) => {
          if (p.y > peak) peak = p.y;
          return peak > 0 ? ((p.y - peak) / peak) : 0;
        });
      })();
    const avgDrawdown = drawdownFull.filter((d) => d < 0).reduce((a, b, _, arr) => a + (b / Math.max(1, arr.length)), 0) * 100;
    const timeUnderwaterPct = drawdownFull.length > 0 ? (drawdownFull.filter((d) => d < 0).length / drawdownFull.length) * 100 : 0;
    const episodes: number[] = [];
    let inEp = false;
    let curMin = 0;
    let curLen = 0;
    let longestDdDuration = 0;
    for (const d of drawdownFull) {
      if (d < 0) {
        if (!inEp) {
          inEp = true;
          curMin = d;
          curLen = 1;
        } else {
          curLen += 1;
          if (d < curMin) curMin = d;
        }
        if (curLen > longestDdDuration) longestDdDuration = curLen;
      } else if (inEp) {
        episodes.push(curMin);
        inEp = false;
        curLen = 0;
      }
    }
    if (inEp) episodes.push(curMin);
    const avgMaxDdEpisode = episodes.length > 0 ? (episodes.reduce((a, b) => a + b, 0) / episodes.length) * 100 : NaN;
    const weeklyReturns = weeklyData.slice(1).map((w, i) => {
      const prev = weeklyData[i].bal;
      return prev > 0 ? (w.bal / prev) - 1 : 0;
    });
    const weeklyWinRate = weeklyReturns.length > 0 ? (weeklyReturns.filter((r) => r > 0).length / weeklyReturns.length) * 100 : 0;
    const activeDaily = dailyRets.filter((r) => Math.abs(r) > 1e-12);
    const activeDayWinRate = activeDaily.length > 0 ? (activeDaily.filter((r) => r > 0).length / activeDaily.length) * 100 : 0;
    const avgWin = activeDaily.filter((r) => r > 0);
    const avgLoss = activeDaily.filter((r) => r < 0);
    const avgWinPct = avgWin.length > 0 ? (avgWin.reduce((a, b) => a + b, 0) / avgWin.length) * 100 : NaN;
    const avgLossPct = avgLoss.length > 0 ? (avgLoss.reduce((a, b) => a + b, 0) / avgLoss.length) * 100 : NaN;
    const winLossRatio = Number.isFinite(avgWinPct) && Number.isFinite(avgLossPct) && avgLossPct !== 0 ? Math.abs(avgWinPct / avgLossPct) : NaN;
    let maxConsecLosses = 0;
    let curLossStreak = 0;
    for (const r of activeDaily) {
      if (r < 0) {
        curLossStreak += 1;
        if (curLossStreak > maxConsecLosses) maxConsecLosses = curLossStreak;
      } else {
        curLossStreak = 0;
      }
    }
    const grossPos = dailyRets.filter((r) => r > 0).reduce((a, b) => a + b, 0);
    const grossNeg = Math.abs(dailyRets.filter((r) => r < 0).reduce((a, b) => a + b, 0));
    const profitFactor = grossNeg > 0 ? grossPos / grossNeg : NaN;
    const gainToPain = grossNeg > 0 ? (grossPos - grossNeg) / grossNeg : NaN;
    const textOutput = auditOutput ?? '';
    const pickActual = (labelRx: RegExp): string | null => {
      const lines = textOutput.split('\n').filter((ln) => labelRx.test(ln));
      if (lines.length === 0) return null;
      const line = lines[lines.length - 1];
      const mActual = line.match(/\s{2,}([^\s][^✅❌⚠─]*)\s+(?:✅|❌|⚠|──)/);
      return mActual ? mActual[1].trim() : null;
    };
    const totalFeeDragText = (
      textOutput.match(/Total Fee Drag %[\s\S]{0,40}?([0-9]+(?:\.[0-9]+)?%)/i)?.[1]
      ?? null
    );
    const turnoverText = (
      textOutput.match(/Turnover[\s\S]{0,30}?([0-9]+\/yr)/i)?.[1]
      ?? null
    );
    const institutionalCap = (
      textOutput.match(/Institutional\s*\(Sharpe\s*[≥>=]\s*2\.0\)\s*:\s*capacity up to\s*\$([0-9,]+)/i)?.[1]
      ?? null
    );
    const takerFeePct = asNum(params?.taker_fee_pct);
    const takerFeeSideText = takerFeePct !== null
      ? `${(takerFeePct * 100).toFixed(3)}%`
      : null;
    const ruinProbText = pickActual(/Ruin Prob\s*\(50%\s*DD,\s*365d\)\s*%/i);
    const eqR2Text = pickActual(/Equity Curve R[²^]2?/i);
    const isOosCagrRatioText = pickActual(/IS\/?OOS\s+CAGR\s+Ratio/i);
    const oosSharpeText = pickActual(/^.*\bOOS Sharpe\b.*$/i);
    const sharpeDecayText = pickActual(/Sharpe Decay/i);
    const positiveWfFoldsText = pickActual(/Positive WF Folds/i);
    const faWfMeanDsrText = pickActual(/FA-WF Mean DSR/i);
    const sharpe2xSlipText = pickActual(/Sharpe @2(?:x|×)\s+Slippage/i);
    const slippageSensitivityText = (() => {
      const explicit = pickActual(/Slippage Sensitivity/i);
      if (explicit) return explicit;
      const base = sharpe;
      const slip = sharpe2xSlipText ? Number(sharpe2xSlipText.replace(/[^0-9.\-]/g, '')) : NaN;
      if (!Number.isFinite(base) || !Number.isFinite(slip) || base === 0) return null;
      const drop = ((base - slip) / Math.abs(base)) * 100;
      return `LOW (-${Math.abs(drop).toFixed(2)}%)`;
    })();
    const scorecard92 = (() => {
      const m2 = textOutput.match(/✅\s*(\d+)\s*Pass\s+❌\s*(\d+)\s*Fail\s+⚠\s*(\d+)\s*Borderline\s+──\s*(\d+)\s*N\/A\s+\(of\s+92\s+metrics\)/i);
      if (!m2) return null;
      return {
        pass: Number(m2[1]),
        fail: Number(m2[2]),
        warn: Number(m2[3]),
        na: Number(m2[4]),
        total: 92,
      };
    })();
    const fallbackBtcUsdWeekly = [
      100000, 98462, 92308, 85128, 86667, 84103, 82051, 81026,
      84103, 88205, 92308, 97436, 101026, 104615, 106667, 109231,
      105641, 110769, 114872, 117949, 121026, 118462, 123077, 118974,
      123590, 126154, 129231, 126667, 122051, 115897, 112821, 109744,
      106667, 102564, 99487, 101538, 98462, 95385, 91795, 89231,
      86154, 89231, 86154, 86667, 85128, 82051, 81026, 78974,
      76923, 75897, 80744, 78974, 77436, 75385, 73846, 71795,
      69744, 68718,
    ];
    const btcMetricCandidates: unknown[] = [
      (m as Record<string, unknown>).btc_weekly_usd,
      (m as Record<string, unknown>).btc_usd_weekly,
      (m as Record<string, unknown>).btc_curve_usd,
      (m as Record<string, unknown>).btc_curve,
      (m as Record<string, unknown>).btc_weekly,
    ];
    const rawBtcSeries = (() => {
      for (const candidate of btcMetricCandidates) {
        if (!Array.isArray(candidate)) continue;
        const parsed = candidate
          .map((v) => {
            if (typeof v === 'number' && Number.isFinite(v)) return v;
            if (v && typeof v === 'object') {
              const y = (v as { y?: unknown }).y;
              if (typeof y === 'number' && Number.isFinite(y)) return y;
              if (typeof y === 'string') {
                const n = Number(y);
                return Number.isFinite(n) ? n : null;
              }
            }
            if (typeof v === 'string') {
              const n = Number(v);
              return Number.isFinite(n) ? n : null;
            }
            return null;
          })
          .filter((n): n is number => typeof n === 'number' && Number.isFinite(n));
        if (parsed.length >= 2) return parsed;
      }
      return fallbackBtcUsdWeekly;
    })();
    const btcWeekly = sampleNumericSeriesToLength(rawBtcSeries, sampleN).map((v) => Math.round(v));
    const btcRetPct = btcWeekly.length > 1 ? ((btcWeekly[btcWeekly.length - 1] / btcWeekly[0]) - 1) * 100 : NaN;

    const passCount = scorecard.filter((s) => s.status === 'pass').length;
    const warnCount = scorecard.filter((s) => s.status === 'warn').length;
    const failCount = scorecard.filter((s) => s.status && s.status !== 'pass' && s.status !== 'warn').length;
    const naCount = Math.max(0, scorecard.length - passCount - warnCount - failCount);
    let html = tearTemplate;
    html = html.replace(/const weeklyData = \[[\s\S]*?\];/, `const weeklyData = ${JSON.stringify(weeklyData)};`);
    html = html.replace(/const btcWeekly = \[[\s\S]*?\];/, `const btcWeekly = ${JSON.stringify(btcWeekly)};`);
    html = html.replace(/const drawdownWeekly = \[[\s\S]*?\];/, `const drawdownWeekly = ${JSON.stringify(drawdownWeekly)};`);
    html = html.replace(/<!-- CONFIG STRIP -->[\s\S]*?<!-- FOOTER -->/, '<!-- FOOTER -->');

    const injected = `
<script>
(() => {
  const setText = (sel, txt) => { const el = document.querySelector(sel); if (el) el.textContent = txt; };
  setText('.strategy-name', ${JSON.stringify(selectedFilter ?? 'Selected Filter')});
  setText('.header-sub', ${JSON.stringify(`${fmtDateLong(first.d)} - ${fmtDateLong(last.d)} · ${days} Calendar Days (${(days / 365).toFixed(2)} yrs) · ${new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(runStartingCapital)} Starting Capital`)});
  setText('.as-of-stamp span', ${JSON.stringify(fmtDateLong(last.d))});
  setText('.run-id', ${JSON.stringify(`RUN: ${jobId ?? 'N/A'}`)});
  const hero = document.querySelectorAll('.hero-card');
  const heroVals = [
    ${JSON.stringify(fmtSignedPct(avgMonthly, 2))},
    ${JSON.stringify(fmtPercent2(cagr))},
    ${JSON.stringify(fmtMetric(sharpe))},
    ${JSON.stringify(fmtMetric(sortino))},
    ${JSON.stringify(fmtPercent2(maxDd))},
    ${JSON.stringify(fmtMetric(calmar))}
  ];
  hero.forEach((h, i) => {
    const v = h.querySelector('.hero-value');
    if (v && heroVals[i]) v.textContent = heroVals[i];
  });
  const heroSubs = document.querySelectorAll('.hero-card .hero-sub');
  if (heroSubs[0]) heroSubs[0].textContent = ${JSON.stringify(`${monthlyWinRate.toFixed(1)}% monthly win rate`)};
  setText('.chart-title-left', ${JSON.stringify(`Equity Curve - ${selectedFilter ?? 'Selected Filter'}`)});
  const chartStats = document.querySelector('.chart-stats');
  if (chartStats) {
    chartStats.innerHTML = ${JSON.stringify(
      `${fmtSignedPct(totRet, 1)} net &nbsp;·&nbsp; <span>${new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(runStartingCapital)}</span> → <span>${new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(last.y)}</span> &nbsp;·&nbsp; R² <span>${r2 !== null ? fmtMetric(r2) : 'N/A'}</span> &nbsp;·&nbsp; Sharpe <span>${fmtMetric(sharpe)}</span>`,
    )};
  }
  const rows = document.querySelectorAll('.stat-row');
  rows.forEach((r) => {
    const key = (r.querySelector('.stat-key')?.textContent || '').trim().toLowerCase();
    const val = r.querySelector('.stat-val');
    if (!val) return;
    if (key.includes('cumulative net return')) val.textContent = ${JSON.stringify(fmtSignedPct(totRet, 2))};
    else if (key.includes('cagr (annualised)')) val.textContent = ${JSON.stringify(fmtPercent2(cagr))};
    else if (key.includes('final equity')) val.textContent = ${JSON.stringify(new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(last.y))};
    else if (key.includes('best single day')) val.textContent = ${JSON.stringify(fmtSignedPct(bestDay, 2))};
    else if (key.includes('worst single day')) val.textContent = ${JSON.stringify(fmtSignedPct(worstDay, 2))};
    else if (key.includes('avg daily return')) val.textContent = ${JSON.stringify(fmtSignedPct(avgDaily, 2))};
    else if (key.includes('avg weekly return')) val.textContent = ${JSON.stringify(fmtSignedPct((weeklyReturns.reduce((a, b) => a + b, 0) / Math.max(1, weeklyReturns.length)) * 100, 2))};
    else if (key.includes('avg monthly return')) val.textContent = ${JSON.stringify(fmtSignedPct(avgMonthly, 2))};
    else if (key.includes('equity multiplier')) val.textContent = ${JSON.stringify(Number.isFinite(equityMultiplier) ? `${equityMultiplier.toFixed(2)}×` : 'N/A')};
    else if (key.includes('vs btc buy-and-hold')) val.textContent = ${JSON.stringify(Number.isFinite(btcRetPct) ? `${btcRetPct >= 0 ? '+' : ''}${btcRetPct.toFixed(1)}% (same period)` : 'N/A')};
    else if (key === 'sharpe ratio') val.textContent = ${JSON.stringify(fmtMetric(sharpe))};
    else if (key === 'sortino ratio') val.textContent = ${JSON.stringify(fmtMetric(sortino))};
    else if (key === 'calmar ratio') val.textContent = ${JSON.stringify(fmtMetric(calmar))};
    else if (key === 'omega ratio') val.textContent = ${JSON.stringify(omega !== null ? fmtMetric(omega) : 'N/A')};
    else if (key === 'profit factor') val.textContent = ${JSON.stringify(Number.isFinite(profitFactor) ? profitFactor.toFixed(2) : 'N/A')};
    else if (key.includes('gain-to-pain ratio')) val.textContent = ${JSON.stringify(Number.isFinite(gainToPain) ? gainToPain.toFixed(2) : 'N/A')};
    else if (key === 'ulcer index') val.textContent = ${JSON.stringify(ulcer !== null ? fmtMetric(ulcer) : 'N/A')};
    else if (key === 'skewness') val.textContent = ${JSON.stringify(Number.isFinite(skew) ? `${skew >= 0 ? '+' : ''}${skew.toFixed(2)}` : 'N/A')};
    else if (key === 'kurtosis') val.textContent = ${JSON.stringify(Number.isFinite(kurt) ? `${kurt >= 0 ? '+' : ''}${kurt.toFixed(2)}` : 'N/A')};
    else if (key.includes('max drawdown')) val.textContent = ${JSON.stringify(fmtPercent2(maxDd))};
    else if (key.includes('avg drawdown')) val.textContent = ${JSON.stringify(Number.isFinite(avgDrawdown) ? fmtPercent2(avgDrawdown) : 'N/A')};
    else if (key.includes('avg max dd / episode')) val.textContent = ${JSON.stringify(Number.isFinite(avgMaxDdEpisode) ? fmtPercent2(avgMaxDdEpisode) : 'N/A')};
    else if (key.includes('longest dd duration')) val.textContent = ${JSON.stringify(`${Math.round(longestDdDuration)} days`)};
    else if (key.includes('max dd recovery')) val.textContent = 'N/A';
    else if (key.includes('% time underwater')) val.textContent = ${JSON.stringify(fmtPercent2(timeUnderwaterPct))};
    else if (key.includes('cvar 5%')) val.textContent = ${JSON.stringify(Number.isFinite(cvar5) ? fmtPercent2(cvar5 * 100) : 'N/A')};
    else if (key.includes('cvar 1%')) val.textContent = ${JSON.stringify(Number.isFinite(cvar1) ? fmtPercent2(cvar1 * 100) : 'N/A')};
    else if (key === 'tail ratio') val.textContent = ${JSON.stringify(Number.isFinite(tailRatio) ? tailRatio.toFixed(2) : 'N/A')};
    else if (key.includes('monthly win rate')) val.textContent = ${JSON.stringify(`${monthlyWinRate.toFixed(1)}% (${monthlyRows.filter((r) => r.pct > 0).length}W/${monthlyRows.filter((r) => r.pct <= 0).length}L)`)};
    else if (key.includes('weekly win rate')) val.textContent = ${JSON.stringify(fmtPercent2(weeklyWinRate))};
    else if (key.includes('active day win rate')) val.textContent = ${JSON.stringify(fmtPercent2(activeDayWinRate))};
    else if (key.includes('best month')) val.textContent = ${JSON.stringify(Number.isFinite(bestMonth) ? fmtSignedPct(bestMonth, 2) : 'N/A')};
    else if (key.includes('worst month')) val.textContent = ${JSON.stringify(Number.isFinite(worstMonth) ? fmtSignedPct(worstMonth, 2) : 'N/A')};
    else if (key.includes('avg win (daily)')) val.textContent = ${JSON.stringify(Number.isFinite(avgWinPct) ? fmtSignedPct(avgWinPct, 2) : 'N/A')};
    else if (key.includes('avg loss (daily)')) val.textContent = ${JSON.stringify(Number.isFinite(avgLossPct) ? fmtSignedPct(avgLossPct, 2) : 'N/A')};
    else if (key.includes('avg win / avg loss')) val.textContent = ${JSON.stringify(Number.isFinite(winLossRatio) ? `${winLossRatio.toFixed(2)}×` : 'N/A')};
    else if (key.includes('max consec. losses')) val.textContent = ${JSON.stringify(`${maxConsecLosses} days`)};
    else if (key.includes('active days')) val.textContent = ${JSON.stringify(`${activeDays} / ${days} (${days > 0 ? ((activeDays / days) * 100).toFixed(2) : '0.00'}%)`)};
    else if (key.includes('filtered (guardrail)')) val.textContent = ${JSON.stringify(`${Math.max(0, days - activeDays)} days`)};
    else if (key.includes('avg leverage used')) val.textContent = 'N/A';
    else if (key.includes('leverage range')) val.textContent = 'N/A';
    else if (key.includes('avg symbols / day')) val.textContent = 'N/A';
    else if (key.includes('turnover')) val.textContent = ${JSON.stringify(turnoverText ?? 'N/A')};
    else if (key.includes('total fee drag')) val.textContent = ${JSON.stringify(totalFeeDragText ?? 'N/A')};
    else if (key.includes('taker fee / side')) val.textContent = ${JSON.stringify(takerFeeSideText ?? 'N/A')};
    else if (key.includes('capacity ceiling')) val.textContent = ${JSON.stringify(institutionalCap ? `~$${institutionalCap}` : 'N/A')};
    else if (key.includes('deflated sharpe ratio')) val.textContent = ${JSON.stringify(fmtPercent2(dsr))};
    else if (key.includes('dsr — genuine sharpe prob')) val.textContent = ${JSON.stringify(fmtPercent2(dsr))};
    else if (key.includes('false positive probability')) val.textContent = ${JSON.stringify(fmtPercent2(Math.max(0, 100 - dsr)))};
    else if (key.includes('track record length')) val.textContent = ${JSON.stringify(`${days} days (${(days / 365).toFixed(1)} yrs)`)};
    else if (key.includes('min track record needed')) val.textContent = '500d';
    else if (key.includes('probability of loss')) val.textContent = ${JSON.stringify(activeDaily.length > 0 ? fmtPercent2((activeDaily.filter((r) => r < 0).length / activeDaily.length) * 100) : 'N/A')};
    else if (key.includes('ruin prob')) val.textContent = ${JSON.stringify(ruinProbText ?? 'N/A')};
    else if (key.includes('equity curve r')) val.textContent = ${JSON.stringify(eqR2Text ?? (r2 !== null ? fmtMetric(r2) : 'N/A'))};
    else if (key.includes('is / oos cagr ratio')) val.textContent = ${JSON.stringify(isOosCagrRatioText ?? 'N/A')};
    else if (key.includes('oos sharpe')) val.textContent = ${JSON.stringify(oosSharpeText ?? 'N/A')};
    else if (key.includes('sharpe decay')) val.textContent = ${JSON.stringify(sharpeDecayText ?? 'N/A')};
    else if (key.includes('walk-forward cv')) val.textContent = ${JSON.stringify(fmtMetric(cv))};
    else if (key.includes('positive wf folds')) val.textContent = ${JSON.stringify(positiveWfFoldsText ?? 'N/A')};
    else if (key.includes('fa-wf mean dsr')) val.textContent = ${JSON.stringify(faWfMeanDsrText ?? 'N/A')};
    else if (key.includes('sharpe @2')) val.textContent = ${JSON.stringify(sharpe2xSlipText ?? 'N/A')};
    else if (key.includes('slippage sensitivity')) val.textContent = ${JSON.stringify(slippageSensitivityText ?? 'N/A')};
  });
  const moGrid = document.querySelector('.monthly-grid');
  if (moGrid) {
    moGrid.innerHTML = ${JSON.stringify(
      monthlyRows.slice(-14).map((m) => {
        const cls = m.pct >= 0 ? 'mo-pos' : 'mo-neg';
        return `<div class="mo-cell ${cls}"><div class="mo-label">${m.label}</div><div class="mo-val">${m.pct >= 0 ? '+' : ''}${m.pct.toFixed(2)}%</div></div>`;
      }).join('')
    )};
  }
  const moHdr = document.querySelector('.monthly-sec .sec-title span:last-child');
  if (moHdr) {
    moHdr.textContent = ${JSON.stringify(`${monthlyRows.filter((r) => r.pct > 0).length} positive | ${monthlyRows.filter((r) => r.pct <= 0).length} negative | ${monthlyWinRate.toFixed(1)}% win rate`)};
    moHdr.style.whiteSpace = 'nowrap';
  }
  const sc92 = ${JSON.stringify(scorecard92)};
  const sbPass = sc92 ? sc92.pass : ${passCount};
  const sbFail = sc92 ? sc92.fail : ${failCount};
  const sbWarn = sc92 ? sc92.warn : ${warnCount};
  const sbNa = sc92 ? sc92.na : ${naCount};
  const sbTotal = sc92 ? sc92.total : Math.max(1, sbPass + sbFail + sbWarn + sbNa);
  const sb = document.querySelectorAll('.scorecard-bar .sb-track > div');
  if (sb[0]) sb[0].style.width = ((sbPass / sbTotal) * 100).toFixed(1) + '%';
  if (sb[1]) sb[1].style.width = ((sbFail / sbTotal) * 100).toFixed(1) + '%';
  if (sb[2]) sb[2].style.width = ((sbWarn / sbTotal) * 100).toFixed(1) + '%';
  if (sb[3]) sb[3].style.width = ((sbNa / sbTotal) * 100).toFixed(1) + '%';
  const sbNum = document.querySelectorAll('.scorecard-bar .sb-num span:last-child');
  if (sbNum[0]) sbNum[0].textContent = String(sbPass) + ' Pass';
  if (sbNum[1]) sbNum[1].textContent = String(sbFail) + ' Fail';
  if (sbNum[2]) sbNum[2].textContent = String(sbWarn) + ' Borderline';
  if (sbNum[3]) sbNum[3].textContent = String(sbNa) + ' N/A';
  if (${String(grade !== null)}) {
    const badge = document.querySelector('.header-badges');
    if (badge) {
      const el = document.createElement('span');
      el.className = 'badge badge-gold';
      el.textContent = 'Grade ${grade !== null ? Math.round(grade) : ''}';
      badge.appendChild(el);
    }
  }
  const footL = document.querySelector('.footer-left');
  if (footL) {
    footL.innerHTML = ${JSON.stringify(
      `Generated: ${fmtDateLong(last.d)} &nbsp;|&nbsp; Backtested on ${days} days of exchange data &nbsp;|&nbsp; Past performance does not guarantee future results<br>* Dates/metrics reflect selected audit and filter<br>Strategy trades USDT-margined perp futures on Top-100 altcoins, long-only, daily bars`,
    )};
  }
})();
</script>`;

    html = html.replace('</body>', `${injected}</body>`);
    return html;
  }, [tearTemplate, equityCurveDollars, selectedRow, selectedDrawdownCurve, m, scorecard, selectedFilter, runStartingCapital, jobId, auditOutput, params]);

  const metricCards = selectedRow
    ? [
      { label: 'Sharpe', key: 'sharpe', value: fmtMetric(selectedRow.sharpe), colorValue: selectedRow.sharpe },
      { label: 'CAGR %', key: 'cagr', value: fmtPercent2(selectedRow.cagr), colorValue: selectedRow.cagr },
      { label: 'Max DD %', key: 'max_dd', value: fmtPercent2(selectedRow.max_dd), colorValue: selectedRow.max_dd },
      {
        label: 'Active Days',
        key: 'active',
        value: activeDaysMain,
        unit: activeDaysSuffix,
        unitColor: 'var(--t1)',
        secondary: selectedActivePct !== null ? `(${selectedActivePct.toFixed(2)}%)` : undefined,
        colorValue: selectedRow.active,
      },
      { label: 'WF-CV', key: 'cv', value: fmtMetric((selectedRow.wf_cv ?? selectedRow.cv) as unknown), colorValue: (selectedRow.wf_cv ?? selectedRow.cv) },
      { label: 'DSR %', key: 'dsr_pct', value: fmtPercent2(selectedRow.dsr_pct), colorValue: selectedRow.dsr_pct },
      { label: 'Sum of Daily Return %', key: 'tot_ret', value: fmtPercent2(selectedRow.tot_ret), colorValue: selectedRow.tot_ret },
      { label: 'Grade', key: 'grade', value: selectedRow.grade_score != null ? String(selectedRow.grade_score) : String(selectedRow.grade ?? 'N/A'), colorValue: selectedRow.grade_score },
      {
        label: 'Avg Win / Avg Loss',
        key: 'avg_win_loss',
        value: derivedSummaryStats.avgWinLoss !== null ? `${derivedSummaryStats.avgWinLoss.toFixed(2)}x` : 'N/A',
        colorValue: derivedSummaryStats.avgWinLoss,
      },
      {
        label: 'Profit Factor',
        key: 'profit_factor',
        value: derivedSummaryStats.profitFactor !== null ? derivedSummaryStats.profitFactor.toFixed(2) : 'N/A',
        colorValue: derivedSummaryStats.profitFactor,
      },
      {
        label: 'Longest UW Streak',
        key: 'uw_streak',
        value: String(derivedSummaryStats.longestUnderwaterStreak),
        unit: 'days',
        unitColor: 'var(--t1)',
        colorValue: derivedSummaryStats.longestUnderwaterStreak,
      },
      {
        label: 'Avg 1M Return %',
        key: 'avg_1m',
        value: derivedSummaryStats.avg1M !== null ? fmtPercent2(derivedSummaryStats.avg1M) : 'N/A',
        colorValue: derivedSummaryStats.avg1M,
      },
    ]
    : [
      { label: 'Sortino', key: 'sortino', value: fmtMetric(m.sortino), colorValue: m.sortino },
      { label: 'Calmar', key: 'calmar', value: fmtMetric(m.calmar ?? m.calmar_ratio), colorValue: m.calmar ?? m.calmar_ratio },
      { label: 'Omega', key: 'omega', value: fmtMetric(m.omega), colorValue: m.omega },
      { label: 'Ulcer Index', key: 'ulcer_index', value: fmtMetric(m.ulcer_index), colorValue: m.ulcer_index },
      { label: 'FA-OOS Sharpe', key: 'fa_oos_sharpe', value: fmtMetric(m.fa_oos_sharpe), colorValue: m.fa_oos_sharpe },
      { label: 'DSR %', key: 'dsr_pct', value: fmtPercent2(m.dsr_pct), colorValue: m.dsr_pct },
      { label: 'WF-CV', key: 'cv', value: fmtMetric(m.cv), colorValue: m.cv },
      { label: 'Flat Days', key: 'flat_days', value: fmtMetric(m.flat_days, true), colorValue: m.flat_days },
    ];
  const equityStatsBar = selectedRow ? [
    {
      label: 'Total Return %',
      value: fmtPercent2(asNum(selectedRow.tot_ret) ?? 0),
      color: metricColor('tot_ret', selectedRow.tot_ret),
    },
    {
      label: 'Sharpe',
      value: fmtMetric(selectedRow.sharpe),
      color: metricColor('sharpe', selectedRow.sharpe),
    },
    {
      label: 'Max DD',
      value: fmtPercent2(asNum(selectedRow.max_dd) ?? 0),
      color: metricColor('max_dd', selectedRow.max_dd),
    },
    {
      label: 'Volatility',
      value: dailyVolatilityPct !== null ? fmtPercent2(dailyVolatilityPct) : 'N/A',
      color: dailyVolatilityPct !== null
        ? (dailyVolatilityPct <= 4 ? 'var(--green)' : dailyVolatilityPct <= 8 ? 'var(--amber)' : 'var(--red)')
        : 'var(--t2)',
    },
  ] : null;
  if (!results) {
    return (
      <div style={{ padding: 16, color: 'var(--t3)', fontSize: 10 }}>
        No results available.
      </div>
    );
  }

  async function ensureAuditOutputLoaded() {
    if (!jobId || auditOutput || outputLoading) return;
    setOutputLoading(true);
    setOutputError(null);
    try {
      const res = await fetch(`http://localhost:8000/api/jobs/${jobId}/output`);
      if (!res.ok) throw new Error(`GET /api/jobs/${jobId}/output failed: ${res.status}`);
      const data = (await res.json()) as { text?: string };
      setAuditOutput(typeof data.text === 'string' ? data.text : '');
    } catch (err) {
      setOutputError(String(err));
    } finally {
      setOutputLoading(false);
    }
  }

  async function copyRawOutput() {
    try {
      await navigator.clipboard.writeText(auditOutput || '');
      setCopyState('copied');
      setTimeout(() => setCopyState('idle'), 1400);
    } catch {
      setCopyState('error');
      setTimeout(() => setCopyState('idle'), 1800);
    }
  }

  function jumpToFilterComparison() {
    setActiveTab('summary');
    requestAnimationFrame(() => {
      filterComparisonRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  }
  function scrollMonthlyHeatmap(direction: 'left' | 'right') {
    const el = monthlyHeatmapRailRef.current;
    if (!el) return;
    const delta = Math.max(180, Math.floor(el.clientWidth * 0.65));
    el.scrollBy({ left: direction === 'left' ? -delta : delta, behavior: 'smooth' });
  }

  return (
    <div style={{ padding: '0 16px 16px 16px', display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          height: 40,
          position: 'sticky',
          top: 0,
          zIndex: 20,
          background: 'var(--bg0)',
          borderBottom: '1px solid var(--line)',
        }}
      >
        <div
          style={{
            fontSize: 10,
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
            color: 'var(--t3)',
            fontWeight: 700,
            whiteSpace: 'nowrap',
          }}
        >
          Audit id: {jobId ?? 'N/A'}
        </div>
        <div style={{ display: 'flex', gap: 6, justifyContent: 'flex-end' }}>
          {([
            ['summary', 'SUMMARY'],
            ['breakdown', 'Breakdown'],
            ['tear_sheet', 'Tear Sheet'],
            ['full_report', 'Full Report'],
            ['raw_output', 'Raw Output'],
          ] as Array<[ReportTab, string]>).map(([tab, label]) => (
            <button
              key={tab}
              onClick={async () => {
                setActiveTab(tab);
                if (tab !== 'summary' && tab !== 'breakdown') await ensureAuditOutputLoaded();
              }}
              style={{
                height: 28,
                padding: '0 10px',
                borderRadius: 3,
                border: `1px solid ${activeTab === tab ? 'rgba(255, 255, 255, 0.5)' : 'var(--line2)'}`,
                background: activeTab === tab ? 'rgba(255, 255, 255, 0.08)' : 'var(--bg1)',
                color: activeTab === tab ? 'var(--t1)' : 'var(--t2)',
                fontSize: 9,
                letterSpacing: '0.1em',
                fontWeight: 700,
                cursor: 'pointer',
              }}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {activeTab === 'summary' && (
        <>
          {/* Metric cards 4×2 grid */}
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(6, 1fr)',
              gap: 8,
            }}
          >
            {metricCards.map(({ label, key, value, colorValue, secondary, unit, unitColor }) => (
              <MetricCard
                key={key}
                label={label}
                value={value}
                unit={unit}
                unitColor={unitColor}
                secondary={secondary}
                color={metricColor(key, colorValue)}
              />
            ))}
          </div>

          {/* Full-width stacked charts */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
            <details open style={{ background: 'transparent', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}>
              <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
                Equity Curve ($)
              </summary>
              <div style={{ marginTop: 8, display: 'flex', flexDirection: 'column', gap: 0 }}>
                <CurveCard
                  title="Equity Curve ($)"
                  data={equityCurveDollars}
                  color="#00c896"
                  gradientId="equity-gradient"
                  backgroundColor="var(--bg0)"
                  showBorder={false}
                  height={480}
                  showMonthlyGridlines
                  showAthLine
                  showMovingAverage
                  movingAverageWindow={20}
                  baselineValue={100000}
                  compactCurrencyTicks
                  valueFormatter={fmtCurrency}
                  statsBar={equityStatsBar ?? undefined}
                  showTitle={false}
                />
                <CurveCard
                  title="Drawdown Curve"
                  data={selectedDrawdownCurve}
                  color="#ff4d4d"
                  gradientId="drawdown-gradient"
                  showBorder={false}
                  height={100}
                  fillAbove
                  valueFormatter={fmtPercent2}
                  annotateMin
                  annotationLabel="Max DD"
                  showTitle={false}
                />
              </div>
            </details>
            <details open style={{ marginTop: 16, background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}>
              <summary
                style={{
                  cursor: 'pointer',
                  fontSize: 9,
                  color: 'var(--t3)',
                  textTransform: 'uppercase',
                  letterSpacing: '0.12em',
                  fontWeight: 700,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: 8,
                }}
              >
                <span>Monthly Returns Heat Map</span>
                <span
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 8,
                    fontSize: 9,
                    color: 'var(--t3)',
                    letterSpacing: '0.12em',
                    textTransform: 'uppercase',
                    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                    whiteSpace: 'nowrap',
                  }}
                >
                  <span>
                    {monthlyHeatmap.positive} positive&nbsp; | &nbsp;{monthlyHeatmap.negative} negative&nbsp; | &nbsp;{monthlyHeatmap.winRate.toFixed(1)}% win rate&nbsp; | &nbsp;mean {monthlyHeatmap.mean >= 0 ? '+' : ''}{monthlyHeatmap.mean.toFixed(2)}%
                  </span>
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        scrollMonthlyHeatmap('left');
                      }}
                      aria-label="Scroll monthly heatmap left"
                      style={{
                        height: 22,
                        width: 22,
                        borderRadius: 3,
                        border: '1px solid var(--line2)',
                        background: 'var(--bg1)',
                        color: 'var(--t2)',
                        cursor: 'pointer',
                        lineHeight: 1,
                        padding: 0,
                        fontSize: 11,
                      }}
                    >
                      ‹
                    </button>
                    <button
                      type="button"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        scrollMonthlyHeatmap('right');
                      }}
                      aria-label="Scroll monthly heatmap right"
                      style={{
                        height: 22,
                        width: 22,
                        borderRadius: 3,
                        border: '1px solid var(--line2)',
                        background: 'var(--bg1)',
                        color: 'var(--t2)',
                        cursor: 'pointer',
                        lineHeight: 1,
                        padding: 0,
                        fontSize: 11,
                      }}
                    >
                      ›
                    </button>
                  </span>
                </span>
              </summary>
              <div style={{ marginTop: 8 }}>
                <div
                  style={{
                    background: 'var(--bg2)',
                    border: '1px solid var(--line)',
                    borderRadius: 3,
                    padding: 10,
                  }}
                >
                  <div
                    ref={monthlyHeatmapRailRef}
                    style={{
                      display: 'flex',
                      gap: 6,
                      overflowX: 'hidden',
                      paddingBottom: 2,
                    }}
                  >
                    {monthlyHeatmap.rows.map((mRow) => {
                      const strength = Math.min(1, Math.abs(mRow.pct) / Math.max(1e-9, monthlyHeatmap.scale));
                      const alpha = 0.12 + (0.58 * strength);
                      const isPos = mRow.pct >= 0;
                      const bg = isPos
                        ? `rgba(0, 200, 150, ${alpha.toFixed(3)})`
                        : `rgba(255, 77, 77, ${alpha.toFixed(3)})`;
                      const border = isPos ? 'rgba(0, 200, 150, 0.35)' : 'rgba(255, 77, 77, 0.35)';
                      const valColor = isPos ? '#6ad6ac' : '#f17a73';
                      return (
                        <div
                          key={`m-heat-${mRow.label}`}
                          style={{
                            minWidth: 102,
                            borderRadius: 2,
                            border: `1px solid ${border}`,
                            background: bg,
                            padding: '8px 10px',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 6,
                            flexShrink: 0,
                          }}
                        >
                          <div
                            style={{
                              fontSize: 8,
                              color: 'rgba(255,255,255,0.68)',
                              letterSpacing: '0.1em',
                              textTransform: 'uppercase',
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                            }}
                          >
                            {mRow.label}
                          </div>
                          <div
                            style={{
                              fontSize: 12,
                              color: valColor,
                              fontWeight: 700,
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                            }}
                          >
                            {mRow.pct >= 0 ? '+' : ''}{mRow.pct.toFixed(2)}%
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            </details>
          </div>

          <details
            ref={filterComparisonRef}
            open
            style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}
          >
            <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
              Filter Comparison
            </summary>
            <div style={{ marginTop: 8 }}>
              {(hints && hints.length > 0) && (
                <div
                  style={{
                    marginBottom: 10,
                    fontSize: 10,
                    color: 'var(--amber)',
                    background: 'rgba(255, 186, 77, 0.09)',
                    border: '1px solid rgba(255, 186, 77, 0.35)',
                    borderRadius: 3,
                    padding: '7px 9px',
                    lineHeight: 1.45,
                  }}
                >
                  {hints.map((h, i) => <div key={i}>{h}</div>)}
                </div>
              )}
              {showDispersionVpnHint && (!hints || hints.length === 0) && (
                <div
                  style={{
                    marginBottom: 10,
                    fontSize: 10,
                    color: 'var(--amber)',
                    background: 'rgba(255, 186, 77, 0.09)',
                    border: '1px solid rgba(255, 186, 77, 0.35)',
                    borderRadius: 3,
                    padding: '7px 9px',
                    lineHeight: 1.45,
                  }}
                >
                  Dispersion filters were requested but did not run. If Binance API access is geo-blocked, turn on VPN and re-run.
                </div>
              )}
              <FilterTable
                rows={mergedFilters}
                selectedFilter={selectedFilter}
                onSelectFilter={(filter) => setManualSelectedFilter(filter)}
              />
            </div>
          </details>

          <details
            open
            style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}
          >
            <summary
              style={{
                cursor: 'pointer',
                fontSize: 9,
                color: 'var(--t3)',
                textTransform: 'uppercase',
                letterSpacing: '0.12em',
                fontWeight: 700,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 8,
              }}
            >
              <span>Return Profile (Box Plots)</span>
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 10,
                  color: 'var(--t2)',
                  fontSize: 8.5,
                  letterSpacing: '0.08em',
                  fontWeight: 600,
                }}
              >
                <span>1D μ {returnProfileStats.daily ? fmtPercent2(returnProfileStats.daily.mean) : 'N/A'}</span>
                <span>1W μ {returnProfileStats.weekly ? fmtPercent2(returnProfileStats.weekly.mean) : 'N/A'}</span>
                <span>1M μ {returnProfileStats.monthly ? fmtPercent2(returnProfileStats.monthly.mean) : 'N/A'}</span>
                <span>1Q μ {returnProfileStats.quarterly ? fmtPercent2(returnProfileStats.quarterly.mean) : 'N/A'}</span>
              </span>
            </summary>
            <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 8 }}>
              <ReturnBoxPlotCard
                title="Daily Returns"
                stats={returnProfileStats.daily}
                count={returnProfile.daily.length}
              />
              <ReturnBoxPlotCard
                title="Weekly Returns"
                stats={returnProfileStats.weekly}
                count={returnProfile.weekly.length}
              />
              <ReturnBoxPlotCard
                title="Monthly Returns"
                stats={returnProfileStats.monthly}
                count={returnProfile.monthly.length}
              />
              <ReturnBoxPlotCard
                title="Quarterly Returns"
                stats={returnProfileStats.quarterly}
                count={returnProfile.quarterly.length}
              />
            </div>
          </details>

          <details
            open
            style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}
          >
            <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
              Statistical Functions (Daily Active Returns)
            </summary>
            <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 8 }}>
              <DistributionCard
                title="Cumulative Distribution Function (CDF)"
                subtitle="P(Return ≤ x)"
                mode="line"
                splitColorBySign
                signAxis="x"
                points={statisticalCharts.cdf}
                xMinLabel={Number.isFinite(statisticalCharts.min) ? `${statisticalCharts.min.toFixed(2)}%` : 'N/A'}
                xMidLabel={Number.isFinite(statisticalCharts.median) ? `med ${statisticalCharts.median.toFixed(2)}%` : 'N/A'}
                xMaxLabel={Number.isFinite(statisticalCharts.max) ? `${statisticalCharts.max.toFixed(2)}%` : 'N/A'}
                xAxisLabel="daily return %"
                yAxisLabel="cum prob"
                yTickFormatter={(v) => v.toFixed(2)}
                hoverXFormatter={(v) => `${v.toFixed(2)}%`}
                hoverYFormatter={(v) => v.toFixed(3)}
                xMarkers={[
                  { x: statisticalCharts.p10, label: 'p10', color: 'rgba(255,186,77,0.9)' },
                  { x: statisticalCharts.median, label: 'p50', color: 'rgba(255,255,255,0.8)' },
                  { x: statisticalCharts.p90, label: 'p90', color: 'rgba(0,200,150,0.85)' },
                ]}
                chips={[
                  { label: 'p10', value: Number.isFinite(statisticalCharts.p10) ? `${statisticalCharts.p10.toFixed(2)}%` : 'N/A', color: 'rgba(255,186,77,0.9)' },
                  { label: 'p50', value: Number.isFinite(statisticalCharts.median) ? `${statisticalCharts.median.toFixed(2)}%` : 'N/A', color: 'rgba(255,255,255,0.9)' },
                  { label: 'p90', value: Number.isFinite(statisticalCharts.p90) ? `${statisticalCharts.p90.toFixed(2)}%` : 'N/A', color: 'rgba(0,200,150,0.9)' },
                ]}
                summary={`90% of active days are ≤ ${Number.isFinite(statisticalCharts.p90) ? `${statisticalCharts.p90.toFixed(2)}%` : 'N/A'} return.`}
                guideX={0}
              />
              <DistributionCard
                title="Empirical Quantile Function (EQF)"
                subtitle="Return at quantile q"
                mode="line"
                splitColorBySign
                signAxis="y"
                points={statisticalCharts.eqf}
                xMinLabel="q0.00"
                xMidLabel="q0.50"
                xMaxLabel="q1.00"
                xAxisLabel="quantile q"
                yAxisLabel="return %"
                yTickFormatter={(v) => `${v.toFixed(2)}%`}
                hoverXFormatter={(v) => `q${v.toFixed(3)}`}
                hoverYFormatter={(v) => `${v.toFixed(2)}%`}
                chips={[
                  { label: 'median', value: Number.isFinite(statisticalCharts.median) ? `${statisticalCharts.median.toFixed(2)}%` : 'N/A' },
                ]}
                summary={`Median active-day return is ${Number.isFinite(statisticalCharts.median) ? `${statisticalCharts.median.toFixed(2)}%` : 'N/A'}.`}
                guideX={0.5}
              />
              <DistributionCard
                title="Probability Density Function (PDF)"
                subtitle="Relative likelihood of return x"
                mode="step_area"
                points={statisticalCharts.pdf}
                xMinLabel={Number.isFinite(statisticalCharts.min) ? `${statisticalCharts.min.toFixed(2)}%` : 'N/A'}
                xMidLabel="mode density"
                xMaxLabel={Number.isFinite(statisticalCharts.max) ? `${statisticalCharts.max.toFixed(2)}%` : 'N/A'}
                xAxisLabel="daily return %"
                yAxisLabel="density"
                yTickFormatter={(v) => v.toFixed(3)}
                hoverXFormatter={(v) => `${v.toFixed(2)}%`}
                hoverYFormatter={(v) => v.toFixed(4)}
                xMarkers={[
                  { x: statisticalCharts.p10, label: 'p10', color: 'rgba(255,186,77,0.9)' },
                  { x: statisticalCharts.median, label: 'p50', color: 'rgba(255,255,255,0.8)' },
                  { x: statisticalCharts.p90, label: 'p90', color: 'rgba(0,200,150,0.85)' },
                ]}
                chips={[
                  { label: 'tail', value: Number.isFinite(statisticalCharts.min) ? `${statisticalCharts.min.toFixed(2)}%` : 'N/A' },
                  { label: 'center', value: Number.isFinite(statisticalCharts.median) ? `${statisticalCharts.median.toFixed(2)}%` : 'N/A' },
                ]}
                summary="Right tail indicates occasional large upside days; left tail captures downside shock frequency."
                guideX={0}
              />
              <QqPlotCard
                points={statisticalCharts.qq}
                summary="Closer alignment to the diagonal suggests returns are approximately normal; tail curvature indicates fat tails/skew."
              />
              <DistributionCard
                title="Sorted Returns with Percentile Markers"
                subtitle="Ordered active-day returns with key percentile cuts"
                mode="line"
                splitColorBySign
                signAxis="y"
                points={statisticalCharts.eqf}
                xMinLabel="q0.00"
                xMidLabel="q0.50"
                xMaxLabel="q1.00"
                xAxisLabel="quantile q"
                yAxisLabel="return %"
                yTickFormatter={(v) => `${v.toFixed(2)}%`}
                hoverXFormatter={(v) => `q${v.toFixed(3)}`}
                hoverYFormatter={(v) => `${v.toFixed(2)}%`}
                xMarkers={statisticalCharts.percentileMarkers.map((m) => ({
                  x: m.q,
                  label: `p${Math.round(m.q * 100)}`,
                  color: m.q === 0.5 ? 'rgba(255,255,255,0.9)' : 'rgba(0,200,150,0.65)',
                }))}
                chips={statisticalCharts.percentileMarkers.map((m) => ({
                  label: `p${Math.round(m.q * 100)}`,
                  value: `${m.value.toFixed(2)}%`,
                  color: m.q === 0.5 ? 'rgba(255,255,255,0.9)' : 'var(--t1)',
                }))}
                summary="Percentile cuts show return asymmetry and tail risk concentration directly on the sorted curve."
                guideX={0}
              />
              <DailyReturnBarStatCard values={returnProfile.daily} />
            </div>
            <div style={{ marginTop: 8, fontSize: 9, color: 'var(--t3)', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
              n: {statisticalCharts.n}
            </div>
          </details>

          <details
            open
            style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 10px' }}
          >
            <summary
              style={{
                cursor: 'pointer',
                fontSize: 9,
                color: 'var(--t3)',
                textTransform: 'uppercase',
                letterSpacing: '0.12em',
                fontWeight: 700,
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                gap: 8,
              }}
            >
              <span>Calendar Returns Heatmap</span>
              <span
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                }}
                style={{ display: 'inline-flex', border: '1px solid var(--line2)', borderRadius: 3, overflow: 'hidden' }}
              >
                {(['grid', 'chart'] as const).map((mode) => {
                  const active = calendarViewMode === mode;
                  return (
                    <button
                      key={`calendar-mode-${mode}`}
                      type="button"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        setCalendarViewMode(mode);
                        setCalendarRowHoverKey(null);
                        setCalendarHover(null);
                      }}
                      style={{
                        border: 'none',
                        borderRight: mode === 'grid' ? '1px solid var(--line2)' : 'none',
                        background: active ? 'rgba(255,255,255,0.08)' : 'var(--bg1)',
                        color: active ? 'var(--t1)' : 'var(--t2)',
                        fontSize: 9,
                        letterSpacing: '0.08em',
                        textTransform: 'uppercase',
                        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                        padding: '4px 8px',
                        cursor: 'pointer',
                      }}
                    >
                      {mode}
                    </button>
                  );
                })}
              </span>
            </summary>
            <div style={{ marginTop: 8, display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', gap: 8 }}>
              {calendarMonths.map((month) => (
                <div
                  key={month.monthKey}
                  style={{
                    border: '1px solid var(--line)',
                    borderRadius: 3,
                    padding: 8,
                    background: 'var(--bg1)',
                  }}
                >
                  {(() => {
                    const monthSum = month.cells
                      .map((c) => c.ret)
                      .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
                      .reduce((a, b) => a + b, 0);
                    const monthSumColor = monthSum > 0 ? 'var(--green)' : monthSum < 0 ? 'var(--red)' : 'var(--t2)';
                    const monthSumText = `${monthSum >= 0 ? '+' : ''}${monthSum.toFixed(2)}%`;
                    return (
                      <div
                        style={{
                          fontSize: 9,
                          color: 'var(--t2)',
                          textTransform: 'uppercase',
                          letterSpacing: '0.08em',
                          fontWeight: 700,
                          marginBottom: 6,
                          display: 'flex',
                          alignItems: 'center',
                          gap: 6,
                        }}
                      >
                        <span>{month.label}</span>
                        <span style={{ color: 'var(--t3)' }}>•</span>
                        <span style={{ color: monthSumColor }}>{monthSumText}</span>
                      </div>
                    );
                  })()}
                  <div style={{ minHeight: 190, display: 'flex', flexDirection: 'column' }}>
                    {calendarViewMode === 'grid' ? (
                      <>
                      <div
                        style={{
                          display: 'grid',
                          gridTemplateColumns: '22px repeat(7, minmax(0, 1fr)) 22px',
                          gap: 2,
                          marginBottom: 4,
                        }}
                      >
                        <div />
                        {['S', 'M', 'T', 'W', 'T', 'F', 'S'].map((w, i) => (
                          <div
                            key={`${month.monthKey}-wd-${i}`}
                            style={{
                              textAlign: 'center',
                              fontSize: 8,
                              color: 'var(--t3)',
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                            }}
                          >
                            {w}
                          </div>
                        ))}
                        <div />
                      </div>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                        {Array.from({ length: Math.ceil(month.cells.length / 7) }, (_, rowIdx) => {
                          const rowCells = month.cells.slice(rowIdx * 7, (rowIdx + 1) * 7);
                          const rowSum = rowCells
                            .map((c) => c.ret)
                            .filter((v): v is number => typeof v === 'number' && Number.isFinite(v))
                            .reduce((a, b) => a + b, 0);
                          const rowSumText = `${rowSum >= 0 ? '+' : ''}${rowSum.toFixed(1)}%`;
                          const rowSumColor = rowSum > 0 ? 'var(--green)' : rowSum < 0 ? 'var(--red)' : 'rgba(255,255,255,0.55)';
                          const rowKey = `${month.monthKey}-row-${rowIdx}`;
                          return (
                            <div
                              key={rowKey}
                              onMouseEnter={() => setCalendarRowHoverKey(rowKey)}
                              onMouseLeave={() => setCalendarRowHoverKey((prev) => (prev === rowKey ? null : prev))}
                              style={{
                                display: 'grid',
                                gridTemplateColumns: '22px repeat(7, minmax(0, 1fr)) 22px',
                                gap: 2,
                                alignItems: 'stretch',
                              }}
                            >
                              <div />
                              {rowCells.map((cell) => {
                                if (!cell.date || cell.day === null) {
                                  return (
                                    <div
                                      key={cell.key}
                                      style={{
                                        aspectRatio: '1 / 1',
                                        borderRadius: 2,
                                        background: 'transparent',
                                        border: '1px solid transparent',
                                      }}
                                    />
                                  );
                                }
                                const r = cell.ret;
                                let bg = 'rgba(255,255,255,0.03)';
                                let border = 'rgba(255,255,255,0.08)';
                                if (typeof r === 'number' && Number.isFinite(r)) {
                                  const strength = Math.min(1, Math.abs(r) / Math.max(1e-9, calendarScale));
                                  const alpha = 0.10 + (0.70 * strength);
                                  if (r >= 0) {
                                    bg = `rgba(0, 200, 150, ${alpha.toFixed(3)})`;
                                    border = 'rgba(0, 200, 150, 0.35)';
                                  } else {
                                    bg = `rgba(255, 77, 77, ${alpha.toFixed(3)})`;
                                    border = 'rgba(255, 77, 77, 0.35)';
                                  }
                                }
                                const title = `${cell.key}${typeof r === 'number' && Number.isFinite(r) ? ` | ${r >= 0 ? '+' : ''}${r.toFixed(2)}%` : ' | no return'}`;
                                return (
                                  <div
                                    key={cell.key}
                                    title={title}
                                    onMouseEnter={(e) => {
                                      if (typeof r !== 'number' || !Number.isFinite(r)) return;
                                      setCalendarHover({
                                        x: e.clientX,
                                        y: e.clientY,
                                        text: `${cell.key} | ${r >= 0 ? '+' : ''}${r.toFixed(2)}%`,
                                      });
                                    }}
                                    onMouseMove={(e) => {
                                      if (typeof r !== 'number' || !Number.isFinite(r)) return;
                                      setCalendarHover((prev) => (prev
                                        ? { ...prev, x: e.clientX, y: e.clientY }
                                        : {
                                          x: e.clientX,
                                          y: e.clientY,
                                          text: `${cell.key} | ${r >= 0 ? '+' : ''}${r.toFixed(2)}%`,
                                        }));
                                    }}
                                    onMouseLeave={() => setCalendarHover(null)}
                                    style={{
                                      aspectRatio: '1 / 1',
                                      borderRadius: 2,
                                      background: bg,
                                      border: `1px solid ${border}`,
                                      display: 'flex',
                                      alignItems: 'center',
                                      justifyContent: 'center',
                                      fontSize: 8,
                                      color: 'rgba(255,255,255,0.9)',
                                      fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                                      lineHeight: 1,
                                    }}
                                  >
                                    {cell.day}
                                  </div>
                                );
                              })}
                              <div
                                style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  justifyContent: 'center',
                                  color: rowSumColor,
                                  fontSize: 8,
                                  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                                  transform: 'rotate(90deg)',
                                  transformOrigin: 'center center',
                                  whiteSpace: 'nowrap',
                                  opacity: calendarRowHoverKey === rowKey ? 1 : 0,
                                  transition: 'opacity 120ms ease',
                                }}
                              >
                                {rowSumText}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                      </>
                    ) : (
                      (() => {
                      const daily = month.cells
                        .filter((c): c is CalendarCell & { date: Date; day: number; ret: number } => (
                          c.date instanceof Date
                          && typeof c.day === 'number'
                          && typeof c.ret === 'number'
                          && Number.isFinite(c.ret)
                        ));
                      const cum: Array<{ key: string; day: number; pct: number }> = [];
                      let eq = 1;
                      for (const c of daily) {
                        eq *= (1 + (c.ret / 100));
                        cum.push({
                          key: c.key,
                          day: c.day,
                          pct: (eq - 1) * 100,
                        });
                      }
                      if (cum.length === 0) {
                        return (
                          <div style={{ fontSize: 9, color: 'var(--t3)', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
                            No daily return data
                          </div>
                        );
                      }
                      const h = 190;
                      const w = 240;
                      const padL = 24;
                      const padR = 8;
                      const padT = 8;
                      const padB = 18;
                      const minCum = Math.min(0, ...cum.map((c) => c.pct));
                      const maxCum = Math.max(0, ...cum.map((c) => c.pct));
                      const range = Math.max(1e-9, maxCum - minCum);
                      const toY = (v: number) => h - padB - ((v - minCum) / range) * (h - padT - padB);
                      const plotW = w - padL - padR;
                      const barW = Math.max(1, Math.min(6, plotW / Math.max(1, cum.length) * 0.9));
                      const yZero = toY(0);
                      return (
                        <svg viewBox={`0 0 ${w} ${h}`} style={{ width: '100%', height: '100%', display: 'block' }} preserveAspectRatio="none">
                          <line x1={padL} y1={padT} x2={padL} y2={h - padB} stroke="var(--line2)" />
                          <line x1={padL} y1={h - padB} x2={w - padR} y2={h - padB} stroke="var(--line2)" />
                          <line x1={padL} y1={yZero} x2={w - padR} y2={yZero} stroke="rgba(255,255,255,0.35)" strokeDasharray="2 2" />
                          {cum.map((c, i) => {
                            const x = padL + ((i + 0.5) / cum.length) * plotW;
                            const y = toY(c.pct);
                            const top = Math.min(yZero, y);
                            const hh = Math.max(1, Math.abs(y - yZero));
                            const isPos = c.pct >= 0;
                            return (
                              <rect
                                key={`${month.monthKey}-cum-${c.key}`}
                                x={x - (barW / 2)}
                                y={top}
                                width={barW}
                                height={hh}
                                fill={isPos ? 'rgba(0, 200, 150, 0.35)' : 'rgba(255, 77, 77, 0.35)'}
                                stroke={isPos ? 'rgba(0, 200, 150, 0.75)' : 'rgba(255, 77, 77, 0.75)'}
                                strokeWidth={0.35}
                                onMouseEnter={(e) => setCalendarHover({
                                  x: e.clientX,
                                  y: e.clientY,
                                  text: `${c.key} | cum ${c.pct >= 0 ? '+' : ''}${c.pct.toFixed(2)}%`,
                                })}
                                onMouseMove={(e) => setCalendarHover((prev) => (prev
                                  ? { ...prev, x: e.clientX, y: e.clientY }
                                  : { x: e.clientX, y: e.clientY, text: `${c.key} | cum ${c.pct >= 0 ? '+' : ''}${c.pct.toFixed(2)}%` }))}
                                onMouseLeave={() => setCalendarHover(null)}
                              />
                            );
                          })}
                          <text x={4} y={toY(maxCum) + 3} fill="var(--t3)" fontSize="8" fontFamily="var(--font-space-mono), Space Mono, monospace">
                            {`${maxCum.toFixed(1)}%`}
                          </text>
                          <text x={4} y={toY(minCum) + 3} fill="var(--t3)" fontSize="8" fontFamily="var(--font-space-mono), Space Mono, monospace">
                            {`${minCum.toFixed(1)}%`}
                          </text>
                        </svg>
                      );
                    })()
                    )}
                  </div>
                </div>
              ))}
            </div>
            {calendarHover && (
              <div
                style={{
                  position: 'fixed',
                  left: calendarHover.x + 10,
                  top: calendarHover.y + 10,
                  zIndex: 80,
                  background: 'var(--bg1)',
                  border: '1px solid var(--line2)',
                  borderRadius: 3,
                  padding: '4px 6px',
                  fontSize: 9,
                  color: 'var(--t1)',
                  pointerEvents: 'none',
                  whiteSpace: 'nowrap',
                  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                }}
              >
                {calendarHover.text}
              </div>
            )}
          </details>
        </>
      )}

      {activeTab === 'breakdown' && (
        <div />
      )}

      {activeTab === 'stress_tests' && (
        <div style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: 12, display: 'flex', flexDirection: 'column', gap: 10 }}>
          {outputLoading && <div style={{ fontSize: 10, color: 'var(--t3)' }}>Loading audit output...</div>}
          {outputError && <div style={{ fontSize: 10, color: 'var(--red)' }}>{outputError}</div>}
          {!outputLoading && !outputError && advancedSections.length > 0 && (
            <div style={{ border: '1px solid var(--line)', borderRadius: 3, padding: 10 }}>
              <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>
                Risk Audit Tests • {selectedFilter ?? 'Selected Filter'}
              </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                {advancedSections.map((section) => (
                  <details key={section.title} style={{ border: '1px solid var(--line)', borderRadius: 3, padding: '6px 8px', background: 'var(--bg1)' }}>
                    <summary style={{ cursor: 'pointer', fontSize: 10, color: 'var(--t1)', fontWeight: 600 }}>
                      {section.title}
                    </summary>
                    <pre
                      style={{
                        margin: '8px 0 0 0',
                        whiteSpace: 'pre-wrap',
                        fontSize: 10,
                        lineHeight: 1.45,
                        color: 'var(--t2)',
                        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                      }}
                    >
                      {section.body}
                    </pre>
                  </details>
                ))}
              </div>
            </div>
          )}
          {!outputLoading && !outputError && advancedSections.length === 0 && (
            <div style={{ fontSize: 10, color: 'var(--t3)' }}>No tear sheet sections detected for the selected filter.</div>
          )}
          {!outputLoading && !outputError && alertLines.length > 0 && (
            <div style={{ border: '1px solid rgba(255, 186, 77, 0.35)', borderRadius: 3, padding: 10, background: 'rgba(255, 186, 77, 0.06)' }}>
              <div style={{ fontSize: 9, color: 'var(--amber)', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 8 }}>
                Alerts & Warnings
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4, fontSize: 10, color: 'var(--amber)' }}>
                {alertLines.map((line, idx) => <div key={`${idx}-${line}`}>{line}</div>)}
              </div>
            </div>
          )}
        </div>
      )}

      {activeTab === 'raw_output' && (
        <div
          style={{
            background: 'var(--bg2)',
            border: '1px solid var(--line)',
            borderRadius: 3,
            padding: 12,
            height: 'calc(100vh - 170px)',
            display: 'flex',
            flexDirection: 'column',
            overflowY: 'auto',
          }}
        >
          {outputLoading && <div style={{ fontSize: 10, color: 'var(--t3)' }}>Loading full report...</div>}
          {outputError && <div style={{ fontSize: 10, color: 'var(--red)' }}>{outputError}</div>}
          {!outputLoading && !outputError && (
            <>
              <div
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  marginBottom: 8,
                  position: 'sticky',
                  top: 0,
                  zIndex: 2,
                  background: 'var(--bg2)',
                  paddingBottom: 8,
                  borderBottom: '1px solid var(--line)',
                }}
              >
                <div
                  style={{
                    fontSize: 9,
                    color: 'var(--t3)',
                    textTransform: 'uppercase',
                    letterSpacing: '0.1em',
                    fontWeight: 700,
                  }}
                >
                  Raw Output
                </div>
                <button
                  onClick={copyRawOutput}
                  style={{
                    height: 26,
                    padding: '0 10px',
                    borderRadius: 3,
                    border: '1px solid var(--line2)',
                    background: 'var(--bg1)',
                    color: copyState === 'copied' ? 'var(--green)' : copyState === 'error' ? 'var(--red)' : 'var(--t1)',
                    fontSize: 9,
                    letterSpacing: '0.08em',
                    fontWeight: 700,
                    cursor: 'pointer',
                  }}
                >
                  {copyState === 'copied' ? 'COPIED' : copyState === 'error' ? 'COPY FAILED' : 'COPY ALL'}
                </button>
              </div>
              <pre
                style={{
                  margin: 0,
                  whiteSpace: 'pre-wrap',
                  fontSize: 10,
                  lineHeight: 1.45,
                  color: 'var(--t2)',
                  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                  flex: 1,
                  minHeight: 0,
                  overflowY: 'auto',
                }}
              >
                {auditOutput || 'No full report text available for this audit.'}
              </pre>
            </>
          )}
        </div>
      )}

      {activeTab === 'tear_sheet' && (
        <div
          style={{
            background: 'var(--bg2)',
            border: '1px solid var(--line)',
            borderRadius: 3,
            overflow: 'hidden',
            height: 'calc(100vh - 170px)',
          }}
        >
          <iframe
            title="Tear Sheet"
            srcDoc={tearSheetHtml || '<!doctype html><html><body style="background:#07090f;color:#7a8aaa;font-family:monospace;padding:16px">Loading tear sheet template...</body></html>'}
            style={{
              width: '100%',
              height: '100%',
              border: 'none',
              display: 'block',
              background: '#07090f',
            }}
          />
        </div>
      )}

      {activeTab === 'full_report' && (
        <div
          style={{
            background: 'var(--bg2)',
            border: '1px solid var(--line)',
            borderRadius: 3,
            height: 'calc(100vh - 170px)',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
          }}
        >
          {outputLoading && <div style={{ fontSize: 10, color: 'var(--t3)' }}>Loading full report sections...</div>}
          {outputError && <div style={{ fontSize: 10, color: 'var(--red)' }}>{outputError}</div>}
          {!outputLoading && !outputError && (
            <>
              <div style={{ padding: '12px 12px 0 12px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10 }}>
                <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.1em', fontWeight: 700 }}>
                  Full Report • {fullReportSectionCount} sections
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <button
                    onClick={jumpToFilterComparison}
                    style={{
                      height: 24,
                      padding: '0 8px',
                      borderRadius: 3,
                      border: '1px solid var(--line2)',
                      background: 'var(--bg1)',
                      color: 'var(--t2)',
                      fontSize: 9,
                      letterSpacing: '0.06em',
                      textTransform: 'uppercase',
                      cursor: 'pointer',
                    }}
                    title="Jump to Filter Comparison in Summary"
                  >
                    Selected Filter: {selectedFilter ?? 'N/A'}
                  </button>
                  <button
                    onClick={copyRawOutput}
                    style={{
                      height: 24,
                      padding: '0 8px',
                      borderRadius: 3,
                      border: '1px solid var(--line2)',
                      background: 'var(--bg1)',
                      color: copyState === 'copied' ? 'var(--green)' : copyState === 'error' ? 'var(--red)' : 'var(--t1)',
                      fontSize: 9,
                      letterSpacing: '0.08em',
                      fontWeight: 700,
                      cursor: 'pointer',
                    }}
                  >
                    {copyState === 'copied' ? 'COPIED' : copyState === 'error' ? 'COPY FAILED' : 'COPY ALL'}
                  </button>
                </div>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minHeight: 0, overflowY: 'auto', padding: '0 12px 12px 12px' }}>
                <div
                  style={{
                    position: 'sticky',
                    top: 0,
                    zIndex: 3,
                    background: 'var(--bg2)',
                    paddingTop: 8,
                    paddingBottom: 8,
                    borderBottom: '1px solid var(--line)',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: 8,
                  }}
                >
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                    {fullReportCategoryGroups.map((cat) => (
                      <button
                        key={`toc-${cat.key}`}
                        onClick={() => {
                          const el = document.getElementById(`full-report-cat-${cat.key}`);
                          if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
                        }}
                        style={{
                          height: 22,
                          padding: '0 8px',
                          borderRadius: 3,
                          border: '1px solid var(--line2)',
                          background: 'var(--bg1)',
                          color: 'var(--t2)',
                          fontSize: 9,
                          letterSpacing: '0.06em',
                          textTransform: 'uppercase',
                          cursor: 'pointer',
                        }}
                      >
                        {cat.title} ({cat.sections.length})
                      </button>
                    ))}
                  </div>
                  <div
                    style={{
                      display: 'grid',
                      gridTemplateColumns: 'repeat(5, minmax(0, 1fr))',
                      gap: 6,
                    }}
                  >
                    {fullReportKpis.map((kpi) => (
                      <div
                        key={`fr-kpi-${kpi.key}`}
                        style={{
                          border: '1px solid var(--line)',
                          borderRadius: 3,
                          background: 'var(--bg1)',
                          padding: '6px 8px',
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          gap: 10,
                        }}
                      >
                        <span style={{ fontSize: 9, color: 'var(--t3)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>{kpi.label}</span>
                        <span style={{ fontSize: 12, fontWeight: 700, color: metricColor(kpi.key, kpi.colorValue) }}>{kpi.value}</span>
                      </div>
                    ))}
                  </div>
                </div>
                {fullReportSections.length === 0 && (
                  <div style={{ fontSize: 10, color: 'var(--t3)' }}>No full report sections detected.</div>
                )}
                {fullReportCategoryGroups.map((cat) => (
                  <details
                    key={`group-${cat.key}`}
                    id={`full-report-cat-${cat.key}`}
                    open={openFullReportCategories[cat.key]}
                    onToggle={(e) => {
                      const isOpen = (e.currentTarget as HTMLDetailsElement).open;
                      setOpenFullReportCategories((prev) => ({ ...prev, [cat.key]: isOpen }));
                    }}
                    style={{ border: '1px solid var(--line)', borderRadius: 3, padding: '6px 8px', background: 'var(--bg1)' }}
                  >
                    <summary
                      style={{
                        cursor: 'pointer',
                        fontSize: 10,
                        color: 'var(--t1)',
                        fontWeight: 700,
                        letterSpacing: '0.08em',
                        textTransform: 'uppercase',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        gap: 8,
                      }}
                    >
                      <span>{cat.title} • {cat.sections.length}</span>
                      <span style={{ display: 'inline-flex', gap: 6 }}>
                        <button
                          onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            setOpenFullReportSectionKeys((prev) => {
                              const next = { ...prev };
                              for (let idx = 0; idx < cat.sections.length; idx += 1) {
                                next[`${cat.key}-${idx}-${cat.sections[idx].title}`] = true;
                              }
                              return next;
                            });
                          }}
                          title="Expand all sections"
                          aria-label="Expand all sections"
                          style={{
                            width: 18,
                            height: 18,
                            border: 'none',
                            background: 'transparent',
                            color: 'var(--t2)',
                            fontSize: 13,
                            lineHeight: 1,
                            cursor: 'pointer',
                            padding: 0,
                          }}
                        >
                          ⊞
                        </button>
                        <button
                          onClick={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            setOpenFullReportSectionKeys((prev) => {
                              const next = { ...prev };
                              for (let idx = 0; idx < cat.sections.length; idx += 1) {
                                next[`${cat.key}-${idx}-${cat.sections[idx].title}`] = false;
                              }
                              return next;
                            });
                          }}
                          title="Collapse all sections"
                          aria-label="Collapse all sections"
                          style={{
                            width: 18,
                            height: 18,
                            border: 'none',
                            background: 'transparent',
                            color: 'var(--t2)',
                            fontSize: 13,
                            lineHeight: 1,
                            cursor: 'pointer',
                            padding: 0,
                          }}
                        >
                          ⊟
                        </button>
                      </span>
                    </summary>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginTop: 8, marginLeft: 24, paddingLeft: 10, borderLeft: '1px solid var(--line2)' }}>
                      {cat.sections.map((section, idx) => (
                        <details
                          key={`${cat.key}-${idx}-${section.title}`}
                          open={!!openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`]}
                          onToggle={(e) => {
                            const key = `${cat.key}-${idx}-${section.title}`;
                            const isOpen = (e.currentTarget as HTMLDetailsElement).open;
                            setOpenFullReportSectionKeys((prev) => ({ ...prev, [key]: isOpen }));
                          }}
                          style={{
                            border: '1px solid var(--line2)',
                            borderRadius: 3,
                            padding: '6px 8px',
                            background: 'var(--bg0)',
                            width: 'calc(100% - 24px)',
                          }}
                        >
                          <summary style={{ cursor: 'pointer', fontSize: 10, color: 'var(--t1)', fontWeight: 600 }}>
                            {section.title}
                          </summary>
                          <pre
                            style={{
                              margin: '8px 0 0 0',
                              whiteSpace: 'pre-wrap',
                              fontSize: 10,
                              lineHeight: 1.45,
                              color: 'color-mix(in srgb, var(--t1) 88%, white 12%)',
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                            }}
                          >
                            {section.body}
                          </pre>
                        </details>
                      ))}
                    </div>
                  </details>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

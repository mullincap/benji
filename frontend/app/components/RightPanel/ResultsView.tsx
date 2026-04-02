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
          {chips.map((chip, idx) => (
            <span
              key={`${chip.label}-${chip.value}-${idx}`}
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

function normalizeFilterLabelCore(s: string): string {
  const base = normalizeFilterLabel(s);
  // Strip leading scorecard prefix patterns like "A -", "B+", etc.
  return base.replace(/^[a-f](?:\s*[\+\-])?\s+/, '').trim();
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
  // Also stop at the first special section title (milestones, scorecards, sweeps) after stressRef
  let firstSpecialIdx: number | undefined;
  for (let i = stressRef.idx + 1; i < lines.length; i += 1) {
    if (isSpecialSectionTitleLine(lines[i].trim())) { firstSpecialIdx = i; break; }
  }
  const endCandidates = [nextFeesIdx, nextDailyAuditIdx, runSummaryIdx >= 0 ? runSummaryIdx : undefined, firstSpecialIdx]
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
    || /^WHAT YOU HAVE\b/i.test(t)
    || /^WHAT YOU STILL NEED\b/i.test(t)
    || /^BOTTOM LINE\b/i.test(t)
    || /^RUN SUMMARY\b/i.test(t)
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
    const dividerEnd = findDividerBoundedEnd(lines, start);
    const allocatorIdx = lines.findIndex((l, j) => j > start && l.includes('ALLOCATOR VIEW SCORECARD'));
    let nextHeadingIdx = lines.length;
    for (let j = start + 1; j < lines.length; j += 1) {
      if (parseHeadingLine(lines[j].trim())) { nextHeadingIdx = j; break; }
    }
    const end = Math.min(
      nextSignal,
      dividerEnd,
      nextHeadingIdx,
      allocatorIdx > -1 ? allocatorIdx : lines.length,
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
      if (parseHeadingLine(t)) {
        end = i;
        break;
      }
      if (isDividerLine(t)) {
        const next = (lines[i + 1] ?? '').trim();
        if (
          /^OUTPUT FILES$/i.test(next)
          || /^RUN INPUTS SUMMARY$/i.test(next)
          || isSpecialSectionTitleLine(next)
          || parseHeadingLine(next)
        ) {
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
      let nextMilestoneOrHeading = lines.length;
      for (let j = start + 1; j < lines.length; j += 1) {
        const tl = lines[j].trim();
        if (
          parseHeadingLine(tl)
          || tl.includes('WEEKLY MILESTONES')
          || tl.includes('MONTHLY MILESTONES')
          || tl.includes('ALLOCATOR VIEW SCORECARD')
          || tl.includes('TECHNICAL APPENDIX SCORECARD')
          || tl.includes('RUN INPUTS SUMMARY')
          || tl.includes('OUTPUT FILES')
        ) { nextMilestoneOrHeading = j; break; }
      }
      const end = Math.min(findDividerBoundedEnd(lines, start), nextMilestoneOrHeading);
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
    let nextSectionIdx = lines.length;
    for (let j = i + 1; j < lines.length; j += 1) {
      const tl = lines[j].trim();
      if (parseHeadingLine(tl) || sweepTitleMatchers.some((rx) => rx.test(tl))) {
        nextSectionIdx = j;
        break;
      }
    }
    let end = Math.min(findDividerBoundedEnd(lines, i), nextSectionIdx);
    if (/^MARKET CAP DIAGNOSTIC\b/i.test(line)) {
      let sawMissingRowsLine = false;
      let forcedEnd: number | null = null;
      for (let j = i + 1; j < nextSectionIdx; j += 1) {
        const tl = lines[j].trim();
        if (/^MCAP_STATS_MISSING_ROWS\s*:/i.test(tl)) {
          sawMissingRowsLine = true;
          continue;
        }
        if (sawMissingRowsLine && /^[-─═]{20,}$/.test(tl)) {
          forcedEnd = j + 1; // include trailing divider line, and stop there
          break;
        }
      }
      if (forcedEnd !== null) {
        end = Math.min(forcedEnd, nextSectionIdx);
      }
    }
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

function parseNumberToken(token: string | undefined): number | null {
  if (!token) return null;
  const n = Number(token.replace(/,/g, ''));
  return Number.isFinite(n) ? n : null;
}

function parsePercentToken(token: string | undefined): number | null {
  if (!token) return null;
  const n = Number(token.replace('%', ''));
  return Number.isFinite(n) ? n : null;
}

function parseFeesTablesFromAuditOutput(text: string): Record<string, FeesRow[]> {
  if (!text) return {};
  const lines = text.split('\n');
  const out: Record<string, FeesRow[]> = {};
  const activeRe = /^\s*(\d{4}-\d{2}-\d{2})\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d+\.\d+)%\s+([+-]?\d+\.\d+)%\s+([+-]?\d[\d,]*\.\d+)\s*$/;
  const noEntryRe = /^\s*(\d{4}-\d{2}-\d{2})\s+([+-]?\d[\d,]*\.\d+)\s+(?:—\s*(NO ENTRY|FILTERED)\s*—|-+\s*(NO ENTRY|FILTERED)\s*-+)\s+([+-]?\d[\d,]*\.\d+)\s+([+-]?\d+\.\d+)%\s+([+-]?\d+\.\d+)%\s+([+-]?\d[\d,]*\.\d+)\s*$/;
  let currentFilter = '';

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    const sim = line.match(/^SIMULATING:\s*(.+)$/i);
    if (sim) {
      currentFilter = sim[1].trim();
      continue;
    }
    if (!/^FEES PANEL\b/i.test(line)) continue;

    const key = currentFilter || `fees_panel_${Object.keys(out).length + 1}`;
    const rows: FeesRow[] = [];
    let headerSeen = false;
    for (let j = i + 1; j < lines.length; j += 1) {
      const raw = lines[j];
      const t = raw.trim();
      if (!t) continue;
      if (/^SIMULATING:\s*/i.test(t) || /^FEES PANEL\b/i.test(t)) break;
      if (!headerSeen) {
        if (/^Date\s+Start\s+\(\$\)\s+Margin\s+\(\$\)\s+Lev\s+Invested\s+\(\$\)\s+Trade Vol\s+\(\$\)\s+Taker Fee\s+\(\$\)\s+Funding\s+\(\$\)\s+End\s+\(\$\)\s+Ret Gross%\s+Ret Net%\s+Net P&L\s+\(\$\)\s*$/i.test(t)) {
          headerSeen = true;
        }
        continue;
      }
      if (!/^\d{4}-\d{2}-\d{2}\b/.test(t)) continue;
      const mNo = t.match(noEntryRe);
      if (mNo) {
        const reason = (mNo[3] || mNo[4] || '').toUpperCase();
        rows.push({
          date: mNo[1],
          start: parseNumberToken(mNo[2]),
          margin: null,
          lev: null,
          invested: null,
          trade_vol: null,
          taker_fee: null,
          funding: null,
          end: parseNumberToken(mNo[5]),
          ret_gross: parsePercentToken(mNo[6]),
          ret_net: parsePercentToken(mNo[7]),
          net_pnl: parseNumberToken(mNo[8]),
          no_entry: true,
          no_entry_reason: reason === 'FILTERED' ? 'filter' : 'conviction_gate',
        });
        continue;
      }
      const mAct = t.match(activeRe);
      if (mAct) {
        rows.push({
          date: mAct[1],
          start: parseNumberToken(mAct[2]),
          margin: parseNumberToken(mAct[3]),
          lev: parseNumberToken(mAct[4]),
          invested: parseNumberToken(mAct[5]),
          trade_vol: parseNumberToken(mAct[6]),
          taker_fee: parseNumberToken(mAct[7]),
          funding: parseNumberToken(mAct[8]),
          end: parseNumberToken(mAct[9]),
          ret_gross: parsePercentToken(mAct[10]),
          ret_net: parsePercentToken(mAct[11]),
          net_pnl: parseNumberToken(mAct[12]),
          no_entry: false,
        });
      }
    }
    if (rows.length > 0) out[key] = rows;
  }
  return out;
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
  logScale = false,
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
  logScale?: boolean;
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
      trend?: number | null;
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
        trend: null as number | null,
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
    // OLS trendline: linear on y (linear view) or on log(y) (log view), fit over index
    const n = out.length;
    if (n >= 2) {
      const ys = logScale
        ? out.map((r) => (r.y > 0 ? Math.log(r.y) : null))
        : out.map((r) => r.y);
      const validPairs = out.map((_, i) => ({ i, lv: ys[i] })).filter((p): p is { i: number; lv: number } => p.lv !== null);
      if (validPairs.length >= 2) {
        const vn = validPairs.length;
        const sumX = validPairs.reduce((a, p) => a + p.i, 0);
        const sumY = validPairs.reduce((a, p) => a + p.lv, 0);
        const sumXY = validPairs.reduce((a, p) => a + p.i * p.lv, 0);
        const sumX2 = validPairs.reduce((a, p) => a + p.i * p.i, 0);
        const denom = vn * sumX2 - sumX * sumX;
        if (denom !== 0) {
          const slope = (vn * sumXY - sumX * sumY) / denom;
          const intercept = (sumY - slope * sumX) / vn;
          for (let i = 0; i < n; i += 1) {
            const predicted = slope * i + intercept;
            out[i].trend = logScale ? Math.exp(predicted) : predicted;
          }
        }
      }
    }
    return out;
  }, [rows, showAthLine, showMovingAverage, movingAverageWindow, logScale]);
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
          {statsBar.map((s, idx) => (
            <span key={`s-${s.label}-${idx}`} style={{ color: 'var(--t2)' }}>
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
                scale={logScale ? 'log' : 'auto'}
                domain={logScale ? ['auto', 'auto'] : undefined}
                allowDataOverflow={logScale}
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
              {!isDrawdownPanel && (
                <Line
                  type="monotone"
                  dataKey="trend"
                  stroke="var(--amber)"
                  strokeOpacity={0.5}
                  strokeDasharray="4 4"
                  strokeWidth={1}
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
type FeesRow = {
  date: string;
  start: number | null;
  margin: number | null;
  lev: number | null;
  invested: number | null;
  trade_vol: number | null;
  taker_fee: number | null;
  funding: number | null;
  end: number | null;
  ret_gross: number | null;
  ret_net: number | null;
  net_pnl: number | null;
  no_entry?: boolean;
  no_entry_reason?: 'filter' | 'conviction_gate';
};

// ── Filter Equity Curve Overlay ──────────────────────────────────────────────
const FILTER_PALETTE = [
  '#00c896', '#378ADD', '#f0a500', '#cc66ff', '#ff6b6b',
  '#00d4ff', '#ffd166', '#ff9f43', '#a29bfe', '#55efc4',
];

function FilterEquityCurveOverlay({
  filters,
  selectedFilter,
  onSelectFilter,
}: {
  filters: FilterRow[];
  selectedFilter: string | null;
  onSelectFilter: (f: string) => void;
}) {
  const [hoveredFilter, setHoveredFilter] = useState<string | null>(null);

  const active = filters.filter(
    (r) => r.equity_curve && (r.equity_curve as Point[]).length > 0 && !r.not_run,
  );
  if (active.length === 0) return null;

  const maxLen = Math.max(...active.map((r) => (r.equity_curve as Point[]).length));

  let globalMin = Infinity;
  let globalMax = -Infinity;
  for (const row of active) {
    for (const p of row.equity_curve as Point[]) {
      const v = typeof p === 'number' ? p : (p as { y: number }).y;
      if (Number.isFinite(v)) { globalMin = Math.min(globalMin, v); globalMax = Math.max(globalMax, v); }
    }
  }
  if (!Number.isFinite(globalMin)) return null;
  const rangePad = (globalMax - globalMin) * 0.08 || 0.05;
  const yMin = globalMin - rangePad;
  const yMax = globalMax + rangePad;

  const W = 600; const H = 140;
  const padL = 52; const padR = 12; const padT = 10; const padB = 22;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  const toX = (i: number) => padL + (i / Math.max(maxLen - 1, 1)) * plotW;
  const toY = (v: number) => padT + plotH - ((v - yMin) / (yMax - yMin)) * plotH;
  const yBase = toY(1.0);

  const yTicks: number[] = [0, 1, 2, 3, 4].map((t) => yMin + t * (yMax - yMin) / 4);
  const fmtPct = (v: number) => { const p = (v - 1) * 100; return `${p >= 0 ? '+' : ''}${p.toFixed(0)}%`; };

  return (
    <div style={{ marginTop: 8, border: '1px solid var(--line)', borderRadius: 3, background: 'var(--bg1)', overflow: 'hidden' }}>
      <div style={{ padding: '6px 10px', borderBottom: '1px solid var(--line)', display: 'flex', justifyContent: 'flex-end' }}>
        <span style={{ fontSize: 9, color: 'var(--t2)', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
          {active.length} filter{active.length !== 1 ? 's' : ''} · normalized returns
        </span>
      </div>
      <div style={{ padding: '8px 10px 6px 10px' }}>
        <svg
          viewBox={`0 0 ${W} ${H}`}
          style={{ width: '100%', display: 'block' }}
          onMouseLeave={() => setHoveredFilter(null)}
        >
          {yTicks.map((v, i) => (
            <line key={`yg-${i}`} x1={padL} y1={toY(v)} x2={W - padR} y2={toY(v)} stroke="rgba(255,255,255,0.04)" />
          ))}
          {yBase >= padT && yBase <= H - padB && (
            <line x1={padL} y1={yBase} x2={W - padR} y2={yBase} stroke="rgba(255,255,255,0.18)" strokeDasharray="3 3" />
          )}
          {active.map((row, ci) => {
            const label = String(row.filter ?? '');
            const color = FILTER_PALETTE[ci % FILTER_PALETTE.length];
            const isSel = selectedFilter != null && normalizeFilterLabel(label) === normalizeFilterLabel(selectedFilter);
            const isHov = hoveredFilter != null && normalizeFilterLabel(label) === normalizeFilterLabel(hoveredFilter);
            const hasHighlight = selectedFilter != null || hoveredFilter != null;
            const opacity = hasHighlight ? (isSel || isHov ? 1 : 0.12) : 0.65;
            const sw = isSel || isHov ? 1.5 : 0.75;
            const curve = row.equity_curve as Point[];
            const pts = curve
              .map((p, i) => {
                const v = typeof p === 'number' ? p : (p as { y: number }).y;
                if (!Number.isFinite(v)) return null;
                return `${toX(i).toFixed(1)},${toY(v).toFixed(1)}`;
              })
              .filter(Boolean)
              .join(' ');
            return (
              <g key={`${label}-${ci}`} style={{ cursor: 'pointer' }}
                onClick={() => onSelectFilter(label)}
                onMouseEnter={() => setHoveredFilter(label)}
              >
                <polyline points={pts} stroke={color} strokeWidth={sw} fill="none" opacity={opacity} />
              </g>
            );
          })}
          <line x1={padL} y1={padT} x2={padL} y2={H - padB} stroke="var(--line2)" />
          {yTicks.map((v, i) => (
            <text key={`yt-${i}`} x={padL - 4} y={toY(v) + 3} textAnchor="end" fontSize={7} fill="var(--t3)" fontFamily="var(--font-space-mono), Space Mono, monospace">
              {fmtPct(v)}
            </text>
          ))}
          {[0, 0.25, 0.5, 0.75, 1].map((frac) => {
            const x = padL + frac * plotW;
            const day = Math.round(frac * (maxLen - 1));
            return (
              <g key={`xt-${frac}`}>
                <line x1={x} y1={H - padB} x2={x} y2={H - padB + 3} stroke="var(--line2)" />
                <text x={x} y={H - padB + 11} textAnchor="middle" fontSize={7} fill="var(--t3)" fontFamily="var(--font-space-mono), Space Mono, monospace">
                  Day {day}
                </text>
              </g>
            );
          })}
        </svg>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 12px', marginTop: 6, paddingLeft: padL }}>
          {active.map((row, ci) => {
            const label = String(row.filter ?? '');
            const color = FILTER_PALETTE[ci % FILTER_PALETTE.length];
            const isSel = selectedFilter != null && normalizeFilterLabel(label) === normalizeFilterLabel(selectedFilter);
            const isHov = hoveredFilter != null && normalizeFilterLabel(label) === normalizeFilterLabel(hoveredFilter);
            const shortLabel = label.replace(/^A\s*-\s*/i, '');
            const sharpe = typeof row.sharpe === 'number' && Number.isFinite(row.sharpe) ? row.sharpe.toFixed(2) : null;
            return (
              <button
                key={`${label}-${ci}`}
                onClick={() => onSelectFilter(label)}
                onMouseEnter={() => setHoveredFilter(label)}
                onMouseLeave={() => setHoveredFilter(null)}
                style={{
                  display: 'flex', alignItems: 'center', gap: 5,
                  background: 'none', border: 'none', cursor: 'pointer', padding: 0,
                  opacity: (selectedFilter || hoveredFilter) ? (isSel || isHov ? 1 : 0.35) : 1,
                }}
              >
                <span style={{ width: 14, height: 2, background: color, display: 'inline-block', borderRadius: 1, flexShrink: 0 }} />
                <span style={{ fontSize: 8, color: isSel || isHov ? 'var(--t0)' : 'var(--t2)', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
                  {shortLabel}{sharpe ? ` · SR ${sharpe}` : ''}
                </span>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

type FeesRowWithCumulative = FeesRow & {
  cum_trade_vol: number;
  cum_fees: number;
  cum_pnl: number;
};

function normalizeIntradaySeriesToDayReturn(
  bars: Array<number | null>,
  targetReturnPct: number | null | undefined,
  exitBar: number | null | undefined,
): Array<number | null> {
  if (!Array.isArray(bars) || bars.length === 0) return [];
  const finiteVals = bars.filter((v): v is number => v !== null && Number.isFinite(v));
  if (finiteVals.length === 0) return bars.map(() => null);
  const idx = typeof exitBar === 'number' && Number.isFinite(exitBar)
    ? Math.max(0, Math.min(bars.length - 1, Math.floor(exitBar)))
    : bars.length - 1;
  let endpoint: number | null = null;
  for (let i = idx; i >= 0; i -= 1) {
    const v = bars[i];
    if (v !== null && Number.isFinite(v)) {
      endpoint = v;
      break;
    }
  }
  if (!Number.isFinite(endpoint) || endpoint === null || Math.abs(endpoint) < 1e-12 || targetReturnPct === null || targetReturnPct === undefined || !Number.isFinite(targetReturnPct)) {
    return bars;
  }
  // Intraday payload units can vary by run (e.g., pct points vs centi-pct).
  // Pick the scale that best matches the day's realized return endpoint.
  const candidates = [1, 0.1, 0.01, 0.001, 0.0001, 10, 100, 1000];
  let bestScale = 1;
  let bestErr = Number.POSITIVE_INFINITY;
  for (const s of candidates) {
    const err = Math.abs((endpoint * s) - targetReturnPct);
    if (err < bestErr) {
      bestErr = err;
      bestScale = s;
    }
  }
  return bars.map((v) => (v === null || !Number.isFinite(v) ? null : v * bestScale));
}

function DailyReturnOverlapChart({
  rows,
  intradayBars,
  intradayExitBars,
}: {
  rows: FeesRow[];
  intradayBars?: Record<string, (number | null)[]> | null;
  intradayExitBars?: Record<string, number> | null;
}) {
  const [hovered, setHovered] = useState<{ date: string; ret: number; cx: number; cy: number } | null>(null);
  const [mode, setMode] = useState<'poly' | 'linear'>('poly');
  const [isOpen, setIsOpen] = useState(true);
  const [zoomLevel, setZoomLevel] = useState(5); // 1=zoomed out, 10=zoomed in
  const [fullscreen, setFullscreen] = useState(false);
  const [sliderTrackH, setSliderTrackH] = useState(90);
  const sliderContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!fullscreen) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setFullscreen(false); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [fullscreen]);

  useEffect(() => {
    const el = sliderContainerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      // subtract button heights (~8px each) + gaps (5px each) + padding (4px total)
      const h = Math.max(20, el.getBoundingClientRect().height - 8 - 8 - 10 - 4);
      setSliderTrackH(h);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const activeDays = rows.filter((r) => !r.no_entry && r.ret_net !== null && Number.isFinite(r.ret_net));
  if (activeDays.length === 0) return null;

  const hasBarData = intradayBars != null && Object.keys(intradayBars).length > 0;
  const returns = activeDays.map((r) => r.ret_net as number);
  const intradayByDate: Record<string, Array<number | null>> = {};
  if (hasBarData && intradayBars) {
    for (const day of activeDays) {
      const raw = intradayBars[day.date];
      if (!raw || raw.length < 2) continue;
      const target = (typeof day.ret_gross === 'number' && Number.isFinite(day.ret_gross))
        ? day.ret_gross
        : day.ret_net;
      const exitBar = intradayExitBars?.[day.date];
      intradayByDate[day.date] = normalizeIntradaySeriesToDayReturn(raw, target, exitBar);
    }
  }
  const intradayMatchedDays = Object.keys(intradayByDate).length;
  const usePolylines = mode === 'poly' && intradayMatchedDays > 0;

  // Scale y-axis to the 98th percentile of absolute values to avoid outliers
  // dominating the range. Outlier paths are still drawn but clipped at the edge.
  let absMax = 0.5;
  {
    const allVals: number[] = [];
    if (usePolylines) {
      for (const day of activeDays) {
        const bars = intradayByDate[day.date];
        if (!bars) continue;
        for (const v of bars) {
          if (v !== null && Number.isFinite(v)) allVals.push(Math.abs(v));
        }
      }
    } else {
      for (const r of returns) {
        if (Number.isFinite(r)) allVals.push(Math.abs(r));
      }
    }
    if (allVals.length > 0) {
      allVals.sort((a, b) => a - b);
      const p98idx = Math.floor(allVals.length * 0.98);
      absMax = Math.max(allVals[p98idx] ?? allVals[allVals.length - 1], 0.5);
    }
  }
  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const winDays = returns.filter((r) => r > 0).length;
  const lossDays = returns.filter((r) => r <= 0).length;

  const W = 600; const H = 160;
  const padL = 44; const padR = 12; const padT = 12; const padB = 26;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;
  // zoomLevel 1–10: equal steps from 1× (factor 1.0) to 4× (factor 0.25)
  const zoomFactor = 1 - (zoomLevel - 1) * (0.75 / 9); // 1→1.0, 2→0.917, …, 10→0.25
  const yRange = absMax * 1.2 * zoomFactor;

  const toX = (h: number) => padL + (h / 18) * plotW;
  const toY = (pct: number) => padT + plotH / 2 - (pct / yRange) * (plotH / 2);
  const yZero = toY(0);

  const xTicks = [0, 3, 6, 9, 12, 15, 18];
  const xLabels = ['06:00', '09:00', '12:00', '15:00', '18:00', '21:00', '00:00'];
  const yTickVals = [-yRange * 0.75, -yRange * 0.25, 0, yRange * 0.25, yRange * 0.75];

  const fsOverlay: React.CSSProperties = fullscreen ? {
    position: 'fixed', inset: 0, zIndex: 200,
    background: '#000',
    display: 'flex', flexDirection: 'column',
  } : {};

  return (
    <>
    {fullscreen && <div style={{ position: 'fixed', inset: 0, zIndex: 199, background: 'rgba(0,0,0,0.75)' }} onClick={() => setFullscreen(false)} />}
    <div style={{ border: '1px solid var(--line)', borderRadius: fullscreen ? 0 : 3, overflow: 'hidden', background: '#000', flexShrink: 0, ...fsOverlay }}>
      <div
        onClick={() => setIsOpen((v) => !v)}
        style={{
          padding: '8px 10px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          cursor: 'pointer',
          userSelect: 'none',
          background: '#000',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span
            style={{
              fontSize: 10,
              color: 'var(--t2)',
              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
              width: 10,
              display: 'inline-block',
            }}
          >
            {isOpen ? '▾' : '▸'}
          </span>
          <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
            Daily Return Overlay — 06:00 → 00:00 UTC
          </span>
          {!hasBarData
            ? <span style={{ fontSize: 9, color: 'var(--t3)', fontWeight: 400 }}>· straight-line approx — re-run for intraday paths</span>
            : (
              <div style={{ display: 'flex', border: '1px solid var(--line2)', borderRadius: 3, overflow: 'hidden' }}>
                {(['poly', 'linear'] as const).map((m) => (
                  <button
                    key={m}
                    onClick={(e) => {
                      e.stopPropagation();
                      setMode(m);
                    }}
                    style={{
                      padding: '2px 7px',
                      fontSize: 8,
                      fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                      letterSpacing: '0.08em',
                      textTransform: 'uppercase',
                      background: mode === m ? 'var(--bg4)' : 'transparent',
                      color: mode === m ? 'var(--t0)' : 'var(--t3)',
                      border: 'none',
                      cursor: 'pointer',
                      borderRight: m === 'poly' ? '1px solid var(--line2)' : 'none',
                    }}
                  >
                    {m}
                  </button>
                ))}
              </div>
            )
          }
          {hasBarData && mode === 'poly' && intradayMatchedDays === 0 && (
            <span style={{ fontSize: 9, color: 'var(--amber)', fontWeight: 400 }}>
              · no intraday/day-date matches for selected filter — falling back to linear
            </span>
          )}
        </div>
        <span style={{ display: 'flex', alignItems: 'center', gap: 12, fontSize: 9, color: 'var(--t2)', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
          <span style={{ color: 'var(--green)' }}>{winDays}W</span>
          <span style={{ color: 'var(--red)' }}>{lossDays}L</span>
          <span>{((winDays / returns.length) * 100).toFixed(1)}% win</span>
          <span>μ {mean >= 0 ? '+' : ''}{mean.toFixed(3)}%</span>
          <span style={{ color: 'var(--t3)' }}>{activeDays.length} days</span>
          <button
            onClick={(e) => { e.stopPropagation(); setFullscreen((v) => !v); }}
            title="Fullscreen"
            style={{
              background: 'none', border: 'none', cursor: 'pointer', padding: '0 2px',
              color: 'var(--t2)', display: 'flex', alignItems: 'center',
            }}
          >
            <svg width="11" height="11" viewBox="0 0 11 11" fill="none" xmlns="http://www.w3.org/2000/svg">
              <path d="M1 4V1H4M7 1H10V4M10 7V10H7M4 10H1V7" stroke="currentColor" strokeWidth="1.2" strokeLinecap="square"/>
            </svg>
          </button>
        </span>
      </div>
      {(isOpen || fullscreen) && (
      <div style={{ padding: '10px 10px 6px 10px', position: 'relative', background: '#000', display: 'flex', alignItems: 'stretch', gap: 6, ...(fullscreen ? { flex: 1 } : {}) }}>
        <svg
          viewBox={`0 0 ${W} ${H}`}
          style={{ width: '100%', display: 'block', flex: 1, overflow: 'hidden', ...(fullscreen ? { height: '100%' } : {}) }}
          onMouseLeave={() => setHovered(null)}
          preserveAspectRatio={fullscreen ? 'none' : 'xMidYMid meet'}
        >
          {xTicks.map((h) => (
            <line
              key={`xg-${h}`}
              x1={toX(h)} y1={padT} x2={toX(h)} y2={H - padB}
              stroke={h === 0 ? 'var(--line2)' : 'rgba(255,255,255,0.04)'}
              strokeDasharray={h === 0 ? undefined : '2 3'}
            />
          ))}
          <line x1={padL} y1={padT} x2={padL} y2={H - padB} stroke="var(--line2)" />
          <line x1={padL} y1={yZero} x2={W - padR} y2={yZero} stroke="rgba(255,255,255,0.2)" strokeDasharray="3 3" />
          {activeDays.map((row, i) => {
            const ret = row.ret_net as number;
            const isHov = hovered?.date === row.date;
            const col = ret >= 0
              ? `rgba(0,200,150,${isHov ? 0.9 : 0.28})`
              : `rgba(255,77,77,${isHov ? 0.9 : 0.28})`;
            const bars = usePolylines ? intradayByDate[row.date] : null;
            if (bars && bars.length > 1) {
              const rawExitBar = intradayExitBars?.[row.date];
              // exitBar === -1 means early-kill fired before entry (no trade placed).
              // exitBar >= 0 means position was opened and closed at that bar.
              // undefined means exit bars not loaded yet — show full path.
              const noTrade = rawExitBar === -1;
              const exitBar = (rawExitBar === undefined || rawExitBar === null) ? (bars.length - 1) : Math.max(rawExitBar, 0);
              const activeBars = noTrade ? [] : bars.slice(0, exitBar + 1);
              const n = bars.length;
              // No-trade day: early kill fired before entry — show flat 0% dashed line
              if (noTrade) {
                return (
                  <line
                    key={`d-${row.date}-${i}`}
                    x1={toX(0)} y1={toY(0)}
                    x2={toX(18)} y2={toY(0)}
                    stroke="rgba(255,255,255,0.10)"
                    strokeWidth={0.5}
                    strokeDasharray="2 3"
                    style={{ cursor: 'crosshair' }}
                    onMouseEnter={(e) => setHovered({ date: row.date, ret, cx: e.clientX, cy: e.clientY })}
                  />
                );
              }
              const pts = activeBars
                .map((v, bi) => {
                  if (v === null || !Number.isFinite(v)) return null;
                  return `${toX((bi / (n - 1)) * 18).toFixed(1)},${toY(v).toFixed(1)}`;
                })
                .filter(Boolean)
                .join(' ');
              // Find the last valid exit value for the flat tail
              const exitVal = (() => {
                for (let k = activeBars.length - 1; k >= 0; k--) {
                  const v = activeBars[k];
                  if (v !== null && Number.isFinite(v)) return v;
                }
                return null;
              })();
              const hasEarlyExit = exitBar < n - 1 && exitVal !== null;
              return (
                <g key={`d-${row.date}-${i}`}
                  style={{ cursor: 'crosshair' }}
                  onMouseEnter={(e) => setHovered({ date: row.date, ret, cx: e.clientX, cy: e.clientY })}
                >
                  <polyline points={pts} stroke={col} strokeWidth={isHov ? 1.5 : 0.75} fill="none" />
                  {hasEarlyExit && (
                    <line
                      x1={toX((exitBar / (n - 1)) * 18)}
                      y1={toY(exitVal!)}
                      x2={toX(18)}
                      y2={toY(exitVal!)}
                      stroke={col}
                      strokeWidth={isHov ? 1 : 0.5}
                      strokeDasharray="2 3"
                    />
                  )}
                </g>
              );
            }
            return (
              <line
                key={`d-${row.date}-${i}`}
                x1={toX(0)} y1={yZero}
                x2={toX(18)} y2={toY(ret)}
                stroke={col}
                strokeWidth={isHov ? 1.5 : 0.75}
                style={{ cursor: 'crosshair' }}
                onMouseEnter={(e) => setHovered({ date: row.date, ret, cx: e.clientX, cy: e.clientY })}
              />
            );
          })}
          <line
            x1={toX(0)} y1={yZero}
            x2={toX(18)} y2={toY(mean)}
            stroke="rgba(255,255,255,0.45)"
            strokeWidth={1}
            strokeDasharray="5 3"
          />
          {xTicks.map((h, i) => (
            <g key={`xt-${h}`}>
              <line x1={toX(h)} y1={H - padB} x2={toX(h)} y2={H - padB + 3} stroke="var(--line2)" />
              <text x={toX(h)} y={H - padB + 12} textAnchor="middle" fontSize={7.5} fill="var(--t3)" fontFamily="var(--font-space-mono), Space Mono, monospace">
                {xLabels[i]}
              </text>
            </g>
          ))}
          {yTickVals.map((v) => (
            <text key={`yt-${v}`} x={padL - 4} y={toY(v) + 3} textAnchor="end" fontSize={7.5} fill="var(--t3)" fontFamily="var(--font-space-mono), Space Mono, monospace">
              {v === 0 ? '0%' : `${v >= 0 ? '+' : ''}${Math.round(v)}%`}
            </text>
          ))}
        </svg>
        {/* Custom vertical zoom slider */}
        {(() => {
          const trackH = sliderTrackH;
          const pip = 1 - (zoomLevel - 1) / 9; // 0 = bottom (zoom out), 1 = top (zoom in)
          const pipY = Math.round(pip * trackH);
          return (
            <div
              ref={sliderContainerRef}
              style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 5, flexShrink: 0, paddingTop: 2, paddingBottom: 2, userSelect: 'none', alignSelf: 'stretch' }}
              title={`Zoom: ${zoomLevel}/10`}
            >
              <button
                onClick={() => setZoomLevel((v) => Math.min(10, v + 1))}
                style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'var(--t3)', lineHeight: 1, fontSize: 9, display: 'flex' }}
              >
                <svg width="8" height="8" viewBox="0 0 8 8" fill="none"><path d="M4 1V7M1 4H7" stroke="currentColor" strokeWidth="1.2" strokeLinecap="square"/></svg>
              </button>
              <div
                style={{ position: 'relative', width: 14, flex: 1, cursor: 'ns-resize' }}
                onMouseDown={(e) => {
                  e.preventDefault();
                  const rect = e.currentTarget.getBoundingClientRect();
                  const move = (ev: MouseEvent) => {
                    const frac = Math.max(0, Math.min(1, (ev.clientY - rect.top) / trackH));
                    const level = Math.round(10 - frac * 9);
                    setZoomLevel(level);
                  };
                  const up = () => { window.removeEventListener('mousemove', move); window.removeEventListener('mouseup', up); };
                  window.addEventListener('mousemove', move);
                  window.addEventListener('mouseup', up);
                }}
              >
                {/* Track */}
                <div style={{ position: 'absolute', left: '50%', top: 0, transform: 'translateX(-50%)', width: 1, height: trackH, background: 'var(--line2)' }} />
                {/* Tick marks */}
                {[0, 0.25, 0.5, 0.75, 1].map((f) => (
                  <div key={f} style={{ position: 'absolute', left: '50%', top: Math.round(f * trackH), transform: 'translate(-50%, -50%)', width: 4, height: 1, background: 'var(--line2)' }} />
                ))}
                {/* Active fill above pip */}
                <div style={{ position: 'absolute', left: '50%', top: 0, transform: 'translateX(-50%)', width: 1, height: pipY, background: 'var(--green)', opacity: 0.5 }} />
                {/* Pip */}
                <div style={{
                  position: 'absolute',
                  left: '50%', top: pipY,
                  transform: 'translate(-50%, -50%)',
                  width: 5, height: 5,
                  background: 'var(--bg1)',
                  border: '1px solid var(--green)',
                  borderRadius: 0,
                }} />
              </div>
              <button
                onClick={() => setZoomLevel((v) => Math.max(1, v - 1))}
                style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'var(--t3)', lineHeight: 1, fontSize: 9, display: 'flex' }}
              >
                <svg width="8" height="8" viewBox="0 0 8 8" fill="none"><path d="M1 4H7" stroke="currentColor" strokeWidth="1.2" strokeLinecap="square"/></svg>
              </button>
            </div>
          );
        })()}
        {hovered && (
          <div
            style={{
              position: 'fixed',
              left: hovered.cx + 10,
              top: hovered.cy + 10,
              zIndex: 80,
              background: 'var(--bg1)',
              border: '1px solid var(--line2)',
              borderRadius: 3,
              padding: '4px 8px',
              fontSize: 9,
              color: 'var(--t1)',
              pointerEvents: 'none',
              whiteSpace: 'nowrap',
              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
            }}
          >
            <span style={{ color: 'var(--t3)', marginRight: 6 }}>{hovered.date}</span>
            <span style={{ color: hovered.ret >= 0 ? 'var(--green)' : 'var(--red)' }}>
              {hovered.ret >= 0 ? '+' : ''}{hovered.ret.toFixed(3)}%
            </span>
          </div>
        )}
      </div>
      )}
    </div>
    </>
  );
}

type ReportTab = 'summary' | 'breakdown' | 'stress_tests' | 'raw_output' | 'tear_sheet' | 'full_report';

function asNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

// ── Full Report Section Visualizations ───────────────────────────────────────

const MONO: React.CSSProperties = { fontFamily: 'var(--font-space-mono), Space Mono, monospace' };

function sectionLabel(text: string): React.ReactNode {
  return (
    <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 6, ...MONO }}>
      {text}
    </div>
  );
}

// ── Generic parsers ───────────────────────────────────────────────────────────

// Column-position aware table parser — anchors each column to the position where
// the header token starts, then slices data lines at those positions.
function parseColumnarTable(body: string): { headers: string[]; rows: Array<{ label: string; values: string[] }> } | null {
  const rawLines = body.split('\n');
  const lines = rawLines.filter((l) => l.trim() && !/^[-=─═┌┐└┘│]+$/.test(l.trim()));
  if (lines.length < 2) return null;

  // Find the first line that has ≥3 tokens when split by 2+ spaces — that's our header
  let headerIdx = -1;
  let headerLine = '';
  for (let i = 0; i < Math.min(lines.length, 15); i += 1) {
    const tokens = lines[i].split(/\s{2,}/);
    if (tokens.length >= 3) { headerIdx = i; headerLine = lines[i]; break; }
  }
  if (headerIdx < 0) return null;

  // Record start position of each header token
  const headers = headerLine.split(/\s{2,}/).map((h) => h.trim()).filter(Boolean);
  const colStarts: number[] = [];
  let searchFrom = 0;
  for (const h of headers) {
    const idx = headerLine.indexOf(h, searchFrom);
    if (idx < 0) return null;
    colStarts.push(idx);
    searchFrom = idx + h.length;
  }

  const rows: Array<{ label: string; values: string[] }> = [];
  for (let i = headerIdx + 1; i < lines.length; i += 1) {
    const line = lines[i];
    const trimmed = line.trim();
    if (!trimmed || /^[-=─═]+$/.test(trimmed)) continue;
    // Slice at column positions
    const slices: string[] = [];
    for (let ci = 0; ci < colStarts.length; ci += 1) {
      const start = colStarts[ci];
      const end = ci + 1 < colStarts.length ? colStarts[ci + 1] : undefined;
      slices.push((end !== undefined ? line.slice(start, end) : line.slice(start)).trim());
    }
    if (slices.filter(Boolean).length < 2) continue;
    rows.push({ label: slices[0], values: slices.slice(1) });
  }
  return rows.length > 0 ? { headers, rows } : null;
}

// Key-value parser: only picks up lines of the form "Label: numeric_value [optional_suffix]"
function parseKV(body: string): Array<{ key: string; raw: string; num: number }> {
  const out: Array<{ key: string; raw: string; num: number }> = [];
  for (const line of body.split('\n')) {
    const m = line.match(/^[ \t]*([A-Za-z][^:]{1,50}):\s*([+-]?\d[\d.,% ]+)$/);
    if (!m) continue;
    const key = m[1].trim();
    const raw = m[2].trim();
    const num = parseFloat(raw.replace(/[, ]/g, '').replace(/%$/, ''));
    if (!Number.isFinite(num)) continue;
    out.push({ key, raw, num });
  }
  return out;
}

// Numeric grid parser — expects a header row then data rows, all 2+-space separated
function parseNumericGrid(body: string): { rowLabels: string[]; colLabels: string[]; data: (number | null)[][] } | null {
  const lines = body.split('\n').filter((l) => l.trim() && !/^[-=─═┌┐└┘│]+$/.test(l.trim()));
  if (lines.length < 3) return null;
  const colLabels = lines[0].trim().split(/\s{2,}/).map((s) => s.trim());
  if (colLabels.length < 2) return null;
  const rowLabels: string[] = [];
  const data: (number | null)[][] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const tokens = lines[i].trim().split(/\s{2,}/);
    if (tokens.length < 2) continue;
    rowLabels.push(tokens[0]);
    data.push(tokens.slice(1).map((tok) => {
      const n = parseFloat(tok.replace(/[,%]/g, ''));
      return Number.isFinite(n) ? n : null;
    }));
  }
  return data.length > 0 ? { rowLabels, colLabels, data } : null;
}

// ── Generic viz components ────────────────────────────────────────────────────

function PreFallback({ body }: { body: string }) {
  return (
    <pre style={{ margin: 0, whiteSpace: 'pre-wrap', fontSize: 9.5, lineHeight: 1.5, color: 'var(--t1)', ...MONO }}>
      {body}
    </pre>
  );
}

// Horizontal bar chart with auto label width and a zero-baseline for mixed +/−
function SectionHBarChart({ items, colorFn }: {
  items: Array<{ label: string; value: number; raw?: string }>;
  colorFn?: (v: number) => string;
}) {
  if (items.length === 0) return null;
  const color = colorFn ?? ((v: number) => v >= 0 ? 'var(--green)' : 'var(--red)');
  const maxAbs = Math.max(...items.map((i) => Math.abs(i.value)), 1e-9);
  const hasNeg = items.some((i) => i.value < 0);
  const hasPos = items.some((i) => i.value > 0);
  const labelW = Math.min(Math.max(...items.map((i) => i.label.length)) * 7 + 8, 240);
  const valueW = Math.max(...items.map((i) => (i.raw ?? i.value.toFixed(3)).length)) * 7 + 4;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
      {items.map((item, idx) => {
        const frac = Math.abs(item.value) / maxAbs;
        const barLeft = item.value < 0 ? `${(0.5 - frac * 0.5) * 100}%` : '50%';
        const barWidth = `${frac * 50}%`;
        return (
          <div key={`${item.label}-${item.raw ?? item.value}-${idx}`} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 9, ...MONO }}>
            <div style={{ width: labelW, color: 'var(--t2)', flexShrink: 0, textAlign: 'right', paddingRight: 4, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={item.label}>{item.label}</div>
            <div style={{ flex: 1, height: 9, background: 'var(--bg3)', borderRadius: 1, position: 'relative', minWidth: 60 }}>
              <div style={{ position: 'absolute', left: '50%', top: 0, width: 1, height: '100%', background: 'var(--line2)' }} />
              <div style={{ position: 'absolute', left: barLeft, top: 0, height: '100%', width: barWidth, background: color(item.value), borderRadius: 1 }} />
            </div>
            <div style={{ width: valueW, color: color(item.value), textAlign: 'right', flexShrink: 0 }}>{item.raw ?? item.value.toFixed(3)}</div>
          </div>
        );
      })}
    </div>
  );
}

// Clean table with position-parsed columns
function SectionTable({ headers, rows }: { headers: string[]; rows: Array<{ label: string; values: string[] }> }) {
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
        <thead>
          <tr>
            {headers.map((h, i) => (
              <th key={i} style={{ padding: '3px 10px 3px 6px', textAlign: i === 0 ? 'left' : 'right', color: 'var(--t3)', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap' }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri} style={{ background: ri % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent' }}>
              <td style={{ padding: '3px 10px 3px 6px', color: 'var(--t2)', whiteSpace: 'nowrap' }}>{row.label}</td>
              {row.values.map((v, ci) => {
                const stripped = v.replace(/[,%$x]/g, '');
                const n = parseFloat(stripped);
                const isNum = Number.isFinite(n);
                const isNeg = isNum && n < 0;
                const isPos = isNum && n > 0;
                const clr = isNeg ? 'var(--red)' : (isPos && (v.includes('%') || v.includes('x'))) ? 'var(--green)' : 'var(--t1)';
                return (
                  <td key={ci} style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: clr, whiteSpace: 'nowrap' }}>{v || '—'}</td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Heatmap grid with correct green=high, red=low coloring
function HeatmapGrid({ rowLabels, colLabels, data }: {
  rowLabels: string[]; colLabels: string[]; data: (number | null)[][];
}) {
  const allVals = data.flat().filter((v): v is number => v !== null);
  if (allVals.length === 0) return null;
  const min = Math.min(...allVals);
  const max = Math.max(...allVals);

  function cellBg(v: number | null): string {
    if (v === null) return 'transparent';
    const t = max === min ? 0.5 : (v - min) / (max - min); // 0=low(bad), 1=high(good)
    // low → red (#ff4d4d), mid → amber (#f0a500), high → green (#00c896)
    let r: number, g: number, b: number;
    if (t < 0.5) {
      const s = t * 2; // 0→1
      r = Math.round(255 + s * (240 - 255));   // 255→240
      g = Math.round(77  + s * (165 - 77));    // 77→165
      b = Math.round(77  + s * (0   - 77));    // 77→0
    } else {
      const s = (t - 0.5) * 2; // 0→1
      r = Math.round(240 + s * (0   - 240));   // 240→0
      g = Math.round(165 + s * (200 - 165));   // 165→200
      b = Math.round(0   + s * (150 - 0));     // 0→150
    }
    return `rgba(${r},${g},${b},0.55)`;
  }

  function cellText(v: number | null): string {
    if (v === null) return '—';
    return Math.abs(v) >= 10 ? v.toFixed(1) : v.toFixed(2);
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ borderCollapse: 'separate', borderSpacing: 2, fontSize: 8.5, ...MONO }}>
        <thead>
          <tr>
            <th style={{ padding: '2px 8px', color: 'var(--t3)', fontWeight: 400 }} />
            {colLabels.map((c, i) => (
              <th key={i} style={{ padding: '2px 8px', color: 'var(--t2)', fontWeight: 400, textAlign: 'center', whiteSpace: 'nowrap' }}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rowLabels.map((rl, ri) => (
            <tr key={ri}>
              <td style={{ padding: '2px 8px', color: 'var(--t2)', whiteSpace: 'nowrap', textAlign: 'right' }}>{rl}</td>
              {(data[ri] ?? []).map((v, ci) => (
                <td key={ci} style={{ padding: '3px 8px', textAlign: 'center', background: cellBg(v), borderRadius: 2, color: v !== null ? 'rgba(255,255,255,0.9)' : 'var(--t3)', fontWeight: v !== null && Math.abs(v) === Math.max(...allVals.map(Math.abs)) ? 700 : 400 }}>
                  {cellText(v)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Pure SVG sparkline — no recharts overhead, handles simple series cleanly
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function Sparkline({ series, color = 'var(--green)', height = 80, showZero = false }: {
  series: Array<{ x: number; y: number }>; color?: string; height?: number; showZero?: boolean;
}) {
  if (series.length < 2) return null;
  const W = 600; const H = height;
  const pad = { t: 6, r: 4, b: 4, l: 4 };
  const ys = series.map((p) => p.y);
  const yMin = Math.min(...ys);
  const yMax = Math.max(...ys);
  const yRange = yMax - yMin || 1;
  const toX = (i: number) => pad.l + (i / (series.length - 1)) * (W - pad.l - pad.r);
  const toY = (v: number) => pad.t + (1 - (v - yMin) / yRange) * (H - pad.t - pad.b);
  const pts = series.map((p, i) => `${toX(i).toFixed(1)},${toY(p.y).toFixed(1)}`).join(' ');
  const zeroY = toY(0);
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height, display: 'block' }}>
      {showZero && yMin < 0 && yMax > 0 && (
        <line x1={pad.l} y1={zeroY} x2={W - pad.r} y2={zeroY} stroke="var(--line2)" strokeWidth={0.8} />
      )}
      <polyline points={pts} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

// ── Per-section renderers ─────────────────────────────────────────────────────

function renderTableSection(body: string, label?: string) {
  const table = parseColumnarTable(body);
  if (!table || table.rows.length === 0) return <PreFallback body={body} />;
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {label && sectionLabel(label)}
      <SectionTable headers={table.headers} rows={table.rows} />
    </div>
  );
}

function renderKVSection(body: string, label: string, colorFn?: (v: number) => string) {
  const kv = parseKV(body);
  if (kv.length === 0) return renderTableSection(body, label);
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {sectionLabel(label)}
      <SectionHBarChart items={kv.map((k) => ({ label: k.key, value: k.num, raw: k.raw }))} colorFn={colorFn} />
    </div>
  );
}

function renderReturnRatesByPeriod(body: string) {
  return <PreFallback body={body} />;
}

function renderReturnDistribution(body: string) {
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
  return renderKVSection(body, 'Distribution Stats', (v) => v >= 0 ? 'var(--green)' : 'var(--red)');
}

function renderReturnConditional(body: string) {
  return renderTableSection(body);
}

function renderRollingMaxDrawdown(body: string) {
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
  return <PreFallback body={body} />;
}

function renderDrawdownEpisodes(body: string) {
  const lines = body.split('\n');
  const metrics: { label: string; valueRaw: string }[] = [];
  const episodes: { num: string; depth: string; dur: string; rec: string; total: string; recovered: string }[] = [];
  
  let inTable = false;
  for (const line of lines) {
    if (/^[┌└─=═]{5,}/.test(line)) continue;
    
    if (/TOP \d+ WORST DRAWDOWN EPISODES/i.test(line)) {
      inTable = true;
      continue;
    }
    if (inTable && (/(Depth\s+Dur|----)/i.test(line))) continue;

    if (!inTable) {
      const match = line.match(/^\s*│?\s*([A-Za-z0-9 %]+?):\s+([+-]?\d+(?:\.\d+)?[%]?|NaN|N\/A|n\/a)\s*$/i);
      if (match) {
        metrics.push({
          label: match[1].trim(),
          valueRaw: match[2].trim(),
        });
      }
    } else {
      const t = line.replace(/^[│|]\s*/, '').trim();
      if (!t) continue;
      
      const parts = t.split(/\s{2,}/);
      if (parts.length >= 6) {
        episodes.push({
          num: parts[0],
          depth: parts[1],
          dur: parts[2],
          rec: parts[3],
          total: parts[4],
          recovered: parts[5],
        });
      }
    }
  }

  if (metrics.length === 0 && episodes.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {sectionLabel('Drawdown Episode Analysis')}
      
      {metrics.length > 0 && (
        <div style={{ 
          display: 'grid', 
          gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', 
          gap: 12,
        }}>
          {metrics.map((m, idx) => {
            let color = 'var(--t1)';
            const valNum = Number(m.valueRaw.replace(/[%]/g, ''));
            
            if (Number.isFinite(valNum)) {
              if (m.label.toLowerCase().includes('depth')) {
                if (valNum < -30) color = 'var(--red)';
                else if (valNum < -10) color = 'var(--orange)';
              } else if (m.label.toLowerCase().includes('underwater')) {
                if (valNum > 50) color = 'var(--red)';
                else if (valNum > 20) color = 'var(--orange)';
                else color = 'var(--green)';
              }
            }

            return (
              <div key={idx} style={{
                background: 'var(--bg2)',
                border: '1px solid var(--line1)',
                borderRadius: 6,
                padding: '12px 16px',
                display: 'flex',
                flexDirection: 'column',
                gap: 4,
                boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.02)',
              }}>
                <div style={{ fontSize: 9.5, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
                  {m.label}
                </div>
                <div style={{ fontSize: 18, color, fontWeight: 500, letterSpacing: -0.5, ...MONO }}>
                  {m.valueRaw}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {episodes.length > 0 && (
        <div style={{ 
          background: 'var(--bg1)', 
          border: '1px solid var(--line2)', 
          borderRadius: 6, 
          overflow: 'hidden' 
        }}>
          <div style={{ 
            display: 'grid', 
            gridTemplateColumns: '40px 1fr 1fr 1fr 1fr 80px', 
            background: 'var(--bg2)', 
            padding: '8px 12px',
            borderBottom: '1px solid var(--line2)',
            fontSize: 9.5,
            color: 'var(--t3)',
            textTransform: 'uppercase',
            letterSpacing: 0.5,
            ...MONO
          }}>
            <div>#</div>
            <div>Depth</div>
            <div>Dur(d)</div>
            <div>Rec(d)</div>
            <div>Total(d)</div>
            <div style={{ textAlign: 'center' }}>Status</div>
          </div>
          
          <div style={{ display: 'flex', flexDirection: 'column' }}>
            {episodes.map((ep, idx) => {
              const isNo = ep.recovered.toUpperCase() === 'NO';
              const isYes = ep.recovered.toUpperCase() === 'YES';
              return (
                <div key={idx} style={{ 
                  display: 'grid', 
                  gridTemplateColumns: '40px 1fr 1fr 1fr 1fr 80px', 
                  padding: '10px 12px',
                  borderBottom: idx === episodes.length - 1 ? 'none' : '1px solid var(--line1)',
                  fontSize: 11,
                  color: 'var(--t2)',
                  alignItems: 'center',
                  ...MONO
                }}>
                  <div style={{ color: 'var(--t4)' }}>{ep.num}</div>
                  <div style={{ color: 'var(--red)', fontWeight: 500 }}>{ep.depth}</div>
                  <div>{ep.dur}</div>
                  <div style={{ color: ep.rec.toLowerCase().includes('open') ? 'var(--t4)' : 'inherit', fontStyle: ep.rec.toLowerCase().includes('open') ? 'italic' : 'normal' }}>
                    {ep.rec}
                  </div>
                  <div>{ep.total}</div>
                  <div style={{ display: 'flex', justifyContent: 'center' }}>
                    <div style={{
                      padding: '2px 8px',
                      borderRadius: 12,
                      fontSize: 8.5,
                      fontWeight: 600,
                      background: isNo ? 'rgba(255, 60, 60, 0.15)' : isYes ? 'rgba(60, 255, 100, 0.15)' : 'var(--bg3)',
                      color: isNo ? 'var(--red)' : isYes ? 'var(--green)' : 'var(--t2)',
                    }}>
                      {ep.recovered}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

function renderRiskAdjustedQuality(body: string) {
  const lines = body.split('\n');
  const metrics = [];
  for (const line of lines) {
    const match = line.match(/^\s*│?\s*([A-Za-z0-9 -]+?):\s+([+-]?\d+(?:\.\d+)?|NaN|N\/A|n\/a|inf|-inf)\s*(.*)?$/i);
    if (match) {
      metrics.push({
        label: match[1].trim(),
        valueRaw: match[2].trim(),
        value: Number(match[2].replace(/,/g, '')),
        note: match[3]?.replace(/[()│\\]/g, '').replace(/lower\s*=\s*better/i, '').trim() || '',
      });
    }
  }

  if (metrics.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {sectionLabel('Risk-Adjusted Return Quality')}
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', 
        gap: 12,
        paddingBottom: 8
      }}>
        {metrics.map((m, idx) => {
          let color = 'var(--t1)';
          let bgColor = 'var(--bg2)';
          let borderColor = 'var(--line1)';
          
          const isNum = Number.isFinite(m.value);

          if (isNum) {
            if (m.label.toLowerCase().includes('ulcer')) {
              if (m.value > 20) color = 'var(--red)';
              else if (m.value > 5) color = 'var(--orange)';
              else color = 'var(--green)';
            } else if (m.label.toLowerCase().includes('profit') || m.label.toLowerCase().includes('omega')) {
              if (m.value > 1) color = 'var(--green)';
              else if (m.value < 1) color = 'var(--red)';
            } else {
              if (m.value >= 0.5) color = 'var(--green)';
              else if (m.value <= 0) color = 'var(--red)';
            }
          }

          const hasPlus = isNum && m.value > 0 && !m.label.toLowerCase().includes('ulcer') && !m.label.toLowerCase().includes('profit') && !m.label.toLowerCase().includes('omega');

          return (
            <div key={idx} style={{
              background: bgColor,
              border: `1px solid ${borderColor}`,
              borderRadius: 6,
              padding: '14px 18px',
              display: 'flex',
              flexDirection: 'column',
              gap: 6,
              boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.02)',
              transition: 'all 0.2s ease',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = 'var(--t3)';
              e.currentTarget.style.background = 'var(--bg3)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = borderColor;
              e.currentTarget.style.background = bgColor;
            }}
            >
              <div style={{ fontSize: 9.5, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
                {m.label}
              </div>
              <div style={{ fontSize: 20, color, fontWeight: 500, letterSpacing: -0.5, ...MONO }}>
                {hasPlus ? '+' : ''}{isNum ? m.value.toFixed(3) : m.valueRaw}
              </div>
              {m.note && (
                <div style={{ fontSize: 8.5, color: 'var(--t4)', fontStyle: 'italic', ...MONO }}>
                  {m.note}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderDailyVarCvar(body: string) {
  const lines = body.split('\n');
  const items: { label: string; pct: string; value: string }[] = [];

  for (const line of lines) {
    if (/^[┌└─=═]{5,}/.test(line)) continue;
    const regex = /([A-Za-z]+)\(\s*(\d+%)\s*\):\s*([+-]?\d+(?:\.\d+)?[%]?|NaN|N\/A|n\/a|inf|-inf)/gi;
    let match;
    while ((match = regex.exec(line)) !== null) {
      items.push({
        label: match[1],
        pct: match[2],
        value: match[3],
      });
    }
  }

  if (items.length > 0) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        {sectionLabel('Daily VaR / CVaR')}
        <div style={{ 
          display: 'grid', 
          gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', 
          gap: 12,
          paddingBottom: 8
        }}>
          {items.map((m, idx) => (
            <div key={idx} style={{
              background: 'var(--bg2)',
              border: '1px solid rgba(255, 60, 60, 0.15)',
              borderRadius: 6,
              padding: '14px 18px',
              display: 'flex',
              flexDirection: 'column',
              gap: 6,
              boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.02)',
              transition: 'all 0.2s ease',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = 'rgba(255, 60, 60, 0.4)';
              e.currentTarget.style.background = 'var(--bg3)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = 'rgba(255, 60, 60, 0.15)';
              e.currentTarget.style.background = 'var(--bg2)';
            }}
            >
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
                <span style={{ fontSize: 10, color: 'var(--t2)', textTransform: 'uppercase', letterSpacing: 0.5, fontWeight: 500, ...MONO }}>
                  {m.label}
                </span>
                <span style={{ fontSize: 8.5, color: 'var(--red)', opacity: 0.7, ...MONO }}>
                  {m.pct}
                </span>
              </div>
              <div style={{ fontSize: 20, color: 'var(--red)', fontWeight: 500, letterSpacing: -0.5, ...MONO }}>
                {m.value}
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
  return <PreFallback body={body} />;
}

function renderRuinProbability(body: string) {
  const lines = body.split('\n');
  const metadata: { label: string; value: string }[] = [];
  const rows: { threshold: string; pruin: string; onein: string; verdict: string }[] = [];

  for (const line of lines) {
    if (/[=─_]{5,}/.test(line)) continue;
    if (/Threshold\s+P\(ruin\)/i.test(line)) continue;
    if (/RUIN PROBABILITY/i.test(line)) continue;

    const metaMatch = line.match(/^\s*([A-Za-z0-9 ]+?)\s*:\s*(.+)$/);
    if (metaMatch && !line.includes('1-in-') && !line.includes('%')) { 
      metadata.push({ label: metaMatch[1].trim(), value: metaMatch[2].trim() });
      continue;
    }

    const t = line.trim();
    if (!t) continue;

    const parts = t.split(/\s{2,}/);
    if (parts.length >= 4) {
      rows.push({
        threshold: parts[0],
        pruin: parts[1],
        onein: parts[2],
        verdict: parts.slice(3).join(' ').trim(),
      });
    }
  }

  if (metadata.length === 0 && rows.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {sectionLabel('Ruin Probability')}
      
      {metadata.length > 0 && (
        <div style={{ display: 'flex', gap: 24, padding: '8px 12px', background: 'var(--bg2)', borderRadius: 6, border: '1px solid var(--line1)' }}>
          {metadata.map((m, idx) => (
            <div key={idx} style={{ display: 'flex', gap: 6, fontSize: 11, ...MONO }}>
              <span style={{ color: 'var(--t3)' }}>{m.label}:</span>
              <span style={{ color: 'var(--t1)' }}>{m.value}</span>
            </div>
          ))}
        </div>
      )}

      {rows.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {rows.map((r, idx) => {
            const isHigh = r.verdict.toUpperCase().includes('HIGH');
            const isWarn = r.verdict.toUpperCase().includes('WARN') || r.verdict.toUpperCase().includes('MODERATE');
            
            let borderColor = 'var(--line1)';
            let bgColor = 'var(--bg2)';
            let accentColor = 'var(--t1)';
            
            if (isHigh) {
              borderColor = 'rgba(255, 60, 60, 0.3)';
              bgColor = 'rgba(255, 60, 60, 0.05)';
              accentColor = 'var(--red)';
            } else if (isWarn) {
              borderColor = 'rgba(255, 160, 60, 0.3)';
              bgColor = 'rgba(255, 160, 60, 0.05)';
              accentColor = 'var(--orange)';
            } else {
              borderColor = 'rgba(60, 255, 100, 0.3)';
              bgColor = 'rgba(60, 255, 100, 0.05)';
              accentColor = 'var(--green)';
            }

            return (
              <div key={idx} style={{
                display: 'grid',
                gridTemplateColumns: 'minmax(80px, 1fr) 1fr 1fr 2fr',
                gap: 16,
                padding: '12px 16px',
                background: bgColor,
                border: `1px solid ${borderColor}`,
                borderRadius: 6,
                alignItems: 'center',
                ...MONO
              }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase' }}>Threshold</span>
                  <span style={{ fontSize: 16, color: accentColor, fontWeight: 600 }}>{r.threshold}</span>
                </div>
                
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase' }}>P(ruin)</span>
                  <span style={{ fontSize: 14, color: 'var(--t1)' }}>{r.pruin}</span>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase' }}>Frequency</span>
                  <span style={{ fontSize: 14, color: 'var(--t2)' }}>{r.onein}</span>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', gap: 2, alignItems: 'flex-end', textAlign: 'right' }}>
                  <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase' }}>Verdict</span>
                  <span style={{ fontSize: 11, color: accentColor, fontWeight: 600 }}>{r.verdict}</span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function renderDeflatedSharpe(body: string) {
  const lines = body.split('\n');
  const groups: { title: string; items: { label: string; value: string; note: string }[] }[] = [];
  let currentGroup = null;

  for (const line of lines) {
    if (/[=─_]{5,}/.test(line)) continue;
    if (line.includes('DEFLATED SHARPE RATIO + MINIMUM TRACK RECORD LENGTH')) continue;
    if (line.includes('Filter:')) continue;
    if (/Results saved/i.test(line)) continue;
    
    const groupMatch = line.match(/^  ([A-Za-z0-9()@%,. +=-]+):\s*$/);
    if (groupMatch) {
      currentGroup = { title: groupMatch[1].trim(), items: [] };
      groups.push(currentGroup);
      continue;
    }
    
    if (currentGroup && line.includes(':')) {
      const parts = line.split(':');
      if (parts.length >= 2) {
        const label = parts[0].trim();
        const rest = parts.slice(1).join(':').trim();
        let val = rest;
        let note = '';
        
        const bracketIdx = rest.indexOf('(');
        if (bracketIdx !== -1 && !label.toLowerCase().includes('verdict')) {
           val = rest.slice(0, bracketIdx).trim();
           note = rest.slice(bracketIdx).trim();
        }

        currentGroup.items.push({ label, value: val, note });
      }
    }
  }

  if (groups.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {sectionLabel('Deflated Sharpe Ratio (DSR)')}
      
      {groups.map((g, gIdx) => (
        <div key={gIdx} style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div style={{ fontSize: 11, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, borderBottom: '1px solid var(--line1)', paddingBottom: 6, ...MONO }}>
            {g.title}
          </div>
          <div style={{ 
            display: 'grid', 
            gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
            gap: 12 
          }}>
            {g.items.map((item, idx) => {
              const isVerdict = item.label.toLowerCase().includes('verdict');
              let valColor = 'var(--t1)';
              
              if (isVerdict) {
                if (item.value.toUpperCase().includes('FAIL') || item.value.toUpperCase().includes('HIGH')) valColor = 'var(--red)';
                else if (item.value.toUpperCase().includes('WARN')) valColor = 'var(--orange)';
                else valColor = 'var(--green)';
              }

              return (
                <div key={idx} style={{
                  background: 'var(--bg2)',
                  border: isVerdict && valColor === 'var(--red)' ? '1px solid rgba(255, 60, 60, 0.4)' : '1px solid var(--line1)',
                  borderRadius: 6,
                  padding: '12px 16px',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 4,
                  boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.02)',
                }}>
                  <div style={{ fontSize: 9.5, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
                    {item.label}
                  </div>
                  <div style={{ fontSize: 16, color: valColor, fontWeight: 500, letterSpacing: -0.5, ...MONO }}>
                    {item.value}
                  </div>
                  {item.note && (
                    <div style={{ fontSize: 9, color: 'var(--t4)', fontStyle: 'italic', paddingTop: 2, ...MONO }}>
                      {item.note.replace(/[()]/g, '').trim()}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

function renderSignalPredictiveness(body: string) {
  const lines = body.split('\n');
  const consumedIdx = new Set<number>();
  const markLine = (re: RegExp): string => {
    const idx = lines.findIndex((l) => re.test(l));
    if (idx >= 0) {
      consumedIdx.add(idx);
      return lines[idx].trim();
    }
    return '';
  };
  const fwdMeta = markLine(/Forward horizons:/i);
  const guideMeta = markLine(/IC guide:/i);
  const csvLine = markLine(/\[predictiveness\]\s*CSV saved:/i);

  const headerIdx = lines.findIndex(
    (l) => /^\s*Signal\s+Kind\s+Fwd\s+Pearson r\s+Spearman IC\s+p-val\s+N\s+\*\s+Bar\s*$/i.test(l.trim())
      || /^\s*Signal\s+Kind\s+Fwd\s+Pearson\s+r\s+Spearman\s+IC\s+p-?val\s+N\s+\*\s+Bar\s*$/i.test(l.trim()),
  );
  if (headerIdx >= 0) consumedIdx.add(headerIdx);

  type SignalRow = {
    signal: string;
    kind: string;
    fwd: string;
    pearson: number | null;
    ic: number | null;
    pval: string;
    n: string;
    sig: string;
    bar: string;
  };
  const parsedRows: SignalRow[] = [];
  const unparsedTableLines: string[] = [];
  const isStopLine = (t: string) =>
    /^Top predictive signals/i.test(t)
    || /^\[predictiveness\]/i.test(t)
    || /^[-=─═]{20,}$/.test(t);

  const parseNum = (v: string): number | null => {
    const n = Number(v.replace(/,/g, ''));
    return Number.isFinite(n) ? n : null;
  };
  const normalizeFwd = (rawFwd: string): string => {
    const t = rawFwd.trim().toLowerCase();
    const m = t.match(/^fwd(\d+)(?:d)?$/);
    if (m) return `fwd${m[1]}`;
    if (/^\d+$/.test(t)) return `fwd${t}`;
    return rawFwd.trim();
  };

  if (headerIdx >= 0) {
    let started = false;
    for (let i = headerIdx + 1; i < lines.length; i += 1) {
      const raw = lines[i];
      const t = raw.trim();
      if (!t) {
        if (started) break;
        continue;
      }
      if (isStopLine(t)) {
        consumedIdx.add(i);
        if (started) break;
        continue;
      }

      const parts = raw.trim().split(/\s{2,}/).filter(Boolean);
      if (parts.length >= 8) {
        const signal = parts[0] ?? '';
        const kind = parts[1] ?? '—';
        const fwd = normalizeFwd(parts[2] ?? '—');
        const pearsonRaw = parts[3] ?? 'n/a';
        const icRaw = parts[4] ?? 'n/a';
        const pval = parts[5] ?? 'n/a';
        const n = parts[6] ?? 'n/a';
        const sig = (parts[7] ?? '').trim() || '—';
        const bar = parts.slice(8).join(' ').trim() || '—';
        const pearson = /^n\/a$/i.test(pearsonRaw) ? null : parseNum(pearsonRaw);
        const ic = /^n\/a$/i.test(icRaw) ? null : parseNum(icRaw);
        parsedRows.push({ signal, kind, fwd, pearson, ic, pval, n, sig, bar });
        consumedIdx.add(i);
        started = true;
      } else {
        unparsedTableLines.push(raw);
      }
    }
  }

  // Parse the full forward-horizon blocks so the full section is visualized,
  // not only the condensed "top" table.
  let currentForwardDays: string | null = null;
  for (let i = 0; i < lines.length; i += 1) {
    const raw = lines[i];
    const t = raw.trim();
    const forwardMatch = t.match(/^--\s*Forward\s+(\d+)d/i);
    if (forwardMatch) {
      currentForwardDays = forwardMatch[1];
      consumedIdx.add(i);
      continue;
    }
    if (!currentForwardDays || !t || /^[-=─═]{12,}$/.test(t)) continue;

    const parts = t.split(/\s+/);
    if (parts.length >= 7 && (parts[1].toLowerCase() === 'level' || parts[1].toLowerCase() === 'delta') && /^\d+$/.test(parts[2])) {
      const signal = parts[0];
      const kind = parts[1].toLowerCase();
      const fwdDays = parts[2];
      const pearson = parseNum(parts[3]);
      const ic = parseNum(parts[4]);
      const pval = parts[5];
      const n = parts[6].replace(/,/g, '');
      const sig = parts[7] ?? '—';
      parsedRows.push({
        signal,
        kind,
        fwd: `fwd${fwdDays}`,
        pearson,
        ic,
        pval,
        n,
        sig,
        bar: '—',
      });
      consumedIdx.add(i);
      continue;
    }
  }

  const uniqueParsedRows = parsedRows.filter((row, idx, arr) => {
    const key = `${row.signal}|${row.kind}|${row.fwd}|${row.pearson ?? 'na'}|${row.ic ?? 'na'}|${row.pval}|${row.n}|${row.sig}`;
    return idx === arr.findIndex((x) => {
      const xKey = `${x.signal}|${x.kind}|${x.fwd}|${x.pearson ?? 'na'}|${x.ic ?? 'na'}|${x.pval}|${x.n}|${x.sig}`;
      return xKey === key;
    });
  });

  const topPredictive = lines
    .map((l, idx) => ({ line: l.trim(), idx }))
    .filter(({ line }) => /^#\d+\s+.+\s+->\s+fwd\d+d\s+IC=[+-]?\d+(?:\.\d+)?/i.test(line))
    .map(({ line, idx }) => {
      consumedIdx.add(idx);
      const m = line.match(/^#(\d+)\s+(.+?)\s+->\s+(fwd\d+d)\s+IC=([+-]?\d+(?:\.\d+)?)(?:\s+\[(.+)\])?/i);
      if (!m) return null;
      return {
        rank: Number(m[1]),
        signal: m[2],
        fwd: normalizeFwd(m[3]),
        ic: Number(m[4]),
        tag: m[5] ?? '',
      };
    })
    .filter((v): v is { rank: number; signal: string; fwd: string; ic: number; tag: string } => v !== null);
  const topLabelIdx = lines.findIndex((l) => /^Top predictive signals/i.test(l.trim()));
  if (topLabelIdx >= 0) consumedIdx.add(topLabelIdx);

  const rawDetails = lines
    .map((line, idx) => ({ line: line.trimEnd(), idx }))
    .filter(({ line, idx }) => !consumedIdx.has(idx) && line.trim() && !/^[-=─═]{12,}$/.test(line.trim()))
    .map(({ line }) => line);

  const uniqueTopPredictive = topPredictive
    .sort((a, b) => a.rank - b.rank)
    .filter((row, idx, arr) => idx === arr.findIndex((x) => x.rank === row.rank && x.signal === row.signal && x.fwd === row.fwd));

  if (uniqueParsedRows.length === 0 && uniqueTopPredictive.length === 0 && rawDetails.length === 0) {
    return <PreFallback body={body} />;
  }

  const allFwds = new Set<string>();
  uniqueParsedRows.forEach((r) => allFwds.add(normalizeFwd(r.fwd)));
  uniqueTopPredictive.forEach((r) => allFwds.add(normalizeFwd(r.fwd)));
  const fwdGroups = Array.from(allFwds).sort((a, b) => {
    const numA = parseInt(a.replace(/\D/g, ''), 10) || 0;
    const numB = parseInt(b.replace(/\D/g, ''), 10) || 0;
    return numA - numB;
  });
  const icRowsForViz = (uniqueParsedRows.length > 0
    ? uniqueParsedRows
      .filter((r) => r.ic !== null)
      .map((r) => ({ signal: r.signal, kind: r.kind, fwd: normalizeFwd(r.fwd), ic: r.ic as number }))
    : uniqueTopPredictive.map((r) => ({ signal: r.signal, kind: 'ranked', fwd: normalizeFwd(r.fwd), ic: r.ic })));
  const groupedIcItems = fwdGroups.map((fwd) => ({
    fwd,
    items: icRowsForViz
      .filter((r) => r.fwd === fwd)
      .map((r) => ({
        label: `${r.signal} (${r.kind})`,
        value: r.ic,
        raw: `${r.ic >= 0 ? '+' : ''}${r.ic.toFixed(4)}`,
      }))
      .sort((a, b) => b.value - a.value),
  }));
  const totalGroupedBars = groupedIcItems.reduce((acc, g) => acc + g.items.length, 0);

  const tableHeaders = ['Signal', 'Kind', 'Fwd', 'Pearson r', 'Spearman IC', 'p-val', 'N', '*', 'Bar'];
  const sortSignalRows = (a: SignalRow, b: SignalRow) => {
    const aAbs = a.ic === null ? -1 : Math.abs(a.ic);
    const bAbs = b.ic === null ? -1 : Math.abs(b.ic);
    if (bAbs !== aAbs) return bAbs - aAbs;
    const aP = Number(a.pval);
    const bP = Number(b.pval);
    const aPN = Number.isFinite(aP) ? aP : Number.POSITIVE_INFINITY;
    const bPN = Number.isFinite(bP) ? bP : Number.POSITIVE_INFINITY;
    if (aPN !== bPN) return aPN - bPN;
    return a.signal.localeCompare(b.signal);
  };
  const tableRowsByFwd = fwdGroups.map((fwd) => ({
    fwd,
    rows: uniqueParsedRows
      .filter((r) => normalizeFwd(r.fwd) === fwd)
      .sort(sortSignalRows)
      .map((r) => ({
        label: r.signal,
        values: [
          r.kind,
          r.fwd,
          r.pearson === null ? 'n/a' : r.pearson.toFixed(4),
          r.ic === null ? 'n/a' : r.ic.toFixed(4),
          r.pval,
          r.n,
          r.sig,
          r.bar,
        ],
      })),
  }));

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {(fwdMeta || guideMeta) && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {fwdMeta && (
            <span style={{ border: '1px solid var(--line2)', background: 'var(--bg1)', borderRadius: 3, padding: '2px 6px', fontSize: 8.5, color: 'var(--t2)', ...MONO }}>
              {fwdMeta}
            </span>
          )}
          {guideMeta && (
            <span style={{ border: '1px solid var(--line2)', background: 'var(--bg1)', borderRadius: 3, padding: '2px 6px', fontSize: 8.5, color: 'var(--t2)', ...MONO }}>
              {guideMeta}
            </span>
          )}
        </div>
      )}

      {totalGroupedBars > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {sectionLabel(`Top |IC| Signals (${totalGroupedBars})`)}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
            {groupedIcItems.map((group) => (
              group.items.length > 0 ? (
                <div key={group.fwd} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {sectionLabel(group.fwd.toUpperCase())}
                  <SectionHBarChart
                    items={group.items}
                    colorFn={(v) => (Math.abs(v) >= 0.2 ? 'var(--green)' : Math.abs(v) >= 0.1 ? 'var(--amber)' : 'var(--red)')}
                  />
                </div>
              ) : null
            ))}
          </div>
        </div>
      )}

      {tableRowsByFwd.some((g) => g.rows.length > 0) && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {tableRowsByFwd.map((group) => (
            group.rows.length > 0 ? (
              <div key={`tbl-${group.fwd}`} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {sectionLabel(`${group.fwd.toUpperCase()} Table`)}
                <SectionTable headers={tableHeaders} rows={group.rows} />
              </div>
            ) : null
          ))}
        </div>
      )}

      {uniqueTopPredictive.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {sectionLabel(`Significant Rankings (${uniqueTopPredictive.length})`)}
          {uniqueTopPredictive.map((r) => (
            <div key={`${r.rank}-${r.signal}-${r.fwd}`} style={{ fontSize: 9, color: 'var(--t1)', ...MONO }}>
              #{r.rank} {r.signal} → {r.fwd} | IC {r.ic >= 0 ? '+' : ''}{r.ic.toFixed(4)}{r.tag ? ` [${r.tag}]` : ''}
            </div>
          ))}
        </div>
      )}



      {csvLine && (
        <div style={{ fontSize: 8.5, color: 'var(--t3)', ...MONO }}>
          {csvLine}
        </div>
      )}
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderSlippageSweep(body: string) {
  const table = parseColumnarTable(body);
  if (!table || table.rows.length === 0) return <PreFallback body={body} />;
  const sharpeIdx = table.headers.findIndex((h) => /sharpe/i.test(h));
  if (sharpeIdx >= 1) {
    const items = table.rows.flatMap((r) => {
      const raw = r.values[sharpeIdx - 1];
      if (!raw) return [];
      const num = parseFloat(raw.replace(/[,%]/g, ''));
      if (!Number.isFinite(num)) return [];
      return [{ label: r.label, value: num, raw }];
    });
    if (items.length >= 2) {
      return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {sectionLabel('Sharpe vs Slippage')}
          <SectionHBarChart items={items} colorFn={(v) => v >= 1.5 ? 'var(--green)' : v >= 0.5 ? 'var(--amber)' : 'var(--red)'} />
          <SectionTable headers={table.headers} rows={table.rows} />
        </div>
      );
    }
  }
  return <SectionTable headers={table.headers} rows={table.rows} />;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderNoisePerturbation(body: string) {
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
  return renderKVSection(body, 'Noise Stability', (v) => v > 0 ? 'var(--green)' : 'var(--amber)');
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderParamJitter(body: string) {
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
  return renderKVSection(body, 'Param Jitter — Sharpe Stability', (v) => v > 0 ? 'var(--green)' : 'var(--amber)');
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderReturnConcentration(body: string) {
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
  return renderKVSection(body, 'Concentration Metrics', (v) => v >= 0 ? 'var(--green)' : 'var(--amber)');
}

function renderPeriodicBreakdown(body: string) {
  return <PreFallback body={body} />;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderShockInjection(body: string) {
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
  return renderKVSection(body, 'Shock Impact', (v) => v >= 0 ? 'var(--green)' : 'var(--red)');
}

interface StabCubeRow { params: Record<string, number>; sharpe: number; cagr: number; maxdd: number; }

function renderStabilityCube(body: string) {
  const lines = body.split('\n');
  let filter = '';
  let gridSpec = '';
  const baseline: Record<string, number> = {};
  const peakAt: Record<string, number> = {};
  const summaryKV: Array<{ label: string; value: string }> = [];
  const rows: StabCubeRow[] = [];
  let paramKeys: string[] | null = null;
  let inSummary = false;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || /^[=─]+$/.test(trimmed)) continue;

    if (/STABILITY CUBE SUMMARY/i.test(trimmed)) { inSummary = true; continue; }

    if (inSummary) {
      if (/saved:/i.test(trimmed)) continue;
      const sm = trimmed.match(/^(.+?)\s*:\s*(.+)$/);
      if (sm) {
        const peakM = sm[2].match(/at\s+(.+)/i);
        if (peakM) {
          const re = /(\w+)=([-\d.]+)/g; let pm;
          while ((pm = re.exec(peakM[1])) !== null) peakAt[pm[1]] = parseFloat(pm[2]);
        }
        summaryKV.push({ label: sm[1].trim(), value: sm[2].trim() });
      }
      continue;
    }

    const filterM = trimmed.match(/^Filter\s*:\s*(.+?)\s*\|\s*Grid:\s*(.+)$/);
    if (filterM) { filter = filterM[1].trim(); gridSpec = filterM[2].trim(); continue; }

    const baselineM = trimmed.match(/^Baseline:\s*(.+)$/);
    if (baselineM) {
      const re = /(\w+)=([-\d.]+)/g; let bm;
      while ((bm = re.exec(baselineM[1])) !== null) baseline[bm[1]] = parseFloat(bm[2]);
      continue;
    }

    const dataM = trimmed.match(/^\[\s*\d+\/\d+\]\s+(.+?)\s*\|\s*Sharpe=\s*([-\d.]+)\s+CAGR=\s*([-\d.]+)%\s+MaxDD=\s*([-\d.]+)%/);
    if (dataM) {
      const params: Record<string, number> = {};
      const re = /(\w+)=([-\d.]+)/g; let dm;
      while ((dm = re.exec(dataM[1])) !== null) params[dm[1]] = parseFloat(dm[2]);
      if (!paramKeys) paramKeys = Object.keys(params);
      rows.push({ params, sharpe: parseFloat(dataM[2]), cagr: parseFloat(dataM[3]), maxdd: parseFloat(dataM[4]) });
    }
  }

  if (rows.length === 0 || !paramKeys || paramKeys.length < 3) return <PreFallback body={body} />;

  // Detect dimensions: fewest unique = outer slice, middle = cols, most = rows
  const uniq: Record<string, number[]> = {};
  for (const k of paramKeys) {
    uniq[k] = [...new Set(rows.map(r => r.params[k]))].sort((a, b) => a - b);
  }
  const sorted = [...paramKeys].sort((a, b) => uniq[a].length - uniq[b].length);
  const [outerKey, colKey, rowKey] = sorted;
  const outerVals = uniq[outerKey];
  const colVals = uniq[colKey];
  const rowVals = uniq[rowKey];

  const lookup = new Map<string, StabCubeRow>();
  for (const r of rows) {
    lookup.set(paramKeys.map(k => r.params[k]).join('|'), r);
  }
  const getCell = (o: number, c: number, r: number) => {
    const p: Record<string, number> = { [outerKey]: o, [colKey]: c, [rowKey]: r };
    return lookup.get(paramKeys!.map(k => p[k]).join('|'));
  };

  const allSharpes = rows.map(r => r.sharpe).filter(s => isFinite(s));
  const maxSharpe = Math.max(...allSharpes);

  const sharpeColor = (s: number) => {
    if (!isFinite(s)) return 'var(--t3)';
    if (s >= 3.0) return 'var(--green)';
    if (s >= 2.0) return 'var(--amber)';
    return 'var(--red)';
  };

  const isBase = (o: number, c: number, r: number) =>
    baseline[outerKey] === o && baseline[colKey] === c && baseline[rowKey] === r;
  const isPeak = (o: number, c: number, r: number) =>
    Object.keys(peakAt).length > 0 &&
    peakAt[outerKey] === o && peakAt[colKey] === c && peakAt[rowKey] === r;

  const verdictEntry = summaryKV.find(kv => /verdict/i.test(kv.label));
  const verdictColor = !verdictEntry ? 'var(--t1)'
    : /low.sensitivity|stable|robust/i.test(verdictEntry.value) ? 'var(--green)'
    : /moderate/i.test(verdictEntry.value) ? 'var(--amber)'
    : 'var(--red)';

  const thStyle: React.CSSProperties = {
    padding: '3px 10px', textAlign: 'center', fontWeight: 700,
    textTransform: 'uppercase', letterSpacing: '0.06em',
    borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap', fontSize: 8.5,
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, fontSize: 9, ...MONO }}>
      {/* Metadata */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', color: 'var(--t2)', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        {filter && <span>Filter: <span style={{ color: 'var(--t1)' }}>{filter}</span></span>}
        {gridSpec && <span>Grid: <span style={{ color: 'var(--t1)' }}>{gridSpec}</span></span>}
        {Object.entries(baseline).map(([k, v]) => (
          <span key={k}>{k} baseline: <span style={{ color: 'var(--green)' }}>{v}</span></span>
        ))}
      </div>

      {/* Summary cards */}
      {summaryKV.length > 0 && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(155px, 1fr))', gap: 6 }}>
          {summaryKV.map((kv, i) => {
            const isVerdict = /verdict/i.test(kv.label);
            return (
              <div key={i} style={{
                border: `1px solid ${isVerdict ? verdictColor : 'var(--line)'}`,
                borderRadius: 3, background: 'var(--bg1)', padding: '5px 8px',
                display: 'flex', flexDirection: 'column', gap: 3,
              }}>
                <span style={{ fontSize: 8, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>{kv.label}</span>
                <span style={{ fontSize: isVerdict ? 10 : 11, fontWeight: 700, color: isVerdict ? verdictColor : 'var(--t0)' }}>{kv.value}</span>
              </div>
            );
          })}
        </div>
      )}

      {/* Legend */}
      <div style={{ display: 'flex', gap: 14, fontSize: 8.5, color: 'var(--t2)' }}>
        <span><span style={{ color: 'var(--green)' }}>■</span> ≥ 3.0</span>
        <span><span style={{ color: 'var(--amber)' }}>■</span> ≥ 2.0</span>
        <span><span style={{ color: 'var(--red)' }}>■</span> &lt; 2.0</span>
        <span style={{ color: 'var(--t3)' }}>◆ baseline  ★ peak  — not run</span>
        <span style={{ color: 'var(--t3)' }}>hover cell for CAGR / MaxDD</span>
      </div>

      {/* One table per outer-slice value */}
      {outerVals.map(ov => {
        const isBaseSlice = baseline[outerKey] === ov;
        return (
          <div key={ov} style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
            <div style={{
              fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em',
              color: isBaseSlice ? 'var(--green)' : 'var(--t2)',
              paddingBottom: 4, borderBottom: '1px solid var(--line)',
              display: 'flex', alignItems: 'center', gap: 8,
            }}>
              {outerKey} = {ov}
              {isBaseSlice && <span style={{ color: 'var(--green)', fontWeight: 400, fontSize: 8, letterSpacing: '0.06em' }}>◆ BASELINE SLICE</span>}
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
                <thead>
                  <tr>
                    <th style={{ ...thStyle, textAlign: 'left', color: 'var(--t3)', paddingLeft: 0, paddingRight: 12 }}>
                      {rowKey} ╲ {colKey}
                    </th>
                    {colVals.map(cv => (
                      <th key={cv} style={{
                        ...thStyle,
                        color: baseline[colKey] === cv ? 'var(--green)' : 'var(--t3)',
                      }}>
                        {cv}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rowVals.map(rv => {
                    const isBaseRow = baseline[rowKey] === rv;
                    return (
                      <tr key={rv} style={{ background: isBaseRow ? 'rgba(0,200,150,0.03)' : 'transparent' }}>
                        <td style={{
                          padding: '2px 12px 2px 0', color: isBaseRow ? 'var(--green)' : 'var(--t2)',
                          fontWeight: isBaseRow ? 700 : 400, whiteSpace: 'nowrap', fontSize: 9,
                          borderBottom: '1px solid var(--line)', borderRight: '1px solid var(--line2)',
                        }}>
                          {rv}
                        </td>
                        {colVals.map(cv => {
                          const cell = getCell(ov, cv, rv);
                          const bl = isBase(ov, cv, rv);
                          const pk = cell ? (cell.sharpe === maxSharpe) : isPeak(ov, cv, rv);
                          return (
                            <td
                              key={cv}
                              style={{
                                padding: '2px 10px', textAlign: 'center',
                                color: cell ? sharpeColor(cell.sharpe) : 'var(--t3)',
                                background: bl ? 'var(--green-dim)' : 'transparent',
                                boxShadow: bl ? 'inset 0 0 0 1px var(--green-mid)' : 'none',
                                fontWeight: bl || pk ? 700 : 400,
                                whiteSpace: 'nowrap', fontSize: 9,
                                borderBottom: '1px solid var(--line)',
                              }}
                              title={cell ? `CAGR ${cell.cagr.toFixed(1)}%  MaxDD ${cell.maxdd.toFixed(2)}%` : undefined}
                            >
                              {cell ? `${cell.sharpe.toFixed(3)}${pk ? ' ★' : bl ? ' ◆' : ''}` : '—'}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function renderMinCumReturn(body: string) {
  interface MCRRow { window: string; minCumRet: number; worstStart: number; worstEnd: number; raw: string; }
  const rows: MCRRow[] = [];

  for (const line of body.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || /^[-=─═╔╠═]+/.test(trimmed)) continue;
    // Data row: "  1d   -23.51%   200   200"
    const m = trimmed.match(/^(\d+d)\s+([-+]?[\d.]+%)\s+(\d+)\s+(\d+)$/);
    if (!m) continue;
    rows.push({
      window: m[1],
      minCumRet: parseFloat(m[2]),
      worstStart: parseInt(m[3], 10),
      worstEnd: parseInt(m[4], 10),
      raw: m[2],
    });
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  const vals = rows.map(r => r.minCumRet);
  const minVal = Math.min(...vals);   // most negative
  const maxVal = Math.max(...vals);   // most positive
  const absMax = Math.max(Math.abs(minVal), Math.abs(maxVal));
  const BAR_MAX = 56; // px

  const retColor = (v: number) => v >= 0 ? 'var(--green)' : 'var(--red)';
  const barColor = (v: number) => v >= 0 ? 'var(--green)' : 'var(--red)';

  const thStyle: React.CSSProperties = {
    padding: '3px 8px', fontWeight: 700, textTransform: 'uppercase',
    letterSpacing: '0.08em', borderBottom: '1px solid var(--line2)',
    whiteSpace: 'nowrap', fontSize: 9, color: 'var(--t3)',
  };

  return (
    <div style={{ overflowX: 'auto', ...MONO }}>
      <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
        <thead>
          <tr>
            <th style={{ ...thStyle, textAlign: 'left' }}>Window</th>
            <th style={{ ...thStyle, textAlign: 'right' }}>Min Cum Ret</th>
            <th style={{ ...thStyle, textAlign: 'left', paddingLeft: 10 }}>Bar</th>
            <th style={{ ...thStyle, textAlign: 'right' }}>Worst Start</th>
            <th style={{ ...thStyle, textAlign: 'right' }}>Worst End</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => {
            const barW = Math.round((Math.abs(row.minCumRet) / absMax) * BAR_MAX);
            const isWorst = row.minCumRet === minVal;
            return (
              <tr key={ri} style={{ background: isWorst ? 'var(--red-dim)' : ri % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent' }}>
                <td style={{ padding: '2px 8px', color: 'var(--t2)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)', fontWeight: isWorst ? 700 : 400 }}>
                  {row.window}
                </td>
                <td style={{ padding: '2px 8px', textAlign: 'right', color: retColor(row.minCumRet), whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)', fontWeight: isWorst ? 700 : 400 }}>
                  {row.raw}
                </td>
                <td style={{ padding: '2px 4px 2px 10px', borderBottom: '1px solid var(--line)', verticalAlign: 'middle' }}>
                  <div style={{ width: BAR_MAX, display: 'flex', alignItems: 'center' }}>
                    <div style={{ width: barW, height: 4, borderRadius: 1, background: barColor(row.minCumRet), opacity: isWorst ? 1 : 0.7 }} />
                  </div>
                </td>
                <td style={{ padding: '2px 8px', textAlign: 'right', color: 'var(--t2)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                  {row.worstStart}
                </td>
                <td style={{ padding: '2px 8px', textAlign: 'right', color: 'var(--t2)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                  {row.worstEnd}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function renderMilestones(body: string) {
  const lines = body.split('\n');
  const headers = ['Period', 'Balance ($)', 'Net PnL ($)', 'Period ROI', 'Cum ROI'];
  const dataRows: string[][] = [];
  let totalRow: string[] | null = null;
  let statsLine = '';

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || /^[-=─]+$/.test(trimmed)) continue;
    // Collapse "+ spaces" → "+" so the sign stays attached to its value
    const norm = line.replace(/\+\s+/g, '+');
    if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
      const tokens = norm.split(/\s{2,}/).filter(Boolean);
      if (tokens.length >= 3) dataRows.push(tokens);
    } else if (/^TOTAL\b/i.test(trimmed)) {
      totalRow = norm.split(/\s{2,}/).filter(Boolean);
    } else if (/\bperiods?\b.*\bpositive\b/i.test(trimmed)) {
      statsLine = trimmed;
    }
  }

  const valColor = (val: string) => {
    if (!val || val === '—') return 'var(--t1)';
    if (val.startsWith('+')) return 'var(--green)';
    if (val.startsWith('-')) return 'var(--red)';
    return 'var(--t1)';
  };

  const renderCells = (row: string[], isTotal = false) =>
    headers.map((_, ci) => {
      const val = row[ci] ?? '';
      return (
        <td
          key={ci}
          style={{
            padding: '2px 8px',
            textAlign: ci === 0 ? 'left' : 'right',
            color: ci === 0 ? (isTotal ? 'var(--t1)' : 'var(--t2)') : valColor(val),
            whiteSpace: 'nowrap',
            fontSize: 9,
            fontWeight: isTotal ? 700 : 400,
            borderTop: isTotal ? '1px solid var(--line)' : undefined,
          }}
        >
          {val || '—'}
        </td>
      );
    });

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, ...MONO }}>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
          <thead>
            <tr>
              {headers.map((h, i) => (
                <th
                  key={i}
                  style={{
                    padding: '3px 8px',
                    textAlign: i === 0 ? 'left' : 'right',
                    color: 'var(--t3)',
                    fontWeight: 700,
                    textTransform: 'uppercase',
                    letterSpacing: '0.08em',
                    borderBottom: '1px solid var(--line2)',
                    whiteSpace: 'nowrap',
                    fontSize: 9,
                  }}
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {dataRows.map((row, ri) => (
              <tr key={ri} style={{ background: ri % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent' }}>
                {renderCells(row)}
              </tr>
            ))}
            {totalRow && <tr>{renderCells(totalRow, true)}</tr>}
          </tbody>
        </table>
      </div>
      {statsLine && (
        <div style={{ fontSize: 9, color: 'var(--t2)', paddingTop: 2 }}>{statsLine}</div>
      )}
    </div>
  );
}

function renderLiquidityCapacityCurve(body: string) {
  const lines = body.split('\n');

  // Find the actual table header line (contains both "AUM" and "Impact%")
  const headerLineIdx = lines.findIndex((l) => /\bAUM\b/.test(l) && /Impact%/.test(l));
  if (headerLineIdx < 0) return <PreFallback body={body} />;

  // KV metadata lines before the table header
  const kvLines = lines
    .slice(0, headerLineIdx)
    .filter((l) => l.trim() && !/^[─=\s]+$/.test(l.trim()));

  // Parse headers from the header line
  const headerLine = lines[headerLineIdx];
  const headers = headerLine.trim().split(/\s{2,}/).filter(Boolean);

  // Find the divider line right after the header
  const dividerIdx = lines.findIndex(
    (l, i) => i > headerLineIdx && /^[\s─]+$/.test(l) && l.trim().length > 5,
  );
  const tableStartIdx = dividerIdx >= 0 ? dividerIdx + 1 : headerLineIdx + 1;

  // Find where the break-even section starts
  const breakEvenIdx = lines.findIndex((l) => /Break-even AUM/i.test(l));
  const tableEndIdx = breakEvenIdx >= 0 ? breakEvenIdx : lines.length;

  // Parse table rows using token splitting.
  // The AUM column is right-aligned so position-based slicing fails — the "$" and
  // the number are separated by many spaces and always tokenise as two pieces.
  // We detect this by checking whether the first token is exactly "$", and if so
  // merge it with the second token to reconstruct the full AUM value.
  const rows: string[][] = [];
  for (let i = tableStartIdx; i < tableEndIdx; i += 1) {
    const line = lines[i];
    if (!line.trim() || /^[─=]+$/.test(line.trim())) continue;
    const tokens = line.split(/\s{2,}/).filter(Boolean);
    if (tokens.length === 0) continue;
    let row: string[];
    if (tokens[0] === '$') {
      // Merge "$" and the number into a single AUM token
      row = [`$${tokens[1]}`, ...tokens.slice(2)];
    } else {
      row = tokens;
    }
    if (row.length >= 2) rows.push(row);
  }

  // Break-even footer lines
  const breakEvenLines = breakEvenIdx >= 0
    ? lines.slice(breakEvenIdx).filter((l) => l.trim() && !/^[=─]+$/.test(l.trim()))
    : [];

  const gradeIdx = headers.findIndex((h) => /^grade$/i.test(h));
  const sharpeIdx = headers.findIndex((h) => /^sharpe$/i.test(h));

  const gradeColor = (g: string) => {
    if (/excellent/i.test(g)) return 'var(--green)';
    if (/institutional/i.test(g)) return '#60a5fa';
    if (/survival/i.test(g)) return 'var(--amber)';
    if (/marginal/i.test(g)) return '#f97316';
    if (/unusable/i.test(g)) return 'var(--red)';
    return 'var(--t1)';
  };

  const sharpeColor = (v: string) => {
    const n = parseFloat(v);
    if (!Number.isFinite(n)) return 'var(--t1)';
    if (n >= 2.0) return 'var(--green)';
    if (n >= 1.5) return 'var(--amber)';
    return 'var(--red)';
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10, fontSize: 9, ...MONO }}>
      {/* KV metadata */}
      {kvLines.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {kvLines.map((line, i) => {
            const m = line.match(/^[\s]*(.+?):\s+(.+)$/);
            if (!m) return null;
            return (
              <div key={i} style={{ display: 'flex', gap: 8 }}>
                <span style={{ color: 'var(--t2)', minWidth: 160 }}>{m[1].trim()}</span>
                <span style={{ color: 'var(--t1)' }}>{m[2].trim()}</span>
              </div>
            );
          })}
        </div>
      )}

      {/* Capacity table */}
      {rows.length > 0 && (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
            <thead>
              <tr>
                {headers.map((h, i) => (
                  <th
                    key={i}
                    style={{
                      padding: '3px 10px 3px 6px',
                      textAlign: i === 0 ? 'left' : 'right',
                      color: 'var(--t3)',
                      fontWeight: 700,
                      textTransform: 'uppercase',
                      letterSpacing: '0.08em',
                      borderBottom: '1px solid var(--line2)',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, ri) => {
                const gradeVal = gradeIdx >= 0 ? row[gradeIdx] : '';
                const isFloor = /inst\.\s*floor/i.test(gradeVal);
                return (
                  <tr
                    key={ri}
                    style={{
                      background: isFloor
                        ? 'var(--amber-dim)'
                        : ri % 2 === 1
                          ? 'rgba(255,255,255,0.015)'
                          : 'transparent',
                    }}
                  >
                    {row.map((cell, ci) => {
                      let color = 'var(--t1)';
                      if (ci === gradeIdx) color = gradeColor(cell);
                      else if (ci === sharpeIdx) color = sharpeColor(cell);
                      else {
                        const n = parseFloat(cell.replace(/[,%$]/g, ''));
                        if (Number.isFinite(n)) {
                          color = n < 0 ? 'var(--red)' : cell.includes('%') ? 'var(--t1)' : 'var(--t1)';
                        }
                      }
                      return (
                        <td
                          key={ci}
                          style={{
                            padding: '3px 10px 3px 6px',
                            textAlign: ci === 0 ? 'left' : 'right',
                            color,
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {cell || '—'}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Break-even footer */}
      {breakEvenLines.length > 0 && (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: 3,
            paddingTop: 6,
            borderTop: '1px solid var(--line)',
          }}
        >
          {breakEvenLines.map((line, i) => {
            const isHeader = /Break-even AUM/i.test(line);
            return (
              <div
                key={i}
                style={{
                  color: isHeader ? 'var(--t2)' : 'var(--t1)',
                  fontWeight: isHeader ? 700 : 400,
                  paddingLeft: isHeader ? 0 : 12,
                  fontSize: 9,
                }}
              >
                {line.trim()}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderHeatmapSection(body: string, label: string) {
  const grid = parseNumericGrid(body);
  if (grid && grid.data.length >= 2 && grid.colLabels.length >= 2) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {sectionLabel(label)}
        <HeatmapGrid rowLabels={grid.rowLabels} colLabels={grid.colLabels} data={grid.data} />
      </div>
    );
  }
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {sectionLabel(label)}
        <SectionTable headers={table.headers} rows={table.rows} />
      </div>
    );
  }
  return <PreFallback body={body} />;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderRankedSection(body: string, label: string) {
  const table = parseColumnarTable(body);
  if (!table || table.rows.length === 0) return <PreFallback body={body} />;
  const sharpeIdx = table.headers.findIndex((h) => /sharpe/i.test(h));
  if (sharpeIdx >= 1) {
    const items = table.rows.flatMap((r) => {
      const raw = r.values[sharpeIdx - 1];
      if (!raw) return [];
      const num = parseFloat(raw.replace(/[,%]/g, ''));
      if (!Number.isFinite(num)) return [];
      return [{ label: r.label, value: num, raw }];
    });
    if (items.length >= 2) {
      return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {sectionLabel(label + ' — Sharpe')}
          <SectionHBarChart items={items} colorFn={(v) => v >= 1.5 ? 'var(--green)' : v >= 0.5 ? 'var(--amber)' : 'var(--red)'} />
          <SectionTable headers={table.headers} rows={table.rows} />
        </div>
      );
    }
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
      {sectionLabel(label)}
      <SectionTable headers={table.headers} rows={table.rows} />
    </div>
  );
}

// ── Main dispatcher ───────────────────────────────────────────────────────────

function renderMarketCapDiagnostic(body: string) {
  const lines = body.split('\n');

  // --- metadata KV (Symbol src / Mcap file) ---
  const metaKV: { label: string; value: string }[] = [];
  for (const line of lines) {
    const m = line.match(/^\s{2}(Symbol src|Mcap file)\s*:\s*(.+)$/);
    if (m) metaKV.push({ label: m[1].trim(), value: m[2].trim() });
  }

  // --- Section 1: Symbol-level coverage ---
  let symTotal = 0, symMatched = 0, symMatchedPct = '', symUnmatched = 0, symUnmatchedPct = '';
  let unmatchedSymbols: string[] = [];
  for (const line of lines) {
    let m: RegExpMatchArray | null;
    m = line.match(/Unique symbols.*?:\s*([\d,]+)/i);
    if (m) symTotal = parseInt(m[1].replace(/,/g, ''), 10);
    m = line.match(/Matched in mcap.*?:\s*([\d,]+)\s*\(([^)]+)\)/i);
    if (m) { symMatched = parseInt(m[1].replace(/,/g, ''), 10); symMatchedPct = m[2]; }
    m = line.match(/Unmatched.*?:\s*([\d,]+)\s*\(([^)]+)\)/i);
    if (m) { symUnmatched = parseInt(m[1].replace(/,/g, ''), 10); symUnmatchedPct = m[2]; }
    m = line.match(/Unmatched symbols:\s*\[(.+)\]/);
    if (m) {
      unmatchedSymbols = m[1].split(',').map(s => s.trim().replace(/^'|'$/g, '').replace(/"/g, ''));
    }
  }

  // --- Section 2: Row-level match rate ---
  let rowTotal = 0, rowMatched = 0, rowMatchedPct = '', rowMissing = 0, rowMissingPct = '';
  for (const line of lines) {
    let m: RegExpMatchArray | null;
    m = line.match(/Total rows\s*:\s*([\d,]+)/i);
    if (m) rowTotal = parseInt(m[1].replace(/,/g, ''), 10);
    m = line.match(/Matched rows\s*:\s*([\d,]+)\s*\(([^)]+)\)/i);
    if (m) { rowMatched = parseInt(m[1].replace(/,/g, ''), 10); rowMatchedPct = m[2]; }
    m = line.match(/Missing mcap\s*:\s*([\d,]+)\s*\(([^)]+)\)/i);
    if (m) { rowMissing = parseInt(m[1].replace(/,/g, ''), 10); rowMissingPct = m[2]; }
  }

  // --- Section 2b: Missing mcap breakdown ---
  interface MCRow { symbol: string; missingDays: number; dates: string; }
  const mcRows: MCRow[] = [];
  let inBreakdown = false;
  for (const line of lines) {
    if (/Missing mcap breakdown/i.test(line)) { inBreakdown = true; continue; }
    if (!inBreakdown) continue;
    if (/^[\s─\-=]+$/.test(line) || /Symbol\s+Missing days/i.test(line)) continue;
    // data row: "  SYMBOL                       15  dates..."
    const m = line.match(/^\s{2}(\S+)\s+([\d]+)\s+(.+)$/);
    if (m) {
      mcRows.push({ symbol: m[1], missingDays: parseInt(m[2], 10), dates: m[3].trim() });
    }
  }

  const maxMissing = mcRows.length > 0 ? Math.max(...mcRows.map(r => r.missingDays)) : 1;
  const BAR_MAX = 60;

  const covColor = (pct: string) => {
    const n = parseFloat(pct);
    if (n >= 90) return 'var(--green)';
    if (n >= 75) return 'var(--amber)';
    return 'var(--red)';
  };

  const thStyle: React.CSSProperties = {
    padding: '3px 8px', fontWeight: 700, textTransform: 'uppercase',
    letterSpacing: '0.08em', borderBottom: '1px solid var(--line2)',
    whiteSpace: 'nowrap', fontSize: 9, color: 'var(--t3)',
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, ...MONO }}>
      {/* Metadata KV */}
      {metaKV.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {metaKV.map((kv, i) => (
            <div key={i} style={{ display: 'flex', gap: 6, fontSize: 9 }}>
              <span style={{ color: 'var(--t2)', minWidth: 80 }}>{kv.label}</span>
              <span style={{ color: 'var(--t1)' }}>{kv.value}</span>
            </div>
          ))}
        </div>
      )}

      {/* Symbol-level coverage */}
      {symTotal > 0 && (
        <div>
          <div style={{ fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.12em', color: 'var(--t3)', marginBottom: 8 }}>
            1. Symbol-level coverage
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 10 }}>
            {[
              { label: 'Total symbols', value: symTotal.toLocaleString(), color: 'var(--t0)' },
              { label: 'Matched', value: `${symMatched.toLocaleString()} (${symMatchedPct})`, color: covColor(symMatchedPct) },
              { label: 'Unmatched', value: `${symUnmatched.toLocaleString()} (${symUnmatchedPct})`, color: symUnmatched === 0 ? 'var(--green)' : 'var(--red)' },
            ].map((card, i) => (
              <div key={i} style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 12px', minWidth: 120 }}>
                <div style={{ fontSize: 9, color: 'var(--t2)', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>{card.label}</div>
                <div style={{ fontSize: 13, fontWeight: 700, color: card.color }}>{card.value}</div>
              </div>
            ))}
          </div>
          {/* Coverage bar */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 9 }}>
            <span style={{ color: 'var(--t2)', minWidth: 90 }}>Symbol coverage</span>
            <div style={{ flex: 1, maxWidth: 180, height: 4, background: 'var(--bg4)', borderRadius: 2, overflow: 'hidden' }}>
              <div style={{ height: '100%', width: `${(symMatched / symTotal) * 100}%`, background: covColor(symMatchedPct), borderRadius: 2 }} />
            </div>
            <span style={{ color: covColor(symMatchedPct), fontWeight: 700 }}>{symMatchedPct}</span>
          </div>
          {/* Unmatched chips */}
          {unmatchedSymbols.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <div style={{ fontSize: 9, color: 'var(--t2)', marginBottom: 5 }}>Unmatched symbols ({unmatchedSymbols.length})</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                {unmatchedSymbols.map((sym, i) => (
                  <span key={i} style={{
                    background: 'var(--red-dim)', border: '1px solid var(--red)', borderRadius: 2,
                    padding: '1px 5px', fontSize: 9, color: 'var(--red)',
                  }}>{sym}</span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Row-level match rate */}
      {rowTotal > 0 && (
        <div>
          <div style={{ fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.12em', color: 'var(--t3)', marginBottom: 8 }}>
            2. Row-level match rate
          </div>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 10 }}>
            {[
              { label: 'Total rows', value: rowTotal.toLocaleString(), color: 'var(--t0)' },
              { label: 'Matched', value: `${rowMatched.toLocaleString()} (${rowMatchedPct})`, color: covColor(rowMatchedPct) },
              { label: 'Missing', value: `${rowMissing.toLocaleString()} (${rowMissingPct})`, color: rowMissing === 0 ? 'var(--green)' : parseFloat(rowMissingPct) > 25 ? 'var(--amber)' : 'var(--t1)' },
            ].map((card, i) => (
              <div key={i} style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '8px 12px', minWidth: 120 }}>
                <div style={{ fontSize: 9, color: 'var(--t2)', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>{card.label}</div>
                <div style={{ fontSize: 13, fontWeight: 700, color: card.color }}>{card.value}</div>
              </div>
            ))}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 9 }}>
            <span style={{ color: 'var(--t2)', minWidth: 90 }}>Row coverage</span>
            <div style={{ flex: 1, maxWidth: 180, height: 4, background: 'var(--bg4)', borderRadius: 2, overflow: 'hidden' }}>
              <div style={{ height: '100%', width: `${(rowMatched / rowTotal) * 100}%`, background: covColor(rowMatchedPct), borderRadius: 2 }} />
            </div>
            <span style={{ color: covColor(rowMatchedPct), fontWeight: 700 }}>{rowMatchedPct}</span>
          </div>
        </div>
      )}

      {/* Missing breakdown table */}
      {mcRows.length > 0 && (
        <div>
          <div style={{ fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.12em', color: 'var(--t3)', marginBottom: 8 }}>
            Missing mcap breakdown by symbol
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO, width: '100%' }}>
              <thead>
                <tr>
                  <th style={{ ...thStyle, textAlign: 'left' }}>Symbol</th>
                  <th style={{ ...thStyle, textAlign: 'right' }}>Missing days</th>
                  <th style={{ ...thStyle, textAlign: 'left', paddingLeft: 10 }}></th>
                  <th style={{ ...thStyle, textAlign: 'left' }}>Dates</th>
                </tr>
              </thead>
              <tbody>
                {mcRows.map((row, ri) => {
                  const barW = Math.round((row.missingDays / maxMissing) * BAR_MAX);
                  const barColor = row.missingDays >= 10 ? 'var(--red)' : row.missingDays >= 5 ? 'var(--amber)' : 'var(--t2)';
                  return (
                    <tr key={ri} style={{ background: ri % 2 === 1 ? 'rgba(255,255,255,0.012)' : 'transparent' }}>
                      <td style={{ padding: '2px 8px', color: 'var(--t0)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)', fontWeight: row.missingDays >= 10 ? 700 : 400 }}>
                        {row.symbol}
                      </td>
                      <td style={{ padding: '2px 8px', textAlign: 'right', color: barColor, whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)', fontWeight: row.missingDays >= 10 ? 700 : 400 }}>
                        {row.missingDays}
                      </td>
                      <td style={{ padding: '2px 4px 2px 10px', borderBottom: '1px solid var(--line)', verticalAlign: 'middle', width: BAR_MAX + 8 }}>
                        <div style={{ width: BAR_MAX, display: 'flex', alignItems: 'center' }}>
                          <div style={{ width: barW, height: 3, borderRadius: 1, background: barColor, opacity: 0.8 }} />
                        </div>
                      </td>
                      <td style={{ padding: '2px 8px', color: 'var(--t2)', borderBottom: '1px solid var(--line)', maxWidth: 320, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {row.dates}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function renderRunSummary(body: string) {
  interface RunRow {
    filter: string; sharpe: number; cagr: number; maxdd: number; active: number;
    wfcv: number; totret: number; eq: number; wst1d: number; wst1w: number;
    wst1m: number; dsr: number; grd: number; isBest: boolean;
  }
  const rows: RunRow[] = [];
  const metaKV: { label: string; value: string }[] = [];
  let inTable = false;

  for (const line of body.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // Dividers / block headers
    if (/^[═─=\-█]+$/.test(trimmed)) continue;

    // Column header line
    if (/^Filter\s/.test(trimmed) && /Sharpe/.test(trimmed)) { inTable = true; continue; }

    if (!inTable) {
      // Metadata KV before the table
      const m = trimmed.match(/^(.+?)\s*:\s*(.+)$/);
      if (m) metaKV.push({ label: m[1].trim(), value: m[2].trim() });
      continue;
    }

    // Data rows
    const isBest = trimmed.endsWith('◄');
    const cleanLine = isBest ? trimmed.slice(0, -1).trim() : trimmed;
    const tokens = cleanLine.split(/\s{2,}/).filter(Boolean);
    if (tokens.length < 12) continue;

    const p = (s: string) => parseFloat(s.replace(/[%×,]/g, '')) || 0;
    rows.push({
      filter: tokens[0],
      sharpe: p(tokens[1]),
      cagr: p(tokens[2]),
      maxdd: p(tokens[3]),
      active: parseInt(tokens[4], 10) || 0,
      wfcv: p(tokens[5]),
      totret: p(tokens[6]),
      eq: p(tokens[7]),
      wst1d: p(tokens[8]),
      wst1w: p(tokens[9]),
      wst1m: p(tokens[10]),
      dsr: p(tokens[11]),
      grd: parseInt(tokens[12] || tokens[11], 10) || 0,
      isBest,
    });
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  const maxSharpe = Math.max(...rows.map(r => r.sharpe));
  const BAR_MAX = 48;

  const sharpeColor = (v: number) => v >= 3 ? 'var(--green)' : v >= 2 ? 'var(--amber)' : 'var(--red)';
  const ddColor = (v: number) => v >= -30 ? 'var(--green)' : v >= -50 ? 'var(--amber)' : 'var(--red)';
  const wfcvColor = (v: number) => v <= 0.3 ? 'var(--green)' : v <= 0.5 ? 'var(--amber)' : 'var(--red)';
  const dsrColor = (v: number) => v >= 90 ? 'var(--green)' : v >= 80 ? 'var(--amber)' : 'var(--red)';
  const pctFmt = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;

  const thStyle: React.CSSProperties = {
    padding: '3px 8px', fontWeight: 700, textTransform: 'uppercase',
    letterSpacing: '0.08em', borderBottom: '1px solid var(--line2)',
    whiteSpace: 'nowrap', fontSize: 9, color: 'var(--t3)',
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14, ...MONO }}>
      {/* Metadata KV */}
      {metaKV.length > 0 && (
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
          {metaKV.map((kv, i) => (
            <div key={i} style={{ display: 'flex', gap: 5, fontSize: 9 }}>
              <span style={{ color: 'var(--t2)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>{kv.label}</span>
              <span style={{ color: 'var(--t1)', fontWeight: 700 }}>{kv.value}</span>
            </div>
          ))}
        </div>
      )}

      {/* Filter comparison table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO, width: '100%' }}>
          <thead>
            <tr>
              <th style={{ ...thStyle, textAlign: 'left' }}>Filter</th>
              <th style={{ ...thStyle, textAlign: 'left', paddingLeft: 4, paddingRight: 4 }}></th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Sharpe</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>CAGR%</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>MaxDD%</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Active</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>WF-CV</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>TotRet%</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Eq</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Wst 1D</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Wst 1W</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Wst 1M</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>DSR%</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Grd</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => {
              const barW = Math.round((row.sharpe / maxSharpe) * BAR_MAX);
              return (
                <tr key={ri} style={{ background: row.isBest ? 'var(--green-dim)' : ri % 2 === 1 ? 'rgba(255,255,255,0.012)' : 'transparent' }}>
                  <td style={{
                    padding: '3px 8px', color: row.isBest ? 'var(--green)' : 'var(--t1)',
                    whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)',
                    fontWeight: row.isBest ? 700 : 400,
                  }}>
                    {row.isBest && <span style={{ marginRight: 4, fontSize: 8 }}>★</span>}
                    {row.filter}
                  </td>
                  {/* Sharpe bar */}
                  <td style={{ padding: '3px 4px', borderBottom: '1px solid var(--line)', verticalAlign: 'middle' }}>
                    <div style={{ width: BAR_MAX, display: 'flex', alignItems: 'center' }}>
                      <div style={{ width: barW, height: 3, borderRadius: 1, background: sharpeColor(row.sharpe), opacity: row.isBest ? 1 : 0.7 }} />
                    </div>
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: sharpeColor(row.sharpe), fontWeight: row.isBest ? 700 : 400, whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.sharpe.toFixed(3)}
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: 'var(--green)', fontWeight: row.isBest ? 700 : 400, whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.cagr.toLocaleString(undefined, { maximumFractionDigits: 1 })}%
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: ddColor(row.maxdd), fontWeight: row.isBest ? 700 : 400, whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {pctFmt(row.maxdd)}
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: 'var(--t2)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.active}
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: wfcvColor(row.wfcv), fontWeight: row.isBest ? 700 : 400, whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.wfcv.toFixed(3)}
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: 'var(--t1)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.totret.toLocaleString(undefined, { maximumFractionDigits: 1 })}%
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: 'var(--t1)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.eq.toFixed(2)}×
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: ddColor(row.wst1d), whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {pctFmt(row.wst1d)}
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: ddColor(row.wst1w), whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {pctFmt(row.wst1w)}
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: ddColor(row.wst1m), whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {pctFmt(row.wst1m)}
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: dsrColor(row.dsr), fontWeight: row.isBest ? 700 : 400, whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.dsr.toFixed(1)}%
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: row.isBest ? 'var(--green)' : 'var(--t1)', fontWeight: row.isBest ? 700 : 400, whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.grd}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function renderScorecardTable(body: string) {
  interface ScorecardRow { metric: string; goal: string; actual: string; status: 'pass' | 'fail' | 'borderline' | 'na'; }
  const rows: ScorecardRow[] = [];
  let bestFilter = '';
  let summaryLine = '';

  for (const line of body.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    if (/^[═─=\-]+$/.test(trimmed)) continue;

    // Best filter header
    const bfm = trimmed.match(/^Best filter:\s*(.+)$/i);
    if (bfm) { bestFilter = bfm[1].trim(); continue; }

    // Summary line: "✅ 53 Pass   ❌ 8 Fail ..."
    if (/✅.*Pass.*❌.*Fail/i.test(trimmed)) { summaryLine = trimmed; continue; }

    // Saved → ... line
    if (/^Saved\s*→/.test(trimmed)) continue;

    // Column header line
    if (/^Metric\s/.test(trimmed) && /Goal/.test(trimmed) && /Status/.test(trimmed)) continue;

    // Data rows: split on 2+ spaces, strip empty first token from leading indent
    const tokens = trimmed.split(/\s{2,}/).filter(Boolean);
    if (tokens.length < 3) continue;

    const metric = tokens[0];
    const goal = tokens[1];
    const actual = tokens[2];
    const statusRaw = tokens.slice(3).join(' ').trim();

    let status: ScorecardRow['status'] = 'na';
    if (statusRaw.startsWith('✅')) status = 'pass';
    else if (statusRaw.startsWith('❌')) status = 'fail';
    else if (statusRaw.startsWith('⚠')) status = 'borderline';
    else if (statusRaw.startsWith('──')) status = 'na';
    else continue; // not a data row

    rows.push({ metric, goal, actual, status });
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  // Parse summary counts
  let passCount = 0, failCount = 0, borderlineCount = 0, naCount = 0, totalCount = 0;
  if (summaryLine) {
    const pm = summaryLine.match(/✅\s*(\d+)\s*Pass/); if (pm) passCount = parseInt(pm[1], 10);
    const fm = summaryLine.match(/❌\s*(\d+)\s*Fail/); if (fm) failCount = parseInt(fm[1], 10);
    const bm = summaryLine.match(/⚠\s*(\d+)\s*Borderline/); if (bm) borderlineCount = parseInt(bm[1], 10);
    const nm = summaryLine.match(/──\s*(\d+)\s*N\/A/); if (nm) naCount = parseInt(nm[1], 10);
    const tm = summaryLine.match(/of\s*(\d+)\s*metrics/); if (tm) totalCount = parseInt(tm[1], 10);
  }
  const scoredCount = passCount + failCount + borderlineCount;
  const passRate = scoredCount > 0 ? (passCount / scoredCount) * 100 : 0;

  const statusColor = (s: ScorecardRow['status']) => {
    if (s === 'pass') return 'var(--green)';
    if (s === 'fail') return 'var(--red)';
    if (s === 'borderline') return 'var(--amber)';
    return 'var(--t2)';
  };
  const rowBg = (s: ScorecardRow['status']) => {
    if (s === 'fail') return 'var(--red-dim)';
    if (s === 'borderline') return 'var(--amber-dim)';
    return 'transparent';
  };
  const statusLabel = (s: ScorecardRow['status']) => {
    if (s === 'pass') return 'Pass';
    if (s === 'fail') return 'Fail';
    if (s === 'borderline') return 'Borderline';
    return 'N/A';
  };
  const statusDot = (s: ScorecardRow['status']) => {
    if (s === 'pass') return '✓';
    if (s === 'fail') return '✗';
    if (s === 'borderline') return '⚠';
    return '—';
  };

  const thStyle: React.CSSProperties = {
    padding: '3px 8px', fontWeight: 700, textTransform: 'uppercase',
    letterSpacing: '0.08em', borderBottom: '1px solid var(--line2)',
    whiteSpace: 'nowrap', fontSize: 9, color: 'var(--t3)',
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14, ...MONO }}>
      {/* Best filter */}
      {bestFilter && (
        <div style={{ display: 'flex', gap: 6, fontSize: 9, alignItems: 'center' }}>
          <span style={{ color: 'var(--t2)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Best filter</span>
          <span style={{ color: 'var(--t0)', fontWeight: 700 }}>{bestFilter}</span>
        </div>
      )}

      {/* Summary cards + pass-rate bar */}
      {scoredCount > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {[
              { label: 'Pass', count: passCount, color: 'var(--green)' },
              { label: 'Fail', count: failCount, color: 'var(--red)' },
              { label: 'Borderline', count: borderlineCount, color: 'var(--amber)' },
              { label: 'N/A', count: naCount, color: 'var(--t2)' },
              ...(totalCount > 0 ? [{ label: 'Total metrics', count: totalCount, color: 'var(--t1)' }] : []),
            ].map((card, i) => (
              <div key={i} style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '6px 10px', minWidth: 72 }}>
                <div style={{ fontSize: 9, color: 'var(--t2)', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 3 }}>{card.label}</div>
                <div style={{ fontSize: 15, fontWeight: 700, color: card.color }}>{card.count}</div>
              </div>
            ))}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 9 }}>
            <span style={{ color: 'var(--t2)', minWidth: 70 }}>Pass rate</span>
            <div style={{ flex: 1, maxWidth: 200, height: 4, background: 'var(--bg4)', borderRadius: 2, overflow: 'hidden' }}>
              <div style={{ height: '100%', width: `${passRate}%`, background: passRate >= 80 ? 'var(--green)' : passRate >= 60 ? 'var(--amber)' : 'var(--red)', borderRadius: 2 }} />
            </div>
            <span style={{ color: passRate >= 80 ? 'var(--green)' : passRate >= 60 ? 'var(--amber)' : 'var(--red)', fontWeight: 700 }}>{passRate.toFixed(0)}%</span>
          </div>
        </div>
      )}

      {/* Scorecard table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO, width: '100%' }}>
          <thead>
            <tr>
              <th style={{ ...thStyle, textAlign: 'left' }}>Metric</th>
              <th style={{ ...thStyle, textAlign: 'left' }}>Goal</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Actual</th>
              <th style={{ ...thStyle, textAlign: 'center' }}>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, ri) => (
              <tr key={ri} style={{ background: rowBg(row.status) }}>
                <td style={{
                  padding: '2px 8px', color: row.status === 'na' ? 'var(--t2)' : 'var(--t1)',
                  whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)',
                  opacity: row.status === 'na' ? 0.6 : 1,
                }}>
                  {row.metric}
                </td>
                <td style={{
                  padding: '2px 8px', color: 'var(--t2)',
                  whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)',
                  opacity: row.status === 'na' ? 0.6 : 1,
                }}>
                  {row.goal}
                </td>
                <td style={{
                  padding: '2px 8px', textAlign: 'right',
                  color: statusColor(row.status),
                  whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)',
                  fontWeight: row.status === 'fail' || row.status === 'borderline' ? 700 : 400,
                  opacity: row.status === 'na' ? 0.5 : 1,
                }}>
                  {row.actual}
                </td>
                <td style={{
                  padding: '2px 8px', textAlign: 'center',
                  borderBottom: '1px solid var(--line)', whiteSpace: 'nowrap',
                }}>
                  <span style={{
                    display: 'inline-flex', alignItems: 'center', gap: 3,
                    color: statusColor(row.status),
                    fontSize: 9, fontWeight: row.status !== 'na' ? 700 : 400,
                    opacity: row.status === 'na' ? 0.45 : 1,
                  }}>
                    <span>{statusDot(row.status)}</span>
                    <span>{statusLabel(row.status)}</span>
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function renderTailGuardrailGridSweep(body: string) {
  const lines = body.split('\n');

  let baselineDrop = '';
  let baselineVol = '';
  let gridSpec = '';

  interface TailGridCell {
    drop: string;
    vol: string;
    sharpe: number;
    cagr: number;
    maxdd: number;
    active: number;
    wfcv: number;
    isBaseline: boolean;
    flaggedDays: number;
    dropDays: number;
    volDays: number;
    bothDays: number;
  }

  const cells: TailGridCell[] = [];
  let pendingGuardrail: { flaggedDays: number; dropDays: number; volDays: number; bothDays: number } | null = null;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || /^[═─]+$/.test(trimmed)) continue;

    const baseM = trimmed.match(/^Baseline:\s*drop=([\d.]+)%\s+vol=([\d.]+)/i);
    if (baseM) { baselineDrop = baseM[1]; baselineVol = baseM[2]; continue; }

    const gridM = trimmed.match(/^Grid:\s*(.+)$/i);
    if (gridM) { gridSpec = gridM[1]; continue; }

    const guardrailM = trimmed.match(/drop[^=]+=(\d+)d\s+vol[^=]+=(\d+)d\s+both=(\d+)d\s+total=(\d+)d/i);
    if (guardrailM && trimmed.startsWith('Tail guardrail')) {
      pendingGuardrail = {
        dropDays: parseInt(guardrailM[1]),
        volDays: parseInt(guardrailM[2]),
        bothDays: parseInt(guardrailM[3]),
        flaggedDays: parseInt(guardrailM[4]),
      };
      continue;
    }

    const metricsM = trimmed.match(/^drop=([\d.]+)%\s+vol=([\d.]+)x\s+[│|]\s+Sharpe=\s*([-\d.]+)\s+CAGR=\s*([-\d.]+)%\s+MaxDD=\s*([-\d.]+)%\s+Active=\s*(\d+)\s+WF_CV=\s*([-\d.]+)(.*)?/);
    if (metricsM) {
      cells.push({
        drop: metricsM[1],
        vol: metricsM[2],
        sharpe: parseFloat(metricsM[3]),
        cagr: parseFloat(metricsM[4]),
        maxdd: parseFloat(metricsM[5]),
        active: parseInt(metricsM[6]),
        wfcv: parseFloat(metricsM[7]),
        isBaseline: /BASELINE/i.test(metricsM[8] ?? ''),
        flaggedDays: pendingGuardrail?.flaggedDays ?? 0,
        dropDays: pendingGuardrail?.dropDays ?? 0,
        volDays: pendingGuardrail?.volDays ?? 0,
        bothDays: pendingGuardrail?.bothDays ?? 0,
      });
      pendingGuardrail = null;
      continue;
    }
  }

  if (cells.length === 0) return <PreFallback body={body} />;

  const allSharpes = cells.map((c) => c.sharpe).filter((s) => isFinite(s));
  const maxSharpe = Math.max(...allSharpes);
  const minSharpe = Math.min(...allSharpes);

  const sharpeColor = (s: number) => {
    if (!isFinite(s)) return 'var(--t3)';
    if (s >= 3.0) return 'var(--green)';
    if (s >= 2.0) return 'var(--amber)';
    return 'var(--red)';
  };

  const maxddColor = (v: number) => {
    if (Math.abs(v) <= 20) return 'var(--green)';
    if (Math.abs(v) <= 35) return 'var(--amber)';
    return 'var(--red)';
  };

  const thS: React.CSSProperties = {
    padding: '3px 10px 3px 6px', textAlign: 'right', fontWeight: 700,
    textTransform: 'uppercase', letterSpacing: '0.08em', fontSize: 8.5,
    color: 'var(--t3)', borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap',
  };

  const baselineCell = cells.find((c) => c.isBaseline);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, fontSize: 9, ...MONO }}>
      {/* Metadata bar */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        {baselineDrop && (
          <span style={{ color: 'var(--t2)' }}>
            Baseline: <span style={{ color: 'var(--green)' }}>drop={baselineDrop}%</span>
            {baselineVol && <span> &nbsp;vol=<span style={{ color: 'var(--green)' }}>{baselineVol}×</span></span>}
          </span>
        )}
        {gridSpec && <span style={{ color: 'var(--t3)' }}>{gridSpec}</span>}
      </div>

      {/* Baseline guardrail summary */}
      {baselineCell && (
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', padding: '5px 8px', background: 'var(--bg0)', border: '1px solid var(--line)', borderRadius: 3 }}>
          <span style={{ color: 'var(--t3)', fontSize: 8.5, textTransform: 'uppercase', letterSpacing: '0.1em' }}>Baseline guardrail</span>
          <span style={{ color: 'var(--t2)' }}>drop trigger: <span style={{ color: 'var(--t1)' }}>{baselineCell.dropDays}d</span></span>
          <span style={{ color: 'var(--t2)' }}>vol trigger: <span style={{ color: 'var(--t1)' }}>{baselineCell.volDays}d</span></span>
          <span style={{ color: 'var(--t2)' }}>both: <span style={{ color: 'var(--t1)' }}>{baselineCell.bothDays}d</span></span>
          <span style={{ color: 'var(--t2)' }}>total flagged: <span style={{ color: 'var(--amber)' }}>{baselineCell.flaggedDays}d</span></span>
        </div>
      )}

      {/* Main grid table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
          <thead>
            <tr>
              <th style={{ ...thS, textAlign: 'left' }}>Drop%</th>
              <th style={{ ...thS }}>Vol×</th>
              <th style={{ ...thS }}>Sharpe</th>
              <th style={{ ...thS }}>CAGR%</th>
              <th style={{ ...thS }}>MaxDD</th>
              <th style={{ ...thS }}>Active</th>
              <th style={{ ...thS }}>WF-CV</th>
              <th style={{ ...thS }}>Flagged</th>
            </tr>
          </thead>
          <tbody>
            {cells.map((cell, i) => {
              const sharpeFrac = maxSharpe > minSharpe ? (cell.sharpe - minSharpe) / (maxSharpe - minSharpe) : 1;
              const isBase = cell.isBaseline;
              return (
                <tr
                  key={i}
                  style={{
                    background: isBase ? 'var(--green-dim)' : i % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent',
                    outline: isBase ? '1px solid var(--green-mid)' : undefined,
                  }}
                >
                  <td style={{ padding: '3px 10px 3px 6px', color: 'var(--t1)', whiteSpace: 'nowrap' }}>
                    {cell.drop}%
                    {isBase && <span style={{ marginLeft: 5, fontSize: 7.5, color: 'var(--green)', letterSpacing: '0.06em', textTransform: 'uppercase' }}>◄ base</span>}
                  </td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{cell.vol}×</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5, justifyContent: 'flex-end' }}>
                      <div style={{ width: 36, height: 3, background: 'var(--bg3)', borderRadius: 1, flexShrink: 0 }}>
                        <div style={{ width: `${Math.round(sharpeFrac * 100)}%`, height: '100%', background: sharpeColor(cell.sharpe), borderRadius: 1 }} />
                      </div>
                      <span style={{ color: sharpeColor(cell.sharpe), fontWeight: isBase ? 700 : 400 }}>{cell.sharpe.toFixed(3)}</span>
                    </div>
                  </td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t1)' }}>{cell.cagr.toFixed(1)}%</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: maxddColor(cell.maxdd) }}>{cell.maxdd.toFixed(2)}%</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{cell.active}</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: cell.wfcv >= 0.7 ? 'var(--green)' : cell.wfcv >= 0.5 ? 'var(--amber)' : 'var(--red)' }}>{cell.wfcv.toFixed(3)}</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{cell.flaggedDays}d</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── L_HIGH SURFACE — RANKED BY SHARPE ────────────────────────────────────────
function renderLHighRanked(body: string): React.ReactNode {
  interface LHighRankedRow {
    lhigh: number; sharpe: number; cagr: number; maxdd: number;
    isSh: number; oosSh: number | null; decay: number | null;
    wfcv: number; meanOos: number; f5: number; f8: number; unstbl: number;
    isBaseline: boolean;
  }

  const rows: LHighRankedRow[] = [];
  let inData = false;
  for (const line of body.split('\n')) {
    const t = line.trim();
    if (/^L_HIGH\s+Sharpe/i.test(t)) { inData = true; continue; }
    if (/^[─═]+$/.test(t)) continue;
    if (!inData || !t) continue;
    // stop at CSV/Chart lines
    if (/^(CSV|Chart):/i.test(t)) break;
    const tok = t.replace(/\s*◄.*$/, '').trim().split(/\s+/);
    if (tok.length < 11) continue;
    const pf = (s: string) => parseFloat(s);
    const nanOrNum = (s: string) => (s.toLowerCase() === 'nan' ? null : pf(s));
    rows.push({
      lhigh: pf(tok[0]), sharpe: pf(tok[1]), cagr: pf(tok[2]),
      maxdd: pf(tok[3]), isSh: pf(tok[4]),
      oosSh: nanOrNum(tok[5]), decay: nanOrNum(tok[6]),
      wfcv: pf(tok[7]), meanOos: pf(tok[8]), f5: pf(tok[9]), f8: pf(tok[10]),
      unstbl: tok[11] ? pf(tok[11]) : 0,
      isBaseline: /◄/.test(line),
    });
  }
  if (rows.length === 0) return <PreFallback body={body} />;

  const allSharpes = rows.map((r) => r.sharpe).filter((s) => isFinite(s));
  const maxSharpe = Math.max(...allSharpes);
  const minSharpe = Math.min(...allSharpes);
  const sharpeFrac = (s: number) => maxSharpe > minSharpe ? (s - minSharpe) / (maxSharpe - minSharpe) : 1;
  const sharpeColor = (s: number) => s >= 3.0 ? 'var(--green)' : s >= 2.0 ? 'var(--amber)' : 'var(--red)';
  const maxddColor = (v: number) => Math.abs(v) <= 20 ? 'var(--green)' : Math.abs(v) <= 35 ? 'var(--amber)' : 'var(--red)';
  const wfcvColor = (v: number) => v >= 0.7 ? 'var(--green)' : v >= 0.5 ? 'var(--amber)' : 'var(--red)';
  const unstblColor = (v: number) => v === 0 ? 'var(--t3)' : v <= 1 ? 'var(--amber)' : 'var(--red)';
  const fmt = (v: number | null, digits: number, suffix = '') =>
    v === null || !isFinite(v) ? <span style={{ color: 'var(--t4)' }}>—</span> : <>{v.toFixed(digits)}{suffix}</>;

  const thS: React.CSSProperties = {
    padding: '3px 8px 3px 6px', textAlign: 'right', fontWeight: 700,
    textTransform: 'uppercase', letterSpacing: '0.08em', fontSize: 8,
    color: 'var(--t3)', borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap',
  };

  const baselineRow = rows.find((r) => r.isBaseline);
  const peakRow = rows.reduce((a, b) => a.sharpe >= b.sharpe ? a : b, rows[0]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10, fontSize: 9, ...MONO }}>
      {/* Meta bar */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        <span style={{ color: 'var(--t2)' }}>
          Peak Sharpe: <span style={{ color: 'var(--green)', fontWeight: 700 }}>{peakRow.sharpe.toFixed(3)}</span>
          <span style={{ color: 'var(--t3)' }}> @ L_HIGH={peakRow.lhigh.toFixed(1)}</span>
        </span>
        {baselineRow && (
          <span style={{ color: 'var(--t2)' }}>
            Baseline: <span style={{ color: 'var(--green)' }}>{baselineRow.sharpe.toFixed(3)}</span>
            <span style={{ color: 'var(--t3)' }}> @ L_HIGH={baselineRow.lhigh.toFixed(1)}</span>
          </span>
        )}
        <span style={{ marginLeft: 'auto', color: 'var(--t3)' }}>{rows.length} values</span>
      </div>

      {/* Table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
          <thead>
            <tr>
              <th style={{ ...thS, textAlign: 'left' }}>L_HIGH</th>
              <th style={{ ...thS, minWidth: 80 }}>Sharpe</th>
              <th style={{ ...thS }}>CAGR%</th>
              <th style={{ ...thS }}>MaxDD%</th>
              <th style={{ ...thS }}>IS Sh</th>
              <th style={{ ...thS }}>OOS Sh</th>
              <th style={{ ...thS }}>Decay%</th>
              <th style={{ ...thS }}>WF-CV</th>
              <th style={{ ...thS }}>MeanOOS</th>
              <th style={{ ...thS }}>F5 Sh</th>
              <th style={{ ...thS }}>F8 Sh</th>
              <th style={{ ...thS }}>Unstbl</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const frac = sharpeFrac(row.sharpe);
              const isBase = row.isBaseline;
              const isPeak = row.sharpe === maxSharpe && !isBase;
              return (
                <tr
                  key={i}
                  style={{
                    background: isBase ? 'var(--green-dim)' : isPeak ? 'rgba(255,255,255,0.03)' : i % 2 === 1 ? 'rgba(255,255,255,0.012)' : 'transparent',
                    outline: isBase ? '1px solid var(--green-mid)' : undefined,
                  }}
                >
                  <td style={{ padding: '3px 10px 3px 6px', color: 'var(--t1)', whiteSpace: 'nowrap' }}>
                    {row.lhigh.toFixed(1)}
                    {isBase && <span style={{ marginLeft: 5, fontSize: 7.5, color: 'var(--green)', letterSpacing: '0.06em', textTransform: 'uppercase' }}>◄ base</span>}
                    {isPeak && <span style={{ marginLeft: 5, fontSize: 7.5, color: 'var(--green)', opacity: 0.7, letterSpacing: '0.06em' }}>★</span>}
                  </td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5, justifyContent: 'flex-end' }}>
                      <div style={{ width: 40, height: 3, background: 'var(--bg3)', borderRadius: 1, flexShrink: 0 }}>
                        <div style={{ width: `${Math.round(frac * 100)}%`, height: '100%', background: sharpeColor(row.sharpe), borderRadius: 1 }} />
                      </div>
                      <span style={{ color: sharpeColor(row.sharpe), fontWeight: isBase || isPeak ? 700 : 400 }}>{row.sharpe.toFixed(3)}</span>
                    </div>
                  </td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{fmt(row.cagr, 0)}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: maxddColor(row.maxdd) }}>{fmt(row.maxdd, 2, '%')}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: sharpeColor(row.isSh) }}>{fmt(row.isSh, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: row.oosSh !== null ? sharpeColor(row.oosSh) : 'var(--t4)' }}>{fmt(row.oosSh, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{fmt(row.decay, 1, '%')}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: wfcvColor(row.wfcv) }}>{fmt(row.wfcv, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: sharpeColor(row.meanOos) }}>{fmt(row.meanOos, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: sharpeColor(row.f5) }}>{fmt(row.f5, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: sharpeColor(row.f8) }}>{fmt(row.f8, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 6px', textAlign: 'right', color: unstblColor(row.unstbl) }}>{row.unstbl.toFixed(1)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── PARAMETER SWEEP — L_HIGH SURFACE ─────────────────────────────────────────
function renderLHighSweep(body: string, title: string): React.ReactNode {
  interface LHighRow {
    lhigh: number; sharpe: number; maxdd: number; wfcv: number; f5: number; f8: number; isBaseline: boolean;
  }
  const rows: LHighRow[] = [];
  for (const line of body.split('\n')) {
    const m = line.match(/L_HIGH=([\d.]+)\s+Sharpe=([-\d.]+)\s+MaxDD=([-\d.]+)%\s+WF_CV=([\d.]+)\s+F5=([-\d.]+)\s+F8=([-\d.]+)/i);
    if (!m) continue;
    rows.push({ lhigh: parseFloat(m[1]), sharpe: parseFloat(m[2]), maxdd: parseFloat(m[3]), wfcv: parseFloat(m[4]), f5: parseFloat(m[5]), f8: parseFloat(m[6]), isBaseline: /◄/.test(line) });
  }
  if (rows.length === 0) return <PreFallback body={body} />;

  const filterM = title.match(/\(([^)]+)\)\s*$/);
  const filterCtx = filterM ? filterM[1] : '';
  const allSharpes = rows.map((r) => r.sharpe).filter((s) => isFinite(s));
  const maxSharpe = Math.max(...allSharpes);
  const minSharpe = Math.min(...allSharpes);
  const sharpeColor = (s: number) => s >= 3.0 ? 'var(--green)' : s >= 2.0 ? 'var(--amber)' : 'var(--red)';
  const maxddColor = (v: number) => Math.abs(v) <= 20 ? 'var(--green)' : Math.abs(v) <= 35 ? 'var(--amber)' : 'var(--red)';
  const wfcvColor = (v: number) => v >= 0.7 ? 'var(--green)' : v >= 0.5 ? 'var(--amber)' : 'var(--red)';
  const thS: React.CSSProperties = { padding: '3px 10px 3px 6px', textAlign: 'right', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', fontSize: 8.5, color: 'var(--t3)', borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap' };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, fontSize: 9, ...MONO }}>
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        {filterCtx && <span style={{ color: 'var(--t2)' }}>{filterCtx}</span>}
        <span style={{ marginLeft: 'auto', color: 'var(--t3)' }}>{rows.length} L_HIGH values</span>
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
          <thead>
            <tr>
              <th style={{ ...thS, textAlign: 'left' }}>L_HIGH</th>
              <th style={{ ...thS }}>Sharpe</th>
              <th style={{ ...thS }}>MaxDD%</th>
              <th style={{ ...thS }}>WF_CV</th>
              <th style={{ ...thS }}>F5 Sh</th>
              <th style={{ ...thS }}>F8 Sh</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const frac = maxSharpe > minSharpe ? (row.sharpe - minSharpe) / (maxSharpe - minSharpe) : 1;
              return (
                <tr key={i} style={{ background: row.isBaseline ? 'var(--green-dim)' : i % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent', outline: row.isBaseline ? '1px solid var(--green-mid)' : undefined }}>
                  <td style={{ padding: '3px 10px 3px 6px', color: 'var(--t1)', whiteSpace: 'nowrap' }}>
                    {row.lhigh.toFixed(1)}
                    {row.isBaseline && <span style={{ marginLeft: 5, fontSize: 7.5, color: 'var(--green)', letterSpacing: '0.06em', textTransform: 'uppercase' }}>◄ base</span>}
                  </td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5, justifyContent: 'flex-end' }}>
                      <div style={{ width: 36, height: 3, background: 'var(--bg3)', borderRadius: 1, flexShrink: 0 }}>
                        <div style={{ width: `${Math.round(frac * 100)}%`, height: '100%', background: sharpeColor(row.sharpe), borderRadius: 1 }} />
                      </div>
                      <span style={{ color: sharpeColor(row.sharpe), fontWeight: row.isBaseline ? 700 : 400 }}>{row.sharpe.toFixed(3)}</span>
                    </div>
                  </td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: maxddColor(row.maxdd) }}>{row.maxdd.toFixed(1)}%</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: wfcvColor(row.wfcv) }}>{row.wfcv.toFixed(3)}</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{row.f5.toFixed(3)}</td>
                  <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{row.f8.toFixed(3)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── PARAMETER SWEEP — TRAIL_DD × EARLY_X (WIDE / NARROW) ─────────────────────
function renderTrailSweep(body: string, title: string): React.ReactNode {
  interface TrailRow { trail: number; earlyX: number; sharpe: number; maxdd: number; wfcv: number; f5: number; f8: number; }
  interface RankedSection { label: string; rows: TrailRow[]; }

  const lines = body.split('\n');
  let gridSpec = '';
  let totalCombos = 0;
  let ranCombos = 0;

  for (const line of lines) {
    const gm = line.match(/Grid:\s*(\d+)\s+TRAIL_DD\s*[×x]\s*(\d+)\s+EARLY_X\s*=\s*(\d+)/i);
    if (gm) { gridSpec = `${gm[1]} TRAIL_DD × ${gm[2]} EARLY_X`; totalCombos = parseInt(gm[3]); continue; }
    const cm = line.match(/\[\s*(\d+)\/(\d+)\]/);
    if (cm) { ranCombos = parseInt(cm[1]); if (!totalCombos) totalCombos = parseInt(cm[2]); }
  }

  const rankedSections: RankedSection[] = [];
  let curSection: RankedSection | null = null;
  let inTable = false;

  for (const line of lines) {
    const t = line.trim();
    const topM = t.match(/^TOP\s+(\d+)\s+-\s+(\w+)\s+-\s+RANKED\s+BY\s+(.+)/i);
    if (topM) {
      if (curSection && curSection.rows.length > 0) rankedSections.push(curSection);
      const rankBy = topM[3].replace(/\s*\(.*\)/, '').trim();
      curSection = { label: `Top ${topM[1]} · ${topM[2]} · by ${rankBy}`, rows: [] };
      inTable = false;
      continue;
    }
    if (/Trail\s+EarlyX\s+Sharpe/i.test(t)) { inTable = true; continue; }
    if (/^[-─]+$/.test(t)) continue;
    if (inTable && curSection) {
      const rm = t.match(/^([\d.]+)\s+(\d+)\s+([-\d.]+)\s+([-\d.]+)\s+([\d.]+)\s+([-\d.]+)\s+([-\d.]+)/);
      if (rm) {
        curSection.rows.push({ trail: parseFloat(rm[1]), earlyX: parseInt(rm[2]), sharpe: parseFloat(rm[3]), maxdd: parseFloat(rm[4]), wfcv: parseFloat(rm[5]), f5: parseFloat(rm[6]), f8: parseFloat(rm[7]) });
      } else if (t && !/^(CSV|Chart):/i.test(t)) {
        inTable = false;
      }
    }
  }
  if (curSection && curSection.rows.length > 0) rankedSections.push(curSection);
  if (rankedSections.length === 0) return <PreFallback body={body} />;

  const sharpeColor = (s: number) => s >= 3.0 ? 'var(--green)' : s >= 2.0 ? 'var(--amber)' : 'var(--red)';
  const maxddColor = (v: number) => Math.abs(v) <= 20 ? 'var(--green)' : Math.abs(v) <= 35 ? 'var(--amber)' : 'var(--red)';
  const wfcvColor = (v: number) => v >= 0.7 ? 'var(--green)' : v >= 0.5 ? 'var(--amber)' : 'var(--red)';
  const filterM = title.match(/\(([^)]+filter[^)]*)\)/i);
  const filterCtx = filterM ? filterM[1] : '';
  const thS: React.CSSProperties = { padding: '3px 10px 3px 6px', textAlign: 'right', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', fontSize: 8.5, color: 'var(--t3)', borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap' };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, fontSize: 9, ...MONO }}>
      {/* Metadata bar */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        {gridSpec && (
          <span style={{ color: 'var(--t2)' }}>
            Grid: <span style={{ color: 'var(--t1)' }}>{gridSpec}</span>
            {totalCombos > 0 && <span style={{ color: 'var(--t3)' }}> = {totalCombos} combinations</span>}
          </span>
        )}
        {totalCombos > 0 && ranCombos >= totalCombos && (
          <span style={{ color: 'var(--green)', fontSize: 8 }}>✓ complete</span>
        )}
        {filterCtx && <span style={{ marginLeft: 'auto', color: 'var(--t3)' }}>{filterCtx}</span>}
      </div>
      {/* Ranked tables */}
      {rankedSections.map((sec, si) => {
        const allS = sec.rows.map((r) => r.sharpe).filter((s) => isFinite(s));
        const maxS = Math.max(...allS);
        const minS = Math.min(...allS);
        return (
          <div key={si} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {sectionLabel(sec.label)}
            <div style={{ overflowX: 'auto' }}>
              <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
                <thead>
                  <tr>
                    <th style={{ ...thS, textAlign: 'left' }}>Trail</th>
                    <th style={{ ...thS }}>Early X</th>
                    <th style={{ ...thS }}>Sharpe</th>
                    <th style={{ ...thS }}>MaxDD%</th>
                    <th style={{ ...thS }}>WF_CV</th>
                    <th style={{ ...thS }}>F5 Sh</th>
                    <th style={{ ...thS }}>F8 Sh</th>
                  </tr>
                </thead>
                <tbody>
                  {sec.rows.map((row, i) => {
                    const frac = maxS > minS ? (row.sharpe - minS) / (maxS - minS) : 1;
                    return (
                      <tr key={i} style={{ background: i % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent' }}>
                        <td style={{ padding: '3px 10px 3px 6px', color: 'var(--t1)', whiteSpace: 'nowrap' }}>{row.trail.toFixed(3)}</td>
                        <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{row.earlyX}</td>
                        <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right' }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 5, justifyContent: 'flex-end' }}>
                            <div style={{ width: 36, height: 3, background: 'var(--bg3)', borderRadius: 1, flexShrink: 0 }}>
                              <div style={{ width: `${Math.round(frac * 100)}%`, height: '100%', background: sharpeColor(row.sharpe), borderRadius: 1 }} />
                            </div>
                            <span style={{ color: sharpeColor(row.sharpe) }}>{row.sharpe.toFixed(3)}</span>
                          </div>
                        </td>
                        <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: maxddColor(row.maxdd) }}>{row.maxdd.toFixed(2)}%</td>
                        <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: wfcvColor(row.wfcv) }}>{row.wfcv.toFixed(3)}</td>
                        <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{row.f5.toFixed(3)}</td>
                        <td style={{ padding: '3px 10px 3px 6px', textAlign: 'right', color: 'var(--t2)' }}>{row.f8.toFixed(3)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── PARAMETER SURFACE MAP (2-D HEAT MAP) ─────────────────────────────────────
function renderParamSurfaceMap(title: string, body: string): React.ReactNode {
  // Extract the two param names from title: "... -- PARAM_X x PARAM_Y"
  const axisM = title.match(/--\s*(\w+)\s+x\s+(\w+)/i);
  if (!axisM) return <PreFallback body={body} />;
  const [, paramX, paramY] = axisM;

  interface SurfaceCell {
    xVal: string; yVal: string;
    sharpe: number; cagr: number; maxdd: number; worst1d: number; wfcv: number;
    isBaseline: boolean;
  }

  let filter = '';
  let gridSpec = '';
  let baselineXStr = '';
  let baselineYStr = '';
  const cells: SurfaceCell[] = [];

  for (const line of body.split('\n')) {
    const t = line.trim();
    if (!t || /^[=─]+$/.test(t)) continue;
    if (/^--\s+Sharpe surface/i.test(t)) break; // stop at ASCII matrix block

    const fgm = t.match(/^Filter:\s*(.+?)\s*\|\s*Grid:\s*(.+)$/i);
    if (fgm) { filter = fgm[1].trim(); gridSpec = fgm[2].trim(); continue; }

    const bm = t.match(/^Baseline:\s*\S+=(\S+)\s+\S+=(\S+)/i);
    if (bm) { baselineXStr = bm[1]; baselineYStr = bm[2]; continue; }

    // Data row: PARAM_X=val  PARAM_Y=val  |  Sharpe= val  CAGR= val%  MaxDD= val%  Worst1D= val%  WF_CV= val
    const dm = t.match(/^\S+=(\S+)\s+\S+=(\S+)\s*\|\s*Sharpe=\s*([-\d.]+)\s+CAGR=\s*([-\d.]+)%\s+MaxDD=\s*([-\d.]+)%\s+Worst1D=\s*([-\d.]+)%\s+WF_CV=\s*([-\d.]+)/i);
    if (dm) {
      cells.push({
        xVal: dm[1], yVal: dm[2],
        sharpe: parseFloat(dm[3]), cagr: parseFloat(dm[4]),
        maxdd: parseFloat(dm[5]), worst1d: parseFloat(dm[6]), wfcv: parseFloat(dm[7]),
        isBaseline: /<-- BASELINE/i.test(t),
      });
      continue;
    }
  }

  if (cells.length === 0) return <PreFallback body={body} />;

  // Mark baseline by value match if not already flagged inline
  if (baselineXStr && baselineYStr && !cells.some((c) => c.isBaseline)) {
    for (const c of cells) {
      if (c.xVal === baselineXStr && c.yVal === baselineYStr) c.isBaseline = true;
    }
  }

  const xVals = [...new Set(cells.map((c) => c.xVal))];
  const yVals = [...new Set(cells.map((c) => c.yVal))];
  const cellMap = new Map(cells.map((c) => [`${c.xVal}|${c.yVal}`, c]));

  const allSharpes = cells.map((c) => c.sharpe).filter((s) => isFinite(s));
  const maxSharpe = Math.max(...allSharpes);
  const minSharpe = Math.min(...allSharpes);
  const baselineCell = cells.find((c) => c.isBaseline);

  const sharpeTextColor = (s: number) => s >= 3.0 ? 'var(--green)' : s >= 2.0 ? 'var(--amber)' : 'var(--red)';

  // Background heat: smooth interpolation red→amber→green
  const heatBg = (s: number): string => {
    if (!isFinite(s)) return 'transparent';
    const norm = maxSharpe > minSharpe ? (s - minSharpe) / (maxSharpe - minSharpe) : 0.5;
    let r: number, g: number, b: number;
    if (norm < 0.5) {
      const f = norm * 2;
      r = Math.round(160 + (180 - 160) * (1 - f));
      g = Math.round(50 * (1 - f) + 120 * f);
      b = Math.round(40 * (1 - f));
    } else {
      const f = (norm - 0.5) * 2;
      r = Math.round(180 * (1 - f) + 30 * f);
      g = Math.round(120 * (1 - f) + 160 * f);
      b = Math.round(0 + 60 * f);
    }
    const alpha = 0.06 + norm * 0.18;
    return `rgba(${r},${g},${b},${alpha.toFixed(2)})`;
  };

  const thS: React.CSSProperties = {
    padding: '3px 5px', fontSize: 7.5, fontWeight: 700, textTransform: 'uppercase',
    letterSpacing: '0.07em', color: 'var(--t3)', borderBottom: '1px solid var(--line2)',
    whiteSpace: 'nowrap', textAlign: 'center',
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10, fontSize: 9, ...MONO }}>
      {/* Meta bar */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'center', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        {filter && <span style={{ color: 'var(--t2)' }}>{filter}</span>}
        {gridSpec && <span style={{ color: 'var(--t3)' }}>{gridSpec}</span>}
        {baselineCell && (
          <span style={{ color: 'var(--t2)' }}>
            Baseline: <span style={{ color: 'var(--green)' }}>{paramX}={baselineXStr}</span>
            {' '}<span style={{ color: 'var(--green)' }}>{paramY}={baselineYStr}</span>
            <span style={{ color: 'var(--t3)' }}> · SR {baselineCell.sharpe.toFixed(3)}</span>
          </span>
        )}
        <span style={{ marginLeft: 'auto', color: 'var(--t3)' }}>
          peak SR: <span style={{ color: 'var(--green)', fontWeight: 700 }}>{maxSharpe.toFixed(3)}</span>
        </span>
      </div>

      {/* Heat map */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', tableLayout: 'auto', ...MONO }}>
          <thead>
            <tr>
              <th style={{ ...thS, textAlign: 'left', paddingLeft: 6, minWidth: 60, borderRight: '1px solid var(--line2)' }}>
                {paramX} ↓ / {paramY} →
              </th>
              {yVals.map((y) => (
                <th key={y} style={{ ...thS, minWidth: 40 }}>{y}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {xVals.map((x) => (
              <tr key={x}>
                <td style={{ padding: '2px 8px 2px 6px', color: 'var(--t2)', fontWeight: 700, fontSize: 8, whiteSpace: 'nowrap', borderRight: '1px solid var(--line2)' }}>
                  {x}
                </td>
                {yVals.map((y) => {
                  const cell = cellMap.get(`${x}|${y}`);
                  if (!cell) return <td key={y} style={{ padding: '3px 5px', textAlign: 'center', color: 'var(--t4)', fontSize: 8 }}>—</td>;
                  const isBase = cell.isBaseline;
                  const isPeak = cell.sharpe === maxSharpe && !isBase;
                  return (
                    <td
                      key={y}
                      title={`${paramX}=${x}  ${paramY}=${y}\nSharpe ${cell.sharpe.toFixed(3)}  CAGR ${cell.cagr.toFixed(1)}%  MaxDD ${cell.maxdd.toFixed(2)}%  Worst1D ${cell.worst1d.toFixed(2)}%  WF_CV ${cell.wfcv.toFixed(3)}`}
                      style={{
                        padding: '4px 5px',
                        textAlign: 'center',
                        background: isBase ? 'var(--green-dim)' : heatBg(cell.sharpe),
                        outline: isBase ? '1px solid var(--green-mid)' : undefined,
                        color: isBase ? 'var(--green)' : sharpeTextColor(cell.sharpe),
                        fontWeight: isBase || isPeak ? 700 : 400,
                        fontSize: 8.5,
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {cell.sharpe.toFixed(2)}
                      {isBase && <span style={{ marginLeft: 2, fontSize: 6.5, opacity: 0.8 }}>◄</span>}
                      {isPeak && <span style={{ marginLeft: 2, fontSize: 6.5, color: 'var(--green)', opacity: 0.7 }}>★</span>}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Legend */}
      <div style={{ display: 'flex', gap: 14, alignItems: 'center', paddingTop: 2 }}>
        <span style={{ color: 'var(--t3)', fontSize: 7.5, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Sharpe</span>
        <span style={{ color: 'var(--red)', fontSize: 8 }}>▪ &lt;2.0</span>
        <span style={{ color: 'var(--amber)', fontSize: 8 }}>▪ 2.0–3.0</span>
        <span style={{ color: 'var(--green)', fontSize: 8 }}>▪ ≥3.0</span>
        <span style={{ color: 'var(--t4)', fontSize: 7.5, marginLeft: 'auto' }}>hover cells for full metrics</span>
      </div>
    </div>
  );
}

function renderParameterSweep(title: string, body: string): React.ReactNode {
  const tu = title.toUpperCase();
  if (tu.includes('L_HIGH SURFACE')) return renderLHighSweep(body, title);
  return renderTrailSweep(body, title);
}

function renderSectionViz(title: string, body: string): React.ReactNode {
  const t = title.toUpperCase();

  if (t.includes('RUN SUMMARY')) return renderRunSummary(body);
  if (t.includes('ALLOCATOR VIEW SCORECARD') || t.includes('TECHNICAL APPENDIX SCORECARD')) return renderScorecardTable(body);
  if (t.includes('RETURN RATES BY PERIOD')) return renderReturnRatesByPeriod(body);
  if (t.includes('RETURN DISTRIBUTION')) return renderReturnDistribution(body);
  if (t.includes('RETURN + CONDITIONAL ANALYSIS') || t.includes('RETURN+CONDITIONAL')) return renderReturnConditional(body);
  if (t.includes('ROLLING MAX DRAWDOWN')) return renderRollingMaxDrawdown(body);
  if (t.includes('DRAWDOWN EPISODE ANALYSIS')) return renderDrawdownEpisodes(body);
  if (t.includes('RISK-ADJUSTED RETURN QUALITY')) return renderRiskAdjustedQuality(body);
  if (t.includes('DAILY VAR') || t.includes('CVAR')) return renderDailyVarCvar(body);
  if (t.includes('SIGNAL PREDICTIVENESS')) return renderSignalPredictiveness(body);
  if (t.includes('SLIPPAGE IMPACT SWEEP')) return <PreFallback body={body} />;
  if (t.includes('NOISE PERTURBATION STABILITY TEST')) return <PreFallback body={body} />;
  if (t.includes('PARAM JITTER')) return <PreFallback body={body} />;
  if (t.includes('RETURN CONCENTRATION ANALYSIS')) return <PreFallback body={body} />;
  if (t.includes('PERIODIC RETURN BREAKDOWN')) return renderPeriodicBreakdown(body);
  if (t.includes('SHOCK INJECTION TEST')) return <PreFallback body={body} />;
  if (t.includes('WEEKLY MILESTONES') || t.includes('MONTHLY MILESTONES')) return renderMilestones(body);
  if (t.includes('MINIMUM CUMULATIVE RETURN')) return renderMinCumReturn(body);
  if (t.includes('LIQUIDITY CAPACITY CURVE')) return renderLiquidityCapacityCurve(body);
  if (t.includes('MARKET CAP DIAGNOSTIC')) return renderMarketCapDiagnostic(body);
  if (/^RUIN PROBABILITY/i.test(t)) return renderRuinProbability(body);
  if (t.includes('DEFLATED SHARPE RATIO')) return renderDeflatedSharpe(body);

  if (t.includes('TAIL GUARDRAIL GRID SWEEP')) return renderTailGuardrailGridSweep(body);
  if (t.includes('PARAMETER SURFACE MAP')) return renderParamSurfaceMap(title, body);
  if (t.includes('SHARPE RIDGE MAP')) return <PreFallback body={body} />;
  if (t.includes('SHARPE PLATEAU DETECTOR')) return <PreFallback body={body} />;
  if (t.includes('PARAMETRIC STABILITY CUBE')) return renderStabilityCube(body);
  if (t.includes('RISK THROTTLE STABILITY CUBE')) return renderStabilityCube(body);
  if (t.includes('EXIT ARCHITECTURE STABILITY CUBE')) return renderStabilityCube(body);

  if (t.includes('PARAMETER SWEEP')) return renderParameterSweep(title, body);
  if (t.includes('L_HIGH SURFACE') && t.includes('RANKED BY SHARPE')) return renderLHighRanked(body);
  if (t.includes('L_HIGH SURFACE')) return renderLHighSweep(body, title);

  return <PreFallback body={body} />;
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

  const [equityLogScale, setEquityLogScale] = useState(false);
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
  const [levHistHover, setLevHistHover] = useState<{
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
  const feesTablesByFilter = useMemo(
    () => ((m.fees_tables_by_filter ?? results?.fees_tables_by_filter ?? {}) as Record<string, FeesRow[]>),
    [m, results],
  );
  const feesTablesFromOutput = useMemo(
    () => parseFeesTablesFromAuditOutput(auditOutput),
    [auditOutput],
  );
  const selectedFeesTableRows = useMemo(() => {
    const primary = Object.keys(feesTablesByFilter).length > 0 ? feesTablesByFilter : feesTablesFromOutput;
    const entries = Object.entries(primary);
    if (selectedFilter && entries.length > 0) {
      const norm = normalizeFilterLabel(selectedFilter);
      const normCore = normalizeFilterLabelCore(selectedFilter);
      const matched = entries.find(([k]) => {
        const nk = normalizeFilterLabel(k);
        const nkCore = normalizeFilterLabelCore(k);
        return nk === norm || nkCore === normCore || nkCore === norm || nk === normCore;
      });
      if (matched) return matched[1];
    }
    const topLevel = (results?.fees_table ?? m.fees_table) as FeesRow[] | null | undefined;
    if (Array.isArray(topLevel) && topLevel.length > 0) return topLevel;
    if (entries.length > 0) return entries[0][1];
    return [] as FeesRow[];
  }, [feesTablesByFilter, feesTablesFromOutput, selectedFilter, results, m]);
  const feesBreakdownKpis = useMemo(() => {
    const rows = selectedFeesTableRows;
    const activeRows = rows.filter((r) => !r.no_entry && typeof r.ret_net === 'number' && Number.isFinite(r.ret_net));
    const totalCalendarDays = rows.length;
    const totalActiveDays = rows.filter((r) => !r.no_entry).length;
    const totalNoEntries = rows.filter((r) => r.no_entry).length;
    const totalNoEntriesFilter = rows.filter((r) => r.no_entry && r.no_entry_reason === 'filter').length;
    const totalNoEntriesConviction = rows.filter((r) => r.no_entry && r.no_entry_reason !== 'filter').length;
    const dailyWinratePct = activeRows.length > 0
      ? (activeRows.filter((r) => (r.ret_net as number) > 0).length / activeRows.length) * 100
      : null;
    const aggregatePeriodWinratePct = (period: 'week' | 'month'): number | null => {
      const buckets = new Map<string, number>();
      for (const r of activeRows) {
        const d = new Date(`${r.date}T00:00:00Z`);
        if (Number.isNaN(d.getTime())) continue;
        let key: string;
        if (period === 'month') {
          key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
        } else {
          // Week bucket by UTC Sunday start.
          const start = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
          start.setUTCDate(start.getUTCDate() - start.getUTCDay());
          key = start.toISOString().slice(0, 10);
        }
        const prevFactor = buckets.get(key) ?? 1;
        const dayRet = (r.ret_net as number) / 100;
        buckets.set(key, prevFactor * (1 + dayRet));
      }
      const vals = [...buckets.values()].map((factor) => (factor - 1) * 100);
      if (vals.length === 0) return null;
      return (vals.filter((v) => v > 0).length / vals.length) * 100;
    };
    const weeklyWinratePct = aggregatePeriodWinratePct('week');
    const monthlyWinratePct = aggregatePeriodWinratePct('month');
    const endingCapital = [...rows]
      .reverse()
      .find((r) => typeof r.end === 'number' && Number.isFinite(r.end))?.end ?? null;
    const totalFees = rows.reduce((acc, r) => {
      const taker = typeof r.taker_fee === 'number' && Number.isFinite(r.taker_fee) ? r.taker_fee : 0;
      const funding = typeof r.funding === 'number' && Number.isFinite(r.funding) ? r.funding : 0;
      return acc + taker + funding;
    }, 0);
    const netFeeDragPct = (endingCapital && endingCapital > 0) ? (totalFees / endingCapital) * 100 : null;
    const netFeeDragPerActiveDayPct = (netFeeDragPct !== null && totalActiveDays > 0)
      ? (netFeeDragPct / totalActiveDays)
      : null;
    const netFeeDragPerCalendarDayPct = (netFeeDragPct !== null && totalCalendarDays > 0)
      ? (netFeeDragPct / totalCalendarDays)
      : null;
    return {
      totalCalendarDays,
      totalActiveDays,
      totalNoEntries,
      totalNoEntriesFilter,
      totalNoEntriesConviction,
      dailyWinratePct,
      weeklyWinratePct,
      monthlyWinratePct,
      netFeeDragPct,
      netFeeDragPerActiveDayPct,
      netFeeDragPerCalendarDayPct,
    };
  }, [selectedFeesTableRows]);
  const selectedFeesTableRowsWithCumulative = useMemo<FeesRowWithCumulative[]>(() => {
    let cumTradeVol = 0;
    let cumFees = 0;
    let cumPnl = 0;
    return selectedFeesTableRows.map((row) => {
      const tradeVol = Number.isFinite(row.trade_vol ?? NaN) ? (row.trade_vol as number) : 0;
      const taker = Number.isFinite(row.taker_fee ?? NaN) ? (row.taker_fee as number) : 0;
      const funding = Number.isFinite(row.funding ?? NaN) ? (row.funding as number) : 0;
      const netPnl = Number.isFinite(row.net_pnl ?? NaN) ? (row.net_pnl as number) : 0;
      cumTradeVol += tradeVol;
      cumFees += (taker + funding);
      cumPnl += netPnl;
      return {
        ...row,
        cum_trade_vol: cumTradeVol,
        cum_fees: cumFees,
        cum_pnl: cumPnl,
      };
    });
  }, [selectedFeesTableRows]);
  const feesKeyValues = useMemo(() => {
    const rows = selectedFeesTableRowsWithCumulative;
    const last = rows[rows.length - 1] ?? null;
    const entryDays = rows.filter((r) => !r.no_entry).length;
    const noEntryDays = rows.filter((r) => r.no_entry).length;
    const activeLeverages = rows
      .filter((r) => !r.no_entry && typeof r.lev === 'number' && Number.isFinite(r.lev))
      .map((r) => r.lev as number);
    const avgLeverageActive = activeLeverages.length > 0
      ? (activeLeverages.reduce((acc, v) => acc + v, 0) / activeLeverages.length)
      : null;
    const start = rows[0]?.start ?? null;
    const end = last?.end ?? null;
    const netReturnPct = (start && end && start !== 0) ? ((end / start) - 1) * 100 : null;
    return {
      entryDays,
      noEntryDays,
      avgLeverageActive,
      cumulativeVolume: last?.cum_trade_vol ?? 0,
      cumulativeFees: last?.cum_fees ?? 0,
      cumulativePnl: last?.cum_pnl ?? 0,
      netReturnPct,
    };
  }, [selectedFeesTableRowsWithCumulative]);
  const feesLeverageDiagnostics = useMemo(() => {
    const activeLevs = selectedFeesTableRowsWithCumulative
      .filter((r) => !r.no_entry && typeof r.lev === 'number' && Number.isFinite(r.lev))
      .map((r) => r.lev as number);
    if (activeLevs.length === 0) {
      return {
        activeDays: 0,
        min: null as number | null,
        avg: null as number | null,
        max: null as number | null,
        cap: null as number | null,
        capHitDays: 0,
        capHitPct: null as number | null,
        histogram: [] as Array<{ x0: number; x1: number; count: number }>,
      };
    }
    const min = Math.min(...activeLevs);
    const max = Math.max(...activeLevs);
    const avg = activeLevs.reduce((acc, v) => acc + v, 0) / activeLevs.length;
    const volLevEnabled = !!params?.enable_vol_lev_scaling;
    const lHigh = asNum(params?.l_high);
    const volLevMaxBoost = asNum(params?.vol_lev_max_boost);
    const cap = (volLevEnabled && lHigh !== null && volLevMaxBoost !== null)
      ? (lHigh * volLevMaxBoost)
      : null;
    const capTol = 1e-6;
    const capHitDays = cap !== null
      ? activeLevs.filter((v) => v >= (cap - capTol)).length
      : 0;
    const capHitPct = cap !== null && activeLevs.length > 0
      ? (capHitDays / activeLevs.length) * 100
      : null;

    const bins = 10;
    const histogram: Array<{ x0: number; x1: number; count: number }> = [];
    if (max - min < 1e-9) {
      histogram.push({ x0: min, x1: max, count: activeLevs.length });
    } else {
      const bw = (max - min) / bins;
      const counts = new Array(bins).fill(0);
      for (const v of activeLevs) {
        const idx = Math.min(bins - 1, Math.max(0, Math.floor((v - min) / bw)));
        counts[idx] += 1;
      }
      for (let i = 0; i < bins; i += 1) {
        const x0 = min + i * bw;
        const x1 = i === bins - 1 ? max : (x0 + bw);
        histogram.push({ x0, x1, count: counts[i] });
      }
    }
    return {
      activeDays: activeLevs.length,
      min,
      avg,
      max,
      cap,
      capHitDays,
      capHitPct,
      histogram,
    };
  }, [selectedFeesTableRowsWithCumulative, params]);
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
  ) ? `/${selectedTotalDays}` : undefined;
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
    const feeDates = selectedFeesTableRows
      .map((r) => parseDateLike(r.date))
      .filter((d): d is Date => d instanceof Date && !Number.isNaN(d.getTime()));
    const canMapByFees = feeDates.length >= src.length && src.length > 0;
    return src.map((p, idx) => {
      const parsedPointDate = typeof p === 'number' ? null : parseDateLike(p.x);
      const mappedFeeDate = canMapByFees ? feeDates[feeDates.length - src.length + idx] : null;
      if (typeof p === 'number') return { d: mappedFeeDate ?? syntheticDateAt(idx, src.length), y: p };
      return { d: parsedPointDate ?? mappedFeeDate ?? syntheticDateAt(idx, src.length), y: p.y };
    }).filter((p) => Number.isFinite(p.y));
  }, [equityCurveDollars, selectedFeesTableRows]);
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
    const eqSrc = equityCurveDollars ?? [];
    const feeDates = selectedFeesTableRows
      .map((r) => parseDateLike(r.date))
      .filter((d): d is Date => d instanceof Date && !Number.isNaN(d.getTime()));
    const canMapByFees = feeDates.length >= eqSrc.length && eqSrc.length > 0;
    const eq = eqSrc.map((p, idx) => {
      const mappedFeeDate = canMapByFees ? feeDates[feeDates.length - eqSrc.length + idx] : null;
      if (typeof p === 'number') return { d: mappedFeeDate ?? syntheticDateAt(idx, eqSrc.length), y: p };
      return { d: parseDateLike(p.x) ?? mappedFeeDate ?? syntheticDateAt(idx, eqSrc.length), y: p.y };
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
  }, [tearTemplate, equityCurveDollars, selectedFeesTableRows, selectedRow, selectedDrawdownCurve, m, scorecard, selectedFilter, runStartingCapital, jobId, auditOutput, params]);

  const metricCards: Array<{ label: string; key: string; value: string; colorValue: unknown; secondary?: string; unit?: string; unitColor?: string }> = selectedRow
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
            ['full_report', 'Full Report'],
            ['tear_sheet', 'Tear Sheet'],
            ['raw_output', 'Raw Output'],
          ] as Array<[ReportTab, string]>).map(([tab, label]) => (
            <button
              key={tab}
              onClick={async () => {
                setActiveTab(tab);
                if (tab !== 'summary') await ensureAuditOutputLoaded();
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
              <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
                <span>Equity Curve ($)</span>
                {equityStatsBar && (
                  <span style={{ display: 'flex', alignItems: 'center', gap: 12, fontSize: 9, color: 'var(--t2)', fontFamily: 'var(--font-space-mono), Space Mono, monospace', textTransform: 'none', letterSpacing: '0.06em', fontWeight: 400 }}>
                    {equityStatsBar.map((s, idx) => (
                      <span key={`${s.label}-${idx}`}>
                        {s.label}: <span style={{ color: s.color, fontWeight: 700 }}>{s.value}</span>
                      </span>
                    ))}
                  </span>
                )}
                <button
                  onClick={(e) => { e.preventDefault(); setEquityLogScale((v) => !v); }}
                  style={{
                    background: 'none', border: '1px solid var(--line2)', borderRadius: 2,
                    padding: '1px 5px', cursor: 'pointer', fontSize: 8,
                    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                    letterSpacing: '0.08em', textTransform: 'uppercase',
                    color: equityLogScale ? 'var(--green)' : 'var(--t3)',
                    borderColor: equityLogScale ? 'var(--green)' : 'var(--line2)',
                  }}
                >
                  Log
                </button>
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
                  showTitle={false}
                  logScale={equityLogScale}
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
              <details style={{ marginTop: 10 }}>
                <summary style={{ cursor: 'pointer', fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, userSelect: 'none' }}>
                  Equity Curve Overlay
                </summary>
                <FilterEquityCurveOverlay
                  filters={mergedFilters}
                  selectedFilter={selectedFilter}
                  onSelectFilter={(filter) => setManualSelectedFilter(filter)}
                />
              </details>
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
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: 10,
          }}
        >
          {outputLoading && <div style={{ fontSize: 10, color: 'var(--t3)' }}>Loading fees panel...</div>}
          {outputError && <div style={{ fontSize: 10, color: 'var(--red)' }}>{outputError}</div>}
          {!outputLoading && !outputError && (
            <>
            {selectedFeesTableRows.length > 0 && (
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(7, minmax(0, 1fr))',
                  gap: 8,
                  padding: '0 0 8px 0',
                }}
              >
                {[
                  { label: 'Calendar Days', value: feesBreakdownKpis.totalCalendarDays, color: 'var(--t1)' },
                  { label: 'Active Days', value: feesBreakdownKpis.totalActiveDays, color: 'var(--green)' },
                  { label: 'No Entries', value: feesBreakdownKpis.totalNoEntries, color: 'var(--amber)' },
                  { label: 'No Entry (Filter)', value: feesBreakdownKpis.totalNoEntriesFilter, color: 'var(--red)' },
                  { label: 'No Entry (Conviction)', value: feesBreakdownKpis.totalNoEntriesConviction, color: 'var(--green)' },
                  {
                    label: 'Daily Winrate',
                    value: feesBreakdownKpis.dailyWinratePct === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.dailyWinratePct.toFixed(1)}%`,
                    color: 'var(--green)',
                  },
                  {
                    label: 'Weekly Winrate',
                    value: feesBreakdownKpis.weeklyWinratePct === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.weeklyWinratePct.toFixed(1)}%`,
                    color: 'var(--green)',
                  },
                  {
                    label: 'Monthly Winrate',
                    value: feesBreakdownKpis.monthlyWinratePct === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.monthlyWinratePct.toFixed(1)}%`,
                    color: 'var(--green)',
                  },
                  {
                    label: 'Net Fee Drag',
                    value: feesBreakdownKpis.netFeeDragPct === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.netFeeDragPct.toFixed(2)}%`,
                    color: 'var(--amber)',
                  },
                  {
                    label: 'Net Fee Drag / Active Day',
                    value: feesBreakdownKpis.netFeeDragPerActiveDayPct === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.netFeeDragPerActiveDayPct.toFixed(4)}%`,
                    color: 'var(--amber)',
                  },
                  {
                    label: 'Net Fee Drag / Calendar Day',
                    value: feesBreakdownKpis.netFeeDragPerCalendarDayPct === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.netFeeDragPerCalendarDayPct.toFixed(4)}%`,
                    color: 'var(--amber)',
                  },
                ].map((kpi) => (
                  <div
                    key={`fees-kpi-${kpi.label}`}
                    style={{
                      border: '1px solid var(--line)',
                      background: 'var(--bg1)',
                      borderRadius: 3,
                      padding: '8px 10px',
                      display: 'flex',
                      flexDirection: 'column',
                      gap: 4,
                    }}
                  >
                    <div
                      style={{
                        fontSize: 8,
                        color: 'var(--t3)',
                        textTransform: 'uppercase',
                        letterSpacing: '0.08em',
                        fontWeight: 700,
                      }}
                    >
                      {kpi.label}
                    </div>
                    <div
                      style={{
                        fontSize: 13,
                        color: kpi.color,
                        fontWeight: 700,
                        fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                      }}
                    >
                      {kpi.value}
                    </div>
                  </div>
                ))}
              </div>
            )}
            <DailyReturnOverlapChart
              rows={selectedFeesTableRows}
              intradayBars={(m.intraday_bars ?? (results as Record<string, unknown>)?.intraday_bars) as Record<string, (number | null)[]> | null | undefined}
              intradayExitBars={selectedFilter
                ? ((m.intraday_exit_bars ?? (results as Record<string, unknown>)?.intraday_exit_bars) as Record<string, Record<string, number>> | null | undefined)?.[selectedFilter] ?? null
                : null}
            />
            <div
              style={{
                display: 'flex',
                flexDirection: 'column',
                background: 'var(--bg0)',
              }}
            >
              <div
                style={{
                  height: 24,
                  display: 'flex',
                  alignItems: 'center',
                  borderBottom: '1px solid var(--line)',
                  fontSize: 9,
                  color: 'var(--t3)',
                  textTransform: 'uppercase',
                  letterSpacing: '0.1em',
                  fontWeight: 700,
                  lineHeight: 1,
                  position: 'sticky',
                  top: 40,
                  zIndex: 8,
                  background: 'var(--bg0)',
                  boxShadow: '0 -1px 0 var(--bg0)',
                }}
              >
                {`Fees Panel${selectedFilter ? ` • ${selectedFilter}` : ''}`}
              </div>
              {selectedFeesTableRowsWithCumulative.length > 0 ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: 0, tableLayout: 'fixed' }}>
                    <colgroup>
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '4%' }} />
                      <col style={{ width: '8%' }} />
                      <col style={{ width: '8%' }} />
                      <col style={{ width: '8%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '6%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '6%' }} />
                      <col style={{ width: '6%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '8%' }} />
                    </colgroup>
                    <thead
                      style={{
                        position: 'sticky',
                        top: 63,
                        zIndex: 7,
                        background: 'var(--bg0)',
                      }}
                    >
                      <tr style={{ borderBottom: '1px solid var(--line)' }}>
                        {[
                          'Date', 'Start ($)', 'Margin ($)', 'Lev', 'Invested ($)', 'Trade Vol ($)',
                          'Cum Vol ($)', 'Taker Fee ($)', 'Funding ($)', 'Cum Fees ($)', 'End ($)', 'Ret Gross%', 'Ret Net%', 'Net P&L ($)', 'Cum P&L ($)',
                        ].map((h) => (
                          <th
                            key={`fees-head-${h}`}
                            style={{
                              textAlign: h === 'Date' ? 'left' : 'right',
                              padding: '6px 8px',
                              fontSize: 8,
                              color: 'var(--t3)',
                              textTransform: 'uppercase',
                              letterSpacing: '0.08em',
                              fontWeight: 700,
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                              whiteSpace: 'nowrap',
                              background: 'var(--bg0)',
                              borderBottom: '1px solid var(--line)',
                            }}
                          >
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {selectedFeesTableRowsWithCumulative.map((row, i) => {
                        const bg = i % 2 === 0 ? 'var(--bg1)' : 'var(--bg2)';
                        const fmtN = (v: number | null | undefined) => (v === null || v === undefined || !Number.isFinite(v) ? '—' : new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(v));
                        const fmtLev = (v: number | null | undefined) => (v === null || v === undefined || !Number.isFinite(v) ? '—' : v.toFixed(3));
                        const fmtPct = (v: number | null | undefined) => (v === null || v === undefined || !Number.isFinite(v) ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(3)}%`);
                        const retNetColor = row.no_entry
                          ? 'var(--t1)'
                          : (row.ret_net ?? 0) >= 0 ? 'var(--green)' : 'var(--red)';
                        return (
                          <tr
                            key={`fees-row-${row.date}-${i}`}
                            style={{
                              background: bg,
                              borderBottom: '1px solid var(--line)',
                              opacity: row.no_entry ? 0.5 : 1,
                            }}
                          >
                            <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t2)', fontFamily: 'var(--font-space-mono), Space Mono, monospace', whiteSpace: 'nowrap' }}>{row.date}</td>
                            <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.start)}</td>
                            {row.no_entry ? (
                              <>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t3)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>—</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t3)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>0.000</td>
                                <td
                                  colSpan={6}
                                  style={{
                                    padding: '6px 8px',
                                    fontSize: 10,
                                    color: 'var(--t3)',
                                    textAlign: 'center',
                                    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                                    textTransform: 'lowercase',
                                  }}
                                >
                                  {`— ${row.no_entry_reason === 'filter' ? 'filtered' : 'conviction gate'} —`}
                                </td>
                              </>
                            ) : (
                              <>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.margin)}</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtLev(row.lev)}</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.invested)}</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.trade_vol)}</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.cum_trade_vol)}</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.taker_fee)}</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.funding)}</td>
                                <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.cum_fees)}</td>
                              </>
                            )}
                            <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.end)}</td>
                            <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtPct(row.ret_gross)}</td>
                            <td style={{ padding: '6px 8px', fontSize: 10, color: retNetColor, textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtPct(row.ret_net)}</td>
                            <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.net_pnl)}</td>
                            <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>{fmtN(row.cum_pnl)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                  <div
                    style={{
                      border: '1px solid var(--line)',
                      borderRadius: 3,
                      background: 'var(--bg1)',
                      overflow: 'hidden',
                    }}
                  >
                    <div
                      style={{
                        padding: '6px 8px',
                        fontSize: 8,
                        color: 'var(--t3)',
                        textTransform: 'uppercase',
                        letterSpacing: '0.08em',
                        fontWeight: 700,
                        borderBottom: '1px solid var(--line)',
                        background: 'var(--bg0)',
                      }}
                    >
                      Fees Panel Highlights
                    </div>
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(7, minmax(0, 1fr))',
                        gap: 0,
                        background: 'rgba(0,200,150,0.08)',
                        borderTop: '1px solid rgba(0,200,150,0.35)',
                      }}
                    >
                        {[
                          { label: 'Entry Days', value: String(feesKeyValues.entryDays), color: 'var(--t1)' },
                          { label: 'No Entry Days', value: String(feesKeyValues.noEntryDays), color: 'var(--t1)' },
                        {
                          label: 'Avg Lev (Active)',
                          value: feesKeyValues.avgLeverageActive === null ? 'N/A' : `${feesKeyValues.avgLeverageActive.toFixed(3)}x`,
                          color: 'var(--t1)',
                        },
                        { label: 'Cum Vol ($)', value: new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(feesKeyValues.cumulativeVolume), color: 'var(--t1)' },
                        { label: 'Cum Fees ($)', value: new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(feesKeyValues.cumulativeFees), color: 'var(--amber)' },
                        { label: 'Cum P&L ($)', value: new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(feesKeyValues.cumulativePnl), color: feesKeyValues.cumulativePnl >= 0 ? 'var(--green)' : 'var(--red)' },
                        {
                          label: 'Net Return %',
                          value: feesKeyValues.netReturnPct === null
                            ? 'N/A'
                            : `${feesKeyValues.netReturnPct >= 0 ? '+' : ''}${feesKeyValues.netReturnPct.toFixed(2)}%`,
                          color: feesKeyValues.netReturnPct === null
                            ? 'var(--t3)'
                            : feesKeyValues.netReturnPct >= 0 ? 'var(--green)' : 'var(--red)',
                          },
                        ].map((item, idx) => (
                        <div
                          key={`fees-highlight-${item.label}`}
                          style={{
                            padding: '8px 10px',
                            borderRight: idx === 6 ? 'none' : '1px solid var(--line)',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 4,
                          }}
                        >
                          <div
                            style={{
                              fontSize: 8,
                              color: 'var(--t3)',
                              textTransform: 'uppercase',
                              letterSpacing: '0.08em',
                              fontWeight: 700,
                            }}
                          >
                            {item.label}
                          </div>
                          <div
                            style={{
                              fontSize: 11,
                              color: item.color,
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                              fontWeight: 700,
                            }}
                          >
                            {item.value}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div
                    style={{
                      border: '1px solid var(--line)',
                      borderRadius: 3,
                      background: 'var(--bg1)',
                      overflow: 'hidden',
                    }}
                  >
                    <div
                      style={{
                        padding: '6px 8px',
                        fontSize: 8,
                        color: 'var(--t3)',
                        textTransform: 'uppercase',
                        letterSpacing: '0.08em',
                        fontWeight: 700,
                        borderBottom: '1px solid var(--line)',
                        background: 'var(--bg0)',
                      }}
                    >
                      Leverage Diagnostics
                    </div>
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(5, minmax(0, 1fr))',
                        borderBottom: '1px solid var(--line)',
                      }}
                    >
                      {[
                        { label: 'Active Days', value: `${feesLeverageDiagnostics.activeDays}`, color: 'var(--t1)' },
                        { label: 'Min Lev', value: feesLeverageDiagnostics.min === null ? 'N/A' : `${feesLeverageDiagnostics.min.toFixed(3)}x`, color: 'var(--t1)' },
                        { label: 'Avg Lev', value: feesLeverageDiagnostics.avg === null ? 'N/A' : `${feesLeverageDiagnostics.avg.toFixed(3)}x`, color: 'var(--green)' },
                        { label: 'Max Lev', value: feesLeverageDiagnostics.max === null ? 'N/A' : `${feesLeverageDiagnostics.max.toFixed(3)}x`, color: 'var(--t1)' },
                        {
                          label: 'Hit Boost Cap',
                          value: feesLeverageDiagnostics.capHitPct === null
                            ? 'N/A'
                            : `${feesLeverageDiagnostics.capHitDays}/${feesLeverageDiagnostics.activeDays} (${feesLeverageDiagnostics.capHitPct.toFixed(1)}%)`,
                          color: feesLeverageDiagnostics.capHitPct === null ? 'var(--t3)' : 'var(--amber)',
                        },
                      ].map((item, idx) => (
                        <div
                          key={`lev-diag-${item.label}`}
                          style={{
                            padding: '8px 10px',
                            borderRight: idx === 4 ? 'none' : '1px solid var(--line)',
                            display: 'flex',
                            flexDirection: 'column',
                            gap: 4,
                          }}
                        >
                          <div
                            style={{
                              fontSize: 8,
                              color: 'var(--t3)',
                              textTransform: 'uppercase',
                              letterSpacing: '0.08em',
                              fontWeight: 700,
                            }}
                          >
                            {item.label}
                          </div>
                          <div
                            style={{
                              fontSize: 11,
                              color: item.color,
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                              fontWeight: 700,
                            }}
                          >
                            {item.value}
                          </div>
                        </div>
                      ))}
                    </div>
                    <div style={{ padding: '10px' }}>
                      <div
                        style={{
                          fontSize: 8,
                          color: 'var(--t3)',
                          textTransform: 'uppercase',
                          letterSpacing: '0.08em',
                          marginBottom: 6,
                        }}
                      >
                        Active Leverage Distribution
                        {feesLeverageDiagnostics.cap !== null ? ` • Cap ${feesLeverageDiagnostics.cap.toFixed(3)}x` : ''}
                      </div>
                      {feesLeverageDiagnostics.histogram.length > 0 ? (
                        <div
                          style={{ display: 'flex', alignItems: 'end', gap: 2, height: 72, position: 'relative' }}
                          onMouseLeave={() => setLevHistHover(null)}
                        >
                          {(() => {
                            const maxCount = Math.max(...feesLeverageDiagnostics.histogram.map((b) => b.count), 1);
                            return feesLeverageDiagnostics.histogram.map((bin, i) => (
                              <div
                                key={`lev-hist-${i}`}
                                style={{
                                  flex: 1,
                                  height: `${Math.max(4, (bin.count / maxCount) * 100)}%`,
                                  background: 'rgba(0, 200, 150, 0.35)',
                                  border: '1px solid rgba(0, 200, 150, 0.55)',
                                  borderRadius: 2,
                                }}
                                onMouseEnter={(e) => {
                                  const pct = feesLeverageDiagnostics.activeDays > 0
                                    ? (bin.count / feesLeverageDiagnostics.activeDays) * 100
                                    : 0;
                                  setLevHistHover({
                                    x: e.clientX,
                                    y: e.clientY,
                                    text: `${bin.x0.toFixed(3)}x - ${bin.x1.toFixed(3)}x • ${bin.count} day${bin.count === 1 ? '' : 's'} (${pct.toFixed(1)}%)`,
                                  });
                                }}
                                onMouseMove={(e) => {
                                  setLevHistHover((prev) => (prev ? { ...prev, x: e.clientX, y: e.clientY } : prev));
                                }}
                              />
                            ));
                          })()}
                        </div>
                      ) : (
                        <div style={{ fontSize: 10, color: 'var(--t3)' }}>No active leverage samples</div>
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div style={{ padding: '10px', fontSize: 10, color: 'var(--t3)' }}>
                  No Fees Panel rows found for the selected filter.
                </div>
              )}
            </div>
            {levHistHover && (
              <div
                style={{
                  position: 'fixed',
                  left: levHistHover.x + 10,
                  top: levHistHover.y + 10,
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
                {levHistHover.text}
              </div>
            )}
            </>
          )}
        </div>
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
            background: 'transparent',
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
                  background: 'transparent',
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
                  color: 'var(--t1)',
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
            display: 'flex',
            flexDirection: 'column',
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
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: '0 12px 12px 12px' }}>
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
                          <div style={{ marginTop: 8 }}>
                            {renderSectionViz(section.title, section.body)}
                          </div>
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

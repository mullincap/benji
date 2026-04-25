'use client';

import { Fragment, useEffect, useMemo, useRef, useState } from 'react';
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
import { asNum, fmtPercent2, metricColor, normalizeFilterLabel, normalizeFilterLabelCore } from '@/app/lib/format';

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


function fmtCagr(v: unknown): string {
  if (v === null || v === undefined) return 'N/A';
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) return String(v);
  const digits = Math.abs(n) >= 100 ? 0 : 2;
  return `${new Intl.NumberFormat(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(n)}%`;
}

function fmtSummaryReturn(v: unknown): string {
  if (v === null || v === undefined) return 'N/A';
  const n = typeof v === 'number' ? v : Number(v);
  if (!Number.isFinite(n)) return String(v);
  const digits = Math.abs(n) >= 100 ? 0 : 2;
  return `${new Intl.NumberFormat(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(n)}%`;
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
          Daily Returns
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
        Daily Returns
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
        {/* Area fill — positive region */}
        <clipPath id="clip-pos">
          <rect x={padL} y={0} width={plotW} height={yZero} />
        </clipPath>
        <clipPath id="clip-neg">
          <rect x={padL} y={yZero} width={plotW} height={H - yZero} />
        </clipPath>
        {(() => {
          const points = sampled.map((v, i) => {
            const x = padL + ((i + 0.5) / sampled.length) * plotW;
            const y = toPy(v);
            return { x, y };
          });
          const areaPath = `M${points[0].x},${yZero} ${points.map(p => `L${p.x},${p.y}`).join(' ')} L${points[points.length - 1].x},${yZero} Z`;
          const linePath = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ');
          return (
            <>
              <path d={areaPath} fill="rgba(0, 200, 150, 0.15)" clipPath="url(#clip-pos)" />
              <path d={areaPath} fill="rgba(255, 77, 77, 0.15)" clipPath="url(#clip-neg)" />
              <path d={linePath} fill="none" stroke="rgba(0, 200, 150, 0.6)" strokeWidth="1" clipPath="url(#clip-pos)" />
              <path d={linePath} fill="none" stroke="rgba(255, 77, 77, 0.6)" strokeWidth="1" clipPath="url(#clip-neg)" />
            </>
          );
        })()}
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
  | 'verdict_grades'
  | 'scorecards_summary'
  | 'return_profile'
  | 'risk_drawdown'
  | 'statistical_validity'
  | 'walkforward_regime'
  | 'sensitivity_stress'
  | 'cost_capacity'
  | 'parameter_optimization';

type FullReportCategoryDef = {
  key: FullReportCategoryKey;
  title: string;
  defaultOpen: boolean;
};

const FULL_REPORT_CATEGORIES: FullReportCategoryDef[] = [
  { key: 'verdict_grades', title: '1. Verdict & Grades', defaultOpen: true },
  { key: 'scorecards_summary', title: '2. Scorecards & Summary', defaultOpen: true },
  { key: 'return_profile', title: '3. Return Profile', defaultOpen: false },
  { key: 'risk_drawdown', title: '4. Risk & Drawdown', defaultOpen: false },
  { key: 'statistical_validity', title: '5. Statistical Validity', defaultOpen: false },
  { key: 'walkforward_regime', title: '6. Walk-Forward & Regime', defaultOpen: false },
  { key: 'sensitivity_stress', title: '7. Sensitivity & Stress', defaultOpen: false },
  { key: 'cost_capacity', title: '8. Cost & Capacity', defaultOpen: false },
  { key: 'parameter_optimization', title: '9. Parameter Optimization', defaultOpen: false },
];

function fullReportCategoryForTitle(title: string): FullReportCategoryKey {
  const t = title.toUpperCase();
  // 1. Verdict & Grades
  if (
    /^BOTTOM LINE\b/.test(t)
    || /^VERDICT\b/.test(t)
    || /^GATING METRICS\b/.test(t)
    || /^CORE METRICS\b/.test(t)
    || /^SUPPORTING METRICS\b/.test(t)
    || /^WHAT YOU HAVE\b/.test(t)
    || /^WHAT YOU STILL NEED\b/.test(t)
  ) {
    return 'verdict_grades';
  }

  // 2. Scorecards & Summary
  if (
    /^ALLOCATOR VIEW SCORECARD\b/.test(t)
    || /^TECHNICAL APPENDIX SCORECARD\b/.test(t)
    || /^INSTITUTIONAL SCORECARD\b/.test(t)
    || /^BEST FILTER HEADLINE STATS\b/.test(t)
    || /^RUN SUMMARY\b/.test(t)
    || /^WEEKLY MILESTONES\b/.test(t)
    || /^MONTHLY MILESTONES\b/.test(t)
    || /^EQUITY ENSEMBLE\b/.test(t)
    || /OUTPUT FILES/i.test(t)
  ) {
    return 'scorecards_summary';
  }

  // 3. Return Profile
  if (
    /^RETURN RATES BY PERIOD\b/.test(t)
    || /^PERIODIC RETURN BREAKDOWN\b/.test(t)
    || /^RETURN DISTRIBUTION\b/.test(t)
    || /^RETURN CONCENTRATION ANALYSIS\b/.test(t)
    || /^RETURN \+ CONDITIONAL ANALYSIS\b/.test(t)
    || /^REGIME & CONDITIONAL ANALYSIS\b/.test(t)
    || /^ROLLING MAX DRAWDOWN\b/.test(t)
    || /^ALPHA VS BETA DECOMPOSITION\b/.test(t)
  ) {
    return 'return_profile';
  }

  // 4. Risk & Drawdown
  if (
    /^RISK-ADJUSTED RETURN QUALITY\b/.test(t)
    || /^DAILY VAR \/ CVAR\b/.test(t)
    || /^TAIL RISK\b/.test(t)
    || /^DRAWDOWN EPISODE ANALYSIS\b/.test(t)
    || /^RUIN PROBABILITY\b/.test(t)
    || /^SHOCK INJECTION TEST\b/.test(t)
  ) {
    return 'risk_drawdown';
  }

  // 5. Statistical Validity
  if (
    /^DEFLATED SHARPE RATIO\b/.test(t)
    || /^STATISTICAL VALIDITY\b/.test(t)
    || /^PBO\b/.test(t)
    || /^PROBABILITY OF BACKTEST OVERFITTING\b/.test(t)
    || /^SIGNAL PREDICTIVENESS\b/.test(t)
    || /^SIMULATION BIAS AUDIT\b/.test(t)
    || /^DAILY SERIES AUDIT\b/.test(t)
    || /^STRESS TEST SUMMARY\b/.test(t)
  ) {
    return 'statistical_validity';
  }

  // 6. Walk-Forward & Regime
  if (
    /^WALK-FORWARD VALIDATION\b/.test(t)
    || /^WALK-FORWARD ROLLING\b/.test(t)
    || /^FILTER-AWARE WALK-FORWARD\b/.test(t)
    || /^SHARPE STABILITY ANALYSIS\b/.test(t)
    || /^REGIME ROBUSTNESS TEST\b/.test(t)
    || /^REGIME CONSISTENCY SUMMARY\b/.test(t)
  ) {
    return 'walkforward_regime';
  }

  // 7. Sensitivity & Stress
  if (
    /^NOISE PERTURBATION STABILITY TEST\b/.test(t)
    || /^PARAM JITTER \/ SHARPE STABILITY TEST\b/.test(t)
    || /^NEIGHBOR PLATEAU TEST\b/.test(t)
    || /^PARAMETER SENSITIVITY MAP\b/.test(t)
    || /^PARAMETER SENSITIVITY SUMMARY\b/.test(t)
    || /^LUCKY STREAK TEST\b/.test(t)
    || /^TOP-N DAY REMOVAL TEST\b/.test(t)
    || /^CAPPED RETURN SENSITIVITY TABLE\b/.test(t)
  ) {
    return 'sensitivity_stress';
  }

  // 8. Cost & Capacity
  if (
    /^SLIPPAGE SENSITIVITY TABLE\b/.test(t)
    || /^SLIPPAGE IMPACT SWEEP\b/.test(t)
    || /^COST CURVE TEST\b/.test(t)
    || /^CAPACITY CURVE TEST\b/.test(t)
    || /^LIQUIDITY CAPACITY CURVE\b/.test(t)
    || /^CAPITAL & OPERATIONAL\b/.test(t)
    || /^MARKET CAP DIAGNOSTIC\b/.test(t)
    || /^MARKET CAP UNIVERSE SUMMARY\b/.test(t)
    || /^MINIMUM CUMULATIVE RETURN\b/.test(t)
  ) {
    return 'cost_capacity';
  }

  // 9. Parameter Optimization
  if (
    /^TAIL GUARDRAIL GRID SWEEP\b/.test(t)
    || /^PARAMETER SWEEP\b/.test(t)
    || /^PARAMETER SURFACE MAP\b/.test(t)
    || /^L_HIGH SURFACE\b/.test(t)
    || /^SHARPE RIDGE MAP\b/.test(t)
    || /^SHARPE PLATEAU DETECTOR\b/.test(t)
    || /^PARAMETRIC STABILITY CUBE\b/.test(t)
    || /^STABILITY CUBE SUMMARY\b/.test(t)
    || /^RISK THROTTLE STABILITY CUBE\b/.test(t)
    || /^EXIT ARCHITECTURE STABILITY CUBE\b/.test(t)
  ) {
    return 'parameter_optimization';
  }

  return 'return_profile';
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
    // For box-drawn sections, truncate at the closing └── line
    const closeIdx = sectionLines.findIndex((l) => /^└[─]+/.test(l.trim()));
    if (closeIdx >= 0) sectionLines = sectionLines.slice(0, closeIdx + 1);
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
    let sectionLines = lines.slice(start, end);
    // For box-drawn sections, truncate at the closing └── line
    const closeIdx = sectionLines.findIndex((l) => /^└[─]+/.test(l.trim()));
    if (closeIdx >= 0) sectionLines = sectionLines.slice(0, closeIdx + 1);
    const body = sectionLines.join('\n').trim();
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
    || /^MINIMUM CUMULATIVE RETURN\b/i.test(t)
    || /^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/i.test(t)
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
    /^MINIMUM CUMULATIVE RETURN\b/i,
    /^DEFLATED SHARPE RATIO \+ MINIMUM TRACK RECORD LENGTH\b/i,
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
    // PARAM JITTER embeds a FEES PANEL inside its output — skip FEES PANEL
    // headings when searching for the next section boundary.
    const skipFeesPanel = /^PARAM JITTER/i.test(line);
    for (let j = i + 1; j < lines.length; j += 1) {
      const tl = lines[j].trim();
      if (skipFeesPanel && /^FEES PANEL\b/i.test(tl)) continue;
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
    // 1. Verdict & Grades
    [/^BOTTOM LINE\b/, 100],
    [/^VERDICT\b/, 101],
    [/^GATING METRICS\b/, 102],
    [/^CORE METRICS\b/, 103],
    [/^SUPPORTING METRICS\b/, 104],
    [/^WHAT YOU HAVE\b/, 105],
    [/^WHAT YOU STILL NEED\b/, 106],

    // 2. Scorecards & Summary
    [/^ALLOCATOR VIEW SCORECARD\b/, 200],
    [/^TECHNICAL APPENDIX SCORECARD\b/, 201],
    [/^INSTITUTIONAL SCORECARD\b/, 202],
    [/^BEST FILTER HEADLINE STATS\b/, 203],
    [/^RUN SUMMARY\b/, 204],
    [/^WEEKLY MILESTONES\b/, 205],
    [/^MONTHLY MILESTONES\b/, 206],
    [/^EQUITY ENSEMBLE\b/, 207],
    [/OUTPUT FILES/i, 208],

    // 3. Return Profile
    [/^RETURN RATES BY PERIOD\b/, 300],
    [/^PERIODIC RETURN BREAKDOWN\b/, 301],
    [/^RETURN DISTRIBUTION\b/, 302],
    [/^RETURN CONCENTRATION ANALYSIS\b/, 303],
    [/^RETURN \+ CONDITIONAL ANALYSIS\b/, 304],
    [/^REGIME & CONDITIONAL ANALYSIS\b/, 305],
    [/^ROLLING MAX DRAWDOWN\b/, 306],
    [/^ALPHA VS BETA DECOMPOSITION\b/, 307],

    // 4. Risk & Drawdown
    [/^RISK-ADJUSTED RETURN QUALITY\b/, 400],
    [/^DAILY VAR \/ CVAR\b/, 401],
    [/^TAIL RISK\b/, 402],
    [/^DRAWDOWN EPISODE ANALYSIS\b/, 403],
    [/^RUIN PROBABILITY\b/, 404],
    [/^SHOCK INJECTION TEST\b/, 405],

    // 5. Statistical Validity
    [/^DEFLATED SHARPE RATIO\b/, 500],
    [/^STATISTICAL VALIDITY\b/, 501],
    [/^PBO\b/, 502],
    [/^PROBABILITY OF BACKTEST OVERFITTING\b/, 503],
    [/^SIGNAL PREDICTIVENESS\b/, 504],
    [/^SIMULATION BIAS AUDIT\b/, 505],
    [/^DAILY SERIES AUDIT\b/, 506],
    [/^STRESS TEST SUMMARY\b/, 507],

    // 6. Walk-Forward & Regime
    [/^WALK-FORWARD VALIDATION\b/, 600],
    [/^WALK-FORWARD ROLLING\b/, 601],
    [/^FILTER-AWARE WALK-FORWARD\b/, 602],
    [/^SHARPE STABILITY ANALYSIS\b/, 603],
    [/^REGIME ROBUSTNESS TEST\b/, 604],
    [/^REGIME CONSISTENCY SUMMARY\b/, 605],

    // 7. Sensitivity & Stress
    [/^NOISE PERTURBATION STABILITY TEST\b/, 700],
    [/^PARAM JITTER \/ SHARPE STABILITY TEST\b/, 701],
    [/^NEIGHBOR PLATEAU TEST\b/, 702],
    [/^PARAMETER SENSITIVITY MAP\b/, 703],
    [/^PARAMETER SENSITIVITY SUMMARY\b/, 704],
    [/^LUCKY STREAK TEST\b/, 705],
    [/^TOP-N DAY REMOVAL TEST\b/, 706],
    [/^CAPPED RETURN SENSITIVITY TABLE\b/, 707],

    // 8. Cost & Capacity
    [/^SLIPPAGE SENSITIVITY TABLE\b/, 800],
    [/^SLIPPAGE IMPACT SWEEP\b/, 801],
    [/^COST CURVE TEST\b/, 802],
    [/^CAPACITY CURVE TEST\b/, 803],
    [/^LIQUIDITY CAPACITY CURVE\b/, 804],
    [/^CAPITAL & OPERATIONAL\b/, 805],
    [/^MARKET CAP DIAGNOSTIC\b/, 806],
    [/^MARKET CAP UNIVERSE SUMMARY\b/, 807],
    [/^MINIMUM CUMULATIVE RETURN\b/, 808],

    // 9. Parameter Optimization
    [/^TAIL GUARDRAIL GRID SWEEP\b/, 900],
    [/^PARAMETER SWEEP\b/, 901],
    [/^PARAMETER SURFACE MAP\b/, 902],
    [/^L_HIGH SURFACE - RANKED BY SHARPE\b/, 903],
    [/^L_HIGH SURFACE\b/, 904],
    [/^SHARPE RIDGE MAP\b/, 905],
    [/^SHARPE PLATEAU DETECTOR\b/, 906],
    [/^PARAMETRIC STABILITY CUBE\b/, 907],
    [/^STABILITY CUBE SUMMARY\b/, 908],
    [/^RISK THROTTLE STABILITY CUBE\b/, 909],
    [/^RISK THROTTLE STABILITY CUBE SUMMARY\b/, 910],
    [/^EXIT ARCHITECTURE STABILITY CUBE\b/, 911],
    [/^EXIT ARCHITECTURE STABILITY CUBE SUMMARY\b/, 912],
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
    if (
      prev
      && t.startsWith('REGIME CONSISTENCY SUMMARY')
      && prev.title.toUpperCase().includes('REGIME ROBUSTNESS TEST')
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
  leverage: number,
): Array<number | null> {
  if (!Array.isArray(bars) || bars.length === 0) return [];
  return bars.map((v) => (v === null || !Number.isFinite(v) ? null : v * leverage));
}

// ── Performance by Hour of Day ──────────────────────────────────────────────
//
// Bins each active day's intraday session into 18 hour buckets (06:00–07:00,
// 07:00–08:00, … 23:00–00:00 UTC). For each bucket on each day, computes
// (bars[end_of_hour] - bars[start_of_hour]) × leverage — i.e. the leveraged
// return realized within that hour. Days where the bucket isn't fully
// available (early exit before end_of_hour, missing data) are OMITTED from
// that bucket's average rather than counted as 0% — preserves signal for
// hours that don't always fire.
//
// Two view modes:
//   HOURLY      bar chart of avg per-hour return
//   CUMULATIVE  line chart of running sum (= hypothetical equity curve assuming
//               every day got every hour's average; ends roughly at the avg
//               daily strat_roi assuming all days completed all hours)
function HourlyPerformanceChart({
  activeDays,
  intradayBars,
  exitBars,
  levByDate,
}: {
  activeDays: Array<{ date: string; stratRoi: number }>;
  intradayBars: Record<string, Array<number | null>> | null | undefined;
  exitBars: Record<string, number> | null | undefined;
  levByDate: Record<string, number>;
}) {
  const [mode, setMode] = useState<'hourly' | 'cumulative'>('hourly');
  const [dayFilter, setDayFilter] = useState<'all' | 'pos' | 'neg'>('all');
  const [firstHourAnchor, setFirstHourAnchor] = useState<'entry' | 'full'>('entry');
  const [hovered, setHovered] = useState<number | null>(null);
  const [open, setOpen] = useState(true);

  const N_HOURS = 18;
  const BARS_PER_HOUR = 12;
  // Bar index at 06:35 UTC (entry timestamp = session_open + 35 min, 5-min bars).
  // Used when firstHourAnchor='entry' to start hour-0's binning at the moment
  // positions actually exist, excluding the 06:00-06:35 pre-entry window
  // where intraday_bars values reflect symbol price drift the strategy
  // didn't capture (no positions open yet).
  const ENTRY_BAR_IDX = 7;

  // Filter the day set based on each day's strat_roi sign before
  // aggregating: ALL keeps every active day, POS keeps strat_roi > 0
  // (winning days only — answers "when does the win happen?"), NEG keeps
  // strat_roi < 0 (losing days only — answers "when does the damage land?").
  const filteredDays = activeDays.filter(({ stratRoi }) => {
    if (dayFilter === 'pos') return stratRoi > 0;
    if (dayFilter === 'neg') return stratRoi < 0;
    return true;
  });
  const filteredDates = filteredDays.map((d) => d.date);

  // Aggregate per-hour stats. Bucket h covers session hour [06+h, 06+h+1).
  const stats = Array.from({ length: N_HOURS }, () => ({
    count: 0, wins: 0, total: 0,
  }));
  if (!intradayBars || filteredDates.length === 0) {
    // fall through with empty stats — render below shows "no data"
  } else {
    for (const date of filteredDates) {
      const rawBars = intradayBars[date];
      if (!rawBars || rawBars.length < 2) continue;
      const lev = levByDate[date] ?? 1;
      const exitBar = exitBars?.[date] ?? rawBars.length;
      const effectiveLen = Math.min(rawBars.length, exitBar);

      for (let h = 0; h < N_HOURS; h++) {
        // Hour 0 has a special start anchor: 06:35 (entry) by default,
        // 06:00 (session open) when toggled to FULL. All other hours
        // always start at their natural h*12 boundary.
        const startIdx = (h === 0 && firstHourAnchor === 'entry')
          ? ENTRY_BAR_IDX
          : h * BARS_PER_HOUR;
        const targetEndIdx = (h + 1) * BARS_PER_HOUR;
        // Skip hours we don't have full data for. Allow the trailing hour 17
        // to use the very last available bar even if it sits one bar shy of
        // a full 12-bar hour (session closes at 23:55 UTC = bar 215, not 216).
        if (startIdx >= effectiveLen) break;
        let endIdx: number;
        if (targetEndIdx <= effectiveLen - 1) {
          endIdx = targetEndIdx;
        } else if (h === N_HOURS - 1 && effectiveLen >= N_HOURS * BARS_PER_HOUR - 1) {
          endIdx = effectiveLen - 1;
        } else {
          break; // partial hour from early exit — omit
        }
        const sv = rawBars[startIdx];
        const ev = rawBars[endIdx];
        if (sv == null || ev == null || !Number.isFinite(sv) || !Number.isFinite(ev)) continue;
        const r = (ev - sv) * lev;
        stats[h].count++;
        stats[h].total += r;
        if (r > 0) stats[h].wins++;
      }
    }
  }

  const avgs: Array<number | null> = stats.map((s) => (s.count > 0 ? s.total / s.count : null));
  // Cumulative: running sum, starting at 0 at the 06:00 anchor. Hours with
  // no data contribute 0 to the cumulative line so the curve doesn't
  // disappear — this is a hypothetical "average path" not a real equity.
  const cumulative: number[] = [0];
  for (let h = 0; h < N_HOURS; h++) {
    cumulative.push(cumulative[cumulative.length - 1] + (avgs[h] ?? 0));
  }

  const yValues = mode === 'hourly'
    ? avgs.filter((v): v is number => v !== null)
    : cumulative;
  // Asymmetric y-range: pad just enough above the data max and below the
  // data min so the chart fits the actual values tightly. Always include 0
  // in range so the zero line renders. Earlier symmetric ±yRange wasted
  // huge vertical space when data was mostly positive (or vice versa).
  const dataMax = yValues.length > 0 ? Math.max(...yValues, 0) : 0.5;
  const dataMin = yValues.length > 0 ? Math.min(...yValues, 0) : -0.5;
  const span = Math.max(dataMax - dataMin, 0.5);
  // 8% pad above + 4% pad below — top needs more room for hover tooltips
  // sitting above positive bars; bottom rarely has hover text below.
  const yMaxPadded = dataMax + span * 0.08;
  const yMinPadded = dataMin - span * 0.04;
  const yPaddedSpan = Math.max(yMaxPadded - yMinPadded, 1e-9);

  const W = 600, H = 240;
  const padL = 36, padR = 8, padT = 10, padB = 24;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  // Compact 12-hour label: "6a", "11p", "12a" — fits 19 ticks across plotW.
  const fmtHourLabel = (utcHour: number): string => {
    const h = ((utcHour % 24) + 24) % 24;
    const period = h < 12 ? 'a' : 'p';
    let h12 = h % 12;
    if (h12 === 0) h12 = 12;
    return `${h12}${period}`;
  };

  const xForBar = (h: number) => padL + (h + 0.5) * (plotW / N_HOURS);
  const xForCum = (h: number) => padL + (h / N_HOURS) * plotW;
  const yFor = (v: number) => padT + plotH * (1 - (v - yMinPadded) / yPaddedSpan);
  const yZero = yFor(0);

  const barW = (plotW / N_HOURS) * 0.7;

  const totalDays = filteredDates.length;
  const dataDays = stats.reduce((m, s) => Math.max(m, s.count), 0);

  // Header stats — bucket-level (each of the 18 hour buckets is one data
  // point). posCount = buckets whose mean is > 0; negCount = mean < 0.
  // hourlyWinrate = posCount / (posCount + negCount). avgPos / avgNeg are
  // the average of the bucket means by sign.
  const validAvgs = avgs.filter((v): v is number => v !== null);
  const posAvgs = validAvgs.filter((v) => v > 0);
  const negAvgs = validAvgs.filter((v) => v < 0);
  const posCount = posAvgs.length;
  const negCount = negAvgs.length;
  const totalWithData = posCount + negCount;
  const hourlyWinrate = totalWithData > 0 ? (posCount / totalWithData) * 100 : 0;
  const avgPos = posCount > 0 ? posAvgs.reduce((a, b) => a + b, 0) / posCount : 0;
  const avgNeg = negCount > 0 ? negAvgs.reduce((a, b) => a + b, 0) / negCount : 0;

  return (
    <div style={{ border: '1px solid var(--line)', borderRadius: 3, padding: 10, background: 'var(--bg2)', marginBottom: 14 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: open ? 8 : 0 }}>
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          aria-label={open ? 'Collapse hourly performance' : 'Expand hourly performance'}
          style={{
            background: 'transparent',
            border: 'none',
            color: 'var(--t2)',
            cursor: 'pointer',
            padding: 0,
            fontSize: 10,
            lineHeight: 1,
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: 14,
            height: 14,
            transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
            transition: 'transform 0.15s ease',
          }}
        >
          ▸
        </button>
        <div style={{ fontSize: 8, fontWeight: 700, letterSpacing: '0.12em', color: 'var(--t3)', textTransform: 'uppercase' }}>
          Performance by Hour of Day (UTC)
        </div>
        {/* Header stats — visible whether open or collapsed */}
        {totalWithData > 0 && (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 12,
              fontSize: 9,
              fontFamily: 'var(--font-space-mono)',
              flex: 1,
            }}
          >
            <span><span style={{ color: 'var(--green)', fontWeight: 700 }}>{posCount}</span><span style={{ color: 'var(--t3)' }}>+</span></span>
            <span><span style={{ color: 'var(--red)', fontWeight: 700 }}>{negCount}</span><span style={{ color: 'var(--t3)' }}>−</span></span>
            <span style={{ color: 'var(--t2)' }}>
              <span style={{ color: hourlyWinrate >= 50 ? 'var(--green)' : 'var(--red)', fontWeight: 700 }}>{hourlyWinrate.toFixed(0)}%</span>
              <span style={{ color: 'var(--t3)', marginLeft: 2 }}>W</span>
            </span>
            <span>
              <span style={{ color: 'var(--t3)', marginRight: 4 }}>μ+</span>
              <span style={{ color: 'var(--green)', fontWeight: 700 }}>+{avgPos.toFixed(2)}%</span>
            </span>
            <span>
              <span style={{ color: 'var(--t3)', marginRight: 4 }}>μ−</span>
              <span style={{ color: 'var(--red)', fontWeight: 700 }}>{avgNeg.toFixed(2)}%</span>
            </span>
          </div>
        )}
        {open && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            {/* First-hour anchor: ENTRY (06:35, default — truthful, captures
                only the period the strategy actually had positions on) vs
                FULL (06:00, includes the 06:00-06:35 pre-entry price drift
                that the strategy never realized). */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ fontSize: 8, color: 'var(--t3)', letterSpacing: '0.08em', fontFamily: 'var(--font-space-mono)' }}>1H</span>
              <div style={{ display: 'flex', gap: 0 }}>
                {([
                  { key: 'full', label: '06:00' },
                  { key: 'entry', label: '06:35' },
                ] as const).map((opt, i, arr) => {
                  const active = firstHourAnchor === opt.key;
                  return (
                    <button
                      key={opt.key}
                      type="button"
                      onClick={() => setFirstHourAnchor(opt.key)}
                      title={opt.key === 'entry'
                        ? "Hour 0 anchored at 06:35 entry — excludes pre-entry price drift the strategy didn't capture"
                        : 'Hour 0 spans the full 06:00–07:00 window — includes 06:00–06:35 pre-entry price drift'}
                      style={{
                        padding: '3px 7px',
                        fontSize: 9,
                        fontWeight: 700,
                        letterSpacing: '0.08em',
                        background: active ? 'var(--bg4)' : 'transparent',
                        color: active ? 'var(--t0)' : 'var(--t2)',
                        border: '1px solid var(--line)',
                        borderRight: i === arr.length - 1 ? '1px solid var(--line)' : 'none',
                        cursor: 'pointer',
                        fontFamily: 'var(--font-space-mono)',
                      }}
                    >
                      {opt.label}
                    </button>
                  );
                })}
              </div>
            </div>
            {/* Day-set filter: include all active days, only winning days,
                or only losing days. Lets the user see WHEN the wins happen
                separately from WHEN the losses happen. */}
            <div style={{ display: 'flex', gap: 0 }}>
              {([
                { key: 'all', label: 'ALL' },
                { key: 'pos', label: 'POS' },
                { key: 'neg', label: 'NEG' },
              ] as const).map((opt, i, arr) => {
                const active = dayFilter === opt.key;
                return (
                  <button
                    key={opt.key}
                    type="button"
                    onClick={() => setDayFilter(opt.key)}
                    style={{
                      padding: '3px 7px',
                      fontSize: 9,
                      fontWeight: 700,
                      letterSpacing: '0.08em',
                      background: active ? 'var(--bg4)' : 'transparent',
                      color: active
                        ? (opt.key === 'pos' ? 'var(--green)' : opt.key === 'neg' ? 'var(--red)' : 'var(--t0)')
                        : 'var(--t2)',
                      border: '1px solid var(--line)',
                      borderRight: i === arr.length - 1 ? '1px solid var(--line)' : 'none',
                      cursor: 'pointer',
                      fontFamily: 'var(--font-space-mono)',
                    }}
                  >
                    {opt.label}
                  </button>
                );
              })}
            </div>
            <div style={{ display: 'flex', gap: 0 }}>
              {(['hourly', 'cumulative'] as const).map((m) => {
                const active = mode === m;
                return (
                  <button
                    key={m}
                    type="button"
                    onClick={() => setMode(m)}
                    style={{
                      padding: '3px 8px',
                      fontSize: 9,
                      fontWeight: 700,
                      letterSpacing: '0.08em',
                      textTransform: 'uppercase',
                      background: active ? 'var(--bg4)' : 'transparent',
                      color: active ? 'var(--t0)' : 'var(--t2)',
                      border: '1px solid var(--line)',
                      borderRight: m === 'hourly' ? 'none' : '1px solid var(--line)',
                      cursor: 'pointer',
                      fontFamily: 'var(--font-space-mono)',
                    }}
                  >
                    {m}
                  </button>
                );
              })}
            </div>
          </div>
        )}
      </div>
      {open && (<>

      <svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{ display: 'block' }}>
        {/* Minor horizontal grid lines at "nice" round intervals across the
            asymmetric y-range. Picks step from the {1, 2, 5} × 10^n family
            so the resulting ticks are always at clean values like 0.5%, 1%,
            2%, 5% rather than data-driven fractions. Always renders ≥3
            lines (the algorithm targets ~5 lines). Zero line is rendered
            on top with a dashed style for emphasis. */}
        {(() => {
          const niceStep = (() => {
            const range = yPaddedSpan;
            const rough = range / 5;
            const exp = Math.pow(10, Math.floor(Math.log10(rough)));
            const norm = rough / exp;
            let nice: number;
            if (norm < 1.5) nice = 1;
            else if (norm < 3) nice = 2;
            else if (norm < 7) nice = 5;
            else nice = 10;
            return nice * exp;
          })();
          const ticks: number[] = [];
          const start = Math.ceil(yMinPadded / niceStep) * niceStep;
          for (let v = start; v <= yMaxPadded + 1e-9; v += niceStep) {
            ticks.push(Math.round(v / niceStep) * niceStep);
          }
          // Determine label precision from step magnitude so we don't show
          // "+0%" when step is 0.5 — at least one decimal then.
          const decimals = niceStep >= 1 ? 0 : niceStep >= 0.1 ? 1 : 2;
          return ticks.map((v) => {
            const isZero = Math.abs(v) < niceStep / 2;
            return (
              <g key={v}>
                <line
                  x1={padL}
                  x2={W - padR}
                  y1={yFor(v)}
                  y2={yFor(v)}
                  stroke={isZero ? 'var(--line2)' : 'var(--line)'}
                  strokeWidth={0.5}
                  strokeDasharray={isZero ? '2 2' : undefined}
                />
                <text
                  x={padL - 4}
                  y={yFor(v) + 3}
                  fontSize={8}
                  fill="var(--t3)"
                  textAnchor="end"
                  fontFamily="var(--font-space-mono)"
                >
                  {v > 0 ? '+' : ''}{v.toFixed(decimals)}%
                </text>
              </g>
            );
          });
        })()}
        {/* x-axis hour labels — every hour, compact 12h format ("6a", "11p"). */}
        {Array.from({ length: N_HOURS + 1 }).map((_, h) => (
          <text
            key={h}
            x={xForCum(h)}
            y={H - 8}
            fontSize={9}
            fill="var(--t2)"
            textAnchor="middle"
            fontFamily="var(--font-space-mono)"
          >
            {fmtHourLabel(6 + h)}
          </text>
        ))}

        {mode === 'hourly' ? (
          // Bar chart
          <>
            {avgs.map((v, h) => {
              if (v === null) return null;
              const cx = xForBar(h);
              const top = yFor(Math.max(0, v));
              const bot = yFor(Math.min(0, v));
              const fill = v >= 0 ? 'var(--green)' : 'var(--red)';
              return (
                <g key={h}>
                  <rect
                    x={cx - barW / 2}
                    y={top}
                    width={barW}
                    height={Math.max(1, bot - top)}
                    fill={fill}
                    opacity={hovered == null || hovered === h ? 0.7 : 0.3}
                    onMouseEnter={() => setHovered(h)}
                    onMouseLeave={() => setHovered(null)}
                    style={{ cursor: 'crosshair' }}
                  />
                </g>
              );
            })}
            {/* Mean dashed line — average across non-null hour buckets,
                drawn on top of bars so it's visible against the fills. */}
            {validAvgs.length > 0 && (() => {
              const mean = validAvgs.reduce((a, b) => a + b, 0) / validAvgs.length;
              const meanY = yFor(mean);
              const labelAbove = mean >= 0;
              return (
                <g>
                  <line
                    x1={padL}
                    x2={W - padR}
                    y1={meanY}
                    y2={meanY}
                    stroke="var(--t0)"
                    strokeWidth={0.8}
                    strokeDasharray="4 3"
                    opacity={0.65}
                  />
                  <text
                    x={W - padR - 4}
                    y={labelAbove ? meanY - 4 : meanY + 11}
                    fontSize={9}
                    fill="var(--t1)"
                    textAnchor="end"
                    fontFamily="var(--font-space-mono)"
                    fontWeight={700}
                  >
                    mean {mean >= 0 ? '+' : ''}{mean.toFixed(2)}%
                  </text>
                </g>
              );
            })()}
            {/* Hourly hover tooltip */}
            {hovered !== null && avgs[hovered] !== null && (
              <g>
                <text
                  x={xForBar(hovered)}
                  y={yFor(avgs[hovered] as number) + ((avgs[hovered] as number) >= 0 ? -4 : 12)}
                  fontSize={9}
                  fill={(avgs[hovered] as number) >= 0 ? 'var(--green)' : 'var(--red)'}
                  fontWeight={700}
                  textAnchor="middle"
                  fontFamily="var(--font-space-mono)"
                >
                  {(avgs[hovered] as number) >= 0 ? '+' : ''}{(avgs[hovered] as number).toFixed(2)}%
                </text>
              </g>
            )}
          </>
        ) : (
          // Cumulative line — per-hour segment shading by slope direction
          // (green = up-segment, red = down-segment), plus a faint dashed
          // trendline from session-start (0%) to the final cumulative value
          // as a "constant pace" reference.
          <>
            {/* Per-segment fill + line. Each hour's segment is its own
                trapezoid colored by avgs[h] sign, and its line stroke
                matches. Visualizes which hours pull the cumulative path
                up vs back. */}
            {Array.from({ length: N_HOURS }).map((_, h) => {
              const v = avgs[h];
              if (v === null) return null;
              const x0 = xForCum(h);
              const x1 = xForCum(h + 1);
              const y0 = yFor(cumulative[h]);
              const y1 = yFor(cumulative[h + 1]);
              const color = v >= 0 ? 'var(--green)' : 'var(--red)';
              return (
                <g key={`seg-${h}`}>
                  <path
                    d={`M ${x0} ${y0} L ${x1} ${y1} L ${x1} ${yZero} L ${x0} ${yZero} Z`}
                    fill={color}
                    opacity={0.14}
                  />
                  <line x1={x0} y1={y0} x2={x1} y2={y1} stroke={color} strokeWidth={1.5} />
                </g>
              );
            })}
            {/* Constant-pace trendline — straight from start (0%) to the
                final cumulative value. Path above this line = strategy
                outperforming its average pace; path below = lagging. */}
            {cumulative.length > 1 && (
              <line
                x1={xForCum(0)}
                y1={yFor(cumulative[0])}
                x2={xForCum(N_HOURS)}
                y2={yFor(cumulative[N_HOURS])}
                stroke="var(--t2)"
                strokeWidth={0.7}
                strokeDasharray="3 3"
                opacity={0.5}
              />
            )}
            {/* Hover dots at each hour boundary */}
            {cumulative.map((v, i) => (
              <circle
                key={i}
                cx={xForCum(i)}
                cy={yFor(v)}
                r={hovered === i - 1 || hovered === i ? 3 : 1.5}
                fill={v >= 0 ? 'var(--green)' : 'var(--red)'}
                onMouseEnter={() => setHovered(i)}
                onMouseLeave={() => setHovered(null)}
                style={{ cursor: 'crosshair' }}
              />
            ))}
            {hovered !== null && cumulative[hovered] !== undefined && (
              <text
                x={xForCum(hovered)}
                y={yFor(cumulative[hovered]) + (cumulative[hovered] >= 0 ? -6 : 14)}
                fontSize={9}
                fill={cumulative[hovered] >= 0 ? 'var(--green)' : 'var(--red)'}
                fontWeight={700}
                textAnchor="middle"
                fontFamily="var(--font-space-mono)"
              >
                {cumulative[hovered] >= 0 ? '+' : ''}{cumulative[hovered].toFixed(2)}%
              </text>
            )}
          </>
        )}
      </svg>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 6, fontSize: 8, color: 'var(--t3)' }}>
        <span>{totalDays} active day{totalDays === 1 ? '' : 's'} · max {dataDays} day{dataDays === 1 ? '' : 's'}/hour</span>
        {hovered !== null && stats[mode === 'cumulative' ? Math.max(0, hovered - 1) : hovered] && (
          <span>
            {(() => {
              const idx = mode === 'cumulative' ? Math.max(0, hovered - 1) : hovered;
              const s = stats[idx];
              if (!s || s.count === 0) return null;
              const hourLabel = String((6 + idx) % 24).padStart(2, '0') + '–' + String((6 + idx + 1) % 24).padStart(2, '0');
              return `${hourLabel} · ${s.count}d · ${(s.wins / s.count * 100).toFixed(0)}%W`;
            })()}
          </span>
        )}
      </div>
      </>)}
    </div>
  );
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
  const [zoomLevel, setZoomLevel] = useState(1); // 1=zoomed out, 10=zoomed in
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
      const lev = typeof day.lev === 'number' && Number.isFinite(day.lev) ? day.lev : 1;
      intradayByDate[day.date] = normalizeIntradaySeriesToDayReturn(raw, lev);
    }
  }
  const intradayMatchedDays = Object.keys(intradayByDate).length;
  const usePolylines = mode === 'poly' && intradayMatchedDays > 0;

  // Scale y-axis: use day returns for linear, day returns for poly baseline
  // Poly mode uses the same scale as linear so zoom levels are consistent
  let absMax = 0.5;
  {
    const absReturns = returns.filter(Number.isFinite).map(Math.abs);
    if (absReturns.length > 0) {
      absMax = Math.max(...absReturns, 0.5);
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
  const yRange = absMax * 1.2 * zoomFactor * (usePolylines ? 2.25 : 1);

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
          {/* Y-axis: zero line label only */}
          <text x={padL - 4} y={toY(0) + 3} textAnchor="end" fontSize={7.5} fill="var(--t3)" fontFamily="var(--font-space-mono), Space Mono, monospace">0%</text>
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
      <SectionHBarChart items={kv.map((k) => ({ label: k.key, value: k.num, raw: k.raw }))} colorFn={colorFn} />
    </div>
  );
}

function renderReturnRatesByPeriod(body: string) {
  const lines = body.split('\n');

  type PeriodGroup = {
    period: string;
    count: string;
    winRate: string;
    winLoss: string;
    avg: string;
    avgWin: string;
    avgLoss: string;
    best: string;
    worst: string;
  };

  const groups: PeriodGroup[] = [];
  let cur: Partial<PeriodGroup> | null = null;

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/RETURN RATES BY PERIOD/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "MONTHLY  (13 months)"
    const periodMatch = clean.match(/^(MONTHLY|WEEKLY|DAILY)\s+\((.+?)\)/i);
    if (periodMatch) {
      if (cur && cur.period) groups.push(cur as PeriodGroup);
      cur = { period: periodMatch[1].toUpperCase(), count: periodMatch[2], winRate: '', winLoss: '', avg: '', avgWin: '', avgLoss: '', best: '', worst: '' };
      continue;
    }

    if (!cur) continue;

    // "Win rate:        69.2%  (9W / 4L)"
    const wrMatch = clean.match(/Win rate\s*:\s*([0-9.]+%)\s*\((.+?)\)/i);
    if (wrMatch) { cur.winRate = wrMatch[1]; cur.winLoss = wrMatch[2]; continue; }

    // "Avg month:       +46.54%" or "Avg week:" or "Avg day:"
    const avgMatch = clean.match(/^Avg\s+(?:month|week|day)\s*:\s*([0-9.+%-]+)/i);
    if (avgMatch) { cur.avg = avgMatch[1]; continue; }

    // "Avg win:         76.49%"
    const awMatch = clean.match(/Avg win\s*:\s*([0-9.+%-]+)/i);
    if (awMatch) { cur.avgWin = awMatch[1]; continue; }

    // "Avg loss:        -20.85%"
    const alMatch = clean.match(/Avg loss\s*:\s*([0-9.+%-]+)/i);
    if (alMatch) { cur.avgLoss = alMatch[1]; continue; }

    // "Best month:      202.88%   Worst:  -34.96%"
    const bwMatch = clean.match(/Best\s+\S+\s*:\s*([0-9.+%-]+)\s+Worst\s*:\s*([0-9.+%-]+)/i);
    if (bwMatch) { cur.best = bwMatch[1]; cur.worst = bwMatch[2]; continue; }
  }
  if (cur && cur.period) groups.push(cur as PeriodGroup);

  if (groups.length === 0) return <PreFallback body={body} />;

  const periodIcon: Record<string, string> = { MONTHLY: 'M', WEEKLY: 'W', DAILY: 'D' };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        {groups.map((g, gIdx) => {
          const wr = parseFloat(g.winRate);
          const wrColor = wr >= 60 ? 'var(--green)' : wr >= 45 ? 'var(--orange)' : 'var(--red)';
          const avgNum = parseFloat(g.avg);
          const avgColor = avgNum >= 0 ? 'var(--green)' : 'var(--red)';

          return (
            <div key={gIdx} style={{
              flex: '1 1 200px',
              background: 'var(--bg2)',
              border: '1px solid var(--line1)',
              borderRadius: 8,
              padding: '14px 16px',
              display: 'flex',
              flexDirection: 'column',
              gap: 12,
            }}>
              {/* Period header */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{
                  width: 28, height: 28, borderRadius: 6,
                  background: 'rgba(255,255,255,0.05)', border: '1px solid var(--line1)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 14, fontWeight: 700, color: 'var(--t2)', ...MONO,
                }}>
                  {periodIcon[g.period] ?? g.period[0]}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--t1)', ...MONO }}>{g.period}</span>
                  <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{g.count}</span>
                </div>
              </div>

              {/* Win rate bar */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                  <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Win Rate</span>
                  <span style={{ fontSize: 14, fontWeight: 600, color: wrColor, ...MONO }}>{g.winRate}</span>
                </div>
                <div style={{ height: 4, borderRadius: 2, background: 'rgba(255,255,255,0.06)', overflow: 'hidden' }}>
                  <div style={{ height: '100%', width: `${wr}%`, borderRadius: 2, background: wrColor }} />
                </div>
                <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO, textAlign: 'right' }}>{g.winLoss}</span>
              </div>

              {/* Avg return */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '6px 0', borderTop: '1px solid var(--line1)' }}>
                <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Avg Return</span>
                <span style={{ fontSize: 16, fontWeight: 600, color: avgColor, ...MONO }}>{g.avg}</span>
              </div>

              {/* Avg win / Avg loss */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Avg Win</span>
                  <span style={{ fontSize: 12, color: 'var(--green)', fontWeight: 500, ...MONO }}>{g.avgWin}</span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2, textAlign: 'right' }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Avg Loss</span>
                  <span style={{ fontSize: 12, color: 'var(--red)', fontWeight: 500, ...MONO }}>{g.avgLoss}</span>
                </div>
              </div>

              {/* Best / Worst */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, borderTop: '1px solid var(--line1)', paddingTop: 8 }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Best</span>
                  <span style={{ fontSize: 12, color: 'var(--green)', fontWeight: 500, ...MONO }}>{g.best}</span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2, textAlign: 'right' }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Worst</span>
                  <span style={{ fontSize: 12, color: 'var(--red)', fontWeight: 500, ...MONO }}>{g.worst}</span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderReturnDistribution(body: string) {
  const lines = body.split('\n');

  type DistItem = { label: string; value: string; note: string; status: 'ok' | 'warn' | 'none' };
  const items: DistItem[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/RETURN DISTRIBUTION/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "Skewness:                   1.6885  OK"
    // "Excess kurtosis:            6.0174  ⚠ fat tails"
    // "JB p-value:                 0.0000  (NON-NORMAL ⚠)"
    // "LB p-value:                 0.8353  (no autocorrelation)"
    const match = clean.match(/^(.+?):\s+([0-9.+-]+)\s*(.*)/);
    if (match) {
      const label = match[1].trim();
      const value = match[2].trim();
      const rest = match[3].trim();

      let status: 'ok' | 'warn' | 'none' = 'none';
      if (/⚠/.test(rest) || /NON-NORMAL/i.test(rest)) status = 'warn';
      else if (/OK|✅|no autocorrelation/i.test(rest)) status = 'ok';

      const note = rest.replace(/[⚠✅]/g, '').replace(/[()]/g, '').trim();
      items.push({ label, value, note, status });
    }
  }

  if (items.length === 0) {
    const table = parseColumnarTable(body);
    if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
    return <PreFallback body={body} />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {items.map((item, idx) => {
          const color = item.status === 'ok' ? 'var(--green)' : item.status === 'warn' ? 'var(--orange)' : 'var(--t1)';
          const bg = item.status === 'warn' ? 'rgba(255,160,60,0.04)' : 'var(--bg2)';
          const border = item.status === 'warn' ? 'rgba(255,160,60,0.2)' : item.status === 'ok' ? 'rgba(60,255,100,0.15)' : 'var(--line1)';

          return (
            <div key={idx} style={{
              flex: '1 1 160px', background: bg, border: `1px solid ${border}`, borderRadius: 6,
              padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 3,
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>{item.label}</span>
              <span style={{ fontSize: 16, color, fontWeight: 600, ...MONO }}>{item.value}</span>
              {item.note && <span style={{ fontSize: 9, color: item.status === 'warn' ? 'var(--orange)' : 'var(--t4)', ...MONO }}>{item.note}</span>}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderReturnConditional(body: string) {
  return renderTableSection(body);
}

function renderRollingMaxDrawdown(body: string) {
  const lines = body.split('\n');

  type DDRow = { window: string; value: string; warn: boolean };
  const rows: DDRow[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/ROLLING MAX DRAWDOWN/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "Worst  3d window:          -38.11%  ⚠"
    const match = clean.match(/Worst\s+(\d+d)\s+window:\s+([0-9.+-]+%)\s*(⚠|✅)?/i);
    if (match) {
      rows.push({ window: match[1], value: match[2], warn: match[3] === '⚠' });
    }
  }

  if (rows.length === 0) {
    const table = parseColumnarTable(body);
    if (table && table.rows.length > 0) return <SectionTable headers={table.headers} rows={table.rows} />;
    return <PreFallback body={body} />;
  }

  // Find the worst value for scaling
  const allNums = rows.map(r => Math.abs(parseFloat(r.value.replace('%', '')))).filter(Number.isFinite);
  const maxAbs = Math.max(...allNums, 1);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {rows.map((r, idx) => {
          const num = Math.abs(parseFloat(r.value.replace('%', '')));
          const barWidth = (num / maxAbs) * 100;
          const ddColor = num >= 50 ? 'var(--red)' : num >= 30 ? 'var(--orange)' : 'var(--green)';

          return (
            <div key={idx} style={{
              display: 'flex', alignItems: 'center', gap: 12,
              padding: '8px 14px', borderRadius: 4,
              background: 'var(--bg2)', border: '1px solid var(--line1)',
              position: 'relative', overflow: 'hidden',
              ...MONO,
            }}>
              {/* Background bar */}
              <div style={{
                position: 'absolute', top: 0, left: 0, bottom: 0,
                width: `${barWidth}%`, opacity: 0.06,
                background: ddColor,
              }} />
              <span style={{ fontSize: 11, color: 'var(--t3)', minWidth: 50, position: 'relative' }}>{r.window}</span>
              <div style={{ flex: 1, position: 'relative' }}>
                <div style={{ height: 6, borderRadius: 3, background: 'rgba(255,255,255,0.04)', overflow: 'hidden' }}>
                  <div style={{ height: '100%', width: `${barWidth}%`, borderRadius: 3, background: ddColor }} />
                </div>
              </div>
              <span style={{ fontSize: 14, color: ddColor, fontWeight: 600, minWidth: 70, textAlign: 'right', position: 'relative' }}>{r.value}</span>
              {r.warn && <span style={{ fontSize: 10, color: 'var(--orange)', position: 'relative' }}>⚠</span>}
            </div>
          );
        })}
      </div>
    </div>
  );
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
      <div style={{
        display: 'grid',
        gridTemplateColumns: `repeat(${Math.min(metrics.length, 6)}, 1fr)`,
        gap: 10,
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
              padding: '10px 12px',
              display: 'flex',
              flexDirection: 'column',
              gap: 4,
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
  type DSRGroup = { title: string; items: { label: string; value: string; note: string }[] };
  const groups: DSRGroup[] = [];
  let currentGroup: DSRGroup | null = null;

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

function renderShockInjection(body: string) {
  const lines = body.split('\n');

  // Parse baseline
  let baselineSharpe = '';
  let baselineMaxDD = '';
  const rows: { shock: string; nDays: string; sharpe: string; dSharpe: string; maxdd: string; dMaxdd: string; survived: boolean }[] = [];

  for (const line of lines) {
    // Skip box-drawing / separator lines
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─═]{5,}/.test(line)) continue;
    if (/Shock\s+N\s*days/i.test(line)) continue;
    if (/SHOCK INJECTION/i.test(line)) continue;

    // Strip leading │ and whitespace
    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Baseline line
    const baseMatch = clean.match(/Baseline.*?Sharpe:\s*([0-9.+-]+)\s+MaxDD:\s*([0-9.+%-]+)/i);
    if (baseMatch) {
      baselineSharpe = baseMatch[1];
      baselineMaxDD = baseMatch[2];
      continue;
    }

    // Data rows: -10%  1  2.912  +0.093  -55.86%  +1.24%  ⚠ NO / ✅ YES
    const rowMatch = clean.match(
      /^([+-]?\d+%?)\s+(\d+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+%-]+)\s+([0-9.+%-]+)\s+(.*)/
    );
    if (rowMatch) {
      const status = rowMatch[7].trim();
      rows.push({
        shock: rowMatch[1],
        nDays: rowMatch[2],
        sharpe: rowMatch[3],
        dSharpe: rowMatch[4],
        maxdd: rowMatch[5],
        dMaxdd: rowMatch[6],
        survived: /YES/i.test(status),
      });
    }
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  const survivedCount = rows.filter(r => r.survived).length;
  const survivalRate = rows.length > 0 ? Math.round((survivedCount / rows.length) * 100) : 0;
  const overallColor = survivalRate >= 60 ? 'var(--green)' : survivalRate >= 30 ? 'var(--orange)' : 'var(--red)';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Baseline + survival summary */}
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        {baselineSharpe && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 140px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineSharpe}</span>
          </div>
        )}
        {baselineMaxDD && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 140px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline MaxDD</span>
            <span style={{ fontSize: 18, color: 'var(--red)', fontWeight: 600, ...MONO }}>{baselineMaxDD}</span>
          </div>
        )}
        <div style={{
          background: survivalRate >= 60 ? 'rgba(60,255,100,0.05)' : survivalRate >= 30 ? 'rgba(255,160,60,0.05)' : 'rgba(255,60,60,0.05)',
          border: `1px solid ${survivalRate >= 60 ? 'rgba(60,255,100,0.3)' : survivalRate >= 30 ? 'rgba(255,160,60,0.3)' : 'rgba(255,60,60,0.3)'}`,
          borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 140px',
        }}>
          <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Survival Rate</span>
          <span style={{ fontSize: 18, color: overallColor, fontWeight: 600, ...MONO }}>{survivedCount}/{rows.length} ({survivalRate}%)</span>
        </div>
      </div>

      {/* Data rows */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {/* Header */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: '60px 56px 1fr 1fr 1fr 1fr 72px',
          gap: 8,
          padding: '6px 12px',
          borderBottom: '1px solid var(--line1)',
        }}>
          {['Shock', 'Days', 'Sharpe', '\u0394Sharpe', 'MaxDD%', '\u0394MaxDD%', 'Status'].map((h, i) => (
            <span key={i} style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, textAlign: i >= 2 ? 'right' : 'left', ...MONO }}>{h}</span>
          ))}
        </div>

        {rows.map((r, idx) => {
          const survived = r.survived;
          const borderColor = survived ? 'rgba(60,255,100,0.25)' : 'rgba(255,60,60,0.15)';
          const bgColor = survived ? 'rgba(60,255,100,0.04)' : 'rgba(255,60,60,0.03)';
          const dSharpeNum = parseFloat(r.dSharpe);
          const dMaxddNum = parseFloat(r.dMaxdd.replace('%', ''));

          return (
            <div key={idx} style={{
              display: 'grid',
              gridTemplateColumns: '60px 56px 1fr 1fr 1fr 1fr 72px',
              gap: 8,
              padding: '8px 12px',
              background: bgColor,
              border: `1px solid ${borderColor}`,
              borderRadius: 5,
              alignItems: 'center',
              ...MONO,
            }}>
              <span style={{ fontSize: 12, color: 'var(--t1)', fontWeight: 600 }}>{r.shock}</span>
              <span style={{ fontSize: 12, color: 'var(--t2)' }}>{r.nDays}</span>
              <span style={{ fontSize: 12, color: 'var(--t1)', textAlign: 'right' }}>{r.sharpe}</span>
              <span style={{ fontSize: 12, color: dSharpeNum >= 0 ? 'var(--green)' : 'var(--red)', textAlign: 'right' }}>{r.dSharpe}</span>
              <span style={{ fontSize: 12, color: 'var(--t1)', textAlign: 'right' }}>{r.maxdd}</span>
              <span style={{ fontSize: 12, color: dMaxddNum >= 0 ? 'var(--green)' : 'var(--red)', textAlign: 'right' }}>{r.dMaxdd}</span>
              <span style={{
                fontSize: 10,
                fontWeight: 600,
                textAlign: 'center',
                padding: '2px 6px',
                borderRadius: 4,
                background: survived ? 'rgba(60,255,100,0.12)' : 'rgba(255,60,60,0.12)',
                color: survived ? 'var(--green)' : 'var(--red)',
              }}>
                {survived ? 'SURVIVED' : 'FAILED'}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderRegimeRobustness(body: string) {
  const lines = body.split('\n');

  // Detect format: filter variant has "[A]" section headers, IS/OOS variant has "IN-SAMPLE"
  const isRegimeBreakdown = lines.some(l => /\[A\]\s+VOLATILITY/i.test(l));

  if (isRegimeBreakdown) return renderRegimeBreakdown(body);
  return renderRegimeISvsOOS(body);
}

// ── Filter variant: regime breakdown by volatility / momentum / trend ────────
type RegimeRow = { label: string; n: string; sharpe: number; cagr: string; maxdd: string; calmar: string; wfcv: string };
type RegimeGroup = { title: string; tag: string; rows: RegimeRow[] };

function renderRegimeBreakdown(body: string) {
  const lines = body.split('\n');
  const thresholds: string[] = [];
  const groups: RegimeGroup[] = [];
  let currentGroup: RegimeGroup | null = null;

  // Consistency summary fields (merged from REGIME CONSISTENCY SUMMARY)
  let consistencySharpeRange = '';
  let consistencySpread = '';
  let consistencyVerdict = '';
  let consistencyVerdictDetail = '';

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─═]{5,}/.test(line)) continue;
    if (/REGIME ROBUSTNESS/i.test(line)) continue;
    if (/REGIME CONSISTENCY SUMMARY/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Skip "Taxonomy thresholds:" header
    if (/^Taxonomy thresholds/i.test(clean)) continue;
    // Skip CSV/Chart saved lines
    if (/saved:/i.test(clean)) continue;

    // Consistency summary: "Sharpe range across regimes: 1.462 – 4.174  (spread=2.712)"
    const rangeMatch = clean.match(/Sharpe range across regimes:\s*([0-9.+-]+)\s*[–-]\s*([0-9.+-]+)\s*\(spread=([0-9.+-]+)\)/i);
    if (rangeMatch) {
      consistencySharpeRange = `${rangeMatch[1]} – ${rangeMatch[2]}`;
      consistencySpread = rangeMatch[3];
      continue;
    }

    // Consistency verdict: "Verdict: HIGH SENSITIVITY — strong regime dependence"
    const verdictMatch = clean.match(/^Verdict:\s*(.+?)(?:\s*[—–-]\s*(.+))?$/i);
    if (verdictMatch) {
      consistencyVerdict = verdictMatch[1].trim();
      consistencyVerdictDetail = verdictMatch[2]?.trim() ?? '';
      continue;
    }

    // Threshold lines: "Rvol30 low/mid/high splits : p33=0.4615  p67=0.6719"
    if (/p33=|p67=|bull>|bear</i.test(clean)) {
      thresholds.push(clean);
      continue;
    }

    // Group headers: "[A] VOLATILITY REGIMES"
    const groupHeaderMatch = clean.match(/^\[([A-Z])\]\s+(.+)/);
    if (groupHeaderMatch) {
      currentGroup = { tag: groupHeaderMatch[1], title: groupHeaderMatch[2].trim(), rows: [] };
      groups.push(currentGroup);
      continue;
    }

    // Regime rows: "Vol: Low  (rvol30 ≤ p33)       : n=274d  Sharpe= 2.949  CAGR=  3156.3%  MaxDD= -40.69%  Calmar=  77.57  WF_CV=0.645"
    if (currentGroup) {
      const rowMatch = clean.match(
        /^(.+?):\s+n=\s*(\d+)d\s+Sharpe=\s*([0-9.+-]+)\s+CAGR=\s*([0-9.+-]+%?)\s+MaxDD=\s*([0-9.+-]+%?)\s+Calmar=\s*([0-9.+-]+)\s+WF_CV=\s*([0-9.nan+-]+)/
      );
      if (rowMatch) {
        currentGroup.rows.push({
          label: rowMatch[1].trim(),
          n: rowMatch[2],
          sharpe: parseFloat(rowMatch[3]),
          cagr: rowMatch[4].includes('%') ? rowMatch[4] : `${rowMatch[4]}%`,
          maxdd: rowMatch[5].includes('%') ? rowMatch[5] : `${rowMatch[5]}%`,
          calmar: rowMatch[6],
          wfcv: rowMatch[7],
        });
      }
    }
  }

  if (groups.length === 0) return <PreFallback body={body} />;

  // Find overall Sharpe range for color gradient
  const allSharpes = groups.flatMap(g => g.rows.map(r => r.sharpe)).filter(Number.isFinite);
  const minSharpe = Math.min(...allSharpes);
  const maxSharpe = Math.max(...allSharpes);
  const sharpeRange = maxSharpe - minSharpe || 1;

  function sharpeColor(s: number): string {
    const t = (s - minSharpe) / sharpeRange;
    if (t >= 0.66) return 'var(--green)';
    if (t >= 0.33) return 'var(--orange)';
    return 'var(--red)';
  }

  // Derive verdict styling
  const isHighSensitivity = /HIGH/i.test(consistencyVerdict);
  const isModerate = /MODERATE/i.test(consistencyVerdict);
  const verdictColor = isHighSensitivity ? 'var(--red)' : isModerate ? 'var(--orange)' : 'var(--green)';
  const verdictBg = isHighSensitivity ? 'rgba(255,60,60,0.05)' : isModerate ? 'rgba(255,160,60,0.05)' : 'rgba(60,255,100,0.05)';
  const verdictBorder = isHighSensitivity ? 'rgba(255,60,60,0.3)' : isModerate ? 'rgba(255,160,60,0.3)' : 'rgba(60,255,100,0.3)';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Consistency summary banner (merged from REGIME CONSISTENCY SUMMARY) */}
      {consistencyVerdict && (
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <div style={{
            background: verdictBg,
            border: `1px solid ${verdictBorder}`,
            borderRadius: 6,
            padding: '10px 16px',
            display: 'flex',
            flexDirection: 'column',
            gap: 3,
            flex: '1 1 160px',
          }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Verdict</span>
            <span style={{ fontSize: 14, color: verdictColor, fontWeight: 600, ...MONO }}>{consistencyVerdict}</span>
            {consistencyVerdictDetail && (
              <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{consistencyVerdictDetail}</span>
            )}
          </div>
          {consistencySharpeRange && (
            <div style={{
              background: 'var(--bg2)',
              border: '1px solid var(--line1)',
              borderRadius: 6,
              padding: '10px 16px',
              display: 'flex',
              flexDirection: 'column',
              gap: 3,
              flex: '1 1 140px',
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Sharpe Range</span>
              <span style={{ fontSize: 16, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{consistencySharpeRange}</span>
            </div>
          )}
          {consistencySpread && (
            <div style={{
              background: 'var(--bg2)',
              border: '1px solid var(--line1)',
              borderRadius: 6,
              padding: '10px 16px',
              display: 'flex',
              flexDirection: 'column',
              gap: 3,
              flex: '1 1 100px',
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Spread</span>
              <span style={{ fontSize: 16, color: parseFloat(consistencySpread) > 2 ? 'var(--red)' : parseFloat(consistencySpread) > 1 ? 'var(--orange)' : 'var(--green)', fontWeight: 600, ...MONO }}>{consistencySpread}</span>
            </div>
          )}
        </div>
      )}

      {/* Taxonomy thresholds */}
      {thresholds.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2, padding: '8px 12px', background: 'var(--bg2)', borderRadius: 6, border: '1px solid var(--line1)' }}>
          {thresholds.map((th, i) => (
            <div key={i} style={{ fontSize: 9.5, color: 'var(--t3)', ...MONO }}>{th}</div>
          ))}
        </div>
      )}

      {/* Regime groups */}
      {groups.map((g, gIdx) => (
        <div key={gIdx} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontSize: 10, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO, borderBottom: '1px solid var(--line1)', paddingBottom: 4 }}>
            <span style={{ color: 'var(--t4)', marginRight: 6 }}>[{g.tag}]</span>{g.title}
          </div>

          {/* Column header */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: '2fr 50px 70px 90px 80px 70px 60px',
            gap: 8,
            padding: '4px 12px',
          }}>
            {['Regime', 'Days', 'Sharpe', 'CAGR', 'MaxDD', 'Calmar', 'WF CV'].map((h, i) => (
              <span key={i} style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: i >= 1 ? 'right' : 'left', ...MONO }}>{h}</span>
            ))}
          </div>

          {g.rows.map((r, idx) => {
            const sc = sharpeColor(r.sharpe);
            const maxddNum = parseFloat(r.maxdd.replace('%', ''));

            return (
              <div key={idx} style={{
                display: 'grid',
                gridTemplateColumns: '2fr 50px 70px 90px 80px 70px 60px',
                gap: 8,
                padding: '7px 12px',
                background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
                borderRadius: 4,
                borderLeft: `3px solid ${sc}`,
                ...MONO,
              }}>
                <span style={{ fontSize: 10.5, color: 'var(--t2)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.label}</span>
                <span style={{ fontSize: 11, color: 'var(--t2)', textAlign: 'right' }}>{r.n}</span>
                <span style={{ fontSize: 12, color: sc, textAlign: 'right', fontWeight: 600 }}>{r.sharpe.toFixed(3)}</span>
                <span style={{ fontSize: 11, color: 'var(--t1)', textAlign: 'right' }}>{r.cagr}</span>
                <span style={{ fontSize: 11, color: maxddNum <= -30 ? 'var(--red)' : maxddNum <= -15 ? 'var(--orange)' : 'var(--t1)', textAlign: 'right' }}>{r.maxdd}</span>
                <span style={{ fontSize: 11, color: 'var(--t2)', textAlign: 'right' }}>{r.calmar}</span>
                <span style={{ fontSize: 11, color: r.wfcv === 'nan' ? 'var(--t4)' : 'var(--t2)', textAlign: 'right' }}>{r.wfcv === 'nan' ? '—' : r.wfcv}</span>
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

// ── IS vs OOS variant (box-drawn format) ─────────────────────────────────────
function renderRegimeISvsOOS(body: string) {
  const lines = body.split('\n');

  let subtitle = '';
  const metricRows: { metric: string; inSample: string; outOfSample: string }[] = [];
  const verdicts: { label: string; value: string; status: 'pass' | 'warn' | 'fail'; note: string }[] = [];

  let inStabilitySection = false;

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─═]{5,}/.test(line)) continue;
    if (/REGIME ROBUSTNESS/i.test(line)) continue;
    if (/Metric\s+IN-SAMPLE/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    const subMatch = clean.match(/^First\s+\d+\s+days.*vs.*remaining/i);
    if (subMatch) {
      subtitle = clean;
      continue;
    }

    if (/STABILITY SUMMARY/i.test(clean)) {
      inStabilitySection = true;
      continue;
    }

    if (inStabilitySection) {
      const verdictMatch = clean.match(/^(.+?):\s+([0-9.+%-]+)\s+(✅|⚠|❌)\s*(.*)/);
      if (verdictMatch) {
        const statusIcon = verdictMatch[3];
        const status: 'pass' | 'warn' | 'fail' = statusIcon === '✅' ? 'pass' : statusIcon === '⚠' ? 'warn' : 'fail';
        let note = verdictMatch[4].trim();
        const extraMatch = note.match(/^([A-Z]+)\s+(.*)$/);
        if (extraMatch) {
          note = `${extraMatch[1]} ${extraMatch[2]}`.trim();
        }
        verdicts.push({ label: verdictMatch[1].trim(), value: verdictMatch[2].trim(), status, note });
        continue;
      }
    }

    if (!inStabilitySection) {
      const parts = clean.split(/\s{2,}/);
      if (parts.length >= 3) {
        metricRows.push({
          metric: parts[0].trim(),
          inSample: parts[1].trim(),
          outOfSample: parts[2].trim(),
        });
      }
    }
  }

  if (metricRows.length === 0 && verdicts.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {subtitle && (
        <div style={{ fontSize: 10.5, color: 'var(--t3)', ...MONO, padding: '6px 12px', background: 'var(--bg2)', borderRadius: 6, border: '1px solid var(--line1)' }}>
          {subtitle}
        </div>
      )}

      {metricRows.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <div style={{
            display: 'grid',
            gridTemplateColumns: '1.4fr 1fr 1fr',
            gap: 12,
            padding: '6px 12px',
            borderBottom: '1px solid var(--line1)',
          }}>
            <span style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Metric</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, textAlign: 'right', ...MONO }}>In-Sample</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, textAlign: 'right', ...MONO }}>Out-of-Sample</span>
          </div>

          {metricRows.map((r, idx) => {
            let oosColor = 'var(--t1)';
            const isNum = parseFloat(r.inSample.replace(/[x%,]/g, ''));
            const oosNum = parseFloat(r.outOfSample.replace(/[x%,]/g, ''));

            if (Number.isFinite(isNum) && Number.isFinite(oosNum)) {
              const metricLower = r.metric.toLowerCase();
              if (metricLower.includes('sharpe') || metricLower.includes('cagr') || metricLower.includes('final mult') || metricLower.includes('mean return') || metricLower.includes('best day')) {
                oosColor = oosNum >= isNum ? 'var(--green)' : 'var(--red)';
              } else if (metricLower.includes('maxdd') || metricLower.includes('worst day') || metricLower.includes('std return')) {
                oosColor = Math.abs(oosNum) <= Math.abs(isNum) ? 'var(--green)' : 'var(--red)';
              }
            }

            return (
              <div key={idx} style={{
                display: 'grid',
                gridTemplateColumns: '1.4fr 1fr 1fr',
                gap: 12,
                padding: '8px 12px',
                background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
                borderRadius: 4,
                ...MONO,
              }}>
                <span style={{ fontSize: 11, color: 'var(--t3)' }}>{r.metric}</span>
                <span style={{ fontSize: 12, color: 'var(--t1)', textAlign: 'right', fontWeight: 500 }}>{r.inSample}</span>
                <span style={{ fontSize: 12, color: oosColor, textAlign: 'right', fontWeight: 500 }}>{r.outOfSample}</span>
              </div>
            );
          })}
        </div>
      )}

      {verdicts.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          <div style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO, paddingBottom: 2 }}>
            Stability Summary
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 10 }}>
            {verdicts.map((v, idx) => {
              const color = v.status === 'pass' ? 'var(--green)' : v.status === 'warn' ? 'var(--orange)' : 'var(--red)';
              const bgAlpha = v.status === 'pass' ? 'rgba(60,255,100,0.05)' : v.status === 'warn' ? 'rgba(255,160,60,0.05)' : 'rgba(255,60,60,0.05)';
              const borderAlpha = v.status === 'pass' ? 'rgba(60,255,100,0.25)' : v.status === 'warn' ? 'rgba(255,160,60,0.25)' : 'rgba(255,60,60,0.25)';

              return (
                <div key={idx} style={{
                  background: bgAlpha,
                  border: `1px solid ${borderAlpha}`,
                  borderRadius: 6,
                  padding: '10px 14px',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: 4,
                }}>
                  <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>{v.label}</span>
                  <span style={{ fontSize: 18, color, fontWeight: 600, ...MONO }}>{v.value}</span>
                  {v.note && (
                    <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{v.note.replace(/[()]/g, '').trim()}</span>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}
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

function renderSlippageSweep(body: string) {
  const lines = body.split('\n');

  // Metadata
  let activeDays = '';
  let takerFee = '';
  let scalabilityNote = '';

  // Sweep rows
  type SlippageRow = { slippage: string; rtCost: string; sharpe: number; vsBase: string; cagr: string; maxdd: string; calmar: string; grade: string; isBaseline: boolean };
  const rows: SlippageRow[] = [];

  // Break-even
  const breakEvens: { threshold: string; maxSlippage: string }[] = [];

  for (const line of lines) {
    if (/^[═]{5,}$/.test(line.trim())) continue;
    if (/[─]{5,}/.test(line)) continue;
    if (/SLIPPAGE IMPACT/i.test(line)) continue;
    if (/saved:/i.test(line)) continue;
    if (/^Slippage\s+RT Cost/i.test(line.replace(/^[│\s]+/, ''))) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "Active days : 113 / 398  (28.4%)"
    const adMatch = clean.match(/Active days\s*:\s*(.+)/i);
    if (adMatch) { activeDays = adMatch[1].trim(); continue; }

    // "Taker fee already baked in: 0.080% per side (0.160% round-trip)"
    const tfMatch = clean.match(/Taker fee.*?:\s*(.+)/i);
    if (tfMatch) { takerFee = tfMatch[1].trim(); continue; }

    // "Scalability bar: Sharpe ≥ 2.0 @ 0.25% → institutional-grade"
    const scMatch = clean.match(/Scalability bar:\s*(.+)/i);
    if (scMatch) { scalabilityNote = scMatch[1].trim(); continue; }

    // "Slippage below is ADDITIONAL..." — skip
    if (/slippage below/i.test(clean)) continue;

    // Baseline row: "Baseline     0.000%     3.462    +0.000    5914.8%    -37.95%    155.85  (taker fee...)"
    const blMatch = clean.match(/^Baseline\s+([0-9.]+%)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%?)\s+([0-9.+-]+%?)\s+([0-9.+-]+)\s+(.*)/i);
    if (blMatch) {
      rows.push({
        slippage: 'Baseline', rtCost: blMatch[1], sharpe: parseFloat(blMatch[2]),
        vsBase: blMatch[3], cagr: blMatch[4], maxdd: blMatch[5], calmar: blMatch[6],
        grade: blMatch[7].replace(/[()]/g, '').trim(), isBaseline: true,
      });
      continue;
    }

    // Data rows: "0.050%     0.100%     3.397    -0.065    5339.7%    -38.23%    139.66  Excellent"
    const rowMatch = clean.match(
      /^([0-9.]+%)\s+([0-9.]+%)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%?)\s+([0-9.+-]+%?)\s+([0-9.+-]+)\s+(.*)/
    );
    if (rowMatch) {
      rows.push({
        slippage: rowMatch[1], rtCost: rowMatch[2], sharpe: parseFloat(rowMatch[3]),
        vsBase: rowMatch[4], cagr: rowMatch[5], maxdd: rowMatch[6], calmar: rowMatch[7],
        grade: rowMatch[8].trim(), isBaseline: false,
      });
      continue;
    }

    // Break-even: "Sharpe ≥ 2.5 : survives up to 0.500% one-way slippage"
    const beMatch = clean.match(/Sharpe\s*(≥|>=)\s*([0-9.]+)\s*:\s*survives up to\s*([0-9.]+%)/i);
    if (beMatch) {
      breakEvens.push({ threshold: `Sharpe ≥ ${beMatch[2]}`, maxSlippage: beMatch[3] });
      continue;
    }
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  // Sort: baseline first, then by slippage ascending
  const baseline = rows.find(r => r.isBaseline);
  const dataRows = rows.filter(r => !r.isBaseline).sort((a, b) => parseFloat(a.slippage) - parseFloat(b.slippage));

  function gradeColor(grade: string): string {
    if (/excellent/i.test(grade)) return 'var(--green)';
    if (/strong/i.test(grade)) return '#5bc0de';
    if (/adequate|moderate/i.test(grade)) return 'var(--orange)';
    return 'var(--red)';
  }

  // Find the scalable row (marked with ✓)
  const scalableIdx = dataRows.findIndex(r => /scalable/i.test(r.grade));

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Metadata bar */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {baseline && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baseline.sharpe.toFixed(3)}</span>
          </div>
        )}
        {activeDays && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 130px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Active Days</span>
            <span style={{ fontSize: 14, color: 'var(--t1)', fontWeight: 500, ...MONO }}>{activeDays}</span>
          </div>
        )}
        {takerFee && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1.5 1 180px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Taker Fee (baked in)</span>
            <span style={{ fontSize: 11, color: 'var(--t2)', ...MONO }}>{takerFee}</span>
          </div>
        )}
      </div>

      {/* Scalability note */}
      {scalabilityNote && (
        <div style={{
          fontSize: 10, color: 'var(--t3)', ...MONO, padding: '6px 12px',
          background: 'rgba(60,255,100,0.03)', borderRadius: 6, border: '1px solid rgba(60,255,100,0.15)',
        }}>
          Scalability: {scalabilityNote}
        </div>
      )}

      {/* Sweep table */}
      <div style={{ display: 'inline-flex', flexDirection: 'column', gap: 4 }}>
        {/* Header */}
        <div style={{
          display: 'grid', gridTemplateColumns: '68px 68px 70px 70px 90px 75px 70px 90px',
          gap: 6, padding: '6px 12px', borderBottom: '1px solid var(--line1)',
        }}>
          {['Slippage', 'RT Cost', 'Sharpe', 'vs Base', 'CAGR', 'MaxDD', 'Calmar', 'Grade'].map((h, i) => (
            <span key={i} style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: i >= 2 ? 'right' : 'left', ...MONO }}>{h}</span>
          ))}
        </div>

        {/* Baseline row */}
        {baseline && (
          <div style={{
            display: 'grid', gridTemplateColumns: '68px 68px 70px 70px 90px 75px 70px 90px',
            gap: 6, padding: '8px 12px', borderRadius: 4,
            background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(255,255,255,0.08)',
            ...MONO,
          }}>
            <span style={{ fontSize: 11, color: '#f0c040', fontWeight: 600 }}>Base</span>
            <span style={{ fontSize: 11, color: 'var(--t3)' }}>{baseline.rtCost}</span>
            <span style={{ fontSize: 12, color: '#f0c040', textAlign: 'right', fontWeight: 600 }}>{baseline.sharpe.toFixed(3)}</span>
            <span style={{ fontSize: 11, color: 'var(--t4)', textAlign: 'right' }}>—</span>
            <span style={{ fontSize: 11, color: 'var(--t1)', textAlign: 'right' }}>{baseline.cagr}</span>
            <span style={{ fontSize: 11, color: 'var(--red)', textAlign: 'right' }}>{baseline.maxdd}</span>
            <span style={{ fontSize: 11, color: 'var(--t2)', textAlign: 'right' }}>{baseline.calmar}</span>
            <span style={{ fontSize: 10, color: 'var(--t4)', textAlign: 'right' }}>{baseline.grade}</span>
          </div>
        )}

        {/* Data rows */}
        {dataRows.map((r, idx) => {
          const vsBaseNum = parseFloat(r.vsBase);
          const isScalable = idx === scalableIdx;
          const gc = gradeColor(r.grade);
          const maxddNum = parseFloat(r.maxdd.replace('%', ''));

          return (
            <div key={idx} style={{
              display: 'grid', gridTemplateColumns: '68px 68px 70px 70px 90px 75px 70px 90px',
              gap: 6, padding: '8px 12px', borderRadius: 4,
              background: isScalable ? 'rgba(60,255,100,0.04)' : idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
              border: isScalable ? '1px solid rgba(60,255,100,0.2)' : '1px solid transparent',
              ...MONO,
            }}>
              <span style={{ fontSize: 11, color: 'var(--t1)', fontWeight: 500 }}>{r.slippage}</span>
              <span style={{ fontSize: 11, color: 'var(--t3)' }}>{r.rtCost}</span>
              <span style={{ fontSize: 12, color: r.sharpe >= 2.0 ? 'var(--green)' : r.sharpe >= 1.5 ? 'var(--orange)' : 'var(--red)', textAlign: 'right', fontWeight: 600 }}>{r.sharpe.toFixed(3)}</span>
              <span style={{ fontSize: 11, color: vsBaseNum >= 0 ? 'var(--green)' : 'var(--red)', textAlign: 'right' }}>{r.vsBase}</span>
              <span style={{ fontSize: 11, color: 'var(--t1)', textAlign: 'right' }}>{r.cagr}</span>
              <span style={{ fontSize: 11, color: maxddNum <= -40 ? 'var(--red)' : maxddNum <= -30 ? 'var(--orange)' : 'var(--t2)', textAlign: 'right' }}>{r.maxdd}</span>
              <span style={{ fontSize: 11, color: 'var(--t2)', textAlign: 'right' }}>{r.calmar}</span>
              <span style={{ fontSize: 10, color: gc, textAlign: 'right', fontWeight: isScalable ? 600 : 400 }}>
                {r.grade.replace(/[✓✗]\s*scalable/i, '').trim()}
                {isScalable && <span style={{ marginLeft: 4, fontSize: 9, color: 'var(--green)' }}>✓ scalable</span>}
              </span>
            </div>
          );
        })}
      </div>

      {/* Sharpe degradation bar */}
      {dataRows.length >= 2 && baseline && (() => {
        const blSharpe = baseline.sharpe;
        const allSharpes = [blSharpe, ...dataRows.map(r => r.sharpe)];
        const minS = Math.min(...allSharpes);
        const maxS = Math.max(...allSharpes);
        // Add 5% padding on each side so edge markers don't clip
        const pad = (maxS - minS) * 0.05 || 0.1;
        const rangeMin = minS - pad;
        const rangeMax = maxS + pad;
        const range = rangeMax - rangeMin || 1;
        const pos = (v: number) => ((v - rangeMin) / range) * 100;

        return (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            <div style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
              Sharpe Degradation
            </div>
            <div style={{
              background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
              padding: '14px 20px 28px', position: 'relative',
            }}>
              <div style={{ position: 'relative', height: 10, background: 'rgba(255,255,255,0.03)', borderRadius: 5, overflow: 'hidden' }}>
                {/* Sharpe ≥ 2.0 threshold zone */}
                <div style={{
                  position: 'absolute', top: 0, bottom: 0, borderRadius: 5,
                  left: `${pos(2.0)}%`,
                  width: `${pos(rangeMax) - pos(2.0)}%`,
                  background: 'rgba(60,255,100,0.06)',
                }} />
                {/* Gradient fill from baseline to worst */}
                <div style={{
                  position: 'absolute', top: 0, bottom: 0, borderRadius: 5,
                  left: `${pos(Math.min(...dataRows.map(r => r.sharpe)))}%`,
                  width: `${pos(blSharpe) - pos(Math.min(...dataRows.map(r => r.sharpe)))}%`,
                  background: 'linear-gradient(to right, rgba(255,60,60,0.2), rgba(60,255,100,0.15))',
                }} />
                {/* Baseline marker */}
                <div style={{ position: 'absolute', top: -3, height: 16, width: 3, borderRadius: 1, background: '#f0c040', left: `${pos(blSharpe)}%` }} />
                {/* Data point markers */}
                {dataRows.map((r, i) => (
                  <div key={i} style={{
                    position: 'absolute', top: 1, width: 8, height: 8, borderRadius: '50%',
                    background: r.sharpe >= 2.0 ? 'var(--green)' : r.sharpe >= 1.5 ? 'var(--orange)' : 'var(--red)',
                    border: '1px solid rgba(0,0,0,0.3)',
                    left: `${pos(r.sharpe)}%`, transform: 'translateX(-4px)',
                  }} />
                ))}
              </div>
              {/* Labels */}
              <div style={{ position: 'relative', height: 34, marginTop: 6, ...MONO }}>
                {dataRows.map((r, i) => (
                  <div key={i} style={{
                    position: 'absolute', left: `${pos(r.sharpe)}%`, transform: 'translateX(-50%)',
                    display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 1,
                  }}>
                    <span style={{ fontSize: 8, color: 'var(--t3)' }}>{r.slippage}</span>
                    <span style={{ fontSize: 8, color: r.sharpe >= 2.0 ? 'var(--green)' : r.sharpe >= 1.5 ? 'var(--orange)' : 'var(--red)', fontWeight: 500 }}>{r.sharpe.toFixed(3)}</span>
                  </div>
                ))}
                <div style={{ position: 'absolute', left: `${pos(blSharpe)}%`, transform: 'translateX(-50%)', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 1 }}>
                  <span style={{ fontSize: 8, color: '#f0c040' }}>Base</span>
                  <span style={{ fontSize: 8, color: '#f0c040', fontWeight: 500 }}>{blSharpe.toFixed(3)}</span>
                </div>
              </div>
            </div>
          </div>
        );
      })()}

      {/* Break-even analysis */}
      {breakEvens.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
            Break-Even Analysis
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 8 }}>
            {breakEvens.map((be, idx) => (
              <div key={idx} style={{
                background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
                padding: '8px 14px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', ...MONO,
              }}>
                <span style={{ fontSize: 10, color: 'var(--t3)' }}>{be.threshold}</span>
                <span style={{ fontSize: 12, color: 'var(--green)', fontWeight: 600 }}>{be.maxSlippage}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function renderNoisePerturbation(body: string) {
  const lines = body.split('\n');

  // Baseline
  let baselineSharpe = '';
  let baselineCAGR = '';
  let baselineMaxDD = '';
  let baselineCalmar = '';
  let trialsPerLevel = '';

  // Mode groups
  type NoiseRow = { level: string; meanSharpe: string; std: string; min: string; meanCAGR: string; meanMaxDD: string; score: number; pass: boolean; worst: string };
  type NoiseMode = { tag: string; title: string; rows: NoiseRow[] };
  const modes: NoiseMode[] = [];
  let currentMode: NoiseMode | null = null;

  // Verdicts
  type NoiseVerdict = { mode: string; verdict: string; pass: boolean; note: string; worst: string };
  const verdicts: NoiseVerdict[] = [];
  let scoreThreshold = '';

  for (const line of lines) {
    if (/^[═]{5,}$/.test(line.trim())) continue;
    if (/NOISE PERTURBATION/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;
    if (/saved:/i.test(clean)) continue;
    if (/^Level\s+Mean Sharpe/i.test(clean)) continue;

    // "Baseline → Sharpe=3.736  CAGR=6484.6%  MaxDD=-29.76%  Calmar=217.89"
    const blMatch = clean.match(/Baseline.*?Sharpe=([0-9.+-]+)\s+CAGR=([0-9.+-]+%?)\s+MaxDD=([0-9.+-]+%?)\s+Calmar=([0-9.+-]+)/i);
    if (blMatch) {
      baselineSharpe = blMatch[1]; baselineCAGR = blMatch[2]; baselineMaxDD = blMatch[3]; baselineCalmar = blMatch[4];
      continue;
    }

    // "Trials per level: 100"
    const trialMatch = clean.match(/Trials per level:\s*(\d+)/i);
    if (trialMatch) { trialsPerLevel = trialMatch[1]; continue; }

    // Mode headers: "── Mode A: Return Noise (0.1%|0.3%|0.5%|1.0% daily vol) ──"
    const modeMatch = clean.match(/Mode\s+([A-Z]):\s*(.+?)(?:\s*──|$)/i);
    if (modeMatch) {
      currentMode = { tag: modeMatch[1], title: modeMatch[2].replace(/[───]+$/, '').trim(), rows: [] };
      modes.push(currentMode);
      continue;
    }

    // Data rows: "0.1%         5.646    0.000    5.646    22134.4%     -10.93%   1.511 ✓   1.511"
    if (currentMode) {
      const rowMatch = clean.match(
        /^([0-9.]+%)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%?)\s+([0-9.+-]+%?)\s+([0-9.+-]+)\s*(✓|✗)?\s+([0-9.+-]+)/
      );
      if (rowMatch) {
        currentMode.rows.push({
          level: rowMatch[1],
          meanSharpe: rowMatch[2],
          std: rowMatch[3],
          min: rowMatch[4],
          meanCAGR: rowMatch[5],
          meanMaxDD: rowMatch[6],
          score: parseFloat(rowMatch[7]),
          pass: rowMatch[8] !== '✗',
          worst: rowMatch[9],
        });
        continue;
      }
    }

    // Verdict lines: "Return noise verdict  : ROBUST       ✓  (all levels ≥ 0.85)  [worst=1.511]"
    const vMatch = clean.match(/^(.+?)\s+verdict\s*:\s*(\S+)\s*(✓|✗)\s*\((.+?)\)\s*\[worst=([0-9.+-]+)\]/i);
    if (vMatch) {
      currentMode = null;
      verdicts.push({
        mode: vMatch[1].trim(),
        verdict: vMatch[2].trim(),
        pass: vMatch[3] === '✓',
        note: vMatch[4].trim(),
        worst: vMatch[5],
      });
      continue;
    }

    // "Score threshold       : ≥0.85 = robust  |  0.70–0.84 = marginal  |  <0.70 = fragile"
    const threshMatch = clean.match(/Score threshold\s*:\s*(.+)/i);
    if (threshMatch) { scoreThreshold = threshMatch[1].trim(); continue; }
  }

  if (modes.length === 0 && verdicts.length === 0) return <PreFallback body={body} />;

  function scoreColor(s: number): string {
    if (s >= 0.85) return 'var(--green)';
    if (s >= 0.70) return 'var(--orange)';
    return 'var(--red)';
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Baseline + trials */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {baselineSharpe && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 100px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineSharpe}</span>
          </div>
        )}
        {baselineCAGR && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 100px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>CAGR</span>
            <span style={{ fontSize: 16, color: 'var(--green)', fontWeight: 600, ...MONO }}>{baselineCAGR}</span>
          </div>
        )}
        {baselineMaxDD && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 100px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>MaxDD</span>
            <span style={{ fontSize: 16, color: 'var(--red)', fontWeight: 600, ...MONO }}>{baselineMaxDD}</span>
          </div>
        )}
        {baselineCalmar && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 80px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Calmar</span>
            <span style={{ fontSize: 16, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineCalmar}</span>
          </div>
        )}
        {trialsPerLevel && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 80px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Trials / Level</span>
            <span style={{ fontSize: 16, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{trialsPerLevel}</span>
          </div>
        )}
      </div>

      {/* Verdict cards */}
      {verdicts.length > 0 && (
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {verdicts.map((v, idx) => {
            const isRobust = /ROBUST/i.test(v.verdict);
            const isMarginal = /MARGINAL/i.test(v.verdict);
            const color = isRobust ? 'var(--green)' : isMarginal ? 'var(--orange)' : 'var(--red)';
            const bg = isRobust ? 'rgba(60,255,100,0.05)' : isMarginal ? 'rgba(255,160,60,0.05)' : 'rgba(255,60,60,0.05)';
            const border = isRobust ? 'rgba(60,255,100,0.25)' : isMarginal ? 'rgba(255,160,60,0.25)' : 'rgba(255,60,60,0.25)';

            return (
              <div key={idx} style={{
                flex: '1 1 180px', background: bg, border: `1px solid ${border}`, borderRadius: 6,
                padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 4,
              }}>
                <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>{v.mode}</span>
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                  <span style={{ fontSize: 15, color, fontWeight: 600, ...MONO }}>{v.verdict}</span>
                  <span style={{ fontSize: 10, color: 'var(--t3)', ...MONO }}>worst: {v.worst}</span>
                </div>
                <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{v.note}</span>
              </div>
            );
          })}
        </div>
      )}

      {/* Mode tables */}
      {modes.map((m, mIdx) => (
        <div key={mIdx} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontSize: 10, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO, borderBottom: '1px solid var(--line1)', paddingBottom: 4 }}>
            <span style={{ color: 'var(--t4)', marginRight: 6 }}>[{m.tag}]</span>{m.title}
          </div>

          {/* Header */}
          <div style={{
            display: 'grid', gridTemplateColumns: '55px 1fr 60px 60px 90px 75px 65px',
            gap: 6, padding: '4px 12px',
          }}>
            {['Level', 'Mean SR', 'Std', 'Min', 'Mean CAGR', 'MaxDD', 'Score'].map((h, i) => (
              <span key={i} style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: i >= 1 ? 'right' : 'left', ...MONO }}>{h}</span>
            ))}
          </div>

          {m.rows.map((r, idx) => {
            const sc = scoreColor(r.score);
            const maxddNum = parseFloat(r.meanMaxDD.replace('%', ''));

            return (
              <div key={idx} style={{
                display: 'grid', gridTemplateColumns: '55px 1fr 60px 60px 90px 75px 65px',
                gap: 6, padding: '7px 12px',
                background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
                borderRadius: 4,
                borderLeft: `3px solid ${sc}`,
                ...MONO,
              }}>
                <span style={{ fontSize: 11, color: 'var(--t1)', fontWeight: 600 }}>{r.level}</span>
                <span style={{ fontSize: 12, color: 'var(--t1)', textAlign: 'right', fontWeight: 500 }}>{r.meanSharpe}</span>
                <span style={{ fontSize: 11, color: 'var(--t3)', textAlign: 'right' }}>{r.std}</span>
                <span style={{ fontSize: 11, color: parseFloat(r.min) < parseFloat(baselineSharpe) * 0.7 ? 'var(--red)' : 'var(--t2)', textAlign: 'right' }}>{r.min}</span>
                <span style={{ fontSize: 11, color: 'var(--t2)', textAlign: 'right' }}>{r.meanCAGR}</span>
                <span style={{ fontSize: 11, color: maxddNum <= -30 ? 'var(--red)' : maxddNum <= -15 ? 'var(--orange)' : 'var(--t2)', textAlign: 'right' }}>{r.meanMaxDD}</span>
                <span style={{
                  fontSize: 11, fontWeight: 600, textAlign: 'right', color: sc,
                }}>{r.score.toFixed(3)}</span>
              </div>
            );
          })}
        </div>
      ))}

      {/* Threshold legend */}
      {scoreThreshold && (
        <div style={{ fontSize: 9, color: 'var(--t4)', ...MONO, padding: '6px 12px', background: 'var(--bg2)', borderRadius: 6, border: '1px solid var(--line1)' }}>
          {scoreThreshold}
        </div>
      )}
    </div>
  );
}


function renderRegimeConditional(body: string) {
  const lines = body.split('\n');

  type DayStat = { label: string; count: string; mean: string; sharpe: string };
  const dayStats: DayStat[] = [];

  type VolRegime = { label: string; mean: string; sharpe: string };
  const volRegimes: VolRegime[] = [];
  let volThreshold = '';

  type RollingMetric = { label: string; min: string; med: string; max: string };
  const rollingMetrics: RollingMetric[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/REGIME.*CONDITIONAL/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "Up days:     329   mean=  3.33%  Sharpe=  7.71"
    const dayMatch = clean.match(/^(Up|Down) days:\s+(\d+)\s+mean=\s*([0-9.+-]+%?)\s+Sharpe=\s*([0-9.+-]+)/i);
    if (dayMatch) {
      dayStats.push({ label: `${dayMatch[1]} days`, count: dayMatch[2], mean: dayMatch[3], sharpe: dayMatch[4] });
      continue;
    }

    // "Low-vol regime:   mean=  0.34%  Sharpe=  1.05"
    const volMatch = clean.match(/^(Low-vol|High-vol)\s+regime:\s+mean=\s*([0-9.+-]+%?)\s+Sharpe=\s*([0-9.+-]+)/i);
    if (volMatch) {
      volRegimes.push({ label: volMatch[1], mean: volMatch[2], sharpe: volMatch[3] });
      continue;
    }

    // "Vol split threshold: 8.5411% daily vol"
    const vtMatch = clean.match(/Vol split threshold:\s*(.+)/i);
    if (vtMatch) { volThreshold = vtMatch[1].trim(); continue; }

    // "Rolling 60d Sharpe:  min=-1.51  med=3.00  max=5.78"
    const rollMatch = clean.match(/Rolling\s+(\d+d\s+\S+):\s+min=([0-9.+-]+%?)\s+med=([0-9.+-]+%?)\s+max=([0-9.+-]+%?)/i);
    if (rollMatch) {
      rollingMetrics.push({ label: `Rolling ${rollMatch[1]}`, min: rollMatch[2], med: rollMatch[3], max: rollMatch[4] });
      continue;
    }
  }

  if (dayStats.length === 0 && volRegimes.length === 0 && rollingMetrics.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Up/Down day stats */}
      {dayStats.length > 0 && (
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {dayStats.map((d, idx) => {
            const isUp = /up/i.test(d.label);
            const color = isUp ? 'var(--green)' : 'var(--red)';
            const bg = isUp ? 'rgba(60,255,100,0.04)' : 'rgba(255,60,60,0.04)';
            const border = isUp ? 'rgba(60,255,100,0.2)' : 'rgba(255,60,60,0.2)';

            return (
              <div key={idx} style={{
                flex: '1 1 200px', background: bg, border: `1px solid ${border}`, borderRadius: 6,
                padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 8,
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={{ fontSize: 12, color, fontWeight: 600, ...MONO }}>{d.label}</span>
                  <span style={{ fontSize: 14, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{d.count}</span>
                </div>
                <div style={{ display: 'flex', gap: 16, ...MONO }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>Mean</span>
                    <span style={{ fontSize: 13, color, fontWeight: 500 }}>{d.mean}</span>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>Sharpe</span>
                    <span style={{ fontSize: 13, color: 'var(--t1)', fontWeight: 500 }}>{d.sharpe}</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Volatility regimes */}
      {volRegimes.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Volatility Regimes</span>
            {volThreshold && <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>threshold: {volThreshold}</span>}
          </div>
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            {volRegimes.map((v, idx) => {
              const sharpeNum = parseFloat(v.sharpe);
              const sharpeColor = sharpeNum >= 2.0 ? 'var(--green)' : sharpeNum >= 1.0 ? 'var(--orange)' : 'var(--red)';

              return (
                <div key={idx} style={{
                  flex: '1 1 160px', background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
                  padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 6,
                }}>
                  <span style={{ fontSize: 10, color: 'var(--t3)', fontWeight: 500, ...MONO }}>{v.label}</span>
                  <div style={{ display: 'flex', gap: 16, ...MONO }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                      <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>Mean</span>
                      <span style={{ fontSize: 14, color: parseFloat(v.mean) >= 0 ? 'var(--green)' : 'var(--red)', fontWeight: 500 }}>{v.mean}</span>
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                      <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>Sharpe</span>
                      <span style={{ fontSize: 14, color: sharpeColor, fontWeight: 600 }}>{v.sharpe}</span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Rolling metrics */}
      {rollingMetrics.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <span style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Rolling Statistics</span>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {rollingMetrics.map((r, idx) => (
              <div key={idx} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 14px', borderRadius: 4,
                background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
                ...MONO,
              }}>
                <span style={{ fontSize: 10, color: 'var(--t3)' }}>{r.label}</span>
                <div style={{ display: 'flex', gap: 16 }}>
                  <div style={{ display: 'flex', gap: 4, alignItems: 'baseline' }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)' }}>min</span>
                    <span style={{ fontSize: 12, color: 'var(--red)', fontWeight: 500 }}>{r.min}</span>
                  </div>
                  <div style={{ display: 'flex', gap: 4, alignItems: 'baseline' }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)' }}>med</span>
                    <span style={{ fontSize: 12, color: 'var(--t1)', fontWeight: 500 }}>{r.med}</span>
                  </div>
                  <div style={{ display: 'flex', gap: 4, alignItems: 'baseline' }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)' }}>max</span>
                    <span style={{ fontSize: 12, color: 'var(--green)', fontWeight: 500 }}>{r.max}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function renderTailRiskExtended(body: string) {
  const lines = body.split('\n');

  type TailItem = { label: string; value: string };
  const items: TailItem[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/TAIL RISK/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    const match = clean.match(/^(.+?):\s+(.+)$/);
    if (match) {
      items.push({ label: match[1].trim(), value: match[2].trim() });
    }
  }

  if (items.length === 0) return <PreFallback body={body} />;

  function valueColor(label: string, value: string): string {
    const num = parseFloat(value.replace(/[,%]/g, ''));
    const k = label.toLowerCase();
    if (k.includes('cvar')) return num <= -30 ? 'var(--red)' : num <= -15 ? 'var(--orange)' : 'var(--green)';
    if (k.includes('consec') && k.includes('losing')) return num >= 5 ? 'var(--red)' : num >= 3 ? 'var(--orange)' : 'var(--green)';
    return 'var(--t1)';
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {items.map((item, idx) => {
          const color = valueColor(item.label, item.value);
          const isCvar = /cvar/i.test(item.label);
          const bg = isCvar && color === 'var(--red)' ? 'rgba(255,60,60,0.05)' : 'var(--bg2)';
          const border = isCvar && color === 'var(--red)' ? 'rgba(255,60,60,0.25)' : 'var(--line1)';

          return (
            <div key={idx} style={{
              flex: '1 1 140px', background: bg, border: `1px solid ${border}`, borderRadius: 6,
              padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3,
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>{item.label}</span>
              <span style={{ fontSize: 18, color, fontWeight: 600, ...MONO }}>{item.value}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderStatisticalValidity(body: string) {
  const lines = body.split('\n');

  type StatItem = { label: string; value: string; status: 'pass' | 'fail' | 'none' };
  const items: StatItem[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/STATISTICAL VALIDITY/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Match "Label:  value  ✅ PASS" or "Label:  value  ✅" or "Label:  value"
    const match = clean.match(/^(.+?):\s+(.+?)(?:\s+(✅|❌)\s*(.*?))?$/);
    if (match) {
      const label = match[1].trim();
      let value = match[2].trim();
      const icon = match[3];
      const suffix = match[4]?.trim() ?? '';

      // Clean up value — remove trailing icon if captured as part of value
      value = value.replace(/[✅❌]\s*(?:PASS|FAIL|YES|NO)?\s*$/, '').trim();

      let status: 'pass' | 'fail' | 'none' = 'none';
      if (icon === '✅') status = 'pass';
      else if (icon === '❌') status = 'fail';

      // Append suffix to value if present (e.g. "PASS", "YES")
      if (suffix) value = `${value}  ${suffix}`;

      items.push({ label, value, status });
    }
  }

  if (items.length === 0) return <PreFallback body={body} />;

  // Separate headline metrics from supporting details
  const headlines = items.filter(i =>
    /sharpe|DSR.*prob|DSR.*genuine|profit factor/i.test(i.label)
  );
  const details = items.filter(i => !headlines.includes(i));

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Headline cards */}
      {headlines.length > 0 && (
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {headlines.map((item, idx) => {
            const color = item.status === 'pass' ? 'var(--green)' : item.status === 'fail' ? 'var(--red)' : 'var(--t1)';
            const bg = item.status === 'pass' ? 'rgba(60,255,100,0.05)' : item.status === 'fail' ? 'rgba(255,60,60,0.05)' : 'var(--bg2)';
            const border = item.status === 'pass' ? 'rgba(60,255,100,0.25)' : item.status === 'fail' ? 'rgba(255,60,60,0.25)' : 'var(--line1)';

            return (
              <div key={idx} style={{
                flex: '1 1 140px', background: bg, border: `1px solid ${border}`, borderRadius: 6,
                padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3,
              }}>
                <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>{item.label}</span>
                <span style={{ fontSize: 18, color, fontWeight: 600, ...MONO }}>{item.value}</span>
              </div>
            );
          })}
        </div>
      )}

      {/* Detail rows */}
      {details.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {details.map((item, idx) => {
            const color = item.status === 'pass' ? 'var(--green)' : item.status === 'fail' ? 'var(--red)' : 'var(--t1)';

            return (
              <div key={idx} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '6px 12px', borderRadius: 4,
                background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
                ...MONO,
              }}>
                <span style={{ fontSize: 10, color: 'var(--t3)' }}>{item.label}</span>
                <span style={{ fontSize: 12, color, fontWeight: 500 }}>{item.value}</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function renderMarketCapUniverse(body: string) {
  const lines = body.split('\n');

  type KVItem = { label: string; value: string };
  const kvs: KVItem[] = [];
  const warnings: string[] = [];

  for (const line of lines) {
    if (/^[═]{5,}$/.test(line.trim())) continue;
    if (/MARKET CAP UNIVERSE/i.test(line)) continue;
    if (/saved:/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Warning lines: "⚠  36 symbol(s) missing..." or "⚠  LOW match rate..."
    if (/^⚠/.test(clean)) {
      warnings.push(clean.replace(/^⚠\s*/, '').trim());
      continue;
    }

    // KV lines: "Symbol coverage   : 93.4%  (509 / 545 unique symbols matched in parquet)"
    const kvMatch = clean.match(/^(.+?)\s*:\s+(.+)/);
    if (kvMatch) {
      kvs.push({ label: kvMatch[1].trim(), value: kvMatch[2].trim() });
    }
  }

  if (kvs.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {kvs.map((kv, idx) => {
          const isRate = /rate|coverage/i.test(kv.label);
          const num = parseFloat(kv.value);
          let color = 'var(--t1)';
          if (isRate && Number.isFinite(num)) {
            color = num >= 90 ? 'var(--green)' : num >= 70 ? 'var(--orange)' : 'var(--red)';
          }

          return (
            <div key={idx} style={{
              flex: '1 1 180px', background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
              padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 3,
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>{kv.label}</span>
              <span style={{ fontSize: 14, color, fontWeight: 600, ...MONO }}>{kv.value.split('(')[0].trim()}</span>
              {kv.value.includes('(') && (
                <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{kv.value.slice(kv.value.indexOf('('))}</span>
              )}
            </div>
          );
        })}
      </div>

      {warnings.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {warnings.map((w, idx) => (
            <div key={idx} style={{
              padding: '6px 12px', borderRadius: 4,
              background: 'rgba(255,160,60,0.04)', border: '1px solid rgba(255,160,60,0.15)',
              fontSize: 9.5, color: 'var(--orange)', ...MONO, lineHeight: 1.5,
            }}>
              ⚠ {w}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function renderCapitalOperational(body: string) {
  const lines = body.split('\n');

  let fullKelly = '';
  let fullKellyPct = '';
  let halfKelly = '';
  let halfKellyPct = '';
  let ruinProb = '';

  type LevRow = { leverage: string; cagr: string; sharpe: string; maxdd: string };
  const levRows: LevRow[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─]{5,}/.test(line)) continue;
    if (/CAPITAL.*OPERATIONAL/i.test(line)) continue;
    if (/LEVERAGE SENSITIVITY/i.test(line)) continue;
    if (/Leverage\s+CAGR/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "Full Kelly fraction:      1.6275  (162.75% of capital per day)"
    const fkMatch = clean.match(/Full Kelly fraction:\s*([0-9.+-]+)\s*\((.+?)\)/i);
    if (fkMatch) { fullKelly = fkMatch[1]; fullKellyPct = fkMatch[2].trim(); continue; }

    // "Half Kelly fraction:      0.8137  (81.37% of capital per day)"
    const hkMatch = clean.match(/Half Kelly fraction:\s*([0-9.+-]+)\s*\((.+?)\)/i);
    if (hkMatch) { halfKelly = hkMatch[1]; halfKellyPct = hkMatch[2].trim(); continue; }

    // "Ruin probability (50% DD in 365d): 71.2600%"
    const rpMatch = clean.match(/Ruin probability.*?:\s*([0-9.]+%)/i);
    if (rpMatch) { ruinProb = rpMatch[1]; continue; }

    // Leverage rows: "0.50x    699.62     2.818   -32.73%"
    const levMatch = clean.match(/^([0-9.]+x)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%)/);
    if (levMatch) {
      levRows.push({ leverage: levMatch[1], cagr: levMatch[2], sharpe: levMatch[3], maxdd: levMatch[4] });
      continue;
    }
  }

  if (!fullKelly && levRows.length === 0) return <PreFallback body={body} />;

  const ruinNum = parseFloat(ruinProb);
  const ruinColor = ruinNum <= 10 ? 'var(--green)' : ruinNum <= 40 ? 'var(--orange)' : 'var(--red)';
  const ruinBg = ruinNum <= 10 ? 'rgba(60,255,100,0.05)' : ruinNum <= 40 ? 'rgba(255,160,60,0.05)' : 'rgba(255,60,60,0.05)';
  const ruinBorder = ruinNum <= 10 ? 'rgba(60,255,100,0.25)' : ruinNum <= 40 ? 'rgba(255,160,60,0.25)' : 'rgba(255,60,60,0.25)';

  // Find the 1.00x row for baseline reference
  const baseRow = levRows.find(r => r.leverage === '1.00x');

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Kelly + Ruin cards */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {fullKelly && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 140px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Full Kelly</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{fullKelly}</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{fullKellyPct}</span>
          </div>
        )}
        {halfKelly && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 140px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Half Kelly</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{halfKelly}</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{halfKellyPct}</span>
          </div>
        )}
        {ruinProb && (
          <div style={{
            background: ruinBg, border: `1px solid ${ruinBorder}`, borderRadius: 6,
            padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 160px',
          }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Ruin Probability</span>
            <span style={{ fontSize: 18, color: ruinColor, fontWeight: 600, ...MONO }}>{ruinProb}</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>50% DD in 365d</span>
          </div>
        )}
      </div>

      {/* Leverage sensitivity table */}
      {levRows.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
            Leverage Sensitivity
          </div>
          <div style={{ display: 'inline-flex', flexDirection: 'column', gap: 2 }}>
            {/* Header */}
            <div style={{
              display: 'grid', gridTemplateColumns: '70px 100px 70px 80px',
              gap: 8, padding: '6px 12px', borderBottom: '1px solid var(--line1)',
            }}>
              {['Leverage', 'CAGR %', 'Sharpe', 'MaxDD %'].map((h, i) => (
                <span key={i} style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: i >= 1 ? 'right' : 'left', ...MONO }}>{h}</span>
              ))}
            </div>

            {levRows.map((r, idx) => {
              const isBase = r.leverage === '1.00x';
              const maxddNum = parseFloat(r.maxdd.replace('%', ''));
              const sharpeNum = parseFloat(r.sharpe);

              return (
                <div key={idx} style={{
                  display: 'grid', gridTemplateColumns: '70px 100px 70px 80px',
                  gap: 8, padding: '7px 12px', borderRadius: 4,
                  background: isBase ? 'rgba(255,255,255,0.04)' : idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
                  border: isBase ? '1px solid rgba(255,255,255,0.08)' : '1px solid transparent',
                  ...MONO,
                }}>
                  <span style={{ fontSize: 12, color: isBase ? '#f0c040' : 'var(--t1)', fontWeight: isBase ? 600 : 500 }}>{r.leverage}</span>
                  <span style={{ fontSize: 11, color: 'var(--t1)', textAlign: 'right' }}>{parseFloat(r.cagr).toLocaleString(undefined, { maximumFractionDigits: 2 })}%</span>
                  <span style={{ fontSize: 12, color: sharpeNum >= 2.0 ? 'var(--green)' : sharpeNum >= 1.5 ? 'var(--orange)' : 'var(--red)', textAlign: 'right', fontWeight: 500 }}>{r.sharpe}</span>
                  <span style={{ fontSize: 11, color: maxddNum <= -70 ? 'var(--red)' : maxddNum <= -50 ? 'var(--orange)' : 'var(--t2)', textAlign: 'right' }}>{r.maxdd}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

function renderSlippageSensitivity(body: string) {
  const lines = body.split('\n');

  type SlipRow = { slippage: string; cagr: string; sharpe: number; maxdd: string };
  const rows: SlipRow[] = [];
  let elasticity = '';
  let elasticityRaw = '';
  let sensitivityVerdict = '';
  let sensitivityNote = '';

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─]{5,}/.test(line)) continue;
    if (/SLIPPAGE SENSITIVITY/i.test(line)) continue;
    if (/Slippage\s+CAGR/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Data rows: "0.0%   3192.25     2.818   -57.10%"
    const rowMatch = clean.match(/^([0-9.]+%)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%)/);
    if (rowMatch) {
      rows.push({ slippage: rowMatch[1], cagr: `${rowMatch[2]}%`, sharpe: parseFloat(rowMatch[3]), maxdd: rowMatch[4] });
      continue;
    }

    // "Cost Elasticity (d log CAGR / d slip): -6.2346"
    const elMatch = clean.match(/Cost Elasticity.*?:\s*([0-9.+-]+)/i);
    if (elMatch) { elasticity = elMatch[1]; elasticityRaw = clean; continue; }

    // "Sensitivity: ✅ LOW  (|elasticity| < 100 = low cost sensitivity)"
    const sensMatch = clean.match(/Sensitivity:\s*(✅|⚠|❌)\s*(\S+)\s*\((.+)\)/i);
    if (sensMatch) {
      sensitivityVerdict = sensMatch[2];
      sensitivityNote = sensMatch[3].trim();
      continue;
    }
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  const isLow = /LOW/i.test(sensitivityVerdict);
  const isHigh = /HIGH/i.test(sensitivityVerdict);
  const verdictColor = isLow ? 'var(--green)' : isHigh ? 'var(--red)' : 'var(--orange)';
  const verdictBg = isLow ? 'rgba(60,255,100,0.05)' : isHigh ? 'rgba(255,60,60,0.05)' : 'rgba(255,160,60,0.05)';
  const verdictBorder = isLow ? 'rgba(60,255,100,0.25)' : isHigh ? 'rgba(255,60,60,0.25)' : 'rgba(255,160,60,0.25)';

  const baseline = rows[0];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Verdict + elasticity */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {sensitivityVerdict && (
          <div style={{
            background: verdictBg, border: `1px solid ${verdictBorder}`, borderRadius: 6,
            padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1.5 1 180px',
          }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Cost Sensitivity</span>
            <span style={{ fontSize: 16, color: verdictColor, fontWeight: 600, ...MONO }}>{sensitivityVerdict}</span>
            {sensitivityNote && <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{sensitivityNote}</span>}
          </div>
        )}
        {elasticity && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 120px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Cost Elasticity</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{elasticity}</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>d log CAGR / d slip</span>
          </div>
        )}
      </div>

      {/* Table */}
      <div style={{ display: 'inline-flex', flexDirection: 'column', gap: 2 }}>
        <div style={{
          display: 'grid', gridTemplateColumns: '70px 100px 70px 80px',
          gap: 8, padding: '6px 12px', borderBottom: '1px solid var(--line1)',
        }}>
          {['Slippage', 'CAGR %', 'Sharpe', 'MaxDD %'].map((h, i) => (
            <span key={i} style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: i >= 1 ? 'right' : 'left', ...MONO }}>{h}</span>
          ))}
        </div>

        {rows.map((r, idx) => {
          const isBase = idx === 0;
          const sharpeColor = r.sharpe >= 2.0 ? 'var(--green)' : r.sharpe >= 1.5 ? 'var(--orange)' : 'var(--red)';
          const maxddNum = parseFloat(r.maxdd.replace('%', ''));

          return (
            <div key={idx} style={{
              display: 'grid', gridTemplateColumns: '70px 100px 70px 80px',
              gap: 8, padding: '7px 12px', borderRadius: 4,
              background: isBase ? 'rgba(255,255,255,0.04)' : idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
              border: isBase ? '1px solid rgba(255,255,255,0.08)' : '1px solid transparent',
              ...MONO,
            }}>
              <span style={{ fontSize: 11, color: isBase ? '#f0c040' : 'var(--t1)', fontWeight: isBase ? 600 : 500 }}>{r.slippage}</span>
              <span style={{ fontSize: 11, color: 'var(--t1)', textAlign: 'right' }}>{r.cagr}</span>
              <span style={{ fontSize: 12, color: sharpeColor, textAlign: 'right', fontWeight: 600 }}>{r.sharpe.toFixed(3)}</span>
              <span style={{ fontSize: 11, color: maxddNum <= -50 ? 'var(--red)' : maxddNum <= -30 ? 'var(--orange)' : 'var(--t2)', textAlign: 'right' }}>{r.maxdd}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderCappedReturnSensitivity(body: string) {
  const lines = body.split('\n');

  type CapRow = { cap: string; days: string; pctDays: string; logGrw: string; cagr: string; sharpe: number; maxdd: string; isBaseline: boolean };
  const rows: CapRow[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─]{5,}/.test(line)) continue;
    if (/CAPPED RETURN/i.test(line)) continue;
    if (/Cap\s+Days\s+%Days/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "none      0    0.0%      0.0%     3192.25    2.818  -57.10%"
    // "10%     69   17.3%     81.0%       94.40    1.188  -40.01%"
    const rowMatch = clean.match(/^(\S+)\s+(\d+)\s+([0-9.]+%)\s+([0-9.]+%)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%)/);
    if (rowMatch) {
      rows.push({
        cap: rowMatch[1],
        days: rowMatch[2],
        pctDays: rowMatch[3],
        logGrw: rowMatch[4],
        cagr: `${rowMatch[5]}%`,
        sharpe: parseFloat(rowMatch[6]),
        maxdd: rowMatch[7],
        isBaseline: /none/i.test(rowMatch[1]),
      });
    }
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  const baseline = rows.find(r => r.isBaseline);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      <div style={{ display: 'inline-flex', flexDirection: 'column', gap: 2 }}>
        {/* Header */}
        <div style={{
          display: 'grid', gridTemplateColumns: '55px 50px 60px 65px 90px 70px 75px',
          gap: 6, padding: '6px 12px', borderBottom: '1px solid var(--line1)',
        }}>
          {['Cap', 'Days', '% Days', 'LogGrw%', 'CAGR %', 'Sharpe', 'MaxDD %'].map((h, i) => (
            <span key={i} style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: i >= 3 ? 'right' : 'left', ...MONO }}>{h}</span>
          ))}
        </div>

        {rows.map((r, idx) => {
          const sharpeColor = r.sharpe >= 2.0 ? 'var(--green)' : r.sharpe >= 1.5 ? 'var(--orange)' : 'var(--red)';
          const maxddNum = parseFloat(r.maxdd.replace('%', ''));
          const pctDaysNum = parseFloat(r.pctDays);

          return (
            <div key={idx} style={{
              display: 'grid', gridTemplateColumns: '55px 50px 60px 65px 90px 70px 75px',
              gap: 6, padding: '7px 12px', borderRadius: 4,
              background: r.isBaseline ? 'rgba(255,255,255,0.04)' : idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
              border: r.isBaseline ? '1px solid rgba(255,255,255,0.08)' : '1px solid transparent',
              ...MONO,
            }}>
              <span style={{ fontSize: 11, color: r.isBaseline ? '#f0c040' : 'var(--t1)', fontWeight: r.isBaseline ? 600 : 500 }}>{r.cap}</span>
              <span style={{ fontSize: 11, color: 'var(--t2)' }}>{r.days}</span>
              <span style={{ fontSize: 11, color: pctDaysNum > 10 ? 'var(--orange)' : 'var(--t2)' }}>{r.pctDays}</span>
              <span style={{ fontSize: 11, color: parseFloat(r.logGrw) > 50 ? 'var(--red)' : 'var(--t2)', textAlign: 'right' }}>{r.logGrw}</span>
              <span style={{ fontSize: 11, color: 'var(--t1)', textAlign: 'right' }}>{r.cagr}</span>
              <span style={{ fontSize: 12, color: sharpeColor, textAlign: 'right', fontWeight: 600 }}>{r.sharpe.toFixed(3)}</span>
              <span style={{ fontSize: 11, color: maxddNum <= -50 ? 'var(--red)' : maxddNum <= -30 ? 'var(--orange)' : 'var(--t2)', textAlign: 'right' }}>{r.maxdd}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderTopNDayRemoval(body: string) {
  const lines = body.split('\n');

  let baselineSharpe = '';
  let baselineCAGR = '';
  let baselineMaxDD = '';

  type RemovalRow = { n: string; sharpe: number; dSharpe: string; cagr: string; dCagr: string; maxdd: string; removed: string };
  const rows: RemovalRow[] = [];
  let lastRowIdx = -1;

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─]{5,}/.test(line)) continue;
    if (/TOP-N DAY REMOVAL/i.test(line)) continue;
    if (/saved:/i.test(line)) continue;
    if (/N\s+Removed\s+CAGR|N\s+removed\s+Days/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // === variant baseline: "Baseline  Sharpe=3.462  CAGR=5914.8%  MaxDD=-37.95%"
    const blMatch = clean.match(/Baseline\s+Sharpe=([0-9.+-]+)\s+CAGR=([0-9.+-]+%?)\s+MaxDD=([0-9.+-]+%?)/i);
    if (blMatch) {
      baselineSharpe = blMatch[1]; baselineCAGR = blMatch[2]; baselineMaxDD = blMatch[3];
      continue;
    }

    // Box-drawn baseline: "Baseline     3192.25              2.818            -57.10%"
    const blBox = clean.match(/^Baseline\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%)/i);
    if (blBox) {
      baselineCAGR = `${blBox[1]}%`; baselineSharpe = blBox[2]; baselineMaxDD = blBox[3];
      continue;
    }

    // ↳ annotation: "↳ removed: 47.1%, 39.4%, 39.2%"
    const annMatch = clean.match(/^↳\s*removed:\s*(.+)/i);
    if (annMatch && lastRowIdx >= 0) {
      rows[lastRowIdx].removed = annMatch[1].trim();
      continue;
    }

    // === variant rows: "1    39.4%     3.324    -0.138    4378.4%    -37.95%"
    // The "days removed" column may contain commas/spaces
    const eqMatch = clean.match(/^\s*(\d+)\s+(.+?)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%?)\s+([0-9.+-]+%)/);
    if (eqMatch && !blMatch) {
      rows.push({
        n: eqMatch[1], removed: eqMatch[2].trim(),
        sharpe: parseFloat(eqMatch[3]), dSharpe: eqMatch[4],
        cagr: eqMatch[5], dCagr: '', maxdd: eqMatch[6],
      });
      lastRowIdx = rows.length - 1;
      continue;
    }

    // Box-drawn rows: "Remove top 1     2229.68   -962.56    2.659    -0.159  -57.10%"
    const boxMatch = clean.match(/Remove top\s+(\d+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%)/i);
    if (boxMatch) {
      rows.push({
        n: boxMatch[1], cagr: `${boxMatch[2]}%`, dCagr: boxMatch[3],
        sharpe: parseFloat(boxMatch[4]), dSharpe: boxMatch[5], maxdd: boxMatch[6], removed: '',
      });
      lastRowIdx = rows.length - 1;
      continue;
    }
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  const blSharpe = parseFloat(baselineSharpe);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Baseline cards */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {baselineSharpe && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineSharpe}</span>
          </div>
        )}
        {baselineCAGR && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline CAGR</span>
            <span style={{ fontSize: 16, color: 'var(--green)', fontWeight: 600, ...MONO }}>{baselineCAGR}</span>
          </div>
        )}
        {baselineMaxDD && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline MaxDD</span>
            <span style={{ fontSize: 16, color: 'var(--red)', fontWeight: 600, ...MONO }}>{baselineMaxDD}</span>
          </div>
        )}
      </div>

      {/* Removal rows */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {rows.map((r, idx) => {
          const sharpeColor = r.sharpe >= 2.0 ? 'var(--green)' : r.sharpe >= 1.5 ? 'var(--orange)' : 'var(--red)';
          const dSharpeNum = parseFloat(r.dSharpe);
          const retention = Number.isFinite(blSharpe) && blSharpe > 0 ? (r.sharpe / blSharpe) * 100 : 0;

          return (
            <div key={idx} style={{
              background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
              padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 6,
              position: 'relative', overflow: 'hidden',
            }}>
              {/* Retention bar */}
              <div style={{
                position: 'absolute', top: 0, left: 0, bottom: 0,
                width: `${Math.min(retention, 100)}%`,
                background: retention >= 60 ? 'rgba(60,255,100,0.03)' : 'rgba(255,60,60,0.03)',
              }} />

              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'relative', flexWrap: 'wrap', gap: 8 }}>
                <span style={{ fontSize: 11, color: 'var(--t2)', fontWeight: 500, ...MONO }}>
                  Remove top {r.n} day{parseInt(r.n) > 1 ? 's' : ''}
                </span>
                <div style={{ display: 'flex', gap: 16, alignItems: 'baseline', ...MONO }}>
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>Sharpe</span>
                    <span style={{ fontSize: 14, color: sharpeColor, fontWeight: 600 }}>{r.sharpe.toFixed(3)}</span>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>ΔSharpe</span>
                    <span style={{ fontSize: 12, color: dSharpeNum >= 0 ? 'var(--green)' : 'var(--red)' }}>{r.dSharpe}</span>
                  </div>
                  {r.cagr && (
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                      <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>CAGR</span>
                      <span style={{ fontSize: 12, color: 'var(--t2)' }}>{r.cagr}</span>
                    </div>
                  )}
                  {r.maxdd && (
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                      <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>MaxDD</span>
                      <span style={{ fontSize: 12, color: 'var(--t2)' }}>{r.maxdd}</span>
                    </div>
                  )}
                </div>
              </div>

              {r.removed && (
                <div style={{ fontSize: 9, color: 'var(--t4)', ...MONO, position: 'relative' }}>
                  Removed: {r.removed}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderLuckyStreakTest(title: string, body: string) {
  const lines = body.split('\n');

  // Extract window size from title first (box-drawn: "LUCKY STREAK TEST (30-day windows)")
  const titleWinMatch = title.match(/(\d+)-day/i);
  let windowSize = titleWinMatch ? `${titleWinMatch[1]}d` : '';
  let nBlocks = '';
  let baselineSharpe = '';
  let baselineCAGR = '';
  let baselineMaxDD = '';

  type StreakRow = { scenario: string; cagr: string; dCagr: string; sharpe: number; dSharpe: string; blocks: string; dCalmar: string };
  const rows: StreakRow[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─]{5,}/.test(line)) continue;
    if (/LUCKY STREAK/i.test(line)) continue;
    if (/saved:/i.test(line)) continue;
    if (/Scenario\s+CAGR|Blocks removed/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Window from title: "(30-day windows)"
    const winMatch = clean.match(/(\d+)-day windows/i);
    if (winMatch) { windowSize = `${winMatch[1]}d`; continue; }

    // "13 non-overlapping 30-day blocks"
    const nbMatch = clean.match(/(\d+)\s+non-overlapping\s+(\d+)-day blocks/i);
    if (nbMatch) { nBlocks = nbMatch[1]; windowSize = `${nbMatch[2]}d`; continue; }

    // "Baseline  Sharpe=3.462  CAGR=5914.8%  MaxDD=-37.95%" (=== variant)
    const blMatch2 = clean.match(/Baseline\s+Sharpe=([0-9.+-]+)\s+CAGR=([0-9.+-]+%?)\s+MaxDD=([0-9.+-]+%?)/i);
    if (blMatch2) {
      baselineSharpe = blMatch2[1]; baselineCAGR = blMatch2[2]; baselineMaxDD = blMatch2[3];
      continue;
    }

    // Box-drawn baseline: "Baseline (all blocks)     3192.25              2.818"
    const blMatch1 = clean.match(/Baseline\s*\(all blocks\)\s+([0-9.+-]+)\s+([0-9.+-]+)/i);
    if (blMatch1) {
      baselineCAGR = `${blMatch1[1]}%`; baselineSharpe = blMatch1[2];
      continue;
    }

    // "↳ best block(s): +202.9%, +154.0%" — skip, annotation line
    if (/^↳/.test(clean)) continue;

    // === variant rows: "1    +187.0%     2.932    -0.531    2186.9%   -3727.8%"
    const eqRowMatch = clean.match(/^\s*(\d+)\s+([0-9.+%,\s]+?)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+%?)\s+([0-9.+-]+%?)/);
    if (eqRowMatch) {
      rows.push({
        scenario: `Remove best ${eqRowMatch[1]} block${parseInt(eqRowMatch[1]) > 1 ? 's' : ''}`,
        blocks: eqRowMatch[2].trim(),
        sharpe: parseFloat(eqRowMatch[3]),
        dSharpe: eqRowMatch[4],
        cagr: eqRowMatch[5],
        dCagr: '',
        dCalmar: eqRowMatch[6],
      });
      continue;
    }

    // Box-drawn rows: "Remove best 1 block     1091.59  -2100.65    2.276    -0.543"
    const boxRowMatch = clean.match(/Remove best (\d+) blocks?\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)\s+([0-9.+-]+)/i);
    if (boxRowMatch) {
      rows.push({
        scenario: `Remove best ${boxRowMatch[1]} block${parseInt(boxRowMatch[1]) > 1 ? 's' : ''}`,
        cagr: `${boxRowMatch[2]}%`,
        dCagr: boxRowMatch[3],
        sharpe: parseFloat(boxRowMatch[4]),
        dSharpe: boxRowMatch[5],
        blocks: '',
        dCalmar: '',
      });
      continue;
    }
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  const blSharpe = parseFloat(baselineSharpe);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Metadata */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {baselineSharpe && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineSharpe}</span>
          </div>
        )}
        {baselineCAGR && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline CAGR</span>
            <span style={{ fontSize: 16, color: 'var(--green)', fontWeight: 600, ...MONO }}>{baselineCAGR}</span>
          </div>
        )}
        {windowSize && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 80px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Window</span>
            <span style={{ fontSize: 16, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{windowSize}</span>
          </div>
        )}
        {nBlocks && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 80px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Blocks</span>
            <span style={{ fontSize: 16, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{nBlocks}</span>
          </div>
        )}
      </div>

      {/* Removal rows */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {rows.map((r, idx) => {
          const sharpeColor = r.sharpe >= 2.0 ? 'var(--green)' : r.sharpe >= 1.5 ? 'var(--orange)' : 'var(--red)';
          const dSharpeNum = parseFloat(r.dSharpe);
          // How much of baseline Sharpe is retained
          const retention = Number.isFinite(blSharpe) && blSharpe > 0 ? (r.sharpe / blSharpe) * 100 : 0;

          return (
            <div key={idx} style={{
              background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
              padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 6,
              position: 'relative', overflow: 'hidden',
            }}>
              {/* Retention bar background */}
              <div style={{
                position: 'absolute', top: 0, left: 0, bottom: 0,
                width: `${Math.min(retention, 100)}%`,
                background: retention >= 60 ? 'rgba(60,255,100,0.03)' : 'rgba(255,60,60,0.03)',
              }} />

              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'relative' }}>
                <span style={{ fontSize: 11, color: 'var(--t2)', fontWeight: 500, ...MONO }}>{r.scenario}</span>
                <div style={{ display: 'flex', gap: 16, alignItems: 'baseline', ...MONO }}>
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>Sharpe</span>
                    <span style={{ fontSize: 14, color: sharpeColor, fontWeight: 600 }}>{r.sharpe.toFixed(3)}</span>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                    <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>ΔSharpe</span>
                    <span style={{ fontSize: 12, color: dSharpeNum >= 0 ? 'var(--green)' : 'var(--red)' }}>{r.dSharpe}</span>
                  </div>
                  {r.cagr && (
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                      <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>CAGR</span>
                      <span style={{ fontSize: 12, color: 'var(--t2)' }}>{r.cagr}</span>
                    </div>
                  )}
                  {r.dCalmar && (
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1 }}>
                      <span style={{ fontSize: 8, color: 'var(--t4)', textTransform: 'uppercase' }}>ΔCalmar</span>
                      <span style={{ fontSize: 12, color: parseFloat(r.dCalmar) >= 0 ? 'var(--green)' : 'var(--red)' }}>{r.dCalmar}</span>
                    </div>
                  )}
                </div>
              </div>

              {/* Block returns */}
              {r.blocks && (
                <div style={{ fontSize: 9, color: 'var(--t4)', ...MONO, position: 'relative' }}>
                  Zeroed: {r.blocks}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderParamSensitivityMap(body: string) {
  const lines = body.split('\n');

  let baselineSharpe = '';
  let perturbations = '';
  let legend = '';

  type SensRow = { param: string; values: { label: string; sharpe: number }[]; baseSharpe: number; range: number; verdict: string };
  const rows: SensRow[] = [];
  let colHeaders: string[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─═]{5,}/.test(line)) continue;
    if (/PARAMETER SENSITIVITY/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "Baseline Sharpe (from simulation): 2.886"
    const blMatch = clean.match(/Baseline Sharpe.*?:\s*([0-9.+-]+)/i);
    if (blMatch) { baselineSharpe = blMatch[1]; continue; }

    // "Perturbations tested: ±10%, ±20%, ±30%"
    const pertMatch = clean.match(/Perturbations tested:\s*(.+)/i);
    if (pertMatch) { perturbations = pertMatch[1].trim(); continue; }

    // "Range interpretation: ..."
    const legMatch = clean.match(/Range interpretation:\s*(.+)/i);
    if (legMatch) { legend = legMatch[1].trim(); continue; }

    // Header row: "Parameter     −30%     −20%     −10%     BASE     +10%     +20%     +30%    Range"
    if (/Parameter\s+[−+-]?\d+%/.test(clean)) {
      colHeaders = clean.split(/\s{2,}/).map(h => h.trim()).filter(Boolean);
      continue;
    }

    // Data rows: "EARLY_KILL_X     1.84     2.05     2.05     2.89     2.00     2.00     2.38    1.05  ⚠ FRAGILE"
    const parts = clean.split(/\s{2,}/);
    if (parts.length >= 8 && colHeaders.length > 0) {
      const param = parts[0].trim();
      // Skip if it looks like a non-data line
      if (!/^[A-Z_]/.test(param)) continue;

      const numParts = parts.slice(1);
      const values: { label: string; sharpe: number }[] = [];
      let baseSharpe = 0;
      let range = 0;
      let verdict = '';

      // Map values to column headers (skip "Parameter" header)
      const dataHeaders = colHeaders.slice(1);
      for (let i = 0; i < numParts.length; i++) {
        const raw = numParts[i].trim();
        const num = parseFloat(raw);

        if (i < dataHeaders.length) {
          const header = dataHeaders[i];
          if (/BASE/i.test(header)) {
            baseSharpe = num;
            values.push({ label: 'BASE', sharpe: num });
          } else if (/Range/i.test(header)) {
            range = num;
          } else if (Number.isFinite(num)) {
            values.push({ label: header, sharpe: num });
          }
        } else {
          // Remaining parts: range value then verdict
          if (Number.isFinite(num) && !range) {
            range = num;
          } else {
            verdict += ` ${raw}`;
          }
        }
      }

      verdict = verdict.replace(/[⚠✅]/g, '').trim();
      rows.push({ param, values, baseSharpe, range, verdict });
    }
  }

  if (rows.length === 0) return <PreFallback body={body} />;

  function rangeColor(r: number): string {
    if (r <= 0.30) return 'var(--green)';
    if (r <= 1.00) return 'var(--orange)';
    return 'var(--red)';
  }

  // Find global sharpe min/max for heatmap coloring
  const allSharpes = rows.flatMap(r => r.values.map(v => v.sharpe)).filter(Number.isFinite);
  const globalMin = Math.min(...allSharpes);
  const globalMax = Math.max(...allSharpes);
  const globalRange = globalMax - globalMin || 1;

  function heatColor(s: number): string {
    const t = (s - globalMin) / globalRange;
    if (t >= 0.8) return 'rgba(60,255,100,0.12)';
    if (t >= 0.5) return 'rgba(60,255,100,0.05)';
    if (t >= 0.3) return 'rgba(255,160,60,0.06)';
    return 'rgba(255,60,60,0.08)';
  }

  function textColor(s: number, base: number): string {
    const diff = s - base;
    if (Math.abs(diff) < 0.05) return 'var(--t1)';
    return diff >= 0 ? 'var(--green)' : 'var(--red)';
  }

  // Get unique perturbation labels in order
  const pertLabels = rows[0]?.values.map(v => v.label) ?? [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Metadata */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {baselineSharpe && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineSharpe}</span>
          </div>
        )}
        {perturbations && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1.5 1 160px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Perturbations</span>
            <span style={{ fontSize: 13, color: 'var(--t1)', fontWeight: 500, ...MONO }}>{perturbations}</span>
          </div>
        )}
      </div>

      {/* Heatmap table */}
      <div style={{ overflowX: 'auto' }}>
        <div style={{ display: 'inline-flex', flexDirection: 'column', gap: 2, minWidth: 'min-content' }}>
          {/* Header */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: `140px repeat(${pertLabels.length}, 58px) 60px 80px`,
            gap: 4, padding: '6px 8px', borderBottom: '1px solid var(--line1)',
          }}>
            <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Parameter</span>
            {pertLabels.map((h, i) => (
              <span key={i} style={{
                fontSize: 8.5, color: h === 'BASE' ? '#f0c040' : 'var(--t4)',
                textTransform: 'uppercase', letterSpacing: 0.3, textAlign: 'center',
                fontWeight: h === 'BASE' ? 600 : 400, ...MONO,
              }}>{h}</span>
            ))}
            <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: 'right', ...MONO }}>Range</span>
            <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, textAlign: 'right', ...MONO }}>Status</span>
          </div>

          {/* Data rows */}
          {rows.map((r, idx) => {
            const rc = rangeColor(r.range);
            return (
              <div key={idx} style={{
                display: 'grid',
                gridTemplateColumns: `140px repeat(${pertLabels.length}, 58px) 60px 80px`,
                gap: 4, padding: '6px 8px', borderRadius: 4,
                background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
                ...MONO,
              }}>
                <span style={{ fontSize: 10, color: 'var(--t2)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.param}</span>
                {r.values.map((v, vi) => (
                  <span key={vi} style={{
                    fontSize: 11, textAlign: 'center', fontWeight: v.label === 'BASE' ? 600 : 400,
                    color: v.label === 'BASE' ? '#f0c040' : textColor(v.sharpe, r.baseSharpe),
                    background: v.label === 'BASE' ? 'transparent' : heatColor(v.sharpe),
                    borderRadius: 3, padding: '2px 0',
                  }}>
                    {v.sharpe.toFixed(2)}
                  </span>
                ))}
                <span style={{ fontSize: 11, color: rc, textAlign: 'right', fontWeight: 600 }}>{r.range.toFixed(2)}</span>
                <span style={{ fontSize: 9, color: rc, textAlign: 'right', fontWeight: 500 }}>{r.verdict || '—'}</span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Legend */}
      {legend && (
        <div style={{ fontSize: 9, color: 'var(--t4)', ...MONO, padding: '6px 12px', background: 'var(--bg2)', borderRadius: 6, border: '1px solid var(--line1)' }}>
          {legend}
        </div>
      )}
    </div>
  );
}

function renderNeighborPlateau(body: string) {
  const lines = body.split('\n');

  let description = '';
  let nNeighbors = '';
  let baselineSharpe = '';
  let plateauRatio = '';
  let plateauVerdict = '';  // "SPIKE" or "PLATEAU"
  let p10 = '';
  let p25 = '';
  let median = '';
  let p75 = '';
  let std = '';

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/[─═]{5,}/.test(line)) continue;
    if (/NEIGHBOR PLATEAU/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Skip interpretation legend lines
    if (/^Interpretation:/i.test(clean)) continue;
    if (/^[<≥]/.test(clean)) continue;

    // "Joint ±15% perturbation of all parameters simultaneously"
    if (/perturbation/i.test(clean) && !description) { description = clean; continue; }

    // "n_neighbors: 200  |  baseline Sharpe: 2.886"
    const metaMatch = clean.match(/n_neighbors:\s*(\d+)\s*\|\s*baseline Sharpe:\s*([0-9.+-]+)/i);
    if (metaMatch) { nNeighbors = metaMatch[1]; baselineSharpe = metaMatch[2]; continue; }

    // "Plateau ratio (within ±0.5 Sharpe):  41.5%  ⚠ SPIKE"
    const prMatch = clean.match(/Plateau ratio.*?:\s*([0-9.]+%)\s*(⚠|✅)?\s*(SPIKE|PLATEAU)?/i);
    if (prMatch) {
      plateauRatio = prMatch[1];
      plateauVerdict = prMatch[3]?.toUpperCase() ?? '';
      continue;
    }

    // Percentile lines
    const pMatch = clean.match(/Neighbor Sharpe\s+(p\d+|median|std)\s*:\s*([0-9.+-]+)/i);
    if (pMatch) {
      const key = pMatch[1].toLowerCase();
      const val = pMatch[2];
      if (key === 'p10') p10 = val;
      else if (key === 'p25') p25 = val;
      else if (key === 'median') median = val;
      else if (key === 'p75') p75 = val;
      else if (key === 'std') std = val;
      continue;
    }
  }

  if (!plateauRatio && !baselineSharpe) return <PreFallback body={body} />;

  const isSpike = plateauVerdict === 'SPIKE';
  const ratioNum = parseFloat(plateauRatio);
  const ratioColor = ratioNum >= 70 ? 'var(--green)' : ratioNum >= 50 ? 'var(--orange)' : 'var(--red)';
  const ratioBg = ratioNum >= 70 ? 'rgba(60,255,100,0.05)' : ratioNum >= 50 ? 'rgba(255,160,60,0.05)' : 'rgba(255,60,60,0.05)';
  const ratioBorder = ratioNum >= 70 ? 'rgba(60,255,100,0.3)' : ratioNum >= 50 ? 'rgba(255,160,60,0.3)' : 'rgba(255,60,60,0.3)';

  // Distribution visual
  const pctiles = [
    { label: 'p10', value: p10 },
    { label: 'p25', value: p25 },
    { label: 'Median', value: median },
    { label: 'p75', value: p75 },
  ].filter(p => p.value);

  const blNum = parseFloat(baselineSharpe);
  const allNums = [...pctiles.map(p => parseFloat(p.value)), blNum].filter(Number.isFinite);
  const distMin = allNums.length ? Math.min(...allNums) * 0.9 : 0;
  const distMax = allNums.length ? Math.max(...allNums) * 1.05 : 5;
  const distRange = distMax - distMin || 1;
  function pctPos(v: number): number { return ((v - distMin) / distRange) * 100; }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {description && (
        <div style={{ fontSize: 10, color: 'var(--t3)', ...MONO, padding: '6px 12px', background: 'var(--bg2)', borderRadius: 6, border: '1px solid var(--line1)' }}>
          {description}
        </div>
      )}

      {/* Top cards */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {/* Plateau ratio card — the key metric */}
        <div style={{
          background: ratioBg, border: `1px solid ${ratioBorder}`, borderRadius: 6,
          padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 4, flex: '1.5 1 180px',
        }}>
          <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Plateau Ratio</span>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 10 }}>
            <span style={{ fontSize: 22, color: ratioColor, fontWeight: 700, ...MONO }}>{plateauRatio}</span>
            {plateauVerdict && (
              <span style={{
                fontSize: 10, fontWeight: 600, padding: '2px 8px', borderRadius: 4,
                background: isSpike ? 'rgba(255,60,60,0.12)' : 'rgba(60,255,100,0.12)',
                color: isSpike ? 'var(--red)' : 'var(--green)',
                ...MONO,
              }}>
                {plateauVerdict}
              </span>
            )}
          </div>
          <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>within ±0.5 Sharpe of baseline</span>
          {/* Mini progress bar */}
          <div style={{ height: 4, borderRadius: 2, background: 'rgba(255,255,255,0.06)', overflow: 'hidden', marginTop: 2 }}>
            <div style={{ height: '100%', width: `${Math.min(ratioNum, 100)}%`, borderRadius: 2, background: ratioColor }} />
          </div>
        </div>

        {baselineSharpe && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 20, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineSharpe}</span>
          </div>
        )}
        {nNeighbors && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 90px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Neighbors</span>
            <span style={{ fontSize: 20, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{nNeighbors}</span>
          </div>
        )}
        {std && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 90px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Std Dev</span>
            <span style={{ fontSize: 20, color: 'var(--t2)', fontWeight: 600, ...MONO }}>{std}</span>
          </div>
        )}
      </div>

      {/* Distribution range visualization */}
      {pctiles.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
            Neighbor Sharpe Distribution
          </div>
          <div style={{
            background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
            padding: '16px 20px 28px', position: 'relative',
          }}>
            {/* Range bar */}
            <div style={{ position: 'relative', height: 8, background: 'rgba(255,255,255,0.04)', borderRadius: 4, marginBottom: 22 }}>
              {/* IQR fill (p25 to p75) */}
              {p25 && p75 && (
                <div style={{
                  position: 'absolute', top: 0, bottom: 0, borderRadius: 4,
                  left: `${pctPos(parseFloat(p25))}%`,
                  width: `${pctPos(parseFloat(p75)) - pctPos(parseFloat(p25))}%`,
                  background: 'rgba(255,255,255,0.08)',
                }} />
              )}
              {/* Baseline marker */}
              {baselineSharpe && (
                <div style={{
                  position: 'absolute', top: -5, height: 18, width: 2, borderRadius: 1,
                  background: 'var(--green)', left: `${pctPos(blNum)}%`,
                }} />
              )}
              {/* Median marker */}
              {median && (
                <div style={{
                  position: 'absolute', top: -4, height: 16, width: 2, borderRadius: 1,
                  background: 'var(--t1)', left: `${pctPos(parseFloat(median))}%`,
                }} />
              )}
            </div>
            {/* Labels */}
            <div style={{ position: 'relative', height: 28, ...MONO }}>
              {pctiles.map((p, i) => {
                const pos = pctPos(parseFloat(p.value));
                return (
                  <div key={i} style={{
                    position: 'absolute', left: `${pos}%`, transform: 'translateX(-50%)',
                    display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 1,
                  }}>
                    <span style={{ fontSize: 11, color: p.label === 'Median' ? 'var(--t1)' : 'var(--t2)', fontWeight: p.label === 'Median' ? 600 : 400 }}>{p.value}</span>
                    <span style={{ fontSize: 7.5, color: 'var(--t4)', textTransform: 'uppercase' }}>{p.label}</span>
                  </div>
                );
              })}
              {baselineSharpe && (
                <div style={{
                  position: 'absolute', left: `${pctPos(blNum)}%`, transform: 'translateX(-50%)', top: -34,
                  display: 'flex', flexDirection: 'column', alignItems: 'center',
                }}>
                  <span style={{ fontSize: 7.5, color: 'var(--green)', textTransform: 'uppercase' }}>Base</span>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function renderReturnConcentration(body: string) {
  const lines = body.split('\n');

  // Metadata
  let activeDays = '';
  let flatDays = '';
  let totalPnl = '';
  let grossPos = '';
  let grossNeg = '';

  // Gini
  let giniValue = '';
  let giniNote = '';

  // Concentration table
  const concRows: { topN: string; nDays: string; pctGross: string; threshold: string; pass: boolean | null }[] = [];

  // Verdict
  let verdict = '';
  let verdictDetail = '';

  // Worst-day concentration
  let worstDayLabel = '';
  let worstDayPct = '';
  let worstDayNote = '';

  // PnL half-life
  let halfLifePct = '';
  let halfLifeDetail = '';
  let halfLifeNote = '';

  for (const line of lines) {
    if (/^[─═]{5,}$/.test(line.trim())) continue;
    if (/RETURN CONCENTRATION/i.test(line)) continue;
    if (/saved/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Metadata: "Filter: Tail + Disp + Vol  |  Active days: 113  |  Flat days: 285"
    const activeMatch = clean.match(/Active days:\s*(\d+)/i);
    if (activeMatch) activeDays = activeMatch[1];
    const flatMatch = clean.match(/Flat days:\s*(\d+)/i);
    if (flatMatch) flatDays = flatMatch[1];

    // "Total PnL: +556.19%   Gross+: 924.91%   Gross-: -368.71%"
    const pnlMatch = clean.match(/Total PnL:\s*([0-9.+%-]+)\s+Gross\+:\s*([0-9.+%-]+)\s+Gross-:\s*([0-9.+%-]+)/i);
    if (pnlMatch) {
      totalPnl = pnlMatch[1];
      grossPos = pnlMatch[2];
      grossNeg = pnlMatch[3];
      continue;
    }

    // "Gini coefficient : 0.378 (winners only)  → moderate inequality"
    const giniMatch = clean.match(/Gini coefficient\s*:\s*([0-9.]+).*?→\s*(.+)/i);
    if (giniMatch) {
      giniValue = giniMatch[1];
      giniNote = giniMatch[2].trim();
      continue;
    }

    // Table rows: "1%        1            4.3%       < 20%   ✓"
    const rowMatch = clean.match(/^(\d+%)\s+(\d+)\s+([0-9.]+%)\s+([<> 0-9%—-]+)\s*(✓|✗|—)?/);
    if (rowMatch) {
      concRows.push({
        topN: rowMatch[1],
        nDays: rowMatch[2],
        pctGross: rowMatch[3],
        threshold: rowMatch[4].trim(),
        pass: rowMatch[5] === '✓' ? true : rowMatch[5] === '✗' ? false : null,
      });
      continue;
    }

    // "Verdict  : DIVERSIFIED — return broadly distributed across winning days"
    const verdictMatch = clean.match(/^Verdict\s*:\s*(\S+)\s*[—–-]\s*(.+)/i);
    if (verdictMatch) {
      verdict = verdictMatch[1].trim();
      verdictDetail = verdictMatch[2].trim();
      continue;
    }

    // "Worst 5% contribute : 26.3% of total losses  → healthy — losses distributed"
    const worstMatch = clean.match(/Worst\s+(\d+%)\s+contribute\s*:\s*([0-9.]+%)\s+of total losses\s*→\s*(.+)/i);
    if (worstMatch) {
      worstDayLabel = `Worst ${worstMatch[1]}`;
      worstDayPct = worstMatch[2];
      worstDayNote = worstMatch[3].trim();
      continue;
    }

    // "Worst-day concentration (worst 5% of days = 6 days)" — capture label
    const worstHeaderMatch = clean.match(/Worst-day concentration\s*\((.+?)\)/i);
    if (worstHeaderMatch && !worstDayLabel) {
      worstDayLabel = worstHeaderMatch[1];
      continue;
    }

    // "PnL Half-Life : top 8.0% of days generate 50% of total PnL"
    const halfMatch = clean.match(/PnL Half-Life\s*:\s*top\s+([0-9.]+%)\s+of days\s+generate\s+50%\s+of total PnL/i);
    if (halfMatch) {
      halfLifePct = halfMatch[1];
      continue;
    }

    // "(9 of 113 active days)  → excellent — highly concentrated winners"
    const halfDetailMatch = clean.match(/\((\d+\s+of\s+\d+\s+active days)\)\s*→\s*(.+)/i);
    if (halfDetailMatch) {
      halfLifeDetail = halfDetailMatch[1];
      halfLifeNote = halfDetailMatch[2].trim();
      continue;
    }
  }

  if (concRows.length === 0 && !verdict && !giniValue) return <PreFallback body={body} />;

  // Verdict color
  const isConcentrated = /CONCENTRATED/i.test(verdict);
  const isDiversified = /DIVERSIFIED/i.test(verdict);
  const verdictColor = isConcentrated ? 'var(--red)' : isDiversified ? 'var(--green)' : 'var(--orange)';
  const verdictBg = isConcentrated ? 'rgba(255,60,60,0.05)' : isDiversified ? 'rgba(60,255,100,0.05)' : 'rgba(255,160,60,0.05)';
  const verdictBorder = isConcentrated ? 'rgba(255,60,60,0.3)' : isDiversified ? 'rgba(60,255,100,0.3)' : 'rgba(255,160,60,0.3)';

  // Gini color: lower is more distributed
  const giniNum = parseFloat(giniValue);
  const giniColor = giniNum <= 0.3 ? 'var(--green)' : giniNum <= 0.5 ? 'var(--orange)' : 'var(--red)';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Summary cards row */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {verdict && (
          <div style={{
            background: verdictBg, border: `1px solid ${verdictBorder}`, borderRadius: 6,
            padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1.5 1 180px',
          }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Verdict</span>
            <span style={{ fontSize: 15, color: verdictColor, fontWeight: 600, ...MONO }}>{verdict}</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{verdictDetail}</span>
          </div>
        )}
        {totalPnl && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 100px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Total PnL</span>
            <span style={{ fontSize: 16, color: parseFloat(totalPnl) >= 0 ? 'var(--green)' : 'var(--red)', fontWeight: 600, ...MONO }}>{totalPnl}</span>
          </div>
        )}
        {giniValue && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 100px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Gini Coeff</span>
            <span style={{ fontSize: 16, color: giniColor, fontWeight: 600, ...MONO }}>{giniValue}</span>
            <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{giniNote}</span>
          </div>
        )}
      </div>

      {/* Activity + PnL breakdown bar */}
      {(activeDays || grossPos) && (
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {activeDays && (
            <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '8px 14px', display: 'flex', gap: 16, flex: '1 1 160px', alignItems: 'center' }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Active</span>
                <span style={{ fontSize: 14, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{activeDays}d</span>
              </div>
              {flatDays && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Flat</span>
                  <span style={{ fontSize: 14, color: 'var(--t3)', fontWeight: 500, ...MONO }}>{flatDays}d</span>
                </div>
              )}
            </div>
          )}
          {grossPos && (
            <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '8px 14px', display: 'flex', gap: 16, flex: '1 1 160px', alignItems: 'center' }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Gross +</span>
                <span style={{ fontSize: 13, color: 'var(--green)', fontWeight: 500, ...MONO }}>{grossPos}</span>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Gross −</span>
                <span style={{ fontSize: 13, color: 'var(--red)', fontWeight: 500, ...MONO }}>{grossNeg}</span>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Concentration table */}
      {concRows.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <div style={{
            display: 'grid', gridTemplateColumns: '60px 60px 1fr 90px 48px',
            gap: 8, padding: '6px 12px', borderBottom: '1px solid var(--line1)',
          }}>
            {['Top N%', 'Days', '% of Gross +', 'Threshold', 'Pass'].map((h, i) => (
              <span key={i} style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, textAlign: i >= 2 ? 'right' : 'left', ...MONO }}>{h}</span>
            ))}
          </div>
          {concRows.map((r, idx) => {
            const pctNum = parseFloat(r.pctGross);
            // Visual bar width capped at 100
            const barWidth = Math.min(pctNum, 100);

            return (
              <div key={idx} style={{
                display: 'grid', gridTemplateColumns: '60px 60px 1fr 90px 48px',
                gap: 8, padding: '8px 12px', borderRadius: 4, alignItems: 'center',
                background: 'var(--bg2)', position: 'relative', overflow: 'hidden',
                ...MONO,
              }}>
                {/* Background bar */}
                <div style={{
                  position: 'absolute', top: 0, left: 0, bottom: 0,
                  width: `${barWidth}%`, opacity: 0.04,
                  background: 'var(--green)',
                }} />
                <span style={{ fontSize: 12, color: 'var(--t1)', fontWeight: 600, position: 'relative' }}>{r.topN}</span>
                <span style={{ fontSize: 11, color: 'var(--t2)', position: 'relative' }}>{r.nDays}</span>
                <span style={{ fontSize: 12, color: 'var(--t1)', textAlign: 'right', fontWeight: 500, position: 'relative' }}>{r.pctGross}</span>
                <span style={{ fontSize: 11, color: 'var(--t3)', textAlign: 'right', position: 'relative' }}>{r.threshold}</span>
                <span style={{
                  fontSize: 10, fontWeight: 600, textAlign: 'center', position: 'relative',
                  color: r.pass === true ? 'var(--green)' : r.pass === false ? 'var(--red)' : 'var(--t4)',
                }}>
                  {r.pass === true ? '✓' : r.pass === false ? '✗' : '—'}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {/* Bottom insight cards: Worst-day + PnL half-life */}
      {(worstDayPct || halfLifePct) && (
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
          {worstDayPct && (
            <div style={{
              flex: '1 1 200px', background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
              padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 4,
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Worst-Day Concentration</span>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{worstDayPct}</span>
                <span style={{ fontSize: 10, color: 'var(--t3)', ...MONO }}>of total losses ({worstDayLabel})</span>
              </div>
              {worstDayNote && <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{worstDayNote}</span>}
            </div>
          )}
          {halfLifePct && (
            <div style={{
              flex: '1 1 200px', background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
              padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 4,
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>PnL Half-Life</span>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>Top {halfLifePct}</span>
                <span style={{ fontSize: 10, color: 'var(--t3)', ...MONO }}>of days → 50% of PnL</span>
              </div>
              {halfLifeDetail && <span style={{ fontSize: 9.5, color: 'var(--t3)', ...MONO }}>{halfLifeDetail}</span>}
              {halfLifeNote && <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{halfLifeNote}</span>}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function renderParamJitter(body: string) {
  const lines = body.split('\n');

  let trials = '';
  const jitterParams: { name: string; baseline: string; range: string }[] = [];

  // Results
  let baselineSharpe = '';
  let trialCount = '';
  let mean = '';
  let bias = '';
  let biasPct = '';
  let biasNote = '';
  let median = '';
  let std = '';
  let p5 = '';
  let p10 = '';
  let p25 = '';
  let p75 = '';
  let minVal = '';
  let maxVal = '';
  let pctBelow2 = '';
  let pctBelow15 = '';
  let elasticity = '';
  let elasticityNote = '';
  let verdict = '';
  let verdictDetail = '';

  for (const line of lines) {
    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Trials count: "Filter: Tail Guardrail  |  Trials: 300"
    const trialsMatch = clean.match(/Trials:\s*(\d+)/i);
    if (trialsMatch) { trials = trialsMatch[1]; continue; }

    // Jitter spec: "L_HIGH                 baseline=2.2  ±10%"
    const specMatch = clean.match(/^([A-Z_][A-Z0-9_]*)\s+baseline=([0-9.+-]+)\s+(±[0-9.]+[%]?)/);
    if (specMatch) {
      jitterParams.push({ name: specMatch[1], baseline: specMatch[2], range: specMatch[3] });
      continue;
    }

    // "✓  300/300 trials completed successfully"
    const trialCompleteMatch = clean.match(/(\d+\/\d+)\s+trials completed/i);
    if (trialCompleteMatch) { trialCount = trialCompleteMatch[1]; continue; }

    // "Baseline Sharpe : 3.741"
    const blMatch = clean.match(/^Baseline Sharpe\s*:\s*([0-9.+-]+)/i);
    if (blMatch) { baselineSharpe = blMatch[1]; continue; }

    // "Mean            : 2.754   Bias: -0.987  (-26.4%)  → LARGE DRIFT ..."
    const meanMatch = clean.match(/^Mean\s*:\s*([0-9.+-]+)\s+Bias:\s*([0-9.+-]+)\s*\(([0-9.+-]+%)\)\s*→?\s*(.*)/i);
    if (meanMatch) { mean = meanMatch[1]; bias = meanMatch[2]; biasPct = meanMatch[3]; biasNote = meanMatch[4].trim(); continue; }

    // "Median          : 2.777"
    const medMatch = clean.match(/^Median\s*:\s*([0-9.+-]+)/i);
    if (medMatch) { median = medMatch[1]; continue; }

    // "Std             : 0.542"
    const stdMatch = clean.match(/^Std\s*:\s*([0-9.+-]+)/i);
    if (stdMatch) { std = stdMatch[1]; continue; }

    // Percentiles
    const p5Match = clean.match(/^p5\s*:\s*([0-9.+-]+)/i);
    if (p5Match) { p5 = p5Match[1]; continue; }
    const p10Match = clean.match(/^p10\s*:\s*([0-9.+-]+)/i);
    if (p10Match) { p10 = p10Match[1]; continue; }
    const p25Match = clean.match(/^p25\s*:\s*([0-9.+-]+)/i);
    if (p25Match) { p25 = p25Match[1]; continue; }
    const p75Match = clean.match(/^p75\s*:\s*([0-9.+-]+)/i);
    if (p75Match) { p75 = p75Match[1]; continue; }

    // "Min / Max       : 1.774  /  3.870"
    const mmMatch = clean.match(/Min\s*\/\s*Max\s*:\s*([0-9.+-]+)\s*\/\s*([0-9.+-]+)/i);
    if (mmMatch) { minVal = mmMatch[1]; maxVal = mmMatch[2]; continue; }

    // "% trials < 2.0  : 4.3%"
    const pct2Match = clean.match(/%\s*trials\s*<\s*2\.0\s*:\s*([0-9.]+%)/i);
    if (pct2Match) { pctBelow2 = pct2Match[1]; continue; }
    const pct15Match = clean.match(/%\s*trials\s*<\s*1\.5\s*:\s*([0-9.]+%)/i);
    if (pct15Match) { pctBelow15 = pct15Match[1]; continue; }

    // "Elasticity      : 0.1448  → moderate sensitivity"
    const elMatch = clean.match(/^Elasticity\s*:\s*([0-9.+-]+)\s*→?\s*(.*)/i);
    if (elMatch) { elasticity = elMatch[1]; elasticityNote = elMatch[2].trim(); continue; }

    // "Verdict         : Moderate sensitivity — review left tail and bias"
    const vMatch = clean.match(/^Verdict\s*:\s*(.+?)(?:\s*[—–-]\s*(.+))?$/i);
    if (vMatch && !verdict) { verdict = vMatch[1].trim(); verdictDetail = vMatch[2]?.trim() ?? ''; continue; }
  }

  if (!baselineSharpe && !mean && jitterParams.length === 0) return <PreFallback body={body} />;

  // Verdict coloring
  const isLow = /low|stable|robust/i.test(verdict);
  const isHigh = /high|large|unstable/i.test(verdict);
  const verdictColor = isLow ? 'var(--green)' : isHigh ? 'var(--red)' : 'var(--orange)';
  const verdictBg = isLow ? 'rgba(60,255,100,0.05)' : isHigh ? 'rgba(255,60,60,0.05)' : 'rgba(255,160,60,0.05)';
  const verdictBorder = isLow ? 'rgba(60,255,100,0.3)' : isHigh ? 'rgba(255,60,60,0.3)' : 'rgba(255,160,60,0.3)';

  // Bias color
  const biasNum = parseFloat(bias);
  const biasColor = Math.abs(biasNum) <= 0.3 ? 'var(--green)' : Math.abs(biasNum) <= 0.7 ? 'var(--orange)' : 'var(--red)';

  // Build percentile distribution for visual
  const pctiles = [
    { label: 'p5', value: p5 },
    { label: 'p10', value: p10 },
    { label: 'p25', value: p25 },
    { label: 'Median', value: median },
    { label: 'p75', value: p75 },
  ].filter(p => p.value);

  const blNum = parseFloat(baselineSharpe);
  const allNums = [
    ...pctiles.map(p => parseFloat(p.value)),
    parseFloat(minVal), parseFloat(maxVal), blNum,
  ].filter(Number.isFinite);
  const distMin = allNums.length ? Math.min(...allNums) : 0;
  const distMax = allNums.length ? Math.max(...allNums) : 5;
  const distRange = distMax - distMin || 1;

  function pctPos(v: number): number {
    return ((v - distMin) / distRange) * 100;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Verdict + headline stats */}
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {verdict && (
          <div style={{
            background: verdictBg, border: `1px solid ${verdictBorder}`, borderRadius: 6,
            padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1.5 1 180px',
          }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Verdict</span>
            <span style={{ fontSize: 14, color: verdictColor, fontWeight: 600, ...MONO }}>{verdict}</span>
            {verdictDetail && <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{verdictDetail}</span>}
          </div>
        )}
        {baselineSharpe && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Baseline Sharpe</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{baselineSharpe}</span>
          </div>
        )}
        {mean && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 110px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Mean (Jittered)</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{mean}</span>
            {bias && <span style={{ fontSize: 10, color: biasColor, ...MONO }}>Bias: {bias}</span>}
          </div>
        )}
        {trials && (
          <div style={{ background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6, padding: '10px 16px', display: 'flex', flexDirection: 'column', gap: 3, flex: '1 1 80px' }}>
            <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Trials</span>
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{trialCount || trials}</span>
          </div>
        )}
      </div>

      {/* Distribution + Stats side by side */}
      {pctiles.length > 0 && (
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          {/* Distribution visualization */}
          <div style={{
            flex: '2 1 340px', background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
            padding: '16px 20px 28px', position: 'relative', overflow: 'hidden',
          }}>
            <div style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, marginBottom: 14, ...MONO }}>
              Sharpe Distribution under Parameter Jitter
            </div>

            {/* Range track with threshold zones */}
            <div style={{ position: 'relative', height: 32, background: 'rgba(255,255,255,0.03)', borderRadius: 4, marginBottom: 24 }}>
              {/* Danger zone: < 1.5 */}
              {parseFloat(minVal) < 1.5 && (
                <div style={{
                  position: 'absolute', top: 0, bottom: 0, left: 0, borderRadius: '4px 0 0 4px',
                  width: `${Math.max(pctPos(1.5), 0)}%`,
                  background: 'rgba(255,60,60,0.08)',
                }} />
              )}
              {/* Warning zone: 1.5 – 2.0 */}
              {parseFloat(minVal) < 2.0 && (
                <div style={{
                  position: 'absolute', top: 0, bottom: 0,
                  left: `${Math.max(pctPos(1.5), 0)}%`,
                  width: `${Math.max(pctPos(2.0) - Math.max(pctPos(1.5), 0), 0)}%`,
                  background: 'rgba(255,160,60,0.06)',
                }} />
              )}
              {/* IQR fill (p25 to p75) */}
              {p25 && p75 && (
                <div style={{
                  position: 'absolute', top: 4, bottom: 4, borderRadius: 3,
                  left: `${pctPos(parseFloat(p25))}%`,
                  width: `${pctPos(parseFloat(p75)) - pctPos(parseFloat(p25))}%`,
                  background: 'rgba(255,180,50,0.18)',
                  border: '1px solid rgba(255,180,50,0.25)',
                }} />
              )}
              {/* Min whisker */}
              {minVal && <div style={{ position: 'absolute', top: 6, bottom: 6, width: 1, background: 'var(--t4)', left: `${pctPos(parseFloat(minVal))}%` }} />}
              {/* Max whisker */}
              {maxVal && <div style={{ position: 'absolute', top: 6, bottom: 6, width: 1, background: 'var(--t4)', left: `${pctPos(parseFloat(maxVal))}%` }} />}
              {/* Whisker connectors */}
              {minVal && p25 && (
                <div style={{ position: 'absolute', top: '50%', height: 1, background: 'rgba(255,255,255,0.1)',
                  left: `${pctPos(parseFloat(minVal))}%`, width: `${pctPos(parseFloat(p25)) - pctPos(parseFloat(minVal))}%`,
                }} />
              )}
              {maxVal && p75 && (
                <div style={{ position: 'absolute', top: '50%', height: 1, background: 'rgba(255,255,255,0.1)',
                  left: `${pctPos(parseFloat(p75))}%`, width: `${pctPos(parseFloat(maxVal)) - pctPos(parseFloat(p75))}%`,
                }} />
              )}
              {/* p5 marker — red dashed */}
              {p5 && <div style={{ position: 'absolute', top: 2, bottom: 2, width: 2, borderRadius: 1, background: 'var(--red)', opacity: 0.7, left: `${pctPos(parseFloat(p5))}%` }} />}
              {/* p10 marker — amber dashed */}
              {p10 && <div style={{ position: 'absolute', top: 2, bottom: 2, width: 2, borderRadius: 1, background: 'var(--orange)', opacity: 0.7, left: `${pctPos(parseFloat(p10))}%` }} />}
              {/* Mean marker — green */}
              {mean && (
                <div style={{ position: 'absolute', top: 0, bottom: 0, width: 2, borderRadius: 1, background: 'var(--green)', left: `${pctPos(parseFloat(mean))}%` }} />
              )}
              {/* Median marker — cyan/blue */}
              {median && (
                <div style={{ position: 'absolute', top: 0, bottom: 0, width: 2, borderRadius: 1, background: '#5bc0de', left: `${pctPos(parseFloat(median))}%` }} />
              )}
              {/* Baseline marker — yellow, bold */}
              {baselineSharpe && (
                <div style={{ position: 'absolute', top: -2, bottom: -2, width: 3, borderRadius: 1, background: '#f0c040', left: `${pctPos(blNum)}%` }} />
              )}
            </div>

            {/* Labels row */}
            <div style={{ position: 'relative', height: 32, ...MONO }}>
              {/* Threshold labels */}
              {parseFloat(minVal) < 2.0 && (
                <div style={{ position: 'absolute', left: `${Math.max(pctPos(2.0), 0)}%`, transform: 'translateX(-50%)', top: 18 }}>
                  <span style={{ fontSize: 7.5, color: 'var(--orange)', opacity: 0.6 }}>2.0</span>
                </div>
              )}
              {parseFloat(minVal) < 1.5 && (
                <div style={{ position: 'absolute', left: `${Math.max(pctPos(1.5), 0)}%`, transform: 'translateX(-50%)', top: 18 }}>
                  <span style={{ fontSize: 7.5, color: 'var(--red)', opacity: 0.6 }}>1.5</span>
                </div>
              )}
              {/* Percentile labels */}
              {[
                { label: 'p5', value: p5, color: 'var(--red)' },
                { label: 'p10', value: p10, color: 'var(--orange)' },
                { label: 'p25', value: p25, color: 'var(--t2)' },
                { label: 'p75', value: p75, color: 'var(--t2)' },
              ].filter(p => p.value).map((p, i) => (
                <div key={i} style={{
                  position: 'absolute', left: `${pctPos(parseFloat(p.value))}%`, transform: 'translateX(-50%)',
                  display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 0,
                }}>
                  <span style={{ fontSize: 9.5, color: p.color, fontWeight: 400 }}>{p.value}</span>
                  <span style={{ fontSize: 7, color: 'var(--t4)', textTransform: 'uppercase' }}>{p.label}</span>
                </div>
              ))}
              {/* Mean label */}
              {mean && (
                <div style={{ position: 'absolute', left: `${pctPos(parseFloat(mean))}%`, transform: 'translateX(-50%)', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                  <span style={{ fontSize: 9.5, color: 'var(--green)', fontWeight: 500 }}>{mean}</span>
                  <span style={{ fontSize: 7, color: 'var(--green)', textTransform: 'uppercase' }}>Mean</span>
                </div>
              )}
              {/* Median label */}
              {median && (
                <div style={{ position: 'absolute', left: `${pctPos(parseFloat(median))}%`, transform: 'translateX(-50%)', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                  <span style={{ fontSize: 9.5, color: '#5bc0de', fontWeight: 500 }}>{median}</span>
                  <span style={{ fontSize: 7, color: '#5bc0de', textTransform: 'uppercase' }}>Med</span>
                </div>
              )}
              {/* Baseline label */}
              {baselineSharpe && (
                <div style={{ position: 'absolute', left: `${pctPos(blNum)}%`, transform: 'translateX(-50%)', top: -48, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                  <span style={{ fontSize: 9.5, color: '#f0c040', fontWeight: 600 }}>{baselineSharpe}</span>
                  <span style={{ fontSize: 7, color: '#f0c040', textTransform: 'uppercase' }}>Base</span>
                </div>
              )}
            </div>

            {/* Legend */}
            <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', marginTop: 8, paddingTop: 8, borderTop: '1px solid var(--line1)' }}>
              {[
                { color: '#f0c040', label: 'Baseline' },
                { color: 'var(--green)', label: 'Mean' },
                { color: '#5bc0de', label: 'Median' },
                { color: 'var(--red)', label: 'p5' },
                { color: 'var(--orange)', label: 'p10' },
              ].map((l, i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <div style={{ width: 10, height: 2, borderRadius: 1, background: l.color }} />
                  <span style={{ fontSize: 8, color: 'var(--t4)', ...MONO }}>{l.label}</span>
                </div>
              ))}
              <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <div style={{ width: 10, height: 6, borderRadius: 2, background: 'rgba(255,180,50,0.2)', border: '1px solid rgba(255,180,50,0.3)' }} />
                <span style={{ fontSize: 8, color: 'var(--t4)', ...MONO }}>IQR (p25–p75)</span>
              </div>
            </div>
          </div>

          {/* Stats panel — right side, matching the chart's JITTER STATS panel */}
          <div style={{
            flex: '1 1 180px', display: 'flex', flexDirection: 'column', gap: 0,
            background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
            padding: '12px 0', overflow: 'hidden',
          }}>
            <div style={{ fontSize: 10, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 700, padding: '0 14px 8px', borderBottom: '1px solid var(--line1)', ...MONO }}>
              Jitter Stats
            </div>
            {[
              { label: 'Baseline', value: baselineSharpe, color: '#f0c040', bold: true },
              { label: 'Mean', value: mean, color: 'var(--green)', bold: false },
              { label: 'Median', value: median, color: '#5bc0de', bold: false },
              { label: 'Std', value: std, color: 'var(--t2)', bold: false },
              { label: 'Min', value: minVal, color: 'var(--t2)', bold: false },
              { label: 'p5', value: p5, color: 'var(--red)', bold: false },
              { label: 'p10', value: p10, color: 'var(--orange)', bold: false },
              { label: 'p25', value: p25, color: 'var(--t2)', bold: false },
              { label: 'p75', value: p75, color: 'var(--t2)', bold: false },
              { label: 'Max', value: maxVal, color: 'var(--t2)', bold: false },
              { label: '< 2.0', value: pctBelow2, color: parseFloat(pctBelow2) > 10 ? 'var(--red)' : parseFloat(pctBelow2) > 5 ? 'var(--orange)' : 'var(--green)', bold: false },
              { label: '< 1.5', value: pctBelow15, color: parseFloat(pctBelow15) > 5 ? 'var(--red)' : parseFloat(pctBelow15) > 0 ? 'var(--orange)' : 'var(--green)', bold: false },
              { label: 'Bias', value: bias ? `${bias} (${biasPct})` : '', color: biasColor, bold: false },
              { label: 'Elasticity', value: elasticity, color: 'var(--t2)', bold: false },
            ].filter(s => s.value).map((s, idx) => (
              <div key={idx} style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '4px 14px',
                background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.015)',
                ...MONO,
              }}>
                <span style={{ fontSize: 10, color: s.color, fontWeight: s.bold ? 700 : 400 }}>{s.label}</span>
                <span style={{ fontSize: 11, color: s.color, fontWeight: s.bold ? 700 : 500 }}>{s.value}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Jitter parameters */}
      {jitterParams.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <div style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO, paddingBottom: 2 }}>
            Jitter Specification
          </div>
          <div style={{
            display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 6,
          }}>
            {jitterParams.map((p, idx) => (
              <div key={idx} style={{
                background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 5,
                padding: '6px 10px', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                ...MONO,
              }}>
                <span style={{ fontSize: 10, color: 'var(--t3)' }}>{p.name}</span>
                <div style={{ display: 'flex', gap: 6, alignItems: 'baseline' }}>
                  <span style={{ fontSize: 11, color: 'var(--t1)', fontWeight: 500 }}>{p.baseline}</span>
                  <span style={{ fontSize: 9, color: 'var(--t4)' }}>{p.range}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function renderPeriodicBreakdown(body: string) {
  const lines = body.split('\n');

  type PeriodGroup = {
    period: string;   // "MONTHLY", "WEEKLY", "DAILY"
    count: string;    // "(13 months)", "(56 weeks)", etc.
    winRate: string;
    winLoss: string;  // "9W / 4L"
    avg: string;
    avgWin: string;
    avgLoss: string;
    best: string;
    worst: string;
  };

  const groups: PeriodGroup[] = [];
  let cur: Partial<PeriodGroup> | null = null;

  for (const line of lines) {
    if (/[─═]{5,}/.test(line)) continue;
    if (/PERIODIC RETURN/i.test(line)) continue;
    if (/saved:/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // Period header: "MONTHLY  (13 months)"
    const periodMatch = clean.match(/^(MONTHLY|WEEKLY|DAILY)\s+\((.+?)\)/i);
    if (periodMatch) {
      if (cur && cur.period) groups.push(cur as PeriodGroup);
      cur = { period: periodMatch[1].toUpperCase(), count: periodMatch[2], winRate: '', winLoss: '', avg: '', avgWin: '', avgLoss: '', best: '', worst: '' };
      continue;
    }

    if (!cur) continue;

    // Win rate : 69.2%  (9W / 4L)
    const wrMatch = clean.match(/Win rate\s*:\s*([0-9.]+%)\s*\((.+?)\)/i);
    if (wrMatch) { cur.winRate = wrMatch[1]; cur.winLoss = wrMatch[2]; continue; }

    // Avg      : +48.32%
    const avgMatch = clean.match(/^Avg\s*:\s*([0-9.+%-]+)/i);
    if (avgMatch) { cur.avg = avgMatch[1]; continue; }

    // Avg win  : 72.89%
    const awMatch = clean.match(/Avg win\s*:\s*([0-9.+%-]+)/i);
    if (awMatch) { cur.avgWin = awMatch[1]; continue; }

    // Avg loss : -6.95%
    const alMatch = clean.match(/Avg loss\s*:\s*([0-9.+%-]+)/i);
    if (alMatch) { cur.avgLoss = alMatch[1]; continue; }

    // Best     : 187.03%   Worst: -9.45%
    const bwMatch = clean.match(/Best\s*:\s*([0-9.+%-]+)\s+Worst\s*:\s*([0-9.+%-]+)/i);
    if (bwMatch) { cur.best = bwMatch[1]; cur.worst = bwMatch[2]; continue; }
  }
  if (cur && cur.period) groups.push(cur as PeriodGroup);

  if (groups.length === 0) return <PreFallback body={body} />;

  const periodIcon: Record<string, string> = { MONTHLY: 'M', WEEKLY: 'W', DAILY: 'D' };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        {groups.map((g, gIdx) => {
          const wr = parseFloat(g.winRate);
          const wrColor = wr >= 60 ? 'var(--green)' : wr >= 45 ? 'var(--orange)' : 'var(--red)';
          const avgNum = parseFloat(g.avg);
          const avgColor = avgNum >= 0 ? 'var(--green)' : 'var(--red)';

          return (
            <div key={gIdx} style={{
              flex: '1 1 200px',
              background: 'var(--bg2)',
              border: '1px solid var(--line1)',
              borderRadius: 8,
              padding: '14px 16px',
              display: 'flex',
              flexDirection: 'column',
              gap: 12,
            }}>
              {/* Period header */}
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{
                  width: 28, height: 28, borderRadius: 6,
                  background: 'rgba(255,255,255,0.05)', border: '1px solid var(--line1)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 14, fontWeight: 700, color: 'var(--t2)', ...MONO,
                }}>
                  {periodIcon[g.period] ?? g.period[0]}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--t1)', ...MONO }}>{g.period}</span>
                  <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO }}>{g.count}</span>
                </div>
              </div>

              {/* Win rate bar */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                  <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Win Rate</span>
                  <span style={{ fontSize: 14, fontWeight: 600, color: wrColor, ...MONO }}>{g.winRate}</span>
                </div>
                <div style={{ height: 4, borderRadius: 2, background: 'rgba(255,255,255,0.06)', overflow: 'hidden' }}>
                  <div style={{ height: '100%', width: `${wr}%`, borderRadius: 2, background: wrColor, transition: 'width 0.3s' }} />
                </div>
                <span style={{ fontSize: 9, color: 'var(--t4)', ...MONO, textAlign: 'right' }}>{g.winLoss}</span>
              </div>

              {/* Avg return */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '6px 0', borderTop: '1px solid var(--line1)' }}>
                <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Avg Return</span>
                <span style={{ fontSize: 16, fontWeight: 600, color: avgColor, ...MONO }}>{g.avg}</span>
              </div>

              {/* Avg win / Avg loss */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Avg Win</span>
                  <span style={{ fontSize: 12, color: 'var(--green)', fontWeight: 500, ...MONO }}>{g.avgWin}</span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2, textAlign: 'right' }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Avg Loss</span>
                  <span style={{ fontSize: 12, color: 'var(--red)', fontWeight: 500, ...MONO }}>{g.avgLoss}</span>
                </div>
              </div>

              {/* Best / Worst */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, borderTop: '1px solid var(--line1)', paddingTop: 8 }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Best</span>
                  <span style={{ fontSize: 12, color: 'var(--green)', fontWeight: 500, ...MONO }}>{g.best}</span>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2, textAlign: 'right' }}>
                  <span style={{ fontSize: 8.5, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.4, ...MONO }}>Worst</span>
                  <span style={{ fontSize: 12, color: 'var(--red)', fontWeight: 500, ...MONO }}>{g.worst}</span>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
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

  // Parse table rows — collapse "$      10,000" → "$10,000" before splitting
  const rows: string[][] = [];
  for (let i = tableStartIdx; i < tableEndIdx; i += 1) {
    const normalized = lines[i].replace(/\$\s+([\d,]+)/g, '$$$1').trim();
    if (!normalized || /^[─=]+$/.test(normalized)) continue;
    const tokens = normalized.split(/\s{2,}/).filter(Boolean);
    if (tokens.length >= 2) rows.push(tokens);
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

function renderCapacityCurveTest(body: string) {
  const lines = body.split('\n');

  // Extract metadata line (Start: $X | Impact ...)
  const metaLines: string[] = [];
  const tableHeaderIdx = lines.findIndex((l) => /Particip%/i.test(l));
  for (let i = 0; i < (tableHeaderIdx >= 0 ? tableHeaderIdx : lines.length); i += 1) {
    const stripped = lines[i].replace(/^\s*│\s*/, '').trim();
    if (stripped && !/^[─=]+$/.test(stripped)) metaLines.push(stripped);
  }

  // Parse table headers
  const headers = tableHeaderIdx >= 0
    ? lines[tableHeaderIdx].replace(/^\s*│\s*/, '').trim().split(/\s{2,}/).filter(Boolean)
    : [];

  // Find divider after header
  const dividerIdx = lines.findIndex(
    (l, i) => i > tableHeaderIdx && /^[\s│─]+$/.test(l) && l.includes('─'),
  );
  const tableStartIdx = dividerIdx >= 0 ? dividerIdx + 1 : tableHeaderIdx + 1;

  // Find footer (⚠ or ✅ line after blank)
  const footerIdx = lines.findIndex((l, i) => i > tableStartIdx && /^\s*│\s*$/.test(l));
  const tableEndIdx = footerIdx >= 0 ? footerIdx : lines.length;

  // Parse data rows — collapse "$      10,000" → "$10,000" before splitting
  const rows: string[][] = [];
  for (let i = tableStartIdx; i < tableEndIdx; i += 1) {
    const normalized = lines[i].replace(/^\s*│\s*/, '').replace(/\$\s+([\d,]+)/g, '$$$1').trim();
    if (!normalized || /^[─=]+$/.test(normalized)) continue;
    const tokens = normalized.split(/\s{2,}/).filter(Boolean);
    if (tokens.length >= 2) rows.push(tokens);
  }

  // Footer lines (⚠ or ✅)
  const footerLines: string[] = [];
  for (let i = footerIdx >= 0 ? footerIdx + 1 : lines.length; i < lines.length; i += 1) {
    const stripped = lines[i].replace(/^\s*[│└─┌┐┘]+\s*/, '').trim();
    if (stripped && !/^[─=]+$/.test(stripped)) footerLines.push(stripped);
  }

  // Column indices
  const flagIdx = headers.findIndex((h) => /^flag$/i.test(h));
  const sharpeIdx = headers.findIndex((h) => /^sharpe$/i.test(h));
  const impactIdx = headers.findIndex((h) => /impact/i.test(h));

  const isHighRow = (row: string[]) => flagIdx >= 0 && /HIGH/i.test(row[flagIdx] ?? '');

  const sharpeColor = (v: string) => {
    const n = parseFloat(v);
    if (!Number.isFinite(n)) return 'var(--t1)';
    if (n >= 2.0) return 'var(--green)';
    if (n >= 1.5) return 'var(--amber)';
    return 'var(--red)';
  };

  if (headers.length === 0 && rows.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, fontSize: 9, ...MONO }}>
      {/* Metadata banner */}
      {metaLines.length > 0 && (
        <div style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: '6px 20px',
          padding: '8px 12px',
          background: 'var(--bg2)',
          borderRadius: 6,
          border: '1px solid var(--line1)',
          fontSize: 10,
        }}>
          {metaLines.map((line, i) => {
            const m = line.match(/^(.+?):\s+(.+)$/);
            if (m) {
              return (
                <div key={i} style={{ display: 'flex', gap: 6 }}>
                  <span style={{ color: 'var(--t3)' }}>{m[1].trim()}:</span>
                  <span style={{ color: 'var(--t1)' }}>{m[2].trim()}</span>
                </div>
              );
            }
            // Handle "Start: $X  |  Impact ..." format — split on |
            const parts = line.split(/\s*\|\s*/);
            return (
              <Fragment key={i}>
                {parts.map((part, j) => {
                  const kvMatch = part.match(/^(.+?):\s+(.+)$/);
                  if (kvMatch) {
                    return (
                      <div key={j} style={{ display: 'flex', gap: 6 }}>
                        <span style={{ color: 'var(--t3)' }}>{kvMatch[1].trim()}:</span>
                        <span style={{ color: 'var(--t1)' }}>{kvMatch[2].trim()}</span>
                      </div>
                    );
                  }
                  return <span key={j} style={{ color: 'var(--t2)' }}>{part}</span>;
                })}
              </Fragment>
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
                      padding: '4px 10px 4px 6px',
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
                const high = isHighRow(row);
                return (
                  <tr
                    key={ri}
                    style={{
                      background: high
                        ? 'rgba(255, 60, 60, 0.06)'
                        : ri % 2 === 1
                          ? 'rgba(255,255,255,0.015)'
                          : 'transparent',
                    }}
                  >
                    {row.map((cell, ci) => {
                      let color = 'var(--t1)';
                      if (ci === flagIdx) {
                        color = /HIGH/i.test(cell) ? 'var(--red)' : 'var(--green)';
                      } else if (ci === sharpeIdx) {
                        color = sharpeColor(cell);
                      } else if (ci === impactIdx) {
                        const n = parseFloat(cell);
                        if (Number.isFinite(n) && n >= 0.005) color = 'var(--amber)';
                      }
                      return (
                        <td
                          key={ci}
                          style={{
                            padding: '3px 10px 3px 6px',
                            textAlign: ci === 0 ? 'left' : 'right',
                            color,
                            whiteSpace: 'nowrap',
                            fontWeight: ci === flagIdx ? 600 : 400,
                          }}
                        >
                          {cell || '\u2014'}
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

      {/* Footer verdict */}
      {footerLines.length > 0 && (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          gap: 4,
          padding: '8px 12px',
          borderRadius: 6,
          border: `1px solid ${footerLines.some((l) => l.includes('\u26A0')) ? 'rgba(255, 160, 60, 0.3)' : 'rgba(60, 255, 100, 0.3)'}`,
          background: footerLines.some((l) => l.includes('\u26A0')) ? 'rgba(255, 160, 60, 0.05)' : 'rgba(60, 255, 100, 0.05)',
        }}>
          {footerLines.map((line, i) => (
            <div key={i} style={{
              fontSize: 10,
              color: line.includes('\u26A0') ? 'var(--amber)' : 'var(--green)',
              fontWeight: 600,
            }}>
              {line}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function CostCurveTooltip({ lines }: { lines: string[] }) {
  const [show, setShow] = useState(false);
  return (
    <span
      style={{ position: 'relative', display: 'inline-block', marginLeft: 4, cursor: 'help' }}
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
    >
      <span style={{
        fontSize: 7,
        color: 'var(--t3)',
        opacity: 0.6,
        verticalAlign: 'super',
        borderBottom: '1px dotted var(--t3)',
        lineHeight: 1,
      }}>
        ?
      </span>
      {show && (
        <div style={{
          position: 'absolute',
          bottom: '100%',
          right: 0,
          marginBottom: 6,
          padding: '8px 10px',
          background: 'var(--bg1, #1a1a2e)',
          border: '1px solid var(--line2)',
          borderRadius: 6,
          boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
          zIndex: 100,
          whiteSpace: 'nowrap',
          textTransform: 'none',
          letterSpacing: 'normal',
          fontWeight: 400,
        }}>
          {lines.map((line, i) => (
            <div key={i} style={{
              fontSize: i === 0 ? 9 : 8,
              color: i === 0 ? 'var(--t1)' : 'var(--t3)',
              fontWeight: i === 0 ? 600 : 400,
              marginTop: i > 0 ? 3 : 0,
              ...MONO,
            }}>
              {line}
            </div>
          ))}
        </div>
      )}
    </span>
  );
}

function renderCostCurveTest(body: string) {
  const lines = body.split('\n');

  // Find the columnar header row — must have "AUM" plus "Slip%" as discrete column names
  const headerIdx = lines.findIndex((l) => {
    const s = l.replace(/^\s*│\s*/, '').trim();
    return /\bAUM\b/.test(s) && /\bSlip%?\b/.test(s) && s.split(/\s{2,}/).filter(Boolean).length >= 5;
  });

  // Everything before the header row is description text
  const descParts: string[] = [];
  for (let i = 0; i < (headerIdx >= 0 ? headerIdx : lines.length); i += 1) {
    const stripped = lines[i].replace(/^\s*│\s*/, '').trim();
    if (stripped && !/^[─=]+$/.test(stripped)) descParts.push(stripped);
  }
  const desc = descParts.join(' ');

  const headers = headerIdx >= 0
    ? lines[headerIdx].replace(/^\s*│\s*/, '').trim().split(/\s{2,}/).filter(Boolean)
    : [];

  // Find divider after header
  const dividerIdx = lines.findIndex(
    (l, i) => i > headerIdx && /^[\s│─]+$/.test(l) && l.includes('─'),
  );
  const tableStartIdx = dividerIdx >= 0 ? dividerIdx + 1 : headerIdx + 1;

  // Parse data rows — collapse "$   5,000" → "$5,000" before splitting
  const rows: string[][] = [];
  for (let i = tableStartIdx; i < lines.length; i += 1) {
    const raw = lines[i].trim();
    if (!raw || /^[─└┘]/.test(raw)) continue;
    const normalized = raw.replace(/^\s*│\s*/, '').replace(/\$\s+([\d,]+)/g, '$$$1').trim();
    if (!normalized) continue;
    const tokens = normalized.split(/\s{2,}/).filter(Boolean);
    if (tokens.length >= 2) rows.push(tokens);
  }

  // Column indices for color coding
  const sharpeIdx = headers.findIndex((h) => /^sharpe$/i.test(h));
  const cagrIdx = headers.findIndex((h) => /cagr/i.test(h));
  const totalIdx = headers.findIndex((h) => /^total/i.test(h));
  const maxDdIdx = headers.findIndex((h) => /maxdd/i.test(h));
  const slipIdx = headers.findIndex((h) => /^slip/i.test(h));
  const impactIdx = headers.findIndex((h) => /^impact/i.test(h));

  const sharpeColor = (v: string) => {
    const n = parseFloat(v);
    if (!Number.isFinite(n)) return 'var(--t1)';
    if (n >= 2.0) return 'var(--green)';
    if (n >= 1.0) return 'var(--amber)';
    return 'var(--red)';
  };

  const cagrColor = (v: string) => {
    const n = parseFloat(v);
    if (!Number.isFinite(n)) return 'var(--t1)';
    if (n > 100) return 'var(--green)';
    if (n > 0) return 'var(--amber)';
    return 'var(--red)';
  };

  const costColor = (v: string) => {
    const n = parseFloat(v);
    if (!Number.isFinite(n)) return 'var(--t1)';
    if (n >= 1.0) return 'var(--red)';
    if (n >= 0.5) return 'var(--amber)';
    return 'var(--green)';
  };

  if (headers.length === 0 && rows.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14, fontSize: 9, ...MONO }}>
      {/* Description banner */}
      {desc && (
        <div style={{
          padding: '8px 12px',
          background: 'var(--bg2)',
          borderRadius: 6,
          border: '1px solid var(--line1)',
          fontSize: 10,
          color: 'var(--t2)',
          lineHeight: 1.5,
        }}>
          {desc}
        </div>
      )}

      {/* Cost curve table */}
      {rows.length > 0 && (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 9, ...MONO }}>
            <thead>
              <tr>
                {headers.map((h, i) => {
                  const labelMap: Record<string, string> = {
                    'Slip%': 'Base Slip%/day',
                    'Impact%': 'Mkt Impact%/day',
                    'Total%': 'Total Cost%/day',
                    'CAGR%': 'CAGR%',
                    'MaxDD%': 'Max DD%',
                  };
                  const label = labelMap[h] ?? h;

                  const tooltipMap: Record<string, string[]> = {
                    'Total%': [
                      'Total daily cost drag applied to each return',
                      'Formula: Base Slip + Mkt Impact',
                      '= slip + slip \u00D7 (AUM / ref_AUM)^exp',
                    ],
                    'Impact%': [
                      'Market impact cost per day',
                      'Scales with \u221A(AUM) \u2014 bigger size = more friction',
                    ],
                    'Slip%': [
                      'Fixed base slippage per day',
                      'Constant across all AUM levels',
                    ],
                  };
                  const tipLines = tooltipMap[h];

                  return (
                    <th
                      key={i}
                      style={{
                        padding: '6px 12px 6px 8px',
                        textAlign: i === 0 ? 'left' : 'right',
                        color: 'var(--t3)',
                        fontWeight: 700,
                        fontSize: 8,
                        textTransform: 'uppercase',
                        letterSpacing: '0.08em',
                        borderBottom: '2px solid var(--line2)',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {label}
                      {tipLines && <CostCurveTooltip lines={tipLines} />}
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, ri) => {
                const sharpeVal = sharpeIdx >= 0 ? parseFloat(row[sharpeIdx] ?? '') : NaN;
                const degraded = Number.isFinite(sharpeVal) && sharpeVal < 1.0;
                return (
                  <tr
                    key={ri}
                    style={{
                      background: degraded
                        ? 'rgba(255, 60, 60, 0.07)'
                        : ri % 2 === 1
                          ? 'rgba(255,255,255,0.02)'
                          : 'transparent',
                      borderBottom: '1px solid var(--line1)',
                    }}
                  >
                    {row.map((cell, ci) => {
                      let color = 'var(--t1)';
                      let fontWeight = 400;
                      if (ci === 0) {
                        // AUM column — always bright
                        color = 'var(--t1)';
                        fontWeight = 600;
                      } else if (ci === sharpeIdx) {
                        color = sharpeColor(cell);
                        fontWeight = 600;
                      } else if (ci === cagrIdx) {
                        color = cagrColor(cell);
                        fontWeight = 600;
                      } else if (ci === totalIdx) {
                        color = costColor(cell);
                      } else if (ci === slipIdx || ci === impactIdx) {
                        color = 'var(--t2)';
                      } else if (ci === maxDdIdx) {
                        const n = parseFloat(cell);
                        if (Number.isFinite(n)) {
                          const abs = Math.abs(n);
                          color = abs <= 50 ? 'var(--green)' : abs <= 70 ? 'var(--amber)' : 'var(--red)';
                        }
                      }
                      return (
                        <td
                          key={ci}
                          style={{
                            padding: '5px 12px 5px 8px',
                            textAlign: ci === 0 ? 'left' : 'right',
                            color,
                            fontWeight,
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {cell || '\u2014'}
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
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function renderHeatmapSection(body: string, label: string) {
  const grid = parseNumericGrid(body);
  if (grid && grid.data.length >= 2 && grid.colLabels.length >= 2) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <HeatmapGrid rowLabels={grid.rowLabels} colLabels={grid.colLabels} data={grid.data} />
      </div>
    );
  }
  const table = parseColumnarTable(body);
  if (table && table.rows.length > 0) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
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
          <SectionHBarChart items={items} colorFn={(v) => v >= 1.5 ? 'var(--green)' : v >= 0.5 ? 'var(--amber)' : 'var(--red)'} />
          <SectionTable headers={table.headers} rows={table.rows} />
        </div>
      );
    }
  }
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
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
              <th style={{ ...thStyle, textAlign: 'right' }}>Simple%</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Comp%</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Mult</th>
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
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: 'var(--green)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {((row.eq - 1) * 100).toLocaleString(undefined, { maximumFractionDigits: 1 })}%
                  </td>
                  <td style={{ padding: '3px 8px', textAlign: 'right', color: row.totret > 0 ? 'var(--t1)' : 'var(--t3)', whiteSpace: 'nowrap', borderBottom: '1px solid var(--line)' }}>
                    {row.totret > 0 ? (((row.eq - 1) * 100) / row.totret).toFixed(2) : '—'}×
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

function renderGradedMetrics(title: string, body: string) {
  const lines = body.split('\n');

  // Parse header from title: "GATING METRICS  (25/45)  Grade: C"
  const titleMatch = title.match(/^(.+?)\s*\((\d+)\/(\d+)\)\s*Grade:\s*(\S+)/i);
  const sectionName = titleMatch ? titleMatch[1].trim() : title;
  const score = titleMatch ? titleMatch[2] : '';
  const total = titleMatch ? titleMatch[3] : '';
  const grade = titleMatch ? titleMatch[4] : '';

  type GradedMetric = { icon: string; name: string; score: string; description: string; goalActual: string; passed: boolean; warn: boolean };
  const metrics: GradedMetric[] = [];

  let currentMetric: GradedMetric | null = null;

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/GATING METRICS|CORE METRICS|SUPPORTING METRICS/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) {
      // Empty line separates metrics
      if (currentMetric) { metrics.push(currentMetric); currentMetric = null; }
      continue;
    }

    // "✅ Deflated Sharpe Ratio (DSR)  —  10/15" or "❌ Walk-Forward..." or "⚠  Neighbor..."
    const metricMatch = clean.match(/^(✅|❌|⚠)\s+(.+?)\s+—\s+(\d+\/\d+)/);
    if (metricMatch) {
      if (currentMetric) metrics.push(currentMetric);
      currentMetric = {
        icon: metricMatch[1],
        name: metricMatch[2].trim(),
        score: metricMatch[3],
        description: '',
        goalActual: '',
        passed: metricMatch[1] === '✅',
        warn: metricMatch[1] === '⚠',
      };
      continue;
    }

    // "Goal: ≥95%  (≥99.5% ideal)   Actual: 98.94%"
    if (currentMetric && /^Goal:/i.test(clean)) {
      currentMetric.goalActual = clean;
      continue;
    }

    // Description lines
    if (currentMetric) {
      currentMetric.description = currentMetric.description
        ? `${currentMetric.description} ${clean}`
        : clean;
    }
  }
  if (currentMetric) metrics.push(currentMetric);

  if (metrics.length === 0) return <PreFallback body={body} />;

  // Grade color
  const gradeColor = /^A/i.test(grade) ? 'var(--green)' : /^B/i.test(grade) ? '#5bc0de' : /^C/i.test(grade) ? 'var(--orange)' : 'var(--red)';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      {/* Header with score and grade */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
        padding: '10px 16px',
      }}>
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 10 }}>
          {score && total && (
            <span style={{ fontSize: 18, color: 'var(--t1)', fontWeight: 600, ...MONO }}>{score}/{total}</span>
          )}
          <span style={{ fontSize: 10, color: 'var(--t3)', ...MONO }}>points</span>
        </div>
        {grade && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>Grade</span>
            <span style={{
              fontSize: 16, fontWeight: 700, color: gradeColor,
              padding: '2px 10px', borderRadius: 4,
              background: `color-mix(in srgb, ${gradeColor} 10%, transparent)`,
              border: `1px solid color-mix(in srgb, ${gradeColor} 25%, transparent)`,
              ...MONO,
            }}>
              {grade}
            </span>
          </div>
        )}
      </div>

      {/* Metrics */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {metrics.map((m, idx) => {
          const color = m.passed ? 'var(--green)' : m.warn ? 'var(--orange)' : 'var(--red)';
          const bg = m.passed ? 'transparent' : m.warn ? 'rgba(255,160,60,0.03)' : 'rgba(255,60,60,0.03)';
          const border = m.passed ? 'var(--line1)' : m.warn ? 'rgba(255,160,60,0.15)' : 'rgba(255,60,60,0.15)';

          return (
            <div key={idx} style={{
              background: bg, border: `1px solid ${border}`, borderRadius: 6,
              padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 6,
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: 13 }}>{m.icon}</span>
                  <span style={{ fontSize: 11, color: 'var(--t1)', fontWeight: 500, ...MONO }}>{m.name}</span>
                </div>
                <span style={{ fontSize: 11, color, fontWeight: 600, ...MONO }}>{m.score}</span>
              </div>
              {m.description && (
                <span style={{ fontSize: 9.5, color: 'var(--t3)', lineHeight: 1.5, ...MONO }}>{m.description}</span>
              )}
              {m.goalActual && (
                <div style={{ fontSize: 9, color: 'var(--t4)', ...MONO, padding: '4px 8px', background: 'rgba(255,255,255,0.02)', borderRadius: 3 }}>
                  {m.goalActual}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function renderWhatYouHave(body: string) {
  const lines = body.split('\n');
  const items: string[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/WHAT YOU HAVE/i.test(line)) continue;
    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    const match = clean.match(/^✅\s+(.+)/);
    if (match) items.push(match[1].trim());
  }

  if (items.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {items.map((item, idx) => (
          <div key={idx} style={{
            display: 'flex', gap: 8, alignItems: 'flex-start',
            padding: '6px 10px', borderRadius: 4,
            background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
            ...MONO,
          }}>
            <span style={{ fontSize: 11, flexShrink: 0, marginTop: 1, color: 'var(--green)' }}>✓</span>
            <span style={{ fontSize: 10, color: 'var(--t2)', lineHeight: 1.5 }}>{item}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function renderWhatYouNeed(body: string) {
  const lines = body.split('\n');
  const items: { num: string; text: string }[] = [];
  let current: { num: string; text: string } | null = null;

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/WHAT YOU STILL NEED/i.test(line)) continue;
    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) {
      if (current) { items.push(current); current = null; }
      continue;
    }

    // "1. │     DSR below threshold — ..."
    const numMatch = clean.match(/^(\d+)\.\s*│?\s*(.+)/);
    if (numMatch) {
      if (current) items.push(current);
      current = { num: numMatch[1], text: numMatch[2].trim() };
      continue;
    }

    // Continuation line
    if (current) {
      current.text += ` ${clean}`;
    }
  }
  if (current) items.push(current);

  if (items.length === 0) return <PreFallback body={body} />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        {items.map((item, idx) => (
          <div key={idx} style={{
            display: 'flex', gap: 10, alignItems: 'flex-start',
            padding: '8px 12px', borderRadius: 4,
            background: 'var(--bg2)', border: '1px solid rgba(255,160,60,0.12)',
            ...MONO,
          }}>
            <span style={{ fontSize: 12, color: 'var(--orange)', fontWeight: 600, flexShrink: 0, minWidth: 16 }}>{item.num}.</span>
            <span style={{ fontSize: 10, color: 'var(--t2)', lineHeight: 1.5 }}>{item.text}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function renderBottomLine(body: string) {
  const lines = body.split('\n');
  const textParts: string[] = [];

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/BOTTOM LINE/i.test(line)) continue;
    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;
    textParts.push(clean);
  }

  if (textParts.length === 0) return <PreFallback body={body} />;

  const text = textParts.join(' ');

  // Extract score if present: "scores 63/100"
  const scoreMatch = text.match(/scores?\s+(\d+)\/(\d+)/i);
  const score = scoreMatch ? parseInt(scoreMatch[1]) : null;
  const scoreColor = score !== null ? (score >= 80 ? 'var(--green)' : score >= 60 ? 'var(--orange)' : 'var(--red)') : 'var(--t1)';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{
        padding: '14px 18px', borderRadius: 6,
        background: 'var(--bg2)', border: '1px solid var(--line1)',
        ...MONO,
      }}>
        {score !== null && (
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 10 }}>
            <span style={{ fontSize: 24, fontWeight: 700, color: scoreColor }}>{score}</span>
            <span style={{ fontSize: 12, color: 'var(--t3)' }}>/ 100</span>
          </div>
        )}
        <p style={{ fontSize: 10.5, color: 'var(--t2)', lineHeight: 1.6, margin: 0 }}>{text}</p>
      </div>
    </div>
  );
}

function renderAuditChecklist(body: string, label: string) {
  const lines = body.split('\n');

  let filterName = '';
  let passCount = '';
  let totalCount = '';
  let status = '';  // CLEAN or FAIL

  type TestResult = { id: string; name: string; description: string; passed: boolean };
  const tests: TestResult[] = [];
  let summary = '';

  for (const line of lines) {
    if (/^[│┌┐└┘─═]+$/.test(line.trim())) continue;
    if (/DAILY SERIES AUDIT/i.test(line)) continue;

    const clean = line.replace(/^[│\s]+/, '').replace(/[│\s]+$/, '');
    if (!clean) continue;

    // "A - No Filter  |  Passed: 6/6   ✅ CLEAN"
    const headerMatch = clean.match(/^(.+?)\s*\|\s*Passed:\s*(\d+)\/(\d+)\s*(✅|❌)\s*(\S+)/);
    if (headerMatch) {
      filterName = headerMatch[1].trim();
      passCount = headerMatch[2];
      totalCount = headerMatch[3];
      status = headerMatch[5];
      continue;
    }

    // "✅ T11 Filter Zero Injection" or "❌ T11 ..."
    const testMatch = clean.match(/^(✅|❌)\s+(T\d+)\s+(.+)/);
    if (testMatch) {
      tests.push({
        id: testMatch[2],
        name: testMatch[3].trim(),
        description: '',
        passed: testMatch[1] === '✅',
      });
      continue;
    }

    // Description line (indented, follows a test)
    if (tests.length > 0 && !tests[tests.length - 1].description && clean.length > 0 && !/^All \d+/.test(clean)) {
      tests[tests.length - 1].description = clean;
      continue;
    }

    // "All 6 daily-series checks passed."
    if (/^All \d+/.test(clean)) {
      summary = clean;
      continue;
    }

    // "Portfolio-level return construction is causally sound."
    if (/Portfolio-level/i.test(clean)) {
      summary = summary ? `${summary} ${clean}` : clean;
    }
  }

  if (tests.length === 0) return <PreFallback body={body} />;

  const allPassed = status === 'CLEAN';
  const statusColor = allPassed ? 'var(--green)' : 'var(--red)';
  const statusBg = allPassed ? 'rgba(60,255,100,0.05)' : 'rgba(255,60,60,0.05)';
  const statusBorder = allPassed ? 'rgba(60,255,100,0.25)' : 'rgba(255,60,60,0.25)';

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      {/* Header card */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        background: statusBg, border: `1px solid ${statusBorder}`, borderRadius: 6,
        padding: '10px 16px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: 18, color: statusColor, fontWeight: 700, ...MONO }}>{passCount}/{totalCount}</span>
          <span style={{ fontSize: 11, color: 'var(--t2)', ...MONO }}>{filterName}</span>
        </div>
        <span style={{
          fontSize: 10, fontWeight: 600, padding: '3px 10px', borderRadius: 4,
          background: allPassed ? 'rgba(60,255,100,0.12)' : 'rgba(255,60,60,0.12)',
          color: statusColor, ...MONO,
        }}>
          {status}
        </span>
      </div>

      {/* Test results */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {tests.map((t, idx) => (
          <div key={idx} style={{
            display: 'flex', gap: 10, padding: '8px 12px', borderRadius: 4,
            background: idx % 2 === 0 ? 'var(--bg2)' : 'transparent',
            alignItems: 'flex-start',
            ...MONO,
          }}>
            <span style={{ fontSize: 12, flexShrink: 0, marginTop: 1 }}>{t.passed ? '✅' : '❌'}</span>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2, minWidth: 0 }}>
              <div style={{ display: 'flex', gap: 8, alignItems: 'baseline' }}>
                <span style={{ fontSize: 9, color: 'var(--t4)', flexShrink: 0 }}>{t.id}</span>
                <span style={{ fontSize: 10, color: t.passed ? 'var(--t1)' : 'var(--red)', fontWeight: 500 }}>{t.name}</span>
              </div>
              {t.description && (
                <span style={{ fontSize: 9, color: 'var(--t4)', lineHeight: 1.4 }}>{t.description}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Summary */}
      {summary && (
        <div style={{ fontSize: 9.5, color: 'var(--t3)', ...MONO, padding: '6px 12px', background: 'var(--bg2)', borderRadius: 6, border: '1px solid var(--line1)' }}>
          {summary}
        </div>
      )}
    </div>
  );
}

function renderBestFilterHeadline(body: string) {
  const lines = body.split('\n');
  const kv: { key: string; value: string }[] = [];

  for (const line of lines) {
    const clean = line.trim();
    if (!clean) continue;
    const match = clean.match(/^(.+?):\s*(.+)$/);
    if (match) kv.push({ key: match[1].trim(), value: match[2].trim() });
  }

  if (kv.length === 0) return <PreFallback body={body} />;

  function metricColor(key: string, value: string): string {
    const k = key.toLowerCase();
    const num = parseFloat(value.replace(/[,%]/g, ''));

    if (k.includes('sharpe')) return num >= 2.0 ? 'var(--green)' : num >= 1.5 ? 'var(--orange)' : 'var(--red)';
    if (k.includes('cagr')) return num >= 0 ? 'var(--green)' : 'var(--red)';
    if (k.includes('max dd')) return num <= -50 ? 'var(--red)' : num <= -30 ? 'var(--orange)' : 'var(--green)';
    if (k.includes('dsr')) return num >= 95 ? 'var(--green)' : num >= 70 ? 'var(--orange)' : 'var(--red)';
    if (k.includes('grade')) return num >= 80 ? 'var(--green)' : num >= 60 ? 'var(--orange)' : 'var(--red)';
    return 'var(--t1)';
  }

  function metricSize(key: string): number {
    const k = key.toLowerCase();
    if (k.includes('sharpe')) return 22;
    if (k.includes('filter')) return 13;
    return 18;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {kv.map((item, idx) => {
          const isFilter = item.key.toLowerCase().includes('filter');
          const color = isFilter ? 'var(--t1)' : metricColor(item.key, item.value);
          const size = metricSize(item.key);

          return (
            <div key={idx} style={{
              flex: isFilter ? '1 1 100%' : '1 1 130px',
              background: 'var(--bg2)',
              border: `1px solid ${isFilter ? 'var(--line1)' : color === 'var(--green)' ? 'rgba(60,255,100,0.15)' : color === 'var(--red)' ? 'rgba(255,60,60,0.15)' : color === 'var(--orange)' ? 'rgba(255,160,60,0.15)' : 'var(--line1)'}`,
              borderRadius: 6,
              padding: isFilter ? '8px 16px' : '12px 16px',
              display: 'flex',
              flexDirection: isFilter ? 'row' : 'column',
              gap: isFilter ? 8 : 4,
              alignItems: isFilter ? 'center' : 'flex-start',
            }}>
              <span style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>{item.key}</span>
              <span style={{ fontSize: size, color, fontWeight: isFilter ? 500 : 600, ...MONO }}>{item.value}</span>
            </div>
          );
        })}
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

// ── SHARPE STABILITY ANALYSIS (walk-forward folds) ──────────────────────────
function renderSharpeStability(body: string): React.ReactNode {
  let nFolds: number | null = null;
  let meanSharpe: number | null = null;
  let stdDev: number | null = null;
  let pctGt2: string | null = null;
  let ciLow: number | null = null;
  let ciHigh: number | null = null;
  let tStat: number | null = null;
  let pValue: number | null = null;
  let significance: string | null = null;

  for (const raw of body.split('\n')) {
    const line = raw.replace(/^[│┌└─\s]*/, '').trim();
    if (!line) continue;
    const foldsM = line.match(/^Folds:\s*(\d+)/i);
    if (foldsM) { nFolds = parseInt(foldsM[1]); continue; }
    const meanM = line.match(/^Mean OOS Sharpe:\s*([-\d.]+)/i);
    if (meanM) { meanSharpe = parseFloat(meanM[1]); continue; }
    const stdM = line.match(/^Sharpe Std Dev:\s*([-\d.]+)/i);
    if (stdM) { stdDev = parseFloat(stdM[1]); continue; }
    const pctM = line.match(/%\s*Folds\s*>\s*2\.0:\s*([\d.]+%)/i);
    if (pctM) { pctGt2 = pctM[1]; continue; }
    const ciM = line.match(/^95%\s*CI:\s*\[([-\d.]+),\s*([-\d.]+)\]/i);
    if (ciM) { ciLow = parseFloat(ciM[1]); ciHigh = parseFloat(ciM[2]); continue; }
    const tM = line.match(/^T-stat.*?:\s*([-\d.]+)/i);
    if (tM) { tStat = parseFloat(tM[1]); continue; }
    const pM = line.match(/^P-value:\s*([\d.]+)\s*(.*)/i);
    if (pM) { pValue = parseFloat(pM[1]); significance = pM[2].replace(/[✅⚠❌]/g, '').trim(); }
  }

  if (meanSharpe === null) return <PreFallback body={body} />;

  const isSig = pValue !== null && pValue < 0.05;
  const isMarginal = pValue !== null && pValue >= 0.05 && pValue < 0.10;
  const sigColor = isSig ? 'var(--green)' : isMarginal ? 'var(--amber)' : 'var(--red)';
  const sigLabel = isSig ? 'SIGNIFICANT' : isMarginal ? 'MARGINAL' : 'NOT SIGNIFICANT';
  const sharpeColor = meanSharpe >= 3.0 ? 'var(--green)' : meanSharpe >= 2.0 ? 'var(--amber)' : 'var(--red)';

  const stat = (label: string, value: string, color?: string, sub?: string) => (
    <div style={{
      background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
      padding: '8px 10px', display: 'flex', flexDirection: 'column' as const, gap: 3,
      boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.02)',
    }}>
      <div style={{ fontSize: 8.5, color: 'var(--t3)', textTransform: 'uppercase' as const, letterSpacing: 0.5, ...MONO }}>
        {label}
      </div>
      <div style={{ fontSize: 14, color: color ?? 'var(--t1)', fontWeight: 500, letterSpacing: -0.5, ...MONO }}>
        {value}
      </div>
      {sub && (
        <div style={{ fontSize: 9, color: 'var(--t4)', fontStyle: 'italic' as const, paddingTop: 2, ...MONO }}>
          {sub}
        </div>
      )}
    </div>
  );

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, fontSize: 9, ...MONO }}>
      {/* Significance banner */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 10, padding: '8px 14px',
        background: isSig ? 'rgba(80,200,120,0.08)' : isMarginal ? 'rgba(240,180,40,0.08)' : 'rgba(220,80,60,0.08)',
        border: `1px solid ${sigColor}`,
        borderRadius: 6,
      }}>
        <span style={{ fontSize: 14 }}>{isSig ? '✅' : '⚠'}</span>
        <span style={{ color: sigColor, fontWeight: 700, fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.08em' }}>
          {sigLabel}
        </span>
        {pValue !== null && (
          <span style={{ color: 'var(--t3)', fontSize: 9, marginLeft: 'auto' }}>
            p = {pValue < 0.001 ? pValue.toExponential(2) : pValue.toFixed(6)}
          </span>
        )}
        {nFolds !== null && (
          <span style={{ color: 'var(--t4)', fontSize: 9 }}>
            {nFolds} folds
          </span>
        )}
      </div>

      {/* Stat cards grid */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(120px, 1fr))',
        gap: 8,
      }}>
        {stat('Mean OOS Sharpe', meanSharpe.toFixed(3), sharpeColor)}
        {stdDev !== null && stat('Std Dev', stdDev.toFixed(3), undefined,
          stdDev > 0 && meanSharpe > 0
            ? `CV = ${(stdDev / meanSharpe).toFixed(2)}`
            : undefined,
        )}
        {pctGt2 !== null && stat('% Folds > 2.0', pctGt2,
          parseFloat(pctGt2) >= 75 ? 'var(--green)' : parseFloat(pctGt2) >= 50 ? 'var(--amber)' : 'var(--red)',
        )}
        {ciLow !== null && ciHigh !== null && stat(
          '95% Confidence Interval',
          `[${ciLow.toFixed(3)}, ${ciHigh.toFixed(3)}]`,
          ciLow > 0 ? 'var(--green)' : 'var(--amber)',
          ciLow > 0 ? 'Lower bound > 0' : 'Interval includes zero',
        )}
        {tStat !== null && stat('T-Statistic (vs 0)', tStat.toFixed(3),
          tStat >= 2.0 ? 'var(--green)' : tStat >= 1.5 ? 'var(--amber)' : 'var(--red)',
        )}
        {pValue !== null && stat('P-Value', pValue < 0.001 ? pValue.toExponential(2) : pValue.toFixed(6), sigColor,
          significance ?? undefined,
        )}
      </div>
    </div>
  );
}

// ── WALK-FORWARD VALIDATION (expanding window) ──────────────────────────────
function renderWalkForwardValidation(title: string, body: string): React.ReactNode {
  // Parse fold count & window type from title
  const titleM = title.match(/\((\d+)\s+folds?,\s*(\w[\w\s]*)\)/i);
  const foldCount = titleM ? parseInt(titleM[1]) : null;
  const windowType = titleM ? titleM[2].trim() : null;

  interface WFVFold {
    num: number; train: string; test: string; days: number;
    sharpe: number; cagr: number; maxdd: number; sortino: number;
    r2: number; dsr: number; fp: number;
  }

  const folds: WFVFold[] = [];
  let meanSharpe: number | null = null;
  let stdSharpe: number | null = null;
  let minSharpe: number | null = null;
  let maxSharpe: number | null = null;
  let meanCagr: string | null = null;
  let meanMaxdd: string | null = null;
  let meanSortino: string | null = null;
  let meanR2: string | null = null;
  let meanDsr: number | null = null;
  let dsrStatus: string | null = null;
  let pctPositive: number | null = null;
  let cv: number | null = null;
  let cvStable: boolean | null = null;
  let cvNote: string | null = null;
  let notes: string[] = [];

  for (const rawLine of body.split('\n')) {
    const stripped = rawLine.replace(/^\s*│\s*/, '').trim();
    if (!stripped || /^[─└┌┐┘]+$/.test(stripped) || /^Fold\s+Train\s+Test/i.test(stripped) || /^────/.test(stripped)) continue;

    // Note lines at top
    if (/^Each fold:/i.test(stripped) || /^DSR per fold/i.test(stripped)) {
      notes.push(stripped);
      continue;
    }

    // Data row: num  train  test  days  sharpe  cagr  maxdd  sortino  r2  dsr  fp
    const dm = stripped.match(/^\s*(\d+)\s+([\w-]+)\s+([\w-]+\s*-\d+)\s+(\d+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)/);
    if (dm) {
      folds.push({
        num: parseInt(dm[1]), train: dm[2], test: dm[3].trim(), days: parseInt(dm[4]),
        sharpe: parseFloat(dm[5]), cagr: parseFloat(dm[6]), maxdd: parseFloat(dm[7]),
        sortino: parseFloat(dm[8]), r2: parseFloat(dm[9]), dsr: parseFloat(dm[10]),
        fp: parseFloat(dm[11]),
      });
      continue;
    }

    // Aggregates
    const mshM = stripped.match(/^Mean Sharpe:\s*([-\d.]+)\s+\(±([\d.]+)\s+min=([-\d.]+)\s+max=([-\d.]+)\)/i);
    if (mshM) { meanSharpe = parseFloat(mshM[1]); stdSharpe = parseFloat(mshM[2]); minSharpe = parseFloat(mshM[3]); maxSharpe = parseFloat(mshM[4]); continue; }
    const mcagrM = stripped.match(/^Mean CAGR:\s*([-\d.%]+)/i);
    if (mcagrM) { meanCagr = mcagrM[1]; continue; }
    const mmddM = stripped.match(/^Mean MaxDD:\s*([-\d.%]+)/i);
    if (mmddM) { meanMaxdd = mmddM[1]; continue; }
    const msortM = stripped.match(/^Mean Sortino:\s*([-\d.]+)/i);
    if (msortM) { meanSortino = msortM[1]; continue; }
    const mr2M = stripped.match(/^Mean R²:\s*([-\d.]+)/i);
    if (mr2M) { meanR2 = mr2M[1]; continue; }
    const mdsrM = stripped.match(/^Mean OOS DSR:\s*([\d.]+)%\s*(.*)/i);
    if (mdsrM) { meanDsr = parseFloat(mdsrM[1]); dsrStatus = mdsrM[2].replace(/[⚠✅❌]/g, '').trim(); continue; }
    const pctM = stripped.match(/%\s*folds positive.*?:\s*(\d+)%/i);
    if (pctM) { pctPositive = parseInt(pctM[1]); continue; }
    const stabM = stripped.match(/^Stability\s*\(CV=([\d.]+)\):\s*(.*)/i);
    if (stabM) {
      cv = parseFloat(stabM[1]);
      cvStable = /STABLE/i.test(stabM[2]) && !/UNSTABLE/i.test(stabM[2]);
      cvNote = stabM[2].replace(/[⚠✅❌]/g, '').trim();
      continue;
    }
  }

  if (folds.length === 0) return <PreFallback body={body} />;

  const sharpeColor = (s: number) => s >= 3.0 ? 'var(--green)' : s >= 2.0 ? 'var(--amber)' : 'var(--red)';
  const maxddColor  = (v: number) => Math.abs(v) <= 20 ? 'var(--green)' : Math.abs(v) <= 35 ? 'var(--amber)' : 'var(--red)';
  const dsrColor    = (v: number) => v >= 95 ? 'var(--green)' : v >= 80 ? 'var(--amber)' : 'var(--red)';
  const r2Color     = (v: number) => v >= 0.7 ? 'var(--green)' : v >= 0.4 ? 'var(--amber)' : 'var(--red)';
  const sortinoColor = (v: number) => v >= 3.0 ? 'var(--green)' : v >= 1.0 ? 'var(--amber)' : 'var(--red)';
  const fmt = (v: number, d: number, suf = '') => `${v.toFixed(d)}${suf}`;

  // Sharpe bar scaling
  const allSharpes = folds.map((f) => f.sharpe);
  const maxSh = Math.max(...allSharpes);
  const minSh = Math.min(...allSharpes);

  const thS: React.CSSProperties = {
    padding: '3px 8px 3px 5px', textAlign: 'right', fontWeight: 700,
    textTransform: 'uppercase', letterSpacing: '0.08em', fontSize: 8,
    color: 'var(--t3)', borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap',
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, fontSize: 9, ...MONO }}>
      {/* Config bar */}
      <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'center', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        {foldCount !== null && (
          <span style={{ color: 'var(--t2)' }}>
            <span style={{ color: 'var(--t1)', fontWeight: 700 }}>{foldCount}</span> folds
          </span>
        )}
        {windowType && (
          <span style={{ color: 'var(--t3)' }}>{windowType}</span>
        )}
        {notes.length > 0 && (
          <span style={{ marginLeft: 'auto', color: 'var(--t4)', fontSize: 8 }}>
            {notes.join(' · ')}
          </span>
        )}
      </div>

      {/* Fold table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
          <thead>
            <tr>
              <th style={{ ...thS, textAlign: 'left', paddingLeft: 5 }}>#</th>
              <th style={{ ...thS, textAlign: 'left' }}>Train</th>
              <th style={{ ...thS, textAlign: 'left' }}>Test</th>
              <th style={{ ...thS }}>Days</th>
              <th style={{ ...thS, minWidth: 88 }}>Sharpe</th>
              <th style={{ ...thS }}>CAGR%</th>
              <th style={{ ...thS }}>MaxDD%</th>
              <th style={{ ...thS }}>Sortino</th>
              <th style={{ ...thS }}>R²</th>
              <th style={{ ...thS }}>DSR%</th>
              <th style={{ ...thS }}>FP%</th>
            </tr>
          </thead>
          <tbody>
            {folds.map((fold) => {
              const isNeg = fold.sharpe < 0;
              const frac = maxSh > minSh
                ? Math.max(0, (fold.sharpe - minSh) / (maxSh - minSh))
                : fold.sharpe > 0 ? 1 : 0;
              const rowBg = isNeg ? 'rgba(220,80,60,0.06)'
                : fold.num % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent';
              return (
                <tr key={fold.num} style={{ background: rowBg }}>
                  <td style={{ padding: '3px 8px 3px 5px', color: 'var(--t3)', fontSize: 8 }}>{fold.num}</td>
                  <td style={{ padding: '3px 10px 3px 5px', whiteSpace: 'nowrap', color: 'var(--t3)', fontSize: 8 }}>{fold.train}</td>
                  <td style={{ padding: '3px 10px 3px 5px', whiteSpace: 'nowrap', color: 'var(--t2)' }}>{fold.test}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: 'var(--t3)' }}>{fold.days}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5, justifyContent: 'flex-end' }}>
                      <div style={{ width: 36, height: 3, background: 'var(--bg3)', borderRadius: 1, flexShrink: 0 }}>
                        {fold.sharpe > 0 && (
                          <div style={{ width: `${Math.round(frac * 100)}%`, height: '100%', background: sharpeColor(fold.sharpe), borderRadius: 1 }} />
                        )}
                      </div>
                      <span style={{ color: sharpeColor(fold.sharpe), fontWeight: isNeg ? 700 : 400 }}>{fmt(fold.sharpe, 3)}</span>
                    </div>
                  </td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: fold.cagr >= 0 ? 'var(--t2)' : 'var(--red)' }}>{fold.cagr.toLocaleString()}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: maxddColor(fold.maxdd) }}>{fmt(fold.maxdd, 2, '%')}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: sortinoColor(fold.sortino) }}>{fmt(fold.sortino, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: r2Color(fold.r2) }}>{fmt(fold.r2, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: dsrColor(fold.dsr) }}>{fmt(fold.dsr, 1, '%')}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: fold.fp > 50 ? 'var(--red)' : 'var(--t3)' }}>{fmt(fold.fp, 1, '%')}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Aggregate summary */}
      {meanSharpe !== null && (
        <div style={{ display: 'flex', gap: 18, flexWrap: 'wrap', padding: '6px 10px', background: 'var(--bg0)', border: '1px solid var(--line)', borderRadius: 3, alignItems: 'center' }}>
          <span style={{ color: 'var(--t3)', fontSize: 8, textTransform: 'uppercase', letterSpacing: '0.1em' }}>
            Aggregate — {folds.length} folds
          </span>
          <span style={{ color: 'var(--t2)' }}>
            Mean SR <span style={{ color: sharpeColor(meanSharpe), fontWeight: 700 }}>{meanSharpe.toFixed(3)}</span>
            {stdSharpe !== null && <span style={{ color: 'var(--t3)' }}> ±{stdSharpe.toFixed(3)}</span>}
            {minSharpe !== null && maxSharpe !== null && (
              <span style={{ color: 'var(--t4)', fontSize: 8 }}> [{minSharpe.toFixed(3)}, {maxSharpe.toFixed(3)}]</span>
            )}
          </span>
          {meanDsr !== null && (
            <span style={{ color: 'var(--t2)' }}>
              DSR <span style={{ color: dsrColor(meanDsr) }}>{meanDsr.toFixed(1)}%</span>
              {dsrStatus && (
                <span style={{ marginLeft: 4, color: /PASS/i.test(dsrStatus) ? 'var(--green)' : /FAIL/i.test(dsrStatus) ? 'var(--red)' : 'var(--amber)', fontSize: 8 }}>
                  {/PASS/i.test(dsrStatus) ? '✅' : /FAIL/i.test(dsrStatus) ? '❌' : '⚠'} {dsrStatus}
                </span>
              )}
            </span>
          )}
          {pctPositive !== null && (
            <span style={{ color: 'var(--t2)' }}>
              Positive <span style={{ color: pctPositive >= 80 ? 'var(--green)' : pctPositive >= 60 ? 'var(--amber)' : 'var(--red)' }}>{pctPositive}%</span>
            </span>
          )}
          {cv !== null && (
            <span style={{ color: 'var(--t2)' }}>
              CV=<span style={{ color: cvStable ? 'var(--green)' : 'var(--amber)', fontWeight: 700 }}>{cv.toFixed(2)}</span>
              <span style={{ marginLeft: 5, color: cvStable ? 'var(--green)' : 'var(--amber)' }}>
                {cvStable ? '✅ STABLE' : '⚠ UNSTABLE'}
              </span>
            </span>
          )}
        </div>
      )}

      {/* Additional aggregate stats */}
      {(meanCagr || meanMaxdd || meanSortino || meanR2) && (
        <div style={{ display: 'flex', gap: 18, flexWrap: 'wrap', padding: '4px 10px', alignItems: 'center' }}>
          {meanCagr && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean CAGR <span style={{ color: 'var(--t2)' }}>{meanCagr}</span></span>}
          {meanMaxdd && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean MaxDD <span style={{ color: 'var(--t2)' }}>{meanMaxdd}</span></span>}
          {meanSortino && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean Sortino <span style={{ color: 'var(--t2)' }}>{meanSortino}</span></span>}
          {meanR2 && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean R² <span style={{ color: 'var(--t2)' }}>{meanR2}</span></span>}
        </div>
      )}

      {/* CV note */}
      {cvNote && !cvStable && (
        <div style={{ padding: '4px 10px', fontSize: 8, color: 'var(--amber)' }}>
          {cvNote}
        </div>
      )}
    </div>
  );
}

// ── WALK-FORWARD ROLLING ─────────────────────────────────────────────────────
function renderWalkForwardRolling(title: string, body: string): React.ReactNode {
  const paramM = title.match(/\(train=(\d+)d\s+test=(\d+)d\s+step=(\d+)d\)/i);
  const trainD = paramM ? parseInt(paramM[1]) : null;
  const testD  = paramM ? parseInt(paramM[2]) : null;
  const stepD  = paramM ? parseInt(paramM[3]) : null;
  const isFilterAware = /FILTER.AWARE/i.test(title);

  interface WFRFold {
    num: number; dateFrom: string; dateTo: string;
    trainRange: string; testRange: string;
    isSaturated: boolean; isUnstable: boolean; isExcluded: boolean;
    isSharpe: number | null; isCagr: string | null; isMaxdd: number | null;
    oosSharpe: number | null; oosCagr: string | null; oosMaxdd: number | null;
    oosSortino: number | null; oosR2: number | null; oosDsr: number | null;
    activeDays: number | null; flatDays: number | null;
    dailyBreakdown: DailyRow[] | null;
    dailySummary: DailySummary | null;
  }

  interface DailyRow {
    day: number; date: string; ret: number; cumul: number;
    type: 'WIN' | 'LOSS'; isBig: boolean;
  }

  interface DailySummary {
    winDays: number; totalDays: number; winPct: number; lossPct: number;
    avgDaily: number; avgWin: number; avgLoss: number;
    worstDay: number; bestDay: number; pattern: string;
  }

  const folds: WFRFold[] = [];
  let curFold: WFRFold | null = null;
  let inDailyBreakdown = false;
  let dailyRows: DailyRow[] = [];
  let dailySummary: Partial<DailySummary> = {};
  const notes: string[] = [];

  // Aggregates
  let meanSharpe: number | null = null;
  let stdSharpe: number | null = null;
  let minSharpe: number | null = null;
  let maxSharpe: number | null = null;
  let robustScore: number | null = null;
  let meanCagr: string | null = null;
  let meanMaxdd: string | null = null;
  let meanSortino: string | null = null;
  let meanR2: string | null = null;
  let meanDsr: number | null = null;
  let dsrStatus: string | null = null;
  let pctPositive: number | null = null;
  let cv: number | null = null;
  let cvStable: boolean | null = null;
  let saturatedCount = 0;
  let totalFoldsCount = 0;
  let activeFoldsUsed = 0;
  let unstableCount = 0;
  const oiCorrs: Array<{ label: string; value: string }> = [];
  let inOI = false;

  const finishDailyBreakdown = () => {
    if (curFold && dailyRows.length > 0) {
      curFold.dailyBreakdown = [...dailyRows];
      curFold.dailySummary = (dailySummary.winDays !== undefined && dailySummary.totalDays !== undefined)
        ? dailySummary as DailySummary : null;
    }
    dailyRows = [];
    dailySummary = {};
    inDailyBreakdown = false;
  };

  for (const rawLine of body.split('\n')) {
    const stripped = rawLine.replace(/^\s*│\s*/, '').replace(/^[└─┌┐┘]+$/, '').trim();
    if (!stripped) continue;

    // Notes at top
    if (/^Params fixed|^Filter applied|^0% return|^Calendar-saturation|^Saturated folds \(\d/i.test(stripped) && folds.length === 0 && !curFold) {
      notes.push(stripped);
      continue;
    }
    if (/^DSR per fold/i.test(stripped) && folds.length === 0 && !curFold) {
      notes.push(stripped);
      continue;
    }
    if (/^OI regime:/i.test(stripped) && folds.length === 0) {
      notes.push(stripped);
      continue;
    }

    // Fold header — with or without dates
    const foldMDated = stripped.match(/^FOLD\s+(\d+)\s+Train:\s*([\w-]+)\s*\(\d+d\)\s+Test:\s*([\w-]+)\s*\(\d+d\)\s+\((\d{4}-\d{2}-\d{2})\s*->\s*(\d{4}-\d{2}-\d{2})\)/i);
    const foldMNoDates = !foldMDated ? stripped.match(/^FOLD\s+(\d+)\s+Train:\s*([\w-]+)\s*\(\d+d\)\s+Test:\s*([\w-]+)\s*\(\d+d\)/i) : null;
    const foldM = foldMDated || foldMNoDates;
    if (foldM) {
      if (inDailyBreakdown) finishDailyBreakdown();
      if (curFold) folds.push(curFold);
      const sat = /⊘\s*SATURATED/i.test(stripped);
      curFold = {
        num: parseInt(foldM[1]), trainRange: foldM[2], testRange: foldM[3],
        dateFrom: foldMDated ? foldMDated[4] : '',
        dateTo: foldMDated ? foldMDated[5] : '',
        isSaturated: sat, isUnstable: /⚠.*unstable/i.test(stripped), isExcluded: sat,
        isSharpe: null, isCagr: null, isMaxdd: null,
        oosSharpe: null, oosCagr: null, oosMaxdd: null,
        oosSortino: null, oosR2: null, oosDsr: null,
        activeDays: null, flatDays: null,
        dailyBreakdown: null, dailySummary: null,
      };
      continue;
    }

    // IS line
    const isM = stripped.match(/^In-sample.*?Sharpe=\s*([-\d.]+).*?CAGR=\s*([-\d.,%]+).*?MaxDD=\s*([-\d.]+)%/i);
    if (isM && curFold) {
      curFold.isSharpe = parseFloat(isM[1]);
      curFold.isCagr = isM[2].trim();
      curFold.isMaxdd = parseFloat(isM[3]);
      continue;
    }

    // OOS line — with or without active/flat days
    const oosM = stripped.match(/^OOS.*?Sharpe=\s*([-\d.]+).*?CAGR=\s*([-\d.,%]+).*?MaxDD=\s*([-\d.]+)%\s+Sortino=\s*([-\d.]+)\s+R²=([-\d.]+)\s+DSR=\s*([-\d.]+)%/i);
    if (oosM && curFold) {
      curFold.oosSharpe = parseFloat(oosM[1]);
      curFold.oosCagr = oosM[2].trim();
      curFold.oosMaxdd = parseFloat(oosM[3]);
      curFold.oosSortino = parseFloat(oosM[4]);
      curFold.oosR2 = parseFloat(oosM[5]);
      curFold.oosDsr = parseFloat(oosM[6]);
      const actM = stripped.match(/\[active=(\d+)d\s+flat=(\d+)d\]/i);
      if (actM) { curFold.activeDays = parseInt(actM[1]); curFold.flatDays = parseInt(actM[2]); }
      if (/EXCLUDED/i.test(stripped)) curFold.isExcluded = true;
      continue;
    }

    // Unstable fold daily breakdown start
    if (/UNSTABLE FOLD.*DAILY RETURN/i.test(stripped) || /UNSTABLE FOLD DETAIL/i.test(stripped)) {
      inDailyBreakdown = true;
      continue;
    }
    // Daily breakdown header / separator
    if (inDailyBreakdown && (/^Day\s+Date/i.test(stripped) || /^────/.test(stripped))) continue;

    // Daily data row
    if (inDailyBreakdown) {
      const dayM = stripped.match(/^(\d+)\s+(\S+)\s+([-\d.]+)%\s+([-\d.]+)%\s+(WIN|LOSS)/i);
      if (dayM) {
        dailyRows.push({
          day: parseInt(dayM[1]), date: dayM[2],
          ret: parseFloat(dayM[3]), cumul: parseFloat(dayM[4]),
          type: dayM[5].toUpperCase() as 'WIN' | 'LOSS',
          isBig: /BIG/i.test(stripped),
        });
        continue;
      }
      // Daily summary lines
      const winM = stripped.match(/^Win days:\s+(\d+)\/(\d+)\s+\((\d+)%\)/i);
      if (winM) { dailySummary.winDays = parseInt(winM[1]); dailySummary.totalDays = parseInt(winM[2]); dailySummary.winPct = parseInt(winM[3]); continue; }
      const lossM = stripped.match(/^Loss days:\s+\d+\/\d+\s+\((\d+)%\)/i);
      if (lossM) { dailySummary.lossPct = parseInt(lossM[1]); continue; }
      const avgDM = stripped.match(/^Avg daily:\s+([-+\d.]+)%/i);
      if (avgDM) { dailySummary.avgDaily = parseFloat(avgDM[1]); continue; }
      const avgWM = stripped.match(/^Avg win:\s+\+?([-\d.]+)%/i);
      if (avgWM) { dailySummary.avgWin = parseFloat(avgWM[1]); continue; }
      const avgLM = stripped.match(/^Avg loss:\s+([-\d.]+)%/i);
      if (avgLM) { dailySummary.avgLoss = parseFloat(avgLM[1]); continue; }
      const worstM = stripped.match(/^Worst day:\s+([-\d.]+)%/i);
      if (worstM) { dailySummary.worstDay = parseFloat(worstM[1]); continue; }
      const bestM = stripped.match(/^Best day:\s+\+?([-\d.]+)%/i);
      if (bestM) { dailySummary.bestDay = parseFloat(bestM[1]); continue; }
      const patM = stripped.match(/^Pattern:\s+(.+)/i);
      if (patM) { dailySummary.pattern = patM[1].trim(); continue; }
      // Arrow continuation line for pattern
      if (/^→/.test(stripped) && dailySummary.pattern) {
        dailySummary.pattern += ' ' + stripped;
        continue;
      }
      // Filter detail line
      if (/^Filter blocked/i.test(stripped)) continue;
    }

    // Counts
    const satCountM = stripped.match(/^Saturated folds excluded.*?(\d+)\/(\d+).*?active folds used:\s*(\d+)/i);
    if (satCountM) {
      if (inDailyBreakdown) finishDailyBreakdown();
      if (curFold) { folds.push(curFold); curFold = null; }
      saturatedCount = parseInt(satCountM[1]); totalFoldsCount = parseInt(satCountM[2]); activeFoldsUsed = parseInt(satCountM[3]);
      continue;
    }
    const unstCountM = stripped.match(/^Unstable folds.*?:\s*(\d+)\/(\d+)/i);
    if (unstCountM) {
      if (inDailyBreakdown) finishDailyBreakdown();
      if (curFold) { folds.push(curFold); curFold = null; }
      unstableCount = parseInt(unstCountM[1]);
      totalFoldsCount = totalFoldsCount || parseInt(unstCountM[2]);
      continue;
    }

    // Aggregates
    if (/^(PRIMARY )?AGGREGATE/i.test(stripped)) {
      if (inDailyBreakdown) finishDailyBreakdown();
      if (curFold) { folds.push(curFold); curFold = null; }
      const countM = stripped.match(/(\d+)\s+(VALID\s+)?FOLDS/i);
      if (countM) activeFoldsUsed = activeFoldsUsed || parseInt(countM[1]);
      continue;
    }
    const mshM = stripped.match(/^Mean Sharpe:\s*([-\d.]+)\s+\(±([\d.]+)(?:\s+min=([-\d.]+)\s+max=([-\d.]+))?\)/i);
    if (mshM) { meanSharpe = parseFloat(mshM[1]); stdSharpe = parseFloat(mshM[2]); minSharpe = mshM[3] ? parseFloat(mshM[3]) : null; maxSharpe = mshM[4] ? parseFloat(mshM[4]) : null; continue; }
    const robM = stripped.match(/^Robust score:\s*([-\d.]+)/i);
    if (robM) { robustScore = parseFloat(robM[1]); continue; }
    const mcagrM = stripped.match(/^Mean CAGR:\s*([-\d.,%]+)/i);
    if (mcagrM) { meanCagr = mcagrM[1]; continue; }
    const mmddM = stripped.match(/^Mean MaxDD:\s*([-\d.%]+)/i);
    if (mmddM) { meanMaxdd = mmddM[1]; continue; }
    const msortM = stripped.match(/^Mean Sortino:\s*([-\d.]+)/i);
    if (msortM) { meanSortino = msortM[1]; continue; }
    const mr2M = stripped.match(/^Mean R²:\s*([-\d.]+)/i);
    if (mr2M) { meanR2 = mr2M[1]; continue; }
    const mdsrM = stripped.match(/^Mean OOS DSR:\s*([\d.]+)%\s*(.*)/i);
    if (mdsrM) { meanDsr = parseFloat(mdsrM[1]); dsrStatus = mdsrM[2].replace(/[⚠✅❌]/g, '').trim(); continue; }
    const pctM = stripped.match(/%\s*folds positive.*?:\s*(\d+)%/i);
    if (pctM) { pctPositive = parseInt(pctM[1]); continue; }
    const stabM = stripped.match(/^Stability\s*\(CV=([\d.]+)\):\s*(.*)/i);
    if (stabM) { cv = parseFloat(stabM[1]); cvStable = /STABLE/i.test(stabM[2]) && !/UNSTABLE/i.test(stabM[2]); continue; }

    // OI correlations
    if (/^── OI Regime Diagnostics/i.test(stripped)) { inOI = true; continue; }
    if (inOI) {
      const corrM = stripped.match(/^Spearman corr\((.+?)\)\s*=\s*(\S+)/i);
      if (corrM && corrM[2] !== 'n/a') oiCorrs.push({ label: corrM[1].trim(), value: corrM[2] });
    }
  }
  if (inDailyBreakdown) finishDailyBreakdown();
  if (curFold) folds.push(curFold);
  if (folds.length === 0) return <PreFallback body={body} />;

  // Sharpe range for bar scaling (active folds only)
  const activeSharpes = folds.filter((f) => !f.isExcluded && f.oosSharpe !== null).map((f) => f.oosSharpe!);
  const maxS = activeSharpes.length > 0 ? Math.max(...activeSharpes) : 0;
  const minS = activeSharpes.length > 0 ? Math.min(...activeSharpes) : 0;

  const sharpeColor = (s: number | null) => s === null ? 'var(--t3)' : s >= 3.0 ? 'var(--green)' : s >= 2.0 ? 'var(--amber)' : 'var(--red)';
  const maxddColor  = (v: number | null) => v === null ? 'var(--t3)' : Math.abs(v) <= 20 ? 'var(--green)' : Math.abs(v) <= 35 ? 'var(--amber)' : 'var(--red)';
  const dsrColor    = (v: number | null) => v === null ? 'var(--t3)' : v >= 95 ? 'var(--green)' : v >= 80 ? 'var(--amber)' : 'var(--red)';
  const r2Color     = (v: number | null) => v === null ? 'var(--t3)' : v >= 0.7 ? 'var(--green)' : v >= 0.4 ? 'var(--amber)' : 'var(--red)';
  const sortinoColor = (v: number | null) => v === null ? 'var(--t3)' : v >= 3.0 ? 'var(--green)' : v >= 1.0 ? 'var(--amber)' : 'var(--red)';
  const fmt = (v: number | null, d: number, suf = '') => v === null ? '—' : `${v.toFixed(d)}${suf}`;

  const thS: React.CSSProperties = {
    padding: '3px 8px 3px 5px', textAlign: 'right', fontWeight: 700,
    textTransform: 'uppercase', letterSpacing: '0.08em', fontSize: 8,
    color: 'var(--t3)', borderBottom: '1px solid var(--line2)', whiteSpace: 'nowrap',
  };

  const hasDates = folds.some((f) => f.dateFrom);
  const hasActiveDays = folds.some((f) => f.activeDays !== null);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, fontSize: 9, ...MONO }}>
      {/* Config bar */}
      <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', alignItems: 'center', paddingBottom: 8, borderBottom: '1px solid var(--line)' }}>
        {trainD !== null && (
          <span style={{ color: 'var(--t2)' }}>
            train <span style={{ color: 'var(--t1)', fontWeight: 700 }}>{trainD}d</span>
            {' · '}test <span style={{ color: 'var(--t1)', fontWeight: 700 }}>{testD}d</span>
            {' · '}step <span style={{ color: 'var(--t1)', fontWeight: 700 }}>{stepD}d</span>
          </span>
        )}
        {totalFoldsCount > 0 && <span style={{ color: 'var(--t3)' }}>{totalFoldsCount} folds</span>}
        {saturatedCount > 0 && (
          <span style={{ color: 'var(--t3)' }}>
            <span style={{ color: 'var(--amber)', fontWeight: 700 }}>{saturatedCount}</span> saturated excluded
          </span>
        )}
        {unstableCount > 0 && (
          <span style={{ color: 'var(--t3)' }}>
            <span style={{ color: 'var(--red)', fontWeight: 700 }}>{unstableCount}</span> unstable
          </span>
        )}
        {isFilterAware && (
          <span style={{ marginLeft: 'auto', color: 'var(--t4)', fontSize: 8 }}>filter on both windows · 0% return on flat days</span>
        )}
      </div>

      {/* Notes */}
      {notes.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2, padding: '0 2px' }}>
          {notes.map((n, i) => (
            <span key={i} style={{ color: 'var(--t4)', fontSize: 8 }}>{n}</span>
          ))}
        </div>
      )}

      {/* Fold table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 9, tableLayout: 'auto', ...MONO }}>
          <thead>
            <tr>
              <th style={{ ...thS, textAlign: 'left', paddingLeft: 5 }}>#</th>
              {hasDates && <th style={{ ...thS, textAlign: 'left' }}>OOS Period</th>}
              <th style={{ ...thS, textAlign: 'left' }}>Train</th>
              <th style={{ ...thS, textAlign: 'left' }}>Test</th>
              <th style={{ ...thS }}>IS Sh</th>
              <th style={{ ...thS, minWidth: 88 }}>OOS Sh</th>
              <th style={{ ...thS }}>CAGR</th>
              <th style={{ ...thS }}>MaxDD</th>
              <th style={{ ...thS }}>Sortino</th>
              <th style={{ ...thS }}>R²</th>
              <th style={{ ...thS }}>DSR</th>
              {hasActiveDays && <th style={{ ...thS }}>Active</th>}
            </tr>
          </thead>
          <tbody>
            {folds.map((fold) => {
              const excl = fold.isExcluded;
              const unstb = fold.isUnstable && !excl;
              const frac = fold.oosSharpe !== null && maxS > minS
                ? Math.max(0, (fold.oosSharpe - minS) / (maxS - minS))
                : fold.oosSharpe !== null && fold.oosSharpe > 0 ? 1 : 0;
              const rowBg = excl ? 'transparent'
                : unstb ? 'rgba(220,80,60,0.06)'
                : fold.num % 2 === 1 ? 'rgba(255,255,255,0.015)' : 'transparent';
              return (
                <tr key={fold.num} style={{ background: rowBg, opacity: excl ? 0.35 : 1 }}>
                  <td style={{ padding: '3px 8px 3px 5px', color: 'var(--t3)', fontSize: 8 }}>{fold.num}</td>
                  {hasDates && (
                    <td style={{ padding: '3px 10px 3px 5px', whiteSpace: 'nowrap', color: 'var(--t2)' }}>
                      {fold.dateFrom} <span style={{ color: 'var(--t4)' }}>→</span> {fold.dateTo}
                    </td>
                  )}
                  <td style={{ padding: '3px 10px 3px 5px', whiteSpace: 'nowrap', color: 'var(--t3)', fontSize: 8 }}>{fold.trainRange}</td>
                  <td style={{ padding: '3px 10px 3px 5px', whiteSpace: 'nowrap', color: 'var(--t2)', fontSize: 8 }}>
                    {fold.testRange}
                    {excl && <span style={{ marginLeft: 5, fontSize: 7, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>⊘ sat</span>}
                    {unstb && <span style={{ marginLeft: 5, fontSize: 7, color: 'var(--amber)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>⚠ unstable</span>}
                  </td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: excl ? 'var(--t4)' : sharpeColor(fold.isSharpe) }}>{fmt(fold.isSharpe, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right' }}>
                    {excl ? (
                      <span style={{ color: 'var(--t4)' }}>{fmt(fold.oosSharpe, 3)}</span>
                    ) : (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 5, justifyContent: 'flex-end' }}>
                        <div style={{ width: 36, height: 3, background: 'var(--bg3)', borderRadius: 1, flexShrink: 0 }}>
                          {fold.oosSharpe !== null && fold.oosSharpe > 0 && (
                            <div style={{ width: `${Math.round(frac * 100)}%`, height: '100%', background: sharpeColor(fold.oosSharpe), borderRadius: 1 }} />
                          )}
                        </div>
                        <span style={{ color: sharpeColor(fold.oosSharpe), fontWeight: unstb ? 700 : 400 }}>{fmt(fold.oosSharpe, 3)}</span>
                      </div>
                    )}
                  </td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: excl ? 'var(--t4)' : 'var(--t2)', fontSize: 8 }}>{fold.oosCagr ?? '—'}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: excl ? 'var(--t4)' : maxddColor(fold.oosMaxdd) }}>{fmt(fold.oosMaxdd, 2, '%')}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: excl ? 'var(--t4)' : sortinoColor(fold.oosSortino) }}>{fmt(fold.oosSortino, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: excl ? 'var(--t4)' : r2Color(fold.oosR2) }}>{fmt(fold.oosR2, 3)}</td>
                  <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', color: excl ? 'var(--t4)' : dsrColor(fold.oosDsr) }}>{fmt(fold.oosDsr, 1, '%')}</td>
                  {hasActiveDays && (
                    <td style={{ padding: '3px 8px 3px 5px', textAlign: 'right', whiteSpace: 'nowrap' }}>
                      {fold.activeDays !== null && fold.flatDays !== null
                        ? <><span style={{ color: 'var(--t2)' }}>{fold.activeDays}</span><span style={{ color: 'var(--t4)' }}>/{fold.activeDays + fold.flatDays}d</span></>
                        : <span style={{ color: 'var(--t4)' }}>—</span>}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Unstable fold daily breakdowns (collapsible) */}
      {folds.filter((f) => f.dailyBreakdown && f.dailyBreakdown.length > 0).map((fold) => (
        <details key={`daily-${fold.num}`} style={{ border: '1px solid var(--line)', borderRadius: 3, background: 'rgba(220,80,60,0.03)' }}>
          <summary style={{ padding: '5px 10px', cursor: 'pointer', color: 'var(--amber)', fontSize: 8, textTransform: 'uppercase', letterSpacing: '0.08em' }}>
            ⚠ Fold {fold.num} — Daily Return Breakdown
            {fold.dailySummary && (
              <span style={{ marginLeft: 10, color: 'var(--t3)', textTransform: 'none', letterSpacing: 0 }}>
                {fold.dailySummary.winDays}/{fold.dailySummary.totalDays} wins ({fold.dailySummary.winPct}%) · avg {fold.dailySummary.avgDaily?.toFixed(3)}%/d
              </span>
            )}
          </summary>
          <div style={{ padding: '4px 10px 8px' }}>
            {/* Daily return bar chart */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 1, marginBottom: 8 }}>
              {fold.dailyBreakdown!.map((row) => {
                const maxRet = Math.max(...fold.dailyBreakdown!.map((r) => Math.abs(r.ret)));
                const barW = maxRet > 0 ? Math.abs(row.ret) / maxRet * 100 : 0;
                const isWin = row.type === 'WIN';
                return (
                  <div key={row.day} style={{ display: 'flex', alignItems: 'center', gap: 4, height: 10 }}>
                    <span style={{ width: 18, textAlign: 'right', color: 'var(--t4)', fontSize: 7 }}>{row.day}</span>
                    <div style={{ flex: 1, display: 'flex', justifyContent: isWin ? 'flex-start' : 'flex-end', position: 'relative' }}>
                      <div style={{ position: 'absolute', left: '50%', top: 0, bottom: 0, width: 1, background: 'var(--line)' }} />
                      <div style={{
                        position: 'absolute',
                        [isWin ? 'left' : 'right']: '50%',
                        width: `${barW * 0.5}%`,
                        height: 6, borderRadius: 1, top: 2,
                        background: isWin ? 'var(--green)' : 'var(--red)',
                        opacity: row.isBig ? 0.9 : 0.5,
                      }} />
                    </div>
                    <span style={{ width: 44, textAlign: 'right', color: isWin ? 'var(--green)' : 'var(--red)', fontSize: 8 }}>
                      {row.ret > 0 ? '+' : ''}{row.ret.toFixed(2)}%
                    </span>
                    <span style={{ width: 44, textAlign: 'right', color: 'var(--t4)', fontSize: 7.5 }}>{row.cumul.toFixed(1)}%</span>
                  </div>
                );
              })}
            </div>
            {/* Summary stats */}
            {fold.dailySummary && (
              <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', padding: '4px 0', borderTop: '1px solid var(--line)', fontSize: 8 }}>
                <span style={{ color: 'var(--t3)' }}>Avg win <span style={{ color: 'var(--green)' }}>+{fold.dailySummary.avgWin?.toFixed(2)}%</span></span>
                <span style={{ color: 'var(--t3)' }}>Avg loss <span style={{ color: 'var(--red)' }}>{fold.dailySummary.avgLoss?.toFixed(2)}%</span></span>
                <span style={{ color: 'var(--t3)' }}>Worst <span style={{ color: 'var(--red)' }}>{fold.dailySummary.worstDay?.toFixed(2)}%</span></span>
                <span style={{ color: 'var(--t3)' }}>Best <span style={{ color: 'var(--green)' }}>+{fold.dailySummary.bestDay?.toFixed(2)}%</span></span>
                {fold.dailySummary.pattern && (
                  <span style={{ color: 'var(--amber)', fontSize: 7.5 }}>{fold.dailySummary.pattern}</span>
                )}
              </div>
            )}
          </div>
        </details>
      ))}

      {/* Aggregate summary */}
      {meanSharpe !== null && (
        <div style={{ display: 'flex', gap: 18, flexWrap: 'wrap', padding: '6px 10px', background: 'var(--bg0)', border: '1px solid var(--line)', borderRadius: 3, alignItems: 'center' }}>
          <span style={{ color: 'var(--t3)', fontSize: 8, textTransform: 'uppercase', letterSpacing: '0.1em' }}>
            Aggregate — {activeFoldsUsed || folds.filter((f) => !f.isExcluded).length} active folds
          </span>
          <span style={{ color: 'var(--t2)' }}>
            Mean SR <span style={{ color: sharpeColor(meanSharpe), fontWeight: 700 }}>{meanSharpe.toFixed(3)}</span>
            {stdSharpe !== null && <span style={{ color: 'var(--t3)' }}> ±{stdSharpe.toFixed(3)}</span>}
            {minSharpe !== null && maxSharpe !== null && (
              <span style={{ color: 'var(--t4)', fontSize: 8 }}> [{minSharpe.toFixed(3)}, {maxSharpe.toFixed(3)}]</span>
            )}
          </span>
          {robustScore !== null && (
            <span style={{ color: 'var(--t2)' }}>
              Robust <span style={{ color: robustScore >= 0 ? 'var(--green)' : 'var(--red)', fontWeight: 700 }}>{robustScore.toFixed(3)}</span>
            </span>
          )}
          {meanDsr !== null && (
            <span style={{ color: 'var(--t2)' }}>
              DSR <span style={{ color: dsrColor(meanDsr) }}>{meanDsr.toFixed(1)}%</span>
              {dsrStatus && (
                <span style={{ marginLeft: 4, color: /PASS/i.test(dsrStatus) ? 'var(--green)' : /FAIL/i.test(dsrStatus) ? 'var(--red)' : 'var(--amber)', fontSize: 8 }}>
                  {/PASS/i.test(dsrStatus) ? '✅' : /FAIL/i.test(dsrStatus) ? '❌' : '⚠'} {dsrStatus}
                </span>
              )}
            </span>
          )}
          {pctPositive !== null && (
            <span style={{ color: 'var(--t2)' }}>
              Positive <span style={{ color: pctPositive >= 80 ? 'var(--green)' : pctPositive >= 60 ? 'var(--amber)' : 'var(--red)' }}>{pctPositive}%</span>
            </span>
          )}
          {cv !== null && (
            <span style={{ color: 'var(--t2)' }}>
              CV=<span style={{ color: cvStable ? 'var(--green)' : 'var(--amber)', fontWeight: 700 }}>{cv.toFixed(3)}</span>
              <span style={{ marginLeft: 5, color: cvStable ? 'var(--green)' : 'var(--amber)' }}>
                {cvStable ? '✅ STABLE' : '⚠ UNSTABLE'}
              </span>
            </span>
          )}
        </div>
      )}

      {/* Secondary aggregate stats */}
      {(meanCagr || meanMaxdd || meanSortino || meanR2) && (
        <div style={{ display: 'flex', gap: 18, flexWrap: 'wrap', padding: '4px 10px', alignItems: 'center' }}>
          {meanCagr && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean CAGR <span style={{ color: 'var(--t2)' }}>{meanCagr}</span></span>}
          {meanMaxdd && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean MaxDD <span style={{ color: 'var(--t2)' }}>{meanMaxdd}</span></span>}
          {meanSortino && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean Sortino <span style={{ color: 'var(--t2)' }}>{meanSortino}</span></span>}
          {meanR2 && <span style={{ color: 'var(--t3)', fontSize: 8 }}>Mean R² <span style={{ color: 'var(--t2)' }}>{meanR2}</span></span>}
        </div>
      )}

      {/* OI diagnostics */}
      {oiCorrs.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4, padding: '6px 10px', background: 'var(--bg0)', border: '1px solid var(--line)', borderRadius: 3 }}>
          <span style={{ color: 'var(--t3)', fontSize: 8, textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 2 }}>OI Regime Correlations</span>
          {oiCorrs.map((c) => (
            <div key={c.label} style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
              <span style={{ color: 'var(--t3)', minWidth: 200, fontSize: 8.5 }}>{c.label}</span>
              <span style={{ color: Math.abs(parseFloat(c.value)) > 0.5 ? 'var(--amber)' : 'var(--t2)' }}>{c.value}</span>
              {Math.abs(parseFloat(c.value)) > 0.5 && <span style={{ color: 'var(--amber)', fontSize: 7.5 }}>★ consider as regime filter</span>}
            </div>
          ))}
        </div>
      )}
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

function renderStressTestSummary(body: string): React.ReactNode {
  const lines = body.split('\n');

  // Parse header info (MC iters, BB iters, block length)
  let mcIters = '';
  let bbIters = '';
  for (const line of lines) {
    const m = line.match(/MC Reshuffle:\s*([\d,]+)\s*iters.*Block Bootstrap:\s*([\d,]+)\s*iters\s*block=(\d+)/i);
    if (m) { mcIters = m[1]; bbIters = `${m[2]} (block=${m[3]}d)`; break; }
  }

  // Parse Max Drawdown Distribution table
  const ddRows: { label: string; mc: string; bb: string }[] = [];
  for (const line of lines) {
    const m = line.match(/│?\s*(p\d+\s*\([^)]+\)|Median)\s+([-\d.]+%)\s+([-\d.]+%)/i);
    if (m) ddRows.push({ label: m[1].trim(), mc: m[2].trim(), bb: m[3].trim() });
  }

  // Parse Total Return Multiple Distribution
  const multRows: { label: string; value: string }[] = [];
  const simStats: { label: string; value: string }[] = [];
  let collapsed = false;
  let inMult = false;
  for (const line of lines) {
    if (/TOTAL RETURN MULTIPLE/i.test(line)) { inMult = true; continue; }
    if (!inMult) continue;
    if (/path-order invariant/i.test(line)) { collapsed = true; continue; }
    const pMatch = line.match(/│?\s*(p\d+\s*\([^)]+\)|Median):\s+([\d.]+x)/i);
    if (pMatch) { multRows.push({ label: pMatch[1].trim(), value: pMatch[2].trim() }); continue; }
    const sMatch = line.match(/│?\s*(% sims[^:]+):\s+([\d.]+%)/i);
    if (sMatch) { simStats.push({ label: sMatch[1].trim(), value: sMatch[2].trim() }); continue; }
  }

  if (ddRows.length === 0 && multRows.length === 0) return <PreFallback body={body} />;

  const parseNum = (s: string) => parseFloat(s.replace(/[%x,]/g, ''));

  const ddColor = (val: string) => {
    const n = parseNum(val);
    if (!Number.isFinite(n)) return 'var(--t1)';
    if (n < -40) return 'var(--red)';
    if (n < -20) return 'var(--orange)';
    return 'var(--green)';
  };

  const multColor = (val: string) => {
    const n = parseNum(val);
    if (!Number.isFinite(n)) return 'var(--t1)';
    if (n < 1) return 'var(--red)';
    if (n < 5) return 'var(--t1)';
    return 'var(--green)';
  };

  const simColor = (label: string, val: string) => {
    const n = parseNum(val);
    if (!Number.isFinite(n)) return 'var(--t2)';
    if (label.includes('lost money')) return n > 10 ? 'var(--red)' : n > 0 ? 'var(--orange)' : 'var(--green)';
    return n > 50 ? 'var(--green)' : n > 10 ? 'var(--t1)' : 'var(--t3)';
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      {/* Config badges */}
      {(mcIters || bbIters) && (
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          {mcIters && (
            <div style={{
              padding: '4px 10px', borderRadius: 12, fontSize: 9, ...MONO,
              background: 'rgba(100, 160, 255, 0.1)', color: 'var(--blue)', border: '1px solid rgba(100, 160, 255, 0.2)',
            }}>
              MC Reshuffle: {mcIters} iters
            </div>
          )}
          {bbIters && (
            <div style={{
              padding: '4px 10px', borderRadius: 12, fontSize: 9, ...MONO,
              background: 'rgba(180, 130, 255, 0.1)', color: 'var(--purple)', border: '1px solid rgba(180, 130, 255, 0.2)',
            }}>
              Block Bootstrap: {bbIters}
            </div>
          )}
        </div>
      )}

      {/* Max Drawdown Distribution */}
      {ddRows.length > 0 && (
        <div style={{
          background: 'var(--bg1)', border: '1px solid var(--line2)', borderRadius: 6, overflow: 'hidden',
        }}>
          <div style={{
            padding: '10px 14px', borderBottom: '1px solid var(--line2)', background: 'var(--bg2)',
            fontSize: 9.5, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700, ...MONO,
          }}>
            Max Drawdown Distribution
          </div>
          {/* Header */}
          <div style={{
            display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
            padding: '8px 14px', borderBottom: '1px solid var(--line1)',
            fontSize: 9, color: 'var(--t4)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO,
          }}>
            <div>Percentile</div>
            <div style={{ textAlign: 'right' }}>MC Reshuffle</div>
            <div style={{ textAlign: 'right' }}>Block Bootstrap</div>
          </div>
          {/* Rows */}
          {ddRows.map((row, idx) => (
            <div key={idx} style={{
              display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
              padding: '10px 14px',
              borderBottom: idx === ddRows.length - 1 ? 'none' : '1px solid var(--line1)',
              fontSize: 11.5, alignItems: 'center', ...MONO,
            }}>
              <div style={{ color: 'var(--t2)', fontSize: 10 }}>{row.label}</div>
              <div style={{ textAlign: 'right', fontWeight: 600, color: ddColor(row.mc) }}>{row.mc}</div>
              <div style={{ textAlign: 'right', fontWeight: 600, color: ddColor(row.bb) }}>{row.bb}</div>
            </div>
          ))}
        </div>
      )}

      {/* Total Return Multiple Distribution */}
      {multRows.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div style={{
            fontSize: 9.5, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700, ...MONO,
          }}>
            Total Return Multiple (MC Reshuffle)
          </div>

          {collapsed && (
            <div style={{
              padding: '6px 10px', borderRadius: 4, fontSize: 9, lineHeight: 1.5,
              background: 'rgba(100, 160, 255, 0.06)', color: 'var(--blue)', border: '1px solid rgba(100, 160, 255, 0.15)', ...MONO,
            }}>
              TotalMultiple is path-order invariant — variance captured by MaxDD distribution above.
            </div>
          )}

          <div style={{
            display: 'grid', gridTemplateColumns: `repeat(${multRows.length}, 1fr)`, gap: 10,
          }}>
            {multRows.map((row, idx) => (
              <div key={idx} style={{
                background: 'var(--bg2)', border: '1px solid var(--line1)', borderRadius: 6,
                padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 5,
                boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.02)',
              }}>
                <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: 0.5, ...MONO }}>
                  {row.label}
                </div>
                <div style={{ fontSize: 20, fontWeight: 500, letterSpacing: -0.5, color: multColor(row.value), ...MONO }}>
                  {row.value}
                </div>
              </div>
            ))}
          </div>

          {/* Sim threshold stats */}
          {simStats.length > 0 && (
            <div style={{
              display: 'grid', gridTemplateColumns: `repeat(${simStats.length}, 1fr)`, gap: 8,
            }}>
              {simStats.map((s, idx) => {
                const n = parseNum(s.value);
                const barWidth = Number.isFinite(n) ? Math.min(n, 100) : 0;
                return (
                  <div key={idx} style={{
                    background: 'var(--bg1)', border: '1px solid var(--line1)', borderRadius: 4,
                    padding: '8px 12px', position: 'relative', overflow: 'hidden',
                  }}>
                    <div style={{
                      position: 'absolute', top: 0, left: 0, bottom: 0,
                      width: `${barWidth}%`, opacity: 0.08,
                      background: s.label.includes('lost money') ? 'var(--red)' : 'var(--green)',
                    }} />
                    <div style={{ position: 'relative', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div style={{ fontSize: 9, color: 'var(--t3)', ...MONO }}>{s.label}</div>
                      <div style={{ fontSize: 12, fontWeight: 600, color: simColor(s.label, s.value), ...MONO }}>{s.value}</div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function renderSectionViz(title: string, body: string): React.ReactNode {
  const t = title.toUpperCase();

  if (t.includes('RUN SUMMARY')) return renderRunSummary(body);
  if (t.includes('DAILY SERIES AUDIT')) return renderAuditChecklist(body, 'Daily Series Audit');
  if (t.includes('SIMULATION BIAS AUDIT')) return renderAuditChecklist(body, 'Simulation Bias Audit');
  if (t.includes('GATING METRICS')) return renderGradedMetrics(title, body);
  if (t.includes('CORE METRICS')) return renderGradedMetrics(title, body);
  if (t.includes('SUPPORTING METRICS')) return renderGradedMetrics(title, body);
  if (t.includes('WHAT YOU HAVE')) return renderWhatYouHave(body);
  if (t.includes('WHAT YOU STILL NEED')) return renderWhatYouNeed(body);
  if (t.includes('BOTTOM LINE')) return renderBottomLine(body);
  if (t.includes('BEST FILTER HEADLINE STATS')) return renderBestFilterHeadline(body);
  if (t.includes('ALLOCATOR VIEW SCORECARD') || t.includes('TECHNICAL APPENDIX SCORECARD')) return renderScorecardTable(body);
  if (t.includes('RETURN RATES BY PERIOD')) return renderReturnRatesByPeriod(body);
  if (t.includes('RETURN DISTRIBUTION')) return renderReturnDistribution(body);
  if (t.includes('RETURN + CONDITIONAL ANALYSIS') || t.includes('RETURN+CONDITIONAL')) return renderReturnConditional(body);
  if (t.includes('REGIME & CONDITIONAL ANALYSIS')) return renderRegimeConditional(body);
  if (t.includes('ROLLING MAX DRAWDOWN')) return renderRollingMaxDrawdown(body);
  if (t.includes('DRAWDOWN EPISODE ANALYSIS')) return renderDrawdownEpisodes(body);
  if (t.includes('RISK-ADJUSTED RETURN QUALITY')) return renderRiskAdjustedQuality(body);
  if (t.includes('DAILY VAR') || t.includes('CVAR')) return renderDailyVarCvar(body);
  if (t.includes('STATISTICAL VALIDITY')) return renderStatisticalValidity(body);
  if (t.includes('TAIL RISK') && t.includes('EXTENDED')) return renderTailRiskExtended(body);
  if (t.includes('SIGNAL PREDICTIVENESS')) return renderSignalPredictiveness(body);
  if (t.includes('SLIPPAGE IMPACT SWEEP')) return renderSlippageSweep(body);
  if (t.includes('NOISE PERTURBATION STABILITY TEST')) return renderNoisePerturbation(body);
  if (t.includes('PARAM JITTER')) return renderParamJitter(body);
  if (t.includes('NEIGHBOR PLATEAU TEST')) return renderNeighborPlateau(body);
  if (t.includes('PARAMETER SENSITIVITY MAP')) return renderParamSensitivityMap(body);
  if (t.includes('SLIPPAGE SENSITIVITY TABLE')) return renderSlippageSensitivity(body);
  if (t.includes('CAPPED RETURN SENSITIVITY')) return renderCappedReturnSensitivity(body);
  if (t.includes('TOP-N DAY REMOVAL TEST') || t.includes('TOP‑N DAY REMOVAL')) return renderTopNDayRemoval(body);
  if (t.includes('LUCKY STREAK TEST')) return renderLuckyStreakTest(title, body);
  if (t.includes('RETURN CONCENTRATION ANALYSIS')) return renderReturnConcentration(body);
  if (t.includes('PERIODIC RETURN BREAKDOWN')) return renderPeriodicBreakdown(body);
  if (t.includes('SHOCK INJECTION TEST')) return renderShockInjection(body);
  if (t.includes('REGIME ROBUSTNESS TEST')) return renderRegimeRobustness(body);
  if (t.includes('WEEKLY MILESTONES') || t.includes('MONTHLY MILESTONES')) return renderMilestones(body);
  if (t.includes('MINIMUM CUMULATIVE RETURN')) return renderMinCumReturn(body);
  if (t.includes('LIQUIDITY CAPACITY CURVE')) return renderLiquidityCapacityCurve(body);
  if (t.includes('CAPACITY CURVE TEST')) return renderCapacityCurveTest(body);
  if (t.includes('COST CURVE TEST')) return renderCostCurveTest(body);
  if (t.includes('CAPITAL & OPERATIONAL')) return renderCapitalOperational(body);
  if (t.includes('MARKET CAP UNIVERSE SUMMARY')) return renderMarketCapUniverse(body);
  if (t.includes('MARKET CAP DIAGNOSTIC')) return renderMarketCapDiagnostic(body);
  if (/^RUIN PROBABILITY/i.test(t)) return renderRuinProbability(body);
  if (t.includes('DEFLATED SHARPE RATIO')) return renderDeflatedSharpe(body);

  if (t.includes('SHARPE STABILITY ANALYSIS')) return renderSharpeStability(body);
  if (t.includes('WALK-FORWARD VALIDATION')) return renderWalkForwardValidation(title, body);
  if (t.includes('WALK-FORWARD ROLLING') || t.includes('FILTER-AWARE WALK-FORWARD')) return renderWalkForwardRolling(title, body);

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

  if (t.includes('STRESS TEST SUMMARY')) return renderStressTestSummary(body);

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
  const [hideFlatDays, setHideFlatDays] = useState(false);
  const [breakdownPeriod, setBreakdownPeriod] = useState<'daily' | 'weekly' | 'monthly'>('monthly');
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
  const [showFullReportBackToTop, setShowFullReportBackToTop] = useState(false);
  const [fullReportTocOpen, setFullReportTocOpen] = useState(true);
  const [openFullReportCategories, setOpenFullReportCategories] = useState<Record<FullReportCategoryKey, boolean>>(() => (
    FULL_REPORT_CATEGORIES.reduce((acc, cat) => {
      acc[cat.key] = cat.defaultOpen;
      return acc;
    }, {} as Record<FullReportCategoryKey, boolean>)
  ));
  const [openFullReportSectionKeys, setOpenFullReportSectionKeys] = useState<Record<string, boolean>>({});
  const filterComparisonRef = useRef<HTMLDetailsElement | null>(null);
  const monthlyHeatmapRailRef = useRef<HTMLDivElement | null>(null);
  const resultsViewRootRef = useRef<HTMLDivElement | null>(null);
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
    // Avg leverage (active days only)
    const levValues = activeRows
      .map((r) => r.lev as number)
      .filter((v) => typeof v === 'number' && Number.isFinite(v));
    const avgLeverage = levValues.length > 0
      ? levValues.reduce((a, b) => a + b, 0) / levValues.length
      : null;

    // Avg drawdown — compute running drawdown from cumulative equity
    let peak = 0;
    let ddSum = 0;
    let ddCount = 0;
    for (const r of rows) {
      const end = typeof r.end === 'number' && Number.isFinite(r.end) ? r.end : null;
      if (end === null) continue;
      if (end > peak) peak = end;
      if (peak > 0) {
        const dd = ((end - peak) / peak) * 100;
        ddSum += dd;
        ddCount += 1;
      }
    }
    const avgDrawdownPct = ddCount > 0 ? ddSum / ddCount : null;

    // Worst day (most negative net return)
    const worstDayRet = activeRows.length > 0
      ? Math.min(...activeRows.map(r => r.ret_net as number).filter(v => typeof v === 'number' && Number.isFinite(v)))
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
      avgLeverage,
      avgDrawdownPct,
      worstDayRet,
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

  type AggregatedPeriodRow = {
    label: string;
    startEquity: number;
    endEquity: number;
    retNet: number;
    pnl: number;
    totalFees: number;
    totalVolume: number;
    activeDays: number;
    totalDays: number;
    avgLev: number | null;
  };

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
    { label: 'CAGR %', key: 'cagr', value: fmtCagr(selectedRow?.cagr ?? m.cagr), colorValue: selectedRow?.cagr ?? m.cagr },
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

  useEffect(() => {
    if (activeTab !== 'full_report') {
      setShowFullReportBackToTop(false);
      return;
    }

    const scrollContainer = resultsViewRootRef.current?.parentElement;
    if (!scrollContainer) {
      setShowFullReportBackToTop(false);
      return;
    }

    const onScroll = () => {
      setShowFullReportBackToTop(scrollContainer.scrollTop > 480);
    };

    onScroll();
    scrollContainer.addEventListener('scroll', onScroll, { passive: true });
    return () => {
      scrollContainer.removeEventListener('scroll', onScroll);
    };
  }, [activeTab]);

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
  const aggregatedPeriodRows = useMemo<AggregatedPeriodRow[]>(() => {
    if (breakdownPeriod === 'daily') return [];
    const rows = selectedFeesTableRowsWithCumulative;
    if (rows.length === 0) return [];

    const equityByDate = new Map<string, { first: number; last: number }>();
    for (const p of equitySeriesDated) {
      const key = breakdownPeriod === 'monthly'
        ? `${p.d.getFullYear()}-${String(p.d.getMonth() + 1).padStart(2, '0')}`
        : (() => {
            const start = new Date(p.d.getFullYear(), p.d.getMonth(), p.d.getDate());
            const day = start.getDay();
            const diff = day === 0 ? 6 : day - 1;
            start.setDate(start.getDate() - diff);
            return `${start.getFullYear()}-${String(start.getMonth() + 1).padStart(2, '0')}-${String(start.getDate()).padStart(2, '0')}`;
          })();
      if (!equityByDate.has(key)) {
        equityByDate.set(key, { first: p.y, last: p.y });
      } else {
        equityByDate.get(key)!.last = p.y;
      }
    }

    const feesBuckets = new Map<string, typeof rows>();
    for (const row of rows) {
      if (!row.date) continue;
      const d = new Date(`${row.date}T00:00:00Z`);
      if (Number.isNaN(d.getTime())) continue;
      let key: string;
      if (breakdownPeriod === 'monthly') {
        key = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
      } else {
        const start = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
        const day = start.getUTCDay();
        const diff = day === 0 ? 6 : day - 1;
        start.setUTCDate(start.getUTCDate() - diff);
        key = start.toISOString().slice(0, 10);
      }
      if (!feesBuckets.has(key)) feesBuckets.set(key, []);
      feesBuckets.get(key)!.push(row);
    }

    const result: AggregatedPeriodRow[] = [];
    const allKeys = new Set([...equityByDate.keys(), ...feesBuckets.keys()]);
    const sortedKeys = [...allKeys].sort();

    for (const key of sortedKeys) {
      const eq = equityByDate.get(key);
      const periodRows = feesBuckets.get(key) ?? [];

      const startEquity = eq?.first ?? (periodRows[0] && typeof periodRows[0].start === 'number' ? periodRows[0].start : 0);
      const endEquity = eq?.last ?? (periodRows.length > 0 && typeof periodRows[periodRows.length - 1].end === 'number' ? (periodRows[periodRows.length - 1].end as number) : 0);
      const retNet = startEquity > 0 ? ((endEquity / startEquity) - 1) * 100 : 0;
      const pnl = endEquity - startEquity;

      const activeRows = periodRows.filter(r => !r.no_entry);
      const totalFees = periodRows.reduce((acc, r) => {
        const t = typeof r.taker_fee === 'number' && Number.isFinite(r.taker_fee) ? r.taker_fee : 0;
        const f = typeof r.funding === 'number' && Number.isFinite(r.funding) ? r.funding : 0;
        return acc + t + f;
      }, 0);
      const totalVolume = periodRows.reduce((acc, r) => {
        const v = typeof r.trade_vol === 'number' && Number.isFinite(r.trade_vol) ? r.trade_vol : 0;
        return acc + v;
      }, 0);
      const levs = activeRows
        .map(r => r.lev as number)
        .filter(v => typeof v === 'number' && Number.isFinite(v));
      const avgLev = levs.length > 0 ? levs.reduce((a, b) => a + b, 0) / levs.length : null;

      let label: string;
      if (breakdownPeriod === 'monthly') {
        const [y, m] = key.split('-');
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        label = `${monthNames[parseInt(m) - 1]} ${y}`;
      } else {
        const endDate = periodRows.length > 0 ? periodRows[periodRows.length - 1].date : key;
        label = `${key} → ${endDate}`;
      }

      result.push({ label, startEquity, endEquity, retNet, pnl, totalFees, totalVolume, activeDays: activeRows.length, totalDays: periodRows.length, avgLev });
    }

    return result;
  }, [selectedFeesTableRowsWithCumulative, equitySeriesDated, breakdownPeriod]);

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
      { label: 'CAGR %', key: 'cagr', value: fmtCagr(selectedRow.cagr), colorValue: selectedRow.cagr },
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
      { label: 'Simple Return %', key: 'tot_ret', value: fmtSummaryReturn(selectedRow.tot_ret), colorValue: selectedRow.tot_ret },
      {
        label: 'Compounded Return %',
        key: 'compounded_ret',
        value: feesKeyValues.netReturnPct !== null ? fmtSummaryReturn(feesKeyValues.netReturnPct) : 'N/A',
        colorValue: feesKeyValues.netReturnPct,
      },
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
      const apiBase = process.env.NEXT_PUBLIC_API_BASE || '';
      const res = await fetch(`${apiBase}/api/jobs/${jobId}/output`);
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
    <div ref={resultsViewRootRef} style={{ padding: '0 16px 16px 16px', display: 'flex', flexDirection: 'column', gap: 16 }}>
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
          {mergedFilters.length > 0 && (
            <select
              value={selectedFilter ?? ''}
              onChange={(e) => setManualSelectedFilter(e.target.value || null)}
              style={{
                marginLeft: 12,
                height: 22,
                padding: '0 6px',
                borderRadius: 3,
                border: '1px solid var(--line2)',
                background: 'var(--bg1)',
                color: 'var(--t1)',
                fontSize: 9,
                letterSpacing: '0.06em',
                cursor: 'pointer',
                fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                outline: 'none',
              }}
            >
              {mergedFilters.filter((r) => !r.not_run).map((r) => (
                <option key={String(r.filter)} value={String(r.filter)}>
                  {String(r.filter)}
                </option>
              ))}
            </select>
          )}
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
                                if (typeof r === 'number' && Number.isFinite(r) && r !== 0) {
                                  const strength = Math.min(1, Math.abs(r) / Math.max(1e-9, calendarScale));
                                  const alpha = 0.10 + (0.70 * strength);
                                  if (r > 0) {
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
                      const yZero = toY(0);
                      const points = cum.map((c, i) => ({
                        x: padL + ((i + 0.5) / cum.length) * plotW,
                        y: toY(c.pct),
                      }));
                      // Build smooth cubic bezier curve
                      const smoothLine = (pts: typeof points): string => {
                        if (pts.length < 2) return pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x},${p.y}`).join(' ');
                        let d = `M${pts[0].x},${pts[0].y}`;
                        for (let i = 0; i < pts.length - 1; i++) {
                          const p0 = pts[Math.max(0, i - 1)];
                          const p1 = pts[i];
                          const p2 = pts[i + 1];
                          const p3 = pts[Math.min(pts.length - 1, i + 2)];
                          const tension = 0.3;
                          const cp1x = p1.x + (p2.x - p0.x) * tension;
                          const cp1y = p1.y + (p2.y - p0.y) * tension;
                          const cp2x = p2.x - (p3.x - p1.x) * tension;
                          const cp2y = p2.y - (p3.y - p1.y) * tension;
                          d += ` C${cp1x},${cp1y} ${cp2x},${cp2y} ${p2.x},${p2.y}`;
                        }
                        return d;
                      };
                      const linePath = smoothLine(points);
                      const areaPath = points.length > 0
                        ? `${linePath} L${points[points.length - 1].x},${yZero} L${points[0].x},${yZero} Z`
                        : '';
                      const lastPct = cum[cum.length - 1]?.pct ?? 0;
                      const isPos = lastPct >= 0;
                      return (
                        <svg
                          viewBox={`0 0 ${w} ${h}`}
                          style={{ width: '100%', height: '100%', display: 'block' }}
                          preserveAspectRatio="none"
                          onMouseMove={(e) => {
                            const rect = e.currentTarget.getBoundingClientRect();
                            const mx = ((e.clientX - rect.left) / rect.width) * w;
                            let idx = Math.floor(((mx - padL) / Math.max(1, plotW)) * cum.length);
                            idx = Math.max(0, Math.min(cum.length - 1, idx));
                            const c = cum[idx];
                            if (c) {
                              setCalendarHover({
                                x: e.clientX, y: e.clientY,
                                text: `${c.key} | cum ${c.pct >= 0 ? '+' : ''}${c.pct.toFixed(2)}%`,
                              });
                            }
                          }}
                          onMouseLeave={() => setCalendarHover(null)}
                        >
                          <defs>
                            <clipPath id={`clip-pos-${month.monthKey}`}><rect x={padL} y={0} width={plotW} height={yZero} /></clipPath>
                            <clipPath id={`clip-neg-${month.monthKey}`}><rect x={padL} y={yZero} width={plotW} height={h - yZero} /></clipPath>
                          </defs>
                          <line x1={padL} y1={padT} x2={padL} y2={h - padB} stroke="var(--line2)" />
                          <line x1={padL} y1={h - padB} x2={w - padR} y2={h - padB} stroke="var(--line2)" />
                          <line x1={padL} y1={yZero} x2={w - padR} y2={yZero} stroke="rgba(255,255,255,0.35)" strokeDasharray="2 2" />
                          {areaPath && (
                            <>
                              <path d={areaPath} fill="rgba(0, 200, 150, 0.15)" clipPath={`url(#clip-pos-${month.monthKey})`} />
                              <path d={areaPath} fill="rgba(255, 77, 77, 0.15)" clipPath={`url(#clip-neg-${month.monthKey})`} />
                              <path d={linePath} fill="none" stroke="rgba(0, 200, 150, 0.7)" strokeWidth="1" clipPath={`url(#clip-pos-${month.monthKey})`} />
                              <path d={linePath} fill="none" stroke="rgba(255, 77, 77, 0.7)" strokeWidth="1" clipPath={`url(#clip-neg-${month.monthKey})`} />
                            </>
                          )}
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
                    label: 'Avg Leverage',
                    value: feesBreakdownKpis.avgLeverage === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.avgLeverage.toFixed(2)}x`,
                    color: 'var(--t1)',
                  },
                  {
                    label: 'Avg Drawdown',
                    value: feesBreakdownKpis.avgDrawdownPct === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.avgDrawdownPct.toFixed(2)}%`,
                    color: 'var(--red)',
                  },
                  {
                    label: 'Worst Day',
                    value: feesBreakdownKpis.worstDayRet === null
                      ? 'N/A'
                      : `${feesBreakdownKpis.worstDayRet.toFixed(2)}%`,
                    color: 'var(--red)',
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
                  justifyContent: 'space-between',
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
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  {`Fees Panel${selectedFilter ? ` • ${selectedFilter}` : ''}`}
                  <div style={{ display: 'flex', gap: 2, marginLeft: 8 }}>
                    {(['daily', 'weekly', 'monthly'] as const).map((p) => (
                      <button
                        key={p}
                        onClick={() => setBreakdownPeriod(p)}
                        style={{
                          background: breakdownPeriod === p ? 'var(--t1)' : 'transparent',
                          border: `1px solid ${breakdownPeriod === p ? 'var(--t1)' : 'var(--line)'}`,
                          borderRadius: 3,
                          padding: '1px 6px',
                          fontSize: 8,
                          color: breakdownPeriod === p ? 'var(--bg0)' : 'var(--t3)',
                          textTransform: 'uppercase',
                          letterSpacing: '0.06em',
                          fontWeight: 600,
                          cursor: 'pointer',
                          fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                          lineHeight: 1,
                        }}
                      >
                        {p === 'daily' ? 'D' : p === 'weekly' ? 'W' : 'M'}
                      </button>
                    ))}
                  </div>
                </div>
                <button
                  onClick={() => setHideFlatDays((v) => !v)}
                  style={{
                    background: hideFlatDays ? 'var(--green-dim)' : 'transparent',
                    border: `1px solid ${hideFlatDays ? 'var(--green)' : 'var(--line)'}`,
                    borderRadius: 3,
                    padding: '2px 8px',
                    fontSize: 8,
                    color: hideFlatDays ? 'var(--green)' : 'var(--t2)',
                    textTransform: 'uppercase',
                    letterSpacing: '0.08em',
                    fontWeight: 700,
                    cursor: 'pointer',
                    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                    lineHeight: 1,
                  }}
                >
                  {hideFlatDays ? 'Inactive Hidden' : 'Hide Inactive'}
                </button>
              </div>
              {selectedFeesTableRowsWithCumulative.length > 0 && breakdownPeriod !== 'daily' ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: 0, tableLayout: 'fixed' }}>
                    <colgroup>
                      <col style={{ width: '14%' }} />
                      <col style={{ width: '10%' }} />
                      <col style={{ width: '10%' }} />
                      <col style={{ width: '8%' }} />
                      <col style={{ width: '9%' }} />
                      <col style={{ width: '9%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '7%' }} />
                      <col style={{ width: '8%' }} />
                      <col style={{ width: '10%' }} />
                      <col style={{ width: '6%' }} />
                    </colgroup>
                    <thead style={{ position: 'sticky', top: 63, zIndex: 7, background: 'var(--bg0)' }}>
                      <tr style={{ borderBottom: '1px solid var(--line)' }}>
                        {['Period', 'Start ($)', 'End ($)', 'Return %', 'P&L ($)', 'Cum P&L ($)', 'Fees ($)', 'Volume ($)', 'Avg Lev', 'Exposure', 'CapEx'].map((h) => (
                          <th
                            key={`agg-head-${h}`}
                            style={{
                              textAlign: h === 'Period' ? 'left' : 'right',
                              padding: '6px 8px',
                              fontSize: 8,
                              color: 'var(--t3)',
                              textTransform: 'uppercase',
                              letterSpacing: '0.08em',
                              fontWeight: 700,
                              fontFamily: 'var(--font-space-mono), Space Mono, monospace',
                              borderBottom: '1px solid var(--line2)',
                            }}
                          >
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {(() => {
                        let cumPnl = 0;
                        const fmtN = (v: number) => new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(v);
                        const MONO = 'var(--font-space-mono), Space Mono, monospace';
                        const fmtD = (v: number) => `$${new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(v)}`;
                        const fmtD0 = (v: number) => `$${new Intl.NumberFormat(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 }).format(v)}`;
                        return aggregatedPeriodRows.filter((row) => !hideFlatDays || row.activeDays > 0).map((row, i) => {
                          cumPnl += row.pnl;
                          const bg = i % 2 === 0 ? 'var(--bg1)' : 'var(--bg2)';
                          const retColor = row.retNet >= 0 ? 'var(--green)' : 'var(--red)';
                          const inactive = row.activeDays === 0;
                          return (
                            <tr key={`agg-row-${i}`} style={{ background: bg, borderBottom: '1px solid var(--line)', opacity: inactive ? 0.5 : 1 }}>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t2)', fontFamily: MONO, whiteSpace: 'nowrap' }}>{row.label}</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: MONO }}>{fmtD0(row.startEquity)}</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: MONO }}>{fmtD0(row.endEquity)}</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: retColor, textAlign: 'right', fontFamily: MONO, fontWeight: 600 }}>{row.retNet >= 0 ? '+' : ''}{row.retNet.toFixed(2)}%</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: row.pnl >= 0 ? 'var(--green)' : 'var(--red)', textAlign: 'right', fontFamily: MONO }}>{row.pnl >= 0 ? '+' : ''}{fmtD0(row.pnl)}</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: cumPnl >= 0 ? 'var(--green)' : 'var(--red)', textAlign: 'right', fontFamily: MONO }}>{cumPnl >= 0 ? '+' : ''}{fmtD0(cumPnl)}</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--amber)', textAlign: 'right', fontFamily: MONO }}>{fmtD(row.totalFees)}</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: MONO }}>{fmtD(row.totalVolume)}</td>
                              <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: MONO }}>{row.avgLev !== null ? row.avgLev.toFixed(2) : '—'}</td>
                              {(() => {
                                const exposure = row.totalDays > 0 ? row.activeDays / row.totalDays : 0;
                                const capEx = exposure > 0 ? row.retNet / exposure : 0;
                                return (
                                  <>
                                    <td style={{ padding: '6px 8px', fontSize: 10, color: 'var(--t1)', textAlign: 'right', fontFamily: MONO }}>{(exposure * 100).toFixed(1)}%</td>
                                    <td style={{ padding: '6px 8px', fontSize: 10, color: capEx >= 0 ? 'var(--green)' : 'var(--red)', textAlign: 'right', fontFamily: MONO }}>{capEx >= 0 ? '+' : ''}{capEx.toFixed(2)}%</td>
                                  </>
                                );
                              })()}
                            </tr>
                          );
                        });
                      })()}
                    </tbody>
                  </table>
                </div>
              ) : selectedFeesTableRowsWithCumulative.length > 0 ? (
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
                      {selectedFeesTableRowsWithCumulative.filter((row) => !hideFlatDays || !row.no_entry).map((row, i) => {
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

          {/* ── Performance by Hour of Day ──────────────────────────────
              Sits directly under the Fees Panel. Filter-specific via
              activeDays (filter='pass' + conviction='pass') + per-filter
              intraday_exit_bars to omit hours past each day's actual exit. */}
          {(() => {
            const dpRoot = (results as Record<string, unknown>)?.metrics as Record<string, unknown> | undefined;
            type PortfolioDayHour = {
              symbols: string[];
              filter: string;
              filter_name: string;
              conviction: string;
              raw_roi: number;
              strat_roi: number;
              exit_reason: string;
            };
            const byFilter = dpRoot?.daily_portfolio_by_filter as Record<string, Record<string, PortfolioDayHour>> | undefined;
            const portfolio: Record<string, PortfolioDayHour> | undefined = (() => {
              if (byFilter && selectedFilter) {
                const exact = byFilter[selectedFilter];
                if (exact) return exact;
                const normSel = selectedFilter.replace(/\s+/g, '_');
                for (const [k, v] of Object.entries(byFilter)) {
                  if (k.replace(/\s+/g, '_') === normSel) return v;
                }
              }
              return dpRoot?.daily_portfolio as Record<string, PortfolioDayHour> | undefined;
            })();
            if (!portfolio || Object.keys(portfolio).length === 0) return null;
            const activeDays = Object.entries(portfolio)
              .filter(([, v]) => v.filter === 'pass' && v.conviction === 'pass')
              .map(([date, v]) => ({ date, stratRoi: v.strat_roi }));
            if (activeDays.length === 0) return null;
            const intradayBars = (m.intraday_bars ?? (results as Record<string, unknown>)?.intraday_bars) as Record<string, Array<number | null>> | null | undefined;
            if (!intradayBars || Object.keys(intradayBars).length === 0) return null;
            const exitBars = selectedFilter
              ? ((m.intraday_exit_bars ?? (results as Record<string, unknown>)?.intraday_exit_bars) as Record<string, Record<string, number>> | null | undefined)?.[selectedFilter] ?? null
              : null;
            const levByDate: Record<string, number> = {};
            for (const r of selectedFeesTableRows) {
              if (typeof r.lev === 'number' && Number.isFinite(r.lev)) {
                levByDate[r.date] = r.lev;
              }
            }
            return (
              <HourlyPerformanceChart
                activeDays={activeDays}
                intradayBars={intradayBars}
                exitBars={exitBars}
                levByDate={levByDate}
              />
            );
          })()}

          {/* ── Per-Day Portfolio Breakdown ──────────────────────────── */}
          {(() => {
            const dp = (results as Record<string, unknown>)?.metrics as Record<string, unknown> | undefined;
            type PortfolioDay = {
              symbols: string[];
              filter: string;
              filter_name: string;
              conviction: string;
              raw_roi: number;
              strat_roi: number;
              exit_reason: string;
            };
            // Prefer per-filter portfolio keyed by selectedFilter, fall back to legacy single portfolio
            const byFilter = dp?.daily_portfolio_by_filter as Record<string, Record<string, PortfolioDay>> | undefined;
            const portfolio: Record<string, PortfolioDay> | undefined = (() => {
              if (byFilter && selectedFilter) {
                // Try exact match first, then normalized match
                const exact = byFilter[selectedFilter];
                if (exact) return exact;
                const normSel = selectedFilter.replace(/\s+/g, '_');
                for (const [k, v] of Object.entries(byFilter)) {
                  if (k.replace(/\s+/g, '_') === normSel) return v;
                }
              }
              // Fall back to legacy single portfolio
              return dp?.daily_portfolio as Record<string, PortfolioDay> | undefined;
            })();
            if (!portfolio || Object.keys(portfolio).length === 0) return null;
            const days = Object.entries(portfolio).sort(([a], [b]) => a.localeCompare(b));

            // Compute KPIs
            const activeDays = days.filter(([,v]) => v.filter === 'pass' && v.conviction === 'pass');
            const filteredDays = days.filter(([,v]) => v.filter === 'filtered');
            const convFailDays = days.filter(([,v]) => v.conviction === 'fail');
            const rawRois = activeDays.map(([,v]) => v.raw_roi);
            const stratRois = activeDays.map(([,v]) => v.strat_roi);
            const rawWins = rawRois.filter(r => r > 0);
            const rawLosses = rawRois.filter(r => r < 0);
            const stratWins = stratRois.filter(r => r > 0);
            const stratLosses = stratRois.filter(r => r < 0);
            const avg = (arr: number[]) => arr.length > 0 ? arr.reduce((a,b) => a+b, 0) / arr.length : 0;
            const rawWinRate = rawRois.length > 0 ? (rawWins.length / rawRois.length * 100) : 0;
            const stratWinRate = stratRois.length > 0 ? (stratWins.length / stratRois.length * 100) : 0;
            const exitReasons: Record<string, number> = {};
            activeDays.forEach(([,v]) => { exitReasons[v.exit_reason] = (exitReasons[v.exit_reason] || 0) + 1; });

            const kpiStyle: React.CSSProperties = { background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: '10px 12px', flex: 1, minWidth: 0 };
            const kpiLabel: React.CSSProperties = { fontSize: 8, fontWeight: 700, letterSpacing: '0.12em', color: 'var(--t3)', textTransform: 'uppercase', marginBottom: 4 };
            const kpiVal: React.CSSProperties = { fontSize: 14, fontWeight: 700, fontFamily: 'var(--font-space-mono), Space Mono, monospace' };
            const kpiSub: React.CSSProperties = { fontSize: 9, color: 'var(--t2)', marginTop: 2 };

            return (
              <div style={{ marginTop: 16, border: '1px solid var(--line)', borderRadius: 3, padding: 12, background: 'var(--bg1)' }}>
                <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 10 }}>
                  Daily Portfolio Breakdown
                </div>

                {/* KPI Cards: Raw vs Strategy comparison */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 8, marginBottom: 14 }}>
                  <div style={kpiStyle}>
                    <div style={kpiLabel}>Days</div>
                    <div style={{ ...kpiVal, color: 'var(--t0)' }}>{activeDays.length} / {days.length}</div>
                    <div style={kpiSub}>active · {filteredDays.length} filtered · {convFailDays.length} conv fail</div>
                  </div>
                  <div style={kpiStyle}>
                    <div style={kpiLabel}>Win Rate</div>
                    <div style={{ display: 'flex', gap: 12 }}>
                      <div>
                        <div style={{ ...kpiVal, color: rawWinRate >= 50 ? 'var(--green)' : 'var(--red)' }}>{rawWinRate.toFixed(1)}%</div>
                        <div style={kpiSub}>raw</div>
                      </div>
                      <div>
                        <div style={{ ...kpiVal, color: stratWinRate >= 50 ? 'var(--green)' : 'var(--red)' }}>{stratWinRate.toFixed(1)}%</div>
                        <div style={kpiSub}>strat</div>
                      </div>
                    </div>
                  </div>
                  <div style={kpiStyle}>
                    <div style={kpiLabel}>Avg Return / Day</div>
                    <div style={{ display: 'flex', gap: 12 }}>
                      <div>
                        <div style={{ ...kpiVal, color: avg(rawRois) >= 0 ? 'var(--green)' : 'var(--red)' }}>{avg(rawRois) >= 0 ? '+' : ''}{avg(rawRois).toFixed(2)}%</div>
                        <div style={kpiSub}>raw</div>
                      </div>
                      <div>
                        <div style={{ ...kpiVal, color: avg(stratRois) >= 0 ? 'var(--green)' : 'var(--red)' }}>{avg(stratRois) >= 0 ? '+' : ''}{avg(stratRois).toFixed(2)}%</div>
                        <div style={kpiSub}>strat</div>
                      </div>
                    </div>
                  </div>
                  <div style={kpiStyle}>
                    <div style={kpiLabel}>Cumulative</div>
                    <div style={{ display: 'flex', gap: 12 }}>
                      <div>
                        <div style={{ ...kpiVal, fontSize: 12, color: rawRois.reduce((a,b)=>a+b,0) >= 0 ? 'var(--green)' : 'var(--red)' }}>{rawRois.reduce((a,b)=>a+b,0) >= 0 ? '+' : ''}{rawRois.reduce((a,b)=>a+b,0).toFixed(1)}%</div>
                        <div style={kpiSub}>raw</div>
                      </div>
                      <div>
                        <div style={{ ...kpiVal, fontSize: 12, color: stratRois.reduce((a,b)=>a+b,0) >= 0 ? 'var(--green)' : 'var(--red)' }}>{stratRois.reduce((a,b)=>a+b,0) >= 0 ? '+' : ''}{stratRois.reduce((a,b)=>a+b,0).toFixed(1)}%</div>
                        <div style={kpiSub}>strat</div>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Second row: Avg Win/Loss + Best/Worst + Exit Reasons */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8, marginBottom: 14 }}>
                  <div style={kpiStyle}>
                    <div style={kpiLabel}>Avg Win / Avg Loss</div>
                    <div style={{ display: 'flex', gap: 16 }}>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--t2)', marginBottom: 2 }}>Raw</div>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--green)', fontFamily: 'var(--font-space-mono)' }}>+{avg(rawWins).toFixed(2)}%</span>
                        <span style={{ fontSize: 10, color: 'var(--t3)', margin: '0 4px' }}>/</span>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--red)', fontFamily: 'var(--font-space-mono)' }}>{avg(rawLosses).toFixed(2)}%</span>
                      </div>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--t2)', marginBottom: 2 }}>Strat</div>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--green)', fontFamily: 'var(--font-space-mono)' }}>+{avg(stratWins).toFixed(2)}%</span>
                        <span style={{ fontSize: 10, color: 'var(--t3)', margin: '0 4px' }}>/</span>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--red)', fontFamily: 'var(--font-space-mono)' }}>{avg(stratLosses).toFixed(2)}%</span>
                      </div>
                    </div>
                  </div>
                  <div style={kpiStyle}>
                    <div style={kpiLabel}>Best / Worst Day</div>
                    <div style={{ display: 'flex', gap: 16 }}>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--t2)', marginBottom: 2 }}>Raw</div>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--green)', fontFamily: 'var(--font-space-mono)' }}>+{rawRois.length ? Math.max(...rawRois).toFixed(2) : '0.00'}%</span>
                        <span style={{ fontSize: 10, color: 'var(--t3)', margin: '0 4px' }}>/</span>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--red)', fontFamily: 'var(--font-space-mono)' }}>{rawRois.length ? Math.min(...rawRois).toFixed(2) : '0.00'}%</span>
                      </div>
                      <div>
                        <div style={{ fontSize: 10, color: 'var(--t2)', marginBottom: 2 }}>Strat</div>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--green)', fontFamily: 'var(--font-space-mono)' }}>+{stratRois.length ? Math.max(...stratRois).toFixed(2) : '0.00'}%</span>
                        <span style={{ fontSize: 10, color: 'var(--t3)', margin: '0 4px' }}>/</span>
                        <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--red)', fontFamily: 'var(--font-space-mono)' }}>{stratRois.length ? Math.min(...stratRois).toFixed(2) : '0.00'}%</span>
                      </div>
                    </div>
                  </div>
                  <div style={kpiStyle}>
                    <div style={kpiLabel}>Exit Reasons</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                      {Object.entries(exitReasons).sort(([,a],[,b]) => b - a).map(([reason, count]) => (
                        <span key={reason} style={{ fontSize: 9, padding: '2px 6px', border: '1px solid var(--line2)', borderRadius: 2, color: 'var(--t2)', fontFamily: 'var(--font-space-mono)' }}>
                          {reason === 'early_exit' ? 'EXIT' : reason === 'held' ? 'HELD' : reason.toUpperCase()} {count}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>

                {/* ── Risk Management Effectiveness ──────────────────── */}
                {(() => {
                  const whipsawed = activeDays.filter(([,v]) => v.raw_roi > 0 && v.strat_roi < 0);
                  const protected_ = activeDays.filter(([,v]) => v.raw_roi < -3 && v.strat_roi > v.raw_roi * 1.33);
                  const whipsawCost = whipsawed.reduce((s,[,v]) => s + (v.raw_roi - v.strat_roi), 0);
                  const protectionValue = protected_.reduce((s,[,v]) => s + (v.strat_roi - v.raw_roi * 1.33), 0);
                  return (
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8, marginBottom: 14 }}>
                      <div style={kpiStyle}>
                        <div style={kpiLabel}>Whipsaw Cost (raw win → strat loss)</div>
                        <div style={{ ...kpiVal, color: 'var(--amber)' }}>{whipsawed.length} days</div>
                        <div style={kpiSub}>Lost {whipsawCost.toFixed(1)}% cumulative from stops on recovering days</div>
                      </div>
                      <div style={kpiStyle}>
                        <div style={kpiLabel}>Protection Value (raw deep loss → strat less bad)</div>
                        <div style={{ ...kpiVal, color: 'var(--green)' }}>{protected_.length} days</div>
                        <div style={kpiSub}>Saved {protectionValue.toFixed(1)}% cumulative from risk controls</div>
                      </div>
                    </div>
                  );
                })()}

                {/* ── Filter Analysis ────────────────────────────────── */}
                {(() => {
                  // What would have happened on filtered days if we traded?
                  // We can only show raw_roi=0 for filtered days, but we know
                  // the conviction/strat_roi for days that passed filter but failed conviction
                  const convFailRois = convFailDays.map(([,v]) => v.raw_roi);
                  const filteredCount = filteredDays.length;
                  const convFailCount = convFailDays.length;
                  return (
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 8, marginBottom: 14 }}>
                      <div style={kpiStyle}>
                        <div style={kpiLabel}>Filtered Days (sat flat)</div>
                        <div style={{ ...kpiVal, color: 'var(--t1)' }}>{filteredCount}</div>
                        <div style={kpiSub}>{(filteredCount / days.length * 100).toFixed(1)}% of all days</div>
                      </div>
                      <div style={kpiStyle}>
                        <div style={kpiLabel}>Conviction Failures</div>
                        <div style={{ ...kpiVal, color: convFailCount > 0 ? 'var(--amber)' : 'var(--t1)' }}>{convFailCount}</div>
                        <div style={kpiSub}>{convFailCount > 0 ? `Avg raw ROI on fail days: ${avg(convFailRois).toFixed(2)}%` : 'No conviction failures'}</div>
                      </div>
                      <div style={kpiStyle}>
                        <div style={kpiLabel}>Filter Hit Rate</div>
                        <div style={{ ...kpiVal, color: 'var(--t0)' }}>{activeDays.length > 0 ? (rawWins.length / activeDays.length * 100).toFixed(1) : '0'}%</div>
                        <div style={kpiSub}>of active days were profitable (raw)</div>
                      </div>
                    </div>
                  );
                })()}

                {/* ── Symbol Frequency Analysis ──────────────────────── */}
                {(() => {
                  const symbolStats: Record<string, { count: number; wins: number; totalRaw: number; totalStrat: number }> = {};
                  activeDays.forEach(([,v]) => {
                    const isWin = v.raw_roi > 0;
                    v.symbols.forEach(sym => {
                      if (!symbolStats[sym]) symbolStats[sym] = { count: 0, wins: 0, totalRaw: 0, totalStrat: 0 };
                      symbolStats[sym].count++;
                      if (isWin) symbolStats[sym].wins++;
                      symbolStats[sym].totalRaw += v.raw_roi / Math.max(v.symbols.length, 1);
                      symbolStats[sym].totalStrat += v.strat_roi / Math.max(v.symbols.length, 1);
                    });
                  });
                  const sorted = Object.entries(symbolStats).sort(([,a],[,b]) => b.count - a.count);
                  const top10 = sorted.slice(0, 10);
                  const uniqueSymbols = sorted.length;
                  const topConcentration = top10.reduce((s,[,v]) => s + v.count, 0);
                  const totalAppearances = sorted.reduce((s,[,v]) => s + v.count, 0);
                  return (
                    <div style={{ marginBottom: 14, border: '1px solid var(--line)', borderRadius: 3, padding: 10, background: 'var(--bg2)' }}>
                      <div style={{ ...kpiLabel, marginBottom: 8 }}>
                        Symbol Frequency · {uniqueSymbols} unique symbols · top 10 = {totalAppearances > 0 ? (topConcentration / totalAppearances * 100).toFixed(0) : 0}% of appearances
                      </div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                        {top10.map(([sym, s]) => (
                          <div key={sym} style={{ padding: '4px 8px', border: '1px solid var(--line)', borderRadius: 3, background: 'var(--bg1)', fontSize: 9, fontFamily: 'var(--font-space-mono)' }}>
                            <span style={{ color: 'var(--t0)', fontWeight: 700 }}>{sym}</span>
                            <span style={{ color: 'var(--t3)', margin: '0 4px' }}>×{s.count}</span>
                            <span style={{ color: s.wins / s.count >= 0.5 ? 'var(--green)' : 'var(--red)', fontWeight: 700 }}>{(s.wins / s.count * 100).toFixed(0)}%W</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })()}

                {/* ── Temporal Patterns ──────────────────────────────── */}
                {(() => {
                  const dowNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
                  const dowStats: Record<number, { count: number; wins: number; totalRaw: number; totalStrat: number }> = {};
                  const monthStats: Record<string, { count: number; wins: number; totalRaw: number; totalStrat: number }> = {};
                  activeDays.forEach(([dateStr, v]) => {
                    const d = new Date(dateStr + 'T00:00:00Z');
                    const dow = d.getUTCDay();
                    if (!dowStats[dow]) dowStats[dow] = { count: 0, wins: 0, totalRaw: 0, totalStrat: 0 };
                    dowStats[dow].count++;
                    if (v.strat_roi > 0) dowStats[dow].wins++;
                    dowStats[dow].totalRaw += v.raw_roi;
                    dowStats[dow].totalStrat += v.strat_roi;

                    const monthKey = dateStr.slice(0, 7);
                    if (!monthStats[monthKey]) monthStats[monthKey] = { count: 0, wins: 0, totalRaw: 0, totalStrat: 0 };
                    monthStats[monthKey].count++;
                    if (v.strat_roi > 0) monthStats[monthKey].wins++;
                    monthStats[monthKey].totalRaw += v.raw_roi;
                    monthStats[monthKey].totalStrat += v.strat_roi;
                  });

                  // Streaks
                  let maxWinStreak = 0, maxLossStreak = 0, curWin = 0, curLoss = 0;
                  activeDays.forEach(([,v]) => {
                    if (v.strat_roi > 0) { curWin++; curLoss = 0; maxWinStreak = Math.max(maxWinStreak, curWin); }
                    else { curLoss++; curWin = 0; maxLossStreak = Math.max(maxLossStreak, curLoss); }
                  });

                  return (
                    <>
                      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 8, marginBottom: 14 }}>
                        {/* Day of Week */}
                        <div style={{ border: '1px solid var(--line)', borderRadius: 3, padding: 10, background: 'var(--bg2)' }}>
                          <div style={{ ...kpiLabel, marginBottom: 8 }}>Performance by Day of Week</div>
                          <div style={{ display: 'flex', gap: 4 }}>
                            {[1,2,3,4,5,6,0].map(dow => {
                              const s = dowStats[dow];
                              if (!s || s.count === 0) return (
                                <div key={dow} style={{ flex: 1, textAlign: 'center', padding: '6px 0' }}>
                                  <div style={{ fontSize: 9, color: 'var(--t3)' }}>{dowNames[dow]}</div>
                                  <div style={{ fontSize: 9, color: 'var(--t3)' }}>—</div>
                                </div>
                              );
                              const avgRet = s.totalStrat / s.count;
                              return (
                                <div key={dow} style={{ flex: 1, textAlign: 'center', padding: '6px 4px', borderRadius: 2, background: avgRet > 0 ? 'var(--green-dim)' : 'var(--red-dim)' }}>
                                  <div style={{ fontSize: 9, color: 'var(--t2)', marginBottom: 2 }}>{dowNames[dow]}</div>
                                  <div style={{ fontSize: 11, fontWeight: 700, color: avgRet >= 0 ? 'var(--green)' : 'var(--red)', fontFamily: 'var(--font-space-mono)' }}>{avgRet >= 0 ? '+' : ''}{avgRet.toFixed(1)}%</div>
                                  <div style={{ fontSize: 8, color: 'var(--t3)' }}>{s.count}d · {(s.wins/s.count*100).toFixed(0)}%W</div>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                        {/* Streaks */}
                        <div style={kpiStyle}>
                          <div style={kpiLabel}>Streaks</div>
                          <div style={{ display: 'flex', gap: 16 }}>
                            <div>
                              <div style={{ ...kpiVal, color: 'var(--green)' }}>{maxWinStreak}</div>
                              <div style={kpiSub}>longest win</div>
                            </div>
                            <div>
                              <div style={{ ...kpiVal, color: 'var(--red)' }}>{maxLossStreak}</div>
                              <div style={kpiSub}>longest loss</div>
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Monthly Heatmap */}
                      <div style={{ border: '1px solid var(--line)', borderRadius: 3, padding: 10, background: 'var(--bg2)', marginBottom: 14 }}>
                        <div style={{ ...kpiLabel, marginBottom: 8 }}>Monthly Performance (Strat ROI)</div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                          {Object.entries(monthStats).sort(([a],[b]) => a.localeCompare(b)).map(([month, s]) => {
                            const avgRet = s.totalStrat / s.count;
                            return (
                              <div key={month} style={{ padding: '4px 8px', borderRadius: 2, background: avgRet > 0 ? 'var(--green-dim)' : 'var(--red-dim)', border: `1px solid ${avgRet > 0 ? 'var(--green)' : 'var(--red)'}`, minWidth: 80, textAlign: 'center' }}>
                                <div style={{ fontSize: 9, color: 'var(--t2)' }}>{month}</div>
                                <div style={{ fontSize: 11, fontWeight: 700, color: avgRet >= 0 ? 'var(--green)' : 'var(--red)', fontFamily: 'var(--font-space-mono)' }}>{avgRet >= 0 ? '+' : ''}{avgRet.toFixed(1)}%</div>
                                <div style={{ fontSize: 8, color: 'var(--t3)' }}>{s.count}d · {(s.wins/s.count*100).toFixed(0)}%W</div>
                              </div>
                            );
                          })}
                        </div>
                      </div>

                    </>
                  );
                })()}

                {/* ── Portfolio Construction ─────────────────────────── */}
                {(() => {
                  const sizeStats: Record<number, { count: number; totalRaw: number; totalStrat: number; wins: number }> = {};
                  activeDays.forEach(([,v]) => {
                    const size = v.symbols.length;
                    if (!sizeStats[size]) sizeStats[size] = { count: 0, totalRaw: 0, totalStrat: 0, wins: 0 };
                    sizeStats[size].count++;
                    sizeStats[size].totalRaw += v.raw_roi;
                    sizeStats[size].totalStrat += v.strat_roi;
                    if (v.strat_roi > 0) sizeStats[size].wins++;
                  });
                  const avgSize = activeDays.length > 0 ? activeDays.reduce((s,[,v]) => s + v.symbols.length, 0) / activeDays.length : 0;
                  const winAvgSize = rawWins.length > 0 ? activeDays.filter(([,v]) => v.raw_roi > 0).reduce((s,[,v]) => s + v.symbols.length, 0) / rawWins.length : 0;
                  const lossAvgSize = rawLosses.length > 0 ? activeDays.filter(([,v]) => v.raw_roi < 0).reduce((s,[,v]) => s + v.symbols.length, 0) / rawLosses.length : 0;
                  return (
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: 8, marginBottom: 14 }}>
                      <div style={kpiStyle}>
                        <div style={kpiLabel}>Portfolio Size</div>
                        <div style={{ ...kpiVal, color: 'var(--t0)' }}>{avgSize.toFixed(1)}</div>
                        <div style={kpiSub}>avg symbols/day</div>
                        <div style={{ marginTop: 6, fontSize: 9, color: 'var(--t2)' }}>
                          Win days: {winAvgSize.toFixed(1)} avg · Loss days: {lossAvgSize.toFixed(1)} avg
                        </div>
                      </div>
                      <div style={{ border: '1px solid var(--line)', borderRadius: 3, padding: 10, background: 'var(--bg2)' }}>
                        <div style={{ ...kpiLabel, marginBottom: 8 }}>Return by Portfolio Size</div>
                        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                          {Object.entries(sizeStats).sort(([a],[b]) => Number(a) - Number(b)).map(([size, s]) => {
                            const avgRet = s.totalStrat / s.count;
                            return (
                              <div key={size} style={{ padding: '4px 8px', borderRadius: 2, background: avgRet > 0 ? 'var(--green-dim)' : 'var(--red-dim)', border: `1px solid ${avgRet > 0 ? 'var(--green)' : 'var(--red)'}`, textAlign: 'center', minWidth: 60 }}>
                                <div style={{ fontSize: 9, color: 'var(--t2)' }}>{size} sym{Number(size)!==1?'s':''}</div>
                                <div style={{ fontSize: 11, fontWeight: 700, color: avgRet >= 0 ? 'var(--green)' : 'var(--red)', fontFamily: 'var(--font-space-mono)' }}>{avgRet >= 0 ? '+' : ''}{avgRet.toFixed(1)}%</div>
                                <div style={{ fontSize: 8, color: 'var(--t3)' }}>{s.count}d · {(s.wins/s.count*100).toFixed(0)}%W</div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    </div>
                  );
                })()}

                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
                    <thead>
                      <tr>
                        {['Date', 'Symbols', 'Filter', 'Conviction', 'Raw ROI', 'Strat ROI', 'Exit'].map((h) => (
                          <th key={h} style={{
                            fontSize: 9, fontWeight: 700, letterSpacing: '0.08em',
                            color: 'var(--t3)', textTransform: 'uppercase',
                            textAlign: h === 'Symbols' ? 'left' : 'right',
                            padding: '8px 8px', borderBottom: '1px solid var(--line)',
                            whiteSpace: 'nowrap',
                          }}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {days.map(([date, d]) => {
                        const filterColor = d.filter === 'pass' ? 'var(--green)' : d.filter === 'filtered' ? 'var(--amber)' : 'var(--t3)';
                        const filterBg = d.filter === 'pass' ? 'var(--green-dim)' : d.filter === 'filtered' ? 'var(--amber-dim)' : 'transparent';
                        const convColor = d.conviction === 'pass' ? 'var(--green)' : d.conviction === 'fail' ? 'var(--red)' : 'var(--t3)';
                        const convBg = d.conviction === 'pass' ? 'var(--green-dim)' : d.conviction === 'fail' ? 'var(--red-dim)' : 'transparent';
                        const exitLabel = d.exit_reason === 'held' ? '' : d.exit_reason === 'filtered' ? '' : d.exit_reason === 'no_entry' ? '' : d.exit_reason === 'early_exit' ? 'EXIT' : d.exit_reason.toUpperCase();
                        return (
                          <tr key={date} style={{ borderBottom: '1px solid var(--line)' }}>
                            <td style={{ fontSize: 10, color: 'var(--t0)', fontWeight: 700, padding: '6px 8px', whiteSpace: 'nowrap', textAlign: 'right' }}>{date}</td>
                            <td style={{ fontSize: 9, color: 'var(--t2)', padding: '6px 8px', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={d.symbols.join(', ')}>
                              {d.symbols.length > 0 ? d.symbols.join(', ') : '—'}
                            </td>
                            <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                              <span style={{ fontSize: 8, fontWeight: 700, letterSpacing: '0.06em', padding: '2px 5px', borderRadius: 2, background: filterBg, color: filterColor, border: `1px solid ${filterColor}` }}>
                                {d.filter === 'pass' ? 'PASS' : d.filter === 'filtered' ? 'FLAT' : d.filter.toUpperCase()}
                              </span>
                            </td>
                            <td style={{ padding: '6px 8px', textAlign: 'right' }}>
                              <span style={{ fontSize: 8, fontWeight: 700, letterSpacing: '0.06em', padding: '2px 5px', borderRadius: 2, background: convBg, color: convColor, border: `1px solid ${convColor}` }}>
                                {d.conviction === 'n/a' ? '—' : d.conviction === 'pass' ? 'PASS' : 'FAIL'}
                              </span>
                            </td>
                            <td style={{ fontSize: 10, fontWeight: 700, padding: '6px 8px', textAlign: 'right', color: d.raw_roi >= 0 ? 'var(--green)' : 'var(--red)' }}>
                              {d.filter === 'filtered' || d.conviction === 'fail' ? '—' : `${d.raw_roi >= 0 ? '+' : ''}${d.raw_roi.toFixed(2)}%`}
                            </td>
                            <td style={{ fontSize: 10, fontWeight: 700, padding: '6px 8px', textAlign: 'right', color: d.strat_roi >= 0 ? 'var(--green)' : 'var(--red)' }}>
                              {d.filter === 'filtered' || d.conviction === 'fail' ? '—' : `${d.strat_roi >= 0 ? '+' : ''}${d.strat_roi.toFixed(2)}%`}
                            </td>
                            <td style={{ fontSize: 9, color: 'var(--t3)', padding: '6px 8px', textAlign: 'right' }}>
                              {exitLabel || '—'}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            );
          })()}
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
        <>
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
                  <div
                    onClick={() => setFullReportTocOpen((v) => !v)}
                    style={{ fontSize: 12, color: 'var(--t1)', textTransform: 'uppercase', letterSpacing: '0.1em', fontWeight: 700, cursor: 'pointer', userSelect: 'none' }}
                  >
                    {fullReportTocOpen ? '▾' : '▸'} Full Report{' '}
                    <span style={{ fontSize: 9, color: 'var(--t4)', letterSpacing: '0.08em' }}>
                      • {fullReportSectionCount} sections
                    </span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    {(() => {
                      const anyOpen = Object.values(openFullReportCategories).some(Boolean)
                        || Object.values(openFullReportSectionKeys).some(Boolean);
                      return (
                        <button
                          onClick={() => {
                            if (anyOpen) {
                              setOpenFullReportCategories((prev) => {
                                const next = { ...prev };
                                for (const cat of fullReportCategoryGroups) next[cat.key] = false;
                                return next;
                              });
                              setOpenFullReportSectionKeys((prev) => {
                                const next = { ...prev };
                                for (const cat of fullReportCategoryGroups) {
                                  for (let idx = 0; idx < cat.sections.length; idx += 1) {
                                    next[`${cat.key}-${idx}-${cat.sections[idx].title}`] = false;
                                  }
                                }
                                return next;
                              });
                            } else {
                              setOpenFullReportCategories((prev) => {
                                const next = { ...prev };
                                for (const cat of fullReportCategoryGroups) next[cat.key] = true;
                                return next;
                              });
                              setOpenFullReportSectionKeys((prev) => {
                                const next = { ...prev };
                                for (const cat of fullReportCategoryGroups) {
                                  for (let idx = 0; idx < cat.sections.length; idx += 1) {
                                    next[`${cat.key}-${idx}-${cat.sections[idx].title}`] = true;
                                  }
                                }
                                return next;
                              });
                            }
                          }}
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
                        >
                          {anyOpen ? 'Collapse All' : 'Expand All'}
                        </button>
                      );
                    })()}
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
                      zIndex: 12,
                      background: 'var(--bg2)',
                      paddingTop: 8,
                      paddingBottom: fullReportTocOpen ? 8 : 4,
                      borderBottom: '1px solid var(--line)',
                      display: 'flex',
                      flexDirection: 'column',
                      gap: 8,
                    }}
                  >
                    {fullReportTocOpen && (
                      <>
                        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }}>
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
                      </>
                    )}
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
                        setOpenFullReportCategories((prev) => {
                          if (prev[cat.key] === isOpen) return prev;
                          return { ...prev, [cat.key]: isOpen };
                        });
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
                          position: 'sticky',
                          top: fullReportTocOpen ? 104 : 34,
                          zIndex: 9,
                          background: 'var(--bg1)',
                          padding: '6px 0',
                          margin: '-6px 0',
                        }}
                      >
                        <span>{cat.title} • {cat.sections.length}</span>
                        <span style={{ display: 'inline-flex', gap: 6 }}>
                          <button
                            onClick={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              setOpenFullReportCategories((prev) => ({ ...prev, [cat.key]: true }));
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
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginTop: 8, marginLeft: 24, paddingLeft: 10 }}>
                        {cat.sections.map((section, idx) => (
                        <details
                          key={`${cat.key}-${idx}-${section.title}`}
                          open={!!openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`]}
                            onToggle={(e) => {
                              const key = `${cat.key}-${idx}-${section.title}`;
                              const isOpen = (e.currentTarget as HTMLDetailsElement).open;
                              setOpenFullReportSectionKeys((prev) => {
                                if (prev[key] === isOpen) return prev;
                                return { ...prev, [key]: isOpen };
                              });
                            }}
                            style={{
                              border: '1px solid var(--line2)',
                              borderRadius: 3,
                              padding: '6px 8px',
                            background: 'var(--bg0)',
                            width: 'calc(100% - 24px)',
                          }}
                        >
                          <summary style={{
                            cursor: 'pointer',
                            fontSize: 11,
                            color: openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`] ? 'var(--t0)' : 'var(--t1)',
                            fontWeight: 700,
                            letterSpacing: '0.06em',
                            textTransform: 'uppercase',
                            position: 'sticky',
                            top: fullReportTocOpen ? 134 : 64,
                            zIndex: 6,
                            background: openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`]
                              ? 'rgba(20, 22, 28, 0.98)'
                              : 'var(--bg0)',
                            padding: openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`] ? '8px 10px' : '4px 8px',
                            margin: openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`] ? '-6px -8px 0 -8px' : '-4px -8px',
                            borderBottom: openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`]
                              ? '1px solid var(--line)'
                              : '1px solid transparent',
                            boxShadow: openFullReportSectionKeys[`${cat.key}-${idx}-${section.title}`]
                              ? '0 8px 20px rgba(0,0,0,0.18)'
                              : 'none',
                          }}>
                            {section.title}
                          </summary>
                          <div style={{ marginTop: 14, marginLeft: 16, paddingLeft: 12, overflow: 'hidden' }}>
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
          {showFullReportBackToTop && (
            <button
              type="button"
              onClick={() => {
                resultsViewRootRef.current?.parentElement?.scrollTo({ top: 0, behavior: 'smooth' });
              }}
              aria-label="Back to top"
              title="Back to top"
              style={{
                position: 'fixed',
                left: '50%',
                transform: 'translateX(-50%)',
                bottom: 24,
                zIndex: 40,
                height: 40,
                minWidth: 40,
                padding: '0 12px',
                borderRadius: 999,
                border: '1px solid rgba(255,255,255,0.16)',
                background: 'rgba(10, 13, 20, 0.92)',
                color: 'var(--t1)',
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: '0.08em',
                textTransform: 'uppercase',
                cursor: 'pointer',
                boxShadow: '0 10px 30px rgba(0,0,0,0.35)',
                backdropFilter: 'blur(10px)',
              }}
            >
              ↑ Top
            </button>
          )}
        </>
      )}
    </div>
  );
}

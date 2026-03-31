'use client';

import { useMemo, useRef, useState } from 'react';

import MetricCard from '../ui/MetricCard';
import FilterTable from '../ui/FilterTable';

interface ResultsViewProps {
  results: Record<string, unknown> | null;
}

function fmtMetric(v: unknown, isInt = false): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return isInt ? String(Math.round(v)) : v.toFixed(3);
  return String(v);
}

function metricColor(key: string, value: unknown): string {
  if (typeof value !== 'number') return 'var(--t2)';
  const heuristics: Record<string, (v: number) => string> = {
    sortino: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar_ratio: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    omega: (v) => (v > 1.5 ? 'var(--green)' : v > 1 ? 'var(--amber)' : 'var(--red)'),
    ulcer_index: (v) => (v < 5 ? 'var(--green)' : v < 15 ? 'var(--amber)' : 'var(--red)'),
    fa_oos_sharpe: (v) => (v > 1 ? 'var(--green)' : v > 0.5 ? 'var(--amber)' : 'var(--red)'),
    dsr_pct: (v) => (v > 95 ? 'var(--green)' : v > 80 ? 'var(--amber)' : 'var(--red)'),
    cv: (v) => (v < 0.25 ? 'var(--green)' : v < 0.5 ? 'var(--amber)' : 'var(--red)'),
    flat_days: (v) => (v < 30 ? 'var(--green)' : v < 60 ? 'var(--amber)' : 'var(--red)'),
  };
  return heuristics[key]?.(value) ?? 'var(--t0)';
}

type XValue = number | string | Date;
type Point = { x: XValue; y: number } | number;

type ChartPoint = { px: number; py: number; y: number; x?: XValue };

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

function syntheticDateAt(index: number, total: number): Date {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  d.setDate(d.getDate() - (total - 1 - index));
  return d;
}

function makeTickIndices(total: number): number[] {
  if (total <= 1) return [0];
  const raw = [0, Math.floor((total - 1) * 0.25), Math.floor((total - 1) * 0.5), Math.floor((total - 1) * 0.75), total - 1];
  return Array.from(new Set(raw)).sort((a, b) => a - b);
}

function buildChartPoints(data: Point[], width: number, height: number, pad = 4): ChartPoint[] {
  if (!data || data.length === 0) return [];
  const values = data.map((d) => (typeof d === 'number' ? d : d.y));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  return values.map((v, i) => {
    const x = pad + (i / (values.length - 1)) * (width - pad * 2);
    const y = height - pad - ((v - min) / range) * (height - pad * 2);
    return {
      px: x,
      py: y,
      y: v,
      x: typeof data[i] === 'number' ? undefined : (data[i] as { x: XValue; y: number }).x,
    };
  });
}

function pointsToPolyline(points: ChartPoint[]): string {
  return points.map((p) => `${p.px.toFixed(1)},${p.py.toFixed(1)}`).join(' ');
}

function CurveCard({
  title,
  data,
  color,
  gradientId,
  height = 160,
  fillAbove = false,
}: {
  title: string;
  data: Point[] | null | undefined;
  color: string;
  gradientId: string;
  height?: number;
  fillAbove?: boolean;
}) {
  const W = 480;
  const H = height;
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const points = useMemo(() => (data && data.length > 1 ? buildChartPoints(data, W, H) : []), [data]);
  const polyline = points.length > 1 ? pointsToPolyline(points) : null;
  const hoverPoint = hoverIdx !== null ? points[hoverIdx] : null;
  const xHasRealDates = useMemo(() => points.some((p) => parseDateLike(p.x) !== null), [points]);
  const tickIndices = useMemo(() => makeTickIndices(points.length), [points.length]);

  // Build area path from polyline
  let areaPath = '';
  if (polyline) {
    const pts = polyline.split(' ');
    const first = pts[0];
    const last = pts[pts.length - 1];
    const lastX = last.split(',')[0];
    const firstX = first.split(',')[0];
    if (fillAbove) {
      areaPath = `M ${first} ${pts.slice(1).map((p) => `L ${p}`).join(' ')} L ${lastX},0 L ${firstX},0 Z`;
    } else {
      areaPath = `M ${first} ${pts.slice(1).map((p) => `L ${p}`).join(' ')} L ${lastX},${H} L ${firstX},${H} Z`;
    }
  }

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
      {polyline ? (
        <svg
          ref={svgRef}
          viewBox={`0 0 ${W} ${H}`}
          style={{ width: '100%', height: H, display: 'block' }}
          preserveAspectRatio="none"
          onMouseMove={(e) => {
            if (!svgRef.current || points.length === 0) return;
            const rect = svgRef.current.getBoundingClientRect();
            const x = ((e.clientX - rect.left) / rect.width) * W;
            let bestIdx = 0;
            let bestDist = Math.abs(points[0].px - x);
            for (let i = 1; i < points.length; i += 1) {
              const dist = Math.abs(points[i].px - x);
              if (dist < bestDist) {
                bestDist = dist;
                bestIdx = i;
              }
            }
            setHoverIdx(bestIdx);
          }}
          onMouseLeave={() => setHoverIdx(null)}
        >
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.20" />
              <stop offset="100%" stopColor={color} stopOpacity="0" />
            </linearGradient>
          </defs>
          {areaPath && <path d={areaPath} fill={`url(#${gradientId})`} />}
          <polyline
            points={polyline}
            fill="none"
            stroke={color}
            strokeWidth="1.5"
            vectorEffect="non-scaling-stroke"
          />
          {hoverPoint && (
            <>
              <line
                x1={hoverPoint.px}
                y1={0}
                x2={hoverPoint.px}
                y2={H}
                stroke="var(--line2)"
                strokeWidth="1"
                strokeDasharray="2 2"
              />
              <circle
                cx={hoverPoint.px}
                cy={hoverPoint.py}
                r={3}
                fill={color}
                stroke="var(--bg2)"
                strokeWidth="1"
              />
            </>
          )}
        </svg>
      ) : (
        <div style={{ height: H, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <span style={{ fontSize: 10, color: 'var(--t3)' }}>No chart data</span>
        </div>
      )}
      {hoverPoint && (
        <div
          style={{
            position: 'absolute',
            left: `${Math.min(92, Math.max(8, (hoverPoint.px / W) * 100))}%`,
            top: 30,
            transform: 'translateX(-50%)',
            background: 'var(--bg1)',
            border: '1px solid var(--line2)',
            color: 'var(--t0)',
            fontSize: 9,
            padding: '4px 6px',
            borderRadius: 2,
            pointerEvents: 'none',
            whiteSpace: 'nowrap',
          }}
        >
          x: {fmtDateLabel(parseDateLike(hoverPoint.x) ?? syntheticDateAt(hoverIdx ?? 0, points.length || 1))}  y: {hoverPoint.y.toFixed(3)}
        </div>
      )}
      {points.length > 1 && (
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            marginTop: 6,
            color: 'var(--t3)',
            fontSize: 9,
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          }}
        >
          {tickIndices.map((idx) => {
            const p = points[idx];
            const d = parseDateLike(p?.x) ?? syntheticDateAt(idx, points.length);
            return <span key={idx}>{fmtDateLabel(d)}</span>;
          })}
        </div>
      )}
      {!xHasRealDates && points.length > 1 && (
        <div style={{ marginTop: 4, color: 'var(--t3)', fontSize: 8 }}>
          Date axis inferred from series length.
        </div>
      )}
    </div>
  );
}

export default function ResultsView({ results }: ResultsViewProps) {
  if (!results) {
    return (
      <div style={{ padding: 16, color: 'var(--t3)', fontSize: 10 }}>
        No results available.
      </div>
    );
  }

  const m = (results.metrics ?? {}) as Record<string, unknown>;

  const metricCards = [
    { label: 'Sortino', key: 'sortino', value: fmtMetric(m.sortino) },
    { label: 'Calmar', key: 'calmar', value: fmtMetric(m.calmar ?? m.calmar_ratio) },
    { label: 'Omega', key: 'omega', value: fmtMetric(m.omega) },
    { label: 'Ulcer Index', key: 'ulcer_index', value: fmtMetric(m.ulcer_index) },
    { label: 'FA-OOS Sharpe', key: 'fa_oos_sharpe', value: fmtMetric(m.fa_oos_sharpe) },
    { label: 'DSR %', key: 'dsr_pct', value: fmtMetric(m.dsr_pct, false) },
    { label: 'WF-CV', key: 'cv', value: fmtMetric(m.cv) },
    { label: 'Flat Days', key: 'flat_days', value: fmtMetric(m.flat_days, true) },
  ];

  const equityCurve = (m.equity_curve ?? results.equity_curve) as Point[] | null | undefined;
  const drawdownCurve = (m.drawdown_curve ?? results.drawdown_curve) as Point[] | null | undefined;
  const filterComparison = (m.filter_comparison ?? results.filter_comparison) as Record<string, unknown>[] | null | undefined;

  return (
    <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Metric cards 4×2 grid */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(4, 1fr)',
          gap: 8,
        }}
      >
        {metricCards.map(({ label, key, value }) => (
          <MetricCard
            key={key}
            label={label}
            value={value}
            color={metricColor(key, m[key === 'calmar' ? (m.calmar !== undefined ? 'calmar' : 'calmar_ratio') : key])}
          />
        ))}
      </div>

      {/* Full-width stacked charts */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <CurveCard
          title="Equity Curve"
          data={equityCurve}
          color="#00c896"
          gradientId="equity-gradient"
          height={480}
        />
        <CurveCard
          title="Drawdown Curve"
          data={drawdownCurve}
          color="#ff4d4d"
          gradientId="drawdown-gradient"
          fillAbove
        />
      </div>

      {/* Filter comparison table */}
      <div
        style={{
          background: 'var(--bg2)',
          border: '1px solid var(--line)',
          borderRadius: 3,
          padding: 12,
        }}
      >
        <div
          style={{
            fontSize: 9,
            textTransform: 'uppercase',
            letterSpacing: '0.12em',
            color: 'var(--t3)',
            fontWeight: 700,
            marginBottom: 10,
          }}
        >
          FILTER COMPARISON
        </div>
        {/* eslint-disable-next-line @typescript-eslint/no-explicit-any */}
        <FilterTable rows={filterComparison as any} />
      </div>
    </div>
  );
}

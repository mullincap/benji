'use client';

/**
 * BasketDetailModal.tsx
 * =====================
 * Per-day basket diagnostic modal for the simulator's Breakdown tab.
 *
 * Triggered by clicking a daily row in the Breakdown table. Fetches
 * GET /api/jobs/{jobId}/baskets/{date} and renders:
 *
 *   - Header: date, basket sizes, Binance/BloFin toggle pill
 *   - Chart: per-symbol cumulative ROI lines + bold portfolio aggregate
 *   - Matrix: 30-min-sampled ROI grid (rows=bars, cols=symbols, values=%)
 *
 * The toggle controls which symbols are visible in both the chart and
 * the matrix. The portfolio aggregate switches between Binance and
 * BloFin variants (computed server-side, embedded in each bar).
 */

import { useEffect, useMemo, useState } from 'react';
import {
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';

interface SymbolMeta {
  base: string;
  in_binance_basket: boolean;
  in_blofin_basket: boolean;
  blofin_listed_at_date: boolean;
  list_ms: number | null;
  list_date: string | null;
}

interface Bar {
  ts: string;
  sym_returns: Record<string, number | null>;
  portfolio_binance: number;
  portfolio_blofin: number;
}

interface BasketDetail {
  job_id: string;
  date: string;
  session_start: string;
  bar_minutes: number;
  leverage: number;
  stop_raw_pct: number;
  binance_basket: string[];
  blofin_basket: string[];
  non_blofin_dropped: string[];
  bar_timestamps: string[];
  bars: Bar[];
  symbols: SymbolMeta[];
  // Audit-canonical fields. Present when the modal is opened with a
  // filter prop and that filter has a fees_table row for `date`. These
  // mirror the equity-curve / monthly-heatmap / fees-panel for the
  // same filter+date so all four views agree.
  audit_filter_label?: string | null;
  audit_daily_return_pct?: number | null;  // e.g. -9.01 for -9.01%
  audit_no_entry?: boolean | null;
  audit_no_entry_reason?: string | null;   // "filter" | "conviction_gate"
}

const PALETTE = [
  '#00c896', '#378ADD', '#f0a500', '#cc66ff', '#ff6b6b',
  '#00d4ff', '#ffd166', '#ff9f43', '#a29bfe', '#55efc4',
  '#fd79a8', '#74b9ff', '#fdcb6e', '#e17055', '#81ecec',
];

// Sample the bar grid for the matrix display: every Nth bar.
// 5-min bars × 6 = 30-min sampling. Keeps the matrix readable.
const MATRIX_SAMPLE_EVERY = 6;

function fmtPct(v: number | null | undefined, digits = 2): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return '—';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(digits)}%`;
}

function pctColor(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return 'var(--t3)';
  if (v > 0.05) return 'var(--green)';
  if (v < -0.05) return 'var(--red)';
  return 'var(--t2)';
}

function fmtTime(ts: string): string {
  // ts like "2025-04-28T06:00:00" or "2025-04-28T06:00:00Z"
  const t = ts.replace(/Z$/, '').split('T')[1] || '';
  return t.slice(0, 5);
}

// Card shape mirrors the manager portfolio-detail page (page.tsx:643).
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
        background: 'var(--bg1)',
        border: '1px solid var(--line)',
        borderRadius: 5,
        padding: '14px 16px',
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: '0.12em',
          color: 'var(--t3)',
          textTransform: 'uppercase',
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 18,
          fontWeight: 700,
          color: color || 'var(--t0)',
          fontFamily: 'var(--font-space-mono), Space Mono, monospace',
        }}
      >
        {value}
      </div>
      {subtitle && (
        <div style={{ fontSize: 9, color: 'var(--t3)', marginTop: 4, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
          {subtitle}
        </div>
      )}
    </div>
  );
}

interface ModalProps {
  jobId: string;
  date: string;
  /** Currently-selected filter label (e.g. "A - Tail Guardrail"). When
   *  passed, the modal shows the audit-canonical daily return for that
   *  filter as the headline KPI, matching the equity curve. */
  filter?: string | null;
  onClose: () => void;
}

export default function BasketDetailModal({ jobId, date, filter, onClose }: ModalProps) {
  const [data, setData] = useState<BasketDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [view, setView] = useState<'binance' | 'blofin'>('binance');

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    setData(null);
    const apiBase = process.env.NEXT_PUBLIC_API_BASE || '';
    const qs = filter ? `?filter=${encodeURIComponent(filter)}` : '';
    fetch(`${apiBase}/api/jobs/${jobId}/baskets/${date}${qs}`)
      .then((r) => (r.ok ? r.json() : r.text().then((t) => Promise.reject(`HTTP ${r.status}: ${t}`))))
      .then((d) => { if (!cancelled) setData(d as BasketDetail); })
      .catch((e) => { if (!cancelled) setError(String(e)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [jobId, date, filter]);

  // Esc to close
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const visibleSymbols = useMemo(() => {
    if (!data) return [] as string[];
    return view === 'blofin' ? data.blofin_basket : data.binance_basket;
  }, [data, view]);

  const symColor = useMemo(() => {
    const map: Record<string, string> = {};
    if (data) {
      // Stable colour assignment based on the binance basket's alpha order
      const ordered = [...data.binance_basket].sort();
      ordered.forEach((sym, i) => { map[sym] = PALETTE[i % PALETTE.length]; });
    }
    return map;
  }, [data]);

  // Chart data: per-bar dict with one column per visible symbol + portfolio
  const chartRows = useMemo(() => {
    if (!data) return [];
    return data.bars.map((bar) => {
      const row: Record<string, number | string | null> = {
        ts: bar.ts,
        portfolio: view === 'blofin' ? bar.portfolio_blofin : bar.portfolio_binance,
      };
      for (const sym of visibleSymbols) {
        row[sym] = bar.sym_returns[sym] ?? null;
      }
      return row;
    });
  }, [data, view, visibleSymbols]);

  // Matrix sampling
  const matrixBars = useMemo(() => {
    if (!data) return [] as Bar[];
    return data.bars.filter((_, i) => i % MATRIX_SAMPLE_EVERY === 0);
  }, [data]);

  const finalRowReturns = useMemo(() => {
    if (!data || data.bars.length === 0) return {} as Record<string, number | null>;
    return data.bars[data.bars.length - 1].sym_returns;
  }, [data]);

  // KPIs — react to the Binance/BloFin toggle. Computes leveraged
  // portfolio metrics from the per-bar series the backend already
  // produced for the active view, plus best/worst-symbol from the
  // session-end per-symbol unleveraged returns.
  const kpis = useMemo(() => {
    if (!data || data.bars.length === 0) {
      return {
        finalRoi: 0,
        peakRoi: 0,
        peakBarTs: '',
        maxDD: 0,
        maxDDBarTs: '',
        symbolsTotal: 0,
        symbolsStopped: 0,
        bestSym: null as { base: string; ret: number } | null,
        worstSym: null as { base: string; ret: number } | null,
      };
    }
    const portKey: 'portfolio_binance' | 'portfolio_blofin' =
      view === 'blofin' ? 'portfolio_blofin' : 'portfolio_binance';
    let runningPeak = 0;
    let peakBarTs = data.bars[0].ts;
    let maxDD = 0;
    let maxDDBarTs = data.bars[0].ts;
    for (const bar of data.bars) {
      const v = bar[portKey];
      if (v > runningPeak) {
        runningPeak = v;
        peakBarTs = bar.ts;
      }
      const dd = v - runningPeak;
      if (dd < maxDD) {
        maxDD = dd;
        maxDDBarTs = bar.ts;
      }
    }
    const lastBar = data.bars[data.bars.length - 1];
    const finalRoi = lastBar[portKey];

    // Best / worst per-symbol final ROI (unleveraged %)
    let bestSym: { base: string; ret: number } | null = null;
    let worstSym: { base: string; ret: number } | null = null;
    let stoppedCount = 0;
    const stopFloor = data.stop_raw_pct + 0.001; // tolerance for float compare
    for (const sym of visibleSymbols) {
      const v = finalRowReturns[sym];
      if (v == null || !Number.isFinite(v)) continue;
      if (v <= stopFloor) stoppedCount += 1;
      if (bestSym == null || v > bestSym.ret) bestSym = { base: sym, ret: v };
      if (worstSym == null || v < worstSym.ret) worstSym = { base: sym, ret: v };
    }

    return {
      finalRoi,
      peakRoi: runningPeak,
      peakBarTs,
      maxDD,
      maxDDBarTs,
      symbolsTotal: visibleSymbols.length,
      symbolsStopped: stoppedCount,
      bestSym,
      worstSym,
    };
  }, [data, view, visibleSymbols, finalRowReturns]);

  // Audit-canonical daily return (from metrics.fees_tables_by_filter).
  // When present, this is the source of truth — same number the equity
  // curve, monthly heatmap, and fees panel all read. The basket-aggregate
  // KPI is shown as a secondary "if no exits had fired" reference.
  const auditCanonical = useMemo(() => {
    if (!data) return null;
    const r = data.audit_daily_return_pct;
    if (typeof r !== 'number' || !Number.isFinite(r)) return null;
    return {
      retNetPct: r,
      noEntry: data.audit_no_entry === true,
      noEntryReason: data.audit_no_entry_reason || null,
      filterLabel: data.audit_filter_label || null,
    };
  }, [data]);

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <div
      onClick={onClose}
      role="dialog"
      aria-label={`Basket detail for ${date}`}
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(8, 9, 13, 0.92)',
        zIndex: 100,
        display: 'flex',
        alignItems: 'flex-start',
        justifyContent: 'center',
        padding: 24,
        cursor: 'zoom-out',
        overflow: 'auto',
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: 'min(95vw, 1500px)',
          maxHeight: '92vh',
          background: 'var(--bg1)',
          border: '1px solid var(--line2)',
          borderRadius: 4,
          padding: 16,
          cursor: 'default',
          display: 'flex',
          flexDirection: 'column',
          gap: 12,
          fontFamily: 'var(--font-space-mono), Space Mono, monospace',
          color: 'var(--t1)',
          overflow: 'auto',
        }}
      >
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
          <div>
            <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700 }}>
              Basket Detail · {date}
            </div>
            {data && (
              <div style={{ fontSize: 10, color: 'var(--t2)', marginTop: 2 }}>
                Binance: <span style={{ color: 'var(--t1)' }}>{data.binance_basket.length} symbols</span>
                {' · '}
                BloFin: <span style={{ color: 'var(--t1)' }}>{data.blofin_basket.length} symbols</span>
                {data.non_blofin_dropped.length > 0 && (
                  <>
                    {' · '}
                    Dropped on BloFin:{' '}
                    <span style={{ color: 'rgba(70, 130, 180, 0.9)' }}>{data.non_blofin_dropped.join(', ')}</span>
                  </>
                )}
                {' · '}
                Leverage: <span style={{ color: 'var(--t1)' }}>{data.leverage}×</span>
                {' · '}
                Stop: <span style={{ color: 'var(--t1)' }}>{data.stop_raw_pct.toFixed(1)}% (1x)</span>
              </div>
            )}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {/* View toggle */}
            <div style={{ display: 'inline-flex', border: '1px solid var(--line2)', borderRadius: 3, overflow: 'hidden' }}>
              {(['binance', 'blofin'] as const).map((v) => (
                <button
                  key={v}
                  onClick={() => setView(v)}
                  style={{
                    background: view === v ? 'var(--green-dim)' : 'var(--bg2)',
                    border: 'none',
                    color: view === v ? 'var(--green)' : 'var(--t2)',
                    fontFamily: 'inherit',
                    fontSize: 10,
                    padding: '4px 10px',
                    cursor: 'pointer',
                    letterSpacing: '0.08em',
                    textTransform: 'uppercase',
                  }}
                >
                  {v === 'binance' ? 'Binance' : 'BloFin'}
                </button>
              ))}
            </div>
            <button
              onClick={onClose}
              aria-label="Close"
              style={{
                background: 'transparent',
                border: '1px solid var(--line2)',
                borderRadius: 3,
                color: 'var(--t2)',
                fontFamily: 'inherit',
                fontSize: 10,
                padding: '4px 10px',
                cursor: 'pointer',
              }}
            >
              ESC
            </button>
          </div>
        </div>

        {/* Loading / error */}
        {loading && <div style={{ fontSize: 11, color: 'var(--t3)', padding: 24 }}>Loading basket detail…</div>}
        {error && <div style={{ fontSize: 11, color: 'var(--red)', padding: 12 }}>Error: {error}</div>}

        {data && !loading && (
          <>
            {/* KPI strip — mirrors manager portfolio-detail page layout.
                When audit-canonical data is available, Portfolio ROI uses
                the audit's reported daily return (matches equity curve /
                heatmap / fees panel). Otherwise falls back to the naive
                basket aggregate. */}
            <div style={{ display: 'flex', gap: 10 }}>
              {(() => {
                const useCanonical = auditCanonical !== null;
                const headlineRoi = useCanonical ? auditCanonical!.retNetPct : kpis.finalRoi;
                const portColor = headlineRoi >= 0 ? 'var(--green)' : 'var(--red)';
                const oneXFinal = data.leverage > 0 ? headlineRoi / data.leverage : headlineRoi;
                const portSubtitle = useCanonical
                  ? (auditCanonical!.noEntry
                      ? `SIT-FLAT (${auditCanonical!.noEntryReason || 'no entry'})`
                      : `audit · ${oneXFinal >= 0 ? '+' : ''}${oneXFinal.toFixed(2)}% (${data.leverage.toFixed(0)}x 1x)`)
                  : (data.leverage > 0
                      ? `${oneXFinal >= 0 ? '+' : ''}${oneXFinal.toFixed(2)}% (${data.leverage.toFixed(0)}x 1x)`
                      : undefined);
                const peakSubtitle = (() => {
                  // bar timestamp like "2025-07-29T14:30:00" — extract HH:MM
                  const t = (kpis.peakBarTs || '').replace(/Z$/, '').split('T')[1] || '';
                  return t ? `at ${t.slice(0, 5)} UTC` : undefined;
                })();
                const ddSubtitle = (() => {
                  if (kpis.maxDD === 0) return undefined;
                  const recovery = kpis.finalRoi - kpis.maxDD;
                  return `${recovery >= 0 ? '+' : ''}${recovery.toFixed(2)}% since`;
                })();
                const stoppedSubtitle = kpis.symbolsStopped > 0
                  ? `${kpis.symbolsStopped} stopped`
                  : 'none stopped';
                return (
                  <>
                    <KpiCard
                      label="Portfolio ROI"
                      value={fmtPct(headlineRoi, 2)}
                      color={portColor}
                      subtitle={portSubtitle}
                    />
                    <KpiCard
                      label="Peak ROI"
                      value={fmtPct(kpis.peakRoi, 2)}
                      subtitle={peakSubtitle}
                    />
                    <KpiCard
                      label="Max Drawdown"
                      value={fmtPct(kpis.maxDD, 2)}
                      color={kpis.maxDD <= -2 ? 'var(--red)' : undefined}
                      subtitle={ddSubtitle}
                    />
                    <KpiCard
                      label="Symbols"
                      value={`${kpis.symbolsTotal - kpis.symbolsStopped} / ${kpis.symbolsTotal}`}
                      subtitle={stoppedSubtitle}
                    />
                    <KpiCard
                      label="Eff Leverage"
                      value={`${data.leverage.toFixed(2)}x`}
                      subtitle="deployed: 100%"
                    />
                    <KpiCard
                      label="Best Symbol"
                      value={kpis.bestSym ? kpis.bestSym.base : '—'}
                      color={kpis.bestSym && kpis.bestSym.ret >= 0 ? 'var(--green)' : 'var(--red)'}
                      subtitle={kpis.bestSym ? fmtPct(kpis.bestSym.ret, 2) : undefined}
                    />
                  </>
                );
              })()}
            </div>

            {/* Chart */}
            <div style={{ height: 360, background: 'var(--bg0)', border: '1px solid var(--line)', borderRadius: 3, padding: 8 }}>
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={chartRows} margin={{ top: 6, right: 12, bottom: 12, left: 8 }}>
                  <CartesianGrid stroke="var(--line)" strokeOpacity={0.2} vertical={false} />
                  <XAxis
                    dataKey="ts"
                    type="category"
                    tickFormatter={fmtTime}
                    tick={{ fill: 'var(--t2)', fontSize: 9, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}
                    axisLine={{ stroke: 'var(--line)', strokeOpacity: 0.35 }}
                    interval={Math.max(1, Math.floor(chartRows.length / 12))}
                  />
                  <YAxis
                    tickFormatter={(v) => `${(v as number).toFixed(0)}%`}
                    tick={{ fill: 'var(--t2)', fontSize: 9, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}
                    axisLine={{ stroke: 'var(--line)', strokeOpacity: 0.35 }}
                    width={50}
                  />
                  <ReferenceLine y={0} stroke="rgba(255,255,255,0.18)" strokeDasharray="3 3" />
                  <ReferenceLine
                    y={data.stop_raw_pct}
                    stroke="var(--red-dim)"
                    strokeDasharray="3 3"
                    label={{ value: `Stop ${data.stop_raw_pct.toFixed(1)}%`, fill: 'var(--red)', fontSize: 9, position: 'right' }}
                  />
                  <Tooltip
                    cursor={{ stroke: 'var(--line2)', strokeDasharray: '2 2' }}
                    content={({ active, payload, label }) => {
                      if (!active || !payload || payload.length === 0) return null;
                      const sortedPayload = [...payload].sort((a, b) => {
                        const av = (a.value as number) ?? 0;
                        const bv = (b.value as number) ?? 0;
                        return bv - av;
                      });
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
                            maxHeight: 360,
                            overflowY: 'auto',
                          }}
                        >
                          <div style={{ color: 'var(--t2)', marginBottom: 4 }}>{label ? fmtTime(String(label)) : ''} UTC</div>
                          {sortedPayload.map((entry) => {
                            const k = String(entry.dataKey);
                            const v = entry.value as number | null | undefined;
                            const isPort = k === 'portfolio';
                            return (
                              <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 6, fontWeight: isPort ? 700 : 400 }}>
                                <span style={{ display: 'inline-block', width: 8, height: 2, background: entry.color }} />
                                <span style={{ color: isPort ? 'var(--green)' : 'var(--t1)', minWidth: 90 }}>
                                  {isPort ? `Portfolio (${view === 'blofin' ? 'BloFin' : 'Binance'})` : k}
                                </span>
                                <span style={{ color: pctColor(v), fontWeight: 700 }}>{fmtPct(v ?? null, 2)}</span>
                              </div>
                            );
                          })}
                        </div>
                      );
                    }}
                  />
                  {/* Per-symbol thin lines */}
                  {visibleSymbols.map((sym) => (
                    <Line
                      key={`sym-${sym}`}
                      type="monotone"
                      dataKey={sym}
                      stroke={symColor[sym]}
                      strokeWidth={1}
                      dot={false}
                      isAnimationActive={false}
                      connectNulls
                    />
                  ))}
                  {/* Portfolio aggregate — bold, on top */}
                  <Line
                    type="monotone"
                    dataKey="portfolio"
                    stroke="#ffffff"
                    strokeWidth={2.5}
                    dot={false}
                    isAnimationActive={false}
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            {auditCanonical && (
              <div style={{ fontSize: 9, color: 'var(--t3)', textAlign: 'right', marginTop: -6, fontStyle: 'italic' }}>
                Chart's white line is the intraday basket aggregate (no portfolio-level exits applied).
                Audit's reported daily return —
                <span style={{ color: pctColor(auditCanonical.retNetPct), fontWeight: 700, marginLeft: 4 }}>
                  {fmtPct(auditCanonical.retNetPct, 2)}
                </span>
                {auditCanonical.filterLabel ? ` · ${auditCanonical.filterLabel}` : ''}
                {' '}— is shown above (matches equity curve / heatmap / fees panel).
              </div>
            )}

            {/* Symbol summary row */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, fontSize: 10 }}>
              {data.symbols
                .filter((s) => view === 'blofin' ? s.in_blofin_basket : s.in_binance_basket)
                .map((s) => {
                  const finalRet = finalRowReturns[s.base];
                  const dropped = !s.blofin_listed_at_date && view === 'binance';
                  return (
                    <div
                      key={s.base}
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 5,
                        padding: '2px 6px',
                        background: 'var(--bg2)',
                        border: '1px solid var(--line)',
                        borderRadius: 2,
                      }}
                    >
                      <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: 2, background: symColor[s.base] }} />
                      <span style={{ color: 'var(--t1)' }}>{s.base}</span>
                      <span style={{ color: pctColor(finalRet), fontWeight: 700 }}>{fmtPct(finalRet, 2)}</span>
                      {dropped && (
                        <span title={s.list_date ? `Listed on BloFin ${s.list_date}` : 'Not on BloFin'} style={{ color: 'rgba(70,130,180,0.85)', fontSize: 8 }}>
                          NOT-ON-BLOFIN
                        </span>
                      )}
                      {!dropped && view === 'binance' && (
                        <span title={`Listed on BloFin ${s.list_date ?? '?'}`} style={{ color: 'var(--t3)', fontSize: 8 }}>
                          ✓
                        </span>
                      )}
                    </div>
                  );
                })}
            </div>

            {/* ROI matrix — 30-min sampled */}
            <div style={{ overflow: 'auto', border: '1px solid var(--line)', borderRadius: 3, maxHeight: '50vh' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 9, fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
                <thead style={{ position: 'sticky', top: 0, background: 'var(--bg2)', zIndex: 1 }}>
                  <tr>
                    <th style={{ textAlign: 'left', padding: '6px 8px', color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700, borderBottom: '1px solid var(--line2)' }}>
                      Time
                    </th>
                    {visibleSymbols.map((sym) => (
                      <th key={sym} style={{ textAlign: 'right', padding: '6px 6px', color: symColor[sym], textTransform: 'uppercase', letterSpacing: '0.06em', fontWeight: 700, borderBottom: '1px solid var(--line2)' }}>
                        {sym}
                      </th>
                    ))}
                    <th style={{ textAlign: 'right', padding: '6px 8px', color: 'var(--green)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700, borderBottom: '1px solid var(--line2)', borderLeft: '1px solid var(--line2)' }}>
                      PORT
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {matrixBars.map((bar, i) => (
                    <tr key={`${bar.ts}-${i}`} style={{ background: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)' }}>
                      <td style={{ padding: '4px 8px', color: 'var(--t2)', borderBottom: '1px solid var(--line)' }}>{fmtTime(bar.ts)}</td>
                      {visibleSymbols.map((sym) => {
                        const v = bar.sym_returns[sym];
                        return (
                          <td key={sym} style={{ padding: '4px 6px', textAlign: 'right', color: pctColor(v), borderBottom: '1px solid var(--line)' }}>
                            {fmtPct(v, 2)}
                          </td>
                        );
                      })}
                      <td style={{ padding: '4px 8px', textAlign: 'right', color: pctColor(view === 'blofin' ? bar.portfolio_blofin : bar.portfolio_binance), fontWeight: 700, borderBottom: '1px solid var(--line)', borderLeft: '1px solid var(--line2)' }}>
                        {fmtPct(view === 'blofin' ? bar.portfolio_blofin : bar.portfolio_binance, 2)}
                      </td>
                    </tr>
                  ))}
                </tbody>
                <tfoot style={{ background: 'var(--bg2)' }}>
                  {(() => {
                    const last = data.bars[data.bars.length - 1];
                    if (!last) return null;
                    const portFinal = view === 'blofin' ? last.portfolio_blofin : last.portfolio_binance;
                    return (
                      <tr>
                        <td style={{ padding: '6px 8px', color: 'var(--t2)', textTransform: 'uppercase', letterSpacing: '0.08em', fontSize: 9, fontWeight: 700, borderTop: '1px solid var(--line2)' }}>
                          End of session
                        </td>
                        {visibleSymbols.map((sym) => {
                          const v = last.sym_returns[sym];
                          return (
                            <td key={sym} style={{ padding: '6px 6px', textAlign: 'right', color: pctColor(v), fontWeight: 700, borderTop: '1px solid var(--line2)' }}>
                              {fmtPct(v, 2)}
                            </td>
                          );
                        })}
                        <td style={{ padding: '6px 8px', textAlign: 'right', color: pctColor(portFinal), fontWeight: 700, borderTop: '1px solid var(--line2)', borderLeft: '1px solid var(--line2)' }}>
                          {fmtPct(portFinal, 2)}
                        </td>
                      </tr>
                    );
                  })()}
                </tfoot>
              </table>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

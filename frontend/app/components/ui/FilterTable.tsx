'use client';

import { asNum, fmt, fmtPercent2, fmtInt, metricColor, normalizeFilterLabel } from '@/app/lib/format';

interface FilterRow {
  filter?: string;
  sharpe?: number | null;
  max_dd?: number | null;
  active?: number | null;
  cagr?: number | null;
  cv?: number | null;
  dsr_pct?: number | null;
  tot_ret?: number | null;
  eq?: number | null;
  grade?: string | null;
  not_run?: boolean;
  [key: string]: unknown;
}

interface FilterTableProps {
  rows: FilterRow[] | null | undefined;
  selectedFilter?: string | null;
  onSelectFilter?: (filter: string) => void;
}

export default function FilterTable({ rows, selectedFilter, onSelectFilter }: FilterTableProps) {
  if (!rows || rows.length === 0) {
    return (
      <div style={{ fontSize: 9, color: 'var(--t3)', padding: '12px 0' }}>
        No filter comparison data available.
      </div>
    );
  }

  const sorted = [...rows].sort((a, b) => (b.sharpe ?? -Infinity) - (a.sharpe ?? -Infinity));
  const maxSharpe = Math.max(...sorted.map((r) => r.sharpe ?? 0));
  const bestIdx = 0;
  const selectedNorm = normalizeFilterLabel(selectedFilter ?? '');

  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 9 }}>
      <thead>
        <tr style={{ borderBottom: '1px solid var(--line)' }}>
          {['Filter', 'Sharpe', 'Max DD', 'Active Days', 'Simple%', 'Comp%', 'Comp Mult', 'CAGR', 'WF-CV', 'DSR%', 'Grade'].map((h) => (
            <th
              key={h}
              style={{
                color: 'var(--t3)',
                textTransform: 'uppercase',
                letterSpacing: '0.08em',
                padding: '4px 4px',
                textAlign: h === 'Filter' ? 'left' : 'right',
                fontWeight: 700,
              }}
            >
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {sorted.map((row, i) => {
          const isBest = i === bestIdx;
          const isMissing = !!row.not_run;
          const rowFilter = String(row.filter ?? '—');
          const isSelected = selectedNorm !== '' && selectedNorm === normalizeFilterLabel(rowFilter);
          const rowOpacity = selectedNorm !== '' && !isSelected ? 0.66 : 1;
          return (
            <tr
              key={rowFilter || i}
              onClick={() => {
                if (!isMissing) onSelectFilter?.(rowFilter);
              }}
              style={{
                borderBottom: '1px solid var(--line)',
                background: isSelected
                  ? 'rgba(255, 255, 255, 0.08)'
                  : isBest
                    ? 'var(--green-dim)'
                    : isMissing
                      ? 'rgba(255,255,255,0.03)'
                      : 'transparent',
                cursor: isMissing ? 'default' : 'pointer',
                opacity: rowOpacity,
              }}
            >
              <td
                style={{
                  padding: '6px 4px',
                  color: isSelected ? '#e8e8e8' : isBest ? 'var(--green)' : isMissing ? 'var(--t3)' : 'var(--t1)',
                  whiteSpace: 'nowrap',
                }}
              >
                {row.filter ?? '—'}{isMissing ? ' (not run)' : ''}{' '}
                {isBest && (
                  <span
                    style={{
                      fontSize: 8,
                      letterSpacing: '0.08em',
                      border: '1px solid var(--green-mid)',
                      color: 'var(--green)',
                      borderRadius: 2,
                      padding: '1px 4px',
                    }}
                  >
                    BEST
                  </span>
                )}
              </td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: metricColor('sharpe', row.sharpe) }}>
                <div>{fmt(row.sharpe)}</div>
                {row.sharpe != null && (
                  <div
                    style={{
                      marginTop: 2,
                      height: 2,
                      width: 40,
                      background: 'var(--line)',
                      marginLeft: 'auto',
                    }}
                  >
                    <div
                      style={{
                        height: '100%',
                        width: maxSharpe > 0 ? `${Math.max(0, (row.sharpe ?? 0) / maxSharpe) * 40}px` : '0',
                        background: (row.sharpe ?? 0) > 1 ? 'var(--green)' : 'var(--amber)',
                      }}
                    />
                  </div>
                )}
              </td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: metricColor('max_dd', row.max_dd) }}>{fmtPercent2(row.max_dd)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--t1)' }}>{fmtInt(row.active)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: (row.tot_ret ?? 0) >= 0 ? 'var(--green)' : 'var(--red)' }}>
                {row.tot_ret != null ? `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(row.tot_ret as number)}%` : 'N/A'}
              </td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--green)' }}>
                {(() => {
                  const ec = row.equity_curve as Array<number | { y: number }> | undefined;
                  if (!ec || ec.length === 0) return 'N/A';
                  const last = ec[ec.length - 1];
                  const val = typeof last === 'number' ? last : (last as { y: number })?.y;
                  if (val == null || !Number.isFinite(val)) return 'N/A';
                  return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format((val - 1) * 100)}%`;
                })()}
              </td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--t1)' }}>
                {(() => {
                  const ec = row.equity_curve as Array<number | { y: number }> | undefined;
                  const simple = row.tot_ret as number | null | undefined;
                  if (!ec || ec.length === 0 || !simple || simple <= 0) return 'N/A';
                  const last = ec[ec.length - 1];
                  const val = typeof last === 'number' ? last : (last as { y: number })?.y;
                  if (val == null || !Number.isFinite(val)) return 'N/A';
                  const compRet = (val - 1) * 100;
                  return `${(compRet / simple).toFixed(2)}×`;
                })()}
              </td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: metricColor('cagr', row.cagr) }}>{fmtPercent2(row.cagr)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: metricColor('cv', (row.wf_cv ?? row.cv) as number) }}>{fmt((row.wf_cv ?? row.cv) as number)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: metricColor('dsr_pct', row.dsr_pct as number) }}>{fmt(row.dsr_pct as number, 1)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: metricColor('grade', row.grade_score ?? row.grade) }}>
                {String(row.grade_score ?? row.grade ?? 'N/A')}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

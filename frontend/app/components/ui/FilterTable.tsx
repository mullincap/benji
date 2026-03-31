'use client';

interface FilterRow {
  filter: string;
  sharpe?: number | null;
  max_dd?: number | null;
  cagr?: number | null;
  cv?: number | null;
  dsr_pct?: number | null;
  grade?: string | null;
  [key: string]: unknown;
}

interface FilterTableProps {
  rows: FilterRow[] | null | undefined;
}

function fmt(v: number | null | undefined, decimals = 3): string {
  if (v === null || v === undefined) return 'N/A';
  return v.toFixed(decimals);
}

export default function FilterTable({ rows }: FilterTableProps) {
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

  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 9 }}>
      <thead>
        <tr style={{ borderBottom: '1px solid var(--line)' }}>
          {['Filter', 'Sharpe', 'Max DD', 'CAGR', 'WF-CV', 'DSR%', 'Grade'].map((h) => (
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
          return (
            <tr
              key={row.filter ?? i}
              style={{
                borderBottom: '1px solid var(--line)',
                background: isBest ? 'var(--green-dim)' : 'transparent',
              }}
            >
              <td
                style={{
                  padding: '6px 4px',
                  color: isBest ? 'var(--green)' : 'var(--t1)',
                  whiteSpace: 'nowrap',
                }}
              >
                {row.filter ?? '—'}{isBest ? ' ★' : ''}
              </td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: isBest ? 'var(--green)' : 'var(--t0)' }}>
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
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--t0)' }}>{fmt(row.max_dd)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--t0)' }}>{fmt(row.cagr)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--t0)' }}>{fmt((row.wf_cv ?? row.cv) as number)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--t0)' }}>{fmt(row.dsr_pct as number, 1)}</td>
              <td style={{ padding: '6px 4px', textAlign: 'right', color: 'var(--t1)' }}>{row.grade_score ?? row.grade ?? 'N/A'}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

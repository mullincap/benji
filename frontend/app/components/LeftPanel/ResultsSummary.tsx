'use client';

interface ResultsSummaryProps {
  results: Record<string, unknown> | null;
  params: Record<string, unknown>;
  onRerun: () => void;
}

function fmtVal(v: unknown, decimals = 2): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return v.toFixed(decimals);
  return String(v);
}

function kpiColor(key: string, value: unknown): string {
  if (typeof value !== 'number') return 'var(--t0)';
  if (key === 'sharpe' || key === 'sortino') {
    if (value >= 1.5) return 'var(--green)';
    if (value >= 0.8) return 'var(--amber)';
    return 'var(--red)';
  }
  if (key === 'max_dd') {
    if (value > -0.15) return 'var(--green)';
    if (value > -0.3) return 'var(--amber)';
    return 'var(--red)';
  }
  if (key === 'cagr') {
    if (value > 0.3) return 'var(--green)';
    if (value > 0) return 'var(--amber)';
    return 'var(--red)';
  }
  return value >= 0 ? 'var(--green)' : 'var(--red)';
}

const KEY_PARAMS: string[] = [
  'leaderboard_index',
  'sort_by',
  'mode',
  'leverage',
  'stop_raw_pct',
  'starting_capital',
];

export default function ResultsSummary({ results, params, onRerun }: ResultsSummaryProps) {
  const metrics = (results?.metrics ?? {}) as Record<string, unknown>;
  const canonical = (metrics?.best_filter ?? results?.canonical_filter ?? '') as string;
  const grade = (metrics?.grade ?? 'N/A') as string;
  const scorecard = (results?.scorecard ?? []) as Array<{ label: string; status: string; value?: unknown }>;

  const sharpe = metrics?.sharpe as number | undefined;
  const maxDd = metrics?.max_drawdown as number | undefined;
  const cagr = metrics?.cagr as number | undefined;

  return (
    <div style={{ padding: 12, paddingBottom: 60 }}>
      {/* Grade Hero Block */}
      <div
        style={{
          background: 'var(--bg0)',
          border: '1px solid var(--green)',
          borderRadius: 3,
          padding: 12,
          marginBottom: 12,
        }}
      >
        {/* Grade circle + filter name */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', marginBottom: 10 }}>
          <div
            style={{
              width: 48,
              height: 48,
              borderRadius: '50%',
              border: '2px solid var(--green)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              marginBottom: 6,
            }}
          >
            <span style={{ fontSize: 20, fontWeight: 700, color: 'var(--green)' }}>{grade}</span>
          </div>
          <span style={{ fontSize: 10, color: 'var(--t1)', marginBottom: 4 }}>{canonical || '—'}</span>
          <span
            style={{
              fontSize: 7,
              color: 'var(--green)',
              border: '1px solid var(--green-mid)',
              borderRadius: 2,
              padding: '1px 4px',
              letterSpacing: '0.08em',
            }}
          >
            CANONICAL FILTER
          </span>
        </div>

        {/* KPI trio */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 6 }}>
          {[
            { label: 'Sharpe', key: 'sharpe', value: sharpe },
            { label: 'Max DD', key: 'max_dd', value: maxDd },
            { label: 'CAGR', key: 'cagr', value: cagr },
          ].map(({ label, key, value }) => (
            <div key={key} style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 9, color: 'var(--t3)', letterSpacing: '0.08em', marginBottom: 2 }}>
                {label}
              </div>
              <div style={{ fontSize: 15, fontWeight: 700, color: kpiColor(key, value) }}>
                {fmtVal(value)}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Parameters */}
      <div style={{ marginBottom: 12 }}>
        <div
          style={{
            fontSize: 9,
            textTransform: 'uppercase',
            letterSpacing: '0.12em',
            color: 'var(--t3)',
            fontWeight: 700,
            marginBottom: 6,
          }}
        >
          PARAMETERS
        </div>
        {KEY_PARAMS.map((k) => (
          <div
            key={k}
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              marginBottom: 3,
            }}
          >
            <span style={{ fontSize: 10, color: 'var(--t2)' }}>{k}</span>
            <span style={{ fontSize: 10, color: 'var(--t1)', fontFamily: 'Space Mono, monospace' }}>
              {params[k] !== undefined ? String(params[k]) : '—'}
            </span>
          </div>
        ))}
      </div>

      {/* Scorecard */}
      {scorecard.length > 0 && (
        <div style={{ marginBottom: 12 }}>
          <div
            style={{
              fontSize: 9,
              textTransform: 'uppercase',
              letterSpacing: '0.12em',
              color: 'var(--t3)',
              fontWeight: 700,
              marginBottom: 6,
            }}
          >
            SCORECARD
          </div>
          {scorecard.map((item, i) => {
            const isPass = item.status === 'pass';
            const isWarn = item.status === 'warn';
            const icon = isPass ? '✓' : isWarn ? '⚠' : '✗';
            const color = isPass ? 'var(--green)' : isWarn ? 'var(--amber)' : 'var(--red)';
            return (
              <div
                key={i}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  marginBottom: 4,
                }}
              >
                <span style={{ fontSize: 10, color, fontWeight: 700 }}>{icon}</span>
                <span style={{ fontSize: 10, color: 'var(--t1)' }}>{item.label}</span>
                {item.value !== undefined && (
                  <span style={{ fontSize: 10, color: 'var(--t2)', marginLeft: 'auto' }}>
                    {fmtVal(item.value)}
                  </span>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* Re-run button */}
      <button
        onClick={onRerun}
        style={{
          width: '100%',
          height: 32,
          border: '1px solid var(--line2)',
          background: 'transparent',
          color: 'var(--t1)',
          fontFamily: 'Space Mono, monospace',
          fontSize: 10,
          borderRadius: 3,
          cursor: 'pointer',
          letterSpacing: '0.06em',
        }}
        onMouseEnter={(e) => ((e.target as HTMLButtonElement).style.background = 'var(--bg4)')}
        onMouseLeave={(e) => ((e.target as HTMLButtonElement).style.background = 'transparent')}
      >
        EDIT &amp; RE-RUN
      </button>
    </div>
  );
}

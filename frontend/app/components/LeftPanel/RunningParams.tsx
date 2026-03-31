'use client';

interface RunningParamsProps {
  params: Record<string, unknown>;
}

const KEY_PARAMS: { key: string; label: string }[] = [
  { key: 'leaderboard_index', label: 'leaderboard_index' },
  { key: 'sort_by', label: 'sort_by' },
  { key: 'mode', label: 'mode' },
  { key: 'leverage', label: 'leverage' },
  { key: 'stop_raw_pct', label: 'stop_raw_pct' },
  { key: 'starting_capital', label: 'starting_capital' },
];

function getEnabledFilters(params: Record<string, unknown>): string[] {
  const filters: string[] = [];
  const filterMap: Record<string, string> = {
    enable_tail_guardrail: 'Tail Guardrail',
    enable_dispersion_filter: 'Dispersion',
    enable_tail_plus_disp: 'Tail+Disp',
    enable_vol_filter: 'Vol',
    enable_tail_disp_vol: 'Tail+Disp+Vol',
    enable_tail_or_vol: 'Tail OR Vol',
    enable_tail_and_vol: 'Tail AND Vol',
    enable_blofin_filter: 'BloFin',
    enable_btc_ma_filter: 'BTC MA',
    enable_ic_filter: 'IC Filter',
  };
  for (const [k, label] of Object.entries(filterMap)) {
    if (params[k]) filters.push(label);
  }
  return filters;
}

export default function RunningParams({ params }: RunningParamsProps) {
  const enabledFilters = getEnabledFilters(params);

  return (
    <div style={{ padding: 12 }}>
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
        PARAMETERS
      </div>
      {KEY_PARAMS.map(({ key, label }) => (
        <div
          key={key}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: 4,
          }}
        >
          <span style={{ fontSize: 10, color: 'var(--t2)' }}>{label}</span>
          <span style={{ fontSize: 10, color: 'var(--t1)', fontFamily: 'Space Mono, monospace' }}>
            {params[key] !== undefined && params[key] !== null ? String(params[key]) : '—'}
          </span>
        </div>
      ))}
      {enabledFilters.length > 0 && (
        <>
          <div
            style={{
              fontSize: 9,
              textTransform: 'uppercase',
              letterSpacing: '0.12em',
              color: 'var(--t3)',
              fontWeight: 700,
              marginBottom: 4,
              marginTop: 10,
            }}
          >
            ACTIVE FILTERS
          </div>
          {enabledFilters.map((f) => (
            <div key={f} style={{ fontSize: 10, color: 'var(--t1)', marginBottom: 3 }}>
              · {f}
            </div>
          ))}
        </>
      )}
    </div>
  );
}

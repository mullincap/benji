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

function isActiveConfigValue(v: unknown): boolean {
  if (v === null || v === undefined) return false;
  if (typeof v === 'boolean') return v;
  if (typeof v === 'string') return v.trim().length > 0;
  return true;
}

function getInactiveChildKeys(params: Record<string, unknown>): Set<string> {
  const hidden = new Set<string>();
  const hide = (keys: string[]) => keys.forEach((k) => hidden.add(k));

  if (params.capital_mode !== 'fixed') {
    hide(['fixed_notional_cap']);
  }
  if (!params.enable_dispersion_filter) {
    hide(['dispersion_threshold', 'dispersion_baseline_win', 'dispersion_n', 'dispersion_dynamic_universe', 'run_filter_dispersion']);
  }
  if (!params.enable_vol_filter) {
    hide(['vol_lookback', 'vol_percentile', 'vol_baseline_win', 'run_filter_vol']);
  }
  if (!params.enable_tail_guardrail) {
    hide(['tail_drop_pct', 'tail_vol_mult', 'run_filter_tail']);
  }
  if (!params.enable_tail_plus_disp) {
    hide(['run_filter_tail_disp']);
  }
  if (!params.enable_tail_disp_vol) {
    hide(['run_filter_tail_disp_vol']);
  }
  if (!params.enable_tail_or_vol) {
    hide(['run_filter_tail_or_vol']);
  }
  if (!params.enable_tail_and_vol) {
    hide(['run_filter_tail_and_vol']);
  }
  if (!params.enable_blofin_filter) {
    hide(['blofin_min_symbols', 'run_filter_tail_blofin']);
  }
  if (!params.enable_btc_ma_filter) {
    hide(['btc_ma_days']);
  }
  if (!params.enable_ic_diagnostic && !params.enable_ic_filter) {
    hide(['ic_signal', 'ic_window', 'ic_threshold']);
  }
  if (!params.enable_perf_lev_scaling) {
    hide(['perf_lev_window', 'perf_lev_sortino_target', 'perf_lev_max_boost']);
  }
  if (!params.enable_vol_lev_scaling) {
    hide(['vol_lev_window', 'vol_lev_target_vol', 'vol_lev_max_boost', 'vol_lev_dd_threshold']);
  }
  if (!params.enable_contra_lev_scaling) {
    hide(['contra_lev_window', 'contra_lev_max_boost', 'contra_lev_dd_threshold']);
  }
  if (!params.enable_pph) {
    hide(['pph_frequency', 'pph_threshold', 'pph_harvest_frac', 'pph_sweep_enabled']);
  }
  if (!params.enable_ratchet) {
    hide(['ratchet_frequency', 'ratchet_trigger', 'ratchet_lock_pct', 'ratchet_risk_off_lev_scale', 'ratchet_sweep_enabled']);
  }
  if (!params.enable_adaptive_ratchet) {
    hide([
      'adaptive_ratchet_frequency',
      'adaptive_ratchet_vol_window',
      'adaptive_ratchet_vol_low',
      'adaptive_ratchet_vol_high',
      'adaptive_ratchet_risk_off_scale',
      'adaptive_ratchet_floor_decay',
      'adaptive_ratchet_sweep_enabled',
    ]);
  }

  return hidden;
}

export default function ResultsSummary({ results, params, onRerun }: ResultsSummaryProps) {
  const metrics = (results?.metrics ?? {}) as Record<string, unknown>;
  const canonical = (metrics?.best_filter ?? results?.canonical_filter ?? '') as string;
  const grade = (metrics?.grade ?? 'N/A') as string;
  const scorecard = (results?.scorecard ?? []) as Array<{ label: string; status: string; value?: unknown }>;

  const sharpe = metrics?.sharpe as number | undefined;
  const maxDd = metrics?.max_drawdown as number | undefined;
  const cagr = metrics?.cagr as number | undefined;

  const hiddenKeys = getInactiveChildKeys(params);
  const activeConfigs = Object.entries(params)
    .filter(([k, v]) => !hiddenKeys.has(k) && isActiveConfigValue(v))
    .sort(([a], [b]) => a.localeCompare(b));

  return (
    <div style={{ padding: 12, paddingBottom: 12 }}>
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
          ACTIVE CONFIGS
        </div>
        {activeConfigs.length === 0 && (
          <div style={{ fontSize: 10, color: 'var(--t2)' }}>No active configs.</div>
        )}
        {activeConfigs.map(([k, v]) => (
          <div
            key={k}
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              marginBottom: 3,
              gap: 8,
            }}
          >
            <span style={{ fontSize: 10, color: 'var(--t2)' }}>{k}</span>
            <span style={{ fontSize: 10, color: 'var(--t1)', fontFamily: 'Space Mono, monospace' }}>
              {String(v)}
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

      {/* Re-run button (sticky so it's always reachable) */}
      <div
        style={{
          position: 'sticky',
          bottom: 0,
          paddingTop: 8,
          background: 'linear-gradient(to bottom, transparent, var(--bg0) 35%)',
        }}
      >
        <button
          onClick={onRerun}
          style={{
            width: '100%',
            height: 32,
            border: '1px solid var(--line2)',
            background: 'var(--bg1)',
            color: 'var(--t1)',
            fontFamily: 'Space Mono, monospace',
            fontSize: 10,
            borderRadius: 3,
            cursor: 'pointer',
            letterSpacing: '0.06em',
          }}
          onMouseEnter={(e) => ((e.target as HTMLButtonElement).style.background = 'var(--bg4)')}
          onMouseLeave={(e) => ((e.target as HTMLButtonElement).style.background = 'var(--bg1)')}
        >
          EDIT &amp; RE-RUN
        </button>
      </div>
    </div>
  );
}

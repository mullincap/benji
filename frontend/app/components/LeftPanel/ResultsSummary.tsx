'use client';

import { useState } from 'react';

interface ResultsSummaryProps {
  results: Record<string, unknown> | null;
  params: Record<string, unknown>;
  onRerun: () => void;
  startAuditConfigsCollapsed?: boolean;
}

type FilterMetricRow = {
  filter?: string;
  sharpe?: number | null;
  max_dd?: number | null;
  cagr?: number | null;
  grade?: string | null;
  grade_score?: number | null;
  not_run?: boolean;
  [key: string]: unknown;
};

function fmtVal(v: unknown, decimals = 2): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return v.toFixed(decimals);
  return String(v);
}

function fmtPercent1(v: unknown): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return `${v.toFixed(1)}%`;
  const n = Number(v);
  if (Number.isFinite(n)) return `${n.toFixed(1)}%`;
  return String(v);
}

function parseDateLike(v: unknown): Date | null {
  if (v instanceof Date && Number.isFinite(v.getTime())) return v;
  if (typeof v === 'string') {
    const d = new Date(v);
    return Number.isFinite(d.getTime()) ? d : null;
  }
  if (typeof v === 'number') {
    const ms = v > 1e12 ? v : v > 1e9 ? v * 1000 : null;
    if (!ms) return null;
    const d = new Date(ms);
    return Number.isFinite(d.getTime()) ? d : null;
  }
  return null;
}

function avgMonthlyReturnPctFromCurve(curve: unknown): number | null {
  if (!Array.isArray(curve) || curve.length < 2) return null;
  const points = curve
    .map((p) => {
      if (!p || typeof p !== 'object') return null;
      const rec = p as { x?: unknown; y?: unknown };
      const d = parseDateLike(rec.x);
      const y = typeof rec.y === 'number' ? rec.y : Number(rec.y);
      if (!d || !Number.isFinite(y)) return null;
      return { d, y };
    })
    .filter((p): p is { d: Date; y: number } => p !== null);
  if (points.length < 2) return null;

  const buckets = new Map<string, { first: number; last: number }>();
  for (const p of points) {
    const key = `${p.d.getFullYear()}-${String(p.d.getMonth() + 1).padStart(2, '0')}`;
    const cur = buckets.get(key);
    if (!cur) {
      buckets.set(key, { first: p.y, last: p.y });
    } else {
      cur.last = p.y;
    }
  }
  const monthly = Array.from(buckets.values())
    .map((m) => (m.first !== 0 ? ((m.last / m.first) - 1) * 100 : NaN))
    .filter((v) => Number.isFinite(v));
  if (monthly.length === 0) return null;
  return monthly.reduce((a, b) => a + b, 0) / monthly.length;
}

function avgMonthlyFromCagrPct(cagrPct: unknown): number | null {
  const c = typeof cagrPct === 'number' ? cagrPct : Number(cagrPct);
  if (!Number.isFinite(c)) return null;
  // CAGR is stored/displayed in percent points (e.g., 842.96 means 842.96%).
  const annual = 1 + (c / 100);
  if (annual <= 0) return null;
  return (Math.pow(annual, 1 / 12) - 1) * 100;
}

function kpiColor(key: string, value: unknown): string {
  if (typeof value !== 'number' || !Number.isFinite(value)) return 'var(--t0)';
  if (key === 'sharpe' || key === 'sortino') {
    if (value >= 1.5) return 'var(--green)';
    if (value >= 0.8) return 'var(--amber)';
    return 'var(--red)';
  }
  if (key === 'max_dd') {
    return 'var(--red)';
  }
  if (key === 'avg_1m_ret') {
    if (value > 0) return 'var(--green)';
    if (value > -5) return 'var(--amber)';
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

function normalizeFilterLabel(s: string): string {
  return s
    .toLowerCase()
    .replace(/\+/g, 'p')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function asNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

type ConfigSection = { title: string; keys: string[] };

const CONFIG_SECTIONS: ConfigSection[] = [
  {
    title: 'STRATEGY',
    keys: ['leaderboard_index', 'sort_by', 'mode', 'freq_width', 'freq_cutoff', 'sample_interval'],
  },
  {
    title: 'DEPLOYMENT WINDOW',
    keys: ['deployment_start_hour', 'index_lookback', 'sort_lookback', 'deployment_runtime_hours'],
  },
  {
    title: 'UNIVERSE + RISK',
    keys: [
      'starting_capital', 'capital_mode', 'fixed_notional_cap', 'pivot_leverage', 'min_mcap', 'max_mcap',
      'min_listing_age', 'max_port', 'drop_unverified', 'leverage', 'stop_raw_pct', 'price_source',
      'save_charts', 'trial_purchases', 'quick', 'taker_fee_pct', 'funding_rate_daily_pct',
    ],
  },
  {
    title: 'EXECUTION CONFIG',
    keys: ['early_kill_x', 'early_kill_y', 'early_instill_y', 'l_base', 'l_high', 'port_tsl', 'port_sl', 'early_fill_y', 'early_fill_x'],
  },
  {
    title: 'FILTERS',
    keys: [
      'enable_tail_guardrail', 'run_filter_tail', 'enable_dispersion_filter', 'run_filter_dispersion',
      'enable_tail_plus_disp', 'run_filter_tail_disp', 'enable_vol_filter', 'run_filter_vol',
      'enable_tail_disp_vol', 'run_filter_tail_disp_vol', 'enable_tail_or_vol', 'run_filter_tail_or_vol',
      'enable_tail_and_vol', 'run_filter_tail_and_vol', 'enable_blofin_filter', 'run_filter_tail_blofin',
      'run_filter_none', 'enable_btc_ma_filter', 'enable_ic_diagnostic', 'enable_ic_filter', 'run_filter_calendar',
    ],
  },
  {
    title: 'ADVANCED • STRATEGY TUNING',
    keys: [
      'dispersion_threshold', 'dispersion_baseline_win', 'dispersion_n', 'dispersion_dynamic_universe',
      'vol_lookback', 'vol_percentile', 'vol_baseline_win', 'tail_drop_pct', 'tail_vol_mult',
      'ic_signal', 'ic_window', 'ic_threshold', 'btc_ma_days', 'blofin_min_symbols',
      'leaderboard_top_n', 'train_test_split', 'n_trials',
    ],
  },
  {
    title: 'ADVANCED • LEVERAGE SCALING',
    keys: [
      'enable_perf_lev_scaling', 'perf_lev_window', 'perf_lev_sortino_target', 'perf_lev_max_boost',
      'enable_vol_lev_scaling', 'vol_lev_window', 'vol_lev_target_vol', 'vol_lev_max_boost', 'vol_lev_dd_threshold',
      'enable_contra_lev_scaling', 'contra_lev_window', 'contra_lev_max_boost', 'contra_lev_dd_threshold',
    ],
  },
  {
    title: 'ADVANCED • RISK OVERLAYS',
    keys: [
      'enable_pph', 'pph_frequency', 'pph_threshold', 'pph_harvest_frac', 'pph_sweep_enabled',
      'enable_ratchet', 'ratchet_frequency', 'ratchet_trigger', 'ratchet_lock_pct', 'ratchet_risk_off_lev_scale', 'ratchet_sweep_enabled',
      'enable_adaptive_ratchet', 'adaptive_ratchet_frequency', 'adaptive_ratchet_vol_window', 'adaptive_ratchet_vol_low',
      'adaptive_ratchet_vol_high', 'adaptive_ratchet_risk_off_scale', 'adaptive_ratchet_floor_decay', 'adaptive_ratchet_sweep_enabled',
    ],
  },
  {
    title: 'ADVANCED • PARAMETER SWEEPS',
    keys: ['enable_sweep_l_high', 'enable_sweep_tail_guardrail', 'enable_sweep_trail_wide', 'enable_sweep_trail_narrow', 'enable_param_surfaces'],
  },
  {
    title: 'ADVANCED • STABILITY CUBES',
    keys: ['enable_stability_cube', 'enable_risk_throttle_cube', 'enable_exit_cube'],
  },
  {
    title: 'ADVANCED • ROBUSTNESS + STRESS TESTS',
    keys: [
      'enable_noise_stability', 'enable_slippage_sweep', 'enable_equity_ensemble', 'enable_param_jitter',
      'enable_return_concentration', 'enable_sharpe_ridge_map', 'enable_sharpe_plateau', 'enable_top_n_removal',
      'enable_lucky_streak', 'enable_periodic_breakdown', 'enable_weekly_milestones', 'enable_monthly_milestones',
      'enable_dsr_mtl', 'enable_shock_injection', 'enable_ruin_probability',
    ],
  },
  {
    title: 'ADVANCED • DIAGNOSTICS',
    keys: ['enable_mcap_diagnostic', 'enable_capacity_curve', 'enable_regime_robustness', 'enable_min_cum_return'],
  },
  {
    title: 'EXPERT',
    keys: ['annualization_factor', 'bar_minutes', 'end_cross_midnight', 'save_daily_files', 'build_master_file'],
  },
];

function groupActiveConfigs(activeConfigs: Array<[string, unknown]>): Array<{ title: string; entries: Array<[string, unknown]> }> {
  const byKey = new Map(activeConfigs);
  const used = new Set<string>();
  const grouped: Array<{ title: string; entries: Array<[string, unknown]> }> = [];

  for (const section of CONFIG_SECTIONS) {
    const entries: Array<[string, unknown]> = [];
    for (const k of section.keys) {
      if (byKey.has(k)) {
        used.add(k);
        if (section.title === 'FILTERS' && k.startsWith('enable_')) continue;
        entries.push([k, byKey.get(k)]);
      }
    }
    if (entries.length > 0) grouped.push({ title: section.title, entries });
  }

  const uncategorized = activeConfigs
    .filter(([k]) => !used.has(k))
    .sort(([a], [b]) => a.localeCompare(b));
  if (uncategorized.length > 0) {
    grouped.push({ title: 'OTHER', entries: uncategorized });
  }
  return grouped;
}

function pickBestFilterRow(metrics: Record<string, unknown>): FilterMetricRow | null {
  const rows = [
    ...(((metrics.filters as FilterMetricRow[] | undefined) ?? [])),
    ...(((metrics.filter_comparison as FilterMetricRow[] | undefined) ?? [])),
  ].filter((r) => r && r.filter && !r.not_run);
  if (rows.length === 0) return null;

  const bestFilter = String(metrics.best_filter ?? '').trim();
  if (bestFilter) {
    const target = normalizeFilterLabel(bestFilter);
    const exact = rows.find((r) => normalizeFilterLabel(String(r.filter ?? '')) === target);
    if (exact) return exact;
  }

  return [...rows].sort((a, b) => {
    const ag = asNum(a.grade_score);
    const bg = asNum(b.grade_score);
    if (ag !== null || bg !== null) {
      if (ag === null) return 1;
      if (bg === null) return -1;
      if (bg !== ag) return bg - ag;
    }
    const as = asNum(a.sharpe) ?? Number.NEGATIVE_INFINITY;
    const bs = asNum(b.sharpe) ?? Number.NEGATIVE_INFINITY;
    return bs - as;
  })[0] ?? null;
}

export default function ResultsSummary({
  results,
  params,
  onRerun,
  startAuditConfigsCollapsed = false,
}: ResultsSummaryProps) {
  const metrics = (results?.metrics ?? {}) as Record<string, unknown>;
  const bestRow = pickBestFilterRow(metrics);
  const canonical = (bestRow?.filter ?? metrics?.best_filter ?? results?.canonical_filter ?? '') as string;
  const grade = (bestRow?.grade ?? metrics?.grade ?? (bestRow?.grade_score != null ? String(bestRow.grade_score) : 'N/A')) as string;
  const scorecard = (results?.scorecard ?? []) as Array<{ label: string; status: string; value?: unknown }>;

  const sharpe = (bestRow?.sharpe ?? metrics?.sharpe) as number | undefined;
  const maxDd = (bestRow?.max_dd ?? metrics?.max_drawdown) as number | undefined;
  const cagr = (bestRow?.cagr ?? metrics?.cagr) as number | undefined;
  const avg1mRet = (
    avgMonthlyReturnPctFromCurve(bestRow?.equity_curve ?? metrics?.equity_curve)
    ?? avgMonthlyFromCagrPct(cagr)
  );

  const hiddenKeys = getInactiveChildKeys(params);
  const activeConfigs = Object.entries(params)
    .filter(([k, v]) => !hiddenKeys.has(k) && isActiveConfigValue(v))
    .sort(([a], [b]) => a.localeCompare(b));
  const groupedActiveConfigs = groupActiveConfigs(activeConfigs);
  const [auditConfigsOpen, setAuditConfigsOpen] = useState(!startAuditConfigsCollapsed);
  const [openConfigSections, setOpenConfigSections] = useState<Record<string, boolean>>({});

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
          <span
            style={{
              fontSize: 8,
              color: 'var(--green)',
              border: '1px solid var(--green-mid)',
              borderRadius: 2,
              padding: '1px 6px',
              letterSpacing: '0.08em',
            }}
          >
            {canonical || '—'}
          </span>
        </div>

        {/* KPI trio */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 6 }}>
          {[
            { label: 'Sharpe', key: 'sharpe', value: sharpe },
            { label: 'Max DD', key: 'max_dd', value: maxDd },
            { label: 'avg. 1M ret', key: 'avg_1m_ret', value: avg1mRet },
          ].map(({ label, key, value }) => (
            <div key={key} style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 9, color: 'var(--t3)', letterSpacing: '0.08em', marginBottom: 2, opacity: 0.72 }}>
                {label}
              </div>
              <div style={{ fontSize: 15, fontWeight: 700, color: kpiColor(key, value), opacity: 0.72 }}>
                {key === 'max_dd' || key === 'avg_1m_ret' ? fmtPercent1(value) : fmtVal(value)}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Parameters */}
      <div style={{ marginBottom: 12 }}>
        <details
          open={auditConfigsOpen}
          onToggle={(e) => setAuditConfigsOpen((e.currentTarget as HTMLDetailsElement).open)}
          style={{
            border: '1px solid var(--line)',
            borderRadius: 3,
            background: 'var(--bg1)',
            padding: '6px 8px',
          }}
        >
          <summary
            style={{
              fontSize: 9,
              textTransform: 'uppercase',
              letterSpacing: '0.12em',
              color: 'var(--t3)',
              fontWeight: 700,
              cursor: 'pointer',
              userSelect: 'none',
            }}
          >
            AUDIT CONFIGS
          </summary>
          <div style={{ marginTop: 8 }}>
            {activeConfigs.length === 0 && (
              <div style={{ fontSize: 10, color: 'var(--t2)' }}>No active configs.</div>
            )}
            {groupedActiveConfigs.map((section) => (
              <details
                key={section.title}
                open={!!openConfigSections[section.title]}
                onToggle={(e) => {
                  const isOpen = (e.currentTarget as HTMLDetailsElement).open;
                  setOpenConfigSections((prev) => ({ ...prev, [section.title]: isOpen }));
                }}
                style={{
                  marginBottom: 8,
                  border: '1px solid var(--line)',
                  borderRadius: 3,
                  background: 'var(--bg0)',
                  padding: '4px 6px',
                }}
              >
                <summary
                  style={{
                    fontSize: 8,
                    textTransform: 'uppercase',
                    letterSpacing: '0.1em',
                    color: 'var(--t3)',
                    fontWeight: 700,
                    cursor: 'pointer',
                    userSelect: 'none',
                  }}
                >
                  {section.title}
                </summary>
                <div style={{ marginTop: 6 }}>
                  {section.entries.map(([k, v]) => (
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
              </details>
            ))}
          </div>
        </details>
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

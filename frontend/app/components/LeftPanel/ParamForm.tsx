'use client';

import { useEffect, useState } from 'react';

import Toggle from '../ui/Toggle';
import TierSection from '../ui/TierSection';
import ConditionalParams from '../ui/ConditionalParams';

// ─────────────────────────────────────────────
// Shared input styles
// ─────────────────────────────────────────────
const numStyle: React.CSSProperties = {
  background: 'var(--bg3)',
  border: '1px solid var(--line2)',
  borderRadius: 2,
  padding: '3px 7px',
  fontFamily: 'Space Mono, monospace',
  fontSize: 10,
  color: 'var(--t0)',
  textAlign: 'right',
  width: 82,
  outline: 'none',
};

const selStyle: React.CSSProperties = {
  ...numStyle,
  width: 100,
  textAlign: 'right',
};

const sectionLabel: React.CSSProperties = {
  fontSize: 9,
  textTransform: 'uppercase',
  letterSpacing: '0.12em',
  color: 'var(--t3)',
  fontWeight: 700,
  marginBottom: 6,
};

const subSectionLabel: React.CSSProperties = {
  fontSize: 9,
  textTransform: 'uppercase',
  letterSpacing: '0.10em',
  color: 'var(--t3)',
  fontWeight: 700,
  marginBottom: 4,
  marginTop: 8,
  opacity: 0.7,
};

const subSectionHeader: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  marginTop: 8,
  marginBottom: 4,
};

const fieldLabel: React.CSSProperties = {
  fontSize: 10,
  color: 'var(--t2)',
  fontWeight: 400,
};

const row: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  marginBottom: 4,
};

const FILTER_ENABLE_KEYS = [
  'enable_tail_guardrail',
  'enable_dispersion_filter',
  'enable_tail_plus_disp',
  'enable_vol_filter',
  'enable_tail_disp_vol',
  'enable_tail_or_vol',
  'enable_tail_and_vol',
  'enable_blofin_filter',
];

// ─────────────────────────────────────────────
// Sub-components
// ─────────────────────────────────────────────
function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={row}>
      <span style={fieldLabel}>{label}</span>
      {children}
    </div>
  );
}

function NumInput({
  value,
  onChange,
  placeholder,
  min,
  max,
  style,
}: {
  value: unknown;
  onChange: (v: number | string) => void;
  placeholder?: string;
  min?: number;
  max?: number;
  style?: React.CSSProperties;
}) {
  return (
    <input
      type="number"
      value={value === null || value === undefined ? '' : value as number}
      placeholder={placeholder}
      min={min}
      max={max}
      onChange={(e) => onChange(e.target.value === '' ? '' : Number(e.target.value))}
      style={{ ...numStyle, ...style }}
      onFocus={(e) => (e.target.style.borderColor = 'var(--green)')}
      onBlur={(e) => (e.target.style.borderColor = 'var(--line2)')}
    />
  );
}

function TextInput({
  value,
  onChange,
  placeholder,
}: {
  value: unknown;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <input
      type="text"
      value={value as string}
      placeholder={placeholder}
      onChange={(e) => onChange(e.target.value)}
      style={numStyle}
      onFocus={(e) => (e.target.style.borderColor = 'var(--green)')}
      onBlur={(e) => (e.target.style.borderColor = 'var(--line2)')}
    />
  );
}

function SelInput({
  value,
  onChange,
  options,
}: {
  value: unknown;
  onChange: (v: string) => void;
  options: { value: string; label: string }[];
}) {
  return (
    <select
      value={value as string}
      onChange={(e) => onChange(e.target.value)}
      style={selStyle}
      onFocus={(e) => (e.target.style.borderColor = 'var(--green)')}
      onBlur={(e) => (e.target.style.borderColor = 'var(--line2)')}
    >
      {options.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  );
}

function CollapsibleSection({
  title,
  open,
  onToggle,
  children,
}: {
  title: string;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  return (
    <div style={{ marginBottom: 12 }}>
      <button
        type="button"
        onClick={onToggle}
        style={{
          width: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '8px 10px',
          background: 'var(--bg1)',
          border: '1px solid var(--line)',
          borderRadius: 3,
          cursor: 'pointer',
          userSelect: 'none',
          textAlign: 'left',
        }}
      >
        <span style={{ ...sectionLabel, marginBottom: 0 }}>{title}</span>
        <svg
          width="8"
          height="8"
          viewBox="0 0 8 8"
          fill="none"
          style={{
            transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
            transition: 'transform 0.2s',
            flexShrink: 0,
          }}
        >
          <path d="M2 1.5L6 4L2 6.5" stroke="var(--t2)" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </button>
      <div
        style={{
          overflow: 'hidden',
          maxHeight: open ? '2200px' : '0',
          transition: 'max-height 0.25s ease',
        }}
      >
        <div style={{ padding: open ? '8px 2px 0 2px' : '0 2px' }}>
          {children}
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────
// Main component
// ─────────────────────────────────────────────
interface ParamFormProps {
  params: Record<string, unknown>;
  onChange: (p: Record<string, unknown>) => void;
  onSubmit: (p: Record<string, unknown>) => void;
}

export default function ParamForm({ params, onChange, onSubmit }: ParamFormProps) {
  function set(key: string, value: unknown) {
    onChange({ ...params, [key]: value });
  }
  function setAllTrue(keys: string[]) {
    const next = { ...params };
    for (const key of keys) next[key] = true;
    onChange(next);
  }

  const p = params;
  const volLevEnabled = !!p.enable_vol_lev_scaling;
  const parameterSweepKeys = [
    'enable_sweep_l_high',
    'enable_sweep_tail_guardrail',
    'enable_sweep_trail_wide',
    'enable_sweep_trail_narrow',
    'enable_param_surfaces',
  ];
  const stabilityCubeKeys = ['enable_stability_cube', 'enable_risk_throttle_cube', 'enable_exit_cube'];
  const robustnessKeys = [
    'enable_noise_stability',
    'enable_slippage_sweep',
    'enable_equity_ensemble',
    'enable_param_jitter',
    'enable_return_concentration',
    'enable_sharpe_ridge_map',
    'enable_sharpe_plateau',
    'enable_top_n_removal',
    'enable_lucky_streak',
    'enable_periodic_breakdown',
    'enable_weekly_milestones',
    'enable_monthly_milestones',
    'enable_dsr_mtl',
    'enable_shock_injection',
    'enable_ruin_probability',
  ];
  const diagnosticsKeys = [
    'enable_mcap_diagnostic',
    'enable_capacity_curve',
    'enable_regime_robustness',
    'enable_min_cum_return',
  ];
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({});
  useEffect(() => {
    const missing = FILTER_ENABLE_KEYS.filter((k) => !params[k]);
    if (missing.length === 0) return;
    const next = { ...params };
    for (const k of missing) next[k] = true;
    onChange(next);
  }, [params, onChange]);
  useEffect(() => {
    if (volLevEnabled) return;
    const keys = ['enable_stability_cube', 'enable_risk_throttle_cube', 'enable_exit_cube'];
    const hasEnabled = keys.some((k) => !!params[k]);
    if (!hasEnabled) return;
    const next = { ...params };
    for (const k of keys) next[k] = false;
    onChange(next);
  }, [volLevEnabled, params, onChange]);
  const isOpen = (key: string) => !!openSections[key];
  const toggleSection = (key: string) => {
    setOpenSections((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <div style={{ padding: 12 }}>
      {/* ── STRATEGY ── */}
      <CollapsibleSection title="STRATEGY" open={isOpen('strategy')} onToggle={() => toggleSection('strategy')}>
        <Row label="leaderboard_index">
          <NumInput value={p.leaderboard_index} onChange={(v) => set('leaderboard_index', v)} />
        </Row>
        <Row label="sort_by">
          <SelInput
            value={p.sort_by}
            onChange={(v) => set('sort_by', v)}
            options={[
              { value: 'price', label: 'price' },
              { value: 'open_interest', label: 'open_interest' },
              { value: 'combined', label: 'combined' },
              { value: 'price-only', label: 'price-only' },
              { value: 'oi-only', label: 'oi-only' },
            ]}
          />
        </Row>
        <Row label="mode">
          <SelInput
            value={p.mode}
            onChange={(v) => set('mode', v)}
            options={[
              { value: 'snapshot', label: 'snapshot' },
              { value: 'frequency', label: 'frequency' },
            ]}
          />
        </Row>
        <Row label="price_ranking_metric">
          <SelInput
            value={p.price_ranking_metric}
            onChange={(v) => set('price_ranking_metric', v)}
            options={[
              { value: 'pct_change', label: 'pct_change' },
              { value: 'log_return', label: 'log_return (v1)' },
              { value: 'abs_dollar', label: 'abs_dollar' },
            ]}
          />
        </Row>
        <Row label="oi_ranking_metric">
          <SelInput
            value={p.oi_ranking_metric}
            onChange={(v) => set('oi_ranking_metric', v)}
            options={[
              { value: 'pct_change', label: 'pct_change' },
              { value: 'abs_dollar', label: 'abs_dollar (v1)' },
            ]}
          />
        </Row>
        <Row label="apply_blofin_filter">
          <Toggle
            checked={!!p.apply_blofin_filter}
            onChange={(v) => set('apply_blofin_filter', v)}
          />
        </Row>
        <Row label="freq_width">
          <NumInput value={p.freq_width} onChange={(v) => set('freq_width', v)} />
        </Row>
        <Row label="freq_cutoff">
          <NumInput value={p.freq_cutoff} onChange={(v) => set('freq_cutoff', v)} />
        </Row>
        <Row label="sample_interval">
          <NumInput value={p.sample_interval} onChange={(v) => set('sample_interval', v)} />
        </Row>
      </CollapsibleSection>

      {/* ── DEPLOYMENT WINDOW ── */}
      <CollapsibleSection title="DEPLOYMENT WINDOW" open={isOpen('deployment')} onToggle={() => toggleSection('deployment')}>
        <Row label="deployment_start_hour">
          <NumInput value={p.deployment_start_hour} onChange={(v) => set('deployment_start_hour', v)} min={0} max={23} />
        </Row>
        <Row label="index_lookback">
          <NumInput value={p.index_lookback} onChange={(v) => set('index_lookback', v)} />
        </Row>
        <Row label="sort_lookback">
          <NumInput value={p.sort_lookback} onChange={(v) => set('sort_lookback', v)} />
        </Row>
        <Row label="deployment_runtime_hours">
          <TextInput value={p.deployment_runtime_hours} onChange={(v) => set('deployment_runtime_hours', v)} />
        </Row>
      </CollapsibleSection>

      {/* ── UNIVERSE + RISK ── */}
      <CollapsibleSection title="UNIVERSE + RISK" open={isOpen('universe')} onToggle={() => toggleSection('universe')}>
        <Row label="starting_capital">
          <NumInput value={p.starting_capital} onChange={(v) => set('starting_capital', v)} />
        </Row>
        <Row label="capital_mode">
          <SelInput
            value={p.capital_mode}
            onChange={(v) => set('capital_mode', v)}
            options={[
              { value: 'fixed', label: 'fixed' },
              { value: 'compounding', label: 'compounding' },
            ]}
          />
        </Row>
        {p.capital_mode === 'fixed' && (
          <Row label="fixed_notional_cap">
            <SelInput
              value={p.fixed_notional_cap}
              onChange={(v) => set('fixed_notional_cap', v)}
              options={[
                { value: 'internal', label: 'internal' },
                { value: 'external', label: 'external' },
              ]}
            />
          </Row>
        )}
        <Row label="pivot_leverage">
          <NumInput value={p.pivot_leverage} onChange={(v) => set('pivot_leverage', v)} />
        </Row>
        <Row label="min_mcap">
          <NumInput value={p.min_mcap} onChange={(v) => set('min_mcap', v)} />
        </Row>
        <Row label="max_mcap">
          <NumInput value={p.max_mcap} onChange={(v) => set('max_mcap', v)} />
        </Row>
        <Row label="min_listing_age">
          <NumInput value={p.min_listing_age} onChange={(v) => set('min_listing_age', v)} />
        </Row>
        <Row label="max_port">
          <NumInput value={p.max_port} onChange={(v) => set('max_port', v)} placeholder="∞" />
        </Row>
        <Row label="drop_unverified">
          <Toggle checked={!!p.drop_unverified} onChange={(v) => set('drop_unverified', v)} />
        </Row>
        <Row label="leverage">
          <NumInput value={p.leverage} onChange={(v) => set('leverage', v)} />
        </Row>
        <Row label="stop_raw_pct">
          <NumInput value={p.stop_raw_pct} onChange={(v) => set('stop_raw_pct', v)} />
        </Row>
        <Row label="price_source">
          <SelInput
            value={p.price_source}
            onChange={(v) => set('price_source', v)}
            options={[
              { value: 'parquet', label: 'parquet' },
              { value: 'db', label: 'database' },
              { value: 'binance', label: 'binance' },
            ]}
          />
        </Row>
        <Row label="mcap_source">
          <SelInput
            value={p.mcap_source}
            onChange={(v) => set('mcap_source', v)}
            options={[
              { value: 'parquet', label: 'parquet' },
              { value: 'db', label: 'database' },
            ]}
          />
        </Row>
        <Row label="save_charts">
          <Toggle checked={!!p.save_charts} onChange={(v) => set('save_charts', v)} />
        </Row>
        <Row label="trial_purchases">
          <Toggle checked={!!p.trial_purchases} onChange={(v) => set('trial_purchases', v)} />
        </Row>
        <Row label="quick">
          <Toggle checked={!!p.quick} onChange={(v) => set('quick', v)} />
        </Row>

        {/* Trading costs sub-section */}
        <div style={subSectionLabel}>TRADING COSTS</div>
        <Row label="taker_fee_pct">
          <NumInput value={p.taker_fee_pct} onChange={(v) => set('taker_fee_pct', v)} />
        </Row>
        <Row label="funding_rate_daily_pct">
          <NumInput value={p.funding_rate_daily_pct} onChange={(v) => set('funding_rate_daily_pct', v)} />
        </Row>
      </CollapsibleSection>

      {/* ── EXECUTION CONFIG ── */}
      <CollapsibleSection title="EXECUTION CONFIG" open={isOpen('execution')} onToggle={() => toggleSection('execution')}>
      <div
        style={{
          background: 'var(--bg0)',
          border: '1px solid var(--line)',
          borderRadius: 3,
          padding: 8,
          margin: '2px 0',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
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
            CANDIDATE_CONFIGS
          </span>
        </div>
        <div style={subSectionLabel}>EARLY EXIT</div>
        <Row label="early_kill_x">
          <NumInput value={p.early_kill_x} onChange={(v) => set('early_kill_x', v)} />
        </Row>
        <Row label="early_kill_y">
          <NumInput value={p.early_kill_y} onChange={(v) => set('early_kill_y', v)} />
        </Row>
        <Row label="early_instill_y">
          <NumInput value={p.early_instill_y} onChange={(v) => set('early_instill_y', v)} />
        </Row>

        <div style={subSectionLabel}>LEVERAGE</div>
        <Row label="l_base">
          <NumInput value={p.l_base} onChange={(v) => set('l_base', v)} />
        </Row>
        <Row label="l_high">
          <NumInput value={p.l_high} onChange={(v) => set('l_high', v)} />
        </Row>

        <div style={subSectionLabel}>STOPS</div>
        <Row label="port_tsl">
          <NumInput value={p.port_tsl} onChange={(v) => set('port_tsl', v)} />
        </Row>
        <Row label="port_sl">
          <NumInput value={p.port_sl} onChange={(v) => set('port_sl', v)} />
        </Row>

        <div style={subSectionLabel}>EARLY FILL</div>
        <Row label="early_fill_y">
          <NumInput value={p.early_fill_y} onChange={(v) => set('early_fill_y', v)} />
        </Row>
        <Row label="early_fill_x">
          <NumInput value={p.early_fill_x} onChange={(v) => set('early_fill_x', v)} />
        </Row>
      </div>
      </CollapsibleSection>

      {/* ── FILTERS ── */}
      <CollapsibleSection title="FILTERS" open={isOpen('filters')} onToggle={() => toggleSection('filters')}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
          <span style={{ ...fieldLabel, opacity: 0.8 }}>Mode</span>
          <span style={{ fontSize: 9, color: 'var(--t3)', width: 26, textAlign: 'center' }}>RUN</span>
        </div>

        {(
          [
            ['Tail Guardrail', 'enable_tail_guardrail', 'run_filter_tail'],
            ['Dispersion', 'enable_dispersion_filter', 'run_filter_dispersion'],
            ['Tail+Disp', 'enable_tail_plus_disp', 'run_filter_tail_disp'],
            ['Vol', 'enable_vol_filter', 'run_filter_vol'],
            ['Tail+Disp+Vol', 'enable_tail_disp_vol', 'run_filter_tail_disp_vol'],
            ['Tail OR Vol', 'enable_tail_or_vol', 'run_filter_tail_or_vol'],
            ['Tail AND Vol', 'enable_tail_and_vol', 'run_filter_tail_and_vol'],
            ['BloFin', 'enable_blofin_filter', 'run_filter_tail_blofin'],
          ] as [string, string, string][]
        ).map(([label, enableKey, runKey]) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
            <span style={fieldLabel}>{label}</span>
            <Toggle
              checked={!!p[runKey]}
              onChange={(v) => {
                set(runKey, v);
                if (!p[enableKey]) set(enableKey, true);
              }}
            />
          </div>
        ))}

        <div style={{ marginTop: 8, borderTop: '1px solid var(--line)', paddingTop: 8 }}>
          <Row label="No Filter baseline">
            <Toggle checked={!!p.run_filter_none} onChange={(v) => set('run_filter_none', v)} />
          </Row>
          <Row label="BTC MA Filter">
            <Toggle checked={!!p.enable_btc_ma_filter} onChange={(v) => set('enable_btc_ma_filter', v)} />
          </Row>
          <Row label="IC Diagnostic">
            <Toggle checked={!!p.enable_ic_diagnostic} onChange={(v) => set('enable_ic_diagnostic', v)} />
          </Row>
          <Row label="IC Filter">
            <Toggle checked={!!p.enable_ic_filter} onChange={(v) => set('enable_ic_filter', v)} />
          </Row>
          <Row label="Calendar">
            <Toggle checked={!!p.run_filter_calendar} onChange={(v) => set('run_filter_calendar', v)} />
          </Row>
        </div>
      </CollapsibleSection>

      {/* ── ADVANCED TIER ── */}
      <TierSection title="ADVANCED" color="#378ADD" subtitle="strategy tuning + audit modules" defaultExpanded={false}>
        <div style={{ padding: '8px 12px' }}>
          {/* Strategy Tuning */}
          <div style={subSectionLabel}>STRATEGY TUNING</div>
          <ConditionalParams show={!!p.enable_dispersion_filter}>
            <Row label="dispersion_threshold">
              <NumInput value={p.dispersion_threshold} onChange={(v) => set('dispersion_threshold', v)} />
            </Row>
            <Row label="dispersion_baseline_win">
              <NumInput value={p.dispersion_baseline_win} onChange={(v) => set('dispersion_baseline_win', v)} />
            </Row>
            <Row label="dispersion_n">
              <NumInput value={p.dispersion_n} onChange={(v) => set('dispersion_n', v)} />
            </Row>
            <Row label="dispersion_dynamic_universe">
              <Toggle checked={!!p.dispersion_dynamic_universe} onChange={(v) => set('dispersion_dynamic_universe', v)} />
            </Row>
          </ConditionalParams>
          <ConditionalParams show={!!p.enable_vol_filter}>
            <Row label="vol_lookback">
              <NumInput value={p.vol_lookback} onChange={(v) => set('vol_lookback', v)} />
            </Row>
            <Row label="vol_percentile">
              <NumInput value={p.vol_percentile} onChange={(v) => set('vol_percentile', v)} />
            </Row>
            <Row label="vol_baseline_win">
              <NumInput value={p.vol_baseline_win} onChange={(v) => set('vol_baseline_win', v)} />
            </Row>
          </ConditionalParams>
          <ConditionalParams show={!!p.enable_tail_guardrail}>
            <Row label="tail_drop_pct">
              <NumInput value={p.tail_drop_pct} onChange={(v) => set('tail_drop_pct', v)} />
            </Row>
            <Row label="tail_vol_mult">
              <NumInput value={p.tail_vol_mult} onChange={(v) => set('tail_vol_mult', v)} />
            </Row>
          </ConditionalParams>
          <ConditionalParams show={!!p.enable_ic_diagnostic || !!p.enable_ic_filter}>
            <Row label="ic_signal">
              <SelInput
                value={p.ic_signal}
                onChange={(v) => set('ic_signal', v)}
                options={[
                  { value: 'mom1d', label: 'mom1d' },
                  { value: 'mom5d', label: 'mom5d' },
                  { value: 'skew20d', label: 'skew20d' },
                  { value: 'vol20d_inv', label: 'vol20d_inv' },
                ]}
              />
            </Row>
            <Row label="ic_window">
              <NumInput value={p.ic_window} onChange={(v) => set('ic_window', v)} />
            </Row>
            <Row label="ic_threshold">
              <NumInput value={p.ic_threshold} onChange={(v) => set('ic_threshold', v)} />
            </Row>
          </ConditionalParams>
          <ConditionalParams show={!!p.enable_btc_ma_filter}>
            <Row label="btc_ma_days">
              <NumInput value={p.btc_ma_days} onChange={(v) => set('btc_ma_days', v)} />
            </Row>
          </ConditionalParams>
          <ConditionalParams show={!!p.enable_blofin_filter}>
            <Row label="blofin_min_symbols">
              <NumInput value={p.blofin_min_symbols} onChange={(v) => set('blofin_min_symbols', v)} />
            </Row>
          </ConditionalParams>
          <Row label="leaderboard_top_n">
            <NumInput value={p.leaderboard_top_n} onChange={(v) => set('leaderboard_top_n', v)} />
          </Row>
          <Row label="train_test_split">
            <NumInput value={p.train_test_split} onChange={(v) => set('train_test_split', v)} />
          </Row>
          <Row label="n_trials">
            <NumInput value={p.n_trials} onChange={(v) => set('n_trials', v)} />
          </Row>

          {/* Leverage Scaling */}
          <div style={subSectionLabel}>LEVERAGE SCALING</div>
          <Row label="enable_perf_lev_scaling">
            <Toggle checked={!!p.enable_perf_lev_scaling} onChange={(v) => set('enable_perf_lev_scaling', v)} />
          </Row>
          <ConditionalParams show={!!p.enable_perf_lev_scaling}>
            <Row label="perf_lev_window">
              <NumInput value={p.perf_lev_window} onChange={(v) => set('perf_lev_window', v)} />
            </Row>
            <Row label="perf_lev_sortino_target">
              <NumInput value={p.perf_lev_sortino_target} onChange={(v) => set('perf_lev_sortino_target', v)} />
            </Row>
            <Row label="perf_lev_max_boost">
              <NumInput value={p.perf_lev_max_boost} onChange={(v) => set('perf_lev_max_boost', v)} />
            </Row>
          </ConditionalParams>

          <Row label="enable_vol_lev_scaling">
            <Toggle checked={!!p.enable_vol_lev_scaling} onChange={(v) => set('enable_vol_lev_scaling', v)} />
          </Row>
          <ConditionalParams show={!!p.enable_vol_lev_scaling}>
            <Row label="vol_lev_window">
              <NumInput value={p.vol_lev_window} onChange={(v) => set('vol_lev_window', v)} />
            </Row>
            <Row label="vol_lev_target_vol">
              <NumInput value={p.vol_lev_target_vol} onChange={(v) => set('vol_lev_target_vol', v)} />
            </Row>
            <Row label="vol_lev_max_boost">
              <NumInput value={p.vol_lev_max_boost} onChange={(v) => set('vol_lev_max_boost', v)} />
            </Row>
            <Row label="vol_lev_dd_threshold">
              <NumInput value={p.vol_lev_dd_threshold} onChange={(v) => set('vol_lev_dd_threshold', v)} />
            </Row>
            <Row label="lev_quantization_mode">
              <SelInput
                value={p.lev_quantization_mode}
                onChange={(v) => set('lev_quantization_mode', v)}
                options={[
                  { value: 'off', label: 'off' },
                  { value: 'binary', label: 'binary (1x/cap)' },
                  { value: 'stepped', label: 'stepped' },
                ]}
              />
            </Row>
            <ConditionalParams show={String(p.lev_quantization_mode ?? 'off') === 'stepped'}>
              <Row label="lev_quantization_step">
                <NumInput value={p.lev_quantization_step} onChange={(v) => set('lev_quantization_step', v)} />
              </Row>
            </ConditionalParams>
          </ConditionalParams>

          <Row label="enable_contra_lev_scaling">
            <Toggle checked={!!p.enable_contra_lev_scaling} onChange={(v) => set('enable_contra_lev_scaling', v)} />
          </Row>
          <ConditionalParams show={!!p.enable_contra_lev_scaling}>
            <Row label="contra_lev_window">
              <NumInput value={p.contra_lev_window} onChange={(v) => set('contra_lev_window', v)} />
            </Row>
            <Row label="contra_lev_max_boost">
              <NumInput value={p.contra_lev_max_boost} onChange={(v) => set('contra_lev_max_boost', v)} />
            </Row>
            <Row label="contra_lev_dd_threshold">
              <NumInput value={p.contra_lev_dd_threshold} onChange={(v) => set('contra_lev_dd_threshold', v)} />
            </Row>
          </ConditionalParams>

          {/* Risk Overlays */}
          <div style={subSectionLabel}>RISK OVERLAYS</div>
          <Row label="enable_pph">
            <Toggle checked={!!p.enable_pph} onChange={(v) => set('enable_pph', v)} />
          </Row>
          <ConditionalParams show={!!p.enable_pph}>
            <Row label="pph_frequency">
              <SelInput
                value={p.pph_frequency}
                onChange={(v) => set('pph_frequency', v)}
                options={[
                  { value: 'daily', label: 'daily' },
                  { value: 'weekly', label: 'weekly' },
                  { value: 'monthly', label: 'monthly' },
                ]}
              />
            </Row>
            <Row label="pph_threshold">
              <NumInput value={p.pph_threshold} onChange={(v) => set('pph_threshold', v)} />
            </Row>
            <Row label="pph_harvest_frac">
              <NumInput value={p.pph_harvest_frac} onChange={(v) => set('pph_harvest_frac', v)} />
            </Row>
          </ConditionalParams>
          <Row label="pph_sweep_enabled">
            <Toggle
              checked={!!p.pph_sweep_enabled}
              onChange={(v) => set('pph_sweep_enabled', v)}
              disabled={!p.enable_pph}
            />
          </Row>

          <Row label="enable_ratchet">
            <Toggle checked={!!p.enable_ratchet} onChange={(v) => set('enable_ratchet', v)} />
          </Row>
          <ConditionalParams show={!!p.enable_ratchet}>
            <Row label="ratchet_frequency">
              <SelInput
                value={p.ratchet_frequency}
                onChange={(v) => set('ratchet_frequency', v)}
                options={[
                  { value: 'daily', label: 'daily' },
                  { value: 'weekly', label: 'weekly' },
                  { value: 'monthly', label: 'monthly' },
                ]}
              />
            </Row>
            <Row label="ratchet_trigger">
              <NumInput value={p.ratchet_trigger} onChange={(v) => set('ratchet_trigger', v)} />
            </Row>
            <Row label="ratchet_lock_pct">
              <NumInput value={p.ratchet_lock_pct} onChange={(v) => set('ratchet_lock_pct', v)} />
            </Row>
            <Row label="ratchet_risk_off_lev_scale">
              <NumInput value={p.ratchet_risk_off_lev_scale} onChange={(v) => set('ratchet_risk_off_lev_scale', v)} />
            </Row>
          </ConditionalParams>
          <Row label="ratchet_sweep_enabled">
            <Toggle
              checked={!!p.ratchet_sweep_enabled}
              onChange={(v) => set('ratchet_sweep_enabled', v)}
              disabled={!p.enable_ratchet}
            />
          </Row>

          <Row label="enable_adaptive_ratchet">
            <Toggle checked={!!p.enable_adaptive_ratchet} onChange={(v) => set('enable_adaptive_ratchet', v)} />
          </Row>
          <ConditionalParams show={!!p.enable_adaptive_ratchet}>
            <Row label="adaptive_ratchet_frequency">
              <SelInput
                value={p.adaptive_ratchet_frequency}
                onChange={(v) => set('adaptive_ratchet_frequency', v)}
                options={[
                  { value: 'daily', label: 'daily' },
                  { value: 'weekly', label: 'weekly' },
                  { value: 'monthly', label: 'monthly' },
                ]}
              />
            </Row>
            <Row label="adaptive_ratchet_vol_window">
              <NumInput value={p.adaptive_ratchet_vol_window} onChange={(v) => set('adaptive_ratchet_vol_window', v)} />
            </Row>
            <Row label="adaptive_ratchet_vol_low">
              <NumInput value={p.adaptive_ratchet_vol_low} onChange={(v) => set('adaptive_ratchet_vol_low', v)} />
            </Row>
            <Row label="adaptive_ratchet_vol_high">
              <NumInput value={p.adaptive_ratchet_vol_high} onChange={(v) => set('adaptive_ratchet_vol_high', v)} />
            </Row>
            <Row label="adaptive_ratchet_risk_off_scale">
              <NumInput value={p.adaptive_ratchet_risk_off_scale} onChange={(v) => set('adaptive_ratchet_risk_off_scale', v)} />
            </Row>
            <Row label="adaptive_ratchet_floor_decay">
              <NumInput value={p.adaptive_ratchet_floor_decay} onChange={(v) => set('adaptive_ratchet_floor_decay', v)} />
            </Row>
          </ConditionalParams>
          <Row label="adaptive_ratchet_sweep_enabled">
            <Toggle
              checked={!!p.adaptive_ratchet_sweep_enabled}
              onChange={(v) => set('adaptive_ratchet_sweep_enabled', v)}
              disabled={!p.enable_adaptive_ratchet}
            />
          </Row>

          {/* Parameter Sweeps */}
          <div style={subSectionHeader}>
            <div style={{ ...subSectionLabel, margin: 0 }}>PARAMETER SWEEPS</div>
            <button
              type="button"
              onClick={() => setAllTrue(parameterSweepKeys)}
              style={{ background: 'transparent', border: '1px solid var(--line2)', color: 'var(--t2)', borderRadius: 2, fontSize: 9, padding: '2px 6px', cursor: 'pointer' }}
            >
              Enable All
            </button>
          </div>
          {parameterSweepKeys.map((k) => (
            <Row key={k} label={k}>
              <Toggle checked={!!p[k]} onChange={(v) => set(k, v)} />
            </Row>
          ))}

          {/* Stability Cubes */}
          <div style={subSectionHeader}>
            <div style={{ ...subSectionLabel, margin: 0 }}>STABILITY CUBES</div>
            <button
              type="button"
              onClick={() => setAllTrue(stabilityCubeKeys)}
              disabled={!volLevEnabled}
              style={{
                background: 'transparent',
                border: '1px solid var(--line2)',
                color: 'var(--t2)',
                borderRadius: 2,
                fontSize: 9,
                padding: '2px 6px',
                cursor: volLevEnabled ? 'pointer' : 'not-allowed',
                opacity: volLevEnabled ? 1 : 0.55,
              }}
            >
              Enable All
            </button>
          </div>
          {!volLevEnabled && (
            <div style={{ fontSize: 9, color: 'var(--t3)', marginBottom: 6 }}>
              Requires <span style={{ color: 'var(--t2)' }}>enable_vol_lev_scaling</span>.
            </div>
          )}
          {stabilityCubeKeys.map((k) => (
            <Row key={k} label={k}>
              <Toggle checked={!!p[k]} onChange={(v) => set(k, v)} disabled={!volLevEnabled} />
            </Row>
          ))}

          {/* Robustness + Stress Tests */}
          <div style={subSectionHeader}>
            <div style={{ ...subSectionLabel, margin: 0 }}>ROBUSTNESS + STRESS TESTS</div>
            <button
              type="button"
              onClick={() => setAllTrue(robustnessKeys)}
              style={{ background: 'transparent', border: '1px solid var(--line2)', color: 'var(--t2)', borderRadius: 2, fontSize: 9, padding: '2px 6px', cursor: 'pointer' }}
            >
              Enable All
            </button>
          </div>
          {robustnessKeys.map((k) => (
            <Row key={k} label={k}>
              <Toggle checked={!!p[k]} onChange={(v) => set(k, v)} />
            </Row>
          ))}

          {/* Diagnostics */}
          <div style={subSectionHeader}>
            <div style={{ ...subSectionLabel, margin: 0 }}>DIAGNOSTICS</div>
            <button
              type="button"
              onClick={() => setAllTrue(diagnosticsKeys)}
              style={{ background: 'transparent', border: '1px solid var(--line2)', color: 'var(--t2)', borderRadius: 2, fontSize: 9, padding: '2px 6px', cursor: 'pointer' }}
            >
              Enable All
            </button>
          </div>
          {diagnosticsKeys.map((k) => (
            <Row key={k} label={k}>
              <Toggle checked={!!p[k]} onChange={(v) => set(k, v)} />
            </Row>
          ))}
        </div>
      </TierSection>

      {/* ── EXPERT TIER ── */}
      <TierSection title="EXPERT" color="var(--amber)" subtitle="simulation mechanics ⚠" defaultExpanded={false}>
        <div style={{ padding: '8px 12px' }}>
          {(
            [
              ['annualization_factor', 'number'],
              ['bar_minutes', 'number'],
            ] as [string, string][]
          ).map(([key]) => (
            <div key={key} style={row}>
              <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={fieldLabel}>{key}</span>
                <span style={{ fontSize: 8, color: 'var(--amber)', opacity: 0.7 }}>⚠ caution</span>
              </span>
              <NumInput value={p[key]} onChange={(v) => set(key, v)} />
            </div>
          ))}
          {(
            [
              ['end_cross_midnight', 'toggle'],
              ['save_daily_files', 'toggle'],
              ['build_master_file', 'toggle'],
            ] as [string, string][]
          ).map(([key]) => (
            <div key={key} style={row}>
              <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={fieldLabel}>{key}</span>
                <span style={{ fontSize: 8, color: 'var(--amber)', opacity: 0.7 }}>⚠ caution</span>
              </span>
              <Toggle checked={!!p[key]} onChange={(v) => set(key, v)} />
            </div>
          ))}
        </div>
      </TierSection>

      {/* ── RUN BUTTON ── */}
      <div style={{ position: 'sticky', bottom: 0, background: 'var(--bg1)', padding: '10px 12px', borderTop: '1px solid var(--line)' }}>
        <button
          onClick={() => onSubmit(params)}
          style={{
            width: '100%',
            height: 36,
            background: 'var(--green)',
            color: '#000',
            fontFamily: 'Space Mono, monospace',
            fontSize: 11,
            fontWeight: 700,
            borderRadius: 3,
            border: 'none',
            cursor: 'pointer',
            letterSpacing: '0.08em',
          }}
        >
          RUN AUDIT
        </button>
      </div>
    </div>
  );
}

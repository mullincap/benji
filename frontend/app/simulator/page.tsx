'use client';

import { useState, useEffect, useMemo, useRef } from 'react';
import Topbar from '../components/Topbar';
import ParamForm from '../components/LeftPanel/ParamForm';
import RunningParams from '../components/LeftPanel/RunningParams';
import ResultsSummary from '../components/LeftPanel/ResultsSummary';
import CanonicalCompareCard from './CanonicalCompareCard';
import EmptyState from '../components/RightPanel/EmptyState';
import RunningView from '../components/RightPanel/RunningView';
import ResultsView from '../components/RightPanel/ResultsView';
import AuditHistory from '../components/RightSidebar/AuditHistory';
import PromoteStrategyModal, { type PromoteFilterOption, type PromoteResult } from './PromoteStrategyModal';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || '';

// ─────────────────────────────────────────────
// Default parameters
// ─────────────────────────────────────────────
const DEFAULT_PARAMS: Record<string, unknown> = {
  // Strategy
  leaderboard_index: 100,
  sort_by: 'price',
  mode: 'snapshot',
  // D-medium-split (2026-04-23): individual ranking-metric knobs per metric
  // + BloFin universe filter. Replaces the earlier single ranking_metric
  // symmetric dropdown. Canonical defaults: pct_change on both, no BloFin.
  price_ranking_metric: 'pct_change',
  oi_ranking_metric: 'pct_change',
  apply_blofin_filter: false,
  // D-explore (2026-04-23): basket overlap dimensions knob. 'price_oi' is
  // canonical (price ∩ OI). Other values add volume as a third axis for
  // candidate-exploration variants — not promoted unless §5 gate passes.
  overlap_dimensions: 'price_oi',
  freq_width: 20,
  freq_cutoff: 20,
  sample_interval: 5,
  // Deployment window
  deployment_start_hour: 6,
  index_lookback: 6,
  sort_lookback: 6,
  deployment_runtime_hours: 'daily',
  end_cross_midnight: true,
  // Universe + Risk
  starting_capital: 100000,
  capital_mode: 'fixed',
  fixed_notional_cap: 'internal',
  pivot_leverage: 4.0,
  min_mcap: 0,
  max_mcap: 0,
  min_listing_age: 0,
  max_port: null,
  drop_unverified: false,
  leverage: 4.0,
  stop_raw_pct: -6.0,
  price_source: 'db',
  mcap_source: 'db',
  save_charts: true,
  trial_purchases: false,
  quick: false,
  taker_fee_pct: 0.0008,
  funding_rate_daily_pct: 0.0002,
  // Execution config
  early_kill_x: 5,
  early_kill_y: -999,
  early_instill_y: -999,
  l_base: 0.0,
  l_high: 1.0,
  port_tsl: 0.99,
  port_sl: -0.99,
  early_fill_y: 0.99,
  early_fill_x: 5,
  // Filters
  enable_tail_guardrail: true,
  run_filter_tail: false,
  enable_dispersion_filter: true,
  run_filter_dispersion: false,
  enable_tail_plus_disp: true,
  run_filter_tail_disp: false,
  enable_vol_filter: true,
  run_filter_vol: false,
  enable_tail_disp_vol: false,
  run_filter_tail_disp_vol: false,
  enable_tail_or_vol: false,
  run_filter_tail_or_vol: false,
  enable_tail_and_vol: false,
  run_filter_tail_and_vol: false,
  enable_blofin_filter: false,
  run_filter_tail_blofin: false,
  run_filter_none: true,
  enable_btc_ma_filter: false,
  enable_ic_diagnostic: false,
  enable_ic_filter: false,
  run_filter_calendar: false,
  // Advanced — strategy tuning
  dispersion_threshold: 0.66,
  dispersion_baseline_win: 33,
  dispersion_n: 40,
  dispersion_dynamic_universe: true,
  vol_lookback: 10,
  vol_percentile: 0.25,
  vol_baseline_win: 90,
  tail_drop_pct: 0.04,
  tail_vol_mult: 1.4,
  ic_signal: 'mom1d',
  ic_window: 30,
  ic_threshold: 0.02,
  btc_ma_days: 20,
  blofin_min_symbols: 1,
  leaderboard_top_n: 333,
  train_test_split: 0.60,
  n_trials: 3,
  // Leverage scaling
  enable_perf_lev_scaling: false,
  perf_lev_window: 10,
  perf_lev_sortino_target: 3.0,
  perf_lev_max_boost: 1.5,
  enable_vol_lev_scaling: false,
  vol_lev_window: 30,
  vol_lev_target_vol: 0.02,
  vol_lev_max_boost: 2.0,
  vol_lev_dd_threshold: -0.06,
  lev_quantization_mode: 'off',
  lev_quantization_step: 0.1,
  enable_contra_lev_scaling: false,
  contra_lev_window: 30,
  contra_lev_max_boost: 2.0,
  contra_lev_dd_threshold: -0.15,
  // Risk overlays
  enable_pph: false,
  pph_frequency: 'weekly',
  pph_threshold: 0.20,
  pph_harvest_frac: 0.50,
  pph_sweep_enabled: false,
  enable_ratchet: false,
  ratchet_frequency: 'weekly',
  ratchet_trigger: 0.20,
  ratchet_lock_pct: 0.15,
  ratchet_risk_off_lev_scale: 0.0,
  ratchet_sweep_enabled: false,
  enable_adaptive_ratchet: false,
  adaptive_ratchet_frequency: 'weekly',
  adaptive_ratchet_vol_window: 20,
  adaptive_ratchet_vol_low: 0.03,
  adaptive_ratchet_vol_high: 0.07,
  adaptive_ratchet_risk_off_scale: 0.0,
  adaptive_ratchet_floor_decay: 0.995,
  adaptive_ratchet_sweep_enabled: false,
  // Parameter sweeps
  enable_sweep_l_high: false,
  enable_sweep_tail_guardrail: false,
  enable_sweep_trail_wide: false,
  enable_sweep_trail_narrow: false,
  enable_param_surfaces: false,
  // Stability cubes
  enable_stability_cube: false,
  enable_risk_throttle_cube: false,
  enable_exit_cube: false,
  // Robustness
  enable_noise_stability: false,
  enable_slippage_sweep: false,
  enable_equity_ensemble: false,
  enable_param_jitter: false,
  enable_return_concentration: false,
  enable_sharpe_ridge_map: false,
  enable_sharpe_plateau: false,
  enable_top_n_removal: false,
  enable_lucky_streak: false,
  enable_periodic_breakdown: false,
  enable_weekly_milestones: false,
  enable_monthly_milestones: false,
  enable_dsr_mtl: false,
  enable_shock_injection: false,
  enable_ruin_probability: false,
  // Diagnostics
  enable_mcap_diagnostic: false,
  enable_capacity_curve: false,
  enable_regime_robustness: false,
  enable_min_cum_return: false,
  // Expert
  annualization_factor: 365,
  bar_minutes: 5,
  save_daily_files: false,
  build_master_file: true,
};

function nowHHMMSS(): string {
  const d = new Date();
  const h = String(d.getHours()).padStart(2, '0');
  const m = String(d.getMinutes()).padStart(2, '0');
  const s = String(d.getSeconds()).padStart(2, '0');
  return `${h}:${m}:${s}`;
}

const STAGE_LABELS: Record<string, string> = {
  overlap: 'overlap analysis',
  rebuild: 'portfolio matrix rebuild',
  audit: 'institutional audit',
  report: 'report generation',
};

interface AuditHistoryItem {
  id: string;
  display_name?: string | null;
  folder_id?: string | null;
  status: string;
  stage?: string | null;
  progress?: number;
  params?: Record<string, unknown>;
  error?: string | null;
  created_at?: number;
  updated_at?: number;
}

function extractPromoteFilters(results: Record<string, unknown> | null): PromoteFilterOption[] {
  if (!results) return [];
  const metrics = (results.metrics ?? {}) as Record<string, unknown>;
  const filters = (metrics.filters as Array<Record<string, unknown>> | undefined) ?? [];
  const out: PromoteFilterOption[] = [];
  for (const f of filters) {
    if (!f || typeof f !== 'object') continue;
    const name = f.filter;
    if (typeof name !== 'string' || !name) continue;
    const sharpe = typeof f.sharpe === 'number' ? f.sharpe : null;
    out.push({ filter: name, sharpe });
  }
  return out;
}

function extractBestFilter(results: Record<string, unknown> | null): string | null {
  if (!results) return null;
  const metrics = (results.metrics ?? {}) as Record<string, unknown>;
  const bf = metrics.best_filter;
  return typeof bf === 'string' && bf ? bf : null;
}

function PromoteBar({
  jobData,
  promoteToast,
  onOpen,
}: {
  jobData: Record<string, unknown> | null;
  promoteToast: { kind: 'ok' | 'err'; msg: string } | null;
  onOpen: () => void;
}) {
  const strategyVersionId = jobData?.strategy_version_id as string | null | undefined;
  const promotedAt = jobData?.promoted_at as string | null | undefined;
  const isPromoted = Boolean(strategyVersionId);

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        padding: '10px 14px',
        background: 'var(--bg1)',
        border: '1px solid var(--line)',
        borderRadius: 4,
        margin: '0 0 12px 0',
      }}
    >
      <span
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: '0.12em',
          color: 'var(--t3)',
          textTransform: 'uppercase',
        }}
      >
        Strategy Promotion
      </span>

      <div style={{ flex: 1, fontSize: 10, color: 'var(--t2)' }}>
        {isPromoted ? (
          <span>
            Published as strategy version{' '}
            <span style={{ color: 'var(--t0)' }}>{strategyVersionId}</span>
            {promotedAt ? ` on ${new Date(promotedAt).toLocaleString()}` : ''}.
          </span>
        ) : (
          <span>Admin: promote this audit as a strategy version for allocators to use.</span>
        )}
      </div>

      {promoteToast && (
        <span
          style={{
            fontSize: 10,
            color: promoteToast.kind === 'ok' ? 'var(--green)' : 'var(--red)',
            background: promoteToast.kind === 'ok' ? 'var(--green-dim)' : 'var(--red-dim)',
            border: `1px solid ${promoteToast.kind === 'ok' ? 'var(--green)' : 'var(--red)'}`,
            borderRadius: 3,
            padding: '3px 8px',
          }}
        >
          {promoteToast.msg}
        </span>
      )}

      {isPromoted ? (
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: '0.12em',
            textTransform: 'uppercase',
            color: 'var(--green)',
            background: 'var(--green-dim)',
            border: '1px solid var(--green)',
            borderRadius: 3,
            padding: '4px 10px',
          }}
        >
          Already Promoted
        </span>
      ) : (
        <button
          type="button"
          onClick={onOpen}
          style={{
            background: 'var(--green-dim)',
            border: '1px solid var(--green)',
            color: 'var(--green)',
            borderRadius: 4,
            padding: '6px 14px',
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: '0.12em',
            textTransform: 'uppercase',
            fontFamily: 'var(--font-space-mono), Space Mono, monospace',
            cursor: 'pointer',
          }}
        >
          Promote as Strategy
        </button>
      )}
    </div>
  );
}

export default function Home() {
  const [appState, setAppState] = useState<'idle' | 'running' | 'results' | 'failed'>('idle');
  const [jobId, setJobId] = useState<string | null>(null);
  const [jobData, setJobData] = useState<Record<string, unknown> | null>(null);
  const [results, setResults] = useState<Record<string, unknown> | null>(null);
  // 1s client clock used to derive elapsedSeconds from jobData.created_at
  // below. We don't track elapsed imperatively any more — the old approach
  // (manual interval + setElapsedSeconds calls on every transition) drifted
  // when the user navigated between audits because the interval only got
  // started inside handleSubmit, never inside handleSelectAudit for a
  // running job. Derived state is always correct on mount.
  const [clockTick, setClockTick] = useState(() => Date.now());
  const [logLines, setLogLines] = useState<string[]>([]);
  const [errorInfo, setErrorInfo] = useState<{ message: string; jobId: string | null } | null>(null);
  const [params, setParams] = useState<Record<string, unknown>>(DEFAULT_PARAMS);
  const [historyCollapsed, setHistoryCollapsed] = useState(false);
  const [leftCollapsed, setLeftCollapsed] = useState(false);
  const [auditHistory, setAuditHistory] = useState<AuditHistoryItem[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [deletingJobId, setDeletingJobId] = useState<string | null>(null);
  const [renamingJobId, setRenamingJobId] = useState<string | null>(null);
  const [cancellingAudit, setCancellingAudit] = useState(false);
  const [editingFromResults, setEditingFromResults] = useState(false);
  const [collapseAuditConfigsSignal, setCollapseAuditConfigsSignal] = useState(0);
  const [isAdmin, setIsAdmin] = useState(false);
  const [promoteOpen, setPromoteOpen] = useState(false);
  const [promoteToast, setPromoteToast] = useState<{ kind: 'ok' | 'err'; msg: string } | null>(null);

  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const lastStageRef = useRef<string | null>(null);

  // Single mount-level clock: ticks once a second so running-job elapsed
  // values re-render live. Terminal jobs ignore the tick (memoized).
  useEffect(() => {
    const id = setInterval(() => setClockTick(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/admin/whoami`, { credentials: 'include' });
        if (!cancelled) setIsAdmin(res.ok);
      } catch {
        if (!cancelled) setIsAdmin(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (!promoteToast) return;
    const id = setTimeout(() => setPromoteToast(null), 6000);
    return () => clearTimeout(id);
  }, [promoteToast]);

  // Elapsed is derived from jobData.created_at vs either now() (running) or
  // updated_at (terminal). Navigation between audits therefore always shows
  // the correct elapsed without needing to manage timers across transitions.
  const elapsedSeconds = useMemo(() => {
    if (!jobData) return 0;
    const createdRaw = jobData.created_at as number | string | undefined;
    const updatedRaw = jobData.updated_at as number | string | undefined;
    if (createdRaw === undefined) return 0;
    const createdMs = Number(createdRaw) * 1000;
    const updatedMs = updatedRaw !== undefined ? Number(updatedRaw) * 1000 : clockTick;
    const status = String(jobData.status || '').toLowerCase();
    const TERMINAL = ['complete', 'completed', 'done', 'failed', 'error', 'cancelled', 'canceled'];
    const refMs = TERMINAL.includes(status) ? updatedMs : clockTick;
    return Math.max(0, Math.round((refMs - createdMs) / 1000));
  }, [jobData, clockTick]);

  // Stop polling on unmount
  useEffect(() => {
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, []);

  async function loadAuditHistory(background = false) {
    if (!background) setHistoryLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/jobs`);
      if (!res.ok) throw new Error(`GET /api/jobs failed: ${res.status}`);
      const data = (await res.json()) as AuditHistoryItem[];
      setAuditHistory(data);
      setHistoryError(null);
    } catch (err) {
      setHistoryError(String(err));
    } finally {
      if (!background) setHistoryLoading(false);
    }
  }

  const initialLoadDone = useRef(false);
  useEffect(() => {
    async function initialLoad() {
      await loadAuditHistory(false);
    }
    initialLoad();
    const interval = setInterval(() => {
      loadAuditHistory(true);
    }, 10000);
    return () => clearInterval(interval);
  }, []);

  // Auto-select the most recent completed audit on first load
  useEffect(() => {
    if (initialLoadDone.current) return;
    if (auditHistory.length === 0) return;
    const latest = auditHistory.find(
      (j) => ['complete', 'completed', 'done'].includes(String(j.status || '').toLowerCase())
    );
    if (latest && appState === 'idle') {
      initialLoadDone.current = true;
      handleSelectAudit(latest);
    }
  }, [auditHistory]);

  async function handleDeleteAudit(job: AuditHistoryItem) {
    const ok = window.confirm(`Delete audit ${job.id.slice(0, 8)}? This cannot be undone.`);
    if (!ok) return;
    setDeletingJobId(job.id);
    try {
      const res = await fetch(`${API_BASE}/api/jobs/${job.id}`, { method: 'DELETE' });
      if (!res.ok) throw new Error(`DELETE /api/jobs/${job.id} failed: ${res.status}`);
      setAuditHistory((prev) => prev.filter((j) => j.id !== job.id));
      if (jobId === job.id) {
        handleResetToIdle();
      }
    } catch (err) {
      setHistoryError(String(err));
    } finally {
      setDeletingJobId(null);
    }
  }

  async function handleCancelAudit() {
    if (!jobId || cancellingAudit) return;
    const ok = window.confirm('Cancel the currently running audit?');
    if (!ok) return;
    setCancellingAudit(true);
    try {
      const res = await fetch(`${API_BASE}/api/jobs/${jobId}/cancel`, { method: 'POST' });
      if (!res.ok) throw new Error(`POST /api/jobs/${jobId}/cancel failed: ${res.status}`);
      stopPolling();
      appendLog(`[${nowHHMMSS()}] audit cancelled by user`);
      setErrorInfo({ message: 'Cancelled by user.', jobId });
      setAppState('failed');
      loadAuditHistory(true);
    } catch (err) {
      appendLog(`[${nowHHMMSS()}] cancel error: ${String(err)}`);
      setErrorInfo({ message: String(err), jobId });
      setAppState('failed');
    } finally {
      setCancellingAudit(false);
    }
  }

  async function handleSelectAudit(job: AuditHistoryItem) {
    stopPolling();
    setEditingFromResults(false);
    setJobId(job.id);
    setJobData(job as unknown as Record<string, unknown>);
    setErrorInfo(null);
    // Merge saved params over DEFAULT_PARAMS so fields absent from older
    // audits (e.g. nightly cron's sparse params missing mcap_source) get a
    // defined form value instead of staying undefined. Without this, the
    // <select> renders as the first option visually but the underlying
    // state stays undefined, so submitting that audit silently omits the
    // field and the backend's Pydantic default (e.g. mcap_source='db')
    // applies — invisibly diverging from what the user thought they sent.
    // Discovered 2026-04-25 trying to re-run the ALTS MAIN nightly with
    // mcap_source='parquet' explicitly: dropdown showed 'parquet' but
    // submission carried no mcap_source field → backend defaulted to 'db'.
    setParams({ ...DEFAULT_PARAMS, ...((job.params as Record<string, unknown>) ?? {}) });

    const status = String(job.status || '').toLowerCase();
    if (status === 'complete' || status === 'completed' || status === 'done') {
      try {
        const rRes = await fetch(`${API_BASE}/api/jobs/${job.id}/results`);
        if (!rRes.ok) throw new Error(`GET /api/jobs/${job.id}/results failed: ${rRes.status}`);
        const rData = await rRes.json();
        setResults(rData);
        setAppState('results');
        // elapsedSeconds is derived from jobData (created_at / updated_at)
        // and updates automatically when setJobData above lands.
      } catch (err) {
        setErrorInfo({ message: String(err), jobId: job.id });
        setAppState('failed');
      }
      return;
    }

    if (status === 'failed' || status === 'error') {
      setResults(null);
      setErrorInfo({ message: String(job.error ?? 'Job failed'), jobId: job.id });
      setAppState('failed');
      return;
    }

    setResults(null);
    setAppState('running');
  }

  async function handleRenameAudit(job: AuditHistoryItem, displayName: string) {
    setRenamingJobId(job.id);
    try {
      const payload = { display_name: displayName.length > 0 ? displayName : null };
      let res = await fetch(`${API_BASE}/api/jobs/${job.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      // Backward-compatible fallback for servers that do not allow PATCH on /api/jobs/{id}.
      if (res.status === 405) {
        res = await fetch(`${API_BASE}/api/jobs/${job.id}/rename`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
      }
      if (!res.ok) throw new Error(`PATCH /api/jobs/${job.id} failed: ${res.status}`);
      const updated = (await res.json()) as AuditHistoryItem;
      setAuditHistory((prev) => prev.map((j) => (j.id === updated.id ? { ...j, display_name: updated.display_name ?? null } : j)));
      if (jobId === updated.id) {
        setJobData((prev) => (prev ? { ...prev, display_name: updated.display_name ?? null } : prev));
      }
      setHistoryError(null);
    } catch (err) {
      setHistoryError(String(err));
    } finally {
      setRenamingJobId(null);
    }
  }

  function stopPolling() {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  }

  function appendLog(line: string) {
    setLogLines((prev) => [...prev, line]);
  }

  async function handleSubmit(submittedParams: Record<string, unknown>) {
    setEditingFromResults(false);
    setAppState('running');
    setLogLines([]);
    lastStageRef.current = null;
    setJobData(null);
    setResults(null);

    try {
      const res = await fetch(`${API_BASE}/api/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(submittedParams),
      });
      if (!res.ok) throw new Error(`POST /api/jobs failed: ${res.status}`);
      const data = await res.json();
      const id: string = data.job_id ?? data.id;
      setJobId(id);
      appendLog(`[${nowHHMMSS()}] job submitted — id: ${id}`);

      // Polling
      pollingRef.current = setInterval(async () => {
        try {
          const poll = await fetch(`${API_BASE}/api/jobs/${id}`);
          if (!poll.ok) return;
          const jobJson = await poll.json();
          setJobData(jobJson);

          // Append log on stage change
          const stage = jobJson.stage as string | null;
          if (stage && stage !== lastStageRef.current) {
            lastStageRef.current = stage;
            appendLog(`[${nowHHMMSS()}] stage → ${STAGE_LABELS[stage] ?? stage}`);
          }

          const status = jobJson.status as string;
          if (status === 'complete' || status === 'completed' || status === 'done') {
            stopPolling();
            appendLog(`[${nowHHMMSS()}] audit complete — fetching results`);
            const rRes = await fetch(`${API_BASE}/api/jobs/${id}/results`);
            if (rRes.ok) {
              const rData = await rRes.json();
              setResults(rData);
              appendLog(`[${nowHHMMSS()}] results loaded`);
            }
            setAppState('results');
            loadAuditHistory(true);
          } else if (status === 'failed' || status === 'error') {
            stopPolling();
            const errMsg = jobJson.error ? String(jobJson.error) : 'Unknown error';
            appendLog(`[${nowHHMMSS()}] job failed: ${errMsg}`);
            setErrorInfo({ message: errMsg, jobId: id });
            setAppState('failed');
          } else if (status === 'cancelled' || status === 'canceled') {
            stopPolling();
            const errMsg = jobJson.error ? String(jobJson.error) : 'Cancelled by user.';
            appendLog(`[${nowHHMMSS()}] job cancelled: ${errMsg}`);
            setErrorInfo({ message: errMsg, jobId: id });
            setAppState('failed');
          }
        } catch (err) {
          appendLog(`[${nowHHMMSS()}] poll error: ${String(err)}`);
        }
      }, 2000);
    } catch (err) {
      const errMsg = String(err);
      appendLog(`[${nowHHMMSS()}] error: ${errMsg}`);
      setErrorInfo({ message: errMsg, jobId: null });
      setAppState('failed');
    }
  }

  function handleEditFromResults() {
    setCollapseAuditConfigsSignal((n) => n + 1);
    setEditingFromResults(true);
    setErrorInfo(null);
    setAppState('results');
  }

  function handleResetToIdle() {
    stopPolling();
    setAppState('idle');
    setJobId(null);
    setJobData(null);
    setResults(null);
    setErrorInfo(null);
    setLogLines([]);
    lastStageRef.current = null;
    setEditingFromResults(false);
    setCollapseAuditConfigsSignal(0);
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', background: 'var(--bg0)' }}>
      <Topbar />

      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          {/* Left panel */}
          <div
            style={{
              width: leftCollapsed ? 38 : 288,
              borderRight: '1px solid var(--line)',
              overflow: 'hidden',
              flexShrink: 0,
              transition: 'width 0.2s ease',
              background: 'var(--bg0)',
              display: 'flex',
              flexDirection: 'column',
            }}
          >
            <div
              style={{
                height: 40,
                borderBottom: '1px solid var(--line)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: leftCollapsed ? 'center' : 'space-between',
                padding: leftCollapsed ? 0 : '0 8px 0 16px',
                flexShrink: 0,
              }}
            >
              {!leftCollapsed && (
                <span
                  style={{
                    fontSize: 9,
                    textTransform: 'uppercase',
                    letterSpacing: '0.12em',
                    color: 'var(--t3)',
                    fontWeight: 700,
                  }}
                >
                  Audit Panel
                </span>
              )}
              <button
                onClick={() => setLeftCollapsed((v) => !v)}
                title={leftCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                style={{
                  width: 24,
                  height: 24,
                  border: '1px solid var(--line2)',
                  borderRadius: 3,
                  background: 'transparent',
                  color: 'var(--t1)',
                  fontSize: 11,
                  cursor: 'pointer',
                }}
              >
                {leftCollapsed ? '»' : '«'}
              </button>
            </div>

            {!leftCollapsed && (
              <div
                style={{
                  flex: 1,
                  overflowY: 'auto',
                  opacity: appState === 'running' ? 0.4 : 1,
                  pointerEvents: appState === 'running' ? 'none' : 'auto',
                  transition: 'opacity 0.3s',
                }}
              >
                {appState === 'idle' && (
                  <ParamForm params={params} onChange={setParams} onSubmit={handleSubmit} />
                )}
                {appState === 'running' && <RunningParams params={params} />}
                {appState === 'results' && (
                  <>
                    <ResultsSummary
                      key={`summary-${jobId ?? 'none'}-${collapseAuditConfigsSignal}`}
                      results={results}
                      params={params}
                      onRerun={handleEditFromResults}
                      startAuditConfigsCollapsed={editingFromResults}
                      hideActionBar={editingFromResults}
                    />
                    {!editingFromResults && (
                      <CanonicalCompareCard results={results} params={params} />
                    )}
                    {editingFromResults && (
                      <div style={{ borderTop: '1px solid var(--line)' }}>
                        <ParamForm params={params} onChange={setParams} onSubmit={handleSubmit} />
                      </div>
                    )}
                  </>
                )}
                {appState === 'failed' && (
                  <div style={{ padding: 16 }}>
                    <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 12 }}>JOB FAILED</div>
                    <div style={{ fontSize: 10, color: 'var(--red)', background: 'var(--red-dim)', border: '1px solid var(--red)', borderRadius: 3, padding: '8px 10px', marginBottom: 12, wordBreak: 'break-word', lineHeight: 1.6 }}>
                      {errorInfo?.message ?? 'Unknown error'}
                    </div>
                    {errorInfo?.jobId && (
                      <div style={{ fontSize: 9, color: 'var(--t3)', marginBottom: 12, wordBreak: 'break-all' }}>
                        job: {errorInfo.jobId}
                      </div>
                    )}
                    <button
                      onClick={handleResetToIdle}
                      style={{ width: '100%', height: 32, background: 'transparent', border: '1px solid var(--line2)', borderRadius: 3, color: 'var(--t1)', fontFamily: 'Space Mono, monospace', fontSize: 10, cursor: 'pointer' }}
                    >
                      EDIT &amp; RETRY
                    </button>
                  </div>
                )}
              </div>
            )}
            {leftCollapsed && (
              <div
                style={{
                  flex: 1,
                  display: 'flex',
                  alignItems: 'flex-end',
                  justifyContent: 'center',
                  paddingBottom: 8,
                  pointerEvents: 'none',
                }}
              >
                <div style={{ fontSize: 8, color: 'var(--t3)', transform: 'rotate(-90deg)', whiteSpace: 'nowrap', letterSpacing: '0.08em' }}>
                  SIDEBAR
                </div>
              </div>
            )}
          </div>

          {/* Right panel */}
          <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
            {appState === 'idle' && <EmptyState />}
            {appState === 'running' && (
              <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
                <div
                  style={{
                    height: 40,
                    borderBottom: '1px solid var(--line)',
                    display: 'flex',
                    alignItems: 'center',
                    padding: '0 10px',
                    flexShrink: 0,
                  }}
                >
                  <span
                    style={{
                      fontSize: 9,
                      textTransform: 'uppercase',
                      letterSpacing: '0.12em',
                      color: 'var(--t3)',
                      fontWeight: 700,
                    }}
                  >
                    Audit Engine
                  </span>
                </div>
                <div style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}>
                  <RunningView
                    jobId={jobId}
                    jobData={jobData}
                    logLines={logLines}
                    elapsedSeconds={elapsedSeconds}
                    onCancel={handleCancelAudit}
                    cancelling={cancellingAudit}
                  />
                </div>
              </div>
            )}
            {appState === 'results' && (
              <div
                style={{
                  flex: 1,
                  width: '100%',
                  minWidth: 0,
                  overflowY: 'auto',
                  scrollbarGutter: 'stable both-edges',
                }}
              >
                {isAdmin && jobId && (
                  <PromoteBar
                    jobData={jobData}
                    promoteToast={promoteToast}
                    onOpen={() => setPromoteOpen(true)}
                  />
                )}
                <ResultsView
                  key={jobId ?? 'results-none'}
                  results={results}
                  jobId={jobId}
                  startingCapital={typeof params.starting_capital === 'number' ? params.starting_capital : Number(params.starting_capital ?? 100000)}
                  params={params}
                />
              </div>
            )}
            {promoteOpen && jobId && (
              <PromoteStrategyModal
                jobId={jobId}
                filters={extractPromoteFilters(results)}
                defaultFilter={extractBestFilter(results)}
                onCancel={() => setPromoteOpen(false)}
                onSuccess={(result: PromoteResult) => {
                  setPromoteOpen(false);
                  setPromoteToast({
                    kind: 'ok',
                    msg: `Promoted as "${result.strategy_name}" (${result.version_label}).`,
                  });
                  setJobData((prev) => (prev ? {
                    ...prev,
                    strategy_version_id: result.strategy_version_id,
                    promoted_at: new Date().toISOString(),
                  } : prev));
                  loadAuditHistory(true);
                }}
              />
            )}
            {appState === 'failed' && (
              <RunningView
                jobId={jobId}
                jobData={jobData}
                logLines={logLines}
                elapsedSeconds={elapsedSeconds}
                onCancel={handleCancelAudit}
                cancelling={cancellingAudit}
              />
            )}
          </div>

          <AuditHistory
            collapsed={historyCollapsed}
            jobs={auditHistory}
            selectedJobId={jobId}
            loading={historyLoading}
            error={historyError}
            deletingJobId={deletingJobId}
            renamingJobId={renamingJobId}
            onToggle={() => setHistoryCollapsed((v) => !v)}
            onSelect={handleSelectAudit}
            onDelete={handleDeleteAudit}
            onRename={handleRenameAudit}
            onJobsChanged={() => loadAuditHistory(true)}
          />
        </div>
      </div>
    </div>
  );
}

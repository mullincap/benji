'use client';

import { useState } from 'react';

interface AuditHistoryItem {
  id: string;
  status: string;
  stage?: string | null;
  created_at?: number;
  updated_at?: number;
  params?: Record<string, unknown>;
  results?: Record<string, unknown> | null;
}

type FilterMetricRow = {
  filter?: string;
  sharpe?: number | null;
  grade_score?: number | null;
  not_run?: boolean;
  [key: string]: unknown;
};

interface AuditHistoryProps {
  collapsed: boolean;
  jobs: AuditHistoryItem[];
  selectedJobId: string | null;
  loading: boolean;
  error: string | null;
  deletingJobId: string | null;
  onToggle: () => void;
  onSelect: (job: AuditHistoryItem) => void;
  onDelete: (job: AuditHistoryItem) => void;
}

function fmtWhen(ts?: number): string {
  if (!ts || Number.isNaN(ts)) return '—';
  return new Date(ts * 1000).toLocaleString(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function normalizeStatus(status: string): string {
  const s = status.toLowerCase();
  if (s === 'complete' || s === 'completed' || s === 'done') return 'complete';
  if (s === 'failed' || s === 'error') return 'failed';
  if (s === 'running') return 'running';
  return s;
}

function statusColor(status: string): string {
  const s = normalizeStatus(status);
  if (s === 'complete') return 'var(--t1)';
  if (s === 'failed') return 'var(--red)';
  if (s === 'running') return 'var(--amber)';
  return 'var(--t2)';
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

function pickBestSharpe(results: Record<string, unknown> | null | undefined): number | null {
  const metrics = (results?.metrics ?? {}) as Record<string, unknown>;
  const rows = [
    ...(((metrics.filters as FilterMetricRow[] | undefined) ?? [])),
    ...(((metrics.filter_comparison as FilterMetricRow[] | undefined) ?? [])),
  ].filter((r) => r && r.filter && !r.not_run);
  if (rows.length > 0) {
    const bestFilter = String(metrics.best_filter ?? '').trim();
    if (bestFilter) {
      const target = normalizeFilterLabel(bestFilter);
      const exact = rows.find((r) => normalizeFilterLabel(String(r.filter ?? '')) === target);
      if (exact && typeof exact.sharpe === 'number') return exact.sharpe;
    }
    const ranked = [...rows].sort((a, b) => {
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
    });
    const top = ranked[0];
    if (top && typeof top.sharpe === 'number') return top.sharpe;
  }

  const legacy = asNum(metrics.sharpe);
  return legacy;
}

export default function AuditHistory({
  collapsed,
  jobs,
  selectedJobId,
  loading,
  error,
  deletingJobId,
  onToggle,
  onSelect,
  onDelete,
}: AuditHistoryProps) {
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null);

  return (
    <div
      style={{
        width: collapsed ? 40 : 300,
        borderLeft: '1px solid var(--line)',
        background: 'var(--bg0)',
        display: 'flex',
        flexDirection: 'column',
        flexShrink: 0,
        transition: 'width 0.2s ease',
        overflow: 'hidden',
      }}
    >
      <div
        style={{
          height: 40,
          borderBottom: '1px solid var(--line)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: collapsed ? 'center' : 'space-between',
          padding: collapsed ? 0 : '0 10px',
        }}
      >
        {!collapsed && (
          <span
            style={{
              fontSize: 9,
              textTransform: 'uppercase',
              letterSpacing: '0.12em',
              color: 'var(--t3)',
              fontWeight: 700,
            }}
          >
            Audit History
          </span>
        )}
        <button
          onClick={onToggle}
          title={collapsed ? 'Expand history' : 'Collapse history'}
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
          {collapsed ? '«' : '»'}
        </button>
      </div>

      {!collapsed && (
        <div style={{ flex: 1, overflowY: 'auto', padding: 8 }}>
          {loading && <div style={{ fontSize: 10, color: 'var(--t2)', padding: 6 }}>Refreshing audits...</div>}
          {error && (
            <div style={{ fontSize: 10, color: 'var(--red)', padding: 6, border: '1px solid var(--red)', borderRadius: 3 }}>
              {error}
            </div>
          )}
          {!error && jobs.length === 0 && (
            <div style={{ fontSize: 10, color: 'var(--t2)', padding: 6 }}>No audits found.</div>
          )}
          {!error &&
            jobs.map((job) => {
              const selected = selectedJobId === job.id;
              const mode = String(job.params?.mode ?? '—');
              const sortBy = String(job.params?.sort_by ?? '—');
              const bestSharpe = pickBestSharpe(job.results as Record<string, unknown> | null | undefined);
              const sharpeVal = bestSharpe !== null ? bestSharpe.toFixed(3) : '—';
              const deleting = deletingJobId === job.id;
              return (
                <div
                  key={job.id}
                  onClick={() => onSelect(job)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      onSelect(job);
                    }
                  }}
                  role="button"
                  tabIndex={0}
                  style={{
                    width: '100%',
                    textAlign: 'left',
                    marginBottom: 8,
                    padding: 8,
                    borderRadius: 3,
                    border: `1px solid ${selected ? 'rgba(255, 255, 255, 0.45)' : 'var(--line2)'}`,
                    background: selected ? 'rgba(255, 255, 255, 0.06)' : 'var(--bg2)',
                    cursor: 'pointer',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', marginBottom: 6, gap: 6 }}>
                    <span style={{ fontSize: 10, color: statusColor(job.status), textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                      {normalizeStatus(job.status)}
                    </span>
                    <span style={{ fontSize: 9, color: 'var(--t2)', marginLeft: 'auto', fontFamily: 'Space Mono, monospace' }}>
                      {job.id.slice(0, 8)}
                    </span>
                    {pendingDeleteId === job.id ? (
                      <>
                        <span
                          role="button"
                          tabIndex={0}
                          onClick={(e) => {
                            e.stopPropagation();
                            setPendingDeleteId(null);
                          }}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                              e.preventDefault();
                              e.stopPropagation();
                              setPendingDeleteId(null);
                            }
                          }}
                          title="Cancel delete"
                          style={{
                            height: 18,
                            padding: '0 6px',
                            borderRadius: 2,
                            border: '1px solid var(--line2)',
                            background: 'var(--bg1)',
                            color: 'var(--t1)',
                            fontSize: 9,
                            cursor: 'pointer',
                            display: 'inline-flex',
                            alignItems: 'center',
                          }}
                        >
                          CANCEL
                        </span>
                        <span
                          role="button"
                          tabIndex={deleting ? -1 : 0}
                          onClick={(e) => {
                            e.stopPropagation();
                            if (deleting) return;
                            onDelete(job);
                            setPendingDeleteId(null);
                          }}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter' || e.key === ' ') {
                              e.preventDefault();
                              e.stopPropagation();
                              if (deleting) return;
                              onDelete(job);
                              setPendingDeleteId(null);
                            }
                          }}
                          title="Confirm delete"
                          aria-disabled={deleting}
                          style={{
                            height: 18,
                            padding: '0 6px',
                            borderRadius: 2,
                            border: '1px solid var(--line2)',
                            background: 'var(--bg1)',
                            color: deleting ? 'var(--t2)' : 'var(--t1)',
                            fontSize: 9,
                            cursor: deleting ? 'not-allowed' : 'pointer',
                            transition: 'color 0.15s ease',
                            display: 'inline-flex',
                            alignItems: 'center',
                          }}
                          onMouseEnter={(e) => {
                            if (!deleting) (e.currentTarget.style.color = 'var(--red)');
                          }}
                          onMouseLeave={(e) => {
                            if (!deleting) (e.currentTarget.style.color = 'var(--t1)');
                          }}
                        >
                          {deleting ? '…' : 'DELETE'}
                        </span>
                      </>
                    ) : (
                      <span
                        role="button"
                        tabIndex={deleting ? -1 : 0}
                        onClick={(e) => {
                          e.stopPropagation();
                          if (deleting) return;
                          setPendingDeleteId(job.id);
                        }}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault();
                            e.stopPropagation();
                            if (deleting) return;
                            setPendingDeleteId(job.id);
                          }
                        }}
                        title="Delete audit"
                        aria-disabled={deleting}
                        style={{
                          height: 18,
                          padding: '0 6px',
                          borderRadius: 2,
                          border: '1px solid var(--line2)',
                          background: 'var(--bg1)',
                          color: deleting ? 'var(--t2)' : 'var(--t1)',
                          fontSize: 9,
                          cursor: deleting ? 'not-allowed' : 'pointer',
                          transition: 'color 0.15s ease',
                          display: 'inline-flex',
                          alignItems: 'center',
                        }}
                        onMouseEnter={(e) => {
                          if (!deleting) (e.currentTarget.style.color = 'var(--red)');
                        }}
                        onMouseLeave={(e) => {
                          if (!deleting) (e.currentTarget.style.color = 'var(--t1)');
                        }}
                      >
                        {deleting ? '…' : 'DEL'}
                      </span>
                    )}
                  </div>
                  <div style={{ fontSize: 9, color: 'var(--t1)', marginBottom: 4 }}>
                    mode: {mode} · sort: {sortBy}
                  </div>
                  <div style={{ fontSize: 9, color: 'var(--t1)', marginBottom: 4 }}>
                    sharpe: {sharpeVal}
                  </div>
                  <div style={{ fontSize: 9, color: 'var(--t2)' }}>updated: {fmtWhen(job.updated_at)}</div>
                </div>
              );
            })}
        </div>
      )}
    </div>
  );
}

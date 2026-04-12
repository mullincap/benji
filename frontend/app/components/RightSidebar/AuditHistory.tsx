'use client';

import { useState, useEffect } from 'react';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || '';

interface AuditHistoryItem {
  id: string;
  display_name?: string | null;
  folder_id?: string | null;
  status: string;
  stage?: string | null;
  created_at?: number;
  updated_at?: number;
  params?: Record<string, unknown>;
  results?: Record<string, unknown> | null;
}

interface Folder {
  id: string;
  name: string;
  created_at: number;
  position: number;
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
  renamingJobId: string | null;
  onToggle: () => void;
  onSelect: (job: AuditHistoryItem) => void;
  onDelete: (job: AuditHistoryItem) => void;
  onRename: (job: AuditHistoryItem, displayName: string) => void | Promise<void>;
  onJobsChanged?: () => void;
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

const btnStyle: React.CSSProperties = {
  height: 18,
  padding: '0 6px',
  borderRadius: 2,
  border: '1px solid var(--line2)',
  background: 'var(--bg1)',
  fontSize: 9,
  fontFamily: 'Space Mono, monospace',
  cursor: 'pointer',
  display: 'inline-flex',
  alignItems: 'center',
};

export default function AuditHistory({
  collapsed,
  jobs,
  selectedJobId,
  loading,
  error,
  deletingJobId,
  renamingJobId,
  onToggle,
  onSelect,
  onDelete,
  onRename,
  onJobsChanged,
}: AuditHistoryProps) {
  const [pendingDeleteId, setPendingDeleteId] = useState<string | null>(null);
  const [editingRenameId, setEditingRenameId] = useState<string | null>(null);
  const [renameDraft, setRenameDraft] = useState('');

  // Folder state
  const [folders, setFolders] = useState<Folder[]>([]);
  const [activeFolderId, setActiveFolderId] = useState<string | null>(null); // null = "All"
  const [creatingFolder, setCreatingFolder] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');
  const [editingFolderId, setEditingFolderId] = useState<string | null>(null);
  const [editFolderName, setEditFolderName] = useState('');
  const [movingJobId, setMovingJobId] = useState<string | null>(null);

  useEffect(() => {
    fetchFolders();
  }, []);

  async function fetchFolders() {
    try {
      const res = await fetch(`${API_BASE}/api/jobs/folders/list`);
      if (res.ok) {
        const data = await res.json();
        setFolders(data);
      }
    } catch { /* ignore */ }
  }

  async function handleCreateFolder() {
    const name = newFolderName.trim();
    if (!name) return;
    try {
      const res = await fetch(`${API_BASE}/api/jobs/folders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
      if (res.ok) {
        const folder = await res.json();
        setFolders((prev) => [...prev, folder]);
        setActiveFolderId(folder.id);
      }
    } catch { /* ignore */ }
    setCreatingFolder(false);
    setNewFolderName('');
  }

  async function handleRenameFolder(folderId: string) {
    const name = editFolderName.trim();
    if (!name) return;
    try {
      const res = await fetch(`${API_BASE}/api/jobs/folders/${folderId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
      if (res.ok) {
        const updated = await res.json();
        setFolders((prev) => prev.map((f) => f.id === folderId ? updated : f));
      }
    } catch { /* ignore */ }
    setEditingFolderId(null);
    setEditFolderName('');
  }

  async function handleDeleteFolder(folderId: string) {
    try {
      await fetch(`${API_BASE}/api/jobs/folders/${folderId}`, { method: 'DELETE' });
      setFolders((prev) => prev.filter((f) => f.id !== folderId));
      if (activeFolderId === folderId) setActiveFolderId(null);
      onJobsChanged?.();
    } catch { /* ignore */ }
  }

  async function handleMoveJob(jobId: string, folderId: string | null) {
    try {
      await fetch(`${API_BASE}/api/jobs/${jobId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ folder_id: folderId }),
      });
      onJobsChanged?.();
    } catch { /* ignore */ }
    setMovingJobId(null);
  }

  // Filter jobs by active folder
  const filteredJobs = activeFolderId === null
    ? jobs
    : jobs.filter((j) => j.folder_id === activeFolderId);

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
      {/* Header */}
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
          <span style={{
            fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.12em',
            color: 'var(--t3)', fontWeight: 700,
          }}>
            Audit History
          </span>
        )}
        <button
          onClick={onToggle}
          title={collapsed ? 'Expand history' : 'Collapse history'}
          style={{
            width: 24, height: 24,
            border: '1px solid var(--line2)', borderRadius: 3,
            background: 'transparent', color: 'var(--t1)',
            fontSize: 11, cursor: 'pointer',
          }}
        >
          {collapsed ? '«' : '»'}
        </button>
      </div>

      {!collapsed && (
        <>
          {/* Folder tabs */}
          <div style={{
            borderBottom: '1px solid var(--line)',
            padding: '6px 8px',
            display: 'flex',
            flexDirection: 'column',
            gap: 4,
          }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
              <button
                onClick={() => setActiveFolderId(null)}
                style={{
                  ...btnStyle,
                  background: activeFolderId === null ? 'var(--bg4)' : 'var(--bg1)',
                  color: activeFolderId === null ? 'var(--t0)' : 'var(--t3)',
                  fontWeight: 700,
                  letterSpacing: '0.06em',
                }}
              >
                ALL
              </button>
              {folders.map((f) => (
                <button
                  key={f.id}
                  onClick={() => setActiveFolderId(f.id)}
                  onContextMenu={(e) => {
                    e.preventDefault();
                    setEditingFolderId(f.id);
                    setEditFolderName(f.name);
                  }}
                  title="Click to filter · Right-click to edit"
                  style={{
                    ...btnStyle,
                    background: activeFolderId === f.id ? 'var(--bg4)' : 'var(--bg1)',
                    color: activeFolderId === f.id ? 'var(--t0)' : 'var(--t3)',
                    fontWeight: 700,
                    letterSpacing: '0.06em',
                    maxWidth: 120,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {f.name}
                </button>
              ))}
              <button
                onClick={() => setCreatingFolder(true)}
                style={{
                  ...btnStyle,
                  color: 'var(--t3)',
                  fontSize: 11,
                }}
                title="New folder"
              >
                +
              </button>
            </div>

            {/* New folder input */}
            {creatingFolder && (
              <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
                <input
                  autoFocus
                  value={newFolderName}
                  onChange={(e) => setNewFolderName(e.target.value)}
                  placeholder="Folder name"
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleCreateFolder();
                    if (e.key === 'Escape') { setCreatingFolder(false); setNewFolderName(''); }
                  }}
                  style={{
                    flex: 1, height: 22,
                    border: '1px solid var(--line2)', borderRadius: 2,
                    background: 'var(--bg1)', color: 'var(--t1)',
                    fontSize: 9, fontFamily: 'Space Mono, monospace',
                    padding: '0 6px', outline: 'none',
                  }}
                />
                <button onClick={handleCreateFolder} style={{ ...btnStyle, color: 'var(--t1)' }}>OK</button>
                <button onClick={() => { setCreatingFolder(false); setNewFolderName(''); }} style={{ ...btnStyle, color: 'var(--t2)' }}>X</button>
              </div>
            )}

            {/* Edit folder inline */}
            {editingFolderId && (
              <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
                <input
                  autoFocus
                  value={editFolderName}
                  onChange={(e) => setEditFolderName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleRenameFolder(editingFolderId);
                    if (e.key === 'Escape') { setEditingFolderId(null); setEditFolderName(''); }
                  }}
                  style={{
                    flex: 1, height: 22,
                    border: '1px solid var(--line2)', borderRadius: 2,
                    background: 'var(--bg1)', color: 'var(--t1)',
                    fontSize: 9, fontFamily: 'Space Mono, monospace',
                    padding: '0 6px', outline: 'none',
                  }}
                />
                <button onClick={() => handleRenameFolder(editingFolderId)} style={{ ...btnStyle, color: 'var(--t1)' }}>SAVE</button>
                <button onClick={() => handleDeleteFolder(editingFolderId)} style={{ ...btnStyle, color: 'var(--red)' }}>DEL</button>
                <button onClick={() => { setEditingFolderId(null); setEditFolderName(''); }} style={{ ...btnStyle, color: 'var(--t2)' }}>X</button>
              </div>
            )}
          </div>

          {/* Job list */}
          <div style={{ flex: 1, overflowY: 'auto', padding: 8 }}>
            {loading && <div style={{ fontSize: 10, color: 'var(--t2)', padding: 6 }}>Refreshing audits...</div>}
            {error && (
              <div style={{ fontSize: 10, color: 'var(--red)', padding: 6, border: '1px solid var(--red)', borderRadius: 3 }}>
                {error}
              </div>
            )}
            {!error && filteredJobs.length === 0 && (
              <div style={{ fontSize: 10, color: 'var(--t2)', padding: 6 }}>
                {activeFolderId ? 'No audits in this folder.' : 'No audits found.'}
              </div>
            )}
            {!error &&
              filteredJobs.map((job, jobIdx) => {
                const selected = selectedJobId === job.id;
                const nextIsSelected = jobIdx + 1 < filteredJobs.length && selectedJobId === filteredJobs[jobIdx + 1].id;
                const bestSharpe = pickBestSharpe(job.results as Record<string, unknown> | null | undefined);
                const sharpeVal = bestSharpe !== null ? bestSharpe.toFixed(3) : '—';
                const deleting = deletingJobId === job.id;
                const renaming = renamingJobId === job.id;
                const isEditingName = editingRenameId === job.id;
                const isMoving = movingJobId === job.id;
                const displayName = (typeof job.display_name === 'string' && job.display_name.trim().length > 0)
                  ? job.display_name.trim()
                  : '';
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
                      marginBottom: 0,
                      padding: '8px 8px',
                      borderRadius: selected ? 3 : 0,
                      border: selected ? '1px solid var(--line)' : 'none',
                      borderBottom: selected ? '1px solid var(--line)' : nextIsSelected ? '1px solid transparent' : '1px solid var(--line2)',
                      background: selected ? 'var(--bg2)' : 'transparent',
                      cursor: 'pointer',
                    }}
                  >
                    {/* Row 1: status + id + actions */}
                    <div style={{ display: 'flex', alignItems: 'center', marginBottom: 6, gap: 6 }}>
                      <span style={{ fontSize: 10, color: statusColor(job.status), textTransform: 'uppercase', letterSpacing: '0.08em' }}>
                        {normalizeStatus(job.status)}
                      </span>
                      <span style={{ fontSize: 9, color: 'var(--t2)', marginLeft: 'auto', fontFamily: 'Space Mono, monospace' }}>
                        {job.id.slice(0, 8)}
                      </span>
                      {pendingDeleteId === job.id ? (
                        <>
                          <span role="button" tabIndex={0}
                            onClick={(e) => { e.stopPropagation(); setPendingDeleteId(null); }}
                            onKeyDown={(e) => { if (e.key === 'Enter') { e.stopPropagation(); setPendingDeleteId(null); } }}
                            style={{ ...btnStyle, color: 'var(--t1)' }}>CANCEL</span>
                          <span role="button" tabIndex={deleting ? -1 : 0}
                            onClick={(e) => { e.stopPropagation(); if (!deleting) { onDelete(job); setPendingDeleteId(null); } }}
                            onKeyDown={(e) => { if (e.key === 'Enter') { e.stopPropagation(); if (!deleting) { onDelete(job); setPendingDeleteId(null); } } }}
                            style={{ ...btnStyle, color: deleting ? 'var(--t2)' : 'var(--t1)' }}
                            onMouseEnter={(e) => { if (!deleting) e.currentTarget.style.color = 'var(--red)'; }}
                            onMouseLeave={(e) => { if (!deleting) e.currentTarget.style.color = 'var(--t1)'; }}
                          >{deleting ? '...' : 'DELETE'}</span>
                        </>
                      ) : (
                        <span role="button" tabIndex={deleting ? -1 : 0}
                          onClick={(e) => { e.stopPropagation(); if (!deleting) setPendingDeleteId(job.id); }}
                          onKeyDown={(e) => { if (e.key === 'Enter') { e.stopPropagation(); if (!deleting) setPendingDeleteId(job.id); } }}
                          style={{ ...btnStyle, color: deleting ? 'var(--t2)' : 'var(--t1)' }}
                          onMouseEnter={(e) => { if (!deleting) e.currentTarget.style.color = 'var(--red)'; }}
                          onMouseLeave={(e) => { if (!deleting) e.currentTarget.style.color = 'var(--t1)'; }}
                        >{deleting ? '...' : 'DEL'}</span>
                      )}
                    </div>

                    {/* Row 2: name / rename */}
                    {isEditingName ? (
                      <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginBottom: 6 }}>
                        <input
                          value={renameDraft}
                          onChange={(e) => setRenameDraft(e.target.value)}
                          placeholder="Audit name"
                          onClick={(e) => e.stopPropagation()}
                          onKeyDown={async (e) => {
                            e.stopPropagation();
                            if (e.key === 'Escape') { setEditingRenameId(null); setRenameDraft(''); return; }
                            if (e.key === 'Enter') { if (!renaming) { await onRename(job, renameDraft.trim()); setEditingRenameId(null); setRenameDraft(''); } }
                          }}
                          style={{
                            flex: 1, height: 22,
                            border: '1px solid var(--line2)', borderRadius: 2,
                            background: 'var(--bg1)', color: 'var(--t1)',
                            fontSize: 9, fontFamily: 'Space Mono, monospace',
                            padding: '0 6px', outline: 'none',
                          }}
                        />
                        <button type="button"
                          onClick={async (e) => { e.stopPropagation(); if (!renaming) { await onRename(job, renameDraft.trim()); setEditingRenameId(null); setRenameDraft(''); } }}
                          disabled={renaming}
                          style={{ ...btnStyle, color: renaming ? 'var(--t2)' : 'var(--t1)' }}
                        >{renaming ? '...' : 'SAVE'}</button>
                        <button type="button"
                          onClick={(e) => { e.stopPropagation(); setEditingRenameId(null); setRenameDraft(''); }}
                          style={{ ...btnStyle, color: 'var(--t2)' }}
                        >X</button>
                      </div>
                    ) : (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                        <div
                          style={{
                            fontSize: 9, color: displayName ? 'var(--t1)' : 'var(--t2)',
                            fontFamily: 'Space Mono, monospace',
                            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', flex: 1,
                          }}
                          title={displayName || 'Unnamed audit'}
                        >
                          {displayName || 'Unnamed audit'}
                        </div>
                        <button type="button"
                          onClick={(e) => { e.stopPropagation(); if (!renaming) { setEditingRenameId(job.id); setRenameDraft(displayName); } }}
                          disabled={renaming}
                          style={{ ...btnStyle, color: 'var(--t2)' }}
                        >RENAME</button>
                        <button type="button"
                          onClick={(e) => { e.stopPropagation(); setMovingJobId(isMoving ? null : job.id); }}
                          style={{ ...btnStyle, color: 'var(--t2)' }}
                          title="Move to folder"
                        >MOVE</button>
                      </div>
                    )}

                    {/* Move-to-folder picker */}
                    {isMoving && (
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginBottom: 6 }}
                           onClick={(e) => e.stopPropagation()}>
                        <button
                          onClick={() => handleMoveJob(job.id, null)}
                          style={{
                            ...btnStyle,
                            color: !job.folder_id ? 'var(--t0)' : 'var(--t3)',
                            background: !job.folder_id ? 'var(--bg4)' : 'var(--bg1)',
                          }}
                        >None</button>
                        {folders.map((f) => (
                          <button
                            key={f.id}
                            onClick={() => handleMoveJob(job.id, f.id)}
                            style={{
                              ...btnStyle,
                              color: job.folder_id === f.id ? 'var(--t0)' : 'var(--t3)',
                              background: job.folder_id === f.id ? 'var(--bg4)' : 'var(--bg1)',
                            }}
                          >{f.name}</button>
                        ))}
                      </div>
                    )}

                    {/* Row 3: sharpe + timestamp */}
                    <div style={{ fontSize: 9, color: 'var(--t1)', marginBottom: 4 }}>
                      sharpe: {sharpeVal}
                    </div>
                    <div style={{ fontSize: 9, color: 'var(--t2)' }}>updated: {fmtWhen(job.updated_at)}</div>
                  </div>
                );
              })}
          </div>
        </>
      )}
    </div>
  );
}

'use client';

import { useState } from 'react';

const API_BASE = 'http://localhost:8000';

interface TopbarProps {
  jobId: string | null;
  appState: 'idle' | 'running' | 'results' | 'failed';
  results: Record<string, unknown> | null;
}

export default function Topbar({ jobId, appState, results }: TopbarProps) {
  const [exportState, setExportState] = useState<'idle' | 'generating' | 'error'>('idle');

  const handleExportReport = async () => {
    if (!jobId) return;
    setExportState('generating');
    try {
      const res = await fetch(`${API_BASE}/api/jobs/${jobId}/report`, { method: 'POST' });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: 'Unknown error' }));
        throw new Error(err.detail ?? 'Report generation failed');
      }
      // Trigger download
      const a = document.createElement('a');
      a.href = `${API_BASE}/api/jobs/${jobId}/download/report`;
      a.download = `audit_report_${jobId.slice(0, 8)}.docx`;
      a.click();
      setExportState('idle');
    } catch {
      setExportState('error');
      setTimeout(() => setExportState('idle'), 4000);
    }
  };

  return (
    <div
      style={{
        height: 46,
        background: 'var(--bg1)',
        borderBottom: '1px solid var(--line)',
        padding: '0 16px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexShrink: 0,
      }}
    >
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 10 }}>
        <span style={{ fontSize: 14, fontWeight: 700, color: 'var(--t0)', letterSpacing: '0.05em' }}>
          BENJI3M
        </span>
        <span style={{ fontSize: 9, color: 'var(--t3)', letterSpacing: '0.18em', textTransform: 'uppercase' }}>
          RISK AUDIT ENGINE
        </span>
      </div>

      {appState === 'results' && Boolean(results) && (
        <button
          onClick={handleExportReport}
          disabled={exportState === 'generating'}
          style={{
            height: 28,
            padding: '0 12px',
            background: 'var(--bg3)',
            border: `1px solid ${exportState === 'error' ? 'var(--red)' : 'var(--line2)'}`,
            borderRadius: 3,
            color: exportState === 'error' ? 'var(--red)' : exportState === 'generating' ? 'var(--amber)' : 'var(--t1)',
            fontSize: 10,
            fontFamily: 'Space Mono, monospace',
            cursor: exportState === 'generating' ? 'not-allowed' : 'pointer',
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            opacity: exportState === 'generating' ? 0.7 : 1,
          }}
        >
          {exportState === 'generating' ? '⟳ GENERATING...' : exportState === 'error' ? '✕ FAILED' : '↓ EXPORT REPORT'}
        </button>
      )}
    </div>
  );
}

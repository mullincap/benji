'use client';

interface StatusBarProps {
  appState: 'idle' | 'running' | 'results' | 'failed';
  jobData: Record<string, unknown> | null;
  elapsedSeconds: number;
}

function formatElapsed(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${String(s).padStart(2, '0')}`;
}

function getStageLabel(stage?: string | null): string {
  if (!stage) return '';
  const map: Record<string, string> = {
    overlap: 'OVERLAP ANALYSIS',
    rebuild: 'PORTFOLIO MATRIX REBUILD',
    audit: 'INSTITUTIONAL AUDIT',
    report: 'REPORT GENERATION',
  };
  return map[stage] ?? stage.toUpperCase();
}

export default function StatusBar({ appState, jobData, elapsedSeconds }: StatusBarProps) {
  const dotColor =
    appState === 'results'
      ? 'var(--green)'
      : appState === 'running'
      ? 'var(--amber)'
      : 'var(--t3)';

  const statusText =
    appState === 'idle'
      ? 'IDLE'
      : appState === 'running'
      ? 'RUNNING'
      : appState === 'results'
      ? 'COMPLETE'
      : 'FAILED';

  const stage = jobData ? (jobData.stage as string | null) : null;

  return (
    <div
      style={{
        height: 32,
        background: 'var(--bg1)',
        borderTop: '1px solid var(--line)',
        padding: '0 16px',
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        fontSize: 10,
        color: 'var(--t2)',
        flexShrink: 0,
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <div
          style={{
            width: 4,
            height: 4,
            borderRadius: '50%',
            background: dotColor,
            animation: appState === 'running' ? 'pulse-dot 1.2s ease-in-out infinite' : 'none',
          }}
        />
        <span>{statusText}</span>
      </div>

      {stage && appState === 'running' && (
        <span style={{ color: 'var(--t2)' }}>{getStageLabel(stage)}</span>
      )}

      <div style={{ marginLeft: 'auto' }}>
        {(appState === 'running' || appState === 'results') && (
          <span>{formatElapsed(elapsedSeconds)}</span>
        )}
      </div>
    </div>
  );
}

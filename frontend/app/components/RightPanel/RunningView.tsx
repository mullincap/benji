'use client';

import { useState, useEffect, useRef } from 'react';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || '';

interface RunningViewProps {
  jobId: string | null;
  jobData: Record<string, unknown> | null;
  logLines: string[];
  elapsedSeconds: number;
  onCancel: () => void;
  cancelling?: boolean;
}

type StageStatus = 'done' | 'active' | 'pending';

const STAGES = [
  { key: 'overlap', label: 'Overlap Analysis' },
  { key: 'rebuild', label: 'Portfolio Matrix Rebuild' },
  { key: 'audit', label: 'Institutional Audit' },
  { key: 'report', label: 'Report Generation' },
];

const STAGE_ORDER = ['overlap', 'rebuild', 'audit', 'report'];

function getStageStatus(stageKey: string, currentStage: string | null, progress: number): StageStatus {
  if (!currentStage) return 'pending';
  const currentIdx = STAGE_ORDER.indexOf(currentStage);
  const thisIdx = STAGE_ORDER.indexOf(stageKey);
  if (thisIdx < currentIdx) return 'done';
  if (thisIdx === currentIdx) return progress >= 100 ? 'done' : 'active';
  return 'pending';
}

function fmtDuration(secs: number): string {
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  if (m === 0) return `${s}s`;
  return `${m}m ${s.toString().padStart(2, '0')}s`;
}

export default function RunningView({
  jobId,
  jobData,
  logLines,
  elapsedSeconds,
  onCancel,
  cancelling = false,
}: RunningViewProps) {
  const currentStage = (jobData?.stage as string | null) ?? null;
  const progress = typeof jobData?.progress === 'number' ? jobData.progress : 0;
  const filterProgress = jobData?.filter_progress as string | undefined;
  const visibleStages = STAGES
    .map(({ key, label }) => ({ key, label, status: getStageStatus(key, currentStage, progress) }))
    .filter(({ status }) => status !== 'pending');

  const [verbose, setVerbose] = useState(true);
  const [logCollapsed, setLogCollapsed] = useState(false);
  const [rawLines, setRawLines] = useState<string[]>([]);
  const logRef = useRef<HTMLDivElement>(null);

  // ETA: based on elapsed + progress
  const etaSecs = progress > 2 ? Math.round((elapsedSeconds / progress) * (100 - progress)) : null;

  // Poll raw output when verbose is on
  useEffect(() => {
    if (!verbose || !jobId) return;
    let active = true;

    const poll = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/jobs/${jobId}/output`);
        if (res.ok) {
          const data = await res.json();
          if (active) setRawLines((data.text as string).split('\n').filter(Boolean));
        }
      } catch { /* ignore */ }
    };

    poll();
    const interval = setInterval(poll, 2000);
    return () => { active = false; clearInterval(interval); };
  }, [verbose, jobId]);

  // Auto-scroll to bottom
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [verbose, rawLines, logLines]);

  const displayLines = verbose ? rawLines : logLines;

  return (
    <div style={{ padding: 16, display: 'flex', flexDirection: 'column', height: '100%', boxSizing: 'border-box', gap: 12 }}>

      {/* Progress card */}
      <div style={{ background: 'var(--bg2)', border: '1px solid var(--line)', borderRadius: 3, padding: 16, flexShrink: 0 }}>
        <div style={{ fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.12em', color: 'var(--t3)', fontWeight: 700, marginBottom: 12 }}>
          PIPELINE PROGRESS
        </div>

        {visibleStages.map(({ key, label, status }) => {
          const isActive = status === 'active';
          const isDone = status === 'done';
          return (
            <div key={key} style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
              <div style={{
                width: 20, height: 20, borderRadius: 3, flexShrink: 0,
                border: `1px solid ${isDone ? 'var(--green)' : isActive ? 'var(--amber)' : 'var(--line)'}`,
                background: isDone ? 'var(--green-dim)' : isActive ? 'var(--amber-dim)' : 'var(--bg3)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                animation: isActive ? 'pulse-dot 1.2s ease-in-out infinite' : 'none',
              }}>
                {isDone && <span style={{ fontSize: 9, color: 'var(--green)', fontWeight: 700 }}>✓</span>}
                {isActive && <div style={{ width: 8, height: 8, borderRadius: '50%', background: 'var(--amber)', animation: 'spin 0.8s linear infinite' }} />}
                {!isDone && !isActive && <span style={{ fontSize: 9, color: 'var(--t3)' }}>—</span>}
              </div>
              <span style={{ fontSize: 10, color: isDone ? 'var(--t1)' : isActive ? 'var(--t0)' : 'var(--t3)', flex: 1 }}>
                {label}
                {key === 'audit' && filterProgress && isActive && (
                  <span style={{ color: 'var(--amber)', marginLeft: 6 }}>({filterProgress})</span>
                )}
              </span>
              <span style={{ fontSize: 9, color: isDone ? 'var(--green)' : isActive ? 'var(--amber)' : 'var(--t3)' }}>
                {isDone ? 'done' : isActive ? 'running' : 'pending'}
              </span>
            </div>
          );
        })}

        {/* Progress bar */}
        <div style={{ width: '100%', height: 2, background: 'var(--line)', borderRadius: 1, marginTop: 8, overflow: 'hidden' }}>
          <div style={{ height: '100%', background: 'var(--green)', width: `${progress}%`, transition: 'width 0.5s ease', borderRadius: 1 }} />
        </div>

        {/* Stats row */}
        <div style={{ display: 'flex', gap: 20, marginTop: 10 }}>
          <div>
            <div style={{ fontSize: 9, color: 'var(--t3)', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 2 }}>ELAPSED</div>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--t1)', fontFamily: 'Space Mono, monospace' }}>{fmtDuration(elapsedSeconds)}</div>
          </div>
          <div>
            <div style={{ fontSize: 9, color: 'var(--t3)', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 2 }}>PROGRESS</div>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--t1)', fontFamily: 'Space Mono, monospace' }}>{Math.round(progress)}%</div>
          </div>
          {etaSecs !== null && (
            <div>
              <div style={{ fontSize: 9, color: 'var(--t3)', letterSpacing: '0.1em', textTransform: 'uppercase', marginBottom: 2 }}>ETA</div>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--amber)', fontFamily: 'Space Mono, monospace' }}>~{fmtDuration(etaSecs)}</div>
            </div>
          )}
        </div>

        <div style={{ marginTop: 12 }}>
          <button
            onClick={onCancel}
            disabled={!jobId || cancelling}
            style={{
              width: '100%',
              height: 28,
              border: '1px solid var(--line2)',
              borderRadius: 3,
              background: 'var(--bg1)',
              color: cancelling ? 'var(--t2)' : 'var(--t1)',
              fontFamily: 'Space Mono, monospace',
              fontSize: 10,
              cursor: !jobId || cancelling ? 'not-allowed' : 'pointer',
              letterSpacing: '0.06em',
            }}
            onMouseEnter={(e) => {
              if (!cancelling && jobId) (e.currentTarget.style.color = 'var(--red)');
            }}
            onMouseLeave={(e) => {
              if (!cancelling && jobId) (e.currentTarget.style.color = 'var(--t1)');
            }}
          >
            {cancelling ? 'CANCELLING...' : 'CANCEL AUDIT'}
          </button>
        </div>
      </div>

      {/* Live log card — fills remaining height, collapses to header strip */}
      <div style={{
        background: '#000',
        border: '1px solid var(--line)',
        borderRadius: 3,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        flex: logCollapsed ? '0 0 auto' : 1,
        overflow: 'hidden',
      }}>
        {/* Header — always visible, click to toggle */}
        <div
          onClick={() => setLogCollapsed((c) => !c)}
          style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', cursor: 'pointer', userSelect: 'none', flexShrink: 0 }}
        >
          <span style={{ fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.12em', color: 'var(--t3)', fontWeight: 700 }}>LIVE OUTPUT</span>
          {!logCollapsed && (
            <>
              <div style={{ width: 4, height: 4, borderRadius: '50%', background: 'var(--amber)', animation: 'pulse-dot 1.2s ease-in-out infinite' }} />
              <span style={{ fontSize: 9, color: 'var(--amber)', letterSpacing: '0.08em' }}>STREAMING</span>
            </>
          )}
          <div style={{ flex: 1 }} />
          {!logCollapsed && (
            <>
              <span style={{ fontSize: 9, color: 'var(--t2)', marginRight: 4 }}>VERBOSE</span>
              <div
                onClick={(e) => { e.stopPropagation(); setVerbose((v) => !v); }}
                style={{
                  width: 26, height: 14, borderRadius: 7, position: 'relative', cursor: 'pointer', flexShrink: 0, marginRight: 8,
                  background: verbose ? 'var(--green-mid)' : 'var(--bg4)',
                  border: `1px solid ${verbose ? 'var(--green)' : 'var(--line2)'}`,
                  transition: 'background 0.15s, border-color 0.15s',
                }}
              >
                <div style={{
                  width: 10, height: 10, borderRadius: '50%', position: 'absolute', top: 1,
                  left: verbose ? 13 : 1, transition: 'left 0.2s, background 0.15s',
                  background: verbose ? 'var(--green)' : 'var(--t2)',
                }} />
              </div>
            </>
          )}
          <span style={{ fontSize: 9, color: 'var(--t3)', display: 'inline-block', transform: logCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)', transition: 'transform 0.2s' }}>▾</span>
        </div>

        {/* Log body */}
        {!logCollapsed && (
          <div
            ref={logRef}
            style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: '0 14px 14px', fontFamily: 'Space Mono, monospace', fontSize: 9, lineHeight: 1.6 }}
          >
            {displayLines.length === 0 ? (
              <span style={{ color: 'var(--t3)' }}>Waiting for output...</span>
            ) : verbose ? (
              displayLines.map((line, i) => {
                const isLast = i === displayLines.length - 1;
                const color = /\[WARNING\]/.test(line) ? 'var(--amber)' : /\[ERROR\]|error|Error/.test(line) ? 'var(--red)' : /\[INFO\]/.test(line) ? 'var(--t1)' : 'var(--t0)';
                return (
                  <div key={i} style={{ color, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                    {line}
                    {isLast && <span style={{ color: 'var(--amber)', animation: 'blink-cursor 1s step-end infinite', marginLeft: 2 }}>█</span>}
                  </div>
                );
              })
            ) : (
              displayLines.map((line, i) => {
                const isLast = i === displayLines.length - 1;
                const match = line.match(/^(\[\d{2}:\d{2}:\d{2}\])\s(.+)$/);
                return (
                  <div key={i}>
                    {match ? (
                      <><span style={{ color: 'var(--t3)' }}>{match[1]} </span><span style={{ color: 'var(--t0)' }}>{match[2]}</span></>
                    ) : (
                      <span style={{ color: 'var(--t0)' }}>{line}</span>
                    )}
                    {isLast && <span style={{ color: 'var(--amber)', animation: 'blink-cursor 1s step-end infinite', marginLeft: 2 }}>█</span>}
                  </div>
                );
              })
            )}
          </div>
        )}
      </div>
    </div>
  );
}

'use client';

import { useEffect, useMemo, useRef, useState } from 'react';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || '';

export interface PromoteFilterOption {
  filter: string;
  sharpe?: number | null;
}

interface PromoteStrategyModalProps {
  jobId: string;
  filters: PromoteFilterOption[];
  defaultFilter?: string | null;
  onCancel: () => void;
  onSuccess: (result: PromoteResult) => void;
}

export interface PromoteResult {
  strategy_id: number | string;
  strategy_version_id: string;
  result_id: string;
  strategy_name: string;
  version_label: string;
  filter_mode: string;
  is_published: boolean;
}

interface StrategySuggestion {
  strategy_id: number | string;
  name: string;
  display_name: string | null;
}

const labelStyle: React.CSSProperties = {
  display: 'block',
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: '0.12em',
  color: 'var(--t3)',
  textTransform: 'uppercase',
  marginBottom: 6,
};

const inputStyle: React.CSSProperties = {
  width: '100%',
  background: 'var(--bg3)',
  border: '1px solid var(--line)',
  borderRadius: 4,
  padding: '8px 10px',
  fontSize: 11,
  color: 'var(--t0)',
  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
  outline: 'none',
};

const buttonBase: React.CSSProperties = {
  border: '1px solid var(--line2)',
  borderRadius: 4,
  padding: '8px 16px',
  fontSize: 9,
  fontWeight: 700,
  letterSpacing: '0.12em',
  textTransform: 'uppercase',
  fontFamily: 'var(--font-space-mono), Space Mono, monospace',
  cursor: 'pointer',
};

export default function PromoteStrategyModal({
  jobId,
  filters,
  defaultFilter,
  onCancel,
  onSuccess,
}: PromoteStrategyModalProps) {
  const [strategyName, setStrategyName] = useState('');
  const [versionLabel, setVersionLabel] = useState('v1');
  const [description, setDescription] = useState('');
  const [filterMode, setFilterMode] = useState<string>(defaultFilter || filters[0]?.filter || '');

  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [suggestions, setSuggestions] = useState<StrategySuggestion[]>([]);
  const [suggestOpen, setSuggestOpen] = useState(false);
  const suggestBoxRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/allocator/strategies`, {
          credentials: 'include',
        });
        if (!res.ok) return;
        const data = await res.json();
        if (cancelled) return;
        const list = Array.isArray(data?.strategies) ? data.strategies : [];
        setSuggestions(
          list
            .filter((s: StrategySuggestion) => s && (s.display_name || s.name))
            .map((s: StrategySuggestion) => ({
              strategy_id: s.strategy_id,
              name: s.name,
              display_name: s.display_name ?? s.name,
            })),
        );
      } catch {
        // suggestions are optional
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (suggestBoxRef.current && !suggestBoxRef.current.contains(e.target as Node)) {
        setSuggestOpen(false);
      }
    }
    document.addEventListener('mousedown', onDocClick);
    return () => document.removeEventListener('mousedown', onDocClick);
  }, []);

  const filteredSuggestions = useMemo(() => {
    const q = strategyName.trim().toLowerCase();
    if (!q) return suggestions.slice(0, 8);
    return suggestions
      .filter((s) => (s.display_name || s.name).toLowerCase().includes(q))
      .slice(0, 8);
  }, [suggestions, strategyName]);

  const canSubmit =
    strategyName.trim().length > 0 &&
    versionLabel.trim().length > 0 &&
    filterMode.trim().length > 0 &&
    !submitting;

  async function handleSubmit() {
    if (!canSubmit) return;
    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch(
        `${API_BASE}/api/simulator/audits/${jobId}/promote`,
        {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            strategy_name: strategyName.trim(),
            version_label: versionLabel.trim(),
            description: description.trim() || null,
            filter_mode: filterMode,
          }),
        },
      );
      if (res.status === 401) {
        setError('Admin session required. Sign in as an admin and retry.');
        return;
      }
      if (res.status === 409) {
        const body = await res.json().catch(() => ({}));
        setError(body?.detail || 'Already promoted or version label already exists.');
        return;
      }
      if (res.status === 400) {
        const body = await res.json().catch(() => ({}));
        setError(body?.detail || 'Invalid request.');
        return;
      }
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        setError(body?.detail || `Request failed: ${res.status}`);
        return;
      }
      const result = (await res.json()) as PromoteResult;
      onSuccess(result);
    } catch (err) {
      setError(String(err));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      onClick={() => {
        if (!submitting) onCancel();
      }}
      style={{
        position: 'fixed',
        inset: 0,
        background: 'rgba(0,0,0,0.7)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          background: 'var(--bg2)',
          border: '1px solid var(--line2)',
          borderRadius: 6,
          padding: '20px 24px',
          width: 520,
          maxWidth: '92vw',
          fontFamily: 'var(--font-space-mono), Space Mono, monospace',
        }}
      >
        <div
          style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: '0.12em',
            color: 'var(--t3)',
            textTransform: 'uppercase',
            marginBottom: 12,
          }}
        >
          Promote Audit as Strategy
        </div>

        <div style={{ fontSize: 11, color: 'var(--t1)', lineHeight: 1.6, marginBottom: 16 }}>
          Publish this audit as a strategy version that appears in the Allocator.
          Metrics for the chosen filter will be stored in <span style={{ color: 'var(--t0)' }}>audit.results</span>.
        </div>

        {/* Strategy Name with autocomplete */}
        <div style={{ marginBottom: 14, position: 'relative' }} ref={suggestBoxRef}>
          <label style={labelStyle}>Strategy Name</label>
          <input
            type="text"
            value={strategyName}
            onChange={(e) => {
              setStrategyName(e.target.value);
              setSuggestOpen(true);
            }}
            onFocus={() => setSuggestOpen(true)}
            placeholder="e.g. Alpha Tail Guardrail"
            style={inputStyle}
            disabled={submitting}
          />
          {suggestOpen && filteredSuggestions.length > 0 && (
            <ul
              style={{
                position: 'absolute',
                top: '100%',
                left: 0,
                right: 0,
                marginTop: 2,
                maxHeight: 160,
                overflowY: 'auto',
                background: 'var(--bg3)',
                border: '1px solid var(--line2)',
                borderRadius: 4,
                listStyle: 'none',
                padding: 0,
                zIndex: 1,
              }}
            >
              {filteredSuggestions.map((s) => (
                <li
                  key={String(s.strategy_id)}
                  onClick={() => {
                    setStrategyName(s.display_name || s.name);
                    setSuggestOpen(false);
                  }}
                  style={{
                    padding: '6px 10px',
                    fontSize: 11,
                    color: 'var(--t1)',
                    cursor: 'pointer',
                    borderBottom: '1px solid var(--line)',
                  }}
                  onMouseEnter={(e) => {
                    (e.currentTarget as HTMLLIElement).style.background = 'var(--bg4)';
                  }}
                  onMouseLeave={(e) => {
                    (e.currentTarget as HTMLLIElement).style.background = 'transparent';
                  }}
                >
                  {s.display_name || s.name}
                </li>
              ))}
            </ul>
          )}
          <div style={{ fontSize: 9, color: 'var(--t3)', marginTop: 4 }}>
            Match an existing strategy to add a new version, or enter a new name to create one.
          </div>
        </div>

        {/* Version Label */}
        <div style={{ marginBottom: 14 }}>
          <label style={labelStyle}>Version Label</label>
          <input
            type="text"
            value={versionLabel}
            onChange={(e) => setVersionLabel(e.target.value)}
            placeholder="v1, 2026-04-16, etc."
            style={inputStyle}
            disabled={submitting}
          />
        </div>

        {/* Filter */}
        <div style={{ marginBottom: 14 }}>
          <label style={labelStyle}>Filter</label>
          <select
            value={filterMode}
            onChange={(e) => setFilterMode(e.target.value)}
            disabled={submitting}
            style={{
              ...inputStyle,
              appearance: 'none',
              paddingRight: 28,
              cursor: 'pointer',
            }}
          >
            {filters.length === 0 && <option value="">(no filters in audit)</option>}
            {filters.map((f) => (
              <option key={f.filter} value={f.filter}>
                {f.filter}
                {typeof f.sharpe === 'number' ? ` — Sharpe ${f.sharpe.toFixed(2)}` : ''}
              </option>
            ))}
          </select>
          <div style={{ fontSize: 9, color: 'var(--t3)', marginTop: 4 }}>
            Metrics are copied from this filter&apos;s per-filter block. Top-level metrics (Sortino, Calmar, …) are attached only if this is the audit&apos;s best filter.
          </div>
        </div>

        {/* Description */}
        <div style={{ marginBottom: 16 }}>
          <label style={labelStyle}>Description (optional)</label>
          <textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Notes for allocators about this version…"
            rows={3}
            style={{
              ...inputStyle,
              resize: 'vertical',
              minHeight: 60,
            }}
            disabled={submitting}
          />
        </div>

        {error && (
          <div
            style={{
              fontSize: 11,
              color: 'var(--red)',
              background: 'var(--red-dim)',
              border: '1px solid var(--red)',
              borderRadius: 4,
              padding: '8px 10px',
              marginBottom: 14,
              lineHeight: 1.5,
            }}
          >
            {error}
          </div>
        )}

        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
          <button
            type="button"
            onClick={onCancel}
            disabled={submitting}
            style={{
              ...buttonBase,
              background: 'transparent',
              color: 'var(--t2)',
              cursor: submitting ? 'not-allowed' : 'pointer',
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSubmit}
            disabled={!canSubmit}
            style={{
              ...buttonBase,
              background: canSubmit ? 'var(--green-dim)' : 'transparent',
              border: `1px solid ${canSubmit ? 'var(--green)' : 'var(--line2)'}`,
              color: canSubmit ? 'var(--green)' : 'var(--t3)',
              cursor: canSubmit ? 'pointer' : 'not-allowed',
            }}
          >
            {submitting ? 'Promoting…' : 'Promote'}
          </button>
        </div>
      </div>
    </div>
  );
}

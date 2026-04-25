'use client';

/**
 * CanonicalCompareCard
 * ====================
 * Side-by-side comparison of a Simulator candidate's audit results vs
 * whichever published strategy is currently flagged canonical in
 * `audit.strategies.is_canonical` (see migration 002). The card reads
 * the flag via GET /api/simulator/canonical-reference and renders
 * whatever row comes back — no strategy name is hardcoded here.
 *
 * Seeded default is 'Alpha Main' since Sharpe is leverage-invariant
 * across the Alpha Low/Main/Max family; admins can promote any other
 * published strategy via the Strategies page "Make Canonical" action.
 *
 * Governance rule applied per docs/strategy_specification.md § 5:
 *   1. Candidate Sharpe ≥ canonical Sharpe + 0.3
 *   2. Candidate CAGR > canonical CAGR
 *   3. Candidate Max DD ≥ 1.10 × canonical Max DD  (i.e. within 10% deeper)
 * Overall PASS only if all three ✓.
 *
 * No automation — this card is an advisory display. Promotion is still
 * a manual admin action per the spec's § 5.2 governance requirements.
 */

import { useEffect, useState } from 'react';

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || '';

/** Methodology-relevant params that actually define basket selection.
 *  If a candidate and canonical agree on ALL of these, the comparison
 *  short-circuits (no meaningful delta). */
const METHODOLOGY_KEYS = [
  'mode',
  // D-medium-split (2026-04-23): individual ranking-metric keys per metric
  // + BloFin filter. `ranking_metric` (deprecated) retained in this list
  // so an old saved candidate with the legacy single field still
  // participates in the short-circuit check.
  'price_ranking_metric',
  'oi_ranking_metric',
  'apply_blofin_filter',
  'ranking_metric',
  'leaderboard_index',
  'sort_by',
  'freq_width',
  'freq_cutoff',
  'sample_interval',
  'min_mcap',
  'max_mcap',
  'drop_unverified',
  'deployment_start_hour',
  'index_lookback',
  'sort_lookback',
] as const;

interface CanonicalReference {
  strategy_name: string;
  version_label: string;
  filter_mode: string;
  metrics_data_through: string | null;
  metrics: Record<string, unknown>;
  config: Record<string, unknown>;
}

interface CanonicalCompareCardProps {
  results: Record<string, unknown> | null;
  params: Record<string, unknown>;
}

function asNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function findFilterRow(
  results: Record<string, unknown> | null,
  filterName: string,
): Record<string, unknown> | null {
  const metrics = (results?.metrics ?? null) as Record<string, unknown> | null;
  const filters = (metrics?.filters as Array<Record<string, unknown>> | undefined) ?? [];
  for (const f of filters) {
    if (f?.filter === filterName) return f;
  }
  return null;
}

function paramsMatchCanonical(
  candidate: Record<string, unknown>,
  canonical: Record<string, unknown>,
): boolean {
  for (const k of METHODOLOGY_KEYS) {
    const a = candidate[k];
    const b = canonical[k];
    // Treat undefined/null as equivalent (both "unset"); otherwise compare by value.
    const aDef = a !== undefined && a !== null;
    const bDef = b !== undefined && b !== null;
    if (!aDef && !bDef) continue;
    if (aDef !== bDef) return false;
    if (String(a) !== String(b)) return false;
  }
  return true;
}

function fmt(v: number | null, decimals = 2, suffix = ''): string {
  if (v === null) return 'N/A';
  const sign = v > 0 ? '+' : '';
  return `${sign}${v.toFixed(decimals)}${suffix}`;
}

const LABEL_STYLE: React.CSSProperties = {
  fontSize: 9,
  textTransform: 'uppercase',
  letterSpacing: '0.12em',
  fontWeight: 700,
  color: 'var(--t3)',
};

const SECTION_STYLE: React.CSSProperties = {
  padding: '14px 16px',
  borderTop: '1px solid var(--line)',
};

function Row({
  label, candidate, canonical, delta, ok,
}: {
  label: string;
  candidate: string;
  canonical: string;
  delta: string;
  ok: boolean | null;
}) {
  const okColor = ok === null ? 'var(--t2)' : ok ? 'var(--green)' : 'var(--red)';
  const okMark = ok === null ? '–' : ok ? '✓' : '✗';
  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 1fr 1fr 1fr 12px',
        gap: 4,
        fontSize: 10,
        color: 'var(--t1)',
        fontFamily: 'Space Mono, monospace',
        padding: '3px 0',
        alignItems: 'baseline',
      }}
    >
      <div style={{ color: 'var(--t2)' }}>{label}</div>
      <div style={{ textAlign: 'right' }}>{candidate}</div>
      <div style={{ textAlign: 'right', color: 'var(--t2)' }}>{canonical}</div>
      <div style={{ textAlign: 'right' }}>{delta}</div>
      <div style={{ textAlign: 'center', color: okColor, fontWeight: 700 }}>{okMark}</div>
    </div>
  );
}

export default function CanonicalCompareCard({ results, params }: CanonicalCompareCardProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [canonical, setCanonical] = useState<CanonicalReference | null>(null);

  const fetchCanonical = async () => {
    setError(null);
    setLoading(true);
    try {
      const resp = await fetch(`${API_BASE}/api/simulator/canonical-reference`, {
        credentials: 'include',
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
      }
      const data = (await resp.json()) as CanonicalReference;
      setCanonical(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  // Auto-fetch on mount so the governance comparison is visible the moment
  // a candidate audit finishes — no extra click. Re-fetch via the refresh
  // button in the error state. `results` dep is intentional: if the user
  // re-runs with different params, we keep the same canonical reference
  // (canonical is per-strategy-version, not per-candidate-run), so no
  // refetch is needed on `results` change.
  useEffect(() => {
    if (!canonical && !loading && !error) {
      fetchCanonical();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Initial render (pre-click) ─────────────────────────────────────────
  if (!canonical && !loading && !error) {
    // Auto-fetch is about to fire on mount; render a slim placeholder.
    return (
      <div style={SECTION_STYLE}>
        <div style={LABEL_STYLE}>GOVERNANCE</div>
        <div style={{ fontSize: 10, color: 'var(--t2)', marginTop: 8 }}>
          Preparing comparison...
        </div>
      </div>
    );
  }

  // ── Loading ────────────────────────────────────────────────────────────
  if (loading) {
    return (
      <div style={SECTION_STYLE}>
        <div style={LABEL_STYLE}>GOVERNANCE</div>
        <div style={{ fontSize: 10, color: 'var(--t2)', marginTop: 8 }}>
          Fetching canonical reference...
        </div>
      </div>
    );
  }

  // ── Error ──────────────────────────────────────────────────────────────
  if (error) {
    return (
      <div style={SECTION_STYLE}>
        <div style={LABEL_STYLE}>GOVERNANCE — ERROR</div>
        <div
          style={{
            fontSize: 10, color: 'var(--red)',
            background: 'var(--red-dim)', border: '1px solid var(--red)',
            borderRadius: 3, padding: '8px 10px', marginTop: 8,
            wordBreak: 'break-word', lineHeight: 1.5,
          }}
        >
          {error}
        </div>
        <button
          onClick={fetchCanonical}
          style={{
            width: '100%', height: 28, marginTop: 8,
            background: 'transparent', border: '1px solid var(--line2)',
            borderRadius: 3, color: 'var(--t0)',
            fontFamily: 'Space Mono, monospace', fontSize: 9,
            fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.12em',
            cursor: 'pointer',
          }}
        >
          RETRY
        </button>
      </div>
    );
  }

  if (!canonical) return null;

  // ── Short-circuit: identical methodology params ────────────────────────
  const identical = paramsMatchCanonical(params, canonical.config);
  if (identical) {
    return (
      <div style={SECTION_STYLE}>
        <div style={LABEL_STYLE}>GOVERNANCE</div>
        <div
          style={{
            fontSize: 10, color: 'var(--t1)',
            background: 'var(--bg3)', border: '1px solid var(--line2)',
            borderRadius: 3, padding: '10px 12px', marginTop: 8, lineHeight: 1.6,
          }}
        >
          Candidate params match canonical on all methodology-relevant axes
          (mode, universe size, ranking, filters, window). Nothing to compare —
          this candidate IS canonical.
        </div>
        <div style={{ fontSize: 9, color: 'var(--t3)', marginTop: 8 }}>
          Canonical: {canonical.strategy_name} {canonical.version_label}
          {canonical.metrics_data_through
            ? ` (through ${canonical.metrics_data_through})` : ''}
        </div>
      </div>
    );
  }

  // ── Render side-by-side + rule ─────────────────────────────────────────
  const filterRow = findFilterRow(results, canonical.filter_mode);
  if (!filterRow) {
    return (
      <div style={SECTION_STYLE}>
        <div style={LABEL_STYLE}>GOVERNANCE</div>
        <div
          style={{
            fontSize: 10, color: 'var(--amber)',
            background: 'var(--amber-dim)', border: '1px solid var(--amber)',
            borderRadius: 3, padding: '10px 12px', marginTop: 8, lineHeight: 1.5,
          }}
        >
          Candidate has no &quot;{canonical.filter_mode}&quot; filter result; cannot compare
          against canonical (which uses that filter). Re-run with
          enable_tail_guardrail = true and run_filter_tail_disp (or equivalent)
          enabled to produce a Tail Guardrail filter row.
        </div>
      </div>
    );
  }

  const candSharpe = asNum(filterRow.sharpe);
  const candCagr   = asNum(filterRow.cagr);
  const candMaxDD  = asNum(filterRow.max_dd);
  const candTotRet = asNum(filterRow.tot_ret);

  const m = canonical.metrics;
  const canSharpe = asNum(m.sharpe);
  const canCagr   = asNum(m.cagr_pct);
  const canMaxDD  = asNum(m.max_dd_pct);
  const canTotRet = asNum(m.total_return_pct);

  // Governance rule (spec § 5)
  const sharpeOK =
    candSharpe !== null && canSharpe !== null ? candSharpe >= canSharpe + 0.3 : null;
  const cagrOK =
    candCagr !== null && canCagr !== null ? candCagr > canCagr : null;
  // Max DD: both negative; 1.10× canonical means candidate may be up to 10% deeper.
  // e.g. canonical -20.22 → threshold -22.24 → candidate OK if ≥ -22.24 (i.e. shallower or within 10%).
  const ddOK =
    candMaxDD !== null && canMaxDD !== null ? candMaxDD >= 1.10 * canMaxDD : null;
  const allPass = sharpeOK === true && cagrOK === true && ddOK === true;
  const anyChecked = sharpeOK !== null && cagrOK !== null && ddOK !== null;

  const deltaSharpe = candSharpe !== null && canSharpe !== null ? candSharpe - canSharpe : null;
  const deltaCagr   = candCagr   !== null && canCagr   !== null ? candCagr - canCagr   : null;
  const deltaMaxDD  = candMaxDD  !== null && canMaxDD  !== null ? candMaxDD - canMaxDD : null;
  const deltaTotRet = candTotRet !== null && canTotRet !== null ? candTotRet - canTotRet : null;

  const verdictColor = !anyChecked ? 'var(--t2)' : allPass ? 'var(--green)' : 'var(--red)';
  const verdictBg    = !anyChecked ? 'var(--bg3)' : allPass ? 'var(--green-dim)' : 'var(--red-dim)';
  const verdictBorder = !anyChecked ? 'var(--line2)' : allPass ? 'var(--green-mid)' : 'var(--red)';
  const verdictText = !anyChecked
    ? 'Insufficient data to evaluate — candidate or canonical missing a required metric.'
    : allPass
      ? 'This candidate QUALIFIES for promotion (all three rules pass).'
      : `This candidate DOES NOT QUALIFY: ${[
          sharpeOK === false ? 'Sharpe below canonical + 0.3' : null,
          cagrOK === false ? 'CAGR below canonical' : null,
          ddOK === false ? 'Max DD > 10% worse than canonical' : null,
        ].filter(Boolean).join('; ')}.`;

  return (
    <div style={SECTION_STYLE}>
      <div style={LABEL_STYLE}>GOVERNANCE — vs {canonical.strategy_name}</div>

      {/* Column header */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr 1fr 1fr 12px',
          gap: 4,
          fontSize: 9,
          color: 'var(--t3)',
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          fontWeight: 700,
          paddingTop: 10,
          paddingBottom: 4,
          borderBottom: '1px solid var(--line)',
          marginBottom: 4,
        }}
      >
        <div>Metric</div>
        <div style={{ textAlign: 'right' }}>Candidate</div>
        <div style={{ textAlign: 'right' }}>Canonical</div>
        <div style={{ textAlign: 'right' }}>Δ</div>
        <div style={{ textAlign: 'center' }}>✓</div>
      </div>

      <Row
        label="Sharpe"
        candidate={fmt(candSharpe, 3)}
        canonical={fmt(canSharpe, 3)}
        delta={fmt(deltaSharpe, 3)}
        ok={sharpeOK}
      />
      <Row
        label="CAGR %"
        candidate={fmt(candCagr, 2)}
        canonical={fmt(canCagr, 2)}
        delta={fmt(deltaCagr, 2)}
        ok={cagrOK}
      />
      <Row
        label="Max DD %"
        candidate={fmt(candMaxDD, 2)}
        canonical={fmt(canMaxDD, 2)}
        delta={fmt(deltaMaxDD, 2)}
        ok={ddOK}
      />
      <Row
        label="Tot Ret %"
        candidate={fmt(candTotRet, 2)}
        canonical={fmt(canTotRet, 2)}
        delta={fmt(deltaTotRet, 2)}
        ok={null}
      />

      {/* Verdict */}
      <div
        style={{
          marginTop: 10,
          padding: '10px 12px',
          background: verdictBg,
          border: `1px solid ${verdictBorder}`,
          borderRadius: 3,
          fontSize: 10,
          color: verdictColor,
          fontWeight: 700,
          lineHeight: 1.5,
        }}
      >
        {verdictText}
      </div>

      {/* Rule + provenance footer */}
      <div style={{ fontSize: 9, color: 'var(--t3)', marginTop: 10, lineHeight: 1.6 }}>
        Rule: candidate must satisfy ALL of (a) Sharpe ≥ canonical + 0.3,
        (b) CAGR {'>'} canonical, (c) Max DD within 10% of canonical.
        Per docs/strategy_specification.md § 5.
      </div>
      <div style={{ fontSize: 9, color: 'var(--t3)', marginTop: 4 }}>
        Canonical: {canonical.strategy_name} {canonical.version_label}
        {canonical.metrics_data_through ? ` (through ${canonical.metrics_data_through})` : ''}
        {` · filter = ${canonical.filter_mode}`}
      </div>
    </div>
  );
}

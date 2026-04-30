'use client';

/**
 * AttributionPanel.tsx
 * ====================
 * Per-symbol contribution attribution: how much of the audit's total
 * Binance return came from Binance-only symbols vs BloFin-tradeable
 * symbols. Mounted in the simulator's Summary tab below the Filter
 * Confluence panel.
 *
 * Two views:
 *   - Sum-of-contributions: additive in raw-return space; what fraction
 *     of daily-return %-points came from BO symbols.
 *   - Counterfactual: compound (1 + daily_ret) vs (1 + bf_contrib) —
 *     what fraction of compounded profit would disappear if BO returns
 *     were zeroed.
 *
 * The leaderboard ranks BO symbols by total contribution, with a
 * cumulative-share column so the reader can see how concentrated the
 * BO alpha is (typically 4-6 symbols carry 75% of it).
 */

import { useEffect, useState } from 'react';

interface SymRow {
  base: string;
  days: number;
  contrib_pp: number;
  cum_share?: number | null;
}

interface AttributionData {
  job_id: string;
  computed_at: number;
  elapsed_sec: number;
  n_days: number;
  leverage: number;
  stop_raw_pct: number;
  totals: {
    total_pp: number;
    from_blofin_tradeable_pp: number;
    from_binance_only_pp: number;
    bo_pct_of_total: number | null;
  };
  compounded: {
    actual_equity: number;
    no_bo_equity: number;
    actual_profit_pct: number;
    no_bo_profit_pct: number;
    bo_share_of_compounded_profit: number | null;
  };
  bo_total_pp: number;
  bo_count: number;
  bo_leaderboard: SymRow[];
  bf_leaderboard: SymRow[];
  from_cache?: boolean;
}

interface Props {
  jobId: string;
}

function fmtPp(v: number, places = 1): string {
  if (!Number.isFinite(v)) return '—';
  const sign = v >= 0 ? '+' : '';
  return `${sign}${v.toFixed(places)}pp`;
}

function fmtPct(v: number | null | undefined, places = 1): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return '—';
  return `${(v * 100).toFixed(places)}%`;
}

function fmtX(v: number, places = 2): string {
  if (!Number.isFinite(v)) return '—';
  return `${v.toFixed(places)}×`;
}

function pctRetColor(v: number): string {
  if (v > 0.5) return 'var(--green)';
  if (v < -0.5) return 'var(--red)';
  return 'var(--t2)';
}

export default function AttributionPanel({ jobId }: Props) {
  const [data, setData] = useState<AttributionData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = async (refresh: boolean = false) => {
    if (!jobId) return;
    setError(null);
    if (refresh) setRefreshing(true); else setLoading(true);
    const apiBase = process.env.NEXT_PUBLIC_API_BASE || '';
    try {
      const url = `${apiBase}/api/jobs/${jobId}/attribution${refresh ? '?refresh=true' : ''}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const d = (await res.json()) as AttributionData;
      setData(d);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      if (cancelled) return;
      await load(false);
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

  if (loading) {
    return (
      <div style={{ fontSize: 10, color: 'var(--t3)', padding: '12px 4px', lineHeight: 1.5 }}>
        Computing per-symbol contribution attribution… first call queries
        market.futures_1m for every basket date, ~30s for a 14-month audit.
        Result is cached so subsequent loads are instant.
      </div>
    );
  }
  if (error) {
    return <div style={{ fontSize: 10, color: 'var(--red)', padding: '12px 4px' }}>Attribution: {error}</div>;
  }
  if (!data) return null;

  const { totals, compounded, bo_leaderboard, bf_leaderboard, n_days, bo_count, bo_total_pp } = data;
  const totalPp = totals.total_pp;
  const boPp = totals.from_binance_only_pp;
  const bfPp = totals.from_blofin_tradeable_pp;

  const cellStyle: React.CSSProperties = {
    padding: '6px 8px',
    fontFamily: 'var(--font-space-mono), Space Mono, monospace',
    fontSize: 10,
    color: 'var(--t1)',
    borderBottom: '1px solid var(--line)',
  };
  const headStyle: React.CSSProperties = {
    ...cellStyle,
    color: 'var(--t3)',
    fontSize: 9,
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    borderBottom: '1px solid var(--line2)',
  };

  return (
    <div style={{ fontFamily: 'var(--font-space-mono), Space Mono, monospace' }}>
      {/* Lede */}
      <div style={{ padding: '8px 4px 14px 4px', fontSize: 11, color: 'var(--t1)', lineHeight: 1.6 }}>
        Of the strategy&apos;s total Binance return,{' '}
        <span style={{ color: 'var(--t0)', fontWeight: 700 }}>
          {fmtPct(totals.bo_pct_of_total, 1)}
        </span>{' '}
        of daily return-points came from{' '}
        <span style={{ color: 'rgba(70,130,180,0.95)', fontWeight: 700 }}>Binance-only symbols</span>{' '}
        (symbols not listed on BloFin at the basket date). On a compounded basis,{' '}
        <span style={{ color: 'var(--t0)', fontWeight: 700 }}>
          {fmtPct(compounded.bo_share_of_compounded_profit, 1)}
        </span>{' '}
        of profit traces to those symbols.
        {data.from_cache && (
          <span style={{ marginLeft: 8, color: 'var(--t3)', fontSize: 9 }}>
            · cached · n={n_days} days
          </span>
        )}
        {!data.from_cache && (
          <span style={{ marginLeft: 8, color: 'var(--t3)', fontSize: 9 }}>
            · computed in {data.elapsed_sec.toFixed(1)}s · n={n_days} days
          </span>
        )}
        <button
          onClick={() => load(true)}
          disabled={refreshing}
          style={{
            marginLeft: 8,
            background: 'transparent',
            border: '1px solid var(--line2)',
            borderRadius: 2,
            color: 'var(--t2)',
            fontFamily: 'inherit',
            fontSize: 8,
            padding: '1px 6px',
            cursor: refreshing ? 'wait' : 'pointer',
            letterSpacing: '0.06em',
            textTransform: 'uppercase',
          }}
        >
          {refreshing ? 'Recomputing…' : 'Recompute'}
        </button>
      </div>

      {/* Headlines side-by-side */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 14 }}>
        {/* Sum-of-contributions */}
        <div style={{ border: '1px solid var(--line2)', borderRadius: 3, padding: '10px 12px', background: 'var(--bg0)' }}>
          <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 6 }}>
            Sum-of-Contributions (Additive)
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr auto', gap: '4px 12px', alignItems: 'center', fontSize: 11 }}>
            <span style={{ color: 'var(--t2)' }}>Total</span>
            <span />
            <span style={{ color: pctRetColor(totalPp), fontWeight: 700 }}>{fmtPp(totalPp, 2)}</span>

            <span style={{ color: 'rgba(70,130,180,0.95)' }}>From Binance-only</span>
            <span style={{ background: 'rgba(70,130,180,0.18)', height: 4, borderRadius: 1 }}>
              <div
                style={{
                  height: '100%',
                  width: `${Math.min(100, Math.abs(boPp / totalPp) * 100)}%`,
                  background: 'rgba(70,130,180,0.95)',
                  borderRadius: 1,
                }}
              />
            </span>
            <span style={{ color: pctRetColor(boPp), fontWeight: 700 }}>
              {fmtPp(boPp, 1)}{' '}
              <span style={{ color: 'var(--t3)', fontWeight: 400 }}>
                ({totals.bo_pct_of_total != null ? `${(totals.bo_pct_of_total * 100).toFixed(1)}%` : '—'})
              </span>
            </span>

            <span style={{ color: 'var(--green)' }}>From BloFin-tradeable</span>
            <span style={{ background: 'rgba(0,200,150,0.18)', height: 4, borderRadius: 1 }}>
              <div
                style={{
                  height: '100%',
                  width: `${Math.min(100, Math.abs(bfPp / totalPp) * 100)}%`,
                  background: 'rgba(0,200,150,0.95)',
                  borderRadius: 1,
                }}
              />
            </span>
            <span style={{ color: pctRetColor(bfPp), fontWeight: 700 }}>
              {fmtPp(bfPp, 1)}{' '}
              <span style={{ color: 'var(--t3)', fontWeight: 400 }}>
                ({totals.bo_pct_of_total != null ? `${((1 - totals.bo_pct_of_total) * 100).toFixed(1)}%` : '—'})
              </span>
            </span>
          </div>
        </div>

        {/* Counterfactual */}
        <div style={{ border: '1px solid var(--line2)', borderRadius: 3, padding: '10px 12px', background: 'var(--bg0)' }}>
          <div style={{ fontSize: 9, color: 'var(--t3)', textTransform: 'uppercase', letterSpacing: '0.12em', fontWeight: 700, marginBottom: 6 }}>
            Counterfactual (BO Returns Zeroed)
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr auto', gap: '4px 12px', alignItems: 'center', fontSize: 11 }}>
            <span style={{ color: 'var(--t2)' }}>Actual final equity</span>
            <span />
            <span style={{ color: 'var(--green)', fontWeight: 700 }}>{fmtX(compounded.actual_equity)}</span>

            <span style={{ color: 'var(--t2)' }}>Without BO contributions</span>
            <span />
            <span style={{ color: pctRetColor(compounded.no_bo_profit_pct), fontWeight: 700 }}>
              {fmtX(compounded.no_bo_equity)}
            </span>

            <span style={{ color: 'var(--t2)' }}>BO share of profit</span>
            <span />
            <span style={{ color: 'var(--t0)', fontWeight: 700 }}>
              {fmtPct(compounded.bo_share_of_compounded_profit, 1)}
            </span>

            <span style={{ color: 'var(--t3)', fontSize: 9, gridColumn: '1 / -1', marginTop: 4, lineHeight: 1.5 }}>
              {compounded.bo_share_of_compounded_profit != null && compounded.bo_share_of_compounded_profit > 1
                ? "Over 100% — BloFin-tradeable subset alone is a net drag at the basket's 1/N weights."
                : 'Same basket as Binance, with BO symbols contributing 0 each day.'}
            </span>
          </div>
        </div>
      </div>

      {/* BO leaderboard */}
      <div style={{ marginBottom: 12 }}>
        <div
          style={{
            fontSize: 9,
            color: 'var(--t3)',
            textTransform: 'uppercase',
            letterSpacing: '0.12em',
            fontWeight: 700,
            marginBottom: 6,
          }}
        >
          Top Binance-Only Contributors ({bo_count} symbols, total {fmtPp(bo_total_pp, 1)})
        </div>
        <div style={{ overflow: 'auto', maxHeight: 360, border: '1px solid var(--line2)', borderRadius: 3 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead style={{ position: 'sticky', top: 0, background: 'var(--bg2)' }}>
              <tr>
                <th style={{ ...headStyle, textAlign: 'left' }}>#</th>
                <th style={{ ...headStyle, textAlign: 'left' }}>Symbol</th>
                <th style={headStyle}>Days</th>
                <th style={headStyle}>Contrib (pp)</th>
                <th style={headStyle}>Cum Share</th>
              </tr>
            </thead>
            <tbody>
              {bo_leaderboard.slice(0, 25).map((r, i) => (
                <tr key={r.base} style={{ background: i < 4 ? 'rgba(70,130,180,0.08)' : 'transparent' }}>
                  <td style={{ ...cellStyle, color: 'var(--t3)', textAlign: 'left' }}>{i + 1}</td>
                  <td style={{ ...cellStyle, textAlign: 'left', color: i < 4 ? 'var(--t0)' : 'var(--t1)', fontWeight: i < 4 ? 700 : 400 }}>
                    {r.base}
                  </td>
                  <td style={{ ...cellStyle, textAlign: 'right', color: 'var(--t2)' }}>{r.days}</td>
                  <td style={{ ...cellStyle, textAlign: 'right', color: pctRetColor(r.contrib_pp), fontWeight: 700 }}>
                    {fmtPp(r.contrib_pp, 2)}
                  </td>
                  <td style={{ ...cellStyle, textAlign: 'right', color: 'var(--t1)' }}>
                    {r.cum_share != null ? `${(r.cum_share * 100).toFixed(1)}%` : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* BF leaderboard (collapsed by default — reference data) */}
      <details style={{ border: '1px solid var(--line2)', borderRadius: 3, padding: '6px 10px', background: 'var(--bg0)' }}>
        <summary
          style={{
            cursor: 'pointer',
            fontSize: 9,
            color: 'var(--t3)',
            textTransform: 'uppercase',
            letterSpacing: '0.12em',
            fontWeight: 700,
            userSelect: 'none',
          }}
        >
          Top BloFin-Tradeable Contributors ({bf_leaderboard.length} symbols)
        </summary>
        <div style={{ marginTop: 8, overflow: 'auto', maxHeight: 280 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead style={{ position: 'sticky', top: 0, background: 'var(--bg0)' }}>
              <tr>
                <th style={{ ...headStyle, textAlign: 'left' }}>#</th>
                <th style={{ ...headStyle, textAlign: 'left' }}>Symbol</th>
                <th style={headStyle}>Days</th>
                <th style={headStyle}>Contrib (pp)</th>
              </tr>
            </thead>
            <tbody>
              {bf_leaderboard.slice(0, 25).map((r, i) => (
                <tr key={r.base}>
                  <td style={{ ...cellStyle, color: 'var(--t3)', textAlign: 'left' }}>{i + 1}</td>
                  <td style={{ ...cellStyle, textAlign: 'left' }}>{r.base}</td>
                  <td style={{ ...cellStyle, textAlign: 'right', color: 'var(--t2)' }}>{r.days}</td>
                  <td style={{ ...cellStyle, textAlign: 'right', color: pctRetColor(r.contrib_pp), fontWeight: 700 }}>
                    {fmtPp(r.contrib_pp, 2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </div>
  );
}

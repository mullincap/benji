/**
 * Shared formatting and conversion utilities used across audit UI components.
 */

/** Safely coerce an unknown value to a finite number, or null. */
export function asNum(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

/** Format a number with N decimal places, or 'N/A'. */
export function fmt(v: number | null | undefined, decimals = 3): string {
  if (v === null || v === undefined) return 'N/A';
  return v.toFixed(decimals);
}

/** Format a number as a percentage with 2 decimal places (e.g. "12.34%"). */
export function fmtPercent2(v: unknown): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number')
    return `${new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(v)}%`;
  const n = Number(v);
  if (Number.isFinite(n))
    return `${new Intl.NumberFormat(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(n)}%`;
  return String(v);
}

/** Format a number as a percentage with 1 decimal place. */
export function fmtPercent1(v: unknown): string {
  if (v === null || v === undefined) return 'N/A';
  if (typeof v === 'number') return `${v.toFixed(1)}%`;
  const n = Number(v);
  if (Number.isFinite(n)) return `${n.toFixed(1)}%`;
  return String(v);
}

/** Format as a rounded integer with locale grouping, or 'N/A'. */
export function fmtInt(v: unknown): string {
  const n = asNum(v);
  if (n === null) return 'N/A';
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 0,
  }).format(Math.round(n));
}

/** Normalize a filter label for consistent comparison/display. */
export function normalizeFilterLabel(s: string): string {
  return s
    .toLowerCase()
    .replace(/\+/g, 'p')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

/** Strip scorecard prefix patterns (e.g. "A -", "B+") from a normalized label. */
export function normalizeFilterLabelCore(s: string): string {
  const base = normalizeFilterLabel(s);
  return base.replace(/^[a-f](?:\s*[+-])?\s+/, '').trim();
}

/** Return a CSS color variable based on metric key + numeric value. */
export function metricColor(key: string, value: unknown): string {
  const v = asNum(value);
  if (v === null) return 'var(--t2)';
  const heuristics: Record<string, (n: number) => string> = {
    sharpe:       (n) => (n > 1 ? 'var(--green)' : n > 0.5 ? 'var(--amber)' : 'var(--red)'),
    cagr:         (n) => (n > 0 ? 'var(--green)' : n > -5 ? 'var(--amber)' : 'var(--red)'),
    max_dd:       (n) => (n > -20 ? 'var(--green)' : n > -30 ? 'var(--amber)' : 'var(--red)'),
    active:       (n) => (n > 90 ? 'var(--green)' : n > 30 ? 'var(--amber)' : 'var(--red)'),
    tot_ret:      (n) => (n > 0 ? 'var(--green)' : n > -10 ? 'var(--amber)' : 'var(--red)'),
    compounded_ret: (n) => (n > 0 ? 'var(--green)' : n > -10 ? 'var(--amber)' : 'var(--red)'),
    grade:        (n) => (n >= 80 ? 'var(--green)' : n >= 65 ? 'var(--amber)' : 'var(--red)'),
    sortino:      (n) => (n > 1 ? 'var(--green)' : n > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar:       (n) => (n > 1 ? 'var(--green)' : n > 0.5 ? 'var(--amber)' : 'var(--red)'),
    calmar_ratio: (n) => (n > 1 ? 'var(--green)' : n > 0.5 ? 'var(--amber)' : 'var(--red)'),
    omega:        (n) => (n > 1.5 ? 'var(--green)' : n > 1 ? 'var(--amber)' : 'var(--red)'),
    ulcer_index:  (n) => (n < 5 ? 'var(--green)' : n < 15 ? 'var(--amber)' : 'var(--red)'),
    fa_oos_sharpe: (n) => (n > 1 ? 'var(--green)' : n > 0.5 ? 'var(--amber)' : 'var(--red)'),
    dsr_pct:      (n) => (n > 95 ? 'var(--green)' : n > 80 ? 'var(--amber)' : 'var(--red)'),
    cv:           (n) => (n < 0.25 ? 'var(--green)' : n < 0.5 ? 'var(--amber)' : 'var(--red)'),
    flat_days:    (n) => (n < 30 ? 'var(--green)' : n < 60 ? 'var(--amber)' : 'var(--red)'),
    avg_win_loss: (n) => (n >= 1.5 ? 'var(--green)' : n >= 1 ? 'var(--amber)' : 'var(--red)'),
    profit_factor: (n) => (n >= 1.5 ? 'var(--green)' : n >= 1 ? 'var(--amber)' : 'var(--red)'),
    uw_streak:    (n) => (n <= 20 ? 'var(--green)' : n <= 60 ? 'var(--amber)' : 'var(--red)'),
    avg_1m:       (n) => (n > 0 ? 'var(--green)' : n > -5 ? 'var(--amber)' : 'var(--red)'),
  };
  return heuristics[key]?.(v) ?? 'var(--t0)';
}

/**
 * frontend/app/manager/(protected)/live/types.ts
 * ================================================
 * TypeScript interfaces mirroring the Pydantic response models in
 * `backend/app/api/routes/manager_live.py`. Hand-written rather than
 * generated — the project has no Pydantic-to-TS tooling and adding one
 * is out of step-5 scope. Keep these in sync by hand when the backend
 * schemas change; the field-naming convention is snake_case to match
 * Pydantic's JSON output.
 *
 * If divergence becomes painful, the right next move is to point
 * `openapi-typescript` at the FastAPI /openapi.json — that's the
 * stable bridge from Pydantic to TS without manual sync.
 */

export type Venue = "blofin" | "binance";
export type Side = "long" | "short";
export type Source = "manual" | "strategy";
export type ConcentrationDirection = "long" | "short" | "balanced";

// ─── /api/manager/live/account ──────────────────────────────────────────

export interface AccountSnapshot {
  venue: Venue;
  connection_id: string;
  connection_label: string | null;
  snapshot_at: string | null;

  // Equity column
  total_equity_usd: number;
  available_usd: number;
  today_pnl_usd: number | null;
  today_pnl_pct: number | null;
  today_anchor_date: string | null;
  today_anchor_missing: boolean;

  // Deployed margin column
  used_margin_usd: number;
  used_margin_pct: number;
  open_position_count: number;

  // Notional column
  total_notional_usd: number;
  notional_to_equity: number;
  long_notional_usd: number;
  short_notional_usd: number;
  long_count: number;
  short_count: number;

  // Net unrealized
  unrealized_pnl_usd: number;
  unrealized_pnl_pct: number;
  green_count: number;

  // Per-position pct stats
  avg_pnl_pct: number;
  median_pnl_pct: number;
  pnl_pct_stdev: number;

  // Leverage
  avg_leverage: number;
  min_leverage: number;
  max_leverage: number;

  // Source counts
  strategy_count: number;
  manual_count: number;
}

// ─── /api/manager/live/risk ─────────────────────────────────────────────

export interface MarginLevel {
  ratio: number | null;
  liquidation_buffer_pct: number | null;
  note: string | null;
}

export interface LargestPosition {
  symbol: string;
  symbol_base: string;
  side: Side;
  notional_usd: number;
  notional_share_pct: number;
  leverage: number;
  source: Source;
  strategy_name: string | null;
}

export interface NearestStop {
  symbol: string | null;
  symbol_base: string | null;
  sl_price: number | null;
  mark_price: number | null;
  distance_pct: number | null;
}

export interface UnhedgedConcentration {
  direction: ConcentrationDirection;
  pct_of_book: number;
  constituent_symbols: string[];
  no_protective_stops: string[];
}

export interface RiskSnapshot {
  venue: Venue;
  connection_id: string;
  snapshot_at: string | null;
  margin_level: MarginLevel;
  largest_position: LargestPosition | null;
  nearest_stop: NearestStop;
  concentration: UnhedgedConcentration | null;
}

// ─── /api/manager/live/positions ────────────────────────────────────────

export interface LivePosition {
  venue: Venue;
  connection_id: string;
  connection_label: string | null;
  symbol: string;
  symbol_base: string;
  side: Side;

  size: number;
  notional_usd: number;
  leverage: number | null;
  margin_mode: string | null;

  entry_price: number | null;
  mark_price: number | null;
  unrealized_pnl_usd: number;
  unrealized_pnl_pct: number;

  source: Source;
  strategy_name: string | null;

  opened_at: string | null;
  age_seconds: number | null;

  sl_price: number | null;
  sl_distance_pct: number | null;
  tp_price: number | null;
  tp_distance_pct: number | null;
  risk_reward: number | null;

  /** Today's PnL contribution: current upl − today's anchor upl.
   *  null when no position_snapshots anchor exists for today (UI shows
   *  an "anchor missing" badge on the waterfall bar). */
  today_pnl_usd: number | null;
  today_anchor_missing: boolean;
}

export interface PositionsResponse {
  venue: Venue;
  connection_id: string;
  connection_label: string | null;
  snapshot_at: string | null;
  positions: LivePosition[];
  counts: {
    total: number;
    strategy: number;
    manual: number;
    long: number;
    short: number;
  };
}

// ─── /api/manager/live/ma-alignment (Data Dictionary §11) ──────────────

export type MaAlignmentTier =
  | "aligned-strong"
  | "aligned-mid"
  | "aligned-soft"
  | "neutral"
  | "against-soft"
  | "against-mid"
  | "against-strong";

export interface MaCell {
  distance_pct: number | null;
  ema_value: number | null;
  tier: MaAlignmentTier;
  /** null on success; otherwise 'not_listed' / 'insufficient_history' /
   *  'fetch_error' / 'no_mark' — UI renders the cell as a neutral '—'. */
  reason: string | null;
}

export interface MaRow {
  symbol: string;
  symbol_base: string;
  side: Side;
  binance_symbol: string | null;
  mark_price: number | null;
  cells: Record<string, MaCell>;
  confluence_aligned: number;
  confluence_total: number;
}

export interface MaAlignmentResponse {
  venue: Venue;
  connection_id: string;
  snapshot_at: string | null;
  timeframes: string[];
  rows: MaRow[];
}

// ─── /api/manager/live/boxplots (Data Dictionary §10) ──────────────────

export type BoxDotClass = "good" | "bad" | "neu";
export type TrendDirection =
  | "strong-up" | "up" | "flat" | "down" | "strong-down";

export interface BoxPlotCell {
  symbol: string;
  symbol_base: string;
  side: Side;
  binance_symbol: string | null;

  // Distribution (null when reason != null)
  p5: number | null;
  p25: number | null;
  p50: number | null;
  p75: number | null;
  p95: number | null;
  win_min: number | null;
  win_max: number | null;

  // Live overlay
  mark_price: number | null;
  entry_price: number | null;
  mark_dot: BoxDotClass;

  // Trend
  slope_sigma: number | null;
  trend_direction: TrendDirection | null;
  trend_color: BoxDotClass;

  // Diagnostics
  last_close_ts: number | null;
  reason: string | null;
}

export interface BoxPlotsResponse {
  venue: Venue;
  connection_id: string;
  snapshot_at: string | null;
  cells: BoxPlotCell[];
}

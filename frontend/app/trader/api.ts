/**
 * Allocator API client — all backend calls for the trader module.
 *
 * Uses the same fetch + credentials pattern as the rest of the app.
 * API_BASE comes from NEXT_PUBLIC_API_BASE (.env.local).
 */

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

async function apiFetch<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    credentials: "include",
    headers: { "Content-Type": "application/json", ...opts?.headers },
    ...opts,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`API ${res.status}: ${body}`);
  }
  return res.json();
}

// ── Types ───────────────────────────────────────────────────────────────────

export interface ApiStrategy {
  strategy_id: number;
  strategy_version_id: string;
  name: string;
  display_name: string;
  description: string;
  filter_mode: string;
  capital_cap_usd: number | null;
  version_label: string;
  metrics: {
    sharpe: number | null;
    sortino: number | null;
    max_dd_pct: number | null;
    cagr_pct: number | null;
    total_return_pct: number | null;
    profit_factor: number | null;
    win_rate_daily: number | null;
    active_days: number | null;
    avg_daily_ret_pct: number | null;
    best_month_pct: number | null;
    worst_month_pct: number | null;
    scorecard_score: number | null;
    grade: string | null;
  };
  capacity: {
    allocators: number;
    deployed_usd: number;
    capacity_usd: number;
  };
}

export interface ApiExchange {
  connection_id: string;
  exchange: string;
  label: string;
  masked_key: string;
  status: string;
  last_validated_at: string | null;
  created_at: string | null;
}

export interface ApiAllocation {
  allocation_id: string;
  strategy_version_id: string;
  connection_id: string;
  capital_usd: number;
  status: string;
  strategy_name: string;
  strategy_slug: string;
  filter_mode: string;
  exchange: string;
  connection_label: string;
  equity_usd: number;
  daily_return_pct: number;
  daily_pnl_usd: number;
}

export interface ApiSnapshot {
  connection_id: string;
  exchange: string;
  label: string;
  snapshot_at: string | null;
  total_equity_usd: number | null;
  available_usd: number | null;
  used_margin_usd: number | null;
  unrealized_pnl: number | null;
  positions: ApiPosition[];
  fetch_ok: boolean | null;
  error_msg: string | null;
}

export interface ApiPosition {
  symbol: string;
  side: string;
  size: number;
  entry_price: number;
  mark_price: number;
  unrealized_pnl: number;
  leverage: number;
  margin_mode: string;
}

export interface ApiBalanceHistory {
  date: string;
  equity_usd: number;
  daily_return: number;
  drawdown: number;
}

export interface ApiPnl {
  allocation_id: string;
  capital_usd: number;
  equity_usd: number;
  all_time_pnl: number;
  daily_return_pct: number;
  daily_pnl_usd: number;
  drawdown: number;
}

// ── API calls ───────────────────────────────────────────────────────────────

export const allocatorApi = {
  // Strategies
  getStrategies: () =>
    apiFetch<{ strategies: ApiStrategy[] }>("/api/allocator/strategies"),

  // Exchanges
  getExchanges: () =>
    apiFetch<{ exchanges: ApiExchange[] }>("/api/allocator/exchanges"),

  storeExchangeKeys: (data: {
    exchange: string;
    label: string;
    api_key: string;
    api_secret: string;
    passphrase?: string;
  }) =>
    apiFetch<{ connection_id: string; exchange: string; label: string; masked_key: string; status: string }>(
      "/api/allocator/exchanges/keys",
      { method: "POST", body: JSON.stringify(data) },
    ),

  removeExchange: (connectionId: string) =>
    apiFetch<{ removed: boolean }>(`/api/allocator/exchanges/${connectionId}`, { method: "DELETE" }),

  // Snapshots (exchange balances + positions)
  getSnapshots: () =>
    apiFetch<{ snapshots: ApiSnapshot[]; total_live_equity_usd: number | null; total_unrealized_pnl: number | null }>(
      "/api/allocator/snapshots",
    ),

  refreshSnapshots: () =>
    apiFetch<{ snapshots: ApiSnapshot[]; total_live_equity_usd: number | null }>(
      "/api/allocator/snapshots/refresh",
      { method: "POST" },
    ),

  // Allocations
  getAllocations: () =>
    apiFetch<{ allocations: ApiAllocation[] }>("/api/allocator/allocations"),

  createAllocation: (data: { strategy_version_id: string; connection_id: string; capital_usd: number }) =>
    apiFetch<{ allocation_id: string; status: string }>(
      "/api/allocator/allocations",
      { method: "POST", body: JSON.stringify(data) },
    ),

  updateAllocation: (allocationId: string, data: { capital_usd?: number; status?: string }) =>
    apiFetch<{ updated: boolean }>(
      `/api/allocator/allocations/${allocationId}`,
      { method: "PATCH", body: JSON.stringify(data) },
    ),

  deleteAllocation: (allocationId: string) =>
    apiFetch<{ closed: boolean }>(`/api/allocator/allocations/${allocationId}`, { method: "DELETE" }),

  // Trader data (per allocation)
  getBalanceHistory: (allocationId: string) =>
    apiFetch<{ allocation_id: string; history: ApiBalanceHistory[] }>(
      `/api/allocator/trader/${allocationId}/balance-history`,
    ),

  getPnl: (allocationId: string) =>
    apiFetch<ApiPnl>(`/api/allocator/trader/${allocationId}/pnl`),

  getPositions: (allocationId: string) =>
    apiFetch<{ allocation_id: string; connection_id: string; exchange: string; snapshot_at: string | null; positions: ApiPosition[] }>(
      `/api/allocator/trader/${allocationId}/positions`,
    ),
};

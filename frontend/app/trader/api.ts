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

/**
 * Extract {status, detail} from errors thrown by apiFetch.
 *
 * CRITICAL — never render raw response bodies. FastAPI's 422 validation
 * responses include the submitted request payload under `detail[i].input`,
 * which can contain API keys, secrets, and passphrases. When the detail is
 * a Pydantic error array, extract only the `msg` + `loc` fields. Fall back
 * to a generic message if parsing fails — never expose the raw JSON.
 */
export function parseApiError(err: unknown): { status: number; detail: string } {
  if (!(err instanceof Error)) return { status: 0, detail: "Request failed" };
  const match = err.message.match(/^API (\d+):\s*([\s\S]*)$/);
  if (!match) return { status: 0, detail: err.message };
  const status = parseInt(match[1], 10);
  const rawBody = match[2];

  let detail = "Request failed";
  try {
    const body = JSON.parse(rawBody);
    if (body && typeof body.detail === "string") {
      detail = body.detail;
    } else if (body && Array.isArray(body.detail)) {
      // Pydantic validation errors — extract msg + loc, drop `input` (secrets).
      detail = body.detail
        .map((e: unknown) => {
          if (!e || typeof e !== "object") return "Validation error";
          const entry = e as { loc?: unknown; msg?: unknown };
          const loc = Array.isArray(entry.loc)
            ? entry.loc.filter(x => x !== "body").join(".")
            : "";
          const msg = typeof entry.msg === "string" ? entry.msg : "Validation error";
          return loc ? `${loc}: ${msg}` : msg;
        })
        .join("; ");
    }
    // Any other body shape falls through to the "Request failed" default.
    // Never dump rawBody — it may contain echoed secrets.
  } catch {
    // Body was not JSON — it's either already a clean server message or
    // something unexpected. Return a generic string, not the raw text.
    detail = "Request failed";
  }
  return { status, detail };
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
  is_published: boolean;
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

export type ExchangeSlug = "binance" | "blofin";

export type ExchangeStatus =
  | "active"
  | "pending_validation"
  | "invalid"
  | "errored"
  | "revoked";

export interface ExchangePermissions {
  read: boolean | null;
  spot_trade: boolean | null;
  futures_trade: boolean | null;
  withdrawals: boolean | null;
}

export interface ApiExchange {
  connection_id: string;
  exchange: string;              // ExchangeSlug at runtime, string for legacy rows
  label: string | null;
  masked_key: string;
  status: ExchangeStatus;
  last_validated_at: string | null;
  last_error_msg: string | null;
  permissions: ExchangePermissions | null;
  balance: number;
  created_at: string | null;
}

export interface StoreKeysRequest {
  exchange: ExchangeSlug;
  label?: string;
  api_key: string;
  api_secret: string;
  passphrase?: string;           // required iff exchange === "blofin"
}

export interface StoreKeysSuccess {
  connection_id: string;
  exchange: ExchangeSlug;
  label: string | null;
  masked_key: string;
  status: "active";
  permissions: ExchangePermissions;
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
  getStrategies: (opts?: { includeRetired?: boolean }) =>
    apiFetch<{ strategies: ApiStrategy[] }>(
      `/api/allocator/strategies${opts?.includeRetired ? "?include_retired=true" : ""}`,
    ),

  publishStrategy: (strategyId: number) =>
    apiFetch<{ strategy_id: number; name: string; display_name: string; is_published: boolean }>(
      `/api/allocator/strategies/${strategyId}/publish`,
      { method: "POST" },
    ),

  unpublishStrategy: (strategyId: number) =>
    apiFetch<{ strategy_id: number; name: string; display_name: string; is_published: boolean }>(
      `/api/allocator/strategies/${strategyId}/unpublish`,
      { method: "POST" },
    ),

  renameStrategy: (strategyId: number, displayName: string, allowDuplicate = false) =>
    apiFetch<{ strategy_id: number; name: string; display_name: string; is_published: boolean }>(
      `/api/allocator/strategies/${strategyId}/rename`,
      {
        method: "POST",
        body: JSON.stringify({ display_name: displayName, allow_duplicate: allowDuplicate }),
      },
    ),

  // Exchanges
  getExchanges: () =>
    apiFetch<{ exchanges: ApiExchange[] }>("/api/allocator/exchanges"),

  storeExchangeKeys: (data: StoreKeysRequest) =>
    apiFetch<StoreKeysSuccess>(
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

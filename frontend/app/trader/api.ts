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
  is_canonical: boolean;
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
  // Exchange-level principal anchor (migration 010). NULL when the operator
  // hasn't set explicit values; UI falls back to "earliest event date" or
  // connection created_at as the display default.
  principal_anchor_at: string | null;
  principal_baseline_usd: number | null;
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

export type CompoundingMode = "compound" | "fixed";

export interface ApiAllocation {
  allocation_id: string;
  strategy_version_id: string;
  connection_id: string;
  capital_usd: number;
  status: string;
  compounding_mode: CompoundingMode;
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
  contract_value?: number;
  notional_usd?: number;
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
  session_start_equity_usd: number | null;
  session_pnl_usd: number | null;
  session_return_pct: number | null;
  session_capital_net_usd: number;
  initial_equity_usd: number | null;
  total_pnl_usd: number;
  total_return_pct: number;
  lifetime_capital_net_usd: number;
  // EXCHANGE-level principal (migration 010). Shared across all
  // allocations on the same exchange wallet. Anchor defaults to the
  // allocation's created_at when no explicit override; baseline defaults
  // to 0 (so principal = SUM(capital events since anchor) when no
  // baseline is set).
  principal_usd: number;
  principal_baseline_usd: number;
  principal_anchor_at: string | null;       // ISO 8601
  principal_anchor_explicit: boolean;       // TRUE = operator set it on the connection
  principal_baseline_explicit: boolean;
  net_since_anchor_usd: number;
  // total_pnl_usd + total_return_pct are ALLOCATION-level now: compounded
  // daily net_return_pct from allocation_returns since the exchange anchor.
  // Not "equity − principal" anymore, because principal is a property of
  // the wallet, not of the strategy.
}

export interface ApiCapitalEvent {
  event_id:               string;
  allocation_id:          string | null;   // NULL on unmapped auto events
  connection_id:          string | null;
  event_at:               string;          // ISO 8601
  amount_usd:             number;
  kind:                   "deposit" | "withdrawal";
  notes:                  string | null;
  created_at:             string;
  source:                 "manual" | "auto" | "auto-anomaly";
  exchange_event_id:      string | null;   // present when source != 'manual'
  is_manually_overridden: boolean;         // operator has touched this row
  exchange_name:          string | null;   // for unmapped events: which exchange surfaced it
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

  promoteCanonical: (strategyId: number) =>
    apiFetch<{
      strategy_id: number;
      name: string;
      display_name: string;
      is_canonical: boolean;
      demoted: { strategy_id: number; display_name: string } | null;
    }>(
      `/api/allocator/strategies/${strategyId}/promote-canonical`,
      { method: "POST" },
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

  updateAllocation: (
    allocationId: string,
    data: {
      capital_usd?: number;
      status?: string;
      compounding_mode?: CompoundingMode;
    },
  ) =>
    apiFetch<{ updated: boolean }>(
      `/api/allocator/allocations/${allocationId}`,
      { method: "PATCH", body: JSON.stringify(data) },
    ),

  // Exchange-level principal anchor + baseline (migration 010). One anchor
  // per exchange wallet, shared by any allocations on the connection.
  updateConnection: (
    connectionId: string,
    data: {
      principal_anchor_at?: string;        // ISO 8601
      principal_baseline_usd?: number;
      clear_principal_anchor?: boolean;    // send true to revert to default
      clear_principal_baseline?: boolean;
    },
  ) =>
    apiFetch<{ updated: boolean; connection_id: string }>(
      `/api/allocator/connections/${connectionId}`,
      { method: "PATCH", body: JSON.stringify(data) },
    ),

  deleteAllocation: (allocationId: string) =>
    apiFetch<{ closed: boolean }>(`/api/allocator/allocations/${allocationId}`, { method: "DELETE" }),

  closeAllocationPositions: (allocationId: string) =>
    apiFetch<{
      closed: boolean;
      allocation_id: string;
      attempted: number;
      closed_ok: number;
      failed: string[];
      note?: string;
    }>(`/api/allocator/allocations/${allocationId}/close-positions`, { method: "POST" }),

  // Trader data (per allocation)
  getBalanceHistory: (allocationId: string, range?: "1D" | "1W" | "1M" | "ALL") =>
    apiFetch<{ allocation_id: string; history: ApiBalanceHistory[] }>(
      `/api/allocator/trader/${allocationId}/balance-history${range ? `?range=${range}` : ""}`,
    ),

  // Aggregate equity across all of the current user's exchange connections
  getAccountBalanceSeries: (range?: "1D" | "1W" | "1M" | "ALL") =>
    apiFetch<{ range: string; bucket_seconds: number; connections_included: number; history: ApiBalanceHistory[] }>(
      `/api/allocator/account-balance-series${range ? `?range=${range}` : ""}`,
    ),

  getPnl: (allocationId: string) =>
    apiFetch<ApiPnl>(`/api/allocator/trader/${allocationId}/pnl`),

  getPositions: (allocationId: string) =>
    apiFetch<{ allocation_id: string; connection_id: string; exchange: string; snapshot_at: string | null; positions: ApiPosition[] }>(
      `/api/allocator/trader/${allocationId}/positions`,
    ),

  // Capital events (manual deposits/withdrawals — operator-recorded)
  getCapitalEvents: (allocationId?: string) =>
    apiFetch<{ events: ApiCapitalEvent[] }>(
      `/api/allocator/capital-events${allocationId ? `?allocation_id=${allocationId}` : ""}`,
    ),

  createCapitalEvent: (data: {
    allocation_id?: string;        // allocation_id OR connection_id required
    connection_id?: string;
    amount_usd: number;
    kind: "deposit" | "withdrawal";
    event_at?: string;
    notes?: string;
  }) =>
    apiFetch<{ event_id: string; allocation_id: string }>(
      "/api/allocator/capital-events",
      { method: "POST", body: JSON.stringify(data) },
    ),

  updateCapitalEvent: (
    eventId: string,
    data: {
      allocation_id?: string;
      amount_usd?: number;
      kind?: "deposit" | "withdrawal";
      event_at?: string;
      notes?: string;
      clear_allocation?: boolean;   // explicit "send back to Unassigned"
    },
  ) =>
    apiFetch<{ updated: boolean; event_id: string }>(
      `/api/allocator/capital-events/${eventId}`,
      { method: "PATCH", body: JSON.stringify(data) },
    ),

  deleteCapitalEvent: (eventId: string) =>
    apiFetch<{ deleted: boolean; event_id: string }>(
      `/api/allocator/capital-events/${eventId}`,
      { method: "DELETE" },
    ),

  // Wipe operator-authored capital events (manual entries + manual
  // overrides on auto rows) and re-sync from the exchange.
  // When connectionId is provided, scope to just that connection;
  // omitting it wipes everything the caller owns.
  resetCapitalEventsToDefaults: (connectionId?: string) =>
    apiFetch<{
      reset: boolean;
      connection_id: string | null;
      deleted_rows: number;
      connections_repolled: number;
    }>(
      `/api/allocator/capital-events/reset-defaults${
        connectionId ? `?connection_id=${connectionId}` : ""
      }`,
      { method: "POST" },
    ),
};

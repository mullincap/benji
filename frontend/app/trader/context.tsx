"use client";

import { createContext, useContext, useState, useCallback, useEffect, ReactNode } from "react";
import {
  allocatorApi, ApiStrategy, ApiExchange, ApiAllocation, ApiSnapshot,
  ExchangePermissions, ExchangeStatus,
} from "./api";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface Exchange {
  id: string;                    // connection_id from backend
  exchange: string;              // canonical slug: "binance" | "blofin"
  name: string;                  // display label (falls back to exchange if null)
  maskedKey: string;
  lastSynced: string;
  balance: number;               // total_equity_usd from latest snapshot
  status: ExchangeStatus;
  lastErrorMsg: string | null;
  permissions: ExchangePermissions | null;  // null for grandfathered rows
  lastValidatedAt: string | null;
}

export interface Position {
  symbol: string;
  side: "LONG" | "SHORT" | "NET";
  size: number;
  entry: number;
  mark: number;
  pnl: number;
  pnlPct: number;
  notionalUsd?: number;
  leverage?: number;
}

export type StrategyType = string;
export type RiskLevel = "low" | "medium" | "high";

export type InstanceStatus = "unlinked" | "live" | "paused";

export interface StrategyInstance {
  id: string;                // allocation_id from backend (or temp id for unlinked)
  strategyType: StrategyType;
  strategyName: string;
  exchangeId: string | null;
  exchangeName: string | null;
  risk: RiskLevel;
  status: InstanceStatus;
  alerts: boolean;
  allocation: number | null;
  equity: number;
  dailyPnl: number;
  compoundingMode: "compound" | "fixed";
  positions: Position[];
  // Backend references
  strategyVersionId?: string;
  connectionId?: string;
}

// ─── Strategy catalog (fetched from backend) ────────────────────────────────

export interface StrategyCatalogEntry {
  name: string;
  risk: RiskLevel;
  description: string;
  sharpe: number;
  maxDd: number;
  winRate: number;
  ytd: number;
  cagr: number;
  profitFactor: number;
  avg1m: number;
  activeDays: number;
  vol: number;
  simpleReturn: number;
  compoundedReturn: number;
  avgWinLoss: number;
  // Backend references
  strategyId: number;
  strategyVersionId: string;
  capitalCapUsd: number | null;
  isPublished: boolean;
  isCanonical: boolean;
}

export interface CapacityData {
  allocators: number;
  deployed: number;
  capacity: number;
}

// Mutable catalog populated from the API
export let STRATEGY_CATALOG: Record<string, StrategyCatalogEntry> = {};
export let CAPACITY_DATA: Record<string, CapacityData> = {};

function riskFromFilterMode(filterMode: string): RiskLevel {
  const lower = (filterMode || "").toLowerCase();
  if (lower.includes("low") || lower.includes("conservative")) return "low";
  if (lower.includes("high") || lower.includes("aggressive")) return "high";
  return "medium";
}

function mapApiStrategyToCatalog(s: ApiStrategy): StrategyCatalogEntry {
  const m = s.metrics;
  return {
    name: s.display_name,
    risk: riskFromFilterMode(s.filter_mode),
    description: s.description || "",
    sharpe: m.sharpe ?? 0,
    maxDd: Math.abs(m.max_dd_pct ?? 0),
    winRate: (m.win_rate_daily ?? 0) * 100,
    ytd: m.total_return_pct ?? 0,
    cagr: m.cagr_pct ?? 0,
    profitFactor: m.profit_factor ?? 0,
    avg1m: (m.avg_daily_ret_pct ?? 0) * 21, // approximate monthly
    activeDays: m.active_days ?? 0,
    vol: 0, // not directly available from this query
    simpleReturn: m.total_return_pct ?? 0,
    compoundedReturn: m.cagr_pct ?? 0,
    avgWinLoss: m.profit_factor ?? 0,
    strategyId: s.strategy_id,
    strategyVersionId: s.strategy_version_id,
    capitalCapUsd: s.capital_cap_usd,
    isPublished: s.is_published,
    isCanonical: s.is_canonical ?? false,
  };
}

// ─── Fallback catalog (used while loading or if API fails) ──────────────────

const FALLBACK_CATALOG: Record<string, StrategyCatalogEntry> = {
  "alpha-low": {
    name: "Alpha Low", risk: "low",
    description: "Conservative capital preservation strategy with tight drawdown controls and reduced position sizing.",
    sharpe: 1.84, maxDd: 8.2, winRate: 61, ytd: 14.2, cagr: 18.7,
    profitFactor: 1.88, avg1m: 1.2, activeDays: 312, vol: 6.4,
    simpleReturn: 14.2, compoundedReturn: 18.7, avgWinLoss: 1.42,
    strategyId: 0, strategyVersionId: "", capitalCapUsd: null, isPublished: true, isCanonical: false,
  },
  "alpha-mid": {
    name: "Alpha Mid", risk: "medium",
    description: "Balanced trend-following approach with dynamic position sizing that scales with conviction.",
    sharpe: 2.63, maxDd: 14.6, winRate: 63, ytd: 38.2, cagr: 42.1,
    profitFactor: 2.25, avg1m: 2.4, activeDays: 342, vol: 11.8,
    simpleReturn: 38.2, compoundedReturn: 42.1, avgWinLoss: 1.71,
    strategyId: 0, strategyVersionId: "", capitalCapUsd: null, isPublished: true, isCanonical: false,
  },
  "alpha-high": {
    name: "Alpha High", risk: "high",
    description: "Aggressive momentum capture across top-20 pairs with concentrated position sizing.",
    sharpe: 3.90, maxDd: 19.9, winRate: 67, ytd: 91.4, cagr: 187.3,
    profitFactor: 3.09, avg1m: 3.2, activeDays: 358, vol: 18.2,
    simpleReturn: 91.4, compoundedReturn: 187.3, avgWinLoss: 2.34,
    strategyId: 0, strategyVersionId: "", capitalCapUsd: null, isPublished: true, isCanonical: false,
  },
};

const FALLBACK_CAPACITY: Record<string, CapacityData> = {
  "alpha-low":  { allocators: 0, deployed: 0, capacity: 1000000 },
  "alpha-mid":  { allocators: 0, deployed: 0, capacity: 1000000 },
  "alpha-high": { allocators: 0, deployed: 0, capacity: 1000000 },
};

// ─── Mock positions (fallback for ghost/demo states) ────────────────────────

export const MOCK_POSITIONS_A: Position[] = [
  { symbol: "BTCUSDT", side: "LONG",  size: 0.25, entry: 61840, mark: 63420, pnl: 395.0,  pnlPct: 2.55 },
  { symbol: "ETHUSDT", side: "SHORT", size: 1.80, entry: 3280,  mark: 3195,  pnl: 153.0,  pnlPct: 2.59 },
  { symbol: "SOLUSDT", side: "LONG",  size: 12.0, entry: 148.5, mark: 144.2, pnl: -51.6,  pnlPct: -2.89 },
];

export const MOCK_POSITIONS_B: Position[] = [
  { symbol: "BTCUSDT", side: "LONG",  size: 0.18, entry: 62100, mark: 63420, pnl: 237.6,  pnlPct: 2.13 },
  { symbol: "BNBUSDT", side: "LONG",  size: 3.5,  entry: 572.0, mark: 589.5, pnl: 61.25,  pnlPct: 3.06 },
];

// ─── Context ─────────────────────────────────────────────────────────────────

interface TraderState {
  // Data
  exchanges: Exchange[];
  instances: StrategyInstance[];

  // Loading / error
  loading: boolean;
  error: string | null;

  // Admin-only: include retired strategies in the catalog
  includeRetired: boolean;
  setIncludeRetired: (v: boolean) => void;

  // Mutations (keep same interface for components)
  addExchange: (e: Exchange) => void;
  removeExchange: (id: string) => void;
  addInstance: (i: StrategyInstance) => void;
  updateInstance: (id: string, patch: Partial<StrategyInstance>) => void;
  removeInstance: (id: string) => void;

  // Refresh from backend
  refresh: () => Promise<void>;
}

const TraderContext = createContext<TraderState | null>(null);

function mergeExchange(apiEx: ApiExchange, snap: ApiSnapshot | undefined): Exchange {
  const lastSynced = snap?.snapshot_at
    ? formatTimeDiff(new Date(snap.snapshot_at))
    : (apiEx.last_validated_at ? formatTimeDiff(new Date(apiEx.last_validated_at)) : "never");
  return {
    id: apiEx.connection_id,
    exchange: apiEx.exchange,
    name: apiEx.label || apiEx.exchange,
    maskedKey: apiEx.masked_key,
    lastSynced,
    balance: snap?.total_equity_usd ?? apiEx.balance ?? 0,
    status: apiEx.status,
    lastErrorMsg: apiEx.last_error_msg,
    permissions: apiEx.permissions,
    lastValidatedAt: apiEx.last_validated_at,
  };
}

function formatTimeDiff(date: Date): string {
  const diff = Date.now() - date.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function mapApiPosition(p: { symbol: string; side: string; size: number; entry_price: number; mark_price: number; unrealized_pnl: number }): Position {
  const entry = p.entry_price;
  const mark = p.mark_price;
  const pnlPct = entry > 0 ? ((mark - entry) / entry) * 100 : 0;
  return {
    symbol: p.symbol,
    side: (p.side || "long").toUpperCase() as "LONG" | "SHORT",
    size: p.size,
    entry,
    mark,
    pnl: p.unrealized_pnl,
    pnlPct: Math.round(pnlPct * 100) / 100,
  };
}

export function TraderProvider({ children }: { children: ReactNode }) {
  const [exchanges, setExchanges] = useState<Exchange[]>([]);
  const [instances, setInstances] = useState<StrategyInstance[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [includeRetired, setIncludeRetired] = useState(false);

  const refresh = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);

      // Fetch all data in parallel
      const [strategiesRes, exchangesRes, snapshotsRes, allocationsRes] = await Promise.all([
        allocatorApi.getStrategies({ includeRetired }).catch(() => ({ strategies: [] as ApiStrategy[] })),
        allocatorApi.getExchanges().catch(() => ({ exchanges: [] as ApiExchange[] })),
        allocatorApi.getSnapshots().catch(() => ({ snapshots: [] as ApiSnapshot[], total_live_equity_usd: null, total_unrealized_pnl: null })),
        allocatorApi.getAllocations().catch(() => ({ allocations: [] as ApiAllocation[] })),
      ]);

      // Build strategy catalog from API data
      const newCatalog: Record<string, StrategyCatalogEntry> = {};
      const newCapacity: Record<string, CapacityData> = {};
      for (const s of strategiesRes.strategies) {
        newCatalog[s.name] = mapApiStrategyToCatalog(s);
        newCapacity[s.name] = {
          allocators: s.capacity.allocators,
          deployed: s.capacity.deployed_usd,
          capacity: s.capacity.capacity_usd,
        };
      }
      // Use API data if available, fallback otherwise
      STRATEGY_CATALOG = Object.keys(newCatalog).length > 0 ? newCatalog : FALLBACK_CATALOG;
      CAPACITY_DATA = Object.keys(newCapacity).length > 0 ? newCapacity : FALLBACK_CAPACITY;

      // Build snapshot lookup by connection_id (used purely to enrich `lastSynced`
      // and override `balance` when a snapshot is fresher than last_validated_at).
      const snapByConn: Record<string, ApiSnapshot> = {};
      for (const snap of snapshotsRes.snapshots) {
        snapByConn[snap.connection_id] = snap;
      }

      // GET /exchanges is authoritative: it returns status, permissions,
      // last_error_msg, last_validated_at, balance, masked_key. Snapshots are
      // only consulted for freshness. Rows with no snapshot still render —
      // status='pending_validation' rows never have snapshots, but they still
      // belong in the list.
      const newExchanges: Exchange[] = exchangesRes.exchanges.map(apiEx =>
        mergeExchange(apiEx, snapByConn[apiEx.connection_id]),
      );

      // Build instances from allocations
      const newInstances: StrategyInstance[] = allocationsRes.allocations.map((a: ApiAllocation) => {
        const snap = snapByConn[a.connection_id];
        const positions = snap?.positions?.map(mapApiPosition) ?? [];

        // Determine risk from strategy catalog
        const catEntry = Object.values(STRATEGY_CATALOG).find(
          c => c.strategyVersionId === a.strategy_version_id
        );
        const strategyType = Object.entries(STRATEGY_CATALOG).find(
          ([, c]) => c.strategyVersionId === a.strategy_version_id
        )?.[0] ?? a.strategy_slug;

        const statusMap: Record<string, InstanceStatus> = { active: "live", paused: "paused" };

        return {
          id: a.allocation_id,
          strategyType,
          strategyName: a.strategy_name,
          exchangeId: a.connection_id,
          exchangeName: a.connection_label || a.exchange,
          risk: catEntry?.risk ?? "medium",
          status: statusMap[a.status] ?? "live",
          alerts: false,
          allocation: a.capital_usd,
          equity: a.equity_usd,
          dailyPnl: a.daily_pnl_usd,
          compoundingMode: a.compounding_mode ?? "compound",
          positions,
          strategyVersionId: a.strategy_version_id,
          connectionId: a.connection_id,
        };
      });

      setExchanges(newExchanges);
      setInstances(prev => {
        // Preserve any unlinked (local-only) instances that haven't been persisted yet
        const unlinked = prev.filter(i => i.status === "unlinked");
        return [...newInstances, ...unlinked];
      });
    } catch (err) {
      console.error("Failed to load allocator data:", err);
      setError(err instanceof Error ? err.message : "Failed to load data");
      // Use fallbacks so the UI still renders
      STRATEGY_CATALOG = FALLBACK_CATALOG;
      CAPACITY_DATA = FALLBACK_CAPACITY;
    } finally {
      setLoading(false);
    }
  }, [includeRetired]);

  useEffect(() => { refresh(); }, [refresh]);

  const addExchange = useCallback((e: Exchange) => setExchanges(prev => [...prev, e]), []);
  const removeExchange = useCallback((id: string) => setExchanges(prev => prev.filter(e => e.id !== id)), []);
  const addInstance = useCallback((i: StrategyInstance) => setInstances(prev => [...prev, i]), []);
  const updateInstance = useCallback((id: string, patch: Partial<StrategyInstance>) => {
    setInstances(prev => prev.map(i => i.id === id ? { ...i, ...patch } : i));
  }, []);
  const removeInstance = useCallback((id: string) => setInstances(prev => prev.filter(i => i.id !== id)), []);

  return (
    <TraderContext.Provider value={{
      exchanges, instances, loading, error,
      includeRetired, setIncludeRetired,
      addExchange, removeExchange, addInstance, updateInstance, removeInstance,
      refresh,
    }}>
      {children}
    </TraderContext.Provider>
  );
}

export function useTrader() {
  const ctx = useContext(TraderContext);
  if (!ctx) throw new Error("useTrader must be used within TraderProvider");
  return ctx;
}

// ─── Shared helpers ──────────────────────────────────────────────────────────

export function fmt(n: number, d = 2) {
  return n.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

export function mask(s: string, show = 6) {
  if (!s) return "";
  return s.slice(0, show) + "••••••" + s.slice(-show);
}

export const RISK_COLOR: Record<RiskLevel, string> = { low: "var(--green)", medium: "var(--t1)", high: "var(--red)" };
export const RISK_DIM: Record<RiskLevel, string> = { low: "var(--green-dim)", medium: "var(--green-dim)", high: "var(--red-dim)" };
export const RISK_MID: Record<RiskLevel, string> = { low: "var(--green-mid)", medium: "var(--green-dim)", high: "var(--red-dim)" };

// ─── Equity curve mock data (used as ghost/fallback) ────────────────────────

export const GHOST_CURVE = [
  100, 101.2, 100.8, 102.5, 103.1, 104.8, 103.9, 105.6, 107.2, 106.4,
  108.1, 109.5, 108.7, 110.3, 112.0, 111.2, 113.5, 114.8, 113.6, 115.9,
  117.2, 116.0, 114.8, 113.2, 112.5, 114.0, 116.3, 118.1, 119.7, 120.5,
  121.8, 123.4, 122.1, 124.0, 125.6, 127.3, 126.0, 128.2, 129.8, 131.5,
  130.2, 132.0, 133.7, 135.1, 134.0, 136.2, 137.8, 138.2,
];

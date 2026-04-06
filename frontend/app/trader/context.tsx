"use client";

import { createContext, useContext, useState, useCallback, ReactNode } from "react";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface Exchange {
  id: string;
  name: string;
  maskedKey: string;
  lastSynced: string;
  balance: number;
}

export interface Position {
  symbol: string;
  side: "LONG" | "SHORT";
  size: number;
  entry: number;
  mark: number;
  pnl: number;
  pnlPct: number;
}

export type StrategyType = "alpha-low" | "alpha-mid" | "alpha-high";
export type RiskLevel = "low" | "medium" | "high";

export type InstanceStatus = "unlinked" | "live" | "paused";

export interface StrategyInstance {
  id: string;
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
  positions: Position[];
}

export const STRATEGY_CATALOG: Record<StrategyType, {
  name: string; risk: RiskLevel; description: string;
  sharpe: number; maxDd: number; winRate: number; ytd: number; cagr: number;
  profitFactor: number; avg1m: number; activeDays: number; vol: number;
  simpleReturn: number; compoundedReturn: number; avgWinLoss: number;
}> = {
  "alpha-low": {
    name: "Alpha Low", risk: "low",
    description: "Conservative capital preservation strategy with tight drawdown controls and reduced position sizing. Designed for accounts prioritising stability over growth. Targets consistent small gains with minimal exposure to tail risk events.",
    sharpe: 1.84, maxDd: 8.2, winRate: 61, ytd: 14.2, cagr: 18.7,
    profitFactor: 1.88, avg1m: 1.2, activeDays: 312, vol: 6.4,
    simpleReturn: 14.2, compoundedReturn: 18.7, avgWinLoss: 1.42,
  },
  "alpha-mid": {
    name: "Alpha Mid", risk: "medium",
    description: "Balanced trend-following approach with dynamic position sizing that scales with conviction. Captures medium-term momentum across top-50 pairs while maintaining disciplined risk limits. The default choice for most accounts.",
    sharpe: 2.63, maxDd: 14.6, winRate: 63, ytd: 38.2, cagr: 42.1,
    profitFactor: 2.25, avg1m: 2.4, activeDays: 342, vol: 11.8,
    simpleReturn: 38.2, compoundedReturn: 42.1, avgWinLoss: 1.71,
  },
  "alpha-high": {
    name: "Alpha High", risk: "high",
    description: "Aggressive momentum capture across top-20 pairs with concentrated position sizing. Accepts higher drawdowns in exchange for outsized returns during trending regimes. Requires tolerance for short-term volatility.",
    sharpe: 3.90, maxDd: 19.9, winRate: 67, ytd: 91.4, cagr: 187.3,
    profitFactor: 3.09, avg1m: 3.2, activeDays: 358, vol: 18.2,
    simpleReturn: 91.4, compoundedReturn: 187.3, avgWinLoss: 2.34,
  },
};

// ─── Mock positions ──────────────────────────────────────────────────────────

export const MOCK_POSITIONS_A: Position[] = [
  { symbol: "BTCUSDT", side: "LONG",  size: 0.25, entry: 61840, mark: 63420, pnl: 395.0,  pnlPct: 2.55 },
  { symbol: "ETHUSDT", side: "SHORT", size: 1.80, entry: 3280,  mark: 3195,  pnl: 153.0,  pnlPct: 2.59 },
  { symbol: "SOLUSDT", side: "LONG",  size: 12.0, entry: 148.5, mark: 144.2, pnl: -51.6,  pnlPct: -2.89 },
];

export const MOCK_POSITIONS_B: Position[] = [
  { symbol: "BTCUSDT", side: "LONG",  size: 0.18, entry: 62100, mark: 63420, pnl: 237.6,  pnlPct: 2.13 },
  { symbol: "BNBUSDT", side: "LONG",  size: 3.5,  entry: 572.0, mark: 589.5, pnl: 61.25,  pnlPct: 3.06 },
];

// ─── Initial state ───────────────────────────────────────────────────────────

const INITIAL_EXCHANGES: Exchange[] = [
  { id: "binance-1", name: "Binance", maskedKey: "aK3x9f\u2022\u2022\u2022\u2022\u2022\u2022mP7qL2", lastSynced: "2m ago", balance: 127369 },
];

const INITIAL_INSTANCES: StrategyInstance[] = [];

// ─── Context ─────────────────────────────────────────────────────────────────

interface TraderState {
  exchanges: Exchange[];
  instances: StrategyInstance[];
  addExchange: (e: Exchange) => void;
  removeExchange: (id: string) => void;
  addInstance: (i: StrategyInstance) => void;
  updateInstance: (id: string, patch: Partial<StrategyInstance>) => void;
  removeInstance: (id: string) => void;
}

const TraderContext = createContext<TraderState | null>(null);

export function TraderProvider({ children }: { children: ReactNode }) {
  const [exchanges, setExchanges] = useState<Exchange[]>(INITIAL_EXCHANGES);
  const [instances, setInstances] = useState<StrategyInstance[]>(INITIAL_INSTANCES);

  const addExchange = useCallback((e: Exchange) => setExchanges(prev => [...prev, e]), []);
  const removeExchange = useCallback((id: string) => setExchanges(prev => prev.filter(e => e.id !== id)), []);
  const addInstance = useCallback((i: StrategyInstance) => setInstances(prev => [...prev, i]), []);
  const updateInstance = useCallback((id: string, patch: Partial<StrategyInstance>) => {
    setInstances(prev => prev.map(i => i.id === id ? { ...i, ...patch } : i));
  }, []);
  const removeInstance = useCallback((id: string) => setInstances(prev => prev.filter(i => i.id !== id)), []);

  return (
    <TraderContext.Provider value={{ exchanges, instances, addExchange, removeExchange, addInstance, updateInstance, removeInstance }}>
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

// ─── Equity curve mock data ──────────────────────────────────────────────────

export const GHOST_CURVE = [
  100, 101.2, 100.8, 102.5, 103.1, 104.8, 103.9, 105.6, 107.2, 106.4,
  108.1, 109.5, 108.7, 110.3, 112.0, 111.2, 113.5, 114.8, 113.6, 115.9,
  117.2, 116.0, 114.8, 113.2, 112.5, 114.0, 116.3, 118.1, 119.7, 120.5,
  121.8, 123.4, 122.1, 124.0, 125.6, 127.3, 126.0, 128.2, 129.8, 131.5,
  130.2, 132.0, 133.7, 135.1, 134.0, 136.2, 137.8, 138.2,
];

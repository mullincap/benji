"use client";

/**
 * Manager → Live tab.
 *
 * Answers "what is the account doing right now?" — independent of any
 * strategy session. Aggregates open positions across the user's exchanges
 * (selectable: BloFin / Binance / Both) and tags each position as either
 * 'strategy' (symbol matches today's basket on that connection) or
 * 'manual' (everything else).
 *
 * Backed by GET /api/manager/positions, which refreshes exchange snapshots
 * before reading.
 */

import { useCallback, useEffect, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type ExchangeFilter = "blofin" | "binance" | "both";

interface Totals {
  total_equity_usd: number;
  available_usd: number;
  used_margin_usd: number;
  unrealized_pnl: number;
  unrealized_pct: number;
  sum_notionals_usd: number;
  margin_used_pct: number;
  open_positions: number;
}

interface Position {
  connection_id: string;
  exchange: string;
  connection_label: string | null;
  symbol: string;
  symbol_base: string;
  side: string;
  size: number;
  entry_price: number;
  mark_price: number;
  unrealized_pnl: number;
  notional_usd: number;
  leverage: number;
  margin_mode: string;
  source: "strategy" | "manual";
  strategy_name: string | null;
}

interface Connection {
  connection_id: string;
  exchange: string;
  label: string | null;
  snapshot_at: string | null;
  total_equity_usd: number;
  available_usd: number;
  used_margin_usd: number;
  unrealized_pnl: number;
  fetch_ok: boolean;
  error_msg: string | null;
}

interface PositionsResponse {
  exchange_filter: ExchangeFilter;
  as_of: string | null;
  totals: Totals;
  positions: Position[];
  connections: Connection[];
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function fmtUsd(v: number, signed = false): string {
  const sign = signed ? (v > 0 ? "+" : v < 0 ? "−" : "") : v < 0 ? "−" : "";
  const abs = Math.abs(v);
  return `${sign}$${abs.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtPct(v: number): string {
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(2)}%`;
}

function pctColor(v: number): string {
  if (v > 0) return "var(--green)";
  if (v < 0) return "var(--red)";
  return "var(--t1)";
}

function relTime(iso: string | null): string {
  if (!iso) return "never";
  const diff = Date.now() - new Date(iso).getTime();
  const s = Math.floor(diff / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

// ─── KPI Card ───────────────────────────────────────────────────────────────

function KpiCard({
  label,
  value,
  color,
  subvalue,
  subvalueColor,
}: {
  label: string;
  value: string;
  color?: string;
  subvalue?: string;
  subvalueColor?: string;
}) {
  return (
    <div
      style={{
        flex: 1,
        background: "var(--bg1)",
        border: "1px solid var(--line)",
        borderRadius: 5,
        padding: "14px 16px",
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontSize: 9,
          fontWeight: 700,
          letterSpacing: "0.12em",
          color: "var(--t3)",
          textTransform: "uppercase",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div
        style={{
          fontSize: 18,
          fontWeight: 700,
          color: color || "var(--t0)",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}
      >
        {value}
      </div>
      {subvalue && (
        <div
          style={{
            fontSize: 11,
            color: subvalueColor || color || "var(--t2)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
            marginTop: 4,
            opacity: 0.8,
          }}
        >
          {subvalue}
        </div>
      )}
    </div>
  );
}

// ─── Exchange selector ──────────────────────────────────────────────────────

function ExchangeTabs({
  value,
  onChange,
}: {
  value: ExchangeFilter;
  onChange: (v: ExchangeFilter) => void;
}) {
  const opts: { key: ExchangeFilter; label: string }[] = [
    { key: "both", label: "Both" },
    { key: "blofin", label: "BloFin" },
    { key: "binance", label: "Binance" },
  ];
  return (
    <div
      style={{
        display: "inline-flex",
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 4,
        overflow: "hidden",
      }}
    >
      {opts.map((o) => {
        const active = value === o.key;
        return (
          <button
            key={o.key}
            onClick={() => onChange(o.key)}
            style={{
              background: active ? "var(--bg3)" : "transparent",
              color: active ? "var(--t0)" : "var(--t2)",
              border: "none",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              padding: "6px 12px",
              cursor: "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

// ─── Source chip ────────────────────────────────────────────────────────────

function SourceChip({ source, strategyName }: { source: "strategy" | "manual"; strategyName: string | null }) {
  const isStrategy = source === "strategy";
  return (
    <span
      title={isStrategy && strategyName ? strategyName : undefined}
      style={{
        display: "inline-block",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.06em",
        padding: "2px 6px",
        borderRadius: 3,
        background: isStrategy ? "var(--green-dim)" : "var(--amber-dim)",
        color: isStrategy ? "var(--green)" : "var(--amber)",
        textTransform: "uppercase",
      }}
    >
      {isStrategy ? "STRATEGY" : "MANUAL"}
    </span>
  );
}

// ─── Page ───────────────────────────────────────────────────────────────────

const REFRESH_MS = 30_000;

export default function LivePage() {
  const [exchange, setExchange] = useState<ExchangeFilter>("both");
  const [data, setData] = useState<PositionsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  const exchangeRef = useRef(exchange);
  exchangeRef.current = exchange;

  const load = useCallback(async (isInitial = false) => {
    if (isInitial) setLoading(true);
    else setRefreshing(true);
    try {
      const resp = await fetch(
        `${API_BASE}/api/manager/positions?exchange=${exchangeRef.current}`,
        { credentials: "include" }
      );
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json: PositionsResponse = await resp.json();
      setData(json);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  // Reload on exchange change.
  useEffect(() => {
    load(true);
  }, [exchange, load]);

  // 30s background refresh.
  useEffect(() => {
    const id = setInterval(() => load(false), REFRESH_MS);
    return () => clearInterval(id);
  }, [load]);

  // 1Hz tick so the "as of" relative time updates without re-fetching.
  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const failedConns = (data?.connections || []).filter((c) => !c.fetch_ok);
  const totals = data?.totals;
  const positions = data?.positions || [];

  // Sort: strategy first, then by notional desc.
  const sortedPositions = [...positions].sort((a, b) => {
    if (a.source !== b.source) return a.source === "strategy" ? -1 : 1;
    return (b.notional_usd || 0) - (a.notional_usd || 0);
  });

  const showExchangeCol = exchange === "both";

  return (
    <div style={{ padding: 20, display: "flex", flexDirection: "column", gap: 10, height: "100%", overflow: "auto" }}>
      {/* Header row: title · exchange selector · as-of · refresh */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <span
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
            }}
          >
            Live Account State
          </span>
          <span style={{ fontSize: 9, color: "var(--t3)" }}>
            {/* tick is read so eslint keeps the dependency; the value drives relTime via Date.now() */}
            {tick >= 0 && data?.as_of ? `As of ${relTime(data.as_of)} · auto-refresh 30s` : "Loading…"}
          </span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <ExchangeTabs value={exchange} onChange={setExchange} />
          <button
            onClick={() => load(false)}
            disabled={refreshing}
            style={{
              background: "transparent",
              border: "1px solid var(--line2)",
              color: refreshing ? "var(--t3)" : "var(--t1)",
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              padding: "6px 12px",
              borderRadius: 4,
              cursor: refreshing ? "default" : "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            {refreshing ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </div>

      {/* Failed-connection banners */}
      {failedConns.length > 0 && (
        <div
          style={{
            background: "var(--red-dim)",
            border: "1px solid var(--red)",
            borderRadius: 4,
            padding: "8px 12px",
            fontSize: 10,
            color: "var(--red)",
          }}
        >
          {failedConns.length} connection{failedConns.length === 1 ? "" : "s"} failed:{" "}
          {failedConns.map((c) => `${c.exchange}${c.label ? ` (${c.label})` : ""}: ${c.error_msg || "unknown"}`).join(" · ")}
        </div>
      )}

      {/* Error banner */}
      {error && (
        <div
          style={{
            background: "var(--red-dim)",
            border: "1px solid var(--red)",
            borderRadius: 4,
            padding: "8px 12px",
            fontSize: 10,
            color: "var(--red)",
          }}
        >
          Failed to load: {error}
        </div>
      )}

      {/* KPI strip */}
      {loading && !data ? (
        <div style={{ fontSize: 10, color: "var(--t3)", padding: 24, textAlign: "center" }}>
          Loading account state…
        </div>
      ) : totals ? (
        <div style={{ display: "flex", gap: 10 }}>
          <KpiCard
            label="Total Equity"
            value={fmtUsd(totals.total_equity_usd)}
          />
          <KpiCard
            label="Available"
            value={fmtUsd(totals.available_usd)}
            subvalue={
              totals.total_equity_usd > 0
                ? `${((totals.available_usd / totals.total_equity_usd) * 100).toFixed(1)}% free`
                : undefined
            }
          />
          <KpiCard
            label="Unrealized P&L"
            value={fmtUsd(totals.unrealized_pnl, true)}
            color={pctColor(totals.unrealized_pnl)}
            subvalue={fmtPct(totals.unrealized_pct)}
          />
          <KpiCard
            label="Notional Exposure"
            value={fmtUsd(totals.sum_notionals_usd)}
            subvalue={
              totals.total_equity_usd > 0
                ? `${(totals.sum_notionals_usd / totals.total_equity_usd).toFixed(2)}× equity`
                : undefined
            }
          />
          <KpiCard
            label="Margin Used"
            value={fmtUsd(totals.used_margin_usd)}
            subvalue={`${totals.margin_used_pct.toFixed(1)}% of equity`}
          />
          <KpiCard
            label="Open Positions"
            value={String(totals.open_positions)}
          />
        </div>
      ) : null}

      {/* Positions table */}
      <div
        style={{
          background: "var(--bg1)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          padding: "12px 16px",
          flex: 1,
          minHeight: 0,
          overflow: "auto",
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "baseline",
            marginBottom: 10,
          }}
        >
          <div
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: "0.12em",
              color: "var(--t3)",
              textTransform: "uppercase",
            }}
          >
            Open Positions
          </div>
          {totals && totals.open_positions > 0 && (
            <div style={{ fontSize: 9, color: "var(--t3)" }}>
              {sortedPositions.filter((p) => p.source === "strategy").length} strategy ·{" "}
              {sortedPositions.filter((p) => p.source === "manual").length} manual
            </div>
          )}
        </div>

        {sortedPositions.length === 0 ? (
          <div style={{ fontSize: 10, color: "var(--t3)", padding: "16px 0" }}>
            No open positions
          </div>
        ) : (
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
          >
            <thead>
              <tr>
                {[
                  ...(showExchangeCol ? ["Exchange"] : []),
                  "Symbol",
                  "Side",
                  "Source",
                  "Size",
                  "Notional",
                  "Entry",
                  "Mark",
                  "Unrealized",
                  "Lev",
                  "Mode",
                ].map((h) => (
                  <th
                    key={h}
                    style={{
                      fontSize: 9,
                      fontWeight: 700,
                      letterSpacing: "0.1em",
                      color: "var(--t3)",
                      textTransform: "uppercase",
                      textAlign: "left",
                      padding: "4px 8px 6px 0",
                      borderBottom: "1px solid var(--line)",
                    }}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sortedPositions.map((p, i) => {
                const upl = p.unrealized_pnl || 0;
                const uplPct =
                  p.entry_price && p.size && p.notional_usd
                    ? (upl / (p.entry_price * p.size)) * 100
                    : 0;
                return (
                  <tr key={`${p.connection_id}-${p.symbol}-${i}`}>
                    {showExchangeCol && (
                      <td style={tdStyle}>
                        <span style={{ color: "var(--t1)" }}>{p.exchange}</span>
                      </td>
                    )}
                    <td style={{ ...tdStyle, color: "var(--t0)", fontWeight: 700 }}>
                      {p.symbol_base}
                    </td>
                    <td
                      style={{
                        ...tdStyle,
                        color: p.side === "short" ? "var(--red)" : "var(--green)",
                        textTransform: "uppercase",
                        fontWeight: 700,
                      }}
                    >
                      {p.side}
                    </td>
                    <td style={tdStyle}>
                      <SourceChip source={p.source} strategyName={p.strategy_name} />
                    </td>
                    <td style={tdStyle}>
                      {p.size?.toLocaleString(undefined, { maximumFractionDigits: 6 }) ?? "—"}
                    </td>
                    <td style={tdStyle}>{fmtUsd(p.notional_usd)}</td>
                    <td style={tdStyle}>
                      {p.entry_price
                        ? `$${p.entry_price.toLocaleString(undefined, { maximumFractionDigits: 6 })}`
                        : "—"}
                    </td>
                    <td style={tdStyle}>
                      {p.mark_price
                        ? `$${p.mark_price.toLocaleString(undefined, { maximumFractionDigits: 6 })}`
                        : "—"}
                    </td>
                    <td style={{ ...tdStyle, color: pctColor(upl) }}>
                      {fmtUsd(upl, true)}
                      {p.entry_price && p.size ? (
                        <span style={{ color: "var(--t3)", marginLeft: 6 }}>
                          {fmtPct(uplPct)}
                        </span>
                      ) : null}
                    </td>
                    <td style={tdStyle}>{p.leverage ? `${p.leverage}×` : "—"}</td>
                    <td style={tdStyle}>{p.margin_mode || "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}

const tdStyle: React.CSSProperties = {
  fontSize: 10,
  color: "var(--t1)",
  padding: "6px 8px 6px 0",
  borderBottom: "1px solid var(--line)",
  whiteSpace: "nowrap",
};

"use client";

/**
 * frontend/app/compiler/(protected)/symbols/page.tsx
 * ==================================================
 * Compiler Symbol Inspector — per-endpoint completeness for a single symbol.
 *
 * Data source:
 *   GET /api/compiler/symbols/{symbol}  (with credentials: 'include')
 *
 * UX:
 *   - Centered search input at the top, autofocus on mount, submit on Enter
 *   - Type a base symbol (BTC, ETH, etc.); the backend looks up symbol_id
 *     from market.symbols.base, returns most recent day with data + per-column
 *     non-null counts + 30-day sparkline
 *   - 404 → "Symbol not found in market.symbols"
 *   - Symbol exists but no data yet → empty endpoint bars, empty sparkline
 *   - 401 → "Session expired"
 *
 * Renders (after a successful fetch):
 *   1. Header: symbol name + symbol_id, source name, latest date, total rows
 *   2. 30-day row count sparkline (inline SVG — matches trader/equity-curve.tsx
 *      pattern; no chart.js Canvas mount needed for 30 data points)
 *   3. Three tier sections (L1 / L2 / L3) with endpoint completeness bars
 *      - L1 = core OHLCV + OI + funding + LS  (5 endpoints)
 *      - L2 = trades + liquidations + ticker  (6 endpoints)
 *      - L3 = order book depth                (4 endpoints)
 *      Tier grouping mirrors the FETCH_* toggle layers in
 *      pipeline/compiler/metl.py so admins immediately see which ETL tiers
 *      were populated vs missed for the symbol they're inspecting.
 *
 * Bar color rules:
 *   pct >= 95   → green
 *   50 - 94     → amber
 *   < 50        → red
 *   total = 0   → muted (no data for this symbol-day at all)
 */

import { useEffect, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Source registry (mirrors market.sources, same as jobs page) ────────────

const SOURCE_NAMES: Record<number, string> = {
  1: "amberdata_binance",
  2: "binance_direct",
  3: "blofin_direct",
  4: "coingecko",
  5: "amberdata_spot",
  6: "amberdata_options",
};

// ─── Tier definitions ───────────────────────────────────────────────────────
// Mirrors the FETCH_* toggle layers in pipeline/compiler/metl.py exactly.
// L1 = always-on cheap endpoints. L2 = mid-cost. L3 = heaviest order-book
// snapshots. Order within each tier matches the column order in
// market.futures_1m so the UI is predictable.

type Tier = {
  label: string;
  description: string;
  endpoints: string[];
};

const TIERS: Tier[] = [
  {
    label: "L1",
    description: "Core: OHLCV + open interest + funding + long/short ratio",
    endpoints: ["close", "volume", "open_interest", "funding_rate", "long_short_ratio"],
  },
  {
    label: "L2",
    description: "Mid-cost: trades + liquidations + ticker spread/imbalance/basis",
    endpoints: ["trade_delta", "long_liqs", "short_liqs", "spread_pct", "bid_ask_imbalance", "basis_pct"],
  },
  {
    label: "L3",
    description: "Heavy: order book depth snapshots",
    endpoints: ["last_bid_depth", "last_ask_depth", "last_depth_imbalance", "last_spread_pct"],
  },
];

// ─── Response types ─────────────────────────────────────────────────────────
// Mirrors backend/app/api/routes/compiler.py symbol_inspector() exactly.

type SparklinePoint = { date: string; rows: number };

type SymbolResponse = {
  symbol: string;
  symbol_id: number;
  source_id: number;
  date: string | null;          // null when symbol exists but has no data yet
  total_rows: number;
  rows_per_endpoint: Record<string, number>;
  sparkline: SparklinePoint[];
};

type LoadState =
  | { kind: "idle" }
  | { kind: "loading"; query: string }
  | { kind: "notfound"; query: string }
  | { kind: "error"; message: string }
  | { kind: "ready"; data: SymbolResponse };

// ─── Helpers ─────────────────────────────────────────────────────────────────

function endpointPct(count: number, total: number): number | null {
  if (total <= 0) return null;
  return (count / total) * 100;
}

function barColor(pct: number | null): { fill: string; border: string } {
  if (pct === null) {
    return { fill: "var(--bg3)", border: "var(--line)" };
  }
  if (pct >= 95) {
    return { fill: "var(--green-mid)", border: "var(--green)" };
  }
  if (pct >= 50) {
    return { fill: "var(--amber-dim)", border: "var(--amber)" };
  }
  return { fill: "var(--red-dim)", border: "var(--red)" };
}

// ─── Sparkline (inline SVG) ─────────────────────────────────────────────────

function Sparkline({ data }: { data: SparklinePoint[] }) {
  const W = 600;
  const H = 80;
  const PAD = 4;

  if (data.length === 0) {
    return (
      <div style={{
        height: H,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontSize: 9,
        color: "var(--t3)",
        textTransform: "uppercase",
        letterSpacing: "0.12em",
      }}>
        No data in the last 30 days
      </div>
    );
  }

  const maxRows = Math.max(1, ...data.map((d) => d.rows));
  const stepX = data.length > 1 ? (W - 2 * PAD) / (data.length - 1) : 0;
  const yFor = (rows: number) => H - PAD - ((rows / maxRows) * (H - 2 * PAD));

  // Build the line path + an area path that closes to the bottom for the fill
  const linePoints = data.map((d, i) => `${PAD + i * stepX},${yFor(d.rows)}`);
  const linePath = `M ${linePoints.join(" L ")}`;
  const areaPath = `${linePath} L ${PAD + (data.length - 1) * stepX},${H - PAD} L ${PAD},${H - PAD} Z`;

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      preserveAspectRatio="none"
      style={{ width: "100%", height: H, display: "block" }}
    >
      <path d={areaPath} fill="var(--module-accent)" opacity="0.12" />
      <path d={linePath} fill="none" stroke="var(--module-accent)" strokeWidth="1.5" />
      {data.map((d, i) => (
        <circle
          key={d.date}
          cx={PAD + i * stepX}
          cy={yFor(d.rows)}
          r={2}
          fill="var(--module-accent)"
        >
          <title>{`${d.date}: ${d.rows.toLocaleString("en-US")} rows`}</title>
        </circle>
      ))}
    </svg>
  );
}

// ─── Endpoint bar ───────────────────────────────────────────────────────────

function EndpointBar({ name, count, total }: { name: string; count: number; total: number }) {
  const pct = endpointPct(count, total);
  const colors = barColor(pct);
  const widthPct = pct === null ? 0 : Math.min(100, pct);
  const labelText =
    pct === null
      ? "—"
      : pct >= 99.95
        ? "100%"
        : `${pct.toFixed(1)}%`;

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "160px 1fr 80px 90px",
      gap: 12,
      alignItems: "center",
      padding: "5px 0",
    }}>
      <div style={{
        fontSize: 10,
        color: "var(--t1)",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        whiteSpace: "nowrap",
        overflow: "hidden",
        textOverflow: "ellipsis",
      }}>
        {name}
      </div>
      <div style={{
        height: 6,
        background: "var(--bg3)",
        borderRadius: 3,
        overflow: "hidden",
        border: "1px solid var(--line)",
      }}>
        <div style={{
          height: "100%",
          width: `${widthPct}%`,
          background: colors.fill,
          borderRight: widthPct > 0 ? `1px solid ${colors.border}` : "none",
          transition: "width 0.3s ease",
        }} />
      </div>
      <div style={{
        fontSize: 9,
        color: "var(--t2)",
        textAlign: "right",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}>
        {count.toLocaleString("en-US")} / {total.toLocaleString("en-US")}
      </div>
      <div style={{
        fontSize: 10,
        fontWeight: 700,
        color: pct === null ? "var(--t3)" : pct >= 95 ? "var(--green)" : pct >= 50 ? "var(--amber)" : "var(--red)",
        textAlign: "right",
      }}>
        {labelText}
      </div>
    </div>
  );
}

// ─── Tier section ───────────────────────────────────────────────────────────

function TierSection({
  tier,
  rowsPerEndpoint,
  totalRows,
}: {
  tier: Tier;
  rowsPerEndpoint: Record<string, number>;
  totalRows: number;
}) {
  return (
    <div style={{ marginBottom: 18 }}>
      <div style={{
        display: "flex",
        alignItems: "baseline",
        gap: 10,
        marginBottom: 8,
      }}>
        <span style={{
          fontSize: 11,
          fontWeight: 700,
          color: "var(--t0)",
          letterSpacing: "0.06em",
        }}>
          {tier.label}
        </span>
        <span style={{
          fontSize: 9,
          color: "var(--t2)",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
        }}>
          {tier.description}
        </span>
      </div>
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "10px 14px",
      }}>
        {tier.endpoints.map((ep) => (
          <EndpointBar
            key={ep}
            name={ep}
            count={rowsPerEndpoint[ep] ?? 0}
            total={totalRows}
          />
        ))}
      </div>
    </div>
  );
}

// ─── Subcomponents ───────────────────────────────────────────────────────────

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase",
      marginBottom: 8,
    }}>
      {children}
    </div>
  );
}

function SearchBar({
  onSubmit,
  loading,
  initialValue,
}: {
  onSubmit: (symbol: string) => void;
  loading: boolean;
  initialValue?: string;
}) {
  const [value, setValue] = useState(initialValue ?? "");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = value.trim().toUpperCase();
    if (trimmed && !loading) onSubmit(trimmed);
  }

  return (
    <form
      onSubmit={handleSubmit}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        marginBottom: 24,
      }}
    >
      <input
        ref={inputRef}
        type="text"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder="Symbol base (e.g. BTC, ETH, SOL)"
        autoComplete="off"
        spellCheck={false}
        disabled={loading}
        style={{
          flex: 1,
          background: "var(--bg3)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          padding: "10px 12px",
          color: "var(--t0)",
          fontSize: 12,
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          outline: "none",
          textTransform: "uppercase",
        }}
        onFocus={(e) => { e.currentTarget.style.borderColor = "var(--module-accent)"; }}
        onBlur={(e) => { e.currentTarget.style.borderColor = "var(--line)"; }}
      />
      <button
        type="submit"
        disabled={!value.trim() || loading}
        style={{
          background: value.trim() && !loading ? "var(--module-accent)" : "var(--bg3)",
          color: value.trim() && !loading ? "#0a0a0a" : "var(--t2)",
          border: "none",
          borderRadius: 6,
          padding: "10px 18px",
          fontSize: 10,
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          cursor: value.trim() && !loading ? "pointer" : "not-allowed",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
          transition: "all 0.15s ease",
        }}
      >
        {loading ? "Loading…" : "Search"}
      </button>
    </form>
  );
}

// ─── Header card ─────────────────────────────────────────────────────────────

function ResultHeader({ data }: { data: SymbolResponse }) {
  const sourceName = SOURCE_NAMES[data.source_id] ?? `source ${data.source_id}`;
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "16px 20px",
      marginBottom: 18,
      display: "grid",
      gridTemplateColumns: "1fr 1fr 1fr 1fr",
      gap: 14,
    }}>
      <div>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 6,
        }}>
          Symbol
        </div>
        <div style={{ fontSize: 18, fontWeight: 700, color: "var(--t0)" }}>
          {data.symbol}
        </div>
        <div style={{ fontSize: 9, color: "var(--t2)", marginTop: 2 }}>
          symbol_id #{data.symbol_id}
        </div>
      </div>
      <div>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 6,
        }}>
          Source
        </div>
        <div style={{ fontSize: 12, fontWeight: 700, color: "var(--t0)" }}>
          {sourceName}
        </div>
      </div>
      <div>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 6,
        }}>
          Latest Date
        </div>
        <div style={{ fontSize: 12, fontWeight: 700, color: data.date ? "var(--t0)" : "var(--t3)" }}>
          {data.date ?? "—"}
        </div>
      </div>
      <div>
        <div style={{
          fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
          color: "var(--t3)", textTransform: "uppercase", marginBottom: 6,
        }}>
          Total Rows
        </div>
        <div style={{ fontSize: 18, fontWeight: 700, color: "var(--t0)" }}>
          {data.total_rows.toLocaleString("en-US")}
        </div>
      </div>
    </div>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function CompilerSymbolsPage() {
  const [state, setState] = useState<LoadState>({ kind: "idle" });

  async function search(symbol: string) {
    setState({ kind: "loading", query: symbol });
    try {
      const res = await fetch(
        `${API_BASE}/api/compiler/symbols/${encodeURIComponent(symbol)}`,
        { credentials: "include" },
      );
      if (res.status === 401) {
        setState({ kind: "error", message: "Session expired. Please log in again." });
        return;
      }
      if (res.status === 404) {
        setState({ kind: "notfound", query: symbol });
        return;
      }
      if (!res.ok) {
        setState({ kind: "error", message: `Symbols endpoint returned ${res.status}` });
        return;
      }
      const data = (await res.json()) as SymbolResponse;
      setState({ kind: "ready", data });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setState({ kind: "error", message: `Network error: ${message}` });
    }
  }

  const lastQuery =
    state.kind === "loading" ? state.query :
    state.kind === "notfound" ? state.query :
    state.kind === "ready" ? state.data.symbol :
    undefined;

  const isLoading = state.kind === "loading";

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 960, margin: "0 auto" }}>
        <SectionLabel>Compiler · Symbols</SectionLabel>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Symbol Inspector
        </h1>

        <SearchBar onSubmit={search} loading={isLoading} initialValue={lastQuery} />

        {state.kind === "idle" && (
          <div style={{
            background: "var(--bg2)",
            border: "1px solid var(--line)",
            borderRadius: 6,
            padding: "20px 24px",
            fontSize: 10,
            color: "var(--t2)",
            lineHeight: 1.6,
          }}>
            Type a symbol base name above (BTC, ETH, SOL, etc.) and press Enter
            to inspect its endpoint coverage. The inspector shows per-endpoint
            row counts for the most recent day with data, grouped by ETL tier
            (L1 / L2 / L3), plus a 30-day sparkline of total row counts.
          </div>
        )}

        {state.kind === "loading" && (
          <div style={{
            fontSize: 9, color: "var(--t3)",
            textTransform: "uppercase", letterSpacing: "0.12em",
            padding: "20px 0",
          }}>
            Inspecting {state.query}…
          </div>
        )}

        {state.kind === "notfound" && (
          <div style={{
            background: "var(--amber-dim)",
            border: "1px solid var(--amber)",
            borderRadius: 6,
            padding: "14px 18px",
            fontSize: 10,
            color: "var(--amber)",
          }}>
            Symbol &ldquo;{state.query}&rdquo; not found in market.symbols. Try a
            different base symbol (e.g. BTC, ETH, SOL).
          </div>
        )}

        {state.kind === "error" && (
          <div style={{
            background: "var(--red-dim)",
            border: "1px solid var(--red)",
            borderRadius: 6,
            padding: "14px 18px",
            fontSize: 10,
            color: "var(--red)",
          }}>
            {state.message}
          </div>
        )}

        {state.kind === "ready" && (
          <ResultBody data={state.data} />
        )}
      </div>
    </div>
  );
}

function ResultBody({ data }: { data: SymbolResponse }) {
  return (
    <>
      <ResultHeader data={data} />

      <div style={{ marginBottom: 24 }}>
        <SectionLabel>Row Count · last 30 days</SectionLabel>
        <div style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          padding: "14px 18px",
        }}>
          <Sparkline data={data.sparkline} />
        </div>
      </div>

      <div>
        <SectionLabel>Endpoint Coverage · {data.date ?? "no data"}</SectionLabel>
        {TIERS.map((tier) => (
          <TierSection
            key={tier.label}
            tier={tier}
            rowsPerEndpoint={data.rows_per_endpoint}
            totalRows={data.total_rows}
          />
        ))}
      </div>
    </>
  );
}

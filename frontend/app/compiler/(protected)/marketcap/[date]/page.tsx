"use client";

/**
 * frontend/app/compiler/(protected)/marketcap/[date]/page.tsx
 * ===========================================================
 * Marketcap day drill-down — table of every market_cap_daily row for
 * the target date, sorted by rank. Each row shows whether the base
 * also exists in market.symbols (has_futures), so you can spot
 * CoinGecko coins that the simulator can't actually use.
 */

import { useEffect, useState, useMemo } from "react";
import { useParams, useRouter } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

type McapRow = {
  base: string;
  coin_id: string | null;
  name: string | null;
  market_cap_usd: number | null;
  price_usd: number | null;
  volume_usd: number | null;
  rank_num: number | null;
  has_futures: boolean;
};

type DayResponse = {
  date: string;
  total_rows: number;
  matched_to_futures: number;
  rows: McapRow[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; data: DayResponse };

type FilterMode = "all" | "futures_only" | "no_futures";

function formatUsd(n: number | null): string {
  if (n === null || n === undefined) return "—";
  if (n >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
  if (n >= 1e9)  return `$${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6)  return `$${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e3)  return `$${(n / 1e3).toFixed(2)}K`;
  return `$${n.toFixed(2)}`;
}

function formatPrice(n: number | null): string {
  if (n === null || n === undefined) return "—";
  if (n >= 1)    return `$${n.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
  if (n >= 0.01) return `$${n.toFixed(4)}`;
  return `$${n.toExponential(2)}`;
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase", marginBottom: 8,
    }}>
      {children}
    </div>
  );
}

function KPICard({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div style={{
      background: "var(--bg2)",
      border: "1px solid var(--line)",
      borderRadius: 6,
      padding: "14px 16px",
      display: "flex",
      flexDirection: "column",
      gap: 6,
    }}>
      <div style={{ fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", color: "var(--t3)", textTransform: "uppercase" }}>
        {label}
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color: "var(--t0)" }}>
        {value}
      </div>
      {hint && <div style={{ fontSize: 9, color: "var(--t2)" }}>{hint}</div>}
    </div>
  );
}

export default function MarketcapDayPage() {
  const params = useParams<{ date: string }>();
  const router = useRouter();
  const targetDate = params?.date ?? "";
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [filter, setFilter] = useState<FilterMode>("all");
  const [search, setSearch] = useState("");

  useEffect(() => {
    if (!targetDate) return;
    let cancelled = false;
    setState({ kind: "loading" });
    fetch(`${API_BASE}/api/compiler/marketcap/days/${targetDate}`, { credentials: "include" })
      .then(async (r) => {
        if (cancelled) return;
        if (r.status === 401) {
          setState({ kind: "error", message: "Session expired." });
          return;
        }
        if (r.status === 404) {
          setState({ kind: "error", message: `No mcap data for ${targetDate}` });
          return;
        }
        if (!r.ok) {
          setState({ kind: "error", message: `Endpoint returned ${r.status}` });
          return;
        }
        const data = (await r.json()) as DayResponse;
        if (cancelled) return;
        setState({ kind: "ready", data });
      })
      .catch((err) => {
        if (cancelled) return;
        const message = err instanceof Error ? err.message : String(err);
        setState({ kind: "error", message: `Network error: ${message}` });
      });
    return () => {
      cancelled = true;
    };
  }, [targetDate]);

  const filteredRows = useMemo(() => {
    if (state.kind !== "ready") return [];
    let rows = state.data.rows;
    if (filter === "futures_only") rows = rows.filter((r) => r.has_futures);
    if (filter === "no_futures")   rows = rows.filter((r) => !r.has_futures);
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      rows = rows.filter((r) =>
        (r.base?.toLowerCase().includes(q)) ||
        (r.coin_id?.toLowerCase().includes(q)) ||
        (r.name?.toLowerCase().includes(q))
      );
    }
    return rows;
  }, [state, filter, search]);

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 1100, margin: "0 auto" }}>
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 24 }}>
          <div>
            <SectionLabel>Compiler · Marketcap · {targetDate}</SectionLabel>
            <h1 style={{
              fontSize: 24, fontWeight: 700, color: "var(--t0)",
              margin: 0, letterSpacing: "-0.01em",
            }}>
              {targetDate}
            </h1>
          </div>
          <button
            onClick={() => router.push("/compiler/marketcap")}
            style={{
              background: "transparent",
              color: "var(--t2)",
              border: "1px solid var(--line2)",
              borderRadius: 4,
              padding: "6px 14px",
              fontSize: 9, fontWeight: 700, letterSpacing: "0.12em", textTransform: "uppercase",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              cursor: "pointer",
            }}
          >
            ← Back
          </button>
        </div>

        {state.kind === "loading" && (
          <div style={{ color: "var(--t3)", fontSize: 10 }}>Loading…</div>
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
          <>
            <div style={{
              display: "grid",
              gridTemplateColumns: "repeat(3, 1fr)",
              gap: 10,
              marginBottom: 24,
            }}>
              <KPICard
                label="Total Rows"
                value={state.data.total_rows.toLocaleString("en-US")}
                hint="all bases in CoinGecko snapshot"
              />
              <KPICard
                label="Matched To Futures"
                value={state.data.matched_to_futures.toLocaleString("en-US")}
                hint="bases also in market.symbols"
              />
              <KPICard
                label="Unmatched"
                value={(state.data.total_rows - state.data.matched_to_futures).toLocaleString("en-US")}
                hint="no Binance futures listing"
              />
            </div>

            <div style={{
              display: "flex",
              gap: 8,
              marginBottom: 16,
              alignItems: "center",
            }}>
              <FilterButton label="All" active={filter === "all"} onClick={() => setFilter("all")} count={state.data.total_rows} />
              <FilterButton label="Futures" active={filter === "futures_only"} onClick={() => setFilter("futures_only")} count={state.data.matched_to_futures} />
              <FilterButton label="No Futures" active={filter === "no_futures"} onClick={() => setFilter("no_futures")} count={state.data.total_rows - state.data.matched_to_futures} />
              <input
                type="text"
                placeholder="search base / name / coin_id"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                style={{
                  flex: 1,
                  background: "var(--bg3)",
                  border: "1px solid var(--line)",
                  borderRadius: 4,
                  padding: "6px 10px",
                  color: "var(--t0)",
                  fontSize: 10,
                  fontFamily: "var(--font-space-mono), Space Mono, monospace",
                  outline: "none",
                }}
              />
            </div>

            <div style={{
              background: "var(--bg2)",
              border: "1px solid var(--line)",
              borderRadius: 6,
              overflow: "hidden",
            }}>
              <table style={{
                width: "100%",
                borderCollapse: "collapse",
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
              }}>
                <thead>
                  <tr>
                    <Th align="right">Rank</Th>
                    <Th>Base</Th>
                    <Th>Name</Th>
                    <Th>Coin ID</Th>
                    <Th align="right">Market Cap</Th>
                    <Th align="right">Price</Th>
                    <Th align="right">24h Vol</Th>
                    <Th align="right">Futures?</Th>
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.length === 0 && (
                    <tr>
                      <td colSpan={8} style={{ padding: 24, textAlign: "center", fontSize: 10, color: "var(--t3)" }}>
                        No rows match the current filter.
                      </td>
                    </tr>
                  )}
                  {filteredRows.map((r) => (
                    <tr
                      key={r.base}
                      style={{ borderTop: "1px solid var(--line)" }}
                    >
                      <Td align="right">
                        <span style={{ color: "var(--t2)" }}>
                          {r.rank_num !== null ? `#${r.rank_num}` : "—"}
                        </span>
                      </Td>
                      <Td>
                        <span style={{ color: "var(--t0)", fontWeight: 700 }}>{r.base}</span>
                      </Td>
                      <Td>{r.name ?? "—"}</Td>
                      <Td>
                        <span style={{ color: "var(--t2)" }}>{r.coin_id ?? "—"}</span>
                      </Td>
                      <Td align="right">{formatUsd(r.market_cap_usd)}</Td>
                      <Td align="right">{formatPrice(r.price_usd)}</Td>
                      <Td align="right">{formatUsd(r.volume_usd)}</Td>
                      <Td align="right">
                        {r.has_futures ? (
                          <span style={{
                            display: "inline-block",
                            fontSize: 9, fontWeight: 700,
                            color: "var(--green)",
                            background: "var(--green-dim)",
                            border: "1px solid var(--green)",
                            borderRadius: 3,
                            padding: "2px 6px",
                            letterSpacing: "0.06em",
                          }}>
                            YES
                          </span>
                        ) : (
                          <span style={{
                            color: "var(--t3)",
                            fontSize: 10,
                          }}>
                            no
                          </span>
                        )}
                      </Td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div style={{ marginTop: 12, fontSize: 9, color: "var(--t3)", textAlign: "right" }}>
              showing {filteredRows.length.toLocaleString("en-US")} of {state.data.total_rows.toLocaleString("en-US")} rows
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function FilterButton({ label, active, onClick, count }: { label: string; active: boolean; onClick: () => void; count: number }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: active ? "var(--bg4)" : "var(--bg2)",
        color: active ? "var(--t0)" : "var(--t2)",
        border: `1px solid ${active ? "var(--line2)" : "var(--line)"}`,
        borderRadius: 4,
        padding: "4px 10px",
        fontSize: 9, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase",
        cursor: "pointer",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
      }}
    >
      {label} · {count.toLocaleString("en-US")}
    </button>
  );
}

function Th({ children, align = "left" }: { children: React.ReactNode; align?: "left" | "right" }) {
  return (
    <th style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.08em",
      color: "var(--t3)", textTransform: "uppercase",
      textAlign: align,
      padding: "10px 12px 12px",
    }}>
      {children}
    </th>
  );
}

function Td({ children, align = "left" }: { children: React.ReactNode; align?: "left" | "right" }) {
  return (
    <td style={{
      fontSize: 10,
      color: "var(--t1)",
      textAlign: align,
      padding: "8px 12px",
      verticalAlign: "middle",
    }}>
      {children}
    </td>
  );
}

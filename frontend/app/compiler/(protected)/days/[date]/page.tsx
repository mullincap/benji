"use client";

/**
 * frontend/app/compiler/(protected)/days/[date]/page.tsx
 * =======================================================
 * Day detail drill-down — middle layer between the universe-level
 * Coverage Map and the symbol-level Symbol Inspector.
 *
 * Routed from:
 *   - Clicking a cell in the Coverage Map heatmap
 *   - Clicking a row in the Gap Days table
 *
 * Shows for the target day:
 *   - KPI strip: Coverage % / Expected / Complete / Partial / Missing
 *   - Job metadata card if compiler_jobs has a row for this date
 *   - Symbol list with per-symbol coverage bar, status badge, and the
 *     row count per endpoint (so you can tell WHICH endpoint was thin).
 *
 * Clicking a symbol row routes to /compiler/symbols?q={base} for full
 * per-endpoint drill-down.
 */

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { KPIGridSkeleton, TableSkeleton } from "../../../../components/Skeleton";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

// ─── Types ───────────────────────────────────────────────────────────────────

type SymbolStatus = "complete" | "partial" | "missing";

type DaySymbol = {
  symbol_id: number;
  base: string;
  binance_id: string;
  total_rows: number;
  completeness_pct: number;
  status: SymbolStatus;
  rows_per_endpoint: Record<string, number>;
};

type JobPayload = {
  job_id: string;
  status: string;
  symbols_total: number | null;
  symbols_done: number | null;
  rows_written: number;
  started_at: string | null;
  completed_at: string | null;
  triggered_by: string | null;
  run_tag: string | null;
  error_msg: string | null;
};

type DayResponse = {
  date: string;
  source_id: number;
  expected_symbols: number;
  symbols_with_data: number;
  symbols_complete: number;
  symbols_partial: number;
  symbols_missing: number;
  has_job_truth: boolean;
  day_completeness_pct: number;
  job: JobPayload | null;
  endpoint_cols: string[];
  symbols: DaySymbol[];
  total_rows: number;
  suspected_duplicates: boolean;
  duplicate_factor: number;
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; data: DayResponse };

// ─── Endpoint tier grouping (matches metl.py tiers) ─────────────────────────

const TIER_L1 = ["close", "volume", "open_interest", "funding_rate", "long_short_ratio"];
const TIER_L2 = ["trade_delta", "long_liqs", "short_liqs"];
const TIER_L3 = [
  "last_bid_depth", "last_ask_depth", "last_depth_imbalance", "last_spread_pct",
  "spread_pct", "bid_ask_imbalance", "basis_pct",
];

function tierStatus(cols: string[], rows_per_endpoint: Record<string, number>): number {
  // Average completeness across the tier's columns, expressed as 0–100.
  if (cols.length === 0) return 0;
  const avg = cols.reduce((sum, c) => sum + Math.min((rows_per_endpoint[c] || 0) / 1440, 1), 0) / cols.length;
  return Math.round(avg * 100);
}

function tierColor(pct: number): string {
  if (pct >= 95) return "var(--green)";
  if (pct >= 5) return "var(--amber)";
  return "var(--red)";
}

// ─── UI helpers ─────────────────────────────────────────────────────────────

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

function KPICard({ label, value, hint, color }: { label: string; value: string; hint?: string; color?: string }) {
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
      <div style={{
        fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
        color: "var(--t3)", textTransform: "uppercase",
      }}>
        {label}
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color: color || "var(--t0)" }}>
        {value}
      </div>
      {hint && (
        <div style={{ fontSize: 9, color: "var(--t3)" }}>{hint}</div>
      )}
    </div>
  );
}

function StatusBadge({ status }: { status: SymbolStatus }) {
  const map: Record<SymbolStatus, { bg: string; color: string; label: string }> = {
    complete: { bg: "var(--green-dim)", color: "var(--green)", label: "COMPLETE" },
    partial:  { bg: "var(--amber-dim)", color: "var(--amber)", label: "PARTIAL" },
    missing:  { bg: "var(--red-dim)",   color: "var(--red)",   label: "MISSING" },
  };
  const s = map[status];
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      padding: "2px 6px",
      borderRadius: 3,
      background: s.bg,
      color: s.color,
      letterSpacing: "0.06em",
    }}>
      {s.label}
    </span>
  );
}

function CoverageBar({ pct }: { pct: number }) {
  const color = pct >= 95 ? "var(--green)" : pct >= 5 ? "var(--amber)" : "var(--red)";
  const bg = pct >= 95 ? "var(--green-dim)" : pct >= 5 ? "var(--amber-dim)" : "var(--red-dim)";
  return (
    <div style={{
      position: "relative",
      width: 80,
      height: 6,
      background: "var(--bg3)",
      borderRadius: 2,
      overflow: "hidden",
    }}>
      <div style={{
        position: "absolute",
        top: 0, left: 0,
        width: `${Math.min(100, Math.max(0, pct))}%`,
        height: "100%",
        background: color,
      }} />
    </div>
  );
}

function TierDots({ sym, endpointCols }: { sym: DaySymbol; endpointCols: string[] }) {
  // Filter the hardcoded tier lists to only columns the API actually returned.
  const l1 = TIER_L1.filter((c) => endpointCols.includes(c));
  const l2 = TIER_L2.filter((c) => endpointCols.includes(c));
  const l3 = TIER_L3.filter((c) => endpointCols.includes(c));
  const tiers = [
    { name: "L1", pct: tierStatus(l1, sym.rows_per_endpoint) },
    { name: "L2", pct: tierStatus(l2, sym.rows_per_endpoint) },
    { name: "L3", pct: tierStatus(l3, sym.rows_per_endpoint) },
  ];
  return (
    <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
      {tiers.map((t) => (
        <div
          key={t.name}
          title={`${t.name}: ${t.pct}%`}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 2,
          }}
        >
          <span style={{
            width: 6,
            height: 6,
            borderRadius: "50%",
            background: tierColor(t.pct),
            display: "inline-block",
          }} />
          <span style={{ fontSize: 8, color: "var(--t3)", fontFamily: "var(--font-space-mono), Space Mono, monospace" }}>
            {t.name}
          </span>
        </div>
      ))}
    </div>
  );
}

function SymbolRow({ sym, endpointCols, onClick }: { sym: DaySymbol; endpointCols: string[]; onClick: () => void }) {
  return (
    <tr
      onClick={onClick}
      style={{
        cursor: "pointer",
        borderBottom: "1px solid var(--line)",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.background = "var(--bg2)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; }}
    >
      <td style={tdStyle}>
        <span style={{ color: "var(--t0)", fontWeight: 700 }}>{sym.base}</span>
        <span style={{ color: "var(--t3)", marginLeft: 6 }}>{sym.binance_id}</span>
      </td>
      <td style={{ ...tdStyle, textAlign: "right" }}>
        <span style={{ color: "var(--t1)" }}>{sym.total_rows.toLocaleString()}</span>
        <span style={{ color: "var(--t3)" }}> / 1440</span>
      </td>
      <td style={tdStyle}>
        <CoverageBar pct={sym.completeness_pct} />
      </td>
      <td style={{ ...tdStyle, textAlign: "right" }}>
        <span style={{ color: "var(--t1)", fontFamily: "var(--font-space-mono), Space Mono, monospace" }}>
          {sym.completeness_pct.toFixed(1)}%
        </span>
      </td>
      <td style={tdStyle}>
        <TierDots sym={sym} endpointCols={endpointCols} />
      </td>
      <td style={tdStyle}>
        <StatusBadge status={sym.status} />
      </td>
    </tr>
  );
}

const tdStyle: React.CSSProperties = {
  padding: "8px 10px",
  fontSize: 10,
  color: "var(--t1)",
  verticalAlign: "middle",
};

// ─── Page ────────────────────────────────────────────────────────────────────

export default function DayDetailPage() {
  const params = useParams<{ date: string }>();
  const router = useRouter();
  const targetDate = params?.date ?? "";

  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [filter, setFilter] = useState<"all" | "partial" | "missing">("all");

  useEffect(() => {
    if (!targetDate) return;
    let cancelled = false;
    setState({ kind: "loading" });

    fetch(`${API_BASE}/api/compiler/days/${targetDate}`, { credentials: "include" })
      .then(async (r) => {
        if (cancelled) return;
        if (r.status === 401) {
          setState({ kind: "error", message: "Session expired. Please log in again." });
          return;
        }
        if (!r.ok) {
          setState({ kind: "error", message: `Day endpoint returned ${r.status}` });
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

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 1200, margin: "0 auto" }}>
        {/* Breadcrumb + back nav */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
          <button
            onClick={() => router.push("/compiler/coverage")}
            style={{
              background: "transparent",
              border: "1px solid var(--line)",
              borderRadius: 3,
              color: "var(--t2)",
              padding: "4px 10px",
              fontSize: 9,
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              cursor: "pointer",
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
            }}
            onMouseEnter={(e) => { e.currentTarget.style.color = "var(--t0)"; e.currentTarget.style.borderColor = "var(--line2)"; }}
            onMouseLeave={(e) => { e.currentTarget.style.color = "var(--t2)"; e.currentTarget.style.borderColor = "var(--line)"; }}
          >
            ← Coverage
          </button>
          <SectionLabel>Compiler · Day Detail</SectionLabel>
        </div>

        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
          fontFamily: "var(--font-space-mono), Space Mono, monospace",
        }}>
          {targetDate}
        </h1>

        {state.kind === "loading" && (
          <>
            <KPIGridSkeleton count={5} />
            <TableSkeleton rows={12} columns={[140, 100, 80, 60, 80, 70]} />
          </>
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
          <DayContent
            data={state.data}
            filter={filter}
            setFilter={setFilter}
            onSymbolClick={(base) => router.push(`/compiler/symbols?q=${encodeURIComponent(base)}`)}
          />
        )}
      </div>
    </div>
  );
}

// ─── Ready content ───────────────────────────────────────────────────────────

function DayContent({
  data,
  filter,
  setFilter,
  onSymbolClick,
}: {
  data: DayResponse;
  filter: "all" | "partial" | "missing";
  setFilter: (f: "all" | "partial" | "missing") => void;
  onSymbolClick: (base: string) => void;
}) {
  const filteredSymbols =
    filter === "partial" ? data.symbols.filter((s) => s.status === "partial") :
    filter === "missing" ? data.symbols.filter((s) => s.status === "missing") :
    data.symbols;

  const coveragePctColor =
    data.day_completeness_pct >= 95 ? "var(--green)" :
    data.day_completeness_pct >= 50 ? "var(--amber)" :
    "var(--red)";

  return (
    <>
      {/* Suspected duplicates warning. Fires when total_rows on this day
          exceeds 1.3× the expected ceiling (symbols × 1440), which is
          how the 1/17, 2/16, 2/23 dupe-row situation manifests: two
          ingestion sources wrote rows with different timestamp
          granularities and they coexist on the same primary key. */}
      {data.suspected_duplicates && (
        <div style={{
          background: "var(--amber-dim)",
          border: "1px solid var(--amber)",
          borderRadius: 6,
          padding: "12px 16px",
          marginBottom: 16,
          display: "flex",
          alignItems: "flex-start",
          gap: 12,
        }}>
          <div style={{
            fontSize: 9,
            fontWeight: 700,
            letterSpacing: "0.12em",
            color: "var(--amber)",
            textTransform: "uppercase",
            flexShrink: 0,
          }}>
            ⚠ Suspected Duplicates
          </div>
          <div style={{ fontSize: 10, color: "var(--t1)", lineHeight: 1.5 }}>
            This day has <strong style={{ color: "var(--t0)" }}>
              {data.total_rows.toLocaleString()}
            </strong> total rows — about <strong style={{ color: "var(--t0)" }}>
              {data.duplicate_factor}×
            </strong> the expected ceiling of {(data.symbols_with_data * 1440).toLocaleString()}{" "}
            (symbols × 1440). Two ingestion sources likely wrote rows with
            different timestamp granularities and now coexist on the
            primary key. Per-endpoint coverage math still works correctly,
            but the row count is bloated.
          </div>
        </div>
      )}

      {/* KPI strip */}
      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(5, 1fr)",
        gap: 10,
        marginBottom: 24,
      }}>
        <KPICard
          label="Coverage"
          value={`${data.day_completeness_pct.toFixed(1)}%`}
          hint={data.has_job_truth ? "vs job truth" : "vs fetched set"}
          color={coveragePctColor}
        />
        <KPICard
          label="Expected"
          value={data.expected_symbols.toLocaleString("en-US")}
          hint={data.has_job_truth ? "from compiler_jobs" : "from symbols_with_data"}
        />
        <KPICard
          label="Complete"
          value={data.symbols_complete.toLocaleString("en-US")}
          hint="≥ 1440 rows"
          color="var(--green)"
        />
        <KPICard
          label="Partial"
          value={data.symbols_partial.toLocaleString("en-US")}
          hint="1 – 1439 rows"
          color={data.symbols_partial > 0 ? "var(--amber)" : undefined}
        />
        <KPICard
          label="Missing"
          value={data.symbols_missing.toLocaleString("en-US")}
          hint="no rows on this day"
          color={data.symbols_missing > 0 ? "var(--red)" : undefined}
        />
      </div>

      {/* Job metadata card */}
      {data.job && (
        <div style={{
          background: "var(--bg2)",
          border: "1px solid var(--line)",
          borderRadius: 6,
          padding: "14px 18px",
          marginBottom: 24,
        }}>
          <SectionLabel>Compiler Job</SectionLabel>
          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(4, 1fr)",
            gap: 16,
            fontSize: 10,
            color: "var(--t1)",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}>
            <div>
              <div style={{ fontSize: 8, color: "var(--t3)", textTransform: "uppercase", marginBottom: 3 }}>Status</div>
              <div>{data.job.status}</div>
            </div>
            <div>
              <div style={{ fontSize: 8, color: "var(--t3)", textTransform: "uppercase", marginBottom: 3 }}>Triggered By</div>
              <div>{data.job.triggered_by ?? "—"}</div>
            </div>
            <div>
              <div style={{ fontSize: 8, color: "var(--t3)", textTransform: "uppercase", marginBottom: 3 }}>Run Tag</div>
              <div>{data.job.run_tag ?? "—"}</div>
            </div>
            <div>
              <div style={{ fontSize: 8, color: "var(--t3)", textTransform: "uppercase", marginBottom: 3 }}>Rows Written</div>
              <div>{data.job.rows_written.toLocaleString()}</div>
            </div>
            {data.job.error_msg && (
              <div style={{ gridColumn: "1 / -1", marginTop: 8 }}>
                <div style={{ fontSize: 8, color: "var(--t3)", textTransform: "uppercase", marginBottom: 3 }}>Error</div>
                <div style={{ color: "var(--red)", wordBreak: "break-word" }}>{data.job.error_msg}</div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Filter chips */}
      <div style={{ display: "flex", gap: 6, marginBottom: 12 }}>
        {(["all", "partial", "missing"] as const).map((f) => {
          const active = filter === f;
          const count =
            f === "all"     ? data.symbols.length :
            f === "partial" ? data.symbols_partial :
                              data.symbols_missing;
          return (
            <button
              key={f}
              onClick={() => setFilter(f)}
              style={{
                background: active ? "var(--bg3)" : "transparent",
                border: "1px solid var(--line)",
                borderRadius: 4,
                padding: "4px 10px",
                fontSize: 9,
                fontWeight: 700,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                color: active ? "var(--t0)" : "var(--t2)",
                cursor: "pointer",
                fontFamily: "var(--font-space-mono), Space Mono, monospace",
              }}
            >
              {f} · {count}
            </button>
          );
        })}
      </div>

      {/* Symbol table */}
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        overflow: "hidden",
      }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "var(--font-space-mono), Space Mono, monospace" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid var(--line)" }}>
              {["Symbol", "Rows / Total", "Coverage", "%", "Tiers", "Status"].map((h, i) => (
                <th key={h} style={{
                  fontSize: 9, fontWeight: 700, letterSpacing: "0.08em",
                  color: "var(--t3)", textTransform: "uppercase",
                  textAlign: i === 1 || i === 3 ? "right" : "left",
                  padding: "10px 10px 12px",
                }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredSymbols.map((sym) => (
              <SymbolRow
                key={sym.symbol_id}
                sym={sym}
                endpointCols={data.endpoint_cols}
                onClick={() => onSymbolClick(sym.base)}
              />
            ))}
            {filteredSymbols.length === 0 && (
              <tr>
                <td colSpan={6} style={{
                  padding: 24,
                  textAlign: "center",
                  fontSize: 10,
                  color: "var(--t3)",
                }}>
                  No symbols match the current filter.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}

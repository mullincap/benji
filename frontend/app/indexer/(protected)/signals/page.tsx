"use client";

/**
 * frontend/app/indexer/(protected)/signals/page.tsx
 * =================================================
 * Indexer Signals page — read-only view of user_mgmt.daily_signals.
 *
 * Data source:
 *   GET /api/indexer/signals?days=90&source=<live|backtest|research>
 *   (omit source param for all sources)
 *
 * The list endpoint already pre-aggregates each batch's symbol list via a
 * LATERAL/jsonb_agg subquery, so clicking a row just toggles inline expand
 * — no second fetch to /api/indexer/signals/{id} needed.
 *
 * Filter chips: ALL · LIVE · BACKTEST · RESEARCH. Changing the chip
 * triggers a re-fetch (the source filter is server-side so the page reads
 * a fresh list scoped to the chosen source).
 *
 * No polling — daily_signals advances at most a few times a day, the user
 * can refresh manually if needed. Matches the Coverage page's no-polling
 * stance for cadence-bound data.
 */

import { useCallback, useEffect, useState } from "react";
import { TableSkeleton } from "../../../components/Skeleton";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";
const LOOKBACK_DAYS = 90;

// ─── Response shapes ─────────────────────────────────────────────────────────

type SignalSource = "live" | "backtest" | "research";

type SignalSymbol = {
  rank: number;
  base: string;
  weight: number | null;
};

type DailySignal = {
  signal_batch_id: string;
  signal_date: string | null;
  strategy_version_id: string | null;
  signal_source: SignalSource;
  sit_flat: boolean;
  filter_name: string | null;
  filter_reason: string | null;
  computed_at: string | null;
  symbol_count: number;
  symbols: SignalSymbol[];
};

type SignalsResponse = {
  lookback_days: number;
  source_filter: SignalSource | null;
  signals_returned: number;
  signals: DailySignal[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; signals: DailySignal[] };

type FilterKey = "all" | SignalSource;

const FILTER_CHIPS: { key: FilterKey; label: string }[] = [
  { key: "all",      label: "All" },
  { key: "live",     label: "Live" },
  { key: "backtest", label: "Backtest" },
  { key: "research", label: "Research" },
];

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

function FilterChip({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      style={{
        background: active ? "var(--bg4)" : "var(--bg2)",
        border: `1px solid ${active ? "var(--module-accent)" : "var(--line)"}`,
        borderRadius: 4,
        padding: "6px 14px",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.08em",
        textTransform: "uppercase",
        color: active ? "var(--t0)" : "var(--t2)",
        cursor: "pointer",
        fontFamily: "var(--font-space-mono), Space Mono, monospace",
        transition: "all 0.15s ease",
      }}
      onMouseEnter={(e) => { if (!active) e.currentTarget.style.color = "var(--t1)"; }}
      onMouseLeave={(e) => { if (!active) e.currentTarget.style.color = "var(--t2)"; }}
    >
      {label}
    </button>
  );
}

function SourceBadge({ source }: { source: SignalSource }) {
  const color =
    source === "live" ? "var(--green)" :
    source === "backtest" ? "var(--amber)" :
    "var(--t1)";
  const bg =
    source === "live" ? "var(--green-dim)" :
    source === "backtest" ? "var(--amber-dim)" :
    "var(--bg3)";
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color,
      background: bg,
      border: `1px solid ${color}`,
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      {source}
    </span>
  );
}

function SitFlatBadge({ sitFlat }: { sitFlat: boolean }) {
  if (sitFlat) {
    return (
      <span style={{
        display: "inline-block",
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.08em",
        textTransform: "uppercase",
        color: "var(--red)",
        background: "var(--red-dim)",
        border: "1px solid var(--red)",
        borderRadius: 3,
        padding: "2px 6px",
      }}>
        Sit Flat
      </span>
    );
  }
  return (
    <span style={{
      display: "inline-block",
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: "var(--green)",
      background: "var(--green-dim)",
      border: "1px solid var(--green)",
      borderRadius: 3,
      padding: "2px 6px",
    }}>
      Active
    </span>
  );
}

function Th({ children, align = "left" }: { children: React.ReactNode; align?: "left" | "right" }) {
  return (
    <th style={{
      fontSize: 9, fontWeight: 700, letterSpacing: "0.12em",
      color: "var(--t3)", textTransform: "uppercase",
      textAlign: align,
      padding: "8px 10px",
      borderBottom: "1px solid var(--line)",
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
      padding: "10px 10px",
      verticalAlign: "middle",
    }}>
      {children}
    </td>
  );
}

function SymbolList({ symbols }: { symbols: SignalSymbol[] }) {
  if (symbols.length === 0) {
    return (
      <div style={{ fontSize: 10, color: "var(--t2)" }}>
        No symbols selected for this batch.
      </div>
    );
  }
  return (
    <div style={{
      display: "flex",
      flexWrap: "wrap",
      gap: 6,
    }}>
      {symbols.map((s) => (
        <div
          key={`${s.rank}-${s.base}`}
          title={s.weight !== null ? `weight ${s.weight}` : undefined}
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 6,
            fontSize: 10,
            color: "var(--t1)",
            background: "var(--bg3)",
            border: "1px solid var(--line)",
            borderRadius: 3,
            padding: "4px 8px",
            fontFamily: "var(--font-space-mono), Space Mono, monospace",
          }}
        >
          <span style={{ color: "var(--t3)", fontSize: 9 }}>
            #{s.rank}
          </span>
          <span style={{ color: "var(--t0)", fontWeight: 700 }}>
            {s.base}
          </span>
          {s.weight !== null && (
            <span style={{ color: "var(--t2)", fontSize: 9 }}>
              {s.weight}
            </span>
          )}
        </div>
      ))}
    </div>
  );
}

function SignalRow({
  signal,
  expanded,
  onToggle,
}: {
  signal: DailySignal;
  expanded: boolean;
  onToggle: () => void;
}) {
  const date = signal.signal_date ?? "—";
  const versionLabel = signal.strategy_version_id
    ? signal.strategy_version_id.slice(0, 8)
    : "—";
  const filterName = signal.filter_name ?? "—";

  return (
    <>
      <tr
        onClick={onToggle}
        style={{
          borderTop: "1px solid var(--line)",
          cursor: "pointer",
          background: expanded ? "var(--bg3)" : "transparent",
        }}
        onMouseEnter={(e) => { if (!expanded) e.currentTarget.style.background = "var(--bg3)"; }}
        onMouseLeave={(e) => { if (!expanded) e.currentTarget.style.background = "transparent"; }}
      >
        <Td>
          <span style={{
            display: "inline-block",
            width: 10,
            color: "var(--t3)",
            fontSize: 9,
          }}>
            {expanded ? "▾" : "▸"}
          </span>{" "}
          {date}
        </Td>
        <Td><SourceBadge source={signal.signal_source} /></Td>
        <Td><SitFlatBadge sitFlat={signal.sit_flat} /></Td>
        <Td>{filterName}</Td>
        <Td>
          <code
            title={signal.strategy_version_id ?? undefined}
            style={{ color: "var(--t2)", fontSize: 9 }}
          >
            {versionLabel}
          </code>
        </Td>
        <Td align="right">{signal.symbol_count.toLocaleString("en-US")}</Td>
      </tr>
      {expanded && (
        <tr style={{ background: "var(--bg3)" }}>
          <td colSpan={6} style={{ padding: "12px 16px 16px 32px" }}>
            {signal.filter_reason && (
              <div style={{
                fontSize: 10,
                color: "var(--t2)",
                marginBottom: 12,
                fontStyle: "italic",
              }}>
                {signal.filter_reason}
              </div>
            )}
            <SymbolList symbols={signal.symbols} />
          </td>
        </tr>
      )}
    </>
  );
}

function SignalsTable({
  signals,
  expandedId,
  onToggle,
}: {
  signals: DailySignal[];
  expandedId: string | null;
  onToggle: (id: string) => void;
}) {
  if (signals.length === 0) {
    return (
      <div style={{
        background: "var(--bg2)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        padding: "20px 24px",
        fontSize: 10,
        color: "var(--t2)",
      }}>
        No signals match the active filter in the last {LOOKBACK_DAYS} days.
      </div>
    );
  }

  return (
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
            <Th>Date</Th>
            <Th>Source</Th>
            <Th>Status</Th>
            <Th>Filter</Th>
            <Th>Version</Th>
            <Th align="right">Symbols</Th>
          </tr>
        </thead>
        <tbody>
          {signals.map((s) => (
            <SignalRow
              key={s.signal_batch_id}
              signal={s}
              expanded={expandedId === s.signal_batch_id}
              onToggle={() => onToggle(s.signal_batch_id)}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

export default function IndexerSignalsPage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [filter, setFilter] = useState<FilterKey>("all");
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const fetchSignals = useCallback(async (sourceFilter: FilterKey) => {
    setState({ kind: "loading" });
    try {
      const params = new URLSearchParams({ days: String(LOOKBACK_DAYS) });
      if (sourceFilter !== "all") params.set("source", sourceFilter);
      const res = await fetch(
        `${API_BASE}/api/indexer/signals?${params.toString()}`,
        { credentials: "include" },
      );
      if (res.status === 401) {
        setState({ kind: "error", message: "Session expired. Please log in again." });
        return;
      }
      if (!res.ok) {
        setState({ kind: "error", message: `Signals endpoint returned ${res.status}` });
        return;
      }
      const data = (await res.json()) as SignalsResponse;
      setState({ kind: "ready", signals: data.signals });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setState({ kind: "error", message: `Network error: ${message}` });
    }
  }, []);

  useEffect(() => {
    fetchSignals(filter);
  }, [fetchSignals, filter]);

  function handleFilterChange(next: FilterKey) {
    if (next === filter) return;
    setExpandedId(null);
    setFilter(next);
  }

  function handleToggle(id: string) {
    setExpandedId((prev) => (prev === id ? null : id));
  }

  return (
    <div style={{ background: "var(--bg0)", padding: 28, minHeight: "100%" }}>
      <div style={{ maxWidth: 1100, margin: "0 auto" }}>
        <SectionLabel>Indexer · Signals</SectionLabel>
        <h1 style={{
          fontSize: 24, fontWeight: 700, color: "var(--t0)",
          margin: 0, marginBottom: 24,
          letterSpacing: "-0.01em",
        }}>
          Daily Signals
        </h1>

        <div style={{
          display: "flex",
          gap: 8,
          marginBottom: 16,
          flexWrap: "wrap",
        }}>
          {FILTER_CHIPS.map((chip) => (
            <FilterChip
              key={chip.key}
              label={chip.label}
              active={filter === chip.key}
              onClick={() => handleFilterChange(chip.key)}
            />
          ))}
        </div>

        {state.kind === "loading" && (
          <TableSkeleton rows={8} columns={[100, 90, 80, 120, 80, 70]} />
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
          <SignalsTable
            signals={state.signals}
            expandedId={expandedId}
            onToggle={handleToggle}
          />
        )}
      </div>
    </div>
  );
}

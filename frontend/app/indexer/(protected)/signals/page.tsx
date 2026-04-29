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
  conviction_roi_x: number | null;
  symbols: SignalSymbol[];
};

type SignalsResponse = {
  lookback_days: number;
  source_filter: SignalSource | null;
  signals_returned: number;
  conviction_kill_y: number;
  signals: DailySignal[];
};

type LoadState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; signals: DailySignal[]; killY: number };

// "today" is a client-side date filter (signal_date == current UTC day),
// not a source filter. "research" maps to the server-side source param.
// "live" + "backtest" filters retired in favour of the simpler All / Today / Research
// triad: Today is the operator's primary "what's happening right now" view,
// Research is for sandboxed strategy experiments.
type FilterKey = "all" | "today" | "research";

// Current UTC date as YYYY-MM-DD. Computed at call time so the Today
// filter automatically picks up the next UTC day-roll without a refresh.
function utcToday(): string {
  return new Date().toISOString().slice(0, 10);
}

const FILTER_CHIPS: { key: FilterKey; label: string }[] = [
  { key: "all",      label: "All" },
  { key: "today",    label: "Today" },
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
  // Plain text — no badge framing. The previous green "LIVE" pill could be
  // misread at a glance as a passing-filter indicator (which is what the
  // date color now encodes).
  return (
    <span style={{
      fontSize: 9,
      fontWeight: 700,
      letterSpacing: "0.08em",
      textTransform: "uppercase",
      color: "var(--t1)",
      fontFamily: "var(--font-space-mono), Space Mono, monospace",
    }}>
      {source}
    </span>
  );
}

// Status badge — four states reflecting the actual outcome of the signal:
//
//   Sit Flat — filter (TG / Disp / combined) fired. Strategy did not deploy.
//   Pending  — filter passed BUT conviction gate hasn't been evaluated yet.
//              Today's row before 06:35 UTC, or a fresh research/backtest
//              row whose conviction wasn't computed.
//   Fail     — filter passed AND conviction gate evaluated AND failed
//              (conviction_roi_x < kill_y → strategy killed at 06:35).
//   Active   — filter passed AND conviction gate passed → strategy deployed
//              for the trade window. Older rows whose conviction wasn't
//              tracked also default here for backward compat.
function StatusBadge({
  sitFlat,
  convictionRoiX,
  killY,
  signalDate,
}: {
  sitFlat: boolean;
  convictionRoiX: number | null;
  killY: number;
  signalDate: string | null;
}) {
  const baseStyle: React.CSSProperties = {
    display: "inline-block",
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    borderRadius: 3,
    padding: "2px 6px",
    border: "1px solid",
  };

  if (sitFlat) {
    return (
      <span style={{
        ...baseStyle,
        color: "var(--red)",
        background: "var(--red-dim)",
        borderColor: "var(--red)",
      }}>
        Sit Flat
      </span>
    );
  }

  // Conviction gate not evaluated. Treat as "Pending" only when this is
  // today's row (so the conviction window genuinely hasn't fired yet);
  // for past rows whose conviction simply wasn't tracked, fall through
  // to the legacy "Active" rendering to preserve historical UX.
  if (convictionRoiX === null) {
    const isToday = signalDate === utcToday();
    if (isToday) {
      return (
        <span style={{
          ...baseStyle,
          color: "var(--amber)",
          background: "var(--amber-dim)",
          borderColor: "var(--amber)",
        }}>
          Pending
        </span>
      );
    }
    // Old/missing conviction → keep Active label (backward-compat)
    return (
      <span style={{
        ...baseStyle,
        color: "var(--green)",
        background: "var(--green-dim)",
        borderColor: "var(--green)",
      }}>
        Active
      </span>
    );
  }

  // Conviction evaluated; compare to kill_y threshold.
  if (convictionRoiX < killY) {
    return (
      <span style={{
        ...baseStyle,
        color: "var(--red)",
        background: "var(--red-dim)",
        borderColor: "var(--red)",
      }}>
        Fail
      </span>
    );
  }
  return (
    <span style={{
      ...baseStyle,
      color: "var(--green)",
      background: "var(--green-dim)",
      borderColor: "var(--green)",
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

function ConvictionBadge({ roiX, killY, sitFlat }: { roiX: number | null; killY: number; sitFlat: boolean }) {
  if (sitFlat) {
    return <span style={{ fontSize: 9, color: "var(--t3)" }}>—</span>;
  }
  if (roiX === null) {
    return <span style={{ fontSize: 9, color: "var(--t3)" }}>no data</span>;
  }
  const passed = roiX >= killY;
  return (
    <span style={{
      fontSize: 9,
      fontWeight: 700,
      fontFamily: "var(--font-space-mono), Space Mono, monospace",
      color: passed ? "var(--green)" : "var(--red)",
    }}>
      {roiX.toFixed(2)}%
      <span style={{
        fontSize: 8,
        fontWeight: 400,
        color: "var(--t3)",
        marginLeft: 4,
      }}>
        / {killY.toFixed(1)}%
      </span>
      {" "}
      <span style={{
        fontSize: 8,
        fontWeight: 700,
        letterSpacing: "0.06em",
        padding: "1px 4px",
        borderRadius: 2,
        background: passed ? "var(--green-dim)" : "var(--red-dim)",
        color: passed ? "var(--green)" : "var(--red)",
        border: `1px solid ${passed ? "var(--green)" : "var(--red)"}`,
      }}>
        {passed ? "PASS" : "FAIL"}
      </span>
    </span>
  );
}

function SignalRow({
  signal,
  expanded,
  onToggle,
  killY,
  rawRoiVal,
  rawRoiLoading,
  stratRoi,
  stratRoiLoading,
}: {
  signal: DailySignal;
  expanded: boolean;
  onToggle: () => void;
  killY: number;
  rawRoiVal: number | null;
  rawRoiLoading: boolean;
  stratRoi: StratRoi | null;
  stratRoiLoading: boolean;
}) {
  const date = signal.signal_date ?? "—";
  const versionLabel = signal.strategy_version_id
    ? signal.strategy_version_id.slice(0, 8)
    : "—";
  const filterName = signal.filter_name ?? "—";

  // Date text color encodes the deploy outcome end-to-end:
  //   green = filter passed AND conviction gate passed → trade went live
  //   red   = filter triggered (sit_flat) OR conviction gate killed the deploy
  //           (conviction_roi_x < killY) → no live trade
  //   amber = pending conv gate (today's row, now < 06:35 UTC)
  // Note: a deploy that passes the filter but fails the conviction gate is
  // ALSO red, since no live trade resulted. (Previously the green/red split
  // tracked sit_flat alone, which painted conviction-killed days green —
  // misleading because the trade never actually went live.)
  const dateColor = (() => {
    if (!signal.signal_date) return "var(--t1)";
    const nowUtc = new Date();
    const todayUtc = nowUtc.toISOString().slice(0, 10);
    if (signal.signal_date === todayUtc) {
      const cutoff = new Date(`${todayUtc}T06:35:00Z`);
      if (nowUtc < cutoff) return "var(--amber)";
    }
    if (signal.sit_flat) return "var(--red)";
    if (signal.conviction_roi_x !== null && signal.conviction_roi_x < killY) {
      return "var(--red)";
    }
    return "var(--green)";
  })();

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
          <span style={{ color: dateColor }}>{date}</span>
        </Td>
        <Td><SourceBadge source={signal.signal_source} /></Td>
        <Td>
          <StatusBadge
            sitFlat={signal.sit_flat}
            convictionRoiX={signal.conviction_roi_x}
            killY={killY}
            signalDate={signal.signal_date}
          />
        </Td>
        <Td>{filterName}</Td>
        <Td><ConvictionBadge roiX={signal.conviction_roi_x} killY={killY} sitFlat={signal.sit_flat} /></Td>
        <Td align="right">
          {signal.sit_flat ? (
            <span style={{ fontSize: 9, color: "var(--t3)" }}>—</span>
          ) : rawRoiLoading ? (
            <span style={{ fontSize: 9, color: "var(--t3)", fontStyle: "italic" }}>computing</span>
          ) : rawRoiVal === null ? (
            <span style={{ fontSize: 9, color: "var(--t3)" }}>—</span>
          ) : (
            <span style={{
              fontSize: 10,
              fontWeight: 700,
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              color: rawRoiVal >= 0 ? "var(--green)" : "var(--red)",
            }}>
              {rawRoiVal >= 0 ? "+" : ""}{rawRoiVal.toFixed(2)}%
            </span>
          )}
        </Td>
        <Td align="right">
          {signal.sit_flat ? (
            <span style={{ fontSize: 9, color: "var(--t3)" }}>—</span>
          ) : stratRoiLoading ? (
            <span style={{ fontSize: 9, color: "var(--t3)", fontStyle: "italic" }}>computing</span>
          ) : !stratRoi ? (
            <span style={{ fontSize: 9, color: "var(--t3)" }}>—</span>
          ) : (
            <span style={{
              fontSize: 10,
              fontWeight: 700,
              fontFamily: "var(--font-space-mono), Space Mono, monospace",
              color: stratRoi.return_pct >= 0 ? "var(--green)" : "var(--red)",
            }}>
              {stratRoi.return_pct >= 0 ? "+" : ""}{stratRoi.return_pct.toFixed(2)}%
              <span style={{
                fontSize: 8,
                fontWeight: 400,
                color: "var(--t3)",
                marginLeft: 4,
                textTransform: "uppercase",
                letterSpacing: "0.04em",
              }}>
                {stratRoi.exit_reason === "held" ? "" :
                 stratRoi.exit_reason === "no_entry" ? "no entry" :
                 stratRoi.exit_reason === "sit_flat" ? "" :
                 stratRoi.exit_reason === "stop_loss" ? "SL" :
                 stratRoi.exit_reason === "trailing_stop" ? "TSL" :
                 stratRoi.exit_reason === "early_kill" ? "KILL" :
                 stratRoi.exit_reason === "profit_take" ? "TP" :
                 stratRoi.exit_reason}
              </span>
            </span>
          )}
        </Td>
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
          <td colSpan={9} style={{ padding: "12px 16px 16px 32px" }}>
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
  killY,
  rawRoi,
  rawRoiLoading,
  stratRoi,
  stratRoiLoading,
}: {
  signals: DailySignal[];
  expandedId: string | null;
  onToggle: (id: string) => void;
  killY: number;
  rawRoi: Record<string, number>;
  rawRoiLoading: boolean;
  stratRoi: Record<string, StratRoi>;
  stratRoiLoading: boolean;
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
            <Th>Conviction</Th>
            <Th align="right">Raw ROI</Th>
            <Th align="right">
              <span title="Hardcoded strategy configs (L=1.33x, SL=-6%, TSL=-7.5%, Kill=0.3%@bar35, TP=9%). Will be dynamic per strategy version in a future update.">
                Strat ROI *
              </span>
            </Th>
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
              killY={killY}
              rawRoiVal={s.signal_date ? rawRoi[s.signal_date] ?? null : null}
              rawRoiLoading={rawRoiLoading}
              stratRoi={s.signal_date ? stratRoi[s.signal_date] ?? null : null}
              stratRoiLoading={stratRoiLoading}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Page ────────────────────────────────────────────────────────────────────

type StratRoi = { return_pct: number; exit_reason: string };

export default function IndexerSignalsPage() {
  const [state, setState] = useState<LoadState>({ kind: "loading" });
  const [filter, setFilter] = useState<FilterKey>("all");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [rawRoi, setRawRoi] = useState<Record<string, number>>({});
  const [rawRoiLoading, setRawRoiLoading] = useState(false);
  const [stratRoi, setStratRoi] = useState<Record<string, StratRoi>>({});
  const [stratRoiLoading, setStratRoiLoading] = useState(false);

  const fetchSignals = useCallback(async (sourceFilter: FilterKey) => {
    setState({ kind: "loading" });
    setRawRoi({});
    setStratRoi({});
    try {
      const params = new URLSearchParams({ days: String(LOOKBACK_DAYS) });
      // "today" is a client-side date filter (applied below in render path),
      // so it fetches everything from the server. Only "research" maps to
      // the source param; "all" omits it.
      if (sourceFilter === "research") params.set("source", "research");
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
      setState({ kind: "ready", signals: data.signals, killY: data.conviction_kill_y });

      // Fetch raw ROI + strat ROI in background
      setRawRoiLoading(true);
      fetch(`${API_BASE}/api/indexer/signals/raw-roi?days=${LOOKBACK_DAYS}`, { credentials: "include" })
        .then(async (r) => {
          if (r.ok) {
            const d = await r.json();
            setRawRoi(d.raw_roi || {});
          }
        })
        .catch(() => {})
        .finally(() => setRawRoiLoading(false));

      setStratRoiLoading(true);
      fetch(`${API_BASE}/api/indexer/signals/strat-roi?days=${LOOKBACK_DAYS}`, { credentials: "include" })
        .then(async (r) => {
          if (r.ok) {
            const d = await r.json();
            setStratRoi(d.strat_roi || {});
          }
        })
        .catch(() => {})
        .finally(() => setStratRoiLoading(false));
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
            signals={
              filter === "today"
                // Client-side date filter — server returned all sources, narrow to today UTC.
                // utcToday() is computed at render time, so the page picks up
                // the new UTC day without a manual refresh.
                ? state.signals.filter(
                    (s) => s.signal_date === utcToday(),
                  )
                : state.signals
            }
            expandedId={expandedId}
            onToggle={handleToggle}
            killY={state.killY}
            rawRoi={rawRoi}
            rawRoiLoading={rawRoiLoading}
            stratRoi={stratRoi}
            stratRoiLoading={stratRoiLoading}
          />
        )}
      </div>
    </div>
  );
}
